from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from ..models.model_postgre import (
    Chunk as PgChunk,
    Class as PgClass,
    Keyword as PgKeyword,
    Lesson as PgLesson,
    Subject as PgSubject,
    Topic as PgTopic,
    Image as PgImage,
    Video as PgVideo,
)
from ..services.doc_preview import get_view_url
from ..services.mongo_client import get_mongo_client
from ..services.neo_client import get_neo4j_session
from ..services.postgre_client import get_db
from ..services.user_semantic_search import semantic_search

router = APIRouter(prefix="/user/docs", tags=["UserDocs"])

mongo_bundle = get_mongo_client()
db_mongo = mongo_bundle["db"]

COL_CLASSES = "classes"
COL_SUBJECTS = "subjects"
COL_TOPICS = "topics"
COL_LESSONS = "lessons"
COL_CHUNKS = "chunks"
COL_KEYWORDS = "keywords"
COL_SAVED = "user_saved_chunks"
COL_IMAGES = "images"
COL_VIDEOS = "videos"


def _now():
    return datetime.now(timezone.utc)


def _actor(request: Request) -> str:
    return (
        request.headers.get("x-username")
        or request.headers.get("x-user")
        or request.headers.get("x-actor")
        or request.cookies.get("username")
        or "user"
    )


def _not_hidden_q():
    return {"$ne": "hidden"}


def _get_any(doc: Optional[dict], keys: list[str], default: Any = None):
    if not isinstance(doc, dict):
        return default
    for k in keys:
        if k in doc and doc.get(k) not in (None, ""):
            return doc.get(k)
    return default


def _pretty_class_name_from_text(raw: str) -> str:
    raw = str(raw or "").strip()
    if not raw:
        return ""
    low = raw.lower()
    if "lớp" in low or "lop" in low:
        return raw
    m = re.search(r"(\d{1,2})", raw)
    if m:
        return f"Lớp {int(m.group(1))}"
    return raw


def _pretty_class_name(doc: dict) -> str:
    raw = (
        _get_any(doc, ["className", "class_name", "name", "title"], "")
        or _get_any(doc, ["classID", "classId", "class_id"], "")
    )
    return _pretty_class_name_from_text(raw)


def _sort_key_by_number(s: str):
    m = re.search(r"(\d{1,2})", str(s or ""))
    return (int(m.group(1)) if m else 999, str(s or ""))


def _ensure_saved_index():
    try:
        db_mongo[COL_SAVED].create_index(
            [("username", 1), ("chunkID", 1), ("category", 1)],
            unique=True,
            name="uniq_user_chunk",
        )
    except Exception:
        pass


_ensure_saved_index()


def _id_match(prefix: str, value: str) -> dict:
    return {
        "$or": [
            {f"{prefix}ID": value},
            {f"{prefix}Id": value},
            {f"{prefix}_id": value},
        ]
    }


def _mongo_find_one_by_id(col_name: str, prefix: str, value: Optional[str]) -> dict:
    if not value:
        return {}
    return (
        db_mongo[col_name].find_one(
            {**_id_match(prefix, value), "status": _not_hidden_q()},
            {"_id": 0},
        )
        or {}
    )


def _mongo_find_chunk_direct(chunk_id: str) -> dict:
    return (
        db_mongo[COL_CHUNKS].find_one(
            {**_id_match("chunk", chunk_id), "status": _not_hidden_q()},
            {"_id": 0},
        )
        or {}
    )


def _pg_bundle_by_chunk_id(pg: Session, chunk_id: str) -> Optional[dict]:
    pg_chunk = pg.query(PgChunk).filter(PgChunk.chunk_id == chunk_id).first()
    if not pg_chunk:
        return None

    pg_lesson = (
        pg.query(PgLesson).filter(PgLesson.lesson_id == pg_chunk.lesson_id).first()
        if pg_chunk.lesson_id
        else None
    )
    pg_topic = (
        pg.query(PgTopic).filter(PgTopic.topic_id == pg_lesson.topic_id).first()
        if pg_lesson and pg_lesson.topic_id
        else None
    )
    pg_subject = (
        pg.query(PgSubject).filter(PgSubject.subject_id == pg_topic.subject_id).first()
        if pg_topic and pg_topic.subject_id
        else None
    )
    pg_class = (
        pg.query(PgClass).filter(PgClass.class_id == pg_subject.class_id).first()
        if pg_subject and pg_subject.class_id
        else None
    )

    return {
        "chunk": pg_chunk,
        "lesson": pg_lesson,
        "topic": pg_topic,
        "subject": pg_subject,
        "class": pg_class,
    }


def _mongo_chunk_from_pg(bundle: Optional[dict]) -> dict:
    if not bundle:
        return {}

    pg_chunk: PgChunk = bundle["chunk"]
    mongo_id = getattr(pg_chunk, "mongo_id", None)
    if not mongo_id:
        return {}

    found = None
    try:
        found = db_mongo[COL_CHUNKS].find_one(
            {"_id": ObjectId(str(mongo_id)), "status": _not_hidden_q()},
            {"_id": 0},
        )
    except Exception:
        found = None

    if not found:
        found = (
            db_mongo[COL_CHUNKS].find_one(
                {"mongo_id": str(mongo_id), "status": _not_hidden_q()},
                {"_id": 0},
            )
            or {}
        )

    if not found:
        return {}

    patched = dict(found)
    patched.setdefault("chunkID", pg_chunk.chunk_id)
    patched.setdefault("chunkName", pg_chunk.chunk_name)
    patched.setdefault("chunkType", pg_chunk.chunk_type)

    pg_lesson: Optional[PgLesson] = bundle.get("lesson")
    pg_topic: Optional[PgTopic] = bundle.get("topic")
    pg_subject: Optional[PgSubject] = bundle.get("subject")
    pg_class: Optional[PgClass] = bundle.get("class")

    if pg_lesson:
        patched.setdefault("lessonID", pg_lesson.lesson_id)
    if pg_topic:
        patched.setdefault("topicID", pg_topic.topic_id)
    if pg_subject:
        patched.setdefault("subjectID", pg_subject.subject_id)
    if pg_class:
        patched.setdefault("classID", pg_class.class_id)

    return patched


def _resolve_chunk(chunk_id: str, pg: Optional[Session]) -> tuple[dict, Optional[dict]]:
    mongo_chunk = _mongo_find_chunk_direct(chunk_id)
    if mongo_chunk:
        return mongo_chunk, None

    if pg is None:
        return {}, None

    bundle = _pg_bundle_by_chunk_id(pg, chunk_id)
    if not bundle:
        return {}, None

    mongo_from_pg = _mongo_chunk_from_pg(bundle)
    if mongo_from_pg:
        return mongo_from_pg, bundle

    return {}, bundle


def _build_doc_from_pg(bundle: dict, *, category: str, username: str) -> dict:
    pg_chunk: PgChunk = bundle["chunk"]
    pg_lesson: Optional[PgLesson] = bundle.get("lesson")
    pg_topic: Optional[PgTopic] = bundle.get("topic")
    pg_subject: Optional[PgSubject] = bundle.get("subject")
    pg_class: Optional[PgClass] = bundle.get("class")

    saved = db_mongo[COL_SAVED].find_one(
        {"username": username, "chunkID": pg_chunk.chunk_id, "category": category}
    )

    class_name = ""
    if pg_class:
        class_name = _pretty_class_name_from_text(pg_class.class_name or pg_class.class_id)

    return {
        "itemType": "chunk",
        "chunkID": pg_chunk.chunk_id,
        "chunkName": pg_chunk.chunk_name,
        "chunkType": pg_chunk.chunk_type or category,
        "chunkUrl": "",
        "chunkDescription": "",
        "keywords": [],
        "keywordItems": [],
        "images": [],
        "videos": [],
        "mediaStats": {"totalImages": 0, "totalVideos": 0},
        "isSaved": bool(saved),
        "class": {
            "classID": pg_class.class_id if pg_class else "",
            "className": class_name,
        },
        "subject": {
            "subjectID": pg_subject.subject_id if pg_subject else "",
            "subjectName": pg_subject.subject_name if pg_subject else "",
            "subjectUrl": "",
        },
        "topic": {
            "topicID": pg_topic.topic_id if pg_topic else "",
            "topicName": pg_topic.topic_name if pg_topic else "",
            "topicUrl": "",
        },
        "lesson": {
            "lessonID": pg_lesson.lesson_id if pg_lesson else "",
            "lessonName": pg_lesson.lesson_name if pg_lesson else "",
            "lessonType": "",
            "lessonUrl": "",
        },
        "category": category,
        "mappedDocuments": [],
    }


def _get_chunk_full(chunk: dict, *, category: str, username: str) -> dict:
    chunk_id = _get_any(chunk, ["chunkID", "chunkId", "chunk_id"], "")
    chunk_name = (
        _get_any(chunk, ["chunkName", "chunk_name", "name", "title"], "") or chunk_id
    )

    lesson_id = _get_any(chunk, ["lessonID", "lessonId", "lesson_id"], "")
    topic_id = _get_any(chunk, ["topicID", "topicId", "topic_id"], "")
    subject_id = _get_any(chunk, ["subjectID", "subjectId", "subject_id"], "")
    class_id = _get_any(chunk, ["classID", "classId", "class_id"], "")

    lesson = _mongo_find_one_by_id(COL_LESSONS, "lesson", lesson_id)
    if not topic_id:
        topic_id = _get_any(lesson, ["topicID", "topicId", "topic_id"], "")

    topic = _mongo_find_one_by_id(COL_TOPICS, "topic", topic_id)
    if not subject_id:
        subject_id = _get_any(topic, ["subjectID", "subjectId", "subject_id"], "")

    subject = _mongo_find_one_by_id(COL_SUBJECTS, "subject", subject_id)
    if not class_id:
        class_id = _get_any(subject, ["classID", "classId", "class_id"], "")

    cls = _mongo_find_one_by_id(COL_CLASSES, "class", class_id)

    saved = db_mongo[COL_SAVED].find_one(
        {"username": username, "chunkID": chunk_id, "category": category}
    )

    subject_name = (
        _get_any(subject, ["subjectName", "subject_name", "name", "title"], "")
        or subject_id
    )
    topic_name = (
        _get_any(topic, ["topicName", "topic_name", "name", "title"], "")
        or topic_id
    )
    lesson_name = (
        _get_any(lesson, ["lessonName", "lesson_name", "name", "title"], "")
        or lesson_id
    )

    class_name = _pretty_class_name(cls) or _pretty_class_name_from_text(class_id or "")

    return {
        "itemType": "chunk",
        "chunkID": chunk_id,
        "chunkName": chunk_name,
        "chunkType": (
            _get_any(chunk, ["chunkType", "chunk_type", "type", "chunkCategory", "category"], "")
            or category
        ),
        "chunkUrl": _get_any(chunk, ["chunkUrl", "chunk_url", "url"], "") or "",
        "chunkDescription": (
            _get_any(chunk, ["chunkDescription", "chunk_description", "description"], "") or ""
        ),
        "keywords": chunk.get("keywords") or chunk.get("keyword") or [],
        "keywordItems": [],
        "images": [],
        "videos": [],
        "mediaStats": {"totalImages": 0, "totalVideos": 0},
        "isSaved": bool(saved),
        "class": {"classID": class_id or "", "className": class_name or ""},
        "subject": {
            "subjectID": subject_id or "",
            "subjectName": subject_name or "",
            "subjectUrl": _get_any(subject, ["subjectUrl", "subject_url", "url"], "") or "",
        },
        "topic": {
            "topicID": topic_id or "",
            "topicName": topic_name or "",
            "topicUrl": _get_any(topic, ["topicUrl", "topic_url", "url"], "") or "",
        },
        "lesson": {
            "lessonID": lesson_id or "",
            "lessonName": lesson_name or "",
            "lessonType": _get_any(lesson, ["lessonType", "lesson_type", "type"], "") or "",
            "lessonUrl": _get_any(lesson, ["lessonUrl", "lesson_url", "url"], "") or "",
        },
        "category": category,
        "mappedDocuments": [],
    }


def _build_generic_doc(*, item_id: str, item_name: str, item_type: str, username: str, item_url: str = "", item_description: str = "", class_info: Optional[dict] = None, subject_info: Optional[dict] = None, topic_info: Optional[dict] = None, lesson_info: Optional[dict] = None, chunk_type: str = "") -> dict:
    save_category = item_type if item_type and item_type != "chunk" else "document"
    saved = db_mongo[COL_SAVED].find_one({"username": username, "chunkID": item_id, "category": save_category})
    return {
        "itemType": item_type or "chunk",
        "chunkID": item_id or "",
        "chunkName": item_name or item_id or "",
        "chunkType": chunk_type or item_type or "",
        "chunkUrl": item_url or "",
        "chunkDescription": item_description or "",
        "keywords": [],
        "keywordItems": [],
        "images": [],
        "videos": [],
        "mediaStats": {"totalImages": 0, "totalVideos": 0},
        "isSaved": bool(saved),
        "class": class_info or {"classID": "", "className": ""},
        "subject": subject_info or {"subjectID": "", "subjectName": "", "subjectUrl": ""},
        "topic": topic_info or {"topicID": "", "topicName": "", "topicUrl": ""},
        "lesson": lesson_info or {"lessonID": "", "lessonName": "", "lessonType": "", "lessonUrl": ""},
        "category": save_category,
        "mappedDocuments": [],
    }


def _find_many_ids(col_name: str, prefix: str, values: list[str]) -> list[dict]:
    vals = [str(v) for v in values if str(v or "").strip()]
    if not vals:
        return []
    return list(db_mongo[col_name].find({"$and": [{"status": _not_hidden_q()}, {"$or": [{f"{prefix}ID": {"$in": vals}}, {f"{prefix}Id": {"$in": vals}}, {f"{prefix}_id": {"$in": vals}}]}]}, {"_id": 0}))


def _load_mapped_chunks_for_target(target_type: str, target_id: str, *, username: str, pg: Optional[Session]) -> list[dict]:
    target_type = str(target_type or "").strip().lower()
    target_id = str(target_id or "").strip()
    if not target_type or not target_id:
        return []

    def _uniq(items: list[dict]) -> list[dict]:
        out = []
        seen = set()
        for item in items:
            cid = item.get("chunkID")
            if not cid or cid in seen:
                continue
            seen.add(cid)
            out.append(item)
        out.sort(key=lambda x: str(x.get("chunkName") or x.get("chunkID") or ""))
        return out

    if target_type == "chunk":
        mongo_chunk, pg_bundle = _resolve_chunk(target_id, pg)
        if mongo_chunk:
            return [_get_chunk_full(mongo_chunk, category="document", username=username)]
        if pg_bundle:
            return [_build_doc_from_pg(pg_bundle, category="document", username=username)]
        return []

    if target_type == "lesson":
        mongo_items = [_get_chunk_full(d, category="document", username=username) for d in db_mongo[COL_CHUNKS].find({**_id_match("lesson", target_id), "status": _not_hidden_q()}, {"_id": 0}).sort([("chunkName", 1)])]
        if mongo_items:
            return _uniq(mongo_items)
        if pg is not None:
            try:
                return _uniq([_build_doc_from_pg(_pg_bundle_by_chunk_id(pg, c.chunk_id), category="document", username=username) for c in pg.query(PgChunk).filter(PgChunk.lesson_id == target_id).all() if _pg_bundle_by_chunk_id(pg, c.chunk_id)])
            except Exception:
                return []
        return []

    if target_type == "topic":
        lessons = _find_many_ids(COL_LESSONS, "topic", [target_id])
        lesson_ids = [_get_any(x, ["lessonID", "lessonId", "lesson_id"], "") for x in lessons]
        mongo_items = [_get_chunk_full(d, category="document", username=username) for d in _find_many_ids(COL_CHUNKS, "lesson", lesson_ids)]
        if mongo_items:
            return _uniq(mongo_items)
        if pg is not None:
            try:
                lesson_ids = [l.lesson_id for l in pg.query(PgLesson).filter(PgLesson.topic_id == target_id).all()]
                out = []
                for c in pg.query(PgChunk).filter(PgChunk.lesson_id.in_(lesson_ids)).all() if lesson_ids else []:
                    bundle = _pg_bundle_by_chunk_id(pg, c.chunk_id)
                    if bundle:
                        out.append(_build_doc_from_pg(bundle, category="document", username=username))
                return _uniq(out)
            except Exception:
                return []
        return []

    if target_type == "subject":
        topics = _find_many_ids(COL_TOPICS, "subject", [target_id])
        topic_ids = [_get_any(x, ["topicID", "topicId", "topic_id"], "") for x in topics]
        lessons = _find_many_ids(COL_LESSONS, "topic", topic_ids)
        lesson_ids = [_get_any(x, ["lessonID", "lessonId", "lesson_id"], "") for x in lessons]
        mongo_items = [_get_chunk_full(d, category="document", username=username) for d in _find_many_ids(COL_CHUNKS, "lesson", lesson_ids)]
        if mongo_items:
            return _uniq(mongo_items)
        if pg is not None:
            try:
                topic_ids = [t.topic_id for t in pg.query(PgTopic).filter(PgTopic.subject_id == target_id).all()]
                lesson_ids = [l.lesson_id for l in pg.query(PgLesson).filter(PgLesson.topic_id.in_(topic_ids)).all()] if topic_ids else []
                out = []
                for c in pg.query(PgChunk).filter(PgChunk.lesson_id.in_(lesson_ids)).all() if lesson_ids else []:
                    bundle = _pg_bundle_by_chunk_id(pg, c.chunk_id)
                    if bundle:
                        out.append(_build_doc_from_pg(bundle, category="document", username=username))
                return _uniq(out)
            except Exception:
                return []
        return []

    if target_type == "class":
        subjects = _find_many_ids(COL_SUBJECTS, "class", [target_id])
        subject_ids = [_get_any(x, ["subjectID", "subjectId", "subject_id"], "") for x in subjects]
        topics = _find_many_ids(COL_TOPICS, "subject", subject_ids)
        topic_ids = [_get_any(x, ["topicID", "topicId", "topic_id"], "") for x in topics]
        lessons = _find_many_ids(COL_LESSONS, "topic", topic_ids)
        lesson_ids = [_get_any(x, ["lessonID", "lessonId", "lesson_id"], "") for x in lessons]
        mongo_items = [_get_chunk_full(d, category="document", username=username) for d in _find_many_ids(COL_CHUNKS, "lesson", lesson_ids)]
        if mongo_items:
            return _uniq(mongo_items)
        if pg is not None:
            try:
                subject_ids = [s.subject_id for s in pg.query(PgSubject).filter(PgSubject.class_id == target_id).all()]
                topic_ids = [t.topic_id for t in pg.query(PgTopic).filter(PgTopic.subject_id.in_(subject_ids)).all()] if subject_ids else []
                lesson_ids = [l.lesson_id for l in pg.query(PgLesson).filter(PgLesson.topic_id.in_(topic_ids)).all()] if topic_ids else []
                out = []
                for c in pg.query(PgChunk).filter(PgChunk.lesson_id.in_(lesson_ids)).all() if lesson_ids else []:
                    bundle = _pg_bundle_by_chunk_id(pg, c.chunk_id)
                    if bundle:
                        out.append(_build_doc_from_pg(bundle, category="document", username=username))
                return _uniq(out)
            except Exception:
                return []
        return []

    return []


def _resolve_entity_doc(entity_id: str, *, category: str, username: str, pg: Optional[Session]) -> Optional[dict]:
    kind = str(category or "").strip().lower()
    entity_id = str(entity_id or "").strip()
    if not entity_id:
        return None

    if kind in ("document", "chunk", "all", ""):
        mongo_chunk, pg_bundle = _resolve_chunk(entity_id, pg)
        if mongo_chunk:
            return _get_chunk_full(mongo_chunk, category="document", username=username)
        if pg_bundle:
            return _build_doc_from_pg(pg_bundle, category="document", username=username)
        return None

    if kind == "class":
        cls = _mongo_find_one_by_id(COL_CLASSES, "class", entity_id)
        class_info = {"classID": entity_id, "className": _pretty_class_name(cls) or _pretty_class_name_from_text(entity_id)}
        doc = _build_generic_doc(item_id=entity_id, item_name=class_info["className"], item_type="class", username=username, item_url=_get_any(cls, ["classUrl", "class_url", "url"], "") or "", item_description=_get_any(cls, ["classDescription", "class_description", "description"], "") or "", class_info=class_info)
        doc["mappedDocuments"] = _load_mapped_chunks_for_target("class", entity_id, username=username, pg=pg)
        return doc

    if kind == "subject":
        subject = _mongo_find_one_by_id(COL_SUBJECTS, "subject", entity_id)
        class_id = _get_any(subject, ["classID", "classId", "class_id"], "")
        cls = _mongo_find_one_by_id(COL_CLASSES, "class", class_id)
        class_info = {"classID": class_id or "", "className": _pretty_class_name(cls) or _pretty_class_name_from_text(class_id or "")}
        subject_info = {"subjectID": entity_id, "subjectName": _get_any(subject, ["subjectName", "subject_name", "name", "title"], "") or entity_id, "subjectUrl": _get_any(subject, ["subjectUrl", "subject_url", "url"], "") or ""}
        doc = _build_generic_doc(item_id=entity_id, item_name=subject_info["subjectName"], item_type="subject", username=username, item_url=subject_info["subjectUrl"], item_description=_get_any(subject, ["subjectDescription", "subject_description", "description"], "") or "", class_info=class_info, subject_info=subject_info)
        doc["mappedDocuments"] = _load_mapped_chunks_for_target("subject", entity_id, username=username, pg=pg)
        if not doc.get("chunkUrl") and doc.get("mappedDocuments"):
            doc["chunkUrl"] = _get_any(doc["mappedDocuments"][0], ["chunkUrl", "chunk_url", "url"], "") or ""
        return doc

    if kind == "topic":
        topic = _mongo_find_one_by_id(COL_TOPICS, "topic", entity_id)
        subject_id = _get_any(topic, ["subjectID", "subjectId", "subject_id"], "")
        subject = _mongo_find_one_by_id(COL_SUBJECTS, "subject", subject_id)
        class_id = _get_any(subject, ["classID", "classId", "class_id"], "")
        cls = _mongo_find_one_by_id(COL_CLASSES, "class", class_id)
        class_info = {"classID": class_id or "", "className": _pretty_class_name(cls) or _pretty_class_name_from_text(class_id or "")}
        subject_info = {"subjectID": subject_id or "", "subjectName": _get_any(subject, ["subjectName", "subject_name", "name", "title"], "") or subject_id or "", "subjectUrl": _get_any(subject, ["subjectUrl", "subject_url", "url"], "") or ""}
        topic_info = {"topicID": entity_id, "topicName": _get_any(topic, ["topicName", "topic_name", "name", "title"], "") or entity_id, "topicUrl": _get_any(topic, ["topicUrl", "topic_url", "url"], "") or ""}
        doc = _build_generic_doc(item_id=entity_id, item_name=topic_info["topicName"], item_type="topic", username=username, item_url=topic_info["topicUrl"], item_description=_get_any(topic, ["topicDescription", "topic_description", "description"], "") or "", class_info=class_info, subject_info=subject_info, topic_info=topic_info)
        doc["mappedDocuments"] = _load_mapped_chunks_for_target("topic", entity_id, username=username, pg=pg)
        if not doc.get("chunkUrl") and doc.get("mappedDocuments"):
            doc["chunkUrl"] = _get_any(doc["mappedDocuments"][0], ["chunkUrl", "chunk_url", "url"], "") or ""
        return doc

    if kind == "lesson":
        lesson = _mongo_find_one_by_id(COL_LESSONS, "lesson", entity_id)
        topic_id = _get_any(lesson, ["topicID", "topicId", "topic_id"], "")
        topic = _mongo_find_one_by_id(COL_TOPICS, "topic", topic_id)
        subject_id = _get_any(topic, ["subjectID", "subjectId", "subject_id"], "")
        subject = _mongo_find_one_by_id(COL_SUBJECTS, "subject", subject_id)
        class_id = _get_any(subject, ["classID", "classId", "class_id"], "")
        cls = _mongo_find_one_by_id(COL_CLASSES, "class", class_id)
        class_info = {"classID": class_id or "", "className": _pretty_class_name(cls) or _pretty_class_name_from_text(class_id or "")}
        subject_info = {"subjectID": subject_id or "", "subjectName": _get_any(subject, ["subjectName", "subject_name", "name", "title"], "") or subject_id or "", "subjectUrl": _get_any(subject, ["subjectUrl", "subject_url", "url"], "") or ""}
        topic_info = {"topicID": topic_id or "", "topicName": _get_any(topic, ["topicName", "topic_name", "name", "title"], "") or topic_id or "", "topicUrl": _get_any(topic, ["topicUrl", "topic_url", "url"], "") or ""}
        lesson_info = {"lessonID": entity_id, "lessonName": _get_any(lesson, ["lessonName", "lesson_name", "name", "title"], "") or entity_id, "lessonType": _get_any(lesson, ["lessonType", "lesson_type", "type"], "") or "", "lessonUrl": _get_any(lesson, ["lessonUrl", "lesson_url", "url"], "") or ""}
        doc = _build_generic_doc(item_id=entity_id, item_name=lesson_info["lessonName"], item_type="lesson", username=username, item_url=lesson_info["lessonUrl"], item_description=_get_any(lesson, ["lessonDescription", "lesson_description", "description"], "") or "", class_info=class_info, subject_info=subject_info, topic_info=topic_info, lesson_info=lesson_info, chunk_type=lesson_info["lessonType"])
        doc["mappedDocuments"] = _load_mapped_chunks_for_target("lesson", entity_id, username=username, pg=pg)
        if not doc.get("chunkUrl") and doc.get("mappedDocuments"):
            doc["chunkUrl"] = _get_any(doc["mappedDocuments"][0], ["chunkUrl", "chunk_url", "url"], "") or ""
        return doc

    if kind in ("image", "video") and pg is not None:
        row = None
        mongo_doc = {}
        follow_type = ""
        follow_id = ""
        if kind == "image":
            row = pg.query(PgImage).filter(PgImage.img_id == entity_id).first()
            if row and getattr(row, "mongo_id", None):
                try:
                    mongo_doc = db_mongo[COL_IMAGES].find_one({"_id": ObjectId(str(row.mongo_id)), "status": _not_hidden_q()}, {"_id": 0}) or {}
                except Exception:
                    mongo_doc = db_mongo[COL_IMAGES].find_one({"mongo_id": str(row.mongo_id), "status": _not_hidden_q()}, {"_id": 0}) or {}
            follow_type = getattr(row, "follow_type", "") or ""
            follow_id = getattr(row, "follow_id", "") or ""
            name = _get_any(mongo_doc, ["imgName", "name", "title"], "") or (getattr(row, "img_name", None) or entity_id)
            url = _get_any(mongo_doc, ["imgUrl", "url"], "") or ""
            desc = _get_any(mongo_doc, ["imgDescription", "description"], "") or ""
        else:
            row = pg.query(PgVideo).filter(PgVideo.video_id == entity_id).first()
            if row and getattr(row, "mongo_id", None):
                try:
                    mongo_doc = db_mongo[COL_VIDEOS].find_one({"_id": ObjectId(str(row.mongo_id)), "status": _not_hidden_q()}, {"_id": 0}) or {}
                except Exception:
                    mongo_doc = db_mongo[COL_VIDEOS].find_one({"mongo_id": str(row.mongo_id), "status": _not_hidden_q()}, {"_id": 0}) or {}
            follow_type = getattr(row, "follow_type", "") or ""
            follow_id = getattr(row, "follow_id", "") or ""
            name = _get_any(mongo_doc, ["videoName", "name", "title"], "") or (getattr(row, "video_name", None) or entity_id)
            url = _get_any(mongo_doc, ["videoUrl", "url"], "") or ""
            desc = _get_any(mongo_doc, ["videoDescription", "description"], "") or ""

        if not row and not mongo_doc:
            return None

        mapped = _load_mapped_chunks_for_target(follow_type, follow_id, username=username, pg=pg)
        sample = mapped[0] if mapped else {}
        doc = _build_generic_doc(item_id=entity_id, item_name=name, item_type=kind, username=username, item_url=url, item_description=desc, class_info=(sample.get("class") or {"classID": "", "className": ""}), subject_info=(sample.get("subject") or {"subjectID": "", "subjectName": "", "subjectUrl": ""}), topic_info=(sample.get("topic") or {"topicID": "", "topicName": "", "topicUrl": ""}), lesson_info=(sample.get("lesson") or {"lessonID": "", "lessonName": "", "lessonType": "", "lessonUrl": ""}))
        doc["mappedDocuments"] = mapped
        if not doc.get("chunkUrl") and mapped:
            doc["chunkUrl"] = _get_any(mapped[0], ["chunkUrl", "chunk_url", "url"], "") or ""
        doc["followType"] = follow_type
        doc["followID"] = follow_id
        return doc

    return None



def _load_media_bucket_from_pg(row: Any, collection: str, *, media_type: str, follow_type: str, follow_id: str) -> Optional[dict]:
    mongo_doc = {}
    mongo_id = getattr(row, "mongo_id", None)
    if mongo_id:
        try:
            mongo_doc = db_mongo[collection].find_one({"_id": ObjectId(str(mongo_id)), "status": _not_hidden_q()}, {"_id": 0}) or {}
        except Exception:
            mongo_doc = db_mongo[collection].find_one({"mongo_id": str(mongo_id), "status": _not_hidden_q()}, {"_id": 0}) or {}

    if not mongo_doc:
        return None

    if media_type == "image":
        name = _get_any(mongo_doc, ["imgName", "name", "title"], "") or getattr(row, "img_name", None) or getattr(row, "img_id", None) or ""
        desc = _get_any(mongo_doc, ["imgDescription", "description"], "") or ""
        url = _get_any(mongo_doc, ["imgUrl", "url"], "") or ""
        media_id = getattr(row, "img_id", None) or name
    else:
        name = _get_any(mongo_doc, ["videoName", "name", "title"], "") or getattr(row, "video_name", None) or getattr(row, "video_id", None) or ""
        desc = _get_any(mongo_doc, ["videoDescription", "description"], "") or ""
        url = _get_any(mongo_doc, ["videoUrl", "url"], "") or ""
        media_id = getattr(row, "video_id", None) or name

    if not media_id:
        return None

    return {
        "type": media_type,
        "id": str(media_id),
        "name": str(name or media_id),
        "description": desc,
        "url": url,
        "followType": follow_type,
        "followID": follow_id,
    }


def _attach_related_media(doc: dict, *, pg: Optional[Session]) -> dict:
    if not doc or pg is None:
        return doc

    targets = []
    item_type = str(doc.get("itemType") or doc.get("category") or "").strip().lower()
    item_id = str(doc.get("chunkID") or "").strip()
    if item_type in {"chunk", "document", "class", "subject", "topic", "lesson"} and item_id:
        targets.append(("chunk" if item_type in {"chunk", "document"} else item_type, item_id))

    lesson_id = _get_any(doc.get("lesson") or {}, ["lessonID", "lessonId", "lesson_id"], "")
    topic_id = _get_any(doc.get("topic") or {}, ["topicID", "topicId", "topic_id"], "")
    subject_id = _get_any(doc.get("subject") or {}, ["subjectID", "subjectId", "subject_id"], "")

    if lesson_id:
        targets.append(("lesson", lesson_id))
    if topic_id:
        targets.append(("topic", topic_id))
    if subject_id:
        targets.append(("subject", subject_id))

    uniq_targets = []
    seen = set()
    for ft, fid in targets:
        key = (str(ft or "").strip(), str(fid or "").strip())
        if not key[0] or not key[1] or key in seen:
            continue
        seen.add(key)
        uniq_targets.append(key)

    images = []
    videos = []
    for ft, fid in uniq_targets:
        try:
            image_rows = pg.query(PgImage).filter(PgImage.follow_type == ft, PgImage.follow_id == fid).all()
        except Exception:
            image_rows = []
        for row in image_rows:
            item = _load_media_bucket_from_pg(row, COL_IMAGES, media_type="image", follow_type=ft, follow_id=fid)
            if item:
                images.append(item)

        try:
            video_rows = pg.query(PgVideo).filter(PgVideo.follow_type == ft, PgVideo.follow_id == fid).all()
        except Exception:
            video_rows = []
        for row in video_rows:
            item = _load_media_bucket_from_pg(row, COL_VIDEOS, media_type="video", follow_type=ft, follow_id=fid)
            if item:
                videos.append(item)

    def _sort_key(item: dict):
        priority = {"chunk": 0, "lesson": 1, "topic": 2, "subject": 3}
        return (priority.get(item.get("followType"), 99), str(item.get("name") or "").lower())

    images.sort(key=_sort_key)
    videos.sort(key=_sort_key)

    def _uniq_media(items: list[dict]) -> list[dict]:
        out = []
        used = set()
        for item in items:
            key = str(item.get("id") or item.get("url") or item.get("name") or "").strip()
            if not key or key in used:
                continue
            used.add(key)
            out.append(item)
        return out

    images = _uniq_media(images)
    videos = _uniq_media(videos)

    doc["images"] = images
    doc["videos"] = videos
    doc["mediaStats"] = {"totalImages": len(images), "totalVideos": len(videos)}
    return doc


@router.get("/classes")
def list_classes(category: str = Query("all")):
    ids: set[str] = set()

    for doc in db_mongo[COL_SUBJECTS].find(
        {"status": _not_hidden_q()},
        {"classID": 1, "classId": 1, "class_id": 1},
    ):
        value = _get_any(doc, ["classID", "classId", "class_id"], "")
        if value:
            ids.add(str(value))

    if not ids:
        for doc in db_mongo[COL_CLASSES].find({"status": _not_hidden_q()}, {"_id": 0}):
            value = _get_any(doc, ["classID", "classId", "class_id"], "")
            if value:
                ids.add(str(value))

    items = []
    for class_id in sorted(ids, key=_sort_key_by_number):
        cls = _mongo_find_one_by_id(COL_CLASSES, "class", class_id)
        items.append(
            {
                "classID": class_id,
                "className": _pretty_class_name(cls) or _pretty_class_name_from_text(class_id),
            }
        )
    return {"total": len(items), "items": items}


@router.get("/subjects")
def list_subjects(classID: str = Query(""), category: str = Query("all")):
    if not classID:
        return {"total": 0, "items": []}

    q = {**_id_match("class", classID), "status": _not_hidden_q()}
    cur = db_mongo[COL_SUBJECTS].find(q, {"_id": 0})

    items = []
    seen = set()
    for d in cur:
        sid = _get_any(d, ["subjectID", "subjectId", "subject_id"], "") or ""
        sname = _get_any(d, ["subjectName", "subject_name", "name"], "") or sid
        if sid and sid not in seen:
            seen.add(sid)
            items.append({"subjectID": sid, "subjectName": sname, "classID": classID})

    items.sort(key=lambda x: str(x.get("subjectName") or x.get("subjectID") or ""))
    return {"total": len(items), "items": items}


@router.get("/topics")
def list_topics(subjectID: str = Query(""), category: str = Query("all")):
    if not subjectID:
        return {"total": 0, "items": []}

    q = {**_id_match("subject", subjectID), "status": _not_hidden_q()}
    cur = db_mongo[COL_TOPICS].find(q, {"_id": 0})

    items = []
    seen = set()
    for d in cur:
        tid = _get_any(d, ["topicID", "topicId", "topic_id"], "") or ""
        tname = _get_any(d, ["topicName", "topic_name", "name"], "") or tid
        if tid and tid not in seen:
            seen.add(tid)
            items.append({"topicID": tid, "topicName": tname, "subjectID": subjectID})

    items.sort(key=lambda x: str(x.get("topicName") or x.get("topicID") or ""))
    return {"total": len(items), "items": items}


@router.get("/lessons")
def list_lessons(topicID: str = Query(""), category: str = Query("all")):
    if not topicID:
        return {"total": 0, "items": []}

    cur = db_mongo[COL_LESSONS].find(
        {**_id_match("topic", topicID), "status": _not_hidden_q()},
        {"_id": 0},
    )

    items = []
    seen = set()
    for d in cur:
        lid = _get_any(d, ["lessonID", "lessonId", "lesson_id"], "") or ""
        lname = _get_any(d, ["lessonName", "lesson_name", "name"], "") or lid
        if lid and lid not in seen:
            seen.add(lid)
            items.append(
                {
                    "lessonID": lid,
                    "lessonName": lname,
                    "lessonType": _get_any(d, ["lessonType", "lesson_type", "type"], "") or "",
                    "topicID": topicID,
                }
            )

    items.sort(key=lambda x: str(x.get("lessonName") or x.get("lessonID") or ""))
    return {"total": len(items), "items": items}


def _list_chunks_impl(
    request: Request,
    lessonID: str,
    category: str,
    limit: int,
    offset: int,
    sort: str,
):
    if not lessonID:
        return {"total": 0, "items": []}

    q = {**_id_match("lesson", lessonID), "status": _not_hidden_q()}

    sort_spec = [("chunkName", 1)]
    if sort == "updated":
        sort_spec = [("updatedAt", -1), ("chunkName", 1)]
    elif sort == "newest":
        sort_spec = [("createdAt", -1), ("chunkName", 1)]
    elif sort == "oldest":
        sort_spec = [("createdAt", 1), ("chunkName", 1)]

    cur = (
        db_mongo[COL_CHUNKS]
        .find(q, {"_id": 0})
        .sort(sort_spec)
        .skip(offset)
        .limit(limit)
    )

    username = _actor(request)
    items = [_get_chunk_full(c, category=category, username=username) for c in cur]
    total = db_mongo[COL_CHUNKS].count_documents(q)
    return {"total": total, "items": items}


@router.get("")
def list_docs_root(
    request: Request,
    lessonID: str = Query(""),
    category: str = Query("all"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    sort: str = Query("name"),
):
    return _list_chunks_impl(request, lessonID, category, limit, offset, sort)


@router.get("/chunks")
def list_chunks(
    request: Request,
    lessonID: str = Query(""),
    category: str = Query("all"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    sort: str = Query("name"),
):
    return _list_chunks_impl(request, lessonID, category, limit, offset, sort)


@router.get("/search")
def search(
    request: Request,
    q: str = Query(""),
    classID: str = Query(""),
    subjectID: str = Query(""),
    topicID: str = Query(""),
    lessonID: str = Query(""),
    category: str = Query("document"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    debug: int = Query(0),
    pg: Session = Depends(get_db),
    neo=Depends(get_neo4j_session),
):
    username = _actor(request)
    return semantic_search(
        q=q,
        category=category,
        classID=classID,
        subjectID=subjectID,
        topicID=topicID,
        lessonID=lessonID,
        limit=limit,
        offset=offset,
        username=username,
        pg=pg,
        neo=neo,
        mongo_db=db_mongo,
        debug=bool(debug),
    )


@router.get("/{chunkID}")
def get_doc_detail(
    request: Request,
    chunkID: str,
    category: str = Query("document"),
    pg: Session = Depends(get_db),
):
    username = _actor(request)
    doc = _resolve_entity_doc(chunkID, category=category, username=username, pg=pg)
    if not doc:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài liệu")

    doc = _attach_related_media(doc, pg=pg)

    if doc.get("itemType") == "chunk":
        keyword_items = []
        try:
            keyword_items = list(
                db_mongo[COL_KEYWORDS].find(
                    {**_id_match("chunk", chunkID), "status": _not_hidden_q()},
                    {"_id": 0, "keywordEmbedding": 0},
                )
            )
        except Exception:
            keyword_items = []

        if not keyword_items and pg:
            try:
                pg_keywords = pg.query(PgKeyword).filter(PgKeyword.chunk_id == chunkID).all()
                keyword_items = [
                    {"keywordID": k.keyword_id, "keywordName": k.keyword_name}
                    for k in pg_keywords
                ]
            except Exception:
                keyword_items = []

        doc["keywordItems"] = keyword_items
        doc["keywords"] = [
            _get_any(item, ["keywordName", "keyword_name", "keyword", "name"], "")
            for item in keyword_items
            if _get_any(item, ["keywordName", "keyword_name", "keyword", "name"], "")
        ]

        related = []
        lesson_id = _get_any(doc.get("lesson") or {}, ["lessonID", "lessonId", "lesson_id"], "")
        if lesson_id:
            try:
                rel_cur = (
                    db_mongo[COL_CHUNKS]
                    .find({**_id_match("lesson", lesson_id), "status": _not_hidden_q()}, {"_id": 0})
                    .sort([("chunkName", 1)])
                )
                for rel in rel_cur:
                    rel_id = _get_any(rel, ["chunkID", "chunkId", "chunk_id"], "")
                    if rel_id and rel_id != chunkID:
                        related.append(_get_chunk_full(rel, category="document", username=username))
            except Exception:
                pass

            if not related and pg:
                try:
                    pg_chunks = pg.query(PgChunk).filter(PgChunk.lesson_id == lesson_id).all()
                    for rel in pg_chunks:
                        if rel.chunk_id == chunkID:
                            continue
                        rel_doc = _resolve_entity_doc(rel.chunk_id, category="document", username=username, pg=pg)
                        if rel_doc:
                            related.append(rel_doc)
                except Exception:
                    pass

        doc["related"] = related
    else:
        doc.setdefault("related", [])

    return doc


@router.get("/{chunkID}/view")
def view_doc(
    request: Request,
    chunkID: str,
    category: str = Query("document"),
    pg: Session = Depends(get_db),
):
    username = _actor(request)
    doc = _resolve_entity_doc(chunkID, category=category, username=username, pg=pg)
    if not doc:
        raise HTTPException(status_code=404, detail="Không tìm thấy URL xem tài liệu")

    if str(doc.get("itemType") or category or "").lower() == "class":
        raise HTTPException(status_code=404, detail="Lớp không có file trực tiếp")

    original_url = doc.get("chunkUrl") or ""
    if not original_url:
        for item in doc.get("mappedDocuments") or []:
            if item.get("chunkUrl"):
                original_url = item.get("chunkUrl")
                break

    if not original_url:
        raise HTTPException(status_code=404, detail="Không tìm thấy URL xem tài liệu")

    return get_view_url(original_url=original_url, chunk_id=chunkID)


@router.post("/{chunkID}/save")
def toggle_save(request: Request, chunkID: str, category: str = Query("document")):
    username = _actor(request)
    key = {"username": username, "chunkID": chunkID, "category": category}
    existing = db_mongo[COL_SAVED].find_one(key)
    if existing:
        db_mongo[COL_SAVED].delete_one({"_id": existing["_id"]})
        return {"saved": False}

    now = _now()
    db_mongo[COL_SAVED].insert_one({**key, "createdAt": now, "updatedAt": now})
    return {"saved": True}


@router.post("/saved/{chunkID}/toggle")
def toggle_save_alias(request: Request, chunkID: str, category: str = Query("document")):
    return toggle_save(request, chunkID, category)


@router.get("/saved/list")
def list_saved(
    request: Request,
    category: str = Query("document"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    pg: Session = Depends(get_db),
):
    username = _actor(request)

    q: dict[str, Any] = {"username": username}
    if category != "all":
        q["category"] = category

    cur = (
        db_mongo[COL_SAVED]
        .find(q, {"_id": 0, "chunkID": 1, "category": 1})
        .sort([("updatedAt", -1), ("createdAt", -1)])
        .skip(offset)
        .limit(limit)
    )

    saved_rows = list(cur)
    items = []
    for row in saved_rows:
        cid = str(row.get("chunkID") or "").strip()
        row_category = str(row.get("category") or "document").strip() or "document"
        if not cid:
            continue
        doc = _resolve_entity_doc(cid, category=row_category, username=username, pg=pg)
        if doc:
            items.append(doc)

    total = db_mongo[COL_SAVED].count_documents(q)
    return {"total": total, "items": items}
