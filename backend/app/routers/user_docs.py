from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from ..services.mongo_client import get_mongo_client
from ..services.postgre_client import get_db
from ..services.neo_client import get_neo4j_session
from ..services.user_semantic_search import semantic_search
from ..services.doc_preview import get_view_url


router = APIRouter(prefix="/user/docs", tags=["UserDocs"])

mongo = get_mongo_client()
db_mongo = mongo["db"]

COL_CLASSES = "classes"
COL_SUBJECTS = "subjects"
COL_TOPICS = "topics"
COL_LESSONS = "lessons"
COL_CHUNKS = "chunks"
COL_KEYWORDS = "keywords"
COL_SAVED = "user_saved_chunks"


def _now():
    return datetime.now(timezone.utc)


def _actor(request: Request) -> str:
    return request.headers.get("x-user") or request.headers.get("x-actor") or "user"


def _active_status_q():
    # Your system uses "activity" (active) and "hidden" (soft delete)
    return {"$in": ["active", "activity"]}


def _not_hidden_q():
    # Some collections might not have status; we only exclude explicit soft-deletes.
    return {"$ne": "hidden"}


def _get_any(doc: dict, keys: list[str]) -> Optional[str]:
    for k in keys:
        if k in doc and doc.get(k) not in (None, ""):
            return str(doc.get(k))
    return None


def _pretty_class_name_from_text(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    low = raw.lower()
    if "lớp" in low:
        return raw
    m = re.search(r"(\d{1,2})", raw)
    if m:
        return f"Lớp {int(m.group(1))}"
    return raw


def _pretty_class_name(doc: dict) -> str:
    raw = (doc.get("className") or doc.get("class_name") or doc.get("classID") or doc.get("classId") or doc.get("class_id") or "").strip()
    return _pretty_class_name_from_text(raw)


def _sort_key_by_number(s: str):
    m = re.search(r"(\d{1,2})", s or "")
    return (int(m.group(1)) if m else 999, s or "")


def _ensure_saved_index():
    try:
        db_mongo[COL_SAVED].create_index(
            [("username", 1), ("chunkID", 1), ("category", 1)],
            unique=True,
            name="uniq_user_chunk",
        )
        db_mongo[COL_SAVED].create_index([("username", 1), ("updatedAt", -1)], name="idx_user_updated")
    except Exception:
        pass


_ensure_saved_index()


def _get_chunk_full(chunk: dict, *, category: str, username: str) -> dict:
    """Build the shape expected by DocumentCard.

    NOTE: For dropdown filters you asked NOT to rely on category fields.
    So this function is tolerant and only excludes status=='hidden'.
    """
    lesson_id = _get_any(chunk, ["lessonID", "lessonId", "lesson_id"])
    lesson = (
        db_mongo[COL_LESSONS].find_one(
            {"$or": [{"lessonID": lesson_id}, {"lessonId": lesson_id}, {"lesson_id": lesson_id}], "status": _not_hidden_q()},
            {"_id": 0},
        )
        or {}
    )

    topic_id = _get_any(lesson, ["topicID", "topicId", "topic_id"])
    topic = (
        db_mongo[COL_TOPICS].find_one(
            {"$or": [{"topicID": topic_id}, {"topicId": topic_id}, {"topic_id": topic_id}], "status": _not_hidden_q()},
            {"_id": 0},
        )
        or {}
    )

    subject_id = _get_any(topic, ["subjectID", "subjectId", "subject_id"])
    subject = (
        db_mongo[COL_SUBJECTS].find_one(
            {"$or": [{"subjectID": subject_id}, {"subjectId": subject_id}, {"subject_id": subject_id}], "status": _not_hidden_q()},
            {"_id": 0},
        )
        or {}
    )

    class_id = _get_any(subject, ["classID", "classId", "class_id"])
    cls = (
        db_mongo[COL_CLASSES].find_one(
            {"$or": [{"classID": class_id}, {"classId": class_id}, {"class_id": class_id}], "status": _not_hidden_q()},
            {"_id": 0},
        )
        or {}
    )

    saved = db_mongo[COL_SAVED].find_one({"username": username, "chunkID": _get_any(chunk, ["chunkID", "chunkId", "chunk_id"]), "category": category})

    chunk_id = _get_any(chunk, ["chunkID", "chunkId", "chunk_id"]) or chunk.get("chunkID")
    chunk_name = _get_any(chunk, ["chunkName", "chunk_name", "name"]) or chunk.get("chunkName") or chunk_id

    subject_name = _get_any(subject, ["subjectName", "subject_name", "name"]) or subject_id
    topic_name = _get_any(topic, ["topicName", "topic_name", "name"]) or topic_id
    lesson_name = _get_any(lesson, ["lessonName", "lesson_name", "name"]) or lesson_id

    class_name = _pretty_class_name(cls) or _pretty_class_name_from_text(class_id or "") or (class_id or "")

    return {
        "chunkID": chunk_id,
        "chunkName": chunk_name,
        "chunkType": _get_any(chunk, ["chunkType", "chunk_type"]) or chunk.get("chunkType"),
        "chunkUrl": _get_any(chunk, ["chunkUrl", "chunk_url", "url"]) or chunk.get("chunkUrl"),
        "chunkDescription": _get_any(chunk, ["chunkDescription", "chunk_description", "description"]) or chunk.get("chunkDescription"),
        "keywords": chunk.get("keywords") or chunk.get("keyword") or [],
        "isSaved": bool(saved),
        "class": {"classID": class_id, "className": class_name},
        "subject": {"subjectID": subject_id, "subjectName": subject_name},
        "topic": {"topicID": topic_id, "topicName": topic_name},
        "lesson": {
            "lessonID": lesson_id,
            "lessonName": lesson_name,
            "lessonType": _get_any(lesson, ["lessonType", "lesson_type", "type"]) or lesson.get("lessonType"),
        },
    }


@router.get("/classes")
def list_classes(category: str = Query("document")):
    """Dropdown: Lớp 10/11/12.

    Không phụ thuộc category.
    Ưu tiên lấy từ subjects.classID (ổn định nhất).
    """
    ids: set[str] = set()

    # Preferred: derive from subjects
    for doc in db_mongo[COL_SUBJECTS].find({"status": _not_hidden_q()}, {"classID": 1, "classId": 1, "class_id": 1}):
        v = _get_any(doc, ["classID", "classId", "class_id"])
        if v:
            ids.add(v)

    # Fallback: classes collection
    if not ids:
        for doc in db_mongo[COL_CLASSES].find({"status": _not_hidden_q()}, {"classID": 1, "classId": 1, "class_id": 1}):
            v = _get_any(doc, ["classID", "classId", "class_id"])
            if v:
                ids.add(v)

    items = [{"classID": cid, "className": _pretty_class_name_from_text(cid)} for cid in sorted(ids, key=_sort_key_by_number)]
    return {"total": len(items), "items": items}


@router.get("/subjects")
def list_subjects(classID: str = Query(""), category: str = Query("document")):
    if not classID:
        return {"total": 0, "items": []}

    cur = db_mongo[COL_SUBJECTS].find(
        {
            "status": _not_hidden_q(),
            "$or": [{"classID": classID}, {"classId": classID}, {"class_id": classID}],
        },
        {"_id": 0},
    )

    items = []
    for d in cur:
        sid = _get_any(d, ["subjectID", "subjectId", "subject_id"]) or ""
        sname = _get_any(d, ["subjectName", "subject_name", "name"]) or sid
        if sid:
            items.append({"subjectID": sid, "subjectName": sname, "classID": classID})

    items.sort(key=lambda x: (x.get("subjectName") or x.get("subjectID") or ""))
    return {"total": len(items), "items": items}


@router.get("/topics")
def list_topics(subjectID: str = Query(""), category: str = Query("document")):
    if not subjectID:
        return {"total": 0, "items": []}

    cur = db_mongo[COL_TOPICS].find(
        {
            "status": _not_hidden_q(),
            "$or": [{"subjectID": subjectID}, {"subjectId": subjectID}, {"subject_id": subjectID}],
        },
        {"_id": 0},
    )

    items = []
    for d in cur:
        tid = _get_any(d, ["topicID", "topicId", "topic_id"]) or ""
        tname = _get_any(d, ["topicName", "topic_name", "name"]) or tid
        if tid:
            items.append({"topicID": tid, "topicName": tname, "subjectID": subjectID})

    items.sort(key=lambda x: (x.get("topicName") or x.get("topicID") or ""))
    return {"total": len(items), "items": items}


@router.get("/lessons")
def list_lessons(topicID: str = Query(""), category: str = Query("document")):
    if not topicID:
        return {"total": 0, "items": []}

    cur = db_mongo[COL_LESSONS].find(
        {
            "status": _not_hidden_q(),
            "$or": [{"topicID": topicID}, {"topicId": topicID}, {"topic_id": topicID}],
        },
        {"_id": 0},
    )

    items = []
    for d in cur:
        lid = _get_any(d, ["lessonID", "lessonId", "lesson_id"]) or ""
        lname = _get_any(d, ["lessonName", "lesson_name", "name"]) or lid
        if lid:
            items.append({"lessonID": lid, "lessonName": lname, "topicID": topicID})

    items.sort(key=lambda x: (x.get("lessonName") or x.get("lessonID") or ""))
    return {"total": len(items), "items": items}


@router.get("/chunks")
def list_chunks(
    request: Request,
    lessonID: str = Query(""),
    category: str = Query("document"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    sort: str = Query("name"),
):
    if not lessonID:
        return {"total": 0, "items": []}

    sort_spec = [("chunkName", 1)]
    if sort == "updated":
        sort_spec = [("updatedAt", -1)]

    cur = (
        db_mongo[COL_CHUNKS]
        .find(
            {
                "status": _not_hidden_q(),
                "$or": [{"lessonID": lessonID}, {"lessonId": lessonID}, {"lesson_id": lessonID}],
            },
            {"_id": 0},
        )
        .sort(sort_spec)
        .skip(offset)
        .limit(limit)
    )

    username = _actor(request)
    items = [_get_chunk_full(c, category=category, username=username) for c in cur]

    total = db_mongo[COL_CHUNKS].count_documents({"status": _not_hidden_q(), "$or": [{"lessonID": lessonID}, {"lessonId": lessonID}, {"lesson_id": lessonID}]})
    return {"total": total, "items": items}


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
    res = semantic_search(
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
    return res


@router.get("/{chunkID}")
def get_doc_detail(request: Request, chunkID: str, category: str = Query("document")):
    # tolerant: no category restriction, only exclude hidden
    chunk = db_mongo[COL_CHUNKS].find_one(
        {"$or": [{"chunkID": chunkID}, {"chunkId": chunkID}, {"chunk_id": chunkID}], "status": _not_hidden_q()},
        {"_id": 0},
    )
    if not chunk:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài liệu")

    username = _actor(request)
    doc = _get_chunk_full(chunk, category=category, username=username)

    try:
        kws = list(db_mongo[COL_KEYWORDS].find({"chunkID": chunkID, "status": _not_hidden_q()}, {"_id": 0, "keywordEmbedding": 0}))
        doc["keywords"] = [k.get("keywordName") for k in kws if k.get("keywordName")]
        doc["keywordItems"] = kws
    except Exception:
        doc["keywordItems"] = []

    lesson_id = _get_any(chunk, ["lessonID", "lessonId", "lesson_id"])
    rel_cur = (
        db_mongo[COL_CHUNKS]
        .find({"status": _not_hidden_q(), "$or": [{"lessonID": lesson_id}, {"lessonId": lesson_id}, {"lesson_id": lesson_id}]}, {"_id": 0})
        .sort([("chunkName", 1)])
    )
    related = [_get_chunk_full(c, category=category, username=username) for c in rel_cur if _get_any(c, ["chunkID", "chunkId", "chunk_id"]) != chunkID]
    doc["related"] = related

    return doc


@router.get("/{chunkID}/view")
def view_doc(chunkID: str, category: str = Query("document")):
    chunk = db_mongo[COL_CHUNKS].find_one(
        {"$or": [{"chunkID": chunkID}, {"chunkId": chunkID}, {"chunk_id": chunkID}], "status": _not_hidden_q()},
        {"_id": 0},
    )
    if not chunk:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài liệu")

    original_url = _get_any(chunk, ["chunkUrl", "chunk_url", "url"]) or ""
    if not original_url:
        raise HTTPException(status_code=400, detail="Tài liệu không có URL")

    return get_view_url(original_url=original_url, chunk_id=chunkID)


@router.post("/{chunkID}/save")
def toggle_save(request: Request, chunkID: str, category: str = Query("document")):
    username = _actor(request)
    now = _now()

    key = {"username": username, "chunkID": chunkID, "category": category}
    existing = db_mongo[COL_SAVED].find_one(key)
    if existing:
        db_mongo[COL_SAVED].delete_one({"_id": existing.get("_id")})
        return {"saved": False}

    doc: Any = {**key, "createdAt": now, "updatedAt": now}
    db_mongo[COL_SAVED].insert_one(doc)
    return {"saved": True}


@router.get("/saved/list")
def list_saved(
    request: Request,
    category: str = Query("document"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    username = _actor(request)

    cur = (
        db_mongo[COL_SAVED]
        .find({"username": username, "category": category}, {"_id": 0, "chunkID": 1})
        .sort([("updatedAt", -1)])
        .skip(offset)
        .limit(limit)
    )
    chunk_ids = [x.get("chunkID") for x in cur if x.get("chunkID")]

    chunks = list(db_mongo[COL_CHUNKS].find({"status": _not_hidden_q(), "chunkID": {"$in": chunk_ids}}, {"_id": 0}))
    by_id = {(_get_any(c, ["chunkID", "chunkId", "chunk_id"]) or c.get("chunkID")): c for c in chunks}

    items = []
    for cid in chunk_ids:
        cdoc = by_id.get(cid)
        if not cdoc:
            continue
        items.append(_get_chunk_full(cdoc, category=category, username=username))

    total = db_mongo[COL_SAVED].count_documents({"username": username, "category": category})
    return {"total": total, "items": items}
