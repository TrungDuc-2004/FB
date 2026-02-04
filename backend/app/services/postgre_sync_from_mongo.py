from __future__ import annotations

"""Sync PostgreSQL FROM MongoDB.

Bản này hỗ trợ 2 chế độ:
1) sync_postgre_from_mongo_ids: (GIỮ NGUYÊN) sync theo mongo _id (ObjectId) -> hash PK
2) sync_postgre_from_mongo_maps: (MỚI) sync theo map ID (L10/TH10/TH10_CD1/TH10_CD1_B1/TH10_CD1_B1_C1)
   để Postgre dùng map làm PK + quan hệ cha/con.

Schema PG (theo bạn mô tả):
- class(class_id, class_name, mongo_id)
- subject(subject_id, subject_name, mongo_id, class_id)
- topic(topic_id, topic_name, mongo_id, subject_id)
- lesson(lesson_id, lesson_name, mongo_id, topic_id)
- chunk(chunk_id, chunk_name, chunk_type, mongo_id, lesson_id)
- keyword(keyword_id, keyword_name, mongo_id, chunk_id)

Nguồn Mongo (plural): classes, subjects, topics, lessons, chunks.
"""

import hashlib
from dataclasses import dataclass
from typing import List, Optional, Tuple

from bson import ObjectId
from sqlalchemy import text

from .mongo_client import get_mongo_client
from .postgre_client import get_engine


def _md5_32(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:32]


def _sha256_64(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:64]


def _sha384_96(s: str) -> str:
    return hashlib.sha384(s.encode("utf-8")).hexdigest()[:96]


def _clean(s) -> str:
    return "" if s is None else str(s).strip()


def _ensure_oid(id_str: str) -> ObjectId:
    try:
        return ObjectId(id_str)
    except Exception as e:
        raise ValueError(f"Invalid mongo id: {id_str}") from e


def _pick_by_oid(db, col: str, _id: str) -> dict:
    doc = db[col].find_one({"_id": _ensure_oid(_id)})
    if not doc:
        raise ValueError(f"Mongo doc not found: {col}({_id})")
    return doc


def _get_pk_by_mongo(conn, table: str, pk_col: str, mongo_id: str) -> Optional[str]:
    row = conn.execute(
        text(f"SELECT {pk_col} FROM {table} WHERE mongo_id = :mongo_id LIMIT 1"),
        {"mongo_id": mongo_id},
    ).fetchone()
    return row[0] if row else None


@dataclass
class PgIds:
    class_id: str
    subject_id: str
    topic_id: str
    lesson_id: str
    chunk_id: str
    keyword_ids: List[str]


# ======================================================================================
# 1) GIỮ NGUYÊN: sync theo mongo ObjectId -> hash PK
# ======================================================================================

def sync_postgre_from_mongo_ids(
    *,
    mongo_class_id: str,
    mongo_subject_id: str,
    mongo_topic_id: str,
    mongo_lesson_id: str,
    mongo_chunk_id: str,
) -> PgIds:
    mg = get_mongo_client()
    db = mg["db"]

    c_doc = _pick_by_oid(db, "classes", mongo_class_id)
    s_doc = _pick_by_oid(db, "subjects", mongo_subject_id)
    t_doc = _pick_by_oid(db, "topics", mongo_topic_id)
    l_doc = _pick_by_oid(db, "lessons", mongo_lesson_id)
    ch_doc = _pick_by_oid(db, "chunks", mongo_chunk_id)

    class_name = _clean(c_doc.get("className"))
    subject_name = _clean(s_doc.get("subjectName"))
    topic_name = _clean(t_doc.get("topicName"))
    lesson_name = _clean(l_doc.get("lessonName"))
    chunk_name = _clean(ch_doc.get("chunkName"))
    chunk_type = _clean(ch_doc.get("chunkType"))

    keywords = ch_doc.get("keywords") or []
    if not isinstance(keywords, list):
        keywords = []

    class_id_guess = _md5_32(mongo_class_id)
    subject_id_guess = _md5_32(mongo_subject_id)
    topic_id_guess = _sha256_64(mongo_topic_id)
    lesson_id_guess = _sha256_64(mongo_lesson_id)
    chunk_id_guess = _sha256_64(mongo_chunk_id)

    engine = get_engine()

    with engine.begin() as conn:
        class_id = _get_pk_by_mongo(conn, "class", "class_id", mongo_class_id) or class_id_guess
        subject_id = _get_pk_by_mongo(conn, "subject", "subject_id", mongo_subject_id) or subject_id_guess
        topic_id = _get_pk_by_mongo(conn, "topic", "topic_id", mongo_topic_id) or topic_id_guess
        lesson_id = _get_pk_by_mongo(conn, "lesson", "lesson_id", mongo_lesson_id) or lesson_id_guess
        chunk_id = _get_pk_by_mongo(conn, "chunk", "chunk_id", mongo_chunk_id) or chunk_id_guess

        conn.execute(
            text(
                """
                INSERT INTO class (class_id, class_name, mongo_id)
                VALUES (:class_id, :class_name, :mongo_id)
                ON CONFLICT (class_id) DO UPDATE
                SET class_name = EXCLUDED.class_name,
                    mongo_id   = COALESCE(EXCLUDED.mongo_id, class.mongo_id)
                """
            ),
            {"class_id": class_id, "class_name": class_name, "mongo_id": mongo_class_id},
        )

        conn.execute(
            text(
                """
                INSERT INTO subject (subject_id, subject_name, mongo_id, class_id)
                VALUES (:subject_id, :subject_name, :mongo_id, :class_id)
                ON CONFLICT (subject_id) DO UPDATE
                SET subject_name = EXCLUDED.subject_name,
                    mongo_id     = COALESCE(EXCLUDED.mongo_id, subject.mongo_id),
                    class_id     = EXCLUDED.class_id
                """
            ),
            {
                "subject_id": subject_id,
                "subject_name": subject_name,
                "mongo_id": mongo_subject_id,
                "class_id": class_id,
            },
        )

        conn.execute(
            text(
                """
                INSERT INTO topic (topic_id, topic_name, mongo_id, subject_id)
                VALUES (:topic_id, :topic_name, :mongo_id, :subject_id)
                ON CONFLICT (topic_id) DO UPDATE
                SET topic_name  = EXCLUDED.topic_name,
                    mongo_id    = COALESCE(EXCLUDED.mongo_id, topic.mongo_id),
                    subject_id  = EXCLUDED.subject_id
                """
            ),
            {
                "topic_id": topic_id,
                "topic_name": topic_name,
                "mongo_id": mongo_topic_id,
                "subject_id": subject_id,
            },
        )

        conn.execute(
            text(
                """
                INSERT INTO lesson (lesson_id, lesson_name, mongo_id, topic_id)
                VALUES (:lesson_id, :lesson_name, :mongo_id, :topic_id)
                ON CONFLICT (lesson_id) DO UPDATE
                SET lesson_name = EXCLUDED.lesson_name,
                    mongo_id    = COALESCE(EXCLUDED.mongo_id, lesson.mongo_id),
                    topic_id    = EXCLUDED.topic_id
                """
            ),
            {
                "lesson_id": lesson_id,
                "lesson_name": lesson_name,
                "mongo_id": mongo_lesson_id,
                "topic_id": topic_id,
            },
        )

        conn.execute(
            text(
                """
                INSERT INTO chunk (chunk_id, chunk_name, chunk_type, mongo_id, lesson_id)
                VALUES (:chunk_id, :chunk_name, :chunk_type, :mongo_id, :lesson_id)
                ON CONFLICT (chunk_id) DO UPDATE
                SET chunk_name = EXCLUDED.chunk_name,
                    chunk_type = EXCLUDED.chunk_type,
                    mongo_id   = COALESCE(EXCLUDED.mongo_id, chunk.mongo_id),
                    lesson_id  = EXCLUDED.lesson_id
                """
            ),
            {
                "chunk_id": chunk_id,
                "chunk_name": chunk_name,
                "chunk_type": chunk_type or None,
                "mongo_id": mongo_chunk_id,
                "lesson_id": lesson_id,
            },
        )

        conn.execute(text("DELETE FROM keyword WHERE chunk_id = :chunk_id"), {"chunk_id": chunk_id})

        keyword_ids: List[str] = []
        for kw in keywords:
            kw_name = _clean(kw)
            if not kw_name:
                continue
            kw_id = _sha384_96(f"{chunk_id}:{kw_name}")
            keyword_ids.append(kw_id)

            conn.execute(
                text(
                    """
                    INSERT INTO keyword (keyword_id, keyword_name, mongo_id, chunk_id)
                    VALUES (:keyword_id, :keyword_name, :mongo_id, :chunk_id)
                    ON CONFLICT (keyword_id) DO UPDATE
                    SET keyword_name = EXCLUDED.keyword_name,
                        mongo_id      = COALESCE(EXCLUDED.mongo_id, keyword.mongo_id),
                        chunk_id      = EXCLUDED.chunk_id
                    """
                ),
                {
                    "keyword_id": kw_id,
                    "keyword_name": kw_name,
                    "mongo_id": None,
                    "chunk_id": chunk_id,
                },
            )

    return PgIds(
        class_id=class_id_guess,
        subject_id=subject_id_guess,
        topic_id=topic_id_guess,
        lesson_id=lesson_id_guess,
        chunk_id=chunk_id_guess,
        keyword_ids=keyword_ids,
    )


# ======================================================================================
# 2) MỚI: sync theo MAP IDs
# ======================================================================================

def _pick_by_map(db, col: str, map_key: str, map_value: str) -> Optional[dict]:
    if not map_value:
        return None
    return db[col].find_one({map_key: map_value})


def _resolve_chain_from_maps(
    db,
    *,
    class_map: str = "",
    subject_map: str = "",
    topic_map: str = "",
    lesson_map: str = "",
    chunk_map: str = "",
) -> Tuple[Optional[dict], Optional[dict], Optional[dict], Optional[dict], Optional[dict]]:
    """Trả về (class_doc, subject_doc, topic_doc, lesson_doc, chunk_doc)"""

    chunk_doc = _pick_by_map(db, "chunks", "chunkID", chunk_map) if chunk_map else None
    if chunk_doc and not lesson_map:
        lesson_map = _clean(chunk_doc.get("lessonID"))

    lesson_doc = _pick_by_map(db, "lessons", "lessonID", lesson_map) if lesson_map else None
    if lesson_doc and not topic_map:
        topic_map = _clean(lesson_doc.get("topicID"))

    topic_doc = _pick_by_map(db, "topics", "topicID", topic_map) if topic_map else None
    if topic_doc and not subject_map:
        subject_map = _clean(topic_doc.get("subjectID"))

    subject_doc = _pick_by_map(db, "subjects", "subjectID", subject_map) if subject_map else None
    if subject_doc and not class_map:
        class_map = _clean(subject_doc.get("classID"))

    class_doc = _pick_by_map(db, "classes", "classID", class_map) if class_map else None

    return class_doc, subject_doc, topic_doc, lesson_doc, chunk_doc


def sync_postgre_from_mongo_maps(
    *,
    class_map: str = "",
    subject_map: str = "",
    topic_map: str = "",
    lesson_map: str = "",
    chunk_map: str = "",
) -> PgIds:
    """Sync PG theo map IDs.

    - Nếu truyền chunk_map: sync đủ chain + chunk + keywords
    - Nếu truyền lesson_map: sync chain tới lesson
    - Nếu truyền topic_map: sync chain tới topic
    - Nếu truyền subject_map: sync chain tới subject
    - class_map đơn lẻ: sync class
    """

    class_map = _clean(class_map)
    subject_map = _clean(subject_map)
    topic_map = _clean(topic_map)
    lesson_map = _clean(lesson_map)
    chunk_map = _clean(chunk_map)

    mg = get_mongo_client()
    db = mg["db"]

    class_doc, subject_doc, topic_doc, lesson_doc, chunk_doc = _resolve_chain_from_maps(
        db,
        class_map=class_map,
        subject_map=subject_map,
        topic_map=topic_map,
        lesson_map=lesson_map,
        chunk_map=chunk_map,
    )

    # fallback nếu không tìm thấy doc: dùng map làm tên
    class_id = _clean((class_doc or {}).get("classID")) or class_map
    subject_id = _clean((subject_doc or {}).get("subjectID")) or subject_map
    topic_id = _clean((topic_doc or {}).get("topicID")) or topic_map
    lesson_id = _clean((lesson_doc or {}).get("lessonID")) or lesson_map
    chunk_id = _clean((chunk_doc or {}).get("chunkID")) or chunk_map

    class_name = _clean((class_doc or {}).get("className")) or class_id
    subject_name = _clean((subject_doc or {}).get("subjectName")) or subject_id
    topic_name = _clean((topic_doc or {}).get("topicName")) or topic_id
    lesson_name = _clean((lesson_doc or {}).get("lessonName")) or lesson_id
    chunk_name = _clean((chunk_doc or {}).get("chunkName")) or chunk_id
    chunk_type = _clean((chunk_doc or {}).get("chunkType"))

    mongo_class_id = str((class_doc or {}).get("_id")) if class_doc else None
    mongo_subject_id = str((subject_doc or {}).get("_id")) if subject_doc else None
    mongo_topic_id = str((topic_doc or {}).get("_id")) if topic_doc else None
    mongo_lesson_id = str((lesson_doc or {}).get("_id")) if lesson_doc else None
    mongo_chunk_id = str((chunk_doc or {}).get("_id")) if chunk_doc else None

    keywords = (chunk_doc or {}).get("keywords") or []
    if not isinstance(keywords, list):
        keywords = []

    engine = get_engine()

    # nếu chỉ sync tới mức nào thì dừng mức đó
    want_subject = bool(subject_id)
    want_topic = bool(topic_id)
    want_lesson = bool(lesson_id)
    want_chunk = bool(chunk_id)

    keyword_ids: List[str] = []

    with engine.begin() as conn:
        if class_id:
            conn.execute(
                text(
                    """
                    INSERT INTO class (class_id, class_name, mongo_id)
                    VALUES (:class_id, :class_name, :mongo_id)
                    ON CONFLICT (class_id) DO UPDATE
                    SET class_name = EXCLUDED.class_name,
                        mongo_id   = COALESCE(EXCLUDED.mongo_id, class.mongo_id)
                    """
                ),
                {"class_id": class_id, "class_name": class_name, "mongo_id": mongo_class_id},
            )

        if want_subject:
            conn.execute(
                text(
                    """
                    INSERT INTO subject (subject_id, subject_name, mongo_id, class_id)
                    VALUES (:subject_id, :subject_name, :mongo_id, :class_id)
                    ON CONFLICT (subject_id) DO UPDATE
                    SET subject_name = EXCLUDED.subject_name,
                        mongo_id     = COALESCE(EXCLUDED.mongo_id, subject.mongo_id),
                        class_id     = EXCLUDED.class_id
                    """
                ),
                {
                    "subject_id": subject_id,
                    "subject_name": subject_name,
                    "mongo_id": mongo_subject_id,
                    "class_id": class_id or None,
                },
            )

        if want_topic:
            conn.execute(
                text(
                    """
                    INSERT INTO topic (topic_id, topic_name, mongo_id, subject_id)
                    VALUES (:topic_id, :topic_name, :mongo_id, :subject_id)
                    ON CONFLICT (topic_id) DO UPDATE
                    SET topic_name  = EXCLUDED.topic_name,
                        mongo_id    = COALESCE(EXCLUDED.mongo_id, topic.mongo_id),
                        subject_id  = EXCLUDED.subject_id
                    """
                ),
                {
                    "topic_id": topic_id,
                    "topic_name": topic_name,
                    "mongo_id": mongo_topic_id,
                    "subject_id": subject_id or None,
                },
            )

        if want_lesson:
            conn.execute(
                text(
                    """
                    INSERT INTO lesson (lesson_id, lesson_name, mongo_id, topic_id)
                    VALUES (:lesson_id, :lesson_name, :mongo_id, :topic_id)
                    ON CONFLICT (lesson_id) DO UPDATE
                    SET lesson_name = EXCLUDED.lesson_name,
                        mongo_id    = COALESCE(EXCLUDED.mongo_id, lesson.mongo_id),
                        topic_id    = EXCLUDED.topic_id
                    """
                ),
                {
                    "lesson_id": lesson_id,
                    "lesson_name": lesson_name,
                    "mongo_id": mongo_lesson_id,
                    "topic_id": topic_id or None,
                },
            )

        if want_chunk:
            conn.execute(
                text(
                    """
                    INSERT INTO chunk (chunk_id, chunk_name, chunk_type, mongo_id, lesson_id)
                    VALUES (:chunk_id, :chunk_name, :chunk_type, :mongo_id, :lesson_id)
                    ON CONFLICT (chunk_id) DO UPDATE
                    SET chunk_name = EXCLUDED.chunk_name,
                        chunk_type = EXCLUDED.chunk_type,
                        mongo_id   = COALESCE(EXCLUDED.mongo_id, chunk.mongo_id),
                        lesson_id  = EXCLUDED.lesson_id
                    """
                ),
                {
                    "chunk_id": chunk_id,
                    "chunk_name": chunk_name,
                    "chunk_type": chunk_type or None,
                    "mongo_id": mongo_chunk_id,
                    "lesson_id": lesson_id or None,
                },
            )

            # keywords: xoá cũ rồi insert lại
            conn.execute(text("DELETE FROM keyword WHERE chunk_id = :chunk_id"), {"chunk_id": chunk_id})

            for kw in keywords:
                kw_name = _clean(kw)
                if not kw_name:
                    continue
                kw_id = _sha384_96(f"{chunk_id}:{kw_name}")
                keyword_ids.append(kw_id)

                conn.execute(
                    text(
                        """
                        INSERT INTO keyword (keyword_id, keyword_name, mongo_id, chunk_id)
                        VALUES (:keyword_id, :keyword_name, :mongo_id, :chunk_id)
                        ON CONFLICT (keyword_id) DO UPDATE
                        SET keyword_name = EXCLUDED.keyword_name,
                            mongo_id      = COALESCE(EXCLUDED.mongo_id, keyword.mongo_id),
                            chunk_id      = EXCLUDED.chunk_id
                        """
                    ),
                    {
                        "keyword_id": kw_id,
                        "keyword_name": kw_name,
                        "mongo_id": None,
                        "chunk_id": chunk_id,
                    },
                )

    return PgIds(
        class_id=class_id or "",
        subject_id=subject_id or "",
        topic_id=topic_id or "",
        lesson_id=lesson_id or "",
        chunk_id=chunk_id or "",
        keyword_ids=keyword_ids,
    )
