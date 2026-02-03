from __future__ import annotations

"""
Sync PostgreSQL FROM MongoDB (không lấy từ meta/path).

Chỉ insert/upsert đúng các cột theo schema bạn đưa:
- class(class_id, class_name, mongo_id)
- subject(subject_id, subject_name, mongo_id, class_id)
- topic(topic_id, topic_name, mongo_id, subject_id)
- lesson(lesson_id, lesson_name, mongo_id, topic_id)
- chunk(chunk_id, chunk_name, chunk_type, mongo_id, lesson_id)
- keyword(keyword_id, keyword_name, mongo_id, chunk_id)

Nguồn dữ liệu: Mongo collections (plural):
- classes, subjects, topics, lessons, chunks
"""

import hashlib
from dataclasses import dataclass
from typing import List, Optional

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


def _pick(db, col: str, _id: str) -> dict:
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

    # ---- đọc dữ liệu từ Mongo (đúng yêu cầu: mongo -> postgre) ----
    c_doc = _pick(db, "classes", mongo_class_id)
    s_doc = _pick(db, "subjects", mongo_subject_id)
    t_doc = _pick(db, "topics", mongo_topic_id)
    l_doc = _pick(db, "lessons", mongo_lesson_id)
    ch_doc = _pick(db, "chunks", mongo_chunk_id)

    class_name = _clean(c_doc.get("className"))
    subject_name = _clean(s_doc.get("subjectName"))
    topic_name = _clean(t_doc.get("topicName"))
    lesson_name = _clean(l_doc.get("lessonName"))
    chunk_name = _clean(ch_doc.get("chunkName"))
    chunk_type = _clean(ch_doc.get("chunkType"))

    keywords = ch_doc.get("keywords") or []
    if not isinstance(keywords, list):
        keywords = []

    # ---- sinh PK theo mongo_id (ổn định + đúng độ dài cột) ----
    class_id_guess = _md5_32(mongo_class_id)         # VARCHAR(32)
    subject_id_guess = _md5_32(mongo_subject_id)     # VARCHAR(32)
    topic_id_guess = _sha256_64(mongo_topic_id)      # VARCHAR(64)
    lesson_id_guess = _sha256_64(mongo_lesson_id)    # VARCHAR(64)
    chunk_id_guess = _sha256_64(mongo_chunk_id)      # VARCHAR(64)

    engine = get_engine()

    with engine.begin() as conn:
        # nếu trước đó đã sync theo mongo_id thì lấy lại PK cũ (tránh lệch phiên bản)
        class_id = _get_pk_by_mongo(conn, "class", "class_id", mongo_class_id) or class_id_guess
        subject_id = _get_pk_by_mongo(conn, "subject", "subject_id", mongo_subject_id) or subject_id_guess
        topic_id = _get_pk_by_mongo(conn, "topic", "topic_id", mongo_topic_id) or topic_id_guess
        lesson_id = _get_pk_by_mongo(conn, "lesson", "lesson_id", mongo_lesson_id) or lesson_id_guess
        chunk_id = _get_pk_by_mongo(conn, "chunk", "chunk_id", mongo_chunk_id) or chunk_id_guess

        # 1) class
        conn.execute(
            text("""
                INSERT INTO class (class_id, class_name, mongo_id)
                VALUES (:class_id, :class_name, :mongo_id)
                ON CONFLICT (class_id) DO UPDATE
                SET class_name = EXCLUDED.class_name,
                    mongo_id   = COALESCE(EXCLUDED.mongo_id, class.mongo_id)
            """),
            {"class_id": class_id, "class_name": class_name, "mongo_id": mongo_class_id},
        )

        # 2) subject
        conn.execute(
            text("""
                INSERT INTO subject (subject_id, subject_name, mongo_id, class_id)
                VALUES (:subject_id, :subject_name, :mongo_id, :class_id)
                ON CONFLICT (subject_id) DO UPDATE
                SET subject_name = EXCLUDED.subject_name,
                    mongo_id     = COALESCE(EXCLUDED.mongo_id, subject.mongo_id),
                    class_id     = EXCLUDED.class_id
            """),
            {
                "subject_id": subject_id,
                "subject_name": subject_name,
                "mongo_id": mongo_subject_id,
                "class_id": class_id,
            },
        )

        # 3) topic
        conn.execute(
            text("""
                INSERT INTO topic (topic_id, topic_name, mongo_id, subject_id)
                VALUES (:topic_id, :topic_name, :mongo_id, :subject_id)
                ON CONFLICT (topic_id) DO UPDATE
                SET topic_name  = EXCLUDED.topic_name,
                    mongo_id    = COALESCE(EXCLUDED.mongo_id, topic.mongo_id),
                    subject_id  = EXCLUDED.subject_id
            """),
            {
                "topic_id": topic_id,
                "topic_name": topic_name,
                "mongo_id": mongo_topic_id,
                "subject_id": subject_id,
            },
        )

        # 4) lesson
        conn.execute(
            text("""
                INSERT INTO lesson (lesson_id, lesson_name, mongo_id, topic_id)
                VALUES (:lesson_id, :lesson_name, :mongo_id, :topic_id)
                ON CONFLICT (lesson_id) DO UPDATE
                SET lesson_name = EXCLUDED.lesson_name,
                    mongo_id    = COALESCE(EXCLUDED.mongo_id, lesson.mongo_id),
                    topic_id    = EXCLUDED.topic_id
            """),
            {
                "lesson_id": lesson_id,
                "lesson_name": lesson_name,
                "mongo_id": mongo_lesson_id,
                "topic_id": topic_id,
            },
        )

        # 5) chunk
        conn.execute(
            text("""
                INSERT INTO chunk (chunk_id, chunk_name, chunk_type, mongo_id, lesson_id)
                VALUES (:chunk_id, :chunk_name, :chunk_type, :mongo_id, :lesson_id)
                ON CONFLICT (chunk_id) DO UPDATE
                SET chunk_name = EXCLUDED.chunk_name,
                    chunk_type = EXCLUDED.chunk_type,
                    mongo_id   = COALESCE(EXCLUDED.mongo_id, chunk.mongo_id),
                    lesson_id  = EXCLUDED.lesson_id
            """),
            {
                "chunk_id": chunk_id,
                "chunk_name": chunk_name,
                "chunk_type": chunk_type or None,
                "mongo_id": mongo_chunk_id,
                "lesson_id": lesson_id,
            },
        )

        # 6) keyword (đồng bộ “đúng theo chunk” -> xoá cũ rồi insert lại)
        conn.execute(
            text("DELETE FROM keyword WHERE chunk_id = :chunk_id"),
            {"chunk_id": chunk_id},
        )

        keyword_ids: List[str] = []
        for kw in keywords:
            kw_name = _clean(kw)
            if not kw_name:
                continue
            kw_id = _sha384_96(f"{chunk_id}:{kw_name}")
            keyword_ids.append(kw_id)

            conn.execute(
                text("""
                    INSERT INTO keyword (keyword_id, keyword_name, mongo_id, chunk_id)
                    VALUES (:keyword_id, :keyword_name, :mongo_id, :chunk_id)
                    ON CONFLICT (keyword_id) DO UPDATE
                    SET keyword_name = EXCLUDED.keyword_name,
                        mongo_id      = COALESCE(EXCLUDED.mongo_id, keyword.mongo_id),
                        chunk_id      = EXCLUDED.chunk_id
                """),
                {
                    "keyword_id": kw_id,
                    "keyword_name": kw_name,
                    "mongo_id": None,     # keyword không có mongo _id riêng
                    "chunk_id": chunk_id,
                },
            )

    return PgIds(
        class_id=class_id,
        subject_id=subject_id,
        topic_id=topic_id,
        lesson_id=lesson_id,
        chunk_id=chunk_id,
        keyword_ids=keyword_ids,
    )
