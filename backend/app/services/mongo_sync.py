from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.parse import quote

from bson import ObjectId

from .mongo_client import get_mongo_client


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _as_str(v: Any) -> str:
    return "" if v is None else str(v)


def _clean_str(v: Any) -> str:
    return _as_str(v).strip()


def _parse_keywords(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [s for s in (_clean_str(x) for x in v) if s]

    s = _clean_str(v)
    if not s:
        return []
    parts = re.split(r"[;,\n\r\t]+", s)
    return [p.strip() for p in parts if p.strip()]


def _extract_number(s: str) -> str:
    m = re.findall(r"\d+", s or "")
    return m[-1] if m else ""


def _minio_public_base() -> str:
    """
    Base URL mà TRÌNH DUYỆT truy cập được.
    Bạn phải set MINIO_PUBLIC_BASE_URL đúng (VD http://localhost:9000).
    """
    public = (os.getenv("MINIO_PUBLIC_BASE_URL") or "").strip()
    if public:
        return public.rstrip("/")

    endpoint = (os.getenv("MINIO_ENDPOINT") or "127.0.0.1:9000").strip()
    secure = (os.getenv("MINIO_SECURE", "false").strip().lower() == "true")
    scheme = "https" if secure else "http"
    return f"{scheme}://{endpoint}".rstrip("/")


def _http_url(bucket: str, key: str) -> str:
    base = _minio_public_base()
    encoded = quote((key or "").lstrip("/"), safe="/")
    return f"{base}/{bucket}/{encoded}"


def _looks_like_file(name: str) -> bool:
    # tối thiểu phải có extension
    return bool(re.search(r"\.[A-Za-z0-9]{1,10}$", name or ""))


@dataclass
class SyncResult:
    class_id: ObjectId
    subject_id: ObjectId
    topic_id: ObjectId
    lesson_id: ObjectId
    chunk_id: ObjectId


def sync_minio_object_to_mongo(
    *,
    bucket: str,
    object_key: str,
    meta: Dict[str, Any],
    actor: str,
) -> SyncResult:
    """
    YÊU CẦU:
    - chunkUrl phải là URL HTTP mở được ngay và PHẢI có tên file
    - collection: classes, subjects, topics, lessons, chunks
    """
    mongo = get_mongo_client()
    db = mongo["db"]

    key = (object_key or "").lstrip("/")
    parts = [p for p in key.split("/") if p]
    if not parts:
        raise ValueError(f"Invalid object_key: '{object_key}'")

    filename = parts[-1]
    if not _looks_like_file(filename):
        # Nếu không phải file (vd 'class-10/lessons') -> KHÔNG ghi chunk để tránh lưu sai URL
        raise ValueError(
            f"object_key is not a file path (missing extension): '{object_key}'. "
            f"Expected something like '.../TT1.docx'"
        )

    dir_parts = parts[:-1]  # folder chứa file

    # ===== URL đúng theo yêu cầu: trỏ thẳng tới FILE =====
    chunk_url = _http_url(bucket, key)

    # (Các URL folder: để metadata thôi, không dùng để mở file)
    subject_prefix = "/".join(dir_parts[:1]) if len(dir_parts) >= 1 else ""
    topic_prefix = "/".join(dir_parts[:2]) if len(dir_parts) >= 2 else subject_prefix
    lesson_prefix = "/".join(dir_parts) if len(dir_parts) >= 1 else topic_prefix

    subject_url = _http_url(bucket, subject_prefix) if subject_prefix else _http_url(bucket, "")
    topic_url = _http_url(bucket, topic_prefix) if topic_prefix else subject_url
    lesson_url = _http_url(bucket, lesson_prefix) if lesson_prefix else topic_url

    # ===== Meta =====
    class_name = _clean_str(meta.get("className") or meta.get("class") or (dir_parts[0] if len(dir_parts) >= 1 else "unknown"))
    subject_name = _clean_str(meta.get("subjectName") or meta.get("subject") or (dir_parts[1] if len(dir_parts) >= 2 else "unknown"))
    subject_title = _clean_str(meta.get("subjectTitle") or meta.get("title") or "")

    topic_name = _clean_str(meta.get("topicName") or meta.get("topic") or (dir_parts[2] if len(dir_parts) >= 3 else "unknown"))
    topic_number = _clean_str(meta.get("topicNumber") or meta.get("topic_number") or _extract_number(topic_name))

    lesson_name = _clean_str(meta.get("lessonName") or meta.get("lesson") or (dir_parts[3] if len(dir_parts) >= 4 else "unknown"))
    lesson_number = _clean_str(meta.get("lessonNumber") or meta.get("lesson_number") or _extract_number(lesson_name))
    lesson_type = _clean_str(meta.get("lessonType") or meta.get("lesson_type") or "")

    chunk_name = _clean_str(meta.get("chunkName") or meta.get("chunk") or (filename.rsplit(".", 1)[0] if filename else "unknown"))
    chunk_number = _clean_str(meta.get("chunk_number") or meta.get("chunkNumber") or _extract_number(chunk_name))
    chunk_desc = _clean_str(meta.get("chunkDescription") or meta.get("description") or "")
    chunk_type = _clean_str(meta.get("chunkType") or meta.get("chunk_type") or lesson_type)
    keywords = _parse_keywords(meta.get("keywords") or meta.get("keyword"))

    category = _clean_str(
        meta.get("subjectCategory")
        or meta.get("topicCategory")
        or meta.get("lessonCategory")
        or meta.get("chunkCategory")
        or "document"
    )

    now = _now()

    # ===== Collections đúng =====
    COL_CLASSES = "classes"
    COL_SUBJECTS = "subjects"
    COL_TOPICS = "topics"
    COL_LESSONS = "lessons"
    COL_CHUNKS = "chunks"

    # ===== Upsert CLASS =====
    class_doc = db[COL_CLASSES].find_one({"className": class_name})
    if class_doc:
        class_id = class_doc["_id"]
        db[COL_CLASSES].update_one({"_id": class_id}, {"$set": {"updatedAt": now}})
    else:
        class_id = db[COL_CLASSES].insert_one({"className": class_name, "createdAt": now, "updatedAt": now}).inserted_id

    # ===== Upsert SUBJECT =====
    subject_filter = {"classID": str(class_id), "subjectName": subject_name, "subjectCategory": category}
    subject_doc = db[COL_SUBJECTS].find_one(subject_filter)
    if subject_doc:
        subject_id = subject_doc["_id"]
        db[COL_SUBJECTS].update_one(
            {"_id": subject_id},
            {"$set": {"subjectUrl": subject_url, "subjectTitle": subject_title, "updatedAt": now}},
        )
    else:
        subject_id = db[COL_SUBJECTS].insert_one(
            {
                **subject_filter,
                "subjectUrl": subject_url,
                "subjectTitle": subject_title,
                "status": "active",
                "createdBy": actor or "system",
                "createdAt": now,
                "updatedAt": now,
            }
        ).inserted_id

    # ===== Upsert TOPIC =====
    topic_filter = {"subjectID": str(subject_id), "topicNumber": topic_number, "topicCategory": category}
    if not topic_filter["topicNumber"]:
        topic_filter.pop("topicNumber")
        topic_filter["topicName"] = topic_name

    topic_doc = db[COL_TOPICS].find_one(topic_filter)
    if topic_doc:
        topic_id = topic_doc["_id"]
        db[COL_TOPICS].update_one(
            {"_id": topic_id},
            {"$set": {"topicName": topic_name, "topicUrl": topic_url, "updatedAt": now}},
        )
    else:
        topic_id = db[COL_TOPICS].insert_one(
            {
                "subjectID": str(subject_id),
                "topicName": topic_name,
                "topicUrl": topic_url,
                "status": "active",
                "createdBy": actor or "system",
                "createdAt": now,
                "updatedAt": now,
                "topicNumber": topic_number,
                "topicCategory": category,
            }
        ).inserted_id

    # ===== Upsert LESSON =====
    lesson_filter = {"topicID": str(topic_id), "lessonNumber": lesson_number, "lessonType": lesson_type, "lessonCategory": category}
    if not lesson_filter["lessonNumber"]:
        lesson_filter.pop("lessonNumber")
        lesson_filter["lessonName"] = lesson_name

    lesson_doc = db[COL_LESSONS].find_one(lesson_filter)
    if lesson_doc:
        lesson_id = lesson_doc["_id"]
        db[COL_LESSONS].update_one(
            {"_id": lesson_id},
            {"$set": {"lessonName": lesson_name, "lessonUrl": lesson_url, "updatedAt": now}},
        )
    else:
        lesson_id = db[COL_LESSONS].insert_one(
            {
                "topicID": str(topic_id),
                "lessonName": lesson_name,
                "lessonUrl": lesson_url,
                "status": "active",
                "createdBy": actor or "system",
                "createdAt": now,
                "updatedAt": now,
                "lessonNumber": lesson_number,
                "lessonType": lesson_type,
                "lessonCategory": category,
            }
        ).inserted_id

    # ===== Upsert CHUNK =====
    # chunkUrl luôn là URL FILE thật (có tên file)
    chunk_filter = {"chunkUrl": chunk_url, "chunkCategory": category}
    chunk_doc = db[COL_CHUNKS].find_one(chunk_filter)
    if chunk_doc:
        chunk_id = chunk_doc["_id"]
        db[COL_CHUNKS].update_one(
            {"_id": chunk_id},
            {"$set": {
                "lessonID": str(lesson_id),
                "chunkName": chunk_name,
                "chunkType": chunk_type,
                "keywords": keywords,
                "chunkDescription": chunk_desc,
                "chunk_number": chunk_number,
                "updatedAt": now,
                "status": "active",
            }},
        )
    else:
        chunk_id = db[COL_CHUNKS].insert_one(
            {
                "lessonID": str(lesson_id),
                "chunkName": chunk_name,
                "chunkUrl": chunk_url,
                "keywords": keywords,
                "chunkCategory": category,
                "chunkType": chunk_type,
                "status": "active",
                "createdBy": actor or "system",
                "createdAt": now,
                "updatedAt": now,
                "chunkDescription": chunk_desc,
                "chunk_number": chunk_number,
            }
        ).inserted_id

    return SyncResult(
        class_id=class_id,
        subject_id=subject_id,
        topic_id=topic_id,
        lesson_id=lesson_id,
        chunk_id=chunk_id,
    )
