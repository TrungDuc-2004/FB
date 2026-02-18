from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from bson import ObjectId

from .mongo_client import get_mongo_client
from .keyword_embedding import embed_keyword_cached, get_keyword_embedder


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _as_str(v: Any) -> str:
    return "" if v is None else str(v)


def _clean_str(v: Any) -> str:
    return _as_str(v).strip()


def _pick(meta: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        if not k:
            continue
        if k not in meta:
            continue
        v = meta.get(k)
        if v is None:
            continue
        s = _clean_str(v)
        if s:
            return s
    return ""


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


def _extract_last_number(s: str) -> str:
    m = re.findall(r"\d+", s or "")
    return m[-1] if m else ""


def _minio_public_base() -> str:
    """Base URL mà TRÌNH DUYỆT truy cập được.

    Bắt buộc set MINIO_PUBLIC_BASE_URL đúng (VD http://localhost:9000)
    để Mongo lưu URL mở trực tiếp được.
    """
    public = (os.getenv("MINIO_PUBLIC_BASE_URL") or "").strip()
    if public:
        return public.rstrip("/")

    # fallback (dev)
    endpoint = (os.getenv("MINIO_ENDPOINT") or "127.0.0.1:9000").strip()
    secure = (os.getenv("MINIO_SECURE", "false").strip().lower() == "true")
    scheme = "https" if secure else "http"
    return f"{scheme}://{endpoint}".rstrip("/")


def _http_url(bucket: str, key: str) -> str:
    base = _minio_public_base()
    encoded = quote((key or "").lstrip("/"), safe="/")
    return f"{base}/{bucket}/{encoded}"


def _infer_category_from_bucket(bucket: str) -> str:
    b = (bucket or "").strip().lower()
    if b == "images":
        return "image"
    if b in ("video", "videos"):
        return "video"
    return "document"


def _normalize_folder_type(s: str) -> str:
    x = (s or "").strip().lower()
    if x == "subjects":
        return "subject"
    if x == "topics":
        return "topic"
    if x == "lessons":
        return "lesson"
    if x == "chunks":
        return "chunk"
    return x


def _infer_folder_type_from_key(object_key: str) -> str:
    parts = [p for p in (object_key or "").split("/") if p]
    # format phổ biến: class-10/chunks/file.pdf
    if len(parts) >= 2:
        t = _normalize_folder_type(parts[1])
        if t in ("subject", "topic", "lesson", "chunk"):
            return t
    return "chunk"  # fallback an toàn


def _derive_class_map_from_subject_map(subject_map: str) -> str:
    n = _extract_last_number(subject_map)
    return f"L{n}" if n else ""


def _parse_topic_map(topic_map: str) -> Optional[Dict[str, str]]:
    s = _clean_str(topic_map)
    m = re.match(r"^(.+?)_CD(\d+)$", s, flags=re.I)
    if not m:
        return None
    subject_map = m.group(1)
    topic_number = m.group(2)
    return {
        "subject_map": subject_map,
        "topic_map": s,
        "topicNumber": topic_number,
        "class_map": _derive_class_map_from_subject_map(subject_map),
    }


def _parse_lesson_map(lesson_map: str) -> Optional[Dict[str, str]]:
    s = _clean_str(lesson_map)
    m = re.match(r"^(.+?)_CD(\d+)_B(\d+)$", s, flags=re.I)
    if not m:
        return None
    subject_map = m.group(1)
    topic_number = m.group(2)
    lesson_number = m.group(3)
    topic_map = f"{subject_map}_CD{topic_number}"
    return {
        "subject_map": subject_map,
        "topic_map": topic_map,
        "lesson_map": s,
        "topicNumber": topic_number,
        "lessonNumber": lesson_number,
        "class_map": _derive_class_map_from_subject_map(subject_map),
    }


def _parse_chunk_map(chunk_map: str) -> Optional[Dict[str, str]]:
    s = _clean_str(chunk_map)
    m = re.match(r"^(.+?)_CD(\d+)_B(\d+)_C(\d+)$", s, flags=re.I)
    if not m:
        return None
    subject_map = m.group(1)
    topic_number = m.group(2)
    lesson_number = m.group(3)
    chunk_number = m.group(4)
    topic_map = f"{subject_map}_CD{topic_number}"
    lesson_map = f"{topic_map}_B{lesson_number}"
    return {
        "subject_map": subject_map,
        "topic_map": topic_map,
        "lesson_map": lesson_map,
        "chunk_map": s,
        "topicNumber": topic_number,
        "lessonNumber": lesson_number,
        "chunkNumber": chunk_number,
        "class_map": _derive_class_map_from_subject_map(subject_map),
    }


@dataclass
class SyncResult:
    folder_type: str

    # maps
    class_map: str
    subject_map: str
    topic_map: Optional[str]
    lesson_map: Optional[str]
    chunk_map: Optional[str]

    # mongo ids
    class_id: ObjectId
    subject_id: ObjectId
    topic_id: Optional[ObjectId]
    lesson_id: Optional[ObjectId]
    chunk_id: Optional[ObjectId]


def sync_minio_object_to_mongo(
    *,
    bucket: str,
    object_key: str,
    meta: Dict[str, Any],
    actor: str,
) -> SyncResult:
    """Sync metadata sau khi upload object vào MinIO.

    QUY TẮC (theo yêu cầu của bạn):
    - URL lưu trong Mongo phải là dạng HTTP truy cập được: {MINIO_PUBLIC_BASE_URL}/{bucket}/{object_key}
    - Map ID là "định danh" để Postgre sync quan hệ cha-con
      class: L10
      subject: TH10
      topic: TH10_CD1
      lesson: TH10_CD1_B1
      chunk: TH10_CD1_B1_C1
    - Chunk chỉ cần lưu lessonID = lesson_map (không cần lưu subject/topic/class trong doc chunk).
    """
    mg = get_mongo_client()
    db = mg["db"]

    key = (object_key or "").lstrip("/")
    parts = [p for p in key.split("/") if p]
    if len(parts) < 2:
        raise ValueError(f"Invalid object_key: '{object_key}'")

    folder_type = _normalize_folder_type(_pick(meta, "folderType") or _infer_folder_type_from_key(key))
    category = _clean_str(_pick(meta, "category") or _infer_category_from_bucket(bucket))

    filename = parts[-1]
    if "." not in filename:
        # nếu user insert không upload file thì name vẫn phải có đuôi (để đúng là FILE)
        raise ValueError(f"Invalid filename (missing extension): '{filename}'")

    file_url = _http_url(bucket, key)

    # ====== MAP: user chỉ nhập 1 map sâu nhất, backend phải suy ra chain ======
    class_map = _pick(meta, "class_map", "classMap", "classID")
    subject_map = _pick(meta, "subject_map", "subjectMap", "subjectID")
    topic_map = _pick(meta, "topic_map", "topicMap", "topicID")
    lesson_map = _pick(meta, "lesson_map", "lessonMap", "lessonID")
    chunk_map = _pick(meta, "chunk_map", "chunkMap", "chunkID")

    if folder_type == "chunk":
        if not chunk_map:
            raise ValueError("chunk_map is required (VD: TH10_CD1_B1_C1)")
        d = _parse_chunk_map(chunk_map)
        if not d:
            raise ValueError("chunk_map format sai. VD đúng: TH10_CD1_B1_C1")
        class_map = class_map or d["class_map"]
        subject_map = subject_map or d["subject_map"]
        topic_map = topic_map or d["topic_map"]
        lesson_map = lesson_map or d["lesson_map"]
        chunk_map = d["chunk_map"]
    elif folder_type == "lesson":
        if not lesson_map:
            raise ValueError("lesson_map is required (VD: TH10_CD1_B1)")
        d = _parse_lesson_map(lesson_map)
        if not d:
            raise ValueError("lesson_map format sai. VD đúng: TH10_CD1_B1")
        class_map = class_map or d["class_map"]
        subject_map = subject_map or d["subject_map"]
        topic_map = topic_map or d["topic_map"]
        lesson_map = d["lesson_map"]
    elif folder_type == "topic":
        if not topic_map:
            raise ValueError("topic_map is required (VD: TH10_CD1)")
        d = _parse_topic_map(topic_map)
        if not d:
            raise ValueError("topic_map format sai. VD đúng: TH10_CD1")
        class_map = class_map or d["class_map"]
        subject_map = subject_map or d["subject_map"]
        topic_map = d["topic_map"]
    else:
        # subject
        if not subject_map:
            raise ValueError("subject_map is required (VD: TH10)")
        class_map = class_map or _derive_class_map_from_subject_map(subject_map)

    if not class_map:
        raise ValueError("Cannot derive class_map. Hãy nhập subject_map đúng (VD TH10) hoặc class_map (VD L10)")

    now = _now()

    # ====== Names (giảm bắt buộc, cho phép rỗng) ======
    subject_name = _pick(meta, "subjectName", "subject", "subject_name") or subject_map
    subject_title = _pick(meta, "subjectTitle", "title", "subject_title")

    topic_name = _pick(meta, "topicName", "topic", "topic_name") or (topic_map or "")
    lesson_name = _pick(meta, "lessonName", "lesson", "lesson_name") or (lesson_map or "")
    lesson_type = _pick(meta, "lessonType", "lesson_type")

    chunk_name = _pick(meta, "chunkName", "chunk", "chunk_name") or (filename.rsplit(".", 1)[0] if filename else (chunk_map or ""))
    chunk_type = _pick(meta, "chunkType", "chunk_type") or lesson_type
    chunk_desc = _pick(meta, "chunkDescription", "description", "chunk_description")
    keywords = _parse_keywords(_pick(meta, "keywords", "keyword"))

    # ====== Collections ======
    COL_CLASSES = "classes"
    COL_SUBJECTS = "subjects"
    COL_TOPICS = "topics"
    COL_LESSONS = "lessons"
    COL_CHUNKS = "chunks"
    COL_KEYWORDS = "keywords"

    # ====== Upsert CLASS (key = classID) ======
    class_filter = {"classID": class_map}
    class_doc = db[COL_CLASSES].find_one(class_filter)
    if class_doc:
        class_id = class_doc["_id"]
        db[COL_CLASSES].update_one(
            {"_id": class_id},
            {"$set": {"className": _pick(meta, "className") or class_doc.get("className") or class_map, "updatedAt": now}},
        )
    else:
        class_id = db[COL_CLASSES].insert_one(
            {
                **class_filter,
                "className": _pick(meta, "className") or class_map,
                "createdAt": now,
                "updatedAt": now,
            }
        ).inserted_id

    # ====== Upsert SUBJECT (key = subjectID) ======
    subject_filter = {"subjectID": subject_map, "subjectCategory": category}
    subject_doc = db[COL_SUBJECTS].find_one(subject_filter)
    if subject_doc:
        subject_id = subject_doc["_id"]
        set_fields = {
            "classID": class_map,
            "subjectName": subject_name,
            "subjectTitle": subject_title,
            "updatedAt": now,
        }
        # chỉ update url khi insert trong folder subjects
        if folder_type == "subject":
            set_fields["subjectUrl"] = file_url
        db[COL_SUBJECTS].update_one({"_id": subject_id}, {"$set": set_fields})
    else:
        subject_id = db[COL_SUBJECTS].insert_one(
            {
                **subject_filter,
                "classID": class_map,
                "subjectName": subject_name,
                "subjectTitle": subject_title,
                "subjectUrl": file_url if folder_type == "subject" else "",
                "status": "active",
                "createdBy": actor or "system",
                "createdAt": now,
                "updatedAt": now,
            }
        ).inserted_id

    # ====== Upsert TOPIC (nếu có topic_map) ======
    topic_id: Optional[ObjectId] = None
    if topic_map:
        topic_filter = {"topicID": topic_map, "topicCategory": category}
        topic_doc = db[COL_TOPICS].find_one(topic_filter)
        if topic_doc:
            topic_id = topic_doc["_id"]
            set_fields = {
                "subjectID": subject_map,
                "topicName": topic_name,
                "updatedAt": now,
            }
            if folder_type == "topic":
                set_fields["topicUrl"] = file_url
            db[COL_TOPICS].update_one({"_id": topic_id}, {"$set": set_fields})
        else:
            topic_id = db[COL_TOPICS].insert_one(
                {
                    **topic_filter,
                    "subjectID": subject_map,
                    "topicName": topic_name,
                    "topicUrl": file_url if folder_type == "topic" else "",
                    "status": "active",
                    "createdBy": actor or "system",
                    "createdAt": now,
                    "updatedAt": now,
                }
            ).inserted_id

    # ====== Upsert LESSON (nếu có lesson_map) ======
    lesson_id: Optional[ObjectId] = None
    if lesson_map:
        lesson_filter = {"lessonID": lesson_map, "lessonCategory": category}
        lesson_doc = db[COL_LESSONS].find_one(lesson_filter)
        if lesson_doc:
            lesson_id = lesson_doc["_id"]
            set_fields = {
                "topicID": topic_map or "",
                "lessonName": lesson_name,
                "lessonType": lesson_type,
                "updatedAt": now,
            }
            if folder_type == "lesson":
                set_fields["lessonUrl"] = file_url
            db[COL_LESSONS].update_one({"_id": lesson_id}, {"$set": set_fields})
        else:
            lesson_id = db[COL_LESSONS].insert_one(
                {
                    **lesson_filter,
                    "topicID": topic_map or "",
                    "lessonName": lesson_name,
                    "lessonType": lesson_type,
                    "lessonUrl": file_url if folder_type == "lesson" else "",
                    "status": "active",
                    "createdBy": actor or "system",
                    "createdAt": now,
                    "updatedAt": now,
                }
            ).inserted_id

    # ====== Upsert CHUNK (chỉ khi folder_type == chunk) ======
    chunk_id: Optional[ObjectId] = None
    if folder_type == "chunk":
        assert chunk_map, "chunk_map must exist"
        assert lesson_map, "lesson_map must exist"

        chunk_filter = {"chunkID": chunk_map, "chunkCategory": category}
        chunk_doc = db[COL_CHUNKS].find_one(chunk_filter)
        if chunk_doc:
            chunk_id = chunk_doc["_id"]
            db[COL_CHUNKS].update_one(
                {"_id": chunk_id},
                {
                    "$set": {
                        "lessonID": lesson_map,  # QUAN TRỌNG: lesson_map
                        "chunkName": chunk_name,
                        "chunkType": chunk_type,
                        "chunkUrl": file_url,
                        # giữ keywords ở chunk để tương thích dữ liệu cũ/UI,
                        # nhưng canonical source mới là collection `keywords`.
                        "keywords": keywords,
                        "chunkDescription": chunk_desc,
                        "updatedAt": now,
                    }
                },
            )
        else:
            chunk_id = db[COL_CHUNKS].insert_one(
                {
                    **chunk_filter,
                    "lessonID": lesson_map,  # QUAN TRỌNG: lesson_map
                    "chunkName": chunk_name,
                    "chunkType": chunk_type,
                    "chunkUrl": file_url,
                    "keywords": keywords,
                    "chunkDescription": chunk_desc,
                    "status": "active",
                    "createdBy": actor or "system",
                    "createdAt": now,
                    "updatedAt": now,
                }
            ).inserted_id

        # ====== Upsert KEYWORDS (tách riêng collection) ======
        # Quy ước keyword map ID: <chunk_map>_K1, <chunk_map>_K2, ...
        # Ví dụ: TH10_CD5_B18_C2_K1

        # 1) Xoá keyword cũ của chunk để tránh lệch (đổi list keyword => rebuild)
        try:
            db[COL_KEYWORDS].delete_many({"chunkID": chunk_map})
        except Exception:
            # collection chưa tồn tại cũng không sao
            pass

        # 2) Tạo lại keyword docs + embedding
        if keywords:
            embedder = get_keyword_embedder()
            for idx, kw_name in enumerate(keywords, start=1):
                kw_name = _clean_str(kw_name)
                if not kw_name:
                    continue

                kw_map = f"{chunk_map}_K{idx}"
                emb = embed_keyword_cached(kw_name)

                db[COL_KEYWORDS].update_one(
                    {"keywordID": kw_map},
                    {
                        "$set": {
                            "chunkID": chunk_map,
                            "keywordName": kw_name,
                            "keywordEmbedding": emb,
                            "embeddingProvider": getattr(embedder, "name", ""),
                            "updatedAt": now,
                        },
                        "$setOnInsert": {
                            "status": "active",
                            "createdBy": actor or "system",
                            "createdAt": now,
                        },
                    },
                    upsert=True,
                )

    return SyncResult(
        folder_type=folder_type,
        class_map=class_map,
        subject_map=subject_map,
        topic_map=topic_map,
        lesson_map=lesson_map,
        chunk_map=chunk_map if folder_type == "chunk" else None,
        class_id=class_id,
        subject_id=subject_id,
        topic_id=topic_id,
        lesson_id=lesson_id,
        chunk_id=chunk_id,
    )
