from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import quote

from bson import ObjectId

from .mongo_client import get_mongo_client


MEDIA_PREFIX = {"image": "IMG", "video": "VD"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _pick(meta: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        if k in meta:
            s = _clean(meta.get(k))
            if s:
                return s
    return ""


def _extract_last_number(s: str) -> str:
    nums = re.findall(r"\d+", _clean(s))
    return nums[-1] if nums else ""


def _derive_class_map_from_subject_map(subject_map: str) -> str:
    n = _extract_last_number(subject_map)
    return f"L{n}" if n else ""


def _minio_public_base() -> str:
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


def _infer_media_type(bucket: str, meta: Dict[str, Any]) -> str:
    cat = _clean(meta.get("category") or meta.get("itemType") or meta.get("type")).lower()
    if cat in ("image", "video"):
        return cat
    b = _clean(bucket).lower()
    if b == "images":
        return "image"
    if b in ("video", "videos"):
        return "video"
    raise ValueError("Cannot infer media type. Use bucket images/video or category=image/video")


def _normalize_folder_type(v: str) -> str:
    s = _clean(v).lower()
    if s == "subjects":
        return "subject"
    if s == "topics":
        return "topic"
    if s == "lessons":
        return "lesson"
    if s == "chunks":
        return "chunk"
    return s


def _infer_folder_type_from_key(object_key: str, meta: Dict[str, Any]) -> str:
    from_meta = _normalize_folder_type(_pick(meta, "folderType", "followType", "follow_type"))
    if from_meta in ("subject", "topic", "lesson", "chunk"):
        return from_meta
    parts = [p for p in _clean(object_key).split("/") if p]
    if len(parts) >= 2:
        t = _normalize_folder_type(parts[1])
        if t in ("subject", "topic", "lesson", "chunk"):
            return t
    return "chunk"


def _parse_follow_map(follow_map: str) -> Optional[dict[str, str]]:
    s = _clean(follow_map)
    m = re.match(r"^(.+?)_CD(\d+)_B(\d+)_C(\d+)$", s, flags=re.I)
    if m:
        subject_map = m.group(1)
        topic_no = m.group(2)
        lesson_no = m.group(3)
        chunk_no = m.group(4)
        topic_map = f"{subject_map}_CD{topic_no}"
        lesson_map = f"{topic_map}_B{lesson_no}"
        return {
            "follow_type": "chunk",
            "subject_map": subject_map,
            "topic_map": topic_map,
            "lesson_map": lesson_map,
            "chunk_map": s,
            "class_map": _derive_class_map_from_subject_map(subject_map),
            "topic_no": topic_no,
            "lesson_no": lesson_no,
            "chunk_no": chunk_no,
        }
    m = re.match(r"^(.+?)_CD(\d+)_B(\d+)$", s, flags=re.I)
    if m:
        subject_map = m.group(1)
        topic_no = m.group(2)
        lesson_no = m.group(3)
        topic_map = f"{subject_map}_CD{topic_no}"
        return {
            "follow_type": "lesson",
            "subject_map": subject_map,
            "topic_map": topic_map,
            "lesson_map": s,
            "chunk_map": "",
            "class_map": _derive_class_map_from_subject_map(subject_map),
            "topic_no": topic_no,
            "lesson_no": lesson_no,
            "chunk_no": "",
        }
    m = re.match(r"^(.+?)_CD(\d+)$", s, flags=re.I)
    if m:
        subject_map = m.group(1)
        topic_no = m.group(2)
        return {
            "follow_type": "topic",
            "subject_map": subject_map,
            "topic_map": s,
            "lesson_map": "",
            "chunk_map": "",
            "class_map": _derive_class_map_from_subject_map(subject_map),
            "topic_no": topic_no,
            "lesson_no": "",
            "chunk_no": "",
        }
    if s:
        return {
            "follow_type": "subject",
            "subject_map": s,
            "topic_map": "",
            "lesson_map": "",
            "chunk_map": "",
            "class_map": _derive_class_map_from_subject_map(s),
            "topic_no": "",
            "lesson_no": "",
            "chunk_no": "",
        }
    return None


def _validate_and_parse_map_id(map_id: str, media_type: str, folder_type: str) -> dict[str, str]:
    s = _clean(map_id)
    prefix = MEDIA_PREFIX[media_type]
    if not s.upper().startswith(prefix + "_"):
        raise ValueError(f"mapID must start with {prefix}_")
    follow_map = s[len(prefix) + 1 :]
    parsed = _parse_follow_map(follow_map)
    if not parsed:
        raise ValueError(f"mapID format invalid: {map_id}")
    if parsed["follow_type"] != folder_type:
        raise ValueError(
            f"mapID {map_id} does not match folder_type={folder_type}. Expected {folder_type} level"
        )
    parsed["map_id"] = s
    parsed["follow_map"] = follow_map
    return parsed


@dataclass
class MediaSyncResult:
    media_type: str
    collection: str
    mongo_id: ObjectId
    map_id: str
    follow_map: str
    follow_type: str
    class_map: str
    subject_map: str
    topic_map: str
    lesson_map: str
    chunk_map: str
    media_name: str
    media_url: str


def sync_minio_media_to_mongo(
    *,
    bucket: str,
    object_key: str,
    meta: Dict[str, Any],
    actor: str,
) -> MediaSyncResult:
    mg = get_mongo_client()
    db = mg["db"]

    key = (object_key or "").lstrip("/")
    filename = key.rsplit("/", 1)[-1] if key else ""
    media_type = _infer_media_type(bucket, meta)
    folder_type = _infer_folder_type_from_key(object_key, meta)
    map_id = _pick(meta, "mapID", "map_id")
    if not map_id:
        raise ValueError("mapID is required for image/video")

    parsed = _validate_and_parse_map_id(map_id, media_type, folder_type)
    now = _now()
    media_url = _http_url(bucket, key)

    if media_type == "image":
        collection = "images"
        name_field = "imgName"
        desc_field = "imgDescription"
    else:
        collection = "videos"
        name_field = "videoName"
        desc_field = "videoDescription"

    media_name = _pick(meta, name_field, "name") or filename or map_id
    media_desc = _pick(meta, desc_field, "description")

    doc = {
        name_field: media_name,
        desc_field: media_desc,
        f"{media_type}Url" if media_type == "video" else "imgUrl": media_url,
        "createdAt": now,
        "createdBy": actor or "system",
        "status": _pick(meta, "status") or "active",
        "mapID": map_id,
    }

    # fix url field name for video/image explicitly
    if media_type == "video":
        doc["videoUrl"] = media_url
    else:
        doc["imgUrl"] = media_url

    existing = db[collection].find_one({"mapID": map_id})
    if existing:
        mongo_id = existing["_id"]
        db[collection].update_one(
            {"_id": mongo_id},
            {"$set": {**doc, "createdAt": existing.get("createdAt") or now, "createdBy": existing.get("createdBy") or actor or "system"}},
        )
    else:
        mongo_id = db[collection].insert_one(doc).inserted_id

    return MediaSyncResult(
        media_type=media_type,
        collection=collection,
        mongo_id=mongo_id,
        map_id=map_id,
        follow_map=parsed["follow_map"],
        follow_type=parsed["follow_type"],
        class_map=parsed["class_map"],
        subject_map=parsed["subject_map"],
        topic_map=parsed["topic_map"],
        lesson_map=parsed["lesson_map"],
        chunk_map=parsed["chunk_map"],
        media_name=media_name,
        media_url=media_url,
    )
