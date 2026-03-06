from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional

from bson import ObjectId
from sqlalchemy import text

from .mongo_client import get_mongo_client
from .postgre_client import get_engine


@dataclass
class PgMediaSyncResult:
    media_type: str
    media_id: str
    media_name: str
    mongo_id: str
    follow_id: str
    follow_type: str


def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _parse_follow_map(follow_map: str) -> dict[str, str]:
    s = _clean(follow_map)
    m = re.match(r"^(.+?)_CD(\d+)_B(\d+)_C(\d+)$", s, flags=re.I)
    if m:
        return {
            "subject_map": m.group(1),
            "follow_type": "chunk",
            "topic_no": m.group(2),
            "lesson_no": m.group(3),
            "chunk_no": m.group(4),
        }
    m = re.match(r"^(.+?)_CD(\d+)_B(\d+)$", s, flags=re.I)
    if m:
        return {
            "subject_map": m.group(1),
            "follow_type": "lesson",
            "topic_no": m.group(2),
            "lesson_no": m.group(3),
            "chunk_no": "",
        }
    m = re.match(r"^(.+?)_CD(\d+)$", s, flags=re.I)
    if m:
        return {
            "subject_map": m.group(1),
            "follow_type": "topic",
            "topic_no": m.group(2),
            "lesson_no": "",
            "chunk_no": "",
        }
    if s:
        return {
            "subject_map": s,
            "follow_type": "subject",
            "topic_no": "",
            "lesson_no": "",
            "chunk_no": "",
        }
    raise ValueError("follow_map is required")


def _follow_id_from_follow_map(follow_map: str) -> tuple[str, str]:
    p = _parse_follow_map(follow_map)
    subject_map = p["subject_map"]
    parts = [subject_map]
    if p["topic_no"]:
        parts.append(f"T{int(p['topic_no'])}")
    if p["lesson_no"]:
        parts.append(f"L{int(p['lesson_no'])}")
    if p["chunk_no"]:
        parts.append(f"C{int(p['chunk_no'])}")
    return "_".join(parts), p["follow_type"]


def _media_id_from_map_id(map_id: str) -> str:
    s = _clean(map_id)
    if s.upper().startswith("IMG_"):
        prefix = "IMG"
        follow_map = s[4:]
    elif s.upper().startswith("VD_"):
        prefix = "VD"
        follow_map = s[3:]
    else:
        raise ValueError("mapID must start with IMG_ or VD_")
    follow_id, _ = _follow_id_from_follow_map(follow_map)
    return f"{prefix}_{follow_id}"


def sync_postgre_media_from_mongo(*, media_type: str, mongo_id: str) -> PgMediaSyncResult:
    mg = get_mongo_client()
    db = mg["db"]
    engine = get_engine()

    collection = "images" if media_type == "image" else "videos"
    table = "image" if media_type == "image" else "video"
    id_col = "img_id" if media_type == "image" else "video_id"
    name_col = "img_name" if media_type == "image" else "video_name"
    mongo_doc = db[collection].find_one({"_id": ObjectId(mongo_id)})
    if not mongo_doc:
        raise ValueError(f"Mongo media not found: {collection}({_clean(mongo_id)})")

    map_id = _clean(mongo_doc.get("mapID"))
    if not map_id:
        raise ValueError("Mongo media missing mapID")

    media_id = _media_id_from_map_id(map_id)
    if media_type == "image":
        media_name = _clean(mongo_doc.get("imgName")) or media_id
    else:
        media_name = _clean(mongo_doc.get("videoName")) or media_id

    prefix_len = 4 if map_id.upper().startswith("IMG_") else 3
    follow_map = map_id[prefix_len:]
    follow_id, follow_type = _follow_id_from_follow_map(follow_map)

    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {table} ({id_col}, {name_col}, mongo_id, follow_id, follow_type)
                VALUES (:{id_col}, :{name_col}, :mongo_id, :follow_id, :follow_type)
                ON CONFLICT ({id_col}) DO UPDATE
                SET {name_col} = EXCLUDED.{name_col},
                    mongo_id = EXCLUDED.mongo_id,
                    follow_id = EXCLUDED.follow_id,
                    follow_type = EXCLUDED.follow_type
                """
            ),
            {
                id_col: media_id,
                name_col: media_name,
                "mongo_id": _clean(mongo_id),
                "follow_id": follow_id,
                "follow_type": follow_type,
            },
        )

    return PgMediaSyncResult(
        media_type=media_type,
        media_id=media_id,
        media_name=media_name,
        mongo_id=_clean(mongo_id),
        follow_id=follow_id,
        follow_type=follow_type,
    )
