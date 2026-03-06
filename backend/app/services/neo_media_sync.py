from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from bson import ObjectId

from .mongo_client import get_mongo_client
from .neo_client import neo4j_driver


PARENT_LABEL = {
    "subject": "Subject",
    "topic": "Topic",
    "lesson": "Lesson",
    "chunk": "Chunk",
}
IMAGE_GROUP_LABEL = {
    "subject": "ImgSubject",
    "topic": "ImgTopic",
    "lesson": "ImgLesson",
    "chunk": "ImgChunk",
}
VIDEO_GROUP_LABEL = {
    "subject": "VideoSubject",
    "topic": "VideoTopic",
    "lesson": "VideoLesson",
    "chunk": "VideoChunk",
}


@dataclass
class NeoMediaSyncResult:
    ok: bool
    created_or_updated: dict[str, int]


def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


def sync_media_to_neo4j(
    *,
    media_type: str,
    media_id: str,
    mongo_id: str,
    follow_id: str,
    follow_type: str,
) -> NeoMediaSyncResult:
    media_type = _clean(media_type).lower()
    follow_type = _clean(follow_type).lower()
    if media_type not in ("image", "video"):
        raise ValueError("media_type must be image or video")
    if follow_type not in PARENT_LABEL:
        raise ValueError("follow_type must be subject/topic/lesson/chunk")

    mg = get_mongo_client()
    db = mg["db"]
    collection = "images" if media_type == "image" else "videos"
    doc = db[collection].find_one({"_id": ObjectId(mongo_id)})
    if not doc:
        raise ValueError(f"Mongo media not found: {collection}({_clean(mongo_id)})")

    name = _clean(doc.get("imgName" if media_type == "image" else "videoName")) or media_id
    url = _clean(doc.get("imgUrl" if media_type == "image" else "videoUrl"))
    description = _clean(doc.get("imgDescription" if media_type == "image" else "videoDescription"))
    map_id = _clean(doc.get("mapID"))

    parent_label = PARENT_LABEL[follow_type]
    group_label = IMAGE_GROUP_LABEL[follow_type] if media_type == "image" else VIDEO_GROUP_LABEL[follow_type]
    group_prefix = "IMG_GROUP" if media_type == "image" else "VIDEO_GROUP"
    rel_parent_to_group = "HAS_IMAGE_GROUP" if media_type == "image" else "HAS_VIDEO_GROUP"
    rel_group_to_media = "HAS_IMAGE" if media_type == "image" else "HAS_VIDEO"
    media_label = "Image" if media_type == "image" else "Video"
    group_pg_id = f"{group_prefix}_{follow_id}"

    cypher = f"""
    MERGE (p:{parent_label} {{pg_id: $follow_id}})
    MERGE (g:{group_label} {{pg_id: $group_pg_id}})
    SET g.follow_id = $follow_id,
        g.follow_type = $follow_type
    MERGE (p)-[:{rel_parent_to_group}]->(g)
    MERGE (m:{media_label} {{pg_id: $media_id}})
    SET m.mongo_id = $mongo_id,
        m.name = $name,
        m.url = $url,
        m.description = $description,
        m.map_id = $map_id
    MERGE (g)-[:{rel_group_to_media}]->(m)
    """

    driver = neo4j_driver()
    db_name = (os.getenv("NEO4J_DATABASE") or "").strip() or None
    with driver.session(database=db_name) as session:  # type: ignore[arg-type]
        session.run(
            cypher,
            follow_id=follow_id,
            group_pg_id=group_pg_id,
            follow_type=follow_type,
            media_id=media_id,
            mongo_id=_clean(mongo_id),
            name=name,
            url=url,
            description=description,
            map_id=map_id,
        ).consume()
    driver.close()

    return NeoMediaSyncResult(
        ok=True,
        created_or_updated={parent_label: 1, group_label: 1, media_label: 1},
    )
