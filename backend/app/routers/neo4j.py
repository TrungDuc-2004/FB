from __future__ import annotations

from typing import Annotated, Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from neo4j import Session as NeoSession
from pydantic import BaseModel, Field

from ..services.neo_client import get_neo4j_session
from ..services.neo_sync import sync_neo4j_from_maps_and_pg_ids
from ..services.postgre_sync_from_mongo import sync_postgre_from_mongo_auto_ids

router = APIRouter(prefix="/admin/neo", tags=["Neo4j (view-only)"])


class NeoSyncBody(BaseModel):
    classMap: str = Field("", description="VD: L10")
    subjectMap: str = Field("", description="VD: TH10")
    topicMap: str = Field("", description="VD: TH10_CD1")
    lessonMap: str = Field("", description="VD: TH10_CD1_B1")
    chunkMap: str = Field("", description="VD: TH10_CD1_B1_C1")


def _get_actor(request: Request | None) -> str:
    if request is None:
        return "system"
    return request.headers.get("x-user") or request.headers.get("x-actor") or "system"


@router.post("/sync", summary="Sync chain Mongo -> Postgre(auto ids) -> Neo4j (light nodes)")
def sync_chain_to_neo(body: NeoSyncBody, request: Request):
    """Dùng khi Neo4j bị lệch dữ liệu và bạn muốn re-sync theo map ID.

    - Postgre sync là idempotent (upsert).
    - Neo4j sync cũng idempotent.
    - Neo4j theo schema light node: neo_id + pg_id + name.
    """
    actor = _get_actor(request)

    pg_ids = sync_postgre_from_mongo_auto_ids(
        class_map=body.classMap,
        subject_map=body.subjectMap,
        topic_map=body.topicMap,
        lesson_map=body.lessonMap,
        chunk_map=body.chunkMap,
    )

    neo_res = sync_neo4j_from_maps_and_pg_ids(
        class_map=body.classMap,
        subject_map=body.subjectMap,
        topic_map=body.topicMap,
        lesson_map=body.lessonMap,
        chunk_map=body.chunkMap,
        pg_ids=pg_ids,
        actor=actor,
    )

    return {
        "postgre": {
            "classId": pg_ids.class_id,
            "subjectId": pg_ids.subject_id,
            "topicId": pg_ids.topic_id,
            "lessonId": pg_ids.lesson_id,
            "chunkId": pg_ids.chunk_id,
            "keywordIds": pg_ids.keyword_ids,
        },
        "neo4j": {
            "synced": bool(getattr(neo_res, "ok", True)),
            "createdOrUpdated": getattr(neo_res, "created_or_updated", {}),
            "keywordCount": getattr(neo_res, "keyword_count", 0),
        },
    }


# ===== VIEW-ONLY API for FE =====

ALLOWED_LABELS = ["Class", "Subject", "Topic", "Lesson", "Chunk", "Keyword"]


def _pick_label(labels: List[str]) -> str:
    for lb in ALLOWED_LABELS:
        if lb in labels:
            return lb
    return labels[0] if labels else ""


def _relation_for_node(session: NeoSession, label: str, node_id: str) -> str:
    if label == "Class":
        cypher = """
        MATCH (c:Class)
        WHERE elementId(c) = $id
        OPTIONAL MATCH (c)-[:HAS_SUBJECT]->(s:Subject)
        RETURN c.name AS class_name, c.pg_id AS class_pg_id, count(s) AS child_count
        """
        r = session.run(cypher, id=node_id).single()
        if not r:
            return "PATH: (Class not found)"
        return f"PATH: Class: {r.get('class_name') or ''} [pg_id={r.get('class_pg_id') or ''}] | children Subjects: {int(r.get('child_count') or 0)}"

    if label == "Subject":
        cypher = """
        MATCH (c:Class)-[:HAS_SUBJECT]->(s:Subject)
        WHERE elementId(s) = $id
        OPTIONAL MATCH (s)-[:HAS_TOPIC]->(t:Topic)
        RETURN c.name AS class_name, c.pg_id AS class_pg_id,
               s.name AS subject_name, s.pg_id AS subject_pg_id,
               count(t) AS child_count
        """
        r = session.run(cypher, id=node_id).single()
        if not r:
            return "PATH: (missing Class -> Subject link)"
        return (
            f"PATH: Class: {r.get('class_name') or ''} [pg_id={r.get('class_pg_id') or ''}]"
            f" > Subject: {r.get('subject_name') or ''} [pg_id={r.get('subject_pg_id') or ''}]"
            f" | children Topics: {int(r.get('child_count') or 0)}"
        )

    if label == "Topic":
        cypher = """
        MATCH (c:Class)-[:HAS_SUBJECT]->(s:Subject)-[:HAS_TOPIC]->(t:Topic)
        WHERE elementId(t) = $id
        OPTIONAL MATCH (t)-[:HAS_LESSON]->(l:Lesson)
        RETURN c.name AS class_name, c.pg_id AS class_pg_id,
               s.name AS subject_name, s.pg_id AS subject_pg_id,
               t.name AS topic_name, t.pg_id AS topic_pg_id,
               count(l) AS child_count
        """
        r = session.run(cypher, id=node_id).single()
        if not r:
            return "PATH: (missing Subject -> Topic link)"
        return (
            f"PATH: Class: {r.get('class_name') or ''} [pg_id={r.get('class_pg_id') or ''}]"
            f" > Subject: {r.get('subject_name') or ''} [pg_id={r.get('subject_pg_id') or ''}]"
            f" > Topic: {r.get('topic_name') or ''} [pg_id={r.get('topic_pg_id') or ''}]"
            f" | children Lessons: {int(r.get('child_count') or 0)}"
        )

    if label == "Lesson":
        cypher = """
        MATCH (c:Class)-[:HAS_SUBJECT]->(s:Subject)-[:HAS_TOPIC]->(t:Topic)-[:HAS_LESSON]->(l:Lesson)
        WHERE elementId(l) = $id
        OPTIONAL MATCH (l)-[:HAS_CHUNK]->(ch:Chunk)
        RETURN c.name AS class_name, c.pg_id AS class_pg_id,
               s.name AS subject_name, s.pg_id AS subject_pg_id,
               t.name AS topic_name, t.pg_id AS topic_pg_id,
               l.name AS lesson_name, l.pg_id AS lesson_pg_id,
               count(ch) AS child_count
        """
        r = session.run(cypher, id=node_id).single()
        if not r:
            return "PATH: (missing Topic -> Lesson link)"
        return (
            f"PATH: Class: {r.get('class_name') or ''} [pg_id={r.get('class_pg_id') or ''}]"
            f" > Subject: {r.get('subject_name') or ''} [pg_id={r.get('subject_pg_id') or ''}]"
            f" > Topic: {r.get('topic_name') or ''} [pg_id={r.get('topic_pg_id') or ''}]"
            f" > Lesson: {r.get('lesson_name') or ''} [pg_id={r.get('lesson_pg_id') or ''}]"
            f" | children Chunks: {int(r.get('child_count') or 0)}"
        )

    if label == "Chunk":
        cypher = """
        MATCH (c:Class)-[:HAS_SUBJECT]->(s:Subject)-[:HAS_TOPIC]->(t:Topic)-[:HAS_LESSON]->(l:Lesson)-[:HAS_CHUNK]->(ch:Chunk)
        WHERE elementId(ch) = $id
        OPTIONAL MATCH (ch)-[:HAS_KEYWORD]->(k:Keyword)
        RETURN c.name AS class_name, c.pg_id AS class_pg_id,
               s.name AS subject_name, s.pg_id AS subject_pg_id,
               t.name AS topic_name, t.pg_id AS topic_pg_id,
               l.name AS lesson_name, l.pg_id AS lesson_pg_id,
               ch.name AS chunk_name, ch.pg_id AS chunk_pg_id,
               count(DISTINCT k) AS kw_count
        """
        r = session.run(cypher, id=node_id).single()
        if not r:
            return "PATH: (missing Lesson -> Chunk link)"
        return (
            f"PATH: Class: {r.get('class_name') or ''} [pg_id={r.get('class_pg_id') or ''}]"
            f" > Subject: {r.get('subject_name') or ''} [pg_id={r.get('subject_pg_id') or ''}]"
            f" > Topic: {r.get('topic_name') or ''} [pg_id={r.get('topic_pg_id') or ''}]"
            f" > Lesson: {r.get('lesson_name') or ''} [pg_id={r.get('lesson_pg_id') or ''}]"
            f" > Chunk: {r.get('chunk_name') or ''} [pg_id={r.get('chunk_pg_id') or ''}]"
            f" | keywords: {int(r.get('kw_count') or 0)}"
        )

    if label == "Keyword":
        cypher = """
        MATCH (c:Class)-[:HAS_SUBJECT]->(s:Subject)-[:HAS_TOPIC]->(t:Topic)-[:HAS_LESSON]->(l:Lesson)-[:HAS_CHUNK]->(ch:Chunk)-[:HAS_KEYWORD]->(k:Keyword)
        WHERE elementId(k) = $id
        RETURN c.name AS class_name, c.pg_id AS class_pg_id,
               s.name AS subject_name, s.pg_id AS subject_pg_id,
               t.name AS topic_name, t.pg_id AS topic_pg_id,
               l.name AS lesson_name, l.pg_id AS lesson_pg_id,
               ch.name AS chunk_name, ch.pg_id AS chunk_pg_id,
               k.name AS keyword_name, k.pg_id AS keyword_pg_id
        """
        r = session.run(cypher, id=node_id).single()
        if not r:
            return "PATH: (missing Chunk -> Keyword link)"
        return (
            f"PATH: Class: {r.get('class_name') or ''} [pg_id={r.get('class_pg_id') or ''}]"
            f" > Subject: {r.get('subject_name') or ''} [pg_id={r.get('subject_pg_id') or ''}]"
            f" > Topic: {r.get('topic_name') or ''} [pg_id={r.get('topic_pg_id') or ''}]"
            f" > Lesson: {r.get('lesson_name') or ''} [pg_id={r.get('lesson_pg_id') or ''}]"
            f" > Chunk: {r.get('chunk_name') or ''} [pg_id={r.get('chunk_pg_id') or ''}]"
            f" > Keyword: {r.get('keyword_name') or ''} [pg_id={r.get('keyword_pg_id') or ''}]"
        )

    return ""


@router.get("/labels", summary="List labels + count (view-only)")
def list_labels(session: Annotated[NeoSession, Depends(get_neo4j_session)]):
    out = []
    for lb in ALLOWED_LABELS:
        cypher = f"MATCH (n:{lb}) RETURN count(n) AS c"
        c = session.run(cypher).single()["c"]
        out.append({"id": lb, "name": lb, "count": int(c)})
    return {"labels": out}


@router.get("/nodes", summary="List nodes by label (view-only)")
def list_nodes(
    label: str = Query(...),
    limit: int = Query(200, ge=1, le=2000),
    skip: int = Query(0, ge=0),
    session: Annotated[NeoSession, Depends(get_neo4j_session)] = None,
):
    if label not in ALLOWED_LABELS:
        raise HTTPException(status_code=404, detail=f"Label '{label}' not allowed")

    cypher = f"""
    MATCH (n:{label})
    RETURN elementId(n) AS id, properties(n) AS p
    ORDER BY coalesce(toString(n.name), "") ASC
    SKIP $skip LIMIT $limit
    """
    rs = session.run(cypher, skip=skip, limit=limit)

    nodes = []
    for r in rs:
        p = r["p"] or {}
        nodes.append(
            {
                "id": str(r["id"]),
                "neoId": str(r["id"]),
                "postgreId": str(p.get("pg_id") or ""),
                "name": str(p.get("name") or ""),
            }
        )

    total = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]
    return {"label": label, "total": int(total), "nodes": nodes}


@router.get("/nodes/{node_id}", summary="Get node detail (view-only, includes relation)")
def get_node_detail(
    node_id: str = Path(...),
    session: Annotated[NeoSession, Depends(get_neo4j_session)] = None,
):
    cypher = """
    MATCH (n)
    WHERE elementId(n) = $id
    RETURN labels(n) AS lbs, properties(n) AS p, elementId(n) AS id
    """
    r = session.run(cypher, id=node_id).single()
    if not r:
        raise HTTPException(status_code=404, detail="Node not found")

    labels = r["lbs"] or []
    props = r["p"] or {}
    label = _pick_label(labels)

    node = {
        "id": str(r["id"]),
        "label": label,
        "entity_id_key": "elementId",
        "entity_id": str(r["id"]),
        "entity_name_key": "name",
        "entity_name": str(props.get("name") or ""),
        "postgre_id": str(props.get("pg_id") or ""),
        "relation": _relation_for_node(session, label, node_id),
    }
    return {"node": node}
