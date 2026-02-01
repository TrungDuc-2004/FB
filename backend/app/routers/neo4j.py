# app/routers/admin_neo.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Path
from neo4j import Session as NeoSession
from typing import Any, Dict, List, Optional, Annotated

from ..services.neo_client import get_neo4j_session

router = APIRouter(prefix="/admin/neo", tags=["Neo4j (view-only)"])

ALLOWED_LABELS = ["Thing", "Class", "Subject", "Topic", "Lesson", "Chunk", "Keyword"]

LABEL_PRIORITY = ["Keyword", "Chunk", "Lesson", "Topic", "Subject", "Class", "Thing"]


def _pick_label(labels: List[str]) -> str:
    for lb in LABEL_PRIORITY:
        if lb in labels:
            return lb
    return labels[0] if labels else ""


def _coalesce(props: Dict[str, Any], keys: List[str], default: str = "") -> str:
    for k in keys:
        v = props.get(k)
        if v is not None and str(v).strip() != "":
            return str(v)
    return default


def _postgre_id(label: str, props: Dict[str, Any]) -> str:
    # ưu tiên id đúng theo từng label, fallback postgre_id
    if label == "Class":
        return _coalesce(props, ["class_id", "postgre_id"])
    if label == "Subject":
        return _coalesce(props, ["subject_id", "postgre_id"])
    if label == "Topic":
        return _coalesce(props, ["topic_id", "postgre_id"])
    if label == "Lesson":
        return _coalesce(props, ["lesson_id", "postgre_id"])
    if label == "Chunk":
        return _coalesce(props, ["chunk_id", "postgre_id"])
    if label == "Keyword":
        # keyword composite key: chunk_id::keyword_name => keyword_key
        return _coalesce(props, ["keyword_key", "postgre_id"])
    return _coalesce(props, ["postgre_id"], "thing")


def _display_name(label: str, props: Dict[str, Any]) -> str:
    if label == "Class":
        return str(props.get("class_name") or "")
    if label == "Subject":
        return str(props.get("subject_name") or "")
    if label == "Topic":
        return str(props.get("topic_name") or "")
    if label == "Lesson":
        return str(props.get("lesson_name") or "")
    if label == "Chunk":
        return str(props.get("chunk_name") or "")
    if label == "Keyword":
        return str(props.get("keyword_name") or "")
    if label == "Thing":
        return str(props.get("name") or "Thing")
    return ""



def _entity_id_key(label: str) -> str:
    return {
        "Class": "class_id",
        "Subject": "subject_id",
        "Topic": "topic_id",
        "Lesson": "lesson_id",
        "Chunk": "chunk_id",
        "Keyword": "keyword_key",  # keyword composite: chunk_id::keyword_name
        "Thing": "thing_id",
    }.get(label, "id")


def _entity_name_key(label: str) -> str:
    return {
        "Class": "class_name",
        "Subject": "subject_name",
        "Topic": "topic_name",
        "Lesson": "lesson_name",
        "Chunk": "chunk_name",
        "Keyword": "keyword_name",
        "Thing": "name",
    }.get(label, "name")


def _entity_id_value(label: str, props: Dict[str, Any]) -> str:
    # Ưu tiên field "đúng label" trước, fallback postgre_id
    key = _entity_id_key(label)
    if key in props and str(props.get(key) or "").strip() != "":
        return str(props.get(key))
    # fallback chung
    return _coalesce(props, ["postgre_id", "keyword_key"], "")


def _entity_name_value(label: str, props: Dict[str, Any]) -> str:
    key = _entity_name_key(label)
    return str(props.get(key) or "")


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

    # List giữ nguyên: chỉ id/postgreId/name (+updatedAt nếu có)
    # NOTE: Neo4j comment dùng //, không dùng --.
    cypher = f"""
    MATCH (n:{label})
    RETURN elementId(n) AS id, properties(n) AS p
    ORDER BY coalesce(toString(n.updated_at), "") DESC
    SKIP $skip LIMIT $limit
    """
    rs = session.run(cypher, skip=skip, limit=limit)

    nodes = []
    for r in rs:
        p = r["p"] or {}
        nodes.append({
            "id": str(r["id"]),
            "postgreId": _postgre_id(label, p),
            "name": _display_name(label, p),
            "updatedAt": str(p.get("updated_at") or ""),
            # list không trả relation để UI giữ nguyên
        })

    total = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]
    return {"label": label, "total": int(total), "nodes": nodes}


def _relation_for_node(session: NeoSession, label: str, node_id: str) -> str:
    # Trả về 1 string "PATH: Thing > Class: ... > Subject: ... > ..."
    # (chỉ dùng cho detail)
    if label == "Class":
        cypher = """
        MATCH (t:Thing {id:"thing"})-[:HAS_CLASS]->(c:Class)
        WHERE elementId(c) = $id
        OPTIONAL MATCH (c)-[:HAS_SUBJECT]->(s:Subject)
        RETURN c.class_name AS class_name, count(s) AS child_count
        """
        r = session.run(cypher, id=node_id).single()
        if not r:
            return "PATH: (missing Thing -> Class link)"
        cn = r.get("class_name") or ""
        cnt = int(r.get("child_count") or 0)
        return f"PATH: Thing > Class: {cn} | children Subjects: {cnt}"

    if label == "Subject":
        cypher = """
        MATCH (c:Class)-[:HAS_SUBJECT]->(s:Subject)
        WHERE elementId(s) = $id
        OPTIONAL MATCH (s)-[:HAS_TOPIC]->(t:Topic)
        RETURN c.class_name AS class_name, s.subject_name AS subject_name, count(t) AS child_count
        """
        r = session.run(cypher, id=node_id).single()
        if not r:
            return "PATH: (missing Class -> Subject link)"
        return (
            f"PATH: Thing > Class: {r.get('class_name') or ''} > Subject: {r.get('subject_name') or ''}"
            f" | children Topics: {int(r.get('child_count') or 0)}"
        )

    if label == "Topic":
        cypher = """
        MATCH (c:Class)-[:HAS_SUBJECT]->(s:Subject)-[:HAS_TOPIC]->(t:Topic)
        WHERE elementId(t) = $id
        OPTIONAL MATCH (t)-[:HAS_LESSON]->(l:Lesson)
        RETURN c.class_name AS class_name, s.subject_name AS subject_name, t.topic_name AS topic_name,
               count(l) AS child_count
        """
        r = session.run(cypher, id=node_id).single()
        if not r:
            return "PATH: (missing Subject -> Topic link)"
        return (
            f"PATH: Thing > Class: {r.get('class_name') or ''} > Subject: {r.get('subject_name') or ''}"
            f" > Topic: {r.get('topic_name') or ''}"
            f" | children Lessons: {int(r.get('child_count') or 0)}"
        )

    if label == "Lesson":
        cypher = """
        MATCH (c:Class)-[:HAS_SUBJECT]->(s:Subject)-[:HAS_TOPIC]->(t:Topic)-[:HAS_LESSON]->(l:Lesson)
        WHERE elementId(l) = $id
        OPTIONAL MATCH (l)-[:HAS_CHUNK]->(ch:Chunk)
        RETURN c.class_name AS class_name, s.subject_name AS subject_name, t.topic_name AS topic_name,
               l.lesson_name AS lesson_name, count(ch) AS child_count
        """
        r = session.run(cypher, id=node_id).single()
        if not r:
            return "PATH: (missing Topic -> Lesson link)"
        return (
            f"PATH: Thing > Class: {r.get('class_name') or ''} > Subject: {r.get('subject_name') or ''}"
            f" > Topic: {r.get('topic_name') or ''} > Lesson: {r.get('lesson_name') or ''}"
            f" | children Chunks: {int(r.get('child_count') or 0)}"
        )

    if label == "Chunk":
        cypher = """
        MATCH (c:Class)-[:HAS_SUBJECT]->(s:Subject)-[:HAS_TOPIC]->(t:Topic)-[:HAS_LESSON]->(l:Lesson)-[:HAS_CHUNK]->(ch:Chunk)
        WHERE elementId(ch) = $id
        OPTIONAL MATCH (ch)-[:HAS_KEYWORD]->(k:Keyword)
        RETURN c.class_name AS class_name, s.subject_name AS subject_name, t.topic_name AS topic_name,
               l.lesson_name AS lesson_name, ch.chunk_name AS chunk_name, count(k) AS child_count
        """
        r = session.run(cypher, id=node_id).single()
        if not r:
            return "PATH: (missing Lesson -> Chunk link)"
        return (
            f"PATH: Thing > Class: {r.get('class_name') or ''} > Subject: {r.get('subject_name') or ''}"
            f" > Topic: {r.get('topic_name') or ''} > Lesson: {r.get('lesson_name') or ''}"
            f" > Chunk: {r.get('chunk_name') or ''}"
            f" | children Keywords: {int(r.get('child_count') or 0)}"
        )

    if label == "Keyword":
        cypher = """
        MATCH (c:Class)-[:HAS_SUBJECT]->(s:Subject)-[:HAS_TOPIC]->(t:Topic)-[:HAS_LESSON]->(l:Lesson)-[:HAS_CHUNK]->(ch:Chunk)-[:HAS_KEYWORD]->(k:Keyword)
        WHERE elementId(k) = $id
        RETURN c.class_name AS class_name, s.subject_name AS subject_name, t.topic_name AS topic_name,
               l.lesson_name AS lesson_name, ch.chunk_name AS chunk_name, k.keyword_name AS keyword_name
        """
        r = session.run(cypher, id=node_id).single()
        if not r:
            return "PATH: (missing Chunk -> Keyword link)"
        return (
            f"PATH: Thing > Class: {r.get('class_name') or ''} > Subject: {r.get('subject_name') or ''}"
            f" > Topic: {r.get('topic_name') or ''} > Lesson: {r.get('lesson_name') or ''}"
            f" > Chunk: {r.get('chunk_name') or ''} > Keyword: {r.get('keyword_name') or ''}"
        )

    return ""

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
        "id": str(r["id"]),  # elementId để FE dùng mở detail
        "label": label,
        "entity_id_key": _entity_id_key(label),
        "entity_id": _entity_id_value(label, props),
        "entity_name_key": _entity_name_key(label),
        "entity_name": _entity_name_value(label, props),
        "relation": _relation_for_node(session, label, node_id),
    }
    return {"node": node}  # ✅ đúng FE: data.node