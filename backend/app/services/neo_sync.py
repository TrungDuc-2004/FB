from __future__ import annotations

"""Sync Neo4j graph (LIGHT NODES) FROM MongoDB + Postgre (auto ids).

Flow hiện tại của project:
MinIO -> MongoDB -> PostgreSQL -> Neo4j

Light node (theo yêu cầu mới):
- Neo4j tự tạo ID cho node (elementId). Backend KHÔNG dùng map-id (L10/TH10/...) làm ID Neo4j nữa.
- Node chỉ lưu 2 properties:
  - pg_id: ID bên PostgreSQL
  - name: tên hiển thị (lấy từ Mongo, fallback về pg_id)

Graph:
(:Class)-[:HAS_SUBJECT]->(:Subject)-[:HAS_TOPIC]->(:Topic)-[:HAS_LESSON]->(:Lesson)-[:HAS_CHUNK]->(:Chunk)

Keywords:
- Có thêm node (:Keyword) và quan hệ (:Chunk)-[:HAS_KEYWORD]->(:Keyword)
- Keyword là light node: chỉ lưu pg_id + name (Neo4j id vẫn auto)

NOTE:
- MERGE theo pg_id (idempotent).
- Không lưu url/status/mongo_id trong Neo4j.
"""

from dataclasses import dataclass
from typing import Any, Dict

from sqlalchemy import text

from .mongo_client import get_mongo_client
from .neo_client import neo4j_driver
from .postgre_client import get_engine
from .postgre_sync_from_mongo import PgIds, _resolve_chain_from_maps


def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


@dataclass
class NeoSyncResult:
    ok: bool
    created_or_updated: Dict[str, int]
    keyword_count: int = 0


# ===== Cypher writers (LIGHT) =====


def _merge_class(tx, *, pg_id: str, name: str):
    tx.run(
        """
        MERGE (c:Class {pg_id: $pg_id})
        SET c.name = $name
        REMOVE c.neo_id
        """,
        pg_id=pg_id,
        name=name,
    )


def _merge_subject(tx, *, class_pg_id: str, class_name: str, pg_id: str, name: str):
    tx.run(
        """
        MERGE (c:Class {pg_id: $class_pg_id})
        SET c.name = $class_name
        REMOVE c.neo_id
        MERGE (s:Subject {pg_id: $pg_id})
        SET s.name = $name
        REMOVE s.neo_id
        WITH c, s
        OPTIONAL MATCH (old:Class)-[r:HAS_SUBJECT]->(s)
        WHERE old.pg_id <> $class_pg_id
        DELETE r
        MERGE (c)-[:HAS_SUBJECT]->(s)
        """,
        class_pg_id=class_pg_id,
        class_name=class_name,
        pg_id=pg_id,
        name=name,
    )


def _merge_topic(tx, *, subject_pg_id: str, subject_name: str, pg_id: str, name: str):
    tx.run(
        """
        MERGE (s:Subject {pg_id: $subject_pg_id})
        SET s.name = $subject_name
        REMOVE s.neo_id
        MERGE (t:Topic {pg_id: $pg_id})
        SET t.name = $name
        REMOVE t.neo_id
        WITH s, t
        OPTIONAL MATCH (old:Subject)-[r:HAS_TOPIC]->(t)
        WHERE old.pg_id <> $subject_pg_id
        DELETE r
        MERGE (s)-[:HAS_TOPIC]->(t)
        """,
        subject_pg_id=subject_pg_id,
        subject_name=subject_name,
        pg_id=pg_id,
        name=name,
    )


def _merge_lesson(tx, *, topic_pg_id: str, topic_name: str, pg_id: str, name: str):
    tx.run(
        """
        MERGE (t:Topic {pg_id: $topic_pg_id})
        SET t.name = $topic_name
        REMOVE t.neo_id
        MERGE (l:Lesson {pg_id: $pg_id})
        SET l.name = $name
        REMOVE l.neo_id
        WITH t, l
        OPTIONAL MATCH (old:Topic)-[r:HAS_LESSON]->(l)
        WHERE old.pg_id <> $topic_pg_id
        DELETE r
        MERGE (t)-[:HAS_LESSON]->(l)
        """,
        topic_pg_id=topic_pg_id,
        topic_name=topic_name,
        pg_id=pg_id,
        name=name,
    )


def _merge_chunk(tx, *, lesson_pg_id: str, lesson_name: str, pg_id: str, name: str):
    tx.run(
        """
        MERGE (l:Lesson {pg_id: $lesson_pg_id})
        SET l.name = $lesson_name
        REMOVE l.neo_id
        MERGE (c:Chunk {pg_id: $pg_id})
        SET c.name = $name
        REMOVE c.neo_id
        WITH l, c
        OPTIONAL MATCH (old:Lesson)-[r:HAS_CHUNK]->(c)
        WHERE old.pg_id <> $lesson_pg_id
        DELETE r
        MERGE (l)-[:HAS_CHUNK]->(c)
        """,
        lesson_pg_id=lesson_pg_id,
        lesson_name=lesson_name,
        pg_id=pg_id,
        name=name,
    )


def _merge_keywords(tx, *, chunk_pg_id: str, keywords: list[dict[str, object]]):
    """Upsert keyword nodes + rels for a chunk.

    keywords: [{"pg_id": "...", "name": "...", "embedding": [...]|None}, ...]
    """
    kw_ids = [k.get("pg_id", "") for k in (keywords or []) if k.get("pg_id")]

    # remove stale relations (và xoá keyword mồ côi vừa bị stale)
    tx.run(
        """
        MATCH (ch:Chunk {pg_id: $chunk_pg_id})-[r:HAS_KEYWORD]->(k:Keyword)
        WHERE size($kw_ids) = 0 OR NOT k.pg_id IN $kw_ids
        DELETE r
        WITH collect(DISTINCT k) AS ks
        UNWIND ks AS k
        WITH DISTINCT k
        WHERE NOT ( (:Chunk)-[:HAS_KEYWORD]->(k) )
        DETACH DELETE k
        """,
        chunk_pg_id=chunk_pg_id,
        kw_ids=kw_ids,
    )

    # upsert + link
    for kw in keywords or []:
        kw_pg_id = _clean(kw.get("pg_id"))
        kw_name = _clean(kw.get("name")) or kw_pg_id
        kw_emb = kw.get("embedding")
        if not kw_pg_id:
            continue
        tx.run(
            """
            MATCH (ch:Chunk {pg_id: $chunk_pg_id})
            MERGE (k:Keyword {pg_id: $kw_pg_id})
            SET k.name = $kw_name
            SET k.embedding = CASE WHEN $kw_emb IS NULL THEN k.embedding ELSE $kw_emb END
            REMOVE k.neo_id
            MERGE (ch)-[:HAS_KEYWORD]->(k)
            """,
            chunk_pg_id=chunk_pg_id,
            kw_pg_id=kw_pg_id,
            kw_name=kw_name,
            kw_emb=kw_emb,
        )


def _fetch_keywords_from_postgre(*, chunk_pg_id: str) -> list[dict[str, object]]:
    """Lấy keyword từ Postgre để sync xuống Neo4j (đúng flow Mongo -> PG -> Neo).

    Trả về list: {pg_id, name, embedding}
    """
    engine = get_engine()
    out: list[dict[str, object]] = []
    if not chunk_pg_id:
        return out
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT keyword_id, keyword_name, keyword_embedding
                FROM keyword
                WHERE chunk_id = :chunk_id
                ORDER BY keyword_id
                """
            ),
            {"chunk_id": chunk_pg_id},
        ).fetchall()
        for r in rows:
            out.append({
                "pg_id": _clean(r[0]),
                "name": _clean(r[1]) or _clean(r[0]),
                "embedding": r[2],
            })
    return out


def sync_neo4j_from_maps_and_pg_ids(
    *,
    class_map: str,
    subject_map: str,
    topic_map: str = "",
    lesson_map: str = "",
    chunk_map: str = "",
    pg_ids: PgIds,
    actor: str = "system",  # giữ arg để tương thích, hiện không lưu trong Neo
) -> NeoSyncResult:
    """Upsert graph theo chain map IDs + pg_ids.

    - map IDs chỉ dùng để resolve tên từ Mongo (không đưa sang Neo4j).
    - Neo4j MERGE theo pg_id.
    """

    # pg ids (bắt buộc tối thiểu class + subject)
    class_pg_id = _clean(getattr(pg_ids, "class_id", ""))
    subject_pg_id = _clean(getattr(pg_ids, "subject_id", ""))
    topic_pg_id = _clean(getattr(pg_ids, "topic_id", ""))
    lesson_pg_id = _clean(getattr(pg_ids, "lesson_id", ""))
    chunk_pg_id = _clean(getattr(pg_ids, "chunk_id", ""))

    if not class_pg_id:
        raise ValueError("Missing pg_ids.class_id for Neo4j sync")

    # Resolve names from Mongo (fallback về pg_id)
    mg = get_mongo_client()
    db = mg["db"]

    class_map = _clean(class_map)
    subject_map = _clean(subject_map)
    topic_map = _clean(topic_map)
    lesson_map = _clean(lesson_map)
    chunk_map = _clean(chunk_map)

    class_doc, subject_doc, topic_doc, lesson_doc, chunk_doc = _resolve_chain_from_maps(
        db,
        class_map=class_map,
        subject_map=subject_map,
        topic_map=topic_map,
        lesson_map=lesson_map,
        chunk_map=chunk_map,
    )

    class_name = _clean((class_doc or {}).get("className")) or class_pg_id
    subject_name = _clean((subject_doc or {}).get("subjectName")) or subject_pg_id or ""
    topic_name = _clean((topic_doc or {}).get("topicName")) or topic_pg_id or ""
    lesson_name = _clean((lesson_doc or {}).get("lessonName")) or lesson_pg_id or ""
    chunk_name = _clean((chunk_doc or {}).get("chunkName")) or chunk_pg_id or ""

    created_or_updated = {"Class": 0, "Subject": 0, "Topic": 0, "Lesson": 0, "Chunk": 0}
    keyword_count = 0

    driver = neo4j_driver()
    try:
        import os

        db_name = (os.getenv("NEO4J_DATABASE") or "").strip() or None

        with driver.session(database=db_name) as session:  # type: ignore[arg-type]
            # 1) Class
            session.execute_write(_merge_class, pg_id=class_pg_id, name=class_name)
            created_or_updated["Class"] = 1

            # 2) Subject
            if subject_pg_id:
                session.execute_write(
                    _merge_subject,
                    class_pg_id=class_pg_id,
                    class_name=class_name,
                    pg_id=subject_pg_id,
                    name=subject_name or subject_pg_id,
                )
                created_or_updated["Subject"] = 1

            # 3) Topic
            if topic_pg_id and subject_pg_id:
                session.execute_write(
                    _merge_topic,
                    subject_pg_id=subject_pg_id,
                    subject_name=subject_name or subject_pg_id,
                    pg_id=topic_pg_id,
                    name=topic_name or topic_pg_id,
                )
                created_or_updated["Topic"] = 1

            # 4) Lesson
            if lesson_pg_id and topic_pg_id:
                session.execute_write(
                    _merge_lesson,
                    topic_pg_id=topic_pg_id,
                    topic_name=topic_name or topic_pg_id,
                    pg_id=lesson_pg_id,
                    name=lesson_name or lesson_pg_id,
                )
                created_or_updated["Lesson"] = 1

            # 5) Chunk
            if chunk_pg_id and lesson_pg_id:
                session.execute_write(
                    _merge_chunk,
                    lesson_pg_id=lesson_pg_id,
                    lesson_name=lesson_name or lesson_pg_id,
                    pg_id=chunk_pg_id,
                    name=chunk_name or chunk_pg_id,
                )
                created_or_updated["Chunk"] = 1

                # 6) Keywords: lấy từ Postgre (đúng flow Mongo -> PG -> Neo)
                kw_rows = _fetch_keywords_from_postgre(chunk_pg_id=chunk_pg_id)
                if kw_rows:
                    session.execute_write(_merge_keywords, chunk_pg_id=chunk_pg_id, keywords=kw_rows)
                    keyword_count = len(kw_rows)

    finally:
        try:
            driver.close()
        except Exception:
            pass

    if keyword_count:
        created_or_updated["Keyword"] = keyword_count
    return NeoSyncResult(ok=True, created_or_updated=created_or_updated, keyword_count=keyword_count)
