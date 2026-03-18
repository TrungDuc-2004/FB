from __future__ import annotations

"""Sync Neo4j graph (LIGHT NODES) FROM MongoDB + Postgre (auto ids).

Flow hiện tại:
MinIO -> MongoDB -> PostgreSQL -> Neo4j

Neo4j vẫn MERGE theo pg_id, nhưng bổ sung thêm các property phục vụ search:
- Topic:
  pg_id, name, topic_name, topic_number, topic_embedding, embed_model
- Lesson:
  pg_id, name, lesson_name, lesson_number, lesson_embedding, embed_model
- Chunk:
  pg_id, name, chunk_name, chunk_number, chunk_embedding, embed_model
- Keyword:
  pg_id, name, embedding, chunk_id

Giữ nguyên property `name` để không vỡ code/query cũ.
"""

import os
from dataclasses import dataclass
from typing import Any, Dict

from sqlalchemy import text

from .keyword_embedding import embed_keyword_cached, get_keyword_embedder
from .mongo_client import get_mongo_client
from .neo_client import neo4j_driver
from .postgre_client import get_engine
from .postgre_sync_from_mongo import (
    PgIds,
    _parse_topic_lesson_chunk_numbers_from_chunk_map,
    _parse_topic_lesson_numbers_from_lesson_map,
    _parse_topic_number_from_topic_map,
    _resolve_chain_from_maps,
)


def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _embed_model_name() -> str:
    model = (os.getenv("OPENAI_EMBEDDING_MODEL") or "").strip()
    if model:
        return model
    embedder = get_keyword_embedder()
    return _clean(getattr(embedder, "name", ""))


def _embed_name(name: str):
    s = _clean(name)
    if not s:
        return None
    try:
        return embed_keyword_cached(s)
    except Exception:
        return None


@dataclass
class NeoSyncResult:
    ok: bool
    created_or_updated: Dict[str, int]
    keyword_count: int = 0


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


def _merge_topic(
    tx,
    *,
    subject_pg_id: str,
    subject_name: str,
    pg_id: str,
    name: str,
    topic_number: int | None = None,
    topic_embedding=None,
    embed_model: str | None = None,
):
    tx.run(
        """
        MERGE (s:Subject {pg_id: $subject_pg_id})
        SET s.name = $subject_name
        REMOVE s.neo_id

        MERGE (t:Topic {pg_id: $pg_id})
        SET t.name = $name,
            t.topic_name = $name,
            t.topic_number = $topic_number,
            t.embed_model = CASE
                WHEN $embed_model IS NULL OR $embed_model = '' THEN t.embed_model
                ELSE $embed_model
            END,
            t.topic_embedding = CASE
                WHEN $topic_embedding IS NULL THEN t.topic_embedding
                ELSE $topic_embedding
            END
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
        topic_number=topic_number,
        topic_embedding=topic_embedding,
        embed_model=embed_model,
    )


def _merge_lesson(
    tx,
    *,
    topic_pg_id: str,
    topic_name: str,
    pg_id: str,
    name: str,
    lesson_number: int | None = None,
    lesson_embedding=None,
    embed_model: str | None = None,
):
    tx.run(
        """
        MERGE (t:Topic {pg_id: $topic_pg_id})
        SET t.name = $topic_name
        REMOVE t.neo_id

        MERGE (l:Lesson {pg_id: $pg_id})
        SET l.name = $name,
            l.lesson_name = $name,
            l.lesson_number = $lesson_number,
            l.embed_model = CASE
                WHEN $embed_model IS NULL OR $embed_model = '' THEN l.embed_model
                ELSE $embed_model
            END,
            l.lesson_embedding = CASE
                WHEN $lesson_embedding IS NULL THEN l.lesson_embedding
                ELSE $lesson_embedding
            END
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
        lesson_number=lesson_number,
        lesson_embedding=lesson_embedding,
        embed_model=embed_model,
    )


def _merge_chunk(
    tx,
    *,
    lesson_pg_id: str,
    lesson_name: str,
    pg_id: str,
    name: str,
    chunk_number: int | None = None,
    chunk_embedding=None,
    embed_model: str | None = None,
):
    tx.run(
        """
        MERGE (l:Lesson {pg_id: $lesson_pg_id})
        SET l.name = $lesson_name
        REMOVE l.neo_id

        MERGE (c:Chunk {pg_id: $pg_id})
        SET c.name = $name,
            c.chunk_name = $name,
            c.chunk_number = $chunk_number,
            c.embed_model = CASE
                WHEN $embed_model IS NULL OR $embed_model = '' THEN c.embed_model
                ELSE $embed_model
            END,
            c.chunk_embedding = CASE
                WHEN $chunk_embedding IS NULL THEN c.chunk_embedding
                ELSE $chunk_embedding
            END
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
        chunk_number=chunk_number,
        chunk_embedding=chunk_embedding,
        embed_model=embed_model,
    )


def _merge_keywords_for_owner(tx, *, owner_label: str, owner_pg_id: str, keywords: list[dict[str, object]]):
    if owner_label not in {"Subject", "Topic", "Lesson", "Chunk"}:
        raise ValueError(f"Unsupported owner label: {owner_label}")

    kw_ids = [k.get("pg_id", "") for k in (keywords or []) if k.get("pg_id")]

    tx.run(
        f"""
        MATCH (owner:{owner_label} {{pg_id: $owner_pg_id}})-[r:HAS_KEYWORD]->(k:Keyword)
        WHERE size($kw_ids) = 0 OR NOT k.pg_id IN $kw_ids
        DELETE r
        WITH collect(DISTINCT k) AS ks
        UNWIND ks AS k
        WITH DISTINCT k
        WHERE NOT ((:Subject)-[:HAS_KEYWORD]->(k))
          AND NOT ((:Topic)-[:HAS_KEYWORD]->(k))
          AND NOT ((:Lesson)-[:HAS_KEYWORD]->(k))
          AND NOT ((:Chunk)-[:HAS_KEYWORD]->(k))
        DETACH DELETE k
        """,
        owner_pg_id=owner_pg_id,
        kw_ids=kw_ids,
    )

    for kw in keywords or []:
        kw_pg_id = _clean(kw.get("pg_id"))
        kw_name = _clean(kw.get("name")) or kw_pg_id
        kw_emb = kw.get("embedding")
        if not kw_pg_id:
            continue

        tx.run(
            f"""
            MATCH (owner:{owner_label} {{pg_id: $owner_pg_id}})
            MERGE (k:Keyword {{pg_id: $kw_pg_id}})
            SET k.name = $kw_name
            SET k.embedding = CASE WHEN $kw_emb IS NULL THEN k.embedding ELSE $kw_emb END
            SET k.map_id = $owner_pg_id
            REMOVE k.neo_id
            MERGE (owner)-[:HAS_KEYWORD]->(k)
            """,
            owner_pg_id=owner_pg_id,
            kw_pg_id=kw_pg_id,
            kw_name=kw_name,
            kw_emb=kw_emb,
        )


def _fetch_keywords_from_postgre(*, map_id: str) -> list[dict[str, object]]:
    engine = get_engine()
    out: list[dict[str, object]] = []
    if not map_id:
        return out

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT keyword_id, keyword_name, keyword_embedding
                FROM keyword
                WHERE map_id = :map_id
                ORDER BY keyword_id
                """
            ),
            {"map_id": map_id},
        ).fetchall()

        for r in rows:
            kw_name = _clean(r[1]) or _clean(r[0])
            kw_embedding = r[2]
            if kw_embedding is None and kw_name:
                kw_embedding = embed_keyword_cached(kw_name)
            out.append(
                {
                    "pg_id": _clean(r[0]),
                    "name": kw_name,
                    "embedding": kw_embedding,
                }
            )
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
            kw_name = _clean(r[1]) or _clean(r[0])
            kw_embedding = r[2]
            if kw_embedding is None and kw_name:
                kw_embedding = embed_keyword_cached(kw_name)
            out.append(
                {
                    "pg_id": _clean(r[0]),
                    "name": kw_name,
                    "embedding": kw_embedding,
                }
            )
    return out


def sync_neo4j_from_maps_and_pg_ids(
    *,
    class_map: str,
    subject_map: str,
    topic_map: str = "",
    lesson_map: str = "",
    chunk_map: str = "",
    pg_ids: PgIds,
    actor: str = "system",
) -> NeoSyncResult:
    class_pg_id = _clean(getattr(pg_ids, "class_id", ""))
    subject_pg_id = _clean(getattr(pg_ids, "subject_id", ""))
    topic_pg_id = _clean(getattr(pg_ids, "topic_id", ""))
    lesson_pg_id = _clean(getattr(pg_ids, "lesson_id", ""))
    chunk_pg_id = _clean(getattr(pg_ids, "chunk_id", ""))

    if not class_pg_id:
        raise ValueError("Missing pg_ids.class_id for Neo4j sync")

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

    topic_map_for_number = topic_map or _clean((topic_doc or {}).get("topicID"))
    lesson_map_for_number = lesson_map or _clean((lesson_doc or {}).get("lessonID"))
    chunk_map_for_number = chunk_map or _clean((chunk_doc or {}).get("chunkID"))

    topic_number = None
    lesson_number = None
    chunk_number = None

    if topic_map_for_number:
        tnum = _parse_topic_number_from_topic_map(topic_map_for_number)
        topic_number = int(tnum) if tnum else None

    if lesson_map_for_number:
        _tnum, lnum = _parse_topic_lesson_numbers_from_lesson_map(lesson_map_for_number)
        lesson_number = int(lnum) if lnum else None

    if chunk_map_for_number:
        _tnum, _lnum, cnum = _parse_topic_lesson_chunk_numbers_from_chunk_map(chunk_map_for_number)
        chunk_number = int(cnum) if cnum else None

    embed_model = _embed_model_name()
    topic_embedding = _embed_name(topic_name) if topic_pg_id else None
    lesson_embedding = _embed_name(lesson_name) if lesson_pg_id else None
    chunk_embedding = _embed_name(chunk_name) if chunk_pg_id else None

    created_or_updated = {"Class": 0, "Subject": 0, "Topic": 0, "Lesson": 0, "Chunk": 0}
    keyword_count = 0

    driver = neo4j_driver()
    try:
        db_name = (os.getenv("NEO4J_DATABASE") or "").strip() or None

        with driver.session(database=db_name) as session:  # type: ignore[arg-type]
            session.execute_write(_merge_class, pg_id=class_pg_id, name=class_name)
            created_or_updated["Class"] = 1

            if subject_pg_id:
                session.execute_write(
                    _merge_subject,
                    class_pg_id=class_pg_id,
                    class_name=class_name,
                    pg_id=subject_pg_id,
                    name=subject_name or subject_pg_id,
                )
                created_or_updated["Subject"] = 1

            if topic_pg_id and subject_pg_id:
                session.execute_write(
                    _merge_topic,
                    subject_pg_id=subject_pg_id,
                    subject_name=subject_name or subject_pg_id,
                    pg_id=topic_pg_id,
                    name=topic_name or topic_pg_id,
                    topic_number=topic_number,
                    topic_embedding=topic_embedding,
                    embed_model=embed_model,
                )
                created_or_updated["Topic"] = 1

            if lesson_pg_id and topic_pg_id:
                session.execute_write(
                    _merge_lesson,
                    topic_pg_id=topic_pg_id,
                    topic_name=topic_name or topic_pg_id,
                    pg_id=lesson_pg_id,
                    name=lesson_name or lesson_pg_id,
                    lesson_number=lesson_number,
                    lesson_embedding=lesson_embedding,
                    embed_model=embed_model,
                )
                created_or_updated["Lesson"] = 1

            if chunk_pg_id and lesson_pg_id:
                session.execute_write(
                    _merge_chunk,
                    lesson_pg_id=lesson_pg_id,
                    lesson_name=lesson_name or lesson_pg_id,
                    pg_id=chunk_pg_id,
                    name=chunk_name or chunk_pg_id,
                    chunk_number=chunk_number,
                    chunk_embedding=chunk_embedding,
                    embed_model=embed_model,
                )
                created_or_updated["Chunk"] = 1

                keyword_count = 0

            keyword_targets = [
                ("Subject", subject_pg_id),
                ("Topic", topic_pg_id),
                ("Lesson", lesson_pg_id),
                ("Chunk", chunk_pg_id),
            ]
            for owner_label, owner_pg_id in keyword_targets:
                if not owner_pg_id:
                    continue
                kw_rows = _fetch_keywords_from_postgre(map_id=owner_pg_id)
                if not kw_rows:
                    continue
                session.execute_write(
                    _merge_keywords_for_owner,
                    owner_label=owner_label,
                    owner_pg_id=owner_pg_id,
                    keywords=kw_rows,
                )
                keyword_count += len(kw_rows)

    finally:
        try:
            driver.close()
        except Exception:
            pass

    if keyword_count:
        created_or_updated["Keyword"] = keyword_count

    return NeoSyncResult(ok=True, created_or_updated=created_or_updated, keyword_count=keyword_count)
