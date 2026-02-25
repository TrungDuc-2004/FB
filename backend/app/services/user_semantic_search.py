from __future__ import annotations

import math
import re
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from bson import ObjectId

from ..models.model_postgre import Chunk, Class, Keyword, Lesson, Subject, Topic
from .keyword_embedding import embed_keyword_cached

_TOKEN_RE = re.compile(r"[0-9A-Za-zÀ-ỹ]+", flags=re.UNICODE)

# bump this when you replace the file so you can confirm the running code
_SERVICE_VERSION = "search_join_pg_chunk_mongo_id_v1"


def _extract_keywords(q: str) -> List[str]:
    s = (q or "").strip()
    if not s:
        return []

    low = s.lower()
    tokens = _TOKEN_RE.findall(low)
    kws: List[str] = []

    # keep full query
    compact = re.sub(r"\s+", " ", low).strip()
    if len(compact) >= 3:
        kws.append(compact)

    for t in tokens:
        if len(t) < 2:
            continue
        if t not in kws:
            kws.append(t)

    return kws[:12]


def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b:
        return 0.0
    n = min(len(a), len(b))
    dot = 0.0
    na = 0.0
    nb = 0.0
    for i in range(n):
        x = float(a[i])
        y = float(b[i])
        dot += x * y
        na += x * x
        nb += y * y
    den = math.sqrt(na) * math.sqrt(nb)
    return float(dot / den) if den > 0 else 0.0


def _type_priority(t: str) -> int:
    return {"chunk": 0, "lesson": 1, "topic": 2, "subject": 3}.get(t, 9)


def _valid_object_id_hex(s: str) -> bool:
    if not s or len(s) != 24:
        return False
    try:
        int(s, 16)
        return True
    except Exception:
        return False


def _status_visible(doc: dict) -> bool:
    # bạn dùng activity/hidden, đôi khi active
    st = (doc or {}).get("status")
    return st not in {"hidden", "HIDDEN"}


def _candidate_chunk_ids_from_filters_pg(
    *,
    pg: Session,
    classID: str,
    subjectID: str,
    topicID: str,
    lessonID: str,
) -> Optional[List[str]]:
    """Return list of chunk_ids to restrict scan, or None for no restriction."""
    try:
        if lessonID:
            rows = list(pg.execute(select(Chunk.chunk_id).where(Chunk.lesson_id == lessonID)).all())
            return [r[0] for r in rows]

        if topicID:
            stmt = (
                select(Chunk.chunk_id)
                .join(Lesson, Lesson.lesson_id == Chunk.lesson_id)
                .where(Lesson.topic_id == topicID)
            )
            rows = list(pg.execute(stmt).all())
            return [r[0] for r in rows]

        if subjectID:
            stmt = (
                select(Chunk.chunk_id)
                .join(Lesson, Lesson.lesson_id == Chunk.lesson_id)
                .join(Topic, Topic.topic_id == Lesson.topic_id)
                .where(Topic.subject_id == subjectID)
            )
            rows = list(pg.execute(stmt).all())
            return [r[0] for r in rows]

        if classID:
            stmt = (
                select(Chunk.chunk_id)
                .join(Lesson, Lesson.lesson_id == Chunk.lesson_id)
                .join(Topic, Topic.topic_id == Lesson.topic_id)
                .join(Subject, Subject.subject_id == Topic.subject_id)
                .where(Subject.class_id == classID)
            )
            rows = list(pg.execute(stmt).all())
            return [r[0] for r in rows]

        return None
    except Exception:
        return None


def semantic_search(
    *,
    q: str,
    category: str,
    classID: str,
    subjectID: str,
    topicID: str,
    lessonID: str,
    limit: int,
    offset: int,
    username: str,
    pg: Session,
    neo,
    mongo_db,
    debug: bool = False,
) -> dict:
    """Semantic search: query -> keyword embeddings -> PG similarity -> join Mongo chunks by **PG chunk.mongo_id**.

    - Không filter Mongo theo category.
    - Multi-level output (flat list): chunk -> lesson -> topic -> subject.
    """

    query = (q or "").strip()
    if not query:
        return {"total": 0, "items": []}

    dbg: Dict[str, object] = {
        "service_version": _SERVICE_VERSION,
        "category": category,
    }

    # 1) Embed query
    kws = _extract_keywords(query)
    q_embs = [embed_keyword_cached(k) for k in kws]

    # 2) Candidate restriction by filters (PG graph)
    cand_chunks = _candidate_chunk_ids_from_filters_pg(
        pg=pg, classID=classID, subjectID=subjectID, topicID=topicID, lessonID=lessonID
    )

    # 3) Load PG keywords with embeddings
    try:
        stmt = select(Keyword.keyword_embedding, Keyword.chunk_id).where(Keyword.keyword_embedding.isnot(None))
        if cand_chunks is not None:
            if len(cand_chunks) == 0:
                res = {"total": 0, "items": []}
                if debug:
                    res["debug"] = {**dbg, "pg_rows_with_embedding": 0, "ranked_chunks": 0}
                return res
            stmt = stmt.where(Keyword.chunk_id.in_(cand_chunks))
        rows = list(pg.execute(stmt).all())
    except SQLAlchemyError:
        rows = []

    dbg["pg_rows_with_embedding"] = len(rows)

    # 4) Score per chunk
    chunk_best: Dict[str, float] = {}
    for emb, chunk_id in rows:
        if not chunk_id:
            continue
        if not emb:
            continue
        best = 0.0
        for qe in q_embs:
            best = max(best, _cosine(list(emb), qe))
        if best <= 0:
            continue
        prev = chunk_best.get(chunk_id, 0.0)
        if best > prev:
            chunk_best[chunk_id] = best

    ranked: List[Tuple[str, float]] = sorted(chunk_best.items(), key=lambda x: x[1], reverse=True)
    dbg["ranked_chunks"] = len(ranked)

    if not ranked:
        res = {"total": 0, "items": []}
        if debug:
            res["debug"] = dbg
        return res

    # 5) Multi-level needs hierarchy; fetch chunk + joins from PG
    ranked_chunk_ids = [cid for cid, _ in ranked]

    # only fetch enough for pagination + roll-up
    # we still need full ranked list for "total" at all levels; but for speed, we cap roll-up to top N
    # you can increase cap if needed.
    cap_for_rollup = max(offset + limit, 50)
    top_chunk_ids = ranked_chunk_ids[:cap_for_rollup]

    try:
        stmt = (
            select(
                Chunk.chunk_id,
                Chunk.chunk_name,
                Chunk.chunk_type,
                Chunk.mongo_id,
                Lesson.lesson_id,
                Lesson.lesson_name,
                Lesson.mongo_id,
                Topic.topic_id,
                Topic.topic_name,
                Topic.mongo_id,
                Subject.subject_id,
                Subject.subject_name,
                Subject.mongo_id,
                Class.class_id,
                Class.class_name,
                Class.mongo_id,
            )
            .join(Lesson, Lesson.lesson_id == Chunk.lesson_id)
            .join(Topic, Topic.topic_id == Lesson.topic_id)
            .join(Subject, Subject.subject_id == Topic.subject_id)
            .join(Class, Class.class_id == Subject.class_id)
            .where(Chunk.chunk_id.in_(top_chunk_ids))
        )
        pg_rows = list(pg.execute(stmt).all())
    except SQLAlchemyError:
        pg_rows = []

    dbg["pg_chunk_rows"] = len(pg_rows)

    # Map chunk_id -> hierarchy/meta
    pg_map: Dict[str, dict] = {}
    chunk_mongo_hex: List[str] = []
    for r in pg_rows:
        (
            chunk_id,
            chunk_name,
            chunk_type,
            chunk_mongo_id,
            lesson_id_v,
            lesson_name,
            lesson_mongo_id,
            topic_id_v,
            topic_name,
            topic_mongo_id,
            subject_id_v,
            subject_name,
            subject_mongo_id,
            class_id_v,
            class_name,
            class_mongo_id,
        ) = r

        pg_map[chunk_id] = {
            "chunkID": chunk_id,
            "chunkName": chunk_name,
            "chunkType": chunk_type,
            "chunkMongoId": chunk_mongo_id,
            "lesson": {"lessonID": lesson_id_v, "lessonName": lesson_name, "mongoId": lesson_mongo_id},
            "topic": {"topicID": topic_id_v, "topicName": topic_name, "mongoId": topic_mongo_id},
            "subject": {"subjectID": subject_id_v, "subjectName": subject_name, "mongoId": subject_mongo_id},
            "class": {"classID": class_id_v, "className": class_name, "mongoId": class_mongo_id},
        }
        if _valid_object_id_hex(chunk_mongo_id or ""):
            chunk_mongo_hex.append(chunk_mongo_id)

    dbg["chunk_mongo_ids_found"] = len(chunk_mongo_hex)

    # 6) Load chunk docs from Mongo by _id using **chunk.mongo_id**
    mongo_chunks_by_oid: Dict[str, dict] = {}
    mongo_chunks_raw = 0
    mongo_keys_sample: List[str] = []
    if chunk_mongo_hex:
        oids = [ObjectId(x) for x in chunk_mongo_hex]
        cur = mongo_db["chunks"].find({"_id": {"$in": oids}})
        docs = list(cur)
        mongo_chunks_raw = len(docs)
        for d in docs:
            oid = str(d.get("_id"))
            mongo_chunks_by_oid[oid] = d
        if docs:
            mongo_keys_sample = sorted(list(docs[0].keys()))

    dbg["mongo_chunks_raw"] = mongo_chunks_raw
    dbg["mongo_chunk_keys_sample"] = mongo_keys_sample

    # 7) Build chunk items (only those we can show)
    # also compute roll-up scores
    lesson_best: Dict[str, float] = {}
    topic_best: Dict[str, float] = {}
    subject_best: Dict[str, float] = {}

    chunk_items: List[dict] = []

    # helper: get score for chunk
    score_by_chunk = dict(ranked)

    for chunk_id in top_chunk_ids:
        base = pg_map.get(chunk_id)
        if not base:
            continue

        s = float(score_by_chunk.get(chunk_id, 0.0))

        # roll-up
        lid = base["lesson"]["lessonID"]
        tid = base["topic"]["topicID"]
        sid = base["subject"]["subjectID"]
        if lid:
            lesson_best[lid] = max(lesson_best.get(lid, 0.0), s)
        if tid:
            topic_best[tid] = max(topic_best.get(tid, 0.0), s)
        if sid:
            subject_best[sid] = max(subject_best.get(sid, 0.0), s)

        # join mongo chunk doc
        chunk_doc = None
        oid_hex = base.get("chunkMongoId")
        if _valid_object_id_hex(oid_hex or ""):
            chunk_doc = mongo_chunks_by_oid.get(oid_hex)

        # if doc exists but hidden -> skip
        if chunk_doc and not _status_visible(chunk_doc):
            continue

        # build fields expected by frontend
        item = {
            "type": "chunk",
            "id": chunk_id,
            "name": base.get("chunkName") or chunk_doc.get("chunkName") if chunk_doc else base.get("chunkName"),
            "score": s,
            "chunkID": chunk_id,
            "chunkName": (chunk_doc.get("chunkName") if chunk_doc else None) or base.get("chunkName"),
            "chunkType": (chunk_doc.get("chunkType") if chunk_doc else None) or base.get("chunkType"),
            "chunkUrl": (chunk_doc.get("chunkUrl") if chunk_doc else None),
            "chunkDescription": (chunk_doc.get("chunkDescription") if chunk_doc else None),
            "keywords": (chunk_doc.get("keywords") if chunk_doc else None) or [],
            "isSaved": False,
            "class": {"classID": base["class"]["classID"], "className": base["class"]["className"]},
            "subject": {"subjectID": base["subject"]["subjectID"], "subjectName": base["subject"]["subjectName"]},
            "topic": {"topicID": base["topic"]["topicID"], "topicName": base["topic"]["topicName"]},
            "lesson": {"lessonID": base["lesson"]["lessonID"], "lessonName": base["lesson"]["lessonName"]},
        }

        # saved check (optional)
        try:
            saved = mongo_db["user_saved_chunks"].find_one({"username": username, "chunkID": chunk_id})
            item["isSaved"] = bool(saved)
        except Exception:
            pass

        chunk_items.append(item)

    # If we still have 0 mongo chunks, show why
    if debug:
        dbg["chunk_items_built"] = len(chunk_items)
        dbg["missing_chunk_ids_in_pg_join"] = [cid for cid in top_chunk_ids if cid not in pg_map]
        dbg["chunk_mongo_ids_used"] = chunk_mongo_hex[:10]

    # 8) Build lesson/topic/subject items from roll-up
    # Use PG names from the first chunk that maps to them.
    # For convenience, build maps from pg_rows.
    lesson_meta: Dict[str, dict] = {}
    topic_meta: Dict[str, dict] = {}
    subject_meta: Dict[str, dict] = {}

    for base in pg_map.values():
        lid = base["lesson"]["lessonID"]
        if lid and lid not in lesson_meta:
            lesson_meta[lid] = {
                "lessonID": lid,
                "lessonName": base["lesson"]["lessonName"],
                "class": {"classID": base["class"]["classID"], "className": base["class"]["className"]},
                "subject": {"subjectID": base["subject"]["subjectID"], "subjectName": base["subject"]["subjectName"]},
                "topic": {"topicID": base["topic"]["topicID"], "topicName": base["topic"]["topicName"]},
            }
        tid = base["topic"]["topicID"]
        if tid and tid not in topic_meta:
            topic_meta[tid] = {
                "topicID": tid,
                "topicName": base["topic"]["topicName"],
                "class": {"classID": base["class"]["classID"], "className": base["class"]["className"]},
                "subject": {"subjectID": base["subject"]["subjectID"], "subjectName": base["subject"]["subjectName"]},
            }
        sid = base["subject"]["subjectID"]
        if sid and sid not in subject_meta:
            subject_meta[sid] = {
                "subjectID": sid,
                "subjectName": base["subject"]["subjectName"],
                "class": {"classID": base["class"]["classID"], "className": base["class"]["className"]},
            }

    lesson_items = [
        {
            "type": "lesson",
            "id": lid,
            "name": lesson_meta.get(lid, {}).get("lessonName") or lid,
            "score": float(sc),
            **lesson_meta.get(lid, {}),
        }
        for lid, sc in lesson_best.items()
        if lid in lesson_meta
    ]

    topic_items = [
        {
            "type": "topic",
            "id": tid,
            "name": topic_meta.get(tid, {}).get("topicName") or tid,
            "score": float(sc),
            **topic_meta.get(tid, {}),
        }
        for tid, sc in topic_best.items()
        if tid in topic_meta
    ]

    subject_items = [
        {
            "type": "subject",
            "id": sid,
            "name": subject_meta.get(sid, {}).get("subjectName") or sid,
            "score": float(sc),
            **subject_meta.get(sid, {}),
        }
        for sid, sc in subject_best.items()
        if sid in subject_meta
    ]

    # 9) Merge + sort (chunk -> subject)
    all_items = chunk_items + lesson_items + topic_items + subject_items
    all_items.sort(key=lambda x: (_type_priority(x.get("type", "")), -float(x.get("score", 0.0))))

    total = len(all_items)
    paged = all_items[offset : offset + limit]

    res = {"total": total, "items": paged}
    if debug:
        dbg["total_items_after_merge"] = total
        res["debug"] = dbg
    return res
