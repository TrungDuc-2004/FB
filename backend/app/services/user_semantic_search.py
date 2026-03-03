from __future__ import annotations

import math
import re
from typing import Dict, List, Optional, Tuple

from bson import ObjectId
from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ..models.model_postgre import Chunk, Class, Keyword, Lesson, Subject, Topic
from .keyword_embedding import embed_keyword_cached

_TOKEN_RE = re.compile(r"[0-9A-Za-zÀ-ỹ]+", flags=re.UNICODE)

# bump this when you replace the file so you can confirm the running code
_SERVICE_VERSION = "search_chunk_only_parent_links_lex_first_v1"


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


def _lex_terms_from_keywords(kws: List[str]) -> List[str]:
    """Pick a small set of terms for lexical matching.

    We primarily care about phrases like 'thông tin' and a few longer tokens.
    """
    out: List[str] = []
    for k in kws:
        k = (k or "").strip()
        if len(k) < 3:
            continue
        # keep the full phrase
        if " " in k:
            if k not in out:
                out.append(k)
            continue
        # keep only longer tokens to avoid too-broad matches
        if len(k) >= 4 and k not in out:
            out.append(k)
    return out[:6]


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


def _read_keywords_from_chunk_doc(doc: Optional[dict]) -> List[str]:
    if not doc:
        return []

    # common variants seen across your repo
    for k in ("keywordItems", "keywords", "keyword", "keyword_names", "keywordNames"):
        v = doc.get(k)
        if not v:
            continue

        # list of strings
        if isinstance(v, list) and (len(v) == 0 or isinstance(v[0], str)):
            return [str(x) for x in v if str(x).strip()]

        # list of dicts like {keywordName: ...}
        if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
            out = []
            for it in v:
                name = it.get("keywordName") or it.get("name")
                if name:
                    out.append(str(name))
            return out

        # string
        if isinstance(v, str):
            return [v]

    return []


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


def _load_by_oids(mongo_db, col: str, oid_hex_list: List[str]) -> Dict[str, dict]:
    """Return map oid_hex -> doc. No category filter."""
    out: Dict[str, dict] = {}
    if not oid_hex_list:
        return out
    try:
        oids = [ObjectId(x) for x in oid_hex_list if _valid_object_id_hex(x)]
        if not oids:
            return out
        docs = list(mongo_db[col].find({"_id": {"$in": oids}}))
        for d in docs:
            out[str(d.get("_id"))] = d
    except Exception:
        return out
    return out


def _pg_chunks_matching_terms(
    *,
    pg: Session,
    terms: List[str],
    cand_chunks: Optional[List[str]],
) -> List[str]:
    """Return chunk_ids where keyword_name ILIKE any term."""
    if not terms:
        return []

    try:
        cond = or_(*[Keyword.keyword_name.ilike(f"%{t}%") for t in terms])
        stmt = select(Keyword.chunk_id).where(cond)
        if cand_chunks is not None:
            if len(cand_chunks) == 0:
                return []
            stmt = stmt.where(Keyword.chunk_id.in_(cand_chunks))
        rows = list(pg.execute(stmt).all())
        out = []
        seen = set()
        for (cid,) in rows:
            if not cid or cid in seen:
                continue
            seen.add(cid)
            out.append(cid)
        return out
    except Exception:
        return []


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
    """Semantic search (chunk-only output), but with **lexical-first** filtering.

    Why:
      You reported "tìm 'thông tin' nhưng chunk không có keyword thông tin".
      If the query terms can be found lexically in keyword_name, we restrict results
      to ONLY chunks that have matching keywords.

    Flow:
      - extract keywords from query
      - (optional) lexical filter: keyword_name ILIKE '%term%'
      - embed query keywords
      - score PG Keyword.keyword_embedding -> rank chunks
      - join Mongo chunks by PG Chunk.mongo_id
      - attach lesson/topic/subject URLs (also by mongo_id)

    Output: 1 list chỉ gồm chunk items.
    """

    query = (q or "").strip()
    if not query:
        return {"total": 0, "items": []}

    dbg: Dict[str, object] = {"service_version": _SERVICE_VERSION, "category": category}

    # 1) Extract keywords
    kws = _extract_keywords(query)
    dbg["query_keywords"] = kws[:]

    # 2) Candidate restriction by filters (PG graph)
    cand_chunks = _candidate_chunk_ids_from_filters_pg(
        pg=pg, classID=classID, subjectID=subjectID, topicID=topicID, lessonID=lessonID
    )

    # 3) Lexical-first filter (only if it yields anything)
    lex_terms = _lex_terms_from_keywords(kws)
    lex_chunk_ids = _pg_chunks_matching_terms(pg=pg, terms=lex_terms, cand_chunks=cand_chunks)
    dbg["lex_terms"] = lex_terms
    dbg["lex_chunk_hits"] = len(lex_chunk_ids)

    # 4) Embed query
    q_embs = [embed_keyword_cached(k) for k in kws]

    # 5) Load PG keywords with embeddings (also load keyword_name for transparency)
    try:
        stmt = (
            select(Keyword.keyword_embedding, Keyword.chunk_id, Keyword.keyword_name)
            .where(Keyword.keyword_embedding.isnot(None))
        )
        # if lexical hits exist, restrict to those chunk ids
        if lex_chunk_ids:
            stmt = stmt.where(Keyword.chunk_id.in_(lex_chunk_ids))
        elif cand_chunks is not None:
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

    # 6) Score per chunk + keep top matched keyword names
    chunk_best: Dict[str, float] = {}
    chunk_top_kw: Dict[str, List[Tuple[float, str]]] = {}

    for emb, chunk_id, kw_name in rows:
        if not chunk_id or not emb:
            continue

        best = 0.0
        for qe in q_embs:
            best = max(best, _cosine(list(emb), qe))
        if best <= 0:
            continue

        prev = chunk_best.get(chunk_id, 0.0)
        if best > prev:
            chunk_best[chunk_id] = best

        if kw_name:
            arr = chunk_top_kw.get(chunk_id, [])
            arr.append((best, str(kw_name)))
            # keep top 5 by score
            arr.sort(key=lambda x: x[0], reverse=True)
            chunk_top_kw[chunk_id] = arr[:5]

    ranked: List[Tuple[str, float]] = sorted(chunk_best.items(), key=lambda x: x[1], reverse=True)
    dbg["ranked_chunks"] = len(ranked)
    dbg["lex_mode"] = bool(lex_chunk_ids)

    if not ranked:
        res = {"total": 0, "items": []}
        if debug:
            res["debug"] = dbg
        return res

    # We will build items and set total = len(items) to avoid confusing "2/3".
    page_pairs = ranked[offset : offset + limit]
    page_chunk_ids = [cid for cid, _ in page_pairs]
    score_by_chunk = dict(page_pairs)

    # 7) Fetch chunk + hierarchy from PG (includes mongo_id for each level)
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
            .where(Chunk.chunk_id.in_(page_chunk_ids))
        )
        pg_rows = list(pg.execute(stmt).all())
    except SQLAlchemyError:
        pg_rows = []

    dbg["pg_chunk_rows"] = len(pg_rows)

    # Map chunk_id -> hierarchy/meta (and mongo ids)
    pg_map: Dict[str, dict] = {}
    chunk_mongo_hex: List[str] = []
    lesson_mongo_hex: List[str] = []
    topic_mongo_hex: List[str] = []
    subject_mongo_hex: List[str] = []

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
        if _valid_object_id_hex(lesson_mongo_id or ""):
            lesson_mongo_hex.append(lesson_mongo_id)
        if _valid_object_id_hex(topic_mongo_id or ""):
            topic_mongo_hex.append(topic_mongo_id)
        if _valid_object_id_hex(subject_mongo_id or ""):
            subject_mongo_hex.append(subject_mongo_id)

    # 8) Load Mongo docs by _id (no category filter)
    mongo_chunks_by_oid = _load_by_oids(mongo_db, "chunks", chunk_mongo_hex)
    mongo_lessons_by_oid = _load_by_oids(mongo_db, "lessons", lesson_mongo_hex)
    mongo_topics_by_oid = _load_by_oids(mongo_db, "topics", topic_mongo_hex)
    mongo_subjects_by_oid = _load_by_oids(mongo_db, "subjects", subject_mongo_hex)

    dbg["mongo_chunks_raw"] = len(mongo_chunks_by_oid)

    # 9) Build items (chunk only) + attach parent links
    items: List[dict] = []
    dropped_hidden = 0
    dropped_missing_pg_join = 0

    for cid in page_chunk_ids:
        base = pg_map.get(cid)
        if not base:
            dropped_missing_pg_join += 1
            continue

        s = float(score_by_chunk.get(cid, 0.0))

        # join mongo chunk doc
        chunk_doc = None
        oid_hex = base.get("chunkMongoId")
        if _valid_object_id_hex(oid_hex or ""):
            chunk_doc = mongo_chunks_by_oid.get(oid_hex)

        if chunk_doc and not _status_visible(chunk_doc):
            dropped_hidden += 1
            continue

        # join parent docs (for url)
        lesson_doc = None
        topic_doc = None
        subject_doc = None

        l_oid = base["lesson"].get("mongoId")
        t_oid = base["topic"].get("mongoId")
        s_oid = base["subject"].get("mongoId")

        if _valid_object_id_hex(l_oid or ""):
            lesson_doc = mongo_lessons_by_oid.get(l_oid)
        if _valid_object_id_hex(t_oid or ""):
            topic_doc = mongo_topics_by_oid.get(t_oid)
        if _valid_object_id_hex(s_oid or ""):
            subject_doc = mongo_subjects_by_oid.get(s_oid)

        # if any parent is hidden, still allow chunk but just don't show parent link
        lesson_url = lesson_doc.get("lessonUrl") if (lesson_doc and _status_visible(lesson_doc)) else ""
        topic_url = topic_doc.get("topicUrl") if (topic_doc and _status_visible(topic_doc)) else ""
        subject_url = subject_doc.get("subjectUrl") if (subject_doc and _status_visible(subject_doc)) else ""

        matched_kw = [name for _, name in chunk_top_kw.get(cid, [])]

        item = {
            "type": "chunk",
            "id": cid,
            "name": base.get("chunkName") or (chunk_doc.get("chunkName") if chunk_doc else cid),
            "score": s,
            "chunkID": cid,
            "chunkName": (chunk_doc.get("chunkName") if chunk_doc else None) or base.get("chunkName"),
            "chunkType": (chunk_doc.get("chunkType") if chunk_doc else None) or base.get("chunkType"),
            "chunkUrl": (chunk_doc.get("chunkUrl") if chunk_doc else None),
            "chunkDescription": (chunk_doc.get("chunkDescription") if chunk_doc else None),
            "keywords": _read_keywords_from_chunk_doc(chunk_doc),
            "matchedKeywords": matched_kw,
            "isSaved": False,
            "class": {"classID": base["class"]["classID"], "className": base["class"]["className"]},
            "subject": {
                "subjectID": base["subject"]["subjectID"],
                "subjectName": base["subject"]["subjectName"],
                "subjectUrl": subject_url,
            },
            "topic": {
                "topicID": base["topic"]["topicID"],
                "topicName": base["topic"]["topicName"],
                "topicUrl": topic_url,
            },
            "lesson": {
                "lessonID": base["lesson"]["lessonID"],
                "lessonName": base["lesson"]["lessonName"],
                "lessonUrl": lesson_url,
            },
        }

        # saved check
        try:
            saved = mongo_db["user_saved_chunks"].find_one({"username": username, "chunkID": cid})
            item["isSaved"] = bool(saved)
        except Exception:
            pass

        items.append(item)

    # total should reflect what we actually return (avoid confusing 2/3)
    res = {"total": len(items), "items": items}

    if debug:
        dbg["items_built"] = len(items)
        dbg["dropped_hidden"] = dropped_hidden
        dbg["dropped_missing_pg_join"] = dropped_missing_pg_join
        # show why a chunk was returned
        if items:
            dbg["sample_item_match"] = {
                "chunkID": items[0].get("chunkID"),
                "keywords_in_doc": items[0].get("keywords"),
                "matchedKeywords": items[0].get("matchedKeywords"),
            }
        res["debug"] = dbg

    return res
