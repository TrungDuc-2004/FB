from __future__ import annotations

import math
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from bson import ObjectId
from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ..models.model_postgre import Chunk, Class, Keyword, Lesson, Subject, Topic
from .keyword_embedding import embed_keyword_cached

_TOKEN_RE = re.compile(r"[0-9A-Za-zÀ-ỹ]+", flags=re.UNICODE)

# bump this when you replace the file so you can confirm the running code
_SERVICE_VERSION = "search_chunk_only_phrase_strict_v4_intent_strip"


# Vietnamese-ish stop words + a few generic fillers. Keep it small; we just want to avoid
# building concepts from "tài liệu", "về", ... which makes lexical matching too broad.
_STOP = {
    "tài",
    "liệu",
    "về",
    "của",
    "cho",
    "là",
    "các",
    "những",
    "một",
    "này",
    "đó",
    "ở",
    "trong",
    "và",
    "hoặc",
    "the",
    "a",
    "an",
    "of",
    "to",
    "in",
}


# Strip intent-style prefixes ONLY when they are clearly just a lead-in, e.g.
#   "thông tin về phần cứng máy tính" -> "phần cứng máy tính"
# while keeping searches like "thông tin" or "thông tin dữ liệu" intact.
_INTENT_PREFIX_RE = re.compile(
    r"^\s*(?:(?:tìm|tìm\s+kiếm|tra\s+cứu)\s+)?"  # optional leading verb
    r"(?:(?:các|những)\s+)?"  # optional plural
    r"(?:(thông\s+tin|tài\s+liệu|kiến\s+thức|nội\s+dung))\s+"  # intent head
    r"(về|cho|liên\s+quan\s+đến|liên\s+quan\s+tới|nói\s+về)\s+"  # connector
    r"(.+?)\s*$",
    flags=re.IGNORECASE | re.UNICODE,
)


def _strip_intent_prefix(q: str) -> Tuple[str, Dict[str, object]]:
    """Remove only *lead-in* phrases like 'thông tin về ...'.

    Returns (new_query, debug_meta).
    """

    s = _norm_spaces(q)
    if not s:
        return s, {"intent_stripped": False}

    m = _INTENT_PREFIX_RE.match(s)
    if not m:
        return s, {"intent_stripped": False}

    head = _norm_spaces(m.group(1) or "")
    link = _norm_spaces(m.group(2) or "")
    rest = _norm_spaces(m.group(3) or "")
    if not rest:
        return s, {"intent_stripped": False}

    return rest, {"intent_stripped": True, "intent_head": head, "intent_link": link}



def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _like_pat(term: str) -> str:
    """ILIKE pattern tolerant to '_' / multiple separators.

    Example: 'phần cứng' -> '%phần%cứng%'
    This matches 'phần cứng', 'phần_cứng', 'phần---cứng', ...
    """

    t = _norm_spaces((term or "").lower())
    if not t:
        return "%"
    t = t.replace(" ", "%")
    return f"%{t}%"


def _tokens_no_stop(q: str) -> list[str]:
    s = (q or "").strip().lower()
    if not s:
        return []
    raw = [t for t in _TOKEN_RE.findall(s) if t]
    toks = [t for t in raw if t not in _STOP and len(t) >= 2]
    return toks


def _pg_term_has_hit(*, pg: Session, term: str, cand_chunks: Optional[list[str]]) -> bool:
    """Return True if term appears in PG keyword_name OR chunk_name (within cand_chunks if provided)."""

    pat = _like_pat(term)
    try:
        stmt = select(Keyword.keyword_id).where(Keyword.keyword_name.ilike(pat))
        if cand_chunks is not None:
            if len(cand_chunks) == 0:
                return False
            stmt = stmt.where(Keyword.chunk_id.in_(cand_chunks))
        stmt = stmt.limit(1)
        if list(pg.execute(stmt).all()):
            return True
    except Exception:
        pass

    try:
        stmt2 = select(Chunk.chunk_id).where(Chunk.chunk_name.ilike(pat))
        if cand_chunks is not None:
            if len(cand_chunks) == 0:
                return False
            stmt2 = stmt2.where(Chunk.chunk_id.in_(cand_chunks))
        stmt2 = stmt2.limit(1)
        if list(pg.execute(stmt2).all()):
            return True
    except Exception:
        pass

    return False


def _segment_concepts_strict(*, pg: Session, tokens: list[str], cand_chunks: Optional[list[str]]) -> list[str]:
    """Greedy longest-match segmentation into phrases that actually exist in data.

    Goal: multi-word queries behave like AND (no fallback to single-word OR).
    We only accept a segmentation if it *covers all tokens* (each token is part of
    a chosen phrase/unigram that has a hit). Otherwise we return [].
    """

    if not tokens:
        return []

    n = len(tokens)
    max_ngram = 5  # up to 5-gram to catch phrases like 'hệ điều hành linux'
    out: list[str] = []
    i = 0
    covered = 0

    while i < n:
        found = None
        # try longest phrase starting at i
        for L in range(min(max_ngram, n - i), 1, -1):
            phrase = " ".join(tokens[i : i + L])
            if _pg_term_has_hit(pg=pg, term=phrase, cand_chunks=cand_chunks):
                found = phrase
                out.append(phrase)
                i += L
                covered += L
                break

        if found:
            continue

        # unigram fallback (still strict, because we must cover all tokens)
        t = tokens[i]
        if _pg_term_has_hit(pg=pg, term=t, cand_chunks=cand_chunks):
            out.append(t)
            covered += 1
        i += 1

    # must cover all tokens (otherwise we'd broaden too much)
    if covered < n:
        return []

    # de-dup preserve order
    seen = set()
    dedup = []
    for c in out:
        if c and c not in seen:
            seen.add(c)
            dedup.append(c)

    return dedup[:8]
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


def _concepts_from_query(q: str) -> List[str]:
    """Build *concepts* for phrase search.

    IMPORTANT: We DO NOT want the query to be treated as OR of single tokens.
    So we prioritize bigrams/phrases ("phần cứng", "máy tính") and only add
    unigrams when needed (typically English/alpha-numeric like "cpu", "ram", "linux").
    """

    s = (q or "").strip().lower()
    if not s:
        return []

    raw_tokens = [t for t in _TOKEN_RE.findall(s) if t]
    tokens = [t for t in raw_tokens if t not in _STOP and len(t) >= 2]
    if not tokens:
        return []

    concepts: List[str] = []

    # 1) Bigrams first (phrase intent)
    for i in range(len(tokens) - 1):
        a, b = tokens[i], tokens[i + 1]
        if len(a) < 2 or len(b) < 2:
            continue
        bg = f"{a} {b}".strip()
        if bg not in concepts:
            concepts.append(bg)

    # 2) Add alpha-numeric unigrams (CPU/RAM/Linux/Neo4j...) to not lose intent
    for t in tokens:
        if re.search(r"[0-9A-Za-z]", t) and len(t) >= 3:
            if t not in concepts:
                concepts.append(t)

    # 3) Fallback: if we failed to build bigrams (single-word query, etc.)
    if not concepts:
        for t in tokens:
            if len(t) >= 3 and t not in concepts:
                concepts.append(t)

    return concepts[:6]


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
        # BUT allow short alpha-numeric (cpu/ram) too.
        if (len(k) >= 4 or (re.search(r"[0-9A-Za-z]", k) and len(k) >= 3)) and k not in out:
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
    """Return chunk_ids where keyword_name or chunk_name ILIKE any term."""
    if not terms:
        return []

    out: List[str] = []
    seen: set[str] = set()

    try:
        cond = or_(*[Keyword.keyword_name.ilike(_like_pat(t)) for t in terms if t])
        stmt = select(Keyword.chunk_id).where(cond)
        if cand_chunks is not None:
            if len(cand_chunks) == 0:
                return []
            stmt = stmt.where(Keyword.chunk_id.in_(cand_chunks))
        rows = list(pg.execute(stmt).all())
        for (cid,) in rows:
            if cid and cid not in seen:
                seen.add(cid)
                out.append(cid)
    except Exception:
        pass

    try:
        cond2 = or_(*[Chunk.chunk_name.ilike(_like_pat(t)) for t in terms if t])
        stmt2 = select(Chunk.chunk_id).where(cond2)
        if cand_chunks is not None:
            if len(cand_chunks) == 0:
                return out
            stmt2 = stmt2.where(Chunk.chunk_id.in_(cand_chunks))
        rows2 = list(pg.execute(stmt2).all())
        for (cid,) in rows2:
            if cid and cid not in seen:
                seen.add(cid)
                out.append(cid)
    except Exception:
        pass

    return out


def _pg_chunks_matching_terms_min_hits(
    *,
    pg: Session,
    terms: List[str],
    cand_chunks: Optional[List[str]],
    min_hits: int,
) -> List[str]:
    """Return chunk_ids where >= min_hits distinct terms match (AND-ish).

    Matching sources:
      - Keyword.keyword_name ILIKE
      - Chunk.chunk_name ILIKE

    Patterns are tolerant to '_' vs ' ' (see _like_pat).
    """

    if not terms:
        return []

    if min_hits <= 1:
        return _pg_chunks_matching_terms(pg=pg, terms=terms, cand_chunks=cand_chunks)

    counts: Dict[str, int] = defaultdict(int)

    def _chunk_ids_for_term(t: str) -> set[str]:
        pat = _like_pat(t)
        ids: set[str] = set()
        try:
            stmt = select(Keyword.chunk_id).where(Keyword.keyword_name.ilike(pat))
            if cand_chunks is not None:
                if len(cand_chunks) == 0:
                    return set()
                stmt = stmt.where(Keyword.chunk_id.in_(cand_chunks))
            rows = list(pg.execute(stmt).all())
            ids |= {cid for (cid,) in rows if cid}
        except Exception:
            pass

        try:
            stmt2 = select(Chunk.chunk_id).where(Chunk.chunk_name.ilike(pat))
            if cand_chunks is not None:
                if len(cand_chunks) == 0:
                    return ids
                stmt2 = stmt2.where(Chunk.chunk_id.in_(cand_chunks))
            rows2 = list(pg.execute(stmt2).all())
            ids |= {cid for (cid,) in rows2 if cid}
        except Exception:
            pass

        return ids

    try:
        for t in terms:
            if not t:
                continue
            ids = _chunk_ids_for_term(t)
            for cid in ids:
                counts[cid] += 1

        return [cid for cid, c in counts.items() if c >= int(min_hits)]
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
    """Semantic search (chunk-only), with **strict AND for multi-word queries**.

    Requirement (from your report):
      - Query like "phần cứng máy tính" must NOT return docs about "phần cứng" OR "máy tính" alone.
      - If we can't satisfy the phrase/AND intent, return empty (no broad fallback).

    Strategy:
      - If query has >=2 meaningful tokens: strict mode
          * segment the query into phrases/unigrams that actually exist in PG data
          * require ALL concepts to match (keyword_name OR chunk_name)
          * do NOT fallback to looser semantic OR
      - Otherwise: keep the previous behavior (coverage-aware semantic ranking).
    """

    query = (q or "").strip()
    if not query:
        return {"total": 0, "items": []}

    dbg: Dict[str, object] = {"service_version": _SERVICE_VERSION, "category": category}

    # Intent lead-in stripping (ONLY for patterns like "thông tin về ...").
    # This prevents strict-mode from incorrectly requiring "thông tin" to exist as a keyword
    # when the user is clearly just asking *about* something.
    stripped_query, intent_meta = _strip_intent_prefix(query)
    if intent_meta.get("intent_stripped"):
        dbg["original_query"] = query
        dbg.update(intent_meta)
        query = stripped_query

    # Candidate restriction by filters (PG graph)
    cand_chunks = _candidate_chunk_ids_from_filters_pg(
        pg=pg, classID=classID, subjectID=subjectID, topicID=topicID, lessonID=lessonID
    )

    tokens = _tokens_no_stop(query)
    strict_mode = len(tokens) >= 2

    # 1) Build concepts
    if strict_mode:
        concepts = _segment_concepts_strict(pg=pg, tokens=tokens, cand_chunks=cand_chunks)
        dbg["strict_mode"] = True
        dbg["query_tokens"] = tokens
        dbg["query_concepts"] = concepts[:]

        if not concepts:
            res = {"total": 0, "items": []}
            if debug:
                dbg["reason"] = "strict_mode_no_full_coverage"
                res["debug"] = dbg
            return res

        # strict: must match ALL concepts
        must_coverage = len(concepts)
        lex_terms = concepts

        lex_chunk_ids = _pg_chunks_matching_terms_min_hits(
            pg=pg, terms=lex_terms, cand_chunks=cand_chunks, min_hits=must_coverage
        )

        dbg["lex_terms"] = lex_terms
        dbg["must_coverage"] = must_coverage
        dbg["lex_chunk_hits"] = len(lex_chunk_ids)

        if not lex_chunk_ids:
            res = {"total": 0, "items": []}
            if debug:
                dbg["reason"] = "strict_mode_no_lex_hits"
                res["debug"] = dbg
            return res

        restrict_chunk_ids = lex_chunk_ids

    else:
        concepts = _concepts_from_query(query)
        dbg["strict_mode"] = False
        dbg["query_concepts"] = concepts[:]

        if not concepts:
            return {"total": 0, "items": []}

        # legacy: require >=2 concept coverage when we have 2+ concepts
        must_coverage = 2 if len(concepts) >= 2 else 1

        # Lexical filter (coverage-aware). Only restrict if it yields anything.
        lex_terms = _lex_terms_from_keywords(concepts)
        lex_chunk_ids = _pg_chunks_matching_terms_min_hits(
            pg=pg, terms=lex_terms, cand_chunks=cand_chunks, min_hits=must_coverage
        )
        dbg["lex_terms"] = lex_terms
        dbg["lex_chunk_hits"] = len(lex_chunk_ids)
        dbg["must_coverage"] = must_coverage

        restrict_chunk_ids = lex_chunk_ids if lex_chunk_ids else cand_chunks

    # 2) Embed concepts (for ranking)
    q_embs = [embed_keyword_cached(c) for c in concepts]

    # 3) Load PG keywords with embeddings (also load keyword_name for transparency)
    try:
        stmt = (
            select(Keyword.keyword_embedding, Keyword.chunk_id, Keyword.keyword_name)
            .where(Keyword.keyword_embedding.isnot(None))
        )

        if restrict_chunk_ids is not None:
            if len(restrict_chunk_ids) == 0:
                res = {"total": 0, "items": []}
                if debug:
                    dbg["pg_rows_with_embedding"] = 0
                    dbg["ranked_chunks"] = 0
                    res["debug"] = dbg
                return res
            stmt = stmt.where(Keyword.chunk_id.in_(restrict_chunk_ids))

        rows = list(pg.execute(stmt).all())
    except SQLAlchemyError:
        rows = []

    dbg["pg_rows_with_embedding"] = len(rows)

    # 4) Score per chunk
    chunk_top_kw: Dict[str, List[Tuple[float, str]]] = {}

    if strict_mode:
        # Strict mode: lexical filtering already ensures AND.
        # We use embedding only for ranking; we do NOT drop chunks based on cosine thresholds.
        chunk_concept_best: Dict[str, List[float]] = defaultdict(lambda: [0.0] * len(q_embs))

        for emb, chunk_id, kw_name in rows:
            if not chunk_id or not emb:
                continue
            vec = list(emb)
            bests = chunk_concept_best[chunk_id]

            max_for_kw = 0.0
            for i, qe in enumerate(q_embs):
                sim = _cosine(vec, qe)
                if sim > max_for_kw:
                    max_for_kw = sim
                if sim > bests[i]:
                    bests[i] = sim

            if kw_name:
                arr = chunk_top_kw.get(chunk_id, [])
                arr.append((max_for_kw, str(kw_name)))
                arr.sort(key=lambda x: x[0], reverse=True)
                chunk_top_kw[chunk_id] = arr[:5]

        chunk_score: Dict[str, float] = {}
        for cid in restrict_chunk_ids or []:
            bests = chunk_concept_best.get(cid, [0.0] * len(q_embs))
            coverage = sum(1 for s in bests if s > 0.0)
            score = float(sum(bests)) + 0.05 * float(coverage)
            chunk_score[cid] = score

        ranked: List[Tuple[str, float]] = sorted(chunk_score.items(), key=lambda x: x[1], reverse=True)
        dbg["ranked_chunks"] = len(ranked)
        dbg["lex_mode"] = True
        dbg["sim_th"] = "disabled_in_strict_mode"

    else:
        # Non-strict: keep coverage + threshold to avoid OR behavior.
        SIM_TH = 0.18 if must_coverage >= 2 else 0.22

        chunk_concept_best: Dict[str, List[float]] = defaultdict(lambda: [0.0] * len(q_embs))

        for emb, chunk_id, kw_name in rows:
            if not chunk_id or not emb:
                continue

            vec = list(emb)
            bests = chunk_concept_best[chunk_id]

            max_for_kw = 0.0
            for i, qe in enumerate(q_embs):
                sim = _cosine(vec, qe)
                if sim > max_for_kw:
                    max_for_kw = sim
                if sim > bests[i]:
                    bests[i] = sim

            if kw_name:
                arr = chunk_top_kw.get(chunk_id, [])
                arr.append((max_for_kw, str(kw_name)))
                arr.sort(key=lambda x: x[0], reverse=True)
                chunk_top_kw[chunk_id] = arr[:5]

        chunk_score: Dict[str, float] = {}
        for cid, bests in chunk_concept_best.items():
            coverage = sum(1 for s in bests if s >= SIM_TH)
            if coverage < must_coverage:
                continue
            score = float(sum(bests)) + 0.15 * float(coverage)
            if coverage == len(bests) and len(bests) >= 2:
                score += 0.10
            chunk_score[cid] = score

        ranked = sorted(chunk_score.items(), key=lambda x: x[1], reverse=True)
        dbg["ranked_chunks"] = len(ranked)
        dbg["lex_mode"] = False
        dbg["sim_th"] = SIM_TH

    if not ranked:
        res = {"total": 0, "items": []}
        if debug:
            res["debug"] = dbg
        return res

    page_pairs = ranked[offset : offset + limit]
    page_chunk_ids = [cid for cid, _ in page_pairs]
    score_by_chunk = dict(page_pairs)

    # 5) Fetch chunk + hierarchy from PG
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

    # Map chunk_id -> hierarchy/meta
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

    # 6) Load Mongo docs
    mongo_chunks_by_oid = _load_by_oids(mongo_db, "chunks", chunk_mongo_hex)
    mongo_lessons_by_oid = _load_by_oids(mongo_db, "lessons", lesson_mongo_hex)
    mongo_topics_by_oid = _load_by_oids(mongo_db, "topics", topic_mongo_hex)
    mongo_subjects_by_oid = _load_by_oids(mongo_db, "subjects", subject_mongo_hex)

    dbg["mongo_chunks_raw"] = len(mongo_chunks_by_oid)

    # 7) Build items
    items: List[dict] = []
    dropped_hidden = 0
    dropped_missing_pg_join = 0

    for cid in page_chunk_ids:
        base = pg_map.get(cid)
        if not base:
            dropped_missing_pg_join += 1
            continue

        s = float(score_by_chunk.get(cid, 0.0))

        chunk_doc = None
        oid_hex = base.get("chunkMongoId")
        if _valid_object_id_hex(oid_hex or ""):
            chunk_doc = mongo_chunks_by_oid.get(oid_hex)

        if chunk_doc and not _status_visible(chunk_doc):
            dropped_hidden += 1
            continue

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

        try:
            saved = mongo_db["user_saved_chunks"].find_one({"username": username, "chunkID": cid})
            item["isSaved"] = bool(saved)
        except Exception:
            pass

        items.append(item)

    res = {"total": len(items), "items": items}

    if debug:
        dbg["items_built"] = len(items)
        dbg["dropped_hidden"] = dropped_hidden
        dbg["dropped_missing_pg_join"] = dropped_missing_pg_join
        if items:
            dbg["sample_item_match"] = {
                "chunkID": items[0].get("chunkID"),
                "keywords_in_doc": items[0].get("keywords"),
                "matchedKeywords": items[0].get("matchedKeywords"),
            }
        res["debug"] = dbg

    return res
