from __future__ import annotations

import json
import math
import os
import re
import urllib.error
import urllib.request
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
_SERVICE_VERSION = "neo4j_gemini_kwvec_top40_closest_v2"

# ===== Neo4j vector search config =====
_NEO4J_TOPK = 40  # per requirement
_NEO4J_KW_VECTOR_INDEX = os.getenv("NEO4J_KW_VECTOR_INDEX", "kw_vec")

# ===== Gemini keyword extraction =====
_GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or "").strip()
_GEMINI_MODEL = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash").strip()
_GEMINI_TIMEOUT_S = float(os.getenv("GEMINI_TIMEOUT_S", "12"))

# ===== "closest" tuning =====
# We filter hits relative to the best hit of each query-term.
_KW_DELTA = float(os.getenv("SEARCH_KW_DELTA", "0.10"))  # keep kw hits within best-DELTA
_CHUNK_DELTA = float(os.getenv("SEARCH_CHUNK_DELTA", "0.12"))  # keep chunks within best-DELTA
_MAX_TERMS = int(os.getenv("SEARCH_MAX_TERMS", "8"))
_MAX_HITS_PER_TERM = int(os.getenv("SEARCH_MAX_HITS_PER_TERM", "40"))  # cap after filtering


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


def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _tokens_no_stop(q: str) -> List[str]:
    s = _norm_spaces(q).lower()
    toks = [t for t in _TOKEN_RE.findall(s) if t]
    return [t for t in toks if t not in _STOP and len(t) >= 2]


def _is_free_question(q: str) -> bool:
    """Heuristic: question-like queries should be allowed to be broader.

    Topic-style queries (no '?', looks like a noun phrase) should be tighter.
    """

    s = _norm_spaces(q).lower()
    if not s:
        return False
    if "?" in s:
        return True
    # common Vietnamese question patterns
    starters = (
        "là gì",
        "là sao",
        "như thế nào",
        "tại sao",
        "vì sao",
        "có phải",
        "cách",
        "hướng dẫn",
        "làm sao",
    )
    return any(st in s for st in starters)


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
    s = _norm_spaces(q)
    if not s:
        return s, {"intent_stripped": False}
    m = _INTENT_PREFIX_RE.match(s)
    if not m:
        return s, {"intent_stripped": False}
    rest = _norm_spaces(m.group(3) or "")
    if not rest:
        return s, {"intent_stripped": False}
    return rest, {"intent_stripped": True, "intent_head": _norm_spaces(m.group(1) or ""), "intent_link": _norm_spaces(m.group(2) or "")}


def _parse_json_obj_or_array(text: str) -> Tuple[List[str], Dict[str, object]]:
    """Parse Gemini output.

    Accept:
      - JSON object: {"primary":"...", "keywords":[...]}
      - JSON array: ["...", ...]
      - fallback: split by newline/comma
    """

    s = (text or "").strip()
    if not s:
        return [], {"parse": "empty"}
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s*```$", "", s)
        s = s.strip()

    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            kws = obj.get("keywords") or obj.get("kws") or []
            primary = obj.get("primary") or obj.get("main")
            out: List[str] = []
            if isinstance(primary, str) and primary.strip():
                out.append(_norm_spaces(primary))
            if isinstance(kws, list):
                for it in kws:
                    if isinstance(it, str) and it.strip():
                        out.append(_norm_spaces(it))
            uniq: List[str] = []
            for k in out:
                if k and k not in uniq:
                    uniq.append(k)
            return uniq, {"parse": "json_obj", "primary": primary}

        if isinstance(obj, list):
            out = []
            for it in obj:
                if isinstance(it, str) and it.strip():
                    out.append(_norm_spaces(it))
            uniq: List[str] = []
            for k in out:
                if k and k not in uniq:
                    uniq.append(k)
            return uniq, {"parse": "json_array"}
    except Exception:
        pass

    parts = [p.strip() for p in re.split(r"[\n,;]+", s) if p.strip()]
    uniq: List[str] = []
    for p in parts:
        p2 = _norm_spaces(p)
        if p2 and p2 not in uniq:
            uniq.append(p2)
    return uniq, {"parse": "split"}


def _gemini_keywords_expand(q: str) -> Tuple[List[str], Dict[str, object]]:
    """Ask Gemini for a primary keyword + a few close related components/synonyms.

    This is used so topic queries like "phần cứng máy tính" can also retrieve
    close subtopics like CPU/RAM/Mainboard.
    """

    s = _norm_spaces(q)
    if not s:
        return [], {"gemini_used": False, "gemini_error": "empty_query"}
    if not _GEMINI_API_KEY:
        return [], {"gemini_used": False, "gemini_error": "missing_GEMINI_API_KEY"}

    prompt = (
        "Bạn là bộ trích xuất từ khoá cho hệ thống tra cứu tài liệu. "
        "Hãy trả về JSON *object* duy nhất với cấu trúc: "
        "{\"primary\": <cụm từ khoá chính>, \"keywords\": [<3-8 từ khoá liên quan gần nhất>]}. "
        "- primary: là cụm đủ nghĩa, ưu tiên đúng chủ đề người dùng. "
        "- keywords: là các khái niệm con/thành phần/đồng nghĩa gần nhất với primary (ví dụ: 'phần cứng máy tính' -> cpu, ram, mainboard, gpu...). "
        "- Loại bỏ các từ dẫn như 'thông tin', 'tài liệu', 'về', 'cho' khi chúng chỉ là câu dẫn. "
        "- Không viết giải thích, chỉ JSON.\n\n"
        f"Câu của người dùng: {s}"
    )

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{_GEMINI_MODEL}:generateContent?key={_GEMINI_API_KEY}"
    )
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 256},
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=_GEMINI_TIMEOUT_S) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        data = json.loads(raw)
        text_out = ""
        cands = data.get("candidates") or []
        if cands:
            parts = (((cands[0] or {}).get("content") or {}).get("parts") or [])
            if parts:
                text_out = str((parts[0] or {}).get("text") or "").strip()
        kws, pmeta = _parse_json_obj_or_array(text_out)
        kws = [k for k in kws if k]
        uniq: List[str] = []
        for k in kws:
            if k not in uniq:
                uniq.append(k)
        return uniq[:_MAX_TERMS], {"gemini_used": True, "gemini_raw": text_out, **pmeta}
    except urllib.error.HTTPError as e:
        return [], {"gemini_used": True, "gemini_error": f"HTTPError {getattr(e, 'code', '')}"}
    except Exception as e:
        return [], {"gemini_used": True, "gemini_error": str(e)}


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
        if len(a) < 3 or len(b) < 3:
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


def _pg_chunks_matching_terms_min_hits(
    *,
    pg: Session,
    terms: List[str],
    cand_chunks: Optional[List[str]],
    min_hits: int,
) -> List[str]:
    """Return chunk_ids where keyword_name ILIKE terms with >= min_hits coverage.

    We intentionally count coverage by term (AND-ish), not by keyword rows.
    This prevents phrase queries from being filtered too broadly.
    """

    if not terms:
        return []
    if min_hits <= 1:
        return _pg_chunks_matching_terms(pg=pg, terms=terms, cand_chunks=cand_chunks)

    counts: Dict[str, int] = defaultdict(int)
    try:
        for t in terms:
            if not t:
                continue
            stmt = select(Keyword.chunk_id).where(Keyword.keyword_name.ilike(f"%{t}%"))
            if cand_chunks is not None:
                if len(cand_chunks) == 0:
                    return []
                stmt = stmt.where(Keyword.chunk_id.in_(cand_chunks))
            rows = list(pg.execute(stmt).all())
            # count each chunk once per term
            seen_term = {cid for (cid,) in rows if cid}
            for cid in seen_term:
                counts[cid] += 1

        out = [cid for cid, c in counts.items() if c >= int(min_hits)]
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
    """Search pipeline (as requested):

    user input -> Gemini extract/expand keywords -> embed ->
    compare with Keyword embeddings stored in Neo4j (vector search topK=40) ->
    traverse Keyword->Chunk->Lesson->Topic->Subject->Class (scope filters) ->
    keep only the *closest* results -> return ranked chunks.

    Why your results were "ra gần hết":
      - vector search without a "closest" gate returns many semi-related keywords.
      - so we add best-delta filters on keyword hits and on chunk scores.

    Why your results were "0":
      - Neo4j vector index missing / query fails.
      - so we catch errors and fall back to PG cosine ranking.
    """

    query_raw = (q or "").strip()
    if not query_raw:
        return {"total": 0, "items": []}

    dbg: Dict[str, object] = {"service_version": _SERVICE_VERSION, "category": category}

    # Strip lead-in like "thông tin về ..."
    query, intent_meta = _strip_intent_prefix(query_raw)
    if intent_meta.get("intent_stripped"):
        dbg["original_query"] = query_raw
        dbg.update(intent_meta)

    free_question = _is_free_question(query_raw)
    dbg["free_question"] = bool(free_question)

    # Candidate restriction by filters (PG chain) - used as optional chunk scope
    cand_chunks = _candidate_chunk_ids_from_filters_pg(
        pg=pg, classID=classID, subjectID=subjectID, topicID=topicID, lessonID=lessonID
    )
    if cand_chunks is not None:
        dbg["cand_chunks_from_filters"] = len(cand_chunks)

    # ===== 1) Gemini keywords =====
    terms: List[str] = []
    gem_meta: Dict[str, object] = {}
    try:
        terms, gem_meta = _gemini_keywords_expand(query)
    except Exception as e:
        terms, gem_meta = [], {"gemini_used": False, "gemini_error": str(e)}

    if not terms:
        # fallback: use the stripped query as the only term
        terms = [_norm_spaces(query)]
        dbg["keyword_source"] = "fallback_query"
    else:
        dbg["keyword_source"] = "gemini"

    # uniq + cap
    uniq_terms: List[str] = []
    for t in terms:
        tt = _norm_spaces(t)
        if tt and tt not in uniq_terms:
            uniq_terms.append(tt)
    terms = uniq_terms[:_MAX_TERMS]

    dbg["terms"] = terms
    if debug:
        dbg.update(gem_meta)

    # ===== 2) Embed =====
    term_vecs = [embed_keyword_cached(t) for t in terms]

    # We'll store best score per term for each chunk.
    chunk_term_best: Dict[str, List[float]] = defaultdict(lambda: [0.0] * len(terms))
    chunk_top_kw: Dict[str, List[Tuple[float, str]]] = {}
    neo_errors: List[str] = []
    neo_rows_total = 0

    # Limit cand chunks to not overload Neo4j with huge IN list
    cand_for_neo: Optional[List[str]] = None
    if cand_chunks is not None and len(cand_chunks) <= 2000:
        cand_for_neo = cand_chunks

    def _neo_hits_for_vec(q_vec: List[float]) -> Tuple[List[dict], Optional[str]]:
        if neo is None:
            return [], "neo_session_none"

        cypher = """
        CALL db.index.vector.queryNodes($index_name, $topk, $q_vec) YIELD node AS kw, score
        MATCH (ch:Chunk)-[:HAS_KEYWORD]->(kw)
        WHERE ($cand_chunks IS NULL OR ch.pg_id IN $cand_chunks)
        OPTIONAL MATCH (l:Lesson)-[:HAS_CHUNK]->(ch)
        OPTIONAL MATCH (t:Topic)-[:HAS_LESSON]->(l)
        OPTIONAL MATCH (s:Subject)-[:HAS_TOPIC]->(t)
        OPTIONAL MATCH (c:Class)-[:HAS_SUBJECT]->(s)
        WHERE ($lessonID = '' OR l.pg_id = $lessonID)
          AND ($topicID = '' OR t.pg_id = $topicID)
          AND ($subjectID = '' OR s.pg_id = $subjectID)
          AND ($classID = '' OR c.pg_id = $classID)
        CALL {
          WITH kw
          MATCH (k2:Keyword {name: kw.name})
          RETURN count(k2) AS name_df
        }
        RETURN ch.pg_id AS chunk_id,
               kw.name AS kw_name,
               kw.pg_id AS kw_pg_id,
               score AS score,
               name_df AS name_df
        ORDER BY score DESC
        LIMIT $topk
        """

        try:
            rs = neo.run(
                cypher,
                index_name=_NEO4J_KW_VECTOR_INDEX,
                topk=int(_NEO4J_TOPK),
                q_vec=q_vec,
                cand_chunks=cand_for_neo,
                classID=classID or "",
                subjectID=subjectID or "",
                topicID=topicID or "",
                lessonID=lessonID or "",
            )
            return [dict(r) for r in rs], None
        except Exception as e:
            return [], str(e)

    # ===== 3) Neo4j vector search per term, then keep only "closest" =====
    if neo is not None:
        for i, qv in enumerate(term_vecs):
            rows, err = _neo_hits_for_vec(qv)
            if err:
                neo_errors.append(err)
                continue
            if not rows:
                continue

            neo_rows_total += len(rows)
            # dynamic closest gate for this term
            best_score = max(float(r.get("score") or 0.0) for r in rows)
            keep_th = max(0.0, best_score - float(_KW_DELTA))

            kept = [r for r in rows if float(r.get("score") or 0.0) >= keep_th]
            kept = kept[: int(_MAX_HITS_PER_TERM)]

            for r in kept:
                cid = str(r.get("chunk_id") or "")
                if not cid:
                    continue
                score = float(r.get("score") or 0.0)
                # penalize overly-common keyword names
                name_df = float(r.get("name_df") or 1.0)
                if name_df < 1:
                    name_df = 1.0
                w = score / math.sqrt(name_df)

                bests = chunk_term_best[cid]
                if w > bests[i]:
                    bests[i] = w

                kw_name = str(r.get("kw_name") or "").strip()
                if kw_name:
                    arr = chunk_top_kw.get(cid, [])
                    arr.append((w, kw_name))
                    arr.sort(key=lambda x: x[0], reverse=True)
                    chunk_top_kw[cid] = arr[:5]

    dbg["neo_rows"] = neo_rows_total
    if neo_errors and debug:
        dbg["neo_errors"] = neo_errors[:2]

    # ===== 4) Fallback: PG cosine if Neo4j produced nothing (index missing, etc.) =====
    if not chunk_term_best:
        # PG scan fallback (still uses "closest" gates)
        try:
            stmt = select(Keyword.keyword_embedding, Keyword.chunk_id, Keyword.keyword_name).where(
                Keyword.keyword_embedding.isnot(None)
            )
            if cand_chunks is not None:
                if len(cand_chunks) == 0:
                    res = {"total": 0, "items": []}
                    if debug:
                        dbg["reason"] = "cand_chunks_empty"
                        dbg["pg_rows_with_embedding"] = 0
                        res["debug"] = dbg
                    return res
                stmt = stmt.where(Keyword.chunk_id.in_(cand_chunks))
            rows = list(pg.execute(stmt).all())
        except Exception:
            rows = []

        dbg["pg_rows_with_embedding"] = len(rows)

        # precompute name_df in PG (keyword_name frequency)
        name_df_map: Dict[str, int] = defaultdict(int)
        for _, _, kw_name in rows:
            if kw_name:
                name_df_map[str(kw_name)] += 1

        for term_i, qv in enumerate(term_vecs):
            # compute best score for this term
            sims: List[Tuple[str, float, str]] = []
            for emb, chunk_id, kw_name in rows:
                if not chunk_id or not emb:
                    continue
                sim = _cosine(list(emb), qv)
                sims.append((str(chunk_id), float(sim), str(kw_name or "")))
            if not sims:
                continue
            best_sim = max(s for _, s, _ in sims)
            keep_th = max(0.0, best_sim - float(_KW_DELTA))
            # keep only closest
            sims.sort(key=lambda x: x[1], reverse=True)
            for cid, sim, kw_name in sims:
                if sim < keep_th:
                    break
                df = float(name_df_map.get(kw_name, 1) or 1)
                w = sim / math.sqrt(df)
                bests = chunk_term_best[cid]
                if w > bests[term_i]:
                    bests[term_i] = w
                if kw_name:
                    arr = chunk_top_kw.get(cid, [])
                    arr.append((w, kw_name))
                    arr.sort(key=lambda x: x[0], reverse=True)
                    chunk_top_kw[cid] = arr[:5]
        if debug:
            dbg["fallback_used"] = "pg"

    # ===== 5) Rank chunks + "closest" filter =====
    chunk_score: Dict[str, float] = {}
    for cid, bests in chunk_term_best.items():
        coverage = sum(1 for s in bests if s > 0.0)
        if coverage == 0:
            continue
        # combine: max + sum to allow subtopic matches to help
        score = float(max(bests)) + 0.35 * float(sum(sorted(bests, reverse=True)[:3]))
        score += 0.06 * float(coverage)
        chunk_score[cid] = score

    if not chunk_score:
        res = {"total": 0, "items": []}
        if debug:
            dbg["reason"] = "no_chunk_scores"
            res["debug"] = dbg
        return res

    best_chunk = max(chunk_score.values())
    keep_chunk_th = max(0.0, float(best_chunk) - float(_CHUNK_DELTA))
    ranked: List[Tuple[str, float]] = sorted(
        [(cid, sc) for cid, sc in chunk_score.items() if sc >= keep_chunk_th],
        key=lambda x: x[1],
        reverse=True,
    )

    dbg["ranked_chunks"] = len(ranked)
    dbg["keep_chunk_th"] = keep_chunk_th
    dbg["kw_delta"] = _KW_DELTA
    dbg["chunk_delta"] = _CHUNK_DELTA

    if not ranked:
        res = {"total": 0, "items": []}
        if debug:
            dbg["reason"] = "closest_filter_removed_all"
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
