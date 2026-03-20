from __future__ import annotations

import math
import re
import unicodedata
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from bson import ObjectId
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ..models.model_postgre import Chunk, Class, Image, Keyword, Lesson, Subject, Topic, Video
from .gemini_topic_expander import expand_topic_keywords_debug
from .keyword_embedding import embed_keyword_cached

_TOKEN_RE = re.compile(r"[0-9A-Za-zÀ-ỹ]+", flags=re.UNICODE)
_SERVICE_VERSION = "search_hierarchical_keyword_neo4j_only_v2"
_LABEL_RE = re.compile(
    r"(?P<class>\b(?:lớp|lop|class)\b)|"
    r"(?P<topic>\b(?:chủ\s*đề|chu\s*de|topic)\b)|"
    r"(?P<lesson>\b(?:bài|bai|lesson)\b)|"
    r"(?P<chunk>\b(?:chunk|mục|muc)\b)",
    flags=re.IGNORECASE,
)

_STOP = {
    "a", "an", "and", "các", "cái", "cho", "có", "của", "dạng", "đến", "giúp",
    "hãy", "in", "không", "kiếm", "là", "liên", "muốn", "một", "nào", "những",
    "of", "or", "ở", "quan", "the", "to", "tài", "tìm", "trong", "tôi", "và",
    "về", "với", "xin", "đó", "này", "hoặc", "cần", "liệu",
}


# --------------------------- basic text utils ---------------------------

def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _normalize_for_phrase_strip(text: str) -> str:
    s = _norm_spaces((text or "").lower())
    return re.sub(r"\s+", " ", s).strip()


def _strip_query_filler_phrases(text: str) -> str:
    s = _normalize_for_phrase_strip(text)
    filler_patterns = [
        r"\btài\s+liệu\s+về\b",
        r"\btài\s+liệu\b",
        r"\bthông\s+tin\s+về\b",
        r"\bthông\s+tin\b",
        r"\bnội\s+dung\s+về\b",
        r"\bnội\s+dung\b",
        r"\bcho\s+tôi\b",
        r"\bhãy\b",
        r"\bxin\b",
    ]
    for pat in filler_patterns:
        s = re.sub(pat, " ", s, flags=re.IGNORECASE)
    return _norm_spaces(s)


def _tokens_no_stop(q: str) -> List[str]:
    raw = [t.lower() for t in _TOKEN_RE.findall(q or "") if t]
    return [t for t in raw if len(t) >= 2 and t not in _STOP]


def _core_query_text(q: str) -> str:
    return _norm_spaces(" ".join(_tokens_no_stop(q)))


def _dedupe_keep_order(values: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        clean = _norm_spaces(str(value or "").lower())
        if not clean or clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def _clean_hint_text(segment: str) -> str:
    text = re.sub(r"\d+", " ", segment or "")
    text = re.sub(r"[:;,\.\-_/]+", " ", text)
    return _core_query_text(text)


def _strip_keyword_filler(text: str) -> str:
    return _core_query_text(_strip_query_filler_phrases(text))


def _strip_accents(text: str) -> str:
    s = unicodedata.normalize("NFD", text or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.replace("đ", "d").replace("Đ", "D")


def _norm_keyword_text(text: str) -> str:
    base = _strip_query_filler_phrases(text)
    base = _strip_accents(base)
    return _norm_spaces(" ".join(_tokens_no_stop(base))).lower()


# --------------------------- similarity ---------------------------

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


# --------------------------- parsing ---------------------------

def _parse_query_context(query: str) -> dict:
    raw = _norm_spaces(query)
    matches = list(_LABEL_RE.finditer(raw))
    out = {
        "classNumber": None,
        "topicNumber": None,
        "lessonNumber": None,
        "chunkNumber": None,
        "topicNameHint": "",
        "lessonNameHint": "",
        "chunkNameHint": "",
        "genericQuery": "",
        "raw": raw,
    }
    if not matches:
        out["genericQuery"] = _strip_keyword_filler(raw)
        return out

    generic_parts: List[str] = []
    first_start = matches[0].start()
    if first_start > 0:
        generic_parts.append(raw[:first_start])

    for idx, match in enumerate(matches):
        level = next((name for name, val in match.groupdict().items() if val), None)
        if not level:
            continue
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(raw)
        segment = _norm_spaces(raw[start:end])
        num_match = re.search(r"\d{1,3}", segment)
        number = int(num_match.group(0)) if num_match else None
        name_hint = _clean_hint_text(segment)

        if level == "class":
            if out["classNumber"] is None:
                out["classNumber"] = number
        elif level == "topic":
            if out["topicNumber"] is None:
                out["topicNumber"] = number
            if name_hint and not out["topicNameHint"]:
                out["topicNameHint"] = name_hint
        elif level == "lesson":
            if out["lessonNumber"] is None:
                out["lessonNumber"] = number
            if name_hint and not out["lessonNameHint"]:
                out["lessonNameHint"] = name_hint
        elif level == "chunk":
            if out["chunkNumber"] is None:
                out["chunkNumber"] = number
            if name_hint and not out["chunkNameHint"]:
                out["chunkNameHint"] = name_hint

    last_end = matches[-1].end()
    tail = raw[last_end:]
    # only append trailing text if it is not simply the number/name of the last label segment
    if tail and not re.fullmatch(r"\s*\d+\s*", tail):
        generic_parts.append(tail)

    generic_query = _strip_keyword_filler(" ".join(generic_parts))
    out["genericQuery"] = generic_query
    return out


def _normalize_class_scope(class_id: str, class_number: Optional[int]) -> str:
    if class_id:
        return class_id
    if class_number is not None:
        return str(class_number)
    return ""


def _filter_by_number(rows: List[dict], key: str, number: Optional[int]) -> List[dict]:
    if number is None:
        return rows
    return [row for row in rows if row.get(key) == number]


# --------------------------- db helpers ---------------------------

def _valid_object_id_hex(s: str) -> bool:
    if not s or len(s) != 24:
        return False
    try:
        int(s, 16)
        return True
    except Exception:
        return False


def _status_visible(doc: dict) -> bool:
    st = (doc or {}).get("status")
    return st not in {"hidden", "HIDDEN"}


def _read_keywords_from_chunk_doc(doc: Optional[dict]) -> List[str]:
    if not doc:
        return []
    for key in ("keywordItems", "keywords", "keyword", "keyword_names", "keywordNames"):
        value = doc.get(key)
        if not value:
            continue
        if isinstance(value, list) and (len(value) == 0 or isinstance(value[0], str)):
            return [str(x) for x in value if str(x).strip()]
        if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
            out: List[str] = []
            for item in value:
                name = item.get("keywordName") or item.get("name")
                if name:
                    out.append(str(name))
            return out
        if isinstance(value, str):
            return [value]
    return []


def _load_by_oids(mongo_db, col: str, oid_hex_list: List[str]) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    if mongo_db is None or not oid_hex_list:
        return out
    try:
        oids = [ObjectId(x) for x in oid_hex_list if _valid_object_id_hex(x)]
        if not oids:
            return out
        for doc in mongo_db[col].find({"_id": {"$in": oids}}):
            out[str(doc.get("_id"))] = doc
    except Exception:
        return out
    return out


def _media_sort_key(x: dict) -> tuple:
    priority = {"chunk": 0, "lesson": 1, "topic": 2, "subject": 3}
    return (priority.get((x or {}).get("followType"), 99), ((x or {}).get("name") or "").lower())


def _build_media_item(doc: Optional[dict], *, media_type: str, follow_type: str, follow_id: str, pg_id: str) -> Optional[dict]:
    if not doc or not _status_visible(doc):
        return None
    if media_type == "image":
        name = doc.get("imgName") or doc.get("mapID") or pg_id
        desc = doc.get("imgDescription") or ""
        url = doc.get("imgUrl") or ""
    else:
        name = doc.get("videoName") or doc.get("mapID") or pg_id
        desc = doc.get("videoDescription") or ""
        url = doc.get("videoUrl") or ""
    return {
        "type": media_type,
        "id": pg_id,
        "name": name,
        "description": desc,
        "url": url,
        "mapID": doc.get("mapID") or "",
        "mongoID": str(doc.get("_id")) if doc.get("_id") is not None else "",
        "followType": follow_type,
        "followID": follow_id,
    }


def _load_media_map_for_targets(*, pg: Session, mongo_db, targets: List[tuple[str, str]]) -> Dict[tuple[str, str], dict]:
    out: Dict[tuple[str, str], dict] = {}
    uniq_targets: List[tuple[str, str]] = []
    seen_targets = set()
    for ft, fid in targets:
        key = ((ft or "").strip(), (fid or "").strip())
        if not key[0] or not key[1] or key in seen_targets:
            continue
        seen_targets.add(key)
        uniq_targets.append(key)
    if not uniq_targets:
        return out

    image_rows = []
    video_rows = []
    try:
        conds = [((Image.follow_type == ft) & (Image.follow_id == fid)) for ft, fid in uniq_targets]
        if conds:
            image_rows = list(pg.execute(select(Image.img_id, Image.mongo_id, Image.follow_type, Image.follow_id).where(or_(*conds))).all())
    except Exception:
        image_rows = []
    try:
        conds = [((Video.follow_type == ft) & (Video.follow_id == fid)) for ft, fid in uniq_targets]
        if conds:
            video_rows = list(pg.execute(select(Video.video_id, Video.mongo_id, Video.follow_type, Video.follow_id).where(or_(*conds))).all())
    except Exception:
        video_rows = []

    image_oids = [mongo_id for _pgid, mongo_id, _ft, _fid in image_rows if _valid_object_id_hex(mongo_id or "")]
    video_oids = [mongo_id for _pgid, mongo_id, _ft, _fid in video_rows if _valid_object_id_hex(mongo_id or "")]
    images_by_oid = _load_by_oids(mongo_db, "images", image_oids)
    videos_by_oid = _load_by_oids(mongo_db, "videos", video_oids)

    for img_id, mongo_id, follow_type, follow_id in image_rows:
        item = _build_media_item(images_by_oid.get(mongo_id or ""), media_type="image", follow_type=follow_type, follow_id=follow_id, pg_id=img_id)
        if not item:
            continue
        bucket = out.setdefault((follow_type, follow_id), {"images": [], "videos": []})
        bucket["images"].append(item)
    for video_id, mongo_id, follow_type, follow_id in video_rows:
        item = _build_media_item(videos_by_oid.get(mongo_id or ""), media_type="video", follow_type=follow_type, follow_id=follow_id, pg_id=video_id)
        if not item:
            continue
        bucket = out.setdefault((follow_type, follow_id), {"images": [], "videos": []})
        bucket["videos"].append(item)
    for bucket in out.values():
        bucket["images"].sort(key=_media_sort_key)
        bucket["videos"].sort(key=_media_sort_key)
    return out


def _load_topic_rows_pg(*, pg: Session, class_id: str, subject_id: str, topic_id: str) -> List[dict]:
    try:
        stmt = (
            select(
                Topic.topic_id,
                Topic.topic_name,
                Topic.topic_number,
                Topic.mongo_id,
                Subject.subject_id,
                Subject.subject_name,
                Subject.mongo_id,
                Class.class_id,
                Class.class_name,
                Class.mongo_id,
            )
            .join(Subject, Subject.subject_id == Topic.subject_id)
            .join(Class, Class.class_id == Subject.class_id)
        )
        if class_id:
            stmt = stmt.where(Class.class_id == class_id)
        if subject_id:
            stmt = stmt.where(Subject.subject_id == subject_id)
        if topic_id:
            stmt = stmt.where(Topic.topic_id == topic_id)
        rows = []
        for r in pg.execute(stmt).all():
            rows.append({
                "topicID": str(r[0]), "topicName": r[1], "topicNumber": r[2], "topicMongoId": r[3],
                "subjectID": str(r[4]), "subjectName": r[5], "subjectMongoId": r[6],
                "classID": str(r[7]), "className": r[8], "classMongoId": r[9],
            })
        return rows
    except SQLAlchemyError:
        return []


def _load_lesson_rows_pg(*, pg: Session, class_id: str, subject_id: str, topic_ids: Optional[List[str]], lesson_id: str) -> List[dict]:
    try:
        stmt = (
            select(
                Lesson.lesson_id,
                Lesson.lesson_name,
                Lesson.lesson_number,
                Lesson.mongo_id,
                Topic.topic_id,
                Topic.topic_name,
                Topic.topic_number,
                Topic.mongo_id,
                Subject.subject_id,
                Subject.subject_name,
                Subject.mongo_id,
                Class.class_id,
                Class.class_name,
                Class.mongo_id,
            )
            .join(Topic, Topic.topic_id == Lesson.topic_id)
            .join(Subject, Subject.subject_id == Topic.subject_id)
            .join(Class, Class.class_id == Subject.class_id)
        )
        if class_id:
            stmt = stmt.where(Class.class_id == class_id)
        if subject_id:
            stmt = stmt.where(Subject.subject_id == subject_id)
        if topic_ids is not None:
            if len(topic_ids) == 0:
                return []
            stmt = stmt.where(Topic.topic_id.in_(topic_ids))
        if lesson_id:
            stmt = stmt.where(Lesson.lesson_id == lesson_id)
        rows = []
        for r in pg.execute(stmt).all():
            rows.append({
                "lessonID": str(r[0]), "lessonName": r[1], "lessonNumber": r[2], "lessonMongoId": r[3],
                "topicID": str(r[4]), "topicName": r[5], "topicNumber": r[6], "topicMongoId": r[7],
                "subjectID": str(r[8]), "subjectName": r[9], "subjectMongoId": r[10],
                "classID": str(r[11]), "className": r[12], "classMongoId": r[13],
            })
        return rows
    except SQLAlchemyError:
        return []


def _load_chunk_rows_pg(*, pg: Session, class_id: str, subject_id: str, topic_ids: Optional[List[str]], lesson_ids: Optional[List[str]]) -> List[dict]:
    try:
        stmt = (
            select(
                Chunk.chunk_id,
                Chunk.chunk_name,
                Chunk.chunk_number,
                Chunk.chunk_type,
                Chunk.mongo_id,
                Lesson.lesson_id,
                Lesson.lesson_name,
                Lesson.lesson_number,
                Lesson.mongo_id,
                Topic.topic_id,
                Topic.topic_name,
                Topic.topic_number,
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
        )
        if class_id:
            stmt = stmt.where(Class.class_id == class_id)
        if subject_id:
            stmt = stmt.where(Subject.subject_id == subject_id)
        if topic_ids is not None:
            if len(topic_ids) == 0:
                return []
            stmt = stmt.where(Topic.topic_id.in_(topic_ids))
        if lesson_ids is not None:
            if len(lesson_ids) == 0:
                return []
            stmt = stmt.where(Lesson.lesson_id.in_(lesson_ids))
        rows = []
        for r in pg.execute(stmt).all():
            rows.append({
                "chunkID": str(r[0]), "chunkName": r[1], "chunkNumber": r[2], "chunkType": r[3], "chunkMongoId": r[4],
                "lessonID": str(r[5]), "lessonName": r[6], "lessonNumber": r[7], "lessonMongoId": r[8],
                "topicID": str(r[9]), "topicName": r[10], "topicNumber": r[11], "topicMongoId": r[12],
                "subjectID": str(r[13]), "subjectName": r[14], "subjectMongoId": r[15],
                "classID": str(r[16]), "className": r[17], "classMongoId": r[18],
            })
        return rows
    except SQLAlchemyError:
        return []


def _load_name_embedding_map_from_neo(neo, *, label: str, ids: List[str], embedding_field: str) -> Tuple[Dict[str, dict], Optional[str]]:
    if neo is None or not ids:
        return {}, None if neo is not None else "neo_session_unavailable"
    try:
        records = neo.run(
            f"""
            UNWIND $ids AS node_id
            MATCH (n:{label} {{pg_id: node_id}})
            WHERE n.{embedding_field} IS NOT NULL
            RETURN n.pg_id AS pg_id,
                   coalesce(n.name, n.pg_id) AS name,
                   n.{embedding_field} AS embedding
            """,
            ids=ids,
        )
        out: Dict[str, dict] = {}
        for record in records:
            node_id = str(record.get("pg_id") or "").strip()
            embedding = record.get("embedding")
            if not node_id or not embedding:
                continue
            try:
                out[node_id] = {
                    "id": node_id,
                    "name": str(record.get("name") or "").strip(),
                    "embedding": [float(x) for x in list(embedding)],
                }
            except Exception:
                continue
        return out, None
    except Exception as exc:
        return {}, str(exc)


def _resolve_scope_by_name(
    *,
    rows: List[dict],
    id_key: str,
    name_hint: str,
    neo,
    label: str,
    embedding_field: str,
    threshold: float = 0.28,
    keep_ratio: float = 0.82,
    keep_limit: int = 6,
) -> Tuple[List[str], Dict[str, float], dict]:
    debug = {"label": label, "name_hint": name_hint, "input_count": len(rows)}
    if not name_hint:
        ids = [str(row.get(id_key) or "") for row in rows if str(row.get(id_key) or "")]
        return ids, {}, debug
    ids = [str(row.get(id_key) or "") for row in rows if str(row.get(id_key) or "")]
    emb_map, neo_error = _load_name_embedding_map_from_neo(neo, label=label, ids=ids, embedding_field=embedding_field)
    if neo_error:
        debug["neo_error"] = neo_error
    if not emb_map:
        debug["rejected"] = "no_embedding_rows"
        return [], {}, debug
    query_embedding = embed_keyword_cached(name_hint)
    scored: List[dict] = []
    for node_id, payload in emb_map.items():
        score = _cosine(query_embedding, list(payload.get("embedding") or []))
        scored.append({"id": node_id, "name": payload.get("name") or node_id, "score": float(score)})
    scored.sort(key=lambda item: item["score"], reverse=True)
    debug["top_matches"] = scored[:5]
    if not scored:
        debug["rejected"] = "no_scores"
        return [], {}, debug
    top_score = float(scored[0]["score"])
    debug["top_score"] = top_score
    if top_score < threshold:
        debug["rejected"] = "below_threshold"
        return [], {}, debug
    min_keep = max(threshold, top_score * keep_ratio)
    selected = [item for item in scored if float(item["score"]) >= min_keep][:keep_limit]
    score_map = {str(item["id"]): float(item["score"]) for item in selected}
    return [str(item["id"]) for item in selected], score_map, debug


# --------------------------- keyword search helpers ---------------------------

def _maybe_expand_with_gemini(query: str, cand_chunks: Optional[List[str]], pg: Session) -> Tuple[List[str], Dict[str, object]]:
    debug: Dict[str, object] = {}
    clean = _norm_spaces(query)
    if not clean:
        return [], debug
    try:
        gemini_terms, gem_dbg = expand_topic_keywords_debug(clean, None)
        debug.update(gem_dbg or {})
    except Exception as exc:
        return [], {"error": f"unexpected:{exc}"}

    terms = _dedupe_keep_order(list(gemini_terms or []))[:8]
    debug["before_scope_filter"] = terms[:]
    if not terms:
        return [], debug
    try:
        filtered: List[str] = []
        for term in terms:
            pat = "%" + term.replace(" ", "%") + "%"
            stmt = select(Keyword.keyword_id).where(Keyword.keyword_name.ilike(pat)).limit(1)
            if cand_chunks is not None:
                if len(cand_chunks) == 0:
                    return [], debug
                stmt = stmt.where(Keyword.chunk_id.in_(cand_chunks))
            if bool(list(pg.execute(stmt).all())):
                filtered.append(term)
        debug["after_scope_filter"] = filtered[:]
        return filtered, debug
    except Exception:
        return terms, debug


def _filter_gemini_terms_strict(base_query: str, gemini_terms: List[str]) -> List[str]:
    q = _norm_keyword_text(base_query)
    out: List[str] = []
    for term in gemini_terms or []:
        t = _norm_keyword_text(term)
        if not t:
            continue
        if t in q or q in t:
            out.append(term)
    return _dedupe_keep_order(out)


def _split_keyword_query_parts(raw_query: str, core_query: str) -> List[str]:
    raw = _strip_keyword_filler(raw_query or "")
    pieces = re.split(r"[,;\n]+|\b(?:va|và|hoặc|hay|and|or)\b", raw)
    parts = [_strip_keyword_filler(piece) for piece in pieces]
    parts = [part for part in parts if part]
    if parts:
        return _dedupe_keep_order(parts)
    core = _strip_keyword_filler(core_query or raw_query or "")
    return [core] if core else []


def _query_embedding_text(raw_query: str, core_query: str, gemini_terms: List[str]) -> str:
    parts: List[str] = []
    if core_query:
        parts.append(core_query)
    elif _norm_spaces(raw_query):
        parts.append(_norm_spaces(raw_query.lower()))
    parts.extend(gemini_terms or [])
    return _norm_spaces(" ".join(_dedupe_keep_order(parts)))


def _token_overlap_ratio(query_text: str, keyword_name: str) -> float:
    q_tokens = set(_tokens_no_stop(_strip_keyword_filler(query_text or "")))
    k_tokens = set(_tokens_no_stop(_strip_keyword_filler(keyword_name or "")))
    if not q_tokens or not k_tokens:
        return 0.0
    return float(len(q_tokens & k_tokens) / len(q_tokens))


def _score_keywords_for_query_part(
    query_text: str,
    query_embedding: List[float],
    rows: List[Tuple[str, str, str, List[float]]],
) -> Tuple[List[dict], float]:
    matches: List[dict] = []
    for keyword_id, chunk_id, keyword_name, keyword_embedding in rows:
        cosine = _cosine(query_embedding, keyword_embedding)
        overlap = _token_overlap_ratio(query_text, keyword_name)
        adjusted = float(cosine + 0.06 * overlap)
        matches.append({
            "keywordID": keyword_id,
            "chunkID": chunk_id,
            "keywordName": keyword_name,
            "score": adjusted,
            "cosine": float(cosine),
            "overlap": float(overlap),
            "matchedQueryPart": query_text,
        })
    matches.sort(key=lambda item: item["score"], reverse=True)
    if not matches:
        return [], 0.0
    top_score = float(matches[0]["score"])
    min_score = max(0.82, top_score * 0.96)
    filtered = [item for item in matches if float(item["score"]) >= min_score]
    if len(filtered) < 5:
        filtered = matches[: min(8, len(matches))]
    else:
        filtered = filtered[:20]
    return filtered, min_score


def _load_keyword_rows_from_neo(neo, cand_chunks: Optional[List[str]]) -> Tuple[List[Tuple[str, str, str, List[float]]], Optional[str]]:
    if neo is None:
        return [], "neo_session_unavailable"
    try:
        records = neo.run(
            """
            MATCH (chunk:Chunk)-[:HAS_KEYWORD]->(keyword:Keyword)
            WHERE keyword.embedding IS NOT NULL
              AND keyword.pg_id IS NOT NULL
              AND chunk.pg_id IS NOT NULL
              AND ($cand_chunks IS NULL OR chunk.pg_id IN $cand_chunks)
            RETURN keyword.pg_id AS keyword_id,
                   chunk.pg_id AS chunk_id,
                   coalesce(keyword.name, keyword.pg_id) AS keyword_name,
                   keyword.embedding AS keyword_embedding
            """,
            cand_chunks=cand_chunks,
        )
        rows: List[Tuple[str, str, str, List[float]]] = []
        for record in records:
            keyword_id = str(record.get("keyword_id") or "").strip()
            chunk_id = str(record.get("chunk_id") or "").strip()
            keyword_name = str(record.get("keyword_name") or "").strip()
            embedding = record.get("keyword_embedding")
            if not keyword_id or not chunk_id or not keyword_name or not embedding:
                continue
            try:
                rows.append((keyword_id, chunk_id, keyword_name, [float(x) for x in list(embedding)]))
            except Exception:
                continue
        return rows, None
    except Exception as exc:
        return [], str(exc)


def _load_keyword_rows_from_pg(pg: Session, cand_chunks: Optional[List[str]]) -> List[Tuple[str, str, str, List[float]]]:
    try:
        stmt = select(Keyword.keyword_id, Keyword.chunk_id, Keyword.keyword_name, Keyword.keyword_embedding).where(Keyword.keyword_embedding.isnot(None))
        if cand_chunks is not None:
            if len(cand_chunks) == 0:
                return []
            stmt = stmt.where(Keyword.chunk_id.in_(cand_chunks))
        return [(str(r[0]), str(r[1]), str(r[2]), list(r[3])) for r in pg.execute(stmt).all() if r[0] and r[1] and r[2] and r[3]]
    except SQLAlchemyError:
        return []


def _load_keyword_rows(neo, pg: Session, cand_chunks: Optional[List[str]]) -> Tuple[List[Tuple[str, str, str, List[float]]], str, Optional[str]]:
    neo_rows, neo_error = _load_keyword_rows_from_neo(neo, cand_chunks)
    if neo_rows:
        return neo_rows, "neo4j", neo_error
    pg_rows = _load_keyword_rows_from_pg(pg, cand_chunks)
    return pg_rows, "postgresql", neo_error


def _load_keyword_rows_by_map_ids_from_neo(
    neo,
    *,
    owner_label: str,
    map_ids: List[str],
) -> Tuple[List[Tuple[str, str, str, List[float]]], Optional[str]]:
    clean_map_ids = [str(mid).strip() for mid in map_ids if str(mid).strip()]
    if not clean_map_ids:
        return [], None
    if neo is None:
        return [], "neo_session_unavailable"
    if owner_label not in {"Subject", "Topic", "Lesson", "Chunk"}:
        return [], f"unsupported_owner_label:{owner_label}"
    try:
        records = neo.run(
            f"""
            MATCH (owner:{owner_label})-[:HAS_KEYWORD]->(keyword:Keyword)
            WHERE owner.pg_id IN $map_ids
              AND keyword.embedding IS NOT NULL
              AND keyword.pg_id IS NOT NULL
            RETURN keyword.pg_id AS keyword_id,
                   owner.pg_id AS map_id,
                   coalesce(keyword.name, keyword.pg_id) AS keyword_name,
                   keyword.embedding AS keyword_embedding
            """,
            map_ids=clean_map_ids,
        )
        rows: List[Tuple[str, str, str, List[float]]] = []
        for record in records:
            keyword_id = str(record.get("keyword_id") or "").strip()
            map_id = str(record.get("map_id") or "").strip()
            keyword_name = str(record.get("keyword_name") or "").strip()
            embedding = record.get("keyword_embedding")
            if not keyword_id or not map_id or not keyword_name or not embedding:
                continue
            try:
                rows.append((keyword_id, map_id, keyword_name, [float(x) for x in list(embedding)]))
            except Exception:
                continue
        return rows, None
    except Exception as exc:
        return [], str(exc)


def _load_keyword_rows_by_map_ids_pg(pg: Session, map_ids: List[str]) -> List[Tuple[str, str, str, List[float]]]:
    clean_map_ids = [str(mid).strip() for mid in map_ids if str(mid).strip()]
    if not clean_map_ids:
        return []
    try:
        stmt = (
            select(Keyword.keyword_id, Keyword.map_id, Keyword.keyword_name, Keyword.keyword_embedding)
            .where(Keyword.keyword_embedding.isnot(None))
            .where(Keyword.map_id.in_(clean_map_ids))
        )
        return [
            (str(r[0]), str(r[1]), str(r[2]), list(r[3]))
            for r in pg.execute(stmt).all()
            if r[0] and r[1] and r[2] and r[3]
        ]
    except SQLAlchemyError:
        return []


def _score_entity_keyword_rows(
    query_text: str,
    rows: List[Tuple[str, str, str, List[float]]],
) -> Tuple[List[str], Dict[str, float], Dict[str, List[Tuple[float, str]]], Dict[str, object]]:
    clean_query = _strip_keyword_filler(query_text or "")
    if not clean_query or not rows:
        return [], {}, {}, {"keyword_rows": len(rows)}

    query_embedding = embed_keyword_cached(clean_query)
    score_by_map: Dict[str, float] = {}
    matched_keywords: Dict[str, List[Tuple[float, str]]] = defaultdict(list)
    for keyword_id, map_id, keyword_name, keyword_embedding in rows:
        cosine = _cosine(query_embedding, keyword_embedding)
        overlap = _token_overlap_ratio(clean_query, keyword_name)
        exact_bonus = 0.0
        norm_q = _norm_keyword_text(clean_query)
        norm_kw = _norm_keyword_text(keyword_name)
        if norm_kw and norm_q:
            if norm_kw == norm_q:
                exact_bonus = 0.12
            elif norm_q in norm_kw or norm_kw in norm_q:
                exact_bonus = 0.08
        score = float(cosine + 0.08 * overlap + exact_bonus)
        current = score_by_map.get(map_id)
        if current is None or score > current:
            score_by_map[map_id] = score
        bucket = matched_keywords[map_id]
        if not any(existing_name == keyword_name for _existing_score, existing_name in bucket):
            bucket.append((score, keyword_name))
            bucket.sort(key=lambda item: item[0], reverse=True)
            matched_keywords[map_id] = bucket[:5]

    ranked = sorted(score_by_map.items(), key=lambda item: item[1], reverse=True)
    if not ranked:
        return [], score_by_map, matched_keywords, {"keyword_rows": len(rows)}

    top_score = float(ranked[0][1])
    min_score = max(0.72, top_score * 0.93)
    matched_ids = [map_id for map_id, score in ranked if float(score) >= min_score]
    if len(matched_ids) < 1 and ranked:
        matched_ids = [ranked[0][0]]
    matched_ids = matched_ids[:8]
    debug = {
        "keyword_rows": len(rows),
        "top_score": top_score,
        "min_score": min_score,
        "matched_count": len(matched_ids),
    }
    return matched_ids, score_by_map, matched_keywords, debug


def _exact_keyword_hits(keyword_query: str, rows: List[Tuple[str, str, str, List[float]]]) -> List[dict]:
    q = _norm_keyword_text(keyword_query)
    if not q:
        return []
    hits: List[dict] = []
    for keyword_id, chunk_id, keyword_name, _embedding in rows:
        nk = _norm_keyword_text(keyword_name)
        if not nk:
            continue
        score: Optional[float] = None
        if nk == q:
            score = 1.0
        elif q in nk:
            score = 0.97
        elif nk in q:
            score = 0.94
        if score is None:
            continue
        hits.append({
            "keywordID": keyword_id,
            "chunkID": chunk_id,
            "keywordName": keyword_name,
            "score": float(score),
            "matchType": "exact",
        })
    hits.sort(key=lambda item: item["score"], reverse=True)
    return hits


def _score_keywords(query_embedding: List[float], rows: List[Tuple[str, str, str, List[float]]]) -> Tuple[List[dict], float]:
    matches: List[dict] = []
    for keyword_id, chunk_id, keyword_name, keyword_embedding in rows:
        score = _cosine(query_embedding, keyword_embedding)
        matches.append({
            "keywordID": keyword_id,
            "chunkID": chunk_id,
            "keywordName": keyword_name,
            "score": float(score),
        })
    matches.sort(key=lambda item: item["score"], reverse=True)
    if not matches:
        return [], 0.0
    top_score = float(matches[0]["score"])
    min_score = max(0.82, top_score * 0.96)
    filtered = [item for item in matches if float(item["score"]) >= min_score]
    if len(filtered) < 5:
        filtered = matches[: min(8, len(matches))]
    else:
        filtered = filtered[:20]
    return filtered, min_score


def _rank_chunks_from_keyword_hits(keyword_hits: List[dict]) -> Tuple[List[Tuple[str, float]], Dict[str, List[Tuple[float, str]]]]:
    chunk_score: Dict[str, float] = {}
    chunk_keywords: Dict[str, List[Tuple[float, str]]] = defaultdict(list)
    for hit in keyword_hits:
        chunk_id = str(hit.get("chunkID") or "").strip()
        keyword_name = str(hit.get("keywordName") or "").strip()
        score = float(hit.get("score") or 0.0)
        if not chunk_id or not keyword_name:
            continue
        current = chunk_score.get(chunk_id)
        if current is None or score > current:
            chunk_score[chunk_id] = score
        bucket = chunk_keywords[chunk_id]
        if not any(existing_name == keyword_name for _existing_score, existing_name in bucket):
            bucket.append((score, keyword_name))
            bucket.sort(key=lambda x: x[0], reverse=True)
            chunk_keywords[chunk_id] = bucket[:5]
    ranked = sorted(chunk_score.items(), key=lambda item: item[1], reverse=True)
    return ranked, chunk_keywords


def _neo_hierarchy_for_chunks(neo, chunk_ids: List[str]) -> Tuple[Dict[str, dict], Optional[str]]:
    if neo is None or not chunk_ids:
        return {}, None
    try:
        records = neo.run(
            """
            UNWIND $chunk_ids AS chunk_id
            MATCH (chunk:Chunk {pg_id: chunk_id})
            OPTIONAL MATCH (lesson:Lesson)-[:HAS_CHUNK]->(chunk)
            OPTIONAL MATCH (topic:Topic)-[:HAS_LESSON]->(lesson)
            OPTIONAL MATCH (subject:Subject)-[:HAS_TOPIC]->(topic)
            OPTIONAL MATCH (class:Class)-[:HAS_SUBJECT]->(subject)
            RETURN chunk.pg_id AS chunk_id,
                   chunk.name AS chunk_name,
                   lesson.pg_id AS lesson_id,
                   lesson.name AS lesson_name,
                   topic.pg_id AS topic_id,
                   topic.name AS topic_name,
                   subject.pg_id AS subject_id,
                   subject.name AS subject_name,
                   class.pg_id AS class_id,
                   class.name AS class_name
            """,
            chunk_ids=chunk_ids,
        )
        out: Dict[str, dict] = {}
        for record in records:
            chunk_id = str(record.get("chunk_id") or "").strip()
            if not chunk_id:
                continue
            out[chunk_id] = {
                "chunkID": chunk_id,
                "chunkName": str(record.get("chunk_name") or "").strip(),
                "lesson": {"lessonID": str(record.get("lesson_id") or "").strip(), "lessonName": str(record.get("lesson_name") or "").strip()},
                "topic": {"topicID": str(record.get("topic_id") or "").strip(), "topicName": str(record.get("topic_name") or "").strip()},
                "subject": {"subjectID": str(record.get("subject_id") or "").strip(), "subjectName": str(record.get("subject_name") or "").strip()},
                "class": {"classID": str(record.get("class_id") or "").strip(), "className": str(record.get("class_name") or "").strip()},
            }
        return out, None
    except Exception as exc:
        return {}, str(exc)


def _load_pg_page_rows(pg: Session, chunk_ids: List[str]) -> Dict[str, dict]:
    if not chunk_ids:
        return {}
    try:
        stmt = (
            select(
                Chunk.chunk_id,
                Chunk.chunk_name,
                Chunk.chunk_type,
                Chunk.chunk_number,
                Chunk.mongo_id,
                Lesson.lesson_id,
                Lesson.lesson_name,
                Lesson.lesson_number,
                Lesson.mongo_id,
                Topic.topic_id,
                Topic.topic_name,
                Topic.topic_number,
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
            .where(Chunk.chunk_id.in_(chunk_ids))
        )
        rows = list(pg.execute(stmt).all())
    except SQLAlchemyError:
        return {}
    out: Dict[str, dict] = {}
    for r in rows:
        out[str(r[0])] = {
            "chunkID": str(r[0]), "chunkName": r[1], "chunkType": r[2], "chunkNumber": r[3], "chunkMongoId": r[4],
            "lesson": {"lessonID": r[5], "lessonName": r[6], "lessonNumber": r[7], "mongoId": r[8]},
            "topic": {"topicID": r[9], "topicName": r[10], "topicNumber": r[11], "mongoId": r[12]},
            "subject": {"subjectID": r[13], "subjectName": r[14], "mongoId": r[15]},
            "class": {"classID": r[16], "className": r[17], "mongoId": r[18]},
        }
    return out


# --------------------------- response builders ---------------------------

def _build_topic_items(rows: List[dict]) -> List[dict]:
    items: List[dict] = []
    seen = set()
    for row in rows:
        topic_id = str(row.get("topicID") or "")
        if not topic_id or topic_id in seen:
            continue
        seen.add(topic_id)
        items.append({
            "type": "topic",
            "id": topic_id,
            "name": row.get("topicName") or topic_id,
            "score": 1,
            "topicID": topic_id,
            "topicName": row.get("topicName") or topic_id,
            "topicNumber": row.get("topicNumber"),
            "class": {"classID": row.get("classID") or "", "className": row.get("className") or ""},
            "subject": {"subjectID": row.get("subjectID") or "", "subjectName": row.get("subjectName") or ""},
        })
    return items


def _build_lesson_items(rows: List[dict]) -> List[dict]:
    items: List[dict] = []
    seen = set()
    for row in rows:
        lesson_id = str(row.get("lessonID") or "")
        if not lesson_id or lesson_id in seen:
            continue
        seen.add(lesson_id)
        items.append({
            "type": "lesson",
            "id": lesson_id,
            "name": row.get("lessonName") or lesson_id,
            "score": 1,
            "lessonID": lesson_id,
            "lessonName": row.get("lessonName") or lesson_id,
            "lessonNumber": row.get("lessonNumber"),
            "class": {"classID": row.get("classID") or "", "className": row.get("className") or ""},
            "subject": {"subjectID": row.get("subjectID") or "", "subjectName": row.get("subjectName") or ""},
            "topic": {"topicID": row.get("topicID") or "", "topicName": row.get("topicName") or "", "topicNumber": row.get("topicNumber")},
        })
    return items


def _build_chunk_items(
    *,
    page_chunk_ids: List[str],
    score_by_chunk: Dict[str, float],
    chunk_top_kw: Dict[str, List[Tuple[float, str]]],
    pg_map: Dict[str, dict],
    neo_map: Dict[str, dict],
    mongo_db,
    category: str,
    username: str,
    pg: Session,
    dbg: Dict[str, object],
) -> List[dict]:
    chunk_mongo_hex: List[str] = []
    lesson_mongo_hex: List[str] = []
    topic_mongo_hex: List[str] = []
    subject_mongo_hex: List[str] = []
    for base in pg_map.values():
        if _valid_object_id_hex(base.get("chunkMongoId") or ""):
            chunk_mongo_hex.append(base["chunkMongoId"])
        if _valid_object_id_hex((base.get("lesson") or {}).get("mongoId") or ""):
            lesson_mongo_hex.append(base["lesson"]["mongoId"])
        if _valid_object_id_hex((base.get("topic") or {}).get("mongoId") or ""):
            topic_mongo_hex.append(base["topic"]["mongoId"])
        if _valid_object_id_hex((base.get("subject") or {}).get("mongoId") or ""):
            subject_mongo_hex.append(base["subject"]["mongoId"])
    mongo_chunks_by_oid = _load_by_oids(mongo_db, "chunks", chunk_mongo_hex)
    mongo_lessons_by_oid = _load_by_oids(mongo_db, "lessons", lesson_mongo_hex)
    mongo_topics_by_oid = _load_by_oids(mongo_db, "topics", topic_mongo_hex)
    mongo_subjects_by_oid = _load_by_oids(mongo_db, "subjects", subject_mongo_hex)

    media_targets: List[tuple[str, str]] = []
    chunk_targets_by_chunk: Dict[str, List[tuple[str, str]]] = {}
    for chunk_id in page_chunk_ids:
        pg_base = pg_map.get(chunk_id) or {}
        neo_base = neo_map.get(chunk_id) or {}
        lesson_id_v = (neo_base.get("lesson") or {}).get("lessonID") or (pg_base.get("lesson") or {}).get("lessonID") or ""
        topic_id_v = (neo_base.get("topic") or {}).get("topicID") or (pg_base.get("topic") or {}).get("topicID") or ""
        subject_id_v = (neo_base.get("subject") or {}).get("subjectID") or (pg_base.get("subject") or {}).get("subjectID") or ""
        targets = [("chunk", chunk_id), ("lesson", lesson_id_v), ("topic", topic_id_v), ("subject", subject_id_v)]
        norm_targets = [(ft, fid) for ft, fid in targets if ft and fid]
        chunk_targets_by_chunk[chunk_id] = norm_targets
        media_targets.extend(norm_targets)
    media_map = _load_media_map_for_targets(pg=pg, mongo_db=mongo_db, targets=media_targets)
    dbg["media_hit_groups"] = len(media_map)

    items: List[dict] = []
    for chunk_id in page_chunk_ids:
        pg_base = pg_map.get(chunk_id)
        if not pg_base:
            continue
        neo_base = neo_map.get(chunk_id) or {}
        chunk_doc = None
        if _valid_object_id_hex(pg_base.get("chunkMongoId") or ""):
            chunk_doc = mongo_chunks_by_oid.get(pg_base["chunkMongoId"])
        if chunk_doc and not _status_visible(chunk_doc):
            continue
        if category and category != "all":
            chunk_category = (chunk_doc or {}).get("chunkCategory") or ""
            if chunk_category and chunk_category != category:
                continue

        lesson_oid = (pg_base.get("lesson") or {}).get("mongoId") or ""
        topic_oid = (pg_base.get("topic") or {}).get("mongoId") or ""
        subject_oid = (pg_base.get("subject") or {}).get("mongoId") or ""
        lesson_doc = mongo_lessons_by_oid.get(lesson_oid) if _valid_object_id_hex(lesson_oid) else None
        topic_doc = mongo_topics_by_oid.get(topic_oid) if _valid_object_id_hex(topic_oid) else None
        subject_doc = mongo_subjects_by_oid.get(subject_oid) if _valid_object_id_hex(subject_oid) else None

        lesson_name = (neo_base.get("lesson") or {}).get("lessonName") or (pg_base.get("lesson") or {}).get("lessonName") or ""
        topic_name = (neo_base.get("topic") or {}).get("topicName") or (pg_base.get("topic") or {}).get("topicName") or ""
        subject_name = (neo_base.get("subject") or {}).get("subjectName") or (pg_base.get("subject") or {}).get("subjectName") or ""
        class_name = (neo_base.get("class") or {}).get("className") or (pg_base.get("class") or {}).get("className") or ""
        lesson_id_v = (neo_base.get("lesson") or {}).get("lessonID") or (pg_base.get("lesson") or {}).get("lessonID") or ""
        topic_id_v = (neo_base.get("topic") or {}).get("topicID") or (pg_base.get("topic") or {}).get("topicID") or ""
        subject_id_v = (neo_base.get("subject") or {}).get("subjectID") or (pg_base.get("subject") or {}).get("subjectID") or ""
        class_id_v = (neo_base.get("class") or {}).get("classID") or (pg_base.get("class") or {}).get("classID") or ""

        images: List[dict] = []
        videos: List[dict] = []
        media_sources = {"chunk": 0, "lesson": 0, "topic": 0, "subject": 0}
        for follow_type, follow_id in chunk_targets_by_chunk.get(chunk_id, []):
            bucket = media_map.get((follow_type, follow_id)) or {}
            part_images = bucket.get("images") or []
            part_videos = bucket.get("videos") or []
            if part_images or part_videos:
                media_sources[follow_type] = len(part_images) + len(part_videos)
            images.extend(part_images)
            videos.extend(part_videos)
        images.sort(key=_media_sort_key)
        videos.sort(key=_media_sort_key)

        matched_kw = [name for _score, name in chunk_top_kw.get(chunk_id, [])]
        item = {
            "type": "chunk",
            "id": chunk_id,
            "name": (neo_base.get("chunkName") or pg_base.get("chunkName") or (chunk_doc or {}).get("chunkName") or chunk_id),
            "score": float(score_by_chunk.get(chunk_id, 0.0)),
            "chunkID": chunk_id,
            "chunkName": (chunk_doc.get("chunkName") if chunk_doc else None) or neo_base.get("chunkName") or pg_base.get("chunkName"),
            "chunkType": (chunk_doc.get("chunkType") if chunk_doc else None) or pg_base.get("chunkType"),
            "chunkNumber": pg_base.get("chunkNumber"),
            "chunkUrl": (chunk_doc.get("chunkUrl") if chunk_doc else None),
            "chunkDescription": (chunk_doc.get("chunkDescription") if chunk_doc else None),
            "keywords": _read_keywords_from_chunk_doc(chunk_doc),
            "matchedKeywords": matched_kw,
            "images": images,
            "videos": videos,
            "mediaSummary": {"totalImages": len(images), "totalVideos": len(videos), "byFollowType": media_sources},
            "isSaved": False,
            "class": {"classID": class_id_v, "className": class_name},
            "subject": {
                "subjectID": subject_id_v,
                "subjectName": subject_name,
                "subjectDescription": ((subject_doc.get("subjectDescription") if subject_doc else None) or (subject_doc.get("subjectTitle") if subject_doc else None) or (subject_doc.get("description") if subject_doc else None) or ""),
                "subjectUrl": (subject_doc.get("subjectUrl") if subject_doc and _status_visible(subject_doc) else ""),
            },
            "topic": {
                "topicID": topic_id_v,
                "topicName": topic_name,
                "topicNumber": (pg_base.get("topic") or {}).get("topicNumber"),
                "topicDescription": ((topic_doc.get("topicDescription") if topic_doc else None) or (topic_doc.get("topic_description") if topic_doc else None) or (topic_doc.get("description") if topic_doc else None) or ""),
                "topicUrl": (topic_doc.get("topicUrl") if topic_doc and _status_visible(topic_doc) else ""),
            },
            "lesson": {
                "lessonID": lesson_id_v,
                "lessonName": lesson_name,
                "lessonNumber": (pg_base.get("lesson") or {}).get("lessonNumber"),
                "lessonDescription": ((lesson_doc.get("lessonDescription") if lesson_doc else None) or (lesson_doc.get("lesson_description") if lesson_doc else None) or (lesson_doc.get("description") if lesson_doc else None) or (lesson_doc.get("lessonType") if lesson_doc else None) or ""),
                "lessonUrl": (lesson_doc.get("lessonUrl") if lesson_doc and _status_visible(lesson_doc) else ""),
                "lessonType": (lesson_doc.get("lessonType") if lesson_doc else None) or "",
            },
            "category": (chunk_doc.get("chunkCategory") if chunk_doc else None) or category or "document",
        }
        try:
            if mongo_db is not None:
                saved = mongo_db["user_saved_chunks"].find_one({"username": username, "chunkID": chunk_id})
                item["isSaved"] = bool(saved)
        except Exception:
            pass
        items.append(item)
    return items


def _pick_return_mode(ctx: dict, *, topicID: str, lessonID: str) -> str:
    if ctx.get("chunkNumber") is not None or str(ctx.get("chunkNameHint") or ""):
        return "chunk"
    if lessonID or ctx.get("lessonNumber") is not None or str(ctx.get("lessonNameHint") or ""):
        return "lesson"
    if topicID or ctx.get("topicNumber") is not None or str(ctx.get("topicNameHint") or ""):
        return "topic"
    if ctx.get("classNumber") is not None:
        return "class"
    return "topic"


# --------------------------- main entry ---------------------------

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
    query = _norm_spaces(q)
    if not query:
        return {"total": 0, "items": []}

    dbg: Dict[str, object] = {"service_version": _SERVICE_VERSION, "category": category}

    ctx = _parse_query_context(query)
    dbg["query_context"] = ctx
    class_scope = _normalize_class_scope(classID, ctx.get("classNumber"))

    topic_number = None if topicID else ctx.get("topicNumber")
    lesson_number = None if lessonID else ctx.get("lessonNumber")
    chunk_number = ctx.get("chunkNumber")

    topic_rows = _load_topic_rows_pg(pg=pg, class_id=class_scope, subject_id=subjectID, topic_id=topicID)
    dbg["topic_rows_before_number"] = len(topic_rows)
    topic_rows = _filter_by_number(topic_rows, "topicNumber", topic_number)
    dbg["topic_rows_after_number"] = len(topic_rows)
    topic_ids, _topic_score_map, topic_dbg = _resolve_scope_by_name(
        rows=topic_rows,
        id_key="topicID",
        name_hint=str(ctx.get("topicNameHint") or ""),
        neo=neo,
        label="Topic",
        embedding_field="topic_embedding",
    )
    dbg["topic_scope"] = topic_dbg
    topic_scope_active = bool(topicID or topic_number is not None or str(ctx.get("topicNameHint") or ""))
    if topic_scope_active and not topic_ids:
        res = {"total": 0, "items": []}
        if debug:
            res["debug"] = dbg
        return res

    lessons_rows = _load_lesson_rows_pg(
        pg=pg,
        class_id=class_scope,
        subject_id=subjectID,
        topic_ids=topic_ids if topic_scope_active else None,
        lesson_id=lessonID,
    )
    dbg["lesson_rows_before_number"] = len(lessons_rows)
    lessons_rows = _filter_by_number(lessons_rows, "lessonNumber", lesson_number)
    dbg["lesson_rows_after_number"] = len(lessons_rows)
    lesson_ids, _lesson_score_map, lesson_dbg = _resolve_scope_by_name(
        rows=lessons_rows,
        id_key="lessonID",
        name_hint=str(ctx.get("lessonNameHint") or ""),
        neo=neo,
        label="Lesson",
        embedding_field="lesson_embedding",
    )
    dbg["lesson_scope"] = lesson_dbg
    lesson_scope_active = bool(lessonID or lesson_number is not None or str(ctx.get("lessonNameHint") or ""))
    if lesson_scope_active and not lesson_ids:
        res = {"total": 0, "items": []}
        if debug:
            res["debug"] = dbg
        return res

    chunk_rows = _load_chunk_rows_pg(
        pg=pg,
        class_id=class_scope,
        subject_id=subjectID,
        topic_ids=topic_ids if topic_scope_active else None,
        lesson_ids=lesson_ids if lesson_scope_active else None,
    )
    dbg["chunk_rows_before_number"] = len(chunk_rows)
    chunk_rows = _filter_by_number(chunk_rows, "chunkNumber", chunk_number)
    dbg["chunk_rows_after_number"] = len(chunk_rows)
    explicit_chunk_ids, _explicit_chunk_score_map, chunk_dbg = _resolve_scope_by_name(
        rows=chunk_rows,
        id_key="chunkID",
        name_hint=str(ctx.get("chunkNameHint") or ""),
        neo=neo,
        label="Chunk",
        embedding_field="chunk_embedding",
    )
    dbg["chunk_scope"] = chunk_dbg
    chunk_scope_active = bool(chunk_number is not None or str(ctx.get("chunkNameHint") or ""))
    if chunk_scope_active and not explicit_chunk_ids:
        res = {"total": 0, "items": []}
        if debug:
            res["debug"] = dbg
        return res
    if chunk_scope_active:
        chunk_rows = [row for row in chunk_rows if str(row.get("chunkID") or "") in set(explicit_chunk_ids)]

    generic_query = str(ctx.get("genericQuery") or "")
    dbg["generic_query"] = generic_query
    is_keyword_search = bool(generic_query)
    dbg["is_keyword_search"] = is_keyword_search

    # structured-only path returns the lowest specified level
    if not is_keyword_search:
        return_mode = _pick_return_mode(ctx, topicID=topicID, lessonID=lessonID)
        dbg["return_mode"] = f"structured_{return_mode}"
        if return_mode == "chunk":
            total = len(chunk_rows)
            page_rows = chunk_rows[offset : offset + limit]
            page_chunk_ids = [str(row.get("chunkID") or "") for row in page_rows]
            score_by_chunk = {cid: 1.0 for cid in page_chunk_ids}
            chunk_top_kw: Dict[str, List[Tuple[float, str]]] = {}
            neo_map, neo_error = _neo_hierarchy_for_chunks(neo, page_chunk_ids)
            if neo_error:
                dbg["neo_error"] = neo_error
            pg_map = _load_pg_page_rows(pg, page_chunk_ids)
            items = _build_chunk_items(
                page_chunk_ids=page_chunk_ids,
                score_by_chunk=score_by_chunk,
                chunk_top_kw=chunk_top_kw,
                pg_map=pg_map,
                neo_map=neo_map,
                mongo_db=mongo_db,
                category=category,
                username=username,
                pg=pg,
                dbg=dbg,
            )
            res = {"total": total, "items": items}
            if debug:
                dbg["items_built"] = len(items)
                res["debug"] = dbg
            return res
        if return_mode == "lesson":
            total = len(lessons_rows)
            items = _build_lesson_items(lessons_rows[offset : offset + limit])
            res = {"total": total, "items": items}
            if debug:
                res["debug"] = dbg
            return res
        total = len(topic_rows)
        items = _build_topic_items(topic_rows[offset : offset + limit])
        res = {"total": total, "items": items}
        if debug:
            res["debug"] = dbg
        return res

    # keyword path: subject -> topic -> lesson -> chunk
    keyword_query = _strip_keyword_filler(generic_query)
    dbg["keyword_query"] = keyword_query
    query_parts = _split_keyword_query_parts(ctx.get("raw") or query, keyword_query or generic_query)
    dbg["keyword_query_parts"] = query_parts

    filtered_chunk_rows = list(chunk_rows)
    hierarchy_dbg: Dict[str, object] = {}

    current_subject_ids = _dedupe_keep_order([str(row.get("subjectID") or "") for row in filtered_chunk_rows if str(row.get("subjectID") or "")])
    current_topic_ids = _dedupe_keep_order([str(row.get("topicID") or "") for row in filtered_chunk_rows if str(row.get("topicID") or "")])
    current_lesson_ids = _dedupe_keep_order([str(row.get("lessonID") or "") for row in filtered_chunk_rows if str(row.get("lessonID") or "")])

    subject_keyword_rows, subject_keyword_error = _load_keyword_rows_by_map_ids_from_neo(neo, owner_label="Subject", map_ids=current_subject_ids)
    hierarchy_dbg["subject_keyword_rows"] = len(subject_keyword_rows)
    hierarchy_dbg["subject_keyword_source"] = "neo4j"
    if subject_keyword_error:
        hierarchy_dbg["subject_keyword_error"] = subject_keyword_error
    if not subjectID and len(current_subject_ids) > 1 and subject_keyword_rows:
        matched_subject_ids, subject_scores, subject_kw, subject_match_dbg = _score_entity_keyword_rows(keyword_query or generic_query or query, subject_keyword_rows)
        hierarchy_dbg["subject_match"] = subject_match_dbg
        hierarchy_dbg["subject_matched_ids"] = matched_subject_ids
        if not matched_subject_ids:
            res = {"total": 0, "items": []}
            if debug:
                dbg["hierarchy_keyword_filter"] = hierarchy_dbg
                res["debug"] = dbg
            return res
        filtered_chunk_rows = [row for row in filtered_chunk_rows if str(row.get("subjectID") or "") in set(matched_subject_ids)]
        current_topic_ids = _dedupe_keep_order([str(row.get("topicID") or "") for row in filtered_chunk_rows if str(row.get("topicID") or "")])
        current_lesson_ids = _dedupe_keep_order([str(row.get("lessonID") or "") for row in filtered_chunk_rows if str(row.get("lessonID") or "")])

    topic_keyword_rows, topic_keyword_error = _load_keyword_rows_by_map_ids_from_neo(neo, owner_label="Topic", map_ids=current_topic_ids)
    hierarchy_dbg["topic_keyword_rows"] = len(topic_keyword_rows)
    hierarchy_dbg["topic_keyword_source"] = "neo4j"
    if topic_keyword_error:
        hierarchy_dbg["topic_keyword_error"] = topic_keyword_error
    if not topic_scope_active and len(current_topic_ids) > 1 and topic_keyword_rows:
        matched_topic_ids, topic_scores, topic_kw, topic_match_dbg = _score_entity_keyword_rows(keyword_query or generic_query or query, topic_keyword_rows)
        hierarchy_dbg["topic_match"] = topic_match_dbg
        hierarchy_dbg["topic_matched_ids"] = matched_topic_ids
        if not matched_topic_ids:
            res = {"total": 0, "items": []}
            if debug:
                dbg["hierarchy_keyword_filter"] = hierarchy_dbg
                res["debug"] = dbg
            return res
        filtered_chunk_rows = [row for row in filtered_chunk_rows if str(row.get("topicID") or "") in set(matched_topic_ids)]
        current_lesson_ids = _dedupe_keep_order([str(row.get("lessonID") or "") for row in filtered_chunk_rows if str(row.get("lessonID") or "")])

    lesson_keyword_rows, lesson_keyword_error = _load_keyword_rows_by_map_ids_from_neo(neo, owner_label="Lesson", map_ids=current_lesson_ids)
    hierarchy_dbg["lesson_keyword_rows"] = len(lesson_keyword_rows)
    hierarchy_dbg["lesson_keyword_source"] = "neo4j"
    if lesson_keyword_error:
        hierarchy_dbg["lesson_keyword_error"] = lesson_keyword_error
    if not lesson_scope_active and len(current_lesson_ids) > 1 and lesson_keyword_rows:
        matched_lesson_ids, lesson_scores, lesson_kw, lesson_match_dbg = _score_entity_keyword_rows(keyword_query or generic_query or query, lesson_keyword_rows)
        hierarchy_dbg["lesson_match"] = lesson_match_dbg
        hierarchy_dbg["lesson_matched_ids"] = matched_lesson_ids
        if not matched_lesson_ids:
            res = {"total": 0, "items": []}
            if debug:
                dbg["hierarchy_keyword_filter"] = hierarchy_dbg
                res["debug"] = dbg
            return res
        filtered_chunk_rows = [row for row in filtered_chunk_rows if str(row.get("lessonID") or "") in set(matched_lesson_ids)]

    chunk_ids = [str(row.get("chunkID") or "") for row in filtered_chunk_rows if str(row.get("chunkID") or "")]
    dbg["candidate_chunk_scope"] = len(chunk_ids)
    dbg["hierarchy_keyword_filter"] = hierarchy_dbg
    if not chunk_ids:
        res = {"total": 0, "items": []}
        if debug:
            res["debug"] = dbg
        return res

    gemini_terms: List[str] = []
    gem_dbg: dict = {}
    if len(query_parts) <= 1:
        gemini_terms, gem_dbg = _maybe_expand_with_gemini(keyword_query or generic_query or query, chunk_ids, pg)
        gemini_terms = _filter_gemini_terms_strict(keyword_query or generic_query, gemini_terms)
    dbg["gemini_terms_before_scope_filter"] = gem_dbg.get("before_scope_filter") if gem_dbg else []
    dbg["gemini_terms_after_scope_filter"] = gem_dbg.get("after_scope_filter") if gem_dbg else []
    dbg["gemini_model"] = gem_dbg.get("model") if gem_dbg else None
    dbg["gemini_mode"] = gem_dbg.get("mode") if gem_dbg else None
    dbg["gemini_terms_after_strict_filter"] = gemini_terms

    keyword_rows, keyword_rows_error = _load_keyword_rows_by_map_ids_from_neo(neo, owner_label="Chunk", map_ids=chunk_ids)
    dbg["keyword_rows"] = len(keyword_rows)
    dbg["keyword_embedding_source"] = "neo4j_map_id"
    if keyword_rows_error:
        dbg["keyword_embedding_error"] = keyword_rows_error
    if not keyword_rows:
        res = {"total": 0, "items": []}
        if debug:
            res["debug"] = dbg
        return res

    part_hits: List[dict] = []
    part_min_scores: Dict[str, float] = {}
    semantic_queries: List[str] = []
    parts_to_use = query_parts or [keyword_query or generic_query or query]
    for part in parts_to_use:
        semantic_part = _query_embedding_text(part, part, gemini_terms if len(parts_to_use) == 1 else [])
        semantic_queries.append(semantic_part)
        query_embedding = embed_keyword_cached(semantic_part)
        hits_for_part, min_score = _score_keywords_for_query_part(part, query_embedding, keyword_rows)
        part_min_scores[part] = min_score
        part_hits.extend(hits_for_part)

    if not part_hits:
        semantic_query = _query_embedding_text(query, keyword_query or generic_query, gemini_terms)
        dbg["semantic_query"] = semantic_query
        query_embedding = embed_keyword_cached(semantic_query)
        keyword_hits, min_score = _score_keywords(query_embedding, keyword_rows)
        dbg["keyword_match_mode"] = "semantic_only_fallback"
        dbg["keyword_min_score"] = min_score
    else:
        merged: Dict[tuple, dict] = {}
        for hit in part_hits:
            key = (str(hit.get("keywordID") or ""), str(hit.get("chunkID") or ""))
            prev = merged.get(key)
            if prev is None or float(hit.get("score") or 0.0) > float(prev.get("score") or 0.0):
                merged[key] = dict(hit)
        keyword_hits = sorted(merged.values(), key=lambda item: float(item.get("score") or 0.0), reverse=True)
        keyword_hits = keyword_hits[:60]
        semantic_query = " | ".join([q for q in semantic_queries if q])
        dbg["keyword_match_mode"] = "semantic_only_multi_part"
        dbg["keyword_min_score"] = part_min_scores
    dbg["semantic_query"] = semantic_query
    dbg["keyword_hit_count"] = len(keyword_hits)
    if not keyword_hits:
        res = {"total": 0, "items": []}
        if debug:
            res["debug"] = dbg
        return res

    ranked_chunks, chunk_top_kw = _rank_chunks_from_keyword_hits(keyword_hits)
    dbg["ranked_chunk_count"] = len(ranked_chunks)
    if not ranked_chunks:
        res = {"total": 0, "items": []}
        if debug:
            res["debug"] = dbg
        return res

    total = len(ranked_chunks)
    page_pairs = ranked_chunks[offset : offset + limit]
    page_chunk_ids = [chunk_id for chunk_id, _score in page_pairs]
    score_by_chunk = {chunk_id: float(score) for chunk_id, score in page_pairs}

    neo_map, neo_error = _neo_hierarchy_for_chunks(neo, page_chunk_ids)
    dbg["hierarchy_source"] = "neo4j" if neo_map else "postgresql"
    if neo_error:
        dbg["neo_error"] = neo_error
    pg_map = _load_pg_page_rows(pg, page_chunk_ids)
    dbg["pg_chunk_rows"] = len(pg_map)

    items = _build_chunk_items(
        page_chunk_ids=page_chunk_ids,
        score_by_chunk=score_by_chunk,
        chunk_top_kw=chunk_top_kw,
        pg_map=pg_map,
        neo_map=neo_map,
        mongo_db=mongo_db,
        category=category,
        username=username,
        pg=pg,
        dbg=dbg,
    )

    res = {"total": total, "items": items}
    if debug:
        dbg["items_built"] = len(items)
        if keyword_hits:
            dbg["top_keyword_hits"] = keyword_hits[:10]
        if items:
            dbg["sample_item_match"] = {
                "chunkID": items[0].get("chunkID"),
                "matchedKeywords": items[0].get("matchedKeywords"),
                "score": items[0].get("score"),
            }
        res["debug"] = dbg
    return res
