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
_SERVICE_VERSION = "search_hierarchical_keyword_neo4j_branch_gate_v7_exact_pg_id"
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


def _dedupe_keep_order_text(values: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        clean = _norm_spaces(str(value or "").lower())
        if not clean or clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def _dedupe_keep_order_ids(values: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        clean = _norm_spaces(str(value or ""))
        if not clean or clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def _collect_ids_keep_case(rows: List[dict], key: str) -> List[str]:
    return _dedupe_keep_order_ids([
        str(row.get(key) or "").strip()
        for row in rows
        if str(row.get(key) or "").strip()
    ])


# Backward-compatible alias for text dedupe usage.
def _dedupe_keep_order(values: List[str]) -> List[str]:
    return _dedupe_keep_order_text(values)


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


def _entity_alias_key(*values: str) -> str:
    for value in values:
        clean = str(value or "").strip()
        if clean:
            return clean.upper()
    return ""


def _alias_list_for_value(value: str) -> List[str]:
    key = _entity_alias_key(value)
    return [key] if key else []


def _alias_map_for_rows(rows: List[dict], *, id_key: str, name_key: str) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    alias_by_id: Dict[str, str] = {}
    ids_by_alias: Dict[str, List[str]] = defaultdict(list)
    for row in rows:
        row_id = str(row.get(id_key) or "").strip()
        if not row_id:
            continue
        alias = _entity_alias_key(row_id)
        alias_by_id[row_id] = alias
        bucket = ids_by_alias.setdefault(alias, [])
        if row_id not in bucket:
            bucket.append(row_id)
    return alias_by_id, ids_by_alias


def _filter_rows_by_alias(rows: List[dict], *, id_key: str, name_key: str, allowed_aliases: set[str]) -> List[dict]:
    if not allowed_aliases:
        return []
    out: List[dict] = []
    for row in rows:
        row_id = str(row.get(id_key) or "").strip()
        alias = _entity_alias_key(row_id)
        if alias in allowed_aliases:
            out.append(row)
    return out


def _expand_ids_for_aliases(ids_by_alias: Dict[str, List[str]], aliases: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for alias in aliases:
        for row_id in ids_by_alias.get(alias, []):
            if row_id and row_id not in seen:
                seen.add(row_id)
                out.append(row_id)
    return out


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


# kiểm tra và tách lớp / chủ đề / bài / chunk ra khỏi câu hỏi, lấy class bao nhiêu ,......

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


def _load_topic_rows_neo(*, neo, class_id: str, subject_id: str, topic_id: str) -> Tuple[List[dict], Optional[str]]:
    if neo is None:
        return [], "neo_session_unavailable"
    try:
        records = neo.run(
            """
            MATCH (class:Class)-[:HAS_SUBJECT]->(subject:Subject)-[:HAS_TOPIC]->(topic:Topic)
            WHERE ($class_id = '' OR class.pg_id = $class_id)
              AND ($subject_id = '' OR subject.pg_id = $subject_id)
              AND ($topic_id = '' OR topic.pg_id = $topic_id)
            RETURN topic.pg_id AS topic_id,
                   coalesce(topic.topic_name, topic.name, topic.pg_id) AS topic_name,
                   topic.topic_number AS topic_number,
                   subject.pg_id AS subject_id,
                   coalesce(subject.name, subject.pg_id) AS subject_name,
                   class.pg_id AS class_id,
                   coalesce(class.name, class.pg_id) AS class_name
            ORDER BY class.pg_id, subject.pg_id, topic.topic_number, topic.pg_id
            """,
            class_id=(class_id or "").strip(),
            subject_id=(subject_id or "").strip(),
            topic_id=(topic_id or "").strip(),
        )
        rows: List[dict] = []
        for r in records:
            topic_pg_id = str(r.get("topic_id") or "").strip()
            if not topic_pg_id:
                continue
            rows.append({
                "topicID": topic_pg_id,
                "topicName": str(r.get("topic_name") or topic_pg_id).strip(),
                "topicNumber": r.get("topic_number"),
                "subjectID": str(r.get("subject_id") or "").strip(),
                "subjectName": str(r.get("subject_name") or "").strip(),
                "classID": str(r.get("class_id") or "").strip(),
                "className": str(r.get("class_name") or "").strip(),
            })
        return rows, None
    except Exception as exc:
        return [], str(exc)


def _load_lesson_rows_neo(*, neo, class_id: str, subject_id: str, topic_ids: Optional[List[str]], lesson_id: str) -> Tuple[List[dict], Optional[str]]:
    if neo is None:
        return [], "neo_session_unavailable"
    clean_topic_ids = _dedupe_keep_order_ids(topic_ids or []) if topic_ids is not None else None
    if topic_ids is not None and clean_topic_ids is not None and not clean_topic_ids:
        return [], None
    try:
        records = neo.run(
            """
            MATCH (class:Class)-[:HAS_SUBJECT]->(subject:Subject)-[:HAS_TOPIC]->(topic:Topic)-[:HAS_LESSON]->(lesson:Lesson)
            WHERE ($class_id = '' OR class.pg_id = $class_id)
              AND ($subject_id = '' OR subject.pg_id = $subject_id)
              AND ($topic_ids IS NULL OR size($topic_ids) = 0 OR topic.pg_id IN $topic_ids)
              AND ($lesson_id = '' OR lesson.pg_id = $lesson_id)
            RETURN lesson.pg_id AS lesson_id,
                   coalesce(lesson.lesson_name, lesson.name, lesson.pg_id) AS lesson_name,
                   lesson.lesson_number AS lesson_number,
                   topic.pg_id AS topic_id,
                   coalesce(topic.topic_name, topic.name, topic.pg_id) AS topic_name,
                   topic.topic_number AS topic_number,
                   subject.pg_id AS subject_id,
                   coalesce(subject.name, subject.pg_id) AS subject_name,
                   class.pg_id AS class_id,
                   coalesce(class.name, class.pg_id) AS class_name
            ORDER BY class.pg_id, subject.pg_id, topic.topic_number, topic.pg_id, lesson.lesson_number, lesson.pg_id
            """,
            class_id=(class_id or "").strip(),
            subject_id=(subject_id or "").strip(),
            topic_ids=clean_topic_ids,
            lesson_id=(lesson_id or "").strip(),
        )
        rows: List[dict] = []
        for r in records:
            lesson_pg_id = str(r.get("lesson_id") or "").strip()
            if not lesson_pg_id:
                continue
            rows.append({
                "lessonID": lesson_pg_id,
                "lessonName": str(r.get("lesson_name") or lesson_pg_id).strip(),
                "lessonNumber": r.get("lesson_number"),
                "topicID": str(r.get("topic_id") or "").strip(),
                "topicName": str(r.get("topic_name") or "").strip(),
                "topicNumber": r.get("topic_number"),
                "subjectID": str(r.get("subject_id") or "").strip(),
                "subjectName": str(r.get("subject_name") or "").strip(),
                "classID": str(r.get("class_id") or "").strip(),
                "className": str(r.get("class_name") or "").strip(),
            })
        return rows, None
    except Exception as exc:
        return [], str(exc)


def _load_chunk_rows_neo(*, neo, class_id: str, subject_id: str, topic_ids: Optional[List[str]], lesson_ids: Optional[List[str]]) -> Tuple[List[dict], Optional[str]]:
    if neo is None:
        return [], "neo_session_unavailable"
    clean_topic_ids = _dedupe_keep_order_ids(topic_ids or []) if topic_ids is not None else None
    clean_lesson_ids = _dedupe_keep_order_ids(lesson_ids or []) if lesson_ids is not None else None
    if topic_ids is not None and clean_topic_ids is not None and not clean_topic_ids:
        return [], None
    if lesson_ids is not None and clean_lesson_ids is not None and not clean_lesson_ids:
        return [], None
    try:
        records = neo.run(
            """
            MATCH (class:Class)-[:HAS_SUBJECT]->(subject:Subject)-[:HAS_TOPIC]->(topic:Topic)-[:HAS_LESSON]->(lesson:Lesson)-[:HAS_CHUNK]->(chunk:Chunk)
            WHERE ($class_id = '' OR class.pg_id = $class_id)
              AND ($subject_id = '' OR subject.pg_id = $subject_id)
              AND ($topic_ids IS NULL OR size($topic_ids) = 0 OR topic.pg_id IN $topic_ids)
              AND ($lesson_ids IS NULL OR size($lesson_ids) = 0 OR lesson.pg_id IN $lesson_ids)
            RETURN chunk.pg_id AS chunk_id,
                   coalesce(chunk.chunk_name, chunk.name, chunk.pg_id) AS chunk_name,
                   chunk.chunk_number AS chunk_number,
                   lesson.pg_id AS lesson_id,
                   coalesce(lesson.lesson_name, lesson.name, lesson.pg_id) AS lesson_name,
                   lesson.lesson_number AS lesson_number,
                   topic.pg_id AS topic_id,
                   coalesce(topic.topic_name, topic.name, topic.pg_id) AS topic_name,
                   topic.topic_number AS topic_number,
                   subject.pg_id AS subject_id,
                   coalesce(subject.name, subject.pg_id) AS subject_name,
                   class.pg_id AS class_id,
                   coalesce(class.name, class.pg_id) AS class_name
            ORDER BY class.pg_id, subject.pg_id, topic.topic_number, topic.pg_id, lesson.lesson_number, lesson.pg_id, chunk.chunk_number, chunk.pg_id
            """,
            class_id=(class_id or "").strip(),
            subject_id=(subject_id or "").strip(),
            topic_ids=clean_topic_ids,
            lesson_ids=clean_lesson_ids,
        )
        rows: List[dict] = []
        for r in records:
            chunk_pg_id = str(r.get("chunk_id") or "").strip()
            if not chunk_pg_id:
                continue
            rows.append({
                "chunkID": chunk_pg_id,
                "chunkName": str(r.get("chunk_name") or chunk_pg_id).strip(),
                "chunkNumber": r.get("chunk_number"),
                "lessonID": str(r.get("lesson_id") or "").strip(),
                "lessonName": str(r.get("lesson_name") or "").strip(),
                "lessonNumber": r.get("lesson_number"),
                "topicID": str(r.get("topic_id") or "").strip(),
                "topicName": str(r.get("topic_name") or "").strip(),
                "topicNumber": r.get("topic_number"),
                "subjectID": str(r.get("subject_id") or "").strip(),
                "subjectName": str(r.get("subject_name") or "").strip(),
                "classID": str(r.get("class_id") or "").strip(),
                "className": str(r.get("class_name") or "").strip(),
            })
        return rows, None
    except Exception as exc:
        return [], str(exc)


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


def _filter_scope_by_name_hint(
    *,
    rows: List[dict],
    id_key: str,
    name_key: str,
    name_hint: str,
    label: str,
    keep_ratio: float = 0.9,
    min_score: float = 0.2,
    keep_limit: int = 8,
) -> Tuple[List[str], Dict[str, float], dict]:
    debug = {"label": label, "name_hint": name_hint, "input_count": len(rows), "match_source": "neo4j_name_text"}
    if not name_hint:
        ids = [str(row.get(id_key) or "") for row in rows if str(row.get(id_key) or "")]
        return ids, {}, debug

    norm_hint = _norm_keyword_text(name_hint)
    if not norm_hint:
        ids = [str(row.get(id_key) or "") for row in rows if str(row.get(id_key) or "")]
        debug["fallback"] = "empty_normalized_hint"
        return ids, {}, debug

    scored: List[dict] = []
    for row in rows:
        node_id = str(row.get(id_key) or "").strip()
        node_name = str(row.get(name_key) or "").strip()
        if not node_id or not node_name:
            continue
        norm_name = _norm_keyword_text(node_name)
        overlap = _token_overlap_ratio(norm_hint, norm_name)
        bonus = 0.0
        if norm_name and norm_hint:
            if norm_name == norm_hint:
                bonus = 0.2
            elif norm_hint in norm_name or norm_name in norm_hint:
                bonus = 0.12
        score = float(overlap + bonus)
        scored.append({"id": node_id, "name": node_name, "score": score})

    scored.sort(key=lambda item: item["score"], reverse=True)
    debug["top_matches"] = scored[:5]
    if not scored:
        debug["rejected"] = "no_rows"
        return [], {}, debug

    top_score = float(scored[0]["score"])
    debug["top_score"] = top_score
    if top_score < min_score:
        debug["rejected"] = "below_threshold"
        return [], {}, debug

    threshold = max(min_score, top_score * keep_ratio)
    selected = [item for item in scored if float(item["score"]) >= threshold][:keep_limit]
    score_map = {str(item["id"]): float(item["score"]) for item in selected}
    return [str(item["id"]) for item in selected], score_map, debug


def _load_entity_keyword_rows_from_neo(
    neo,
    *,
    owner_label: str,
    owner_ids: List[str],
) -> Tuple[List[Tuple[str, str, str, List[float]]], Optional[str]]:
    if neo is None:
        return [], "neo_session_unavailable"

    clean_owner_ids = _dedupe_keep_order_ids(owner_ids)
    if not clean_owner_ids:
        return [], None

    try:
        cypher = f"""
        UNWIND $owner_ids AS owner_id
        MATCH (owner:{owner_label} {{pg_id: owner_id}})-[:HAS_KEYWORD]->(keyword:Keyword)
        WHERE keyword.embedding IS NOT NULL
        RETURN DISTINCT owner.pg_id AS owner_id,
               keyword.pg_id AS keyword_id,
               coalesce(keyword.name, keyword.pg_id) AS keyword_name,
               keyword.embedding AS keyword_embedding
        """

        rows: List[Tuple[str, str, str, List[float]]] = []
        for record in neo.run(cypher, owner_ids=clean_owner_ids):
            owner_id = str(record.get("owner_id") or "").strip()
            keyword_id = str(record.get("keyword_id") or "").strip()
            keyword_name = str(record.get("keyword_name") or "").strip()
            embedding = record.get("keyword_embedding")
            if not owner_id or not keyword_id or not keyword_name or not embedding:
                continue
            try:
                rows.append((
                    keyword_id,
                    owner_id,
                    keyword_name,
                    [float(x) for x in list(embedding)],
                ))
            except Exception:
                continue
        return rows, None
    except Exception as exc:
        return [], str(exc)


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
    raw = _strip_query_filler_phrases(raw_query or "")
    pieces = re.split(r"\s*(?:[,;\n/]+|\b(?:va|và|hoặc|hay|and|or)\b)\s*", raw, flags=re.IGNORECASE)
    parts = [_strip_keyword_filler(piece) for piece in pieces]
    parts = [part for part in parts if part]
    if len(parts) >= 2:
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


def _load_keyword_rows_by_map_ids(pg: Session, map_ids: List[str]) -> List[Tuple[str, str, str, List[float]]]:
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
    *,
    owner_alias_by_id: Optional[Dict[str, str]] = None,
    keep_ratio: Optional[float] = None,
    absolute_floor: Optional[float] = None,
    keep_limit: Optional[int] = None,
) -> Tuple[List[str], Dict[str, float], Dict[str, List[Tuple[float, str]]], Dict[str, object]]:
    clean_query = _strip_keyword_filler(query_text or "")
    if not clean_query or not rows:
        return [], {}, {}, {"keyword_rows": len(rows)}

    query_embedding = embed_keyword_cached(clean_query)
    q_tokens = _dedupe_keep_order(_tokens_no_stop(clean_query))
    q_token_set = set(q_tokens)
    token_count = len(q_tokens)
    norm_q = _norm_keyword_text(clean_query)

    if keep_ratio is None:
        keep_ratio = 0.96 if token_count <= 1 else (0.91 if token_count == 2 else 0.89)
    if absolute_floor is None:
        absolute_floor = 0.82 if token_count <= 1 else (0.75 if token_count == 2 else 0.72)
    if keep_limit is None:
        keep_limit = 4 if token_count <= 1 else (10 if token_count == 2 else 14)

    best_score_by_owner: Dict[str, float] = {}
    final_score_by_owner: Dict[str, float] = {}
    matched_keywords: Dict[str, List[Tuple[float, str]]] = defaultdict(list)
    owner_ids_by_alias: Dict[str, List[str]] = defaultdict(list)
    owner_token_hits: Dict[str, set[str]] = defaultdict(set)
    owner_keyword_hit_count: Dict[str, int] = defaultdict(int)
    owner_phrase_hits: Dict[str, int] = defaultdict(int)
    owner_exact_or_phrase: Dict[str, bool] = defaultdict(bool)

    for keyword_id, owner_id, keyword_name, keyword_embedding in rows:
        owner_key = (owner_alias_by_id or {}).get(owner_id) or _entity_alias_key(owner_id) or owner_id
        if not owner_key:
            continue
        owner_ids = owner_ids_by_alias.setdefault(owner_key, [])
        if owner_id not in owner_ids:
            owner_ids.append(owner_id)

        cosine = _cosine(query_embedding, keyword_embedding)
        overlap = _token_overlap_ratio(clean_query, keyword_name)
        norm_kw = _norm_keyword_text(keyword_name)
        kw_tokens = set(_tokens_no_stop(keyword_name or ""))
        shared_tokens = q_token_set & kw_tokens

        exact_bonus = 0.0
        phrase_hit = False
        partial_phrase_hit = False
        shared_ratio = float(len(shared_tokens) / token_count) if token_count > 0 else 0.0
        if norm_kw and norm_q:
            if norm_kw == norm_q:
                exact_bonus = 0.16
                phrase_hit = True
            elif norm_q in norm_kw:
                exact_bonus = 0.12
                phrase_hit = True
            elif norm_kw in norm_q:
                if shared_ratio >= 0.75:
                    exact_bonus = 0.08
                    phrase_hit = True
                else:
                    exact_bonus = 0.02
                    partial_phrase_hit = True

        if token_count >= 2:
            if len(shared_tokens) >= min(token_count, 2):
                exact_bonus += 0.06
            elif overlap >= 0.5:
                exact_bonus += 0.04

        score = float(cosine + 0.10 * overlap + exact_bonus)
        current_best = best_score_by_owner.get(owner_key)
        if current_best is None or score > current_best:
            best_score_by_owner[owner_key] = score

        keyword_is_relevant = bool(shared_tokens or phrase_hit or partial_phrase_hit or overlap >= 0.34)
        if keyword_is_relevant:
            owner_token_hits[owner_key].update(shared_tokens)
            owner_keyword_hit_count[owner_key] += 1
        if phrase_hit:
            owner_phrase_hits[owner_key] += 1
            owner_exact_or_phrase[owner_key] = True

        if keyword_is_relevant:
            bucket = matched_keywords[owner_key]
            if not any(existing_name == keyword_name for _existing_score, existing_name in bucket):
                bucket.append((score, keyword_name))
                bucket.sort(key=lambda item: item[0], reverse=True)
                matched_keywords[owner_key] = bucket[:5]

    for owner_key, best_score in best_score_by_owner.items():
        token_hits = owner_token_hits.get(owner_key) or set()
        coverage = float(len(token_hits) / token_count) if token_count > 0 else 0.0
        keyword_hit_count = int(owner_keyword_hit_count.get(owner_key) or 0)
        phrase_hits = int(owner_phrase_hits.get(owner_key) or 0)
        support_bonus = 0.0
        support_bonus += 0.05 * min(4, max(0, keyword_hit_count - 1))
        support_bonus += 0.08 * coverage
        if keyword_hit_count >= 3:
            support_bonus += 0.03
        if phrase_hits > 0:
            support_bonus += 0.03
        final_score_by_owner[owner_key] = float(best_score + support_bonus)

    ranked = sorted(final_score_by_owner.items(), key=lambda item: item[1], reverse=True)
    if not ranked:
        return [], final_score_by_owner, matched_keywords, {"keyword_rows": len(rows)}

    top_score = float(ranked[0][1])
    min_score = max(float(absolute_floor), top_score * float(keep_ratio))
    matched_aliases: List[str] = []
    evidence_by_owner: Dict[str, dict] = {}

    for owner_key, score in ranked:
        token_hits = owner_token_hits.get(owner_key) or set()
        coverage = float(len(token_hits) / token_count) if token_count > 0 else 0.0
        keyword_hit_count = int(owner_keyword_hit_count.get(owner_key) or 0)
        phrase_hits = int(owner_phrase_hits.get(owner_key) or 0)
        has_phrase = bool(owner_exact_or_phrase.get(owner_key))
        keep_by_threshold = float(score) >= min_score
        keep_by_evidence = False
        if token_count <= 1:
            keep_by_evidence = has_phrase
        else:
            keep_by_evidence = has_phrase or coverage >= 0.50 or keyword_hit_count >= 2
        evidence_by_owner[owner_key] = {
            "score": float(score),
            "coverage": coverage,
            "keyword_hits": keyword_hit_count,
            "phrase_hits": phrase_hits,
            "keep_by_threshold": keep_by_threshold,
            "keep_by_evidence": keep_by_evidence,
        }
        if keep_by_threshold or keep_by_evidence:
            matched_aliases.append(owner_key)

    if not matched_aliases and ranked:
        matched_aliases = [ranked[0][0]]
    if keep_limit and keep_limit > 0:
        matched_aliases = matched_aliases[:keep_limit]

    debug = {
        "keyword_rows": len(rows),
        "query_token_count": token_count,
        "top_score": top_score,
        "min_score": min_score,
        "matched_count": len(matched_aliases),
        "matched_aliases": matched_aliases[:20],
        "owner_ids_by_alias": {alias: ids[:5] for alias, ids in list(owner_ids_by_alias.items())[:20]},
        "owner_evidence": {alias: evidence_by_owner.get(alias) for alias in matched_aliases[:20]},
    }
    return matched_aliases, final_score_by_owner, matched_keywords, debug


def _merge_keyword_buckets(
    base: Dict[str, List[Tuple[float, str]]],
    incoming: Dict[str, List[Tuple[float, str]]],
    *,
    limit: int = 5,
) -> Dict[str, List[Tuple[float, str]]]:
    for owner_key, bucket in (incoming or {}).items():
        target = list(base.get(owner_key) or [])
        for score, keyword_name in bucket or []:
            if any(existing_name == keyword_name for _existing_score, existing_name in target):
                continue
            target.append((float(score), keyword_name))
        target.sort(key=lambda item: item[0], reverse=True)
        base[owner_key] = target[:limit]
    return base



def _score_entity_keyword_rows_multi(
    query_parts: List[str],
    rows: List[Tuple[str, str, str, List[float]]],
    *,
    owner_alias_by_id: Optional[Dict[str, str]] = None,
    keep_ratio: Optional[float] = None,
    absolute_floor: Optional[float] = None,
    keep_limit: Optional[int] = None,
) -> Tuple[List[str], Dict[str, float], Dict[str, List[Tuple[float, str]]], Dict[str, object]]:
    clean_parts = _dedupe_keep_order([
        _strip_keyword_filler(part or "")
        for part in (query_parts or [])
        if _strip_keyword_filler(part or "")
    ])
    if not clean_parts:
        return [], {}, {}, {"keyword_rows": len(rows), "query_parts": []}
    if len(clean_parts) == 1:
        matched_aliases, score_by_owner, matched_keywords, debug = _score_entity_keyword_rows(
            clean_parts[0],
            rows,
            owner_alias_by_id=owner_alias_by_id,
            keep_ratio=keep_ratio,
            absolute_floor=absolute_floor,
            keep_limit=keep_limit,
        )
        debug = dict(debug or {})
        debug["query_parts"] = clean_parts
        debug["multi_mode"] = False
        return matched_aliases, score_by_owner, matched_keywords, debug

    combined_scores: Dict[str, float] = {}
    combined_keywords: Dict[str, List[Tuple[float, str]]] = {}
    matched_parts_by_alias: Dict[str, List[str]] = defaultdict(list)
    owner_ids_by_alias: Dict[str, List[str]] = defaultdict(list)
    part_debugs: List[Dict[str, object]] = []

    part_keep_limit = max(6, keep_limit or 0)
    for part in clean_parts:
        matched_aliases, part_scores, part_keywords, part_debug = _score_entity_keyword_rows(
            part,
            rows,
            owner_alias_by_id=owner_alias_by_id,
            keep_ratio=keep_ratio,
            absolute_floor=absolute_floor,
            keep_limit=part_keep_limit,
        )
        part_debugs.append({
            "query_part": part,
            **(part_debug or {}),
        })
        for owner_key, score in (part_scores or {}).items():
            current = combined_scores.get(owner_key)
            score_v = float(score)
            if current is None or score_v > current:
                combined_scores[owner_key] = score_v
        combined_keywords = _merge_keyword_buckets(combined_keywords, part_keywords, limit=5)
        alias_owner_map = (part_debug or {}).get("owner_ids_by_alias") or {}
        for alias, owner_ids in alias_owner_map.items():
            target = owner_ids_by_alias.setdefault(alias, [])
            for owner_id in owner_ids or []:
                owner_id_s = str(owner_id or "").strip()
                if owner_id_s and owner_id_s not in target:
                    target.append(owner_id_s)
        for owner_key in matched_aliases or []:
            if part not in matched_parts_by_alias[owner_key]:
                matched_parts_by_alias[owner_key].append(part)

    if not combined_scores:
        return [], {}, combined_keywords, {
            "keyword_rows": len(rows),
            "query_parts": clean_parts,
            "multi_mode": True,
            "per_part": part_debugs,
        }

    ranked = sorted(
        combined_scores.items(),
        key=lambda item: (float(item[1]) + 0.04 * len(matched_parts_by_alias.get(item[0], [])), item[0]),
        reverse=True,
    )
    matched_aliases = [owner_key for owner_key, _score in ranked if matched_parts_by_alias.get(owner_key)]
    final_keep_limit = keep_limit if keep_limit is not None else max(10, 6 * len(clean_parts))
    if final_keep_limit > 0:
        matched_aliases = matched_aliases[: max(final_keep_limit, len(clean_parts) * 4)]
    debug = {
        "keyword_rows": len(rows),
        "query_parts": clean_parts,
        "multi_mode": True,
        "per_part": part_debugs,
        "matched_count": len(matched_aliases),
        "matched_aliases": matched_aliases[:20],
        "matched_parts_by_alias": {alias: parts[:8] for alias, parts in list(matched_parts_by_alias.items())[:20]},
        "owner_ids_by_alias": {alias: ids[:5] for alias, ids in list(owner_ids_by_alias.items())[:20]},
        "top_score": float(ranked[0][1]) if ranked else 0.0,
    }
    return matched_aliases, combined_scores, combined_keywords, debug


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

    def _clean_media_items(items: List[dict], *, media_type: str) -> List[dict]:
        out: List[dict] = []
        seen: set[tuple[str, str, str]] = set()
        for raw in items or []:
            media_id = str((raw or {}).get("id") or "").strip()
            follow_type = str((raw or {}).get("followType") or "").strip()
            follow_id = str((raw or {}).get("followID") or "").strip()
            if not media_id or not follow_type or not follow_id:
                continue
            sig = (media_type, follow_type, media_id)
            if sig in seen:
                continue
            seen.add(sig)
            out.append({
                "type": media_type,
                "id": media_id,
                "name": str((raw or {}).get("name") or media_id).strip(),
                "description": str((raw or {}).get("description") or "").strip(),
                "url": str((raw or {}).get("url") or "").strip(),
                "mapID": str((raw or {}).get("mapID") or "").strip(),
                "mongoID": str((raw or {}).get("mongoID") or "").strip(),
                "followType": follow_type,
                "followID": follow_id,
            })
        out.sort(key=_media_sort_key)
        return out

    try:
        records = neo.run(
            """
            UNWIND $chunk_ids AS chunk_id
            MATCH (chunk:Chunk {pg_id: chunk_id})
            OPTIONAL MATCH (lesson:Lesson)-[:HAS_CHUNK]->(chunk)
            OPTIONAL MATCH (topic:Topic)-[:HAS_LESSON]->(lesson)
            OPTIONAL MATCH (subject:Subject)-[:HAS_TOPIC]->(topic)
            OPTIONAL MATCH (class:Class)-[:HAS_SUBJECT]->(subject)
            WITH chunk, lesson, topic, subject, class,
                 [(chunk)-[:HAS_IMAGE_GROUP]->(img_group)-[:HAS_IMAGE]->(img:Image) |
                    {id: img.pg_id, name: coalesce(img.name, img.pg_id), url: coalesce(img.url, ''), description: coalesce(img.description, ''), mapID: coalesce(img.map_id, ''), mongoID: coalesce(img.mongo_id, ''), followType: 'chunk', followID: chunk.pg_id}
                 ] AS chunk_images,
                 CASE WHEN lesson IS NULL THEN [] ELSE [(lesson)-[:HAS_IMAGE_GROUP]->(img_group)-[:HAS_IMAGE]->(img:Image) |
                    {id: img.pg_id, name: coalesce(img.name, img.pg_id), url: coalesce(img.url, ''), description: coalesce(img.description, ''), mapID: coalesce(img.map_id, ''), mongoID: coalesce(img.mongo_id, ''), followType: 'lesson', followID: lesson.pg_id}
                 ] END AS lesson_images,
                 CASE WHEN topic IS NULL THEN [] ELSE [(topic)-[:HAS_IMAGE_GROUP]->(img_group)-[:HAS_IMAGE]->(img:Image) |
                    {id: img.pg_id, name: coalesce(img.name, img.pg_id), url: coalesce(img.url, ''), description: coalesce(img.description, ''), mapID: coalesce(img.map_id, ''), mongoID: coalesce(img.mongo_id, ''), followType: 'topic', followID: topic.pg_id}
                 ] END AS topic_images,
                 CASE WHEN subject IS NULL THEN [] ELSE [(subject)-[:HAS_IMAGE_GROUP]->(img_group)-[:HAS_IMAGE]->(img:Image) |
                    {id: img.pg_id, name: coalesce(img.name, img.pg_id), url: coalesce(img.url, ''), description: coalesce(img.description, ''), mapID: coalesce(img.map_id, ''), mongoID: coalesce(img.mongo_id, ''), followType: 'subject', followID: subject.pg_id}
                 ] END AS subject_images,
                 [(chunk)-[:HAS_VIDEO_GROUP]->(video_group)-[:HAS_VIDEO]->(video:Video) |
                    {id: video.pg_id, name: coalesce(video.name, video.pg_id), url: coalesce(video.url, ''), description: coalesce(video.description, ''), mapID: coalesce(video.map_id, ''), mongoID: coalesce(video.mongo_id, ''), followType: 'chunk', followID: chunk.pg_id}
                 ] AS chunk_videos,
                 CASE WHEN lesson IS NULL THEN [] ELSE [(lesson)-[:HAS_VIDEO_GROUP]->(video_group)-[:HAS_VIDEO]->(video:Video) |
                    {id: video.pg_id, name: coalesce(video.name, video.pg_id), url: coalesce(video.url, ''), description: coalesce(video.description, ''), mapID: coalesce(video.map_id, ''), mongoID: coalesce(video.mongo_id, ''), followType: 'lesson', followID: lesson.pg_id}
                 ] END AS lesson_videos,
                 CASE WHEN topic IS NULL THEN [] ELSE [(topic)-[:HAS_VIDEO_GROUP]->(video_group)-[:HAS_VIDEO]->(video:Video) |
                    {id: video.pg_id, name: coalesce(video.name, video.pg_id), url: coalesce(video.url, ''), description: coalesce(video.description, ''), mapID: coalesce(video.map_id, ''), mongoID: coalesce(video.mongo_id, ''), followType: 'topic', followID: topic.pg_id}
                 ] END AS topic_videos,
                 CASE WHEN subject IS NULL THEN [] ELSE [(subject)-[:HAS_VIDEO_GROUP]->(video_group)-[:HAS_VIDEO]->(video:Video) |
                    {id: video.pg_id, name: coalesce(video.name, video.pg_id), url: coalesce(video.url, ''), description: coalesce(video.description, ''), mapID: coalesce(video.map_id, ''), mongoID: coalesce(video.mongo_id, ''), followType: 'subject', followID: subject.pg_id}
                 ] END AS subject_videos
            RETURN chunk.pg_id AS chunk_id,
                   coalesce(chunk.chunk_name, chunk.name, chunk.pg_id) AS chunk_name,
                   chunk.chunk_number AS chunk_number,
                   lesson.pg_id AS lesson_id,
                   coalesce(lesson.lesson_name, lesson.name, lesson.pg_id) AS lesson_name,
                   lesson.lesson_number AS lesson_number,
                   topic.pg_id AS topic_id,
                   coalesce(topic.topic_name, topic.name, topic.pg_id) AS topic_name,
                   topic.topic_number AS topic_number,
                   subject.pg_id AS subject_id,
                   coalesce(subject.name, subject.pg_id) AS subject_name,
                   class.pg_id AS class_id,
                   coalesce(class.name, class.pg_id) AS class_name,
                   chunk_images, lesson_images, topic_images, subject_images,
                   chunk_videos, lesson_videos, topic_videos, subject_videos
            """,
            chunk_ids=chunk_ids,
        )
        out: Dict[str, dict] = {}
        for record in records:
            chunk_id = str(record.get("chunk_id") or "").strip()
            if not chunk_id:
                continue
            images = _clean_media_items(
                list(record.get("chunk_images") or [])
                + list(record.get("lesson_images") or [])
                + list(record.get("topic_images") or [])
                + list(record.get("subject_images") or []),
                media_type="image",
            )
            videos = _clean_media_items(
                list(record.get("chunk_videos") or [])
                + list(record.get("lesson_videos") or [])
                + list(record.get("topic_videos") or [])
                + list(record.get("subject_videos") or []),
                media_type="video",
            )
            media_sources = {"chunk": 0, "lesson": 0, "topic": 0, "subject": 0}
            for item in images + videos:
                media_sources[item.get("followType") or ""] = media_sources.get(item.get("followType") or "", 0) + 1
            out[chunk_id] = {
                "chunkID": chunk_id,
                "chunkName": str(record.get("chunk_name") or "").strip(),
                "chunkNumber": record.get("chunk_number"),
                "lesson": {
                    "lessonID": str(record.get("lesson_id") or "").strip(),
                    "lessonName": str(record.get("lesson_name") or "").strip(),
                    "lessonNumber": record.get("lesson_number"),
                },
                "topic": {
                    "topicID": str(record.get("topic_id") or "").strip(),
                    "topicName": str(record.get("topic_name") or "").strip(),
                    "topicNumber": record.get("topic_number"),
                },
                "subject": {"subjectID": str(record.get("subject_id") or "").strip(), "subjectName": str(record.get("subject_name") or "").strip()},
                "class": {"classID": str(record.get("class_id") or "").strip(), "className": str(record.get("class_name") or "").strip()},
                "images": images,
                "videos": videos,
                "mediaSummary": {"totalImages": len(images), "totalVideos": len(videos), "byFollowType": media_sources},
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

    dbg["media_hit_groups"] = sum(1 for payload in neo_map.values() if (payload.get("images") or payload.get("videos")))

    items: List[dict] = []
    for chunk_id in page_chunk_ids:
        neo_base = neo_map.get(chunk_id) or {}
        pg_base = pg_map.get(chunk_id) or {}
        if not neo_base and not pg_base:
            continue

        chunk_doc = None
        if _valid_object_id_hex(pg_base.get("chunkMongoId") or ""):
            chunk_doc = mongo_chunks_by_oid.get(pg_base["chunkMongoId"])
        if chunk_doc and not _status_visible(chunk_doc):
            dbg.setdefault("item_build", {}).setdefault("blocked_hidden", []).append({"chunkID": chunk_id, "reason": "chunk_hidden"})
            continue
        if category and category != "all":
            chunk_category = (chunk_doc or {}).get("chunkCategory") or ""
            if chunk_category and chunk_category != category:
                dbg.setdefault("item_build", {}).setdefault("non_blocking_flags", []).append({"chunkID": chunk_id, "reason": "mongo_category_mismatch", "mongo_category": chunk_category})

        lesson_oid = (pg_base.get("lesson") or {}).get("mongoId") or ""
        topic_oid = (pg_base.get("topic") or {}).get("mongoId") or ""
        subject_oid = (pg_base.get("subject") or {}).get("mongoId") or ""
        lesson_doc = mongo_lessons_by_oid.get(lesson_oid) if _valid_object_id_hex(lesson_oid) else None
        topic_doc = mongo_topics_by_oid.get(topic_oid) if _valid_object_id_hex(topic_oid) else None
        subject_doc = mongo_subjects_by_oid.get(subject_oid) if _valid_object_id_hex(subject_oid) else None

        if lesson_doc and not _status_visible(lesson_doc):
            dbg.setdefault("item_build", {}).setdefault("blocked_hidden", []).append({"chunkID": chunk_id, "reason": "lesson_hidden"})
            continue
        if topic_doc and not _status_visible(topic_doc):
            dbg.setdefault("item_build", {}).setdefault("blocked_hidden", []).append({"chunkID": chunk_id, "reason": "topic_hidden"})
            continue
        if subject_doc and not _status_visible(subject_doc):
            dbg.setdefault("item_build", {}).setdefault("blocked_hidden", []).append({"chunkID": chunk_id, "reason": "subject_hidden"})
            continue

        lesson_name = (neo_base.get("lesson") or {}).get("lessonName") or (pg_base.get("lesson") or {}).get("lessonName") or ""
        topic_name = (neo_base.get("topic") or {}).get("topicName") or (pg_base.get("topic") or {}).get("topicName") or ""
        subject_name = (neo_base.get("subject") or {}).get("subjectName") or (pg_base.get("subject") or {}).get("subjectName") or ""
        class_name = (neo_base.get("class") or {}).get("className") or (pg_base.get("class") or {}).get("className") or ""
        lesson_id_v = (neo_base.get("lesson") or {}).get("lessonID") or (pg_base.get("lesson") or {}).get("lessonID") or ""
        topic_id_v = (neo_base.get("topic") or {}).get("topicID") or (pg_base.get("topic") or {}).get("topicID") or ""
        subject_id_v = (neo_base.get("subject") or {}).get("subjectID") or (pg_base.get("subject") or {}).get("subjectID") or ""
        class_id_v = (neo_base.get("class") or {}).get("classID") or (pg_base.get("class") or {}).get("classID") or ""

        images: List[dict] = list(neo_base.get("images") or [])
        videos: List[dict] = list(neo_base.get("videos") or [])
        media_summary = neo_base.get("mediaSummary") or {
            "totalImages": len(images),
            "totalVideos": len(videos),
            "byFollowType": {"chunk": 0, "lesson": 0, "topic": 0, "subject": 0},
        }

        matched_kw = [name for _score, name in chunk_top_kw.get(chunk_id, [])]
        item = {
            "type": "chunk",
            "id": chunk_id,
            "name": (neo_base.get("chunkName") or pg_base.get("chunkName") or (chunk_doc or {}).get("chunkName") or chunk_id),
            "score": float(score_by_chunk.get(chunk_id, 0.0)),
            "chunkID": chunk_id,
            "chunkName": (chunk_doc.get("chunkName") if chunk_doc else None) or neo_base.get("chunkName") or pg_base.get("chunkName") or chunk_id,
            "chunkType": (chunk_doc.get("chunkType") if chunk_doc else None) or pg_base.get("chunkType"),
            "chunkNumber": neo_base.get("chunkNumber") if neo_base.get("chunkNumber") is not None else pg_base.get("chunkNumber"),
            "chunkUrl": (chunk_doc.get("chunkUrl") if chunk_doc else None),
            "chunkDescription": (chunk_doc.get("chunkDescription") if chunk_doc else None),
            "createdAt": (chunk_doc.get("createdAt") if chunk_doc else None),
            "updatedAt": (chunk_doc.get("updatedAt") if chunk_doc else None),
            "keywords": _read_keywords_from_chunk_doc(chunk_doc),
            "matchedKeywords": matched_kw,
            "images": images,
            "videos": videos,
            "mediaSummary": media_summary,
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
                "topicNumber": (neo_base.get("topic") or {}).get("topicNumber") if (neo_base.get("topic") or {}).get("topicNumber") is not None else (pg_base.get("topic") or {}).get("topicNumber"),
                "topicDescription": ((topic_doc.get("topicDescription") if topic_doc else None) or (topic_doc.get("topic_description") if topic_doc else None) or (topic_doc.get("description") if topic_doc else None) or ""),
                "topicUrl": (topic_doc.get("topicUrl") if topic_doc and _status_visible(topic_doc) else ""),
            },
            "lesson": {
                "lessonID": lesson_id_v,
                "lessonName": lesson_name,
                "lessonNumber": (neo_base.get("lesson") or {}).get("lessonNumber") if (neo_base.get("lesson") or {}).get("lessonNumber") is not None else (pg_base.get("lesson") or {}).get("lessonNumber"),
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

    dbg: Dict[str, object] = {
        "service_version": _SERVICE_VERSION,
        "category": category,
        "search_mode": "neo4j_graph_hierarchical_keyword_branch_gate_exact_pg_id",
        "raw_query": query,
    }

    ctx = _parse_query_context(query)
    dbg["query_context"] = ctx
    class_scope = _normalize_class_scope(classID, ctx.get("classNumber"))

    topic_number = None if topicID else ctx.get("topicNumber")
    lesson_number = None if lessonID else ctx.get("lessonNumber")
    chunk_number = ctx.get("chunkNumber")

    topic_rows, topic_neo_error = _load_topic_rows_neo(neo=neo, class_id=class_scope, subject_id=subjectID, topic_id=topicID)
    topic_rows = _filter_by_number(topic_rows, "topicNumber", topic_number)
    dbg["topic_rows"] = len(topic_rows)
    if topic_neo_error:
        dbg["topic_rows_neo_error"] = topic_neo_error
    topic_ids, _topic_score_map, topic_dbg = _filter_scope_by_name_hint(
        rows=topic_rows,
        id_key="topicID",
        name_key="topicName",
        name_hint=str(ctx.get("topicNameHint") or ""),
        label="Topic",
    )
    dbg["topic_scope"] = topic_dbg
    topic_scope_active = bool(topicID or topic_number is not None or str(ctx.get("topicNameHint") or ""))
    if topic_scope_active and not topic_ids:
        res = {"total": 0, "items": []}
        if debug:
            dbg["reason"] = "topic_scope_no_match"
            res["debug"] = dbg
        return res
    if topic_scope_active:
        topic_id_set = set(topic_ids)
        topic_rows = [row for row in topic_rows if str(row.get("topicID") or "") in topic_id_set]

    lessons_rows, lesson_rows_neo_error = _load_lesson_rows_neo(
        neo=neo,
        class_id=class_scope,
        subject_id=subjectID,
        topic_ids=topic_ids if topic_scope_active else None,
        lesson_id=lessonID,
    )
    lessons_rows = _filter_by_number(lessons_rows, "lessonNumber", lesson_number)
    dbg["lesson_rows"] = len(lessons_rows)
    if lesson_rows_neo_error:
        dbg["lesson_rows_neo_error"] = lesson_rows_neo_error
    lesson_ids, _lesson_score_map, lesson_dbg = _filter_scope_by_name_hint(
        rows=lessons_rows,
        id_key="lessonID",
        name_key="lessonName",
        name_hint=str(ctx.get("lessonNameHint") or ""),
        label="Lesson",
    )
    dbg["lesson_scope"] = lesson_dbg
    lesson_scope_active = bool(lessonID or lesson_number is not None or str(ctx.get("lessonNameHint") or ""))
    if lesson_scope_active and not lesson_ids:
        res = {"total": 0, "items": []}
        if debug:
            dbg["reason"] = "lesson_scope_no_match"
            res["debug"] = dbg
        return res
    if lesson_scope_active:
        lesson_id_set = set(lesson_ids)
        lessons_rows = [row for row in lessons_rows if str(row.get("lessonID") or "") in lesson_id_set]

    chunk_rows, chunk_rows_neo_error = _load_chunk_rows_neo(
        neo=neo,
        class_id=class_scope,
        subject_id=subjectID,
        topic_ids=topic_ids if topic_scope_active else None,
        lesson_ids=lesson_ids if lesson_scope_active else None,
    )
    chunk_rows = _filter_by_number(chunk_rows, "chunkNumber", chunk_number)
    dbg["chunk_rows"] = len(chunk_rows)
    if chunk_rows_neo_error:
        dbg["chunk_rows_neo_error"] = chunk_rows_neo_error
    explicit_chunk_ids, _explicit_chunk_score_map, chunk_dbg = _filter_scope_by_name_hint(
        rows=chunk_rows,
        id_key="chunkID",
        name_key="chunkName",
        name_hint=str(ctx.get("chunkNameHint") or ""),
        label="Chunk",
    )
    dbg["chunk_scope"] = chunk_dbg
    chunk_scope_active = bool(chunk_number is not None or str(ctx.get("chunkNameHint") or ""))
    if chunk_scope_active and not explicit_chunk_ids:
        res = {"total": 0, "items": []}
        if debug:
            dbg["reason"] = "chunk_scope_no_match"
            res["debug"] = dbg
        return res
    if chunk_scope_active:
        explicit_chunk_set = set(explicit_chunk_ids)
        chunk_rows = [row for row in chunk_rows if str(row.get("chunkID") or "") in explicit_chunk_set]

    generic_query = str(ctx.get("genericQuery") or "")
    keyword_query = _strip_keyword_filler(generic_query or query)
    if not keyword_query:
        keyword_query = _core_query_text(query)
    dbg["keyword_query"] = keyword_query
    query_parts = _split_keyword_query_parts(query, keyword_query)
    dbg["query_parts"] = query_parts
    dbg["branch_mode"] = "keep_enough_correct_branches"

    if not keyword_query:
        return_mode = _pick_return_mode(ctx, topicID=topicID, lessonID=lessonID)
        dbg["return_mode"] = f"structured_{return_mode}"
        if return_mode == "chunk":
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
            res = {"total": len(items), "items": items}
            if debug:
                dbg["items_built"] = len(items)
                res["debug"] = dbg
            return res
        if return_mode == "lesson":
            items = _build_lesson_items(lessons_rows[offset : offset + limit])
            res = {"total": len(lessons_rows), "items": items}
            if debug:
                res["debug"] = dbg
            return res
        items = _build_topic_items(topic_rows[offset : offset + limit])
        res = {"total": len(topic_rows), "items": items}
        if debug:
            res["debug"] = dbg
        return res

    try:
        dbg["query_embedding_dim"] = len(embed_keyword_cached(keyword_query or query))
    except Exception:
        dbg["query_embedding_dim"] = 0

    filtered_chunk_rows = list(chunk_rows)
    hierarchy_dbg: Dict[str, object] = {}

    # Subject gate
    current_subject_ids = _collect_ids_keep_case(filtered_chunk_rows, "subjectID")
    subject_alias_by_id, subject_ids_by_alias = _alias_map_for_rows(filtered_chunk_rows, id_key="subjectID", name_key="subjectName")
    hierarchy_dbg["subject_candidates"] = len(current_subject_ids)
    hierarchy_dbg["subject_candidate_ids"] = current_subject_ids[:10]
    hierarchy_dbg["subject_alias_candidates"] = list(subject_ids_by_alias.keys())[:10]
    subject_keyword_rows, subject_neo_error = _load_entity_keyword_rows_from_neo(
        neo,
        owner_label="Subject",
        owner_ids=current_subject_ids,
    )
    hierarchy_dbg["subject_keyword_rows"] = len(subject_keyword_rows)
    hierarchy_dbg["subject_keyword_source"] = "neo4j"
    if subject_neo_error:
        hierarchy_dbg["subject_neo_error"] = subject_neo_error
    if not current_subject_ids:
        res = {"total": 0, "items": []}
        if debug:
            dbg["hierarchy_keyword_filter"] = hierarchy_dbg
            dbg["reason"] = "subject_candidates_empty"
            res["debug"] = dbg
        return res
    if not subject_keyword_rows:
        res = {"total": 0, "items": []}
        if debug:
            dbg["hierarchy_keyword_filter"] = hierarchy_dbg
            dbg["reason"] = "subject_keywords_not_found_in_neo4j"
            res["debug"] = dbg
        return res
    matched_subject_aliases, subject_scores, subject_kw, subject_match_dbg = _score_entity_keyword_rows_multi(
        query_parts or [keyword_query or query],
        subject_keyword_rows,
        owner_alias_by_id=subject_alias_by_id,
        keep_limit=12,
    )
    hierarchy_dbg["subject_match"] = subject_match_dbg
    hierarchy_dbg["subject_matched_aliases"] = matched_subject_aliases
    hierarchy_dbg["subject_matched_ids"] = _expand_ids_for_aliases(subject_ids_by_alias, matched_subject_aliases)
    if not matched_subject_aliases:
        res = {"total": 0, "items": []}
        if debug:
            dbg["hierarchy_keyword_filter"] = hierarchy_dbg
            dbg["reason"] = "subject_gate_no_match"
            res["debug"] = dbg
        return res
    subject_alias_set = set(matched_subject_aliases)
    filtered_chunk_rows = _filter_rows_by_alias(
        filtered_chunk_rows,
        id_key="subjectID",
        name_key="subjectName",
        allowed_aliases=subject_alias_set,
    )

    # Topic gate
    current_topic_ids = _collect_ids_keep_case(filtered_chunk_rows, "topicID")
    topic_alias_by_id, topic_ids_by_alias = _alias_map_for_rows(filtered_chunk_rows, id_key="topicID", name_key="topicName")
    hierarchy_dbg["topic_candidates"] = len(current_topic_ids)
    hierarchy_dbg["topic_candidate_ids"] = current_topic_ids[:10]
    hierarchy_dbg["topic_alias_candidates"] = list(topic_ids_by_alias.keys())[:10]
    if not current_topic_ids:
        res = {"total": 0, "items": []}
        if debug:
            dbg["hierarchy_keyword_filter"] = hierarchy_dbg
            dbg["reason"] = "topic_candidates_empty_after_subject_gate"
            res["debug"] = dbg
        return res
    topic_keyword_rows, topic_neo_error = _load_entity_keyword_rows_from_neo(
        neo,
        owner_label="Topic",
        owner_ids=current_topic_ids,
    )
    hierarchy_dbg["topic_keyword_rows"] = len(topic_keyword_rows)
    hierarchy_dbg["topic_keyword_source"] = "neo4j"
    if topic_neo_error:
        hierarchy_dbg["topic_neo_error"] = topic_neo_error
    if not topic_keyword_rows:
        res = {"total": 0, "items": []}
        if debug:
            dbg["hierarchy_keyword_filter"] = hierarchy_dbg
            dbg["reason"] = "topic_keywords_not_found_in_neo4j"
            res["debug"] = dbg
        return res
    matched_topic_aliases, topic_scores, topic_kw, topic_match_dbg = _score_entity_keyword_rows_multi(
        query_parts or [keyword_query or query],
        topic_keyword_rows,
        owner_alias_by_id=topic_alias_by_id,
        keep_limit=16,
    )
    hierarchy_dbg["topic_match"] = topic_match_dbg
    hierarchy_dbg["topic_matched_aliases"] = matched_topic_aliases
    hierarchy_dbg["topic_matched_ids"] = _expand_ids_for_aliases(topic_ids_by_alias, matched_topic_aliases)
    if not matched_topic_aliases:
        res = {"total": 0, "items": []}
        if debug:
            dbg["hierarchy_keyword_filter"] = hierarchy_dbg
            dbg["reason"] = "topic_gate_no_match"
            res["debug"] = dbg
        return res
    topic_alias_set = set(matched_topic_aliases)
    filtered_chunk_rows = _filter_rows_by_alias(
        filtered_chunk_rows,
        id_key="topicID",
        name_key="topicName",
        allowed_aliases=topic_alias_set,
    )

    # Lesson gate
    current_lesson_ids = _collect_ids_keep_case(filtered_chunk_rows, "lessonID")
    lesson_alias_by_id, lesson_ids_by_alias = _alias_map_for_rows(filtered_chunk_rows, id_key="lessonID", name_key="lessonName")
    hierarchy_dbg["lesson_candidates"] = len(current_lesson_ids)
    hierarchy_dbg["lesson_candidate_ids"] = current_lesson_ids[:10]
    hierarchy_dbg["lesson_alias_candidates"] = list(lesson_ids_by_alias.keys())[:10]
    if not current_lesson_ids:
        res = {"total": 0, "items": []}
        if debug:
            dbg["hierarchy_keyword_filter"] = hierarchy_dbg
            dbg["reason"] = "lesson_candidates_empty_after_topic_gate"
            res["debug"] = dbg
        return res
    lesson_keyword_rows, lesson_neo_error = _load_entity_keyword_rows_from_neo(
        neo,
        owner_label="Lesson",
        owner_ids=current_lesson_ids,
    )
    hierarchy_dbg["lesson_keyword_rows"] = len(lesson_keyword_rows)
    hierarchy_dbg["lesson_keyword_source"] = "neo4j"
    if lesson_neo_error:
        hierarchy_dbg["lesson_neo_error"] = lesson_neo_error
    if not lesson_keyword_rows:
        res = {"total": 0, "items": []}
        if debug:
            dbg["hierarchy_keyword_filter"] = hierarchy_dbg
            dbg["reason"] = "lesson_keywords_not_found_in_neo4j"
            res["debug"] = dbg
        return res
    matched_lesson_aliases, lesson_scores, lesson_kw, lesson_match_dbg = _score_entity_keyword_rows_multi(
        query_parts or [keyword_query or query],
        lesson_keyword_rows,
        owner_alias_by_id=lesson_alias_by_id,
        keep_limit=20,
    )
    hierarchy_dbg["lesson_match"] = lesson_match_dbg
    hierarchy_dbg["lesson_matched_aliases"] = matched_lesson_aliases
    hierarchy_dbg["lesson_matched_ids"] = _expand_ids_for_aliases(lesson_ids_by_alias, matched_lesson_aliases)
    if not matched_lesson_aliases:
        res = {"total": 0, "items": []}
        if debug:
            dbg["hierarchy_keyword_filter"] = hierarchy_dbg
            dbg["reason"] = "lesson_gate_no_match"
            res["debug"] = dbg
        return res
    lesson_alias_set = set(matched_lesson_aliases)
    filtered_chunk_rows = _filter_rows_by_alias(
        filtered_chunk_rows,
        id_key="lessonID",
        name_key="lessonName",
        allowed_aliases=lesson_alias_set,
    )

    # Chunk gate
    chunk_ids = _collect_ids_keep_case(filtered_chunk_rows, "chunkID")
    chunk_alias_by_id, chunk_ids_by_alias = _alias_map_for_rows(filtered_chunk_rows, id_key="chunkID", name_key="chunkName")
    dbg["candidate_chunk_scope"] = len(chunk_ids)
    dbg["candidate_chunk_aliases"] = list(chunk_ids_by_alias.keys())[:20]
    if not chunk_ids:
        res = {"total": 0, "items": []}
        if debug:
            dbg["hierarchy_keyword_filter"] = hierarchy_dbg
            dbg["reason"] = "chunk_candidates_empty_after_lesson_gate"
            res["debug"] = dbg
        return res
    chunk_keyword_rows, chunk_neo_error = _load_entity_keyword_rows_from_neo(
        neo,
        owner_label="Chunk",
        owner_ids=chunk_ids,
    )
    dbg["keyword_rows"] = len(chunk_keyword_rows)
    dbg["keyword_embedding_source"] = "neo4j_map_id"
    if chunk_neo_error:
        dbg["chunk_neo_error"] = chunk_neo_error
    if not chunk_keyword_rows:
        res = {"total": 0, "items": []}
        if debug:
            dbg["hierarchy_keyword_filter"] = hierarchy_dbg
            dbg["reason"] = "chunk_keywords_not_found_in_neo4j"
            res["debug"] = dbg
        return res

    matched_chunk_aliases, chunk_scores, chunk_kw, chunk_match_dbg = _score_entity_keyword_rows_multi(
        query_parts or [keyword_query or query],
        chunk_keyword_rows,
        owner_alias_by_id=chunk_alias_by_id,
        keep_limit=max(limit * 5, 30),
    )
    hierarchy_dbg["chunk_match"] = chunk_match_dbg
    hierarchy_dbg["chunk_matched_aliases"] = matched_chunk_aliases[:20]
    hierarchy_dbg["chunk_matched_ids"] = _expand_ids_for_aliases(chunk_ids_by_alias, matched_chunk_aliases)[:20]
    dbg["hierarchy_keyword_filter"] = hierarchy_dbg
    if not matched_chunk_aliases:
        res = {"total": 0, "items": []}
        if debug:
            dbg["reason"] = "chunk_gate_no_match"
            res["debug"] = dbg
        return res

    ranked_chunks = sorted(
        [
            (
                chunk_id,
                float(chunk_scores.get(chunk_alias_by_id.get(chunk_id) or chunk_id, 0.0))
                + 0.035 * len(chunk_kw.get(chunk_alias_by_id.get(chunk_id) or chunk_id, []))
            )
            for chunk_id in _expand_ids_for_aliases(chunk_ids_by_alias, matched_chunk_aliases)
        ],
        key=lambda item: item[1],
        reverse=True,
    )
    dbg["ranked_chunk_count"] = len(ranked_chunks)
    if not ranked_chunks:
        res = {"total": 0, "items": []}
        if debug:
            dbg["reason"] = "ranked_chunk_count_zero"
            res["debug"] = dbg
        return res

    all_ranked_chunk_ids = _dedupe_keep_order_ids([chunk_id for chunk_id, _score in ranked_chunks])
    score_by_chunk = {chunk_id: float(score) for chunk_id, score in ranked_chunks}
    neo_map, neo_error = _neo_hierarchy_for_chunks(neo, all_ranked_chunk_ids)
    if neo_error:
        dbg["neo_error"] = neo_error
    dbg["hierarchy_source"] = "neo4j" if neo_map else "postgresql"
    pg_map = _load_pg_page_rows(pg, all_ranked_chunk_ids)
    dbg["pg_chunk_rows"] = len(pg_map)

    all_items = _build_chunk_items(
        page_chunk_ids=all_ranked_chunk_ids,
        score_by_chunk=score_by_chunk,
        chunk_top_kw=chunk_kw,
        pg_map=pg_map,
        neo_map=neo_map,
        mongo_db=mongo_db,
        category=category,
        username=username,
        pg=pg,
        dbg=dbg,
    )

    items = all_items[offset : offset + limit]
    res = {"total": len(all_items), "items": items}
    if debug:
        dbg["items_built"] = len(all_items)
        if items:
            dbg["sample_item_match"] = {
                "chunkID": items[0].get("chunkID"),
                "matchedKeywords": items[0].get("matchedKeywords"),
                "score": items[0].get("score"),
            }
        res["debug"] = dbg
    return res
