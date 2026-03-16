from __future__ import annotations

import math
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from bson import ObjectId
from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ..models.model_postgre import Chunk, Class, Image, Keyword, Lesson, Subject, Topic, Video
from .gemini_topic_expander import expand_topic_keywords_debug
from .keyword_embedding import embed_keyword_cached

_TOKEN_RE = re.compile(r"[0-9A-Za-zÀ-ỹ]+", flags=re.UNICODE)
_SERVICE_VERSION = "search_semantic_keyword_graph_v4_neo_only"

_STOP = {
    "a", "an", "and", "các", "cái", "cho", "có", "của", "dạng", "đến", "giúp",
    "hãy", "in", "không", "kiếm", "là", "liên", "muốn", "một", "nào", "những",
    "of", "or", "ở", "quan", "the", "to", "tài", "tìm", "trong", "tôi", "và",
    "về", "với", "xin", "đó", "này", "hoặc", "cần", "liệu",
}


def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


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


def _candidate_chunk_ids_from_filters_pg(
    *,
    pg: Session,
    classID: str,
    subjectID: str,
    topicID: str,
    lessonID: str,
) -> Optional[List[str]]:
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
            return [r[0] for r in pg.execute(stmt).all()]

        if subjectID:
            stmt = (
                select(Chunk.chunk_id)
                .join(Lesson, Lesson.lesson_id == Chunk.lesson_id)
                .join(Topic, Topic.topic_id == Lesson.topic_id)
                .where(Topic.subject_id == subjectID)
            )
            return [r[0] for r in pg.execute(stmt).all()]

        if classID:
            stmt = (
                select(Chunk.chunk_id)
                .join(Lesson, Lesson.lesson_id == Chunk.lesson_id)
                .join(Topic, Topic.topic_id == Lesson.topic_id)
                .join(Subject, Subject.subject_id == Topic.subject_id)
                .where(Subject.class_id == classID)
            )
            return [r[0] for r in pg.execute(stmt).all()]

        return None
    except Exception:
        return None


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
            has_hit = bool(list(pg.execute(stmt).all()))
            if has_hit:
                filtered.append(term)
        return filtered, debug
    except Exception:
        return terms, debug


def _query_embedding_text(raw_query: str, core_query: str, gemini_terms: List[str]) -> str:
    parts: List[str] = []
    if core_query:
        parts.append(core_query)
    elif _norm_spaces(raw_query):
        parts.append(_norm_spaces(raw_query.lower()))
    parts.extend(gemini_terms or [])
    return _norm_spaces(" ".join(_dedupe_keep_order(parts)))


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


def _load_keyword_rows(neo, pg: Session, cand_chunks: Optional[List[str]]) -> Tuple[List[Tuple[str, str, str, List[float]]], str, Optional[str]]:
    neo_rows, neo_error = _load_keyword_rows_from_neo(neo, cand_chunks)
    return neo_rows, "neo4j_only", neo_error


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
    min_score = max(0.18, top_score * 0.55)
    filtered = [item for item in matches if float(item["score"]) >= min_score]
    if not filtered:
        filtered = matches[:100]
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
                "lesson": {
                    "lessonID": str(record.get("lesson_id") or "").strip(),
                    "lessonName": str(record.get("lesson_name") or "").strip(),
                },
                "topic": {
                    "topicID": str(record.get("topic_id") or "").strip(),
                    "topicName": str(record.get("topic_name") or "").strip(),
                },
                "subject": {
                    "subjectID": str(record.get("subject_id") or "").strip(),
                    "subjectName": str(record.get("subject_name") or "").strip(),
                },
                "class": {
                    "classID": str(record.get("class_id") or "").strip(),
                    "className": str(record.get("class_name") or "").strip(),
                },
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
            .where(Chunk.chunk_id.in_(chunk_ids))
        )
        rows = list(pg.execute(stmt).all())
    except SQLAlchemyError:
        return {}

    out: Dict[str, dict] = {}
    for r in rows:
        (
            chunk_id,
            chunk_name,
            chunk_type,
            chunk_mongo_id,
            lesson_id,
            lesson_name,
            lesson_mongo_id,
            topic_id,
            topic_name,
            topic_mongo_id,
            subject_id,
            subject_name,
            subject_mongo_id,
            class_id,
            class_name,
            class_mongo_id,
        ) = r
        out[str(chunk_id)] = {
            "chunkID": str(chunk_id),
            "chunkName": chunk_name,
            "chunkType": chunk_type,
            "chunkMongoId": chunk_mongo_id,
            "lesson": {"lessonID": lesson_id, "lessonName": lesson_name, "mongoId": lesson_mongo_id},
            "topic": {"topicID": topic_id, "topicName": topic_name, "mongoId": topic_mongo_id},
            "subject": {"subjectID": subject_id, "subjectName": subject_name, "mongoId": subject_mongo_id},
            "class": {"classID": class_id, "className": class_name, "mongoId": class_mongo_id},
        }
    return out


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

    cand_chunks = _candidate_chunk_ids_from_filters_pg(
        pg=pg,
        classID=classID,
        subjectID=subjectID,
        topicID=topicID,
        lessonID=lessonID,
    )
    dbg["candidate_chunk_scope"] = None if cand_chunks is None else len(cand_chunks)

    core_query = _core_query_text(query)
    dbg["core_query"] = core_query
    dbg["tokens_no_stop"] = _tokens_no_stop(query)

    gemini_terms, gem_dbg = _maybe_expand_with_gemini(core_query or query, cand_chunks, pg)
    dbg["gemini_terms"] = gemini_terms
    if gem_dbg:
        if gem_dbg.get("model"):
            dbg["gemini_model"] = gem_dbg.get("model")
        if gem_dbg.get("error"):
            dbg["gemini_error"] = gem_dbg.get("error")
        if gem_dbg.get("mode"):
            dbg["gemini_mode"] = gem_dbg.get("mode")

    semantic_query = _query_embedding_text(query, core_query, gemini_terms)
    dbg["semantic_query"] = semantic_query
    query_embedding = embed_keyword_cached(semantic_query)

    dbg["embedding_source"] = "neo4j_only"
    keyword_rows, keyword_source, keyword_source_error = _load_keyword_rows(neo, pg, cand_chunks)
    dbg["keyword_rows"] = len(keyword_rows)
    dbg["keyword_embedding_source"] = keyword_source
    if keyword_source_error:
        dbg["keyword_source_error"] = keyword_source_error
    if not keyword_rows:
        res = {"total": 0, "items": []}
        if debug:
            res["debug"] = dbg
        return res

    keyword_hits, min_score = _score_keywords(query_embedding, keyword_rows)
    dbg["keyword_hit_count"] = len(keyword_hits)
    dbg["keyword_min_score"] = min_score
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
            "chunkUrl": (chunk_doc.get("chunkUrl") if chunk_doc else None),
            "chunkDescription": (chunk_doc.get("chunkDescription") if chunk_doc else None),
            "keywords": _read_keywords_from_chunk_doc(chunk_doc),
            "matchedKeywords": matched_kw,
            "images": images,
            "videos": videos,
            "mediaSummary": {
                "totalImages": len(images),
                "totalVideos": len(videos),
                "byFollowType": media_sources,
            },
            "isSaved": False,
            "class": {"classID": class_id_v, "className": class_name},
            "subject": {
                "subjectID": subject_id_v,
                "subjectName": subject_name,
                "subjectDescription": ((subject_doc.get("subjectTitle") if subject_doc else None) or (subject_doc.get("description") if subject_doc else None) or ""),
                "subjectUrl": (subject_doc.get("subjectUrl") if subject_doc and _status_visible(subject_doc) else ""),
            },
            "topic": {
                "topicID": topic_id_v,
                "topicName": topic_name,
                "topicDescription": ((topic_doc.get("topicDescription") if topic_doc else None) or (topic_doc.get("topic_description") if topic_doc else None) or (topic_doc.get("description") if topic_doc else None) or ""),
                "topicUrl": (topic_doc.get("topicUrl") if topic_doc and _status_visible(topic_doc) else ""),
            },
            "lesson": {
                "lessonID": lesson_id_v,
                "lessonName": lesson_name,
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
