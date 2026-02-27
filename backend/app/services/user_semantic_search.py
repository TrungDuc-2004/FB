from __future__ import annotations

import math
import re
from typing import Dict, List, Optional, Tuple

from bson import ObjectId
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ..models.model_postgre import Chunk, Class, Keyword, Lesson, Subject, Topic
from .keyword_embedding import embed_keyword_cached

_TOKEN_RE = re.compile(r"[0-9A-Za-zÀ-ỹ]+", flags=re.UNICODE)

# bump this when you replace the file so you can confirm the running code
_SERVICE_VERSION = "search_chunk_only_parent_links_v2_total_consistent"


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


def _load_by_oids(mongo_db, col: str, oid_hex_list: List[str]) -> Dict[str, dict]:
    """Return map oid_hex -> doc. No category filter."""
    out: Dict[str, dict] = {}
    if not oid_hex_list:
        return out
    try:
        # dedupe
        seen = set()
        oids = []
        for x in oid_hex_list:
            if not _valid_object_id_hex(x):
                continue
            if x in seen:
                continue
            seen.add(x)
            oids.append(ObjectId(x))

        if not oids:
            return out

        docs = list(mongo_db[col].find({"_id": {"$in": oids}}))
        for d in docs:
            out[str(d.get("_id"))] = d
    except Exception:
        return out
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
    """Semantic search (chunk-only output, with parent file links).

    Mục tiêu đúng theo yêu cầu hiện tại của bạn:
    - Kết quả hiển thị: chỉ CHUNK
    - Từ CHUNK suy ra Lesson/Topic/Subject và đính kèm URL file của từng cấp (nếu có)

    Fix quan trọng trong bản này:
    - `total` luôn KHỚP với số item thực tế (không còn kiểu 2/3)
    - Nếu có chunk bị hidden hoặc thiếu metadata, nó sẽ bị loại khỏi danh sách và `total` sẽ giảm tương ứng.
    """

    query = (q or "").strip()
    if not query:
        return {"total": 0, "items": []}

    dbg: Dict[str, object] = {"service_version": _SERVICE_VERSION, "category": category}

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

    ranked_all: List[Tuple[str, float]] = sorted(chunk_best.items(), key=lambda x: x[1], reverse=True)
    dbg["ranked_chunks_scored"] = len(ranked_all)

    if not ranked_all:
        res = {"total": 0, "items": []}
        if debug:
            res["debug"] = dbg
        return res

    # ----
    # IMPORTANT: total/offset/limit phải dựa trên các chunk "có thể hiển thị".
    # Ta sẽ lọc trước: chunk tồn tại trong PG + không hidden trong Mongo (nếu có doc).
    # ----

    ranked_chunk_ids = [cid for cid, _ in ranked_all]

    # Load minimal chunk rows from PG to know mongo_id + lesson_id (không join chain để tránh rớt).
    try:
        stmt = (
            select(Chunk.chunk_id, Chunk.chunk_name, Chunk.chunk_type, Chunk.mongo_id, Chunk.lesson_id)
            .where(Chunk.chunk_id.in_(ranked_chunk_ids))
        )
        chunk_rows = list(pg.execute(stmt).all())
    except SQLAlchemyError:
        chunk_rows = []

    pg_chunk_min: Dict[str, dict] = {}
    chunk_mongo_hex_all: List[str] = []
    for r in chunk_rows:
        cid, cname, ctype, cmongo, lid = r
        pg_chunk_min[cid] = {
            "chunkID": cid,
            "chunkName": cname,
            "chunkType": ctype,
            "chunkMongoId": cmongo,
            "lessonID": lid,
        }
        if _valid_object_id_hex(cmongo or ""):
            chunk_mongo_hex_all.append(cmongo)

    dbg["pg_chunk_min_rows"] = len(pg_chunk_min)

    # Load mongo chunk docs to filter hidden (if found). No category filter.
    mongo_chunks_by_oid_all = _load_by_oids(mongo_db, "chunks", chunk_mongo_hex_all)
    dbg["mongo_chunk_docs_loaded"] = len(mongo_chunks_by_oid_all)

    visible_ranked: List[Tuple[str, float]] = []
    dropped_missing_pg = 0
    dropped_hidden = 0

    for cid, score in ranked_all:
        base = pg_chunk_min.get(cid)
        if not base:
            dropped_missing_pg += 1
            continue

        doc = None
        oid_hex = base.get("chunkMongoId")
        if _valid_object_id_hex(oid_hex or ""):
            doc = mongo_chunks_by_oid_all.get(oid_hex)

        if doc is not None and not _status_visible(doc):
            dropped_hidden += 1
            continue

        visible_ranked.append((cid, score))

    dbg["dropped_missing_pg"] = dropped_missing_pg
    dbg["dropped_hidden"] = dropped_hidden

    total = len(visible_ranked)
    dbg["ranked_chunks_visible"] = total

    page_pairs = visible_ranked[offset : offset + limit]
    page_chunk_ids = [cid for cid, _ in page_pairs]
    score_by_chunk = dict(page_pairs)

    if not page_chunk_ids:
        res = {"total": total, "items": []}
        if debug:
            res["debug"] = dbg
        return res

    # 5) Build hierarchy from PG (không join chain cứng)
    # We already have chunk minimal; now load lessons/topics/subjects/classes for the page.

    lesson_ids = []
    for cid in page_chunk_ids:
        lid = (pg_chunk_min.get(cid) or {}).get("lessonID")
        if lid:
            lesson_ids.append(lid)

    # Lessons
    pg_lessons: Dict[str, dict] = {}
    topic_ids = []
    lesson_mongo_hex: List[str] = []
    if lesson_ids:
        try:
            stmt = select(Lesson.lesson_id, Lesson.lesson_name, Lesson.mongo_id, Lesson.topic_id).where(
                Lesson.lesson_id.in_(list(set(lesson_ids)))
            )
            for lid, lname, lmongo, tid in pg.execute(stmt).all():
                pg_lessons[lid] = {"lessonID": lid, "lessonName": lname, "mongoId": lmongo, "topicID": tid}
                if tid:
                    topic_ids.append(tid)
                if _valid_object_id_hex(lmongo or ""):
                    lesson_mongo_hex.append(lmongo)
        except SQLAlchemyError:
            pass

    # Topics
    pg_topics: Dict[str, dict] = {}
    subject_ids = []
    topic_mongo_hex: List[str] = []
    if topic_ids:
        try:
            stmt = select(Topic.topic_id, Topic.topic_name, Topic.mongo_id, Topic.subject_id).where(
                Topic.topic_id.in_(list(set(topic_ids)))
            )
            for tid, tname, tmongo, sid in pg.execute(stmt).all():
                pg_topics[tid] = {"topicID": tid, "topicName": tname, "mongoId": tmongo, "subjectID": sid}
                if sid:
                    subject_ids.append(sid)
                if _valid_object_id_hex(tmongo or ""):
                    topic_mongo_hex.append(tmongo)
        except SQLAlchemyError:
            pass

    # Subjects
    pg_subjects: Dict[str, dict] = {}
    class_ids = []
    subject_mongo_hex: List[str] = []
    if subject_ids:
        try:
            stmt = select(Subject.subject_id, Subject.subject_name, Subject.mongo_id, Subject.class_id).where(
                Subject.subject_id.in_(list(set(subject_ids)))
            )
            for sid, sname, smongo, cid in pg.execute(stmt).all():
                pg_subjects[sid] = {"subjectID": sid, "subjectName": sname, "mongoId": smongo, "classID": cid}
                if cid:
                    class_ids.append(cid)
                if _valid_object_id_hex(smongo or ""):
                    subject_mongo_hex.append(smongo)
        except SQLAlchemyError:
            pass

    # Classes
    pg_classes: Dict[str, dict] = {}
    if class_ids:
        try:
            stmt = select(Class.class_id, Class.class_name, Class.mongo_id).where(Class.class_id.in_(list(set(class_ids))))
            for cid, cname, cmongo in pg.execute(stmt).all():
                pg_classes[cid] = {"classID": cid, "className": cname, "mongoId": cmongo}
        except SQLAlchemyError:
            pass

    # 6) Load parent docs from Mongo to get URL (no category filter)
    mongo_lessons_by_oid = _load_by_oids(mongo_db, "lessons", lesson_mongo_hex)
    mongo_topics_by_oid = _load_by_oids(mongo_db, "topics", topic_mongo_hex)
    mongo_subjects_by_oid = _load_by_oids(mongo_db, "subjects", subject_mongo_hex)

    # 7) Build items
    items: List[dict] = []

    for cid in page_chunk_ids:
        base = pg_chunk_min.get(cid)
        if not base:
            continue

        s = float(score_by_chunk.get(cid, 0.0))

        # mongo chunk doc (for url/name/desc)
        chunk_doc = None
        oid_hex = base.get("chunkMongoId")
        if _valid_object_id_hex(oid_hex or ""):
            chunk_doc = mongo_chunks_by_oid_all.get(oid_hex)

        # hierarchy
        lesson_obj = {"lessonID": "", "lessonName": "", "lessonUrl": ""}
        topic_obj = {"topicID": "", "topicName": "", "topicUrl": ""}
        subject_obj = {"subjectID": "", "subjectName": "", "subjectUrl": ""}
        class_obj = {"classID": "", "className": ""}

        lid = base.get("lessonID")
        if lid and lid in pg_lessons:
            l = pg_lessons[lid]
            lesson_obj["lessonID"] = l.get("lessonID") or ""
            lesson_obj["lessonName"] = l.get("lessonName") or ""

            l_oid = l.get("mongoId")
            if _valid_object_id_hex(l_oid or ""):
                ldoc = mongo_lessons_by_oid.get(l_oid)
                if ldoc and _status_visible(ldoc):
                    lesson_obj["lessonUrl"] = ldoc.get("lessonUrl") or ""

            tid = l.get("topicID")
            if tid and tid in pg_topics:
                t = pg_topics[tid]
                topic_obj["topicID"] = t.get("topicID") or ""
                topic_obj["topicName"] = t.get("topicName") or ""

                t_oid = t.get("mongoId")
                if _valid_object_id_hex(t_oid or ""):
                    tdoc = mongo_topics_by_oid.get(t_oid)
                    if tdoc and _status_visible(tdoc):
                        topic_obj["topicUrl"] = tdoc.get("topicUrl") or ""

                sid = t.get("subjectID")
                if sid and sid in pg_subjects:
                    sub = pg_subjects[sid]
                    subject_obj["subjectID"] = sub.get("subjectID") or ""
                    subject_obj["subjectName"] = sub.get("subjectName") or ""

                    s_oid = sub.get("mongoId")
                    if _valid_object_id_hex(s_oid or ""):
                        sdoc = mongo_subjects_by_oid.get(s_oid)
                        if sdoc and _status_visible(sdoc):
                            subject_obj["subjectUrl"] = sdoc.get("subjectUrl") or ""

                    cid2 = sub.get("classID")
                    if cid2 and cid2 in pg_classes:
                        cl = pg_classes[cid2]
                        class_obj["classID"] = cl.get("classID") or ""
                        class_obj["className"] = cl.get("className") or ""

        item = {
            "type": "chunk",
            "id": cid,
            "name": (chunk_doc.get("chunkName") if chunk_doc else None) or base.get("chunkName") or cid,
            "score": s,
            "chunkID": cid,
            "chunkName": (chunk_doc.get("chunkName") if chunk_doc else None) or base.get("chunkName"),
            "chunkType": (chunk_doc.get("chunkType") if chunk_doc else None) or base.get("chunkType"),
            "chunkUrl": (chunk_doc.get("chunkUrl") if chunk_doc else None),
            "chunkDescription": (chunk_doc.get("chunkDescription") if chunk_doc else None),
            "keywords": (chunk_doc.get("keywords") if chunk_doc else None) or [],
            "isSaved": False,
            "class": class_obj,
            "subject": subject_obj,
            "topic": topic_obj,
            "lesson": lesson_obj,
        }

        # saved check
        try:
            saved = mongo_db["user_saved_chunks"].find_one({"username": username, "chunkID": cid})
            item["isSaved"] = bool(saved)
        except Exception:
            pass

        items.append(item)

    # total must match visible ranked count (after filters)
    res = {"total": total, "items": items}

    if debug:
        dbg["items_built"] = len(items)
        dbg["sample_item"] = (
            {
                "chunkID": items[0].get("chunkID"),
                "chunkUrl": bool(items[0].get("chunkUrl")),
                "lessonUrl": bool((items[0].get("lesson") or {}).get("lessonUrl")),
                "topicUrl": bool((items[0].get("topic") or {}).get("topicUrl")),
                "subjectUrl": bool((items[0].get("subject") or {}).get("subjectUrl")),
            }
            if items
            else {}
        )
        res["debug"] = dbg

    return res
