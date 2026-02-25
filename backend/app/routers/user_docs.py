from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from ..services.mongo_client import get_mongo_client
from ..services.postgre_client import get_db
from ..services.neo_client import get_neo4j_session
from ..services.user_semantic_search import semantic_search
from ..services.doc_preview import get_view_url


router = APIRouter(prefix="/user/docs", tags=["UserDocs"])

mongo = get_mongo_client()
db_mongo = mongo["db"]

COL_CLASSES = "classes"
COL_SUBJECTS = "subjects"
COL_TOPICS = "topics"
COL_LESSONS = "lessons"
COL_CHUNKS = "chunks"
COL_KEYWORDS = "keywords"
COL_SAVED = "user_saved_chunks"


def _now():
    return datetime.now(timezone.utc)


def _actor(request: Request) -> str:
    return request.headers.get("x-user") or request.headers.get("x-actor") or "user"


def _active_status_q():
    # Your system uses "activity" (active) and "hidden" (soft delete)
    return {"$in": ["active", "activity"]}


def _cat_variants(category: str):
    c = (category or "").strip()
    if not c:
        return []
    low = c.lower()
    base = low[:-1] if low.endswith("s") else low
    out = {c, low, base, base + "s"}
    return [x for x in out if x]


def _ensure_saved_index():
    try:
        db_mongo[COL_SAVED].create_index(
            [("username", 1), ("chunkID", 1), ("category", 1)],
            unique=True,
            name="uniq_user_chunk",
        )
        db_mongo[COL_SAVED].create_index([("username", 1), ("updatedAt", -1)], name="idx_user_updated")
    except Exception:
        pass


_ensure_saved_index()


def _get_chunk_full(chunk: dict, *, category: str, username: str) -> dict:
    cats = _cat_variants(category)
    st = _active_status_q()

    lesson = db_mongo[COL_LESSONS].find_one({"lessonID": chunk.get("lessonID"), "lessonCategory": {"$in": cats}, "status": st}) or {}
    topic = db_mongo[COL_TOPICS].find_one({"topicID": lesson.get("topicID"), "topicCategory": {"$in": cats}, "status": st}) or {}
    subject = db_mongo[COL_SUBJECTS].find_one({"subjectID": topic.get("subjectID"), "subjectCategory": {"$in": cats}, "status": st}) or {}
    cls = db_mongo[COL_CLASSES].find_one({"classID": subject.get("classID"), "classCategory": {"$in": cats}, "status": st}) or {}

    saved = db_mongo[COL_SAVED].find_one({"username": username, "chunkID": chunk.get("chunkID"), "category": category})

    return {
        "chunkID": chunk.get("chunkID"),
        "chunkName": chunk.get("chunkName"),
        "chunkType": chunk.get("chunkType"),
        "chunkUrl": chunk.get("chunkUrl"),
        "chunkDescription": chunk.get("chunkDescription"),
        "keywords": chunk.get("keywords") or [],
        "isSaved": bool(saved),
        "class": {"classID": cls.get("classID"), "className": cls.get("className")},
        "subject": {"subjectID": subject.get("subjectID"), "subjectName": subject.get("subjectName")},
        "topic": {"topicID": topic.get("topicID"), "topicName": topic.get("topicName")},
        "lesson": {
            "lessonID": lesson.get("lessonID"),
            "lessonName": lesson.get("lessonName"),
            "lessonType": lesson.get("lessonType"),
        },
    }


@router.get("/classes")
def list_classes(category: str = Query("document")):
    cats = _cat_variants(category)
    cur = db_mongo[COL_CLASSES].find({"status": _active_status_q(), "classCategory": {"$in": cats}}, {"_id": 0})
    items = sorted(list(cur), key=lambda x: (x.get("className") or x.get("classID") or ""))
    return {"total": len(items), "items": items}


@router.get("/subjects")
def list_subjects(classID: str = Query(""), category: str = Query("document")):
    if not classID:
        return {"total": 0, "items": []}
    cats = _cat_variants(category)
    cur = db_mongo[COL_SUBJECTS].find(
        {"status": _active_status_q(), "subjectCategory": {"$in": cats}, "classID": classID},
        {"_id": 0},
    )
    items = sorted(list(cur), key=lambda x: (x.get("subjectName") or x.get("subjectID") or ""))
    return {"total": len(items), "items": items}


@router.get("/topics")
def list_topics(subjectID: str = Query(""), category: str = Query("document")):
    if not subjectID:
        return {"total": 0, "items": []}
    cats = _cat_variants(category)
    cur = db_mongo[COL_TOPICS].find(
        {"status": _active_status_q(), "topicCategory": {"$in": cats}, "subjectID": subjectID},
        {"_id": 0},
    )
    items = sorted(list(cur), key=lambda x: (x.get("topicName") or x.get("topicID") or ""))
    return {"total": len(items), "items": items}


@router.get("/lessons")
def list_lessons(topicID: str = Query(""), category: str = Query("document")):
    if not topicID:
        return {"total": 0, "items": []}
    cats = _cat_variants(category)
    cur = db_mongo[COL_LESSONS].find(
        {"status": _active_status_q(), "lessonCategory": {"$in": cats}, "topicID": topicID},
        {"_id": 0},
    )
    items = sorted(list(cur), key=lambda x: (x.get("lessonName") or x.get("lessonID") or ""))
    return {"total": len(items), "items": items}


@router.get("/chunks")
def list_chunks(
    request: Request,
    lessonID: str = Query(""),
    category: str = Query("document"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    sort: str = Query("name"),
):
    if not lessonID:
        return {"total": 0, "items": []}

    cats = _cat_variants(category)

    sort_spec = [("chunkName", 1)]
    if sort == "updated":
        sort_spec = [("updatedAt", -1)]

    cur = (
        db_mongo[COL_CHUNKS]
        .find({"status": _active_status_q(), "chunkCategory": {"$in": cats}, "lessonID": lessonID}, {"_id": 0})
        .sort(sort_spec)
        .skip(offset)
        .limit(limit)
    )
    username = _actor(request)
    items = [_get_chunk_full(c, category=category, username=username) for c in cur]

    total = db_mongo[COL_CHUNKS].count_documents({"status": _active_status_q(), "chunkCategory": {"$in": cats}, "lessonID": lessonID})
    return {"total": total, "items": items}


@router.get("/search")
def search(
    request: Request,
    q: str = Query(""),
    classID: str = Query(""),
    subjectID: str = Query(""),
    topicID: str = Query(""),
    lessonID: str = Query(""),
    category: str = Query("document"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    debug: int = Query(0),
    pg: Session = Depends(get_db),
    neo=Depends(get_neo4j_session),
):
    username = _actor(request)
    res = semantic_search(
        q=q,
        category=category,
        classID=classID,
        subjectID=subjectID,
        topicID=topicID,
        lessonID=lessonID,
        limit=limit,
        offset=offset,
        username=username,
        pg=pg,
        neo=neo,
        mongo_db=db_mongo,
        debug=bool(debug),
    )
    return res


@router.get("/{chunkID}")
def get_doc_detail(request: Request, chunkID: str, category: str = Query("document")):
    cats = _cat_variants(category)
    chunk = db_mongo[COL_CHUNKS].find_one(
        {"chunkID": chunkID, "chunkCategory": {"$in": cats}, "status": _active_status_q()},
        {"_id": 0},
    )
    if not chunk:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài liệu")

    username = _actor(request)
    doc = _get_chunk_full(chunk, category=category, username=username)

    try:
        kws = list(db_mongo[COL_KEYWORDS].find({"chunkID": chunkID, "status": _active_status_q()}, {"_id": 0, "keywordEmbedding": 0}))
        doc["keywords"] = [k.get("keywordName") for k in kws if k.get("keywordName")]
        doc["keywordItems"] = kws
    except Exception:
        doc["keywordItems"] = []

    lesson_id = chunk.get("lessonID")
    rel_cur = (
        db_mongo[COL_CHUNKS]
        .find({"status": _active_status_q(), "chunkCategory": {"$in": cats}, "lessonID": lesson_id}, {"_id": 0})
        .sort([("chunkName", 1)])
    )
    related = [_get_chunk_full(c, category=category, username=username) for c in rel_cur if c.get("chunkID") != chunkID]
    doc["related"] = related

    return doc


@router.get("/{chunkID}/view")
def view_doc(chunkID: str, category: str = Query("document")):
    cats = _cat_variants(category)
    chunk = db_mongo[COL_CHUNKS].find_one(
        {"chunkID": chunkID, "chunkCategory": {"$in": cats}, "status": _active_status_q()},
        {"_id": 0},
    )
    if not chunk:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài liệu")

    original_url = chunk.get("chunkUrl") or ""
    if not original_url:
        raise HTTPException(status_code=400, detail="Tài liệu không có URL")

    return get_view_url(original_url=original_url, chunk_id=chunkID)


@router.post("/{chunkID}/save")
def toggle_save(request: Request, chunkID: str, category: str = Query("document")):
    username = _actor(request)
    now = _now()

    key = {"username": username, "chunkID": chunkID, "category": category}
    existing = db_mongo[COL_SAVED].find_one(key)
    if existing:
        db_mongo[COL_SAVED].delete_one({"_id": existing.get("_id")})
        return {"saved": False}

    doc: Any = {**key, "createdAt": now, "updatedAt": now}
    db_mongo[COL_SAVED].insert_one(doc)
    return {"saved": True}


@router.get("/saved/list")
def list_saved(request: Request, category: str = Query("document"), limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)):
    username = _actor(request)

    cur = (
        db_mongo[COL_SAVED]
        .find({"username": username, "category": category}, {"_id": 0, "chunkID": 1})
        .sort([("updatedAt", -1)])
        .skip(offset)
        .limit(limit)
    )
    chunk_ids = [x.get("chunkID") for x in cur if x.get("chunkID")]

    cats = _cat_variants(category)
    st = _active_status_q()

    chunks = list(db_mongo[COL_CHUNKS].find({"chunkID": {"$in": chunk_ids}, "chunkCategory": {"$in": cats}, "status": st}, {"_id": 0}))
    by_id = {c.get("chunkID"): c for c in chunks if c.get("chunkID")}

    items = []
    for cid in chunk_ids:
        cdoc = by_id.get(cid)
        if not cdoc:
            continue
        items.append(_get_chunk_full(cdoc, category=category, username=username))

    total = db_mongo[COL_SAVED].count_documents({"username": username, "category": category})
    return {"total": total, "items": items}
