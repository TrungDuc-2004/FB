from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .gemini_topic_expander import (
    _call_generate_content,
    _clean,
    _extract_json_payload,
    _extract_text,
    _normalize_keywords,
    _rotated_keys,
)
from .mongo_client import get_mongo_client


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _uniq_keep_order(values: Sequence[str], limit: int = 24) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values or []:
        clean = _clean(value)
        key = clean.casefold()
        if not clean or key in seen:
            continue
        seen.add(key)
        out.append(clean)
        if len(out) >= limit:
            break
    return out


def _split_keywords(value: Any) -> List[str]:
    if isinstance(value, list):
        return _uniq_keep_order([_clean(x) for x in value if _clean(x)], limit=24)
    raw = _clean(value)
    if not raw:
        return []
    parts: List[str] = []
    for token in raw.replace("\n", ",").split(","):
        token = _clean(token)
        if token:
            parts.append(token)
    return _uniq_keep_order(parts, limit=24)


def _doc_keywords(doc: Optional[dict], field_name: str) -> List[str]:
    if not doc:
        return []
    return _split_keywords(doc.get(field_name))


def _build_context_lines(level: str, name: str, children: Sequence[dict], *, child_name_key: str, child_desc_key: str, child_kw_key: str) -> str:
    lines: List[str] = []
    title = _clean(name)
    if title:
        lines.append(f"{level}: {title}")
    for idx, child in enumerate(children or [], start=1):
        child_name = _clean(child.get(child_name_key))
        child_desc = _clean(child.get(child_desc_key))
        child_kws = _split_keywords(child.get(child_kw_key))
        seg: List[str] = []
        if child_name:
            seg.append(f"Ten {idx}: {child_name}")
        if child_desc:
            seg.append(f"Mo ta {idx}: {child_desc}")
        if child_kws:
            seg.append(f"Keyword {idx}: {', '.join(child_kws[:12])}")
        if seg:
            lines.append(" | ".join(seg))
    return "\n".join(lines)[:12000]


def _fallback_summary(level: str, name: str, children: Sequence[dict], *, child_name_key: str, child_kw_key: str) -> Tuple[str, List[str]]:
    clean_name = _clean(name) or level
    child_names = _uniq_keep_order([_clean(item.get(child_name_key)) for item in (children or []) if _clean(item.get(child_name_key))], limit=8)
    child_keywords: List[str] = []
    for item in children or []:
        child_keywords.extend(_split_keywords(item.get(child_kw_key)))
    keyword_values = _uniq_keep_order([clean_name, *child_names, *child_keywords], limit=16)
    if child_names:
        description = f"{clean_name} bao quat cac noi dung chinh lien quan den: {', '.join(child_names[:6])}."
    elif keyword_values:
        description = f"{clean_name} bao quat cac noi dung chinh lien quan den: {', '.join(keyword_values[:8])}."
    else:
        description = clean_name
    return description, keyword_values


def _call_desc_keywords_api(*, level: str, name: str, context_text: str, fallback_keywords: Sequence[str]) -> Tuple[str, List[str], Dict[str, Any]]:
    model = _clean(__import__("os").getenv("GEMINI_MODEL")) or "gemini-2.5-flash"
    rotated, collect_meta = _rotated_keys()
    base_meta: Dict[str, Any] = {
        "model": model,
        "key_count": collect_meta.get("key_count", 0),
        "key_sources": collect_meta.get("key_sources", []),
        "env_paths_checked": collect_meta.get("env_paths_checked", []),
        "cwd": collect_meta.get("cwd", ""),
    }
    if not rotated:
        desc, kws = _fallback_summary(level, name, [], child_name_key="name", child_kw_key="keywords")
        kws = _uniq_keep_order([*list(fallback_keywords or []), *kws], limit=16)
        base_meta["mode"] = "fallback_no_api_key"
        return desc, kws, base_meta

    prompt = (
        "Ban dang ho tro xay dung he thong tim kiem hoc lieu. "
        "Hay viet mo ta bao quat, du do dai, co kha nang bao ham noi dung de phuc vu tim kiem. "
        "Sau do trich xuat day du cac keyword co y nghia tim kiem truc tiep. "
        "Chi tra ve JSON dung schema {'description':'...','keywords':['kw1','kw2']}. "
        "Khong markdown. Khong giai thich. "
        f"Cap du lieu: {level}. Ten: {name}.\n"
        f"Ngu canh tong hop:\n{context_text}"
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "topP": 0.9,
            "maxOutputTokens": 800,
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "OBJECT",
                "properties": {
                    "description": {"type": "STRING"},
                    "keywords": {"type": "ARRAY", "items": {"type": "STRING"}},
                },
                "required": ["description", "keywords"],
            },
        },
    }

    last_meta = dict(base_meta)
    for attempt, (slot, key, source) in enumerate(rotated, start=1):
        meta = dict(base_meta)
        meta.update({"attempt": attempt, "key_slot": slot, "key_source": source})
        try:
            raw = _call_generate_content(key, model, payload, timeout=24)
            text = _extract_text(raw)
            if text:
                meta["raw_text"] = text[:1200]
            obj = _extract_json_payload(text)
            if isinstance(obj, dict):
                description = _clean(obj.get("description"))
                keywords = _normalize_keywords(obj.get("keywords") or [], limit=20)
                keywords = _uniq_keep_order([*keywords, *list(fallback_keywords or [])], limit=20)
                if description and keywords:
                    meta["mode"] = "api_json"
                    return description, keywords, meta
                if description:
                    meta["mode"] = "api_desc_only"
                    return description, _uniq_keep_order(list(fallback_keywords or []), limit=20), meta
            meta["error"] = "invalid_or_empty_json"
        except Exception as exc:
            meta["error"] = f"unexpected:{exc}"
        last_meta = meta

    desc, kws = _fallback_summary(level, name, [], child_name_key="name", child_kw_key="keywords")
    kws = _uniq_keep_order([*list(fallback_keywords or []), *kws], limit=16)
    last_meta["mode"] = "fallback_after_api_error"
    return desc, kws, last_meta


def _update_doc(collection_name: str, doc_id, set_fields: Dict[str, Any]) -> None:
    mg = get_mongo_client()
    db = mg["db"]
    db[collection_name].update_one({"_id": doc_id}, {"$set": set_fields})


def _active_docs(cursor) -> List[dict]:
    out: List[dict] = []
    for doc in cursor:
        status = _clean(doc.get("status") or "active").lower()
        if status == "hidden":
            continue
        out.append(doc)
    return out


def rebuild_hierarchy_descriptions_and_keywords(
    *,
    subject_map: str,
    topic_map: str = "",
    lesson_map: str = "",
    chunk_map: str = "",
) -> Dict[str, Any]:
    mg = get_mongo_client()
    db = mg["db"]
    now = _now()
    result: Dict[str, Any] = {"lesson": None, "topic": None, "subject": None}

    lesson_doc = db["lessons"].find_one({"lessonID": _clean(lesson_map)}) if _clean(lesson_map) else None
    if lesson_doc is None and _clean(chunk_map):
        chunk_doc = db["chunks"].find_one({"chunkID": _clean(chunk_map)})
        if chunk_doc:
            lesson_doc = db["lessons"].find_one({"lessonID": _clean(chunk_doc.get("lessonID"))})

    if lesson_doc:
        lesson_chunks = _active_docs(db["chunks"].find({"lessonID": _clean(lesson_doc.get("lessonID"))}))
        context = _build_context_lines(
            "Lesson",
            _clean(lesson_doc.get("lessonName")),
            lesson_chunks,
            child_name_key="chunkName",
            child_desc_key="chunkDescription",
            child_kw_key="keywords",
        )
        fallback_keywords = _uniq_keep_order([
            *_split_keywords(lesson_doc.get("keywordLesson")),
            *[kw for chunk in lesson_chunks for kw in _split_keywords(chunk.get("keywords"))],
        ], limit=20)
        description, keywords, meta = _call_desc_keywords_api(
            level="lesson",
            name=_clean(lesson_doc.get("lessonName")),
            context_text=context,
            fallback_keywords=fallback_keywords,
        )
        _update_doc(
            "lessons",
            lesson_doc["_id"],
            {
                "lessonDescription": description,
                "keywordLesson": keywords,
                "searchUpdatedAt": now,
                "updatedAt": now,
            },
        )
        lesson_doc["lessonDescription"] = description
        lesson_doc["keywordLesson"] = keywords
        result["lesson"] = {"lessonID": lesson_doc.get("lessonID"), "meta": meta, "keywordCount": len(keywords)}

    topic_doc = db["topics"].find_one({"topicID": _clean(topic_map)}) if _clean(topic_map) else None
    if topic_doc is None and lesson_doc:
        topic_doc = db["topics"].find_one({"topicID": _clean(lesson_doc.get("topicID"))})

    if topic_doc:
        topic_lessons = _active_docs(db["lessons"].find({"topicID": _clean(topic_doc.get("topicID"))}).sort("lessonNumber", 1))
        context = _build_context_lines(
            "Topic",
            _clean(topic_doc.get("topicName")),
            topic_lessons,
            child_name_key="lessonName",
            child_desc_key="lessonDescription",
            child_kw_key="keywordLesson",
        )
        fallback_keywords = _uniq_keep_order([
            *_split_keywords(topic_doc.get("keywordTopic")),
            *[kw for lesson in topic_lessons for kw in _split_keywords(lesson.get("keywordLesson"))],
        ], limit=20)
        description, keywords, meta = _call_desc_keywords_api(
            level="topic",
            name=_clean(topic_doc.get("topicName")),
            context_text=context,
            fallback_keywords=fallback_keywords,
        )
        _update_doc(
            "topics",
            topic_doc["_id"],
            {
                "topicDescription": description,
                "keywordTopic": keywords,
                "searchUpdatedAt": now,
                "updatedAt": now,
            },
        )
        topic_doc["topicDescription"] = description
        topic_doc["keywordTopic"] = keywords
        result["topic"] = {"topicID": topic_doc.get("topicID"), "meta": meta, "keywordCount": len(keywords)}

    subject_doc = db["subjects"].find_one({"subjectID": _clean(subject_map)}) if _clean(subject_map) else None
    if subject_doc is None and topic_doc:
        subject_doc = db["subjects"].find_one({"subjectID": _clean(topic_doc.get("subjectID"))})

    if subject_doc:
        subject_topics = _active_docs(db["topics"].find({"subjectID": _clean(subject_doc.get("subjectID"))}).sort("topicNumber", 1))
        context = _build_context_lines(
            "Subject",
            _clean(subject_doc.get("subjectName") or subject_doc.get("subjectTitle")),
            subject_topics,
            child_name_key="topicName",
            child_desc_key="topicDescription",
            child_kw_key="keywordTopic",
        )
        fallback_keywords = _uniq_keep_order([
            *_split_keywords(subject_doc.get("keywordSubject")),
            *[kw for topic in subject_topics for kw in _split_keywords(topic.get("keywordTopic"))],
        ], limit=24)
        description, keywords, meta = _call_desc_keywords_api(
            level="subject",
            name=_clean(subject_doc.get("subjectName") or subject_doc.get("subjectTitle")),
            context_text=context,
            fallback_keywords=fallback_keywords,
        )
        _update_doc(
            "subjects",
            subject_doc["_id"],
            {
                "subjectDescription": description,
                "keywordSubject": keywords,
                "searchUpdatedAt": now,
                "updatedAt": now,
            },
        )
        result["subject"] = {"subjectID": subject_doc.get("subjectID"), "meta": meta, "keywordCount": len(keywords)}

    return result
