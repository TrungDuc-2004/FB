from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .gemini_topic_expander import (
    _call_generate_content,
    _clean,
    _extract_json_payload,
    _extract_text,
    _rotated_keys,
)
from .mongo_client import get_mongo_client


def _now() -> datetime:
    return datetime.now(timezone.utc)


_SPLIT_RE = re.compile(r"[,;\n\r|]+")
_WORD_RE = re.compile(r"[A-Za-zÀ-ỹ0-9]+", re.UNICODE)
_MAP_ID_RE = re.compile(r"^(?:TH|L)\d+(?:_CD\d+)?(?:_B\d+)?(?:_C\d+)?$", re.I)
_MAP_TOKEN_RE = re.compile(r"^(?:TH|L|CD|B|C)\d+$", re.I)
_SHORT_CODE_RE = re.compile(r"^[A-Z0-9]{1,4}$")
_GENERIC_SINGLE_RE = re.compile(
    r"^(?:khái|niệm|liên|quan|thuộc|nội|dung|chính|kiến|thức|mạng|tính|bài|mục|chủ|đề)$",
    re.I,
)
_SINGLE_WORD_ALLOWLIST = {
    "internet",
    "byte",
    "bit",
    "wifi",
    "sql",
    "html",
    "css",
    "python",
    "scratch",
    "linux",
    "windows",
    "word",
    "excel",
    "powerpoint",
    "google",
    "facebook",
    "youtube",
    "email",
    "tcp",
    "udp",
    "ip",
    "dns",
    "web",
}
_GENERIC_PHRASE_PARTS = (
    " là nội dung",
    " là chủ đề",
    " là bài học",
    " bao quát",
    " liên quan đến",
    " thuộc bài",
    " thuộc chủ đề",
    " thuộc môn",
    " thuộc lớp",
    " nội dung chính",
    " kiến thức chính",
)


def _clean_phrase(value: Any) -> str:
    text = _clean(value)
    text = re.sub(r"\s+", " ", text)
    text = text.strip(" ,.;:-_`'\"[]{}()")
    return text


def _trim_generic_phrase(text: str) -> str:
    out = _clean_phrase(text)
    low = out.casefold()
    for part in _GENERIC_PHRASE_PARTS:
        idx = low.find(part)
        if idx > 0:
            out = _clean_phrase(out[:idx])
            low = out.casefold()
    return out


def _normalize_keyword_candidate(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("term") or value.get("keyword") or value.get("name") or ""

    text = _trim_generic_phrase(_clean_phrase(value))
    if not text:
        return ""

    compact = re.sub(r"[\s._-]+", "", text)
    if _MAP_ID_RE.match(compact):
        return ""

    raw_tokens = [tok for tok in re.findall(r"[A-Za-zÀ-ỹ0-9]+", text) if tok]
    if not raw_tokens:
        return ""

    if _MAP_TOKEN_RE.match(raw_tokens[0]):
        return ""

    if all(_SHORT_CODE_RE.match(tok) or _MAP_TOKEN_RE.match(tok) for tok in raw_tokens):
        return ""

    tokens = [tok for tok in _WORD_RE.findall(text) if tok]
    if not tokens:
        return ""

    low_joined = " ".join(tok.casefold() for tok in tokens)
    if low_joined in {
        "nội dung",
        "kiến thức",
        "mạng máy tính và internet là nội dung",
    }:
        return ""

    if len(tokens) == 1:
        low = tokens[0].casefold()
        if low not in _SINGLE_WORD_ALLOWLIST:
            return ""
        return tokens[0]

    if len(tokens) > 8:
        tokens = tokens[:8]

    if all(_GENERIC_SINGLE_RE.match(tok) for tok in tokens):
        return ""

    return " ".join(tokens)


def _uniq_keep_order(values: Sequence[str], limit: Optional[int] = 24) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values or []:
        clean = _clean(value)
        key = clean.casefold()
        if not clean or key in seen:
            continue
        seen.add(key)
        out.append(clean)
        if limit and len(out) >= limit:
            break
    return out


def _normalize_keywords(values: Any, *, limit: Optional[int] = 24) -> List[str]:
    seq: List[Any]
    if isinstance(values, list):
        seq = values
    else:
        raw = _clean(values)
        if not raw:
            return []
        seq = [x.strip() for x in _SPLIT_RE.split(raw) if x.strip()]

    normalized = [_normalize_keyword_candidate(x) for x in seq]
    normalized = [x for x in normalized if x]

    multi = [x for x in normalized if len(x.split()) >= 2]
    single = [x for x in normalized if len(x.split()) == 1]
    return _uniq_keep_order([*multi, *single], limit=limit)


def _split_keywords(value: Any, *, limit: Optional[int] = 24) -> List[str]:
    return _normalize_keywords(value, limit=limit)


def _merge_keywords(*groups: Sequence[Any], limit: Optional[int]) -> List[str]:
    merged: List[Any] = []
    for group in groups:
        if not group:
            continue
        merged.extend(group)
    return _normalize_keywords(merged, limit=limit)


def _description_seed_keywords(text: str, *, limit: Optional[int]) -> List[str]:
    raw = _clean(text)
    if not raw:
        return []
    seeds: List[str] = []
    for line in raw.splitlines():
        line = _clean(line)
        if not line:
            continue
        for seg in re.split(r"[:,]", line):
            seg = _clean(seg)
            if seg and 2 <= len(seg.split()) <= 8:
                seeds.append(seg)
    return _normalize_keywords(seeds, limit=limit)


def _keyword_limit(level: str) -> int:
    if level == "lesson":
        return 10
    if level == "topic":
        return 80
    if level == "subject":
        return 120
    return 20


def _active_docs(cursor) -> List[dict]:
    out: List[dict] = []
    for doc in cursor:
        status = _clean(doc.get("status") or "active").lower()
        if status == "hidden":
            continue
        out.append(doc)
    return out


def _update_doc(collection_name: str, doc_id, set_fields: Dict[str, Any]) -> None:
    mg = get_mongo_client()
    db = mg["db"]
    db[collection_name].update_one({"_id": doc_id}, {"$set": set_fields})


def _build_context_lines(
    level: str,
    name: str,
    children: Sequence[dict],
    *,
    child_name_key: str,
    child_desc_key: str,
    child_kw_key: str,
    existing_description: str = "",
    existing_keywords: Sequence[str] | None = None,
) -> str:
    lines: List[str] = []

    title = _clean(name)
    if title:
        lines.append(f"{level}: {title}")

    if _clean(existing_description):
        lines.append(f"Mô tả hiện có: {_clean(existing_description)}")

    if existing_keywords:
        kws = _uniq_keep_order([_clean(x) for x in existing_keywords if _clean(x)], limit=40)
        if kws:
            lines.append(f"Keyword hiện có: {', '.join(kws)}")

    for idx, child in enumerate(children or [], start=1):
        child_name = _clean(child.get(child_name_key))
        child_desc = _clean(child.get(child_desc_key))
        child_kws = _split_keywords(child.get(child_kw_key), limit=20)

        seg: List[str] = []
        if child_name:
            seg.append(f"Tên {idx}: {child_name}")
        if child_desc:
            seg.append(f"Mô tả {idx}: {child_desc}")
        if child_kws:
            seg.append(f"Keyword {idx}: {', '.join(child_kws)}")

        if seg:
            lines.append(" | ".join(seg))

    return "\n".join(lines)[:18000]


def _fallback_summary(
    level: str,
    name: str,
    children: Sequence[dict],
    *,
    child_name_key: str,
    child_kw_key: str,
    existing_description: str = "",
    existing_keywords: Sequence[str] | None = None,
) -> Tuple[str, List[str]]:
    clean_name = _clean(name) or level
    child_names = _uniq_keep_order(
        [_clean(item.get(child_name_key)) for item in (children or []) if _clean(item.get(child_name_key))],
        limit=12,
    )

    child_keywords: List[str] = []
    for item in children or []:
        child_keywords.extend(_split_keywords(item.get(child_kw_key), limit=40))

    keyword_values = _merge_keywords(
        _split_keywords(existing_keywords or [], limit=80) if existing_keywords else [],
        _description_seed_keywords(existing_description, limit=40),
        [clean_name],
        child_names,
        child_keywords,
        limit=_keyword_limit(level),
    )

    if _clean(existing_description):
        description = _clean(existing_description)
    elif child_names:
        if level == "subject":
            description = f"{clean_name} bao quát toàn bộ cuốn sách hoặc môn học, gồm các mảng nội dung như: {', '.join(child_names[:10])}."
        elif level == "topic":
            description = f"{clean_name} bao quát các bài và nội dung chính như: {', '.join(child_names[:10])}."
        else:
            description = f"{clean_name} bao quát các nội dung chính như: {', '.join(child_names[:8])}."
    elif keyword_values:
        description = f"{clean_name} bao quát các nội dung chính liên quan đến: {', '.join(keyword_values[:12])}."
    else:
        description = clean_name

    return description, keyword_values


def _build_prompt(level: str, name: str, context_text: str, keyword_limit: int) -> Tuple[str, int]:
    if level == "lesson":
        prompt = (
            "Bạn đang xây dựng hệ thống tìm kiếm học liệu. Hãy đọc phần ngữ cảnh tổng hợp của BÀI HỌC và viết mô tả bao quát ở cấp bài. "
            "Mô tả cần nêu được các khái niệm chính, định nghĩa, quy trình, ví dụ, ứng dụng và các phần quan trọng trong bài. "
            "Sau đó trích xuất keyword tìm kiếm thật tốt, ưu tiên cụm từ 2-6 từ, không lấy token rời rạc, không lấy map id. "
            f"Trả về JSON đúng schema {{'description':'...','keywords':['kw1','kw2']}} với tối đa {keyword_limit} keyword. "
            "Keyword phải đủ phủ các ý chính trong description. Không markdown. Không giải thích. "
            f"Tên bài: {name}.\nNgữ cảnh:\n{context_text}"
        )
        return prompt, 900

    if level == "topic":
        prompt = (
            "Bạn đang xây dựng hệ thống tìm kiếm học liệu. Hãy đọc phần ngữ cảnh tổng hợp của CHỦ ĐỀ và viết mô tả đủ chi tiết để bao quát toàn bộ chủ đề. "
            "Mô tả phải thể hiện được các bài lớn, các khái niệm, kỹ năng, ứng dụng và mối liên hệ giữa các phần trong chủ đề. "
            "Sau đó trích xuất keyword thật đầy đủ. "
            f"Trả về JSON đúng schema {{'description':'...','keywords':['kw1','kw2']}} với tối đa {keyword_limit} keyword. "
            "Không markdown. Không giải thích. "
            f"Tên chủ đề: {name}.\nNgữ cảnh:\n{context_text}"
        )
        return prompt, 1400

    prompt = (
        "Bạn đang xây dựng hệ thống tìm kiếm học liệu. Hãy đọc phần ngữ cảnh tổng hợp của MÔN HỌC hoặc CUỐN SÁCH và viết mô tả đủ chi tiết, có tính bao hàm cho toàn bộ tài liệu. "
        "Mô tả cần bao quát phạm vi kiến thức, các nhóm chủ đề lớn, kỹ năng, ứng dụng và nội dung xuyên suốt. "
        "Sau đó trích xuất keyword thật đầy đủ. "
        f"Trả về JSON đúng schema {{'description':'...','keywords':['kw1','kw2']}} với tối đa {keyword_limit} keyword. "
        "Không markdown. Không giải thích. "
        f"Tên môn học/cuốn sách: {name}.\nNgữ cảnh:\n{context_text}"
    )
    return prompt, 1800


def _call_desc_keywords_api(
    *,
    level: str,
    name: str,
    context_text: str,
    fallback_keywords: Sequence[str],
    existing_description: str = "",
) -> Tuple[str, List[str], Dict[str, Any]]:
    model = _clean(__import__("os").getenv("GEMINI_MODEL")) or "gemini-2.5-flash"
    rotated, collect_meta = _rotated_keys()
    keyword_limit = _keyword_limit(level)

    base_meta: Dict[str, Any] = {
        "model": model,
        "key_count": collect_meta.get("key_count", 0),
        "key_sources": collect_meta.get("key_sources", []),
        "env_paths_checked": collect_meta.get("env_paths_checked", []),
        "cwd": collect_meta.get("cwd", ""),
        "keyword_limit": keyword_limit,
    }

    if not _clean(context_text):
        preserved_keywords = _merge_keywords(
            fallback_keywords,
            _description_seed_keywords(existing_description, limit=keyword_limit),
            limit=keyword_limit,
        )
        preserved_desc = _clean(existing_description) or _clean(name)
        base_meta["mode"] = "preserve_existing_no_context"
        return preserved_desc, preserved_keywords, base_meta

    prompt, output_tokens = _build_prompt(level, name, context_text, keyword_limit)

    if not rotated:
        preserved_keywords = _merge_keywords(
            _description_seed_keywords(existing_description, limit=keyword_limit),
            fallback_keywords,
            limit=keyword_limit,
        )
        base_meta["mode"] = "fallback_no_api_key"
        return _clean(existing_description) or _clean(name), preserved_keywords, base_meta

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "topP": 0.9,
            "maxOutputTokens": output_tokens,
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
            raw = _call_generate_content(key, model, payload, timeout=30)
            text = _extract_text(raw)
            if text:
                meta["raw_text"] = text[:1200]

            obj = _extract_json_payload(text)
            if isinstance(obj, dict):
                description = _clean(obj.get("description"))
                keywords = _normalize_keywords(obj.get("keywords") or [], limit=keyword_limit)
                keywords = _merge_keywords(
                    keywords,
                    _description_seed_keywords(description or existing_description, limit=keyword_limit),
                    fallback_keywords,
                    limit=keyword_limit,
                )

                if description and keywords:
                    meta["mode"] = "api_json"
                    return description, keywords, meta

                if description:
                    meta["mode"] = "api_desc_only"
                    return description, _merge_keywords(
                        _description_seed_keywords(description, limit=keyword_limit),
                        fallback_keywords,
                        limit=keyword_limit,
                    ), meta

            meta["error"] = "invalid_or_empty_json"
        except Exception as exc:
            meta["error"] = f"unexpected:{exc}"

        last_meta = meta

    preserved_keywords = _merge_keywords(
        _description_seed_keywords(existing_description, limit=keyword_limit),
        fallback_keywords,
        limit=keyword_limit,
    )
    last_meta["mode"] = "fallback_after_api_error"
    return _clean(existing_description) or _clean(name), preserved_keywords, last_meta


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
        lesson_chunks = _active_docs(
            db["chunks"].find({"lessonID": _clean(lesson_doc.get("lessonID"))}).sort("chunkNumber", 1)
        )
        existing_lesson_keywords = _split_keywords(lesson_doc.get("keywordLesson"), limit=_keyword_limit("lesson"))
        context = _build_context_lines(
            "Lesson",
            _clean(lesson_doc.get("lessonName")),
            lesson_chunks,
            child_name_key="chunkName",
            child_desc_key="chunkDescription",
            child_kw_key="keywords",
            existing_description=_clean(lesson_doc.get("lessonDescription")),
            existing_keywords=existing_lesson_keywords,
        )

        keywords = _uniq_keep_order(
            [kw for chunk in lesson_chunks for kw in _split_keywords(chunk.get("keywords"), limit=None)],
            limit=None,
        )

        description, _ignored_keywords, meta = _call_desc_keywords_api(
            level="lesson",
            name=_clean(lesson_doc.get("lessonName")),
            context_text=context,
            fallback_keywords=keywords,
            existing_description=_clean(lesson_doc.get("lessonDescription")),
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
        result["lesson"] = {
            "lessonID": lesson_doc.get("lessonID"),
            "meta": meta,
            "keywordCount": len(keywords),
        }

    topic_doc = db["topics"].find_one({"topicID": _clean(topic_map)}) if _clean(topic_map) else None
    if topic_doc is None and lesson_doc:
        topic_doc = db["topics"].find_one({"topicID": _clean(lesson_doc.get("topicID"))})

    if topic_doc:
        topic_lessons = _active_docs(
            db["lessons"].find({"topicID": _clean(topic_doc.get("topicID"))}).sort("lessonNumber", 1)
        )

        existing_topic_keywords = _split_keywords(topic_doc.get("keywordTopic"), limit=_keyword_limit("topic"))
        context = _build_context_lines(
            "Topic",
            _clean(topic_doc.get("topicName")),
            topic_lessons,
            child_name_key="lessonName",
            child_desc_key="lessonDescription",
            child_kw_key="keywordLesson",
            existing_description=_clean(topic_doc.get("topicDescription")),
            existing_keywords=existing_topic_keywords,
        )

        # Topic keyword chỉ lấy từ lesson thuộc topic đó.
        keywords = _uniq_keep_order(
            [kw for lesson in topic_lessons for kw in _split_keywords(lesson.get("keywordLesson"), limit=None)],
            limit=None,
        )

        description, _ignored_keywords, meta = _call_desc_keywords_api(
            level="topic",
            name=_clean(topic_doc.get("topicName")),
            context_text=context,
            fallback_keywords=keywords,
            existing_description=_clean(topic_doc.get("topicDescription")),
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
        result["topic"] = {
            "topicID": topic_doc.get("topicID"),
            "meta": meta,
            "keywordCount": len(keywords),
        }

    subject_doc = db["subjects"].find_one({"subjectID": _clean(subject_map)}) if _clean(subject_map) else None
    if subject_doc is None and topic_doc:
        subject_doc = db["subjects"].find_one({"subjectID": _clean(topic_doc.get("subjectID"))})

    if subject_doc:
        subject_topics = _active_docs(
            db["topics"].find({"subjectID": _clean(subject_doc.get("subjectID"))}).sort("topicNumber", 1)
        )

        existing_subject_keywords = _split_keywords(
            subject_doc.get("keywordSubject"),
            limit=_keyword_limit("subject"),
        )
        context = _build_context_lines(
            "Subject",
            _clean(subject_doc.get("subjectName") or subject_doc.get("subjectTitle")),
            subject_topics,
            child_name_key="topicName",
            child_desc_key="topicDescription",
            child_kw_key="keywordTopic",
            existing_description=_clean(subject_doc.get("subjectDescription")),
            existing_keywords=existing_subject_keywords,
        )

        # Subject keyword chỉ lấy từ topic thuộc subject đó.
        keywords = _uniq_keep_order(
            [kw for topic in subject_topics for kw in _split_keywords(topic.get("keywordTopic"), limit=None)],
            limit=None,
        )

        description, _ignored_keywords, meta = _call_desc_keywords_api(
            level="subject",
            name=_clean(subject_doc.get("subjectName") or subject_doc.get("subjectTitle")),
            context_text=context,
            fallback_keywords=keywords,
            existing_description=_clean(subject_doc.get("subjectDescription")),
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

        result["subject"] = {
            "subjectID": subject_doc.get("subjectID"),
            "meta": meta,
            "keywordCount": len(keywords),
        }

    return result