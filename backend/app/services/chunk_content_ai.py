from __future__ import annotations

import json
import mimetypes
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .gemini_topic_expander import _call_generate_content, _clean, _extract_json_payload, _extract_text, _rotated_keys

try:
    import google.generativeai as genai
except Exception:  # pragma: no cover
    genai = None

try:
    import docx  # type: ignore
except Exception:  # pragma: no cover
    docx = None

_TEXT_BASED_SUFFIXES = {'.txt', '.md', '.json', '.docx'}

_WORD_RE = re.compile(r"[0-9A-Za-zÀ-ỹ]+", flags=re.UNICODE)
_MAP_ID_RE = re.compile(r"^[A-Z]{2,}\d*(?:_[A-Z]{1,3}\d+)+$", flags=re.I)
_SPLIT_RE = re.compile(r"[,;\n\r\t]+")
_STOPWORDS = {
    "va", "và", "la", "là", "cua", "của", "cho", "voi", "với", "cac", "các",
    "mot", "một", "nhung", "những", "trong", "tren", "trên", "duoi", "dưới", "tai",
    "tu", "từ", "den", "đến", "ve", "về", "phan", "phần", "noi", "nội", "dung",
    "bai", "bài", "muc", "mục", "chu", "chủ", "de", "đề", "ly", "thuyet", "thực",
    "hanh", "thong", "tin", "du", "lieu", "dữ", "liệu", "don", "vi", "luu", "tru",
    "c1", "c2", "c3", "c4", "c5", "th10", "cd1", "cd2", "cd3", "b1", "b2"
}
_ALLOWED_SUFFIXES = {".pdf", ".txt", ".md", ".json", ".docx", ".png", ".jpg", ".jpeg", ".webp"}


def _uniq_keep_order(values: Iterable[str], limit: Optional[int] = 5) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values or []:
        clean = _clean_phrase(value)
        if not clean:
            continue
        key = clean.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(clean)
        if limit and len(out) >= limit:
            break
    return out


def _clean_phrase(value: Any) -> str:
    text = _clean(value)
    text = re.sub(r"\s+", " ", text)
    text = text.strip(" ,.;:-_`'\"[]{}()")
    if not text:
        return ""
    if _MAP_ID_RE.match(text.replace(" ", "")):
        return ""
    if re.fullmatch(r"[A-Za-zÀ-ỹ]", text):
        return ""
    return text


def _normalize_keyword_candidate(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("term") or value.get("keyword") or value.get("name") or ""
    text = _clean_phrase(value)
    if not text:
        return ""
    tokens = [tok for tok in _WORD_RE.findall(text) if tok]
    if not tokens:
        return ""
    joined_lower = " ".join(tok.casefold() for tok in tokens)
    if joined_lower in _STOPWORDS:
        return ""
    # Ưu tiên cụm 2-6 từ; nếu 1 từ thì chỉ giữ khi thực sự có nghĩa và không phải stopword
    if len(tokens) == 1:
        low = tokens[0].casefold()
        if len(tokens[0]) < 4 or low in _STOPWORDS:
            return ""
        return tokens[0]
    if len(tokens) > 8:
        tokens = tokens[:8]
    return " ".join(tokens)


def _normalize_keywords(values: Any, limit: Optional[int] = 5) -> List[str]:
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


def _fallback_description(chunk_name: str, lesson_name: str = "", topic_name: str = "", subject_name: str = "") -> str:
    title = _clean_phrase(chunk_name) or "Nội dung chunk"
    scope = [x for x in [_clean_phrase(lesson_name), _clean_phrase(topic_name), _clean_phrase(subject_name)] if x]
    if scope:
        return f"{title} là nội dung thuộc {scope[0]}, tập trung vào các khái niệm và kiến thức chính liên quan đến {title.lower()}."
    return f"{title} là nội dung học tập tập trung vào các khái niệm và kiến thức chính liên quan đến {title.lower()}."


def _fallback_keywords(chunk_name: str, chunk_desc: str, lesson_name: str = "", topic_name: str = "", *, limit: Optional[int] = 5) -> List[str]:
    seeds: List[str] = []
    for value in [chunk_name, lesson_name, topic_name]:
        clean = _clean_phrase(value)
        if clean:
            seeds.append(clean)
    desc = _clean(chunk_desc)
    if desc:
        phrases = [p.strip() for p in re.split(r"[.;\n\r]+", desc) if p.strip()]
        for phrase in phrases:
            kw = _normalize_keyword_candidate(phrase)
            if kw:
                seeds.append(kw)
        words = []
        for tok in _WORD_RE.findall(desc):
            low = tok.casefold()
            if len(tok) < 4 or low in _STOPWORDS:
                continue
            words.append(tok)
        freq: Dict[str, int] = {}
        for tok in words:
            k = tok.casefold()
            freq[k] = freq.get(k, 0) + 1
        for key, _count in sorted(freq.items(), key=lambda item: (-item[1], item[0])):
            seeds.append(key)
            if len(seeds) >= (limit or 12) * 3:
                break
    return _normalize_keywords(seeds, limit=limit)


def _description_seed_keywords(description: str, *, limit: Optional[int]) -> List[str]:
    text = _clean(description)
    if not text:
        return []
    seeds: List[str] = []
    for line in re.split(r"[\n\r]+", text):
        line = _clean_phrase(line)
        if not line:
            continue
        # cụm sau dấu : hoặc ; thường là keyword tốt
        for seg in re.split(r"[;:]", line):
            seg = _clean_phrase(seg)
            if not seg:
                continue
            if 2 <= len(seg.split()) <= 8:
                seeds.append(seg)
        # cắt theo dấu phẩy cho các cụm mô tả/liệt kê
        for seg in re.split(r",", line):
            seg = _clean_phrase(seg)
            if not seg:
                continue
            if 2 <= len(seg.split()) <= 8:
                seeds.append(seg)
    return _normalize_keywords(seeds, limit=limit)


def _segment_has_too_many_stopwords(tokens: Sequence[str]) -> bool:
    lows = [tok.casefold() for tok in tokens if tok]
    if not lows:
        return True
    stop_count = sum(1 for tok in lows if tok in _STOPWORDS)
    return stop_count > max(1, len(lows) // 3)


def _segment_is_meaningful(tokens: Sequence[str]) -> bool:
    lows = [tok.casefold() for tok in tokens if tok]
    if not lows:
        return False
    if lows[0] in _STOPWORDS or lows[-1] in _STOPWORDS:
        return False
    if len(lows) <= 3 and any(tok in _STOPWORDS for tok in lows):
        return False
    if _segment_has_too_many_stopwords(tokens):
        return False
    return True


def _name_ngram_keywords(value: str, *, limit: Optional[int] = None, min_words: int = 2, max_words: int = 4) -> List[str]:
    text = _clean_phrase(value)
    if not text:
        return []
    tokens = [tok for tok in _WORD_RE.findall(text) if tok]
    if not tokens:
        return []

    seeds: List[str] = []
    full = " ".join(tokens)
    if full:
        seeds.append(full)

    n_tokens = len(tokens)
    if n_tokens == 1:
        tok = tokens[0]
        if len(tok) >= 4 and tok.casefold() not in _STOPWORDS:
            seeds.append(tok)
        return _normalize_keywords(seeds, limit=limit)

    for size in range(min_words, min(max_words, n_tokens) + 1):
        prefix = tokens[:size]
        suffix = tokens[n_tokens - size:]
        if _segment_is_meaningful(prefix):
            seeds.append(" ".join(prefix))
        if _segment_is_meaningful(suffix):
            seeds.append(" ".join(suffix))

    for size in range(min_words, min(max_words, n_tokens) + 1):
        for start in range(0, n_tokens - size + 1):
            segment = tokens[start:start + size]
            if not _segment_is_meaningful(segment):
                continue
            seeds.append(" ".join(segment))

    return _normalize_keywords(seeds, limit=limit)


def _meaningful_single_tokens(*values: str, limit: Optional[int] = None) -> List[str]:
    seeds: List[str] = []
    for value in values:
        text = _clean(value)
        if not text:
            continue
        for tok in _WORD_RE.findall(text):
            low = tok.casefold()
            if len(tok) < 4 or low in _STOPWORDS:
                continue
            seeds.append(tok)
    return _normalize_keywords(seeds, limit=limit)


_LESSON_BAD_PHRASES = (
    " là nội dung ",
    " nội dung thuộc ",
    " tập trung vào ",
    " kiến thức chính ",
    " liên quan đến ",
    " bao gồm ",
    " gồm ",
)


LESSON_BAD_END_TOKENS = {
    "la", "là", "thuoc", "thuộc", "gom", "gồm", "bao", "cua", "của",
    "voi", "với", "va", "và", "cho", "trong", "tai", "tại", "nhu", "như",
}

CHUNK_BAD_PHRASES = (
    " nội dung chunk ",
    " nội dung thuộc ",
    " tập trung vào ",
    " kiến thức chính ",
    " liên quan đến ",
    " bao gồm ",
    " gồm ",
)


CHUNK_BAD_SINGLE_TOKENS = {
    "chunk", "noi", "nội", "dung", "thong", "thông", "bai", "bài", "muc", "mục",
}


CHUNK_BAD_END_TOKENS = LESSON_BAD_END_TOKENS


def _filter_chunk_keyword_candidate(
    value: Any,
    *,
    chunk_name: str = "",
    lesson_name: str = "",
    topic_name: str = "",
    subject_name: str = "",
    description: str = "",
) -> str:
    kw = _normalize_keyword_candidate(value)
    if not kw:
        return ""
    low = f" {_clean(kw).casefold()} "
    if any(bad in low for bad in CHUNK_BAD_PHRASES):
        return ""

    tokens = [tok for tok in _WORD_RE.findall(kw) if tok]
    lows = [tok.casefold() for tok in tokens]
    if not lows:
        return ""
    if len(tokens) == 1:
        return ""
    if lows[-1] in CHUNK_BAD_END_TOKENS:
        return ""
    if _segment_has_too_many_stopwords(tokens):
        return ""

    desc_low = _clean(description).casefold()
    kw_low = _clean(kw).casefold()
    if desc_low.startswith(f"{kw_low} là") or desc_low.startswith(f"{kw_low} thuộc"):
        return ""

    generic_names = {
        _clean(chunk_name).casefold(),
        _clean(lesson_name).casefold(),
        _clean(topic_name).casefold(),
        _clean(subject_name).casefold(),
    }
    generic_names.discard("")
    if kw_low in generic_names and len(tokens) <= 2:
        return ""
    return kw


def _filter_chunk_keyword_values(
    values: Sequence[Any],
    *,
    chunk_name: str = "",
    lesson_name: str = "",
    topic_name: str = "",
    subject_name: str = "",
    description: str = "",
    limit: Optional[int] = None,
) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values or []:
        kw = _filter_chunk_keyword_candidate(
            value,
            chunk_name=chunk_name,
            lesson_name=lesson_name,
            topic_name=topic_name,
            subject_name=subject_name,
            description=description,
        )
        if not kw:
            continue
        key = kw.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(kw)
        if limit and len(out) >= limit:
            break
    return out



def _filter_lesson_keyword_candidate(
    value: Any,
    *,
    lesson_name: str = "",
    topic_name: str = "",
    subject_name: str = "",
    description: str = "",
) -> str:
    kw = _normalize_keyword_candidate(value)
    if not kw:
        return ""
    low = f" {_clean(kw).casefold()} "
    if any(bad in low for bad in _LESSON_BAD_PHRASES):
        return ""

    tokens = [tok for tok in _WORD_RE.findall(kw) if tok]
    lows = [tok.casefold() for tok in tokens]
    if not lows:
        return ""
    if lows[-1] in LESSON_BAD_END_TOKENS:
        return ""
    if len(tokens) >= 2 and _segment_has_too_many_stopwords(tokens):
        return ""

    desc_low = _clean(description).casefold()
    kw_low = _clean(kw).casefold()
    if desc_low.startswith(f"{kw_low} là") or desc_low.startswith(f"{kw_low} thuộc"):
        return ""

    generic_names = {_clean(lesson_name).casefold(), _clean(topic_name).casefold(), _clean(subject_name).casefold()}
    generic_names.discard("")
    if kw_low in {f"{name} là" for name in generic_names}:
        return ""

    return kw


def _filter_lesson_keyword_values(
    values: Sequence[Any],
    *,
    lesson_name: str = "",
    topic_name: str = "",
    subject_name: str = "",
    description: str = "",
    limit: Optional[int] = None,
) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values or []:
        kw = _filter_lesson_keyword_candidate(
            value,
            lesson_name=lesson_name,
            topic_name=topic_name,
            subject_name=subject_name,
            description=description,
        )
        if not kw:
            continue
        key = kw.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(kw)
        if limit and len(out) >= limit:
            break
    return out


def _read_text_content_for_keyword_extraction(file_path: str) -> str:
    path = Path(file_path)
    suffix = path.suffix.lower()
    if suffix == '.docx':
        if docx is None:
            return ''
        try:
            document = docx.Document(str(path))
            return '\n'.join(p.text for p in document.paragraphs if _clean(p.text))
        except Exception:
            return ''
    for enc in ('utf-8', 'utf-8-sig', 'latin-1'):
        try:
            return path.read_text(encoding=enc)
        except Exception:
            continue
    return ''


def _parse_keyword_json_like_reference(text: str) -> List[str]:
    clean_text = (text or '').strip()
    if not clean_text:
        return []
    try:
        obj = _extract_json_payload(clean_text)
    except Exception:
        obj = None
    if isinstance(obj, dict):
        raw = obj.get('keywords') or []
        if isinstance(raw, list):
            return [
                (item.get('term') if isinstance(item, dict) else item)
                for item in raw
            ]
    if isinstance(obj, list):
        return [item.get('term') if isinstance(item, dict) else item for item in obj]
    return []


def _chunk_description_only_prompt(*, chunk_name: str, lesson_name: str, topic_name: str, subject_name: str) -> str:
    scope_lines = []
    if subject_name:
        scope_lines.append(f"Môn: {subject_name}")
    if topic_name:
        scope_lines.append(f"Chủ đề: {topic_name}")
    if lesson_name:
        scope_lines.append(f"Bài: {lesson_name}")
    if chunk_name:
        scope_lines.append(f"Chunk: {chunk_name}")
    scope_text = "\n".join(scope_lines)
    return (
        "Bạn là chuyên gia phân tích học liệu. Hãy đọc tài liệu chunk và tạo mô tả ngắn gọn nhưng bao quát nội dung chính để phục vụ tìm kiếm. "
        "Chỉ trả về description. Không trả keyword. Không markdown. Không giải thích. "
        "Trả về JSON đúng schema: {'description':'...'}.\n"
        f"Ngữ cảnh:\n{scope_text}"
    )


def _chunk_keywords_only_prompt(*, chunk_name: str, lesson_name: str, topic_name: str, subject_name: str, num_keywords: int = 5) -> str:
    scope_lines = []
    if subject_name:
        scope_lines.append(f"Môn: {subject_name}")
    if topic_name:
        scope_lines.append(f"Chủ đề: {topic_name}")
    if lesson_name:
        scope_lines.append(f"Bài: {lesson_name}")
    if chunk_name:
        scope_lines.append(f"Chunk: {chunk_name}")
    scope_text = "\n".join(scope_lines)
    return (
        "Bạn là một chuyên gia SEO và phân tích nội dung học liệu với 10 năm kinh nghiệm. "
        f"Nhiệm vụ: Đọc hiểu tài liệu chunk và trích xuất đúng {num_keywords} từ khóa quan trọng nhất phục vụ tìm kiếm. "
        "Chỉ lấy keyword có nghĩa tìm kiếm trực tiếp, ưu tiên cụm từ 2-6 từ. "
        "Không lấy nguyên mệnh đề mô tả, không lấy cụm chứa 'nội dung', không lấy cụm kết thúc bằng 'là', 'thuộc', 'gồm', 'bao gồm'. "
        "Không lấy map id, mã bài, ký hiệu kỹ thuật hoặc token rời rạc vô nghĩa. "
        "Chỉ trả về JSON thuần, không markdown, không giải thích. "
        f"JSON bắt buộc: {{\"keywords\":[{{\"term\":\"kw1\"}},{{\"term\":\"kw2\"}}]}} với đúng {num_keywords} phần tử.\n"
        f"Ngữ cảnh:\n{scope_text}"
    )


def _generate_chunk_keywords_from_reference_style(*, file_path: str, prompt: str, limit: int) -> Tuple[List[str], Dict[str, Any]]:
    path = Path(file_path) if file_path else None
    if path and path.exists() and path.suffix.lower() in _TEXT_BASED_SUFFIXES:
        content_text = _read_text_content_for_keyword_extraction(str(path))
        if content_text:
            desc, raw_keywords, meta = _gemini_extract_via_rest(
                prompt=f"{prompt}\n\n---\nNỘI DUNG VĂN BẢN:\n{content_text}",
                limit=None,
                output_tokens=520,
            )
            if raw_keywords:
                return _filter_chunk_keyword_values(raw_keywords, description=desc, limit=limit), {**meta, "mode": "rest_text_embedded"}

    if file_path:
        _desc, raw_keywords, meta = _gemini_extract_from_file(
            file_path=file_path,
            prompt=prompt,
            limit=None,
            output_tokens=520,
        )
        if raw_keywords:
            return _filter_chunk_keyword_values(raw_keywords, limit=limit), meta

    _desc, raw_keywords, meta = _gemini_extract_via_rest(
        prompt=prompt,
        limit=None,
        output_tokens=520,
    )
    return _filter_chunk_keyword_values(raw_keywords, limit=limit), meta


def _lesson_description_only_prompt(*, lesson_name: str, topic_name: str, subject_name: str) -> str:
    scope_lines = []
    if subject_name:
        scope_lines.append(f"Môn: {subject_name}")
    if topic_name:
        scope_lines.append(f"Chủ đề: {topic_name}")
    if lesson_name:
        scope_lines.append(f"Bài: {lesson_name}")
    scope_text = '\n'.join(scope_lines)
    return (
        'Bạn là chuyên gia phân tích học liệu. Hãy đọc toàn bộ file bài học và tạo mô tả ngắn gọn nhưng bao quát nội dung chính để phục vụ tìm kiếm. '
        'Chỉ trả về description. Không trả keyword. Không markdown. Không giải thích. '
        "Trả về JSON đúng schema: {'description':'...','keywords':[]}.\n"
        f'Ngữ cảnh:\n{scope_text}'
    )


def _lesson_keywords_only_prompt(*, lesson_name: str, topic_name: str, subject_name: str, num_keywords: int = 10) -> str:
    scope_lines = []
    if subject_name:
        scope_lines.append(f"Môn: {subject_name}")
    if topic_name:
        scope_lines.append(f"Chủ đề: {topic_name}")
    if lesson_name:
        scope_lines.append(f"Bài: {lesson_name}")
    scope_text = '\n'.join(scope_lines)
    return (
        'Bạn là một chuyên gia SEO và phân tích nội dung học liệu với 10 năm kinh nghiệm. '
        f'Nhiệm vụ: Đọc hiểu tài liệu bài học và trích xuất đúng {num_keywords} từ khóa quan trọng nhất phục vụ tìm kiếm. '
        'Chỉ lấy keyword có nghĩa tìm kiếm trực tiếp, ưu tiên cụm từ 2-6 từ. '
        'Không lấy nguyên mệnh đề mô tả, không lấy cụm kết thúc bằng "là", "thuộc", "gồm", "bao gồm". '
        'Không lấy map id, mã bài, ký hiệu kỹ thuật hoặc token rời rạc vô nghĩa. '
        'Chỉ trả về JSON thuần, không markdown, không giải thích. '
        f'JSON bắt buộc: {{"keywords":[{{"term":"kw1"}},{{"term":"kw2"}}]}} với đúng {num_keywords} phần tử.\n'
        f'Ngữ cảnh:\n{scope_text}'
    )


def _generate_lesson_keywords_from_reference_style(*, file_path: str, prompt: str, limit: int) -> Tuple[List[str], Dict[str, Any]]:
    path = Path(file_path) if file_path else None
    if path and path.exists() and path.suffix.lower() in _TEXT_BASED_SUFFIXES:
        content_text = _read_text_content_for_keyword_extraction(str(path))
        if content_text:
            desc, raw_keywords, meta = _gemini_extract_via_rest(
                prompt=f"{prompt}\n\n---\nNỘI DUNG VĂN BẢN:\n{content_text}",
                limit=None,
                output_tokens=720,
            )
            parsed = _parse_keyword_json_like_reference(json.dumps({'keywords': [{'term': k} for k in raw_keywords]})) if raw_keywords else []
            if parsed:
                return _filter_lesson_keyword_values(parsed, description=desc, limit=limit), {**meta, 'mode': 'rest_text_embedded'}

    if file_path:
        _desc, raw_keywords, meta = _gemini_extract_from_file(
            file_path=file_path,
            prompt=prompt,
            limit=None,
            output_tokens=720,
        )
        if raw_keywords:
            return _filter_lesson_keyword_values(raw_keywords, limit=limit), meta

    _desc, raw_keywords, meta = _gemini_extract_via_rest(
        prompt=prompt,
        limit=None,
        output_tokens=720,
    )
    return _filter_lesson_keyword_values(raw_keywords, limit=limit), meta


def _lesson_keyword_retry_prompt(
    *,
    lesson_name: str,
    topic_name: str,
    subject_name: str,
    existing_keywords: Sequence[str],
    num_keywords: int,
) -> str:
    existing = ", ".join(_clean(k) for k in existing_keywords if _clean(k)) or "(trống)"
    scope_lines = []
    if subject_name:
        scope_lines.append(f"Môn: {subject_name}")
    if topic_name:
        scope_lines.append(f"Chủ đề: {topic_name}")
    if lesson_name:
        scope_lines.append(f"Bài: {lesson_name}")
    scope_text = "\n".join(scope_lines)
    return (
        'Bạn là một chuyên gia SEO và phân tích nội dung học liệu với 10 năm kinh nghiệm. '
        f'Nhiệm vụ: Từ chính tài liệu bài học này, hãy bổ sung đúng {num_keywords} keyword còn thiếu để đủ bộ keyword cho lesson. '
        'Chỉ lấy keyword có nghĩa tìm kiếm trực tiếp, ưu tiên cụm từ 2-6 từ. '
        'Không lấy nguyên mệnh đề mô tả, không lấy cụm kết thúc bằng "là", "thuộc", "gồm", "bao gồm". '
        'Không lấy map id, mã bài, ký hiệu kỹ thuật hoặc token rời rạc vô nghĩa. '
        'Không được lặp lại keyword đã có. '
        'Chỉ trả về JSON thuần, không markdown, không giải thích. '
        f'JSON bắt buộc: {{"keywords":[{{"term":"kw1"}},{{"term":"kw2"}}]}} với đúng {num_keywords} phần tử.\n'
        f'Ngữ cảnh:\n{scope_text}\n'
        f'Danh sách đã có, không được lặp lại: {existing}'
    )


def _collect_lesson_keywords_strict_ai_only(
    *,
    file_path: str,
    lesson_name: str,
    topic_name: str,
    subject_name: str,
    limit: int,
    manual_keywords: Sequence[str],
) -> Tuple[List[str], Dict[str, Any]]:
    final_keywords = _filter_lesson_keyword_values(
        list(manual_keywords),
        lesson_name=lesson_name,
        topic_name=topic_name,
        subject_name=subject_name,
        description="",
        limit=None,
    )

    attempts_meta: List[Dict[str, Any]] = []
    seen_modes: List[str] = []

    def _merge(candidates: Sequence[str]) -> None:
        nonlocal final_keywords
        final_keywords = _filter_lesson_keyword_values(
            [*final_keywords, *list(candidates)],
            lesson_name=lesson_name,
            topic_name=topic_name,
            subject_name=subject_name,
            description="",
            limit=None,
        )

    primary_prompt = _lesson_keywords_only_prompt(
        lesson_name=lesson_name,
        topic_name=topic_name,
        subject_name=subject_name,
        num_keywords=limit,
    )
    primary_keywords, primary_meta = _generate_lesson_keywords_from_reference_style(
        file_path=file_path,
        prompt=primary_prompt,
        limit=limit,
    )
    attempts_meta.append(primary_meta)
    if primary_meta.get("mode"):
        seen_modes.append(str(primary_meta.get("mode")))
    _merge(primary_keywords)

    max_retries = 3
    retry_count = 0
    while len(final_keywords) < limit and retry_count < max_retries:
        missing = limit - len(final_keywords)
        retry_prompt = _lesson_keyword_retry_prompt(
            lesson_name=lesson_name,
            topic_name=topic_name,
            subject_name=subject_name,
            existing_keywords=final_keywords,
            num_keywords=missing,
        )
        retry_keywords, retry_meta = _generate_lesson_keywords_from_reference_style(
            file_path=file_path,
            prompt=retry_prompt,
            limit=missing,
        )
        attempts_meta.append(retry_meta)
        if retry_meta.get("mode"):
            seen_modes.append(str(retry_meta.get("mode")))
        before = len(final_keywords)
        _merge(retry_keywords)
        retry_count += 1
        if len(final_keywords) == before:
            break

    return final_keywords[:limit], {
        "mode": "lesson_keywords_ai_only_strict",
        "attempts": attempts_meta,
        "sources": seen_modes,
        "count": len(final_keywords[:limit]),
        "target": limit,
    }

def _chunk_prompt(*, chunk_name: str, lesson_name: str, topic_name: str, subject_name: str, num_keywords: int = 5) -> str:
    scope_lines = []
    if subject_name:
        scope_lines.append(f"Môn: {subject_name}")
    if topic_name:
        scope_lines.append(f"Chủ đề: {topic_name}")
    if lesson_name:
        scope_lines.append(f"Bài: {lesson_name}")
    if chunk_name:
        scope_lines.append(f"Chunk: {chunk_name}")
    scope_text = "\n".join(scope_lines)
    return (
        "Bạn là chuyên gia phân tích học liệu. Hãy đọc tài liệu/chunk và trích xuất thông tin phục vụ tìm kiếm. "
        "Yêu cầu rất quan trọng: chỉ lấy keyword có nghĩa tìm kiếm, ưu tiên cụm từ 2-6 từ, không lấy từ rời rạc vô nghĩa, "
        "không lấy map id như TH10_CD1_B1, không lấy các token đơn lẻ như 'Đơn', 'lưu', 'trữ'. "
        "Mô tả phải ngắn gọn nhưng bao quát nội dung chunk. "
        f"Trả về JSON đúng schema: {{'description':'...','keywords':[{{'term':'kw1'}},{{'term':'kw2'}}]}} với đúng {num_keywords} keyword. "
        "Không markdown. Không giải thích.\n"
        f"Ngữ cảnh:\n{scope_text}"
    )


def _lesson_prompt(*, lesson_name: str, topic_name: str, subject_name: str, num_keywords: int = 10) -> str:
    scope_lines = []
    if subject_name:
        scope_lines.append(f"Môn: {subject_name}")
    if topic_name:
        scope_lines.append(f"Chủ đề: {topic_name}")
    if lesson_name:
        scope_lines.append(f"Bài: {lesson_name}")
    scope_text = "\n".join(scope_lines)
    return (
        "Bạn là chuyên gia phân tích học liệu. Hãy đọc toàn bộ file bài học và trích xuất thông tin phục vụ tìm kiếm. "
        "Mô tả phải bao quát các phần chính của bài, các khái niệm, định nghĩa, quy trình, ví dụ, ứng dụng nếu có. "
        "Keyword phải là keyword tìm kiếm tốt, là danh từ hoặc cụm danh từ độc lập, ưu tiên cụm từ 2-6 từ. "
        "Không được lấy nguyên mệnh đề từ mô tả. Không lấy cụm kết thúc bằng 'là', 'thuộc', 'gồm', 'bao gồm'. "
        "Không lấy map id, không lấy token rời rạc vô nghĩa. "
        f"Trả về JSON đúng schema: {{'description':'...','keywords':[{{'term':'kw1'}},{{'term':'kw2'}}]}} với đúng {num_keywords} keyword. "
        "Không markdown. Không giải thích.\n"
        f"Ngữ cảnh:\n{scope_text}"
    )

def _hierarchy_prompt(*, level: str, name: str, parent_name: str = "", num_keywords: int = 40) -> str:
    level_vi = {"topic": "chủ đề", "subject": "môn học/cuốn sách"}.get(level, level)
    extra = (
        "Mô tả phải đủ chi tiết, bao quát toàn bộ file, có thể dài hơn bình thường để bao hàm các mảng kiến thức chính,"
        " khái niệm, kỹ năng, nội dung trọng tâm và phạm vi kiến thức xuất hiện trong tài liệu."
    )
    if level == "subject":
        extra = (
            "Mô tả phải giống một bản tóm tắt bao quát của cả cuốn sách/môn học: nêu phạm vi kiến thức, các nhóm chủ đề lớn,"
            " mục tiêu học tập, kỹ năng, ứng dụng và nội dung xuyên suốt toàn tài liệu."
        )
    elif level == "topic":
        extra = (
            "Mô tả phải giống một bản tóm tắt đầy đủ của toàn bộ chủ đề: bao quát tất cả bài/mục chính, các khái niệm, quy trình,"
            " ứng dụng và mối liên hệ giữa các phần trong chủ đề."
        )
    parent_line = f"Thuộc: {parent_name}\n" if parent_name else ""
    return (
        f"Bạn là chuyên gia phân tích học liệu. Hãy đọc toàn bộ file ở cấp {level_vi} và tạo dữ liệu phục vụ semantic search. "
        f"{extra} "
        "Sau đó trích xuất thật đầy đủ các keyword/cụm keyword có nghĩa tìm kiếm trực tiếp, bao phủ toàn bộ các ý chính đã xuất hiện trong phần description. "
        "Ưu tiên cụm từ 2-6 từ, không lấy map id, không lấy các token rời. "
        f"Trả về JSON đúng schema: {{'description':'...','keywords':[{{'term':'kw1'}},{{'term':'kw2'}}]}} với tối đa {num_keywords} keyword. "
        "Không markdown. Không giải thích.\n"
        f"Tên: {name}\n{parent_line}"
    )


def _extract_from_response_text(text: str, limit: Optional[int] = 5) -> Tuple[str, List[str]]:
    obj = _extract_json_payload(text)
    description = ""
    keywords: List[str] = []
    if isinstance(obj, dict):
        description = _clean(obj.get("description"))
        raw_keywords = obj.get("keywords") or []
        keywords = _normalize_keywords(raw_keywords, limit=limit)
    elif isinstance(obj, list):
        keywords = _normalize_keywords(obj, limit=limit)
    return description, keywords


def _gemini_extract_via_rest(*, prompt: str, limit: Optional[int], output_tokens: int = 512) -> Tuple[str, List[str], Dict[str, Any]]:
    model = _clean(os.getenv("GEMINI_MODEL")) or "gemini-2.5-flash"
    rotated, base_meta = _rotated_keys()
    meta: Dict[str, Any] = {
        "mode": "rest_text",
        "model": model,
        "key_count": base_meta.get("key_count", 0),
    }
    if not rotated:
        return "", [], {**meta, "error": "no_api_key"}

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "topP": 0.9,
            "maxOutputTokens": output_tokens,
            "responseMimeType": "application/json",
        },
    }
    last_error = None
    for attempt, (slot, key, source) in enumerate(rotated, start=1):
        try:
            raw = _call_generate_content(key, model, payload, timeout=30)
            text = _extract_text(raw)
            description, keywords = _extract_from_response_text(text, limit=limit)
            if description or keywords:
                return description, keywords, {**meta, "attempt": attempt, "key_slot": slot, "key_source": source}
        except Exception as exc:
            last_error = str(exc)
    return "", [], {**meta, "error": last_error or "empty_response"}


def _gemini_extract_from_file(*, file_path: str, prompt: str, limit: Optional[int], output_tokens: int = 512) -> Tuple[str, List[str], Dict[str, Any]]:
    meta: Dict[str, Any] = {"mode": "file_ai", "library": "google-generativeai"}
    if genai is None:
        return "", [], {**meta, "error": "google-generativeai_not_installed"}
    path = Path(file_path)
    if not path.exists():
        return "", [], {**meta, "error": "file_not_found"}
    if path.suffix.lower() not in _ALLOWED_SUFFIXES:
        return "", [], {**meta, "error": f"unsupported_suffix:{path.suffix.lower()}"}

    model = _clean(os.getenv("GEMINI_MODEL")) or "gemini-2.5-flash"
    rotated, base_meta = _rotated_keys()
    meta.update({"model": model, "key_count": base_meta.get("key_count", 0)})
    if not rotated:
        return "", [], {**meta, "error": "no_api_key"}

    mime_type, _ = mimetypes.guess_type(str(path))
    mime_type = mime_type or "application/octet-stream"
    last_error = None

    for attempt, (slot, key, source) in enumerate(rotated, start=1):
        uploaded = None
        try:
            genai.configure(api_key=key)
            model_obj = genai.GenerativeModel(model)
            uploaded = genai.upload_file(str(path), mime_type=mime_type)
            response = model_obj.generate_content([prompt, uploaded])
            text = getattr(response, "text", "") or ""
            description, keywords = _extract_from_response_text(text, limit=limit)
            if description or keywords:
                return description, keywords, {**meta, "attempt": attempt, "key_slot": slot, "key_source": source}
            last_error = "empty_response"
        except Exception as exc:
            last_error = str(exc)
        finally:
            try:
                if uploaded is not None:
                    uploaded.delete()
            except Exception:
                pass
    return "", [], {**meta, "error": last_error or "extract_failed"}


def _generate_from_file_or_rest(*, prompt: str, file_path: str = "", limit: Optional[int], output_tokens: int = 512) -> Tuple[str, List[str], Dict[str, Any]]:
    description = ""
    keywords: List[str] = []
    meta: Dict[str, Any] = {"mode": "manual"}
    if file_path:
        description, keywords, meta = _gemini_extract_from_file(file_path=file_path, prompt=prompt, limit=limit, output_tokens=output_tokens)
    if not description and not keywords:
        description, keywords, meta = _gemini_extract_via_rest(prompt=prompt, limit=limit, output_tokens=output_tokens)
    return description, keywords, meta


def generate_chunk_description_and_keywords(
    *,
    chunk_name: str,
    explicit_description: str = "",
    explicit_keywords: Sequence[str] | None = None,
    lesson_name: str = "",
    topic_name: str = "",
    subject_name: str = "",
    file_path: str = "",
    limit: int = 5,
) -> Tuple[str, List[str], Dict[str, Any]]:
    description = _clean(explicit_description)
    manual_keywords = _filter_chunk_keyword_values(
        _normalize_keywords(list(explicit_keywords or []), limit=limit),
        chunk_name=chunk_name,
        lesson_name=lesson_name,
        topic_name=topic_name,
        subject_name=subject_name,
        description=description,
        limit=None,
    )

    desc_meta: Dict[str, Any] = {"mode": "manual_description"}
    if not description:
        ai_description, _ignored_keywords, desc_meta = _generate_from_file_or_rest(
            prompt=_chunk_description_only_prompt(
                chunk_name=chunk_name,
                lesson_name=lesson_name,
                topic_name=topic_name,
                subject_name=subject_name,
            ),
            file_path=file_path,
            limit=None,
            output_tokens=360,
        )
        description = ai_description

    final_description = description or _fallback_description(chunk_name, lesson_name, topic_name, subject_name)

    keyword_meta: Dict[str, Any] = {"mode": "manual_keywords"}
    ai_keywords: List[str] = []
    if len(manual_keywords) < limit:
        ai_keywords, keyword_meta = _generate_chunk_keywords_from_reference_style(
            file_path=file_path,
            prompt=_chunk_keywords_only_prompt(
                chunk_name=chunk_name,
                lesson_name=lesson_name,
                topic_name=topic_name,
                subject_name=subject_name,
                num_keywords=limit,
            ),
            limit=limit,
        )

    final_keywords = _filter_chunk_keyword_values(
        [
            *manual_keywords,
            *ai_keywords,
            *_name_ngram_keywords(chunk_name, limit=None, min_words=2, max_words=5),
            *_name_ngram_keywords(lesson_name, limit=None, min_words=2, max_words=5),
            *_name_ngram_keywords(topic_name, limit=None, min_words=2, max_words=5),
        ],
        chunk_name=chunk_name,
        lesson_name=lesson_name,
        topic_name=topic_name,
        subject_name=subject_name,
        description=final_description,
        limit=limit,
    )

    meta: Dict[str, Any] = {
        "mode": "chunk_description_plus_reference_keywords",
        "description_meta": desc_meta,
        "keyword_meta": keyword_meta,
    }
    return final_description, final_keywords, meta


def generate_lesson_description_and_keywords(
    *,
    lesson_name: str,
    explicit_description: str = '',
    explicit_keywords: Sequence[str] | None = None,
    topic_name: str = '',
    subject_name: str = '',
    file_path: str = '',
    limit: int = 10,
) -> Tuple[str, List[str], Dict[str, Any]]:
    limit = 10
    description = _clean(explicit_description)
    manual_keywords = _filter_lesson_keyword_values(
        _normalize_keywords(list(explicit_keywords or []), limit=limit),
        lesson_name=lesson_name,
        topic_name=topic_name,
        subject_name=subject_name,
        description=description,
        limit=None,
    )

    desc_meta: Dict[str, Any] = {'mode': 'manual_description'}
    if not description:
        ai_description, _ignored_keywords, desc_meta = _generate_from_file_or_rest(
            prompt=_lesson_description_only_prompt(
                lesson_name=lesson_name,
                topic_name=topic_name,
                subject_name=subject_name,
            ),
            file_path=file_path,
            limit=None,
            output_tokens=480,
        )
        description = ai_description

    final_description = description or _fallback_description(lesson_name, lesson_name, topic_name, subject_name)

    final_keywords, keyword_meta = _collect_lesson_keywords_strict_ai_only(
        file_path=file_path,
        lesson_name=lesson_name,
        topic_name=topic_name,
        subject_name=subject_name,
        limit=limit,
        manual_keywords=manual_keywords,
    )
    final_keywords = _filter_lesson_keyword_values(
        final_keywords,
        lesson_name=lesson_name,
        topic_name=topic_name,
        subject_name=subject_name,
        description='',
        limit=limit,
    )

    meta: Dict[str, Any] = {
        'mode': 'lesson_description_plus_reference_keywords_strict',
        'description_meta': desc_meta,
        'keyword_meta': keyword_meta,
        'keyword_limit_locked': 10,
    }
    return final_description, final_keywords, meta

def _description_only_prompt(*, level: str, name: str, parent_name: str = "") -> str:
    level_vi = {"topic": "chủ đề", "subject": "môn học/cuốn sách"}.get(level, level)
    parent_line = f"Thuộc: {parent_name}\n" if parent_name else ""
    return (
        f"Bạn là chuyên gia phân tích học liệu. Hãy đọc toàn bộ file ở cấp {level_vi} và chỉ tạo mô tả phục vụ semantic search. "
        "Chỉ trả về description ngắn gọn nhưng đủ ý, bao quát đúng nội dung tài liệu. "
        "Không tạo keyword. Không markdown. Không giải thích. "
        "Trả về JSON đúng schema: {'description':'...'}.\n"
        f"Tên: {name}\n{parent_line}"
    )


def _generate_hierarchy_description_only(
    *,
    level: str,
    name: str,
    explicit_description: str = "",
    parent_name: str = "",
    file_path: str = "",
) -> Tuple[str, Dict[str, Any]]:
    description = _clean(explicit_description)
    if description:
        return description, {"mode": "manual_description"}

    ai_description, _ignored_keywords, meta = _generate_from_file_or_rest(
        prompt=_description_only_prompt(level=level, name=name, parent_name=parent_name),
        file_path=file_path,
        limit=None,
        output_tokens=420 if level == "subject" else 360,
    )
    final_description = description or ai_description or _fallback_description(name, name, parent_name, "")
    return final_description, meta


def generate_topic_description_only(
    *,
    topic_name: str,
    explicit_description: str = "",
    subject_name: str = "",
    file_path: str = "",
) -> Tuple[str, Dict[str, Any]]:
    return _generate_hierarchy_description_only(
        level="topic",
        name=topic_name,
        explicit_description=explicit_description,
        parent_name=subject_name,
        file_path=file_path,
    )


def generate_subject_description_only(
    *,
    subject_name: str,
    explicit_description: str = "",
    file_path: str = "",
) -> Tuple[str, Dict[str, Any]]:
    return _generate_hierarchy_description_only(
        level="subject",
        name=subject_name,
        explicit_description=explicit_description,
        parent_name="",
        file_path=file_path,
    )


def _generate_hierarchy_file_description_and_keywords(
    *,
    level: str,
    name: str,
    explicit_description: str = "",
    explicit_keywords: Sequence[str] | None = None,
    parent_name: str = "",
    file_path: str = "",
    limit: int = 40,
) -> Tuple[str, List[str], Dict[str, Any]]:
    manual_keywords = _normalize_keywords(list(explicit_keywords or []), limit=limit)
    description = _clean(explicit_description)

    ai_description = ""
    ai_keywords: List[str] = []
    meta: Dict[str, Any] = {"mode": "manual"}
    if not description or not manual_keywords:
        ai_description, ai_keywords, meta = _generate_from_file_or_rest(
            prompt=_hierarchy_prompt(level=level, name=name, parent_name=parent_name, num_keywords=limit),
            file_path=file_path,
            limit=limit,
            output_tokens=1600 if level == "subject" else 1200,
        )

    final_description = description or ai_description or _fallback_description(name, name, parent_name, "")
    final_keywords = _uniq_keep_order([
        *manual_keywords,
        *ai_keywords,
        *_description_seed_keywords(final_description, limit=limit),
        *_fallback_keywords(name, final_description, name, parent_name, limit=limit),
    ], limit=limit)
    return final_description, final_keywords, meta


def generate_topic_description_and_keywords(
    *,
    topic_name: str,
    explicit_description: str = "",
    explicit_keywords: Sequence[str] | None = None,
    subject_name: str = "",
    file_path: str = "",
    limit: int = 48,
) -> Tuple[str, List[str], Dict[str, Any]]:
    return _generate_hierarchy_file_description_and_keywords(
        level="topic",
        name=topic_name,
        explicit_description=explicit_description,
        explicit_keywords=explicit_keywords,
        parent_name=subject_name,
        file_path=file_path,
        limit=limit,
    )


def generate_subject_description_and_keywords(
    *,
    subject_name: str,
    explicit_description: str = "",
    explicit_keywords: Sequence[str] | None = None,
    file_path: str = "",
    limit: int = 80,
) -> Tuple[str, List[str], Dict[str, Any]]:
    return _generate_hierarchy_file_description_and_keywords(
        level="subject",
        name=subject_name,
        explicit_description=explicit_description,
        explicit_keywords=explicit_keywords,
        parent_name="",
        file_path=file_path,
        limit=limit,
    )
