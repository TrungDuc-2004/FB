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


def _lesson_prompt(*, lesson_name: str, topic_name: str, subject_name: str, num_keywords: int = 15) -> str:
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
        "Hãy làm giống như với chunk nhưng ở mức bài học: mô tả phải bao quát các phần chính của bài, các khái niệm, định nghĩa, quy trình, ví dụ, ứng dụng nếu có. "
        "Keyword phải là keyword tìm kiếm tốt, ưu tiên cụm từ 2-6 từ, không lấy map id, không lấy token rời rạc vô nghĩa. "
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
    """Sinh chunkDescription + keywords.

    Ưu tiên:
    1) giữ dữ liệu explicit người dùng nhập
    2) dùng Gemini với file upload (tham khảo project Keyword.zip)
    3) fallback Gemini text-only
    4) fallback heuristic local
    """
    manual_keywords = _normalize_keywords(list(explicit_keywords or []), limit=limit)
    description = _clean(explicit_description)

    ai_description = ""
    ai_keywords: List[str] = []
    meta: Dict[str, Any] = {"mode": "manual"}

    if not description or not manual_keywords:
        ai_description, ai_keywords, meta = _generate_from_file_or_rest(
            prompt=_chunk_prompt(
                chunk_name=chunk_name,
                lesson_name=lesson_name,
                topic_name=topic_name,
                subject_name=subject_name,
                num_keywords=limit,
            ),
            file_path=file_path,
            limit=limit,
            output_tokens=512,
        )

    final_description = description or ai_description or _fallback_description(chunk_name, lesson_name, topic_name, subject_name)
    final_keywords = _uniq_keep_order([
        *manual_keywords,
        *ai_keywords,
        *_fallback_keywords(chunk_name, final_description, lesson_name, topic_name, limit=limit),
    ], limit=limit)
    return final_description, final_keywords, meta


def generate_lesson_description_and_keywords(
    *,
    lesson_name: str,
    explicit_description: str = "",
    explicit_keywords: Sequence[str] | None = None,
    topic_name: str = "",
    subject_name: str = "",
    file_path: str = "",
    limit: int = 15,
) -> Tuple[str, List[str], Dict[str, Any]]:
    manual_keywords = _normalize_keywords(list(explicit_keywords or []), limit=limit)
    description = _clean(explicit_description)

    ai_description = ""
    ai_keywords: List[str] = []
    meta: Dict[str, Any] = {"mode": "manual"}
    if not description or not manual_keywords:
        ai_description, ai_keywords, meta = _generate_from_file_or_rest(
            prompt=_lesson_prompt(
                lesson_name=lesson_name,
                topic_name=topic_name,
                subject_name=subject_name,
                num_keywords=limit,
            ),
            file_path=file_path,
            limit=limit,
            output_tokens=900,
        )

    final_description = description or ai_description or _fallback_description(lesson_name, lesson_name, topic_name, subject_name)
    final_keywords = _uniq_keep_order([
        *manual_keywords,
        *ai_keywords,
        *_description_seed_keywords(final_description, limit=limit),
        *_fallback_keywords(lesson_name, final_description, lesson_name, topic_name, limit=limit),
    ], limit=limit)
    return final_description, final_keywords, meta


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
