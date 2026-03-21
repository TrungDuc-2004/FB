from __future__ import annotations

import mimetypes
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Tuple

from .gemini_topic_expander import _clean, _extract_json_payload, _rotated_keys

try:
    import google.generativeai as genai
except Exception:  # pragma: no cover
    genai = None

_ALLOWED_SUFFIXES = {
    ".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp",
    ".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v",
}


def _slug_to_text(value: str) -> str:
    text = _clean(value)
    if not text:
        return ""
    text = Path(text).stem
    text = re.sub(r"[_\-.]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _fallback_media_description(*, media_type: str, file_name: str = "", follow_type: str = "", map_id: str = "") -> str:
    title = _slug_to_text(file_name) or _clean(map_id) or ("hình ảnh" if media_type == "image" else "video")
    follow_vi = {
        "subject": "môn học",
        "topic": "chủ đề",
        "lesson": "bài học",
        "chunk": "nội dung học",
    }.get(_clean(follow_type).lower(), "nội dung học")
    if media_type == "image":
        return f"Hình ảnh liên quan tới {follow_vi}, tập trung vào nội dung {title.lower()} phục vụ tra cứu và học tập."
    return f"Video liên quan tới {follow_vi}, minh hoạ hoặc giải thích nội dung {title.lower()} phục vụ tra cứu và học tập."


def _media_prompt(*, media_type: str, file_name: str = "", follow_type: str = "", map_id: str = "") -> str:
    kind = "ảnh" if media_type == "image" else "video"
    return (
        "Bạn đang hỗ trợ xây dựng metadata cho hệ thống học liệu. "
        f"Hãy xem {kind} và viết đúng 1 câu mô tả ngắn gọn bằng tiếng Việt, tối đa 35 từ, phục vụ tìm kiếm. "
        "Mô tả phải nói rõ đối tượng/chủ đề chính nhìn thấy hoặc nội dung minh hoạ chính. "
        "Không nhắc tới việc bạn là AI, không markdown, không mở đầu bằng 'Đây là', không suy đoán quá mức nếu nội dung không rõ. "
        "Trả về JSON đúng schema: {'description':'...'} và không thêm gì khác.\n"
        f"Tên file: {_clean(file_name)}\n"
        f"Cấp theo dõi: {_clean(follow_type)}\n"
        f"MapID: {_clean(map_id)}"
    )


def _extract_description(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("description") or value.get("caption") or value.get("text") or ""
    text = _clean(value)
    text = re.sub(r"\s+", " ", text).strip(" \n\t\r`'\"")
    if not text:
        return ""
    if len(text.split()) > 40:
        text = " ".join(text.split()[:40]).rstrip(" ,.;:-") + "."
    return text


def _extract_description_from_response(text: str) -> str:
    obj = _extract_json_payload(text)
    if isinstance(obj, dict):
        desc = _extract_description(obj)
        if desc:
            return desc
    return _extract_description(text)


def _wait_until_ready(uploaded: Any, timeout_seconds: int = 90) -> Any:
    if genai is None or uploaded is None:
        return uploaded
    started = time.time()
    current = uploaded
    while time.time() - started < timeout_seconds:
        state = getattr(getattr(current, "state", None), "name", "") or ""
        state = str(state).upper()
        if not state or state in {"ACTIVE", "SUCCEEDED", "READY"}:
            return current
        if state in {"FAILED", "ERROR", "CANCELLED"}:
            raise RuntimeError(f"uploaded_file_state={state}")
        time.sleep(2)
        file_name = getattr(current, "name", "")
        if file_name and hasattr(genai, "get_file"):
            current = genai.get_file(file_name)
    return current


def generate_media_description(
    *,
    media_type: str,
    file_path: str = "",
    file_name: str = "",
    explicit_description: str = "",
    follow_type: str = "",
    map_id: str = "",
) -> Tuple[str, Dict[str, Any]]:
    manual = _extract_description(explicit_description)
    if manual:
        return manual, {"mode": "manual"}

    model = _clean(os.getenv("GEMINI_MODEL")) or "gemini-2.5-flash"
    meta: Dict[str, Any] = {"mode": "fallback", "model": model, "mediaType": media_type}

    path = Path(file_path) if file_path else None
    if genai is None:
        return _fallback_media_description(media_type=media_type, file_name=file_name, follow_type=follow_type, map_id=map_id), {
            **meta,
            "error": "google-generativeai_not_installed",
        }
    if not path or not path.exists():
        return _fallback_media_description(media_type=media_type, file_name=file_name, follow_type=follow_type, map_id=map_id), {
            **meta,
            "error": "file_not_found",
        }
    if path.suffix.lower() not in _ALLOWED_SUFFIXES:
        return _fallback_media_description(media_type=media_type, file_name=file_name, follow_type=follow_type, map_id=map_id), {
            **meta,
            "error": f"unsupported_suffix:{path.suffix.lower()}",
        }

    rotated, key_meta = _rotated_keys()
    meta["key_count"] = key_meta.get("key_count", 0)
    if not rotated:
        return _fallback_media_description(media_type=media_type, file_name=file_name, follow_type=follow_type, map_id=map_id), {
            **meta,
            "error": "no_api_key",
        }

    mime_type, _ = mimetypes.guess_type(str(path))
    mime_type = mime_type or ("image/png" if media_type == "image" else "video/mp4")
    prompt = _media_prompt(media_type=media_type, file_name=file_name or path.name, follow_type=follow_type, map_id=map_id)
    last_error = ""

    for attempt, (slot, key, source) in enumerate(rotated, start=1):
        uploaded = None
        try:
            genai.configure(api_key=key)
            model_obj = genai.GenerativeModel(model)
            uploaded = genai.upload_file(str(path), mime_type=mime_type)
            uploaded = _wait_until_ready(uploaded)
            response = model_obj.generate_content([prompt, uploaded])
            text = getattr(response, "text", "") or ""
            description = _extract_description_from_response(text)
            if description:
                return description, {
                    **meta,
                    "mode": "file_ai",
                    "attempt": attempt,
                    "key_slot": slot,
                    "key_source": source,
                }
            last_error = "empty_response"
        except Exception as exc:
            last_error = str(exc)
        finally:
            try:
                if uploaded is not None:
                    uploaded.delete()
            except Exception:
                pass

    return _fallback_media_description(media_type=media_type, file_name=file_name, follow_type=follow_type, map_id=map_id), {
        **meta,
        "error": last_error or "extract_failed",
    }