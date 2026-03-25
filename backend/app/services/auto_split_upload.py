from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

try:
    from pypdf import PdfReader, PdfWriter
except Exception:  # pragma: no cover
    from PyPDF2 import PdfReader, PdfWriter  # type: ignore

try:
    import google.generativeai as genai
except Exception:  # pragma: no cover
    genai = None

from .gemini_topic_expander import _rotated_keys

ProgressCb = Optional[Callable[[str, str, float, str], None]]

_PREVIEW_PAGES = 50
_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.I | re.S)
_JSON_BLOCK_RE = re.compile(r"(\{.*\}|\[.*\])", re.S)


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _emit(cb: ProgressCb, stage: str, label: str, percent: float, message: str = "") -> None:
    if cb is None:
        return
    try:
        cb(stage, label, percent, message or label)
    except Exception:
        pass


def _parse_json_loose(text: str) -> Dict[str, Any]:
    raw = _clean(text)
    if not raw:
        raise ValueError("empty_response")
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    m = _JSON_FENCE_RE.search(raw)
    if m:
        block = _clean(m.group(1))
        try:
            obj = json.loads(block)
            if isinstance(obj, dict):
                return obj
        except Exception:
            raw = block

    m = _JSON_BLOCK_RE.search(raw)
    if m:
        block = _clean(m.group(1))
        try:
            obj = json.loads(block)
            if isinstance(obj, dict):
                return obj
        except Exception:
            raise ValueError(f"invalid_json; raw={block[:1200]}")

    raise ValueError(f"empty_or_invalid_json; raw={raw[:1200]}")


def _make_preview_first_pages(src_pdf: str, *, first_n_pages: int = _PREVIEW_PAGES) -> str:
    reader = PdfReader(src_pdf)
    total = len(reader.pages)
    n = min(max(1, int(first_n_pages)), total)

    writer = PdfWriter()
    for i in range(n):
        writer.add_page(reader.pages[i])

    fd, tmp_path = tempfile.mkstemp(suffix=f"_preview_{n}p.pdf")
    os.close(fd)
    with open(tmp_path, "wb") as f:
        writer.write(f)
    return tmp_path


def _extract_offset(data: Dict[str, Any]) -> int:
    offsets: List[int] = []
    samples = data.get("page_number_samples")
    if isinstance(samples, list):
        for item in samples:
            if not isinstance(item, dict):
                continue
            pdf_page = item.get("preview_pdf_page")
            printed_page = item.get("printed_page")
            if isinstance(pdf_page, int) and isinstance(printed_page, int) and printed_page > 0:
                offsets.append(pdf_page - printed_page)
    if offsets:
        counter = Counter(offsets)
        return counter.most_common(1)[0][0]
    raw_offset = data.get("page_number_offset")
    if isinstance(raw_offset, int):
        return raw_offset
    return 0


def _normalize_title(title: Any) -> str:
    return _clean(title)


def _extract_heading_number(heading: str) -> Optional[int]:
    m = re.search(r"(\d+)", _clean(heading))
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _normalize_range_list(
    list_ranges: Any,
    prefix: str,
    *,
    offset: int,
    total_pages: int,
    final_cap: Optional[int] = None,
) -> List[Dict[str, Dict[str, Any]]]:
    if not isinstance(list_ranges, list):
        return []

    rows: List[Dict[str, Any]] = []
    seen = set()
    for item in list_ranges:
        if not isinstance(item, dict) or len(item) != 1:
            continue
        name, obj = next(iter(item.items()))
        if not isinstance(name, str) or not name.startswith(prefix):
            continue
        if not isinstance(obj, dict):
            continue
        printed_start = obj.get("printed_start")
        if not isinstance(printed_start, int) or printed_start < 1:
            continue
        start = max(1, min(total_pages, printed_start + offset))
        heading = _clean(obj.get("heading"))
        title = _normalize_title(obj.get("title"))
        num = _extract_heading_number(heading)
        dedup_key = (start, heading.casefold(), title.casefold())
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        rows.append({
            "start": start,
            "printed_start": printed_start,
            "heading": heading,
            "title": title,
            "number": num,
        })

    if not rows:
        return []

    rows.sort(key=lambda x: (x["start"], x["printed_start"], x["heading"], x["title"]))
    cap = total_pages if final_cap is None else max(1, min(int(final_cap), total_pages))

    out: List[Dict[str, Dict[str, Any]]] = []
    for i, row in enumerate(rows):
        start = row["start"]
        if start > cap:
            continue
        if i < len(rows) - 1:
            next_start = rows[i + 1]["start"]
            end = max(start, min(next_start - 1, cap))
        else:
            end = max(start, cap)
        out.append(
            {
                f"{prefix}_{len(out)+1:02d}": {
                    "start": start,
                    "end": end,
                    "heading": row["heading"],
                    "title": row["title"],
                    "printed_start": row["printed_start"],
                    "number": row["number"],
                }
            }
        )
    return out


def _normalize_manifest_subject(data: Dict[str, Any], total_pages: int) -> Dict[str, Any]:
    offset = _extract_offset(data)
    main_content_end_printed = data.get("main_content_end_printed")
    lesson_cap = total_pages
    if isinstance(main_content_end_printed, int) and main_content_end_printed > 0:
        lesson_cap = max(1, min(main_content_end_printed + offset, total_pages))

    lessons = _normalize_range_list(
        data.get("list_lesson"),
        "lesson",
        offset=offset,
        total_pages=total_pages,
        final_cap=lesson_cap,
    )

    topic_cap = lesson_cap
    if lessons:
        last_lesson_obj = next(iter(lessons[-1].values()))
        topic_cap = int(last_lesson_obj.get("end") or topic_cap)

    topics = _normalize_range_list(
        data.get("list_topic"),
        "topic",
        offset=offset,
        total_pages=total_pages,
        final_cap=topic_cap,
    )

    out = dict(data)
    out["page_number_offset"] = offset
    out["main_content_end_pdf"] = lesson_cap
    out["list_topic"] = topics
    out["list_lesson"] = lessons
    return out


def _normalize_manifest_topic(data: Dict[str, Any], total_pages: int, *, fallback_title: str = "") -> Dict[str, Any]:
    offset = _extract_offset(data)
    lessons = _normalize_range_list(
        data.get("list_lesson"),
        "lesson",
        offset=offset,
        total_pages=total_pages,
        final_cap=total_pages,
    )
    topics = _normalize_range_list(
        data.get("list_topic"),
        "topic",
        offset=offset,
        total_pages=total_pages,
        final_cap=total_pages,
    )
    if not topics:
        topics = [{
            "topic_01": {
                "start": 1,
                "end": lessons[-1][next(iter(lessons[-1]))]["end"] if lessons else total_pages,
                "heading": "",
                "title": _clean(fallback_title),
                "printed_start": None,
                "number": None,
            }
        }]
    out = dict(data)
    out["page_number_offset"] = offset
    out["main_content_end_pdf"] = total_pages
    out["list_topic"] = topics
    out["list_lesson"] = lessons
    return out


def _build_subject_prompt(total_pages_full: int, preview_pages: int) -> str:
    return (
        "QUAN TRỌNG:\n"
        f"- File bạn đang xem là bản preview gồm {preview_pages} trang đầu của FILE GỐC.\n"
        f"- Tổng số trang FILE GỐC là {total_pages_full}.\n"
        "- CHỈ trả printed_start trong list_topic/list_lesson, KHÔNG trả số trang PDF ở đó.\n"
        "- Bắt buộc trả page_number_samples và page_number_offset nếu suy ra được.\n"
        "- Không lấy 'Lời nói đầu', 'Mục lục', 'Hướng dẫn sử dụng sách' làm topic hay lesson.\n"
        "- Nếu một trang bắt đầu bài đồng thời có cả 'Chủ đề X' và 'Bài Y' thì đó vẫn là trang bắt đầu hợp lệ của lesson Y.\n"
        "- Nếu không chắc main_content_end_printed thì trả null.\n\n"
        "Bạn là chương trình trích xuất cấu trúc từ SGK PDF.\n"
        "BẠN ĐANG XEM BẢN PREVIEW CỦA FILE GỐC.\n"
        "Mục tiêu của bạn là đọc phần đầu sách (Lời nói đầu, Mục lục, các trang nội dung đầu tiên)\n"
        "để trích xuất CHỦ ĐỀ và BÀI một cách ổn định.\n\n"
        "OUTPUT PHẢI GỒM: page_number_offset, page_number_samples, main_content_end_printed, list_topic, list_lesson.\n"
        "QUY TẮC NHẬN DIỆN:\n"
        "- list_topic: chỉ gồm các dòng bắt đầu bằng đúng mẫu 'Chủ đề <SỐ>.'\n"
        "- list_lesson: chỉ gồm các dòng bắt đầu bằng đúng mẫu 'Bài <SỐ>.'\n"
        "- heading phải giữ nguyên kiểu như 'Chủ đề 1.' hoặc 'Bài 16.'\n"
        "- Không dùng số trang PDF trong list_topic/list_lesson.\n"
        "- Nếu không chắc thì bỏ qua, không đoán bừa.\n\n"
        "CHỈ TRẢ JSON THUẦN. KHÔNG markdown. KHÔNG giải thích.\n\n"
        "FORMAT:\n"
        "{\n"
        "  \"page_number_offset\": 2,\n"
        "  \"page_number_samples\": [\n"
        "    {\"preview_pdf_page\": 7, \"printed_page\": 5},\n"
        "    {\"preview_pdf_page\": 8, \"printed_page\": 6}\n"
        "  ],\n"
        "  \"main_content_end_printed\": 168,\n"
        "  \"list_topic\": [\n"
        "    {\"topic_01\": {\"printed_start\": 5, \"heading\": \"Chủ đề 1.\", \"title\": \"...\"}}\n"
        "  ],\n"
        "  \"list_lesson\": [\n"
        "    {\"lesson_01\": {\"printed_start\": 5, \"heading\": \"Bài 1.\", \"title\": \"...\"}}\n"
        "  ]\n"
        "}"
    )


def _build_topic_prompt(total_pages_full: int, preview_pages: int) -> str:
    return (
        "QUAN TRỌNG:\n"
        f"- File bạn đang xem là bản preview gồm {preview_pages} trang đầu của FILE GỐC.\n"
        f"- Tổng số trang FILE GỐC là {total_pages_full}.\n"
        "- CHỈ trả printed_start trong list_topic/list_lesson, KHÔNG trả số trang PDF ở đó.\n"
        "- Bắt buộc trả page_number_samples và page_number_offset nếu suy ra được.\n"
        "- Đây là file của MỘT CHỦ ĐỀ; hãy trích xuất 1 topic hiện tại (nếu nhìn rõ) và tất cả các BÀI thuộc topic đó.\n"
        "- Không lấy mục lục/phụ lục/đáp án làm bài.\n\n"
        "Bạn là chương trình trích xuất cấu trúc từ PDF của 1 CHỦ ĐỀ trong SGK.\n"
        "Mục tiêu của bạn là đọc toàn bộ file hoặc phần đầu file để xác định topic hiện tại và danh sách lesson.\n\n"
        "OUTPUT PHẢI GỒM: page_number_offset, page_number_samples, list_topic, list_lesson.\n"
        "- list_topic: tối đa 1 topic hiện tại nếu nhìn rõ heading 'Chủ đề <SỐ>.'\n"
        "- list_lesson: các dòng bắt đầu bằng 'Bài <SỐ>.'\n"
        "- Không dùng số trang PDF trực tiếp trong list_topic/list_lesson.\n"
        "- Nếu không chắc topic thì có thể để list_topic rỗng, nhưng list_lesson phải lấy tối đa các bài nhìn rõ.\n\n"
        "CHỈ TRẢ JSON THUẦN. KHÔNG markdown. KHÔNG giải thích.\n\n"
        "FORMAT:\n"
        "{\n"
        "  \"page_number_offset\": 2,\n"
        "  \"page_number_samples\": [\n"
        "    {\"preview_pdf_page\": 7, \"printed_page\": 39}\n"
        "  ],\n"
        "  \"list_topic\": [\n"
        "    {\"topic_01\": {\"printed_start\": 39, \"heading\": \"Chủ đề 4.\", \"title\": \"...\"}}\n"
        "  ],\n"
        "  \"list_lesson\": [\n"
        "    {\"lesson_01\": {\"printed_start\": 39, \"heading\": \"Bài 7.\", \"title\": \"...\"}},\n"
        "    {\"lesson_02\": {\"printed_start\": 46, \"heading\": \"Bài 8.\", \"title\": \"...\"}}\n"
        "  ]\n"
        "}"
    )


def _gemini_extract_pdf(*, pdf_path: str, prompt: str, model: str) -> Dict[str, Any]:
    if genai is None:
        raise RuntimeError("google-generativeai_not_installed")
    rotated, meta = _rotated_keys()
    if not rotated:
        raise RuntimeError("no_api_key")
    last_error: Optional[str] = None
    for _slot, api_key, _source in rotated:
        uploaded = None
        try:
            genai.configure(api_key=api_key)
            model_obj = genai.GenerativeModel(model)
            uploaded = genai.upload_file(pdf_path, mime_type="application/pdf")
            response = model_obj.generate_content(
                [prompt, uploaded],
                generation_config={
                    "temperature": 0,
                    "response_mime_type": "application/json",
                },
            )
            text = getattr(response, "text", "") or ""
            return _parse_json_loose(text)
        except Exception as exc:  # pragma: no cover - network dependent
            last_error = str(exc)
            continue
        finally:
            try:
                if uploaded is not None and hasattr(uploaded, "delete"):
                    uploaded.delete()
            except Exception:
                pass
    raise RuntimeError(last_error or f"gemini_extract_failed; key_count={meta.get('key_count', 0)}")


def _flatten_list_items(list_ranges: List[Dict[str, Dict[str, Any]]], kind: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in list_ranges:
        if not isinstance(item, dict) or len(item) != 1:
            continue
        name, rng = next(iter(item.items()))
        if not isinstance(rng, dict):
            continue
        start = rng.get("start")
        end = rng.get("end")
        if not isinstance(start, int) or not isinstance(end, int):
            continue
        heading = _clean(rng.get("heading"))
        title = _clean(rng.get("title"))
        number = rng.get("number")
        if not isinstance(number, int):
            number = _extract_heading_number(heading)
        out.append({
            "name": str(name),
            "start": start,
            "end": end,
            "kind": kind,
            "heading": heading,
            "title": title,
            "number": number,
            "printed_start": rng.get("printed_start"),
        })
    return out


def _split_pdf_by_ranges(src_pdf: str, ranges: Iterable[Tuple[str, int, int]], out_dir: Path, pdf_stem: str) -> List[Path]:
    reader = PdfReader(src_pdf)
    total_pages = len(reader.pages)
    outputs: List[Path] = []
    for name, start, end in ranges:
        if start < 1 or end < 1 or start > end or start > total_pages:
            continue
        end = min(end, total_pages)
        writer = PdfWriter()
        for idx in range(start - 1, end):
            writer.add_page(reader.pages[idx])
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", _clean(name) or "part").strip("_") or "part"
        out_path = out_dir / f"{pdf_stem}_{safe_name}.pdf"
        with open(out_path, "wb") as f:
            writer.write(f)
        outputs.append(out_path)
    return outputs


def _find_parent_topic_for_lesson(lesson: Dict[str, Any], topics: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    start = int(lesson.get("start") or 0)
    candidates = [t for t in topics if int(t.get("start") or 0) <= start <= int(t.get("end") or 0)]
    if not candidates:
        return None
    candidates.sort(key=lambda x: (int(x.get("end") or 0) - int(x.get("start") or 0), int(x.get("start") or 0)))
    return candidates[0]


def _extract_structure_subject(pdf_path: str, *, model: str, progress_cb: ProgressCb = None) -> Dict[str, Any]:
    total_pages = len(PdfReader(pdf_path).pages)
    preview_pages = min(_PREVIEW_PAGES, total_pages)
    _emit(progress_cb, "analyzing", "Đang đọc cấu trúc sách", 0.12, "Đang đọc mục lục và cấu trúc sách")
    preview_pdf = _make_preview_first_pages(pdf_path, first_n_pages=preview_pages)
    try:
        raw = _gemini_extract_pdf(pdf_path=preview_pdf, prompt=_build_subject_prompt(total_pages, preview_pages), model=model)
    finally:
        try:
            os.remove(preview_pdf)
        except Exception:
            pass
    manifest = _normalize_manifest_subject(raw, total_pages)
    topics = _flatten_list_items(manifest.get("list_topic") or [], "topic")
    lessons = _flatten_list_items(manifest.get("list_lesson") or [], "lesson")
    if not topics:
        raise RuntimeError("Không tách được chủ đề từ file upload")
    if not lessons:
        raise RuntimeError("Không tách được bài học từ file upload")

    _emit(progress_cb, "splitting", "Đang cắt file thành topic và lesson", 0.26, "Đang cắt các file topic và lesson")
    temp_dir = tempfile.mkdtemp(prefix="auto_split_subject_")
    topic_dir = Path(temp_dir) / "topics"
    lesson_dir = Path(temp_dir) / "lessons"
    topic_dir.mkdir(parents=True, exist_ok=True)
    lesson_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(pdf_path).stem

    topic_paths = _split_pdf_by_ranges(pdf_path, [(x["name"], x["start"], x["end"]) for x in topics], topic_dir, stem)
    lesson_paths = _split_pdf_by_ranges(pdf_path, [(x["name"], x["start"], x["end"]) for x in lessons], lesson_dir, stem)

    for idx, item in enumerate(topics):
        item["file_path"] = str(topic_paths[idx]) if idx < len(topic_paths) else ""
    for idx, item in enumerate(lessons):
        item["file_path"] = str(lesson_paths[idx]) if idx < len(lesson_paths) else ""
        parent = _find_parent_topic_for_lesson(item, topics)
        if parent:
            item["topic_number"] = parent.get("number")
            item["topic_heading"] = parent.get("heading")
            item["topic_title"] = parent.get("title")
        else:
            item["topic_number"] = None
            item["topic_heading"] = ""
            item["topic_title"] = ""

    return {
        "mode": "subject",
        "temp_dir": temp_dir,
        "manifest": manifest,
        "topics": topics,
        "lessons": lessons,
        "total_pages": total_pages,
    }


def _extract_structure_topic(pdf_path: str, *, model: str, progress_cb: ProgressCb = None) -> Dict[str, Any]:
    total_pages = len(PdfReader(pdf_path).pages)
    preview_pages = min(_PREVIEW_PAGES, total_pages)
    _emit(progress_cb, "analyzing", "Đang đọc cấu trúc chủ đề", 0.12, "Đang nhận diện các bài trong chủ đề")
    preview_pdf = _make_preview_first_pages(pdf_path, first_n_pages=preview_pages)
    try:
        raw = _gemini_extract_pdf(
            pdf_path=preview_pdf,
            prompt=_build_topic_prompt(total_pages, preview_pages),
            model=model,
        )
    finally:
        try:
            os.remove(preview_pdf)
        except Exception:
            pass
    manifest = _normalize_manifest_topic(raw, total_pages, fallback_title=Path(pdf_path).stem)
    topics = _flatten_list_items(manifest.get("list_topic") or [], "topic")
    lessons = _flatten_list_items(manifest.get("list_lesson") or [], "lesson")
    if not lessons:
        raise RuntimeError("Không tách được bài học từ file upload")
    topic_item = topics[0] if topics else {"number": None, "heading": "", "title": Path(pdf_path).stem, "start": 1, "end": total_pages}

    _emit(progress_cb, "splitting", "Đang cắt file thành lesson", 0.26, "Đang cắt các file lesson")
    temp_dir = tempfile.mkdtemp(prefix="auto_split_topic_")
    lesson_dir = Path(temp_dir) / "lessons"
    lesson_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(pdf_path).stem
    lesson_paths = _split_pdf_by_ranges(pdf_path, [(x["name"], x["start"], x["end"]) for x in lessons], lesson_dir, stem)
    for idx, item in enumerate(lessons):
        item["file_path"] = str(lesson_paths[idx]) if idx < len(lesson_paths) else ""
        item["topic_number"] = topic_item.get("number")
        item["topic_heading"] = topic_item.get("heading")
        item["topic_title"] = topic_item.get("title")
    return {
        "mode": "topic",
        "temp_dir": temp_dir,
        "manifest": manifest,
        "topic": topic_item,
        "lessons": lessons,
        "total_pages": total_pages,
    }


def extract_and_split_structure(pdf_path: str, *, mode: str, progress_cb: ProgressCb = None, model: str = "gemini-2.5-flash") -> Dict[str, Any]:
    current_mode = _clean(mode).lower()
    if current_mode == "subject":
        return _extract_structure_subject(pdf_path, model=model, progress_cb=progress_cb)
    if current_mode == "topic":
        return _extract_structure_topic(pdf_path, model=model, progress_cb=progress_cb)
    raise ValueError("mode phải là 'subject' hoặc 'topic'")


def cleanup_split_result(result: Dict[str, Any] | None) -> None:
    temp_dir = _clean((result or {}).get("temp_dir"))
    if not temp_dir:
        return
    try:
        shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception:
        pass
