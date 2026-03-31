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




def _build_chunk_prompt(total_pages: int) -> str:
    return f"""
Bạn đang đọc 1 file PDF chỉ chứa DUY NHẤT 1 BÀI (LESSON) (PDF scan).

MỤC TIÊU:
Trả về list_chunk là các MỤC CHÍNH của bài theo trang PDF của CHÍNH FILE này.

CHỈ tạo chunk khi THẤY RÕ "TIÊU ĐỀ MỤC CHÍNH" hợp lệ.
Nếu không chắc chắn 100% => BỎ QUA (không bịa).

ĐỊNH NGHĨA "TIÊU ĐỀ MỤC CHÍNH" HỢP LỆ:
- Có mẫu "<số>." ở ĐẦU DÒNG (ví dụ "1.", "2.", "3.", ...)
- Phần chữ ngay sau "<số>." là TIÊU ĐỀ IN HOA TOÀN BỘ (không có chữ thường)
- Không thuộc/không nằm trong các phần: "NHIỆM VỤ", "CÂU HỎI", "BÀI TẬP", "LUYỆN TẬP", "VẬN DỤNG", "HƯỚNG DẪN", "BƯỚC"...
- Không phải câu mệnh lệnh/thao tác.

RẤT QUAN TRỌNG (CHỐNG BỊA):
- Nếu KHÔNG nhìn thấy mục "1." thật sự (ở đầu dòng) => trả list_chunk rỗng [].
- TUYỆT ĐỐI không suy ra "1." chỉ vì thấy chữ IN HOA.

OUTPUT MỖI CHUNK (BẮT BUỘC ĐỦ 4 TRƯỜNG):
- start: SỐ TRANG PDF (1-based) nơi tiêu đề mục chính xuất hiện lần đầu.
- content_head: true/false
- heading: CHỈ CHỨA SỐ MỤC dạng "1." / "2." / "3." ...
- title: CHỈ PHẦN CHỮ SAU "<số>.", GIỮ NGUYÊN IN HOA.

content_head:
- true  nếu trên CÙNG trang start, phía TRÊN tiêu đề còn có nội dung thuộc mục trước.
- false nếu phía trên chỉ có header/footer/số trang hoặc tiêu đề nằm ngay đầu trang nội dung.

RÀNG BUỘC:
- heading phải tăng dần theo thứ tự xuất hiện.
- 1 <= start <= {total_pages}.
- Nếu bài KHÔNG có mục chính hợp lệ => trả list_chunk rỗng [].

YÊU CẦU OUTPUT:
- Chỉ JSON thuần, KHÔNG giải thích, KHÔNG markdown.

FORMAT:
{{
  "list_chunk": [
    {{"chunk_01": {{"start": 1, "content_head": false, "heading": "1.", "title": "..."}}}},
    {{"chunk_02": {{"start": 3, "content_head": true,  "heading": "2.", "title": "..."}}}}
  ]
}}
"""


def _flatten_chunk_start_head(list_chunk: Any) -> List[Tuple[int, bool, str, str]]:
    out: List[Tuple[int, bool, str, str]] = []
    if not isinstance(list_chunk, list):
        return out
    for item in list_chunk:
        if not isinstance(item, dict) or len(item) != 1:
            continue
        _name, obj = next(iter(item.items()))
        if not isinstance(obj, dict):
            continue
        s = obj.get("start")
        ch = obj.get("content_head")
        heading = _clean(obj.get("heading"))
        title = _clean(obj.get("title"))
        if isinstance(s, int) and isinstance(ch, bool):
            out.append((s, ch, heading, title))
    out.sort(key=lambda x: x[0])
    return out


def _compute_chunks_from_start_head(items: List[Tuple[int, bool, str, str]], total_pages: int) -> List[Dict[str, Dict[str, Any]]]:
    if total_pages < 1:
        return []
    if not items:
        return [
            {"chunk_01": {"start": 1, "end": total_pages, "content_head": False, "heading": "", "title": "KHÔNG CÓ MỤC CHÍNH"}}
        ]

    fixed: List[Tuple[int, bool, str, str]] = []
    for idx, (s, ch, heading, title) in enumerate(items):
        s = max(1, min(int(s), total_pages))
        if idx == 0:
            s = 1
            ch = False
        fixed.append((s, bool(ch), _clean(heading), _clean(title)))

    computed: List[Dict[str, Dict[str, Any]]] = []
    for i, (start, content_head, heading, title) in enumerate(fixed):
        if i < len(fixed) - 1:
            next_start, next_content_head, _next_heading, _next_title = fixed[i + 1]
            end = next_start if next_content_head else (next_start - 1)
            end = max(start, min(end, total_pages))
        else:
            end = total_pages
        computed.append(
            {
                f"chunk_{i+1:02d}": {
                    "start": start,
                    "end": end,
                    "content_head": content_head,
                    "heading": heading,
                    "title": title,
                    "number": _extract_heading_number(heading),
                }
            }
        )
    return computed


def _load_chunk_postprocess_module():
    try:
        from . import sgk_chunk_postprocess as mod  # type: ignore
        return mod
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "Không tải được module hậu xử lý chunk. Hãy cài thêm dependencies: "
            "opencv-python-headless, PyMuPDF, pypdfium2, paddleocr==3.2.0. "
            f"Chi tiết: {exc}"
        )


def _apply_manual_or_auto_cutline(
    *,
    chunk_pdf_path: str,
    chunk_meta_path: str,
    out_dir: Path,
    y_line_override: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    mod = _load_chunk_postprocess_module()
    pdf_path = Path(chunk_pdf_path)
    meta_path = Path(chunk_meta_path)
    if y_line_override is None:
        ocr = _apply_manual_or_auto_cutline._ocr if hasattr(_apply_manual_or_auto_cutline, "_ocr") else None
        if ocr is None:
            ocr = mod.build_ocr()
            setattr(_apply_manual_or_auto_cutline, "_ocr", ocr)
        return mod.process_one_chunk(ocr, meta_path, pdf_path, out_dir)

    meta = mod.read_json(meta_path)
    heading = str(meta.get("heading", "")).strip()
    heading_num = mod.extract_heading_num(heading)
    if heading_num is None:
        raise RuntimeError(f"Chunk không có heading hợp lệ để cắt tay: {meta_path.name}")

    img = mod.render_pdf_page0_to_bgr(pdf_path, dpi=mod.DPI)
    y_line = max(0, min(int(y_line_override), int(img.shape[0]) - 1))
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = meta_path.stem
    out_debug_png = out_dir / f"{stem}_cutline.png"
    out_cut_json = out_dir / f"{stem}_cutline.json"
    out_top_png = out_dir / f"{stem}_cutline_top.png"
    out_bot_png = out_dir / f"{stem}_cutline_bot.png"

    line_stub = {"x0": 0, "y0": y_line, "x1": int(img.shape[1]), "y1": y_line + 2}
    mod.draw_debug(img, line_stub, y_line, out_debug_png, label=f"manual_y_line={y_line}")

    is_content_head = bool(meta.get("content_head", False))
    if is_content_head:
        split_info = mod.split_and_save(img, y_line, out_top_png, out_bot_png)
        pdf_update = mod.update_pdfs_for_content_head(
            cur_chunk_pdf=pdf_path,
            cur_chunk_stem=stem,
            top_png=out_top_png,
            bot_png=out_bot_png,
            chunk_pdf_dir=pdf_path.parent,
            make_backup=False,
        ) if split_info.get("top_saved") and split_info.get("bot_saved") else {"skipped": True, "reason": "split_missing"}
        mode = "content_head"
    else:
        split_info = mod.split_and_save_bot_only(img, y_line, out_bot_png)
        pdf_update = mod.update_pdf_page0_with_bot_only(
            cur_chunk_pdf=pdf_path,
            bot_png=out_bot_png,
            make_backup=False,
        ) if split_info.get("bot_saved") else {"skipped": True, "reason": "split_missing"}
        mode = "heading_bot_only"

    payload = {
        "chunk_json": str(meta_path.resolve()),
        "chunk_pdf": str(pdf_path.resolve()),
        "heading": heading,
        "heading_num": int(heading_num),
        "title": str(meta.get("title", "")).strip(),
        "expected_letters": [],
        "matched_prefix": 0,
        "observed_initials": [],
        "line_bbox": line_stub,
        "y_line": int(y_line),
        "dpi": int(mod.DPI),
        "offset_px": int(mod.OFFSET),
        "image_size": {"w": int(img.shape[1]), "h": int(img.shape[0])},
        "split_info": split_info,
        "pdf_update": pdf_update,
        "mode": mode,
        "run_mode": mode,
        "best_mode": "manual",
        "prefix_hits": 0,
        "weak_cut": False,
        "weak_reason": None,
        "failed": False,
        "fail_reason": None,
        "soft_fail": False,
        "soft_fail_reason": None,
        "force_cut": True,
        "manual_override": True,
    }
    mod.write_json_atomic(out_cut_json, payload)
    return payload


def _extract_chunks_for_lesson_pdf(
    lesson_pdf: str,
    *,
    lesson_name: str,
    lesson_heading: str,
    lesson_title: str,
    lesson_number: Optional[int],
    topic_number: Optional[int],
    temp_dir: str,
    progress_cb: ProgressCb = None,
    model: str = "gemini-2.5-flash",
) -> List[Dict[str, Any]]:
    total_pages = len(PdfReader(lesson_pdf).pages)
    raw = _gemini_extract_pdf(pdf_path=lesson_pdf, prompt=_build_chunk_prompt(total_pages), model=model)
    items = _flatten_chunk_start_head(raw.get("list_chunk") or [])
    computed = _compute_chunks_from_start_head(items, total_pages)

    chunk_root = Path(temp_dir) / "chunks"
    lesson_chunk_dir = chunk_root / lesson_name
    lesson_chunk_dir.mkdir(parents=True, exist_ok=True)
    outputs: List[Dict[str, Any]] = []

    for idx, item in enumerate(computed, start=1):
        chunk_name, obj = next(iter(item.items()))
        start = int(obj.get("start") or 1)
        end = int(obj.get("end") or start)
        chunk_dir = lesson_chunk_dir / chunk_name
        chunk_dir.mkdir(parents=True, exist_ok=True)
        paths = _split_pdf_by_ranges(lesson_pdf, [(chunk_name, start, end)], chunk_dir, lesson_name)
        if not paths:
            continue
        chunk_pdf_path = paths[0]
        chunk_meta_path = chunk_pdf_path.with_suffix('.json')
        payload = {
            "source_lesson_pdf": str(Path(lesson_pdf).resolve()),
            "lesson_stem": lesson_name,
            "chunk": chunk_name,
            "chunk_pdf": str(chunk_pdf_path),
            "heading": obj.get("heading", ""),
            "title": obj.get("title", ""),
            "start": start,
            "end": end,
            "content_head": bool(obj.get("content_head", False)),
            "total_pages": total_pages,
        }
        chunk_meta_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

        debug_dir = chunk_dir / 'DebugCutlines'
        cut_payload = None
        cut_error = None
        try:
            cut_payload = _apply_manual_or_auto_cutline(
                chunk_pdf_path=str(chunk_pdf_path),
                chunk_meta_path=str(chunk_meta_path),
                out_dir=debug_dir,
                y_line_override=None,
            )
        except Exception as exc:
            cut_error = str(exc)

        cut_json = debug_dir / f"{chunk_pdf_path.stem}_cutline.json"
        if cut_payload is None and cut_json.exists():
            try:
                cut_payload = json.loads(cut_json.read_text(encoding='utf-8'))
            except Exception:
                pass

        weak = False
        failed = False
        reason = cut_error or ""
        y_line = None
        debug_png = ""
        top_png = ""
        bot_png = ""
        best_mode = "none"
        if isinstance(cut_payload, dict):
            weak = bool(cut_payload.get('soft_fail') or cut_payload.get('weak_cut'))
            failed = bool(cut_payload.get('failed'))
            reason = _clean(cut_payload.get('soft_fail_reason') or cut_payload.get('fail_reason') or cut_error)
            y_line = cut_payload.get('y_line') if isinstance(cut_payload.get('y_line'), int) else None
            best_mode = _clean(cut_payload.get('best_mode')) or _clean(cut_payload.get('mode')) or 'auto'
            base = debug_dir / chunk_pdf_path.stem
            debug_png = str((base.with_name(base.name + '_cutline.png')))
            top_png = str((base.with_name(base.name + '_cutline_top.png')))
            bot_png = str((base.with_name(base.name + '_cutline_bot.png')))

        outputs.append({
            'name': chunk_name,
            'kind': 'chunk',
            'start': start,
            'end': end,
            'heading': _clean(obj.get('heading')),
            'title': _clean(obj.get('title')),
            'number': obj.get('number') if isinstance(obj.get('number'), int) else _extract_heading_number(_clean(obj.get('heading'))),
            'content_head': bool(obj.get('content_head', False)),
            'file_path': str(chunk_pdf_path),
            'meta_path': str(chunk_meta_path),
            'lesson_name': lesson_name,
            'lesson_heading': lesson_heading,
            'lesson_title': lesson_title,
            'lesson_number': lesson_number,
            'topic_number': topic_number,
            'confidence': 'low' if (weak or failed or cut_error) else 'high',
            'confidence_score': 0.35 if (weak or failed or cut_error) else 0.92,
            'confidence_reason': reason,
            'cutline_json': str(cut_json) if cut_json.exists() else '',
            'debug_png': debug_png if debug_png and Path(debug_png).exists() else '',
            'top_png': top_png if top_png and Path(top_png).exists() else '',
            'bot_png': bot_png if bot_png and Path(bot_png).exists() else '',
            'y_line': y_line,
            'best_mode': best_mode,
        })
    return outputs


def _extract_chunks_for_lessons(
    lessons: List[Dict[str, Any]],
    *,
    temp_dir: str,
    progress_cb: ProgressCb = None,
    model: str = 'gemini-2.5-flash',
) -> List[Dict[str, Any]]:
    outputs: List[Dict[str, Any]] = []
    total = max(1, len(lessons))
    for idx, lesson in enumerate(lessons, start=1):
        lesson_pdf = _clean(lesson.get('file_path'))
        if not lesson_pdf:
            continue
        ratio = idx / total
        _emit(progress_cb, 'chunking', 'Đang tách chunk', 26 + (ratio * 54), f"Đang tách chunk cho {lesson.get('title') or lesson.get('heading') or lesson.get('name')}")
        try:
            chunks = _extract_chunks_for_lesson_pdf(
                lesson_pdf,
                lesson_name=_clean(lesson.get('name')) or f"lesson_{idx:02d}",
                lesson_heading=_clean(lesson.get('heading')),
                lesson_title=_clean(lesson.get('title')),
                lesson_number=lesson.get('number') if isinstance(lesson.get('number'), int) else _extract_heading_number(_clean(lesson.get('heading'))),
                topic_number=lesson.get('topic_number') if isinstance(lesson.get('topic_number'), int) else None,
                temp_dir=temp_dir,
                progress_cb=progress_cb,
                model=model,
            )
            outputs.extend(chunks)
        except Exception as exc:
            outputs.append({
                'name': f"chunk_error_{idx:02d}",
                'kind': 'chunk',
                'start': 1,
                'end': 1,
                'heading': '',
                'title': f"Lỗi tách chunk: {lesson.get('title') or lesson.get('name')}",
                'number': idx,
                'content_head': False,
                'file_path': '',
                'meta_path': '',
                'lesson_name': _clean(lesson.get('name')),
                'lesson_heading': _clean(lesson.get('heading')),
                'lesson_title': _clean(lesson.get('title')),
                'lesson_number': lesson.get('number'),
                'topic_number': lesson.get('topic_number'),
                'confidence': 'low',
                'confidence_score': 0.1,
                'confidence_reason': str(exc),
                'cutline_json': '',
                'debug_png': '',
                'top_png': '',
                'bot_png': '',
                'y_line': None,
                'best_mode': 'error',
            })
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

    chunks = _extract_chunks_for_lessons(lessons, temp_dir=temp_dir, progress_cb=progress_cb, model=model)
    return {
        "mode": "subject",
        "temp_dir": temp_dir,
        "manifest": manifest,
        "topics": topics,
        "lessons": lessons,
        "chunks": chunks,
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
    chunks = _extract_chunks_for_lessons(lessons, temp_dir=temp_dir, progress_cb=progress_cb, model=model)
    return {
        "mode": "topic",
        "temp_dir": temp_dir,
        "manifest": manifest,
        "topic": topic_item,
        "lessons": lessons,
        "chunks": chunks,
        "total_pages": total_pages,
    }


def extract_and_split_structure(pdf_path: str, *, mode: str, progress_cb: ProgressCb = None, model: str = "gemini-2.5-flash") -> Dict[str, Any]:
    current_mode = _clean(mode).lower()
    if current_mode == "subject":
        return _extract_structure_subject(pdf_path, model=model, progress_cb=progress_cb)
    if current_mode == "topic":
        return _extract_structure_topic(pdf_path, model=model, progress_cb=progress_cb)
    if current_mode == "lesson":
        temp_dir = tempfile.mkdtemp(prefix="auto_split_lesson_")
        lesson_name = Path(pdf_path).stem
        chunks = _extract_chunks_for_lesson_pdf(
            pdf_path,
            lesson_name=lesson_name,
            lesson_heading="",
            lesson_title=lesson_name,
            lesson_number=None,
            topic_number=None,
            temp_dir=temp_dir,
            progress_cb=progress_cb,
            model=model,
        )
        return {
            "mode": "lesson",
            "temp_dir": temp_dir,
            "manifest": {},
            "lessons": [{"name": lesson_name, "heading": "", "title": lesson_name, "file_path": pdf_path, "start": 1, "end": len(PdfReader(pdf_path).pages)}],
            "chunks": chunks,
            "total_pages": len(PdfReader(pdf_path).pages),
        }
    raise ValueError("mode phải là 'subject' hoặc 'topic' hoặc 'lesson'")


def cleanup_split_result(result: Dict[str, Any] | None) -> None:
    temp_dir = _clean((result or {}).get("temp_dir"))
    if not temp_dir:
        return
    try:
        shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception:
        pass
