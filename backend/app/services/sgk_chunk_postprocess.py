# sgk_extract/chunk_postprocess.py
import os
os.environ["DISABLE_MODEL_SOURCE_CHECK"] = "True"
os.environ["OMP_NUM_THREADS"] = "2"
os.environ["VECLIB_MAXIMUM_THREADS"] = "2"

import re
import json
import unicodedata
import tempfile
import inspect
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import cv2
import numpy as np
try:
    from paddleocr import PaddleOCR  # type: ignore
except Exception:
    PaddleOCR = None  # type: ignore

try:
    import pytesseract  # type: ignore
except Exception:
    pytesseract = None  # type: ignore


# ============================
# CONFIG (chỉ sửa khu này)
# ============================

LANG          = "vi"
DPI           = 260
OFFSET        = 10
MIN_SCORE     = 0.0
DET_NO_RESIZE = True

EXTRACT_KEY = "extract"
FORCE_REPROCESS = False
AUTO_MARK_IF_OUTPUT_EXISTS = True

MAKE_PDF_BACKUP = False  # bạn không muốn .bak
EXTRACT_HEADING_KEY = "extract_heading"   # ✅ key mới để skip
FORCE_HEADING_NUMS = {1}                  # ✅ content_head=False nhưng heading_num=1 vẫn xử lý
BOT_ONLY_HEADING_NUMS = {1}               # ✅ heading_num=1 chỉ update page[0] bằng bot
MIN_MATCH_REQUIRED = 3  # hoặc 4 tuỳ bạn muốn chặt cỡ nào
PDF_UPDATE_DISABLED = False

# --- SOFT CUT ---
ALLOW_WEAK_CUT = True
WEAK_MIN_LCS   = 2
WEAK_COV_EXP   = 0.65   # cover expected >= 65%
WEAK_COV_OBS   = 0.80   # obs “khá sạch”
WEAK_MIN_OBS   = 3      # obs quá ngắn thì không tin
WEAK_ALLOWED_MODES = {"prefix_line", "heading_left_title", "same_line", "merge_next"}

# --- FORCE CUT WHEN HEADING EVIDENCE STRONG ---
FORCE_CUT_ON_MODES = {"prefix_line"}   # bạn có thể thêm "heading_left_title" nếu muốn
# ============================
# JSON helpers
# ============================
def log_skip(jp: Path, reason: str) -> None:
    print(f"[SKIP] {jp.name} | reason={reason}")

def _score(m: int, has_heading: bool, has_dot: bool) -> int:
    # heading_bonus=2, dot_bonus=1 (dot chỉ bonus, không bắt buộc)
    return m * 10 + (2 if has_heading else 0) + (1 if has_dot else 0)

def _is_pure_heading_token(text: str, heading_num: int) -> Tuple[bool, bool]:
    """
    True nếu text chỉ là "1" hoặc "1." (có thể có spaces).
    Return: (ok, has_dot)
    """
    t = (text or "").strip()

    # loại "1)" kiểu câu hỏi
    if re.match(rf"^\s*{heading_num}\s*\)\s*$", t):
        return False, False

    m = re.match(rf"^\s*{heading_num}\s*(\.)?\s*$", t)
    if not m:
        return False, False
    return True, bool(m.group(1))

def _v_overlap_ratio(a_y0: float, a_y1: float, b_y0: float, b_y1: float) -> float:
    inter = max(0.0, min(a_y1, b_y1) - max(a_y0, b_y0))
    denom = max(1.0, min(a_y1 - a_y0, b_y1 - b_y0))
    return inter / denom

def collect_heading_candidates(dets: List[Dict[str, Any]], heading_num: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for d in dets:
        ok, has_dot = _is_pure_heading_token(d.get("text", ""), heading_num)
        if ok:
            dd = dict(d)
            dd["has_dot"] = has_dot
            out.append(dd)
    return out

def find_heading_left_for_line(
    heading_cands: List[Dict[str, Any]],
    ln: Dict[str, Any],
    *,
    x_gap_max: float = 220.0,   # tuỳ trang, có thể tăng lên 300 nếu số "1." cách xa title
    min_v_overlap: float = 0.25 # overlap theo trục y
) -> Optional[Dict[str, Any]]:
    """
    Tìm heading (1/1.) nằm bên trái và gần line title.
    """
    best = None  # (key, cand)
    for h in heading_cands:
        # phải nằm bên trái line
        if float(h["x1"]) > float(ln["x0"]) + 20:
            continue

        gap = float(ln["x0"]) - float(h["x1"])
        if gap < 0 or gap > x_gap_max:
            continue

        ov = _v_overlap_ratio(float(h["y0"]), float(h["y1"]), float(ln["y0"]), float(ln["y1"]))
        if ov < min_v_overlap:
            continue

        # ưu tiên gap nhỏ, overlap lớn
        key = (gap, -ov)
        if best is None or key < best[0]:
            best = (key, h)

    return best[1] if best else None


def _has_dot_heading(text: str, heading_num: int) -> bool:
    t = (text or "").strip()
    return bool(re.search(rf"^\s*{heading_num}\s*\.", t))

def build_seq_from_line_items(items: List[Dict[str, Any]], heading_num: int) -> Tuple[Optional[List[str]], Optional[Dict[str, float]], bool]:
    """
    Trả về:
      - seq: ['1','T','T','V','D','L', ...] (seq[0] luôn là heading_num)
      - hbbox: bbox của item chứa heading (để merge với line kế nếu cần)
      - has_dot: item heading có dạng '1.' không (bonus nhỏ)
    """
    hn = str(heading_num)
    started = False
    seq: List[str] = []
    hbbox: Optional[Dict[str, float]] = None
    has_dot = False

    # items đã sort theo x0 ở group_to_lines, nhưng cứ sort lại cho chắc
    items_sorted = sorted(items, key=lambda d: d["x0"])

    for it in items_sorted:
        toks = tokenize_words(it.get("text", ""))
        if not toks:
            continue

        if not started:
            # tìm token số = heading_num trong item này
            for k, tok in enumerate(toks):
                if tok.isdigit() and tok == hn:
                    started = True
                    seq.append(hn)
                    hbbox = {"x0": float(it["x0"]), "y0": float(it["y0"]), "x1": float(it["x1"]), "y1": float(it["y1"])}
                    has_dot = _has_dot_heading(it.get("text", ""), heading_num)

                    # lấy initials của các token sau heading trong cùng item (nếu có)
                    for tok2 in toks[k + 1:]:
                        if tok2.isdigit():
                            continue
                        base = remove_diacritics_char_no_case_change(tok2[0])
                        if base:
                            seq.append(base)
                    break
            # nếu chưa start thì bỏ qua item này
            continue

        # đã started: lấy initials từ mọi token chữ
        for tok in toks:
            if tok.isdigit():
                continue
            base = remove_diacritics_char_no_case_change(tok[0])
            if base:
                seq.append(base)

    if not started:
        return None, None, False
    return seq, hbbox, has_dot


def try_merge_title_from_next_lines(
    lines: List[Dict[str, Any]],
    idx: int,
    hbbox: Dict[str, float],
    expected_letters: List[str],
    look_ahead: int = 3,
) -> Tuple[int, List[str]]:
    best_m = 0
    best_obs: List[str] = []

    hx1 = hbbox["x1"]
    hmid = 0.5 * (hbbox["y0"] + hbbox["y1"])
    h_h = max(1.0, hbbox["y1"] - hbbox["y0"])

    # ✅ start từ idx+1 (line kế)
    for j in range(idx + 1, min(len(lines), idx + look_ahead + 1)):
        ln2 = lines[j]

        if float(ln2["x0"]) < hx1 - 30:
            continue

        mid2 = 0.5 * (float(ln2["y0"]) + float(ln2["y1"]))
        if abs(mid2 - hmid) > max(60.0, h_h * 2.5):
            continue

        obs2 = extract_initials_no_case_change(ln2["text"])
        m2 = robust_match_count(obs2, expected_letters)
        if m2 > best_m:
            best_m = m2
            best_obs = obs2
        if best_m >= len(expected_letters):
            break

    return best_m, best_obs


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))

def write_json_atomic(path: Path, data: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(str(tmp), str(path))

def mark_chunk_processed(
    chunk_json_path: Path,
    meta: Dict[str, Any],
    *,
    mark_extract: bool,
    mark_extract_heading: bool,
) -> None:
    # chỉ set extract_heading khi bạn muốn nó là "đã xử lý heading-force"
    if mark_extract_heading:
        meta[EXTRACT_HEADING_KEY] = True
    else:
        # không muốn key này tồn tại cho content_head
        meta.pop(EXTRACT_HEADING_KEY, None)

    # extract cũ chỉ dùng cho content_head=True
    if mark_extract:
        meta[EXTRACT_KEY] = True

    write_json_atomic(chunk_json_path, meta)


# ============================
# Image I/O unicode-safe
# ============================
def imwrite_unicode(path: Path, img: np.ndarray) -> None:
    if img is None or img.size == 0 or img.shape[0] == 0 or img.shape[1] == 0:
        raise RuntimeError(f"Empty image, cannot write: {path} | shape={None if img is None else img.shape}")

    path = Path(path)
    ext = path.suffix.lower() if path.suffix else ".png"
    if ext not in [".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"]:
        ext = ".png"
        path = path.with_suffix(ext)

    ok, buf = cv2.imencode(ext, img)
    if not ok:
        raise RuntimeError(f"cv2.imencode failed: {path} | shape={img.shape}")
    buf.tofile(str(path))

def imread_unicode(path: Path) -> np.ndarray:
    data = np.fromfile(str(path), dtype=np.uint8)
    img = cv2.imdecode(data, cv2.IMREAD_COLOR)
    if img is None:
        raise FileNotFoundError(f"Cannot read image: {path}")
    return img


# ============================
# PDF -> image (page 0) (PyMuPDF only, gọn)
# ============================
def render_pdf_page0_to_bgr(pdf_path: Path, dpi: int) -> np.ndarray:
    # ưu tiên pypdfium2 (Kaggle-safe), fallback fitz nếu có
    try:
        import pypdfium2 as pdfium
        pdf = pdfium.PdfDocument(str(pdf_path))
        page = pdf.get_page(0)
        scale = float(dpi) / 72.0
        bitmap = page.render(scale=scale)
        pil_img = bitmap.to_pil()  # RGB
        rgb = np.array(pil_img, dtype=np.uint8)
        bgr = cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR)
        page.close()
        pdf.close()
        return bgr
    except Exception:
        pass

    # fallback PyMuPDF (local nếu bạn muốn)
    import fitz
    doc = fitz.open(str(pdf_path))
    page = doc.load_page(0)
    zoom = float(dpi) / 72.0
    pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
    img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    doc.close()
    return cv2.cvtColor(img, cv2.COLOR_RGB2BGR)

# ============================
# OCR helpers
# ============================

def _call_with_supported_kwargs(func, *args, **kwargs):
    """Chỉ truyền những kwargs mà hàm hiện tại thực sự hỗ trợ."""
    try:
        sig = inspect.signature(func)
        params = sig.parameters
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            filtered = kwargs
        else:
            filtered = {k: v for k, v in kwargs.items() if k in params}
    except Exception:
        filtered = kwargs
    return func(*args, **filtered)


class _TesseractOCR:
    def __init__(self, lang: str = "vie+eng"):
        self.lang = lang


def _tesseract_lang_candidates() -> List[str]:
    if pytesseract is None:
        return []
    try:
        available = set(pytesseract.get_languages(config=""))
    except Exception:
        available = set()
    preferred = ["vie+eng", "vie", "eng"]
    out: List[str] = []
    for cand in preferred:
        parts = cand.split("+")
        if not available or all(part in available for part in parts):
            out.append(cand)
    return out or ["eng"]


def _run_tesseract_ocr(img_bgr: np.ndarray, lang: str) -> List[Dict[str, Any]]:
    if pytesseract is None:
        raise RuntimeError("pytesseract_not_installed")
    rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
    data = pytesseract.image_to_data(
        rgb,
        lang=lang,
        config="--psm 6",
        output_type=pytesseract.Output.DICT,
    )
    out: List[Dict[str, Any]] = []
    texts = data.get("text", [])
    for i in range(len(texts)):
        txt = str(texts[i] or "").strip()
        if not txt:
            continue
        conf_raw = str((data.get("conf") or ["-1"])[i]).strip()
        try:
            score = float(conf_raw) / 100.0
        except Exception:
            score = 0.0
        x = int((data.get("left") or [0])[i])
        y = int((data.get("top") or [0])[i])
        w = int((data.get("width") or [0])[i])
        h = int((data.get("height") or [0])[i])
        if w <= 0 or h <= 0:
            continue
        out.append({
            "x0": float(x),
            "y0": float(y),
            "x1": float(x + w),
            "y1": float(y + h),
            "text": txt,
            "score": score,
        })
    return out


def run_ocr_any(ocr: Any, img_bgr: np.ndarray):
    """
    Tương thích nhiều đời PaddleOCR.
    Nếu không có PaddleOCR thì fallback sang Tesseract để tránh toàn bộ bước crop bị bỏ qua.
    """
    if isinstance(ocr, _TesseractOCR):
        last_t_err = None
        for lang in _tesseract_lang_candidates():
            try:
                return _run_tesseract_ocr(img_bgr, lang)
            except Exception as e:
                last_t_err = e
        raise RuntimeError(f"tesseract_ocr_failed: {last_t_err}")

    last_err = None

    ocr_method = getattr(ocr, "ocr", None)
    if callable(ocr_method):
        for kwargs in ({"cls": False}, {}):
            try:
                return _call_with_supported_kwargs(ocr_method, img_bgr, **kwargs)
            except TypeError as e:
                last_err = e
            except Exception as e:
                last_err = e

    predict_method = getattr(ocr, "predict", None)
    if callable(predict_method):
        variants = [
            {
                "use_textline_orientation": False,
                "use_doc_orientation_classify": False,
                "use_doc_unwarping": False,
            },
            {
                "use_textline_orientation": False,
                "use_doc_orientation_classify": False,
            },
            {
                "use_textline_orientation": False,
            },
            {},
        ]
        for kwargs in variants:
            try:
                return _call_with_supported_kwargs(predict_method, img_bgr, **kwargs)
            except TypeError as e:
                last_err = e
            except Exception as e:
                last_err = e

    if callable(ocr):
        try:
            return ocr(img_bgr)
        except Exception as e:
            last_err = e

    if last_err is not None:
        raise last_err
    raise RuntimeError("Không tìm thấy API OCR phù hợp cho PaddleOCR hiện tại")

def iter_dets_paddleocr(res: Any) -> List[Dict[str, Any]]:
    # Parse output từ ocr.ocr(img, cls=False): [ [ [poly, (text, score)], ... ] ]
    if res is None:
        return []
    if not isinstance(res, list):
        res = [res]
    out: List[Dict[str, Any]] = []
    for page in res:
        if page is None or not isinstance(page, list):
            continue
        for det in page:
            if not (isinstance(det, (list, tuple)) and len(det) >= 2):
                continue
            poly = det[0]
            ts = det[1]
            if not (isinstance(ts, (list, tuple)) and len(ts) >= 2):
                continue
            text = (ts[0] or "").strip()
            score = float(ts[1]) if ts[1] is not None else 0.0
            if not text:
                continue
            x0, y0, x1, y1 = poly_bbox(poly)
            out.append({"x0": x0, "y0": y0, "x1": x1, "y1": y1, "text": text, "score": score})
    return out

def poly_bbox(poly: Any) -> Tuple[float, float, float, float]:
    pts = np.array(poly, dtype=np.float32).reshape(-1, 2)
    x0, y0 = float(np.min(pts[:, 0])), float(np.min(pts[:, 1]))
    x1, y1 = float(np.max(pts[:, 0])), float(np.max(pts[:, 1]))
    return x0, y0, x1, y1

def _merge_res_dict(obj: Any) -> Optional[Dict[str, Any]]:
    if obj is None:
        return None
    if isinstance(obj, dict):
        d = dict(obj)
    elif hasattr(obj, "to_dict"):
        try:
            d = obj.to_dict()
            if not isinstance(d, dict):
                return None
        except Exception:
            return None
    elif hasattr(obj, "res"):
        d = obj.res
        if not isinstance(d, dict):
            return None
    else:
        return None

    # paddlex/paddleocr sometimes nests "res"
    cur = d
    for _ in range(3):
        inner = cur.get("res")
        if isinstance(inner, dict):
            for k, v in inner.items():
                d.setdefault(k, v)
            cur = inner
        else:
            break
    return d

def _get_any(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def iter_dets_predict(res: Any) -> List[Dict[str, Any]]:
    if res is None:
        return []
    if not isinstance(res, list):
        res = [res]

    out: List[Dict[str, Any]] = []
    for page in res:
        d = _merge_res_dict(page)
        if not isinstance(d, dict):
            continue

        rec_polys = _get_any(d, ["rec_polys", "rec_boxes", "rec_points"])
        dt_polys  = _get_any(d, ["dt_polys", "dt_boxes", "det_polys"])
        texts     = _get_any(d, ["rec_texts", "rec_text", "texts"])
        scores    = _get_any(d, ["rec_scores", "rec_score", "scores"])

        if texts is None or scores is None:
            continue

        polys = None
        if rec_polys is not None and len(rec_polys) == len(texts):
            polys = rec_polys
        elif dt_polys is not None and len(dt_polys) == len(texts):
            polys = dt_polys
        else:
            continue

        n = min(len(polys), len(texts), len(scores))
        for poly, text, score in zip(polys[:n], texts[:n], scores[:n]):
            text = (text or "").strip()
            if not text:
                continue
            x0, y0, x1, y1 = poly_bbox(poly)
            out.append({"x0": x0, "y0": y0, "x1": x1, "y1": y1, "text": text, "score": float(score)})
    return out

def group_to_lines(dets: List[Dict[str, Any]], y_tol: float) -> List[Dict[str, Any]]:
    dets = sorted(dets, key=lambda d: (((d["y0"] + d["y1"]) * 0.5), d["x0"]))
    groups: List[Dict[str, Any]] = []

    for d in dets:
        yc = 0.5 * (d["y0"] + d["y1"])
        for g in groups:
            if abs(yc - g["y_ref"]) <= y_tol:
                g["items"].append(d)
                g["y_ref"] = (g["y_ref"] * (len(g["items"]) - 1) + yc) / len(g["items"])
                break
        else:
            groups.append({"y_ref": yc, "items": [d]})

    lines: List[Dict[str, Any]] = []
    for g in sorted(groups, key=lambda x: x["y_ref"]):
        items = sorted(g["items"], key=lambda d: d["x0"])
        x0 = min(it["x0"] for it in items)
        x1 = max(it["x1"] for it in items)
        y0 = min(it["y0"] for it in items)
        y1 = max(it["y1"] for it in items)
        text = " ".join(it["text"] for it in items)
        lines.append({"items": items, "text": text, "x0": x0, "x1": x1, "y0": y0, "y1": y1})
    return lines


# ============================
# Matching rules (giữ nguyên logic)
# ============================
def extract_heading_num(heading: str) -> Optional[int]:
    m = re.search(r"(\d+)", heading or "")
    return int(m.group(1)) if m else None

def remove_diacritics_char_no_case_change(ch: str) -> Optional[str]:
    if not ch:
        return None
    if ch == "Đ":
        return "D"
    if ch == "đ":
        return None
    if (not ch.isalpha()) or (not ch.isupper()):
        return None
    base = unicodedata.normalize("NFD", ch)
    base = "".join(c for c in base if unicodedata.category(c) != "Mn")
    if len(base) != 1 or (not base.isalpha()) or (not base.isupper()):
        return None
    return base

def tokenize_words(text: str) -> List[str]:
    return re.findall(r"[0-9]+|[A-Za-zÀ-Ỵà-ỵĐđ]+", text or "")

def build_expected_letters_from_title(title: str) -> List[str]:
    out: List[str] = []
    for w in re.split(r"\s+", (title or "").strip()):
        if not w:
            continue
        ch0 = None
        for ch in w:
            if ch.isalpha():
                ch0 = ch
                break
        if not ch0:
            continue
        base = remove_diacritics_char_no_case_change(ch0)
        if base:
            out.append(base)
    return out

def split_heading_prefix(raw_text: str, heading_num: int, require_dot: bool = False) -> Tuple[bool, str]:
    t = (raw_text or "").strip()

    # loại "1)" kiểu câu hỏi trắc nghiệm
    if re.match(rf"^\s*{heading_num}\s*\)", t):
        return False, ""

    if require_dot:
        # bắt buộc có dấu chấm sau số: "1. ...."
        m = re.match(rf"^\s*{heading_num}\s*\.\s*(\S.+)$", t)
        if m:
            rem = m.group(1).strip()
            if rem and not rem[0].isdigit():
                return True, rem
        return False, ""

    # không bắt buộc dot (giữ tương thích)
    m = re.match(rf"^\s*{heading_num}\s*\.?\s*(\S.+)$", t)
    if m:
        rem = m.group(1).strip()
        if rem and not rem[0].isdigit():
            return True, rem

    return False, ""


def extract_initials_no_case_change(text: str) -> List[str]:
    initials: List[str] = []
    for tok in tokenize_words(text):
        if tok.isdigit():
            continue
        base = remove_diacritics_char_no_case_change(tok[0])
        if base:
            initials.append(base)
    return initials

def prefix_match_count(observed: List[str], expected: List[str]) -> int:
    
    n = min(len(observed), len(expected))
    m = 0
    for i in range(n):
        if observed[i] == expected[i]:
            m += 1
        else:
            break
    return m

def robust_match_count(observed: List[str], expected: List[str]) -> int:
    """
    Robust match:
    - Prefix + skip chữ dư: xử lý case BTCL vs BTL (THỨC bị tách -> THỨ + C)
    - Với title dài (>=6): dùng LCS nếu đủ cao (>= 80%) để chịu BOTH thiếu + dư (case lesson 11)
    """

    # 1) prefix
    p = prefix_match_count(observed, expected)

    # 2) skip chữ dư (giữ như bạn đang dùng)
    anchor = 2 if len(expected) <= 4 else 3
    if p < min(anchor, len(expected)):
        r = p
    else:
        j = p
        for ch in observed[p:]:
            if j < len(expected) and ch == expected[j]:
                j += 1
        r = j

    # 3) LCS fallback cho title dài (>=6): chịu cả thiếu + dư
    if len(expected) >= 6:
        # ceil(0.8*n) không cần import math
        thresh = (8 * len(expected) + 9) // 10

        # yêu cầu chữ đầu xuất hiện rất sớm (tránh match nhầm dòng)
        begin_ok = (len(expected) == 0) or (expected[0] in observed[:3])

        if begin_ok:
            n, m = len(expected), len(observed)
            dp = [0] * (m + 1)
            for i in range(1, n + 1):
                prev = 0
                ei = expected[i - 1]
                for j2 in range(1, m + 1):
                    cur = dp[j2]
                    if ei == observed[j2 - 1]:
                        dp[j2] = prev + 1
                    else:
                        dp[j2] = dp[j2] if dp[j2] >= dp[j2 - 1] else dp[j2 - 1]
                    prev = cur
            lcs = dp[m]
            if lcs >= thresh:
                return lcs

    return r

# ============================
# Debug draw + split
# ============================

def draw_debug(img: np.ndarray, line: Dict[str, Any], y_line: int, out_path: Path, label: str = "") -> None:
    out = img.copy()
    h, w = out.shape[:2]
    y = max(0, min(int(y_line), h - 1))

    cv2.line(out, (0, y), (w - 1, y), (0, 0, 255), 3)
    x0 = int(max(0, min(line["x0"], w - 1)))
    x1 = int(max(0, min(line["x1"], w - 1)))
    y0 = int(max(0, min(line["y0"], h - 1)))
    y1 = int(max(0, min(line["y1"], h - 1)))
    cv2.rectangle(out, (x0, y0), (x1, y1), (0, 255, 0), 2)

    if label:
        cv2.putText(out, label[:90], (20, max(30, y - 12)),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 0, 0), 3, cv2.LINE_AA)
        cv2.putText(out, label[:90], (20, max(30, y - 12)),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 255, 255), 1, cv2.LINE_AA)

    imwrite_unicode(out_path, out)
    print("Saved:", out_path)

def split_and_save_bot_only(img: np.ndarray, y_line: int, out_bot: Path) -> Dict[str, Any]:
    h, _ = img.shape[:2]
    y = int(round(y_line))
    y = max(0, min(y, h))

    info = {"y_split": y, "bot_saved": False, "bot_h": 0}

    if y == h:
        # bot rỗng => không lưu
        print("[WARN] y_line=h => BOT rỗng, không lưu.")
        return info

    bot = img[y:].copy()
    if bot.size == 0 or bot.shape[0] == 0 or bot.shape[1] == 0:
        print("[WARN] BOT rỗng, không lưu.")
        return info

    imwrite_unicode(out_bot, bot)
    info.update({"bot_saved": True, "bot_h": int(bot.shape[0])})
    return info

def update_pdf_page0_with_bot_only(
    cur_chunk_pdf: Path,
    bot_png: Path,
    make_backup: bool = False,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "cur_pdf_updated": False,
        "cur_pdf_path": str(cur_chunk_pdf.resolve()),
        "cur_first_page_index": 0,
        "mode": "bot_only_page0",
    }

    replace_page_with_png_inplace(cur_chunk_pdf, bot_png, 0, make_backup=make_backup)
    result["cur_pdf_updated"] = True
    return result


def split_and_save(img: np.ndarray, y_line: int, out_top: Path, out_bot: Path) -> Dict[str, Any]:
    h, _ = img.shape[:2]
    y = int(round(y_line))
    y = max(0, min(y, h))

    info = {"y_split": y, "top_saved": False, "bot_saved": False, "top_h": 0, "bot_h": 0}

    if y == 0:
        bot = img.copy()
        imwrite_unicode(out_bot, bot)
        info.update({"bot_saved": True, "bot_h": int(bot.shape[0])})
        print("[WARN] y_line=0 => TOP rỗng, chỉ lưu BOT.")
        return info

    if y == h:
        top = img.copy()
        imwrite_unicode(out_top, top)
        info.update({"top_saved": True, "top_h": int(top.shape[0])})
        print("[WARN] y_line=h => BOT rỗng, chỉ lưu TOP.")
        return info

    top = img[:y].copy()
    bot = img[y:].copy()
    imwrite_unicode(out_top, top)
    imwrite_unicode(out_bot, bot)

    info.update({"top_saved": True, "bot_saved": True, "top_h": int(top.shape[0]), "bot_h": int(bot.shape[0])})
    return info


# ============================
# PDF replace (inplace)
# ============================
def _img_wh(png_path: Path) -> Tuple[int, int]:
    img = imread_unicode(png_path)
    h, w = img.shape[:2]
    return w, h

def _rect_fit_on_page(page_rect, img_w: int, img_h: int, align: str = "top"):
    pw, ph = page_rect.width, page_rect.height
    s = min(pw / float(img_w), ph / float(img_h))
    w = img_w * s
    h = img_h * s

    x0 = (pw - w) / 2.0
    x1 = x0 + w

    if align == "top":
        y0, y1 = 0.0, h
    elif align == "bottom":
        y1, y0 = ph, ph - h
    elif align == "center":
        y0 = (ph - h) / 2.0
        y1 = y0 + h
    else:
        raise ValueError("align phải là 'top' | 'bottom' | 'center'")

    return type(page_rect)(x0, y0, x1, y1)

def replace_page_with_png_inplace(
    pdf_path: Path,
    png_path: Path,
    page_index_to_replace: int,
    align: str = "top",
    crop_to_image: bool = True,
    make_backup: bool = False,
) -> None:
    try:
        import fitz
    except Exception as e:
        raise RuntimeError("Thiếu PyMuPDF (fitz). Cài: pip install pymupdf") from e

    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")
    if not png_path.exists():
        raise FileNotFoundError(f"PNG not found: {png_path}")

    src = fitz.open(str(pdf_path))
    n = src.page_count
    if not (0 <= page_index_to_replace < n):
        src.close()
        raise ValueError(f"page_index_to_replace không hợp lệ: {page_index_to_replace} / n={n}")

    ref_rect = src[page_index_to_replace].rect
    img_w, img_h = _img_wh(png_path)

    out = fitz.open()
    for i in range(n):
        if i != page_index_to_replace:
            out.insert_pdf(src, from_page=i, to_page=i)
        else:
            p = out.new_page(width=ref_rect.width, height=ref_rect.height)
            img_rect = _rect_fit_on_page(p.rect, img_w, img_h, align=align)
            p.insert_image(img_rect, filename=str(png_path))
            if crop_to_image:
                p.set_cropbox(img_rect)

    if make_backup:
        bak = pdf_path.with_suffix(pdf_path.suffix + ".bak")
        if not bak.exists():
            bak.write_bytes(pdf_path.read_bytes())

    tmp_fd, tmp_name = tempfile.mkstemp(suffix=".pdf", dir=str(pdf_path.parent))
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)

    try:
        out.save(str(tmp_path), garbage=4, deflate=True)
        out.close()
        src.close()
        os.replace(str(tmp_path), str(pdf_path))
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass

def _prev_chunk_stem(cur_stem: str) -> Optional[str]:
    m = re.search(r"(.*_chunk_)(\d+)$", cur_stem)
    if not m:
        return None
    prefix, num_str = m.group(1), m.group(2)
    num = int(num_str)
    if num <= 1:
        return None
    return prefix + str(num - 1).zfill(len(num_str))

def update_pdfs_for_content_head(
    cur_chunk_pdf: Path,
    cur_chunk_stem: str,
    top_png: Path,
    bot_png: Path,
    chunk_pdf_dir: Path,
    make_backup: bool = False,
) -> Dict[str, Any]:
    import fitz

    result: Dict[str, Any] = {
        "prev_pdf_updated": False,
        "cur_pdf_updated": False,
        "prev_pdf_path": None,
        "cur_pdf_path": str(cur_chunk_pdf.resolve()),
        "prev_last_page_index": None,
        "cur_first_page_index": 0,
    }

    # current: page 0
    replace_page_with_png_inplace(cur_chunk_pdf, bot_png, 0, make_backup=make_backup)
    result["cur_pdf_updated"] = True

    # prev: last page (cấu trúc folder mới)
    prev_stem = _prev_chunk_stem(cur_chunk_stem)
    if prev_stem:
        cur_chunk_dir = cur_chunk_pdf.parent          # .../chunk_02/
        lesson_dir = cur_chunk_dir.parent             # .../<lesson_stem>/

        m = re.match(r"chunk_(\d+)", cur_chunk_dir.name)
        if m:
            cur_num = int(m.group(1))
            prev_num = cur_num - 1
            if prev_num >= 1:
                prev_dir = lesson_dir / f"chunk_{prev_num:02d}"
                prev_pdf = prev_dir / f"{prev_stem}.pdf"

                if prev_pdf.exists():
                    d = fitz.open(str(prev_pdf))
                    n = d.page_count
                    d.close()
                    if n > 0:
                        last_idx = n - 1
                        replace_page_with_png_inplace(prev_pdf, top_png, last_idx, make_backup=make_backup)
                        result["prev_pdf_updated"] = True
                        result["prev_pdf_path"] = str(prev_pdf.resolve())
                        result["prev_last_page_index"] = last_idx

    return result


# ============================
# Process one chunk
# ============================
def process_one_chunk(
    ocr: PaddleOCR,
    chunk_json_path: Path,
    chunk_pdf_path: Path,
    out_dir: Path,
) -> Optional[Dict[str, Any]]:
    meta = read_json(chunk_json_path)

    heading = str(meta.get("heading", "")).strip()
    title = str(meta.get("title", "")).strip()

    heading_num = extract_heading_num(heading)
    if heading_num is None:
        print("[SKIP] No heading_num:", chunk_json_path.name, "heading=", heading)
        return None

    is_content_head = bool(meta.get("content_head", False))
    is_force_heading = (heading_num in FORCE_HEADING_NUMS)

    # ✅ chỉ xử lý nếu content_head=True hoặc heading_num thuộc FORCE_HEADING_NUMS (vd: 1.)
    if (not is_content_head) and (not is_force_heading):
        return None

    expected_letters = build_expected_letters_from_title(title)
    if not expected_letters:
        print("[SKIP] No expected letters:", chunk_json_path.name, "title=", title)
        return None

    img = render_pdf_page0_to_bgr(chunk_pdf_path, dpi=DPI)

    res = run_ocr_any(ocr, img)
    # fallback Tesseract trả trực tiếp list dict bbox/text/score
    if isinstance(res, list) and res and isinstance(res[0], dict) and {"x0", "y0", "x1", "y1", "text"}.issubset(set(res[0].keys())):
        dets_raw = list(res)
    # nếu res là kiểu predict cũ thì dùng iter_dets_predict, còn ocr.ocr thì dùng iter_dets_paddleocr
    elif isinstance(res, list) and res and isinstance(res[0], list) and (len(res[0]) == 0 or isinstance(res[0][0], (list, tuple))):
        dets_raw = iter_dets_paddleocr(res)
    else:
        dets_raw = iter_dets_predict(res)

    dets = [d for d in dets_raw if d["score"] >= float(MIN_SCORE)]

    if not dets:
        print("[FAIL] NO DETS:", chunk_json_path.name)
        return None

    hs = [(d["y1"] - d["y0"]) for d in dets]
    med_h = float(np.median(hs)) if hs else 20.0
    y_tol = max(10.0, med_h * 0.6)
    lines = group_to_lines(dets, y_tol=y_tol)

    heading_cands = collect_heading_candidates(dets, heading_num)

    best = None  # (score, matched, ln, obs_letters, mode)
    best_mode = "none"

    for i, ln in enumerate(lines):
        items = ln["items"]
        if not items:
            continue

        # luôn tính title initials để dùng cho heading_left_title / scoring
        obs_title = extract_initials_no_case_change(ln["text"])
        matched_title = robust_match_count(obs_title, expected_letters)

        cand_list: List[Tuple[int, int, Dict[str, Any], List[str], str]] = []

        # ✅ prefix_line: line bắt đầu bằng "1." hoặc "1 " => có heading evidence mạnh
        has_pref, rem = split_heading_prefix(ln["text"], heading_num, require_dot=False)
        if has_pref:
            obs_pref = extract_initials_no_case_change(rem)
            matched_pref = robust_match_count(obs_pref, expected_letters)
            has_dot_pref = _has_dot_heading(ln["text"], heading_num)
            sc_pref = _score(matched_pref, True, has_dot_pref)
            cand_list.append((sc_pref, matched_pref, ln, obs_pref, "prefix_line"))

        # ✅ CHỈ cho phép title_only khi KHÔNG phải content_head
        # (tức là trường hợp FORCE_HEADING_NUMS như heading_num=1 bạn muốn “phao cuối”)
        # ✅ title_only chỉ là "phao" cho trường hợp KHÔNG force_heading
        if (not is_content_head) and (not is_force_heading):
            sc_title = _score(matched_title, False, False)
            cand_list.append((sc_title, matched_title, ln, obs_title, "title_only"))

        # 2) heading_left_title: heading bị OCR tách rời (1/1.) nằm bên trái title
        h_left = find_heading_left_for_line(heading_cands, ln)
        if h_left is not None:
            sc_hleft = _score(matched_title, True, bool(h_left.get("has_dot", False)))
            cand_list.append((sc_hleft, matched_title, ln, obs_title, "heading_left_title"))

        # 3) same_line / merge_next: heading nằm trong cùng line-group (đúng theo ý bạn)
        seq, hbbox, has_dot = build_seq_from_line_items(items, heading_num)
        if seq is not None and hbbox is not None:
            obs_same = seq[1:]
            matched_same = robust_match_count(obs_same, expected_letters)
            sc_same = _score(matched_same, True, has_dot)
            cand_list.append((sc_same, matched_same, ln, obs_same, "same_line"))

            # merge_next: title nằm line kế cận
            if matched_same < len(expected_letters):
                m2, obs2 = try_merge_title_from_next_lines(lines, i, hbbox, expected_letters, look_ahead=3)

                # cho phép “ghép” phần chữ sau heading trong cùng item + title ở line kế
                matched_merge_comb = robust_match_count(obs_same + obs2, expected_letters) if obs2 else m2

                if matched_merge_comb >= m2:
                    matched_merge = matched_merge_comb
                    obs_merge = obs_same + obs2
                else:
                    matched_merge = m2
                    obs_merge = obs2

                sc_merge = _score(matched_merge, True, has_dot)
                cand_list.append((sc_merge, matched_merge, ln, obs_merge, "merge_next"))

        # nếu line này không có candidate nào (ví dụ content_head mà không thấy heading evidence)
        if not cand_list:
            continue

        # pick best candidate của line này
        cand_list.sort(key=lambda x: x[0], reverse=True)
        sc, matched, ln_best, obs_best, mode_best = cand_list[0]

        if best is None or sc > best[0]:
            best = (sc, matched, ln_best, obs_best, mode_best)
            best_mode = mode_best

        # stop sớm nếu match full (dù mode nào)
        if matched >= len(expected_letters):
            break

    if best is None:
        print("[FAIL] No line matched:", chunk_json_path.name)
        return None

    score, matched, ln, obs, best_mode = best

    # ✅ content_head bắt buộc phải có heading evidence
    if is_content_head and best_mode not in {"same_line", "merge_next", "heading_left_title", "prefix_line"}:
        print(f"[FAIL] content_head nhưng không có heading evidence => skip cut: {chunk_json_path.name}")
        return None

    nexp = len(expected_letters)
    # min_req "cứng" để gọi là chắc
    if nexp <= 2:
        min_req = 1
    elif nexp == 3:
        min_req = 2
    else:
        min_req = min(MIN_MATCH_REQUIRED, nexp)

    weak_cut = False
    weak_reason = None

    if matched < min_req:
        # ---- tính LCS để xem có phải OCR rụng chữ nhưng vẫn đúng line không ----
        def _lcs_len(a, b):
            n, m = len(a), len(b)
            dp = [0] * (m + 1)
            for i in range(1, n + 1):
                prev = 0
                ai = a[i - 1]
                for j in range(1, m + 1):
                    cur = dp[j]
                    if ai == b[j - 1]:
                        dp[j] = prev + 1
                    else:
                        dp[j] = dp[j] if dp[j] >= dp[j - 1] else dp[j - 1]
                    prev = cur
            return dp[m]

        lcs = _lcs_len(expected_letters, obs)
        cov_obs = lcs / max(1, len(obs))
        cov_exp = lcs / max(1, len(expected_letters))

        begin_ok = (nexp == 0) or (expected_letters[0] in obs[:3])
        weak_min_lcs = 1 if nexp <= 2 else WEAK_MIN_LCS

        allow_weak = (
            ALLOW_WEAK_CUT
            and (best_mode in WEAK_ALLOWED_MODES)
            and begin_ok
            and (lcs >= weak_min_lcs)
            and (cov_exp >= WEAK_COV_EXP)
            and (cov_obs >= WEAK_COV_OBS)
            and (len(obs) >= WEAK_MIN_OBS or nexp <= 3)
        )

        force_cut = (best_mode in FORCE_CUT_ON_MODES)

        if (not allow_weak) and (not force_cut):
            # HARD FAIL như cũ
            y_line = int(round(ln["y0"] - OFFSET))
            y_line = max(0, min(y_line, img.shape[0] - 1))

            stem = chunk_json_path.stem
            out_dir.mkdir(parents=True, exist_ok=True)

            out_debug_png = out_dir / f"{stem}_cutline.png"
            out_cut_json  = out_dir / f"{stem}_cutline.json"

            label = f"{heading} | {best_mode} | match {matched}/{len(expected_letters)} | obs={''.join(obs[:12])}"
            draw_debug(img, ln, y_line, out_debug_png, label=label)

            payload = {
                "failed": True,
                "fail_reason": f"low_match_{matched}_{len(expected_letters)}",
                "chunk_json": str(chunk_json_path.resolve()),
                "chunk_pdf": str(chunk_pdf_path.resolve()),
                "heading": heading,
                "heading_num": int(heading_num),
                "title": title,
                "expected_letters": expected_letters,
                "matched_prefix": int(matched),
                "observed_initials": obs,
                "best_mode": best_mode,
                "line_bbox": {"x0": ln["x0"], "y0": ln["y0"], "x1": ln["x1"], "y1": ln["y1"]},
                "y_line": int(y_line),
                "dpi": int(DPI),
                "offset_px": int(OFFSET),
                "image_size": {"w": int(img.shape[1]), "h": int(img.shape[0])},
                "debug_png": str(out_debug_png),
                "lcs": int(lcs),
                "cov_obs": float(cov_obs),
                "cov_exp": float(cov_exp),
            }
            write_json_atomic(out_cut_json, payload)

            print(f"[FAIL] Low match {matched}/{len(expected_letters)} => saved debug:", out_debug_png)
            return None

        # allow_weak => weak_cut theo tiêu chí LCS/coverage
        if allow_weak:
            weak_cut = True
            weak_reason = f"weak_cut_low_match_{matched}_{nexp}_lcs_{lcs}_covExp_{cov_exp:.2f}_covObs_{cov_obs:.2f}"
            print("[WARN]", weak_reason, "=> still cutting:", chunk_json_path.name)

        # force_cut => match thấp nhưng vẫn tin vì mode mạnh (prefix_line)
        elif force_cut:
            weak_cut = True
            weak_reason = f"force_cut_mode_{best_mode}_low_match_{matched}_{nexp}"
            print("[WARN]", weak_reason, "=> still cutting + updating:", chunk_json_path.name)

    y_line = int(round(ln["y0"] - OFFSET))
    y_line = max(0, min(y_line, img.shape[0] - 1))

    stem = chunk_json_path.stem
    out_dir.mkdir(parents=True, exist_ok=True)

    out_debug_png = out_dir / f"{stem}_cutline.png"
    out_cut_json  = out_dir / f"{stem}_cutline.json"
    out_top_png   = out_dir / f"{stem}_cutline_top.png"
    out_bot_png   = out_dir / f"{stem}_cutline_bot.png"

    label = f"{heading} | {best_mode} | match {matched}/{len(expected_letters)} | obs={''.join(obs[:12])}"

    draw_debug(img, ln, y_line, out_debug_png, label=label)
    pdf_update_allowed = (not PDF_UPDATE_DISABLED)

    # ✅ nếu content_head=True => giữ y nguyên (top+bot + update prev/current)
    if is_content_head:
        split_info = split_and_save(img, y_line, out_top_png, out_bot_png)

        pdf_update: Dict[str, Any] = {"skipped": True, "reason": "disabled or not available", "split_info": split_info}

        if pdf_update_allowed and split_info.get("top_saved") and split_info.get("bot_saved"):
            pdf_update = update_pdfs_for_content_head(
                cur_chunk_pdf=chunk_pdf_path,
                cur_chunk_stem=stem,
                top_png=out_top_png,
                bot_png=out_bot_png,
                chunk_pdf_dir=chunk_pdf_path.parent,
                make_backup=MAKE_PDF_BACKUP,
            )
        else:
            reason = "DISABLE_PDF_UPDATE=1" if PDF_UPDATE_DISABLED else "split_missing"
            pdf_update = {"skipped": True, "reason": reason, "split_info": split_info}

    # ✅ nếu content_head=False nhưng heading_num=1 => bot-only + replace page[0] của chính pdf
    else:
        # chỉ lưu bot
        split_info = split_and_save_bot_only(img, y_line, out_bot_png)

        pdf_update: Dict[str, Any] = {"skipped": True, "reason": "disabled or not available", "split_info": split_info}

        if pdf_update_allowed and split_info.get("bot_saved"):
            pdf_update = update_pdf_page0_with_bot_only(
                cur_chunk_pdf=chunk_pdf_path,
                bot_png=out_bot_png,
                make_backup=MAKE_PDF_BACKUP,
            )
        else:
            reason = "weak_cut" if weak_cut else "DISABLE_PDF_UPDATE=1"
            pdf_update = {"skipped": True, "reason": reason, "split_info": split_info}

    prefix_hits = prefix_match_count(obs, expected_letters)
    payload = {
        "chunk_json": str(chunk_json_path.resolve()),
        "chunk_pdf": str(chunk_pdf_path.resolve()),
        "heading": heading,
        "heading_num": int(heading_num),
        "title": title,
        "expected_letters": expected_letters,
        "matched_prefix": int(matched),
        "observed_initials": obs,
        "line_bbox": {"x0": ln["x0"], "y0": ln["y0"], "x1": ln["x1"], "y1": ln["y1"]},
        "y_line": int(y_line),
        "dpi": int(DPI),
        "offset_px": int(OFFSET),
        "image_size": {"w": int(img.shape[1]), "h": int(img.shape[0])},
        "split_info": split_info,
        "pdf_update": pdf_update,
        "mode": "content_head" if is_content_head else "heading_bot_only",
        "best_mode": best_mode,  # ✅ mode thật sự: title_only / heading_left_title / same_line / merge_next / prefix_line
        "run_mode": "content_head" if is_content_head else "heading_bot_only",  # ✅ giữ cái mode tổng quát cũ nếu bạn muốn
        "expected_initials": expected_letters,
        "prefix_hits": int(prefix_hits),
        "weak_cut": bool(weak_cut),
        "weak_reason": weak_reason,
        # failed chỉ dành cho hard-fail (return None), nên ở đây để False
        "failed": False,
        "fail_reason": None,
        "soft_fail": bool(weak_cut),
        "soft_fail_reason": weak_reason,
        "force_cut": bool(best_mode in FORCE_CUT_ON_MODES),
    }
    write_json_atomic(out_cut_json, payload)
    return payload


# ============================
# Main
# ============================
def build_ocr() -> Any:
    if PaddleOCR is not None:
        common = dict(
            lang=LANG,
            use_textline_orientation=False,
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
        )
        if DET_NO_RESIZE:
            try:
                return PaddleOCR(**{**common, "text_det_limit_type": "max", "text_det_limit_side_len": 4096})
            except Exception:
                return PaddleOCR(**{**common, "det_limit_type": "max", "det_limit_side_len": 4096})
        return PaddleOCR(**common)

    if pytesseract is not None:
        return _TesseractOCR()

    raise RuntimeError("Không có OCR khả dụng. Cần paddleocr hoặc pytesseract+tesseract")

def run_postprocess_for_book(book_dir: str | Path) -> Dict[str, Any]:
    """
    book_dir: Output/<book_stem>
    Đọc meta ở: Output/<book_stem>/Chunk/<lesson_stem>/chunk_XX/*.json  (trừ *.keywords.json)
    PDF nằm cạnh json: jp.with_suffix(".pdf")
    Debug lưu per-chunk: .../chunk_XX/DebugCutlines/
    """
    book_dir = Path(book_dir)
    chunk_root = book_dir / "Chunk"

    json_files = sorted([p for p in chunk_root.rglob("*.json") if not p.name.endswith(".keywords.json")])
    if not json_files:
        raise RuntimeError(f"Không thấy chunk meta json trong: {chunk_root}")

    print("ChunkRoot:", chunk_root)
    print("DebugDir :", "per-chunk => each chunk_XX/DebugCutlines/")
    print("Total meta json:", len(json_files))

    ocr = build_ocr()

    ok_count = skip_count = fail_count = 0
    last_debug_dir: Optional[Path] = None

    for jp in json_files:
        try:
            meta = read_json(jp)
        except Exception:
            print("[FAIL] JSON parse:", jp)
            fail_count += 1
            continue

        heading = str(meta.get("heading", "")).strip()
        heading_num = extract_heading_num(heading)

        is_content_head = bool(meta.get("content_head", False))
        is_force_heading = (heading_num in FORCE_HEADING_NUMS)

        if (not is_content_head) and (not is_force_heading):
            skip_count += 1
            continue

        pdf_path = jp.with_suffix(".pdf")
        if not pdf_path.exists():
            print("[FAIL] Missing chunk pdf:", pdf_path)
            fail_count += 1
            continue

        already_done = (is_content_head and bool(meta.get(EXTRACT_KEY, False))) or (
            (not is_content_head) and bool(meta.get(EXTRACT_HEADING_KEY, False))
        )
        if (not FORCE_REPROCESS) and already_done:
            skip_count += 1
            continue

        try:
            out_dir = jp.parent / "DebugCutlines"   # ✅ per-chunk
            last_debug_dir = out_dir

            payload = process_one_chunk(ocr, jp, pdf_path, out_dir)
            if payload is None:
                skip_count += 1
            else:
                ok_count += 1
                mark_extract = is_content_head
                mark_extract_heading = (not is_content_head) and (heading_num in FORCE_HEADING_NUMS)
                mark_chunk_processed(jp, meta, mark_extract=mark_extract, mark_extract_heading=mark_extract_heading)

        except Exception as e:
            print("[FAIL]", jp, "=>", e)
            fail_count += 1

    print("\n=== POSTPROCESS SUMMARY ===")
    print("OK  :", ok_count)
    print("SKIP:", skip_count)
    print("FAIL:", fail_count)

    return {
        "ok": ok_count,
        "skip": skip_count,
        "fail": fail_count,
        "debug_dir": "per-chunk: each chunk_XX/DebugCutlines/",
        "debug_example": (str(last_debug_dir) if last_debug_dir else None),
    }