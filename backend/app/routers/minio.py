import os
import io
import time
import tempfile
import shutil
import json
import re
import threading
import uuid
from pathlib import Path

try:
    from pypdf import PdfReader, PdfWriter
except Exception:  # pragma: no cover
    from PyPDF2 import PdfReader, PdfWriter  # type: ignore

from urllib.parse import quote, quote_plus
from typing import List, Optional, Tuple, Set, Dict, Any

from sqlalchemy import text
from datetime import timedelta, datetime, timezone

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Query, Request
from fastapi.responses import StreamingResponse, FileResponse

from minio.error import S3Error
from pydantic import BaseModel, Field
from minio.commonconfig import CopySource
from minio.deleteobjects import DeleteObject

from ..services.minio_client import get_minio_client
from ..services.mongo_client import get_mongo_client
from ..services.mongo_sync import sync_minio_object_to_mongo
from ..services.hierarchy_description_keywords import rebuild_hierarchy_descriptions_and_keywords
from ..services.postgre_sync_from_mongo import sync_postgre_from_mongo_auto_ids, PgIds
from ..services.neo_sync import sync_neo4j_from_maps_and_pg_ids
from ..services.postgre_client import get_engine
from ..services.neo_client import neo4j_driver
from ..services.media_sync import sync_minio_media_to_mongo
from ..services.postgre_media_sync import sync_postgre_media_from_mongo
from ..services.neo_media_sync import sync_media_to_neo4j
from ..services.media_content_ai import generate_media_description
from ..services.auto_split_upload import extract_and_split_structure, cleanup_split_result, _extract_heading_number
router = APIRouter(
    prefix="/admin/minio",
    tags=["Minio"]
)

# =================== Helpers =================== #

_UPLOAD_PROGRESS: Dict[str, Dict[str, Any]] = {}
_UPLOAD_PROGRESS_LOCK = threading.Lock()
_UPLOAD_PROGRESS_TTL_SECONDS = 60 * 60 * 6


def _progress_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default

def _normalize_crop_bands(raw: Any) -> List[Dict[str, int]]:
    bands: List[Dict[str, int]] = []
    if not raw:
        return bands
    if isinstance(raw, dict):
        raw = list(raw.values())
    if not isinstance(raw, list):
        return bands

    dedup: Dict[int, Dict[str, int]] = {}
    for item in raw:
        if not isinstance(item, dict):
            continue
        page = _safe_int(item.get("page"), None)
        top = _safe_int(item.get("cropTop"), None)
        bottom = _safe_int(item.get("cropBottom"), None)
        if page is None or top is None or bottom is None or bottom <= top:
            continue
        dedup[int(page)] = {"page": int(page), "cropTop": int(top), "cropBottom": int(bottom)}

    for page in sorted(dedup):
        bands.append(dedup[page])
    return bands


def _crop_bands_from_item(item: Dict[str, Any]) -> List[Dict[str, int]]:
    bands = _normalize_crop_bands(item.get("cropBands"))
    if bands:
        return bands

    page = _safe_int(item.get("cropPage"), 1) or 1
    top = _safe_int(item.get("cropTop"), None)
    bottom = _safe_int(item.get("cropBottom"), None)
    if top is not None and bottom is not None and bottom > top:
        return [{"page": int(page), "cropTop": int(top), "cropBottom": int(bottom)}]
    return []


def _get_crop_band_for_page(item: Dict[str, Any], page: Optional[int] = None) -> Optional[Dict[str, int]]:
    bands = _crop_bands_from_item(item)
    if not bands:
        return None
    target_page = _safe_int(page, None)
    if target_page is None:
        target_page = _safe_int(item.get("cropPage"), None)
    if target_page is not None:
        for band in bands:
            if int(band.get("page") or 0) == int(target_page):
                return dict(band)
    return dict(bands[0])


def _sync_item_crop_fields(item: Dict[str, Any]) -> Dict[str, Any]:
    bands = _crop_bands_from_item(item)
    item["cropBands"] = bands
    selected_page = _safe_int(item.get("cropPage"), 1) or 1
    band = _get_crop_band_for_page(item, selected_page)
    if band:
        item["cropPage"] = int(band["page"])
        item["cropTop"] = int(band["cropTop"])
        item["cropBottom"] = int(band["cropBottom"])
    else:
        item["cropPage"] = int(selected_page)
        item["cropTop"] = None
        item["cropBottom"] = None
    return item


def _has_manual_crop_band(item: Dict[str, Any]) -> bool:
    return bool(_crop_bands_from_item(item))



def _apply_manual_crop_band(
    *,
    chunk_pdf_path: str,
    out_dir: Path,
    crop_page: int = 1,
    crop_top: Optional[int] = None,
    crop_bottom: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    try:
        import fitz  # type: ignore
        import cv2  # type: ignore
        from ..services import sgk_chunk_postprocess as mod  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f'Không thể dùng manual crop band: {exc}') from exc

    pdf_path = Path(chunk_pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f'PDF not found: {pdf_path}')

    doc = fitz.open(str(pdf_path))
    page_count = doc.page_count
    if page_count <= 0:
        doc.close()
        raise RuntimeError('PDF chunk không có trang nào để crop')

    page_idx = max(0, min(int(crop_page or 1) - 1, page_count - 1))
    page = doc[page_idx]
    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
    out_dir.mkdir(parents=True, exist_ok=True)

    stem = pdf_path.stem
    page_png = out_dir / f"{stem}_page_{page_idx + 1}.png"
    debug_png = out_dir / f"{stem}_cutline.png"
    top_png = out_dir / f"{stem}_cutline_top.png"
    mid_png = out_dir / f"{stem}_cutline_middle.png"
    bot_png = out_dir / f"{stem}_cutline_bot.png"
    cut_json = out_dir / f"{stem}_cutline.json"

    pix.save(str(page_png))
    doc.close()

    img = mod.imread_unicode(page_png)
    if img is None:
        raise RuntimeError('Không render được trang PDF để crop')

    h, w = img.shape[:2]
    top = max(0, min(_safe_int(crop_top, 0) or 0, h - 1))
    bottom = max(top + 1, min(_safe_int(crop_bottom, h) or h, h))

    top_img = img[:top].copy() if top > 0 else None
    mid_img = img[top:bottom].copy()
    bot_img = img[bottom:].copy() if bottom < h else None

    if top_img is not None and top_img.size:
        mod.imwrite_unicode(top_png, top_img)
    if mid_img is not None and mid_img.size:
        mod.imwrite_unicode(mid_png, mid_img)
    if bot_img is not None and bot_img.size:
        mod.imwrite_unicode(bot_png, bot_img)

    dbg = img.copy()
    cv2.line(dbg, (0, top), (w, top), (0, 0, 255), 2)
    cv2.line(dbg, (0, bottom), (w, bottom), (255, 0, 0), 2)
    mod.imwrite_unicode(debug_png, dbg)

    mod.replace_page_with_png_inplace(pdf_path, mid_png, page_idx, make_backup=False)

    payload = {
        'mode': 'manual_crop_band',
        'page': int(page_idx + 1),
        'crop_top': int(top),
        'crop_bottom': int(bottom),
        'page_height': int(h),
        'page_width': int(w),
        'top_png': str(top_png) if top_png.exists() else '',
        'middle_png': str(mid_png) if mid_png.exists() else '',
        'bot_png': str(bot_png) if bot_png.exists() else '',
        'debug_png': str(debug_png) if debug_png.exists() else '',
    }
    cut_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def _apply_manual_crop_bands(
    *,
    chunk_pdf_path: str,
    out_dir: Path,
    crop_bands: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for band in _normalize_crop_bands(crop_bands):
        res = _apply_manual_crop_band(
            chunk_pdf_path=chunk_pdf_path,
            out_dir=out_dir,
            crop_page=int(band.get("page") or 1),
            crop_top=_safe_int(band.get("cropTop")),
            crop_bottom=_safe_int(band.get("cropBottom")),
        )
        if res:
            results.append(res)
    if results:
        try:
            stem = Path(chunk_pdf_path).stem
            cut_json = out_dir / f"{stem}_cutline.json"
            cut_json.write_text(json.dumps({"mode": "manual_crop_bands", "bands": results}, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
    return results



_AUTO_REVIEW_SESSIONS: Dict[str, Dict[str, Any]] = {}
_AUTO_REVIEW_LOCK = threading.Lock()
_AUTO_REVIEW_TTL_SECONDS = 60 * 60 * 12
_AUTO_REVIEW_DIR = Path(tempfile.gettempdir()) / 'minio_upload_auto_sessions'


def _ensure_auto_review_dir() -> None:
    _AUTO_REVIEW_DIR.mkdir(parents=True, exist_ok=True)


def _auto_review_session_file(session_id: str) -> Path:
    safe_id = re.sub(r'[^a-zA-Z0-9_-]+', '', _clean(session_id)) or 'session'
    return _AUTO_REVIEW_DIR / f"{safe_id}.json"


def _auto_review_session_dir(session_id: str) -> Path:
    safe_id = re.sub(r'[^a-zA-Z0-9_-]+', '', _clean(session_id)) or 'session'
    return _AUTO_REVIEW_DIR / safe_id


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return value


def _load_auto_review_session_from_disk(session_id: str) -> Dict[str, Any] | None:
    _ensure_auto_review_dir()
    path = _auto_review_session_file(session_id)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return None
    return None


def _write_auto_review_session_to_disk(payload: Dict[str, Any]) -> None:
    _ensure_auto_review_dir()
    session_id = _clean(payload.get('session_id'))
    if not session_id:
        return
    path = _auto_review_session_file(session_id)
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding='utf-8')


def _delete_auto_review_session_from_disk(session_id: str) -> None:
    try:
        path = _auto_review_session_file(session_id)
        if path.exists():
            path.unlink()
    except Exception:
        pass
    try:
        session_dir = _auto_review_session_dir(session_id)
        if session_dir.exists():
            shutil.rmtree(session_dir, ignore_errors=True)
    except Exception:
        pass


def _cleanup_auto_review_sessions() -> None:
    _ensure_auto_review_dir()
    now_ts = time.time()
    stale_ids = []
    with _AUTO_REVIEW_LOCK:
        for session_id, payload in list(_AUTO_REVIEW_SESSIONS.items()):
            updated_at = payload.get("updated_ts") or payload.get("created_ts") or now_ts
            if now_ts - float(updated_at) > _AUTO_REVIEW_TTL_SECONDS:
                stale_ids.append(session_id)
        for session_id in stale_ids:
            _AUTO_REVIEW_SESSIONS.pop(session_id, None)
            _delete_auto_review_session_from_disk(session_id)

    for session_file in _AUTO_REVIEW_DIR.glob('*.json'):
        try:
            payload = json.loads(session_file.read_text(encoding='utf-8'))
            updated_at = (payload or {}).get('updated_ts') or (payload or {}).get('created_ts') or now_ts
            if now_ts - float(updated_at) > _AUTO_REVIEW_TTL_SECONDS:
                sid = _clean((payload or {}).get('session_id')) or session_file.stem
                _delete_auto_review_session_from_disk(sid)
        except Exception:
            try:
                if now_ts - session_file.stat().st_mtime > _AUTO_REVIEW_TTL_SECONDS:
                    session_file.unlink()
            except Exception:
                pass


def _touch_auto_review_session(session_id: str) -> None:
    with _AUTO_REVIEW_LOCK:
        payload = _AUTO_REVIEW_SESSIONS.get(session_id)
        if payload is not None:
            payload["updated_ts"] = time.time()
            payload["updatedAt"] = _progress_now_iso()
            try:
                _write_auto_review_session_to_disk(payload)
            except Exception:
                pass


def _save_auto_review_session(payload: Dict[str, Any]) -> None:
    _cleanup_auto_review_sessions()
    session_id = _clean(payload.get("session_id")) or uuid.uuid4().hex
    now_ts = time.time()
    payload["session_id"] = session_id
    payload.setdefault("createdAt", _progress_now_iso())
    payload.setdefault("updatedAt", payload["createdAt"])
    payload["created_ts"] = payload.get("created_ts") or now_ts
    payload["updated_ts"] = now_ts
    with _AUTO_REVIEW_LOCK:
        _AUTO_REVIEW_SESSIONS[session_id] = payload
    _write_auto_review_session_to_disk(payload)


def _pop_auto_review_session(session_id: str) -> Dict[str, Any] | None:
    sid = _clean(session_id)
    with _AUTO_REVIEW_LOCK:
        payload = _AUTO_REVIEW_SESSIONS.pop(sid, None)
    if payload is None:
        payload = _load_auto_review_session_from_disk(sid)
    _delete_auto_review_session_from_disk(sid)
    return payload


def _get_auto_review_session(session_id: str) -> Dict[str, Any] | None:
    _cleanup_auto_review_sessions()
    sid = _clean(session_id)
    with _AUTO_REVIEW_LOCK:
        payload = _AUTO_REVIEW_SESSIONS.get(sid)
    if payload is not None:
        return payload
    payload = _load_auto_review_session_from_disk(sid)
    if payload is not None:
        with _AUTO_REVIEW_LOCK:
            _AUTO_REVIEW_SESSIONS[sid] = payload
    return payload


def _cleanup_upload_progress() -> None:
    now_ts = time.time()
    stale_ids = []
    with _UPLOAD_PROGRESS_LOCK:
        for upload_id, payload in list(_UPLOAD_PROGRESS.items()):
            updated_at = payload.get("updated_ts") or payload.get("created_ts") or now_ts
            if now_ts - float(updated_at) > _UPLOAD_PROGRESS_TTL_SECONDS:
                stale_ids.append(upload_id)
        for upload_id in stale_ids:
            _UPLOAD_PROGRESS.pop(upload_id, None)


def _init_upload_progress(upload_id: str, *, path: str, total_files: int) -> None:
    if not upload_id:
        return
    _cleanup_upload_progress()
    now_iso = _progress_now_iso()
    now_ts = time.time()
    payload = {
        "uploadId": upload_id,
        "path": path,
        "status": "processing",
        "stage": "preparing",
        "stageLabel": "Đang chuẩn bị xử lý",
        "message": "Đang chuẩn bị xử lý",
        "percent": 12,
        "totalFiles": max(1, int(total_files or 1)),
        "completedFiles": 0,
        "currentFileIndex": 0,
        "currentFileName": "",
        "errors": [],
        "startedAt": now_iso,
        "updatedAt": now_iso,
        "created_ts": now_ts,
        "updated_ts": now_ts,
    }
    with _UPLOAD_PROGRESS_LOCK:
        _UPLOAD_PROGRESS[upload_id] = payload


def _update_upload_progress(upload_id: str, **fields: Any) -> None:
    if not upload_id:
        return
    now_iso = _progress_now_iso()
    now_ts = time.time()
    with _UPLOAD_PROGRESS_LOCK:
        payload = _UPLOAD_PROGRESS.get(upload_id)
        if payload is None:
            payload = {
                "uploadId": upload_id,
                "status": "processing",
                "stage": "preparing",
                "stageLabel": "Đang chuẩn bị xử lý",
                "message": "Đang chuẩn bị xử lý",
                "percent": 0,
                "totalFiles": 1,
                "completedFiles": 0,
                "currentFileIndex": 0,
                "currentFileName": "",
                "errors": [],
                "startedAt": now_iso,
                "created_ts": now_ts,
            }
            _UPLOAD_PROGRESS[upload_id] = payload
        payload.update(fields)
        payload["updatedAt"] = now_iso
        payload["updated_ts"] = now_ts


def _append_upload_error(upload_id: str, error: Dict[str, Any]) -> None:
    if not upload_id:
        return
    with _UPLOAD_PROGRESS_LOCK:
        payload = _UPLOAD_PROGRESS.get(upload_id)
        if not payload:
            return
        errors = list(payload.get("errors") or [])
        errors.append(error)
        payload["errors"] = errors
        payload["updatedAt"] = _progress_now_iso()
        payload["updated_ts"] = time.time()


def _mark_file_progress(
    upload_id: str,
    *,
    file_index: int,
    total_files: int,
    file_name: str,
    stage: str,
    stage_label: str,
    file_percent: float,
    message: str | None = None,
    status: str = "processing",
    completed_files: int | None = None,
) -> None:
    if not upload_id:
        return
    total = max(1, int(total_files or 1))
    idx = max(1, int(file_index or 1))
    normalized = max(0.0, min(1.0, float(file_percent)))
    overall = ((idx - 1) + normalized) / total * 100.0
    if completed_files is None:
        completed_files = idx - 1 + (1 if normalized >= 1.0 else 0)
    _update_upload_progress(
        upload_id,
        status=status,
        stage=stage,
        stageLabel=stage_label,
        message=message or stage_label,
        percent=min(100, max(0, round(overall))),
        totalFiles=total,
        completedFiles=max(0, min(total, int(completed_files))),
        currentFileIndex=idx,
        currentFileName=file_name or "",
    )


def _finish_upload_progress(
    upload_id: str,
    *,
    total_files: int,
    completed_files: int,
    status: str,
    message: str,
    stage: str = "completed",
    stage_label: str = "Hoàn tất",
) -> None:
    if not upload_id:
        return
    _update_upload_progress(
        upload_id,
        status=status,
        stage=stage,
        stageLabel=stage_label,
        message=message,
        percent=100,
        totalFiles=max(1, int(total_files or 1)),
        completedFiles=max(0, int(completed_files or 0)),
    )

def clean_path(path: str) -> str:
    p = (path or "").strip()
    if p.startswith("/"):
        p = p[1:]
    if "\\" in p:
        raise HTTPException(status_code=400, detail="Invalid path (contains backslash)")
    parts = [x for x in p.split("/") if x != ""]
    if ".." in parts:
        raise HTTPException(status_code=400, detail="Invalid path (contains ..)")
    return "/".join(parts)


def folder_marker(rel_path: str) -> str:
    p = clean_path(rel_path)
    return f"{p}/" if p else ""


def _split_keyword_values(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw = value
    else:
        raw = [x.strip() for x in re.split(r"[,;\n\r\t|]+", str(value)) if str(x).strip()]
    out: List[str] = []
    seen = set()
    for item in raw:
        s = str(item).strip()
        if not s:
            continue
        key = s.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
    return out


def _active_docs(cursor) -> List[dict]:
    docs: List[dict] = []
    for doc in cursor:
        if str(doc.get("status") or "active").strip().lower() == "hidden":
            continue
        docs.append(doc)
    return docs


def _refresh_topic_keywords_from_lessons(*, topic_map: str, lesson_map: str = "") -> Dict[str, Any] | None:
    mg = get_mongo_client()
    db = mg["db"]
    now = datetime.now(timezone.utc)

    topic_doc = db["topics"].find_one({"topicID": topic_map}) if topic_map else None
    if topic_doc is None and lesson_map:
        lesson_doc = db["lessons"].find_one({"lessonID": lesson_map})
        if lesson_doc:
            topic_doc = db["topics"].find_one({"topicID": str(lesson_doc.get("topicID") or "").strip()})
    if not topic_doc:
        return None

    topic_id = str(topic_doc.get("topicID") or "").strip()
    lesson_docs = _active_docs(db["lessons"].find({"topicID": topic_id}).sort("lessonNumber", 1))
    merged: List[str] = []
    seen = set()
    for lesson in lesson_docs:
        for kw in _split_keyword_values(lesson.get("keywordLesson")):
            key = kw.casefold()
            if key in seen:
                continue
            seen.add(key)
            merged.append(kw)

    db["topics"].update_one(
        {"_id": topic_doc["_id"]},
        {"$set": {"keywordTopic": merged, "searchUpdatedAt": now, "updatedAt": now}},
    )
    return {
        "topicID": topic_id,
        "subjectID": str(topic_doc.get("subjectID") or "").strip(),
        "keywordCount": len(merged),
    }


def _refresh_subject_keywords_from_topics(*, subject_map: str) -> Dict[str, Any] | None:
    if not subject_map:
        return None
    mg = get_mongo_client()
    db = mg["db"]
    now = datetime.now(timezone.utc)

    subject_doc = db["subjects"].find_one({"subjectID": subject_map})
    if not subject_doc:
        return None

    topics = _active_docs(db["topics"].find({"subjectID": subject_map}).sort("topicNumber", 1))
    merged: List[str] = []
    seen = set()
    for topic in topics:
        for kw in _split_keyword_values(topic.get("keywordTopic")):
            key = kw.casefold()
            if key in seen:
                continue
            seen.add(key)
            merged.append(kw)

    db["subjects"].update_one(
        {"_id": subject_doc["_id"]},
        {"$set": {"keywordSubject": merged, "searchUpdatedAt": now, "updatedAt": now}},
    )
    return {"subjectID": subject_map, "keywordCount": len(merged)}


def _refresh_standard_hierarchy_keywords(*, subject_map: str, topic_map: str = "", lesson_map: str = "") -> Dict[str, Any]:
    result: Dict[str, Any] = {"topic": None, "subject": None}

    topic_res = _refresh_topic_keywords_from_lessons(topic_map=topic_map, lesson_map=lesson_map)
    if topic_res:
        result["topic"] = {"topicID": topic_res["topicID"], "keywordCount": topic_res["keywordCount"]}
        subject_map = subject_map or topic_res["subjectID"]

    subject_res = _refresh_subject_keywords_from_topics(subject_map=subject_map)
    if subject_res:
        result["subject"] = subject_res

    return result


def _finalize_standard_upload_batch(
    *,
    upload_id: str,
    actor: str,
    dirty_topics: Set[str],
    dirty_subjects: Set[str],
) -> Dict[str, Any]:
    total_steps = max(1, len(dirty_topics) + len(dirty_subjects))
    completed = 0
    summary: Dict[str, Any] = {"topics": [], "subjects": []}

    def _percent(base: float = 96.0, span: float = 3.0) -> int:
        return min(99, max(1, round(base + (completed / total_steps) * span)))

    for topic_map in sorted(x for x in dirty_topics if str(x or "").strip()):
        _update_upload_progress(
            upload_id,
            stage="finalizing_hierarchy",
            stageLabel="Đang cập nhật keyword topic",
            message=f"Đang gom keyword lesson -> topic cho {topic_map}",
            percent=_percent(),
        )
        try:
            topic_res = _refresh_topic_keywords_from_lessons(topic_map=topic_map)
            if topic_res:
                summary["topics"].append(topic_res)
                subject_map = str(topic_res.get("subjectID") or "").strip()
                class_map = _derive_class_map_from_subject_map(subject_map)
                if subject_map:
                    pg_ids = sync_postgre_from_mongo_auto_ids(
                        class_map=class_map,
                        subject_map=subject_map,
                        topic_map=topic_map,
                    )
                    sync_neo4j_from_maps_and_pg_ids(
                        class_map=class_map,
                        subject_map=subject_map,
                        topic_map=topic_map,
                        pg_ids=pg_ids,
                        actor=actor,
                    )
                    dirty_subjects.add(subject_map)
        except Exception:
            pass
        completed += 1

    for subject_map in sorted(x for x in dirty_subjects if str(x or "").strip()):
        _update_upload_progress(
            upload_id,
            stage="finalizing_hierarchy",
            stageLabel="Đang cập nhật keyword subject",
            message=f"Đang gom keyword topic -> subject cho {subject_map}",
            percent=_percent(),
        )
        try:
            subject_res = _refresh_subject_keywords_from_topics(subject_map=subject_map)
            if subject_res:
                summary["subjects"].append(subject_res)
                class_map = _derive_class_map_from_subject_map(subject_map)
                pg_ids = sync_postgre_from_mongo_auto_ids(
                    class_map=class_map,
                    subject_map=subject_map,
                )
                sync_neo4j_from_maps_and_pg_ids(
                    class_map=class_map,
                    subject_map=subject_map,
                    pg_ids=pg_ids,
                    actor=actor,
                )
        except Exception:
            pass
        completed += 1

    return summary


def _api_base(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _backend_open_url(request: Request, object_key_virtual: str) -> str:
    # object_key_virtual là đường dẫn kiểu UI dùng (single-bucket: key, multi-bucket: bucket/key)
    return f"{_api_base(request)}/admin/minio/open?object_key={quote_plus(object_key_virtual)}"


def _stream_minio_object(resp, chunk_size: int = 1024 * 1024):
    try:
        while True:
            data = resp.read(chunk_size)
            if not data:
                break
            yield data
    finally:
        try:
            resp.close()
        except Exception:
            pass
        try:
            resp.release_conn()
        except Exception:
            pass


def _runtime():
    """
    Lấy client + chế độ bucket từ ENV (ENV đã được load trong get_minio_client()).
    """
    client = get_minio_client()

    default_bucket = (os.getenv("MINIO_BUCKET") or "").strip() or None

    endpoint = (os.getenv("MINIO_ENDPOINT") or "127.0.0.1:9000").strip()
    secure = (os.getenv("MINIO_SECURE", "false").strip().lower() == "true")
    scheme = "https" if secure else "http"

    public_base = (os.getenv("MINIO_PUBLIC_BASE_URL") or f"{scheme}://{endpoint}").rstrip("/")

    return client, default_bucket, public_base



# UI luôn dùng 3 "bucket" ảo: documents/images/video.
# - Multi-bucket mode: chúng là bucket thật.
# - Single-bucket mode (MINIO_BUCKET set): ta map documents -> root, images/video -> folder prefix,
#   và nếu bucket thật tồn tại (bucket_exists) thì ưu tiên dùng bucket thật.
VIRTUAL_SECTIONS = {"documents", "images", "video"}

def _bucket_exists_safe(client, bucket: str) -> bool:
    try:
        return bool(bucket) and client is not None and client.bucket_exists(bucket)
    except Exception:
        return False


def _split_virtual(
    virtual: str,
    default_bucket: Optional[str],
    client=None,
    *,
    allow_empty_key: bool,
) -> Tuple[str, str]:
    """Virtual path từ UI -> (bucket, key)

    UI của bạn luôn truyền đường dẫn kiểu: <section>/<...>
    với section ∈ {documents, images, video}.

    - Nếu không set MINIO_BUCKET: coi section là bucket thật (multi-bucket mode).
    - Nếu có MINIO_BUCKET (single-bucket mode):
        * Nếu bucket thật tên section tồn tại -> ưu tiên dùng bucket thật.
        * Nếu không tồn tại bucket thật:
            - documents -> map vào root (key bỏ prefix documents/)
            - images/video -> map vào folder prefix images/ hoặc video/ trong bucket default.

    Hàm này vẫn tương thích với trường hợp client khác gửi key thuần (không có section).
    """

    p = clean_path(virtual)

    # single-bucket mode
    if default_bucket:
        if not allow_empty_key and not p:
            raise HTTPException(status_code=400, detail="Path is required")
        if not p:
            return default_bucket, ""

        head, tail = (p.split("/", 1) + [""])[:2]  # always 2 items
        head = (head or "").strip()
        tail = (tail or "").strip()

        # UI-style: <section>/<key>
        if head in VIRTUAL_SECTIONS:
            # If section bucket really exists, treat it as real bucket
            if _bucket_exists_safe(client, head):
                if not allow_empty_key and not tail:
                    raise HTTPException(status_code=400, detail="Thiếu key sau bucket. VD: documents/class-10")
                return head, tail

            # Otherwise, map into default bucket
            if head == "documents":
                # documents maps to root
                if not allow_empty_key and not tail:
                    raise HTTPException(status_code=400, detail="Thiếu key sau bucket. VD: documents/class-10")
                return default_bucket, tail

            # images/video maps to folder prefix under default bucket
            if not tail:
                return default_bucket, head
            return default_bucket, f"{head}/{tail}"

        # Backward compatible: key-only
        return default_bucket, p

    # multi-bucket mode (bucket-per-section)
    if not p:
        if allow_empty_key:
            return "", ""
        raise HTTPException(
            status_code=400,
            detail="Thiếu bucket. VD: ?path=documents hoặc documents/folderA (hoặc cấu hình MINIO_BUCKET)",
        )

    parts = p.split("/", 1)
    bucket = parts[0].strip()
    key = parts[1].strip() if len(parts) > 1 else ""

    if not bucket:
        raise HTTPException(status_code=400, detail="Bucket name rỗng/không hợp lệ")
    if not allow_empty_key and not key:
        raise HTTPException(status_code=400, detail="Thiếu key sau bucket. VD: documents/class-10")

    return bucket, key





def _to_virtual(default_bucket: Optional[str], bucket: str, key: str) -> str:
    """Format đường dẫn trả về cho UI.

    UI luôn dùng format: <section>/<key>.

    - Multi-bucket mode: trả "bucket/key".
    - Single-bucket mode:
        * Nếu bucket != default_bucket (đã auto-detect bucket thật) -> trả "bucket/key".
        * Nếu bucket == default_bucket: suy ra section theo prefix (images/, video/), còn lại coi là documents.
    """

    k = clean_path(key)

    if default_bucket:
        # If we ended up using a real bucket that is not the default, behave like multi-bucket.
        if bucket and bucket != default_bucket:
            return f"{bucket}/{k}" if k else bucket

        # Map internal key -> virtual section
        if not k:
            return "documents"

        # Old data may already include documents/ prefix
        if k == "documents" or k.startswith("documents/"):
            rest = k[len("documents/"):] if k.startswith("documents/") else ""
            return f"documents/{rest}" if rest else "documents"

        for sec in ("images", "video"):
            if k == sec or k.startswith(sec + "/"):
                return k

        # Default: documents section maps to root
        return f"documents/{k}"

    # multi-bucket
    return f"{bucket}/{k}" if k else bucket




def _public_url(client, public_base: str, bucket: str, object_key: str) -> str:
    # Prefer presigned URL để bấm là mở được, không phụ thuộc public_base
    try:
        return client.presigned_get_object(bucket, object_key, expires=timedelta(hours=24))
    except Exception:
        encoded = quote(object_key, safe="/")
        return f"{public_base}/{bucket}/{encoded}"


def prefix_has_anything(client, bucket: str, prefix: str) -> bool:
    it = client.list_objects(bucket, prefix=prefix, recursive=True)
    for _ in it:
        return True
    return False


def _get_actor(request: Request | None) -> str:
    """Lấy người thao tác từ header (frontend sẽ gửi x-user)."""
    if request is None:
        return "system"
    return request.headers.get("x-user") or request.headers.get("x-actor") or "system"


def _normalize_folder_type(s: str) -> str:
    x = str(s or "").strip().lower()
    if x == "subjects":
        return "subject"
    if x == "topics":
        return "topic"
    if x == "lessons":
        return "lesson"
    if x == "chunks":
        return "chunk"
    return x


def _infer_category_from_path(path: str) -> str:
    parts = [p for p in clean_path(path).split("/") if p]
    head = (parts[0] if parts else "").lower()
    if head == "images":
        return "image"
    if head in ("video", "videos"):
        return "video"
    return "document"

def _extract_last_number(value: Any) -> str:
    s = str(value or "").strip()
    nums = []
    current = []
    for ch in s:
        if ch.isdigit():
            current.append(ch)
        elif current:
            nums.append("".join(current))
            current = []
    if current:
        nums.append("".join(current))
    return nums[-1] if nums else ""


def _derive_class_map_from_subject_map(subject_map: str) -> str:
    n = _extract_last_number(subject_map)
    return f"L{n}" if n else ""


def _parse_topic_map(topic_map: str) -> Optional[Dict[str, str]]:
    s = str(topic_map or "").strip()
    parts = s.split("_CD", 1)
    if len(parts) != 2 or not parts[0] or not parts[1].isdigit():
        return None
    subject_map, topic_no = parts
    return {
        "class_map": _derive_class_map_from_subject_map(subject_map),
        "subject_map": subject_map,
        "topic_map": s,
        "topicNumber": topic_no,
    }


def _parse_lesson_map(lesson_map: str) -> Optional[Dict[str, str]]:
    s = str(lesson_map or "").strip()
    base, lesson_no = s.rsplit("_B", 1) if "_B" in s else ("", "")
    topic_meta = _parse_topic_map(base)
    if not topic_meta or not lesson_no.isdigit():
        return None
    return {
        **topic_meta,
        "lesson_map": s,
        "lessonNumber": lesson_no,
    }


def _parse_chunk_map(chunk_map: str) -> Optional[Dict[str, str]]:
    s = str(chunk_map or "").strip()
    base, chunk_no = s.rsplit("_C", 1) if "_C" in s else ("", "")
    lesson_meta = _parse_lesson_map(base)
    if not lesson_meta or not chunk_no.isdigit():
        return None
    return {
        **lesson_meta,
        "chunk_map": s,
        "chunkNumber": chunk_no,
    }


def _derive_chain_from_meta(path: str, meta: Dict[str, Any]) -> Dict[str, str]:
    folder_type = _normalize_folder_type(
        (meta.get("folderType") or meta.get("folder_type") or "").strip()
    )
    subject_map = str(meta.get("subject_map") or meta.get("subjectMap") or meta.get("subjectID") or "").strip()
    topic_map = str(meta.get("topic_map") or meta.get("topicMap") or meta.get("topicID") or "").strip()
    lesson_map = str(meta.get("lesson_map") or meta.get("lessonMap") or meta.get("lessonID") or "").strip()
    chunk_map = str(meta.get("chunk_map") or meta.get("chunkMap") or meta.get("chunkID") or "").strip()
    class_map = str(meta.get("class_map") or meta.get("classMap") or meta.get("classID") or "").strip()

    if folder_type == "chunk" and chunk_map:
        data = _parse_chunk_map(chunk_map) or {}
    elif folder_type == "lesson" and lesson_map:
        data = _parse_lesson_map(lesson_map) or {}
    elif folder_type == "topic" and topic_map:
        data = _parse_topic_map(topic_map) or {}
    elif folder_type == "subject" and subject_map:
        data = {
            "class_map": _derive_class_map_from_subject_map(subject_map),
            "subject_map": subject_map,
        }
    else:
        data = {}

    class_map = class_map or str(data.get("class_map") or "")
    subject_map = subject_map or str(data.get("subject_map") or "")
    topic_map = topic_map or str(data.get("topic_map") or "")
    lesson_map = lesson_map or str(data.get("lesson_map") or "")
    chunk_map = chunk_map or str(data.get("chunk_map") or "")

    return {
        "folder_type": folder_type,
        "class_map": class_map,
        "subject_map": subject_map,
        "topic_map": topic_map,
        "lesson_map": lesson_map,
        "chunk_map": chunk_map,
        "class_number": _extract_last_number(class_map or subject_map),
    }


def _remap_virtual_path_by_meta(path: str, meta: Dict[str, Any]) -> str:
    raw_parts = [p for p in clean_path(path).split("/") if p]
    if not raw_parts:
        return clean_path(path)

    chain = _derive_chain_from_meta(path, meta)
    class_no = chain.get("class_number", "")
    folder_type = chain.get("folder_type", "")
    if not class_no or folder_type not in {"subject", "topic", "lesson", "chunk"}:
        return clean_path(path)

    class_folder = f"class-{class_no}"
    parts = list(raw_parts)
    if len(parts) >= 2:
        parts[1] = class_folder
    else:
        parts.append(class_folder)

    normalized_folder = f"{folder_type}s"
    if parts:
        tail = parts[-1].lower()
        if tail in {"subjects", "topics", "lessons", "chunks"}:
            parts[-1] = normalized_folder

    return "/".join(parts)


def _ensure_folder_markers(client, bucket: str, rel_path: str) -> None:
    parts = [p for p in clean_path(rel_path).split("/") if p]
    acc = []
    for part in parts:
        acc.append(part)
        marker = "/".join(acc) + "/"
        try:
            client.stat_object(bucket, marker)
        except Exception:
            client.put_object(
                bucket,
                marker,
                data=io.BytesIO(b""),
                length=0,
                content_type="application/octet-stream",
            )



def _parse_meta_json(meta_json: str) -> Dict[str, Any]:
    if not meta_json:
        return {}
    try:
        obj = json.loads(meta_json)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid meta_json (must be JSON): {e}") from e
    if obj is None:
        return {}
    if not isinstance(obj, dict):
        raise HTTPException(status_code=422, detail="meta_json must be a JSON object")
    return obj


def _hide_mongo_chunk(chunk_id: str, actor: str = "system") -> None:
    """Nếu Postgres sync fail sau khi Mongo đã upsert, ẩn chunk để tránh hiển thị rác."""
    try:
        mg = get_mongo_client()
        db = mg["db"]
        from bson import ObjectId
        db["chunks"].update_one(
            {"_id": ObjectId(chunk_id)},
            {"$set": {"status": "hidden", "updatedAt": time.time(), "updatedBy": actor}},
        )
    except Exception:
        pass



def _hide_mongo_chunk_by_map(chunk_map: str, actor: str = "system") -> None:
    """Ẩn chunk theo chunk_map (chunkID) khi Postgre sync fail."""
    try:
        mg = get_mongo_client()
        db = mg["db"]
        db["chunks"].update_one(
            {"chunkID": chunk_map},
            {"$set": {"status": "hidden", "updatedAt": datetime.now(timezone.utc), "updatedBy": actor}},
        )
    except Exception:
        pass




def _hide_mongo_media(collection: str, mongo_id: str, actor: str = "system") -> None:
    try:
        mg = get_mongo_client()
        db = mg["db"]
        from bson import ObjectId
        db[collection].update_one(
            {"_id": ObjectId(mongo_id)},
            {"$set": {"status": "hidden", "updatedAt": datetime.now(timezone.utc), "updatedBy": actor}},
        )
    except Exception:
        pass


def _set_hierarchy_sync_status(
    sync_res,
    *,
    minio_ok: bool,
    mongo_ok: bool,
    postgre_ok: bool,
    neo4j_ok: bool,
    actor: str = "system",
    error: str | None = None,
    verify: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        mg = get_mongo_client()
        db = mg["db"]
        now = datetime.now(timezone.utc)
        is_full = bool(minio_ok and mongo_ok and postgre_ok and neo4j_ok and not error)
        status_doc: Dict[str, Any] = {
            "isFullySynced": is_full,
            "minio": bool(minio_ok),
            "mongo": bool(mongo_ok),
            "postgre": bool(postgre_ok),
            "neo4j": bool(neo4j_ok),
            "lastError": (error or "").strip() or None,
            "verifiedAt": now,
        }
        if verify:
            status_doc["verify"] = verify

        targets = [
            ("classes", "classID", getattr(sync_res, "class_map", "")),
            ("subjects", "subjectID", getattr(sync_res, "subject_map", "")),
            ("topics", "topicID", getattr(sync_res, "topic_map", "")),
            ("lessons", "lessonID", getattr(sync_res, "lesson_map", "")),
            ("chunks", "chunkID", getattr(sync_res, "chunk_map", "")),
        ]
        for col, field, value in targets:
            value = (str(value).strip() if value is not None else "")
            if not value:
                continue
            db[col].update_one(
                {field: value},
                {"$set": {"syncStatus": status_doc, "updatedAt": now, "updatedBy": actor}},
            )
    except Exception:
        pass


def _verify_hierarchy_sync(
    *,
    client,
    bucket: str,
    object_key: str,
    sync_res,
    pg_ids: PgIds,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "minio": False,
        "mongo": False,
        "postgre": False,
        "neo4j": False,
        "isFullySynced": False,
        "missing": [],
    }

    try:
        client.stat_object(bucket, object_key)
        result["minio"] = True
    except Exception as e:
        result["missing"].append(f"MinIO: {e}")

    try:
        mg = get_mongo_client()
        db = mg["db"]
        mongo_targets = [
            ("classes", "classID", getattr(sync_res, "class_map", "")),
            ("subjects", "subjectID", getattr(sync_res, "subject_map", "")),
            ("topics", "topicID", getattr(sync_res, "topic_map", "")),
            ("lessons", "lessonID", getattr(sync_res, "lesson_map", "")),
            ("chunks", "chunkID", getattr(sync_res, "chunk_map", "")),
        ]
        mongo_missing = []
        for col, field, value in mongo_targets:
            value = (str(value).strip() if value is not None else "")
            if not value:
                continue
            if not db[col].find_one({field: value, "status": {"$ne": "hidden"}}):
                mongo_missing.append(f"{col}.{field}={value}")
        if mongo_missing:
            result["missing"].append("MongoDB: " + ", ".join(mongo_missing))
        else:
            result["mongo"] = True
    except Exception as e:
        result["missing"].append(f"MongoDB: {e}")

    try:
        engine = get_engine()
        with engine.begin() as conn:
            pg_targets = [
                ("class", "class_id", getattr(pg_ids, "class_id", "")),
                ("subject", "subject_id", getattr(pg_ids, "subject_id", "")),
                ("topic", "topic_id", getattr(pg_ids, "topic_id", "")),
                ("lesson", "lesson_id", getattr(pg_ids, "lesson_id", "")),
                ("chunk", "chunk_id", getattr(pg_ids, "chunk_id", "")),
            ]
            pg_missing = []
            for table, field, value in pg_targets:
                value = (str(value).strip() if value is not None else "")
                if not value:
                    continue
                row = conn.execute(text(f"SELECT 1 FROM {table} WHERE {field} = :value LIMIT 1"), {"value": value}).fetchone()
                if not row:
                    pg_missing.append(f"{table}.{field}={value}")
            if pg_missing:
                result["missing"].append("PostgreSQL: " + ", ".join(pg_missing))
            else:
                result["postgre"] = True
    except Exception as e:
        result["missing"].append(f"PostgreSQL: {e}")

    driver = None
    try:
        driver = neo4j_driver()
        db_name = (os.getenv("NEO4J_DATABASE") or "").strip() or None
        neo_targets = [
            ("Class", getattr(pg_ids, "class_id", "")),
            ("Subject", getattr(pg_ids, "subject_id", "")),
            ("Topic", getattr(pg_ids, "topic_id", "")),
            ("Lesson", getattr(pg_ids, "lesson_id", "")),
            ("Chunk", getattr(pg_ids, "chunk_id", "")),
        ]
        neo_missing = []
        with driver.session(database=db_name) as session:  # type: ignore[arg-type]
            session.run("RETURN 1").consume()
            for label, value in neo_targets:
                value = (str(value).strip() if value is not None else "")
                if not value:
                    continue
                row = session.run(
                    f"MATCH (n:{label} {{pg_id: $pg_id}}) RETURN count(n) AS c",
                    pg_id=value,
                ).single()
                if not row or int(row.get("c", 0)) <= 0:
                    neo_missing.append(f"{label}.pg_id={value}")
        if neo_missing:
            result["missing"].append("Neo4j: " + ", ".join(neo_missing))
        else:
            result["neo4j"] = True
    except Exception as e:
        result["missing"].append(f"Neo4j: {e}")
    finally:
        try:
            driver.close()
        except Exception:
            pass

    result["isFullySynced"] = bool(result["minio"] and result["mongo"] and result["postgre"] and result["neo4j"])
    return result


def _sync_error_message(sync_status: Dict[str, Any]) -> str:
    missing = sync_status.get("missing") or []
    if missing:
        return "Chưa sync đủ MinIO/MongoDB/PostgreSQL/Neo4j: " + " | ".join(str(x) for x in missing)
    return "Chưa sync đủ MinIO/MongoDB/PostgreSQL/Neo4j"


# =================== Models =================== #

class CreateFolderBody(BaseModel):
    full_path: str = Field(..., min_length=1)


class RenameFolderBody(BaseModel):
    old_path: str = Field(..., min_length=1)
    new_path: str = Field(..., min_length=1)


class RenameObjectBody(BaseModel):
    object_key: str = Field(..., min_length=1)
    new_name: str = Field(..., min_length=1)


class UploadAutoApproveBody(BaseModel):
    session_id: str = Field(..., min_length=1)
    upload_id: str = Field(default="")
    items: List[Dict[str, Any]] = Field(default_factory=list)


class UploadAutoRefreshItemBody(BaseModel):
    review_id: str = Field(..., min_length=1)
    item: Dict[str, Any] = Field(default_factory=dict)
    upload_id: str = Field(default="")


# =================== GET =================== #

@router.get("/uploads/progress/{upload_id}", summary="Lấy tiến trình upload/import hiện tại")
def get_upload_progress(upload_id: str):
    _cleanup_upload_progress()
    with _UPLOAD_PROGRESS_LOCK:
        payload = _UPLOAD_PROGRESS.get((upload_id or "").strip())
        if not payload:
            return {
                "uploadId": (upload_id or "").strip(),
                "status": "pending",
                "stage": "waiting",
                "stageLabel": "Đang chờ bắt đầu",
                "message": "Đang chờ backend khởi tạo tiến trình",
                "percent": 0,
                "totalFiles": 1,
                "completedFiles": 0,
                "currentFileIndex": 0,
                "currentFileName": "",
                "errors": [],
            }
        return dict(payload)

@router.get("/open", summary="Mở/Download file (proxy qua backend - copy URL mở được ngay)")
def open_file(
    request: Request,
    object_key: str = Query(..., min_length=1, description="virtual object_key (single-bucket: key; multi-bucket: bucket/key)"),
    download: bool = Query(False, description="true = download, false = open inline"),
):
    client, default_bucket, _public_base = _runtime()

    bucket, key = _split_virtual(object_key, default_bucket, client, allow_empty_key=False)
    key = clean_path(key)

    try:
        stat = client.stat_object(bucket, key)
    except S3Error:
        raise HTTPException(status_code=404, detail="Object not found")

    filename = os.path.basename(key) or "file"

    try:
        resp = client.get_object(bucket, key)
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO get_object error: {e}") from e

    dispo = "attachment" if download else "inline"
    headers = {"Content-Disposition": f'{dispo}; filename="{filename}"'}

    media_type = getattr(stat, "content_type", None) or "application/octet-stream"
    return StreamingResponse(_stream_minio_object(resp), media_type=media_type, headers=headers)



@router.get("/list", summary="Lấy ra cấu trúc list trong MinIO (url mở được ngay qua backend)")
def list_structure(
    request: Request,
    path: str = Query("", description="VD: documents | documents/lop-10 | (multi-bucket) bucketA/folder ..."),
):
    client, default_bucket, _public_base = _runtime()

    # MULTI-BUCKET MODE: path rỗng -> list buckets
    if not default_bucket and not clean_path(path):
        try:
            buckets = client.list_buckets()
            folders = [{"name": b.name, "fullPath": b.name} for b in buckets]
            folders.sort(key=lambda x: x["name"].lower())
            return {
                "bucket": "",
                "path": "",
                "prefix": "",
                "folders": folders,
                "files": [],
                "mode": "bucket_per_section_root",
            }
        except S3Error as e:
            raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e

    bucket, key = _split_virtual(path, default_bucket, client, allow_empty_key=True)
    key = clean_path(key)
    prefix = folder_marker(key)

    try:
        objects = client.list_objects(bucket, prefix=prefix, recursive=False)

        folder_names: Set[str] = set()
        folders: List[Dict[str, Any]] = []
        files: List[Dict[str, Any]] = []

        for obj in objects:
            name = obj.object_name

            if prefix and name == prefix:
                continue

            rest = name[len(prefix):] if prefix else name
            if not rest:
                continue

            # Folder / prefix
            if getattr(obj, "is_dir", False) or rest.endswith("/") or "/" in rest:
                folder = rest.strip("/").split("/", 1)[0]
                if folder:
                    folder_names.add(folder)
                continue

            # File trực tiếp
            file_name = rest.split("/")[-1]
            virtual_key = _to_virtual(default_bucket, bucket, name)

            files.append({
                "object_key": virtual_key,
                "name": file_name,
                "size": getattr(obj, "size", None),
                "etag": getattr(obj, "etag", None),
                "last_modified": obj.last_modified.isoformat() if getattr(obj, "last_modified", None) else None,
                "url": _backend_open_url(request, virtual_key),
            })

        # FALLBACK: một số MinIO/S3 trả delimiter prefixes không ổn định.
        # Nếu list recursive=False ra rỗng nhưng prefix thực sự có dữ liệu, ta list recursive=True và tự suy ra level-1.
        if not folder_names and not files:
            try:
                if prefix_has_anything(client, bucket, prefix):
                    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
                        name = obj.object_name
                        if prefix and name == prefix:
                            continue
                        rest = name[len(prefix):] if prefix else name
                        if not rest:
                            continue
                        if "/" in rest:
                            folder = rest.split("/", 1)[0].strip()
                            if folder:
                                folder_names.add(folder)
                            continue
                        virtual_key = _to_virtual(default_bucket, bucket, name)
                        files.append({
                            "object_key": virtual_key,
                            "name": rest,
                            "size": getattr(obj, "size", None),
                            "etag": getattr(obj, "etag", None),
                            "last_modified": obj.last_modified.isoformat() if getattr(obj, "last_modified", None) else None,
                            "url": _backend_open_url(request, virtual_key),
                        })
            except Exception:
                pass

        for folder in sorted(folder_names, key=lambda x: x.lower()):
            inner_full = f"{key}/{folder}" if key else folder
            folders.append({
                "name": folder,
                "fullPath": _to_virtual(default_bucket, bucket, inner_full),
            })

        files.sort(key=lambda x: x["name"].lower())

        return {
            "bucket": bucket,
            "path": clean_path(path),
            "prefix": prefix,
            "folders": folders,
            "files": files,
            "mode": "single_bucket" if default_bucket else "bucket_per_section",
        }

    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e




# =================== PUT =================== #

@router.put("/folders/", summary="Đổi tên folder")
def rename_folder(body: RenameFolderBody):
    client, default_bucket, _ = _runtime()

    b1, old_rel = _split_virtual(body.old_path, default_bucket, client, allow_empty_key=False)
    b2, new_rel = _split_virtual(body.new_path, default_bucket, client, allow_empty_key=False)

    if b1 != b2:
        raise HTTPException(status_code=400, detail="Không hỗ trợ rename folder giữa 2 bucket khác nhau")

    bucket = b1
    old_path = clean_path(old_rel)
    new_path = clean_path(new_rel)

    if old_path == new_path:
        raise HTTPException(status_code=400, detail="new_path is the same as old_path")

    old_prefix = folder_marker(old_path)
    new_prefix = folder_marker(new_path)

    if new_prefix.startswith(old_prefix):
        raise HTTPException(status_code=400, detail="new_path must not be inside old_path")

    try:
        if not prefix_has_anything(client, bucket, old_prefix):
            raise HTTPException(status_code=404, detail="Folder not found")

        if prefix_has_anything(client, bucket, new_prefix):
            raise HTTPException(status_code=409, detail="Target folder already exists")

        objects = list(client.list_objects(bucket, prefix=old_prefix, recursive=True))

        copied = 0
        to_delete: List[DeleteObject] = []

        for obj in objects:
            old_key = obj.object_name
            suffix = old_key[len(old_prefix):]
            new_key = new_prefix + suffix

            client.copy_object(
                bucket_name=bucket,
                object_name=new_key,
                source=CopySource(bucket, old_key),
            )
            to_delete.append(DeleteObject(old_key))
            copied += 1

        try:
            client.stat_object(bucket, old_prefix)
            client.copy_object(bucket, new_prefix, CopySource(bucket, old_prefix))
            to_delete.append(DeleteObject(old_prefix))
        except S3Error:
            pass

        if to_delete:
            errors = list(client.remove_objects(bucket, to_delete))
            if errors:
                raise HTTPException(status_code=500, detail=f"Delete errors: {[str(e) for e in errors]}")

        return {
            "status": "renamed",
            "bucket": bucket,
            "old_path": _to_virtual(default_bucket, bucket, old_path),
            "new_path": _to_virtual(default_bucket, bucket, new_path),
            "copied_objects": copied,
        }

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e


@router.put("/objects/", summary="Đổi tên file")
def rename_object(request: Request, body: RenameObjectBody):
    client, default_bucket, public_base = _runtime()

    bucket, old_key = _split_virtual(body.object_key, default_bucket, client, allow_empty_key=False)
    old_key = clean_path(old_key)

    new_name = os.path.basename(body.new_name.strip())
    if "/" in body.new_name or "\\" in body.new_name:
        raise HTTPException(status_code=400, detail="new_name must not contain '/' or '\\'")
    if not old_key:
        raise HTTPException(status_code=400, detail="object_key is required")

    parent = old_key.rsplit("/", 1)[0] if "/" in old_key else ""
    new_key = f"{parent}/{new_name}" if parent else new_name

    if new_key == old_key:
        raise HTTPException(status_code=400, detail="New name is the same as current")

    try:
        try:
            client.stat_object(bucket, old_key)
        except S3Error:
            raise HTTPException(status_code=404, detail="Object not found")

        try:
            client.stat_object(bucket, new_key)
            raise HTTPException(status_code=409, detail="Target already exists")
        except S3Error:
            pass

        client.copy_object(bucket, new_key, CopySource(bucket, old_key))
        client.remove_object(bucket, old_key)

        return {
            "status": "renamed",
            "bucket": bucket,
            "old_object_key": _to_virtual(default_bucket, bucket, old_key),
            "new_object_key": _to_virtual(default_bucket, bucket, new_key),
            "url": _backend_open_url(request, _to_virtual(default_bucket, bucket, new_key)),
        }

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e


# =================== POST =================== #

@router.post("/folders", summary="Tạo folder")
def create_folder(body: CreateFolderBody):
    client, default_bucket, _ = _runtime()

    bucket, rel = _split_virtual(body.full_path, default_bucket, client, allow_empty_key=False)
    full_path = clean_path(rel)

    marker = folder_marker(full_path)

    try:
        if prefix_has_anything(client, bucket, marker):
            raise HTTPException(status_code=409, detail="Folder already exists")

        client.put_object(
            bucket,
            marker,
            data=io.BytesIO(b""),
            length=0,
            content_type="application/octet-stream",
        )

        return {
            "status": "created",
            "bucket": bucket,
            "folder": {
                "fullPath": _to_virtual(default_bucket, bucket, full_path),
                "marker": marker
            },
        }

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e


@router.post("/files/", summary="Upload nhiều file vào folder path (upload xong -> sync Mongo -> sync Postgre)")
def upload_files_to_path(
    request: Request,
    path: str = Form(..., description="Ví dụ: images hoặc documents/class-10/tin-hoc/chunk"),
    upload_id: str = Form("", description="ID tiến trình upload để frontend poll"),
    files: List[UploadFile] = File(...),
):
    client, default_bucket, public_base = _runtime()
    actor = _get_actor(request)
    upload_id = (upload_id or "").strip() or uuid.uuid4().hex

    bucket, rel = _split_virtual(path, default_bucket, client, allow_empty_key=True)
    p = clean_path(rel)
    prefix = folder_marker(p)

    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    total_files = len(files)
    _init_upload_progress(upload_id, path=path, total_files=total_files)

    uploaded, failed = [], []
    seen = set()
    dirty_topics: Set[str] = set()
    dirty_subjects: Set[str] = set()

    try:
        for index, f in enumerate(files, start=1):
            current_name = os.path.basename(getattr(f, "filename", "") or "")
            current_virtual_key = _to_virtual(default_bucket, bucket, prefix + current_name) if current_name else ""

            if not f.filename:
                error_msg = "Missing filename"
                failed.append({"filename": None, "error": error_msg})
                _append_upload_error(upload_id, {"filename": None, "error": error_msg})
                _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name="", stage="failed", stage_label="Thiếu tên file", file_percent=1.0, message=error_msg, status="processing")
                continue

            filename = os.path.basename(f.filename)
            object_key = prefix + filename
            current_virtual_key = _to_virtual(default_bucket, bucket, object_key)

            if object_key in seen:
                error_msg = "Duplicate in request batch"
                failed.append({"filename": filename, "object_key": current_virtual_key, "error": error_msg})
                _append_upload_error(upload_id, {"filename": filename, "object_key": current_virtual_key, "error": error_msg})
                _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="failed", stage_label="Trùng file trong batch", file_percent=1.0, message=error_msg, status="processing")
                try:
                    f.file.close()
                except Exception:
                    pass
                continue
            seen.add(object_key)

            try:
                client.stat_object(bucket, object_key)
                error_msg = "Already exists"
                failed.append({"filename": filename, "object_key": current_virtual_key, "error": error_msg})
                _append_upload_error(upload_id, {"filename": filename, "object_key": current_virtual_key, "error": error_msg})
                _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="failed", stage_label="File đã tồn tại", file_percent=1.0, message=error_msg, status="processing")
                try:
                    f.file.close()
                except Exception:
                    pass
                continue
            except S3Error:
                pass

            try:
                _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="uploading_minio", stage_label="Đang upload lên MinIO", file_percent=0.18, message=f"Đang upload {filename} lên MinIO")

                client.put_object(
                    bucket_name=bucket,
                    object_name=object_key,
                    data=f.file,
                    length=-1,
                    part_size=10 * 1024 * 1024,
                    content_type=f.content_type or "application/octet-stream",
                )

                _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="syncing_mongo", stage_label="Đang sync MongoDB", file_percent=0.36, message=f"Đang đồng bộ {filename} vào MongoDB")

                try:
                    sync_res = sync_minio_object_to_mongo(
                        bucket=bucket,
                        object_key=object_key,
                        meta={"__upload_mode": "standard"},
                        actor=actor,
                    )
                except Exception as e:
                    try:
                        client.remove_object(bucket, object_key)
                    except Exception:
                        pass
                    error_msg = f"Mongo sync failed: {e}"
                    failed.append({"filename": filename, "object_key": current_virtual_key, "error": error_msg})
                    _append_upload_error(upload_id, {"filename": filename, "object_key": current_virtual_key, "error": error_msg})
                    _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="syncing_mongo", stage_label="MongoDB lỗi", file_percent=1.0, message=error_msg, status="processing")
                    continue

                _set_hierarchy_sync_status(
                    sync_res,
                    minio_ok=True,
                    mongo_ok=True,
                    postgre_ok=False,
                    neo4j_ok=False,
                    actor=actor,
                    error="Đã sync MongoDB, đang chờ PostgreSQL/Neo4j",
                )

                _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="queueing_hierarchy", stage_label="Ghi nhận cập nhật keyword cha", file_percent=0.54, message=f"Đã ghi nhận {filename}, sẽ cập nhật topic/subject ở cuối batch")

                sync_topic_map = str(sync_res.topic_map or "").strip()
                sync_subject_map = str(sync_res.subject_map or "").strip()
                if sync_topic_map:
                    dirty_topics.add(sync_topic_map)
                if sync_subject_map:
                    dirty_subjects.add(sync_subject_map)

                _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="syncing_postgre", stage_label="Đang import PostgreSQL", file_percent=0.72, message=f"Đang sync {filename} vào PostgreSQL")

                try:
                    pg_ids = sync_postgre_from_mongo_auto_ids(
                        class_map=sync_res.class_map,
                        subject_map=sync_res.subject_map,
                        topic_map=sync_res.topic_map or "",
                        lesson_map=sync_res.lesson_map or "",
                        chunk_map=sync_res.chunk_map or "",
                    )
                except Exception as e:
                    _set_hierarchy_sync_status(
                        sync_res,
                        minio_ok=True,
                        mongo_ok=True,
                        postgre_ok=False,
                        neo4j_ok=False,
                        actor=actor,
                        error=f"Postgre sync failed: {e}",
                    )
                    try:
                        client.remove_object(bucket, object_key)
                    except Exception:
                        pass
                    _hide_mongo_chunk(str(sync_res.chunk_id), actor=actor)
                    error_msg = f"Postgre sync failed: {e}"
                    failed.append({"filename": filename, "object_key": current_virtual_key, "error": error_msg})
                    _append_upload_error(upload_id, {"filename": filename, "object_key": current_virtual_key, "error": error_msg})
                    _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="syncing_postgre", stage_label="PostgreSQL lỗi", file_percent=1.0, message=error_msg, status="processing")
                    continue

                _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="syncing_neo4j", stage_label="Đang sync Neo4j", file_percent=0.88, message=f"Đang đồng bộ {filename} sang Neo4j")

                try:
                    sync_neo4j_from_maps_and_pg_ids(
                        class_map=sync_res.class_map,
                        subject_map=sync_res.subject_map,
                        topic_map=sync_res.topic_map or "",
                        lesson_map=sync_res.lesson_map or "",
                        chunk_map=sync_res.chunk_map or "",
                        pg_ids=pg_ids,
                        actor=actor,
                    )
                except Exception as e:
                    _set_hierarchy_sync_status(
                        sync_res,
                        minio_ok=True,
                        mongo_ok=True,
                        postgre_ok=True,
                        neo4j_ok=False,
                        actor=actor,
                        error=f"Neo4j sync failed: {e}",
                    )
                    error_msg = f"Neo4j sync failed: {e}"
                    failed.append({"filename": filename, "object_key": current_virtual_key, "error": error_msg})
                    _append_upload_error(upload_id, {"filename": filename, "object_key": current_virtual_key, "error": error_msg})
                    _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="syncing_neo4j", stage_label="Neo4j lỗi", file_percent=1.0, message=error_msg, status="processing")
                    continue

                _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="verifying", stage_label="Đang kiểm tra đồng bộ", file_percent=0.96, message=f"Đang kiểm tra 4 hệ cho {filename}")

                sync_status = _verify_hierarchy_sync(
                    client=client,
                    bucket=bucket,
                    object_key=object_key,
                    sync_res=sync_res,
                    pg_ids=pg_ids,
                )
                if not sync_status.get("isFullySynced"):
                    msg = _sync_error_message(sync_status)
                    _set_hierarchy_sync_status(
                        sync_res,
                        minio_ok=bool(sync_status.get("minio")),
                        mongo_ok=bool(sync_status.get("mongo")),
                        postgre_ok=bool(sync_status.get("postgre")),
                        neo4j_ok=bool(sync_status.get("neo4j")),
                        actor=actor,
                        error=msg,
                        verify=sync_status,
                    )
                    failed.append({"filename": filename, "object_key": current_virtual_key, "error": msg})
                    _append_upload_error(upload_id, {"filename": filename, "object_key": current_virtual_key, "error": msg})
                    _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="verifying", stage_label="Kiểm tra chưa đạt", file_percent=1.0, message=msg, status="processing")
                    continue

                _set_hierarchy_sync_status(
                    sync_res,
                    minio_ok=True,
                    mongo_ok=True,
                    postgre_ok=True,
                    neo4j_ok=True,
                    actor=actor,
                    error=None,
                    verify=sync_status,
                )

                uploaded.append({
                    "filename": filename,
                    "object_key": current_virtual_key,
                    "etag": getattr(getattr(client, 'stat_object', lambda *_args, **_kwargs: None)(bucket, object_key), 'etag', None),
                    "url": _backend_open_url(request, current_virtual_key),
                    "syncStatus": sync_status,
                })

                _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="completed", stage_label="Đã hoàn tất file", file_percent=1.0, message=f"Đã xử lý xong {filename}", completed_files=len(uploaded))
            except HTTPException:
                raise
            except S3Error as e:
                error_msg = f"MinIO error: {e}"
                failed.append({"filename": filename, "object_key": current_virtual_key, "error": error_msg})
                _append_upload_error(upload_id, {"filename": filename, "object_key": current_virtual_key, "error": error_msg})
                _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="failed", stage_label="MinIO lỗi", file_percent=1.0, message=error_msg, status="processing")
            finally:
                try:
                    f.file.close()
                except Exception:
                    pass

        final_hierarchy = {"topics": [], "subjects": []}
        if uploaded and (dirty_topics or dirty_subjects):
            final_hierarchy = _finalize_standard_upload_batch(
                upload_id=upload_id,
                actor=actor,
                dirty_topics=dirty_topics,
                dirty_subjects=dirty_subjects,
            )

        status = "completed" if not failed else ("failed" if not uploaded else "completed_with_errors")
        message = "Hoàn tất toàn bộ" if status == "completed" else ("Tất cả file đều lỗi" if status == "failed" else "Hoàn tất nhưng có file lỗi")
        _finish_upload_progress(upload_id, total_files=total_files, completed_files=len(uploaded), status=status, message=message, stage="completed", stage_label="Hoàn tất")

        return {
            "status": "done",
            "bucket": bucket,
            "path": _to_virtual(default_bucket, bucket, p) if p else (bucket if not default_bucket else ""),
            "uploaded": uploaded,
            "failed": failed,
            "uploaded_count": len(uploaded),
            "failed_count": len(failed),
            "upload_id": upload_id,
            "finalHierarchy": final_hierarchy,
        }

    except HTTPException:
        raise
    except S3Error as e:
        _finish_upload_progress(upload_id, total_files=total_files, completed_files=len(uploaded), status="failed", message=f"MinIO error: {e}", stage="failed", stage_label="Lỗi")
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e


@router.post("/objects/", summary="Insert 1 item (có thể có file hoặc không) - insert xong -> sync Mongo -> sync Postgre")
def insert_item(
    request: Request,
    path: str = Form(...),
    upload_id: str = Form("", description="ID tiến trình upload để frontend poll"),
    name: str = Form("", description="Tên file nếu không upload file"),
    meta_json: str = Form("", description="JSON string metadata (tuỳ chọn)"),
    file: UploadFile | None = File(None),
):
    client, default_bucket, public_base = _runtime()
    actor = _get_actor(request)
    upload_id = (upload_id or "").strip() or uuid.uuid4().hex
    meta = _parse_meta_json(meta_json)
    resolved_virtual_path = _remap_virtual_path_by_meta(path, meta)
    category = _infer_category_from_path(resolved_virtual_path or path)

    bucket, rel = _split_virtual(resolved_virtual_path or path, default_bucket, client, allow_empty_key=True)
    p = clean_path(rel)
    prefix = folder_marker(p)

    if file and file.filename:
        filename = os.path.basename(file.filename)
    else:
        filename = (name or "").strip() or f"item-{int(time.time())}.txt"
        if "/" in filename or "\\" in filename:
            raise HTTPException(status_code=400, detail="name must not contain '/' or '\\'")


    object_key = prefix + filename
    virtual_object_key = _to_virtual(default_bucket, bucket, object_key)
    temp_file_path = ""

    _init_upload_progress(upload_id, path=resolved_virtual_path or path, total_files=1)
    _mark_file_progress(upload_id, file_index=1, total_files=1, file_name=filename, stage="preparing", stage_label="Đang chuẩn bị xử lý", file_percent=0.12, message=f"Đang chuẩn bị xử lý {filename}")

    try:
        client.stat_object(bucket, object_key)
        _append_upload_error(upload_id, {"filename": filename, "object_key": virtual_object_key, "error": "Object already exists"})
        _finish_upload_progress(upload_id, total_files=1, completed_files=0, status="failed", message="Object already exists", stage="failed", stage_label="File đã tồn tại")
        raise HTTPException(status_code=409, detail="Object already exists")
    except S3Error:
        pass

    try:
        _ensure_folder_markers(client, bucket, p)

        _mark_file_progress(upload_id, file_index=1, total_files=1, file_name=filename, stage="uploading_minio", stage_label="Đang upload lên MinIO", file_percent=0.20, message=f"Đang upload {filename} lên MinIO")

        if file:
            file_bytes = file.file.read()
            suffix = os.path.splitext(filename)[1] or ""
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
            try:
                tmp.write(file_bytes)
                tmp.flush()
                temp_file_path = tmp.name
            finally:
                tmp.close()

            client.put_object(
                bucket,
                object_key,
                data=io.BytesIO(file_bytes),
                length=len(file_bytes),
                part_size=10 * 1024 * 1024,
                content_type=file.content_type or "application/octet-stream",
            )
        else:
            client.put_object(
                bucket,
                object_key,
                data=io.BytesIO(b""),
                length=0,
                content_type="text/plain",
            )

        if category in ("image", "video"):
            media_desc_key = "imgDescription" if category == "image" else "videoDescription"
            media_ai_info: Dict[str, Any] = {"generated": False, "meta": {"mode": "manual"}}

            if not str(meta.get(media_desc_key) or meta.get("description") or "").strip():
                _mark_file_progress(
                    upload_id,
                    file_index=1,
                    total_files=1,
                    file_name=filename,
                    stage="describing_media",
                    stage_label="Đang tự lấy mô tả media",
                    file_percent=0.32,
                    message=f"Đang sinh mô tả cho {filename}",
                )

                generated_desc, generated_meta = generate_media_description(
                    media_type=category,
                    file_path=temp_file_path,
                    file_name=filename,
                    explicit_description=str(meta.get(media_desc_key) or meta.get("description") or ""),
                    follow_type=str(meta.get("folderType") or ""),
                    map_id=str(meta.get("mapID") or ""),
                )
                media_ai_info = {"generated": bool(generated_desc), "meta": generated_meta}

                if generated_desc:
                    meta = {**meta, media_desc_key: generated_desc, "description": generated_desc}

            _mark_file_progress(upload_id, file_index=1, total_files=1, file_name=filename, stage="syncing_mongo", stage_label="Đang sync MongoDB", file_percent=0.40, message=f"Đang đồng bộ media {filename} vào MongoDB")
            try:
                media_res = sync_minio_media_to_mongo(
                    bucket=bucket,
                    object_key=object_key,
                    meta={**meta, "category": category, "name": name or meta.get("name", "")},
                    actor=actor,
                )
            except Exception as e:
                try:
                    client.remove_object(bucket, object_key)
                except Exception:
                    pass
                _append_upload_error(upload_id, {"filename": filename, "object_key": virtual_object_key, "error": f"Mongo media sync failed: {e}"})
                _finish_upload_progress(upload_id, total_files=1, completed_files=0, status="failed", message=f"Mongo media sync failed: {e}", stage="failed", stage_label="MongoDB lỗi")
                raise HTTPException(status_code=500, detail=f"Mongo media sync failed: {e}") from e

            _mark_file_progress(upload_id, file_index=1, total_files=1, file_name=filename, stage="syncing_postgre", stage_label="Đang import PostgreSQL", file_percent=0.72, message=f"Đang sync media {filename} vào PostgreSQL")
            try:
                pg_media = sync_postgre_media_from_mongo(
                    media_type=media_res.media_type,
                    mongo_id=str(media_res.mongo_id),
                )
            except Exception as e:
                try:
                    client.remove_object(bucket, object_key)
                except Exception:
                    pass
                _hide_mongo_media(media_res.collection, str(media_res.mongo_id), actor=actor)
                _append_upload_error(upload_id, {"filename": filename, "object_key": virtual_object_key, "error": f"Postgre media sync failed: {e}"})
                _finish_upload_progress(upload_id, total_files=1, completed_files=0, status="failed", message=f"Postgre media sync failed: {e}", stage="failed", stage_label="PostgreSQL lỗi")
                raise HTTPException(status_code=500, detail=f"Postgre media sync failed: {e}") from e

            _mark_file_progress(upload_id, file_index=1, total_files=1, file_name=filename, stage="syncing_neo4j", stage_label="Đang sync Neo4j", file_percent=0.88, message=f"Đang đồng bộ media {filename} sang Neo4j")
            neo_info: Dict[str, Any] = {"synced": False, "error": None}
            try:
                neo_res = sync_media_to_neo4j(
                    media_type=pg_media.media_type,
                    media_id=pg_media.media_id,
                    mongo_id=pg_media.mongo_id,
                    follow_id=pg_media.follow_id,
                    follow_type=pg_media.follow_type,
                )
                neo_info = {
                    "synced": bool(getattr(neo_res, "ok", True)),
                    "createdOrUpdated": getattr(neo_res, "created_or_updated", {}),
                    "error": None,
                }
            except Exception as e:
                neo_info = {"synced": False, "error": str(e)}

            if not neo_info.get("synced"):
                _append_upload_error(upload_id, {"filename": filename, "object_key": virtual_object_key, "error": neo_info.get("error") or "Neo4j media sync failed"})
                _finish_upload_progress(upload_id, total_files=1, completed_files=0, status="failed", message=neo_info.get("error") or "Neo4j media sync failed", stage="failed", stage_label="Neo4j lỗi")
                raise HTTPException(status_code=500, detail=neo_info.get("error") or "Neo4j media sync failed")

            _mark_file_progress(upload_id, file_index=1, total_files=1, file_name=filename, stage="verifying", stage_label="Đang kiểm tra đồng bộ", file_percent=0.96, message=f"Đang hoàn tất media {filename}")
            _finish_upload_progress(upload_id, total_files=1, completed_files=1, status="completed", message="Hoàn tất", stage="completed", stage_label="Hoàn tất")

            return {
                "status": "inserted",
                "bucket": bucket,
                "requested_path": clean_path(path),
                "path": _to_virtual(default_bucket, bucket, p) if p else (bucket if not default_bucket else ""),
                "object_key": virtual_object_key,
                "url": _backend_open_url(request, virtual_object_key),
                "meta_json": meta_json or "",
                "mongo": {
                    "collection": media_res.collection,
                    "mediaType": media_res.media_type,
                    "mongoId": str(media_res.mongo_id),
                    "mapID": media_res.map_id,
                    "followMap": media_res.follow_map,
                    "followType": media_res.follow_type,
                },
                "postgre": {
                    "mediaId": pg_media.media_id,
                    "mediaName": pg_media.media_name,
                    "mongoId": pg_media.mongo_id,
                    "followId": pg_media.follow_id,
                    "followType": pg_media.follow_type,
                },
                "neo4j": neo_info,
                "mediaDescription": str(meta.get(media_desc_key) or meta.get("description") or ""),
                "mediaAi": media_ai_info,
                "upload_id": upload_id,
            }

        _mark_file_progress(upload_id, file_index=1, total_files=1, file_name=filename, stage="syncing_mongo", stage_label="Đang sync MongoDB", file_percent=0.40, message=f"Đang đồng bộ {filename} vào MongoDB")
        try:
            sync_res = sync_minio_object_to_mongo(
                bucket=bucket,
                object_key=object_key,
                meta={**meta, "__local_file_path": temp_file_path, "__upload_mode": "manual"},
                actor=actor,
            )
        except Exception as e:
            try:
                client.remove_object(bucket, object_key)
            except Exception:
                pass
            _append_upload_error(upload_id, {"filename": filename, "object_key": virtual_object_key, "error": f"Mongo sync failed: {e}"})
            _finish_upload_progress(upload_id, total_files=1, completed_files=0, status="failed", message=f"Mongo sync failed: {e}", stage="failed", stage_label="MongoDB lỗi")
            raise HTTPException(status_code=500, detail=f"Mongo sync failed: {e}") from e

        _set_hierarchy_sync_status(
            sync_res,
            minio_ok=True,
            mongo_ok=True,
            postgre_ok=False,
            neo4j_ok=False,
            actor=actor,
            error="Đã sync MongoDB, đang chờ PostgreSQL/Neo4j",
        )

        hierarchy_info: Dict[str, Any] = {"updated": False, "details": None, "error": None}
        _mark_file_progress(upload_id, file_index=1, total_files=1, file_name=filename, stage="updating_hierarchy", stage_label="Đang cập nhật keyword cha", file_percent=0.56, message=f"Đang cập nhật keyword cha cho {filename}")
        try:
            hierarchy_details = _refresh_standard_hierarchy_keywords(
                subject_map=sync_res.subject_map,
                topic_map=sync_res.topic_map or "",
                lesson_map=sync_res.lesson_map or "",
            )
            hierarchy_info = {"updated": True, "details": hierarchy_details, "error": None}
        except Exception as e:
            hierarchy_info = {"updated": False, "details": None, "error": str(e)}

        _mark_file_progress(upload_id, file_index=1, total_files=1, file_name=filename, stage="syncing_postgre", stage_label="Đang import PostgreSQL", file_percent=0.74, message=f"Đang sync {filename} vào PostgreSQL")
        try:
            pg_ids = sync_postgre_from_mongo_auto_ids(
                class_map=sync_res.class_map,
                subject_map=sync_res.subject_map,
                topic_map=sync_res.topic_map or "",
                lesson_map=sync_res.lesson_map or "",
                chunk_map=sync_res.chunk_map or "",
            )
        except Exception as e:
            _set_hierarchy_sync_status(
                sync_res,
                minio_ok=True,
                mongo_ok=True,
                postgre_ok=False,
                neo4j_ok=False,
                actor=actor,
                error=f"Postgre sync failed: {e}",
            )
            try:
                client.remove_object(bucket, object_key)
            except Exception:
                pass
            if sync_res.chunk_map:
                _hide_mongo_chunk_by_map(sync_res.chunk_map, actor=actor)
            _append_upload_error(upload_id, {"filename": filename, "object_key": virtual_object_key, "error": f"Postgre sync failed: {e}"})
            _finish_upload_progress(upload_id, total_files=1, completed_files=0, status="failed", message=f"Postgre sync failed: {e}", stage="failed", stage_label="PostgreSQL lỗi")
            raise HTTPException(status_code=500, detail=f"Postgre sync failed: {e}") from e

        _mark_file_progress(upload_id, file_index=1, total_files=1, file_name=filename, stage="syncing_neo4j", stage_label="Đang sync Neo4j", file_percent=0.90, message=f"Đang đồng bộ {filename} sang Neo4j")
        neo_info: Dict[str, Any] = {"synced": False, "error": None}
        try:
            neo_res = sync_neo4j_from_maps_and_pg_ids(
                class_map=sync_res.class_map,
                subject_map=sync_res.subject_map,
                topic_map=sync_res.topic_map or "",
                lesson_map=sync_res.lesson_map or "",
                chunk_map=sync_res.chunk_map or "",
                pg_ids=pg_ids,
                actor=actor,
            )
            neo_info = {
                "synced": bool(getattr(neo_res, "ok", True)),
                "createdOrUpdated": getattr(neo_res, "created_or_updated", {}),
                "keywordCount": getattr(neo_res, "keyword_count", 0),
                "error": None,
            }
        except Exception as e:
            neo_info = {"synced": False, "error": str(e)}

        _mark_file_progress(upload_id, file_index=1, total_files=1, file_name=filename, stage="verifying", stage_label="Đang kiểm tra đồng bộ", file_percent=0.97, message=f"Đang kiểm tra 4 hệ cho {filename}")
        sync_status = _verify_hierarchy_sync(
            client=client,
            bucket=bucket,
            object_key=object_key,
            sync_res=sync_res,
            pg_ids=pg_ids,
        )
        if not sync_status.get("isFullySynced"):
            msg = _sync_error_message(sync_status)
            _set_hierarchy_sync_status(
                sync_res,
                minio_ok=bool(sync_status.get("minio")),
                mongo_ok=bool(sync_status.get("mongo")),
                postgre_ok=bool(sync_status.get("postgre")),
                neo4j_ok=bool(sync_status.get("neo4j")),
                actor=actor,
                error=msg,
                verify=sync_status,
            )
            _append_upload_error(upload_id, {"filename": filename, "object_key": virtual_object_key, "error": msg})
            _finish_upload_progress(upload_id, total_files=1, completed_files=0, status="failed", message=msg, stage="failed", stage_label="Kiểm tra chưa đạt")
            raise HTTPException(
                status_code=500,
                detail={
                    "message": msg,
                    "mongo": {
                        "folderType": sync_res.folder_type,
                        "classMap": sync_res.class_map,
                        "subjectMap": sync_res.subject_map,
                        "topicMap": sync_res.topic_map,
                        "lessonMap": sync_res.lesson_map,
                        "chunkMap": sync_res.chunk_map,
                    },
                    "postgre": {
                        "classId": pg_ids.class_id,
                        "subjectId": pg_ids.subject_id,
                        "topicId": pg_ids.topic_id,
                        "lessonId": pg_ids.lesson_id,
                        "chunkId": pg_ids.chunk_id,
                    },
                    "neo4j": neo_info,
                    "syncStatus": sync_status,
                },
            )

        _set_hierarchy_sync_status(
            sync_res,
            minio_ok=True,
            mongo_ok=True,
            postgre_ok=True,
            neo4j_ok=True,
            actor=actor,
            error=None,
            verify=sync_status,
        )
        _finish_upload_progress(upload_id, total_files=1, completed_files=1, status="completed", message="Hoàn tất", stage="completed", stage_label="Hoàn tất")

        return {
            "status": "inserted",
            "bucket": bucket,
            "requested_path": clean_path(path),
            "path": _to_virtual(default_bucket, bucket, p) if p else (bucket if not default_bucket else ""),
            "object_key": virtual_object_key,
            "url": _backend_open_url(request, virtual_object_key),
            "meta_json": meta_json or "",
            "mongo": {
                "folderType": sync_res.folder_type,
                "classMap": sync_res.class_map,
                "subjectMap": sync_res.subject_map,
                "topicMap": sync_res.topic_map,
                "lessonMap": sync_res.lesson_map,
                "chunkMap": sync_res.chunk_map,
                "classId": str(sync_res.class_id),
                "subjectId": str(sync_res.subject_id),
                "topicId": str(sync_res.topic_id) if sync_res.topic_id else None,
                "lessonId": str(sync_res.lesson_id) if sync_res.lesson_id else None,
                "chunkId": str(sync_res.chunk_id) if sync_res.chunk_id else None,
            },
            "searchHierarchy": hierarchy_info,
            "postgre": {
                "classId": pg_ids.class_id,
                "subjectId": pg_ids.subject_id,
                "topicId": pg_ids.topic_id,
                "lessonId": pg_ids.lesson_id,
                "chunkId": pg_ids.chunk_id,
                "keywordIds": pg_ids.keyword_ids,
            },
            "neo4j": neo_info,
            "syncStatus": sync_status,
            "upload_id": upload_id,
        }

    except HTTPException:
        raise
    except S3Error as e:
        _append_upload_error(upload_id, {"filename": filename, "object_key": virtual_object_key, "error": f"MinIO error: {e}"})
        _finish_upload_progress(upload_id, total_files=1, completed_files=0, status="failed", message=f"MinIO error: {e}", stage="failed", stage_label="MinIO lỗi")
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e
    finally:
        if temp_file_path:
            try:
                os.remove(temp_file_path)
            except Exception:
                pass
        if file:
            try:
                file.file.close()
            except Exception:
                pass


# =================== DELETE =================== #

@router.delete("/folders", summary="Xoá folder (cascade)")
def delete_folder(path: str = Query(..., min_length=1, description="full path folder, ví dụ documents/class-10")):
    client, default_bucket, _ = _runtime()

    bucket, rel = _split_virtual(path, default_bucket, client, allow_empty_key=False)
    p = clean_path(rel)
    prefix = folder_marker(p)

    try:
        if not prefix_has_anything(client, bucket, prefix):
            raise HTTPException(status_code=404, detail="Folder not found")

        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        to_delete = [DeleteObject(obj.object_name) for obj in objects]
        to_delete.append(DeleteObject(prefix))

        errors = list(client.remove_objects(bucket, to_delete))
        if errors:
            raise HTTPException(status_code=500, detail=f"Delete errors: {[str(e) for e in errors]}")

        return {
            "status": "deleted",
            "bucket": bucket,
            "path": _to_virtual(default_bucket, bucket, p),
        }

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e


@router.delete("/files", summary="Xoá 1 file")
def delete_object(object_key: str = Query(..., min_length=1)):
    client, default_bucket, _ = _runtime()

    bucket, key = _split_virtual(object_key, default_bucket, client, allow_empty_key=False)
    key = clean_path(key)

    try:
        try:
            client.stat_object(bucket, key)
        except S3Error:
            raise HTTPException(status_code=404, detail="Object not found")

        client.remove_object(bucket, key)

        return {
            "status": "deleted",
            "bucket": bucket,
            "object_key": _to_virtual(default_bucket, bucket, key),
        }

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e


# =================== AUTO UPLOAD (subject/topic -> lesson) =================== #

def _extract_class_number_from_virtual_path(path: str) -> int:
    m = __import__("re").search(r"(?:^|/)class-(\d+)(?:/|$)", clean_path(path), flags=__import__("re").I)
    if not m:
        raise HTTPException(status_code=400, detail="Không xác định được lớp từ đường dẫn hiện tại")
    return int(m.group(1))


def _resolve_subject_map_from_variant(class_number: int, book_variant: str) -> str:
    if class_number == 10:
        return "TH10"
    variant = (book_variant or "").strip().upper()
    if class_number in (11, 12) and variant in {"UD", "KHMT"}:
        return f"TH{class_number}-{variant}"
    raise HTTPException(status_code=400, detail="Hãy chọn đúng loại sách UD hoặc KHMT")


def _auto_root_for_class(path: str) -> str:
    parts = [p for p in clean_path(path).split("/") if p]
    if len(parts) < 2:
        raise HTTPException(status_code=400, detail="Đường dẫn hiện tại không hợp lệ")
    return "/".join(parts[:2])


def _auto_plural_folder(folder_type: str) -> str:
    t = _normalize_folder_type(folder_type)
    return {"subject": "subjects", "topic": "topics", "lesson": "lessons", "chunk": "chunks"}.get(t, f"{t}s")


def _slug_filename(text: str, fallback: str) -> str:
    import re as _re
    base = _re.sub(r"[^A-Za-z0-9._-]+", "_", (text or "").strip()).strip("_")
    return base or fallback


def _next_topic_number(subject_map: str) -> int:
    try:
        db = get_mongo_client()["db"]
        rows = list(db["topics"].find({"subjectID": subject_map}, {"topicNumber": 1}))
        nums = [int(row.get("topicNumber")) for row in rows if str(row.get("topicNumber") or "").isdigit()]
        return (max(nums) + 1) if nums else 1
    except Exception:
        return 1


def _next_lesson_number_for_topic(topic_map: str) -> int:
    try:
        db = get_mongo_client()["db"]
        rows = list(db["lessons"].find({"topicID": topic_map}, {"lessonNumber": 1}))
        nums = [int(row.get("lessonNumber")) for row in rows if str(row.get("lessonNumber") or "").isdigit()]
        return (max(nums) + 1) if nums else 1
    except Exception:
        return 1


def _put_local_pdf_to_minio(client, bucket: str, object_key: str, local_file_path: str) -> None:
    file_size = os.path.getsize(local_file_path)
    with open(local_file_path, "rb") as fh:
        client.put_object(
            bucket_name=bucket,
            object_name=object_key,
            data=fh,
            length=file_size,
            content_type="application/pdf",
        )


def _sync_local_document_to_system(
    *,
    request: Request,
    actor: str,
    upload_id: str,
    file_index: int,
    total_files: int,
    virtual_folder_path: str,
    local_file_path: str,
    target_filename: str,
    meta: Dict[str, Any],
    skip_hierarchy_rebuild: bool = False,
) -> Dict[str, Any]:
    client, default_bucket, _public_base = _runtime()
    bucket, rel = _split_virtual(virtual_folder_path, default_bucket, client, allow_empty_key=True)
    prefix = folder_marker(clean_path(rel))
    object_key = prefix + target_filename
    virtual_object_key = _to_virtual(default_bucket, bucket, object_key)

    try:
        client.stat_object(bucket, object_key)
        raise HTTPException(status_code=400, detail=f"File đã tồn tại: {virtual_object_key}")
    except S3Error:
        pass

    _mark_file_progress(upload_id, file_index=file_index, total_files=total_files, file_name=target_filename, stage="uploading_minio", stage_label="Đang upload lên MinIO", file_percent=0.15, message=f"Đang upload {target_filename} lên MinIO")
    _put_local_pdf_to_minio(client, bucket, object_key, local_file_path)

    _mark_file_progress(upload_id, file_index=file_index, total_files=total_files, file_name=target_filename, stage="syncing_mongo", stage_label="Đang sync MongoDB", file_percent=0.35, message=f"Đang đồng bộ {target_filename} vào MongoDB")
    sync_res = sync_minio_object_to_mongo(
        bucket=bucket,
        object_key=object_key,
        meta={**meta, "__local_file_path": local_file_path, "__upload_mode": "auto"},
        actor=actor,
    )
    _set_hierarchy_sync_status(sync_res, minio_ok=True, mongo_ok=True, postgre_ok=False, neo4j_ok=False, actor=actor, error="Đã sync MongoDB, đang chờ PostgreSQL/Neo4j")

    hierarchy_details = None
    if not skip_hierarchy_rebuild:
        _mark_file_progress(upload_id, file_index=file_index, total_files=total_files, file_name=target_filename, stage="rebuilding_hierarchy", stage_label="Đang cập nhật mô tả / keyword cha", file_percent=0.58, message=f"Đang cập nhật mô tả và keyword cha cho {target_filename}")
        try:
            hierarchy_details = rebuild_hierarchy_descriptions_and_keywords(
                subject_map=sync_res.subject_map,
                topic_map=sync_res.topic_map or "",
                lesson_map=sync_res.lesson_map or "",
                chunk_map=sync_res.chunk_map or "",
            )
        except Exception:
            hierarchy_details = None
    else:
        hierarchy_details = {"skipped": True, "reason": "approve_final_rebuild"}

    _mark_file_progress(upload_id, file_index=file_index, total_files=total_files, file_name=target_filename, stage="syncing_postgre", stage_label="Đang import PostgreSQL", file_percent=0.76, message=f"Đang sync {target_filename} vào PostgreSQL")
    pg_ids = sync_postgre_from_mongo_auto_ids(
        class_map=sync_res.class_map,
        subject_map=sync_res.subject_map,
        topic_map=sync_res.topic_map or "",
        lesson_map=sync_res.lesson_map or "",
        chunk_map=sync_res.chunk_map or "",
    )

    _mark_file_progress(upload_id, file_index=file_index, total_files=total_files, file_name=target_filename, stage="syncing_neo4j", stage_label="Đang sync Neo4j", file_percent=0.9, message=f"Đang đồng bộ {target_filename} sang Neo4j")
    neo_res = sync_neo4j_from_maps_and_pg_ids(
        class_map=sync_res.class_map,
        subject_map=sync_res.subject_map,
        topic_map=sync_res.topic_map or "",
        lesson_map=sync_res.lesson_map or "",
        chunk_map=sync_res.chunk_map or "",
        pg_ids=pg_ids,
    )
    verify = _verify_hierarchy_sync(client=client, bucket=bucket, object_key=object_key, sync_res=sync_res, pg_ids=pg_ids)
    verify["neo4j"] = bool(getattr(neo_res, "ok", True))
    verify["isFullySynced"] = bool(verify.get("minio") and verify.get("mongo") and verify.get("postgre") and verify.get("neo4j"))
    verify["missing"] = verify.get("missing") or []
    err_msg = None if verify["isFullySynced"] else _sync_status_message(verify)
    _set_hierarchy_sync_status(
        sync_res,
        minio_ok=verify.get("minio", False),
        mongo_ok=verify.get("mongo", False),
        postgre_ok=verify.get("postgre", False),
        neo4j_ok=verify.get("neo4j", False),
        actor=actor,
        error=err_msg,
        verify=verify,
    )
    _mark_file_progress(upload_id, file_index=file_index, total_files=total_files, file_name=target_filename, stage="completed_file", stage_label="Hoàn tất file", file_percent=1.0, message=f"Đã xong {target_filename}", completed_files=file_index)
    return {
        "filename": target_filename,
        "virtualObjectKey": virtual_object_key,
        "syncResult": sync_res,
        "postgre": pg_ids,
        "neo4j": getattr(neo_res, "created_or_updated", {}),
        "hierarchy": hierarchy_details,
        "verify": verify,
    }


def _analysis_progress_to_upload(upload_id: str, path: str, *, analysis_weight: float = 24.0):
    def _cb(stage: str, stage_label: str, percent: float, message: str) -> None:
        p = max(0.0, min(100.0, float(percent)))
        _update_upload_progress(
            upload_id,
            path=path,
            status="processing",
            stage=stage,
            stageLabel=stage_label,
            message=message or stage_label,
            percent=max(1, min(30, round(p * analysis_weight / 100.0))),
        )
    return _cb




def _guess_media_type(file_path: str) -> str:
    suffix = Path(file_path or "").suffix.lower()
    if suffix == ".pdf":
        return "application/pdf"
    if suffix in {".png", ".jpg", ".jpeg", ".webp"}:
        return f"image/{suffix[1:] if suffix != '.jpg' else 'jpeg'}"
    return "application/octet-stream"


def _split_single_pdf(src_pdf: str, start: int, end: int, out_path: str) -> str:
    reader = PdfReader(src_pdf)
    total_pages = len(reader.pages)
    s = max(1, min(int(start), total_pages))
    e = max(s, min(int(end), total_pages))
    writer = PdfWriter()
    for idx in range(s - 1, e):
        writer.add_page(reader.pages[idx])
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, 'wb') as fh:
        writer.write(fh)
    return str(out)


def _build_review_items_from_split(session_id: str, split_result: Dict[str, Any], source_pdf: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    source_total_pages = len(PdfReader(source_pdf).pages) if source_pdf and os.path.exists(source_pdf) else 1
    topic_items = list(split_result.get('topics') or ([] if split_result.get('mode') != 'topic' else [split_result.get('topic') or {}]))
    topic_map: Dict[Tuple[int, str], str] = {}

    for idx, topic in enumerate(topic_items, start=1):
        if not isinstance(topic, dict) or not topic:
            continue
        review_id = f"topic_{idx:04d}"
        topic_number = topic.get('number') if isinstance(topic.get('number'), int) else _extract_heading_number(str(topic.get('heading') or ''))
        item = {
            'reviewId': review_id,
            'kind': 'topic',
            'name': _clean(topic.get('name')) or f'topic_{idx:02d}',
            'start': int(topic.get('start') or 1),
            'end': int(topic.get('end') or topic.get('start') or 1),
            'heading': _clean(topic.get('heading')),
            'title': _clean(topic.get('title')),
            'number': topic_number,
            'contentHead': False,
            'confidence': 'high',
            'confidenceScore': 0.95,
            'confidenceReason': '',
            'filePath': _clean(topic.get('file_path')),
            'metaPath': _clean(topic.get('meta_path')),
            'sourcePath': source_pdf,
            'contextPath': source_pdf,
            'currentPath': _clean(topic.get('file_path')) or source_pdf,
            'totalPages': source_total_pages,
        }
        items.append(item)
        topic_map[(item['start'], item['end'])] = review_id

    lessons = list(split_result.get('lessons') or [])
    lesson_review_map: Dict[str, str] = {}
    for idx, lesson in enumerate(lessons, start=1):
        review_id = f"lesson_{idx:04d}"
        topic_review_id = ''
        for t in items:
            if t['kind'] == 'topic' and int(t['start']) <= int(lesson.get('start') or 0) <= int(t['end']):
                topic_review_id = t['reviewId']
                break
        item = {
            'reviewId': review_id,
            'kind': 'lesson',
            'name': _clean(lesson.get('name')) or f'lesson_{idx:02d}',
            'start': int(lesson.get('start') or 1),
            'end': int(lesson.get('end') or lesson.get('start') or 1),
            'heading': _clean(lesson.get('heading')),
            'title': _clean(lesson.get('title')),
            'number': lesson.get('number') if isinstance(lesson.get('number'), int) else _extract_heading_number(str(lesson.get('heading') or '')),
            'contentHead': False,
            'topicReviewId': topic_review_id,
            'topicNumber': lesson.get('topic_number') if isinstance(lesson.get('topic_number'), int) else None,
            'confidence': 'high',
            'confidenceScore': 0.95,
            'confidenceReason': '',
            'filePath': _clean(lesson.get('file_path')),
            'metaPath': _clean(lesson.get('meta_path')),
            'sourcePath': source_pdf,
            'contextPath': _clean(lesson.get('file_path')) if split_result.get('mode') == 'lesson' else (next((t['filePath'] for t in items if t['reviewId'] == topic_review_id and _clean(t.get('filePath'))), source_pdf)),
            'currentPath': _clean(lesson.get('file_path')),
            'totalPages': source_total_pages,
        }
        items.append(item)
        lesson_review_map[item['name']] = review_id

    chunks = list(split_result.get('chunks') or [])
    for idx, chunk in enumerate(chunks, start=1):
        lesson_name = _clean(chunk.get('lesson_name'))
        lesson_review_id = lesson_review_map.get(lesson_name, '')
        lesson_item = next((x for x in items if x.get('reviewId') == lesson_review_id), None)
        item = {
            'reviewId': f"chunk_{idx:05d}",
            'kind': 'chunk',
            'name': _clean(chunk.get('name')) or f'chunk_{idx:02d}',
            'start': int(chunk.get('start') or 1),
            'end': int(chunk.get('end') or chunk.get('start') or 1),
            'heading': _clean(chunk.get('heading')),
            'title': _clean(chunk.get('title')),
            'number': chunk.get('number') if isinstance(chunk.get('number'), int) else _extract_heading_number(str(chunk.get('heading') or '')),
            'contentHead': bool(chunk.get('content_head')),
            'lessonReviewId': lesson_review_id,
            'topicReviewId': lesson_item.get('topicReviewId') if lesson_item else '',
            'confidence': 'low' if _clean(chunk.get('confidence')).lower() == 'low' else 'high',
            'confidenceScore': float(chunk.get('confidence_score') or (0.35 if _clean(chunk.get('confidence')).lower() == 'low' else 0.92)),
            'confidenceReason': _clean(chunk.get('confidence_reason')),
            'filePath': _clean(chunk.get('file_path')),
            'metaPath': _clean(chunk.get('meta_path')),
            'sourcePath': lesson_item.get('currentPath') if lesson_item else source_pdf,
            'contextPath': lesson_item.get('currentPath') if lesson_item else source_pdf,
            'currentPath': _clean(chunk.get('file_path')),
            'chunkPdfPath': _clean(chunk.get('file_path')),
            'cutlineJson': _clean(chunk.get('cutline_json')),
            'debugPng': _clean(chunk.get('debug_png')),
            'topPng': _clean(chunk.get('top_png')),
            'botPng': _clean(chunk.get('bot_png')),
            'yLine': chunk.get('y_line') if isinstance(chunk.get('y_line'), int) else None,
            'cropPage': 1,
            'cropTop': None,
            'cropBottom': None,
            'cropBands': [],
            'chunkPages': len(PdfReader(_clean(chunk.get('file_path'))).pages) if _clean(chunk.get('file_path')) and os.path.exists(_clean(chunk.get('file_path'))) else max(1, int(chunk.get('end') or chunk.get('start') or 1) - int(chunk.get('start') or 1) + 1),
            'bestMode': _clean(chunk.get('best_mode')),
            'totalPages': len(PdfReader(_clean((lesson_item or {}).get('currentPath'))).pages) if lesson_item and _clean((lesson_item or {}).get('currentPath')) and os.path.exists(_clean((lesson_item or {}).get('currentPath'))) else source_total_pages,
        }
        items.append(item)
    return items


def _public_review_item(session_id: str, item: Dict[str, Any]) -> Dict[str, Any]:
    src = dict(item or {})
    if _clean(src.get('kind')).lower() == 'chunk':
        _sync_item_crop_fields(src)
    out = {k: v for k, v in src.items() if k not in {'filePath', 'metaPath', 'sourcePath', 'contextPath', 'currentPath', 'chunkPdfPath', 'cutlineJson', 'debugPng', 'topPng', 'midPng', 'botPng'}}
    review_id = _clean(item.get('reviewId'))
    quoted = quote_plus(review_id)
    out['previewSourceUrl'] = f"/admin/minio/upload-auto/session/{session_id}/preview?item_id={quoted}&kind=source"
    out['previewContextUrl'] = f"/admin/minio/upload-auto/session/{session_id}/preview?item_id={quoted}&kind=context"
    out['previewCurrentUrl'] = f"/admin/minio/upload-auto/session/{session_id}/preview?item_id={quoted}&kind=current"
    out['previewDebugUrl'] = f"/admin/minio/upload-auto/session/{session_id}/preview?item_id={quoted}&kind=debug"
    out['previewTopUrl'] = f"/admin/minio/upload-auto/session/{session_id}/preview?item_id={quoted}&kind=top"
    out['previewMiddleUrl'] = f"/admin/minio/upload-auto/session/{session_id}/preview?item_id={quoted}&kind=middle"
    out['previewBottomUrl'] = f"/admin/minio/upload-auto/session/{session_id}/preview?item_id={quoted}&kind=bottom"
    return out


def _session_public_payload(session: Dict[str, Any]) -> Dict[str, Any]:
    session_id = _clean(session.get('session_id'))
    review_items = session.get('review_items') or []
    public_items = [_public_review_item(session_id, item) for item in review_items]
    return {
        'status': 'awaiting_review',
        'session_id': session_id,
        'mode': session.get('mode'),
        'subject_map': session.get('subject_map'),
        'class_map': session.get('class_map'),
        'counts': {
            'topics': len([x for x in public_items if x.get('kind') == 'topic']),
            'lessons': len([x for x in public_items if x.get('kind') == 'lesson']),
            'chunks': len([x for x in public_items if x.get('kind') == 'chunk']),
            'highConfidence': len([x for x in public_items if x.get('confidence') == 'high']),
            'lowConfidence': len([x for x in public_items if x.get('confidence') == 'low']),
        },
        'items': public_items,
    }



def _merge_review_items(session: Dict[str, Any], incoming_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    base_map = {str(item.get('reviewId')): dict(item) for item in (session.get('review_items') or [])}
    editable = {
        'start', 'end', 'heading', 'title', 'number', 'contentHead', 'topicReviewId',
        'lessonReviewId', 'yLine', 'cropPage', 'cropTop', 'cropBottom', 'cropBands', 'approved'
    }

    for raw in incoming_items or []:
        review_id = _clean((raw or {}).get('reviewId'))
        if not review_id or review_id not in base_map:
            continue

        target = base_map[review_id]
        for key in editable:
            if key not in raw:
                continue
            value = raw.get(key)

            if key == 'cropBands':
                target['cropBands'] = _normalize_crop_bands(value)
                continue

            if key in {'start', 'end', 'number', 'yLine', 'cropPage', 'cropTop', 'cropBottom'}:
                if value in (None, ''):
                    if key in {'yLine', 'cropTop', 'cropBottom'}:
                        target[key] = None
                    elif key == 'cropPage':
                        target[key] = 1
                    continue
                try:
                    target[key] = int(value)
                except Exception:
                    pass
                continue

            if key in {'contentHead', 'approved'}:
                target[key] = bool(value)
                continue

            target[key] = _clean(value)

        if _clean(target.get('kind')).lower() == 'chunk':
            _sync_item_crop_fields(target)

    merged = list(base_map.values())
    merged.sort(key=lambda x: ({'topic': 1, 'lesson': 2, 'chunk': 3}.get(_clean(x.get('kind')), 9), int(x.get('start') or 0), int(x.get('end') or 0), _clean(x.get('reviewId'))))
    return merged



def _pick_session_preview_path(item: Dict[str, Any], kind: str) -> str:
    kind = _clean(kind).lower() or 'current'
    if kind == 'source':
        return _clean(item.get('sourcePath')) or _clean(item.get('contextPath')) or _clean(item.get('filePath'))
    if kind == 'context':
        return _clean(item.get('contextPath')) or _clean(item.get('sourcePath')) or _clean(item.get('filePath'))
    if kind == 'debug':
        return _clean(item.get('debugPng')) or _clean(item.get('currentPath')) or _clean(item.get('filePath'))
    if kind == 'top':
        return _clean(item.get('topPng'))
    if kind == 'middle':
        return _clean(item.get('midPng'))
    if kind == 'bottom':
        return _clean(item.get('botPng'))
    return _clean(item.get('currentPath')) or _clean(item.get('filePath')) or _clean(item.get('debugPng'))


def _group_items_by_kind(items: List[Dict[str, Any]], kind: str) -> List[Dict[str, Any]]:
    return [dict(x) for x in items if _clean(x.get('kind')) == kind]


def _approve_auto_review_session(
    *,
    request: Request,
    actor: str,
    upload_id: str,
    session: Dict[str, Any],
    reviewed_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    source_pdf = _clean(session.get('source_pdf'))
    if not source_pdf or not os.path.exists(source_pdf):
        raise HTTPException(status_code=410, detail='Phiên duyệt đã hết hạn hoặc mất file nguồn')

    mode = _clean(session.get('mode')).lower()
    root_path = _clean(session.get('root_path'))
    subject_map = _clean(session.get('subject_map'))
    class_map = _clean(session.get('class_map'))
    original_name = _clean(session.get('original_filename')) or Path(source_pdf).name
    subject_stem = Path(original_name).stem

    topic_items = sorted(_group_items_by_kind(reviewed_items, 'topic'), key=lambda x: (int(x.get('start') or 0), int(x.get('end') or 0)))
    lesson_items = sorted(_group_items_by_kind(reviewed_items, 'lesson'), key=lambda x: (int(x.get('start') or 0), int(x.get('end') or 0)))
    chunk_items = sorted(_group_items_by_kind(reviewed_items, 'chunk'), key=lambda x: (_clean(x.get('lessonReviewId')), int(x.get('start') or 0), int(x.get('end') or 0)))

    approved_dir = tempfile.mkdtemp(prefix='auto_review_approve_')
    tasks: List[Dict[str, Any]] = []
    dirty_lessons: Set[str] = set()
    dirty_topics: Set[str] = set()
    dirty_subjects: Set[str] = {subject_map} if subject_map else set()

    if mode == 'subject':
        tasks.append({
            'folder': f"{root_path}/subjects",
            'file_path': source_pdf,
            'filename': f"{_slug_filename(subject_map, subject_map)}.pdf",
            'meta': {
                'folderType': 'subject',
                'classMap': class_map,
                'subjectMap': subject_map,
                'subjectName': subject_stem,
                'subjectTitle': subject_stem,
                'name': subject_stem,
            },
        })

    topic_file_map: Dict[str, str] = {}
    topic_number_map: Dict[str, int] = {}
    if topic_items:
        topic_dir = Path(approved_dir) / 'topics'
        topic_dir.mkdir(parents=True, exist_ok=True)
        for idx, topic in enumerate(topic_items, start=1):
            number = int(topic.get('number') or idx)
            topic_number_map[_clean(topic.get('reviewId'))] = number
            topic_map = f"{subject_map}_CD{number}"
            topic_file = topic_dir / f"{_clean(topic.get('name')) or topic_map}.pdf"
            if mode == 'topic' and idx == 1 and int(topic.get('start') or 1) == 1 and int(topic.get('end') or 0) >= len(PdfReader(source_pdf).pages):
                shutil.copyfile(source_pdf, topic_file)
            else:
                _split_single_pdf(source_pdf, int(topic.get('start') or 1), int(topic.get('end') or topic.get('start') or 1), str(topic_file))
            topic_file_map[_clean(topic.get('reviewId'))] = str(topic_file)
            dirty_topics.add(topic_map)
            tasks.append({
                'folder': f"{root_path}/topics",
                'file_path': str(topic_file),
                'filename': f"{_slug_filename(topic_map, topic_map)}.pdf",
                'meta': {
                    'folderType': 'topic', 'classMap': class_map, 'subjectMap': subject_map, 'topicMap': topic_map,
                    'topicName': _clean(topic.get('title')) or _clean(topic.get('heading')) or topic_map,
                    'name': _clean(topic.get('title')) or topic_map,
                },
            })

    lesson_file_map: Dict[str, str] = {}
    lesson_map_by_review: Dict[str, str] = {}
    lesson_dir = Path(approved_dir) / 'lessons'
    lesson_dir.mkdir(parents=True, exist_ok=True)
    for idx, lesson in enumerate(lesson_items, start=1):
        topic_review_id = _clean(lesson.get('topicReviewId'))
        topic_number = topic_number_map.get(topic_review_id) or (int(lesson.get('topicNumber') or 0) if str(lesson.get('topicNumber') or '').isdigit() else 1)
        lesson_number = None
        if isinstance(lesson.get('number'), int):
            lesson_number = int(lesson.get('number'))
        elif str(lesson.get('number') or '').isdigit():
            lesson_number = int(str(lesson.get('number')).strip())
        else:
            lesson_number = (
                _extract_heading_number(str(lesson.get('heading') or ''))
                or _extract_heading_number(str(lesson.get('title') or ''))
                or _extract_heading_number(str(lesson.get('name') or ''))
            )
        if not isinstance(lesson_number, int) or lesson_number <= 0:
            siblings = [x for x in lesson_items if _clean(x.get('topicReviewId')) == topic_review_id] or lesson_items
            ordered = sorted(siblings, key=lambda x: (int(x.get('start') or 0), int(x.get('end') or 0), _clean(x.get('reviewId'))))
            lesson_number = ordered.index(lesson) + 1 if lesson in ordered else idx
        topic_map = f"{subject_map}_CD{topic_number}"
        lesson_map = f"{topic_map}_B{lesson_number}"
        lesson_map_by_review[_clean(lesson.get('reviewId'))] = lesson_map
        lesson_file = lesson_dir / f"{_clean(lesson.get('name')) or lesson_map}.pdf"
        if mode == 'lesson' and idx == 1 and int(lesson.get('start') or 1) == 1 and int(lesson.get('end') or 0) >= len(PdfReader(source_pdf).pages):
            shutil.copyfile(source_pdf, lesson_file)
        else:
            _split_single_pdf(source_pdf, int(lesson.get('start') or 1), int(lesson.get('end') or lesson.get('start') or 1), str(lesson_file))
        lesson_file_map[_clean(lesson.get('reviewId'))] = str(lesson_file)
        dirty_lessons.add(lesson_map)
        dirty_topics.add(topic_map)
        tasks.append({
            'folder': f"{root_path}/lessons",
            'file_path': str(lesson_file),
            'filename': f"{_slug_filename(lesson_map, lesson_map)}.pdf",
            'meta': {
                'folderType': 'lesson', 'classMap': class_map, 'subjectMap': subject_map, 'topicMap': topic_map,
                'lessonMap': lesson_map,
                'topicName': topic_map,
                'lessonName': _clean(lesson.get('title')) or _clean(lesson.get('heading')) or lesson_map,
                'name': _clean(lesson.get('title')) or lesson_map,
            },
        })

    chunk_root = Path(approved_dir) / 'chunks'
    for lesson_review_id, lesson_pdf in lesson_file_map.items():
        lesson_chunks = [x for x in chunk_items if _clean(x.get('lessonReviewId')) == lesson_review_id]
        if not lesson_chunks:
            continue
        lesson_meta_item = next((x for x in lesson_items if _clean(x.get('reviewId')) == lesson_review_id), None)
        lesson_name = _clean((lesson_meta_item or {}).get('name')) or Path(lesson_pdf).stem
        lesson_chunk_dir = chunk_root / lesson_name
        ordered_chunks = sorted(lesson_chunks, key=lambda x: (int(x.get('start') or 0), int(x.get('end') or 0), _clean(x.get('reviewId'))))
        for cidx, chunk in enumerate(ordered_chunks, start=1):
            chunk_name = f"chunk_{cidx:02d}"
            chunk_dir = lesson_chunk_dir / chunk_name
            chunk_pdf = chunk_dir / f"{lesson_name}_{chunk_name}.pdf"
            _split_single_pdf(lesson_pdf, int(chunk.get('start') or 1), int(chunk.get('end') or chunk.get('start') or 1), str(chunk_pdf))
            chunk_meta = chunk_pdf.with_suffix('.json')
            meta_payload = {
                'source_lesson_pdf': str(Path(lesson_pdf).resolve()),
                'lesson_stem': lesson_name,
                'chunk': chunk_name,
                'chunk_pdf': str(chunk_pdf),
                'heading': _clean(chunk.get('heading')),
                'title': _clean(chunk.get('title')),
                'start': int(chunk.get('start') or 1),
                'end': int(chunk.get('end') or chunk.get('start') or 1),
                'content_head': bool(chunk.get('contentHead')),
                'total_pages': len(PdfReader(lesson_pdf).pages),
            }
            chunk_dir.mkdir(parents=True, exist_ok=True)
            chunk_meta.write_text(json.dumps(meta_payload, ensure_ascii=False, indent=2), encoding='utf-8')
            try:
                debug_dir = chunk_dir / 'DebugCutlines'
                crop_bands = _crop_bands_from_item(chunk)
                if crop_bands:
                    _apply_manual_crop_bands(
                        chunk_pdf_path=str(chunk_pdf),
                        out_dir=debug_dir,
                        crop_bands=crop_bands,
                    )
                else:
                    from ..services.auto_split_upload import _apply_manual_or_auto_cutline  # type: ignore
                    _apply_manual_or_auto_cutline(
                        chunk_pdf_path=str(chunk_pdf),
                        chunk_meta_path=str(chunk_meta),
                        out_dir=debug_dir,
                        y_line_override=(int(chunk.get('yLine')) if str(chunk.get('yLine') or '').isdigit() else None),
                    )
            except Exception:
                pass
            lesson_map = lesson_map_by_review.get(lesson_review_id, '')
            tasks.append({
                'folder': f"{root_path}/chunks",
                'file_path': str(chunk_pdf),
                'filename': f"{_slug_filename(lesson_map + '_C' + str(cidx), lesson_map + '_C' + str(cidx))}.pdf",
                'meta': {
                    'folderType': 'chunk', 'classMap': class_map, 'subjectMap': subject_map,
                    'topicMap': lesson_map.rsplit('_B', 1)[0] if '_B' in lesson_map else '',
                    'lessonMap': lesson_map,
                    'chunkMap': f"{lesson_map}_C{cidx}",
                    'lessonName': _clean((lesson_meta_item or {}).get('title')) or lesson_map,
                    'chunkName': _clean(chunk.get('title')) or _clean(chunk.get('heading')) or f"{lesson_map}_C{cidx}",
                    'name': _clean(chunk.get('title')) or f"{lesson_map}_C{cidx}",
                },
            })

    tasks = [task for task in tasks if _clean(task.get('file_path')) and os.path.exists(_clean(task.get('file_path')))]
    if not tasks:
        raise HTTPException(status_code=400, detail='Không có file nào để sync sau khi duyệt')

    _init_upload_progress(upload_id, path=root_path, total_files=len(tasks))
    _update_upload_progress(upload_id, stage='approving', stageLabel='Đang sinh mô tả và sync', message='Đang sync các file đã duyệt', percent=5, totalFiles=len(tasks))

    synced = []
    for idx, task in enumerate(tasks, start=1):
        synced.append(_sync_local_document_to_system(
            request=request, actor=actor, upload_id=upload_id, file_index=idx, total_files=len(tasks),
            virtual_folder_path=task['folder'], local_file_path=task['file_path'], target_filename=task['filename'], meta=task['meta'],
            skip_hierarchy_rebuild=True,
        ))

    final_rebuild = {'lessons': [], 'topics': [], 'subjects': []}
    _update_upload_progress(upload_id, stage='rebuilding_hierarchy', stageLabel='Đang tổng hợp keyword từ chunk lên lesson/topic/subject', message='Đang rebuild mô tả và keyword từ chunk lên lesson/topic/subject', percent=92)
    for lesson_map in sorted(dirty_lessons):
        try:
            final_rebuild['lessons'].append(rebuild_hierarchy_descriptions_and_keywords(subject_map=subject_map, lesson_map=lesson_map))
        except Exception:
            pass
    for topic_map in sorted(dirty_topics):
        try:
            final_rebuild['topics'].append(rebuild_hierarchy_descriptions_and_keywords(subject_map=subject_map, topic_map=topic_map))
        except Exception:
            pass
    if subject_map:
        try:
            final_rebuild['subjects'].append(rebuild_hierarchy_descriptions_and_keywords(subject_map=subject_map))
        except Exception:
            pass

    _finish_upload_progress(upload_id, total_files=len(tasks), completed_files=len(tasks), status='completed', message='Hoàn tất', stage='completed', stage_label='Hoàn tất')
    return {
        'status': 'ok',
        'upload_id': upload_id,
        'mode': mode,
        'subject_map': subject_map,
        'counts': {
            'synced': len(synced),
            'topics': len(topic_items),
            'lessons': len(lesson_items),
            'chunks': len(chunk_items),
        },
        'final_rebuild': final_rebuild,
    }




def _refresh_review_item_preview(session: Dict[str, Any], merged_items: List[Dict[str, Any]], review_id: str) -> Dict[str, Any]:
    source_pdf = _clean(session.get('source_pdf'))
    if not source_pdf or not os.path.exists(source_pdf):
        raise HTTPException(status_code=410, detail='Phiên duyệt đã hết hạn hoặc mất file nguồn')
    target = next((x for x in merged_items if _clean(x.get('reviewId')) == _clean(review_id)), None)
    if not target:
        raise HTTPException(status_code=404, detail='Không tìm thấy mục cần cập nhật preview')

    refresh_dir = Path((session.get('split_result') or {}).get('temp_dir') or tempfile.mkdtemp(prefix='auto_review_refresh_')) / '_review_refresh'
    refresh_dir.mkdir(parents=True, exist_ok=True)
    kind = _clean(target.get('kind')).lower()

    def _safe_name(value: str, fallback: str) -> str:
        value = _slug_filename(_clean(value), fallback)
        return value or fallback

    source_total_pages = len(PdfReader(source_pdf).pages) if source_pdf and os.path.exists(source_pdf) else 1

    if kind in {'topic', 'lesson'}:
        item_dir = refresh_dir / kind / _safe_name(target.get('reviewId'), kind)
        item_dir.mkdir(parents=True, exist_ok=True)
        out_pdf = item_dir / f"{_safe_name(target.get('name') or target.get('title') or target.get('reviewId'), kind)}.pdf"
        _split_single_pdf(source_pdf, int(target.get('start') or 1), int(target.get('end') or target.get('start') or 1), str(out_pdf))
        target['filePath'] = str(out_pdf)
        target['currentPath'] = str(out_pdf)
        target['sourcePath'] = source_pdf
        target['contextPath'] = source_pdf
        target['metaPath'] = ''
        target['totalPages'] = source_total_pages
        return target

    if kind == 'chunk':
        lesson_review_id = _clean(target.get('lessonReviewId'))
        lesson_item = next((x for x in merged_items if _clean(x.get('reviewId')) == lesson_review_id), None)
        lesson_pdf = _clean((lesson_item or {}).get('currentPath')) or _clean((lesson_item or {}).get('filePath')) or source_pdf
        if not lesson_pdf or not os.path.exists(lesson_pdf):
            raise HTTPException(status_code=400, detail='Không tìm thấy file lesson cha để cắt chunk')
        item_dir = refresh_dir / 'chunks' / _safe_name(lesson_review_id or 'lesson', 'lesson') / _safe_name(target.get('reviewId'), 'chunk')
        item_dir.mkdir(parents=True, exist_ok=True)
        chunk_pdf = item_dir / f"{_safe_name(target.get('name') or target.get('title') or target.get('reviewId'), 'chunk')}.pdf"
        _split_single_pdf(lesson_pdf, int(target.get('start') or 1), int(target.get('end') or target.get('start') or 1), str(chunk_pdf))
        chunk_meta = chunk_pdf.with_suffix('.json')
        meta_payload = {
            'source_lesson_pdf': str(Path(lesson_pdf).resolve()),
            'lesson_stem': Path(lesson_pdf).stem,
            'chunk': _clean(target.get('name')) or Path(chunk_pdf).stem,
            'chunk_pdf': str(chunk_pdf),
            'heading': _clean(target.get('heading')),
            'title': _clean(target.get('title')),
            'start': int(target.get('start') or 1),
            'end': int(target.get('end') or target.get('start') or 1),
            'content_head': bool(target.get('contentHead')),
            'total_pages': len(PdfReader(lesson_pdf).pages),
        }
        chunk_meta.write_text(json.dumps(meta_payload, ensure_ascii=False, indent=2), encoding='utf-8')
        debug_dir = item_dir / 'DebugCutlines'
        try:
            crop_bands = _crop_bands_from_item(target)
            if crop_bands:
                _apply_manual_crop_bands(
                    chunk_pdf_path=str(chunk_pdf),
                    out_dir=debug_dir,
                    crop_bands=crop_bands,
                )
                target['yLine'] = None
            else:
                from ..services.auto_split_upload import _apply_manual_or_auto_cutline  # type: ignore
                _apply_manual_or_auto_cutline(
                    chunk_pdf_path=str(chunk_pdf),
                    chunk_meta_path=str(chunk_meta),
                    out_dir=debug_dir,
                    y_line_override=(int(target.get('yLine')) if str(target.get('yLine') or '').isdigit() else None),
                )
            
        except Exception:
            pass
        stem = chunk_pdf.stem
        target['filePath'] = str(chunk_pdf)
        target['chunkPdfPath'] = str(chunk_pdf)
        target['currentPath'] = str(chunk_pdf)
        target['sourcePath'] = lesson_pdf
        target['contextPath'] = lesson_pdf
        target['metaPath'] = str(chunk_meta)
        target['cutlineJson'] = str(debug_dir / f"{stem}_cutline.json")
        target['debugPng'] = str(debug_dir / f"{stem}_cutline.png")
        target['topPng'] = str(debug_dir / f"{stem}_cutline_top.png")
        target['midPng'] = str(debug_dir / f"{stem}_cutline_middle.png")
        target['botPng'] = str(debug_dir / f"{stem}_cutline_bot.png")
        target['chunkPages'] = len(PdfReader(chunk_pdf).pages) if chunk_pdf and os.path.exists(chunk_pdf) else max(1, int(target.get('end') or target.get('start') or 1) - int(target.get('start') or 1) + 1)
        target['totalPages'] = len(PdfReader(lesson_pdf).pages) if lesson_pdf and os.path.exists(lesson_pdf) else source_total_pages
        _sync_item_crop_fields(target)
        return target

    raise HTTPException(status_code=400, detail='Loại mục không hỗ trợ cập nhật preview')


@router.post("/upload-auto/session/{session_id}/refresh-item", summary="Cập nhật preview cho một mục trong phiên duyệt upload auto")
def refresh_upload_auto_item(session_id: str, body: UploadAutoRefreshItemBody):
    session = _get_auto_review_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail='Không tìm thấy phiên duyệt')
    merged_items = _merge_review_items(session, [dict(body.item or {}, reviewId=body.review_id)])
    updated = _refresh_review_item_preview(session, merged_items, body.review_id)
    session['review_items'] = merged_items
    _save_auto_review_session(session)
    _touch_auto_review_session(session_id)
    return {'status': 'ok', 'item': _public_review_item(session_id, updated)}


@router.post("/upload-auto/", summary="Upload PDF, tự cắt tới chunk và tạo phiên duyệt trước khi sync")
def upload_auto_pdf(
    request: Request,
    current_path: str = Form(..., description="Ví dụ: documents/class-12/subjects hoặc documents/class-12/topics"),
    book_variant: str = Form("", description="TH10 bỏ trống; lớp 11/12 chọn UD hoặc KHMT"),
    upload_id: str = Form("", description="ID tiến trình upload để frontend poll"),
    file: UploadFile = File(...),
):
    actor = _get_actor(request)
    upload_id = (upload_id or "").strip() or uuid.uuid4().hex
    path = clean_path(current_path)
    parts = [p for p in path.split("/") if p]
    if len(parts) < 3 or parts[0].lower() != "documents":
        raise HTTPException(status_code=400, detail="Upload auto chỉ dùng cho documents/class-x/subjects hoặc documents/class-x/topics")
    mode = _normalize_folder_type(parts[-1])
    if mode not in {"subject", "topic", "lesson"}:
        raise HTTPException(status_code=400, detail="Upload auto chỉ hỗ trợ ở thư mục subjects, topics hoặc lessons")
    if not file or not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Chỉ hỗ trợ upload file PDF")

    class_number = _extract_class_number_from_virtual_path(path)
    subject_map = _resolve_subject_map_from_variant(class_number, book_variant)
    class_map = f"L{class_number}"
    root_path = _auto_root_for_class(path)

    _init_upload_progress(upload_id, path=path, total_files=1)
    _update_upload_progress(upload_id, stage="uploading", stageLabel="Đang nhận file", message="Đang nhận file upload", percent=1)

    temp_src = None
    split_result = None
    try:
        session_id = uuid.uuid4().hex
        session_dir = _auto_review_session_dir(session_id)
        session_dir.mkdir(parents=True, exist_ok=True)
        temp_src = str(session_dir / 'source.pdf')
        with open(temp_src, "wb") as out:
            out.write(file.file.read())

        analysis_cb = _analysis_progress_to_upload(upload_id, path)
        try:
            split_result = extract_and_split_structure(temp_src, mode=mode, progress_cb=analysis_cb)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Không tách được cấu trúc file upload. Chi tiết: {exc}")

        review_items = _build_review_items_from_split(session_id, split_result, temp_src)
        session_payload = {
            'session_id': session_id,
            'upload_id': upload_id,
            'actor': actor,
            'current_path': path,
            'root_path': root_path,
            'mode': mode,
            'class_number': class_number,
            'class_map': class_map,
            'subject_map': subject_map,
            'book_variant': book_variant,
            'original_filename': file.filename,
            'source_pdf': temp_src,
            'session_dir': str(session_dir),
            'split_result': split_result,
            'review_items': review_items,
        }
        _save_auto_review_session(session_payload)
        temp_src = None
        split_result = None
        _update_upload_progress(upload_id, stage='awaiting_review', stageLabel='Đang chờ duyệt', message='Đã cắt xong. Hãy kiểm tra màu xanh/đỏ rồi xác nhận.', percent=100, status='awaiting_review')
        return _session_public_payload(session_payload)
    except HTTPException:
        raise
    except Exception as exc:
        _finish_upload_progress(upload_id, total_files=1, completed_files=0, status="failed", message=str(exc), stage="failed", stage_label="Lỗi")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        try:
            if temp_src and split_result is None and os.path.exists(temp_src):
                os.remove(temp_src)
        except Exception:
            pass
        if split_result is not None:
            cleanup_split_result(split_result)


@router.get("/upload-auto/session/{session_id}", summary="Lấy dữ liệu review của một phiên upload auto")
def get_upload_auto_session(session_id: str):
    session = _get_auto_review_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail='Không tìm thấy phiên duyệt')
    _touch_auto_review_session(session_id)
    return _session_public_payload(session)


def _resolve_session_preview_file(session: Dict[str, Any], item: Dict[str, Any], kind: str) -> str:
    file_path = _pick_session_preview_path(item, kind)
    if file_path and os.path.exists(file_path):
        return file_path

    item_kind = _clean(item.get('kind')).lower()
    review_id = _clean(item.get('reviewId'))

    if item_kind in {'topic', 'lesson', 'chunk'}:
        try:
            refreshed = _refresh_review_item_preview(session, session.get('review_items') or [], review_id)
            file_path = _pick_session_preview_path(refreshed, kind)
            if file_path and os.path.exists(file_path):
                try:
                    _save_auto_review_session(session)
                except Exception:
                    pass
                return file_path
        except Exception:
            pass

    if kind in {'context', 'source'}:
        source_pdf = _clean(session.get('source_pdf'))
        if source_pdf and os.path.exists(source_pdf):
            if kind == 'context':
                item['contextPath'] = source_pdf
            else:
                item['sourcePath'] = source_pdf
            try:
                _save_auto_review_session(session)
            except Exception:
                pass
            return source_pdf

    if kind == 'current':
        for fallback_kind in ('context', 'source'):
            fallback_path = _pick_session_preview_path(item, fallback_kind)
            if fallback_path and os.path.exists(fallback_path):
                return fallback_path
        source_pdf = _clean(session.get('source_pdf'))
        if source_pdf and os.path.exists(source_pdf):
            return source_pdf

    return ''


@router.get("/upload-auto/session/{session_id}/preview", summary="Preview file tạm trong phiên duyệt upload auto")
def get_upload_auto_preview(session_id: str, item_id: str = Query(...), kind: str = Query('current')):
    session = _get_auto_review_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail='Không tìm thấy phiên duyệt')
    item = next((x for x in (session.get('review_items') or []) if _clean(x.get('reviewId')) == _clean(item_id)), None)
    if not item:
        raise HTTPException(status_code=404, detail='Không tìm thấy mục preview')
    file_path = _resolve_session_preview_file(session, item, kind)
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail='Không tìm thấy file preview')
    _touch_auto_review_session(session_id)
    return FileResponse(
        path=file_path,
        media_type=_guess_media_type(file_path),
        headers={"Content-Disposition": f'inline; filename="{os.path.basename(file_path)}"'},
    )


@router.get("/upload-auto/session/{session_id}/page-preview", summary="Render một trang PDF trong phiên duyệt upload auto thành PNG")
def get_upload_auto_page_preview(
    session_id: str,
    item_id: str = Query(...),
    kind: str = Query('current'),
    page: int = Query(1, ge=1),
):
    session = _get_auto_review_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail='Không tìm thấy phiên duyệt')
    item = next((x for x in (session.get('review_items') or []) if _clean(x.get('reviewId')) == _clean(item_id)), None)
    if not item:
        raise HTTPException(status_code=404, detail='Không tìm thấy mục preview')
    file_path = _resolve_session_preview_file(session, item, kind)
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail='Không tìm thấy file preview')
    if not str(file_path).lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail='Chỉ render được file PDF')
    try:
        import fitz  # type: ignore
        doc = fitz.open(str(file_path))
        page_idx = max(0, min(int(page) - 1, doc.page_count - 1))
        pix = doc[page_idx].get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
        png_bytes = pix.tobytes('png')
        doc.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Không render được trang PDF: {exc}')
    _touch_auto_review_session(session_id)
    return StreamingResponse(io.BytesIO(png_bytes), media_type='image/png')


@router.post("/upload-auto/approve", summary="Xác nhận phiên review upload auto rồi mới sync toàn hệ thống")
def approve_upload_auto(request: Request, body: UploadAutoApproveBody):
    session = _get_auto_review_session(body.session_id)
    if not session:
        raise HTTPException(status_code=404, detail='Không tìm thấy phiên duyệt hoặc phiên đã hết hạn')
    reviewed_items = _merge_review_items(session, body.items or [])
    actor = _get_actor(request)
    upload_id = _clean(body.upload_id) or _clean(session.get('upload_id')) or uuid.uuid4().hex
    try:
        result = _approve_auto_review_session(
            request=request,
            actor=actor,
            upload_id=upload_id,
            session=session,
            reviewed_items=reviewed_items,
        )
        return result
    finally:
        popped = _pop_auto_review_session(body.session_id)
        if popped:
            try:
                cleanup_split_result(popped.get('split_result'))
            except Exception:
                pass
            try:
                src = _clean(popped.get('source_pdf'))
                if src and os.path.exists(src):
                    os.remove(src)
            except Exception:
                pass
