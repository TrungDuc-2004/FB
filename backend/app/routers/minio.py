import os
import io
import time
import tempfile
import json
import threading
import uuid

from urllib.parse import quote, quote_plus
from typing import List, Optional, Tuple, Set, Dict, Any

from sqlalchemy import text
from datetime import timedelta, datetime, timezone

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

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


# =================== GET =================== #

@router.get("/uploads/progress/{upload_id}", summary="Lấy tiến trình upload/import hiện tại")
def get_upload_progress(upload_id: str):
    _cleanup_upload_progress()
    with _UPLOAD_PROGRESS_LOCK:
        payload = _UPLOAD_PROGRESS.get((upload_id or "").strip())
        if not payload:
            raise HTTPException(status_code=404, detail="Upload progress not found")
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
                        meta={},
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

                _mark_file_progress(upload_id, file_index=index, total_files=total_files, file_name=filename, stage="rebuilding_hierarchy", stage_label="Đang cập nhật mô tả/keyword", file_percent=0.54, message=f"Đang dựng mô tả và keyword cho {filename}")

                try:
                    rebuild_hierarchy_descriptions_and_keywords(
                        subject_map=sync_res.subject_map,
                        topic_map=sync_res.topic_map or "",
                        lesson_map=sync_res.lesson_map or "",
                        chunk_map=sync_res.chunk_map or "",
                    )
                except Exception:
                    pass

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

            _mark_file_progress(
                upload_id,
                file_index=1,
                total_files=1,
                file_name=filename,
                stage="syncing_mongo",
                stage_label="Đang sync MongoDB",
                file_percent=0.40,
                message=f"Đang đồng bộ media {filename} vào MongoDB",
            )
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
                meta={**meta, "__local_file_path": temp_file_path},
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
        _mark_file_progress(upload_id, file_index=1, total_files=1, file_name=filename, stage="rebuilding_hierarchy", stage_label="Đang cập nhật mô tả/keyword", file_percent=0.56, message=f"Đang dựng mô tả và keyword cho {filename}")
        try:
            hierarchy_details = rebuild_hierarchy_descriptions_and_keywords(
                subject_map=sync_res.subject_map,
                topic_map=sync_res.topic_map or "",
                lesson_map=sync_res.lesson_map or "",
                chunk_map=sync_res.chunk_map or "",
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
