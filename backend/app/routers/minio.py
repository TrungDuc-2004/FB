import os
import io
import time
import json

from urllib.parse import quote, quote_plus
from typing import List, Optional, Tuple, Set, Dict, Any
from datetime import timedelta

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from minio.error import S3Error
from pydantic import BaseModel, Field
from minio.commonconfig import CopySource
from minio.deleteobjects import DeleteObject

from ..services.minio_client import get_minio_client
from ..services.mongo_client import get_mongo_client
from ..services.mongo_sync import sync_minio_object_to_mongo
from ..services.postgre_sync_from_mongo import sync_postgre_from_mongo_ids


router = APIRouter(
    prefix="/admin/minio",
    tags=["Minio"]
)

# =================== Helpers =================== #

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


def _split_virtual(virtual: str, default_bucket: Optional[str], *, allow_empty_key: bool) -> Tuple[str, str]:
    """
    Virtual path từ UI -> (bucket, key)

    - Nếu có MINIO_BUCKET: bucket=MINIO_BUCKET, key=virtual (có thể rỗng nếu allow_empty_key=True)
    - Nếu không có MINIO_BUCKET: virtual phải có dạng: "<bucket>" hoặc "<bucket>/<key>"
    """
    p = clean_path(virtual)

    if default_bucket:
        if not allow_empty_key and not p:
            raise HTTPException(status_code=400, detail="Path is required")
        return default_bucket, p

    # multi-bucket mode
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
    """
    Format đường dẫn trả về cho UI.
    - single-bucket: trả key
    - multi-bucket: trả "bucket/key"
    """
    k = clean_path(key)
    if default_bucket:
        return k
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

@router.get("/open", summary="Mở/Download file (proxy qua backend - copy URL mở được ngay)")
def open_file(
    request: Request,
    object_key: str = Query(..., min_length=1, description="virtual object_key (single-bucket: key; multi-bucket: bucket/key)"),
    download: bool = Query(False, description="true = download, false = open inline"),
):
    client, default_bucket, _public_base = _runtime()

    bucket, key = _split_virtual(object_key, default_bucket, allow_empty_key=False)
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

    bucket, key = _split_virtual(path, default_bucket, allow_empty_key=True)
    key = clean_path(key)
    prefix = folder_marker(key)

    try:
        objects = client.list_objects(bucket, prefix=prefix, recursive=False)

        folder_names: Set[str] = set()
        folders = []
        files = []

        for obj in objects:
            name = obj.object_name

            if prefix and name == prefix:
                continue

            rest = name[len(prefix):] if prefix else name
            if not rest:
                continue

            if getattr(obj, "is_dir", False) or rest.endswith("/") or "/" in rest:
                folder = rest.strip("/").split("/", 1)[0]
                if folder:
                    folder_names.add(folder)
                continue

            object_key = name
            file_name = rest.split("/")[-1]

            virtual_key = _to_virtual(default_bucket, bucket, object_key)

            files.append({
                "object_key": virtual_key,
                "name": file_name,
                "size": getattr(obj, "size", None),
                "etag": getattr(obj, "etag", None),
                "last_modified": obj.last_modified.isoformat() if getattr(obj, "last_modified", None) else None,
                "url": _backend_open_url(request, virtual_key),
            })

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

    b1, old_rel = _split_virtual(body.old_path, default_bucket, allow_empty_key=False)
    b2, new_rel = _split_virtual(body.new_path, default_bucket, allow_empty_key=False)

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

    bucket, old_key = _split_virtual(body.object_key, default_bucket, allow_empty_key=False)
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

    bucket, rel = _split_virtual(body.full_path, default_bucket, allow_empty_key=False)
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
async def upload_files_to_path(
    request: Request,
    path: str = Form(..., description="Ví dụ: images hoặc documents/class-10/tin-hoc/chunk"),
    files: List[UploadFile] = File(...),
):
    client, default_bucket, public_base = _runtime()
    actor = _get_actor(request)

    bucket, rel = _split_virtual(path, default_bucket, allow_empty_key=True)
    p = clean_path(rel)
    prefix = folder_marker(p)

    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    uploaded, failed = [], []
    seen = set()

    try:
        for f in files:
            if not f.filename:
                failed.append({"filename": None, "error": "Missing filename"})
                continue

            filename = os.path.basename(f.filename)
            object_key = prefix + filename

            if object_key in seen:
                failed.append({"filename": filename, "object_key": object_key, "error": "Duplicate in request batch"})
                await f.close()
                continue
            seen.add(object_key)

            # check exists
            try:
                client.stat_object(bucket, object_key)
                failed.append({
                    "filename": filename,
                    "object_key": _to_virtual(default_bucket, bucket, object_key),
                    "error": "Already exists"
                })
                await f.close()
                continue
            except S3Error:
                pass

            try:
                # upload minio
                result = client.put_object(
                    bucket_name=bucket,
                    object_name=object_key,
                    data=f.file,
                    length=-1,
                    part_size=10 * 1024 * 1024,
                    content_type=f.content_type or "application/octet-stream",
                )

                # ====== SYNC MongoDB ======
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
                    failed.append({
                        "filename": filename,
                        "object_key": _to_virtual(default_bucket, bucket, object_key),
                        "error": f"Mongo sync failed: {e}",
                    })
                    continue

                # ====== SYNC Postgre FROM Mongo ======
                try:
                    sync_postgre_from_mongo_ids(
                        mongo_class_id=str(sync_res.class_id),
                        mongo_subject_id=str(sync_res.subject_id),
                        mongo_topic_id=str(sync_res.topic_id),
                        mongo_lesson_id=str(sync_res.lesson_id),
                        mongo_chunk_id=str(sync_res.chunk_id),
                    )
                except Exception as e:
                    # rollback file + ẩn chunk mongo
                    try:
                        client.remove_object(bucket, object_key)
                    except Exception:
                        pass
                    _hide_mongo_chunk(str(sync_res.chunk_id), actor=actor)

                    failed.append({
                        "filename": filename,
                        "object_key": _to_virtual(default_bucket, bucket, object_key),
                        "error": f"Postgre sync failed: {e}",
                    })
                    continue

                uploaded.append({
                    "filename": filename,
                    "object_key": _to_virtual(default_bucket, bucket, object_key),
                    "etag": getattr(result, "etag", None),
                    "url": _backend_open_url(request, _to_virtual(default_bucket, bucket, object_key)),
                })

            except S3Error as e:
                failed.append({
                    "filename": filename,
                    "object_key": _to_virtual(default_bucket, bucket, object_key),
                    "error": str(e)
                })

            finally:
                await f.close()

        return {
            "bucket": bucket,
            "path": _to_virtual(default_bucket, bucket, p) if p else (bucket if not default_bucket else ""),
            "uploaded_count": len(uploaded),
            "failed_count": len(failed),
            "uploaded": uploaded,
            "failed": failed,
        }

    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e


@router.post("/objects/", summary="Insert 1 item (có thể có file hoặc không) - insert xong -> sync Mongo -> sync Postgre")
async def insert_item(
    request: Request,
    path: str = Form(...),
    name: str = Form("", description="Tên file nếu không upload file"),
    meta_json: str = Form("", description="JSON string metadata (tuỳ chọn)"),
    file: UploadFile | None = File(None),
):
    client, default_bucket, public_base = _runtime()
    actor = _get_actor(request)
    meta = _parse_meta_json(meta_json)

    bucket, rel = _split_virtual(path, default_bucket, allow_empty_key=True)
    p = clean_path(rel)
    prefix = folder_marker(p)

    if file and file.filename:
        filename = os.path.basename(file.filename)
    else:
        filename = (name or "").strip() or f"item-{int(time.time())}.txt"
        if "/" in filename or "\\" in filename:
            raise HTTPException(status_code=400, detail="name must not contain '/' or '\\'")

    object_key = prefix + filename

    try:
        client.stat_object(bucket, object_key)
        raise HTTPException(status_code=409, detail="Object already exists")
    except S3Error:
        pass

    try:
        # upload minio
        if file:
            client.put_object(
                bucket,
                object_key,
                data=file.file,
                length=-1,
                part_size=10 * 1024 * 1024,
                content_type=file.content_type or "application/octet-stream",
            )
            await file.close()
        else:
            client.put_object(
                bucket,
                object_key,
                data=io.BytesIO(b""),
                length=0,
                content_type="text/plain",
            )

        # sync mongo
        try:
            sync_res = sync_minio_object_to_mongo(
                bucket=bucket,
                object_key=object_key,
                meta=meta,
                actor=actor,
            )
        except Exception as e:
            try:
                client.remove_object(bucket, object_key)
            except Exception:
                pass
            raise HTTPException(status_code=500, detail=f"Mongo sync failed: {e}") from e

        # sync postgre from mongo
        try:
            pg_ids = sync_postgre_from_mongo_ids(
                mongo_class_id=str(sync_res.class_id),
                mongo_subject_id=str(sync_res.subject_id),
                mongo_topic_id=str(sync_res.topic_id),
                mongo_lesson_id=str(sync_res.lesson_id),
                mongo_chunk_id=str(sync_res.chunk_id),
            )
        except Exception as e:
            try:
                client.remove_object(bucket, object_key)
            except Exception:
                pass
            _hide_mongo_chunk(str(sync_res.chunk_id), actor=actor)
            raise HTTPException(status_code=500, detail=f"Postgre sync failed: {e}") from e

        return {
            "status": "inserted",
            "bucket": bucket,
            "path": _to_virtual(default_bucket, bucket, p) if p else (bucket if not default_bucket else ""),
            "object_key": _to_virtual(default_bucket, bucket, object_key),
            "url": _backend_open_url(request, _to_virtual(default_bucket, bucket, object_key)),
            "meta_json": meta_json or "",
            "mongo": {
                "classId": str(sync_res.class_id),
                "subjectId": str(sync_res.subject_id),
                "topicId": str(sync_res.topic_id),
                "lessonId": str(sync_res.lesson_id),
                "chunkId": str(sync_res.chunk_id),
            },
            "postgre": {
                "classId": pg_ids.class_id,
                "subjectId": pg_ids.subject_id,
                "topicId": pg_ids.topic_id,
                "lessonId": pg_ids.lesson_id,
                "chunkId": pg_ids.chunk_id,
                "keywordIds": pg_ids.keyword_ids,
            }
        }

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e


# =================== DELETE =================== #

@router.delete("/folders", summary="Xoá folder (cascade)")
def delete_folder(path: str = Query(..., min_length=1, description="full path folder, ví dụ documents/class-10")):
    client, default_bucket, _ = _runtime()

    bucket, rel = _split_virtual(path, default_bucket, allow_empty_key=False)
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

    bucket, key = _split_virtual(object_key, default_bucket, allow_empty_key=False)
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
