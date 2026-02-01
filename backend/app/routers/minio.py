import os
import io
import time
from urllib.parse import quote
from typing import Literal, List
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Path,  Query
from minio.error import S3Error
from ..services.minio_client import get_minio_client
from pydantic import BaseModel, Field
from minio.commonconfig import CopySource
from minio.deleteobjects import DeleteObject


router = APIRouter(
    prefix="/admin/minio",
    tags=["Minio"]
)

client = get_minio_client()
BUCKET = os.getenv("MINIO_BUCKET")  
MINIO_PUBLIC_BASE_URL = (os.getenv("MINIO_PUBLIC_BASE_URL") or "http://127.0.0.1:9000").rstrip("/")


def clean_path(path: str) -> str:
    p = (path or "").strip()
    if p.startswith("/"):
        p = p[1:]
    if "\\" in p:
        raise HTTPException(status_code=400, detail="Invalid path (contains backslash)")
    if ".." in p.split("/"):
        raise HTTPException(status_code=400, detail="Invalid path (contains ..)")
    return p.strip("/")


def folder_marker(path: str) -> str:
    p = clean_path(path)
    return f"{p}/" if p else ""


def public_url(object_key: str) -> str:
    encoded = quote(object_key, safe="/")
    return f"{MINIO_PUBLIC_BASE_URL}/{BUCKET}/{encoded}"


def prefix_has_anything(client, prefix: str) -> bool:
    # chỉ cần thấy 1 object là coi như "folder tồn tại"
    it = client.list_objects(BUCKET, prefix=prefix, recursive=True)
    for _ in it:
        return True
    return False

# =================== START MODEL =================== #
# =================== Model Create Folder =================== #
class CreateFolderBody(BaseModel):
    full_path: str = Field(..., min_length=1)

# =================== Model Rename Folder =================== #
class RenameFolderBody(BaseModel):
    old_path: str = Field(..., min_length=1)
    new_path: str = Field(..., min_length=1)

# =================== Model Rename File =================== #
class RenameObjectBody(BaseModel):
    object_key: str = Field(..., min_length=1)      
    new_name: str = Field(..., min_length=1)        

# =================== END MODEL =================== #



# =================== START GET =================== #
# =================== List cấu trúc MinIO =================== #
@router.get("/list", summary="Lấy ra cấu trúc list trong MinIO")
def list_structure(path: str = Query("", description="Mẫu: documents, documents/class-10, ...")):
    client = get_minio_client()

    p = (path or "").strip()
    if p.startswith("/"):
        p = p[1:]
    if "\\" in p:
        raise HTTPException(status_code=400, detail="Invalid path (contains backslash)")
    if ".." in p.split("/"):
        raise HTTPException(status_code=400, detail="Invalid path (contains ..)")

    p = p.strip("/")
    prefix = f"{p}/" if p else ""

    try:
        # recursive=False => chỉ lấy con trực tiếp trong folder này
        objects = client.list_objects(BUCKET, prefix=prefix, recursive=False)

        folders = []
        files = []

        for obj in objects:
            if prefix and obj.object_name == prefix:
                continue

            # folder sẽ có is_dir=True hoặc object_name kết thúc bằng "/"
            if getattr(obj, "is_dir", False) or obj.object_name.endswith("/"):
                full = obj.object_name.rstrip("/")
                name = full.split("/")[-1] if full else ""
                if name:
                    folders.append({"name": name, "fullPath": full})
            else:
                object_key = obj.object_name
                name = object_key.split("/")[-1]
                encoded_key = quote(object_key, safe="/")

                files.append({
                    "object_key": object_key,
                    "name": name,
                    "size": obj.size,
                    "etag": obj.etag,
                    "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                    "url": f"{MINIO_PUBLIC_BASE_URL}/{BUCKET}/{encoded_key}",
                })

        folders.sort(key=lambda x: x["name"].lower())
        files.sort(key=lambda x: x["name"].lower())

        return {
            "bucket": BUCKET,
            "path": p,
            "prefix": prefix,
            "folders": folders,
            "files": files,
        }

    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e
    
# =================== END GET =================== #

# =================== START PUT =================== #
# =================== Sửa tên Folder =================== #
@router.put("/folders/", summary="Đổi tên folder")
def rename_folder(body: RenameFolderBody):
    client = get_minio_client()

    old_path = clean_path(body.old_path)
    new_path = clean_path(body.new_path)

    if old_path == new_path:
        raise HTTPException(status_code=400, detail="new_path is the same as old_path")

    old_prefix = folder_marker(old_path)
    new_prefix = folder_marker(new_path)

    # chặn rename folder vào bên trong chính nó
    if new_prefix.startswith(old_prefix):
        raise HTTPException(status_code=400, detail="new_path must not be inside old_path")

    try:
        if not prefix_has_anything(client, old_prefix):
            raise HTTPException(status_code=404, detail="Folder not found")

        if prefix_has_anything(client, new_prefix):
            raise HTTPException(status_code=409, detail="Target folder already exists")

        # list toàn bộ objects trong old_prefix
        objects = list(client.list_objects(BUCKET, prefix=old_prefix, recursive=True))
        if not objects:
            # nếu folder rỗng nhưng có marker, vẫn rename marker
            objects = []

        # copy từng object sang prefix mới
        copied = 0
        to_delete = []

        for obj in objects:
            old_key = obj.object_name
            suffix = old_key[len(old_prefix):]  # phần phía sau old_prefix
            new_key = new_prefix + suffix

            client.copy_object(
                bucket_name=BUCKET,
                object_name=new_key,
                source=CopySource(BUCKET, old_key),
            )
            to_delete.append(DeleteObject(old_key))
            copied += 1
        try:
            client.stat_object(BUCKET, old_prefix)
            client.copy_object(
                BUCKET,
                new_prefix,
                CopySource(BUCKET, old_prefix),
            )
            to_delete.append(DeleteObject(old_prefix))
        except S3Error:
            # không có marker cũng không sao
            pass

        # xoá toàn bộ object cũ
        if to_delete:
            errors = list(client.remove_objects(BUCKET, to_delete))
            if errors:
                # có lỗi khi xoá
                raise HTTPException(status_code=500, detail=f"Delete errors: {[str(e) for e in errors]}")

        return {
            "status": "renamed",
            "bucket": BUCKET,
            "old_path": old_path,
            "new_path": new_path,
            "copied_objects": copied,
        }

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e


# =================== Sửa tên File =================== #
@router.put("/objects/", summary="Đổi tên file")
def rename_object(body: RenameObjectBody):
    client = get_minio_client()

    old_key = clean_path(body.object_key)
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
        # old exists
        try:
            client.stat_object(BUCKET, old_key)
        except S3Error:
            raise HTTPException(status_code=404, detail="Object not found")

        # new not exists
        try:
            client.stat_object(BUCKET, new_key)
            raise HTTPException(status_code=409, detail="Target already exists")
        except S3Error:
            pass

        client.copy_object(BUCKET, new_key, CopySource(BUCKET, old_key))
        client.remove_object(BUCKET, old_key)

        return {
            "status": "renamed",
            "bucket": BUCKET,
            "old_object_key": old_key,
            "new_object_key": new_key,
            "url": public_url(new_key),
        }

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e
    
# =================== END PUT =================== #

# =================== START POST =================== 
# =================== Tạo Folder mới =================== #
@router.post("/folders", summary="Tạo folder")
def create_folder(body: CreateFolderBody):
    client = get_minio_client()
    full_path = clean_path(body.full_path)

    if not full_path:
        raise HTTPException(status_code=400, detail="full_path is required")

    marker = folder_marker(full_path)

    try:
        # nếu đã có object/prefix rồi => coi như đã tồn tại
        if prefix_has_anything(client, marker):
            raise HTTPException(status_code=409, detail="Folder already exists")

        # tạo folder marker (object rỗng)
        client.put_object(
            BUCKET,
            marker,
            data=io.BytesIO(b""),
            length=0,
            content_type="application/octet-stream",
        )

        return {
            "status": "created",
            "bucket": BUCKET,
            "folder": {"fullPath": full_path, "marker": marker},
        }

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e
    
# =================== Upload File vào Folder =================== #
@router.post("/files/", summary="Upload nhiều file vào folder path")
async def upload_files_to_path(
    path: str = Form(..., description="Ví dụ: images hoặc documents/class-10/tin-hoc/chunk"),
    files: List[UploadFile] = File(...),
):
    client = get_minio_client()
    p = clean_path(path)
    prefix = folder_marker(p)  # p/ nếu p có

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
                client.stat_object(BUCKET, object_key)
                failed.append({"filename": filename, "object_key": object_key, "error": "Already exists"})
                await f.close()
                continue
            except S3Error:
                pass

            try:
                result = client.put_object(
                    bucket_name=BUCKET,
                    object_name=object_key,
                    data=f.file,
                    length=-1,
                    part_size=10 * 1024 * 1024,
                    content_type=f.content_type or "application/octet-stream",
                )
                uploaded.append({
                    "filename": filename,
                    "object_key": object_key,
                    "etag": getattr(result, "etag", None),
                    "url": public_url(object_key),
                })
            except S3Error as e:
                failed.append({"filename": filename, "object_key": object_key, "error": str(e)})
            finally:
                await f.close()

        return {
            "bucket": BUCKET,
            "path": p,
            "uploaded_count": len(uploaded),
            "failed_count": len(failed),
            "uploaded": uploaded,
            "failed": failed,
        }

    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e
    

# =================== Insert  =================== #
@router.post("/objects/", summary="Insert 1 item (có thể có file hoặc không)")
async def insert_item(
    path: str = Form(...),
    name: str = Form("", description="Tên file nếu không upload file"),
    meta_json: str = Form("", description="JSON string metadata (tuỳ chọn)"),
    file: UploadFile | None = File(None),
):
    client = get_minio_client()
    p = clean_path(path)
    prefix = folder_marker(p)

    filename = ""
    if file and file.filename:
        filename = os.path.basename(file.filename)
    else:
        filename = (name or "").strip()
        if not filename:
            filename = f"item-{int(time.time())}.txt"
        if "/" in filename or "\\" in filename:
            raise HTTPException(status_code=400, detail="name must not contain '/' or '\\'")

    object_key = prefix + filename

    # nếu tồn tại => 409
    try:
        client.stat_object(BUCKET, object_key)
        raise HTTPException(status_code=409, detail="Object already exists")
    except S3Error:
        pass

    try:
        if file:
            # upload content thật
            client.put_object(
                BUCKET,
                object_key,
                data=file.file,
                length=-1,
                part_size=10 * 1024 * 1024,
                content_type=file.content_type or "application/octet-stream",
            )
            await file.close()
        else:
            # tạo object rỗng
            client.put_object(
                BUCKET,
                object_key,
                data=io.BytesIO(b""),
                length=0,
                content_type="text/plain",
            )

        return {
            "status": "inserted",
            "bucket": BUCKET,
            "path": p,
            "object_key": object_key,
            "url": public_url(object_key),
            "meta_json": meta_json or "",
        }

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e
    
# =================== END PUT =================== #

# =================== START DELETE =================== #
# =================== Xoá Folder =================== #
@router.delete("/folders", summary="Xoá folder (cascade)")
def delete_folder(path: str = Query(..., min_length=1, description="full path folder, ví dụ documents/class-10")):
    client = get_minio_client()
    p = clean_path(path)
    prefix = folder_marker(p)

    try:
        if not prefix_has_anything(client, prefix):
            raise HTTPException(status_code=404, detail="Folder not found")

        # lấy tất cả object dưới prefix
        objects = client.list_objects(BUCKET, prefix=prefix, recursive=True)
        to_delete = [DeleteObject(obj.object_name) for obj in objects]

        # xoá marker nếu có
        to_delete.append(DeleteObject(prefix))

        errors = list(client.remove_objects(BUCKET, to_delete))
        if errors:
            raise HTTPException(status_code=500, detail=f"Delete errors: {[str(e) for e in errors]}")

        return {
            "status": "deleted",
            "bucket": BUCKET,
            "path": p,
        }

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e

# =================== Xoá File =================== #
@router.delete("/files", summary="Xoá 1 file")
def delete_object(object_key: str = Query(..., min_length=1)):
    client = get_minio_client()
    key = clean_path(object_key)

    try:
        try:
            client.stat_object(BUCKET, key)
        except S3Error:
            raise HTTPException(status_code=404, detail="Object not found")

        client.remove_object(BUCKET, key)

        return {
            "status": "deleted",
            "bucket": BUCKET,
            "object_key": key,
        }

    except HTTPException:
        raise
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}") from e
    
# =================== END DELETE =================== #
