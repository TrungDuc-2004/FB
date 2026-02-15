# app/routers/admin_mongo.py
from fastapi import APIRouter, Query, Path, HTTPException, status, Body, Request, UploadFile, File
from ..services.mongo_client import get_mongo_client
from ..services.mongo_bulk_import import import_metadata_xlsx_bytes
from typing import Any, Dict, Tuple, Optional
from fastapi.encoders import jsonable_encoder
from bson import ObjectId
from bson.errors import InvalidId
from pymongo.errors import CollectionInvalid, OperationFailure
import re
from datetime import datetime, timezone

router = APIRouter(prefix="/admin/mongo", tags=["Mongo"])

mongo = get_mongo_client()
db = mongo["db"]

_COLLECTION_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")


# ========================= HELPERS =========================
def _normalize_collection_name(name: str) -> str:
    if name is None:
        raise HTTPException(status_code=422, detail="collection_name is required")
    name = name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="collection_name is required")
    if not _COLLECTION_RE.match(name):
        raise HTTPException(
            status_code=422,
            detail="collection_name chỉ nên gồm chữ/số/_/- và dài 1-64 ký tự",
        )
    return name


def _check_collection_exist(collection_name: str):
    if collection_name not in db.list_collection_names():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Collection '{collection_name}' not exist",
        )


def _now():
    return datetime.now(timezone.utc)


def _get_actor(request: Optional[Request]) -> str:
    # request đôi khi bị None nếu bạn gọi function kiểu khác
    if request is None:
        return "system"
    # hỗ trợ cả 2 header
    return request.headers.get("x-user") or request.headers.get("x-actor") or "system"


def _try_objectid(s: str) -> Optional[ObjectId]:
    try:
        return ObjectId(s)
    except (InvalidId, TypeError):
        return None


def _find_one_by_any_key(
    col: str,
    key: str,
    projection: Optional[dict] = None,
) -> Tuple[Optional[dict], Optional[dict]]:
    """
    Tìm doc theo:
    1) _id = ObjectId(key)
    2) _id = key (string)
    3) (riêng user) username = key

    Return: (doc, id_filter) trong đó id_filter dùng để update/delete.
    """
    # 1) ObjectId
    oid = _try_objectid(key)
    if oid is not None:
        doc = db[col].find_one({"_id": oid}, projection)
        if doc:
            return doc, {"_id": oid}

    # 2) string _id
    doc = db[col].find_one({"_id": key}, projection)
    if doc:
        return doc, {"_id": key}

    # 3) user: lookup by username (rất hay dùng trong UI)
    if col == "user":
        doc = db[col].find_one({"username": key}, projection)
        if doc:
            return doc, {"_id": doc["_id"]}

    return None, None


def _coerce_bool(v, field_name: str):
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "yes", "y", "on"):
            return True
        if s in ("false", "0", "no", "n", "off"):
            return False
    raise HTTPException(status_code=422, detail=f"{field_name} must be boolean (true/false)")


def _user_normalize_and_validate(
    col: str,
    body: Dict[str, Any],
    *,
    is_create: bool,
    doc_id: Any = None,
):
    if col != "user":
        return

    # alias fields (UI dùng role/active)
    if "role" in body and "user_role" not in body:
        body["user_role"] = body.pop("role")
    if "active" in body and "is_active" not in body:
        body["is_active"] = body.pop("active")

    if is_create:
        # create: bắt buộc username/password
        u = str(body.get("username") or "").strip()
        pw = str(body.get("password") or "").strip()
        if not u:
            raise HTTPException(status_code=422, detail="username is required")
        if not pw:
            raise HTTPException(status_code=422, detail="password is required")

        existed = db[col].find_one({"username": u}, {"_id": 1})
        if existed:
            raise HTTPException(status_code=409, detail="Username already exists")

        # ✅ default CHỈ cho create
        body.setdefault("user_role", "user")
        body.setdefault("is_active", True)

    # update: nếu không gửi role/active thì giữ nguyên DB (không setdefault)

    # validate role nếu có gửi
    if "user_role" in body:
        role = str(body.get("user_role") or "").strip().lower()
        if role not in ("admin", "user"):
            raise HTTPException(status_code=422, detail="user_role must be 'admin' or 'user'")
        body["user_role"] = role

    # coerce active nếu có gửi
    if "is_active" in body:
        body["is_active"] = _coerce_bool(body["is_active"], "is_active")

    # nếu update username -> check trùng (khác _id)
    if (not is_create) and ("username" in body):
        u = str(body.get("username") or "").strip()
        if not u:
            raise HTTPException(status_code=422, detail="username cannot be empty")

        existed = db[col].find_one({"username": u}, {"_id": 1})
        if existed and doc_id is not None and existed["_id"] != doc_id:
            raise HTTPException(status_code=409, detail="Username already exists")

# ========================= COLLECTIONS =========================
@router.get("/collections", summary="Lấy tất cả Collections")
def get_all_collections():
    cols = db.list_collection_names()
    cols = [c for c in cols if not c.startswith("system.")]
    return cols


@router.post("/collections/{collection_name}", summary="Tạo một Collection")
def create_collection(collection_name: str = Path(...)):
    name = _normalize_collection_name(collection_name)
    if name in db.list_collection_names():
        raise HTTPException(status_code=409, detail=f"Collection '{name}' already exists")

    try:
        db.create_collection(name)
        return {"created": True, "collection": name}
    except CollectionInvalid as e:
        raise HTTPException(status_code=400, detail=f"Mongo create error: {e}") from e


@router.delete("/collections/{collection_name}", summary="Xoá một collection")
def delete_collection(collection_name: str = Path(...)):
    name = _normalize_collection_name(collection_name)
    _check_collection_exist(name)
    db.drop_collection(name)
    return {"deleted": True, "collection": name}


@router.put("/collections/{collection_name}/rename", summary="Đổi tên collection")
def rename_collection(collection_name: str = Path(...), new_name: str = Query(...)):
    old = _normalize_collection_name(collection_name)
    new = _normalize_collection_name(new_name)

    _check_collection_exist(old)

    if new in db.list_collection_names():
        raise HTTPException(status_code=409, detail=f"Target collection '{new}' already exists")

    try:
        db[old].rename(new, dropTarget=False)
        return {"renamed": True, "from": old, "to": new}
    except OperationFailure as e:
        raise HTTPException(status_code=500, detail=f"Mongo rename error: {e}") from e


# ========================= DOCUMENTS =========================
@router.get("/documents", summary="Lấy Documents trong Collection (có phân trang)")
def get_documents(
    collection_name: str = Query(...),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    col = _normalize_collection_name(collection_name)
    _check_collection_exist(col)

    total = db[col].count_documents({})
    docs = list(db[col].find({}).skip(offset).limit(limit))
    docs = jsonable_encoder(docs, custom_encoder={ObjectId: str})

    return {
        "collection": col,
        "total": total,
        "limit": limit,
        "offset": offset,
        "returned_count": len(docs),
        "documents": docs,
    }


@router.post("/documents/{collection_name}", summary="Thêm document vào collection (generic)")
def create_document(
    collection_name: str,
    request: Request,
    body: Dict[str, Any] = Body(...),
):
    col = _normalize_collection_name(collection_name)
    _check_collection_exist(col)

    actor = _get_actor(request)
    now = _now()

    body.pop("_id", None)

    # user normalize/validate
    _user_normalize_and_validate(col, body, is_create=True)

    # audit defaults
    body.setdefault("is_deleted", False)
    body.setdefault("deleted_at", None)

    body["created_at"] = now
    body["updated_at"] = now
    body["created_by"] = actor
    body["updated_by"] = actor

    # nếu user tạo is_deleted=true ngay từ đầu
    if body.get("is_deleted") is True:
        body["deleted_at"] = now

    result = db[col].insert_one(body)
    return {"inserted": True, "_id": str(result.inserted_id)}


@router.put("/documents/{collection_name}/{oid}", summary="Update document (generic)")
def update_document(
    collection_name: str,
    oid: str,
    request: Request,
    body: Dict[str, Any] = Body(...),
):
    col = _normalize_collection_name(collection_name)
    _check_collection_exist(col)

    actor = _get_actor(request)
    now = _now()

    body.pop("_id", None)
    body.pop("created_at", None)
    body.pop("created_by", None)

    if not body:
        raise HTTPException(status_code=422, detail="Not field change to updated")

    exist, id_filter = _find_one_by_any_key(col, oid, {"_id": 1, "is_deleted": 1, "username": 1})
    if not exist or not id_filter:
        raise HTTPException(status_code=404, detail=f"_id: '{oid}' not exist")

    # user normalize/validate (pass đúng id thật của doc)
    _user_normalize_and_validate(col, body, is_create=False, doc_id=exist["_id"])

    # handle soft delete toggle
    if "is_deleted" in body:
        body["is_deleted"] = _coerce_bool(body["is_deleted"], "is_deleted")
        body["deleted_at"] = now if body["is_deleted"] else None

    body["updated_at"] = now
    body["updated_by"] = actor

    r = db[col].update_one(id_filter, {"$set": body})
    return {"updated": True, "matched": r.matched_count, "modified": r.modified_count, "_id": oid}


@router.delete("/documents/{collection_name}/{oid}", summary="Delete document (generic)")
def delete_document(collection_name: str = Path(...), oid: str = Path(...)):
    col = _normalize_collection_name(collection_name)
    _check_collection_exist(col)

    exist, id_filter = _find_one_by_any_key(col, oid, {"_id": 1})
    if not exist or not id_filter:
        raise HTTPException(status_code=404, detail=f"_id: '{oid}' not exist")

    r = db[col].delete_one(id_filter)
    return {"deleted": True, "deleted_count": r.deleted_count, "_id": oid}


# ========================= BULK IMPORT =========================
@router.post("/import/xlsx", summary="Bulk import metadata (Mongo map IDs) + sync Postgres/Neo4j")
async def import_metadata_xlsx(
    request: Request,
    file: UploadFile = File(...),
    sync: bool = Query(True, description="Nếu true: sau import sẽ sync qua PostgreSQL + Neo4j"),
    category: str = Query("document", description="Giá trị gán cho *_Category trong Mongo"),
):
    """Import 1 file Excel (XLSX).

    - Hỗ trợ template cũ (import_key + *_ref)
    - Hỗ trợ template mới (map IDs trực tiếp, không cần *_ref)
    - Mặc định: auto sync sang PostgreSQL + Neo4j
    """

    actor = _get_actor(request)
    content = await file.read()
    if not content:
        raise HTTPException(status_code=422, detail="Empty file")

    try:
        return import_metadata_xlsx_bytes(content, actor=actor, category=category, do_sync=sync)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import failed: {e}") from e
