"""PostgreSQL admin + auth routes.

Endpoints chính:
- POST /admin/postgre/login
- POST /admin/postgre/register
- POST /admin/postgre/forgot-password
- GET  /admin/postgre/tables
- GET  /admin/postgre/tables/{table}/columns
- GET  /admin/postgre/tables/{table}/rows
- GET  /admin/postgre/tables/{table}/rows/{pk}
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path as FilePath
from typing import Any, Dict, List, Tuple, Annotated
from urllib.parse import parse_qs, quote_plus, urlparse
from uuid import uuid4

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, UploadFile, File, Form
from fastapi.encoders import jsonable_encoder
from minio.error import S3Error
from pydantic import BaseModel
from sqlalchemy import inspect
from sqlalchemy.orm import Session

from ..models import model_postgre as models
from ..services.minio_client import get_minio_client
from ..services.mongo_client import get_mongo_client
from ..services.postgre_client import get_db


router = APIRouter(prefix="/admin/postgre", tags=["PostgreSQL"])

db_dependency = Annotated[Session, Depends(get_db)]

_mongo_bundle = get_mongo_client()
_mongo_db = _mongo_bundle["db"]
_USERS_COLLECTION = "users"
_AVATAR_BUCKET = "images"
_AVATAR_FOLDER = "avatar"
_ALLOWED_AVATAR_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}


# ===================== HELPERS =====================
def _now_utc():
    return datetime.now(timezone.utc)


def _api_base(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _build_backend_open_url(request: Request, object_key_virtual: str) -> str:
    return f"{_api_base(request)}/admin/minio/open?object_key={quote_plus(object_key_virtual)}"


def _normalize_role(role: str | None) -> str:
    v = (role or "user").strip().lower()
    if v not in ("admin", "user"):
        raise HTTPException(status_code=422, detail="user_role must be 'admin' or 'user'")
    return v


def _normalize_active(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s in ("true", "1", "yes", "y", "on"):
            return True
        if s in ("false", "0", "no", "n", "off"):
            return False
    raise HTTPException(status_code=422, detail="is_active must be boolean (true/false)")


def _ensure_users_collection():
    if _USERS_COLLECTION not in _mongo_db.list_collection_names():
        _mongo_db.create_collection(_USERS_COLLECTION)


def _find_user_for_profile(db: Session, user_id: str | None, username: str | None):
    if user_id:
        user = db.query(models.User).filter(models.User.user_id == user_id).first()
        if user:
            return user
    if username:
        return db.query(models.User).filter(models.User.username == username).first()
    return None


def _find_mongo_user_doc(user) -> Dict[str, Any] | None:
    _ensure_users_collection()

    if getattr(user, "mongo_id", None):
        try:
            doc = _mongo_db[_USERS_COLLECTION].find_one({"_id": ObjectId(str(user.mongo_id))})
            if doc:
                return doc
        except Exception:
            pass

    return _mongo_db[_USERS_COLLECTION].find_one({"username": str(user.username)})


def _extract_virtual_object_key_from_url(value: str | None) -> str | None:
    if not value or not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.startswith(f"{_AVATAR_BUCKET}/"):
        return text

    try:
        parsed = urlparse(text)
    except Exception:
        return None

    if not parsed.query:
        return None

    qs = parse_qs(parsed.query)
    key = (qs.get("object_key") or [None])[0]
    if key and isinstance(key, str):
        return key.strip() or None
    return None


def _avatar_virtual_key(object_name: str) -> str:
    return f"{_AVATAR_BUCKET}/{_AVATAR_FOLDER}/{object_name}"


def _resolve_avatar_info(mongo_doc: Dict[str, Any] | None, request: Request | None = None) -> Tuple[str, str]:
    avatar_url = ""
    avatar_object_key = ""

    if mongo_doc:
        avatar_object_key = str(mongo_doc.get("avatar_object_key") or "").strip()
        avatar_url = str(
            mongo_doc.get("avatar_url")
            or mongo_doc.get("avatar_data_url")
            or ""
        ).strip()

    if not avatar_object_key and avatar_url:
        parsed_key = _extract_virtual_object_key_from_url(avatar_url)
        if parsed_key:
            avatar_object_key = parsed_key

    if avatar_object_key and request and (not avatar_url or avatar_url.startswith("data:")):
        avatar_url = _build_backend_open_url(request, avatar_object_key)

    return avatar_url, avatar_object_key


def _load_mongo_profile(user, request: Request | None = None) -> Dict[str, Any]:
    mongo_doc = _find_mongo_user_doc(user)
    avatar_url, avatar_object_key = _resolve_avatar_info(mongo_doc, request)

    return {
        "user_id": str(user.user_id),
        "username": str(user.username),
        "role": str(getattr(user, "user_role", "user")),
        "is_active": bool(getattr(user, "is_active", True)),
        "mongo_id": getattr(user, "mongo_id", None),
        "avatar_url": avatar_url,
        "avatar_object_key": avatar_object_key,
        "avatar_data_url": avatar_url,
    }


def _upsert_user_profile_in_mongo(user, old_username: str, set_fields: Dict[str, Any]):
    _ensure_users_collection()
    now = _now_utc()

    mongo_filter = {"username": old_username}
    if getattr(user, "mongo_id", None):
        try:
            mongo_filter = {"_id": ObjectId(str(user.mongo_id))}
        except Exception:
            mongo_filter = {"username": old_username}

    mongo_set = {
        "username": str(user.username),
        "user_role": getattr(user, "user_role", "user"),
        "is_active": getattr(user, "is_active", True),
        "updated_at": now,
        "updated_by": old_username,
        **set_fields,
    }

    mongo_result = _mongo_db[_USERS_COLLECTION].update_one(
        mongo_filter,
        {
            "$set": mongo_set,
            "$setOnInsert": {
                "created_at": now,
                "created_by": old_username,
                "is_deleted": False,
                "deleted_at": None,
            },
        },
        upsert=True,
    )

    if getattr(user, "mongo_id", None) is None and getattr(mongo_result, "upserted_id", None):
        user.mongo_id = str(mongo_result.upserted_id)


def _ensure_avatar_bucket_and_folder(client) -> None:
    try:
        if not client.bucket_exists(_AVATAR_BUCKET):
            client.make_bucket(_AVATAR_BUCKET)
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"Không tạo được bucket '{_AVATAR_BUCKET}': {e}") from e

    try:
        client.stat_object(_AVATAR_BUCKET, f"{_AVATAR_FOLDER}/")
    except S3Error:
        try:
            client.put_object(
                _AVATAR_BUCKET,
                f"{_AVATAR_FOLDER}/",
                data=io.BytesIO(b""),
                length=0,
                content_type="application/octet-stream",
            )
        except S3Error as e:
            raise HTTPException(status_code=500, detail=f"Không tạo được folder '{_AVATAR_FOLDER}': {e}") from e


def _avatar_extension(upload: UploadFile) -> str:
    ext = FilePath(upload.filename or "").suffix.lower()
    if ext in _ALLOWED_AVATAR_EXTS:
        return ext

    content_type = (upload.content_type or "").lower().strip()
    mapping = {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
    }
    inferred = mapping.get(content_type)
    if inferred:
        return inferred

    raise HTTPException(status_code=422, detail="Ảnh đại diện chỉ hỗ trợ JPG, PNG, WEBP hoặc GIF")


def _delete_minio_avatar_object(object_key_virtual: str | None):
    key_virtual = (object_key_virtual or "").strip()
    if not key_virtual:
        return
    key = _extract_virtual_object_key_from_url(key_virtual) or key_virtual
    if not key.startswith(f"{_AVATAR_BUCKET}/"):
        return

    bucket, object_name = key.split("/", 1)
    if not object_name:
        return

    try:
        client = get_minio_client()
        client.remove_object(bucket, object_name)
    except Exception:
        pass


# ===================== AUTH =====================
class LoginIn(BaseModel):
    username: str
    password: str


class RegisterIn(BaseModel):
    username: str
    password: str
    confirm_password: str
    user_role: str = "user"
    is_active: bool = True


class ForgotPasswordIn(BaseModel):
    username: str
    new_password: str
    confirm_password: str


class ProfileUpdateIn(BaseModel):
    user_id: str | None = None
    current_username: str | None = None
    new_username: str | None = None
    new_password: str | None = None
    confirm_password: str | None = None


@router.get("/profile", summary="Get profile of current account")
def get_profile(
    request: Request,
    db: db_dependency,
    user_id: str | None = Query(None),
    username: str | None = Query(None),
):
    user = _find_user_for_profile(db, user_id, username)
    if not user:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản")
    return _load_mongo_profile(user, request)


@router.put("/profile", summary="Update profile of current account")
def update_profile(payload: ProfileUpdateIn, db: db_dependency):
    user_id = (payload.user_id or "").strip() or None
    current_username = (payload.current_username or "").strip() or None
    new_username = (payload.new_username or "").strip() or None
    new_password = (payload.new_password or "").strip() or None
    confirm_password = (payload.confirm_password or "").strip() or None

    user = _find_user_for_profile(db, user_id, current_username)
    if not user:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản")

    target_username = new_username or str(user.username)
    if not target_username:
        raise HTTPException(status_code=422, detail="Tên tài khoản không được để trống")

    if target_username != str(user.username):
        existed_pg = db.query(models.User).filter(models.User.username == target_username).first()
        if existed_pg and str(existed_pg.user_id) != str(user.user_id):
            raise HTTPException(status_code=409, detail="Tên đăng nhập đã tồn tại")

    if new_password:
        if len(new_password) < 6:
            raise HTTPException(status_code=422, detail="Mật khẩu mới phải có ít nhất 6 ký tự")
        if new_password != confirm_password:
            raise HTTPException(status_code=422, detail="Mật khẩu xác nhận không khớp")

    old_username = str(user.username)
    old_password = str(user.password)

    user.username = target_username
    if new_password:
        user.password = new_password

    try:
        db.commit()
        db.refresh(user)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Cập nhật PostgreSQL thất bại: {e}") from e

    try:
        mongo_set = {}
        if new_password:
            mongo_set["password"] = new_password
        _upsert_user_profile_in_mongo(user, old_username, mongo_set)
        db.commit()
        db.refresh(user)
    except Exception as e:
        user.username = old_username
        user.password = old_password
        try:
            db.commit()
        except Exception:
            db.rollback()
        raise HTTPException(status_code=500, detail=f"Cập nhật MongoDB thất bại: {e}") from e

    return _load_mongo_profile(user)


@router.post("/profile/avatar", summary="Upload avatar lên MinIO rồi lưu URL vào MongoDB")
async def upload_profile_avatar(
    request: Request,
    db: db_dependency,
    file: UploadFile = File(...),
    user_id: str | None = Form(None),
    current_username: str | None = Form(None),
):
    resolved_user_id = (user_id or "").strip() or None
    resolved_username = (current_username or "").strip() or None

    user = _find_user_for_profile(db, resolved_user_id, resolved_username)
    if not user:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản")

    if not (file.content_type or "").lower().startswith("image/"):
        raise HTTPException(status_code=422, detail="Vui lòng chọn file ảnh hợp lệ")

    ext = _avatar_extension(file)
    client = get_minio_client()
    _ensure_avatar_bucket_and_folder(client)

    object_name = f"{_AVATAR_FOLDER}/{str(user.user_id)}-{uuid4().hex}{ext}"
    object_key_virtual = f"{_AVATAR_BUCKET}/{object_name}"
    avatar_url = _build_backend_open_url(request, object_key_virtual)

    old_username = str(user.username)
    mongo_doc = _find_mongo_user_doc(user)
    _old_avatar_url, old_avatar_object_key = _resolve_avatar_info(mongo_doc, request)

    try:
        client.put_object(
            bucket_name=_AVATAR_BUCKET,
            object_name=object_name,
            data=file.file,
            length=-1,
            part_size=5 * 1024 * 1024,
            content_type=file.content_type or "application/octet-stream",
        )
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"Upload ảnh đại diện lên MinIO thất bại: {e}") from e
    finally:
        try:
            await file.close()
        except Exception:
            pass

    try:
        _upsert_user_profile_in_mongo(
            user,
            old_username,
            {
                "avatar_url": avatar_url,
                "avatar_object_key": object_key_virtual,
                "avatar_bucket": _AVATAR_BUCKET,
                "avatar_folder": _AVATAR_FOLDER,
            },
        )
        db.commit()
        db.refresh(user)
    except Exception as e:
        _delete_minio_avatar_object(object_key_virtual)
        raise HTTPException(status_code=500, detail=f"Lưu URL ảnh đại diện vào MongoDB thất bại: {e}") from e

    if old_avatar_object_key and old_avatar_object_key != object_key_virtual:
        _delete_minio_avatar_object(old_avatar_object_key)

    return _load_mongo_profile(user, request)


@router.delete("/profile/avatar", summary="Xóa avatar trong MinIO và MongoDB")
def delete_profile_avatar(
    request: Request,
    db: db_dependency,
    user_id: str | None = Query(None),
    username: str | None = Query(None),
):
    user = _find_user_for_profile(db, (user_id or "").strip() or None, (username or "").strip() or None)
    if not user:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản")

    old_username = str(user.username)
    mongo_doc = _find_mongo_user_doc(user)
    _old_avatar_url, old_avatar_object_key = _resolve_avatar_info(mongo_doc, request)

    try:
        _ensure_users_collection()
        mongo_filter = {"username": old_username}
        if getattr(user, "mongo_id", None):
            try:
                mongo_filter = {"_id": ObjectId(str(user.mongo_id))}
            except Exception:
                mongo_filter = {"username": old_username}

        _mongo_db[_USERS_COLLECTION].update_one(
            mongo_filter,
            {
                "$set": {
                    "updated_at": _now_utc(),
                    "updated_by": old_username,
                },
                "$unset": {
                    "avatar_url": "",
                    "avatar_object_key": "",
                    "avatar_bucket": "",
                    "avatar_folder": "",
                    "avatar_data_url": "",
                },
            },
            upsert=False,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Xóa URL ảnh đại diện trong MongoDB thất bại: {e}") from e

    if old_avatar_object_key:
        _delete_minio_avatar_object(old_avatar_object_key)

    return _load_mongo_profile(user, request)


@router.post("/login", summary="Login by PostgreSQL table 'user'")
def login(payload: LoginIn, db: db_dependency):
    u = (payload.username or "").strip()
    pw = (payload.password or "").strip()

    if not u or not pw:
        raise HTTPException(status_code=422, detail="username/password is required")

    user = db.query(models.User).filter(models.User.username == u).first()
    if not user or (getattr(user, "password", "") or "") != pw:
        raise HTTPException(status_code=401, detail="Tên đăng nhập hoặc mật khẩu không đúng")

    if hasattr(user, "is_active") and user.is_active is False:
        raise HTTPException(status_code=403, detail="Tài khoản đang bị vô hiệu hoá")

    return {
        "user_id": str(user.user_id),
        "username": str(user.username),
        "role": str(getattr(user, "user_role", "user")),
        "mongo_id": getattr(user, "mongo_id", None),
    }


@router.post("/register", summary="Register user in PostgreSQL and sync to MongoDB")
def register(payload: RegisterIn, db: db_dependency):
    username = (payload.username or "").strip()
    password = (payload.password or "").strip()
    confirm_password = (payload.confirm_password or "").strip()
    role = _normalize_role(payload.user_role)
    is_active = _normalize_active(payload.is_active, default=True)

    if not username:
        raise HTTPException(status_code=422, detail="username is required")
    if not password:
        raise HTTPException(status_code=422, detail="password is required")
    if len(password) < 6:
        raise HTTPException(status_code=422, detail="Mật khẩu phải có ít nhất 6 ký tự")
    if password != confirm_password:
        raise HTTPException(status_code=422, detail="Mật khẩu xác nhận không khớp")

    existed_pg = db.query(models.User).filter(models.User.username == username).first()
    if existed_pg:
        raise HTTPException(status_code=409, detail="Tên đăng nhập đã tồn tại")

    _ensure_users_collection()
    existed_mongo = _mongo_db[_USERS_COLLECTION].find_one({"username": username}, {"_id": 1})
    if existed_mongo:
        raise HTTPException(status_code=409, detail="Tên đăng nhập đã tồn tại trong MongoDB")

    mongo_doc_id = None
    now = _now_utc()
    try:
        mongo_payload = {
            "username": username,
            "password": password,
            "user_role": role,
            "is_active": is_active,
            "is_deleted": False,
            "deleted_at": None,
            "created_at": now,
            "updated_at": now,
            "created_by": username,
            "updated_by": username,
            "avatar_url": "",
            "avatar_object_key": "",
        }
        mongo_result = _mongo_db[_USERS_COLLECTION].insert_one(mongo_payload)
        mongo_doc_id = str(mongo_result.inserted_id)

        user = models.User(
            user_id=str(uuid4()),
            username=username,
            password=password,
            user_role=role,
            is_active=is_active,
            mongo_id=mongo_doc_id,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        if mongo_doc_id:
            try:
                _mongo_db[_USERS_COLLECTION].delete_one({"_id": ObjectId(mongo_doc_id)})
            except Exception:
                pass
        raise HTTPException(status_code=500, detail=f"Đăng ký thất bại: {e}") from e

    return {
        "created": True,
        "user_id": str(user.user_id),
        "username": str(user.username),
        "role": str(user.user_role),
        "is_active": bool(user.is_active),
        "mongo_id": mongo_doc_id,
    }


@router.post("/forgot-password", summary="Reset password in PostgreSQL and MongoDB")
def forgot_password(payload: ForgotPasswordIn, db: db_dependency):
    username = (payload.username or "").strip()
    new_password = (payload.new_password or "").strip()
    confirm_password = (payload.confirm_password or "").strip()

    if not username:
        raise HTTPException(status_code=422, detail="username is required")
    if not new_password:
        raise HTTPException(status_code=422, detail="new_password is required")
    if len(new_password) < 6:
        raise HTTPException(status_code=422, detail="Mật khẩu mới phải có ít nhất 6 ký tự")
    if new_password != confirm_password:
        raise HTTPException(status_code=422, detail="Mật khẩu xác nhận không khớp")

    user = db.query(models.User).filter(models.User.username == username).first()
    if not user:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản")

    old_password = user.password
    user.password = new_password

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Cập nhật PostgreSQL thất bại: {e}") from e

    _ensure_users_collection()
    now = _now_utc()
    try:
        mongo_filter = {"username": username}
        if getattr(user, "mongo_id", None):
            try:
                mongo_filter = {"_id": ObjectId(str(user.mongo_id))}
            except Exception:
                mongo_filter = {"username": username}

        mongo_result = _mongo_db[_USERS_COLLECTION].update_one(
            mongo_filter,
            {"$set": {"password": new_password, "updated_at": now, "updated_by": username}},
            upsert=False,
        )

        if mongo_result.matched_count == 0:
            _mongo_db[_USERS_COLLECTION].update_one(
                {"username": username},
                {
                    "$set": {
                        "username": username,
                        "password": new_password,
                        "user_role": getattr(user, "user_role", "user"),
                        "is_active": getattr(user, "is_active", True),
                        "updated_at": now,
                        "updated_by": username,
                    },
                    "$setOnInsert": {
                        "created_at": now,
                        "created_by": username,
                        "is_deleted": False,
                        "deleted_at": None,
                    },
                },
                upsert=True,
            )
    except Exception as e:
        user.password = old_password
        try:
            db.commit()
        except Exception:
            db.rollback()
        raise HTTPException(status_code=500, detail=f"Cập nhật MongoDB thất bại: {e}") from e

    return {"updated": True, "username": username, "synced_mongo": True}


# ===================== READ-ONLY TABLE API =====================
TABLE_MODEL_MAP = {
    "class": models.Class,
    "subject": models.Subject,
    "topic": models.Topic,
    "lesson": models.Lesson,
    "chunk": models.Chunk,
    "keyword": models.Keyword,
    "user": models.User,
}


def _get_model(table_name: str):
    if table_name not in TABLE_MODEL_MAP:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not allowed")
    return TABLE_MODEL_MAP[table_name]



def _pk_cols(model) -> List[str]:
    return [c.name for c in model.__table__.primary_key.columns]



def _row_to_dict(model, row) -> Dict[str, Any]:
    data = {col.name: getattr(row, col.name) for col in model.__table__.columns}
    pks = _pk_cols(model)
    if len(pks) == 1:
        data["_pk"] = str(getattr(row, pks[0]))
    else:
        data["_pk"] = "::".join(str(getattr(row, k)) for k in pks)
    return jsonable_encoder(data)



def _parse_pk(model, pk: str) -> Tuple[str, ...]:
    pks = _pk_cols(model)
    if len(pks) == 1:
        return (pk,)
    parts = pk.split("::")
    if len(parts) != len(pks):
        raise HTTPException(
            status_code=422,
            detail=f"Composite PK required: use '{'::'.join(pks)}' joined by '::' (example: a::b)",
        )
    return tuple(parts)



def _get_one_by_pk(db: Session, model, pk: str):
    pks = _pk_cols(model)
    values = _parse_pk(model, pk)
    q = db.query(model)
    for k, v in zip(pks, values):
        q = q.filter(getattr(model, k) == v)
    obj = q.first()
    if not obj:
        raise HTTPException(status_code=404, detail="Row not found")
    return obj


@router.get("/tables", summary="List allowed tables")
def list_tables(db: db_dependency):
    inspector = inspect(db.bind)
    existing = set(inspector.get_table_names(schema="public"))
    tables = sorted([t for t in TABLE_MODEL_MAP.keys() if t in existing])
    return {"tables": tables}


@router.get("/tables/{table_name}/columns", summary="Get columns of a table")
def table_columns(table_name: str = Path(...), db: db_dependency = None):
    model = _get_model(table_name)
    return {"table_name": table_name, "columns": [c.name for c in model.__table__.columns]}


@router.get("/tables/{table_name}/rows", summary="List rows with paging (read-only)")
def list_rows(
    table_name: str = Path(...),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: db_dependency = None,
):
    model = _get_model(table_name)
    try:
        total = db.query(model).count()
        rows = db.query(model).offset(offset).limit(limit).all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query table '{table_name}' failed: {e}")

    return {
        "table_name": table_name,
        "total": total,
        "limit": limit,
        "offset": offset,
        "count": len(rows),
        "rows": [_row_to_dict(model, r) for r in rows],
    }


@router.get("/tables/{table_name}/rows/{pk}", summary="Get one row by PK (read-only)")
def get_row(
    table_name: str = Path(...),
    pk: str = Path(..., description="PK string"),
    db: db_dependency = None,
):
    model = _get_model(table_name)
    obj = _get_one_by_pk(db, model, pk)
    return {"table_name": table_name, "row": _row_to_dict(model, obj)}
