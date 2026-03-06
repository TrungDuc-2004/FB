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

from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Annotated
from uuid import uuid4

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from sqlalchemy import inspect
from sqlalchemy.orm import Session

from ..models import model_postgre as models
from ..services.mongo_client import get_mongo_client
from ..services.postgre_client import get_db


router = APIRouter(prefix="/admin/postgre", tags=["PostgreSQL"])

db_dependency = Annotated[Session, Depends(get_db)]

_mongo_bundle = get_mongo_client()
_mongo_db = _mongo_bundle["db"]
_USERS_COLLECTION = "users"


# ===================== HELPERS =====================
def _now_utc():
    return datetime.now(timezone.utc)


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
