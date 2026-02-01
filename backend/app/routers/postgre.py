# app/routers/admin_postgre.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi.encoders import jsonable_encoder
from sqlalchemy import inspect
from sqlalchemy.orm import Session
from typing import Any, Dict, List, Tuple, Annotated
from pydantic import BaseModel

from ..services.postgre_client import get_db
from ..models import model_postgre as models

router = APIRouter(prefix="/admin/postgre", tags=["PostgreSQL"])


db_dependency = Annotated[Session, Depends(get_db)]


# ===================== LOGIN (PostgreSQL) =====================
class LoginIn(BaseModel):
    username: str
    password: str


@router.post("/login", summary="Login by PostgreSQL user table")
def login(payload: LoginIn, db: db_dependency):
    u = (payload.username or "").strip()
    pw = (payload.password or "").strip()

    if not u or not pw:
        raise HTTPException(status_code=422, detail="username/password is required")

    user = db.query(models.User).filter(models.User.username == u).first()
    if not user or (user.password or "") != pw:
        raise HTTPException(status_code=401, detail="Tên đăng nhập hoặc mật khẩu không đúng")

    # nếu DB có cột is_active thì chặn user disabled
    if hasattr(user, "is_active") and user.is_active is False:
        raise HTTPException(status_code=403, detail="Tài khoản đang bị vô hiệu hoá")

    return {
        "user_id": str(user.user_id),
        "username": str(user.username),
        "role": str(user.user_role),
    }


# ===================== READ-ONLY TABLE API =====================
TABLE_MODEL_MAP = {
    "class": models.Class,
    "subject": models.Subject,
    "topic": models.Topic,
    "lesson": models.Lesson,
    "chunk": models.Chunk,
    "keyword": models.Keyword,  # PK ghép (chunk_id, keyword_name)
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

    # UI-friendly PK string
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
    pk: str = Path(..., description="PK string. For keyword use chunk_id::keyword_name"),
    db: db_dependency = None,
):
    model = _get_model(table_name)
    obj = _get_one_by_pk(db, model, pk)
    return {"table_name": table_name, "row": _row_to_dict(model, obj)}
