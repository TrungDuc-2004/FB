from __future__ import annotations

"""PostgreSQL client (SQLAlchemy).

Follow the same connection style as the user's reference project:
- Read PG_* from env (.env / core/config.env).
- Lazily create the SQLAlchemy Engine so the API can start even if Postgres is down.
- Surface connection errors when endpoints are called.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker


# 1) Base dùng chung toàn dự án (CHỈ 1 CÁI)
Base = declarative_base()


def _load_env() -> None:
    """Load env from common locations (first match wins)."""
    # backend/app/services -> parents[2] == backend/
    base_dir = Path(__file__).resolve().parents[2]
    for env_path in (
        base_dir / "core" / "config.env",
        base_dir / ".env",
        base_dir / "core" / "config.env.example",
    ):
        if env_path.exists():
            load_dotenv(env_path)
            break


_engine: Engine | None = None
_SessionLocal: sessionmaker | None = None


def get_engine() -> Engine:
    """Return a SQLAlchemy engine (lazy).

    The API can start even if Postgres is unavailable; errors will surface
    when endpoints are called.
    """
    global _engine
    if _engine is None:
        _load_env()

        host = os.getenv("PG_HOST", "127.0.0.1")
        port = os.getenv("PG_PORT", "5432")
        user = os.getenv("PG_USER", "postgres")
        password = os.getenv("PG_PASSWORD", "")  # allow empty password in dev
        name = os.getenv("PG_NAME", "Data")

        url = f"postgresql://{user}:{password}@{host}:{port}/{name}"
        _engine = create_engine(url, pool_pre_ping=True)
    return _engine


def get_session_local() -> sessionmaker:
    """Return a configured SessionLocal (lazy)."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
    return _SessionLocal


def get_db() -> Session:
    """FastAPI dependency."""
    SessionLocal = get_session_local()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
