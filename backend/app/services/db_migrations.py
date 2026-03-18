from __future__ import annotations

from sqlalchemy import text

from .postgre_client import get_engine


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name=:table_name
            LIMIT 1
            """
        ),
        {"table_name": table_name},
    ).first()
    return bool(row)


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name=:table_name
              AND column_name=:column_name
            LIMIT 1
            """
        ),
        {"table_name": table_name, "column_name": column_name},
    ).first()
    return bool(row)




def ensure_keyword_map_id_column() -> None:
    """Ensure PostgreSQL table 'keyword' has column map_id and backfill from chunk_id if needed."""
    engine = get_engine()
    try:
        with engine.begin() as conn:
            if not _table_exists(conn, "keyword"):
                return
            if not _column_exists(conn, "keyword", "map_id"):
                conn.execute(text("ALTER TABLE keyword ADD COLUMN map_id VARCHAR(64)"))
            if _column_exists(conn, "keyword", "chunk_id"):
                conn.execute(text("UPDATE keyword SET map_id = COALESCE(NULLIF(map_id, ''), chunk_id) WHERE chunk_id IS NOT NULL"))
            conn.execute(text("UPDATE keyword SET map_id = keyword_id WHERE map_id IS NULL"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_keyword_map_id ON keyword(map_id)"))
    except Exception:
        return


def ensure_keyword_embedding_column() -> None:
    """Ensure PostgreSQL table 'keyword' has column keyword_embedding REAL[]."""
    engine = get_engine()
    try:
        with engine.begin() as conn:
            if not _table_exists(conn, "keyword"):
                return
            if _column_exists(conn, "keyword", "keyword_embedding"):
                return
            conn.execute(text("ALTER TABLE keyword ADD COLUMN keyword_embedding REAL[]"))
    except Exception:
        return


def ensure_hierarchy_number_columns() -> None:
    """Ensure topic/lesson/chunk có các cột số thứ tự phục vụ search."""
    engine = get_engine()
    try:
        with engine.begin() as conn:
            if _table_exists(conn, "topic") and not _column_exists(conn, "topic", "topic_number"):
                conn.execute(text("ALTER TABLE topic ADD COLUMN topic_number INTEGER"))

            if _table_exists(conn, "lesson") and not _column_exists(conn, "lesson", "lesson_number"):
                conn.execute(text("ALTER TABLE lesson ADD COLUMN lesson_number INTEGER"))

            if _table_exists(conn, "chunk") and not _column_exists(conn, "chunk", "chunk_number"):
                conn.execute(text("ALTER TABLE chunk ADD COLUMN chunk_number INTEGER"))
    except Exception:
        return


def ensure_postgre_search_columns() -> None:
    ensure_keyword_map_id_column()
    ensure_keyword_embedding_column()
    ensure_hierarchy_number_columns()
