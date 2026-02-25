from __future__ import annotations

from sqlalchemy import text

from .postgre_client import get_engine


def ensure_keyword_embedding_column() -> None:
    """Ensure PostgreSQL table 'keyword' has column keyword_embedding REAL[].

    Fix cho lỗi:
      column "keyword_embedding" of relation "keyword" does not exist
    """

    engine = get_engine()
    try:
        with engine.begin() as conn:
            # table exists?
            t = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema='public' AND table_name='keyword'
                    LIMIT 1
                    """
                )
            ).first()
            if not t:
                return

            c = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='keyword' AND column_name='keyword_embedding'
                    LIMIT 1
                    """
                )
            ).first()
            if c:
                return

            conn.execute(text("ALTER TABLE keyword ADD COLUMN keyword_embedding REAL[]"))
    except Exception:
        # do not crash app startup
        return
