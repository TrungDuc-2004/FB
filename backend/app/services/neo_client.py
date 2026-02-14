import os
from pathlib import Path
from typing import Generator, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase, Session, Driver
from neo4j.exceptions import ClientError


def _load_env() -> None:
    base_dir = Path(__file__).resolve().parents[2]  # backend/

    # Load defaults first (do NOT override existing env)
    for p in (
        base_dir / "core" / "config.env",
        base_dir / "core" / "config.env.example",
    ):
        if p.exists():
            load_dotenv(p, override=False)

    # Local overrides
    local_env = base_dir / ".env"
    if local_env.exists():
        load_dotenv(local_env, override=True)


def _normalize_uri(uri: str) -> str:
    uri = (uri or "").strip()
    if not uri:
        return "bolt://localhost:7687"

    # If running a local single-instance Neo4j Desktop DBMS,
    # avoid routing by converting neo4j:// -> bolt://
    if uri.startswith("neo4j://") and ("localhost" in uri or "127.0.0.1" in uri):
        uri = "bolt://" + uri[len("neo4j://"):]

    return uri


def neo4j_driver() -> Driver:
    _load_env()
    uri = _normalize_uri(os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "")

    return GraphDatabase.driver(uri, auth=(user, password))


def _open_session(driver: Driver, db_name: Optional[str]) -> Session:
    session = driver.session(database=db_name) if db_name else driver.session()
    # Force a round-trip so we fail fast with a clear error.
    session.run("RETURN 1").consume()
    return session


def get_neo4j_session() -> Generator[Session, None, None]:
    _load_env()

    db_name = (os.getenv("NEO4J_DATABASE") or "").strip() or None
    driver = neo4j_driver()
    session: Optional[Session] = None

    try:
        try:
            session = _open_session(driver, db_name)
        except ClientError as e:
            # If configured DB does not exist (common when NEO4J_DATABASE=Data in config.env),
            # fall back to the default DB.
            if getattr(e, "code", "") == "Neo.ClientError.Database.DatabaseNotFound":
                for fallback in ("neo4j", None):
                    try:
                        session = _open_session(driver, fallback)
                        break
                    except Exception:
                        session = None
                if session is None:
                    raise
            else:
                raise

        yield session

    finally:
        if session is not None:
            session.close()
        driver.close()
