import os
from pathlib import Path
from dotenv import load_dotenv
from neo4j import GraphDatabase, Driver, Session
from typing import Generator, Optional

def _load_env():
    # backend/app/services -> parents[2] == backend/
    base_dir = Path(__file__).resolve().parents[2]
    for env_path in (
        base_dir / "core" / "config.env",
        base_dir / "core" / "config.env.example",
        base_dir / ".env",
    ):
        if env_path.exists():
            load_dotenv(env_path)
            break

def neo4j_driver():
    _load_env()
    URI = os.getenv("NEO4J_URI")
    USER = os.getenv("NEO4J_USER")
    PASSWORD = os.getenv("NEO4J_PASSWORD")

    _driver = GraphDatabase.driver(
        URI,
        auth=(USER, PASSWORD)
    )

    return _driver

def get_neo4j_session()  -> Generator[Session, None, None]:
    DB = os.getenv("NEO4J_DATABASE")
    driver = neo4j_driver()
    session = driver.session(database=DB)
    try:
        yield session
    finally:
        session.close()

