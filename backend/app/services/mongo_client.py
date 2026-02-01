import os
from pathlib import Path
from dotenv import load_dotenv

from pymongo import MongoClient

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

def get_mongo_client():
    _load_env()
    URI = os.getenv("MONGODB_URI")
    DB = os.getenv("MONGODB_DB")

    missing = [k for k, v in {
        "MONGODB_URI": URI,
        "MONGODB_DB": DB
    }.items() if not v ]

    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)} (check your config.env file)")

    client = MongoClient(URI)
    db = client[DB]
    return {"client": client, "db": db}
