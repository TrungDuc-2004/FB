import os
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio


def _load_env():
    # backend/app/services -> parents[2] = backend/
    base_dir = Path(__file__).resolve().parents[2]
    env_path = base_dir / "core" / "config.env"
    if env_path.exists():
        load_dotenv(env_path)


def get_minio_client() -> Minio:
    _load_env()

    endpoint = os.getenv("MINIO_ENDPOINT")  # vd: localhost:9000 (không có http://)
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    secure = (os.getenv("MINIO_SECURE", "false").strip().lower() == "true")

    if not endpoint or not access_key or not secret_key:
        raise RuntimeError("Thiếu MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY trong backend/core/config.env")

    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )
