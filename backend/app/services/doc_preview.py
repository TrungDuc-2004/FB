from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Dict
from urllib.parse import quote, unquote, urlparse
from urllib.request import Request, urlopen

from minio.error import S3Error

from .minio_client import get_minio_client


def _minio_public_base() -> str:
    public = (os.getenv("MINIO_PUBLIC_BASE_URL") or "").strip()
    if public:
        return public.rstrip("/")

    endpoint = (os.getenv("MINIO_ENDPOINT") or "127.0.0.1:9000").strip()
    secure = (os.getenv("MINIO_SECURE", "false").strip().lower() == "true")
    scheme = "https" if secure else "http"
    return f"{scheme}://{endpoint}".rstrip("/")


def _http_url(bucket: str, key: str) -> str:
    base = _minio_public_base()
    encoded = quote((key or "").lstrip("/"), safe="/")
    return f"{base}/{bucket}/{encoded}"


def _guess_bucket_key(url: str) -> tuple[str, str]:
    p = urlparse(url)
    # path: /<bucket>/<key>
    parts = [x for x in (p.path or "").split("/") if x]
    if len(parts) < 2:
        raise ValueError("chunkUrl không đúng định dạng http://<minio>/<bucket>/<key>")
    bucket = parts[0]
    key = "/".join(parts[1:])
    key = unquote(key)
    return bucket, key


def _ext_from_key(key: str) -> str:
    base = (key or "").split("?")[0]
    if "." not in base:
        return ""
    return base.rsplit(".", 1)[-1].lower()


def _soffice_cmd() -> str:
    # Allow explicit path for Windows users
    p = (os.getenv("SOFFICE_PATH") or "").strip()
    if p:
        return p
    return "soffice"  # should be in PATH if LibreOffice installed


def _convert_to_pdf(input_path: str, out_dir: str) -> str:
    cmd = _soffice_cmd()

    # LibreOffice creates PDF in out_dir with same base name.
    proc = subprocess.run(
        [
            cmd,
            "--headless",
            "--nologo",
            "--nofirststartwizard",
            "--convert-to",
            "pdf",
            "--outdir",
            out_dir,
            input_path,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if proc.returncode != 0:
        raise RuntimeError(f"LibreOffice convert failed: {proc.stderr or proc.stdout}")

    base = Path(input_path).stem
    pdf_path = Path(out_dir) / f"{base}.pdf"

    if pdf_path.exists():
        return str(pdf_path)

    # fallback: find any pdf created
    pdfs = list(Path(out_dir).glob("*.pdf"))
    if not pdfs:
        raise RuntimeError("LibreOffice không tạo ra file PDF")
    return str(pdfs[0])


def get_view_url(*, original_url: str, chunk_id: str) -> Dict[str, str]:
    """Return a URL that the browser can preview.

    - PDF/Image/Video: return original
    - DOCX/PPTX: convert to PDF and cache in MinIO
    """
    bucket, key = _guess_bucket_key(original_url)
    ext = _ext_from_key(key)

    if ext in ("pdf", "png", "jpg", "jpeg", "webp", "gif", "mp4", "webm", "ogg"):
        return {"viewUrl": original_url, "originalUrl": original_url, "ext": ext}

    # Convert office -> pdf
    # Put under: <classFolder>/previews/<chunk_id>.pdf
    parts = [p for p in (key or "").split("/") if p]
    class_folder = parts[0] if parts else "preview"
    preview_key = f"{class_folder}/previews/{chunk_id}.pdf"

    mc = get_minio_client()

    # already exists?
    try:
        mc.stat_object(bucket, preview_key)
        return {"viewUrl": _http_url(bucket, preview_key), "originalUrl": original_url, "ext": "pdf"}
    except S3Error:
        pass

    # download original
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        in_path = td_path / f"{chunk_id}.{ext or 'bin'}"

        req = Request(original_url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req) as r, open(in_path, "wb") as f:
            shutil.copyfileobj(r, f)

        # convert
        try:
            pdf_path = _convert_to_pdf(str(in_path), str(td_path))
        except FileNotFoundError as e:
            raise RuntimeError(
                "Không tìm thấy LibreOffice (soffice). Bạn cần cài LibreOffice và thêm vào PATH, hoặc set SOFFICE_PATH."
            ) from e

        # upload
        size = os.path.getsize(pdf_path)
        with open(pdf_path, "rb") as f:
            mc.put_object(
                bucket,
                preview_key,
                f,
                length=size,
                content_type="application/pdf",
            )

    return {"viewUrl": _http_url(bucket, preview_key), "originalUrl": original_url, "ext": "pdf"}
