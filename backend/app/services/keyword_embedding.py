from __future__ import annotations

"""Keyword embedding utilities.

Mục tiêu:
- Không phụ thuộc thư viện ML nặng (chạy được ngay chỉ với stdlib).
- Có thể chuyển sang provider khác (OpenAI / local model) qua ENV.

ENV hỗ trợ:
- KEYWORD_EMBEDDING_PROVIDER: "hash" (default) | "openai"
- KEYWORD_EMBED_DIM: số chiều embedding (default 256)
- OPENAI_API_KEY, OPENAI_EMBEDDING_MODEL (khi provider=openai)
"""

import hashlib
import math
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import List, Protocol


class Embedder(Protocol):
    name: str
    dim: int

    def embed(self, text: str) -> List[float]: ...


def _clean(s: str | None) -> str:
    return "" if s is None else str(s).strip()


def _env_int(name: str, default: int) -> int:
    try:
        v = int(str(os.getenv(name, str(default))).strip())
        return v if v > 0 else default
    except Exception:
        return default


@dataclass(frozen=True)
class HashEmbedder:
    """Embedding bằng hashing (stable) – dùng được ngay không cần model.

    Đây KHÔNG phải semantic embedding chuẩn như SBERT/OpenAI.
    Nhưng đủ để bạn build pipeline (Mongo -> PG -> Neo4j) và test end-to-end.
    """

    dim: int = 256
    name: str = "hash"

    _token_re = re.compile(r"[0-9A-Za-zÀ-ỹ]+", flags=re.UNICODE)

    def _acc(self, vec: list[float], key: bytes, weight: float = 1.0) -> None:
        h = hashlib.sha256(key).digest()
        for i in range(0, len(h), 4):
            chunk = h[i : i + 4]
            if len(chunk) < 4:
                break
            idx = int.from_bytes(chunk, "little", signed=False) % self.dim
            sign = -1.0 if (h[i] & 1) else 1.0
            vec[idx] += sign * weight

    def embed(self, text: str) -> List[float]:
        s = _clean(text)
        if not s:
            return [0.0] * self.dim

        vec = [0.0] * self.dim

        low = s.lower()
        tokens = self._token_re.findall(low)
        for t in tokens:
            self._acc(vec, t.encode("utf-8"), weight=1.0)

        compact = re.sub(r"\s+", " ", low).strip()
        if len(compact) >= 3:
            for i in range(len(compact) - 2):
                gram = compact[i : i + 3]
                self._acc(vec, gram.encode("utf-8"), weight=0.5)

        norm = math.sqrt(sum(x * x for x in vec))
        if norm > 0:
            vec = [x / norm for x in vec]
        return vec


@dataclass(frozen=True)
class OpenAIEmbedder:
    """OpenAI embedding provider (optional).

    Cần cài thêm package `openai` và set OPENAI_API_KEY.
    """

    model: str
    dim: int
    name: str = "openai"

    def embed(self, text: str) -> List[float]:
        s = _clean(text)
        if not s:
            return [0.0] * self.dim

        try:
            from openai import OpenAI  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "OpenAI provider selected but `openai` package is not installed. Run: pip install openai"
            ) from e

        client = OpenAI()
        res = client.embeddings.create(model=self.model, input=s)
        emb = list(res.data[0].embedding)
        return [float(x) for x in emb]


@lru_cache(maxsize=1)
def get_keyword_embedder() -> Embedder:
    provider = (_clean(os.getenv("KEYWORD_EMBEDDING_PROVIDER")) or "hash").lower()
    dim = _env_int("KEYWORD_EMBED_DIM", 256)

    if provider == "openai":
        model = _clean(os.getenv("OPENAI_EMBEDDING_MODEL")) or "text-embedding-3-small"
        return OpenAIEmbedder(model=model, dim=dim)

    return HashEmbedder(dim=dim)


@lru_cache(maxsize=4096)
def embed_keyword_cached(text: str) -> List[float]:
    """Cache embedding theo keyword text để giảm thời gian khi upsert lại."""
    return get_keyword_embedder().embed(text)
