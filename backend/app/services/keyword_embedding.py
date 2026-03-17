from __future__ import annotations

"""Keyword embedding utilities.

Mặc định dùng sentence-transformers với model `intfloat/multilingual-e5-base`.
- Dữ liệu lưu/index: prefix `passage: `
- Query semantic search: prefix `query: `

Có thể override bằng ENV nếu cần:
- KEYWORD_EMBEDDING_PROVIDER: sentence_transformers (default) | hash | openai
- KEYWORD_EMBEDDING_MODEL: default intfloat/multilingual-e5-base
- KEYWORD_EMBED_DIM: optional, fallback khi provider=hash/openai
- OPENAI_API_KEY, OPENAI_EMBEDDING_MODEL: khi provider=openai
"""

import hashlib
import math
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import List, Literal, Protocol


EmbedMode = Literal["passage", "query"]


class Embedder(Protocol):
    name: str
    dim: int

    def embed(self, text: str, *, mode: EmbedMode = "passage") -> List[float]: ...


def _clean(s: str | None) -> str:
    return "" if s is None else str(s).strip()


def _env_int(name: str, default: int) -> int:
    try:
        v = int(str(os.getenv(name, str(default))).strip())
        return v if v > 0 else default
    except Exception:
        return default


def _normalize(v: List[float]) -> List[float]:
    norm = math.sqrt(sum(float(x) * float(x) for x in v))
    if norm <= 0:
        return [float(x) for x in v]
    return [float(x) / norm for x in v]


@dataclass(frozen=True)
class HashEmbedder:
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

    def embed(self, text: str, *, mode: EmbedMode = "passage") -> List[float]:
        s = _clean(text)
        if not s:
            return [0.0] * self.dim

        prefix = "query: " if mode == "query" else "passage: "
        low = f"{prefix}{s}".lower()
        vec = [0.0] * self.dim

        tokens = self._token_re.findall(low)
        for t in tokens:
            self._acc(vec, t.encode("utf-8"), weight=1.0)

        compact = re.sub(r"\s+", " ", low).strip()
        if len(compact) >= 3:
            for i in range(len(compact) - 2):
                gram = compact[i : i + 3]
                self._acc(vec, gram.encode("utf-8"), weight=0.5)

        return _normalize(vec)


@dataclass(frozen=True)
class OpenAIEmbedder:
    model: str
    dim: int
    name: str = "openai"

    def embed(self, text: str, *, mode: EmbedMode = "passage") -> List[float]:
        s = _clean(text)
        if not s:
            return [0.0] * self.dim

        try:
            from openai import OpenAI  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "OpenAI provider selected but `openai` package is not installed. Run: pip install openai"
            ) from e

        prefix = "query: " if mode == "query" else "passage: "
        client = OpenAI()
        res = client.embeddings.create(model=self.model, input=f"{prefix}{s}")
        emb = list(res.data[0].embedding)
        return _normalize([float(x) for x in emb])


@dataclass(frozen=True)
class SentenceTransformerEmbedder:
    model: str
    dim: int
    name: str = "sentence-transformers"

    def embed(self, text: str, *, mode: EmbedMode = "passage") -> List[float]:
        s = _clean(text)
        if not s:
            return [0.0] * self.dim

        try:
            from sentence_transformers import SentenceTransformer  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "sentence-transformers chưa được cài. Run: pip install sentence-transformers torch"
            ) from e

        model = _load_sentence_transformer(self.model)
        prefix = "query: " if mode == "query" else "passage: "
        vec = model.encode(
            f"{prefix}{s}",
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        return [float(x) for x in vec.tolist()]


@lru_cache(maxsize=2)
def _load_sentence_transformer(model_name: str):
    from sentence_transformers import SentenceTransformer  # type: ignore

    return SentenceTransformer(model_name)


@lru_cache(maxsize=1)
def get_keyword_embedder() -> Embedder:
    provider = (_clean(os.getenv("KEYWORD_EMBEDDING_PROVIDER")) or "sentence_transformers").lower()
    model_name = _clean(os.getenv("KEYWORD_EMBEDDING_MODEL")) or "intfloat/multilingual-e5-base"
    dim = _env_int("KEYWORD_EMBED_DIM", 768)

    if provider in {"sentence_transformers", "sentence-transformers", "hf", "huggingface", "e5"}:
        return SentenceTransformerEmbedder(model=model_name, dim=dim, name=model_name)

    if provider == "openai":
        model = _clean(os.getenv("OPENAI_EMBEDDING_MODEL")) or "text-embedding-3-small"
        return OpenAIEmbedder(model=model, dim=dim)

    return HashEmbedder(dim=dim)


@lru_cache(maxsize=4096)
def embed_text_cached(text: str, *, mode: EmbedMode = "passage") -> List[float]:
    return get_keyword_embedder().embed(text, mode=mode)


@lru_cache(maxsize=4096)
def embed_keyword_cached(text: str) -> List[float]:
    return embed_text_cached(text, mode="passage")


@lru_cache(maxsize=4096)
def embed_query_cached(text: str) -> List[float]:
    return embed_text_cached(text, mode="query")
