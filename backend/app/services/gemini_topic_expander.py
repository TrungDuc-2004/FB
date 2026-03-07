from __future__ import annotations

import json
import os
import re
import threading
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dotenv import load_dotenv

_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.I | re.S)
_JSON_BLOCK_RE = re.compile(r"(\{.*\}|\[.*\])", re.S)
_SPLIT_RE = re.compile(r"[,;\n\r]+")
_GEMINI_SLOT_RE = re.compile(r"^GEMINI_API_KEY_(\d+)$")
_GOOGLE_SLOT_RE = re.compile(r"^GOOGLE_API_KEY_(\d+)$")

_ENV_LOADED = False
_ROUND_ROBIN_INDEX = 0
_ROUND_ROBIN_LOCK = threading.Lock()


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _candidate_env_paths() -> List[Path]:
    here = Path(__file__).resolve()
    paths = [Path.cwd() / ".env", Path.cwd() / "backend" / ".env"]
    parents = list(here.parents)
    if len(parents) >= 3:
        paths.append(parents[2] / ".env")
    if len(parents) >= 4:
        paths.append(parents[3] / ".env")
    return paths


def _load_env_once() -> List[str]:
    global _ENV_LOADED
    checked: List[str] = []
    if _ENV_LOADED:
        return checked
    seen = set()
    for path in _candidate_env_paths():
        try:
            path = path.resolve()
        except Exception:
            continue
        if path in seen:
            continue
        seen.add(path)
        checked.append(str(path))
        if path.exists():
            load_dotenv(path, override=False)
    _ENV_LOADED = True
    return checked


def _split_keys(raw: str) -> List[str]:
    return [_clean(x) for x in _SPLIT_RE.split(_clean(raw)) if _clean(x)]


def _collect_slotted_keys(pattern: re.Pattern[str], label: str) -> List[Tuple[int, str, str]]:
    out: List[Tuple[int, str, str]] = []
    for env_name, env_value in os.environ.items():
        m = pattern.match(env_name)
        if not m:
            continue
        key = _clean(env_value)
        if not key:
            continue
        out.append((int(m.group(1)), key, label))
    out.sort(key=lambda x: x[0])
    return out


def _collect_api_keys() -> Tuple[List[Tuple[int, str, str]], Dict[str, Any]]:
    checked_paths = _load_env_once()
    keys: List[Tuple[int, str, str]] = []

    for idx, key in enumerate(_split_keys(os.getenv("GEMINI_API_KEYS", "")), start=1):
        keys.append((idx, key, "GEMINI_API_KEYS"))
    for idx, key in enumerate(_split_keys(os.getenv("GOOGLE_API_KEYS", "")), start=1):
        keys.append((idx, key, "GOOGLE_API_KEYS"))

    single_gemini = _clean(os.getenv("GEMINI_API_KEY"))
    if single_gemini:
        keys.append((0, single_gemini, "GEMINI_API_KEY"))
    single_google = _clean(os.getenv("GOOGLE_API_KEY"))
    if single_google:
        keys.append((0, single_google, "GOOGLE_API_KEY"))

    keys.extend(_collect_slotted_keys(_GEMINI_SLOT_RE, "GEMINI_API_KEY_n"))
    keys.extend(_collect_slotted_keys(_GOOGLE_SLOT_RE, "GOOGLE_API_KEY_n"))

    deduped: List[Tuple[int, str, str]] = []
    seen = set()
    for slot, key, source in keys:
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append((slot, key, source))

    meta = {
        "env_paths_checked": checked_paths,
        "cwd": str(Path.cwd()),
        "key_count": len(deduped),
        "key_sources": [src for _, _, src in deduped],
    }
    return deduped, meta


def _rotated_keys() -> Tuple[List[Tuple[int, str, str]], Dict[str, Any]]:
    keys, meta = _collect_api_keys()
    if not keys:
        return [], meta
    global _ROUND_ROBIN_INDEX
    with _ROUND_ROBIN_LOCK:
        start = _ROUND_ROBIN_INDEX % len(keys)
        _ROUND_ROBIN_INDEX = (_ROUND_ROBIN_INDEX + 1) % len(keys)
    return keys[start:] + keys[:start], meta


def _extract_text(raw: dict) -> str:
    parts: List[str] = []
    for cand in raw.get("candidates") or []:
        content = (cand or {}).get("content") or {}
        for part in content.get("parts") or []:
            text = _clean((part or {}).get("text"))
            if text:
                parts.append(text)
    return "\n".join(parts).strip()


def _extract_json_payload(text: str) -> Any:
    text = _clean(text)
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        pass
    m = _JSON_FENCE_RE.search(text)
    if m:
        fenced = _clean(m.group(1))
        try:
            return json.loads(fenced)
        except Exception:
            text = fenced
    m = _JSON_BLOCK_RE.search(text)
    if m:
        block = _clean(m.group(1))
        try:
            return json.loads(block)
        except Exception:
            pass
    return {}


def _normalize_keyword(text: str) -> str:
    text = _clean(text).lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip(" ,.;:-_`'\"")


def _normalize_keywords(values: Any, limit: int = 16) -> List[str]:
    if not isinstance(values, list):
        return []
    out: List[str] = []
    seen = set()
    for value in values:
        kw = _normalize_keyword(str(value))
        if not kw or len(kw) < 2 or kw in seen:
            continue
        seen.add(kw)
        out.append(kw)
        if len(out) >= limit:
            break
    return out


def _keywords_from_text(text: str, limit: int = 16) -> List[str]:
    obj = _extract_json_payload(text)
    if isinstance(obj, dict):
        kws = _normalize_keywords(obj.get("keywords") or [], limit)
        if kws:
            return kws
    if isinstance(obj, list):
        kws = _normalize_keywords(obj, limit)
        if kws:
            return kws

    lines: List[str] = []
    for raw_line in _clean(text).splitlines():
        line = raw_line.strip()
        if not line:
            continue
        low = line.lower()
        if low in {"```", "json", "```json"}:
            continue
        if low.startswith("here is the json") or low.startswith("duoi day la json"):
            continue
        line = re.sub(r"^[-*0-9.)\s]+", "", line).strip()
        if line:
            lines.append(line)
    vals: List[str] = []
    for line in lines:
        vals.extend([x.strip() for x in re.split(r"[,;|/]", line) if x.strip()])
    return _normalize_keywords(vals, limit)


def _call_generate_content(api_key: str, model: str, payload: dict, timeout: int = 18) -> dict:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    req = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _json_prompt(query: str, candidates: Sequence[str] | None = None) -> str:
    prompt = (
        "Ban la bo mo rong keyword cho he thong tim kiem hoc lieu. "
        "Neu truy van la mot chu de cha thi tra ve cac keyword con truc tiep, cu the, sat nghia de tim tai lieu con. "
        "Chi tra JSON dung schema {'keywords': ['kw1', 'kw2']}. Khong markdown. Khong giai thich. Khong them chu nao khac. "
        "Vi du: phan cung may tinh -> cpu, ram, bo mach chu, card do hoa, o cung ssd, o cung hdd, chuot, ban phim, man hinh. "
    )
    if candidates:
        joined = ", ".join([_clean(x) for x in candidates if _clean(x)])
        if joined:
            prompt += f"Chi chon keyword tu danh sach ung vien sau neu phu hop: {joined}. "
    return prompt + f"Truy van: {query}"


def _line_prompt(query: str, candidates: Sequence[str] | None = None) -> str:
    prompt = (
        "Liet ke cac keyword con truc tiep cua truy van sau, moi dong dung 1 keyword, khong danh so, khong markdown, khong giai thich. "
        "Neu khong phai chu de cha thi tra rong. "
    )
    if candidates:
        joined = ", ".join([_clean(x) for x in candidates if _clean(x)])
        if joined:
            prompt += f"Chi chon keyword tu danh sach ung vien sau neu phu hop: {joined}. "
    return prompt + f"Truy van: {query}"


def _query_one_key(*, api_key: str, key_slot: int, key_source: str, model: str, query: str, candidates: Sequence[str] | None) -> Tuple[List[str], Dict[str, Any]]:
    meta: Dict[str, Any] = {"model": model, "key_slot": key_slot, "key_source": key_source}

    payload_json = {
        "contents": [{"parts": [{"text": _json_prompt(query, candidates)}]}],
        "generationConfig": {
            "temperature": 0.1,
            "topP": 0.9,
            "maxOutputTokens": 256,
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "OBJECT",
                "properties": {"keywords": {"type": "ARRAY", "items": {"type": "STRING"}}},
                "required": ["keywords"],
            },
        },
    }
    try:
        raw = _call_generate_content(api_key, model, payload_json)
        text = _extract_text(raw)
        if text:
            meta["raw_text"] = text[:1000]
        kws = [k for k in _keywords_from_text(text) if k != _normalize_keyword(query)]
        if kws:
            meta["mode"] = "json_mode"
            return kws, meta
        meta["error"] = "empty_keywords"
    except HTTPError as exc:
        try:
            meta["raw_text"] = exc.read().decode("utf-8", errors="ignore")[:1000]
        except Exception:
            pass
        meta["error"] = f"http_{exc.code}"
    except (URLError, TimeoutError) as exc:
        meta["error"] = f"network_error:{exc}"
    except Exception as exc:
        meta["error"] = f"unexpected:{exc}"

    payload_line = {
        "contents": [{"parts": [{"text": _line_prompt(query, candidates)}]}],
        "generationConfig": {"temperature": 0.1, "topP": 0.9, "maxOutputTokens": 128},
    }
    try:
        raw = _call_generate_content(api_key, model, payload_line)
        text = _extract_text(raw)
        if text:
            meta["raw_text"] = text[:1000]
        kws = [k for k in _keywords_from_text(text) if k != _normalize_keyword(query)]
        if kws:
            meta["mode"] = "line_fallback"
            meta.pop("error", None)
            return kws, meta
        meta["error"] = meta.get("error") or "empty_keywords"
    except HTTPError as exc:
        try:
            meta["raw_text"] = exc.read().decode("utf-8", errors="ignore")[:1000]
        except Exception:
            pass
        meta["error"] = f"http_{exc.code}"
    except (URLError, TimeoutError) as exc:
        meta["error"] = f"network_error:{exc}"
    except Exception as exc:
        meta["error"] = f"unexpected:{exc}"

    return [], meta


@lru_cache(maxsize=256)
def expand_topic_keywords_debug(query: str, candidates: Tuple[str, ...] | None = None) -> Tuple[List[str], Dict[str, Any]]:
    model = _clean(os.getenv("GEMINI_MODEL")) or "gemini-2.5-flash"
    rotated, collect_meta = _rotated_keys()
    base_meta: Dict[str, Any] = {
        "model": model,
        "key_count": collect_meta.get("key_count", 0),
        "key_sources": collect_meta.get("key_sources", []),
        "env_paths_checked": collect_meta.get("env_paths_checked", []),
        "cwd": collect_meta.get("cwd", ""),
    }
    if not rotated:
        base_meta["error"] = "missing_GEMINI_API_KEY"
        return [], base_meta

    last_meta = dict(base_meta)
    for attempt, (slot, key, source) in enumerate(rotated, start=1):
        kws, meta = _query_one_key(
            api_key=key,
            key_slot=slot,
            key_source=source,
            model=model,
            query=_clean(query),
            candidates=list(candidates or ()),
        )
        merged = dict(base_meta)
        merged.update(meta)
        merged["attempt"] = attempt
        if kws:
            return kws, merged
        last_meta = merged
    return [], last_meta


@lru_cache(maxsize=256)
def expand_topic_keywords(query: str) -> List[str]:
    kws, _ = expand_topic_keywords_debug(query, None)
    return kws


def expand_topic_keywords_from_candidates(query: str, candidates: Sequence[str]) -> List[str]:
    kws, _ = expand_topic_keywords_debug(query, tuple(candidates or ()))
    return kws
