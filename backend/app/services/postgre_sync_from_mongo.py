from __future__ import annotations

"""Sync PostgreSQL FROM MongoDB.

Bản này hỗ trợ 2 chế độ:
1) sync_postgre_from_mongo_ids: (GIỮ NGUYÊN) sync theo mongo _id (ObjectId) -> hash PK
2) sync_postgre_from_mongo_maps: (MỚI) sync theo map ID (L10/TH10/TH10_CD1/TH10_CD1_B1/TH10_CD1_B1_C1)
   để Postgre dùng map làm PK + quan hệ cha/con.

Schema PG (theo bạn mô tả):
- class(class_id, class_name, mongo_id)
- subject(subject_id, subject_name, mongo_id, class_id)
- topic(topic_id, topic_name, mongo_id, subject_id)
- lesson(lesson_id, lesson_name, mongo_id, topic_id)
- chunk(chunk_id, chunk_name, chunk_type, mongo_id, lesson_id)
- keyword(keyword_id, keyword_name, mongo_id, chunk_id)

Nguồn Mongo (plural): classes, subjects, topics, lessons, chunks.
"""

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from typing import List, Optional, Tuple

from bson import ObjectId
from sqlalchemy import text

from .mongo_client import get_mongo_client
from .postgre_client import get_engine


def _md5_32(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:32]


def _sha256_64(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:64]


def _sha384_96(s: str) -> str:
    return hashlib.sha384(s.encode("utf-8")).hexdigest()[:96]


def _clean(s) -> str:
    return "" if s is None else str(s).strip()


def _ensure_oid(id_str: str) -> ObjectId:
    try:
        return ObjectId(id_str)
    except Exception as e:
        raise ValueError(f"Invalid mongo id: {id_str}") from e


def _pick_by_oid(db, col: str, _id: str) -> dict:
    doc = db[col].find_one({"_id": _ensure_oid(_id)})
    if not doc:
        raise ValueError(f"Mongo doc not found: {col}({_id})")
    return doc


def _get_pk_by_mongo(conn, table: str, pk_col: str, mongo_id: str) -> Optional[str]:
    row = conn.execute(
        text(f"SELECT {pk_col} FROM {table} WHERE mongo_id = :mongo_id LIMIT 1"),
        {"mongo_id": mongo_id},
    ).fetchone()
    return row[0] if row else None


@dataclass
class PgIds:
    class_id: str
    subject_id: str
    topic_id: str
    lesson_id: str
    chunk_id: str
    keyword_ids: List[str]


def _strip_accents(s: str) -> str:
    """Bỏ dấu tiếng Việt nhưng GIỮ nguyên chữ hoa/thường."""
    if not s:
        return ""
    # NFD tách dấu ra khỏi ký tự
    nf = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in nf if unicodedata.category(ch) != "Mn")


def _keyword_slug(name: str) -> str:
    """Slug cho keyword theo yêu cầu:

    - "Xin chào" -> "Xinchao" (bỏ dấu + bỏ khoảng trắng/ký tự đặc biệt)
    - "USB" -> "USB"
    - GIỮ nguyên hoa/thường từ input.
    """
    s = _strip_accents(_clean(name))
    # chỉ giữ chữ + số
    s = re.sub(r"[^0-9A-Za-z]+", "", s)
    return s


def _class_id_from_class_map(class_map: str, class_name: str = "") -> str:
    # class_map thường là L10/L11/L12
    m = re.search(r"(\d+)", class_map or "")
    if not m:
        m = re.search(r"(\d+)", class_name or "")
    return m.group(1) if m else ""


def _infer_subject_suffix_from_name(subject_name: str) -> str:
    """Suy luận UD/KHMT từ subject_name (không bắt buộc, fallback UD)."""
    x = _strip_accents(_clean(subject_name)).lower()
    # khmt
    if "khmt" in x or "khoa hoc may tinh" in x or "khoa hoc" in x and "may tinh" in x:
        return "KHMT"
    # ứng dụng
    if "ud" in x or "ung dung" in x or "ungdung" in x:
        return "UD"
    return "UD"


def _normalize_subject_id(subject_map: str, *, class_id: str, subject_name: str) -> str:
    """Chuẩn hoá subject_id cho Postgre.

    Quy tắc theo bạn:
    - lớp 10: TH10
    - lớp 11/12: TH11-UD hoặc TH11-KHMT (tương tự TH12-...)
    """
    sm = _clean(subject_map)
    if not sm:
        # fallback: nếu thiếu subject_map, cố đoán từ class_id
        if class_id:
            sm = f"TH{class_id}"
        else:
            sm = "TH"

    # tách prefix THxx
    m = re.match(r"^(TH\d{2})(?:[-_]?([A-Za-z]+))?$", sm, flags=re.I)
    if not m:
        return sm

    base = m.group(1).upper()
    suffix = (m.group(2) or "").upper()

    if class_id == "10":
        # lớp 10 không có -UD/-KHMT
        return base

    if class_id in ("11", "12"):
        if suffix in ("UD", "KHMT"):
            return f"{base}-{suffix}"
        suffix = _infer_subject_suffix_from_name(subject_name)
        return f"{base}-{suffix}"

    return base if not suffix else f"{base}-{suffix}"


def _parse_topic_number_from_topic_map(topic_map: str) -> str:
    m = re.search(r"_CD(\d+)$", _clean(topic_map), flags=re.I)
    return m.group(1) if m else ""


def _parse_topic_lesson_numbers_from_lesson_map(lesson_map: str) -> Tuple[str, str]:
    m = re.search(r"_CD(\d+)_B(\d+)$", _clean(lesson_map), flags=re.I)
    return (m.group(1), m.group(2)) if m else ("", "")


def _parse_topic_lesson_chunk_numbers_from_chunk_map(chunk_map: str) -> Tuple[str, str, str]:
    m = re.search(r"_CD(\d+)_B(\d+)_C(\d+)$", _clean(chunk_map), flags=re.I)
    return (m.group(1), m.group(2), m.group(3)) if m else ("", "", "")


# ======================================================================================
# 1) GIỮ NGUYÊN: sync theo mongo ObjectId -> hash PK
# ======================================================================================

def sync_postgre_from_mongo_ids(
    *,
    mongo_class_id: str,
    mongo_subject_id: str,
    mongo_topic_id: str,
    mongo_lesson_id: str,
    mongo_chunk_id: str,
) -> PgIds:
    mg = get_mongo_client()
    db = mg["db"]

    c_doc = _pick_by_oid(db, "classes", mongo_class_id)
    s_doc = _pick_by_oid(db, "subjects", mongo_subject_id)
    t_doc = _pick_by_oid(db, "topics", mongo_topic_id)
    l_doc = _pick_by_oid(db, "lessons", mongo_lesson_id)
    ch_doc = _pick_by_oid(db, "chunks", mongo_chunk_id)

    class_name = _clean(c_doc.get("className"))
    subject_name = _clean(s_doc.get("subjectName"))
    topic_name = _clean(t_doc.get("topicName"))
    lesson_name = _clean(l_doc.get("lessonName"))
    chunk_name = _clean(ch_doc.get("chunkName"))
    chunk_type = _clean(ch_doc.get("chunkType"))

    keywords = ch_doc.get("keywords") or []
    if not isinstance(keywords, list):
        keywords = []

    class_id_guess = _md5_32(mongo_class_id)
    subject_id_guess = _md5_32(mongo_subject_id)
    topic_id_guess = _sha256_64(mongo_topic_id)
    lesson_id_guess = _sha256_64(mongo_lesson_id)
    chunk_id_guess = _sha256_64(mongo_chunk_id)

    engine = get_engine()

    with engine.begin() as conn:
        class_id = _get_pk_by_mongo(conn, "class", "class_id", mongo_class_id) or class_id_guess
        subject_id = _get_pk_by_mongo(conn, "subject", "subject_id", mongo_subject_id) or subject_id_guess
        topic_id = _get_pk_by_mongo(conn, "topic", "topic_id", mongo_topic_id) or topic_id_guess
        lesson_id = _get_pk_by_mongo(conn, "lesson", "lesson_id", mongo_lesson_id) or lesson_id_guess
        chunk_id = _get_pk_by_mongo(conn, "chunk", "chunk_id", mongo_chunk_id) or chunk_id_guess

        conn.execute(
            text(
                """
                INSERT INTO class (class_id, class_name, mongo_id)
                VALUES (:class_id, :class_name, :mongo_id)
                ON CONFLICT (class_id) DO UPDATE
                SET class_name = EXCLUDED.class_name,
                    mongo_id   = COALESCE(EXCLUDED.mongo_id, class.mongo_id)
                """
            ),
            {"class_id": class_id, "class_name": class_name, "mongo_id": mongo_class_id},
        )

        conn.execute(
            text(
                """
                INSERT INTO subject (subject_id, subject_name, mongo_id, class_id)
                VALUES (:subject_id, :subject_name, :mongo_id, :class_id)
                ON CONFLICT (subject_id) DO UPDATE
                SET subject_name = EXCLUDED.subject_name,
                    mongo_id     = COALESCE(EXCLUDED.mongo_id, subject.mongo_id),
                    class_id     = EXCLUDED.class_id
                """
            ),
            {
                "subject_id": subject_id,
                "subject_name": subject_name,
                "mongo_id": mongo_subject_id,
                "class_id": class_id,
            },
        )

        conn.execute(
            text(
                """
                INSERT INTO topic (topic_id, topic_name, mongo_id, subject_id)
                VALUES (:topic_id, :topic_name, :mongo_id, :subject_id)
                ON CONFLICT (topic_id) DO UPDATE
                SET topic_name  = EXCLUDED.topic_name,
                    mongo_id    = COALESCE(EXCLUDED.mongo_id, topic.mongo_id),
                    subject_id  = EXCLUDED.subject_id
                """
            ),
            {
                "topic_id": topic_id,
                "topic_name": topic_name,
                "mongo_id": mongo_topic_id,
                "subject_id": subject_id,
            },
        )

        conn.execute(
            text(
                """
                INSERT INTO lesson (lesson_id, lesson_name, mongo_id, topic_id)
                VALUES (:lesson_id, :lesson_name, :mongo_id, :topic_id)
                ON CONFLICT (lesson_id) DO UPDATE
                SET lesson_name = EXCLUDED.lesson_name,
                    mongo_id    = COALESCE(EXCLUDED.mongo_id, lesson.mongo_id),
                    topic_id    = EXCLUDED.topic_id
                """
            ),
            {
                "lesson_id": lesson_id,
                "lesson_name": lesson_name,
                "mongo_id": mongo_lesson_id,
                "topic_id": topic_id,
            },
        )

        conn.execute(
            text(
                """
                INSERT INTO chunk (chunk_id, chunk_name, chunk_type, mongo_id, lesson_id)
                VALUES (:chunk_id, :chunk_name, :chunk_type, :mongo_id, :lesson_id)
                ON CONFLICT (chunk_id) DO UPDATE
                SET chunk_name = EXCLUDED.chunk_name,
                    chunk_type = EXCLUDED.chunk_type,
                    mongo_id   = COALESCE(EXCLUDED.mongo_id, chunk.mongo_id),
                    lesson_id  = EXCLUDED.lesson_id
                """
            ),
            {
                "chunk_id": chunk_id,
                "chunk_name": chunk_name,
                "chunk_type": chunk_type or None,
                "mongo_id": mongo_chunk_id,
                "lesson_id": lesson_id,
            },
        )

        conn.execute(text("DELETE FROM keyword WHERE chunk_id = :chunk_id"), {"chunk_id": chunk_id})

        keyword_ids: List[str] = []
        for kw in keywords:
            kw_name = _clean(kw)
            if not kw_name:
                continue
            kw_id = _sha384_96(f"{chunk_id}:{kw_name}")
            keyword_ids.append(kw_id)

            conn.execute(
                text(
                    """
                    INSERT INTO keyword (keyword_id, keyword_name, mongo_id, chunk_id)
                    VALUES (:keyword_id, :keyword_name, :mongo_id, :chunk_id)
                    ON CONFLICT (keyword_id) DO UPDATE
                    SET keyword_name = EXCLUDED.keyword_name,
                        mongo_id      = COALESCE(EXCLUDED.mongo_id, keyword.mongo_id),
                        chunk_id      = EXCLUDED.chunk_id
                    """
                ),
                {
                    "keyword_id": kw_id,
                    "keyword_name": kw_name,
                    "mongo_id": None,
                    "chunk_id": chunk_id,
                },
            )

    return PgIds(
        class_id=class_id_guess,
        subject_id=subject_id_guess,
        topic_id=topic_id_guess,
        lesson_id=lesson_id_guess,
        chunk_id=chunk_id_guess,
        keyword_ids=keyword_ids,
    )


# ======================================================================================
# 2) MỚI: sync theo MAP IDs
# ======================================================================================

def _pick_by_map(db, col: str, map_key: str, map_value: str) -> Optional[dict]:
    if not map_value:
        return None
    return db[col].find_one({map_key: map_value})


def _resolve_chain_from_maps(
    db,
    *,
    class_map: str = "",
    subject_map: str = "",
    topic_map: str = "",
    lesson_map: str = "",
    chunk_map: str = "",
) -> Tuple[Optional[dict], Optional[dict], Optional[dict], Optional[dict], Optional[dict]]:
    """Trả về (class_doc, subject_doc, topic_doc, lesson_doc, chunk_doc)"""

    chunk_doc = _pick_by_map(db, "chunks", "chunkID", chunk_map) if chunk_map else None
    if chunk_doc and not lesson_map:
        lesson_map = _clean(chunk_doc.get("lessonID"))

    lesson_doc = _pick_by_map(db, "lessons", "lessonID", lesson_map) if lesson_map else None
    if lesson_doc and not topic_map:
        topic_map = _clean(lesson_doc.get("topicID"))

    topic_doc = _pick_by_map(db, "topics", "topicID", topic_map) if topic_map else None
    if topic_doc and not subject_map:
        subject_map = _clean(topic_doc.get("subjectID"))

    subject_doc = _pick_by_map(db, "subjects", "subjectID", subject_map) if subject_map else None
    if subject_doc and not class_map:
        class_map = _clean(subject_doc.get("classID"))

    class_doc = _pick_by_map(db, "classes", "classID", class_map) if class_map else None

    return class_doc, subject_doc, topic_doc, lesson_doc, chunk_doc


def sync_postgre_from_mongo_maps(
    *,
    class_map: str = "",
    subject_map: str = "",
    topic_map: str = "",
    lesson_map: str = "",
    chunk_map: str = "",
) -> PgIds:
    """Sync PG theo map IDs.

    - Nếu truyền chunk_map: sync đủ chain + chunk + keywords
    - Nếu truyền lesson_map: sync chain tới lesson
    - Nếu truyền topic_map: sync chain tới topic
    - Nếu truyền subject_map: sync chain tới subject
    - class_map đơn lẻ: sync class
    """

    class_map = _clean(class_map)
    subject_map = _clean(subject_map)
    topic_map = _clean(topic_map)
    lesson_map = _clean(lesson_map)
    chunk_map = _clean(chunk_map)

    mg = get_mongo_client()
    db = mg["db"]

    class_doc, subject_doc, topic_doc, lesson_doc, chunk_doc = _resolve_chain_from_maps(
        db,
        class_map=class_map,
        subject_map=subject_map,
        topic_map=topic_map,
        lesson_map=lesson_map,
        chunk_map=chunk_map,
    )

    # fallback nếu không tìm thấy doc: dùng map làm tên
    class_id = _clean((class_doc or {}).get("classID")) or class_map
    subject_id = _clean((subject_doc or {}).get("subjectID")) or subject_map
    topic_id = _clean((topic_doc or {}).get("topicID")) or topic_map
    lesson_id = _clean((lesson_doc or {}).get("lessonID")) or lesson_map
    chunk_id = _clean((chunk_doc or {}).get("chunkID")) or chunk_map

    class_name = _clean((class_doc or {}).get("className")) or class_id
    subject_name = _clean((subject_doc or {}).get("subjectName")) or subject_id
    topic_name = _clean((topic_doc or {}).get("topicName")) or topic_id
    lesson_name = _clean((lesson_doc or {}).get("lessonName")) or lesson_id
    chunk_name = _clean((chunk_doc or {}).get("chunkName")) or chunk_id
    chunk_type = _clean((chunk_doc or {}).get("chunkType"))

    mongo_class_id = str((class_doc or {}).get("_id")) if class_doc else None
    mongo_subject_id = str((subject_doc or {}).get("_id")) if subject_doc else None
    mongo_topic_id = str((topic_doc or {}).get("_id")) if topic_doc else None
    mongo_lesson_id = str((lesson_doc or {}).get("_id")) if lesson_doc else None
    mongo_chunk_id = str((chunk_doc or {}).get("_id")) if chunk_doc else None

    keywords = (chunk_doc or {}).get("keywords") or []
    if not isinstance(keywords, list):
        keywords = []

    engine = get_engine()

    # nếu chỉ sync tới mức nào thì dừng mức đó
    want_subject = bool(subject_id)
    want_topic = bool(topic_id)
    want_lesson = bool(lesson_id)
    want_chunk = bool(chunk_id)

    keyword_ids: List[str] = []

    with engine.begin() as conn:
        if class_id:
            conn.execute(
                text(
                    """
                    INSERT INTO class (class_id, class_name, mongo_id)
                    VALUES (:class_id, :class_name, :mongo_id)
                    ON CONFLICT (class_id) DO UPDATE
                    SET class_name = EXCLUDED.class_name,
                        mongo_id   = COALESCE(EXCLUDED.mongo_id, class.mongo_id)
                    """
                ),
                {"class_id": class_id, "class_name": class_name, "mongo_id": mongo_class_id},
            )

        if want_subject:
            conn.execute(
                text(
                    """
                    INSERT INTO subject (subject_id, subject_name, mongo_id, class_id)
                    VALUES (:subject_id, :subject_name, :mongo_id, :class_id)
                    ON CONFLICT (subject_id) DO UPDATE
                    SET subject_name = EXCLUDED.subject_name,
                        mongo_id     = COALESCE(EXCLUDED.mongo_id, subject.mongo_id),
                        class_id     = EXCLUDED.class_id
                    """
                ),
                {
                    "subject_id": subject_id,
                    "subject_name": subject_name,
                    "mongo_id": mongo_subject_id,
                    "class_id": class_id or None,
                },
            )

        if want_topic:
            conn.execute(
                text(
                    """
                    INSERT INTO topic (topic_id, topic_name, mongo_id, subject_id)
                    VALUES (:topic_id, :topic_name, :mongo_id, :subject_id)
                    ON CONFLICT (topic_id) DO UPDATE
                    SET topic_name  = EXCLUDED.topic_name,
                        mongo_id    = COALESCE(EXCLUDED.mongo_id, topic.mongo_id),
                        subject_id  = EXCLUDED.subject_id
                    """
                ),
                {
                    "topic_id": topic_id,
                    "topic_name": topic_name,
                    "mongo_id": mongo_topic_id,
                    "subject_id": subject_id or None,
                },
            )

        if want_lesson:
            conn.execute(
                text(
                    """
                    INSERT INTO lesson (lesson_id, lesson_name, mongo_id, topic_id)
                    VALUES (:lesson_id, :lesson_name, :mongo_id, :topic_id)
                    ON CONFLICT (lesson_id) DO UPDATE
                    SET lesson_name = EXCLUDED.lesson_name,
                        mongo_id    = COALESCE(EXCLUDED.mongo_id, lesson.mongo_id),
                        topic_id    = EXCLUDED.topic_id
                    """
                ),
                {
                    "lesson_id": lesson_id,
                    "lesson_name": lesson_name,
                    "mongo_id": mongo_lesson_id,
                    "topic_id": topic_id or None,
                },
            )

        if want_chunk:
            conn.execute(
                text(
                    """
                    INSERT INTO chunk (chunk_id, chunk_name, chunk_type, mongo_id, lesson_id)
                    VALUES (:chunk_id, :chunk_name, :chunk_type, :mongo_id, :lesson_id)
                    ON CONFLICT (chunk_id) DO UPDATE
                    SET chunk_name = EXCLUDED.chunk_name,
                        chunk_type = EXCLUDED.chunk_type,
                        mongo_id   = COALESCE(EXCLUDED.mongo_id, chunk.mongo_id),
                        lesson_id  = EXCLUDED.lesson_id
                    """
                ),
                {
                    "chunk_id": chunk_id,
                    "chunk_name": chunk_name,
                    "chunk_type": chunk_type or None,
                    "mongo_id": mongo_chunk_id,
                    "lesson_id": lesson_id or None,
                },
            )

            # keywords: xoá cũ rồi insert lại
            conn.execute(text("DELETE FROM keyword WHERE chunk_id = :chunk_id"), {"chunk_id": chunk_id})

            for kw in keywords:
                kw_name = _clean(kw)
                if not kw_name:
                    continue
                kw_id = _sha384_96(f"{chunk_id}:{kw_name}")
                keyword_ids.append(kw_id)

                conn.execute(
                    text(
                        """
                        INSERT INTO keyword (keyword_id, keyword_name, mongo_id, chunk_id)
                        VALUES (:keyword_id, :keyword_name, :mongo_id, :chunk_id)
                        ON CONFLICT (keyword_id) DO UPDATE
                        SET keyword_name = EXCLUDED.keyword_name,
                            mongo_id      = COALESCE(EXCLUDED.mongo_id, keyword.mongo_id),
                            chunk_id      = EXCLUDED.chunk_id
                        """
                    ),
                    {
                        "keyword_id": kw_id,
                        "keyword_name": kw_name,
                        "mongo_id": None,
                        "chunk_id": chunk_id,
                    },
                )

    return PgIds(
        class_id=class_id or "",
        subject_id=subject_id or "",
        topic_id=topic_id or "",
        lesson_id=lesson_id or "",
        chunk_id=chunk_id or "",
        keyword_ids=keyword_ids,
    )


# ======================================================================================
# 3) MỚI: sync theo MAP nhưng Postgre lưu ID chuẩn mới (KHÔNG lưu map id)
#
# - class_id  : 10 | 11 | 12
# - subject_id: TH10 | TH11-UD | TH11-KHMT | TH12-UD | TH12-KHMT
# - topic_id  : <subject_id>_T{n}  (n lấy từ ..._CDn)
# - lesson_id : <topic_id>_L{n}    (n lấy từ ..._Bn)
# - chunk_id  : <lesson_id>_C{n}   (n lấy từ ..._Cn)
# - keyword_id: <chunk_id>::<KeywordSlug>   (vd ...::Xinchao)
# ======================================================================================


def sync_postgre_from_mongo_auto_ids(
    *,
    class_map: str = "",
    subject_map: str = "",
    topic_map: str = "",
    lesson_map: str = "",
    chunk_map: str = "",
) -> PgIds:
    """Sync PG từ Mongo nhưng *ID trong PG* theo chuẩn mới.

    Map ID (CD/B/C) chỉ dùng để suy ra số thứ tự T/L/C.
    """

    class_map = _clean(class_map)
    subject_map = _clean(subject_map)
    topic_map = _clean(topic_map)
    lesson_map = _clean(lesson_map)
    chunk_map = _clean(chunk_map)

    mg = get_mongo_client()
    db = mg["db"]

    class_doc, subject_doc, topic_doc, lesson_doc, chunk_doc = _resolve_chain_from_maps(
        db,
        class_map=class_map,
        subject_map=subject_map,
        topic_map=topic_map,
        lesson_map=lesson_map,
        chunk_map=chunk_map,
    )

    # maps fallback từ doc
    class_map = class_map or _clean((class_doc or {}).get("classID"))
    subject_map = subject_map or _clean((subject_doc or {}).get("subjectID"))
    topic_map = topic_map or _clean((topic_doc or {}).get("topicID"))
    lesson_map = lesson_map or _clean((lesson_doc or {}).get("lessonID"))
    chunk_map = chunk_map or _clean((chunk_doc or {}).get("chunkID"))

    class_name = _clean((class_doc or {}).get("className")) or class_map
    subject_name = _clean((subject_doc or {}).get("subjectName")) or subject_map
    topic_name = _clean((topic_doc or {}).get("topicName")) or (topic_map or "")
    lesson_name = _clean((lesson_doc or {}).get("lessonName")) or (lesson_map or "")
    chunk_name = _clean((chunk_doc or {}).get("chunkName")) or (chunk_map or "")
    chunk_type = _clean((chunk_doc or {}).get("chunkType"))

    mongo_class_id = str((class_doc or {}).get("_id")) if class_doc else None
    mongo_subject_id = str((subject_doc or {}).get("_id")) if subject_doc else None
    mongo_topic_id = str((topic_doc or {}).get("_id")) if topic_doc else None
    mongo_lesson_id = str((lesson_doc or {}).get("_id")) if lesson_doc else None
    mongo_chunk_id = str((chunk_doc or {}).get("_id")) if chunk_doc else None

    # ====== Tạo ID chuẩn mới ======
    class_id = _class_id_from_class_map(class_map, class_name)
    subject_id = _normalize_subject_id(subject_map, class_id=class_id, subject_name=subject_name)

    topic_id = ""
    lesson_id = ""
    chunk_id = ""

    if topic_map:
        tnum = _parse_topic_number_from_topic_map(topic_map)
        topic_id = f"{subject_id}_T{tnum}" if tnum else ""

    if lesson_map:
        tnum, lnum = _parse_topic_lesson_numbers_from_lesson_map(lesson_map)
        if tnum and lnum:
            topic_id = topic_id or f"{subject_id}_T{tnum}"
            lesson_id = f"{topic_id}_L{lnum}"

    if chunk_map:
        tnum, lnum, cnum = _parse_topic_lesson_chunk_numbers_from_chunk_map(chunk_map)
        if tnum and lnum and cnum:
            topic_id = topic_id or f"{subject_id}_T{tnum}"
            lesson_id = lesson_id or f"{topic_id}_L{lnum}"
            chunk_id = f"{lesson_id}_C{cnum}"

    # keywords
    keywords = (chunk_doc or {}).get("keywords") or []
    if not isinstance(keywords, list):
        keywords = []

    engine = get_engine()

    keyword_ids: List[str] = []
    want_subject = bool(subject_id)
    want_topic = bool(topic_id)
    want_lesson = bool(lesson_id)
    want_chunk = bool(chunk_id)

    with engine.begin() as conn:
        if class_id:
            conn.execute(
                text(
                    """
                    INSERT INTO class (class_id, class_name, mongo_id)
                    VALUES (:class_id, :class_name, :mongo_id)
                    ON CONFLICT (class_id) DO UPDATE
                    SET class_name = EXCLUDED.class_name,
                        mongo_id   = COALESCE(EXCLUDED.mongo_id, class.mongo_id)
                    """
                ),
                {"class_id": class_id, "class_name": class_name, "mongo_id": mongo_class_id},
            )

        if want_subject:
            conn.execute(
                text(
                    """
                    INSERT INTO subject (subject_id, subject_name, mongo_id, class_id)
                    VALUES (:subject_id, :subject_name, :mongo_id, :class_id)
                    ON CONFLICT (subject_id) DO UPDATE
                    SET subject_name = EXCLUDED.subject_name,
                        mongo_id     = COALESCE(EXCLUDED.mongo_id, subject.mongo_id),
                        class_id     = EXCLUDED.class_id
                    """
                ),
                {
                    "subject_id": subject_id,
                    "subject_name": subject_name or subject_id,
                    "mongo_id": mongo_subject_id,
                    "class_id": class_id or None,
                },
            )

        if want_topic:
            conn.execute(
                text(
                    """
                    INSERT INTO topic (topic_id, topic_name, mongo_id, subject_id)
                    VALUES (:topic_id, :topic_name, :mongo_id, :subject_id)
                    ON CONFLICT (topic_id) DO UPDATE
                    SET topic_name  = EXCLUDED.topic_name,
                        mongo_id    = COALESCE(EXCLUDED.mongo_id, topic.mongo_id),
                        subject_id  = EXCLUDED.subject_id
                    """
                ),
                {
                    "topic_id": topic_id,
                    "topic_name": topic_name or topic_id,
                    "mongo_id": mongo_topic_id,
                    "subject_id": subject_id,
                },
            )

        if want_lesson:
            conn.execute(
                text(
                    """
                    INSERT INTO lesson (lesson_id, lesson_name, mongo_id, topic_id)
                    VALUES (:lesson_id, :lesson_name, :mongo_id, :topic_id)
                    ON CONFLICT (lesson_id) DO UPDATE
                    SET lesson_name = EXCLUDED.lesson_name,
                        mongo_id    = COALESCE(EXCLUDED.mongo_id, lesson.mongo_id),
                        topic_id    = EXCLUDED.topic_id
                    """
                ),
                {
                    "lesson_id": lesson_id,
                    "lesson_name": lesson_name or lesson_id,
                    "mongo_id": mongo_lesson_id,
                    "topic_id": topic_id,
                },
            )

        if want_chunk:
            conn.execute(
                text(
                    """
                    INSERT INTO chunk (chunk_id, chunk_name, chunk_type, mongo_id, lesson_id)
                    VALUES (:chunk_id, :chunk_name, :chunk_type, :mongo_id, :lesson_id)
                    ON CONFLICT (chunk_id) DO UPDATE
                    SET chunk_name = EXCLUDED.chunk_name,
                        chunk_type = EXCLUDED.chunk_type,
                        mongo_id   = COALESCE(EXCLUDED.mongo_id, chunk.mongo_id),
                        lesson_id  = EXCLUDED.lesson_id
                    """
                ),
                {
                    "chunk_id": chunk_id,
                    "chunk_name": chunk_name or chunk_id,
                    "chunk_type": chunk_type or None,
                    "mongo_id": mongo_chunk_id,
                    "lesson_id": lesson_id,
                },
            )

            # keywords: xoá cũ rồi insert lại
            conn.execute(text("DELETE FROM keyword WHERE chunk_id = :chunk_id"), {"chunk_id": chunk_id})

            for kw in keywords:
                kw_name = _clean(kw)
                if not kw_name:
                    continue
                slug = _keyword_slug(kw_name)
                if not slug:
                    continue
                kw_id = f"{chunk_id}::{slug}"
                keyword_ids.append(kw_id)

                conn.execute(
                    text(
                        """
                        INSERT INTO keyword (keyword_id, keyword_name, mongo_id, chunk_id)
                        VALUES (:keyword_id, :keyword_name, :mongo_id, :chunk_id)
                        ON CONFLICT (keyword_id) DO UPDATE
                        SET keyword_name = EXCLUDED.keyword_name,
                            mongo_id      = COALESCE(EXCLUDED.mongo_id, keyword.mongo_id),
                            chunk_id      = EXCLUDED.chunk_id
                        """
                    ),
                    {
                        "keyword_id": kw_id,
                        "keyword_name": kw_name,
                        "mongo_id": None,
                        "chunk_id": chunk_id,
                    },
                )

    return PgIds(
        class_id=class_id or "",
        subject_id=subject_id or "",
        topic_id=topic_id or "",
        lesson_id=lesson_id or "",
        chunk_id=chunk_id or "",
        keyword_ids=keyword_ids,
    )
