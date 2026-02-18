"""SQLAlchemy models for PostgreSQL.

Chỉ map đúng các cột bạn đang dùng trong PostgreSQL.

Ghi chú quan trọng:
- Table đăng nhập là **user** (singular)
- Columns (theo ảnh bạn gửi):
  user_id, username, password, user_role, is_active, mongo_id
"""

from sqlalchemy import Boolean, Column, ForeignKey, String, Text, Float
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID

from ..services.postgre_client import Base


# ===== 1) CLASS =====
class Class(Base):
    __tablename__ = "class"
    class_id = Column(String, primary_key=True)
    class_name = Column(Text, nullable=False)
    mongo_id = Column(String(24), unique=True, nullable=True)


# ===== 2) SUBJECT =====
class Subject(Base):
    __tablename__ = "subject"
    subject_id = Column(String, primary_key=True)
    subject_name = Column(Text, nullable=False)
    mongo_id = Column(String(24), unique=True, nullable=True)

    class_id = Column(String, ForeignKey("class.class_id", ondelete="CASCADE"), nullable=False)


# ===== 3) TOPIC =====
class Topic(Base):
    __tablename__ = "topic"
    topic_id = Column(String, primary_key=True)
    topic_name = Column(Text, nullable=False)
    mongo_id = Column(String(24), unique=True, nullable=True)

    subject_id = Column(String, ForeignKey("subject.subject_id", ondelete="CASCADE"), nullable=False)


# ===== 4) LESSON =====
class Lesson(Base):
    __tablename__ = "lesson"
    lesson_id = Column(String, primary_key=True)
    lesson_name = Column(Text, nullable=False)
    mongo_id = Column(String(24), unique=True, nullable=True)

    topic_id = Column(String, ForeignKey("topic.topic_id", ondelete="CASCADE"), nullable=False)


# ===== 5) CHUNK =====
class Chunk(Base):
    __tablename__ = "chunk"
    chunk_id = Column(String, primary_key=True)
    chunk_name = Column(Text, nullable=False)
    chunk_type = Column(String(32), nullable=True)
    mongo_id = Column(String(24), unique=True, nullable=True)

    lesson_id = Column(String, ForeignKey("lesson.lesson_id", ondelete="CASCADE"), nullable=False)


# ===== 6) KEYWORD =====
class Keyword(Base):
    __tablename__ = "keyword"
    keyword_id = Column(String, primary_key=True)  # VARCHAR(96)
    keyword_name = Column(Text, nullable=False)
    # NOTE: cần migration thêm cột keyword_embedding REAL[] trong Postgre
    keyword_embedding = Column(ARRAY(Float), nullable=True)
    mongo_id = Column(String(24), unique=True, nullable=True)

    chunk_id = Column(String, ForeignKey("chunk.chunk_id", ondelete="CASCADE"), nullable=False)


# ===== USER (login) =====
class User(Base):
    __tablename__ = "user"

    # Nếu DB của bạn không phải UUID, đổi UUID -> String ở đây.
    user_id = Column(String, primary_key=True)


    username = Column(String(50), unique=True, nullable=False)
    password = Column(Text, nullable=False)

    # enum user_role trong DB: đọc ra string OK
    user_role = Column(String, nullable=False)

    is_active = Column(Boolean, nullable=False)
    mongo_id = Column(String(24), nullable=True)
