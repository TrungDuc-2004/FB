# app/models/model_postgre.py
from sqlalchemy import Column, ForeignKey, String, Integer, Boolean
from sqlalchemy.schema import FetchedValue
from ..services.postgre_client import Base

# NOTE:
# - Các id (class_id/subject_id/...) bạn sinh bằng trigger -> dùng server_default=FetchedValue()
# - Keyword: PK ghép (chunk_id, keyword_name) -> KHÔNG có keyword_id

class Class(Base):
    __tablename__ = "class"
    class_id = Column(String, primary_key=True, index=True, server_default=FetchedValue())
    class_name = Column(String, nullable=False)
    mongo_id = Column(String, unique=True, nullable=True)
    __mapper_args__ = {"eager_defaults": True}


class Subject(Base):
    __tablename__ = "subject"
    subject_id = Column(String, primary_key=True, index=True, server_default=FetchedValue())
    class_id = Column(String, ForeignKey("class.class_id", ondelete="CASCADE"), nullable=False)

    subject_name = Column(String, nullable=False)
    subject_type = Column(String, nullable=False)
    mongo_id = Column(String, unique=True, nullable=True)
    __mapper_args__ = {"eager_defaults": True}


class Topic(Base):
    __tablename__ = "topic"
    topic_id = Column(String, primary_key=True, index=True, server_default=FetchedValue())
    subject_id = Column(String, ForeignKey("subject.subject_id", ondelete="CASCADE"), nullable=False)

    topic_num = Column(Integer, nullable=False)  # DB là int
    topic_name = Column(String, nullable=False)
    mongo_id = Column(String, unique=True, nullable=True)
    minio_url = Column(String, nullable=True)
    __mapper_args__ = {"eager_defaults": True}


class Lesson(Base):
    __tablename__ = "lesson"
    lesson_id = Column(String, primary_key=True, index=True, server_default=FetchedValue())
    topic_id = Column(String, ForeignKey("topic.topic_id", ondelete="CASCADE"), nullable=False)

    lesson_num = Column(Integer, nullable=False)  # DB là int
    lesson_name = Column(String, nullable=False)
    lesson_type = Column(String, nullable=True)
    mongo_id = Column(String, unique=True, nullable=True)
    minio_url = Column(String, nullable=True)
    __mapper_args__ = {"eager_defaults": True}


class Chunk(Base):
    __tablename__ = "chunk"
    chunk_id = Column(String, primary_key=True, index=True, server_default=FetchedValue())
    lesson_id = Column(String, ForeignKey("lesson.lesson_id", ondelete="CASCADE"), nullable=False)

    chunk_label = Column(Integer, nullable=False)  # DB là int
    chunk_name = Column(String, nullable=False)
    mongo_id = Column(String, unique=True, nullable=True)
    minio_url = Column(String, nullable=True)
    __mapper_args__ = {"eager_defaults": True}


class Keyword(Base):
    __tablename__ = "keyword"
    # PRIMARY KEY (chunk_id, keyword_name)
    chunk_id = Column(String, ForeignKey("chunk.chunk_id", ondelete="CASCADE"), primary_key=True)
    keyword_name = Column(String, primary_key=True)
    mongo_id = Column(String, unique=True, nullable=True)


class User(Base):
    __tablename__ = "user"
    # user_id sinh bằng trigger/sequence -> server_default
    user_id = Column(String, primary_key=True, index=True, server_default=FetchedValue())
    username = Column(String, unique=True, nullable=False)
    password = Column(String, nullable=False)
    user_role = Column(String, nullable=False)   # DB bạn có CHECK admin/user
    is_active = Column(Boolean, nullable=False, default=True)
    mongo_id = Column(String, unique=True, nullable=True)
    __mapper_args__ = {"eager_defaults": True}
