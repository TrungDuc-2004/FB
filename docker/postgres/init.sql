-- PostgreSQL init (tạo schema tối thiểu để chạy demo)
-- Chạy tự động khi container Postgres khởi tạo lần đầu (volume còn trống).

BEGIN;

-- 1) CLASS
CREATE TABLE IF NOT EXISTS class (
  class_id   VARCHAR PRIMARY KEY,
  class_name TEXT NOT NULL,
  mongo_id   VARCHAR(24) UNIQUE
);

-- 2) SUBJECT
CREATE TABLE IF NOT EXISTS subject (
  subject_id   VARCHAR PRIMARY KEY,
  subject_name TEXT NOT NULL,
  mongo_id     VARCHAR(24) UNIQUE,
  class_id     VARCHAR NOT NULL REFERENCES class(class_id) ON DELETE CASCADE
);

-- 3) TOPIC
CREATE TABLE IF NOT EXISTS topic (
  topic_id   VARCHAR PRIMARY KEY,
  topic_name TEXT NOT NULL,
  mongo_id   VARCHAR(24) UNIQUE,
  subject_id VARCHAR NOT NULL REFERENCES subject(subject_id) ON DELETE CASCADE
);

-- 4) LESSON
CREATE TABLE IF NOT EXISTS lesson (
  lesson_id   VARCHAR PRIMARY KEY,
  lesson_name TEXT NOT NULL,
  mongo_id    VARCHAR(24) UNIQUE,
  topic_id    VARCHAR NOT NULL REFERENCES topic(topic_id) ON DELETE CASCADE
);

-- 5) CHUNK
CREATE TABLE IF NOT EXISTS chunk (
  chunk_id   VARCHAR PRIMARY KEY,
  chunk_name TEXT NOT NULL,
  chunk_type VARCHAR(32),
  mongo_id   VARCHAR(24) UNIQUE,
  lesson_id  VARCHAR NOT NULL REFERENCES lesson(lesson_id) ON DELETE CASCADE
);

-- 6) KEYWORD
CREATE TABLE IF NOT EXISTS keyword (
  keyword_id        VARCHAR PRIMARY KEY,
  keyword_name      TEXT NOT NULL,
  keyword_embedding REAL[],
  mongo_id          VARCHAR(24) UNIQUE,
  chunk_id          VARCHAR NOT NULL REFERENCES chunk(chunk_id) ON DELETE CASCADE
);

-- 7) USER 
CREATE TABLE IF NOT EXISTS "user" (
  user_id    VARCHAR PRIMARY KEY,
  username   VARCHAR(50) UNIQUE NOT NULL,
  password   TEXT NOT NULL,
  user_role  VARCHAR NOT NULL,
  is_active  BOOLEAN NOT NULL,
  mongo_id   VARCHAR(24)
);

-- Tạo tài khoản demo 
INSERT INTO "user" (user_id, username, password, user_role, is_active)
VALUES ('1', 'admin', 'admin', 'admin', TRUE)

INSERT INTO "user" (user_id, username, password, user_role, is_active)
VALUES ('2', 'users', 'users', 'admin', TRUE)

ON CONFLICT (username) DO NOTHING;

COMMIT;
