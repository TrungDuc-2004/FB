CREATE TABLE IF NOT EXISTS class (
    class_id VARCHAR PRIMARY KEY,
    class_name TEXT NOT NULL,
    mongo_id VARCHAR(24) UNIQUE
);

CREATE TABLE IF NOT EXISTS subject (
    subject_id VARCHAR PRIMARY KEY,
    subject_name TEXT NOT NULL,
    mongo_id VARCHAR(24) UNIQUE,
    class_id VARCHAR NOT NULL REFERENCES class(class_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS topic (
    topic_id VARCHAR PRIMARY KEY,
    topic_name TEXT NOT NULL,
    mongo_id VARCHAR(24) UNIQUE,
    topic_number INTEGER,
    subject_id VARCHAR NOT NULL REFERENCES subject(subject_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS lesson (
    lesson_id VARCHAR PRIMARY KEY,
    lesson_name TEXT NOT NULL,
    mongo_id VARCHAR(24) UNIQUE,
    lesson_number INTEGER,
    topic_id VARCHAR NOT NULL REFERENCES topic(topic_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS chunk (
    chunk_id VARCHAR PRIMARY KEY,
    chunk_name TEXT NOT NULL,
    chunk_type VARCHAR(32),
    mongo_id VARCHAR(24) UNIQUE,
    chunk_number INTEGER,
    lesson_id VARCHAR NOT NULL REFERENCES lesson(lesson_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS keyword (
    keyword_id VARCHAR(96) PRIMARY KEY,
    keyword_name TEXT NOT NULL,
    keyword_embedding REAL[],
    mongo_id VARCHAR(24) UNIQUE,
    map_id VARCHAR NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_keyword_map_id ON keyword(map_id);

CREATE TABLE IF NOT EXISTS image (
    img_id VARCHAR PRIMARY KEY,
    img_name TEXT NOT NULL,
    mongo_id VARCHAR(24) UNIQUE NOT NULL,
    follow_id VARCHAR NOT NULL,
    follow_type VARCHAR(16) NOT NULL
);

CREATE TABLE IF NOT EXISTS video (
    video_id VARCHAR PRIMARY KEY,
    video_name TEXT NOT NULL,
    mongo_id VARCHAR(24) UNIQUE NOT NULL,
    follow_id VARCHAR NOT NULL,
    follow_type VARCHAR(16) NOT NULL
);

CREATE TABLE IF NOT EXISTS "user" (
    user_id VARCHAR PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password TEXT NOT NULL,
    user_role VARCHAR NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    mongo_id VARCHAR(24)
);
