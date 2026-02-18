-- Add embedding column for keyword table
-- Run this on your PostgreSQL database BEFORE running the new sync.

ALTER TABLE keyword
  ADD COLUMN IF NOT EXISTS keyword_embedding REAL[];

-- Optional (recommended): if you want to query by embedding later,
-- you can install pgvector and change the column type to VECTOR.
-- (Not applied here because project currently does not include pgvector.)
--
-- CREATE EXTENSION IF NOT EXISTS vector;
-- ALTER TABLE keyword ADD COLUMN IF NOT EXISTS keyword_embedding VECTOR(256);
