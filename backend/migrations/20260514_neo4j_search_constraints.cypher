// Run these once in Neo4j Browser/cypher-shell to speed up branch search.
// They make MATCH (Node {pg_id: ...}) and Keyword lookup much faster.

CREATE CONSTRAINT class_pg_id_unique IF NOT EXISTS
FOR (n:Class) REQUIRE n.pg_id IS UNIQUE;

CREATE CONSTRAINT subject_pg_id_unique IF NOT EXISTS
FOR (n:Subject) REQUIRE n.pg_id IS UNIQUE;

CREATE CONSTRAINT topic_pg_id_unique IF NOT EXISTS
FOR (n:Topic) REQUIRE n.pg_id IS UNIQUE;

CREATE CONSTRAINT lesson_pg_id_unique IF NOT EXISTS
FOR (n:Lesson) REQUIRE n.pg_id IS UNIQUE;

CREATE CONSTRAINT chunk_pg_id_unique IF NOT EXISTS
FOR (n:Chunk) REQUIRE n.pg_id IS UNIQUE;

CREATE CONSTRAINT keyword_pg_id_unique IF NOT EXISTS
FOR (n:Keyword) REQUIRE n.pg_id IS UNIQUE;

CREATE INDEX keyword_map_id IF NOT EXISTS
FOR (n:Keyword) ON (n.map_id);
