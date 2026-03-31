Dat 4 file .sh nay trong thu muc docker/ cua project.

Cau truc khuyen nghi:
- project-root/
  - docker/
    - import-postgres.sh
    - import-mongo.sh
    - import-neo4j.sh
    - import-minio.sh
  - Database/
    - Postgre/dataa.sql
    - MongoDB/...
    - Neo4j/neo4j.dump
    - MinIO/documents, images, video

Chay tu thu muc goc project:
- bash docker/import-postgres.sh
- bash docker/import-mongo.sh
- bash docker/import-neo4j.sh
- bash docker/import-minio.sh

Neu duong dan backup khac, co the override:
- BACKUP_ROOT=/duong/dan/khac bash docker/import-postgres.sh
