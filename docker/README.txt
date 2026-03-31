Docker notes:
- Khong can dua thu muc Database vao image hoac repo.
- Co the dat backup o ngoai project va chay script voi BACKUP_ROOT=/duong/dan/Database.

Vi du:
  BACKUP_ROOT=/data/backup bash docker/postgres/import-postgres.sh
  BACKUP_ROOT=/data/backup bash docker/mongo/import-mongo.sh
  BACKUP_ROOT=/data/backup bash docker/neo4j/import-neo4j.sh
  BACKUP_ROOT=/data/backup bash docker/minio/import-minio.sh
