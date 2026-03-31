#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BACKUP_ROOT="${BACKUP_ROOT:-$PROJECT_ROOT/Database}"
MONGO_SRC="${MONGO_SRC:-$BACKUP_ROOT/MongoDB}"
MONGO_SERVICE="${MONGO_SERVICE:-mongo}"
MONGODB_DB="${MONGODB_DB:-Data}"
TMP_DIR_IN_CONTAINER="/tmp/mongo_restore"

cd "$PROJECT_ROOT"

if [[ ! -d "$MONGO_SRC" ]]; then
  echo "Khong tim thay thu muc MongoDB backup: $MONGO_SRC"
  echo "Dat BACKUP_ROOT=/duong/dan/Database hoac MONGO_SRC=/duong/dan/MongoDB"
  exit 1
fi

echo "[1/5] Khoi dong service MongoDB..."
docker compose up -d "$MONGO_SERVICE"

echo "[2/5] Cho MongoDB san sang..."
until docker compose exec -T "$MONGO_SERVICE" mongosh --quiet --eval 'db.runCommand({ ping: 1 }).ok' | grep -q 1; do
  sleep 2
done

CID="$(docker compose ps -q "$MONGO_SERVICE")"
if [[ -z "$CID" ]]; then
  echo "Khong lay duoc container id cua MongoDB"
  exit 1
fi

echo "[3/5] Copy backup vao container..."
docker compose exec -T "$MONGO_SERVICE" rm -rf "$TMP_DIR_IN_CONTAINER"
docker compose exec -T "$MONGO_SERVICE" mkdir -p "$TMP_DIR_IN_CONTAINER"
docker cp "$MONGO_SRC/." "$CID:$TMP_DIR_IN_CONTAINER"

echo "[4/5] Xac dinh kieu backup va restore..."
if find "$MONGO_SRC" -maxdepth 1 -type f -name '*.bson' | grep -q .; then
  docker compose exec -T "$MONGO_SERVICE" mongorestore --drop --db "$MONGODB_DB" "$TMP_DIR_IN_CONTAINER"
else
  docker compose exec -T "$MONGO_SERVICE" mongorestore --drop "$TMP_DIR_IN_CONTAINER"
fi

echo "[5/5] Don dep file tam..."
docker compose exec -T "$MONGO_SERVICE" rm -rf "$TMP_DIR_IN_CONTAINER"

echo "Hoan tat import MongoDB."
