#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BACKUP_ROOT="${BACKUP_ROOT:-$PROJECT_ROOT/Database}"
NEO4J_BACKUP_DIR="${NEO4J_BACKUP_DIR:-$BACKUP_ROOT/Neo4j}"
NEO4J_DUMP_FILE="${NEO4J_DUMP_FILE:-$NEO4J_BACKUP_DIR/neo4j.dump}"
NEO4J_SERVICE="${NEO4J_SERVICE:-neo4j}"
NEO4J_DATABASE="${NEO4J_DATABASE:-neo4j}"

cd "$PROJECT_ROOT"

if [[ ! -f "$NEO4J_DUMP_FILE" ]]; then
  echo "Khong tim thay file dump: $NEO4J_DUMP_FILE"
  echo "Dat BACKUP_ROOT=/duong/dan/Database hoac NEO4J_DUMP_FILE=/duong/dan/neo4j.dump"
  exit 1
fi

echo "[1/6] Khoi dong service Neo4j neu chua co..."
docker compose up -d "$NEO4J_SERVICE"
sleep 5

CID="$(docker compose ps -q "$NEO4J_SERVICE")"
if [[ -z "$CID" ]]; then
  echo "Khong lay duoc container id cua Neo4j"
  exit 1
fi

IMAGE="$(docker inspect "$CID" --format '{{.Config.Image}}')"
VOLUME_NAME="$(docker inspect "$CID" --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}')"

if [[ -z "$VOLUME_NAME" ]]; then
  echo "Khong xac dinh duoc volume /data cua Neo4j"
  exit 1
fi

echo "[2/6] Dung service Neo4j de restore offline..."
docker compose stop "$NEO4J_SERVICE"

echo "[3/6] Xoa du lieu cu trong volume Neo4j..."
docker run --rm -v "$VOLUME_NAME:/data" "$IMAGE" bash -lc 'rm -rf /data/databases/* /data/transactions/* /data/dumps/*'

echo "[4/6] Copy file dump vao thu muc tam..."
TMP_BACKUP_DIR="$(mktemp -d)"
cp "$NEO4J_DUMP_FILE" "$TMP_BACKUP_DIR/"

echo "[5/6] Load dump vao database $NEO4J_DATABASE ..."
docker run --rm \
  -v "$VOLUME_NAME:/data" \
  -v "$TMP_BACKUP_DIR:/backup" \
  "$IMAGE" \
  neo4j-admin database load "$NEO4J_DATABASE" --from-path=/backup --overwrite-destination=true

rm -rf "$TMP_BACKUP_DIR"

echo "[6/6] Khoi dong lai service Neo4j..."
docker compose up -d "$NEO4J_SERVICE"

echo "Hoan tat import Neo4j."
