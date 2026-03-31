#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BACKUP_ROOT="${BACKUP_ROOT:-$PROJECT_ROOT/Database}"
MINIO_SRC="${MINIO_SRC:-$BACKUP_ROOT/MinIO}"
MINIO_SERVICE="${MINIO_SERVICE:-minio}"
MINIO_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"

cd "$PROJECT_ROOT"

if [[ ! -d "$MINIO_SRC" ]]; then
  echo "Khong tim thay thu muc MinIO backup: $MINIO_SRC"
  exit 1
fi

echo "[1/5] Khoi dong service MinIO..."
docker compose up -d "$MINIO_SERVICE"

echo "[2/5] Cho MinIO san sang..."
until docker compose exec -T "$MINIO_SERVICE" sh -lc 'curl -fsS http://localhost:9000/minio/health/live >/dev/null'; do
  sleep 2
done

CID="$(docker compose ps -q "$MINIO_SERVICE")"
if [[ -z "$CID" ]]; then
  echo "Khong lay duoc container id cua MinIO"
  exit 1
fi

NETWORK_NAME="$(docker inspect "$CID" --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
if [[ -z "$NETWORK_NAME" ]]; then
  echo "Khong xac dinh duoc network cua MinIO"
  exit 1
fi

echo "[3/5] Import tung bucket tu thu muc backup..."
docker run --rm \
  --network "$NETWORK_NAME" \
  -v "$MINIO_SRC:/backup:ro" \
  -e MC_HOST_local="http://$MINIO_USER:$MINIO_PASSWORD@minio:9000" \
  minio/mc:latest \
  sh -c '
    set -e
    for dir in /backup/*; do
      [ -d "$dir" ] || continue
      bucket="$(basename "$dir")"
      echo "-> Import bucket: $bucket"
      mc mb -p "local/$bucket" || true
      mc mirror --overwrite "$dir" "local/$bucket"
    done
  '

echo "[4/5] Kiem tra bucket da import xong."
docker run --rm \
  --network "$NETWORK_NAME" \
  -e MC_HOST_local="http://$MINIO_USER:$MINIO_PASSWORD@minio:9000" \
  minio/mc:latest \
  ls local

echo "[5/5] Hoan tat import MinIO."
