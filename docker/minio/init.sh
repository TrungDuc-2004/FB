#!/bin/sh
set -eu

# Đợi MinIO sẵn sàng
until (mc alias set local "http://minio:9000" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD") >/dev/null 2>&1; do
  echo "[minio-init] waiting for minio..."
  sleep 2
done

# Tạo bucket (single-bucket mode)
mc mb -p "local/$MINIO_BUCKET" >/dev/null 2>&1 || true

# Cho phép anonymous download (phù hợp demo preview/link)
# Nếu muốn private, comment dòng này.
mc anonymous set download "local/$MINIO_BUCKET" >/dev/null 2>&1 || true

echo "[minio-init] done"
