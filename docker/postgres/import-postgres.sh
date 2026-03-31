#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BACKUP_ROOT="${BACKUP_ROOT:-$PROJECT_ROOT/Database}"
SQL_FILE="${SQL_FILE:-$BACKUP_ROOT/Postgre/dataa.sql}"
PG_SERVICE="${PG_SERVICE:-postgres}"
PG_USER="${PG_USER:-postgres}"
PG_DB="${PG_NAME:-dataa}"

cd "$PROJECT_ROOT"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "Khong tim thay file SQL: $SQL_FILE"
  exit 1
fi

echo "[1/4] Khoi dong service PostgreSQL..."
docker compose up -d "$PG_SERVICE"

echo "[2/4] Cho PostgreSQL san sang..."
until docker compose exec -T "$PG_SERVICE" pg_isready -U "$PG_USER" -d postgres >/dev/null 2>&1; do
  sleep 2
done

echo "[3/4] Drop/Create lai database $PG_DB ..."
docker compose exec -T "$PG_SERVICE" psql -U "$PG_USER" -d postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$PG_DB' AND pid <> pg_backend_pid();" >/dev/null 2>&1 || true
docker compose exec -T "$PG_SERVICE" psql -U "$PG_USER" -d postgres -c "DROP DATABASE IF EXISTS \"$PG_DB\";"
docker compose exec -T "$PG_SERVICE" psql -U "$PG_USER" -d postgres -c "CREATE DATABASE \"$PG_DB\";"

echo "[4/4] Import $SQL_FILE vao database $PG_DB ..."
docker compose exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" < "$SQL_FILE"

echo "Hoan tat import PostgreSQL."
