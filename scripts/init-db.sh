#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PGDATA="${PGDATA:-$ROOT_DIR/.local/postgres}"
PGHOST="${PGHOST:-$PGDATA}"
PGPORT="${PGPORT:-5433}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-pg_wal_visualizer}"

mkdir -p "$PGDATA"

if [[ ! -f "$PGDATA/PG_VERSION" ]]; then
  initdb -D "$PGDATA" --username="$PGUSER" --auth=trust >/dev/null
fi

grep -q "^port = $PGPORT$" "$PGDATA/postgresql.conf" 2>/dev/null || cat >>"$PGDATA/postgresql.conf" <<EOF
port = $PGPORT
unix_socket_directories = '$PGHOST'
listen_addresses = '127.0.0.1'
wal_level = replica
max_wal_size = '2GB'
min_wal_size = '256MB'
shared_buffers = '256MB'
log_checkpoints = on
checkpoint_timeout = '5min'
EOF

pg_ctl -D "$PGDATA" -l "$PGDATA/postgres.log" start >/dev/null

for _ in {1..30}; do
  if pg_isready -h 127.0.0.1 -p "$PGPORT" -U "$PGUSER" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

cleanup() {
  if [[ "${KEEP_DB_RUNNING:-1}" != "1" ]]; then
    pg_ctl -D "$PGDATA" stop >/dev/null
  fi
}
trap cleanup EXIT

createdb -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$PGDATABASE" 2>/dev/null || true

psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -f "$ROOT_DIR/scripts/sql/schema.sql" >/dev/null
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -f "$ROOT_DIR/scripts/sql/seed.sql" >/dev/null

echo "Initialized database at $PGDATA"
echo "Database URL: postgresql://$PGUSER@127.0.0.1:$PGPORT/$PGDATABASE"
