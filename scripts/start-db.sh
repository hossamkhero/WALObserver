#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PGDATA="${PGDATA:-$ROOT_DIR/.local/postgres}"

if [[ ! -f "$PGDATA/PG_VERSION" ]]; then
  "$ROOT_DIR/scripts/init-db.sh"
  exit 0
fi

pg_ctl -D "$PGDATA" -l "$PGDATA/postgres.log" start
