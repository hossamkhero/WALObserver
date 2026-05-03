#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PGDATA="${PGDATA:-$ROOT_DIR/.local/postgres}"

if [[ -f "$PGDATA/PG_VERSION" ]]; then
  pg_ctl -D "$PGDATA" stop >/dev/null 2>&1 || true
fi

rm -rf "$PGDATA"
"$ROOT_DIR/scripts/init-db.sh"
