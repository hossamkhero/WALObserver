#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PGHOST="${PGHOST:-$ROOT_DIR/.local/postgres}"
PGPORT="${PGPORT:-5433}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-walobserver}"
RATE_PER_SEC="${RATE_PER_SEC:-5}"
BATCH_SIZE="${BATCH_SIZE:-20}"
MODE="${MODE:-mixed}"

if ! command -v bc >/dev/null 2>&1; then
  echo "bc is required for fractional sleep calculations" >&2
  exit 1
fi

sleep_interval="$(bc <<< "scale=4; 1 / $RATE_PER_SEC")"

run_sql() {
  local sql="$1"
  psql -X -q -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -c "$sql" >/dev/null
}

ensure_demo_schema() {
  psql -X -q -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 \
    -f "$ROOT_DIR/scripts/sql/schema.sql" >/dev/null
  psql -X -q -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 \
    -f "$ROOT_DIR/scripts/sql/seed.sql" >/dev/null
}

insert_batch() {
  run_sql "
    INSERT INTO events (source, payload, quantity, happened_at)
    SELECT
      'generator-' || ((random() * 3)::int + 1),
      jsonb_build_object(
        'kind', 'insert',
        'value', md5(random()::text),
        'batch_size', $BATCH_SIZE
      ),
      (random() * 25)::int + 1,
      now()
    FROM generate_series(1, $BATCH_SIZE);
  "
}

hot_update_batch() {
  run_sql "
    WITH picked AS (
      SELECT id
      FROM hot_items
      ORDER BY random()
      LIMIT $BATCH_SIZE
    )
    UPDATE hot_items
    SET
      quantity = quantity + 1,
      note = note || '-h',
      payload = jsonb_set(payload, '{kind}', '\"hot-update\"'),
      touched_at = now()
    WHERE id IN (SELECT id FROM picked);
  "
}

non_hot_update_batch() {
  run_sql "
    WITH picked AS (
      SELECT id
      FROM indexed_items
      ORDER BY random()
      LIMIT $BATCH_SIZE
    )
    UPDATE indexed_items
    SET
      marker = marker + 1,
      source = 'generator-' || ((random() * 3)::int + 1),
      payload = jsonb_set(payload, '{kind}', '\"non-hot-update\"'),
      happened_at = now(),
      quantity = quantity + 1
    WHERE id IN (SELECT id FROM picked);
  "
}

update_batch() {
  run_sql "
    WITH picked AS (
      SELECT id
      FROM events
      ORDER BY random()
      LIMIT $BATCH_SIZE
    )
    UPDATE events
    SET
      quantity = quantity + 1,
      payload = jsonb_set(payload, '{kind}', '\"update\"'),
      happened_at = now()
    WHERE id IN (SELECT id FROM picked);
  "
}

mixed_batch() {
  insert_batch
  non_hot_update_batch
}

burst_batch() {
  local burst_size=$((BATCH_SIZE * 5))

  run_sql "
    INSERT INTO events (source, payload, quantity, happened_at)
    SELECT
      'burst-' || ((random() * 3)::int + 1),
      jsonb_build_object(
        'kind', 'burst',
        'value', md5(random()::text),
        'batch_size', $burst_size
      ),
      (random() * 25)::int + 1,
      now()
    FROM generate_series(1, $burst_size);
  "

  non_hot_update_batch
}

echo "Generating load against postgresql://$PGUSER@$PGHOST:$PGPORT/$PGDATABASE"
echo "mode=$MODE rate_per_sec=$RATE_PER_SEC batch_size=$BATCH_SIZE"
echo "modes=insert|update|mixed|hot|non_hot|burst"

ensure_demo_schema

while true; do
  case "$MODE" in
    insert) insert_batch ;;
    update) update_batch ;;
    mixed) mixed_batch ;;
    hot) hot_update_batch ;;
    non_hot) non_hot_update_batch ;;
    burst) burst_batch ;;
    *)
      echo "Unsupported MODE=$MODE. Use insert, update, mixed, hot, non_hot, or burst." >&2
      exit 1
      ;;
  esac

  sleep "$sleep_interval"
done
