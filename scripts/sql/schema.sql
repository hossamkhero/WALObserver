CREATE TABLE IF NOT EXISTS accounts (
  id BIGSERIAL PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  tier TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  payload JSONB NOT NULL,
  quantity INTEGER NOT NULL,
  happened_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_happened_at ON events (happened_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_source ON events (source);

CREATE TABLE IF NOT EXISTS hot_items (
  id BIGSERIAL PRIMARY KEY,
  item_key TEXT NOT NULL UNIQUE,
  account_id BIGINT NOT NULL REFERENCES accounts(id),
  note TEXT NOT NULL,
  payload JSONB NOT NULL,
  quantity INTEGER NOT NULL,
  touched_at TIMESTAMPTZ NOT NULL DEFAULT now()
) WITH (fillfactor = 70);

CREATE INDEX IF NOT EXISTS idx_hot_items_account_id ON hot_items (account_id);

CREATE TABLE IF NOT EXISTS indexed_items (
  id BIGSERIAL PRIMARY KEY,
  item_key TEXT NOT NULL UNIQUE,
  source TEXT NOT NULL,
  marker INTEGER NOT NULL,
  payload JSONB NOT NULL,
  quantity INTEGER NOT NULL,
  happened_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_indexed_items_marker ON indexed_items (marker);
CREATE INDEX IF NOT EXISTS idx_indexed_items_source ON indexed_items (source);
