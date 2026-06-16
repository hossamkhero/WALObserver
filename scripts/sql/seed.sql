INSERT INTO accounts (email, tier)
SELECT
  'user' || gs || '@example.com',
  CASE
    WHEN gs % 3 = 0 THEN 'pro'
    WHEN gs % 2 = 0 THEN 'team'
    ELSE 'free'
  END
FROM generate_series(1, 50) AS gs
ON CONFLICT (email) DO NOTHING;

INSERT INTO events (source, payload, quantity, happened_at)
SELECT
  'seed-' || ((gs % 3) + 1),
  jsonb_build_object(
    'kind', 'seed',
    'seed_no', gs,
    'note', md5(gs::text)
  ),
  (gs % 10) + 1,
  now() - make_interval(secs => gs)
FROM generate_series(1, 250) AS gs;

INSERT INTO hot_items (item_key, account_id, note, payload, quantity, touched_at)
SELECT
  'hot-' || gs,
  ((gs - 1) % 50) + 1,
  'seed-hot-' || gs,
  jsonb_build_object(
    'kind', 'hot-seed',
    'seed_no', gs
  ),
  (gs % 10) + 1,
  now() - make_interval(secs => gs)
FROM generate_series(1, 2500) AS gs
ON CONFLICT (item_key) DO NOTHING;

INSERT INTO indexed_items (item_key, source, marker, payload, quantity, happened_at)
SELECT
  'indexed-' || gs,
  'seed-' || ((gs % 3) + 1),
  gs,
  jsonb_build_object(
    'kind', 'indexed-seed',
    'seed_no', gs
  ),
  (gs % 10) + 1,
  now() - make_interval(secs => gs)
FROM generate_series(1, 2500) AS gs
ON CONFLICT (item_key) DO NOTHING;
