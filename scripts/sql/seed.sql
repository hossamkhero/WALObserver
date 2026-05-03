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
