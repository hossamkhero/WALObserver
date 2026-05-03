# pg_wal_visualizer

Local stack scaffolding for a Postgres WAL observer.

## Stack

- Rust stable toolchain plus a dependency manifest in `Cargo.toml`
- PostgreSQL 16 running locally from the repo
- Nix flake dev shell for reproducible setup
- `just` recipes for common workflow commands

## Bootstrapping

```bash
nix develop path:. -c zsh
just db-init
```

The flake shell exports:

- `PGDATA=$PWD/.local/postgres`
- `PGHOST=$PWD/.local/postgres`
- `PGPORT=5433`
- `PGUSER=postgres`
- `PGDATABASE=pg_wal_visualizer`
- `DATABASE_URL=postgresql://postgres@127.0.0.1:5433/pg_wal_visualizer`

## Database lifecycle

```bash
just db-start
just db-stop
just db-reset
```

Schema and seed data live in [scripts/sql/schema.sql](/home/longassnixochad/Desktop/me/pg_wal_visualizer/scripts/sql/schema.sql:1) and [scripts/sql/seed.sql](/home/longassnixochad/Desktop/me/pg_wal_visualizer/scripts/sql/seed.sql:1).

## Continuous WAL-producing load

```bash
RATE_PER_SEC=5 BATCH_SIZE=20 MODE=mixed just load
```

Supported modes:

- `insert`
- `update`
- `mixed`

This keeps writing to the seeded `events` table so the future visualizer has changing WAL volume to observe.

## Rust crates chosen for the setup

- `tokio`: async runtime for polling loops
- `sqlx`: direct Postgres access without an ORM
- `clap`: CLI commands like `inspect live` or `run experiment`
- `tracing` and `tracing-subscriber`: structured logs during experiments
- `serde` and `serde_json`: payload and config serialization
- `chrono`, `uuid`: common DB-facing types

These are listed only as workspace dependencies for now. No Rust app logic is scaffolded.
