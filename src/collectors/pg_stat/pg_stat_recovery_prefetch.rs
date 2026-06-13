use sqlx::{FromRow, Pool, Postgres};

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct PgStatRecoveryPrefetchRow {
    pub prefetch: i64,
    pub hit: i64,
    pub skip_init: i64,
    pub skip_new: i64,
    pub skip_fpw: i64,
    pub skip_rep: i64,
    pub wal_distance: i64,
    pub block_distance: i64,
    pub io_depth: i64,
}

pub struct PgStatRecoveryPrefetchCollector;

impl PgStatRecoveryPrefetchCollector {
    // Standby-oriented collector. These counters only make sense while the server is
    // replaying WAL and attempting recovery prefetch.
    pub async fn collect(pool: &Pool<Postgres>) -> Result<PgStatRecoveryPrefetchRow, sqlx::Error> {
        sqlx::query_as(
            "SELECT
                prefetch,
                hit,
                skip_init,
                skip_new,
                skip_fpw,
                skip_rep,
                wal_distance,
                block_distance,
                io_depth
            FROM pg_stat_recovery_prefetch",
        )
        .fetch_one(pool)
        .await
    }
}
