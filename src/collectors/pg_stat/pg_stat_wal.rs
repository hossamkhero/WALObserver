use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{FromRow, Pool, Postgres};

#[derive(Debug, Clone, PartialEq, FromRow)]
pub struct PgStatWalRow {
    pub wal_records: i64,
    pub wal_fpi: i64,
    pub wal_bytes: Decimal,
    pub wal_buffers_full: i64,
    pub wal_write: i64,
    pub wal_sync: i64,
    pub wal_write_time: f64,
    pub wal_sync_time: f64,
    pub stats_reset: DateTime<Utc>,
}

pub struct PgStatWalCollector;

impl PgStatWalCollector {
    pub async fn collect(pool: &Pool<Postgres>) -> Result<PgStatWalRow, sqlx::Error> {
        sqlx::query_as(
            "SELECT
                wal_records,
                wal_fpi,
                wal_bytes,
                wal_buffers_full,
                wal_write,
                wal_sync,
                wal_write_time,
                wal_sync_time,
                stats_reset
            FROM pg_stat_wal",
        )
        .fetch_one(pool)
        .await
    }
}
