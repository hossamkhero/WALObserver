use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{FromRow, Pool, Postgres};

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct PgStatReplicationSlotsRow {
    pub slot_name: String,
    pub spill_txns: i64,
    pub spill_count: i64,
    pub spill_bytes: Decimal,
    pub stream_txns: i64,
    pub stream_count: i64,
    pub stream_bytes: Decimal,
    pub total_txns: i64,
    pub total_bytes: Decimal,
    pub stats_reset: Option<DateTime<Utc>>,
}

pub struct PgStatReplicationSlotsCollector;

impl PgStatReplicationSlotsCollector {
    pub async fn collect(
        pool: &Pool<Postgres>,
    ) -> Result<Vec<PgStatReplicationSlotsRow>, sqlx::Error> {
        sqlx::query_as(
            "SELECT
                slot_name,
                spill_txns,
                spill_count,
                spill_bytes,
                stream_txns,
                stream_count,
                stream_bytes,
                total_txns,
                total_bytes,
                stats_reset
            FROM pg_stat_replication_slots
            ORDER BY slot_name",
        )
        .fetch_all(pool)
        .await
    }
}
