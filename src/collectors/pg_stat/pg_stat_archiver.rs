use chrono::{DateTime, Utc};
use sqlx::{FromRow, Pool, Postgres};

#[derive(Debug, FromRow)]
pub struct PgStatArchiverRow {
    pub archived_count: i64,
    pub last_archived_wal: Option<String>,
    pub last_archived_time: Option<DateTime<Utc>>,
    pub failed_count: i64,
    pub last_failed_wal: Option<String>,
    pub last_failed_time: Option<DateTime<Utc>>,
    pub stats_reset: Option<DateTime<Utc>>,
}

pub struct PgStatArchiverCollector;

impl PgStatArchiverCollector {
    pub async fn collect(pool: &Pool<Postgres>) -> Result<PgStatArchiverRow, sqlx::Error> {
        sqlx::query_as(
            "SELECT
                archived_count,
                last_archived_wal,
                last_archived_time,
                failed_count,
                last_failed_wal,
                last_failed_time,
                stats_reset
            FROM pg_stat_archiver",
        )
        .fetch_one(pool)
        .await
    }
}
