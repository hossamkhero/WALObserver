use chrono::{DateTime, Utc};
use sqlx::{FromRow, Pool, Postgres};

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct PgStatDatabaseRow {
    pub datname: String,
    pub numbackends: i32,
    pub xact_commit: i64,
    pub xact_rollback: i64,
    pub blks_read: i64,
    pub blks_hit: i64,
    pub tup_inserted: i64,
    pub tup_updated: i64,
    pub tup_deleted: i64,
    pub temp_files: i64,
    pub temp_bytes: i64,
    pub deadlocks: i64,
    pub stats_reset: Option<DateTime<Utc>>,
}

pub struct PgStatDatabaseCollector;

impl PgStatDatabaseCollector {
    pub async fn collect(pool: &Pool<Postgres>) -> Result<Vec<PgStatDatabaseRow>, sqlx::Error> {
        sqlx::query_as(
            "SELECT
                datname,
                numbackends,
                xact_commit,
                xact_rollback,
                blks_read,
                blks_hit,
                tup_inserted,
                tup_updated,
                tup_deleted,
                temp_files,
                temp_bytes,
                deadlocks,
                stats_reset
            FROM pg_stat_database
            WHERE datname IS NOT NULL
            ORDER BY datname",
        )
        .fetch_all(pool)
        .await
    }
}
