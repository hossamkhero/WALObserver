#![allow(dead_code)]

use chrono::{DateTime, Utc};
use sqlx::{FromRow, Pool, Postgres};

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct WalFunctionsRow {
    pub is_in_recovery: bool,
    pub current_wal_lsn: Option<String>,
    pub last_wal_receive_lsn: Option<String>,
    pub last_wal_replay_lsn: Option<String>,
    pub last_xact_replay_timestamp: Option<DateTime<Utc>>,
}

pub struct WalFunctionsCollector;

impl WalFunctionsCollector {
    // Combined WAL function collector. Some fields are primary-oriented and others are
    // standby-oriented, so several columns may be NULL depending on the server role.
    pub async fn collect(pool: &Pool<Postgres>) -> Result<WalFunctionsRow, sqlx::Error> {
        sqlx::query_as(
            "SELECT
                pg_is_in_recovery() AS is_in_recovery,
                CASE
                    WHEN pg_is_in_recovery() THEN NULL
                    ELSE pg_current_wal_lsn()::text
                END AS current_wal_lsn,
                pg_last_wal_receive_lsn()::text AS last_wal_receive_lsn,
                pg_last_wal_replay_lsn()::text AS last_wal_replay_lsn,
                pg_last_xact_replay_timestamp() AS last_xact_replay_timestamp",
        )
        .fetch_one(pool)
        .await
    }
}
