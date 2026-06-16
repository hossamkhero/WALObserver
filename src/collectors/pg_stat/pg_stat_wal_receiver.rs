use chrono::{DateTime, Utc};
use sqlx::{FromRow, Pool, Postgres};

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct PgStatWalReceiverRow {
    pub pid: i32,
    pub status: String,
    pub receive_start_lsn: Option<String>,
    pub written_lsn: Option<String>,
    pub flushed_lsn: Option<String>,
    pub latest_end_lsn: Option<String>,
    pub last_msg_send_time: Option<DateTime<Utc>>,
    pub last_msg_receipt_time: Option<DateTime<Utc>>,
    pub latest_end_time: Option<DateTime<Utc>>,
    pub slot_name: Option<String>,
    pub sender_host: Option<String>,
    pub sender_port: Option<i32>,
}

pub struct PgStatWalReceiverCollector;

impl PgStatWalReceiverCollector {
    // Standby-oriented collector. This view describes the WAL receiver process, so it is
    // only meaningful on nodes that are receiving WAL from an upstream server.
    pub async fn collect(pool: &Pool<Postgres>) -> Result<Option<PgStatWalReceiverRow>, sqlx::Error> {
        sqlx::query_as(
            "SELECT
                pid,
                status,
                receive_start_lsn::text AS receive_start_lsn,
                written_lsn::text AS written_lsn,
                flushed_lsn::text AS flushed_lsn,
                latest_end_lsn::text AS latest_end_lsn,
                last_msg_send_time,
                last_msg_receipt_time,
                latest_end_time,
                slot_name,
                sender_host,
                sender_port
            FROM pg_stat_wal_receiver",
        )
        .fetch_optional(pool)
        .await
    }
}
