use sqlx::{FromRow, Pool, Postgres};

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct PgStatReplicationRow {
    pub pid: i32,
    pub usename: String,
    pub application_name: String,
    pub client_addr: Option<String>,
    pub state: String,
    pub sync_state: String,
    pub sent_lsn: String,
    pub write_lsn: String,
    pub flush_lsn: String,
    pub replay_lsn: String,
}

pub struct PgStatReplicationCollector;

impl PgStatReplicationCollector {
    pub async fn collect(pool: &Pool<Postgres>) -> Result<Vec<PgStatReplicationRow>, sqlx::Error> {
        sqlx::query_as(
            "SELECT
                pid,
                usename,
                application_name,
                client_addr::text AS client_addr,
                state,
                sync_state,
                sent_lsn::text AS sent_lsn,
                write_lsn::text AS write_lsn,
                flush_lsn::text AS flush_lsn,
                replay_lsn::text AS replay_lsn
            FROM pg_stat_replication
            ORDER BY pid",
        )
        .fetch_all(pool)
        .await
    }
}
