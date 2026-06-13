use sqlx::{FromRow, Pool, Postgres};

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct PgReplicationSlotsRow {
    pub slot_name: String,
    pub plugin: Option<String>,
    pub slot_type: String,
    pub database: Option<String>,
    pub temporary: bool,
    pub active: bool,
    pub active_pid: Option<i32>,
    pub xmin: Option<String>,
    pub catalog_xmin: Option<String>,
    pub restart_lsn: Option<String>,
    pub confirmed_flush_lsn: Option<String>,
}

pub struct PgReplicationSlotsCollector;

impl PgReplicationSlotsCollector {
    pub async fn collect(pool: &Pool<Postgres>) -> Result<Vec<PgReplicationSlotsRow>, sqlx::Error> {
        sqlx::query_as(
            "SELECT
                slot_name,
                plugin,
                slot_type,
                database,
                temporary,
                active,
                active_pid,
                xmin::text AS xmin,
                catalog_xmin::text AS catalog_xmin,
                restart_lsn::text AS restart_lsn,
                confirmed_flush_lsn::text AS confirmed_flush_lsn
            FROM pg_replication_slots
            ORDER BY slot_name",
        )
        .fetch_all(pool)
        .await
    }
}
