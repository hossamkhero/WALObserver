use sqlx::{FromRow, Pool, Postgres};

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct PgStatDatabaseConflictsRow {
    pub datname: String,
    pub confl_tablespace: i64,
    pub confl_lock: i64,
    pub confl_snapshot: i64,
    pub confl_bufferpin: i64,
    pub confl_deadlock: i64,
}

pub struct PgStatDatabaseConflictsCollector;

impl PgStatDatabaseConflictsCollector {
    // Standby-oriented collector. Conflict counters matter when a replica is serving reads
    // while recovery/replay is trying to make progress.
    pub async fn collect(
        pool: &Pool<Postgres>,
    ) -> Result<Vec<PgStatDatabaseConflictsRow>, sqlx::Error> {
        sqlx::query_as(
            "SELECT
                datname,
                confl_tablespace,
                confl_lock,
                confl_snapshot,
                confl_bufferpin,
                confl_deadlock
            FROM pg_stat_database_conflicts
            ORDER BY datname",
        )
        .fetch_all(pool)
        .await
    }
}
