use chrono::{DateTime, Utc};
use sqlx::{FromRow, Pool, Postgres};

#[derive(Debug, FromRow)]
pub struct PgStatActivityRow {
    pub pid: i32,
    pub datname: Option<String>,
    pub usename: Option<String>,
    pub application_name: Option<String>,
    pub state: Option<String>,
    pub wait_event_type: Option<String>,
    pub wait_event: Option<String>,
    pub xact_start: Option<DateTime<Utc>>,
    pub query_start: Option<DateTime<Utc>>,
    pub query: String,
}

pub struct PgStatActivityCollector;

impl PgStatActivityCollector {
    pub async fn collect(pool: &Pool<Postgres>) -> Result<Vec<PgStatActivityRow>, sqlx::Error> {
        sqlx::query_as(
            "SELECT
                pid,
                datname,
                usename,
                application_name,
                state,
                wait_event_type,
                wait_event,
                xact_start,
                query_start,
                query
            FROM pg_stat_activity",
        )
        .fetch_all(pool)
        .await
    }
}
