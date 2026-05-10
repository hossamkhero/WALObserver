use sqlx::{FromRow, Pool, Postgres};

#[derive(Debug, FromRow)]
pub struct PgStatUserTablesRow {
    pub schemaname: String,
    pub relname: String,
    pub seq_scan: i64,
    pub idx_scan: i64,
    pub n_tup_ins: i64,
    pub n_tup_upd: i64,
    pub n_tup_del: i64,
    pub n_tup_hot_upd: i64,
    pub n_live_tup: i64,
    pub n_dead_tup: i64,
    pub vacuum_count: i64,
    pub autovacuum_count: i64,
    pub analyze_count: i64,
    pub autoanalyze_count: i64,
}

pub struct PgStatUserTablesCollector;

impl PgStatUserTablesCollector {
    pub async fn collect(pool: &Pool<Postgres>) -> Result<Vec<PgStatUserTablesRow>, sqlx::Error> {
        sqlx::query_as(
            "SELECT
                schemaname,
                relname,
                seq_scan,
                idx_scan,
                n_tup_ins,
                n_tup_upd,
                n_tup_del,
                n_tup_hot_upd,
                n_live_tup,
                n_dead_tup,
                vacuum_count,
                autovacuum_count,
                analyze_count,
                autoanalyze_count
            FROM pg_stat_user_tables",
        )
        .fetch_all(pool)
        .await
    }
}
