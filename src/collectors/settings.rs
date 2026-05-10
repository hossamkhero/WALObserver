use sqlx::{FromRow, Pool, Postgres};

#[derive(Debug, FromRow)]
pub struct PgSettingRow {
    pub name: String,
    pub setting: String,
    pub unit: Option<String>,
    pub source: String,
    pub short_desc: String,
}

pub struct SettingsCollector;

impl SettingsCollector {
    pub async fn collect(pool: &Pool<Postgres>) -> Result<Vec<PgSettingRow>, sqlx::Error> {
        sqlx::query_as(
            "SELECT
                name,
                setting,
                unit,
                source,
                short_desc
            FROM pg_settings
            WHERE name = ANY($1)
            ORDER BY name",
        )
        .bind([
            "full_page_writes",
            "checkpoint_timeout",
            "max_wal_size",
            "min_wal_size",
            "wal_compression",
            "synchronous_commit",
        ])
        .fetch_all(pool)
        .await
    }
}
