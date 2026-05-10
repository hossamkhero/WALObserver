mod collectors;

use std::{env, time::Duration};

use collectors::pg_stat::pg_stat_wal::{PgStatWalCollector, PgStatWalRow};
use collectors::settings::{PgSettingRow, SettingsCollector};
use collectors::wal_dir::{WalDirCollector, WalDirStats};

use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use tokio::time::sleep;

async fn connect_db() -> Pool<Postgres> {
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres@127.0.0.1:5433/pg_wal_visualizer".to_string());

    let pool_res = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await;

    match pool_res {
        Ok(pool) => pool,
        Err(err) => {
            eprintln!("failed to connect to database: {err}");
            std::process::exit(1);
        }
    }
}

const INTERVAL: Duration = Duration::new(1, 0);

#[tokio::main]
async fn main() {
    let pool = connect_db().await;

    let wal_dir_collector: WalDirStats = match WalDirCollector::collect() {
        Ok(wal_dir_collector) => wal_dir_collector,
        Err(err) => {
            eprintln!("failed to collect pg_wal filesystem stats: {err}");
            std::process::exit(1);
        }
    };

    println!("{wal_dir_collector:#?}");

    //loop {
    //    let wal_collector: PgStatWalRow = match PgStatWalCollector::collect(&pool).await {
    //        Ok(wal_collector) => wal_collector,
    //        Err(err) => {
    //            eprintln!("failed to collect pg_stat_wal: {err}");
    //            std::process::exit(1);
    //        }
    //    };
    //
    //    let settings_collector: Vec<PgSettingRow> = match SettingsCollector::collect(&pool).await {
    //        Ok(settings_collector) => settings_collector,
    //        Err(err) => {
    //            eprintln!("failed to collect pg_settings: {err}");
    //            std::process::exit(1);
    //        }
    //    };
    //
    //    println!("{wal_collector:#?}");
    //
    //    settings_collector.iter().for_each(|s| {
    //        println!("{s:#?}");
    //    });
    //
    //
    //    sleep(INTERVAL).await;
    //}
    //
}
