use std::{env, thread::sleep, time::Duration};
use sqlx::{FromRow, Pool, Postgres, postgres::PgPoolOptions};

async fn connect_db() -> Pool<Postgres>{
   let database_url = env::var("DATABASE_URL")
          .unwrap_or_else(|_| "postgresql://postgres@127.0.0.1:5433/pg_wal_visualizer".to_string());

    let pool_res = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url).await;


    match pool_res {
        Ok(pool) => pool,
        Err(err) => {
            eprintln!("failed to connect to database: {err}");
            std::process::exit(1);
        }
    }
}

#[derive(Debug, FromRow)]
struct PgStatWal {
    wal_records: i64,
    wal_fpi: i64,
    wal_bytes: rust_decimal::Decimal,
    wal_buffers_full: i64,
    wal_write: i64,
    wal_sync: i64,
    wal_write_time: f64,
    wal_sync_time: f64,
    stats_reset: chrono::DateTime<chrono::Utc>,
}

const INTERVAL: Duration = Duration::new(1, 0) ; // Interval (ms) to fetch the data from pg_stat_wal table. used in `sleep()`

#[tokio::main]
async fn main() {
    let pool = connect_db().await;
    

    while(true) {
        let row: PgStatWal = match sqlx::query_as(
            "SELECT
                wal_records,
                wal_fpi,
                wal_bytes,
                wal_buffers_full,
                wal_write,
                wal_sync,
                wal_write_time,
                wal_sync_time,
                stats_reset
            FROM pg_stat_wal").fetch_one(&pool).await {
            Ok(row) => row,
            Err(err) => {
                eprintln!("failed to connect to database: {err}");
                std::process::exit(1);
            }
        };

        println!("wal_records = {:?}", row.wal_records);
        println!("wal_fpi = {:?}", row.wal_fpi);
        println!("wal_bytes = {:?}", row.wal_bytes);
        println!("wal_buffers_full = {:?}", row.wal_buffers_full);
        println!("wal_write = {:?}", row.wal_write);
        println!("wal_sync = {:?}", row.wal_sync);
        println!("wal_write_time = {:?}", row.wal_write_time);
        println!("wal_sync_time = {:?}", row.wal_sync_time);
        println!("stats_reset = {:?}", row.stats_reset);

        sleep(INTERVAL);
    }
}
