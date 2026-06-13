mod collectors;
mod events;
mod storage;
mod tick;

use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use std::{env, time::Duration};
use tokio::time::sleep;

use events::{RuntimeState, on_disconnect, on_reconnect, on_role_observed};
use storage::init_storage;
use tick::{TickData, apply_tick_diff, collect_tick, debug_print_tick, diff_tick};

async fn connect_db() -> Result<Pool<Postgres>, sqlx::Error> {
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres@127.0.0.1:5433/pg_wal_visualizer".to_string());

    PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
}

const INTERVAL: Duration = Duration::new(1, 0);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (log_path, checkpoint_path) = init_storage()?;
    let mut runtime = RuntimeState::default();
    let mut last_tick: Option<TickData> = None;
    let mut pool = connect_db().await.ok();

    println!("main log file = {}", log_path.display());
    println!("checkpoint file = {}", checkpoint_path.display());

    if pool.is_some() {
        runtime.connected = true;
    }

    loop {
        // Pool initialization
        if pool.is_none() {
            match connect_db().await {
                Ok(new_pool) => {
                    pool = Some(new_pool);

                    if let Some(event) = on_reconnect(&mut runtime) {
                        println!("{event:#?}");
                    }
                }
                Err(err) => {
                    eprintln!("database still disconnected: {err}");
                    sleep(INTERVAL).await;
                    continue;
                }
            }
        }

        let active_pool = pool.as_ref().unwrap();

        // tick (all collectors / events)
        let tick: TickData = match collect_tick(active_pool, runtime.role).await {
            Ok(tick) => tick,
            Err(err) => {
                if let Some(event) = on_disconnect(&mut runtime) {
                    println!("{event:#?}");
                }

                last_tick = None;
                runtime.role = None;
                pool = None;

                eprintln!("failed to collect tick: {err}");

                sleep(INTERVAL).await;

                continue;
            }
        };

        // collect events
        if let Some(event) = on_role_observed(&mut runtime, &tick.wal_functions) {
            println!("{event:#?}");
        }

        // 
        let changed_mask = diff_tick(last_tick.as_ref(), &tick);
        println!("tick changed_mask = {changed_mask:014b}");

        match last_tick.as_mut() {
            Some(previous_tick) => apply_tick_diff(previous_tick, &tick, changed_mask),
            None => last_tick = Some(tick.clone()),
        }

        debug_print_tick(&tick);

        sleep(INTERVAL).await;
    }
}
