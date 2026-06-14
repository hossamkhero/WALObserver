use pg_wal_visualizer::events::{RuntimeState, on_disconnect, on_reconnect, on_role_observed};
use pg_wal_visualizer::storage::{
    append_event_snapshot, append_settings_snapshot, append_tick_snapshot, init_storage,
};
use pg_wal_visualizer::tick::{
    TickData, apply_tick_diff, build_stored_settings_snapshot, build_stored_tick_snapshot,
    collect_tick, diff_settings, diff_tick,
};
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use std::{env, time::Duration};
use tokio::time::sleep;

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
                        append_event_snapshot(&log_path, &event)?;
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
                    append_event_snapshot(&log_path, &event)?;
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
            append_event_snapshot(&log_path, &event)?;
            println!("{event:#?}");
        }

        let settings_changed_mask = diff_settings(last_tick.as_ref().map(|tick| tick.settings.as_slice()), &tick.settings);
        let changed_mask = diff_tick(last_tick.as_ref(), &tick);
        let stored_tick = build_stored_tick_snapshot(&tick, changed_mask);

        if settings_changed_mask != 0 {
            let stored_settings = build_stored_settings_snapshot(&tick.settings, settings_changed_mask);
            append_settings_snapshot(&log_path, &stored_settings)?;
        }

        append_tick_snapshot(&log_path, &stored_tick)?;
        println!("tick changed_mask = {changed_mask:014b}");

        match last_tick.as_mut() {
            Some(previous_tick) => apply_tick_diff(previous_tick, &tick, changed_mask),
            None => last_tick = Some(tick.clone()),
        }

        // debug_print_tick(&tick);

        sleep(INTERVAL).await;
    }
}
