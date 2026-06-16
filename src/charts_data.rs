use crate::readers::main_log::{MainLogRecord, MaterializedTickState, apply_tick_snapshot};

pub struct SeriesPoint {
    pub ts_ms: u64,
    pub value: f64,
}

pub struct WalActivityChart {
    pub wal_bytes_per_sec: Vec<SeriesPoint>,
    pub wal_records_per_sec: Vec<SeriesPoint>,
}

pub struct SlotRetentionChart {
    pub worst_slot_lag_bytes: Vec<SeriesPoint>,
    pub wal_dir_file_count: Vec<SeriesPoint>,
    pub latest_worst_slot_name: Option<String>,
}

pub struct WalAmplificationChart {
    pub wal_bytes_per_record: Vec<SeriesPoint>,
    pub updates_per_sec: Vec<SeriesPoint>,
    pub hot_update_ratio: Vec<SeriesPoint>,
}

pub struct StandbyReplayChart {
    pub receive_lsn_progress: Vec<SeriesPoint>,
    pub replay_lsn_progress: Vec<SeriesPoint>,
    pub latest_receiver_status: String,
}

pub struct ChartsData {
    pub wal_activity: WalActivityChart,
    pub slot_retention: SlotRetentionChart,
    pub wal_amplification: WalAmplificationChart,
    pub standby_replay: Option<StandbyReplayChart>,
}

pub fn build_charts(records: &[MainLogRecord]) -> ChartsData {
    let ticks = materialize_tick_timeline(records);
    let latest_tick = ticks.last().map(|tick| &tick.state);
    let is_standby = latest_tick.and_then(|tick| tick.wal_functions.as_ref()).map(|wal| wal.is_in_recovery).unwrap_or(false);

    ChartsData {
        wal_activity: WalActivityChart {
            wal_bytes_per_sec: derive_wal_bytes_per_sec(&ticks),
            wal_records_per_sec: derive_wal_records_per_sec(&ticks),
        },
        slot_retention: SlotRetentionChart {
            worst_slot_lag_bytes: derive_worst_slot_lag_bytes(&ticks),
            wal_dir_file_count: derive_wal_dir_file_count(&ticks),
            latest_worst_slot_name: derive_latest_worst_slot_name(&ticks),
        },
        wal_amplification: WalAmplificationChart {
            wal_bytes_per_record: derive_wal_bytes_per_record(&ticks),
            updates_per_sec: derive_updates_per_sec(&ticks),
            hot_update_ratio: derive_hot_update_ratio(&ticks),
        },
        standby_replay: if is_standby {
            Some(StandbyReplayChart {
                receive_lsn_progress: derive_receive_lsn_progress(&ticks),
                replay_lsn_progress: derive_replay_lsn_progress(&ticks),
                latest_receiver_status: latest_tick
                    .and_then(|tick| tick.pg_stat_wal_receiver.as_ref())
                    .and_then(|receiver| receiver.as_ref())
                    .map(|receiver| receiver.status.clone())
                    .unwrap_or_else(|| "-".to_string()),
            })
        } else {
            None
        },
    }
}

struct TickAt {
    ts_ms: u64,
    state: MaterializedTickState,
}

fn materialize_tick_timeline(records: &[MainLogRecord]) -> Vec<TickAt> {
    let mut current = MaterializedTickState::default();
    let mut timeline = Vec::new();

    for record in records {
        if let MainLogRecord::Tick { header, payload } = record {
            apply_tick_snapshot(&mut current, payload);
            timeline.push(TickAt { ts_ms: header.timestamp_ms, state: current.clone() });
        }
    }

    timeline
}

fn derive_wal_bytes_per_sec(ticks: &[TickAt]) -> Vec<SeriesPoint> {
    derive_rate_series(ticks, |tick| tick.state.pg_stat_wal.as_ref().and_then(|wal| wal.wal_bytes.parse::<f64>().ok()))
}

fn derive_wal_records_per_sec(ticks: &[TickAt]) -> Vec<SeriesPoint> {
    derive_rate_series(ticks, |tick| tick.state.pg_stat_wal.as_ref().map(|wal| wal.wal_records as f64))
}

fn derive_worst_slot_lag_bytes(ticks: &[TickAt]) -> Vec<SeriesPoint> {
    ticks.iter().filter_map(|tick| {
        let current_lsn = tick.state.wal_functions.as_ref().and_then(|wal| wal.current_wal_lsn.as_deref()).and_then(parse_lsn)?;
        let slots = tick.state.pg_replication_slots.as_ref()?;
        let value = slots
            .iter()
            .filter_map(|slot| slot.restart_lsn.as_deref().and_then(parse_lsn))
            .map(|restart_lsn| current_lsn.saturating_sub(restart_lsn) as f64)
            .max_by(|a, b| a.partial_cmp(b).unwrap())?;

        Some(SeriesPoint { ts_ms: tick.ts_ms, value })
    }).collect()
}

fn derive_wal_dir_file_count(ticks: &[TickAt]) -> Vec<SeriesPoint> {
    ticks
        .iter()
        .filter_map(|tick| tick.state.wal_dir.as_ref().map(|wal_dir| SeriesPoint { ts_ms: tick.ts_ms, value: wal_dir.n_files as f64 }))
        .collect()
}

fn derive_latest_worst_slot_name(ticks: &[TickAt]) -> Option<String> {
    let latest_tick = ticks.last()?;
    let current_lsn = latest_tick.state.wal_functions.as_ref().and_then(|wal| wal.current_wal_lsn.as_deref()).and_then(parse_lsn)?;

    latest_tick
        .state
        .pg_replication_slots
        .as_ref()?
        .iter()
        .filter_map(|slot| {
            let restart_lsn = slot.restart_lsn.as_deref().and_then(parse_lsn)?;
            Some((current_lsn.saturating_sub(restart_lsn), slot.slot_name.clone()))
        })
        .max_by_key(|(lag, _)| *lag)
        .map(|(_, slot_name)| slot_name)
}

fn derive_wal_bytes_per_record(ticks: &[TickAt]) -> Vec<SeriesPoint> {
    let bytes_per_sec = derive_wal_bytes_per_sec(ticks);
    let records_per_sec = derive_wal_records_per_sec(ticks);

    bytes_per_sec
        .into_iter()
        .zip(records_per_sec)
        .filter_map(|(bytes, records)| {
            if records.value > 0.0 {
                Some(SeriesPoint { ts_ms: bytes.ts_ms, value: bytes.value / records.value })
            } else {
                None
            }
        })
        .collect()
}

fn derive_updates_per_sec(ticks: &[TickAt]) -> Vec<SeriesPoint> {
    derive_rate_series(ticks, |tick| {
        tick.state
            .pg_stat_user_tables
            .as_ref()
            .map(|tables| tables.iter().map(|table| table.n_tup_upd).sum::<i64>() as f64)
    })
}

fn derive_hot_update_ratio(ticks: &[TickAt]) -> Vec<SeriesPoint> {
    let mut points = Vec::new();

    for pair in ticks.windows(2) {
        let prev = &pair[0];
        let curr = &pair[1];

        let prev_tables = match &prev.state.pg_stat_user_tables {
            Some(tables) => tables,
            None => continue,
        };
        let curr_tables = match &curr.state.pg_stat_user_tables {
            Some(tables) => tables,
            None => continue,
        };

        let prev_upd: i64 = prev_tables.iter().map(|table| table.n_tup_upd).sum();
        let curr_upd: i64 = curr_tables.iter().map(|table| table.n_tup_upd).sum();
        let prev_hot: i64 = prev_tables.iter().map(|table| table.n_tup_hot_upd).sum();
        let curr_hot: i64 = curr_tables.iter().map(|table| table.n_tup_hot_upd).sum();

        let upd_delta = curr_upd - prev_upd;
        let hot_delta = curr_hot - prev_hot;

        if upd_delta <= 0 || hot_delta < 0 {
            continue;
        }

        points.push(SeriesPoint { ts_ms: curr.ts_ms, value: hot_delta as f64 / upd_delta as f64 });
    }

    points
}

fn derive_receive_lsn_progress(ticks: &[TickAt]) -> Vec<SeriesPoint> {
    derive_rate_series(ticks, |tick| {
        tick.state
            .wal_functions
            .as_ref()
            .and_then(|wal| wal.last_wal_receive_lsn.as_deref())
            .and_then(parse_lsn)
            .map(|lsn| lsn as f64)
    })
}

fn derive_replay_lsn_progress(ticks: &[TickAt]) -> Vec<SeriesPoint> {
    derive_rate_series(ticks, |tick| {
        tick.state
            .wal_functions
            .as_ref()
            .and_then(|wal| wal.last_wal_replay_lsn.as_deref())
            .and_then(parse_lsn)
            .map(|lsn| lsn as f64)
    })
}

fn derive_rate_series<F>(ticks: &[TickAt], extractor: F) -> Vec<SeriesPoint>
where
    F: Fn(&TickAt) -> Option<f64>,
{
    let mut points = Vec::new();

    for pair in ticks.windows(2) {
        let prev = &pair[0];
        let curr = &pair[1];

        let prev_value = match extractor(prev) {
            Some(value) => value,
            None => continue,
        };
        let curr_value = match extractor(curr) {
            Some(value) => value,
            None => continue,
        };

        let dt_sec = (curr.ts_ms.saturating_sub(prev.ts_ms) as f64) / 1000.0;
        if dt_sec <= 0.0 || curr_value < prev_value {
            continue;
        }

        points.push(SeriesPoint { ts_ms: curr.ts_ms, value: (curr_value - prev_value) / dt_sec });
    }

    points
}

fn parse_lsn(value: &str) -> Option<u64> {
    let (hi, lo) = value.split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some((hi << 32) + lo)
}
