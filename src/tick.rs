use crate::collectors::pg_stat::*;
use crate::collectors::*;
use crate::events::DbRole;
use chrono::{DateTime, Utc};
use rkyv::{Archive, Deserialize, Serialize};
use rust_decimal::Decimal;
use sqlx::{Pool, Postgres};
use std::ffi::OsString;

pub const TICK_BIT_WAL_DIR: u16 = 1 << 0;
pub const TICK_BIT_WAL_FUNCTIONS: u16 = 1 << 1;
pub const TICK_BIT_PG_STAT_WAL: u16 = 1 << 2;
pub const TICK_BIT_PG_REPLICATION_SLOTS: u16 = 1 << 3;
pub const TICK_BIT_PG_STAT_ACTIVITY: u16 = 1 << 4;
pub const TICK_BIT_PG_STAT_ARCHIVER: u16 = 1 << 5;
pub const TICK_BIT_PG_STAT_BGWRITER: u16 = 1 << 6;
pub const TICK_BIT_PG_STAT_DATABASE: u16 = 1 << 7;
pub const TICK_BIT_PG_STAT_REPLICATION: u16 = 1 << 8;
pub const TICK_BIT_PG_STAT_REPLICATION_SLOTS: u16 = 1 << 9;
pub const TICK_BIT_PG_STAT_USER_TABLES: u16 = 1 << 10;
pub const TICK_BIT_PG_STAT_DATABASE_CONFLICTS: u16 = 1 << 11;
pub const TICK_BIT_PG_STAT_RECOVERY_PREFETCH: u16 = 1 << 12;
pub const TICK_BIT_PG_STAT_WAL_RECEIVER: u16 = 1 << 13;

pub const ALL_TICK_BITS: u16 = TICK_BIT_WAL_DIR
    | TICK_BIT_WAL_FUNCTIONS
    | TICK_BIT_PG_STAT_WAL
    | TICK_BIT_PG_REPLICATION_SLOTS
    | TICK_BIT_PG_STAT_ACTIVITY
    | TICK_BIT_PG_STAT_ARCHIVER
    | TICK_BIT_PG_STAT_BGWRITER
    | TICK_BIT_PG_STAT_DATABASE
    | TICK_BIT_PG_STAT_REPLICATION
    | TICK_BIT_PG_STAT_REPLICATION_SLOTS
    | TICK_BIT_PG_STAT_USER_TABLES
    | TICK_BIT_PG_STAT_DATABASE_CONFLICTS
    | TICK_BIT_PG_STAT_RECOVERY_PREFETCH
    | TICK_BIT_PG_STAT_WAL_RECEIVER;

pub const SETTINGS_BIT_FULL_PAGE_WRITES: u16 = 1 << 0;
pub const SETTINGS_BIT_CHECKPOINT_TIMEOUT: u16 = 1 << 1;
pub const SETTINGS_BIT_MAX_WAL_SIZE: u16 = 1 << 2;
pub const SETTINGS_BIT_MIN_WAL_SIZE: u16 = 1 << 3;
pub const SETTINGS_BIT_WAL_COMPRESSION: u16 = 1 << 4;
pub const SETTINGS_BIT_SYNCHRONOUS_COMMIT: u16 = 1 << 5;

pub const ALL_SETTINGS_BITS: u16 = SETTINGS_BIT_FULL_PAGE_WRITES
    | SETTINGS_BIT_CHECKPOINT_TIMEOUT
    | SETTINGS_BIT_MAX_WAL_SIZE
    | SETTINGS_BIT_MIN_WAL_SIZE
    | SETTINGS_BIT_WAL_COMPRESSION
    | SETTINGS_BIT_SYNCHRONOUS_COMMIT;

#[derive(Debug, Clone, PartialEq)]
pub struct TickData {
    pub wal_dir: WalDirStats,
    pub wal_functions: WalFunctionsRow,
    pub pg_stat_wal: PgStatWalRow,
    pub settings: Vec<PgSettingRow>,
    pub pg_replication_slots: Vec<PgReplicationSlotsRow>,
    pub pg_stat_activity: Vec<PgStatActivityRow>,
    pub pg_stat_archiver: PgStatArchiverRow,
    pub pg_stat_bgwriter: PgStatBgwriterRow,
    pub pg_stat_database: Vec<PgStatDatabaseRow>,
    pub pg_stat_replication: Vec<PgStatReplicationRow>,
    pub pg_stat_replication_slots: Vec<PgStatReplicationSlotsRow>,
    pub pg_stat_user_tables: Vec<PgStatUserTablesRow>,
    pub pg_stat_database_conflicts: Option<Vec<PgStatDatabaseConflictsRow>>,
    pub pg_stat_recovery_prefetch: Option<PgStatRecoveryPrefetchRow>,
    pub pg_stat_wal_receiver: Option<Option<PgStatWalReceiverRow>>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredWalDirStats {
    pub total_size_logical: u64,
    pub total_size_physical: u64,
    pub n_files: usize,
    pub files: Vec<String>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredSettingRow {
    pub name: String,
    pub setting: String,
    pub unit: Option<String>,
    pub source: String,
    pub short_desc: String,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredWalFunctionsRow {
    pub is_in_recovery: bool,
    pub current_wal_lsn: Option<String>,
    pub last_wal_receive_lsn: Option<String>,
    pub last_wal_replay_lsn: Option<String>,
    pub last_xact_replay_timestamp_ms: Option<i64>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredPgStatWalRow {
    pub wal_records: i64,
    pub wal_fpi: i64,
    pub wal_bytes: String,
    pub wal_buffers_full: i64,
    pub wal_write: i64,
    pub wal_sync: i64,
    pub wal_write_time: f64,
    pub wal_sync_time: f64,
    pub stats_reset_ms: i64,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredPgReplicationSlotsRow {
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

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredPgStatActivityRow {
    pub pid: i32,
    pub datname: Option<String>,
    pub usename: Option<String>,
    pub application_name: Option<String>,
    pub state: Option<String>,
    pub wait_event_type: Option<String>,
    pub wait_event: Option<String>,
    pub xact_start_ms: Option<i64>,
    pub query_start_ms: Option<i64>,
    pub query: String,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredPgStatArchiverRow {
    pub archived_count: i64,
    pub last_archived_wal: Option<String>,
    pub last_archived_time_ms: Option<i64>,
    pub failed_count: i64,
    pub last_failed_wal: Option<String>,
    pub last_failed_time_ms: Option<i64>,
    pub stats_reset_ms: Option<i64>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredPgStatBgwriterRow {
    pub checkpoints_timed: i64,
    pub checkpoints_req: i64,
    pub checkpoint_write_time: f64,
    pub checkpoint_sync_time: f64,
    pub buffers_checkpoint: i64,
    pub buffers_clean: i64,
    pub maxwritten_clean: i64,
    pub buffers_backend: i64,
    pub buffers_backend_fsync: i64,
    pub buffers_alloc: i64,
    pub stats_reset_ms: Option<i64>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredPgStatDatabaseRow {
    pub datname: String,
    pub numbackends: i32,
    pub xact_commit: i64,
    pub xact_rollback: i64,
    pub blks_read: i64,
    pub blks_hit: i64,
    pub tup_inserted: i64,
    pub tup_updated: i64,
    pub tup_deleted: i64,
    pub temp_files: i64,
    pub temp_bytes: i64,
    pub deadlocks: i64,
    pub stats_reset_ms: Option<i64>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredPgStatReplicationRow {
    pub pid: i32,
    pub usename: String,
    pub application_name: String,
    pub client_addr: Option<String>,
    pub state: String,
    pub sync_state: String,
    pub sent_lsn: String,
    pub write_lsn: String,
    pub flush_lsn: String,
    pub replay_lsn: String,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredPgStatReplicationSlotsRow {
    pub slot_name: String,
    pub spill_txns: i64,
    pub spill_count: i64,
    pub spill_bytes: String,
    pub stream_txns: i64,
    pub stream_count: i64,
    pub stream_bytes: String,
    pub total_txns: i64,
    pub total_bytes: String,
    pub stats_reset_ms: Option<i64>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredPgStatUserTablesRow {
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

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredPgStatDatabaseConflictsRow {
    pub datname: String,
    pub confl_tablespace: i64,
    pub confl_lock: i64,
    pub confl_snapshot: i64,
    pub confl_bufferpin: i64,
    pub confl_deadlock: i64,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredPgStatRecoveryPrefetchRow {
    pub prefetch: i64,
    pub hit: i64,
    pub skip_init: i64,
    pub skip_new: i64,
    pub skip_fpw: i64,
    pub skip_rep: i64,
    pub wal_distance: i64,
    pub block_distance: i64,
    pub io_depth: i64,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredPgStatWalReceiverRow {
    pub pid: i32,
    pub status: String,
    pub receive_start_lsn: Option<String>,
    pub written_lsn: Option<String>,
    pub flushed_lsn: Option<String>,
    pub latest_end_lsn: Option<String>,
    pub last_msg_send_time_ms: Option<i64>,
    pub last_msg_receipt_time_ms: Option<i64>,
    pub latest_end_time_ms: Option<i64>,
    pub slot_name: Option<String>,
    pub sender_host: Option<String>,
    pub sender_port: Option<i32>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredTickSnapshot {
    pub changed_mask: u16,
    pub wal_dir: Option<StoredWalDirStats>,
    pub wal_functions: Option<StoredWalFunctionsRow>,
    pub pg_stat_wal: Option<StoredPgStatWalRow>,
    pub pg_replication_slots: Option<Vec<StoredPgReplicationSlotsRow>>,
    pub pg_stat_activity: Option<Vec<StoredPgStatActivityRow>>,
    pub pg_stat_archiver: Option<StoredPgStatArchiverRow>,
    pub pg_stat_bgwriter: Option<StoredPgStatBgwriterRow>,
    pub pg_stat_database: Option<Vec<StoredPgStatDatabaseRow>>,
    pub pg_stat_replication: Option<Vec<StoredPgStatReplicationRow>>,
    pub pg_stat_replication_slots: Option<Vec<StoredPgStatReplicationSlotsRow>>,
    pub pg_stat_user_tables: Option<Vec<StoredPgStatUserTablesRow>>,
    pub pg_stat_database_conflicts: Option<Option<Vec<StoredPgStatDatabaseConflictsRow>>>,
    pub pg_stat_recovery_prefetch: Option<Option<StoredPgStatRecoveryPrefetchRow>>,
    pub pg_stat_wal_receiver: Option<Option<Option<StoredPgStatWalReceiverRow>>>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct StoredSettingsSnapshot {
    pub changed_mask: u16,
    pub settings: Vec<StoredSettingRow>,
}

fn os_string_lossy(value: &OsString) -> String {
    value.to_string_lossy().into_owned()
}

impl From<&WalDirStats> for StoredWalDirStats {
    fn from(value: &WalDirStats) -> Self {
        Self {
            total_size_logical: value.total_size_logical,
            total_size_physical: value.total_size_physical,
            n_files: value.n_files,
            files: value.files.iter().map(os_string_lossy).collect(),
        }
    }
}

fn dt_to_ms(value: DateTime<Utc>) -> i64 {
    value.timestamp_millis()
}

fn opt_dt_to_ms(value: Option<DateTime<Utc>>) -> Option<i64> {
    value.map(dt_to_ms)
}

fn decimal_to_string(value: &Decimal) -> String {
    value.to_string()
}

impl From<&PgSettingRow> for StoredSettingRow {
    fn from(value: &PgSettingRow) -> Self {
        Self {
            name: value.name.clone(),
            setting: value.setting.clone(),
            unit: value.unit.clone(),
            source: value.source.clone(),
            short_desc: value.short_desc.clone(),
        }
    }
}

impl From<&WalFunctionsRow> for StoredWalFunctionsRow {
    fn from(value: &WalFunctionsRow) -> Self {
        Self {
            is_in_recovery: value.is_in_recovery,
            current_wal_lsn: value.current_wal_lsn.clone(),
            last_wal_receive_lsn: value.last_wal_receive_lsn.clone(),
            last_wal_replay_lsn: value.last_wal_replay_lsn.clone(),
            last_xact_replay_timestamp_ms: opt_dt_to_ms(value.last_xact_replay_timestamp),
        }
    }
}

impl From<&PgStatWalRow> for StoredPgStatWalRow {
    fn from(value: &PgStatWalRow) -> Self {
        Self {
            wal_records: value.wal_records,
            wal_fpi: value.wal_fpi,
            wal_bytes: decimal_to_string(&value.wal_bytes),
            wal_buffers_full: value.wal_buffers_full,
            wal_write: value.wal_write,
            wal_sync: value.wal_sync,
            wal_write_time: value.wal_write_time,
            wal_sync_time: value.wal_sync_time,
            stats_reset_ms: dt_to_ms(value.stats_reset),
        }
    }
}

impl From<&PgReplicationSlotsRow> for StoredPgReplicationSlotsRow {
    fn from(value: &PgReplicationSlotsRow) -> Self {
        Self {
            slot_name: value.slot_name.clone(),
            plugin: value.plugin.clone(),
            slot_type: value.slot_type.clone(),
            database: value.database.clone(),
            temporary: value.temporary,
            active: value.active,
            active_pid: value.active_pid,
            xmin: value.xmin.clone(),
            catalog_xmin: value.catalog_xmin.clone(),
            restart_lsn: value.restart_lsn.clone(),
            confirmed_flush_lsn: value.confirmed_flush_lsn.clone(),
        }
    }
}

impl From<&PgStatActivityRow> for StoredPgStatActivityRow {
    fn from(value: &PgStatActivityRow) -> Self {
        Self {
            pid: value.pid,
            datname: value.datname.clone(),
            usename: value.usename.clone(),
            application_name: value.application_name.clone(),
            state: value.state.clone(),
            wait_event_type: value.wait_event_type.clone(),
            wait_event: value.wait_event.clone(),
            xact_start_ms: opt_dt_to_ms(value.xact_start),
            query_start_ms: opt_dt_to_ms(value.query_start),
            query: value.query.clone(),
        }
    }
}

impl From<&PgStatArchiverRow> for StoredPgStatArchiverRow {
    fn from(value: &PgStatArchiverRow) -> Self {
        Self {
            archived_count: value.archived_count,
            last_archived_wal: value.last_archived_wal.clone(),
            last_archived_time_ms: opt_dt_to_ms(value.last_archived_time),
            failed_count: value.failed_count,
            last_failed_wal: value.last_failed_wal.clone(),
            last_failed_time_ms: opt_dt_to_ms(value.last_failed_time),
            stats_reset_ms: opt_dt_to_ms(value.stats_reset),
        }
    }
}

impl From<&PgStatBgwriterRow> for StoredPgStatBgwriterRow {
    fn from(value: &PgStatBgwriterRow) -> Self {
        Self {
            checkpoints_timed: value.checkpoints_timed,
            checkpoints_req: value.checkpoints_req,
            checkpoint_write_time: value.checkpoint_write_time,
            checkpoint_sync_time: value.checkpoint_sync_time,
            buffers_checkpoint: value.buffers_checkpoint,
            buffers_clean: value.buffers_clean,
            maxwritten_clean: value.maxwritten_clean,
            buffers_backend: value.buffers_backend,
            buffers_backend_fsync: value.buffers_backend_fsync,
            buffers_alloc: value.buffers_alloc,
            stats_reset_ms: opt_dt_to_ms(value.stats_reset),
        }
    }
}

impl From<&PgStatDatabaseRow> for StoredPgStatDatabaseRow {
    fn from(value: &PgStatDatabaseRow) -> Self {
        Self {
            datname: value.datname.clone(),
            numbackends: value.numbackends,
            xact_commit: value.xact_commit,
            xact_rollback: value.xact_rollback,
            blks_read: value.blks_read,
            blks_hit: value.blks_hit,
            tup_inserted: value.tup_inserted,
            tup_updated: value.tup_updated,
            tup_deleted: value.tup_deleted,
            temp_files: value.temp_files,
            temp_bytes: value.temp_bytes,
            deadlocks: value.deadlocks,
            stats_reset_ms: opt_dt_to_ms(value.stats_reset),
        }
    }
}

impl From<&PgStatReplicationRow> for StoredPgStatReplicationRow {
    fn from(value: &PgStatReplicationRow) -> Self {
        Self {
            pid: value.pid,
            usename: value.usename.clone(),
            application_name: value.application_name.clone(),
            client_addr: value.client_addr.clone(),
            state: value.state.clone(),
            sync_state: value.sync_state.clone(),
            sent_lsn: value.sent_lsn.clone(),
            write_lsn: value.write_lsn.clone(),
            flush_lsn: value.flush_lsn.clone(),
            replay_lsn: value.replay_lsn.clone(),
        }
    }
}

impl From<&PgStatReplicationSlotsRow> for StoredPgStatReplicationSlotsRow {
    fn from(value: &PgStatReplicationSlotsRow) -> Self {
        Self {
            slot_name: value.slot_name.clone(),
            spill_txns: value.spill_txns,
            spill_count: value.spill_count,
            spill_bytes: decimal_to_string(&value.spill_bytes),
            stream_txns: value.stream_txns,
            stream_count: value.stream_count,
            stream_bytes: decimal_to_string(&value.stream_bytes),
            total_txns: value.total_txns,
            total_bytes: decimal_to_string(&value.total_bytes),
            stats_reset_ms: opt_dt_to_ms(value.stats_reset),
        }
    }
}

impl From<&PgStatUserTablesRow> for StoredPgStatUserTablesRow {
    fn from(value: &PgStatUserTablesRow) -> Self {
        Self {
            schemaname: value.schemaname.clone(),
            relname: value.relname.clone(),
            seq_scan: value.seq_scan,
            idx_scan: value.idx_scan,
            n_tup_ins: value.n_tup_ins,
            n_tup_upd: value.n_tup_upd,
            n_tup_del: value.n_tup_del,
            n_tup_hot_upd: value.n_tup_hot_upd,
            n_live_tup: value.n_live_tup,
            n_dead_tup: value.n_dead_tup,
            vacuum_count: value.vacuum_count,
            autovacuum_count: value.autovacuum_count,
            analyze_count: value.analyze_count,
            autoanalyze_count: value.autoanalyze_count,
        }
    }
}

impl From<&PgStatDatabaseConflictsRow> for StoredPgStatDatabaseConflictsRow {
    fn from(value: &PgStatDatabaseConflictsRow) -> Self {
        Self {
            datname: value.datname.clone(),
            confl_tablespace: value.confl_tablespace,
            confl_lock: value.confl_lock,
            confl_snapshot: value.confl_snapshot,
            confl_bufferpin: value.confl_bufferpin,
            confl_deadlock: value.confl_deadlock,
        }
    }
}

impl From<&PgStatRecoveryPrefetchRow> for StoredPgStatRecoveryPrefetchRow {
    fn from(value: &PgStatRecoveryPrefetchRow) -> Self {
        Self {
            prefetch: value.prefetch,
            hit: value.hit,
            skip_init: value.skip_init,
            skip_new: value.skip_new,
            skip_fpw: value.skip_fpw,
            skip_rep: value.skip_rep,
            wal_distance: value.wal_distance,
            block_distance: value.block_distance,
            io_depth: value.io_depth,
        }
    }
}

impl From<&PgStatWalReceiverRow> for StoredPgStatWalReceiverRow {
    fn from(value: &PgStatWalReceiverRow) -> Self {
        Self {
            pid: value.pid,
            status: value.status.clone(),
            receive_start_lsn: value.receive_start_lsn.clone(),
            written_lsn: value.written_lsn.clone(),
            flushed_lsn: value.flushed_lsn.clone(),
            latest_end_lsn: value.latest_end_lsn.clone(),
            last_msg_send_time_ms: opt_dt_to_ms(value.last_msg_send_time),
            last_msg_receipt_time_ms: opt_dt_to_ms(value.last_msg_receipt_time),
            latest_end_time_ms: opt_dt_to_ms(value.latest_end_time),
            slot_name: value.slot_name.clone(),
            sender_host: value.sender_host.clone(),
            sender_port: value.sender_port,
        }
    }
}

pub async fn collect_tick(pool: &Pool<Postgres>, role: Option<DbRole>) -> Result<TickData, sqlx::Error> {
    let wal_dir = WalDirCollector::collect().map_err(sqlx::Error::Io)?;
    let wal_functions = WalFunctionsCollector::collect(pool).await?;
    let pg_stat_wal = PgStatWalCollector::collect(pool).await?;
    let settings = SettingsCollector::collect(pool).await?;
    let pg_replication_slots = PgReplicationSlotsCollector::collect(pool).await?;
    let pg_stat_activity = PgStatActivityCollector::collect(pool).await?;
    let pg_stat_archiver = PgStatArchiverCollector::collect(pool).await?;
    let pg_stat_bgwriter = PgStatBgwriterCollector::collect(pool).await?;
    let pg_stat_database = PgStatDatabaseCollector::collect(pool).await?;
    let pg_stat_replication = PgStatReplicationCollector::collect(pool).await?;
    let pg_stat_replication_slots = PgStatReplicationSlotsCollector::collect(pool).await?;
    let pg_stat_user_tables = PgStatUserTablesCollector::collect(pool).await?;

    let (pg_stat_database_conflicts, pg_stat_recovery_prefetch, pg_stat_wal_receiver) = match role {
        Some(DbRole::Standby) => (
            Some(PgStatDatabaseConflictsCollector::collect(pool).await?),
            Some(PgStatRecoveryPrefetchCollector::collect(pool).await?),
            Some(PgStatWalReceiverCollector::collect(pool).await?),
        ),
        _ => (None, None, None),
    };

    Ok(TickData {
        wal_dir,
        wal_functions,
        pg_stat_wal,
        settings,
        pg_replication_slots,
        pg_stat_activity,
        pg_stat_archiver,
        pg_stat_bgwriter,
        pg_stat_database,
        pg_stat_replication,
        pg_stat_replication_slots,
        pg_stat_user_tables,
        pg_stat_database_conflicts,
        pg_stat_recovery_prefetch,
        pg_stat_wal_receiver,
    })
}

pub fn diff_tick(previous: Option<&TickData>, current: &TickData) -> u16 {
    let Some(previous) = previous else {
        return ALL_TICK_BITS;
    };

    let mut changed_mask = 0;

    if previous.wal_dir != current.wal_dir {
        changed_mask |= TICK_BIT_WAL_DIR;
    }
    if previous.wal_functions != current.wal_functions {
        changed_mask |= TICK_BIT_WAL_FUNCTIONS;
    }
    if previous.pg_stat_wal != current.pg_stat_wal {
        changed_mask |= TICK_BIT_PG_STAT_WAL;
    }
    if previous.pg_replication_slots != current.pg_replication_slots {
        changed_mask |= TICK_BIT_PG_REPLICATION_SLOTS;
    }
    if previous.pg_stat_activity != current.pg_stat_activity {
        changed_mask |= TICK_BIT_PG_STAT_ACTIVITY;
    }
    if previous.pg_stat_archiver != current.pg_stat_archiver {
        changed_mask |= TICK_BIT_PG_STAT_ARCHIVER;
    }
    if previous.pg_stat_bgwriter != current.pg_stat_bgwriter {
        changed_mask |= TICK_BIT_PG_STAT_BGWRITER;
    }
    if previous.pg_stat_database != current.pg_stat_database {
        changed_mask |= TICK_BIT_PG_STAT_DATABASE;
    }
    if previous.pg_stat_replication != current.pg_stat_replication {
        changed_mask |= TICK_BIT_PG_STAT_REPLICATION;
    }
    if previous.pg_stat_replication_slots != current.pg_stat_replication_slots {
        changed_mask |= TICK_BIT_PG_STAT_REPLICATION_SLOTS;
    }
    if previous.pg_stat_user_tables != current.pg_stat_user_tables {
        changed_mask |= TICK_BIT_PG_STAT_USER_TABLES;
    }
    if previous.pg_stat_database_conflicts != current.pg_stat_database_conflicts {
        changed_mask |= TICK_BIT_PG_STAT_DATABASE_CONFLICTS;
    }
    if previous.pg_stat_recovery_prefetch != current.pg_stat_recovery_prefetch {
        changed_mask |= TICK_BIT_PG_STAT_RECOVERY_PREFETCH;
    }
    if previous.pg_stat_wal_receiver != current.pg_stat_wal_receiver {
        changed_mask |= TICK_BIT_PG_STAT_WAL_RECEIVER;
    }

    changed_mask
}

pub fn build_stored_tick_snapshot(current: &TickData, changed_mask: u16) -> StoredTickSnapshot {
    macro_rules! changed_value {
        ($bit:expr, $value:expr) => {
            if changed_mask & $bit != 0 { Some($value) } else { None }
        };
    }

    StoredTickSnapshot {
        changed_mask,
        wal_dir: changed_value!(TICK_BIT_WAL_DIR, StoredWalDirStats::from(&current.wal_dir)),
        wal_functions: changed_value!(TICK_BIT_WAL_FUNCTIONS, StoredWalFunctionsRow::from(&current.wal_functions)),
        pg_stat_wal: changed_value!(TICK_BIT_PG_STAT_WAL, StoredPgStatWalRow::from(&current.pg_stat_wal)),
        pg_replication_slots: changed_value!(
            TICK_BIT_PG_REPLICATION_SLOTS,
            current.pg_replication_slots.iter().map(StoredPgReplicationSlotsRow::from).collect()
        ),
        pg_stat_activity: changed_value!(
            TICK_BIT_PG_STAT_ACTIVITY,
            current.pg_stat_activity.iter().map(StoredPgStatActivityRow::from).collect()
        ),
        pg_stat_archiver: changed_value!(TICK_BIT_PG_STAT_ARCHIVER, StoredPgStatArchiverRow::from(&current.pg_stat_archiver)),
        pg_stat_bgwriter: changed_value!(TICK_BIT_PG_STAT_BGWRITER, StoredPgStatBgwriterRow::from(&current.pg_stat_bgwriter)),
        pg_stat_database: changed_value!(
            TICK_BIT_PG_STAT_DATABASE,
            current.pg_stat_database.iter().map(StoredPgStatDatabaseRow::from).collect()
        ),
        pg_stat_replication: changed_value!(
            TICK_BIT_PG_STAT_REPLICATION,
            current.pg_stat_replication.iter().map(StoredPgStatReplicationRow::from).collect()
        ),
        pg_stat_replication_slots: changed_value!(
            TICK_BIT_PG_STAT_REPLICATION_SLOTS,
            current.pg_stat_replication_slots.iter().map(StoredPgStatReplicationSlotsRow::from).collect()
        ),
        pg_stat_user_tables: changed_value!(
            TICK_BIT_PG_STAT_USER_TABLES,
            current.pg_stat_user_tables.iter().map(StoredPgStatUserTablesRow::from).collect()
        ),
        pg_stat_database_conflicts: changed_value!(
            TICK_BIT_PG_STAT_DATABASE_CONFLICTS,
            current.pg_stat_database_conflicts.as_ref().map(|rows| { rows.iter().map(StoredPgStatDatabaseConflictsRow::from).collect() })
        ),
        pg_stat_recovery_prefetch: changed_value!(
            TICK_BIT_PG_STAT_RECOVERY_PREFETCH,
            current.pg_stat_recovery_prefetch.as_ref().map(StoredPgStatRecoveryPrefetchRow::from)
        ),
        pg_stat_wal_receiver: changed_value!(
            TICK_BIT_PG_STAT_WAL_RECEIVER,
            current.pg_stat_wal_receiver.as_ref().map(|row| row.as_ref().map(StoredPgStatWalReceiverRow::from))
        ),
    }
}

pub fn diff_settings(previous: Option<&[PgSettingRow]>, current: &[PgSettingRow]) -> u16 {
    let Some(previous) = previous else {
        return ALL_SETTINGS_BITS;
    };

    let mut changed_mask = 0;

    for current_row in current {
        let Some(bit) = setting_bit(&current_row.name) else {
            continue;
        };

        let previous_row = previous.iter().find(|row| row.name == current_row.name);

        if previous_row != Some(current_row) {
            changed_mask |= bit;
        }
    }

    changed_mask
}

pub fn build_stored_settings_snapshot(current: &[PgSettingRow], changed_mask: u16) -> StoredSettingsSnapshot {
    let mut settings = Vec::new();

    for name in tracked_setting_names() {
        let bit = setting_bit(name).unwrap();

        if changed_mask & bit == 0 {
            continue;
        }

        if let Some(row) = current.iter().find(|row| row.name == name) {
            settings.push(StoredSettingRow::from(row));
        }
    }

    StoredSettingsSnapshot { changed_mask, settings }
}

fn tracked_setting_names() -> [&'static str; 6] {
    ["full_page_writes", "checkpoint_timeout", "max_wal_size", "min_wal_size", "wal_compression", "synchronous_commit"]
}

fn setting_bit(name: &str) -> Option<u16> {
    match name {
        "full_page_writes" => Some(SETTINGS_BIT_FULL_PAGE_WRITES),
        "checkpoint_timeout" => Some(SETTINGS_BIT_CHECKPOINT_TIMEOUT),
        "max_wal_size" => Some(SETTINGS_BIT_MAX_WAL_SIZE),
        "min_wal_size" => Some(SETTINGS_BIT_MIN_WAL_SIZE),
        "wal_compression" => Some(SETTINGS_BIT_WAL_COMPRESSION),
        "synchronous_commit" => Some(SETTINGS_BIT_SYNCHRONOUS_COMMIT),
        _ => None,
    }
}

pub fn apply_tick_diff(last_tick: &mut TickData, current: &TickData, changed_mask: u16) {
    macro_rules! apply_field_diff {
        ($bit:expr, $field:ident) => {
            if changed_mask & $bit != 0 {
                last_tick.$field = current.$field.clone();
            }
        };
    }

    apply_field_diff!(TICK_BIT_WAL_DIR, wal_dir);
    apply_field_diff!(TICK_BIT_WAL_FUNCTIONS, wal_functions);
    apply_field_diff!(TICK_BIT_PG_STAT_WAL, pg_stat_wal);
    apply_field_diff!(TICK_BIT_PG_REPLICATION_SLOTS, pg_replication_slots);
    apply_field_diff!(TICK_BIT_PG_STAT_ACTIVITY, pg_stat_activity);
    apply_field_diff!(TICK_BIT_PG_STAT_ARCHIVER, pg_stat_archiver);
    apply_field_diff!(TICK_BIT_PG_STAT_BGWRITER, pg_stat_bgwriter);
    apply_field_diff!(TICK_BIT_PG_STAT_DATABASE, pg_stat_database);
    apply_field_diff!(TICK_BIT_PG_STAT_REPLICATION, pg_stat_replication);
    apply_field_diff!(TICK_BIT_PG_STAT_REPLICATION_SLOTS, pg_stat_replication_slots);
    apply_field_diff!(TICK_BIT_PG_STAT_USER_TABLES, pg_stat_user_tables);
    apply_field_diff!(TICK_BIT_PG_STAT_DATABASE_CONFLICTS, pg_stat_database_conflicts);
    apply_field_diff!(TICK_BIT_PG_STAT_RECOVERY_PREFETCH, pg_stat_recovery_prefetch);
    apply_field_diff!(TICK_BIT_PG_STAT_WAL_RECEIVER, pg_stat_wal_receiver);

    // Settings are carried separately from the tick changed-mask for now.
    last_tick.settings = current.settings.clone();
}

pub fn debug_print_tick(tick: &TickData) {
    println!("========== Tick Snapshot ==========");
    println!("wal_dir: {:#?}", tick.wal_dir);
    println!("wal_functions: {:#?}", tick.wal_functions);
    println!("pg_stat_wal: {:#?}", tick.pg_stat_wal);
    println!("pg_stat_archiver: {:#?}", tick.pg_stat_archiver);
    println!("pg_stat_bgwriter: {:#?}", tick.pg_stat_bgwriter);
    println!();
    println!("---------- Collection Sizes ----------");
    println!("settings: {} rows", tick.settings.len());
    println!("pg_replication_slots: {} rows", tick.pg_replication_slots.len());
    println!("pg_stat_activity: {} rows", tick.pg_stat_activity.len());
    println!("pg_stat_database: {} rows", tick.pg_stat_database.len());
    println!("pg_stat_replication: {} rows", tick.pg_stat_replication.len());
    println!("pg_stat_replication_slots: {} rows", tick.pg_stat_replication_slots.len());
    println!("pg_stat_user_tables: {} rows", tick.pg_stat_user_tables.len());
    println!();
    println!("---------- Standby-Only State ----------");
    match &tick.pg_stat_database_conflicts {
        Some(rows) => println!("pg_stat_database_conflicts: {} rows", rows.len()),
        None => println!("pg_stat_database_conflicts: not collected"),
    }
    match &tick.pg_stat_recovery_prefetch {
        Some(row) => println!("pg_stat_recovery_prefetch: {row:#?}"),
        None => println!("pg_stat_recovery_prefetch: not collected"),
    }
    match &tick.pg_stat_wal_receiver {
        Some(Some(row)) => println!("pg_stat_wal_receiver: {row:#?}"),
        Some(None) => println!("pg_stat_wal_receiver: collected, but no active row"),
        None => println!("pg_stat_wal_receiver: not collected"),
    }
    println!("====================================");
}
