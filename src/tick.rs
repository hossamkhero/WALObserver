use crate::collectors::*;
use crate::collectors::pg_stat::*;
use crate::events::DbRole;
use sqlx::{Pool, Postgres};

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

pub async fn collect_tick(
    pool: &Pool<Postgres>,
    role: Option<DbRole>,
) -> Result<TickData, sqlx::Error> {
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
    println!(
        "pg_replication_slots: {} rows",
        tick.pg_replication_slots.len()
    );
    println!("pg_stat_activity: {} rows", tick.pg_stat_activity.len());
    println!("pg_stat_database: {} rows", tick.pg_stat_database.len());
    println!(
        "pg_stat_replication: {} rows",
        tick.pg_stat_replication.len()
    );
    println!(
        "pg_stat_replication_slots: {} rows",
        tick.pg_stat_replication_slots.len()
    );
    println!(
        "pg_stat_user_tables: {} rows",
        tick.pg_stat_user_tables.len()
    );
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
