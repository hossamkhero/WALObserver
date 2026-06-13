#![allow(dead_code)]

pub mod pg_stat_activity;
pub mod pg_stat_archiver;
pub mod pg_stat_bgwriter;
pub mod pg_stat_database;
pub mod pg_stat_database_conflicts;
pub mod pg_stat_recovery_prefetch;
pub mod pg_stat_replication;
pub mod pg_stat_replication_slots;
pub mod pg_stat_user_tables;
pub mod pg_stat_wal;
pub mod pg_stat_wal_receiver;

pub use pg_stat_activity::{PgStatActivityCollector, PgStatActivityRow};
pub use pg_stat_archiver::{PgStatArchiverCollector, PgStatArchiverRow};
pub use pg_stat_bgwriter::{PgStatBgwriterCollector, PgStatBgwriterRow};
pub use pg_stat_database::{PgStatDatabaseCollector, PgStatDatabaseRow};
pub use pg_stat_database_conflicts::{
    PgStatDatabaseConflictsCollector, PgStatDatabaseConflictsRow,
};
pub use pg_stat_recovery_prefetch::{
    PgStatRecoveryPrefetchCollector, PgStatRecoveryPrefetchRow,
};
pub use pg_stat_replication::{PgStatReplicationCollector, PgStatReplicationRow};
pub use pg_stat_replication_slots::{
    PgStatReplicationSlotsCollector, PgStatReplicationSlotsRow,
};
pub use pg_stat_user_tables::{PgStatUserTablesCollector, PgStatUserTablesRow};
pub use pg_stat_wal::{PgStatWalCollector, PgStatWalRow};
pub use pg_stat_wal_receiver::{PgStatWalReceiverCollector, PgStatWalReceiverRow};
