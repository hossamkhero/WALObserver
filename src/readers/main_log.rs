use crate::events::EventSnapshot;
use crate::storage::{
    FILE_HEADER_LEN, RECORD_HEADER_LEN, RECORD_KIND_EVENT_SNAPSHOT, RECORD_KIND_SETTINGS_SNAPSHOT,
    RECORD_KIND_TICK_SNAPSHOT, main_log_path,
};
use crate::tick::{
    StoredPgStatRecoveryPrefetchRow, StoredPgStatWalReceiverRow, StoredSettingsSnapshot,
    StoredTickSnapshot, StoredWalDirStats, StoredWalFunctionsRow,
};
use rkyv::{Archive, Deserialize, Infallible, archived_root};
use std::{fs, io, path::Path};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    pub kind: u16,
    pub flags: u16,
    pub header_len: u16,
    pub timestamp_ms: u64,
    pub payload_len: u32,
}

#[derive(Debug, Clone)]
pub enum MainLogRecord {
    Tick {
        header: RecordHeader,
        payload: StoredTickSnapshot,
    },
    Settings {
        header: RecordHeader,
        payload: StoredSettingsSnapshot,
    },
    Event {
        header: RecordHeader,
        payload: EventSnapshot,
    },
}

#[derive(Debug, Clone, Default)]
pub struct MaterializedTickState {
    pub wal_dir: Option<StoredWalDirStats>,
    pub wal_functions: Option<StoredWalFunctionsRow>,
    pub pg_stat_wal: Option<crate::tick::StoredPgStatWalRow>,
    pub pg_replication_slots: Option<Vec<crate::tick::StoredPgReplicationSlotsRow>>,
    pub pg_stat_activity: Option<Vec<crate::tick::StoredPgStatActivityRow>>,
    pub pg_stat_archiver: Option<crate::tick::StoredPgStatArchiverRow>,
    pub pg_stat_bgwriter: Option<crate::tick::StoredPgStatBgwriterRow>,
    pub pg_stat_database: Option<Vec<crate::tick::StoredPgStatDatabaseRow>>,
    pub pg_stat_replication: Option<Vec<crate::tick::StoredPgStatReplicationRow>>,
    pub pg_stat_replication_slots: Option<Vec<crate::tick::StoredPgStatReplicationSlotsRow>>,
    pub pg_stat_user_tables: Option<Vec<crate::tick::StoredPgStatUserTablesRow>>,
    pub pg_stat_database_conflicts: Option<Vec<crate::tick::StoredPgStatDatabaseConflictsRow>>,
    pub pg_stat_recovery_prefetch: Option<StoredPgStatRecoveryPrefetchRow>,
    pub pg_stat_wal_receiver: Option<Option<StoredPgStatWalReceiverRow>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadProgress {
    pub next_offset: u64,
}

#[derive(Debug, Clone)]
pub struct ReadResult {
    pub records: Vec<MainLogRecord>,
    pub progress: ReadProgress,
}

pub fn read_all(path: &Path) -> io::Result<ReadResult> {
    read_from(path, FILE_HEADER_LEN as u64)
}

pub fn read_all_default() -> io::Result<ReadResult> {
    let path = main_log_path();
    read_all(&path)
}

pub fn read_from(path: &Path, start_offset: u64) -> io::Result<ReadResult> {
    let bytes = fs::read(path)?;
    let mut offset = start_offset as usize;
    let mut records = Vec::new();

    while offset + RECORD_HEADER_LEN as usize <= bytes.len() {
        let header = decode_record_header(&bytes[offset..offset + RECORD_HEADER_LEN as usize]);
        let payload_start = offset + header.header_len as usize;
        let payload_end = payload_start + header.payload_len as usize;

        if payload_end > bytes.len() {
            break;
        }

        let payload_bytes = &bytes[payload_start..payload_end];

        let record = match header.kind {
            RECORD_KIND_TICK_SNAPSHOT => MainLogRecord::Tick {
                header,
                payload: decode_payload::<StoredTickSnapshot>(payload_bytes)?,
            },
            RECORD_KIND_SETTINGS_SNAPSHOT => MainLogRecord::Settings {
                header,
                payload: decode_payload::<StoredSettingsSnapshot>(payload_bytes)?,
            },
            RECORD_KIND_EVENT_SNAPSHOT => MainLogRecord::Event {
                header,
                payload: decode_payload::<EventSnapshot>(payload_bytes)?,
            },
            _ => {
                offset = payload_end;
                continue;
            }
        };

        records.push(record);
        offset = payload_end;
    }

    Ok(ReadResult {
        records,
        progress: ReadProgress {
            next_offset: offset as u64,
        },
    })
}

pub fn read_from_default(start_offset: u64) -> io::Result<ReadResult> {
    let path = main_log_path();
    read_from(&path, start_offset)
}

pub fn apply_tick_snapshot(state: &mut MaterializedTickState, snapshot: &StoredTickSnapshot) {
    if let Some(value) = &snapshot.wal_dir {
        state.wal_dir = Some(value.clone());
    }
    if let Some(value) = &snapshot.wal_functions {
        state.wal_functions = Some(value.clone());
    }
    if let Some(value) = &snapshot.pg_stat_wal {
        state.pg_stat_wal = Some(value.clone());
    }
    if let Some(value) = &snapshot.pg_replication_slots {
        state.pg_replication_slots = Some(value.clone());
    }
    if let Some(value) = &snapshot.pg_stat_activity {
        state.pg_stat_activity = Some(value.clone());
    }
    if let Some(value) = &snapshot.pg_stat_archiver {
        state.pg_stat_archiver = Some(value.clone());
    }
    if let Some(value) = &snapshot.pg_stat_bgwriter {
        state.pg_stat_bgwriter = Some(value.clone());
    }
    if let Some(value) = &snapshot.pg_stat_database {
        state.pg_stat_database = Some(value.clone());
    }
    if let Some(value) = &snapshot.pg_stat_replication {
        state.pg_stat_replication = Some(value.clone());
    }
    if let Some(value) = &snapshot.pg_stat_replication_slots {
        state.pg_stat_replication_slots = Some(value.clone());
    }
    if let Some(value) = &snapshot.pg_stat_user_tables {
        state.pg_stat_user_tables = Some(value.clone());
    }
    if let Some(value) = &snapshot.pg_stat_database_conflicts {
        state.pg_stat_database_conflicts = value.clone();
    }
    if let Some(value) = &snapshot.pg_stat_recovery_prefetch {
        state.pg_stat_recovery_prefetch = value.clone();
    }
    if let Some(value) = &snapshot.pg_stat_wal_receiver {
        state.pg_stat_wal_receiver = value.clone();
    }
}

fn decode_record_header(bytes: &[u8]) -> RecordHeader {
    RecordHeader {
        kind: u16::from_le_bytes(bytes[0..2].try_into().unwrap()),
        flags: u16::from_le_bytes(bytes[2..4].try_into().unwrap()),
        header_len: u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
        timestamp_ms: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        payload_len: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
    }
}

fn decode_payload<T>(bytes: &[u8]) -> io::Result<T>
where
    T: Archive,
    T::Archived: Deserialize<T, Infallible>,
{
    let archived = unsafe { archived_root::<T>(bytes) };
    archived
        .deserialize(&mut Infallible)
        .map_err(|never| match never {})
}
