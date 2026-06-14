use crate::events::EventSnapshot;
use crate::tick::{StoredSettingsSnapshot, StoredTickSnapshot};
use rkyv::{Archive, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

// Wal Inspector storage lives under its own hidden directory.
const STORAGE_DIR: &str = ".walinspector";
const LOG_FILENAME: &str = "main.log";
const CHECKPOINT_FILENAME: &str = "checkpoints.log";

// These magic bytes let us distinguish the two storage files immediately.
const LOG_MAGIC: [u8; 4] = *b"PWIL";
const CHECKPOINT_MAGIC: [u8; 4] = *b"PWIC";
const STORAGE_FORMAT_VERSION: u16 = 1;
const RECORD_HEADER_LEN: u16 = 24;

pub const RECORD_KIND_TICK_SNAPSHOT: u16 = 1;
pub const RECORD_KIND_SETTINGS_SNAPSHOT: u16 = 2;
pub const RECORD_KIND_EVENT_SNAPSHOT: u16 = 3;

// Both files start with a fixed 32-byte header for easy future extension.
const FILE_HEADER_LEN: u16 = 32;

// Main log header:
// - magic
// - format version
// - flags
// - header_len
// - reserved
// - created_at_ms placeholder
// - reserved
fn main_log_header_bytes() -> [u8; FILE_HEADER_LEN as usize] {
    let mut header = [0_u8; FILE_HEADER_LEN as usize];

    header[0..4].copy_from_slice(&LOG_MAGIC);
    header[4..6].copy_from_slice(&STORAGE_FORMAT_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&0_u16.to_le_bytes());
    header[8..10].copy_from_slice(&FILE_HEADER_LEN.to_le_bytes());
    header[10..12].copy_from_slice(&0_u16.to_le_bytes());
    header[12..20].copy_from_slice(&0_u64.to_le_bytes());
    header[20..28].copy_from_slice(&0_u64.to_le_bytes());

    header
}

// Checkpoint file header:
// - magic
// - format version
// - flags
// - header_len
// - reserved
// - created_at_ms placeholder
// - latest_checkpoint_offset
fn checkpoint_header_bytes() -> [u8; FILE_HEADER_LEN as usize] {
    let mut header = [0_u8; FILE_HEADER_LEN as usize];

    header[0..4].copy_from_slice(&CHECKPOINT_MAGIC);
    header[4..6].copy_from_slice(&STORAGE_FORMAT_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&0_u16.to_le_bytes());
    header[8..10].copy_from_slice(&FILE_HEADER_LEN.to_le_bytes());
    header[10..12].copy_from_slice(&0_u16.to_le_bytes());
    header[12..20].copy_from_slice(&0_u64.to_le_bytes());
    header[20..28].copy_from_slice(&0_u64.to_le_bytes());

    header
}

pub fn init_storage() -> io::Result<(PathBuf, PathBuf)> {
    let storage_dir = PathBuf::from(STORAGE_DIR);
    fs::create_dir_all(&storage_dir)?;

    let log_path = storage_dir.join(LOG_FILENAME);
    let checkpoint_path = storage_dir.join(CHECKPOINT_FILENAME);

    if !log_path.exists() {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&log_path)?;
        file.write_all(&main_log_header_bytes())?;
        file.flush()?;
    }

    if !checkpoint_path.exists() {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&checkpoint_path)?;
        file.write_all(&checkpoint_header_bytes())?;
        file.flush()?;
    }

    Ok((log_path, checkpoint_path))
}

pub fn append_tick_snapshot(log_path: &Path, payload: &StoredTickSnapshot) -> io::Result<()> {
    append_rkyv_record(log_path, RECORD_KIND_TICK_SNAPSHOT, payload)
}

pub fn append_settings_snapshot(
    log_path: &Path,
    payload: &StoredSettingsSnapshot,
) -> io::Result<()> {
    append_rkyv_record(log_path, RECORD_KIND_SETTINGS_SNAPSHOT, payload)
}

pub fn append_event_snapshot(log_path: &Path, payload: &EventSnapshot) -> io::Result<()> {
    append_rkyv_record(log_path, RECORD_KIND_EVENT_SNAPSHOT, payload)
}

fn append_rkyv_record<T>(log_path: &Path, kind: u16, payload: &T) -> io::Result<()>
where
    T: Archive + Serialize<rkyv::ser::serializers::AllocSerializer<256>>,
{
    let payload_bytes = rkyv::to_bytes::<_, 256>(payload)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

    append_record(log_path, kind, 0, now_unix_ms(), payload_bytes.as_slice())
}

fn append_record(
    log_path: &Path,
    kind: u16,
    flags: u16,
    timestamp_ms: u64,
    payload_bytes: &[u8],
) -> io::Result<()> {
    let payload_len: u32 = payload_bytes
        .len()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "payload too large"))?;

    let mut header = [0_u8; RECORD_HEADER_LEN as usize];
    header[0..2].copy_from_slice(&kind.to_le_bytes());
    header[2..4].copy_from_slice(&flags.to_le_bytes());
    header[4..6].copy_from_slice(&RECORD_HEADER_LEN.to_le_bytes());
    header[6..8].copy_from_slice(&0_u16.to_le_bytes());
    header[8..16].copy_from_slice(&timestamp_ms.to_le_bytes());
    header[16..20].copy_from_slice(&payload_len.to_le_bytes());
    header[20..24].copy_from_slice(&0_u32.to_le_bytes());

    let mut file = OpenOptions::new().append(true).open(log_path)?;
    file.write_all(&header)?;
    file.write_all(payload_bytes)?;
    file.flush()?;

    Ok(())
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
