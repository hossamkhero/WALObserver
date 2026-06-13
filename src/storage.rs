use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::PathBuf,
};

// Wal Inspector storage lives under its own hidden directory.
const STORAGE_DIR: &str = ".walinspector";
const LOG_FILENAME: &str = "main.log";
const CHECKPOINT_FILENAME: &str = "checkpoints.log";

// These magic bytes let us distinguish the two storage files immediately.
const LOG_MAGIC: [u8; 4] = *b"PWIL";
const CHECKPOINT_MAGIC: [u8; 4] = *b"PWIC";
const STORAGE_FORMAT_VERSION: u16 = 1;

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
