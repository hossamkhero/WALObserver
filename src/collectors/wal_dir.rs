use std::env;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalDirStats {
    pub total_size_logical: u64,
    pub total_size_physical: u64,
    pub n_files: usize,
    pub files: Vec<OsString>,
}

pub struct WalDirCollector;

impl WalDirCollector {
    pub fn collect() -> io::Result<WalDirStats> {
        let pgdata = env::var("PGDATA").unwrap_or_else(|_| ".local/postgres".to_string());
        let path = PathBuf::from(pgdata).join("pg_wal");

        walk(&path)
    }
}

fn walk(path: &Path) -> io::Result<WalDirStats> {
    let mut total_size_logical = 0;
    let mut total_size_physical = 0;
    let mut files: Vec<OsString> = Vec::new();
    let mut n_files = 0;

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;

        if metadata.is_file() {
            let size_logical = metadata.len();
            let size_physical = metadata.blocks() * 512;

            total_size_logical += size_logical;
            total_size_physical += size_physical;

            n_files += 1;
            files.push(entry.file_name());
        } else if metadata.is_dir() {
            let child_stats = walk(&entry.path())?;

            total_size_logical += child_stats.total_size_logical;
            total_size_physical += child_stats.total_size_physical;

            n_files += child_stats.n_files;
            files.extend(child_stats.files);
        }
    }

    files.sort();

    Ok(WalDirStats { total_size_logical, total_size_physical, n_files, files })
}

pub fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];

    if bytes == 0 {
        return "0 B".to_string();
    }

    let unit = ((63 - bytes.leading_zeros()) / 10) as usize;
    let unit = unit.min(UNITS.len() - 1);

    if unit == 0 {
        return format!("{bytes} B");
    }

    let size = bytes as f64 / (1u64 << (unit * 10)) as f64;

    format!("{size:.2} {}", UNITS[unit])
}
