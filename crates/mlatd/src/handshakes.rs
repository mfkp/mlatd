//! handshakes.log in the work dir: the handshake of every receiver let
//! in, one JSON line each, as the client sent it. mlat-server keeps the
//! same file, and networks read their feeders' positions and uuids from
//! it (one syncs its feeder map from it, `uuid`, `lat`, `lon`, `alt` per
//! line, the rotated files included). Rotation is mlat-server's: Python's
//! RotatingFileHandler at 1 MiB with two backups, `handshakes.log.1` and
//! `handshakes.log.2`, so the three files stay under 3 MiB.
//!
//! The file holds receiver coordinates; nothing else here writes them.
//! Lines go to a thread of their own, so a slow or failing disk never
//! holds up a connection: a failure is reported once and the next line
//! tries again, and a full queue (a disk stalled for a thousand
//! handshakes) drops lines rather than wait.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::Arc;

/// mlat-server: RotatingFileHandler(maxBytes=1*1024*1024, backupCount=2).
const MAX_BYTES: u64 = 1024 * 1024;
const BACKUPS: u32 = 2;
const QUEUE: usize = 1024;

#[derive(Clone)]
pub struct HandshakeLog {
    tx: SyncSender<Vec<u8>>,
    dropping: Arc<AtomicBool>,
}

impl HandshakeLog {
    /// Start the writer thread for `dir/handshakes.log`. The file is
    /// opened at the first line, appended to if it is there.
    pub fn start(dir: &Path) -> std::io::Result<HandshakeLog> {
        let (tx, rx) = sync_channel::<Vec<u8>>(QUEUE);
        let mut file = RotatingFile::new(dir.join("handshakes.log"), MAX_BYTES, BACKUPS);
        std::thread::Builder::new()
            .name("handshakes".into())
            .spawn(move || {
                let mut failing = false;
                for line in rx {
                    match file.append(&line) {
                        Err(e) if !failing => {
                            failing = true;
                            eprintln!("mlatd: handshakes.log: cannot write ({e})");
                        }
                        Ok(()) if failing => {
                            failing = false;
                            eprintln!("mlatd: handshakes.log: writing again");
                        }
                        _ => {}
                    }
                }
            })?;
        Ok(HandshakeLog {
            tx,
            dropping: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Queue one handshake line (its line ending is dropped); never waits.
    pub fn record(&self, line: &[u8]) {
        match self.tx.try_send(line.trim_ascii_end().to_vec()) {
            Ok(()) => self.dropping.store(false, Ordering::Relaxed),
            Err(TrySendError::Full(_)) => {
                if !self.dropping.swap(true, Ordering::Relaxed) {
                    eprintln!("mlatd: handshakes.log: queue full, lines dropped");
                }
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

/// An append-only file rolled over as RotatingFileHandler rolls it: a
/// non-empty file that the next line would take to the limit or past is
/// renamed to `.1` (`.1` to `.2`, and so on; the last backup is
/// replaced), and the line starts a new file.
struct RotatingFile {
    path: PathBuf,
    max_bytes: u64,
    backups: u32,
    file: Option<File>,
    size: u64,
}

impl RotatingFile {
    fn new(path: PathBuf, max_bytes: u64, backups: u32) -> RotatingFile {
        RotatingFile {
            path,
            max_bytes,
            backups,
            file: None,
            size: 0,
        }
    }

    fn append(&mut self, line: &[u8]) -> std::io::Result<()> {
        if self.file.is_none() {
            self.open()?;
        }
        let len = line.len() as u64 + 1;
        if self.size > 0 && self.size + len >= self.max_bytes {
            self.file = None;
            self.rotate()?;
            self.open()?;
        }
        let mut record = Vec::with_capacity(line.len() + 1);
        record.extend_from_slice(line);
        record.push(b'\n');
        let Some(file) = self.file.as_mut() else {
            return Ok(());
        };
        match file.write_all(&record) {
            Ok(()) => {
                self.size += len;
                Ok(())
            }
            Err(e) => {
                // Reopen next time: the size is read back from the file.
                self.file = None;
                Err(e)
            }
        }
    }

    fn open(&mut self) -> std::io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.size = file.metadata()?.len();
        self.file = Some(file);
        Ok(())
    }

    fn rotate(&self) -> std::io::Result<()> {
        for i in (1..self.backups).rev() {
            let from = numbered(&self.path, i);
            if from.exists() {
                std::fs::rename(&from, numbered(&self.path, i + 1))?;
            }
        }
        if self.backups == 0 {
            return std::fs::remove_file(&self.path);
        }
        std::fs::rename(&self.path, numbered(&self.path, 1))
    }
}

fn numbered(path: &Path, i: u32) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".{i}"));
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("mlatd-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn lines(path: &Path) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .map(String::from)
            .collect()
    }

    /// RotatingFileHandler's rule: a line that would take a non-empty file
    /// to the limit starts the next one; two backups, the oldest dropped.
    #[test]
    fn rolls_over_like_mlat_server() {
        let dir = scratch("rotate");
        let path = dir.join("handshakes.log");
        // 9-byte lines (10 with the newline) under a 35-byte limit: three
        // to a file, since a fourth would reach 40 ≥ 35.
        let mut f = RotatingFile::new(path.clone(), 35, 2);
        for i in 0..10 {
            f.append(format!("{{\"n\":{i:03}}}").as_bytes()).unwrap();
        }
        let n = |l: &[String]| -> Vec<u32> { l.iter().map(|s| s[5..8].parse().unwrap()).collect() };
        assert_eq!(n(&lines(&numbered(&path, 2))), vec![3, 4, 5]);
        assert_eq!(n(&lines(&numbered(&path, 1))), vec![6, 7, 8]);
        assert_eq!(n(&lines(&path)), vec![9]);
        assert!(!numbered(&path, 3).exists(), "two backups, no more");
        for p in [path.clone(), numbered(&path, 1), numbered(&path, 2)] {
            assert!(std::fs::metadata(&p).unwrap().len() < 35);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A restart appends to the file it finds and counts what is there.
    #[test]
    fn reopened_file_is_appended_to() {
        let dir = scratch("reopen");
        let path = dir.join("handshakes.log");
        let mut f = RotatingFile::new(path.clone(), 35, 2);
        f.append(b"{\"a\":1}").unwrap();
        f.append(b"{\"a\":2}").unwrap();
        drop(f);
        // 8-byte records: the reopened file holds 16, so the fifth line
        // (32 + 8 ≥ 35) rolls it; counting from zero would not roll at all.
        let mut f = RotatingFile::new(path.clone(), 35, 2);
        for a in 3..=5 {
            f.append(format!("{{\"a\":{a}}}").as_bytes()).unwrap();
        }
        assert_eq!(lines(&path), vec!["{\"a\":5}"]);
        assert_eq!(
            lines(&numbered(&path, 1)),
            vec!["{\"a\":1}", "{\"a\":2}", "{\"a\":3}", "{\"a\":4}"]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The writer thread takes lines from record() and ends them with one
    /// newline, whatever line ending the client sent.
    #[test]
    fn recorded_lines_reach_the_file() {
        let dir = scratch("record");
        let log = HandshakeLog::start(&dir).unwrap();
        log.record(b"{\"user\":\"a\"}\r\n");
        log.record(b"{\"user\":\"b\"}\n");
        let path = dir.join("handshakes.log");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while lines(&path).len() < 2 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(lines(&path), vec!["{\"user\":\"a\"}", "{\"user\":\"b\"}"]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
