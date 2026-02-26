use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

use crate::format::{
    self, decode_frame, encode_frame, read_file_header, write_file_header, FILE_HEADER_SIZE,
    FRAME_MAGIC,
};
use crate::iter::WalIterator;
use crate::log_record::LogRecord;
use sdb_common::{Lsn, Result, SdbError};

/// Write-ahead log for crash recovery and replication.
pub struct WriteAheadLog {
    log_path: String,
    current_lsn: Lsn,
    flushed_lsn: Lsn,
    file: File,
    write_buffer: Vec<u8>,
    file_end_offset: u64,
    record_count: u64,
    prev_lsn: Lsn,
}

impl WriteAheadLog {
    /// Open or create a WAL file at `{path}/wal.sdb`.
    /// On existing files, scans forward to recover current_lsn.
    pub fn open(path: &str) -> Result<Self> {
        fs::create_dir_all(path).map_err(|_| SdbError::IoError)?;

        let file_path: PathBuf = [path, "wal.sdb"].iter().collect();
        let exists = file_path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&file_path)
            .map_err(|_| SdbError::IoError)?;

        if !exists || file.metadata().map_err(|_| SdbError::IoError)?.len() == 0 {
            // New file: write header
            let mut header = [0u8; FILE_HEADER_SIZE];
            write_file_header(&mut header, FILE_HEADER_SIZE as Lsn, 0);
            file.write_at(&header, 0).map_err(|_| SdbError::IoError)?;
            file.sync_all().map_err(|_| SdbError::IoError)?;

            return Ok(Self {
                log_path: path.to_string(),
                current_lsn: FILE_HEADER_SIZE as Lsn,
                flushed_lsn: FILE_HEADER_SIZE as Lsn,
                file,
                write_buffer: Vec::new(),
                file_end_offset: FILE_HEADER_SIZE as u64,
                record_count: 0,
                prev_lsn: 0,
            });
        }

        // Existing file: read header, then scan to recover
        let file_len = file.metadata().map_err(|_| SdbError::IoError)?.len();
        let mut header = [0u8; FILE_HEADER_SIZE];
        file.read_at(&mut header, 0).map_err(|_| SdbError::IoError)?;
        let (_first_lsn, _record_count) = read_file_header(&header)?;

        // Scan all valid frames to find end offset and prev_lsn
        let mut offset = FILE_HEADER_SIZE as u64;
        let mut count = 0u64;
        let mut last_lsn: Lsn = 0;

        while offset < file_len {
            let remaining = (file_len - offset) as usize;
            let read_size = remaining.min(format::FRAME_HEADER_SIZE + 4);
            if read_size < format::FRAME_HEADER_SIZE + 4 {
                break; // Not enough for minimal frame
            }
            // Read frame_len from header to know full frame size
            let mut peek = [0u8; 6]; // magic(2) + frame_len(4)
            file.read_at(&mut peek, offset).map_err(|_| SdbError::IoError)?;
            if peek[0..2] != FRAME_MAGIC {
                break;
            }
            let frame_len = u32::from_le_bytes(peek[2..6].try_into().unwrap()) as usize;
            if (offset as usize + frame_len) as u64 > file_len {
                break; // Incomplete frame
            }
            let mut frame_buf = vec![0u8; frame_len];
            file.read_at(&mut frame_buf, offset).map_err(|_| SdbError::IoError)?;
            match decode_frame(&frame_buf) {
                Ok((record, _)) => {
                    last_lsn = record.lsn;
                    count += 1;
                    offset += frame_len as u64;
                }
                Err(_) => break, // Corrupted frame, stop here
            }
        }

        // Truncate any trailing garbage
        if offset < file_len {
            file.set_len(offset).map_err(|_| SdbError::IoError)?;
            file.sync_all().map_err(|_| SdbError::IoError)?;
        }

        // Update header with correct record count
        let mut header = [0u8; FILE_HEADER_SIZE];
        write_file_header(&mut header, FILE_HEADER_SIZE as Lsn, count);
        file.write_at(&header, 0).map_err(|_| SdbError::IoError)?;
        file.sync_all().map_err(|_| SdbError::IoError)?;

        Ok(Self {
            log_path: path.to_string(),
            current_lsn: offset,
            flushed_lsn: offset,
            file,
            write_buffer: Vec::new(),
            file_end_offset: offset,
            record_count: count,
            prev_lsn: last_lsn,
        })
    }

    /// Append a record to the write buffer. Assigns LSN and prev_lsn.
    pub fn append(&mut self, record: &mut LogRecord) -> Result<Lsn> {
        let lsn = self.file_end_offset + self.write_buffer.len() as u64;
        record.lsn = lsn;
        record.prev_lsn = self.prev_lsn;

        let frame = encode_frame(record);
        self.write_buffer.extend_from_slice(&frame);
        self.current_lsn = self.file_end_offset + self.write_buffer.len() as u64;
        self.prev_lsn = lsn;
        self.record_count += 1;

        Ok(lsn)
    }

    /// Flush write buffer to disk with fsync, update file header.
    pub fn flush(&mut self) -> Result<()> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        self.file
            .write_at(&self.write_buffer, self.file_end_offset)
            .map_err(|_| SdbError::IoError)?;

        self.file_end_offset += self.write_buffer.len() as u64;
        self.write_buffer.clear();

        // Update file header with record count
        let mut header = [0u8; FILE_HEADER_SIZE];
        write_file_header(&mut header, FILE_HEADER_SIZE as Lsn, self.record_count);
        self.file.write_at(&header, 0).map_err(|_| SdbError::IoError)?;

        self.file.sync_all().map_err(|_| SdbError::IoError)?;
        self.flushed_lsn = self.file_end_offset;

        Ok(())
    }

    /// Read a single record by LSN. Works for both flushed and buffered records.
    pub fn read(&self, lsn: Lsn) -> Result<LogRecord> {
        if lsn < FILE_HEADER_SIZE as u64 || lsn >= self.current_lsn {
            return Err(SdbError::NotFound);
        }

        if lsn >= self.file_end_offset {
            // Read from write_buffer
            let buf_offset = (lsn - self.file_end_offset) as usize;
            if buf_offset >= self.write_buffer.len() {
                return Err(SdbError::NotFound);
            }
            let (record, _) = decode_frame(&self.write_buffer[buf_offset..])?;
            return Ok(record);
        }

        // Read from file via pread
        // First read the frame length
        let mut peek = [0u8; 6];
        self.file.read_at(&mut peek, lsn).map_err(|_| SdbError::IoError)?;
        if peek[0..2] != FRAME_MAGIC {
            return Err(SdbError::CorruptedRecord);
        }
        let frame_len = u32::from_le_bytes(peek[2..6].try_into().unwrap()) as usize;
        let mut frame_buf = vec![0u8; frame_len];
        self.file.read_at(&mut frame_buf, lsn).map_err(|_| SdbError::IoError)?;
        let (record, _) = decode_frame(&frame_buf)?;
        Ok(record)
    }

    /// Create an iterator that scans flushed records starting from `from_lsn`.
    pub fn scan_from(&self, from_lsn: Lsn) -> Result<WalIterator> {
        WalIterator::new(&self.wal_file_path(), from_lsn, self.file_end_offset)
    }

    /// Recover all records from the WAL. Truncates any incomplete trailing frame.
    pub fn recover(&mut self) -> Result<Vec<LogRecord>> {
        let mut records = Vec::new();
        let mut offset = FILE_HEADER_SIZE as u64;

        while offset < self.file_end_offset {
            let mut peek = [0u8; 6];
            self.file.read_at(&mut peek, offset).map_err(|_| SdbError::IoError)?;
            if peek[0..2] != FRAME_MAGIC {
                break;
            }
            let frame_len = u32::from_le_bytes(peek[2..6].try_into().unwrap()) as usize;
            if offset + frame_len as u64 > self.file_end_offset {
                break;
            }
            let mut frame_buf = vec![0u8; frame_len];
            self.file.read_at(&mut frame_buf, offset).map_err(|_| SdbError::IoError)?;
            match decode_frame(&frame_buf) {
                Ok((record, _)) => {
                    records.push(record);
                    offset += frame_len as u64;
                }
                Err(_) => break,
            }
        }

        // Truncate if we stopped early
        if offset < self.file_end_offset {
            self.file.set_len(offset).map_err(|_| SdbError::IoError)?;
            self.file_end_offset = offset;
            self.current_lsn = offset;
            self.flushed_lsn = offset;
            self.record_count = records.len() as u64;

            let mut header = [0u8; FILE_HEADER_SIZE];
            write_file_header(&mut header, FILE_HEADER_SIZE as Lsn, self.record_count);
            self.file.write_at(&header, 0).map_err(|_| SdbError::IoError)?;
            self.file.sync_all().map_err(|_| SdbError::IoError)?;
        }

        Ok(records)
    }

    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn
    }

    pub fn flushed_lsn(&self) -> Lsn {
        self.flushed_lsn
    }

    /// Return the WAL directory path.
    pub fn log_path(&self) -> &str {
        &self.log_path
    }

    fn wal_file_path(&self) -> String {
        let mut p = PathBuf::from(&self.log_path);
        p.push("wal.sdb");
        p.to_string_lossy().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_record::LogOp;
    use std::env;

    fn tmp_dir(name: &str) -> String {
        let p = env::temp_dir().join(format!("sdb_wal_test_{}", name));
        let _ = fs::remove_dir_all(&p);
        p.to_string_lossy().to_string()
    }

    fn cleanup(path: &str) {
        let _ = fs::remove_dir_all(path);
    }

    fn make_record(op: LogOp, txn_id: u64, data: Vec<u8>) -> LogRecord {
        LogRecord {
            lsn: 0,
            prev_lsn: 0,
            txn_id,
            op,
            data,
        }
    }

    #[test]
    fn open_creates_valid_header() {
        let dir = tmp_dir("open_header");
        let wal = WriteAheadLog::open(&dir).unwrap();
        assert_eq!(wal.current_lsn(), FILE_HEADER_SIZE as u64);
        assert_eq!(wal.flushed_lsn(), FILE_HEADER_SIZE as u64);

        // Verify file header
        let file_path: PathBuf = [&dir, "wal.sdb"].iter().collect();
        let data = fs::read(&file_path).unwrap();
        assert!(data.len() >= FILE_HEADER_SIZE);
        let (first_lsn, count) = read_file_header(&data).unwrap();
        assert_eq!(first_lsn, FILE_HEADER_SIZE as u64);
        assert_eq!(count, 0);
        cleanup(&dir);
    }

    #[test]
    fn append_assigns_incrementing_lsn() {
        let dir = tmp_dir("append_lsn");
        let mut wal = WriteAheadLog::open(&dir).unwrap();
        let mut r1 = make_record(LogOp::Insert, 1, vec![0xAA; 10]);
        let lsn1 = wal.append(&mut r1).unwrap();
        assert_eq!(lsn1, 32); // FILE_HEADER_SIZE

        let mut r2 = make_record(LogOp::Update, 1, vec![0xBB; 20]);
        let lsn2 = wal.append(&mut r2).unwrap();
        assert!(lsn2 > lsn1);
        cleanup(&dir);
    }

    #[test]
    fn append_sets_prev_lsn_chain() {
        let dir = tmp_dir("prev_lsn");
        let mut wal = WriteAheadLog::open(&dir).unwrap();
        let mut r1 = make_record(LogOp::TxnBegin, 1, vec![]);
        let lsn1 = wal.append(&mut r1).unwrap();
        assert_eq!(r1.prev_lsn, 0); // first record

        let mut r2 = make_record(LogOp::Insert, 1, vec![1, 2, 3]);
        wal.append(&mut r2).unwrap();
        assert_eq!(r2.prev_lsn, lsn1);
        cleanup(&dir);
    }

    #[test]
    fn flush_persists_and_updates_offset() {
        let dir = tmp_dir("flush");
        let mut wal = WriteAheadLog::open(&dir).unwrap();
        let mut r = make_record(LogOp::Insert, 1, vec![0x42; 50]);
        wal.append(&mut r).unwrap();
        let before = wal.flushed_lsn();
        wal.flush().unwrap();
        assert!(wal.flushed_lsn() > before);
        assert_eq!(wal.flushed_lsn(), wal.current_lsn());
        cleanup(&dir);
    }

    #[test]
    fn flush_empty_is_noop() {
        let dir = tmp_dir("flush_empty");
        let mut wal = WriteAheadLog::open(&dir).unwrap();
        let lsn_before = wal.flushed_lsn();
        wal.flush().unwrap(); // should not error
        assert_eq!(wal.flushed_lsn(), lsn_before);
        cleanup(&dir);
    }

    #[test]
    fn read_flushed_record() {
        let dir = tmp_dir("read_flushed");
        let mut wal = WriteAheadLog::open(&dir).unwrap();
        let mut r = make_record(LogOp::Insert, 7, vec![1, 2, 3, 4, 5]);
        let lsn = wal.append(&mut r).unwrap();
        wal.flush().unwrap();

        let loaded = wal.read(lsn).unwrap();
        assert_eq!(loaded.lsn, lsn);
        assert_eq!(loaded.txn_id, 7);
        assert_eq!(loaded.op, LogOp::Insert);
        assert_eq!(loaded.data, vec![1, 2, 3, 4, 5]);
        cleanup(&dir);
    }

    #[test]
    fn read_unflushed_record() {
        let dir = tmp_dir("read_unflushed");
        let mut wal = WriteAheadLog::open(&dir).unwrap();
        let mut r = make_record(LogOp::Delete, 3, vec![9, 8, 7]);
        let lsn = wal.append(&mut r).unwrap();
        // Do NOT flush

        let loaded = wal.read(lsn).unwrap();
        assert_eq!(loaded.lsn, lsn);
        assert_eq!(loaded.op, LogOp::Delete);
        assert_eq!(loaded.data, vec![9, 8, 7]);
        cleanup(&dir);
    }

    #[test]
    fn read_invalid_lsn() {
        let dir = tmp_dir("read_invalid");
        let wal = WriteAheadLog::open(&dir).unwrap();
        assert_eq!(wal.read(0), Err(SdbError::NotFound));
        assert_eq!(wal.read(9999), Err(SdbError::NotFound));
        cleanup(&dir);
    }

    #[test]
    fn recover_returns_all_records() {
        let dir = tmp_dir("recover_all");
        let mut wal = WriteAheadLog::open(&dir).unwrap();
        for i in 0..5 {
            let mut r = make_record(LogOp::Insert, i, vec![i as u8; 10]);
            wal.append(&mut r).unwrap();
        }
        wal.flush().unwrap();

        let records = wal.recover().unwrap();
        assert_eq!(records.len(), 5);
        for (i, rec) in records.iter().enumerate() {
            assert_eq!(rec.txn_id, i as u64);
        }
        cleanup(&dir);
    }

    #[test]
    fn recover_truncates_incomplete_frame() {
        let dir = tmp_dir("recover_trunc");
        let mut wal = WriteAheadLog::open(&dir).unwrap();
        let mut r = make_record(LogOp::Insert, 1, vec![0xAA; 20]);
        wal.append(&mut r).unwrap();
        wal.flush().unwrap();
        let valid_end = wal.file_end_offset;
        drop(wal);

        // Append garbage at end of file
        let file_path: PathBuf = [&dir, "wal.sdb"].iter().collect();
        let file = OpenOptions::new().write(true).open(&file_path).unwrap();
        file.write_at(&[0xD1, 0x05, 0xFF, 0xFF, 0x00, 0x00], valid_end)
            .unwrap();
        file.set_len(valid_end + 6).unwrap();
        drop(file);

        let mut wal = WriteAheadLog::open(&dir).unwrap();
        let records = wal.recover().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(wal.file_end_offset, valid_end);
        cleanup(&dir);
    }

    #[test]
    fn recover_empty_wal() {
        let dir = tmp_dir("recover_empty");
        let mut wal = WriteAheadLog::open(&dir).unwrap();
        let records = wal.recover().unwrap();
        assert!(records.is_empty());
        cleanup(&dir);
    }

    #[test]
    fn reopen_preserves_lsn() {
        let dir = tmp_dir("reopen");
        let expected_lsn;
        {
            let mut wal = WriteAheadLog::open(&dir).unwrap();
            let mut r1 = make_record(LogOp::Insert, 1, vec![1, 2, 3]);
            wal.append(&mut r1).unwrap();
            let mut r2 = make_record(LogOp::Update, 2, vec![4, 5, 6]);
            wal.append(&mut r2).unwrap();
            wal.flush().unwrap();
            expected_lsn = wal.current_lsn();
        }

        let wal = WriteAheadLog::open(&dir).unwrap();
        assert_eq!(wal.current_lsn(), expected_lsn);
        cleanup(&dir);
    }

    #[test]
    fn read_after_reopen() {
        let dir = tmp_dir("read_reopen");
        let lsn;
        {
            let mut wal = WriteAheadLog::open(&dir).unwrap();
            let mut r = make_record(LogOp::CollectionCreate, 10, b"test_coll".to_vec());
            lsn = wal.append(&mut r).unwrap();
            wal.flush().unwrap();
        }

        let wal = WriteAheadLog::open(&dir).unwrap();
        let loaded = wal.read(lsn).unwrap();
        assert_eq!(loaded.op, LogOp::CollectionCreate);
        assert_eq!(loaded.data, b"test_coll");
        cleanup(&dir);
    }

    #[test]
    fn multiple_flush_cycles() {
        let dir = tmp_dir("multi_flush");
        let mut wal = WriteAheadLog::open(&dir).unwrap();

        let mut r1 = make_record(LogOp::TxnBegin, 1, vec![]);
        let lsn1 = wal.append(&mut r1).unwrap();
        wal.flush().unwrap();

        let mut r2 = make_record(LogOp::Insert, 1, vec![0xFF; 100]);
        let lsn2 = wal.append(&mut r2).unwrap();
        wal.flush().unwrap();

        let mut r3 = make_record(LogOp::TxnCommit, 1, vec![]);
        let lsn3 = wal.append(&mut r3).unwrap();
        wal.flush().unwrap();

        let r = wal.read(lsn1).unwrap();
        assert_eq!(r.op, LogOp::TxnBegin);
        let r = wal.read(lsn2).unwrap();
        assert_eq!(r.op, LogOp::Insert);
        let r = wal.read(lsn3).unwrap();
        assert_eq!(r.op, LogOp::TxnCommit);

        let records = wal.recover().unwrap();
        assert_eq!(records.len(), 3);
        cleanup(&dir);
    }
}
