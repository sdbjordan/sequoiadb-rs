use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use crate::format::{decode_frame, FRAME_HEADER_SIZE, FRAME_MAGIC};
use crate::log_record::LogRecord;
use sdb_common::{Lsn, Result, SdbError};

/// Sequential iterator over flushed WAL records.
pub struct WalIterator {
    reader: BufReader<File>,
    offset: u64,
    file_len: u64,
}

impl WalIterator {
    pub(crate) fn new(path: &str, from_lsn: Lsn, file_len: u64) -> Result<Self> {
        let file = File::open(path).map_err(|_| SdbError::IoError)?;
        let mut reader = BufReader::new(file);
        reader
            .seek(SeekFrom::Start(from_lsn))
            .map_err(|_| SdbError::IoError)?;
        Ok(Self {
            reader,
            offset: from_lsn,
            file_len,
        })
    }
}

impl Iterator for WalIterator {
    type Item = Result<LogRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.file_len {
            return None;
        }

        let remaining = (self.file_len - self.offset) as usize;
        if remaining < FRAME_HEADER_SIZE + 4 {
            return None;
        }

        // Read magic + frame_len
        let mut peek = [0u8; 6];
        if self.reader.read_exact(&mut peek).is_err() {
            return None;
        }
        if peek[0..2] != FRAME_MAGIC {
            return Some(Err(SdbError::CorruptedRecord));
        }
        let frame_len = u32::from_le_bytes(peek[2..6].try_into().unwrap()) as usize;
        if self.offset + frame_len as u64 > self.file_len {
            return None;
        }

        // Read rest of frame
        let mut frame_buf = vec![0u8; frame_len];
        frame_buf[..6].copy_from_slice(&peek);
        if self.reader.read_exact(&mut frame_buf[6..]).is_err() {
            return Some(Err(SdbError::IoError));
        }

        match decode_frame(&frame_buf) {
            Ok((record, consumed)) => {
                self.offset += consumed as u64;
                Some(Ok(record))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_record::LogOp;
    use crate::wal::WriteAheadLog;
    use crate::format::FILE_HEADER_SIZE;
    use std::{env, fs};

    fn tmp_dir(name: &str) -> String {
        let p = env::temp_dir().join(format!("sdb_iter_test_{}", name));
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
    fn scan_from_beginning() {
        let dir = tmp_dir("scan_begin");
        let mut wal = WriteAheadLog::open(&dir).unwrap();
        for i in 0..3 {
            let mut r = make_record(LogOp::Insert, i, vec![i as u8]);
            wal.append(&mut r).unwrap();
        }
        wal.flush().unwrap();

        let iter = wal.scan_from(FILE_HEADER_SIZE as Lsn).unwrap();
        let records: Vec<_> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].txn_id, 0);
        assert_eq!(records[2].txn_id, 2);
        cleanup(&dir);
    }

    #[test]
    fn scan_from_middle() {
        let dir = tmp_dir("scan_mid");
        let mut wal = WriteAheadLog::open(&dir).unwrap();
        let mut lsns = vec![];
        for i in 0..4 {
            let mut r = make_record(LogOp::Insert, i, vec![i as u8; 10]);
            lsns.push(wal.append(&mut r).unwrap());
        }
        wal.flush().unwrap();

        // Scan starting from the 3rd record
        let iter = wal.scan_from(lsns[2]).unwrap();
        let records: Vec<_> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].txn_id, 2);
        assert_eq!(records[1].txn_id, 3);
        cleanup(&dir);
    }

    #[test]
    fn scan_from_end() {
        let dir = tmp_dir("scan_end");
        let mut wal = WriteAheadLog::open(&dir).unwrap();
        let mut r = make_record(LogOp::Insert, 1, vec![1]);
        wal.append(&mut r).unwrap();
        wal.flush().unwrap();

        // Scan from current_lsn (past end)
        let iter = wal.scan_from(wal.flushed_lsn()).unwrap();
        let records: Vec<_> = iter.collect();
        assert!(records.is_empty());
        cleanup(&dir);
    }

    #[test]
    fn scan_empty_wal() {
        let dir = tmp_dir("scan_empty");
        let wal = WriteAheadLog::open(&dir).unwrap();
        let iter = wal.scan_from(FILE_HEADER_SIZE as Lsn).unwrap();
        let records: Vec<_> = iter.collect();
        assert!(records.is_empty());
        cleanup(&dir);
    }
}
