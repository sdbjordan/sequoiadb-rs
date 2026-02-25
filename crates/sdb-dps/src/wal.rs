use crate::log_record::LogRecord;
use sdb_common::{Lsn, Result};

/// Write-ahead log for crash recovery and replication.
pub struct WriteAheadLog {
    pub log_path: String,
    pub current_lsn: Lsn,
    pub flushed_lsn: Lsn,
}

impl WriteAheadLog {
    pub fn open(_path: &str) -> Result<Self> {
        // Stub
        Ok(Self {
            log_path: _path.to_string(),
            current_lsn: 0,
            flushed_lsn: 0,
        })
    }

    pub fn append(&mut self, _record: &LogRecord) -> Result<Lsn> {
        // Stub
        self.current_lsn += 1;
        Ok(self.current_lsn)
    }

    pub fn flush(&mut self) -> Result<()> {
        // Stub
        self.flushed_lsn = self.current_lsn;
        Ok(())
    }

    pub fn read(&self, _lsn: Lsn) -> Result<LogRecord> {
        // Stub
        Err(sdb_common::SdbError::NotFound)
    }
}
