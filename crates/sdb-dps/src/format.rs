use crate::checksum::crc32;
use crate::log_record::{LogOp, LogRecord};
use sdb_common::{Lsn, SdbError, Result};

pub const FILE_MAGIC: [u8; 2] = [0x57, 0x4C]; // "WL"
pub const FILE_HEADER_SIZE: usize = 32;
pub const FRAME_MAGIC: [u8; 2] = [0xD1, 0x05];
pub const FRAME_HEADER_SIZE: usize = 35;

pub fn logop_to_u8(op: &LogOp) -> u8 {
    match op {
        LogOp::Insert => 0,
        LogOp::Update => 1,
        LogOp::Delete => 2,
        LogOp::TxnBegin => 3,
        LogOp::TxnCommit => 4,
        LogOp::TxnAbort => 5,
        LogOp::CollectionCreate => 6,
        LogOp::CollectionDrop => 7,
        LogOp::IndexCreate => 8,
        LogOp::IndexDrop => 9,
    }
}

pub fn logop_from_u8(v: u8) -> Result<LogOp> {
    match v {
        0 => Ok(LogOp::Insert),
        1 => Ok(LogOp::Update),
        2 => Ok(LogOp::Delete),
        3 => Ok(LogOp::TxnBegin),
        4 => Ok(LogOp::TxnCommit),
        5 => Ok(LogOp::TxnAbort),
        6 => Ok(LogOp::CollectionCreate),
        7 => Ok(LogOp::CollectionDrop),
        8 => Ok(LogOp::IndexCreate),
        9 => Ok(LogOp::IndexDrop),
        _ => Err(SdbError::InvalidArg),
    }
}

pub fn write_file_header(buf: &mut [u8], first_lsn: Lsn, record_count: u64) {
    debug_assert!(buf.len() >= FILE_HEADER_SIZE);
    buf[..FILE_HEADER_SIZE].fill(0);
    buf[0..2].copy_from_slice(&FILE_MAGIC);
    buf[2] = 1; // version
    buf[4..8].copy_from_slice(&(FILE_HEADER_SIZE as u32).to_le_bytes());
    buf[8..16].copy_from_slice(&first_lsn.to_le_bytes());
    buf[16..24].copy_from_slice(&record_count.to_le_bytes());
}

pub fn read_file_header(buf: &[u8]) -> Result<(Lsn, u64)> {
    if buf.len() < FILE_HEADER_SIZE {
        return Err(SdbError::CorruptedRecord);
    }
    if buf[0..2] != FILE_MAGIC {
        return Err(SdbError::CorruptedRecord);
    }
    let first_lsn = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let record_count = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    Ok((first_lsn, record_count))
}

/// Encode a LogRecord into a frame. The record's lsn and prev_lsn must already be set.
pub fn encode_frame(record: &LogRecord) -> Vec<u8> {
    let data_len = record.data.len() as u32;
    let frame_len = (FRAME_HEADER_SIZE + record.data.len() + 4) as u32;
    let mut buf = Vec::with_capacity(frame_len as usize);

    // frame magic
    buf.extend_from_slice(&FRAME_MAGIC);
    // frame_len
    buf.extend_from_slice(&frame_len.to_le_bytes());
    // lsn
    buf.extend_from_slice(&record.lsn.to_le_bytes());
    // prev_lsn
    buf.extend_from_slice(&record.prev_lsn.to_le_bytes());
    // txn_id
    buf.extend_from_slice(&record.txn_id.to_le_bytes());
    // op
    buf.push(logop_to_u8(&record.op));
    // data_len
    buf.extend_from_slice(&data_len.to_le_bytes());
    // data
    buf.extend_from_slice(&record.data);
    // crc32 over everything before CRC
    let checksum = crc32(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());

    buf
}

/// Decode a frame from a byte slice. Returns (LogRecord, bytes_consumed).
pub fn decode_frame(buf: &[u8]) -> Result<(LogRecord, usize)> {
    if buf.len() < FRAME_HEADER_SIZE + 4 {
        return Err(SdbError::CorruptedRecord);
    }
    if buf[0..2] != FRAME_MAGIC {
        return Err(SdbError::CorruptedRecord);
    }
    let frame_len = u32::from_le_bytes(buf[2..6].try_into().unwrap()) as usize;
    if buf.len() < frame_len {
        return Err(SdbError::CorruptedRecord);
    }

    let lsn = u64::from_le_bytes(buf[6..14].try_into().unwrap());
    let prev_lsn = u64::from_le_bytes(buf[14..22].try_into().unwrap());
    let txn_id = u64::from_le_bytes(buf[22..30].try_into().unwrap());
    let op = logop_from_u8(buf[30])?;
    let data_len = u32::from_le_bytes(buf[31..35].try_into().unwrap()) as usize;

    if frame_len != FRAME_HEADER_SIZE + data_len + 4 {
        return Err(SdbError::CorruptedRecord);
    }

    let data_end = FRAME_HEADER_SIZE + data_len;
    let data = buf[FRAME_HEADER_SIZE..data_end].to_vec();

    // Verify CRC
    let stored_crc = u32::from_le_bytes(buf[data_end..data_end + 4].try_into().unwrap());
    let computed_crc = crc32(&buf[..data_end]);
    if stored_crc != computed_crc {
        return Err(SdbError::CorruptedRecord);
    }

    Ok((
        LogRecord {
            lsn,
            prev_lsn,
            txn_id,
            op,
            data,
        },
        frame_len,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logop_roundtrip_all() {
        let ops = [
            LogOp::Insert,
            LogOp::Update,
            LogOp::Delete,
            LogOp::TxnBegin,
            LogOp::TxnCommit,
            LogOp::TxnAbort,
            LogOp::CollectionCreate,
            LogOp::CollectionDrop,
            LogOp::IndexCreate,
            LogOp::IndexDrop,
        ];
        for op in ops {
            let v = logop_to_u8(&op);
            let decoded = logop_from_u8(v).unwrap();
            assert_eq!(op, decoded);
        }
    }

    #[test]
    fn logop_invalid() {
        assert_eq!(logop_from_u8(10), Err(SdbError::InvalidArg));
        assert_eq!(logop_from_u8(255), Err(SdbError::InvalidArg));
    }

    #[test]
    fn file_header_roundtrip() {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        write_file_header(&mut buf, 32, 42);
        let (first_lsn, count) = read_file_header(&buf).unwrap();
        assert_eq!(first_lsn, 32);
        assert_eq!(count, 42);
    }

    #[test]
    fn file_header_bad_magic() {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        write_file_header(&mut buf, 0, 0);
        buf[0] = 0xFF;
        assert_eq!(read_file_header(&buf), Err(SdbError::CorruptedRecord));
    }

    #[test]
    fn file_header_too_short() {
        let buf = [0u8; 16];
        assert_eq!(read_file_header(&buf), Err(SdbError::CorruptedRecord));
    }

    #[test]
    fn frame_roundtrip_empty_data() {
        let record = LogRecord {
            lsn: 32,
            prev_lsn: 0,
            txn_id: 1,
            op: LogOp::TxnBegin,
            data: vec![],
        };
        let encoded = encode_frame(&record);
        assert_eq!(encoded.len(), FRAME_HEADER_SIZE + 4); // 39 bytes
        let (decoded, consumed) = decode_frame(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.lsn, 32);
        assert_eq!(decoded.prev_lsn, 0);
        assert_eq!(decoded.txn_id, 1);
        assert_eq!(decoded.op, LogOp::TxnBegin);
        assert!(decoded.data.is_empty());
    }

    #[test]
    fn frame_roundtrip_100b() {
        let data = vec![0xAB; 100];
        let record = LogRecord {
            lsn: 71,
            prev_lsn: 32,
            txn_id: 5,
            op: LogOp::Insert,
            data: data.clone(),
        };
        let encoded = encode_frame(&record);
        assert_eq!(encoded.len(), FRAME_HEADER_SIZE + 100 + 4);
        let (decoded, consumed) = decode_frame(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.data, data);
    }

    #[test]
    fn frame_roundtrip_64kb() {
        let data = vec![0x42; 65536];
        let record = LogRecord {
            lsn: 100,
            prev_lsn: 50,
            txn_id: 99,
            op: LogOp::Update,
            data: data.clone(),
        };
        let encoded = encode_frame(&record);
        let (decoded, _) = decode_frame(&encoded).unwrap();
        assert_eq!(decoded.data, data);
    }

    #[test]
    fn frame_crc_corruption() {
        let record = LogRecord {
            lsn: 32,
            prev_lsn: 0,
            txn_id: 1,
            op: LogOp::Insert,
            data: vec![1, 2, 3],
        };
        let mut encoded = encode_frame(&record);
        // Flip a byte in the data region
        encoded[FRAME_HEADER_SIZE] ^= 0xFF;
        assert_eq!(decode_frame(&encoded), Err(SdbError::CorruptedRecord));
    }

    #[test]
    fn frame_truncated() {
        let record = LogRecord {
            lsn: 32,
            prev_lsn: 0,
            txn_id: 1,
            op: LogOp::Insert,
            data: vec![1, 2, 3, 4, 5],
        };
        let encoded = encode_frame(&record);
        // Truncate the frame
        let truncated = &encoded[..encoded.len() - 3];
        assert_eq!(decode_frame(truncated), Err(SdbError::CorruptedRecord));
    }

    #[test]
    fn frame_bad_magic() {
        let mut encoded = encode_frame(&LogRecord {
            lsn: 32,
            prev_lsn: 0,
            txn_id: 1,
            op: LogOp::Delete,
            data: vec![],
        });
        encoded[0] = 0x00;
        assert_eq!(decode_frame(&encoded), Err(SdbError::CorruptedRecord));
    }
}
