use crate::error::{BsonError, BsonResult};

// ── Write helpers ──

#[inline]
pub fn write_i32_le(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn write_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

#[inline]
pub fn write_i64_le(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn write_u64_le(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn write_f64_le(buf: &mut Vec<u8>, v: f64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// Write a UTF-8 string as a cstring (bytes + 0x00 terminator).
pub fn write_cstring(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

/// Write a BSON string: [i32 len_with_null] [UTF-8 bytes] [0x00].
pub fn write_string(buf: &mut Vec<u8>, s: &str) {
    let len = s.len() as i32 + 1; // includes null terminator
    write_i32_le(buf, len);
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

// ── Read helpers ──

#[inline]
pub fn ensure_remaining(buf: &[u8], offset: usize, need: usize) -> BsonResult<()> {
    if buf.len() < offset + need {
        Err(BsonError::BufferTooShort {
            need: offset + need,
            have: buf.len(),
        })
    } else {
        Ok(())
    }
}

#[inline]
pub fn read_u8(buf: &[u8], offset: &mut usize) -> BsonResult<u8> {
    ensure_remaining(buf, *offset, 1)?;
    let v = buf[*offset];
    *offset += 1;
    Ok(v)
}

#[inline]
pub fn read_i32_le(buf: &[u8], offset: &mut usize) -> BsonResult<i32> {
    ensure_remaining(buf, *offset, 4)?;
    let v = i32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    Ok(v)
}

#[inline]
pub fn read_i64_le(buf: &[u8], offset: &mut usize) -> BsonResult<i64> {
    ensure_remaining(buf, *offset, 8)?;
    let v = i64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    Ok(v)
}

#[inline]
pub fn read_u64_le(buf: &[u8], offset: &mut usize) -> BsonResult<u64> {
    ensure_remaining(buf, *offset, 8)?;
    let v = u64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    Ok(v)
}

#[inline]
pub fn read_f64_le(buf: &[u8], offset: &mut usize) -> BsonResult<f64> {
    ensure_remaining(buf, *offset, 8)?;
    let v = f64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    Ok(v)
}

/// Read a cstring (null-terminated). Returns the string without the null byte.
pub fn read_cstring(buf: &[u8], offset: &mut usize) -> BsonResult<String> {
    let start = *offset;
    loop {
        ensure_remaining(buf, *offset, 1)?;
        if buf[*offset] == 0 {
            let s = std::str::from_utf8(&buf[start..*offset])
                .map_err(|_| BsonError::InvalidUtf8)?;
            *offset += 1; // skip null
            return Ok(s.to_owned());
        }
        *offset += 1;
    }
}

/// Read a BSON string: [i32 len] [UTF-8] [0x00].
pub fn read_string(buf: &[u8], offset: &mut usize) -> BsonResult<String> {
    let len = read_i32_le(buf, offset)?;
    if len < 1 {
        return Err(BsonError::InvalidData);
    }
    let byte_len = len as usize;
    ensure_remaining(buf, *offset, byte_len)?;
    let s = std::str::from_utf8(&buf[*offset..*offset + byte_len - 1])
        .map_err(|_| BsonError::InvalidUtf8)?;
    // verify null terminator
    if buf[*offset + byte_len - 1] != 0 {
        return Err(BsonError::InvalidData);
    }
    *offset += byte_len;
    Ok(s.to_owned())
}

/// Read exactly `n` bytes and return as Vec<u8>.
pub fn read_bytes(buf: &[u8], offset: &mut usize, n: usize) -> BsonResult<Vec<u8>> {
    ensure_remaining(buf, *offset, n)?;
    let v = buf[*offset..*offset + n].to_vec();
    *offset += n;
    Ok(v)
}
