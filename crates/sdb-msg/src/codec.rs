use sdb_common::SdbError;

/// Read an i32 (little-endian) from `buf` at `offset`, advancing the offset.
#[inline]
pub fn read_i32(buf: &[u8], offset: &mut usize) -> Result<i32, SdbError> {
    ensure(buf, *offset, 4)?;
    let v = i32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    Ok(v)
}

/// Read a u32 (little-endian) from `buf` at `offset`.
#[inline]
pub fn read_u32(buf: &[u8], offset: &mut usize) -> Result<u32, SdbError> {
    ensure(buf, *offset, 4)?;
    let v = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    Ok(v)
}

/// Read an i16 (little-endian) from `buf` at `offset`.
#[inline]
pub fn read_i16(buf: &[u8], offset: &mut usize) -> Result<i16, SdbError> {
    ensure(buf, *offset, 2)?;
    let v = i16::from_le_bytes(buf[*offset..*offset + 2].try_into().unwrap());
    *offset += 2;
    Ok(v)
}

/// Read an i64 (little-endian) from `buf` at `offset`.
#[inline]
pub fn read_i64(buf: &[u8], offset: &mut usize) -> Result<i64, SdbError> {
    ensure(buf, *offset, 8)?;
    let v = i64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    Ok(v)
}

/// Read a u64 (little-endian) from `buf` at `offset`.
#[inline]
pub fn read_u64(buf: &[u8], offset: &mut usize) -> Result<u64, SdbError> {
    ensure(buf, *offset, 8)?;
    let v = u64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    Ok(v)
}

/// Read a null-terminated C string. Returns the string (without null) and advances past the null.
pub fn read_cstring(buf: &[u8], offset: &mut usize) -> Result<String, SdbError> {
    let start = *offset;
    while *offset < buf.len() {
        if buf[*offset] == 0 {
            let s = std::str::from_utf8(&buf[start..*offset]).map_err(|_| SdbError::InvalidBson)?;
            *offset += 1; // skip null
            return Ok(s.to_owned());
        }
        *offset += 1;
    }
    Err(SdbError::InvalidArg)
}

/// Read exactly `n` bytes from `buf` at `offset`.
#[inline]
pub fn read_bytes<'a>(buf: &'a [u8], offset: &mut usize, n: usize) -> Result<&'a [u8], SdbError> {
    ensure(buf, *offset, n)?;
    let slice = &buf[*offset..*offset + n];
    *offset += n;
    Ok(slice)
}

// ── Write helpers ──

#[inline]
pub fn write_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn write_i16(buf: &mut Vec<u8>, v: i16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn write_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// Write a null-terminated C string (bytes + 0x00).
pub fn write_cstring(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

/// Pad buffer to 4-byte alignment.
#[inline]
pub fn pad_align4(buf: &mut Vec<u8>) {
    let r = buf.len() % 4;
    if r != 0 {
        for _ in 0..(4 - r) {
            buf.push(0);
        }
    }
}

/// Advance offset to 4-byte alignment.
#[inline]
pub fn skip_align4(buf: &[u8], offset: &mut usize) {
    let r = *offset % 4;
    if r != 0 {
        let pad = 4 - r;
        if *offset + pad <= buf.len() {
            *offset += pad;
        }
    }
}

#[inline]
fn ensure(buf: &[u8], offset: usize, need: usize) -> Result<(), SdbError> {
    if buf.len() < offset + need {
        Err(SdbError::InvalidArg)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_i32() {
        let mut buf = Vec::new();
        write_i32(&mut buf, -12345);
        let mut off = 0;
        assert_eq!(read_i32(&buf, &mut off).unwrap(), -12345);
        assert_eq!(off, 4);
    }

    #[test]
    fn roundtrip_i64() {
        let mut buf = Vec::new();
        write_i64(&mut buf, 0x7FFF_FFFF_FFFF_FFFF);
        let mut off = 0;
        assert_eq!(read_i64(&buf, &mut off).unwrap(), 0x7FFF_FFFF_FFFF_FFFF);
    }

    #[test]
    fn roundtrip_cstring() {
        let mut buf = Vec::new();
        write_cstring(&mut buf, "hello");
        let mut off = 0;
        assert_eq!(read_cstring(&buf, &mut off).unwrap(), "hello");
        assert_eq!(off, 6); // 5 chars + null
    }

    #[test]
    fn pad_align4_works() {
        let mut buf = vec![0u8; 5];
        pad_align4(&mut buf);
        assert_eq!(buf.len(), 8);

        let mut buf2 = vec![0u8; 4];
        pad_align4(&mut buf2);
        assert_eq!(buf2.len(), 4); // already aligned
    }
}
