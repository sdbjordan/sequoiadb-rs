"""Low-level codec helpers: LE read/write, C-string, 4-byte alignment."""

import struct


# ── Write helpers ──────────────────────────────────────────────────────

def write_i32(buf: bytearray, v: int) -> None:
    buf.extend(struct.pack("<i", v))


def write_u32(buf: bytearray, v: int) -> None:
    buf.extend(struct.pack("<I", v))


def write_i16(buf: bytearray, v: int) -> None:
    buf.extend(struct.pack("<h", v))


def write_i64(buf: bytearray, v: int) -> None:
    buf.extend(struct.pack("<q", v))


def write_u64(buf: bytearray, v: int) -> None:
    buf.extend(struct.pack("<Q", v))


def write_cstring(buf: bytearray, s: str) -> None:
    buf.extend(s.encode("utf-8"))
    buf.append(0)


def pad_align4(buf: bytearray) -> None:
    r = len(buf) % 4
    if r != 0:
        buf.extend(b"\x00" * (4 - r))


# ── Read helpers ───────────────────────────────────────────────────────

def read_i32(buf: bytes, offset: int) -> tuple[int, int]:
    return struct.unpack_from("<i", buf, offset)[0], offset + 4


def read_u32(buf: bytes, offset: int) -> tuple[int, int]:
    return struct.unpack_from("<I", buf, offset)[0], offset + 4


def read_i16(buf: bytes, offset: int) -> tuple[int, int]:
    return struct.unpack_from("<h", buf, offset)[0], offset + 2


def read_i64(buf: bytes, offset: int) -> tuple[int, int]:
    return struct.unpack_from("<q", buf, offset)[0], offset + 8


def read_u64(buf: bytes, offset: int) -> tuple[int, int]:
    return struct.unpack_from("<Q", buf, offset)[0], offset + 8


def read_cstring(buf: bytes, offset: int) -> tuple[str, int]:
    end = buf.index(0, offset)
    s = buf[offset:end].decode("utf-8")
    return s, end + 1


def read_bytes(buf: bytes, offset: int, n: int) -> tuple[bytes, int]:
    return buf[offset:offset + n], offset + n


def skip_align4(buf: bytes, offset: int) -> int:
    r = offset % 4
    if r != 0:
        pad = 4 - r
        if offset + pad <= len(buf):
            offset += pad
    return offset
