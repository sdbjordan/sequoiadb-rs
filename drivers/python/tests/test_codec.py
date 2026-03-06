"""Unit tests for protocol/codec.py — 6 tests."""

from sequoiadb.protocol.codec import (
    write_i32, read_i32,
    write_i64, read_i64,
    write_i16, read_i16,
    write_cstring, read_cstring,
    pad_align4,
)


def test_i32_roundtrip():
    buf = bytearray()
    write_i32(buf, -12345)
    val, off = read_i32(bytes(buf), 0)
    assert val == -12345
    assert off == 4


def test_i64_roundtrip():
    buf = bytearray()
    write_i64(buf, 0x7FFFFFFFFFFFFFFF)
    val, off = read_i64(bytes(buf), 0)
    assert val == 0x7FFFFFFFFFFFFFFF
    assert off == 8


def test_i16_roundtrip():
    buf = bytearray()
    write_i16(buf, -300)
    val, off = read_i16(bytes(buf), 0)
    assert val == -300
    assert off == 2


def test_cstring_roundtrip():
    buf = bytearray()
    write_cstring(buf, "hello")
    val, off = read_cstring(bytes(buf), 0)
    assert val == "hello"
    assert off == 6  # 5 chars + null


def test_pad_align4():
    buf = bytearray(b"\x00" * 5)
    pad_align4(buf)
    assert len(buf) == 8

    buf2 = bytearray(b"\x00" * 4)
    pad_align4(buf2)
    assert len(buf2) == 4  # already aligned


def test_empty_cstring():
    buf = bytearray()
    write_cstring(buf, "")
    val, off = read_cstring(bytes(buf), 0)
    assert val == ""
    assert off == 1  # just the null byte
