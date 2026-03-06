"""Unit tests for protocol/header.py — 4 tests."""

import pytest
from sequoiadb.protocol.header import MsgHeader, HEADER_SIZE, EYE_DEFAULT


def test_header_size_is_52():
    assert HEADER_SIZE == 52


def test_header_roundtrip():
    h = MsgHeader(
        msg_len=100,
        eye=EYE_DEFAULT,
        tid=42,
        route_id=0xDEADBEEF,
        request_id=12345,
        opcode=2004,
        version=1,
        flags=0,
        query_id=bytes([1, 2, 3, 4, 5, 6, 7, 8]),
        query_op_id=99,
    )
    buf = bytearray()
    h.encode(buf)
    assert len(buf) == HEADER_SIZE

    decoded = MsgHeader.decode(bytes(buf))
    assert decoded.msg_len == 100
    assert decoded.eye == EYE_DEFAULT
    assert decoded.tid == 42
    assert decoded.route_id == 0xDEADBEEF
    assert decoded.request_id == 12345
    assert decoded.opcode == 2004
    assert decoded.version == 1
    assert decoded.flags == 0
    assert decoded.query_id == bytes([1, 2, 3, 4, 5, 6, 7, 8])
    assert decoded.query_op_id == 99


def test_new_request_defaults():
    h = MsgHeader.new_request(2004, 1)
    assert h.msg_len == HEADER_SIZE
    assert h.eye == EYE_DEFAULT
    assert h.opcode == 2004
    assert h.request_id == 1
    assert h.version == 1
    assert h.flags == 0


def test_decode_too_short():
    with pytest.raises(ValueError):
        MsgHeader.decode(b"\x00" * 40)
