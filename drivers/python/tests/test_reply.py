"""Unit tests for protocol/reply.py — 3 tests."""

from sequoiadb.protocol.header import MsgHeader, HEADER_SIZE
from sequoiadb.protocol.opcodes import OpCode, REPLY_MASK
from sequoiadb.protocol.reply import MsgOpReply


def test_reply_empty_roundtrip():
    reply = MsgOpReply(
        header=MsgHeader.new_request(OpCode.QUERY_REQ | REPLY_MASK, 1),
        context_id=-1,
        flags=0,
        num_returned=0,
        docs=[],
    )
    data = reply.encode()
    header = MsgHeader.decode(data)
    payload = data[HEADER_SIZE:]
    decoded = MsgOpReply.decode(header, payload)
    assert decoded.flags == 0
    assert decoded.num_returned == 0
    assert decoded.docs == []


def test_reply_with_docs_roundtrip():
    docs = [{"a": 1}, {"b": "hello"}]
    reply = MsgOpReply(
        header=MsgHeader.new_request(OpCode.QUERY_REQ | REPLY_MASK, 1),
        context_id=-1,
        flags=0,
        num_returned=2,
        docs=docs,
    )
    data = reply.encode()
    header = MsgHeader.decode(data)
    payload = data[HEADER_SIZE:]
    decoded = MsgOpReply.decode(header, payload)
    assert decoded.num_returned == 2
    assert len(decoded.docs) == 2
    assert decoded.docs[0]["a"] == 1
    assert decoded.docs[1]["b"] == "hello"


def test_error_reply():
    reply = MsgOpReply(
        header=MsgHeader.new_request(OpCode.INSERT_REQ | REPLY_MASK, 42),
        context_id=-1,
        flags=-38,  # DuplicateKey
        num_returned=0,
        docs=[],
    )
    data = reply.encode()
    header = MsgHeader.decode(data)
    payload = data[HEADER_SIZE:]
    decoded = MsgOpReply.decode(header, payload)
    assert decoded.flags < 0
    assert decoded.docs == []
