"""Unit tests for protocol/messages.py — 10 tests."""

from sequoiadb.protocol.header import MsgHeader, HEADER_SIZE
from sequoiadb.protocol.opcodes import OpCode
from sequoiadb.protocol.messages import (
    MsgOpQuery, MsgOpInsert, MsgOpUpdate, MsgOpDelete,
    MsgOpGetMore, MsgOpKillContexts,
)


def test_query_encode_decode_roundtrip():
    q = MsgOpQuery.new(1, "mycs.mycl", condition={"x": 1}, skip=10, limit=100)
    data = q.encode()
    header = MsgHeader.decode(data)
    assert header.opcode == OpCode.QUERY_REQ
    assert header.msg_len == len(data)
    payload = data[HEADER_SIZE:]
    decoded = MsgOpQuery.decode(header, payload)
    assert decoded.name == "mycs.mycl"
    assert decoded.num_to_skip == 10
    assert decoded.num_to_return == 100
    assert decoded.condition is not None
    assert decoded.selector is None


def test_query_command_roundtrip():
    q = MsgOpQuery.new(2, "$create collectionspace", condition={"Name": "mycs"})
    data = q.encode()
    header = MsgHeader.decode(data)
    payload = data[HEADER_SIZE:]
    decoded = MsgOpQuery.decode(header, payload)
    assert decoded.name == "$create collectionspace"


def test_insert_single_doc_roundtrip():
    msg = MsgOpInsert.new(1, "cs.cl", [{"a": 42}])
    data = msg.encode()
    header = MsgHeader.decode(data)
    payload = data[HEADER_SIZE:]
    decoded = MsgOpInsert.decode(header, payload)
    assert decoded.name == "cs.cl"
    assert len(decoded.docs) == 1
    assert decoded.docs[0]["a"] == 42


def test_insert_multi_doc_roundtrip():
    docs = [{"x": 1}, {"x": 2}, {"x": 3}]
    msg = MsgOpInsert.new(1, "cs.cl", docs)
    data = msg.encode()
    header = MsgHeader.decode(data)
    payload = data[HEADER_SIZE:]
    decoded = MsgOpInsert.decode(header, payload)
    assert len(decoded.docs) == 3


def test_update_roundtrip():
    msg = MsgOpUpdate.new(1, "cs.cl", {"x": 1}, {"$set": {"x": 2}})
    data = msg.encode()
    header = MsgHeader.decode(data)
    payload = data[HEADER_SIZE:]
    decoded = MsgOpUpdate.decode(header, payload)
    assert decoded.name == "cs.cl"
    assert decoded.condition["x"] == 1


def test_update_with_hint():
    msg = MsgOpUpdate.new(1, "cs.cl", {"x": 1}, {"$set": {"x": 2}}, hint={"": "idx"})
    data = msg.encode()
    header = MsgHeader.decode(data)
    payload = data[HEADER_SIZE:]
    decoded = MsgOpUpdate.decode(header, payload)
    assert decoded.hint is not None


def test_delete_roundtrip():
    msg = MsgOpDelete.new(1, "cs.cl", {"x": 1})
    data = msg.encode()
    header = MsgHeader.decode(data)
    payload = data[HEADER_SIZE:]
    decoded = MsgOpDelete.decode(header, payload)
    assert decoded.name == "cs.cl"
    assert decoded.condition["x"] == 1


def test_delete_with_hint():
    msg = MsgOpDelete.new(1, "cs.cl", {"x": 1}, hint={"": "idx"})
    data = msg.encode()
    header = MsgHeader.decode(data)
    payload = data[HEADER_SIZE:]
    decoded = MsgOpDelete.decode(header, payload)
    assert decoded.hint is not None


def test_getmore_roundtrip():
    msg = MsgOpGetMore(
        header=MsgHeader.new_request(OpCode.GET_MORE_REQ, 1),
        context_id=42,
        num_to_return=100,
    )
    data = msg.encode()
    header = MsgHeader.decode(data)
    payload = data[HEADER_SIZE:]
    decoded = MsgOpGetMore.decode(header, payload)
    assert decoded.context_id == 42
    assert decoded.num_to_return == 100


def test_kill_context_roundtrip():
    msg = MsgOpKillContexts(
        header=MsgHeader.new_request(OpCode.KILL_CONTEXT_REQ, 1),
        context_ids=[10, 20, 30],
    )
    data = msg.encode()
    header = MsgHeader.decode(data)
    payload = data[HEADER_SIZE:]
    decoded = MsgOpKillContexts.decode(header, payload)
    assert decoded.context_ids == [10, 20, 30]
