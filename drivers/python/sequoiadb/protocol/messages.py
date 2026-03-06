"""Wire-format request messages for SequoiaDB protocol."""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import Optional

import bson

from . import codec
from .header import MsgHeader, HEADER_SIZE
from .opcodes import OpCode


def _encode_bson(doc: dict) -> bytes:
    return bson.BSON.encode(doc)


def _decode_bson(data: bytes, offset: int) -> tuple[dict, int]:
    if offset >= len(data):
        return {}, offset
    doc_len = struct.unpack_from("<i", data, offset)[0]
    doc = bson.BSON(data[offset:offset + doc_len]).decode()
    return doc, offset + doc_len


def _empty_bson() -> bytes:
    return _encode_bson({})


def _encode_optional_doc(buf: bytearray, doc: Optional[dict]) -> None:
    if doc is not None:
        buf.extend(_encode_bson(doc))
    else:
        buf.extend(_empty_bson())


# ── MsgOpQuery ─────────────────────────────────────────────────────────

@dataclass
class MsgOpQuery:
    header: MsgHeader
    version: int = 1
    w: int = 1
    flags: int = 0
    name_length: int = 0
    num_to_skip: int = 0
    num_to_return: int = -1
    name: str = ""
    condition: Optional[dict] = None
    selector: Optional[dict] = None
    order_by: Optional[dict] = None
    hint: Optional[dict] = None

    @staticmethod
    def new(
        request_id: int,
        collection: str,
        condition: Optional[dict] = None,
        selector: Optional[dict] = None,
        order_by: Optional[dict] = None,
        hint: Optional[dict] = None,
        skip: int = 0,
        limit: int = -1,
        flags: int = 0,
    ) -> MsgOpQuery:
        name_length = len(collection.encode("utf-8")) + 1
        return MsgOpQuery(
            header=MsgHeader.new_request(OpCode.QUERY_REQ, request_id),
            version=1,
            w=1,
            flags=flags,
            name_length=name_length,
            num_to_skip=skip,
            num_to_return=limit,
            name=collection,
            condition=condition,
            selector=selector,
            order_by=order_by,
            hint=hint,
        )

    def encode(self) -> bytes:
        buf = bytearray()
        self.header.encode(buf)

        codec.write_i32(buf, self.version)
        codec.write_i16(buf, self.w)
        codec.write_i16(buf, 0)  # padding
        codec.write_i32(buf, self.flags)
        codec.write_i32(buf, self.name_length)
        codec.write_i64(buf, self.num_to_skip)
        codec.write_i64(buf, self.num_to_return)
        codec.write_cstring(buf, self.name)
        codec.pad_align4(buf)

        _encode_optional_doc(buf, self.condition)
        _encode_optional_doc(buf, self.selector)
        _encode_optional_doc(buf, self.order_by)
        _encode_optional_doc(buf, self.hint)

        struct.pack_into("<i", buf, 0, len(buf))
        return bytes(buf)

    @staticmethod
    def decode(header: MsgHeader, payload: bytes) -> MsgOpQuery:
        off = 0
        version, off = codec.read_i32(payload, off)
        w, off = codec.read_i16(payload, off)
        _, off = codec.read_i16(payload, off)  # padding
        flags, off = codec.read_i32(payload, off)
        name_length, off = codec.read_i32(payload, off)
        num_to_skip, off = codec.read_i64(payload, off)
        num_to_return, off = codec.read_i64(payload, off)
        name, off = codec.read_cstring(payload, off)
        off = codec.skip_align4(payload, off)

        condition, off = _decode_optional_doc(payload, off)
        selector, off = _decode_optional_doc(payload, off)
        order_by, off = _decode_optional_doc(payload, off)
        hint, off = _decode_optional_doc(payload, off)

        return MsgOpQuery(
            header=header,
            version=version,
            w=w,
            flags=flags,
            name_length=name_length,
            num_to_skip=num_to_skip,
            num_to_return=num_to_return,
            name=name,
            condition=condition,
            selector=selector,
            order_by=order_by,
            hint=hint,
        )


def _decode_optional_doc(payload: bytes, off: int) -> tuple[Optional[dict], int]:
    if off >= len(payload):
        return None, off
    doc, off = _decode_bson(payload, off)
    if not doc:
        return None, off
    return doc, off


# ── MsgOpInsert ────────────────────────────────────────────────────────

@dataclass
class MsgOpInsert:
    header: MsgHeader
    version: int = 1
    w: int = 1
    flags: int = 0
    name_length: int = 0
    name: str = ""
    docs: list = field(default_factory=list)

    @staticmethod
    def new(request_id: int, collection: str, docs: list[dict], flags: int = 0) -> MsgOpInsert:
        name_length = len(collection.encode("utf-8")) + 1
        return MsgOpInsert(
            header=MsgHeader.new_request(OpCode.INSERT_REQ, request_id),
            version=1,
            w=1,
            flags=flags,
            name_length=name_length,
            name=collection,
            docs=docs,
        )

    def encode(self) -> bytes:
        buf = bytearray()
        self.header.encode(buf)

        codec.write_i32(buf, self.version)
        codec.write_i16(buf, self.w)
        codec.write_i16(buf, 0)  # padding
        codec.write_i32(buf, self.flags)
        codec.write_i32(buf, self.name_length)
        codec.write_cstring(buf, self.name)
        codec.pad_align4(buf)

        for doc in self.docs:
            buf.extend(_encode_bson(doc))

        struct.pack_into("<i", buf, 0, len(buf))
        return bytes(buf)

    @staticmethod
    def decode(header: MsgHeader, payload: bytes) -> MsgOpInsert:
        off = 0
        version, off = codec.read_i32(payload, off)
        w, off = codec.read_i16(payload, off)
        _, off = codec.read_i16(payload, off)
        flags, off = codec.read_i32(payload, off)
        name_length, off = codec.read_i32(payload, off)
        name, off = codec.read_cstring(payload, off)
        off = codec.skip_align4(payload, off)

        docs = []
        while off < len(payload):
            doc, off = _decode_bson(payload, off)
            docs.append(doc)

        return MsgOpInsert(
            header=header,
            version=version,
            w=w,
            flags=flags,
            name_length=name_length,
            name=name,
            docs=docs,
        )


# ── MsgOpUpdate ────────────────────────────────────────────────────────

@dataclass
class MsgOpUpdate:
    header: MsgHeader
    version: int = 1
    w: int = 1
    flags: int = 0
    name_length: int = 0
    name: str = ""
    condition: dict = field(default_factory=dict)
    modifier: dict = field(default_factory=dict)
    hint: Optional[dict] = None

    @staticmethod
    def new(
        request_id: int,
        collection: str,
        condition: dict,
        modifier: dict,
        hint: Optional[dict] = None,
        flags: int = 0,
    ) -> MsgOpUpdate:
        name_length = len(collection.encode("utf-8")) + 1
        return MsgOpUpdate(
            header=MsgHeader.new_request(OpCode.UPDATE_REQ, request_id),
            version=1,
            w=1,
            flags=flags,
            name_length=name_length,
            name=collection,
            condition=condition,
            modifier=modifier,
            hint=hint,
        )

    def encode(self) -> bytes:
        buf = bytearray()
        self.header.encode(buf)

        codec.write_i32(buf, self.version)
        codec.write_i16(buf, self.w)
        codec.write_i16(buf, 0)
        codec.write_i32(buf, self.flags)
        codec.write_i32(buf, self.name_length)
        codec.write_cstring(buf, self.name)
        codec.pad_align4(buf)

        buf.extend(_encode_bson(self.condition))
        buf.extend(_encode_bson(self.modifier))
        if self.hint is not None:
            buf.extend(_encode_bson(self.hint))

        struct.pack_into("<i", buf, 0, len(buf))
        return bytes(buf)

    @staticmethod
    def decode(header: MsgHeader, payload: bytes) -> MsgOpUpdate:
        off = 0
        version, off = codec.read_i32(payload, off)
        w, off = codec.read_i16(payload, off)
        _, off = codec.read_i16(payload, off)
        flags, off = codec.read_i32(payload, off)
        name_length, off = codec.read_i32(payload, off)
        name, off = codec.read_cstring(payload, off)
        off = codec.skip_align4(payload, off)

        condition, off = _decode_bson(payload, off)
        modifier, off = _decode_bson(payload, off)
        hint = None
        if off < len(payload):
            hint, off = _decode_bson(payload, off)

        return MsgOpUpdate(
            header=header,
            version=version,
            w=w,
            flags=flags,
            name_length=name_length,
            name=name,
            condition=condition,
            modifier=modifier,
            hint=hint,
        )


# ── MsgOpDelete ────────────────────────────────────────────────────────

@dataclass
class MsgOpDelete:
    header: MsgHeader
    version: int = 1
    w: int = 1
    flags: int = 0
    name_length: int = 0
    name: str = ""
    condition: dict = field(default_factory=dict)
    hint: Optional[dict] = None

    @staticmethod
    def new(
        request_id: int,
        collection: str,
        condition: dict,
        hint: Optional[dict] = None,
        flags: int = 0,
    ) -> MsgOpDelete:
        name_length = len(collection.encode("utf-8")) + 1
        return MsgOpDelete(
            header=MsgHeader.new_request(OpCode.DELETE_REQ, request_id),
            version=1,
            w=1,
            flags=flags,
            name_length=name_length,
            name=collection,
            condition=condition,
            hint=hint,
        )

    def encode(self) -> bytes:
        buf = bytearray()
        self.header.encode(buf)

        codec.write_i32(buf, self.version)
        codec.write_i16(buf, self.w)
        codec.write_i16(buf, 0)
        codec.write_i32(buf, self.flags)
        codec.write_i32(buf, self.name_length)
        codec.write_cstring(buf, self.name)
        codec.pad_align4(buf)

        buf.extend(_encode_bson(self.condition))
        if self.hint is not None:
            buf.extend(_encode_bson(self.hint))

        struct.pack_into("<i", buf, 0, len(buf))
        return bytes(buf)

    @staticmethod
    def decode(header: MsgHeader, payload: bytes) -> MsgOpDelete:
        off = 0
        version, off = codec.read_i32(payload, off)
        w, off = codec.read_i16(payload, off)
        _, off = codec.read_i16(payload, off)
        flags, off = codec.read_i32(payload, off)
        name_length, off = codec.read_i32(payload, off)
        name, off = codec.read_cstring(payload, off)
        off = codec.skip_align4(payload, off)

        condition, off = _decode_bson(payload, off)
        hint = None
        if off < len(payload):
            hint, off = _decode_bson(payload, off)

        return MsgOpDelete(
            header=header,
            version=version,
            w=w,
            flags=flags,
            name_length=name_length,
            name=name,
            condition=condition,
            hint=hint,
        )


# ── MsgOpGetMore ───────────────────────────────────────────────────────

@dataclass
class MsgOpGetMore:
    header: MsgHeader
    context_id: int = 0
    num_to_return: int = -1

    def encode(self) -> bytes:
        buf = bytearray()
        self.header.encode(buf)
        codec.write_i64(buf, self.context_id)
        codec.write_i32(buf, self.num_to_return)

        struct.pack_into("<i", buf, 0, len(buf))
        return bytes(buf)

    @staticmethod
    def decode(header: MsgHeader, payload: bytes) -> MsgOpGetMore:
        off = 0
        context_id, off = codec.read_i64(payload, off)
        num_to_return, off = codec.read_i32(payload, off)
        return MsgOpGetMore(header=header, context_id=context_id, num_to_return=num_to_return)


# ── MsgOpKillContexts ─────────────────────────────────────────────────

@dataclass
class MsgOpKillContexts:
    header: MsgHeader
    context_ids: list[int] = field(default_factory=list)

    def encode(self) -> bytes:
        buf = bytearray()
        self.header.encode(buf)
        codec.write_i32(buf, 0)  # ZERO
        codec.write_i32(buf, len(self.context_ids))
        for cid in self.context_ids:
            codec.write_i64(buf, cid)

        struct.pack_into("<i", buf, 0, len(buf))
        return bytes(buf)

    @staticmethod
    def decode(header: MsgHeader, payload: bytes) -> MsgOpKillContexts:
        off = 0
        _, off = codec.read_i32(payload, off)  # ZERO
        num_contexts, off = codec.read_i32(payload, off)
        context_ids = []
        for _ in range(num_contexts):
            cid, off = codec.read_i64(payload, off)
            context_ids.append(cid)
        return MsgOpKillContexts(header=header, context_ids=context_ids)
