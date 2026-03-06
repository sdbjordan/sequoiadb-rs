"""MsgOpReply — server reply message."""

from __future__ import annotations

import struct
from dataclasses import dataclass, field

import bson

from . import codec
from .header import MsgHeader, HEADER_SIZE


REPLY_FIXED_SIZE = 28  # 8+4+4+4+4+4


@dataclass
class MsgOpReply:
    header: MsgHeader
    context_id: int = -1
    flags: int = 0
    start_from: int = 0
    num_returned: int = 0
    return_mask: int = 0
    data_len: int = 0
    docs: list[dict] = field(default_factory=list)

    @staticmethod
    def decode(header: MsgHeader, payload: bytes) -> MsgOpReply:
        if len(payload) < REPLY_FIXED_SIZE:
            raise ValueError(f"Reply payload too short: {len(payload)} < {REPLY_FIXED_SIZE}")
        off = 0
        context_id, off = codec.read_i64(payload, off)
        flags, off = codec.read_i32(payload, off)
        start_from, off = codec.read_i32(payload, off)
        num_returned, off = codec.read_i32(payload, off)
        return_mask, off = codec.read_i32(payload, off)
        data_len, off = codec.read_i32(payload, off)

        docs = []
        data_end = off + data_len
        while off < data_end and off < len(payload):
            doc_len = struct.unpack_from("<i", payload, off)[0]
            doc = bson.BSON(payload[off:off + doc_len]).decode()
            docs.append(doc)
            off += doc_len

        return MsgOpReply(
            header=header,
            context_id=context_id,
            flags=flags,
            start_from=start_from,
            num_returned=num_returned,
            return_mask=return_mask,
            data_len=data_len,
            docs=docs,
        )

    def encode(self) -> bytes:
        buf = bytearray()
        self.header.encode(buf)

        codec.write_i64(buf, self.context_id)
        codec.write_i32(buf, self.flags)
        codec.write_i32(buf, self.start_from)
        codec.write_i32(buf, self.num_returned)
        codec.write_i32(buf, self.return_mask)

        doc_buf = bytearray()
        for doc in self.docs:
            doc_buf.extend(bson.BSON.encode(doc))
        codec.write_i32(buf, len(doc_buf))
        buf.extend(doc_buf)

        struct.pack_into("<i", buf, 0, len(buf))
        return bytes(buf)
