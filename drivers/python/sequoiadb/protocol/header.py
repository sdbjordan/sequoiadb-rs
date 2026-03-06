"""52-byte MsgHeader for SequoiaDB wire protocol."""

from __future__ import annotations

from dataclasses import dataclass, field

from . import codec

HEADER_SIZE = 52
EYE_DEFAULT = 0x0000EEEE


@dataclass
class MsgHeader:
    msg_len: int = HEADER_SIZE
    eye: int = EYE_DEFAULT
    tid: int = 0
    route_id: int = 0
    request_id: int = 0
    opcode: int = 0
    version: int = 1
    flags: int = 0
    query_id: bytes = field(default_factory=lambda: b"\x00" * 8)
    query_op_id: int = 0
    reserve: bytes = field(default_factory=lambda: b"\x00" * 4)

    SIZE: int = field(default=HEADER_SIZE, init=False, repr=False)

    @staticmethod
    def new_request(opcode: int, request_id: int) -> MsgHeader:
        return MsgHeader(
            msg_len=HEADER_SIZE,
            eye=EYE_DEFAULT,
            opcode=opcode,
            request_id=request_id,
            version=1,
            flags=0,
        )

    def encode(self, buf: bytearray) -> None:
        codec.write_i32(buf, self.msg_len)
        codec.write_i32(buf, self.eye)
        codec.write_u32(buf, self.tid)
        codec.write_u64(buf, self.route_id)
        codec.write_u64(buf, self.request_id)
        codec.write_i32(buf, self.opcode)
        codec.write_i16(buf, self.version)
        codec.write_i16(buf, self.flags)
        buf.extend(self.query_id[:8].ljust(8, b"\x00"))
        codec.write_u32(buf, self.query_op_id)
        buf.extend(self.reserve[:4].ljust(4, b"\x00"))

    @staticmethod
    def decode(buf: bytes) -> MsgHeader:
        if len(buf) < HEADER_SIZE:
            raise ValueError(f"Header too short: {len(buf)} < {HEADER_SIZE}")
        off = 0
        msg_len, off = codec.read_i32(buf, off)
        eye, off = codec.read_i32(buf, off)
        tid, off = codec.read_u32(buf, off)
        route_id, off = codec.read_u64(buf, off)
        request_id, off = codec.read_u64(buf, off)
        opcode, off = codec.read_i32(buf, off)
        version, off = codec.read_i16(buf, off)
        flags, off = codec.read_i16(buf, off)
        query_id, off = codec.read_bytes(buf, off, 8)
        query_op_id, off = codec.read_u32(buf, off)
        reserve, off = codec.read_bytes(buf, off, 4)
        return MsgHeader(
            msg_len=msg_len,
            eye=eye,
            tid=tid,
            route_id=route_id,
            request_id=request_id,
            opcode=opcode,
            version=version,
            flags=flags,
            query_id=query_id,
            query_op_id=query_op_id,
            reserve=reserve,
        )
