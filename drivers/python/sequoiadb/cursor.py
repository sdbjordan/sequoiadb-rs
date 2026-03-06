"""Cursor — iterates query results with GetMore + KillContext support."""

from __future__ import annotations

import struct
from typing import Optional, TYPE_CHECKING

from .error import check_reply
from .protocol.header import MsgHeader
from .protocol.opcodes import OpCode
from .protocol.messages import MsgOpGetMore, MsgOpKillContexts

if TYPE_CHECKING:
    from .client import SdbClient


class Cursor:
    def __init__(
        self,
        docs: list[dict],
        context_id: int = -1,
        client: Optional[SdbClient] = None,
    ):
        self._buffer = docs
        self._pos = 0
        self._closed = False
        self._context_id = context_id
        self._client = client

    def next(self) -> Optional[dict]:
        if self._closed or self._pos >= len(self._buffer):
            return None
        doc = self._buffer[self._pos]
        self._pos += 1
        return doc

    def has_more(self) -> bool:
        return not self._closed and self._context_id != -1

    def fetch_more(self) -> bool:
        if self._closed or self._context_id == -1 or self._client is None:
            return False

        context_id = self._context_id

        def build(rid):
            msg = MsgOpGetMore(
                header=MsgHeader.new_request(OpCode.GET_MORE_REQ, rid),
                context_id=context_id,
                num_to_return=-1,
            )
            return msg.encode()

        reply = self._client._send_recv(build)
        check_reply(reply)

        self._context_id = reply.context_id
        self._buffer = reply.docs
        self._pos = 0

        if not self._buffer:
            self._closed = True
            return False
        return True

    def collect_all(self) -> list[dict]:
        result = []
        while True:
            while True:
                doc = self.next()
                if doc is None:
                    break
                result.append(doc)
            if not self.fetch_more():
                break
        return result

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        if self._context_id != -1 and self._client is not None:
            context_id = self._context_id

            def build(rid):
                msg = MsgOpKillContexts(
                    header=MsgHeader.new_request(OpCode.KILL_CONTEXT_REQ, rid),
                    context_ids=[context_id],
                )
                return msg.encode()

            try:
                self._client._send_recv(build)
            except Exception:
                pass

            self._context_id = -1
