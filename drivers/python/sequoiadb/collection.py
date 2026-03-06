"""Collection handle — CRUD, count, aggregate."""

from __future__ import annotations

import struct
from typing import Optional, TYPE_CHECKING

from .error import check_reply
from .protocol.messages import MsgOpQuery, MsgOpInsert, MsgOpUpdate, MsgOpDelete
from .cursor import Cursor

if TYPE_CHECKING:
    from .client import SdbClient


class Collection:
    def __init__(self, client: SdbClient, full_name: str):
        self._client = client
        self.full_name = full_name

    def insert(self, doc: dict) -> None:
        self.insert_many([doc])

    def insert_many(self, docs: list[dict]) -> None:
        full_name = self.full_name
        def build(rid):
            return MsgOpInsert.new(rid, full_name, docs, 0).encode()
        check_reply(self._client._send_recv(build))

    def insert_batch(self, docs: list[dict]) -> None:
        self.insert_many(docs)

    def query(self, condition: Optional[dict] = None) -> Cursor:
        full_name = self.full_name
        def build(rid):
            return MsgOpQuery.new(rid, full_name, condition=condition).encode()
        reply = self._client._send_recv(build)
        check_reply(reply)
        return Cursor(
            docs=reply.docs,
            context_id=reply.context_id,
            client=self._client,
        )

    def update(self, condition: dict, modifier: dict) -> None:
        full_name = self.full_name
        def build(rid):
            return MsgOpUpdate.new(rid, full_name, condition, modifier).encode()
        check_reply(self._client._send_recv(build))

    def delete(self, condition: dict) -> None:
        full_name = self.full_name
        def build(rid):
            return MsgOpDelete.new(rid, full_name, condition).encode()
        check_reply(self._client._send_recv(build))

    def count(self, condition: Optional[dict] = None) -> int:
        full_name = self.full_name
        def build(rid):
            cond = {"Collection": full_name}
            if condition is not None:
                cond["Condition"] = condition
            return MsgOpQuery.new(rid, "$count", condition=cond).encode()
        reply = self._client._send_recv(build)
        check_reply(reply)
        if reply.docs:
            doc = reply.docs[0]
            c = doc.get("count", 0)
            return int(c)
        return 0

    def aggregate(self, pipeline: list[dict]) -> list[dict]:
        full_name = self.full_name
        def build(rid):
            cond = {"Collection": full_name, "Pipeline": pipeline}
            return MsgOpQuery.new(rid, "$aggregate", condition=cond).encode()
        reply = self._client._send_recv(build)
        check_reply(reply)
        return reply.docs
