"""SdbClient — main entry point for the SequoiaDB Python driver."""

from __future__ import annotations

import socket
import struct
import threading
from typing import Optional

from .constants import HEADER_SIZE
from .error import SdbError, check_reply
from .protocol.header import MsgHeader
from .protocol.opcodes import OpCode
from .protocol.messages import MsgOpQuery
from .protocol.reply import MsgOpReply
from .collection_space import CollectionSpace
from .collection import Collection


class SdbClient:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 11810,
        username: Optional[str] = None,
        password: Optional[str] = None,
        max_pool_size: int = 10,
        connect_timeout: float = 5.0,
    ):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._max_pool_size = max_pool_size
        self._connect_timeout = connect_timeout

        self._lock = threading.Lock()
        self._pool: list[_Connection] = []
        self._pool_size = 0
        self._txn_conn: Optional[_Connection] = None
        self._connected = False

        # Validate connectivity
        conn = self._create_connection()
        self._release(conn)
        self._connected = True

    def _create_connection(self) -> _Connection:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self._connect_timeout)
        sock.connect((self._host, self._port))
        conn = _Connection(sock)

        if self._username and self._password:
            rid = conn.next_id()
            cond = {"User": self._username, "Passwd": self._password}
            msg = MsgOpQuery.new(rid, "$authenticate", condition=cond)
            reply = conn.send_recv(msg.encode())
            check_reply(reply)

        return conn

    def _acquire(self) -> _Connection:
        with self._lock:
            while self._pool:
                conn = self._pool.pop()
                if conn.is_alive():
                    return conn
                self._pool_size -= 1

        if self._pool_size >= self._max_pool_size:
            raise SdbError(-17, "Connection pool exhausted")

        conn = self._create_connection()
        with self._lock:
            self._pool_size += 1
        return conn

    def _release(self, conn: _Connection) -> None:
        with self._lock:
            self._pool.append(conn)

    def _send_recv(self, build_msg) -> MsgOpReply:
        if self._txn_conn is not None:
            conn = self._txn_conn
            rid = conn.next_id()
            data = build_msg(rid)
            return conn.send_recv(data)
        else:
            conn = self._acquire()
            try:
                rid = conn.next_id()
                data = build_msg(rid)
                reply = conn.send_recv(data)
                return reply
            finally:
                self._release(conn)

    def is_connected(self) -> bool:
        return self._connected

    def disconnect(self) -> None:
        try:
            conn = self._acquire()
            rid = conn.next_id()
            header = MsgHeader.new_request(OpCode.DISCONNECT, rid)
            buf = bytearray()
            header.encode(buf)
            struct.pack_into("<i", buf, 0, len(buf))
            try:
                conn._sock.sendall(buf)
            except Exception:
                pass
        except Exception:
            pass
        self._connected = False

    def get_collection(self, cs_name: str, cl_name: str) -> Collection:
        full_name = f"{cs_name}.{cl_name}"
        return Collection(self, full_name)

    def get_collection_space(self, name: str) -> CollectionSpace:
        return CollectionSpace(self, name)

    # ── DDL ────────────────────────────────────────────────────────────

    def create_collection_space(self, name: str) -> None:
        def build(rid):
            return MsgOpQuery.new(
                rid, "$create collectionspace", condition={"Name": name}
            ).encode()
        check_reply(self._send_recv(build))

    def drop_collection_space(self, name: str) -> None:
        def build(rid):
            return MsgOpQuery.new(
                rid, "$drop collectionspace", condition={"Name": name}
            ).encode()
        check_reply(self._send_recv(build))

    def create_collection(self, cs: str, cl: str) -> None:
        full_name = f"{cs}.{cl}"
        def build(rid):
            return MsgOpQuery.new(
                rid, "$create collection", condition={"Name": full_name}
            ).encode()
        check_reply(self._send_recv(build))

    def drop_collection(self, cs: str, cl: str) -> None:
        full_name = f"{cs}.{cl}"
        def build(rid):
            return MsgOpQuery.new(
                rid, "$drop collection", condition={"Name": full_name}
            ).encode()
        check_reply(self._send_recv(build))

    def create_index(
        self, cs: str, cl: str, idx_name: str, key: dict, unique: bool = False
    ) -> None:
        full_name = f"{cs}.{cl}"
        def build(rid):
            cond = {
                "Collection": full_name,
                "Index": {"name": idx_name, "key": key, "unique": unique},
            }
            return MsgOpQuery.new(rid, "$create index", condition=cond).encode()
        check_reply(self._send_recv(build))

    def drop_index(self, cs: str, cl: str, idx_name: str) -> None:
        full_name = f"{cs}.{cl}"
        def build(rid):
            cond = {"Collection": full_name, "Index": idx_name}
            return MsgOpQuery.new(rid, "$drop index", condition=cond).encode()
        check_reply(self._send_recv(build))

    # ── SQL ────────────────────────────────────────────────────────────

    def exec_sql(self, sql: str) -> list[dict]:
        def build(rid):
            return MsgOpQuery.new(rid, "$sql", condition={"SQL": sql}).encode()
        reply = self._send_recv(build)
        check_reply(reply)
        return reply.docs

    # ── Auth ───────────────────────────────────────────────────────────

    def authenticate(self, username: str, password: str) -> None:
        def build(rid):
            cond = {"User": username, "Passwd": password}
            return MsgOpQuery.new(rid, "$authenticate", condition=cond).encode()
        check_reply(self._send_recv(build))

    def create_user(self, username: str, password: str, roles: list[str]) -> None:
        def build(rid):
            cond = {"User": username, "Passwd": password, "Roles": roles}
            return MsgOpQuery.new(rid, "$create user", condition=cond).encode()
        check_reply(self._send_recv(build))

    def drop_user(self, username: str) -> None:
        def build(rid):
            return MsgOpQuery.new(rid, "$drop user", condition={"User": username}).encode()
        check_reply(self._send_recv(build))

    # ── Transactions ───────────────────────────────────────────────────

    def transaction_begin(self) -> None:
        conn = self._acquire()
        # Remove from pool tracking since we're holding it
        with self._lock:
            self._pool_size -= 1

        rid = conn.next_id()
        header = MsgHeader.new_request(OpCode.TRANS_BEGIN_REQ, rid)
        buf = bytearray()
        header.encode(buf)
        struct.pack_into("<i", buf, 0, len(buf))
        reply = conn.send_recv(bytes(buf))
        check_reply(reply)
        self._txn_conn = conn

    def transaction_commit(self) -> None:
        conn = self._txn_conn
        if conn is None:
            raise SdbError(-253, "No active transaction")
        self._txn_conn = None

        rid = conn.next_id()
        header = MsgHeader.new_request(OpCode.TRANS_COMMIT_REQ, rid)
        buf = bytearray()
        header.encode(buf)
        struct.pack_into("<i", buf, 0, len(buf))
        reply = conn.send_recv(bytes(buf))
        # Return connection to pool
        with self._lock:
            self._pool_size += 1
        self._release(conn)
        check_reply(reply)

    def transaction_rollback(self) -> None:
        conn = self._txn_conn
        if conn is None:
            raise SdbError(-253, "No active transaction")
        self._txn_conn = None

        rid = conn.next_id()
        header = MsgHeader.new_request(OpCode.TRANS_ROLLBACK_REQ, rid)
        buf = bytearray()
        header.encode(buf)
        struct.pack_into("<i", buf, 0, len(buf))
        reply = conn.send_recv(bytes(buf))
        with self._lock:
            self._pool_size += 1
        self._release(conn)
        check_reply(reply)


class _Connection:
    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._next_request_id = 1

    def next_id(self) -> int:
        rid = self._next_request_id
        self._next_request_id += 1
        return rid

    def send_recv(self, data: bytes) -> MsgOpReply:
        self._sock.sendall(data)
        return self._recv_reply()

    def _recv_reply(self) -> MsgOpReply:
        len_buf = self._recv_exact(4)
        msg_len = struct.unpack("<i", len_buf)[0]
        if msg_len < HEADER_SIZE:
            raise SdbError(-6, f"Invalid reply length: {msg_len}")
        remaining = self._recv_exact(msg_len - 4)
        full_buf = len_buf + remaining
        header = MsgHeader.decode(full_buf)
        payload = full_buf[HEADER_SIZE:]
        return MsgOpReply.decode(header, payload)

    def _recv_exact(self, n: int) -> bytes:
        data = bytearray()
        while len(data) < n:
            chunk = self._sock.recv(n - len(data))
            if not chunk:
                raise SdbError(-16, "Connection closed")
            data.extend(chunk)
        return bytes(data)

    def is_alive(self) -> bool:
        try:
            self._sock.setblocking(False)
            try:
                data = self._sock.recv(1, socket.MSG_PEEK)
                return len(data) > 0
            except BlockingIOError:
                return True  # no data available = healthy idle socket
            except Exception:
                return False
            finally:
                self._sock.setblocking(True)
        except Exception:
            return False
