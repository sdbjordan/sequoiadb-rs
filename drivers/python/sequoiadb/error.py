"""SdbError exception and reply checker."""

from __future__ import annotations

from . import constants


_CODE_TO_NAME = {
    0: "OK",
    -1: "SYS",
    -2: "OOM",
    -6: "INVALID_ARG",
    -7: "PERMISSION_DENIED",
    -9: "EOF",
    -15: "NETWORK_ERROR",
    -16: "NETWORK_CLOSE",
    -17: "TIMEOUT",
    -22: "COLLECTION_ALREADY_EXISTS",
    -23: "COLLECTION_NOT_FOUND",
    -29: "NOT_FOUND",
    -31: "QUERY_NOT_FOUND",
    -33: "CS_ALREADY_EXISTS",
    -34: "CS_NOT_FOUND",
    -38: "DUPLICATE_KEY",
    -46: "INDEX_ALREADY_EXISTS",
    -47: "INDEX_NOT_FOUND",
    -179: "AUTH_FAILED",
    -253: "TRANSACTION_ERROR",
}


class SdbError(Exception):
    def __init__(self, code: int, message: str = ""):
        self.code = code
        name = _CODE_TO_NAME.get(code, "UNKNOWN")
        if message:
            super().__init__(f"[{code}] {name}: {message}")
        else:
            super().__init__(f"[{code}] {name}")
        self.name = name


def check_reply(reply) -> None:
    """Raise SdbError if reply.flags indicates a server error."""
    if reply.flags != 0:
        raise SdbError(reply.flags)
