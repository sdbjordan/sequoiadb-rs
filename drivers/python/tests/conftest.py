"""Pytest fixtures — builds the Rust server and starts it on a random port."""

import os
import socket
import subprocess
import time

import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
SERVER_BINARY = os.path.join(PROJECT_ROOT, "target", "release", "sequoiadb")


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"Server did not start on port {port} within {timeout}s")


@pytest.fixture(scope="session")
def server_port():
    """Build the Rust server (release) and start it on a random port."""
    # Build
    subprocess.run(
        ["cargo", "build", "--release"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
    )

    port = _find_free_port()
    data_dir = os.path.join(PROJECT_ROOT, "target", f"test-data-py-{port}")
    os.makedirs(data_dir, exist_ok=True)

    proc = subprocess.Popen(
        [SERVER_BINARY, "-p", str(port), "--db-path", data_dir],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        _wait_for_port(port)
        yield port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
