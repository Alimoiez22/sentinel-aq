"""Shared test helpers: an in-process HTTP server that plays S3 or the OpenAQ API."""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from sentinel.log import configure_logging


@pytest.fixture(autouse=True)
def _fresh_logging() -> None:
    """structlog's configuration is global; some tests point it at a temporary stream.
    Reset it before every test so no test logs into another test's closed stream."""
    configure_logging("INFO")


# A handler gets (path_with_query, headers) and returns (status, headers, body).
Handler = Callable[[str, dict[str, str]], tuple[int, dict[str, str], bytes]]


@dataclass
class FakeServer:
    """Records every request; `handler` decides each response."""

    handler: Handler
    requests: list[tuple[str, dict[str, str]]] = field(default_factory=list)
    url: str = ""


@pytest.fixture
def fake_server() -> Iterator[Callable[[Handler], FakeServer]]:
    servers: list[ThreadingHTTPServer] = []

    def start(handler: Handler) -> FakeServer:
        fake = FakeServer(handler)

        class _H(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                headers = {k.lower(): v for k, v in self.headers.items()}
                fake.requests.append((self.path, headers))
                status, out_headers, body = fake.handler(self.path, headers)
                self.send_response(status)
                for k, v in out_headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: object) -> None:  # keep test output quiet
                return

        srv = ThreadingHTTPServer(("127.0.0.1", 0), _H)
        servers.append(srv)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        fake.url = f"http://127.0.0.1:{srv.server_address[1]}"
        return fake

    yield start
    for srv in servers:
        srv.shutdown()
        srv.server_close()
