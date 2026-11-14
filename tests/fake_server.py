"""Deterministic local HTTP behaviour double for the incremental protocol.

The server is intentionally tiny and stateful: tests describe the pages they
want by cursor and can inject transport failures, duplicate pages, cursor
cycles, invalid bodies and delays.  No test ever touches a real host.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit


@dataclass
class PageBehaviour:
    records: list[Any] = field(default_factory=list)
    next_cursor: str | None = None
    # Optional absolute/relative link.  When omitted the server advertises the
    # canonical ``?cursor=<next_cursor>`` link, which is what real providers do.
    next_url: str | None = None
    status: int = 200
    raw_body: bytes | None = None


@dataclass
class RequestRecord:
    path: str
    query: str
    headers: dict[str, str]


class FakeIncrementalServer:
    """Cursor-addressed page store served by ``ThreadingHTTPServer``."""

    def __init__(self, pages: dict[str | None, PageBehaviour], *, path: str = "/records"):
        self.pages = dict(pages)
        self.path = path
        self.requests: list[RequestRecord] = []
        self._lock = threading.Lock()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    # -- lifecycle -------------------------------------------------------

    def __enter__(self) -> "FakeIncrementalServer":
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._server.shutdown()
        self._thread.join(timeout=5)
        self._server.server_close()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}{self.path}"

    # -- inspection ------------------------------------------------------

    def cursors_requested(self) -> list[str | None]:
        result: list[str | None] = []
        for request in self.requests:
            values = parse_qs(request.query, keep_blank_values=True).get("cursor")
            result.append(values[0] if values else None)
        return result

    def set_page(self, cursor: str | None, page: PageBehaviour) -> None:
        with self._lock:
            self.pages[cursor] = page

    # -- handler ---------------------------------------------------------

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:  # noqa: N802 - http.server API
                parts = urlsplit(self.path)
                cursor_values = parse_qs(parts.query, keep_blank_values=True).get("cursor")
                cursor = cursor_values[0] if cursor_values else None
                with outer._lock:
                    outer.requests.append(RequestRecord(
                        path=parts.path,
                        query=parts.query,
                        headers={key: value for key, value in self.headers.items()},
                    ))
                    page = outer.pages.get(cursor)

                if parts.path != outer.path or page is None:
                    self._send(404, b'{"error":"no such cursor"}')
                    return
                if page.status != 200:
                    self._send(page.status, b'{"error":"injected failure"}')
                    return

                body = page.raw_body
                if body is None:
                    payload: dict[str, Any] = {"records": page.records}
                    if page.next_cursor is not None:
                        payload["next_cursor"] = page.next_cursor
                    if page.next_url is not None:
                        payload["next_url"] = page.next_url
                    body = json.dumps(payload).encode("utf-8")
                self._send(200, body)

            def _send(self, status: int, body: bytes) -> None:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args: object) -> None:
                return

        return Handler


def observation(station: str, observed_at: str, *, value: float = 1.0,
                source: str = "station-feed") -> dict[str, Any]:
    """A valid raw record in the normalizer's input contract."""
    return {
        "source": source,
        "observed_at": observed_at,
        "payload": {
            "station_id": station,
            "metric": "temperature",
            "source_observed_at": observed_at,
            "value": value,
            "unit": "C",
            "quality": "ok",
        },
    }
