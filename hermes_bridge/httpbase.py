"""Small routing layer over http.server, shared by the API and the adapter."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlsplit

log = logging.getLogger("hermes_bridge.http")

MAX_BODY_BYTES = 1 << 20


class HttpError(Exception):
    def __init__(self, status: int, code: str, message: str = ""):
        super().__init__(message or code)
        self.status = status
        self.code = code
        self.message = message or code


@dataclass
class Request:
    method: str
    path: str
    query: dict[str, str]
    headers: Any
    body: bytes
    params: tuple[str, ...]
    handler: "BaseHTTPRequestHandler"

    def json(self) -> dict:
        try:
            value = json.loads(self.body or b"{}")
        except ValueError:
            raise HttpError(400, "invalid_json", "request body is not valid JSON")
        if not isinstance(value, dict):
            raise HttpError(400, "invalid_json", "request body must be a JSON object")
        return value


class Response:
    def __init__(self, status: int = 200, payload: Any = None, headers: Optional[dict] = None):
        self.status = status
        self.payload = payload
        self.headers = headers or {}


Route = tuple[str, re.Pattern, Callable[[Request], Optional[Response]]]


def route(method: str, pattern: str, handler: Callable) -> Route:
    regex = re.sub(r"\{\w+\}", r"([^/]+)", pattern)
    return method, re.compile(f"^{regex}$"), handler


def make_server(host: str, port: int, routes: list[Route]) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "hermes-bridge"
        sys_version = ""
        timeout = 300

        def log_message(self, fmt: str, *args: Any) -> None:
            log.debug("%s %s", self.address_string(), fmt % args)

        def _send(self, response: Response) -> None:
            body = b"" if response.payload is None else json.dumps(response.payload, ensure_ascii=False).encode()
            self.send_response(response.status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            for key, value in response.headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)

        def _dispatch(self) -> None:
            parts = urlsplit(self.path)
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                length = -1
            if length < 0 or length > MAX_BODY_BYTES:
                self.close_connection = True
                self._send(Response(413, {"error": {"code": "payload_too_large", "message": "request body too large"}}))
                return
            body = self.rfile.read(length) if length else b""
            matched_path = False
            for method, regex, handler in routes:
                match = regex.match(parts.path)
                if not match:
                    continue
                matched_path = True
                if method != self.command:
                    continue
                query = {k: v[-1] for k, v in parse_qs(parts.query).items()}
                request = Request(self.command, parts.path, query, self.headers, body, match.groups(), self)
                try:
                    response = handler(request)
                except HttpError as exc:
                    response = Response(exc.status, {"error": {"code": exc.code, "message": exc.message}})
                except (BrokenPipeError, ConnectionResetError):
                    self.close_connection = True
                    return
                except Exception:
                    log.exception("unhandled error on %s %s", self.command, parts.path)
                    response = Response(500, {"error": {"code": "internal_error", "message": "internal error"}})
                if response is not None:  # None: the handler streamed its own response.
                    self._send(response)
                return
            status, code = (405, "method_not_allowed") if matched_path else (404, "not_found")
            self._send(Response(status, {"error": {"code": code, "message": code}}))

        do_GET = _dispatch
        do_POST = _dispatch
        do_PUT = _dispatch
        do_DELETE = _dispatch
        do_PATCH = _dispatch

    class Server(ThreadingHTTPServer):
        daemon_threads = True
        request_queue_size = 128

        def handle_error(self, request: Any, client_address: Any) -> None:
            log.debug("connection error from %s", client_address, exc_info=True)

    return Server((host, port), Handler)
