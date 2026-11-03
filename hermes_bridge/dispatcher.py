"""Delivers outbox notifications to the configured receiver: at-least-once, with backoff."""

from __future__ import annotations

import hashlib
import hmac
import logging
import random
import threading
import time
import urllib.error
import urllib.request
from typing import Optional

from .config import Config
from .store import Store

log = logging.getLogger("hermes_bridge.dispatcher")

SIGNATURE_HEADER = "X-Bridge-Signature"
TIMESTAMP_HEADER = "X-Bridge-Timestamp"
REPLAY_WINDOW_SECONDS = 300
_REQUEST_TIMEOUT = 10


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


# The target is fixed by server config: no redirects and no environment proxy, so a
# response cannot steer a signed request somewhere else.
_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect)


def sign(secret: str, timestamp: str, body: bytes) -> str:
    return hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()


def verify(secret: str, timestamp: str, signature: str, body: bytes, now: Optional[float] = None) -> bool:
    try:
        age = abs((now if now is not None else time.time()) - int(timestamp))
    except (TypeError, ValueError):
        return False
    if age > REPLAY_WINDOW_SECONDS:
        return False
    return hmac.compare_digest(sign(secret, timestamp, body).encode(), signature.encode())


def backoff_delay(attempts: int, base: float, maximum: float) -> float:
    delay = min(maximum, base * (2 ** max(0, attempts - 1)))
    return delay * random.uniform(0.8, 1.2)


class Dispatcher(threading.Thread):
    def __init__(self, config: Config, store: Store):
        super().__init__(name="outbox-dispatcher", daemon=True)
        self.config = config
        self.store = store
        self._stop_requested = threading.Event()

    def stop(self) -> None:
        self._stop_requested.set()
        if self.is_alive():
            self.join(timeout=_REQUEST_TIMEOUT + 1)

    def run(self) -> None:
        if not (self.config.notify_url and self.config.notify_secret):
            log.warning("notify.url or notify.secret is not configured: notifications stay pending")
            return
        while not self._stop_requested.is_set():
            try:
                rows = self.store.claim_due_outbox(limit=20)
                for row in rows:
                    self._deliver(row)
            except Exception:
                log.exception("dispatcher pass failed")
                rows = []
            if not rows:
                self._stop_requested.wait(0.5)

    def _deliver(self, row: dict) -> None:
        body = row["payload"].encode()
        timestamp = str(int(time.time()))
        request = urllib.request.Request(
            self.config.notify_url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                TIMESTAMP_HEADER: timestamp,
                SIGNATURE_HEADER: sign(self.config.notify_secret, timestamp, body),
            },
        )
        try:
            with _opener.open(request, timeout=_REQUEST_TIMEOUT) as response:
                if 200 <= response.status < 300:
                    self.store.outbox_delivered(row["event_id"])
                    log.info("delivered %s for %s", row["event_id"], row["task_id"])
                    return
                error = f"HTTP {response.status}"
        except urllib.error.HTTPError as exc:
            error = f"HTTP {exc.code}"
        except (OSError, ValueError) as exc:
            error = str(exc)[:300]

        attempts = row["attempts"] + 1
        if attempts >= self.config.notify_max_attempts:
            log.error("giving up on %s after %d attempts: %s", row["event_id"], attempts, error)
            self.store.outbox_failed(row["event_id"], error, None)
        else:
            delay = backoff_delay(attempts, self.config.notify_base_backoff, self.config.notify_max_backoff)
            log.warning("delivery of %s failed (%s), retrying in %.1fs", row["event_id"], error, delay)
            self.store.outbox_failed(row["event_id"], error, time.time() + delay)
