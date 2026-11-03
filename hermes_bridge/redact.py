"""Scrub credentials from anything that is persisted as an event or summary."""

from __future__ import annotations

import os
import re
from typing import Any, Iterable

_PATTERNS = [
    re.compile(r"sk-[A-Za-z0-9_\-]{16,}"),
    re.compile(r"gh[pousr]_[A-Za-z0-9]{20,}"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"xox[baprs]-[A-Za-z0-9\-]{10,}"),
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._\-]{8,}"),
    re.compile(
        r"(?i)((?:api[_-]?key|token|secret|password|passwd)[\"']?\s*[:=]\s*[\"']?)[^\s\"',;]{4,}"
    ),
]
_SECRET_ENV = re.compile(r"(?i)(token|secret|password|api_?key|credential)")


class Redactor:
    def __init__(self, extra_secrets: Iterable[str] = ()):
        env_secrets = [v for k, v in os.environ.items() if _SECRET_ENV.search(k) and len(v) >= 8]
        secrets = {s for s in [*env_secrets, *extra_secrets] if s}
        # Longest first so a secret containing another is replaced whole.
        self._literals = sorted(secrets, key=len, reverse=True)

    def text(self, value: str) -> str:
        for literal in self._literals:
            value = value.replace(literal, "***")
        for pattern in _PATTERNS:
            value = pattern.sub(lambda m: (m.group(1) if m.groups() else "") + "***", value)
        return value

    def value(self, value: Any) -> Any:
        if isinstance(value, str):
            return self.text(value)
        if isinstance(value, dict):
            return {k: self.value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self.value(v) for v in value]
        return value
