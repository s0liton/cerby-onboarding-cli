"""File logging for all run modes; sensitive values are redacted before write."""

from __future__ import annotations

import logging
import re
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

_REDACT_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"(?i)(authorization\s*:\s*)bearer\s+\S+", re.MULTILINE), r"\1Bearer <redacted>"),
    (re.compile(r'(?i)"access_token"\s*:\s*"[^"]*"'), '"access_token": "<redacted>"'),
    (re.compile(r'(?i)"accessToken"\s*:\s*"[^"]*"'), '"accessToken": "<redacted>"'),
    (re.compile(r'(?i)"password"\s*:\s*"[^"]*"'), '"password": "<redacted>"'),
    (re.compile(r'(?i)"refreshToken"\s*:\s*"[^"]*"'), '"refreshToken": "<redacted>"'),
    (re.compile(r'(?i)"refresh_token"\s*:\s*"[^"]*"'), '"refresh_token": "<redacted>"'),
    (re.compile(r'(?i)"token"\s*:\s*"ey[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*"'), '"token": "<redacted>"'),
    (re.compile(r"\bey[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b"), "<redacted-jwt>"),
)

from cerby_onboarding.paths import DEFAULT_LOG_FILE


def redact_sensitive(text: str) -> str:
    out = text
    for pattern, repl in _REDACT_PATTERNS:
        out = pattern.sub(repl, out)
    return out


class RunLogger:
    """Structured run log to a rotating file; optional mirror to stderr for services."""

    def __init__(
        self,
        log_path: Path | str,
        *,
        mirror_stderr: bool = False,
        max_bytes: int = 5_000_000,
        backup_count: int = 5,
    ) -> None:
        self.path = Path(log_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._logger = logging.getLogger(f"cerby_onboarding.{self.path.resolve()}")
        self._logger.setLevel(logging.DEBUG)
        self._logger.propagate = False
        self._logger.handlers.clear()

        handler = RotatingFileHandler(
            self.path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        handler.setFormatter(
            logging.Formatter("%(asctime)sZ %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
        )
        self._logger.addHandler(handler)

        if mirror_stderr:
            stream = logging.StreamHandler(sys.stderr)
            stream.setFormatter(
                logging.Formatter(
                    "%(asctime)sZ %(levelname)s %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S",
                )
            )
            self._logger.addHandler(stream)

    def _emit(self, level: int, message: str, **fields: Any) -> None:
        msg = redact_sensitive(message.strip())
        if fields:
            extras = ", ".join(f"{k}={v!r}" for k, v in fields.items() if v is not None)
            if extras:
                msg = f"{msg} | {extras}"
        self._logger.log(level, msg)

    def debug(self, message: str, **fields: Any) -> None:
        self._emit(logging.DEBUG, message, **fields)

    def info(self, message: str, **fields: Any) -> None:
        self._emit(logging.INFO, message, **fields)

    def warning(self, message: str, **fields: Any) -> None:
        self._emit(logging.WARNING, message, **fields)

    def error(self, message: str, **fields: Any) -> None:
        self._emit(logging.ERROR, message, **fields)

    def http_verbose(self, message: str) -> None:
        self.debug(message)

    @staticmethod
    def utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()
