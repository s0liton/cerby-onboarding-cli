"""Project paths: persistent state in ``assets/``, logs in ``log/``, reports in project root."""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_LOG_FILE = Path("log/cerby-onboarding.log")
DEFAULT_RUNNING_REPORT = Path("running_report.json")


def data_dir() -> Path:
    """Project root for runtime output (cwd, or ``CERBY_DATA_DIR``)."""
    raw = os.environ.get("CERBY_DATA_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return Path.cwd()


def assets_dir() -> Path:
    """Important persistent files: session token and work sessions."""
    raw = os.environ.get("CERBY_ASSETS_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (data_dir() / "assets").resolve()


def session_file() -> Path:
    return assets_dir() / ".cerby_session.json"


def work_sessions_dir() -> Path:
    return assets_dir() / "work_sessions"


def ensure_assets_dir() -> Path:
    d = assets_dir()
    d.mkdir(parents=True, exist_ok=True)
    return d


def ensure_log_dir() -> Path:
    d = data_dir() / "log"
    d.mkdir(parents=True, exist_ok=True)
    return d
