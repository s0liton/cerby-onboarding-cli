"""Project paths: persistent state in ``assets/``, logs in ``log/``, reports in project root."""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_LOG_FILE = Path("log/cerby-onboarding.log")
DEFAULT_RUNNING_REPORT = Path("running_report.json")

_PROJECT_ROOT: Path | None = None


def project_root_from_config(config_path: Path | str) -> Path:
    """Infer install root from a service config path (e.g. ``<root>/config/config.yaml``)."""
    path = Path(config_path).expanduser().resolve()
    if path.parent.name == "config":
        return path.parent.parent
    return path.parent


def set_project_root(path: Path | str) -> Path:
    """Pin runtime paths to ``path`` instead of the process working directory."""
    global _PROJECT_ROOT
    root = Path(path).expanduser().resolve()
    _PROJECT_ROOT = root
    return root


def clear_project_root() -> None:
    """Reset project root override (mainly for tests)."""
    global _PROJECT_ROOT
    _PROJECT_ROOT = None


def data_dir() -> Path:
    """Project root for runtime output (pinned root, ``CERBY_DATA_DIR``, or cwd)."""
    if _PROJECT_ROOT is not None:
        return _PROJECT_ROOT
    raw = os.environ.get("CERBY_DATA_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return Path.cwd().resolve()


def assets_dir() -> Path:
    """Directory for ``.cerby_session.json`` and ``work_sessions/`` (never nested twice)."""
    raw = os.environ.get("CERBY_ASSETS_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (data_dir() / "assets").resolve()


def session_file() -> Path:
    return (assets_dir() / ".cerby_session.json").resolve()


def work_sessions_dir() -> Path:
    return (assets_dir() / "work_sessions").resolve()


def ensure_assets_dir() -> Path:
    d = assets_dir()
    d.mkdir(parents=True, exist_ok=True)
    work_sessions_dir().mkdir(parents=True, exist_ok=True)
    return d


def ensure_log_dir() -> Path:
    d = (data_dir() / "log").resolve()
    d.mkdir(parents=True, exist_ok=True)
    return d
