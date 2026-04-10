"""Persist only the Cerby API access token (never passwords)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TypedDict

SESSION_PATH = Path(__file__).resolve().parent / ".cerby_session.json"


class SessionData(TypedDict):
    workspace: str
    access_token: str


def load_session() -> SessionData | None:
    if not SESSION_PATH.exists():
        return None
    try:
        raw: Any = json.loads(SESSION_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return None
        ws = raw.get("workspace")
        tok = raw.get("access_token")
        if isinstance(ws, str) and ws.strip() and isinstance(tok, str) and tok.strip():
            return SessionData(workspace=ws.strip(), access_token=tok.strip())
    except (OSError, json.JSONDecodeError):
        pass
    return None


def save_session(workspace: str, access_token: str) -> None:
    data = {"workspace": workspace.strip(), "access_token": access_token.strip()}
    SESSION_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
    try:
        SESSION_PATH.chmod(0o600)
    except OSError:
        pass


def clear_session() -> None:
    try:
        SESSION_PATH.unlink(missing_ok=True)
    except OSError:
        pass
