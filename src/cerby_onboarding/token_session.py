"""Persist only the Cerby API access token (never passwords)."""

from __future__ import annotations

import json
from typing import Any, TypedDict

from cerby_onboarding.paths import session_file


class SessionData(TypedDict):
    workspace: str
    access_token: str


def load_session() -> SessionData | None:
    path = session_file()
    if not path.is_file():
        return None
    try:
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return None
        ws = raw.get("workspace")
        tok = raw.get("access_token")
        if isinstance(ws, str) and ws.strip() and isinstance(tok, str) and tok.strip():
            return SessionData(workspace=ws.strip(), access_token=tok.strip())
    except (OSError, json.JSONDecodeError):
        pass
    return None


def last_saved_workspace() -> str | None:
    """Workspace from ``assets/.cerby_session.json``, even if the JWT is expired."""
    data = load_session()
    return data["workspace"] if data else None


def save_session(workspace: str, access_token: str) -> None:
    path = session_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {"workspace": workspace.strip(), "access_token": access_token.strip()}
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass


def clear_session() -> None:
    try:
        session_file().unlink(missing_ok=True)
    except OSError:
        pass
