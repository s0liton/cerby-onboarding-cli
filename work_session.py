"""Persist work session state (rotations + role changes) as JSON under ``work_sessions/``.

Older session files may omit ``rotation_events`` / ``role_change_events``; list counts fall
back to ``rotated_account_ids`` / ``role_changed_account_ids``.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_schema(data: dict[str, Any]) -> None:
    data.setdefault("rotated_account_ids", [])
    data.setdefault("role_changed_account_ids", [])
    data.setdefault("rotation_events", [])
    data.setdefault("role_change_events", [])


@dataclass(frozen=True)
class SessionListEntry:
    path: Path
    session_id: str
    label: str
    updated_at: str
    rotated_count: int
    role_changed_count: int


class WorkSessionTracker:
    """Tracks which accounts have been rotated and which had roles changed in this session."""

    def __init__(self, path: Path, data: dict[str, Any]):
        self.path = path
        self.data = data

    def display_label(self) -> str:
        lab = (self.data.get("label") or "").strip()
        return lab if lab else "—"

    def rotated_ids(self) -> set[str]:
        return {str(x) for x in (self.data.get("rotated_account_ids") or [])}

    def role_changed_ids(self) -> set[str]:
        return {str(x) for x in (self.data.get("role_changed_account_ids") or [])}

    def is_persisted(self) -> bool:
        """True once the session file has been written (first successful rotate or role change)."""
        return self.path.is_file()

    @classmethod
    def begin_new(
        cls,
        workspace: str,
        app_name: str,
        label: str = "",
        session_id: str | None = None,
    ) -> WorkSessionTracker:
        sid = session_id or uuid.uuid4().hex[:12]
        now = _iso_now()
        data: dict[str, Any] = {
            "session_id": sid,
            "label": label.strip(),
            "workspace": workspace,
            "app_name": app_name,
            "created_at": now,
            "updated_at": now,
            "rotated_account_ids": [],
            "role_changed_account_ids": [],
            "rotation_events": [],
            "role_change_events": [],
        }
        return cls(path=Path("work_sessions") / f"{sid}.json", data=data)

    @classmethod
    def load(cls, path: Path | str) -> WorkSessionTracker:
        p = Path(path)
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("session file must be a JSON object")
        _ensure_schema(raw)
        return cls(path=p, data=raw)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data["updated_at"] = _iso_now()
        self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")

    def mark_rotated(self, account_id: str, *, account_label: str = "") -> None:
        aid = str(account_id)
        ids: list[str] = self.data.setdefault("rotated_account_ids", [])
        if aid not in ids:
            ids.append(aid)
        events: list[dict[str, Any]] = self.data.setdefault("rotation_events", [])
        events.append(
            {
                "at": _iso_now(),
                "account_id": aid,
                "account_label": account_label or "",
            }
        )
        self.save()

    def mark_role_changed(
        self,
        account_id: str,
        *,
        account_label: str = "",
        target_role: str,
        users: list[dict[str, Any]],
    ) -> None:
        """Record a successful bulk role change for ``account_id`` with per-user before/after."""
        aid = str(account_id)
        ids: list[str] = self.data.setdefault("role_changed_account_ids", [])
        if aid not in ids:
            ids.append(aid)
        events: list[dict[str, Any]] = self.data.setdefault("role_change_events", [])
        events.append(
            {
                "at": _iso_now(),
                "account_id": aid,
                "account_label": account_label or "",
                "target_role": target_role,
                "users": users,
            }
        )
        self.save()

    def has_rotated(self, account_id: str) -> bool:
        return str(account_id) in self.rotated_ids()

    def has_role_changed(self, account_id: str) -> bool:
        return str(account_id) in self.role_changed_ids()


def load_session_for_workspace_app(session_id: str, workspace: str, app_name: str) -> WorkSessionTracker:
    """Load ``work_sessions/<id>.json`` and verify workspace + app."""
    sid = str(session_id).strip()
    if not sid:
        raise ValueError("session id is empty")
    path = Path("work_sessions") / (sid if sid.endswith(".json") else f"{sid}.json")
    if not path.is_file():
        raise ValueError(f"Work session file not found: {path}")
    t = WorkSessionTracker.load(path)
    if (t.data.get("workspace") or "") != workspace or (t.data.get("app_name") or "") != app_name:
        raise ValueError(
            f"Session {t.data.get('session_id')} does not match this workspace ({workspace!r}) "
            f"and app ({app_name!r})."
        )
    return t


def list_matching_sessions(workspace: str, app_name: str) -> list[SessionListEntry]:
    """Return session summaries for the given workspace + app, newest first."""
    root = Path("work_sessions")
    if not root.is_dir():
        return []
    rows: list[SessionListEntry] = []
    for p in sorted(root.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        if data.get("workspace") != workspace or data.get("app_name") != app_name:
            continue
        _ensure_schema(data)
        rot_ids = data.get("rotated_account_ids") or []
        role_ids = data.get("role_changed_account_ids") or []
        rot_ev = data.get("rotation_events") or []
        role_ev = data.get("role_change_events") or []
        rows.append(
            SessionListEntry(
                path=p,
                session_id=str(data.get("session_id") or p.stem),
                label=str(data.get("label") or ""),
                updated_at=str(data.get("updated_at") or ""),
                rotated_count=len(rot_ev) if rot_ev else len(rot_ids),
                role_changed_count=len(role_ev) if role_ev else len(role_ids),
            )
        )
    return rows
