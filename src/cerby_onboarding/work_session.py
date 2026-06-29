"""Persist work session state (rotations + role changes) as JSON under ``assets/work_sessions/``."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from cerby_onboarding.cerby_client import parse_provider_specs
from cerby_onboarding.paths import work_sessions_dir


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_datetime(value: str | None) -> Optional[datetime]:
    if not value or not str(value).strip():
        return None
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _ensure_schema(data: dict[str, Any]) -> None:
    data.setdefault("rotated_account_ids", [])
    data.setdefault("role_changed_account_ids", [])
    data.setdefault("rotation_events", [])
    data.setdefault("role_change_events", [])


def normalize_session_name(name: str) -> str:
    n = str(name).strip()
    if not n:
        raise ValueError("session name is required")
    if len(n) > 120:
        raise ValueError("session name must be at most 120 characters")
    return n


def session_name_slug(name: str) -> str:
    n = normalize_session_name(name)
    slug = re.sub(r"[^\w\s-]", "", n.lower())
    slug = re.sub(r"[\s_]+", "-", slug).strip("-")
    if not slug:
        raise ValueError("session name must contain at least one letter or number")
    return slug


def session_name_from_data(data: dict[str, Any], path: Path | None = None) -> str:
    for key in ("session_name", "label"):
        val = (data.get(key) or "").strip() if isinstance(data.get(key), str) else ""
        if val:
            return val
    if path is not None:
        return path.stem
    sid = str(data.get("session_id") or "").strip()
    return sid or "—"


def session_name_taken(name: str) -> bool:
    want = normalize_session_name(name).casefold()
    root = work_sessions_dir()
    if not root.is_dir():
        return False
    for p in root.glob("*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        if session_name_from_data(data, p).casefold() == want:
            return True
    return False


@dataclass(frozen=True)
class SessionListEntry:
    path: Path
    session_name: str
    updated_at: str
    rotated_count: int
    role_changed_count: int
    session_app_name: str = ""


def _provider_set(app_raw: str) -> frozenset[str]:
    return frozenset(s for s in parse_provider_specs(app_raw) if s)


def session_app_matches_current(session_app: str, current_app: str) -> bool:
    saved = _provider_set(session_app)
    current = _provider_set(current_app)
    if not saved and not current:
        return True
    if not saved or not current:
        return True
    return bool(saved & current)


class WorkSessionTracker:
    """Tracks which accounts have been rotated and which had roles changed in this session."""

    def __init__(self, path: Path, data: dict[str, Any]):
        self.path = path
        self.data = data

    @property
    def session_name(self) -> str:
        return session_name_from_data(self.data, self.path)

    def display_label(self) -> str:
        return self.session_name

    def rotated_ids(self) -> set[str]:
        return {str(x) for x in (self.data.get("rotated_account_ids") or [])}

    def role_changed_ids(self) -> set[str]:
        return {str(x) for x in (self.data.get("role_changed_account_ids") or [])}

    def is_persisted(self) -> bool:
        return self.path.is_file()

    @classmethod
    def begin_new(cls, workspace: str, app_name: str, session_name: str) -> WorkSessionTracker:
        name = normalize_session_name(session_name)
        if session_name_taken(name):
            raise ValueError(
                f"Work session name already exists: {name!r}. Choose a different name."
            )
        slug = session_name_slug(name)
        path = (work_sessions_dir() / f"{slug}.json").resolve()
        if path.exists():
            raise ValueError(
                f"Work session file already exists: {path}. Choose a different session name."
            )
        now = _iso_now()
        data: dict[str, Any] = {
            "session_name": name,
            "workspace": workspace,
            "app_name": app_name,
            "created_at": now,
            "updated_at": now,
            "rotated_account_ids": [],
            "role_changed_account_ids": [],
            "rotation_events": [],
            "role_change_events": [],
        }
        return cls(path=path, data=data)

    @classmethod
    def load(cls, path: Path | str) -> WorkSessionTracker:
        p = Path(path).resolve()
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("session file must be a JSON object")
        _ensure_schema(raw)
        if not (raw.get("session_name") or raw.get("label")):
            raw.setdefault("session_name", p.stem)
        return cls(path=p, data=raw)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists() and self.path.stat().st_size > 0:
            existing = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                old_name = session_name_from_data(existing, self.path)
                new_name = session_name_from_data(self.data, self.path)
                if old_name.casefold() != new_name.casefold():
                    raise ValueError("session name cannot be changed after creation")
        self.data["session_name"] = self.session_name
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

    def last_successful_action_at(self) -> Optional[datetime]:
        timestamps: list[datetime] = []
        for key in ("rotation_events", "role_change_events"):
            for ev in self.data.get(key) or []:
                if not isinstance(ev, dict):
                    continue
                dt = _parse_iso_datetime(str(ev.get("at") or ""))
                if dt is not None:
                    timestamps.append(dt)
        if not timestamps:
            return None
        return max(timestamps)


def _verify_session_match(
    tracker: WorkSessionTracker,
    session_name: str,
    workspace: str,
    app_name: str,
) -> None:
    if tracker.session_name.casefold() != normalize_session_name(session_name).casefold():
        raise ValueError(
            f"Work session {tracker.session_name!r} does not match requested name {session_name!r}."
        )
    if (tracker.data.get("workspace") or "") != workspace or not session_app_matches_current(
        str(tracker.data.get("app_name") or ""), app_name
    ):
        raise ValueError(
            f"Work session {tracker.session_name!r} does not match workspace {workspace!r} "
            f"and app filter {app_name!r}."
        )


def load_session_by_name(session_name: str, workspace: str, app_name: str) -> WorkSessionTracker:
    """Load a work session by its unique ``session_name``."""
    name = normalize_session_name(session_name)
    slug_path = (work_sessions_dir() / f"{session_name_slug(name)}.json").resolve()
    if slug_path.is_file():
        tracker = WorkSessionTracker.load(slug_path)
        _verify_session_match(tracker, name, workspace, app_name)
        return tracker

    root = work_sessions_dir()
    if root.is_dir():
        for p in root.glob("*.json"):
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(data, dict):
                continue
            if session_name_from_data(data, p).casefold() != name.casefold():
                continue
            tracker = WorkSessionTracker.load(p)
            _verify_session_match(tracker, name, workspace, app_name)
            return tracker

    raise ValueError(
        f"Work session not found: {name!r} (looked under {work_sessions_dir()})"
    )


def load_or_create_session_by_name(
    session_name: str, workspace: str, app_name: str
) -> tuple[WorkSessionTracker, bool]:
    """Load a work session, creating ``assets/work_sessions/<slug>.json`` if missing.

    Returns ``(tracker, created)`` where ``created`` is true when a new file was written.
    """
    try:
        return load_session_by_name(session_name, workspace, app_name), False
    except ValueError as e:
        if not str(e).startswith("Work session not found:"):
            raise
    name = normalize_session_name(session_name)
    tracker = WorkSessionTracker.begin_new(workspace, app_name, name)
    tracker.save()
    return tracker, True


def load_session_for_workspace_app(session_name: str, workspace: str, app_name: str) -> WorkSessionTracker:
    """Alias for :func:`load_session_by_name`."""
    return load_session_by_name(session_name, workspace, app_name)


def list_matching_sessions(workspace: str, app_name: str) -> list[SessionListEntry]:
    root = work_sessions_dir()
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
        stored_app = str(data.get("app_name") or "")
        if data.get("workspace") != workspace or not session_app_matches_current(
            stored_app, app_name
        ):
            continue
        _ensure_schema(data)
        rot_ids = data.get("rotated_account_ids") or []
        role_ids = data.get("role_changed_account_ids") or []
        rot_ev = data.get("rotation_events") or []
        role_ev = data.get("role_change_events") or []
        rows.append(
            SessionListEntry(
                path=p,
                session_name=session_name_from_data(data, p),
                updated_at=str(data.get("updated_at") or ""),
                rotated_count=len(rot_ev) if rot_ev else len(rot_ids),
                role_changed_count=len(role_ev) if role_ev else len(role_ids),
                session_app_name=stored_app,
            )
        )
    return rows
