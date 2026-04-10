"""Build JSON exports for a single run or an entire work session."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from cerby_client import cerby_role_to_display_role


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def work_session_display_name(session_data: dict[str, Any]) -> str:
    """Human-facing session name: label when set, otherwise session id."""
    label = (session_data.get("label") or "").strip()
    sid = str(session_data.get("session_id") or "").strip()
    return label if label else sid


def _summarize_run(
    rotations: list[dict[str, Any]],
    role_changes: list[dict[str, Any]],
) -> dict[str, int]:
    accounts: set[str] = set()
    for r in rotations:
        aid = r.get("account_id")
        if aid is not None and str(aid):
            accounts.add(str(aid))
    for r in role_changes:
        aid = r.get("account_id")
        if aid is not None and str(aid):
            accounts.add(str(aid))
    return {
        "total_accounts_affected": len(accounts),
        "total_rotations": sum(1 for r in rotations if r.get("status") == "success"),
        "total_role_changes": sum(1 for r in role_changes if r.get("status") == "success"),
    }


def _summarize_session(session_data: dict[str, Any]) -> dict[str, int]:
    rot_ids = set(str(x) for x in (session_data.get("rotated_account_ids") or []) if x)
    role_ids = set(str(x) for x in (session_data.get("role_changed_account_ids") or []) if x)
    rot_ev = session_data.get("rotation_events") or []
    role_ev = session_data.get("role_change_events") or []
    total_rot = len(rot_ev) if rot_ev else len(rot_ids)
    total_role = len(role_ev) if role_ev else len(role_ids)
    return {
        "total_accounts_affected": len(rot_ids | role_ids),
        "total_rotations": total_rot,
        "total_role_changes": total_role,
    }


def _normalize_user_for_export(u: dict[str, Any]) -> dict[str, Any]:
    nu = {k: v for k, v in u.items() if k not in ("previous_role_api", "new_role_api")}
    if "previous_role" not in nu and "previous_role_api" in u:
        nu["previous_role"] = cerby_role_to_display_role(str(u["previous_role_api"]))
    if "new_role" not in nu and "new_role_api" in u:
        nu["new_role"] = cerby_role_to_display_role(str(u["new_role_api"]))
    return nu


def _normalize_role_change_event_for_export(ev: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in ev.items():
        if k in ("target_role_cli", "target_role_api", "users"):
            continue
        out[k] = v
    if "target_role" not in out:
        if "target_role_cli" in ev:
            out["target_role"] = str(ev["target_role_cli"])
        elif "target_role_api" in ev:
            out["target_role"] = cerby_role_to_display_role(str(ev["target_role_api"]))
    out["users"] = [
        _normalize_user_for_export(dict(x))
        for x in (ev.get("users") or [])
        if isinstance(x, dict)
    ]
    return out


def _normalize_role_change_row_for_export(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize a single-run ``role_changes`` entry (includes optional ``users_preview``)."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        if k in ("target_role_cli", "target_role_api", "users", "users_preview"):
            continue
        out[k] = v
    if "target_role" not in out:
        if "target_role_cli" in row:
            out["target_role"] = str(row["target_role_cli"])
        elif "target_role_api" in row:
            out["target_role"] = cerby_role_to_display_role(str(row["target_role_api"]))
    if "users" in row:
        out["users"] = [
            _normalize_user_for_export(dict(x))
            for x in (row.get("users") or [])
            if isinstance(x, dict)
        ]
    if "users_preview" in row:
        out["users_preview"] = [
            _normalize_user_for_export(dict(x))
            for x in (row.get("users_preview") or [])
            if isinstance(x, dict)
        ]
    return out


def _normalize_session_for_export(session_data: dict[str, Any]) -> dict[str, Any]:
    s = dict(session_data)
    evs = s.get("role_change_events")
    if isinstance(evs, list):
        s["role_change_events"] = [
            _normalize_role_change_event_for_export(dict(x))
            for x in evs
            if isinstance(x, dict)
        ]
    return s


def build_this_run_export(
    *,
    workspace: str,
    app_name: str,
    session_id: str | None,
    work_session_display_name: str,
    run_started_at: str,
    rotations: list[dict[str, Any]],
    role_changes: list[dict[str, Any]],
) -> dict[str, Any]:
    role_changes_out = [
        _normalize_role_change_row_for_export(dict(x)) for x in role_changes if isinstance(x, dict)
    ]
    return {
        "export_kind": "single_run",
        "generated_at": _iso_now(),
        "workspace": workspace,
        "app_name": app_name,
        "work_session_id": session_id,
        "work_session_display_name": work_session_display_name,
        "summary": _summarize_run(rotations, role_changes),
        "run_started_at": run_started_at,
        "rotations": rotations,
        "role_changes": role_changes_out,
    }


def build_full_session_export(session_data: dict[str, Any]) -> dict[str, Any]:
    """``session_data`` is the tracker ``data`` dict (same shape as the JSON under ``work_sessions/``)."""
    session_norm = _normalize_session_for_export(session_data)
    return {
        "export_kind": "full_work_session",
        "generated_at": _iso_now(),
        "work_session_display_name": work_session_display_name(session_data),
        "summary": _summarize_session(session_norm),
        "session": session_norm,
    }


def write_report_json(path: str, payload: dict[str, Any]) -> None:
    import json
    from pathlib import Path

    p = Path(path)
    p.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
