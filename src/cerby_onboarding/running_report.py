"""Live JSON report for long-running service mode (updated while the process runs)."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cerby_onboarding.session_report import _summarize_run, work_session_display_name

from cerby_onboarding.paths import DEFAULT_RUNNING_REPORT
_MAX_RECENT = 200


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunningReportWriter:
    """Atomically writes a cumulative service report admins can inspect anytime."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict[str, Any] = {}
        self._stopped = False

    def start(
        self,
        *,
        workspace: str,
        app_name: str,
        account_role: str,
        session_tracker_data: dict[str, Any],
        actions: str,
        poll_interval: str,
        role_exclude_user_ids: list[str],
    ) -> None:
        sid = str(session_tracker_data.get("session_id") or "")
        self._data = {
            "report_kind": "service_running",
            "status": "running",
            "service_started_at": _iso_now(),
            "last_updated_at": _iso_now(),
            "workspace": workspace,
            "app_name": app_name,
            "account_role": account_role,
            "work_session_id": sid,
            "work_session_display_name": work_session_display_name(session_tracker_data),
            "actions": actions,
            "poll_interval": poll_interval,
            "role_exclude_user_ids": list(role_exclude_user_ids),
            "summary": {
                "baseline_account_count": 0,
                "last_poll_at": None,
                "last_delta_sync_at": None,
                "last_delta_sync_account_count": 0,
                "total_rotations_success": 0,
                "total_role_changes_success": 0,
                "total_accounts_affected": 0,
            },
            "recent_events": [],
            "recent_rotations": [],
            "recent_role_changes": [],
        }
        self.flush()

    def stop(self, *, reason: str = "stopped") -> None:
        if not self._data or self._stopped:
            return
        self._stopped = True
        self._data["status"] = "stopped"
        self._data["stopped_at"] = _iso_now()
        self._data["stop_reason"] = reason
        self._data["last_updated_at"] = _iso_now()
        self.flush()

    def record_event(self, message: str, *, level: str = "info", **fields: Any) -> None:
        if not self._data:
            return
        ev: dict[str, Any] = {"at": _iso_now(), "level": level, "message": message}
        ev.update({k: v for k, v in fields.items() if v is not None})
        events: list[dict[str, Any]] = self._data.setdefault("recent_events", [])
        events.append(ev)
        if len(events) > _MAX_RECENT:
            del events[: len(events) - _MAX_RECENT]
        self._data["last_updated_at"] = _iso_now()
        self.flush()

    def set_baseline(self, account_count: int) -> None:
        if not self._data:
            return
        summary = self._data.setdefault("summary", {})
        summary["baseline_account_count"] = account_count
        self.record_event("Baseline snapshot taken", account_count=account_count)

    def record_delta_sync(self, account_count: int, cutoff: str) -> None:
        if not self._data:
            return
        summary = self._data.setdefault("summary", {})
        summary["last_delta_sync_at"] = _iso_now()
        summary["last_delta_sync_account_count"] = account_count
        self.record_event(
            "Delta sync completed",
            account_count=account_count,
            cutoff=cutoff,
        )

    def record_poll(self, *, new_account_count: int) -> None:
        if not self._data:
            return
        summary = self._data.setdefault("summary", {})
        summary["last_poll_at"] = _iso_now()
        if new_account_count:
            self.record_event("Poll found new accounts", new_account_count=new_account_count)

    def append_action_results(
        self,
        *,
        phase: str,
        rotations: list[dict[str, Any]],
        role_changes: list[dict[str, Any]],
    ) -> None:
        if not self._data:
            return
        self._append_rows("recent_rotations", rotations, phase=phase)
        self._append_rows("recent_role_changes", role_changes, phase=phase)
        run_summary = _summarize_run(rotations, role_changes)
        summary = self._data.setdefault("summary", {})
        summary["total_rotations_success"] = int(summary.get("total_rotations_success") or 0) + int(
            run_summary.get("total_rotations") or 0
        )
        summary["total_role_changes_success"] = int(
            summary.get("total_role_changes_success") or 0
        ) + int(run_summary.get("total_role_changes") or 0)
        affected = set()
        for key in ("recent_rotations", "recent_role_changes"):
            for row in self._data.get(key) or []:
                aid = row.get("account_id")
                if aid and row.get("status") == "success":
                    affected.add(str(aid))
        summary["total_accounts_affected"] = len(affected)
        self._data["last_updated_at"] = _iso_now()
        self.flush()

    def _append_rows(
        self,
        key: str,
        rows: list[dict[str, Any]],
        *,
        phase: str,
    ) -> None:
        bucket: list[dict[str, Any]] = self._data.setdefault(key, [])
        for row in rows:
            entry = dict(row)
            entry["phase"] = phase
            bucket.append(entry)
        if len(bucket) > _MAX_RECENT:
            del bucket[: len(bucket) - _MAX_RECENT]

    def flush(self) -> None:
        if not self._data:
            return
        self._data["last_updated_at"] = _iso_now()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            dir=self.path.parent,
            prefix=f".{self.path.name}.",
            suffix=".tmp",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)
                f.write("\n")
            os.replace(tmp, self.path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
