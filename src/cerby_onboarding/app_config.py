"""Load service configuration from config.yaml for systemd / unattended runs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from cerby_onboarding.paths import DEFAULT_LOG_FILE, DEFAULT_RUNNING_REPORT, data_dir

VALID_ACTIONS = frozenset({"rotate", "role", "both"})
VALID_ROLES = frozenset({"OWNER", "COLLABORATOR"})
BOOTSTRAP_TOKEN_KEYS = ("access_token", "initial_access_token")


@dataclass(frozen=True)
class ServiceConfig:
    workspace: str
    app_name: str
    account_role: str
    session_id: str
    actions: str
    poll_interval: str
    role_exclude_user_ids: frozenset[str]
    log_file: Path
    running_report: Path
    experimental_keep_browser_for_token: bool
    verbose_http: bool
    config_path: Path

    def run_cfg(self) -> dict[str, str]:
        return {
            "CERBY_WORKSPACE": self.workspace,
            "APP_NAME": self.app_name,
            "ACCOUNT_ROLE": self.account_role,
        }


def _require_str(raw: dict[str, Any], key: str) -> str:
    val = raw.get(key)
    if not isinstance(val, str) or not val.strip():
        raise ValueError(f"config: {key!r} is required and must be a non-empty string")
    return val.strip()


def _resolve_path(raw: str | None, default: Path) -> Path:
    base = data_dir()
    if raw is None or not str(raw).strip():
        return (base / default).resolve()
    p = Path(str(raw).strip())
    if p.is_absolute():
        return p
    return (base / p).resolve()


def load_service_config(config_path: Path | str) -> ServiceConfig:
    path = Path(config_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {path}")
    raw_any: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw_any, dict):
        raise ValueError("config must be a YAML mapping at the top level")
    raw: dict[str, Any] = raw_any

    workspace = _require_str(raw, "workspace")
    app_name = _require_str(raw, "app_name")
    session_id = _require_str(raw, "session_id")

    role_raw = str(raw.get("account_role") or "COLLABORATOR").strip().upper()
    if role_raw not in VALID_ROLES:
        raise ValueError(f"config: account_role must be one of {sorted(VALID_ROLES)}")

    actions = str(raw.get("actions") or "rotate").strip().lower()
    if actions not in VALID_ACTIONS:
        raise ValueError(f"config: actions must be one of {sorted(VALID_ACTIONS)}")

    poll_interval = str(raw.get("poll_interval") or "30s").strip()
    if not poll_interval:
        raise ValueError("config: poll_interval must not be empty")

    exclude_raw = raw.get("role_exclude_user_ids") or []
    if not isinstance(exclude_raw, list):
        raise ValueError("config: role_exclude_user_ids must be a list of strings")
    exclude_ids = frozenset(str(x).strip() for x in exclude_raw if str(x).strip())

    log_file = _resolve_path(
        raw.get("log_file") if isinstance(raw.get("log_file"), str) else None,
        DEFAULT_LOG_FILE,
    )
    running_report = _resolve_path(
        raw.get("running_report") if isinstance(raw.get("running_report"), str) else None,
        DEFAULT_RUNNING_REPORT,
    )

    keep_browser = bool(raw.get("experimental_keep_browser_for_token", True))
    verbose_http = bool(raw.get("verbose_http", False))

    return ServiceConfig(
        workspace=workspace,
        app_name=app_name,
        account_role=role_raw,
        session_id=session_id,
        actions=actions,
        poll_interval=poll_interval,
        role_exclude_user_ids=exclude_ids,
        log_file=log_file,
        running_report=running_report,
        experimental_keep_browser_for_token=keep_browser,
        verbose_http=verbose_http,
        config_path=path,
    )


def consume_bootstrap_token(config_path: Path | str) -> str | None:
    """Load a one-time ``access_token`` from config, remove it from the file, return the token."""
    path = Path(config_path).resolve()
    if not path.is_file():
        return None
    raw_any: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw_any, dict):
        return None

    consumed_key: str | None = None
    token: str | None = None
    for key in BOOTSTRAP_TOKEN_KEYS:
        val = raw_any.get(key)
        if isinstance(val, str) and val.strip():
            token = val.strip()
            consumed_key = key
            break

    if not token or not consumed_key:
        return None

    del raw_any[consumed_key]
    path.write_text(
        yaml.dump(raw_any, default_flow_style=False, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    try:
        path.chmod(0o600)
    except OSError:
        pass
    return token
