from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import requests
import typer
from art import artError, text2art
from rich.align import Align
from rich.console import Console
from rich.prompt import IntPrompt, Prompt
from rich.table import Table
from rich.text import Text

from cerby_onboarding import session_report, token_session, work_session
from cerby_onboarding.app_config import consume_bootstrap_token, load_service_config
from cerby_onboarding.auth_handler import (
    BrowserTokenRefreshSession,
    CerbyAuthHandler,
    access_token_seconds_remaining,
    is_access_token_valid,
)
from cerby_onboarding.cerby_client import (
    TOKEN_PROACTIVE_REFRESH_WITHIN_SECONDS,
    CerbyApi,
    fetch_accounts_merged,
    normalize_provider_filter,
    parse_provider_specs,
    refresh_access_token,
    share_role_for_api,
)
from cerby_onboarding.run_context import ActiveRun
from cerby_onboarding.paths import (
    data_dir,
    ensure_assets_dir,
    ensure_log_dir,
    project_root_from_config,
    set_project_root,
    work_sessions_dir,
)
from cerby_onboarding.run_logging import DEFAULT_LOG_FILE, RunLogger, redact_sensitive
from cerby_onboarding.running_report import RunningReportWriter

app = typer.Typer(
    add_completion=False,
    help="Cerby Onboarding CLI - sync accounts, rotate passwords, and change roles.",
)
console = Console()


def _say(active: ActiveRun | None, message: str, *, style: str = "") -> None:
    if active is not None:
        active.log_info(message)
    if style:
        console.print(message, style=style)
    else:
        console.print(message)


def _verbose_http_log_factory(active: ActiveRun | None) -> Callable[[str], None]:
    def _log(message: str) -> None:
        redacted = redact_sensitive(message)
        if active is not None:
            active.logger.http_verbose(redacted)
        console.print(redacted, style="dim", markup=False, overflow="fold")

    return _log

VALID_ACCOUNT_ROLES = ("OWNER", "COLLABORATOR")


def _provider_filter_label(app_name: str) -> str:
    raw = (app_name or "").strip()
    if not raw:
        return "all integrations (no provider filter)"
    specs = parse_provider_specs(raw)
    if len(specs) == 1 and specs[0] == "":
        return "all integrations (no provider filter)"
    if len(specs) > 1:
        return raw
    return specs[0] if specs[0] else "all integrations (no provider filter)"


def _account_row_id(acc: dict[str, Any]) -> str:
    return str(acc.get("id") or acc.get("accountId") or acc.get("account_id") or "")


def _parse_account_created_at(acc: dict[str, Any]) -> Optional[datetime]:
    raw = acc.get("createdAt") or acc.get("created_at")
    if raw is None or not str(raw).strip():
        return None
    s = str(raw).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _accounts_created_after(
    accounts: list[dict[str, Any]], cutoff: datetime
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for acc in accounts:
        created = _parse_account_created_at(acc)
        if created is not None and created > cutoff:
            out.append(acc)
    return out


def _format_utc_short(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def _probe_provider_for_token(app_name_raw: str) -> str:
    # Token probe API still wants a concrete provider when we can give one.
    return next((s for s in parse_provider_specs(app_name_raw) if s), "")


def _session_touch_ids(tracker: work_session.WorkSessionTracker) -> set[str]:
    return tracker.rotated_ids() | tracker.role_changed_ids()


def _parse_row_numbers(s: str, max_row: int) -> Optional[list[int]]:
    out: list[int] = []
    for part in s.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            n = int(p)
        except ValueError:
            console.print(f"[red]Not an integer: {p!r}[/red]")
            return None
        if n < 1 or n > max_row:
            console.print(f"[red]Row {n} is out of range (valid: 1–{max_row}).[/red]")
            return None
        out.append(n)
    if not out:
        return None
    return sorted(set(out))


def _show_intro_banner() -> None:
    try:
        logo = text2art("Cerby", font="doom").rstrip("\n")
    except artError:
        logo = "Cerby"
    console.print()
    console.print(Align.center(Text(logo, style="blue")))
    console.print(Align.center(Text("Cerby Onboarding CLI", style="bold")))
    console.print()


def _is_403_http_error(exc: BaseException) -> bool:
    return (
        isinstance(exc, requests.HTTPError)
        and exc.response is not None
        and exc.response.status_code == 403
    )


def _print_cerby_forbidden_guidance(active: ActiveRun | None = None) -> None:
    msg = (
        "403 Forbidden — Cerby rejected this operation. Need super administrator with "
        "all access mode, or direct owner access to the accounts."
    )
    if active is not None:
        active.log_error(msg)
    console.print(f"\n[yellow]{msg}[/yellow]\n")


def _prompt_yes_no(message: str, *, default: bool) -> bool:
    # Looser than Rich choices= so people can type "yes" / "NO" / etc.
    default_word = "yes" if default else "no"
    while True:
        raw = Prompt.ask(f"{message} (yes/no)", default=default_word).strip()
        t = raw.lower()
        if t in ("y", "yes", "ye", "true", "1", "t", "ok", "sure"):
            return True
        if t in ("n", "no", "false", "0", "f"):
            return False
        console.print("[red]Please answer yes or no (y/n is fine).[/red]")


def _parse_comma_separated_user_ids(raw: str) -> list[str]:
    return [p.strip() for p in raw.split(",") if p.strip()]


def _prompt_role_change_exclude_user_ids() -> frozenset[str]:
    if not _prompt_yes_no("Exclude user IDs from role changes?", default=False):
        return frozenset()
    entered = Prompt.ask("User IDs (comma-separated)").strip()
    ids = _parse_comma_separated_user_ids(entered)
    if not ids:
        console.print(
            "[yellow]No user IDs parsed; proceeding without exclusions.[/yellow]"
        )
        return frozenset()
    return frozenset(ids)


def _prompt_retry_after_permission_fix(active: ActiveRun | None = None) -> bool:
    if active is not None and not active.interactive:
        return False
    return _prompt_yes_no("Retry after fixing Cerby permissions?", default=True)


def _maybe_account_role(value: Optional[str]) -> Optional[str]:
    if value is None or not str(value).strip():
        return None
    u = str(value).strip().upper()
    if u in VALID_ACCOUNT_ROLES:
        return u
    return None


def _validate_cli_account_role(value: Optional[str]) -> None:
    if value is None or not str(value).strip():
        return
    if _maybe_account_role(value) is None:
        raise typer.BadParameter(
            f"ACCOUNT_ROLE must be one of: {', '.join(VALID_ACCOUNT_ROLES)} (got {value!r})."
        )


def _prompt_run_context(
    workspace: Optional[str],
    app_name: Optional[str],
    account_role: Optional[str],
) -> dict[str, str]:
    def ask_nonempty(label: str, default: str) -> str:
        while True:
            value = Prompt.ask(label, default=default).strip()
            if value:
                return value
            console.print("[red]Required.[/red]")

    saved_ws = token_session.last_saved_workspace()
    cli_env_ws = (workspace or os.environ.get("CERBY_WORKSPACE") or "").strip()

    if cli_env_ws:
        ws = cli_env_ws
    elif saved_ws:
        if _prompt_yes_no(f"Use workspace [cyan]{saved_ws}[/cyan]?", default=True):
            ws = saved_ws
        else:
            ws = ask_nonempty("Workspace (subdomain)", "")
    else:
        ws = ask_nonempty("Workspace (subdomain)", "")

    app_prefill = (app_name or os.environ.get("APP_NAME") or "").strip()
    app_prompt_default = app_prefill if app_prefill else "Any"
    r_cli = _maybe_account_role(account_role)
    r_env = _maybe_account_role(os.environ.get("ACCOUNT_ROLE"))
    r_default = r_cli or r_env or "COLLABORATOR"
    role_default = r_default.lower()

    while True:
        app_raw = Prompt.ask(
            "App provider(s), comma-separated, or Any",
            default=app_prompt_default,
        ).strip()
        if not app_raw:
            console.print("[red]Enter provider name(s) or Any.[/red]")
            continue
        app = app_raw
        break
    role_lc = Prompt.ask(
        "Role for role changes",
        choices=["owner", "collaborator"],
        default=role_default,
        case_sensitive=False,
        show_choices=True,
    )
    return {
        "CERBY_WORKSPACE": ws,
        "APP_NAME": app,
        "ACCOUNT_ROLE": str(role_lc).strip().upper(),
    }


def _render_accounts_preview_table(accounts: list[dict[str, Any]]) -> None:
    # # column matches what we ask for in "pick rows".
    preview = Table(title="Accounts to sync / act on")
    preview.add_column("#", justify="right")
    preview.add_column("id", overflow="fold")
    preview.add_column("name")
    preview.add_column("username")
    preview.add_column("createdAt")
    for i, acc in enumerate(accounts, start=1):
        preview.add_row(
            str(i),
            str(acc.get("id") or "—"),
            str(acc.get("name") or "—"),
            str(acc.get("username") or "—"),
            str(acc.get("createdAt") or "—"),
        )
    console.print(preview)


def _prompt_which_accounts(
    accounts: list[dict[str, Any]],
    session_tracker: work_session.WorkSessionTracker,
) -> Optional[list[dict[str, Any]]]:
    # Either everything not already touched in this session, or explicit row numbers.
    touched = _session_touch_ids(session_tracker)
    eligible = [a for a in accounts if _account_row_id(a) not in touched]
    mode = Prompt.ask(
        "Which accounts?",
        choices=["all_eligible", "pick_rows"],
        default="all_eligible",
    )

    if mode == "all_eligible":
        if eligible:
            console.print(f"[dim]{len(eligible)} of {len(accounts)} eligible.[/dim]\n")
            return eligible
        console.print("[yellow]All accounts already handled in this session.[/yellow]\n")
        if not _prompt_yes_no("Pick rows from the table?", default=False):
            return None

    raw = Prompt.ask(
        "Enter row numbers (# column) separated by commas (e.g. 1,3,5)",
        default="",
    ).strip()
    if not raw:
        console.print("[yellow]No rows entered.[/yellow]\n")
        return None
    picks = _parse_row_numbers(raw, len(accounts))
    if picks is None:
        return None
    return [accounts[i - 1] for i in picks]


def _prompt_new_session_name(session_name: Optional[str]) -> str:
    if session_name is not None and str(session_name).strip():
        try:
            name = work_session.normalize_session_name(str(session_name).strip())
        except ValueError as e:
            raise typer.BadParameter(str(e)) from e
        if work_session.session_name_taken(name):
            raise typer.BadParameter(f"Work session name already exists: {name!r}")
        return name
    while True:
        raw = Prompt.ask("Session name (must be unique)", default="").strip()
        if not raw:
            console.print("[red]Session name is required.[/red]")
            continue
        try:
            name = work_session.normalize_session_name(raw)
        except ValueError as e:
            console.print(f"[red]{e}[/red]")
            continue
        if work_session.session_name_taken(name):
            console.print(f"[red]Session name already in use: {name!r}[/red]")
            continue
        return name


def _begin_new_session(ws: str, app: str, session_name: str) -> work_session.WorkSessionTracker:
    try:
        t = work_session.WorkSessionTracker.begin_new(ws, app, session_name)
    except ValueError as e:
        raise typer.BadParameter(str(e)) from e
    console.print(f"[green]New session[/green] [cyan]{t.session_name}[/cyan]\n")
    return t


def _prompt_work_session_tracker(
    ws: str,
    app: str,
    *,
    session_name: Optional[str],
) -> work_session.WorkSessionTracker:
    if session_name is not None and str(session_name).strip():
        try:
            t = work_session.load_session_by_name(str(session_name).strip(), ws, app)
        except ValueError as e:
            raise typer.BadParameter(str(e)) from e
        console.print(f"[green]Session[/green] [cyan]{t.session_name}[/cyan]\n")
        return t

    candidates = work_session.list_matching_sessions(ws, app)
    if not candidates:
        name = _prompt_new_session_name(None)
        return _begin_new_session(ws, app, name)

    mode = Prompt.ask(
        "Work session",
        choices=["continue", "new"],
        default="continue",
    )
    if mode == "new":
        name = _prompt_new_session_name(None)
        return _begin_new_session(ws, app, name)

    sess_table = Table(title="Saved sessions")
    sess_table.add_column("#", justify="right")
    sess_table.add_column("Name")
    sess_table.add_column("App scope", overflow="fold")
    sess_table.add_column("Last updated")
    sess_table.add_column("Rotated")
    sess_table.add_column("Role-changed")
    for i, entry in enumerate(candidates, start=1):
        scope = entry.session_app_name.strip() if entry.session_app_name.strip() else "—"
        sess_table.add_row(
            str(i),
            entry.session_name,
            scope,
            entry.updated_at[:19] if entry.updated_at else "—",
            str(entry.rotated_count),
            str(entry.role_changed_count),
        )
    console.print(sess_table)
    while True:
        n = IntPrompt.ask(
            "Pick session number",
            default=1,
            show_default=True,
        )
        if 1 <= int(n) <= len(candidates):
            picked = candidates[int(n) - 1]
            t = work_session.WorkSessionTracker.load(picked.path)
            console.print(
                f"[green]Continuing[/green] [cyan]{t.session_name}[/cyan] "
                f"({picked.rotated_count} rotated, {picked.role_changed_count} role-changed).\n"
            )
            return t
        console.print(f"[red]Enter a number between 1 and {len(candidates)}.[/red]")


def _fetch_accounts_with_empty_retry(
    cfg: dict[str, str],
    token: str,
    *,
    verbose_log: Optional[Callable[[str], None]] = None,
) -> list[dict[str, Any]]:
    # If Cerby returns zero rows, offer to fix the provider string and retry.
    while True:
        accounts = _fetch_accounts_merged_once_with_403_retry(
            cfg, token, announce_sync=True, verbose_log=verbose_log
        )
        if accounts:
            return accounts

        scope = _provider_filter_label(cfg["APP_NAME"])
        console.print(
            "\n[yellow]The API returned no accounts[/yellow] for "
            f"[bold]{scope}[/bold].\n"
            "That can mean there are no matching accounts yet, "
            "or a provider id does not match Cerby (spelling, underscores, etc.). "
            "You can also use [bold]Any[/bold] to drop the provider filter.\n"
        )
        if not _prompt_yes_no(
            "Try again with different app provider name(s)?",
            default=False,
        ):
            return accounts

        retry_default = cfg["APP_NAME"] if (cfg["APP_NAME"] or "").strip() else "Any"
        while True:
            new_raw = Prompt.ask(
                "Enter app provider name(s), comma-separated, or Any for all accounts",
                default=retry_default,
            ).strip()
            if not new_raw:
                console.print(
                    "[red]Enter provider name(s), or Any for all accounts.[/red]"
                )
                continue
            cfg["APP_NAME"] = new_raw
            break


def _obtain_token(
    workspace: str,
    app_name: str,
    *,
    verbose_log: Optional[Callable[[str], None]] = None,
    keep_browser_alive: bool = False,
    replace_keeper: Optional[BrowserTokenRefreshSession] = None,
    active: ActiveRun | None = None,
) -> tuple[str, Optional[BrowserTokenRefreshSession]]:
    stored = token_session.load_session()
    if stored and stored["workspace"] == workspace:
        token = stored["access_token"]
        if is_access_token_valid(token):
            token = _ensure_token_refreshed_proactively(
                workspace, token, verbose_log=verbose_log
            )
            probe_client = CerbyApi(
                workspace=workspace,
                app_name=_probe_provider_for_token(app_name),
                account_role="",
                token=token,
                verbose_log=verbose_log,
            )
            try:
                while True:
                    ok, status = probe_client.probe_token()
                    if ok:
                        _say(active, "Using saved access token.")
                        return token, replace_keeper
                    if status == 403:
                        _print_cerby_forbidden_guidance(active)
                        if _prompt_retry_after_permission_fix(active):
                            continue
                        raise typer.Exit(1)
                    break
            except typer.Exit:
                raise
            except Exception as e:
                _say(active, f"Saved token rejected ({e!s}); opening browser...", style="yellow")
        else:
            _say(active, "Saved token expired; opening browser...", style="yellow")
            stored2 = token_session.load_session()
            if (
                stored2
                and stored2["workspace"] == workspace
                and is_access_token_valid(stored2["access_token"])
            ):
                return _obtain_token(
                    workspace,
                    app_name,
                    verbose_log=verbose_log,
                    keep_browser_alive=keep_browser_alive,
                    replace_keeper=replace_keeper,
                    active=active,
                )
        token_session.clear_session()

    if replace_keeper is not None:
        replace_keeper.stop()

    if active is not None and not active.interactive:
        active.log_error(
            "No valid access token. Set access_token in config.yaml (one-time bootstrap), "
            "or sign in interactively to create assets/.cerby_session.json."
        )
        raise typer.Exit(1)

    console.print("[bold]Authenticating[/bold] in the browser...")
    if keep_browser_alive:
        console.print(
            "[dim][experimental] Chromium stays open; about every 60s the Cerby tab reloads and "
            "any new access_token in localStorage is written to assets/.cerby_session.json. "
            "Stop the CLI (Ctrl+C) to close the browser.[/dim]\n"
        )
        keeper = BrowserTokenRefreshSession(workspace)
        token = keeper.start_and_wait_first_token()
        token_session.save_session(workspace, token)
        console.print("[green]Access token saved for this workspace.[/green]\n")
        return token, keeper

    handler = CerbyAuthHandler(workspace)
    token = handler.get_access_token()
    token_session.save_session(workspace, token)
    console.print("[green]Access token saved for this workspace.[/green]\n")
    return token, None


def _ensure_token_refreshed_proactively(
    workspace: str,
    token: str,
    *,
    verbose_log: Optional[Callable[[str], None]] = None,
    within_seconds: float = TOKEN_PROACTIVE_REFRESH_WITHIN_SECONDS,
    active: ActiveRun | None = None,
) -> str:
    """If the JWT expires within ``within_seconds``, call ``GET /v1/auth/refresh`` and persist."""
    rem = access_token_seconds_remaining(token)
    if rem is None or rem > within_seconds:
        return token
    try:
        new_tok = refresh_access_token(token, workspace, verbose_log=verbose_log)
        if not is_access_token_valid(new_tok):
            return token
        token_session.save_session(workspace, new_tok)
        _say(active, "Access token refreshed.", style="dim")
        return new_tok
    except Exception as e:
        _say(
            active,
            f"Proactive token refresh failed ({e!s}); continuing with current token.",
            style="yellow",
        )
        return token


def _parse_poll_interval_seconds(raw: str) -> float:
    # Examples: 5s, 1m, 2h (unit optional, defaults to seconds).
    s = raw.strip().lower().replace(" ", "")
    if not s:
        raise ValueError("interval is empty")
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(s|sec|secs|m|min|mins|h|hr|hrs|hour|hours)?", s)
    if not m:
        raise ValueError(
            f"cannot parse interval {raw!r}; use a number plus unit, e.g. 5s, 1m, 2h"
        )
    n = float(m.group(1))
    u = m.group(2) or "s"
    if u in ("s", "sec", "secs"):
        sec = n
    elif u in ("m", "min", "mins"):
        sec = n * 60.0
    elif u in ("h", "hr", "hrs", "hour", "hours"):
        sec = n * 3600.0
    else:
        sec = n
    if sec < 1.0:
        raise ValueError("interval must be at least 1 second")
    if sec > 7 * 24 * 3600:
        raise ValueError("interval too large (max 7 days)")
    return sec


def _fetch_accounts_merged_once_with_403_retry(
    cfg: dict[str, str],
    token: str,
    *,
    announce_sync: bool,
    verbose_log: Optional[Callable[[str], None]] = None,
    active: ActiveRun | None = None,
) -> list[dict[str, Any]]:
    token = _ensure_token_refreshed_proactively(
        cfg["CERBY_WORKSPACE"], token, verbose_log=verbose_log, active=active
    )
    if announce_sync:
        _say(active, "Syncing accounts from the API...")
    while True:
        try:
            rows = fetch_accounts_merged(
                cfg["CERBY_WORKSPACE"],
                parse_provider_specs(cfg["APP_NAME"]),
                cfg["ACCOUNT_ROLE"],
                token,
                verbose_log=verbose_log,
            )
            if active is not None:
                active.log_info("Account sync complete", count=len(rows))
            return rows
        except requests.HTTPError as e:
            if _is_403_http_error(e):
                _print_cerby_forbidden_guidance(active)
                if not _prompt_retry_after_permission_fix(active):
                    msg = "Could not list accounts (403)."
                    if active is not None:
                        active.log_error(msg)
                    console.print(f"\n[red]{msg}[/red]\n")
                    return []
                continue
            raise


def _execute_bulk_account_actions(
    cfg: dict[str, str],
    client: CerbyApi,
    session_tracker: work_session.WorkSessionTracker,
    accounts: list[dict[str, Any]],
    choice: str,
    *,
    run_started_at: str,
    table_title: str = "Actions",
    role_exclude_user_ids: frozenset[str] = frozenset(),
    verbose_log: Optional[Callable[[str], None]] = None,
    active: ActiveRun | None = None,
    phase: str = "manual",
    show_table: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    new_tok = _ensure_token_refreshed_proactively(
        cfg["CERBY_WORKSPACE"], client.token, verbose_log=verbose_log, active=active
    )
    if new_tok != client.token:
        client.replace_token(new_tok)

    api_share_role = share_role_for_api(cfg["ACCOUNT_ROLE"])

    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    run_rotations: list[dict[str, Any]] = []
    run_role_changes: list[dict[str, Any]] = []

    table = Table(title=table_title)
    table.add_column("Account")
    table.add_column("Account ID")
    table.add_column("Action")
    table.add_column("Result")

    for acc in accounts:
        aid = acc.get("id") or acc.get("accountId") or acc.get("account_id")
        label = client.describe_account(acc)
        if not aid:
            table.add_row(label, "—", "skip", "missing id")
            continue

        actions: list[str] = []
        if choice in ("rotate", "both"):
            actions.append("rotate_password")
        if choice in ("role", "both"):
            actions.append("change_role")

        for act in actions:
            if act == "rotate_password":
                if str(aid) in session_tracker.rotated_ids():
                    console.print(
                        f"\n[bold]Account:[/bold] {label}  [dim](id={aid})[/dim]  — "
                        f"[dim]skip rotate (already done in this work session)[/dim]"
                    )
                    table.add_row(label, str(aid), "rotate password", "skipped (session)")
                    run_rotations.append(
                        {
                            "at": _utc_now(),
                            "account_id": str(aid),
                            "account_label": label,
                            "status": "skipped",
                            "reason": "already_rotated_in_session",
                        }
                    )
                    continue
                console.print(
                    f"\n[bold]Account:[/bold] {label}  [dim](id={aid})[/dim]  — [yellow]rotating password (once)[/yellow]"
                )
                rotate_done = False
                while not rotate_done:
                    r = client.rotate_password(str(aid))
                    if r.status_code == 403:
                        _print_cerby_forbidden_guidance(active)
                        if not _prompt_retry_after_permission_fix(active):
                            table.add_row(
                                label,
                                str(aid),
                                "rotate password",
                                "403 forbidden — needs super admin + all access mode",
                            )
                            run_rotations.append(
                                {
                                    "at": _utc_now(),
                                    "account_id": str(aid),
                                    "account_label": label,
                                    "status": "error",
                                    "error": "403 forbidden — needs super admin + all access mode",
                                }
                            )
                            rotate_done = True
                            continue
                        continue
                    try:
                        r.raise_for_status()
                    except requests.HTTPError as e:
                        table.add_row(
                            label, str(aid), "rotate password", f"error: {e!s}"[:120]
                        )
                        run_rotations.append(
                            {
                                "at": _utc_now(),
                                "account_id": str(aid),
                                "account_label": label,
                                "status": "error",
                                "error": str(e)[:500],
                            }
                        )
                        rotate_done = True
                        continue
                    except Exception as e:
                        table.add_row(
                            label, str(aid), "rotate password", f"error: {e!s}"[:120]
                        )
                        run_rotations.append(
                            {
                                "at": _utc_now(),
                                "account_id": str(aid),
                                "account_label": label,
                                "status": "error",
                                "error": str(e)[:500],
                            }
                        )
                        rotate_done = True
                        continue
                    session_tracker.mark_rotated(str(aid), account_label=label)
                    run_rotations.append(
                        {
                            "at": _utc_now(),
                            "account_id": str(aid),
                            "account_label": label,
                            "status": "success",
                        }
                    )
                    if active is not None:
                        active.log_info(
                            "Password rotated",
                            account_id=str(aid),
                            account_label=label,
                            phase=phase,
                        )
                    table.add_row(label, str(aid), "rotate password", "success")
                    rotate_done = True

            if act == "change_role":
                if str(aid) in session_tracker.role_changed_ids():
                    console.print(
                        f"\n[bold]Account:[/bold] {label}  [dim](id={aid})[/dim]  — "
                        f"[dim]skip role change (already done in this work session)[/dim]"
                    )
                    table.add_row(
                        label,
                        str(aid),
                        f"role → {cfg['ACCOUNT_ROLE']}",
                        "skipped (session)",
                    )
                    run_role_changes.append(
                        {
                            "at": _utc_now(),
                            "account_id": str(aid),
                            "account_label": label,
                            "target_role": cfg["ACCOUNT_ROLE"],
                            "status": "skipped",
                            "reason": "already_role_changed_in_session",
                        }
                    )
                    continue
                role_ctx = (
                    f" (skipping {len(role_exclude_user_ids)} configured user id(s) "
                    "when present on this account)"
                    if role_exclude_user_ids
                    else " (all assigned users)"
                )
                console.print(
                    f"\n[bold]Account:[/bold] {label}  [dim](id={aid})[/dim]  — "
                    f"[yellow]role → {cfg['ACCOUNT_ROLE']}[/yellow]{role_ctx}"
                )
                member_rows: list[dict[str, Any]] | None = None
                members_err: Optional[str] = None
                while member_rows is None and members_err is None:
                    try:
                        member_rows = client.fetch_account_assigned_users(str(aid))
                    except requests.HTTPError as e:
                        if _is_403_http_error(e):
                            _print_cerby_forbidden_guidance(active)
                            if _prompt_retry_after_permission_fix(active):
                                continue
                            members_err = (
                                "403 members search — needs super admin + all access mode"
                            )
                        else:
                            members_err = f"members search: {e!s}"[:120]
                    except Exception as e:
                        members_err = f"members search: {e!s}"[:120]
                if members_err is not None:
                    table.add_row(
                        label,
                        str(aid),
                        f"role → {cfg['ACCOUNT_ROLE']}",
                        members_err,
                    )
                    run_role_changes.append(
                        {
                            "at": _utc_now(),
                            "account_id": str(aid),
                            "account_label": label,
                            "target_role": cfg["ACCOUNT_ROLE"],
                            "status": "error",
                            "error": members_err,
                        }
                    )
                    continue
                assert member_rows is not None
                all_user_ids = CerbyApi.user_ids_from_assigned_users(member_rows)
                if not all_user_ids:
                    table.add_row(
                        label,
                        str(aid),
                        f"role → {cfg['ACCOUNT_ROLE']}",
                        "error: no users from members/search",
                    )
                    run_role_changes.append(
                        {
                            "at": _utc_now(),
                            "account_id": str(aid),
                            "account_label": label,
                            "target_role": cfg["ACCOUNT_ROLE"],
                            "status": "error",
                            "error": "no users from members/search",
                        }
                    )
                    continue
                user_ids = [
                    uid for uid in all_user_ids if uid not in role_exclude_user_ids
                ]
                excluded_here = len(all_user_ids) - len(user_ids)
                if excluded_here:
                    console.print(
                        f"  [dim]Skipping role change for {excluded_here} user(s) on this "
                        "account (excluded list).[/dim]"
                    )
                if not user_ids:
                    console.print(
                        "  [dim]No remaining users to update after exclusions.[/dim]"
                    )
                    table.add_row(
                        label,
                        str(aid),
                        f"role → {cfg['ACCOUNT_ROLE']}",
                        "skipped (all assigned users excluded)",
                    )
                    run_role_changes.append(
                        {
                            "at": _utc_now(),
                            "account_id": str(aid),
                            "account_label": label,
                            "target_role": cfg["ACCOUNT_ROLE"],
                            "status": "skipped",
                            "reason": "all_assigned_users_excluded",
                        }
                    )
                    continue
                snapshots = CerbyApi.role_snapshots_for_account_users(
                    member_rows, str(aid), user_ids
                )
                console.print(
                    f"  [dim]Applying to {len(user_ids)} user(s) in one request...[/dim]"
                )
                share_done = False
                while not share_done:
                    r = client.change_role(str(aid), user_ids, api_share_role)
                    if r.status_code == 403:
                        _print_cerby_forbidden_guidance(active)
                        if not _prompt_retry_after_permission_fix(active):
                            table.add_row(
                                label,
                                str(aid),
                                f"role → {cfg['ACCOUNT_ROLE']}",
                                "403 forbidden — needs super admin + all access mode",
                            )
                            run_role_changes.append(
                                {
                                    "at": _utc_now(),
                                    "account_id": str(aid),
                                    "account_label": label,
                                    "target_role": cfg["ACCOUNT_ROLE"],
                                    "status": "error",
                                    "error": "403 forbidden — needs super admin + all access mode",
                                    "users_preview": snapshots,
                                }
                            )
                            share_done = True
                            continue
                        continue
                    try:
                        r.raise_for_status()
                    except requests.HTTPError as e:
                        table.add_row(
                            label,
                            str(aid),
                            f"role → {cfg['ACCOUNT_ROLE']}",
                            f"error: {e!s}"[:120],
                        )
                        run_role_changes.append(
                            {
                                "at": _utc_now(),
                                "account_id": str(aid),
                                "account_label": label,
                                "target_role": cfg["ACCOUNT_ROLE"],
                                "status": "error",
                                "error": str(e)[:500],
                                "users_preview": snapshots,
                            }
                        )
                        share_done = True
                        continue
                    except Exception as e:
                        table.add_row(
                            label,
                            str(aid),
                            f"role → {cfg['ACCOUNT_ROLE']}",
                            f"error: {e!s}"[:120],
                        )
                        run_role_changes.append(
                            {
                                "at": _utc_now(),
                                "account_id": str(aid),
                                "account_label": label,
                                "target_role": cfg["ACCOUNT_ROLE"],
                                "status": "error",
                                "error": str(e)[:500],
                                "users_preview": snapshots,
                            }
                        )
                        share_done = True
                        continue
                    users_for_session: list[dict[str, Any]] = []
                    for s in snapshots:
                        users_for_session.append({**s, "new_role": cfg["ACCOUNT_ROLE"]})
                    session_tracker.mark_role_changed(
                        str(aid),
                        account_label=label,
                        target_role=cfg["ACCOUNT_ROLE"],
                        users=users_for_session,
                    )
                    run_role_changes.append(
                        {
                            "at": _utc_now(),
                            "account_id": str(aid),
                            "account_label": label,
                            "target_role": cfg["ACCOUNT_ROLE"],
                            "status": "success",
                            "users": users_for_session,
                        }
                    )
                    ok_detail = f"{len(user_ids)} users"
                    if excluded_here:
                        ok_detail = f"{len(user_ids)} users ({excluded_here} excluded)"
                    table.add_row(
                        label,
                        str(aid),
                        f"role → {cfg['ACCOUNT_ROLE']} ({ok_detail})",
                        "success",
                    )
                    if active is not None:
                        active.log_info(
                            "Role changed",
                            account_id=str(aid),
                            account_label=label,
                            target_role=cfg["ACCOUNT_ROLE"],
                            user_count=len(user_ids),
                            phase=phase,
                        )
                    share_done = True

    if show_table and (active is None or active.interactive):
        console.print()
        console.print(table)
    if active is not None and active.running_report is not None:
        active.running_report.append_action_results(
            phase=phase,
            rotations=run_rotations,
            role_changes=run_role_changes,
        )
    elif active is not None and (run_rotations or run_role_changes):
        active.log_info(
            "Batch actions finished",
            phase=phase,
            rotations=len(run_rotations),
            role_changes=len(run_role_changes),
        )
    return run_rotations, run_role_changes


def _maybe_prompt_export_report(
    cfg: dict[str, str],
    session_tracker: work_session.WorkSessionTracker,
    *,
    run_started_at: str,
    run_rotations: list[dict[str, Any]],
    run_role_changes: list[dict[str, Any]],
) -> None:
    if not session_tracker.is_persisted():
        console.print("[dim]No session file yet (no successful actions).[/dim]\n")

    ex = Prompt.ask(
        "Export JSON report?",
        choices=["this_run", "full_session", "neither"],
        default="neither",
    )
    if ex == "neither":
        return
    sname = work_session.session_name_slug(session_tracker.session_name)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    if ex == "this_run":
        payload = session_report.build_this_run_export(
            workspace=cfg["CERBY_WORKSPACE"],
            app_name=cfg["APP_NAME"],
            session_name=session_tracker.session_name,
            work_session_display_name=session_report.work_session_display_name(
                session_tracker.data
            ),
            run_started_at=run_started_at,
            rotations=run_rotations,
            role_changes=run_role_changes,
        )
        default_path = Path.cwd() / f"cerby_run_report_{sname}_{ts}.json"
    else:
        payload = session_report.build_full_session_export(session_tracker.data)
        default_path = Path.cwd() / f"cerby_work_session_{sname}_{ts}.json"
    out = Prompt.ask("Write report to", default=str(default_path)).strip()
    if not out:
        console.print("[yellow]No path given; skipping export.[/yellow]")
        return
    session_report.write_report_json(out, payload)
    console.print(f"Wrote [cyan]{out}[/cyan].\n")


def _run_automated_watch(
    cfg: dict[str, str],
    token: str,
    client: CerbyApi,
    session_tracker: work_session.WorkSessionTracker,
    *,
    verbose_log: Optional[Callable[[str], None]] = None,
    experimental_keep_browser_for_token: bool = False,
    token_keeper: Optional[BrowserTokenRefreshSession] = None,
    active: ActiveRun | None = None,
    poll_interval_sec: float | None = None,
    poll_interval_label: str | None = None,
    choice: str | None = None,
    role_exclude_user_ids: frozenset[str] | None = None,
) -> Optional[BrowserTokenRefreshSession]:
    if choice is None:
        choice = Prompt.ask(
            "Action for each new account",
            choices=["rotate", "role", "both"],
            default="rotate",
        )
    if role_exclude_user_ids is None:
        role_exclude_user_ids = (
            _prompt_role_change_exclude_user_ids()
            if choice in ("role", "both")
            else frozenset()
        )

    poll_iv_raw = poll_interval_label or "30s"
    if poll_interval_sec is None:
        while True:
            raw = Prompt.ask("Poll interval (e.g. 5s, 1m, 1h)", default="30s").strip()
            try:
                poll_interval_sec = _parse_poll_interval_seconds(raw)
                poll_iv_raw = raw or "30s"
                break
            except ValueError as e:
                console.print(f"[red]{e}[/red]")
    assert poll_interval_sec is not None

    run_started_at = datetime.now(timezone.utc).isoformat()
    all_rotations: list[dict[str, Any]] = []
    all_role_changes: list[dict[str, Any]] = []

    delta_cutoff: Optional[datetime] = None
    if session_tracker.is_persisted():
        delta_cutoff = session_tracker.last_successful_action_at()

    if delta_cutoff is not None:
        msg = (
            f"Delta sync: accounts created after {_format_utc_short(delta_cutoff)}"
        )
        _say(active, msg)
        accounts_snapshot = _fetch_accounts_merged_once_with_403_retry(
            cfg, token, announce_sync=True, verbose_log=verbose_log, active=active
        )
        catch_up = _accounts_created_after(accounts_snapshot, delta_cutoff)
        if active is not None and active.running_report is not None:
            active.running_report.record_delta_sync(
                len(catch_up), _format_utc_short(delta_cutoff)
            )
        if catch_up:
            _say(active, f"Catch-up: {len(catch_up)} account(s)")
            rots, rcs = _execute_bulk_account_actions(
                cfg,
                client,
                session_tracker,
                catch_up,
                choice,
                run_started_at=run_started_at,
                table_title="Delta sync",
                role_exclude_user_ids=role_exclude_user_ids,
                verbose_log=verbose_log,
                active=active,
                phase="delta_sync",
                show_table=active is None or active.interactive,
            )
            all_rotations.extend(rots)
            all_role_changes.extend(rcs)
        else:
            _say(active, "Delta sync: nothing to catch up")
    else:
        accounts_snapshot = _fetch_accounts_merged_once_with_403_retry(
            cfg, token, announce_sync=True, verbose_log=verbose_log, active=active
        )

    watched_ids: set[str] = {
        i for a in accounts_snapshot if (i := _account_row_id(a))
    }
    _say(active, f"Baseline: {len(watched_ids)} account id(s); polling for new ids")
    if active is not None and active.running_report is not None:
        active.running_report.set_baseline(len(watched_ids))

    _say(active, f"Polling every {poll_iv_raw} (~{poll_interval_sec:.0f}s)")

    try:
        while True:
            time.sleep(poll_interval_sec)
            stored = token_session.load_session()
            if (
                stored
                and stored["workspace"] == cfg["CERBY_WORKSPACE"]
                and is_access_token_valid(stored["access_token"])
                and stored["access_token"] != token
            ):
                token = stored["access_token"]
                client = CerbyApi(
                    workspace=cfg["CERBY_WORKSPACE"],
                    app_name=_probe_provider_for_token(cfg["APP_NAME"]),
                    account_role=cfg["ACCOUNT_ROLE"],
                    token=token,
                    verbose_log=verbose_log,
                )
                _say(active, "Reloaded access token from assets/.cerby_session.json", style="dim")
            token = _ensure_token_refreshed_proactively(
                cfg["CERBY_WORKSPACE"], token, verbose_log=verbose_log, active=active
            )
            if client.token != token:
                client.replace_token(token)
            if not is_access_token_valid(token):
                _say(active, "Access token expired; re-authenticating...", style="yellow")
                token, token_keeper = _obtain_token(
                    cfg["CERBY_WORKSPACE"],
                    cfg["APP_NAME"],
                    verbose_log=verbose_log,
                    keep_browser_alive=experimental_keep_browser_for_token,
                    replace_keeper=token_keeper,
                    active=active,
                )
                client = CerbyApi(
                    workspace=cfg["CERBY_WORKSPACE"],
                    app_name=_probe_provider_for_token(cfg["APP_NAME"]),
                    account_role=cfg["ACCOUNT_ROLE"],
                    token=token,
                    verbose_log=verbose_log,
                )
            current = _fetch_accounts_merged_once_with_403_retry(
                cfg, token, announce_sync=False, verbose_log=verbose_log, active=active
            )
            new_accounts = [
                a
                for a in current
                if (i := _account_row_id(a)) and i not in watched_ids
            ]
            if active is not None and active.running_report is not None:
                active.running_report.record_poll(new_account_count=len(new_accounts))
            if not new_accounts:
                if active is None or active.interactive:
                    console.print(
                        f"[dim]{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z — no new accounts[/dim]"
                    )
                else:
                    active.log_debug("Poll: no new accounts")
                continue
            _say(active, f"New accounts: {len(new_accounts)} — running actions")
            rots, rcs = _execute_bulk_account_actions(
                cfg,
                client,
                session_tracker,
                new_accounts,
                choice,
                run_started_at=run_started_at,
                table_title="Automated actions",
                role_exclude_user_ids=role_exclude_user_ids,
                verbose_log=verbose_log,
                active=active,
                phase="poll",
                show_table=active is None or active.interactive,
            )
            all_rotations.extend(rots)
            all_role_changes.extend(rcs)
            for a in new_accounts:
                if i := _account_row_id(a):
                    watched_ids.add(i)
    except KeyboardInterrupt:
        _say(active, "Automated watch stopped (Ctrl+C)", style="yellow")

    if active is None or active.interactive:
        _maybe_prompt_export_report(
            cfg,
            session_tracker,
            run_started_at=run_started_at,
            run_rotations=all_rotations,
            run_role_changes=all_role_changes,
        )
    return token_keeper


def _build_active_run(
    log_file: Path,
    *,
    interactive: bool = True,
    running_report: RunningReportWriter | None = None,
    mirror_stderr: bool = False,
) -> ActiveRun:
    ensure_log_dir()
    logger = RunLogger(log_file, mirror_stderr=mirror_stderr)
    return ActiveRun(logger=logger, running_report=running_report, interactive=interactive)


def _run_flow(
    cfg: dict[str, str],
    *,
    session_name: Optional[str] = None,
    verbose_log: Optional[Callable[[str], None]] = None,
    experimental_keep_browser_for_token: bool = False,
    active: ActiveRun | None = None,
) -> None:
    keeper: Optional[BrowserTokenRefreshSession] = None
    try:
        if active is not None:
            active.log_info(
                "Run started",
                workspace=cfg["CERBY_WORKSPACE"],
                app_name=cfg["APP_NAME"],
                account_role=cfg["ACCOUNT_ROLE"],
            )
        token, keeper = _obtain_token(
            cfg["CERBY_WORKSPACE"],
            cfg["APP_NAME"],
            verbose_log=verbose_log,
            keep_browser_alive=experimental_keep_browser_for_token,
            active=active,
        )
        token = _ensure_token_refreshed_proactively(
            cfg["CERBY_WORKSPACE"], token, verbose_log=verbose_log, active=active
        )

        client = CerbyApi(
            workspace=cfg["CERBY_WORKSPACE"],
            app_name=_probe_provider_for_token(cfg["APP_NAME"]),
            account_role=cfg["ACCOUNT_ROLE"],
            token=token,
            verbose_log=verbose_log,
        )

        session_tracker = _prompt_work_session_tracker(
            cfg["CERBY_WORKSPACE"],
            cfg["APP_NAME"],
            session_name=session_name,
        )

        run_mode = Prompt.ask(
            "Run mode",
            choices=["manual", "automated"],
            default="manual",
        )
        if run_mode == "automated":
            keeper = _run_automated_watch(
                cfg,
                token,
                client,
                session_tracker,
                verbose_log=verbose_log,
                experimental_keep_browser_for_token=experimental_keep_browser_for_token,
                token_keeper=keeper,
                active=active,
            )
            return

        accounts = _fetch_accounts_with_empty_retry(
            cfg, token, verbose_log=verbose_log
        )
        if not accounts:
            _say(active, "No accounts found; exiting.", style="yellow")
            return

        console.print(
            f"\n[bold]{len(accounts)}[/bold] account(s) — "
            f"{_provider_filter_label(cfg['APP_NAME'])}\n"
        )
        _render_accounts_preview_table(accounts)

        selected = _prompt_which_accounts(accounts, session_tracker)
        if not selected:
            console.print("[yellow]No accounts selected. Exiting.[/yellow]\n")
            return

        if not _prompt_yes_no(f"Proceed with {len(selected)} account(s)?", default=True):
            console.print("[yellow]Aborted.[/yellow]\n")
            return

        choice = Prompt.ask(
            "Actions per account",
            choices=["rotate", "role", "both", "neither"],
            default="neither",
        )

        if choice == "neither":
            console.print("No API actions performed.")
            return

        role_exclude_user_ids = (
            _prompt_role_change_exclude_user_ids()
            if choice in ("role", "both")
            else frozenset()
        )

        run_started_at = datetime.now(timezone.utc).isoformat()
        run_rotations, run_role_changes = _execute_bulk_account_actions(
            cfg,
            client,
            session_tracker,
            selected,
            choice,
            run_started_at=run_started_at,
            role_exclude_user_ids=role_exclude_user_ids,
            verbose_log=verbose_log,
            active=active,
            phase="manual",
        )
        _maybe_prompt_export_report(
            cfg,
            session_tracker,
            run_started_at=run_started_at,
            run_rotations=run_rotations,
            run_role_changes=run_role_changes,
        )
    finally:
        if keeper is not None:
            keeper.stop()


def _interactive_sync_impl(
    workspace: Optional[str],
    app_name: Optional[str],
    account_role: Optional[str],
    session_name: Optional[str],
    *,
    verbose: bool = False,
    experimental_keep_browser_for_token: bool = False,
    log_file: Path | None = None,
) -> None:
    _validate_cli_account_role(account_role)

    _show_intro_banner()
    cfg = _prompt_run_context(workspace, app_name, account_role)

    active = _build_active_run(log_file or DEFAULT_LOG_FILE, interactive=True)
    ensure_assets_dir()
    vlog: Optional[Callable[[str], None]] = (
        _verbose_http_log_factory(active) if verbose else None
    )
    if verbose:
        console.print("[dim]Verbose HTTP logging enabled.[/dim]\n")

    _run_flow(
        cfg,
        session_name=session_name,
        verbose_log=vlog,
        experimental_keep_browser_for_token=experimental_keep_browser_for_token,
        active=active,
    )


def _run_service_impl(config_path: Path) -> None:
    config_path = config_path.resolve()
    project_root = set_project_root(project_root_from_config(config_path))
    ensure_assets_dir()
    ensure_log_dir()

    default_log = (data_dir() / DEFAULT_LOG_FILE).resolve()
    active = _build_active_run(default_log, interactive=False, mirror_stderr=True)
    active.log_info(
        "Cerby onboarding service starting",
        config=str(config_path),
        project_root=str(project_root),
        work_sessions_dir=str(work_sessions_dir()),
    )

    try:
        svc = load_service_config(config_path)
    except (OSError, ValueError) as e:
        active.log_error("Failed to load service config", error=str(e))
        raise typer.BadParameter(str(e)) from e

    configured_log = svc.log_file.resolve()
    running_report = RunningReportWriter(svc.running_report)
    if configured_log != default_log:
        active = _build_active_run(
            configured_log,
            interactive=False,
            running_report=running_report,
            mirror_stderr=True,
        )
    else:
        active.running_report = running_report
    active.log_info(
        "Service config loaded",
        workspace=svc.workspace,
        session_name=svc.session_name,
        log_file=str(configured_log),
    )

    cfg = svc.run_cfg()

    bootstrap = consume_bootstrap_token(svc.config_path)
    if bootstrap:
        token_session.save_session(svc.workspace, bootstrap)
        active.log_info(
            "Bootstrap access token loaded from config and removed from config file"
        )

    try:
        session_tracker, created = work_session.load_or_create_session_by_name(
            svc.session_name, cfg["CERBY_WORKSPACE"], cfg["APP_NAME"]
        )
    except ValueError as e:
        active.log_error("Work session error", error=str(e))
        raise typer.BadParameter(str(e)) from e

    if created:
        active.log_info(
            "Created work session",
            session_name=session_tracker.session_name,
            path=str(session_tracker.path.resolve()),
        )
    else:
        active.log_info(
            "Using existing work session",
            session_name=session_tracker.session_name,
            path=str(session_tracker.path.resolve()),
        )

    poll_interval_sec = _parse_poll_interval_seconds(svc.poll_interval)
    vlog: Optional[Callable[[str], None]] = (
        _verbose_http_log_factory(active) if svc.verbose_http else None
    )
    running_report.start(
        workspace=cfg["CERBY_WORKSPACE"],
        app_name=cfg["APP_NAME"],
        account_role=cfg["ACCOUNT_ROLE"],
        session_tracker_data=session_tracker.data,
        actions=svc.actions,
        poll_interval=svc.poll_interval,
        role_exclude_user_ids=sorted(svc.role_exclude_user_ids),
    )
    active.record_event(
        "Service starting",
        config=str(svc.config_path),
        log_file=str(svc.log_file),
        running_report=str(svc.running_report),
    )

    keeper: Optional[BrowserTokenRefreshSession] = None
    stop_reason = "stopped"
    try:
        token, keeper = _obtain_token(
            cfg["CERBY_WORKSPACE"],
            cfg["APP_NAME"],
            verbose_log=vlog,
            keep_browser_alive=svc.experimental_keep_browser_for_token,
            active=active,
        )
        token = _ensure_token_refreshed_proactively(
            cfg["CERBY_WORKSPACE"], token, verbose_log=vlog, active=active
        )
        client = CerbyApi(
            workspace=cfg["CERBY_WORKSPACE"],
            app_name=_probe_provider_for_token(cfg["APP_NAME"]),
            account_role=cfg["ACCOUNT_ROLE"],
            token=token,
            verbose_log=vlog,
        )
        _run_automated_watch(
            cfg,
            token,
            client,
            session_tracker,
            verbose_log=vlog,
            experimental_keep_browser_for_token=svc.experimental_keep_browser_for_token,
            token_keeper=keeper,
            active=active,
            poll_interval_sec=poll_interval_sec,
            poll_interval_label=svc.poll_interval,
            choice=svc.actions,
            role_exclude_user_ids=svc.role_exclude_user_ids,
        )
    except typer.Exit:
        stop_reason = "error"
        active.log_error("Service exiting with error")
        raise
    except Exception as e:
        stop_reason = "error"
        active.log_error("Service failed", error=str(e)[:500])
        raise
    finally:
        running_report.stop(reason=stop_reason)
        if keeper is not None:
            keeper.stop()


@app.command()
def service(
    config: Path = typer.Option(
        Path("config/config.yaml"),
        "--config",
        "-c",
        help="Path to service config.yaml (workspace, session, poll interval, paths).",
    ),
) -> None:
    """Run unattended automated mode using settings from config.yaml (for systemd)."""
    _run_service_impl(config)


@app.command()
def run(
    workspace: Optional[str] = typer.Option(
        None,
        "--workspace",
        "-w",
        envvar="CERBY_WORKSPACE",
        help="Default for the workspace prompt (subdomain, e.g. mycompany).",
    ),
    app_name: Optional[str] = typer.Option(
        None,
        "--app-name",
        envvar="APP_NAME",
        help="Default for the app prompt: comma-separated provider ids, or ANY (any casing) per segment.",
    ),
    account_role: Optional[str] = typer.Option(
        None,
        "--account-role",
        envvar="ACCOUNT_ROLE",
        help="Default for the role prompt: OWNER or COLLABORATOR.",
    ),
    session_name: Optional[str] = typer.Option(
        None,
        "--session-name",
        envvar="CERBY_SESSION_NAME",
        help="Load this work session by name (under assets/work_sessions/) without prompts.",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Log each Cerby API request/response (token redacted; large bodies truncated).",
    ),
    experimental_keep_browser_for_token: bool = typer.Option(
        False,
        "--experimental-keep-browser-for-token",
        envvar="CERBY_EXPERIMENTAL_KEEP_BROWSER",
        help="TEMPORARY: after browser login, keep Chromium open and reload ~every 60s to persist new access_token to assets/.cerby_session.json.",
    ),
    log_file: Path = typer.Option(
        DEFAULT_LOG_FILE,
        "--log-file",
        help="Append run log to this file (sensitive values redacted).",
    ),
) -> None:
    _interactive_sync_impl(
        workspace,
        app_name,
        account_role,
        session_name,
        verbose=verbose,
        experimental_keep_browser_for_token=experimental_keep_browser_for_token,
        log_file=log_file,
    )


@app.callback(invoke_without_command=True)
def _cli_entry(
    ctx: typer.Context,
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Log each Cerby API request/response (token redacted; large bodies truncated).",
    ),
    experimental_keep_browser_for_token: bool = typer.Option(
        False,
        "--experimental-keep-browser-for-token",
        envvar="CERBY_EXPERIMENTAL_KEEP_BROWSER",
        help="TEMPORARY: after browser login, keep Chromium open and reload ~every 60s to persist new access_token to assets/.cerby_session.json.",
    ),
) -> None:
    if ctx.invoked_subcommand is None:
        _interactive_sync_impl(
            None,
            None,
            None,
            None,
            verbose=verbose,
            experimental_keep_browser_for_token=experimental_keep_browser_for_token,
        )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
