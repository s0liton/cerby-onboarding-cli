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

import account_storage
import session_report
import token_session
import work_session
from auth_handler import CerbyAuthHandler, is_access_token_valid
from cerby_client import (
    CerbyApi,
    fetch_accounts_merged,
    normalize_provider_filter,
    parse_provider_specs,
    share_role_for_api,
)

app = typer.Typer(
    add_completion=False,
    help="Cerby Onboarding CLI - sync accounts, rotate passwords, and change roles.",
)
console = Console()


def _verbose_http_log(message: str) -> None:
    # dim + no markup so JSON braces don't trip Rich
    console.print(message, style="dim", markup=False, overflow="fold")

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
    blurb = (
        "Quickly and effectively rotate account passwords, change user roles, and make onboarding even easier."
    )
    console.print()
    console.print(Align.center(Text(logo, style="blue")))
    console.print()
    console.print(Align.center(Text("Cerby Onboarding CLI", style="bold")))
    console.print(Align.center(Text(blurb, style="dim")))
    console.print(
        Align.center(Text("Made with care by Pedro Santos (pedro@cerby.com)", style="dim"))
    )
    console.print()


def _is_403_http_error(exc: BaseException) -> bool:
    return (
        isinstance(exc, requests.HTTPError)
        and exc.response is not None
        and exc.response.status_code == 403
    )


def _print_cerby_forbidden_guidance() -> None:
    console.print(
        "\n[yellow]403 Forbidden[/yellow] — you are signed in to Cerby, but the API will not "
        "perform this operation for the signed-in user.\n\n"
        "Listing accounts, loading members, rotating passwords, and changing roles require "
        "either:\n"
        "  • [bold]Super administrator[/bold] with [bold]all access mode[/bold] enabled, or\n"
        "  • [bold]Direct owner access[/bold] to the accounts you are working on.\n\n"
        "Adjust the Cerby user whose browser session produced this tool’s token (or grant "
        "them owner access to the relevant accounts), then retry.\n"
    )


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
    if not _prompt_yes_no(
        "Exclude any user IDs from role changes on every account?",
        default=False,
    ):
        return frozenset()
    entered = Prompt.ask("Comma-separated user IDs to exclude").strip()
    ids = _parse_comma_separated_user_ids(entered)
    if not ids:
        console.print(
            "[yellow]No user IDs parsed; proceeding without exclusions.[/yellow]"
        )
        return frozenset()
    return frozenset(ids)


def _prompt_retry_after_permission_fix() -> bool:
    return _prompt_yes_no(
        "Retry after updating Cerby permissions for this user?",
        default=True,
    )


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
    # Workspace + app + role; flags/env only seed the prompts.
    console.print(
        "\n[bold]Run context[/bold] "
        "(each run asks for these three values; use flags or env vars to pre-fill defaults.)\n"
    )

    def ask_nonempty(label: str, default: str) -> str:
        while True:
            value = Prompt.ask(label, default=default).strip()
            if value:
                return value
            console.print("[red]This value is required.[/red]")

    saved_ws = token_session.last_saved_workspace()
    cli_env_ws = (workspace or os.environ.get("CERBY_WORKSPACE") or "").strip()

    if cli_env_ws:
        if saved_ws and cli_env_ws != saved_ws:
            if _prompt_yes_no(
                f"Use workspace [cyan]{cli_env_ws}[/cyan] from flags/env "
                f"instead of the one in [cyan].cerby_session.json[/cyan] ([cyan]{saved_ws}[/cyan])?",
                default=True,
            ):
                ws = cli_env_ws
            else:
                if _prompt_yes_no(
                    f"Keep workspace [cyan]{saved_ws}[/cyan] from your saved session?",
                    default=True,
                ):
                    ws = saved_ws
                else:
                    ws = ask_nonempty("Cerby workspace (subdomain)", "")
        else:
            ws = cli_env_ws
    elif saved_ws:
        if _prompt_yes_no(
            f"Still using workspace [cyan]{saved_ws}[/cyan] (from .cerby_session.json)?",
            default=True,
        ):
            ws = saved_ws
        else:
            ws = ask_nonempty("Cerby workspace (subdomain)", "")
    else:
        ws = ask_nonempty("Cerby workspace (subdomain)", "")

    app_prefill = (app_name or os.environ.get("APP_NAME") or "").strip()
    app_prompt_default = app_prefill if app_prefill else "Any"
    r_cli = _maybe_account_role(account_role)
    r_env = _maybe_account_role(os.environ.get("ACCOUNT_ROLE"))
    r_default = r_cli or r_env or "COLLABORATOR"
    role_default = r_default.lower()

    while True:
        app_raw = Prompt.ask(
            "Enter app provider name(s), comma-separated (e.g. slack, zoom), or Any for all accounts",
            default=app_prompt_default,
        ).strip()
        if not app_raw:
            console.print(
                "[red]Enter one or more provider names (comma-separated), or Any for all accounts.[/red]"
            )
            continue
        app = app_raw
        break
    role_lc = Prompt.ask(
        "Account role (for role changes)",
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
        "Apply bulk actions to which accounts?",
        choices=["all_eligible", "pick_rows"],
        default="all_eligible",
    )

    if mode == "all_eligible":
        if eligible:
            console.print(
                f"\n[dim]{len(eligible)} of {len(accounts)} account(s) are eligible "
                f"(not yet rotated or role-changed in this session).[/dim]\n"
            )
            return eligible
        console.print(
            "[yellow]No eligible accounts: every row is already rotated or role-changed "
            "in this work session.[/yellow]\n"
        )
        if not _prompt_yes_no(
            "Pick specific row numbers from the table instead?",
            default=False,
        ):
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


def _resolve_new_session_label(session_label: Optional[str]) -> str:
    if session_label is not None and str(session_label).strip():
        return str(session_label).strip()
    return Prompt.ask(
        "Session label (optional, helps you recognize this session later)",
        default="",
    ).strip()


def _prompt_work_session_tracker(
    ws: str,
    app: str,
    *,
    session_id: Optional[str],
    session_label: Optional[str],
) -> work_session.WorkSessionTracker:
    # --session-id skips this; otherwise pick new vs continue and maybe a label.
    if session_id is not None and str(session_id).strip():
        sid = str(session_id).strip()
        try:
            t = work_session.load_session_for_workspace_app(sid, ws, app)
        except ValueError as e:
            raise typer.BadParameter(str(e)) from e
        lab = (t.data.get("label") or "").strip()
        lab_note = f" ({lab})" if lab else ""
        console.print(
            f"\n[green]Using work session[/green] [cyan]{t.data['session_id']}[/cyan]{lab_note} "
            f"— loaded via [bold]--session-id[/bold].\n"
        )
        return t

    candidates = work_session.list_matching_sessions(ws, app)
    if not candidates:
        console.print(
            "[dim]Work session: starting new (no saved sessions overlap this workspace and "
            "the provider(s) you entered); accounts will be fetched next.[/dim]\n"
        )
        label = _resolve_new_session_label(session_label)
        t = work_session.WorkSessionTracker.begin_new(ws, app, label=label)
        lab_note = f" — [dim]{label}[/dim]" if label else ""
        console.print(
            f"[green]New work session[/green] [cyan]{t.data['session_id']}[/cyan]{lab_note}. "
            f"[dim]A file under [cyan]work_sessions/[/cyan] is created only after the first "
            f"successful rotate or role change.[/dim]\n"
        )
        return t

    console.print(
        "\n[bold]Work session[/bold]\n"
        "We remember which accounts were already rotated or role-changed so you can "
        "safely re-run after more accounts are onboarded. "
        "This is chosen before we fetch accounts from the API.\n"
        "[dim]Sessions listed here overlap your app string: e.g. a session saved for "
        "two providers still appears if you only type one of them.[/dim]\n"
    )
    mode = Prompt.ask(
        "Start a new session or continue an existing one?",
        choices=["new", "continue"],
        default="continue",
    )
    if mode == "new":
        label = _resolve_new_session_label(session_label)
        t = work_session.WorkSessionTracker.begin_new(ws, app, label=label)
        lab_note = f" — [dim]{label}[/dim]" if label else ""
        console.print(
            f"\n[green]New work session[/green] [cyan]{t.data['session_id']}[/cyan]{lab_note}. "
            f"[dim]A file under [cyan]work_sessions/[/cyan] is created only after the first "
            f"successful rotate or role change.[/dim]\n"
        )
        return t

    sess_table = Table(title="Saved sessions (workspace + overlapping app scope)")
    sess_table.add_column("#", justify="right")
    sess_table.add_column("Label")
    sess_table.add_column("Session id")
    sess_table.add_column("App scope (when saved)", overflow="fold")
    sess_table.add_column("Last updated")
    sess_table.add_column("Rotated")
    sess_table.add_column("Role-changed")
    for i, entry in enumerate(candidates, start=1):
        lab = entry.label if entry.label else "—"
        scope = entry.session_app_name.strip() if entry.session_app_name.strip() else "—"
        sess_table.add_row(
            str(i),
            lab,
            entry.session_id,
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
            lab = t.display_label()
            lab_part = f", label [dim]{lab}[/dim]" if lab != "—" else ""
            console.print(
                f"\n[green]Continuing session[/green] [cyan]{t.data['session_id']}[/cyan]{lab_part} "
                f"({picked.rotated_count} rotated, {picked.role_changed_count} role-changed on file).\n"
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
) -> str:
    stored = token_session.load_session()
    if stored and stored["workspace"] == workspace:
        token = stored["access_token"]
        if is_access_token_valid(token):
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
                        console.print("[green]Using saved access token (still valid).[/green]\n")
                        return token
                    if status == 403:
                        _print_cerby_forbidden_guidance()
                        if _prompt_retry_after_permission_fix():
                            continue
                        raise typer.Exit(1)
                    break
            except typer.Exit:
                raise
            except Exception as e:
                console.print(
                    f"[yellow]Saved token was rejected by the API ({e!s}); opening browser to sign in again...[/yellow]"
                )
        else:
            console.print(
                "[yellow]Saved access token is expired; opening browser to sign in...[/yellow]"
            )
        token_session.clear_session()

    handler = CerbyAuthHandler(workspace)
    console.print("[bold]Authenticating[/bold] in the browser...")
    token = handler.get_access_token()
    token_session.save_session(workspace, token)
    console.print("[green]Access token saved for this workspace.[/green]\n")
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
) -> list[dict[str, Any]]:
    if announce_sync:
        console.print("[bold]Syncing accounts[/bold] from the API...")
    while True:
        try:
            return fetch_accounts_merged(
                cfg["CERBY_WORKSPACE"],
                parse_provider_specs(cfg["APP_NAME"]),
                cfg["ACCOUNT_ROLE"],
                token,
                verbose_log=verbose_log,
            )
        except requests.HTTPError as e:
            if _is_403_http_error(e):
                _print_cerby_forbidden_guidance()
                if not _prompt_retry_after_permission_fix():
                    console.print(
                        "\n[red]Could not list accounts (403). Fix permissions or sign in "
                        "as a different user, then run the tool again.[/red]\n"
                    )
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
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
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
                        _print_cerby_forbidden_guidance()
                        if not _prompt_retry_after_permission_fix():
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
                            _print_cerby_forbidden_guidance()
                            if _prompt_retry_after_permission_fix():
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
                        _print_cerby_forbidden_guidance()
                        if not _prompt_retry_after_permission_fix():
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
                    share_done = True

    console.print()
    console.print(table)
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
        console.print(
            "[dim]No work session file was written: no password rotation or role change "
            "completed successfully, so nothing was saved (new sessions stay in memory only).[/dim]\n"
        )

    ex = Prompt.ask(
        "Export a JSON report?",
        choices=["this_run", "full_session", "neither"],
        default="neither",
    )
    if ex == "neither":
        return
    sid = str(session_tracker.data.get("session_id") or "session")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    if ex == "this_run":
        payload = session_report.build_this_run_export(
            workspace=cfg["CERBY_WORKSPACE"],
            app_name=cfg["APP_NAME"],
            session_id=sid,
            work_session_display_name=session_report.work_session_display_name(
                session_tracker.data
            ),
            run_started_at=run_started_at,
            rotations=run_rotations,
            role_changes=run_role_changes,
        )
        default_path = Path.cwd() / f"cerby_run_report_{sid}_{ts}.json"
    else:
        payload = session_report.build_full_session_export(session_tracker.data)
        default_path = Path.cwd() / f"cerby_work_session_{sid}_{ts}.json"
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
) -> None:
    console.print(
        "\n[dim]Taking the baseline snapshot next, then polling for new account ids only.[/dim]\n"
    )
    poll_iv_raw = "30s"
    while True:
        raw = Prompt.ask("Poll interval (examples: 5s, 1m, 1h)", default="30s").strip()
        try:
            interval_sec = _parse_poll_interval_seconds(raw)
            poll_iv_raw = raw or "30s"
            break
        except ValueError as e:
            console.print(f"[red]{e}[/red]")

    choice = Prompt.ask(
        "What should we do for each net-new account?",
        choices=["rotate", "role", "both"],
        default="rotate",
    )
    role_exclude_user_ids = (
        _prompt_role_change_exclude_user_ids()
        if choice in ("role", "both")
        else frozenset()
    )

    baseline = _fetch_accounts_merged_once_with_403_retry(
        cfg, token, announce_sync=True, verbose_log=verbose_log
    )
    watched_ids: set[str] = {
        i for a in baseline if (i := _account_row_id(a))
    }
    console.print(
        f"\n[green]Baseline:[/green] {len(watched_ids)} account id(s) — "
        "[dim]no actions on these; watching for new ids only.[/dim]\n"
    )

    run_started_at = datetime.now(timezone.utc).isoformat()
    all_rotations: list[dict[str, Any]] = []
    all_role_changes: list[dict[str, Any]] = []

    console.print(
        f"[dim]Polling every {poll_iv_raw} (~{interval_sec:.0f}s). Ctrl+C to stop.[/dim]\n"
    )

    try:
        while True:
            time.sleep(interval_sec)
            if not is_access_token_valid(token):
                console.print(
                    "\n[yellow]Access token expired; opening browser to sign in again...[/yellow]\n"
                )
                token = _obtain_token(
                    cfg["CERBY_WORKSPACE"],
                    cfg["APP_NAME"],
                    verbose_log=verbose_log,
                )
                client = CerbyApi(
                    workspace=cfg["CERBY_WORKSPACE"],
                    app_name=_probe_provider_for_token(cfg["APP_NAME"]),
                    account_role=cfg["ACCOUNT_ROLE"],
                    token=token,
                    verbose_log=verbose_log,
                )
            current = _fetch_accounts_merged_once_with_403_retry(
                cfg, token, announce_sync=False, verbose_log=verbose_log
            )
            new_accounts = [
                a
                for a in current
                if (i := _account_row_id(a)) and i not in watched_ids
            ]
            if not new_accounts:
                console.print(
                    f"[dim]{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z — no new accounts[/dim]"
                )
                continue
            console.print(
                f"\n[bold green]New account(s):[/bold green] {len(new_accounts)} - running actions...\n"
            )
            rots, rcs = _execute_bulk_account_actions(
                cfg,
                client,
                session_tracker,
                new_accounts,
                choice,
                run_started_at=run_started_at,
                table_title="Automated actions",
                role_exclude_user_ids=role_exclude_user_ids,
            )
            all_rotations.extend(rots)
            all_role_changes.extend(rcs)
            for a in new_accounts:
                if i := _account_row_id(a):
                    watched_ids.add(i)
    except KeyboardInterrupt:
        console.print("\n[yellow]Automated watch stopped (Ctrl+C).[/yellow]\n")

    _maybe_prompt_export_report(
        cfg,
        session_tracker,
        run_started_at=run_started_at,
        run_rotations=all_rotations,
        run_role_changes=all_role_changes,
    )


def _run_flow(
    cfg: dict[str, str],
    output: Path,
    fmt: str,
    *,
    session_id: Optional[str] = None,
    session_label: Optional[str] = None,
    verbose_log: Optional[Callable[[str], None]] = None,
) -> None:
    token = _obtain_token(
        cfg["CERBY_WORKSPACE"], cfg["APP_NAME"], verbose_log=verbose_log
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
        session_id=session_id,
        session_label=session_label,
    )

    run_mode = Prompt.ask(
        "Run mode",
        choices=["manual", "automated"],
        default="manual",
    )
    console.print(
        "[dim]Manual: review the account list, pick rows, confirm, then bulk actions run as today. "
        "Automated: poll Cerby on an interval and run actions only on accounts whose ids were not "
        "in the first snapshot (baseline). Press Ctrl+C to stop listening.[/dim]\n"
    )
    if run_mode == "automated":
        _run_automated_watch(
            cfg, token, client, session_tracker, verbose_log=verbose_log
        )
        return

    accounts = _fetch_accounts_with_empty_retry(
        cfg, token, verbose_log=verbose_log
    )
    if not accounts:
        console.print(
            "\n[yellow]No accounts to sync. Exiting before saving or bulk actions.[/yellow]\n"
        )
        return

    console.print(
        f"\nFound [bold]{len(accounts)}[/bold] account(s) for "
        f"[bold]{_provider_filter_label(cfg['APP_NAME'])}[/bold].\n"
    )
    _render_accounts_preview_table(accounts)

    selected = _prompt_which_accounts(accounts, session_tracker)
    if not selected:
        console.print("[yellow]No accounts selected. Exiting.[/yellow]\n")
        return

    if not _prompt_yes_no(
        f"\nProceed with [bold]{len(selected)}[/bold] selected account(s)? "
        "(Bulk actions only affect this subset.)",
        default=True,
    ):
        console.print("[yellow]Aborted by user.[/yellow]\n")
        return

    if fmt == "sqlite":
        account_storage.sync_accounts_sqlite(output, selected)
    else:
        account_storage.sync_accounts_csv(output, selected)
    console.print(f"Saved sync to [cyan]{output}[/cyan] ({fmt}).\n")

    choice = Prompt.ask(
        "What should we do for each account?",
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
    )
    _maybe_prompt_export_report(
        cfg,
        session_tracker,
        run_started_at=run_started_at,
        run_rotations=run_rotations,
        run_role_changes=run_role_changes,
    )


_DEFAULT_SYNC_OUTPUT = Path("accounts.sqlite")


def _interactive_sync_impl(
    workspace: Optional[str],
    app_name: Optional[str],
    account_role: Optional[str],
    output: Path,
    fmt: str,
    session_id: Optional[str],
    session_label: Optional[str],
    *,
    verbose: bool = False,
) -> None:
    fmt_norm = fmt.lower().strip()
    if fmt_norm not in ("sqlite", "csv"):
        raise typer.BadParameter("--format must be 'sqlite' or 'csv'")

    _validate_cli_account_role(account_role)

    _show_intro_banner()
    cfg = _prompt_run_context(workspace, app_name, account_role)

    out_path = output
    if fmt_norm == "csv" and out_path.suffix.lower() != ".csv":
        out_path = out_path.with_suffix(".csv")

    vlog: Optional[Callable[[str], None]] = _verbose_http_log if verbose else None
    if verbose:
        console.print("[dim]Verbose HTTP logging on (Authorization redacted).[/dim]\n")

    _run_flow(
        cfg,
        out_path,
        fmt_norm,
        session_id=session_id,
        session_label=session_label,
        verbose_log=vlog,
    )


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
    output: Path = typer.Option(
        _DEFAULT_SYNC_OUTPUT,
        "--output",
        "-o",
        help="Path for the sync database or CSV file.",
    ),
    fmt: str = typer.Option(
        "sqlite",
        "--format",
        "-f",
        help="sqlite or csv",
    ),
    session_id: Optional[str] = typer.Option(
        None,
        "--session-id",
        envvar="CERBY_SESSION_ID",
        help="Load this work session id (file under work_sessions/) without prompts; must match workspace + app.",
    ),
    session_label: Optional[str] = typer.Option(
        None,
        "--session-label",
        envvar="CERBY_SESSION_LABEL",
        help="Optional label for a new work session (ignored when --session-id is set).",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Log each Cerby API request/response (token redacted; large bodies truncated).",
    ),
) -> None:
    _interactive_sync_impl(
        workspace,
        app_name,
        account_role,
        output,
        fmt,
        session_id,
        session_label,
        verbose=verbose,
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
) -> None:
    if ctx.invoked_subcommand is None:
        _interactive_sync_impl(
            None,
            None,
            None,
            _DEFAULT_SYNC_OUTPUT,
            "sqlite",
            None,
            None,
            verbose=verbose,
        )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
