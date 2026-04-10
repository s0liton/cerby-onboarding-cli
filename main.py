from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

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
from cerby_client import CerbyApi, normalize_provider_filter, share_role_for_api

app = typer.Typer(
    add_completion=False,
    help="Cerby Onboarding CLI — sync accounts, rotate passwords, and change roles.",
)
console = Console()

VALID_ACCOUNT_ROLES = ("OWNER", "COLLABORATOR")


def _provider_filter_label(app_name: str) -> str:
    if not (app_name or "").strip():
        return "all integrations (no provider filter)"
    return (app_name or "").strip()


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
        Align.center(
            Text.from_markup(
                "Made with :heart: by Pedro Santos (pedro@cerby.com)",
                style="dim",
            )
        )
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


def _prompt_retry_after_permission_fix() -> bool:
    return (
        Prompt.ask(
            "Retry after updating Cerby permissions for this user?",
            choices=["y", "n"],
            default="y",
        ).lower()
        == "y"
    )


def _maybe_account_role(value: Optional[str]) -> Optional[str]:
    """Return OWNER / COLLABORATOR if valid; None if unset or unrecognized."""
    if value is None or not str(value).strip():
        return None
    u = str(value).strip().upper()
    if u in VALID_ACCOUNT_ROLES:
        return u
    return None


def _validate_cli_account_role(value: Optional[str]) -> None:
    """Reject invalid --account-role / ACCOUNT_ROLE when set."""
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
    """Always collect workspace, app name, and role (CLI / env only pre-fill defaults)."""
    console.print(
        "\n[bold]Run context[/bold] "
        "(each run asks for these three values; use flags or env vars to pre-fill defaults.)\n"
    )

    w_default = (workspace or os.environ.get("CERBY_WORKSPACE") or "").strip()
    app_prefill = (app_name or os.environ.get("APP_NAME") or "").strip()
    app_prompt_default = (
        app_prefill
        if app_prefill and normalize_provider_filter(app_prefill)
        else "Any"
    )
    r_cli = _maybe_account_role(account_role)
    r_env = _maybe_account_role(os.environ.get("ACCOUNT_ROLE"))
    r_default = r_cli or r_env or "COLLABORATOR"
    role_default = r_default.lower()

    def ask_nonempty(label: str, default: str) -> str:
        while True:
            value = Prompt.ask(label, default=default).strip()
            if value:
                return value
            console.print("[red]This value is required.[/red]")

    ws = ask_nonempty("Cerby workspace (subdomain)", w_default)
    while True:
        app_raw = Prompt.ask(
            "Enter app provider name (Or Any for all accounts)",
            default=app_prompt_default,
        ).strip()
        if not app_raw:
            console.print(
                "[red]Enter a provider name, or Any for all accounts.[/red]"
            )
            continue
        app = normalize_provider_filter(app_raw)
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
    """Show id, name, username, createdAt for confirmation."""
    preview = Table(title="Accounts to sync / act on")
    preview.add_column("id", overflow="fold")
    preview.add_column("name")
    preview.add_column("username")
    preview.add_column("createdAt")
    for acc in accounts:
        preview.add_row(
            str(acc.get("id") or "—"),
            str(acc.get("name") or "—"),
            str(acc.get("username") or "—"),
            str(acc.get("createdAt") or "—"),
        )
    console.print(preview)


def _resolve_new_session_label(session_label: Optional[str]) -> str:
    """CLI ``--session-label`` or interactive optional prompt."""
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
    """
    Resolve work session: optional ``--session-id`` loads that file; otherwise prompt.
    New sessions get an optional label (CLI or prompt).
    """
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
            "[dim]Work session: starting new (no saved sessions for this workspace and app); "
            "accounts will be fetched next.[/dim]\n"
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

    sess_table = Table(title="Saved sessions for this workspace + app")
    sess_table.add_column("#", justify="right")
    sess_table.add_column("Label")
    sess_table.add_column("Session id")
    sess_table.add_column("Last updated")
    sess_table.add_column("Rotated")
    sess_table.add_column("Role-changed")
    for i, entry in enumerate(candidates, start=1):
        lab = entry.label if entry.label else "—"
        sess_table.add_row(
            str(i),
            lab,
            entry.session_id,
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
    client: CerbyApi,
    token: str,
) -> list[dict[str, Any]]:
    """
    Fetch accounts; if the list is empty, explain ambiguity (no accounts vs wrong app)
    and optionally let the user correct APP_NAME and retry.
    """
    while True:
        console.print("[bold]Syncing accounts[/bold] from the API…")
        while True:
            try:
                accounts = client.fetch_all_accounts()
                break
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
        if accounts:
            return accounts

        scope = _provider_filter_label(cfg["APP_NAME"])
        console.print(
            "\n[yellow]The API returned no accounts[/yellow] for "
            f"[bold]{scope}[/bold].\n"
            "That can mean there are no matching accounts yet, "
            "or the provider id does not match Cerby (spelling, underscores, etc.). "
            "You can also use [bold]Any[/bold] to drop the provider filter.\n"
        )
        if (
            Prompt.ask(
                "Try again with a different app / integration name?",
                choices=["y", "n"],
                default="n",
            ).lower()
            != "y"
        ):
            return accounts

        retry_default = (
            cfg["APP_NAME"] if (cfg["APP_NAME"] or "").strip() else "Any"
        )
        while True:
            new_raw = Prompt.ask(
                "Enter app provider name (Or Any for all accounts)",
                default=retry_default,
            ).strip()
            if not new_raw:
                console.print(
                    "[red]Enter a provider name, or Any for all accounts.[/red]"
                )
                continue
            cfg["APP_NAME"] = normalize_provider_filter(new_raw)
            break

        client = CerbyApi(
            workspace=cfg["CERBY_WORKSPACE"],
            app_name=cfg["APP_NAME"],
            account_role=cfg["ACCOUNT_ROLE"],
            token=token,
        )


def _obtain_token(workspace: str, app_name: str) -> str:
    """Reuse stored token when JWT + API probe succeed; otherwise browser login."""
    stored = token_session.load_session()
    if stored and stored["workspace"] == workspace:
        token = stored["access_token"]
        if is_access_token_valid(token):
            probe_client = CerbyApi(
                workspace=workspace,
                app_name=app_name,
                account_role="",
                token=token,
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
                    f"[yellow]Saved token was rejected by the API ({e!s}); opening browser to sign in again…[/yellow]"
                )
        else:
            console.print("[yellow]Saved access token is expired; opening browser to sign in…[/yellow]")
        token_session.clear_session()

    handler = CerbyAuthHandler(workspace)
    console.print("[bold]Authenticating[/bold] in the browser…")
    token = handler.get_access_token()
    token_session.save_session(workspace, token)
    console.print("[green]Access token saved for this workspace.[/green]\n")
    return token


def _run_flow(
    cfg: dict[str, str],
    output: Path,
    fmt: str,
    *,
    session_id: Optional[str] = None,
    session_label: Optional[str] = None,
) -> None:
    token = _obtain_token(cfg["CERBY_WORKSPACE"], cfg["APP_NAME"])

    client = CerbyApi(
        workspace=cfg["CERBY_WORKSPACE"],
        app_name=cfg["APP_NAME"],
        account_role=cfg["ACCOUNT_ROLE"],
        token=token,
    )

    session_tracker = _prompt_work_session_tracker(
        cfg["CERBY_WORKSPACE"],
        cfg["APP_NAME"],
        session_id=session_id,
        session_label=session_label,
    )

    accounts = _fetch_accounts_with_empty_retry(cfg, client, token)
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

    if (
        Prompt.ask(
            "\nProceed with these accounts? (Bulk actions will only affect the accounts above.)",
            choices=["y", "n"],
            default="y",
        ).lower()
        != "y"
    ):
        console.print("[yellow]Aborted by user.[/yellow]\n")
        return

    if fmt == "sqlite":
        account_storage.sync_accounts_sqlite(output, accounts)
    else:
        account_storage.sync_accounts_csv(output, accounts)
    console.print(f"Saved sync to [cyan]{output}[/cyan] ({fmt}).\n")

    api_share_role = share_role_for_api(cfg["ACCOUNT_ROLE"])

    choice = Prompt.ask(
        "What should we do for each account?",
        choices=["rotate", "role", "both", "neither"],
        default="neither",
    )

    if choice == "neither":
        console.print("No API actions performed.")
        return

    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    run_started_at = _utc_now()
    run_rotations: list[dict[str, Any]] = []
    run_role_changes: list[dict[str, Any]] = []

    table = Table(title="Actions")
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
                console.print(
                    f"\n[bold]Account:[/bold] {label}  [dim](id={aid})[/dim]  — "
                    f"[yellow]role → {cfg['ACCOUNT_ROLE']}[/yellow] (all assigned users)"
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
                user_ids = CerbyApi.user_ids_from_assigned_users(member_rows)
                if not user_ids:
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
                snapshots = CerbyApi.role_snapshots_for_account_users(
                    member_rows, str(aid), user_ids
                )
                console.print(
                    f"  [dim]Applying to {len(user_ids)} user(s) in one request…[/dim]"
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
                    table.add_row(
                        label,
                        str(aid),
                        f"role → {cfg['ACCOUNT_ROLE']} ({len(user_ids)} users)",
                        "success",
                    )
                    share_done = True

    console.print()
    console.print(table)

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


_DEFAULT_SYNC_OUTPUT = Path("accounts.sqlite")


def _interactive_sync_impl(
    workspace: Optional[str],
    app_name: Optional[str],
    account_role: Optional[str],
    output: Path,
    fmt: str,
    session_id: Optional[str],
    session_label: Optional[str],
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

    _run_flow(
        cfg,
        out_path,
        fmt_norm,
        session_id=session_id,
        session_label=session_label,
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
        help="Default for the app prompt: Cerby provider id, or ANY (any casing) — omits provider filter.",
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
) -> None:
    """Interactive sync and optional bulk actions on accounts."""
    _interactive_sync_impl(
        workspace,
        app_name,
        account_role,
        output,
        fmt,
        session_id,
        session_label,
    )


@app.callback(invoke_without_command=True)
def _cli_entry(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        _interactive_sync_impl(
            None,
            None,
            None,
            _DEFAULT_SYNC_OUTPUT,
            "sqlite",
            None,
            None,
        )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
