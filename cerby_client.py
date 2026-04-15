from __future__ import annotations

import json
from typing import Any, Callable, Optional

import requests

_VERBOSE_BODY_MAX = 10_000


def _redact_headers(headers: dict[str, str]) -> dict[str, str]:
    h = dict(headers)
    if h.get("Authorization"):
        h["Authorization"] = "Bearer <redacted>"
    return h


def _truncate_body(text: str, limit: int = _VERBOSE_BODY_MAX) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n... ({len(text) - limit} bytes truncated)"

SHARE_ROLE_BY_CLI: dict[str, str] = {
    "OWNER": "owner",
    "COLLABORATOR": "collaborator",
}


def share_role_for_api(cli_role_upper: str) -> str:
    return SHARE_ROLE_BY_CLI.get(cli_role_upper.strip().upper(), cli_role_upper.lower())


def normalize_provider_filter(app_name: str) -> str:
    # Empty string = no provider filter on the accounts API (user said "Any").
    s = (app_name or "").strip()
    if s.casefold() == "any":
        return ""
    return s


def parse_provider_specs(raw: str) -> list[str]:
    # Comma-separated list; Cerby only takes one provider per request, so we fan out calls.
    parts = [p.strip() for p in (raw or "").split(",") if p.strip()]
    if not parts:
        return [""]
    return [normalize_provider_filter(p) for p in parts]


def _account_row_id(row: dict[str, Any]) -> str:
    return str(row.get("id") or row.get("accountId") or row.get("account_id") or "")


def fetch_accounts_merged(
    workspace: str,
    provider_specs: list[str],
    account_role: str,
    token: str,
    *,
    verbose_log: Optional[Callable[[str], None]] = None,
) -> list[dict[str, Any]]:
    """One paginated list call per provider segment, merged (ids deduped; Cerby ids are global)."""
    seen: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for spec in provider_specs:
        api = CerbyApi(
            workspace, spec, account_role, token, verbose_log=verbose_log
        )
        rows = api.fetch_all_accounts()
        for row in rows:
            aid = _account_row_id(row)
            if aid:
                if aid not in seen:
                    seen[aid] = row
                    order.append(aid)
            else:
                key = f"__noid_{id(row)}"
                if key not in seen:
                    seen[key] = row
                    order.append(key)
    return [seen[k] for k in order]


def cerby_role_to_display_role(raw: str) -> str:
    # Cerby uses strings like account_owner; we normalize to OWNER / COLLABORATOR.
    s = (raw or "").strip()
    if not s:
        return ""
    low = s.lower()
    if low in ("account_collaborator", "collaborator"):
        return "COLLABORATOR"
    if low in ("account_owner", "owner"):
        return "OWNER"
    u = s.upper()
    if u == "COLLABORATOR":
        return "COLLABORATOR"
    if u == "OWNER":
        return "OWNER"
    return u


class CerbyApi:
    def __init__(
        self,
        workspace: str,
        app_name: str,
        account_role: str,
        token: str,
        *,
        verbose_log: Optional[Callable[[str], None]] = None,
    ):
        self.workspace = workspace
        self.app_name = normalize_provider_filter(app_name)
        self.account_role = account_role
        self.token = token
        self.api_base_url = "https://api.cerby.com/v1/"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Cerby-Workspace": workspace,
        }
        self._verbose_log = verbose_log

    def _request(
        self,
        method: str,
        url: str,
        *,
        json_body: Any | None = None,
        timeout: int = 120,
    ) -> requests.Response:
        if self._verbose_log:
            lines = [
                f"{method} {url}",
                json.dumps(_redact_headers(self.headers), indent=2),
            ]
            if json_body is not None:
                lines.append(json.dumps(json_body, indent=2, default=str))
            self._verbose_log("\n".join(lines))
        resp = requests.request(
            method,
            url,
            headers=self.headers,
            json=json_body,
            timeout=timeout,
        )
        if self._verbose_log:
            status_line = f"<= {resp.status_code} {resp.reason or ''}".strip()
            body = _truncate_body(resp.text)
            try:
                parsed = resp.json()
                body = _truncate_body(json.dumps(parsed, indent=2, default=str))
            except (ValueError, TypeError):
                pass
            self._verbose_log(f"{status_line}\n{body}")
        return resp

    def _extract_account_list(self, payload: Any) -> list[dict[str, Any]]:
        # List endpoints are annoyingly inconsistent (array vs {data: [...]}).
        if payload is None:
            return []
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if not isinstance(payload, dict):
            return []
        for key in ("data", "accounts", "results", "items"):
            v = payload.get(key)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
        if isinstance(payload.get("account"), dict):
            return [payload["account"]]
        return []

    @staticmethod
    def _total_results(body: Any) -> int | None:
        if isinstance(body, dict):
            raw = body.get("totalResults")
            if raw is not None:
                try:
                    return int(raw)
                except (TypeError, ValueError):
                    return None
        return None

    def _accounts_list_query(self, *, limit: int, start_index: int | None = None) -> str:
        q = (
            f"{self.api_base_url}accounts"
            "?excludeBusinessAssets=true"
            "&includePendingActions=true"
            f"&limit={limit}"
            "&sortBy=newest"
        )
        if start_index is not None:
            q += f"&startIndex={start_index}"
        if self.app_name:
            q += f"&provider={requests.utils.quote(self.app_name, safe='')}"
        return q

    def probe_token(self) -> tuple[bool, int]:
        # Cheap GET; 401 vs 403 matters to the caller so we don't raise on those.
        endpoint = self._accounts_list_query(limit=1)
        resp = self._request("GET", endpoint, timeout=60)
        if resp.status_code == 401:
            return False, 401
        if resp.status_code == 403:
            return False, 403
        resp.raise_for_status()
        return True, resp.status_code

    def fetch_all_accounts(self) -> list[dict[str, Any]]:
        # Walk pages until totalResults, short page, or empty batch.
        all_rows: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        start_index = 1
        limit = 100
        max_pages = 5000
        pages = 0

        while pages < max_pages:
            pages += 1
            endpoint = self._accounts_list_query(limit=limit, start_index=start_index)

            resp = self._request("GET", endpoint, timeout=120)
            resp.raise_for_status()
            body = resp.json()
            batch = self._extract_account_list(body)
            for row in batch:
                aid = _account_row_id(row)
                if aid:
                    if aid in seen_ids:
                        continue
                    seen_ids.add(aid)
                all_rows.append(row)

            total = self._total_results(body)
            if total is not None and len(all_rows) >= total:
                break
            if not batch:
                break
            if len(batch) < limit:
                break
            start_index += len(batch)

        return all_rows

    def rotate_password(self, account_id: str) -> requests.Response:
        url = f"{self.api_base_url}accounts/{account_id}/automation-jobs"
        payload: dict[str, Any] = {
            "automationType": "password_rotation",
            "data": {"logoutAllSessions": False},
            "meta": {"logoutAllSessions": False},
        }
        return self._request("POST", url, json_body=payload, timeout=120)

    def fetch_account_assigned_users(self, account_id: str) -> list[dict[str, Any]]:
        # members/users/search, paginated
        all_rows: list[dict[str, Any]] = []
        start_index = 1
        count = 20
        while True:
            url = f"{self.api_base_url}accounts/{account_id}/members/users/search"
            payload: dict[str, Any] = {
                "search": "",
                "count": count,
                "startIndex": start_index,
                "guests": False,
                "lastUsed": "true",
            }
            resp = self._request("POST", url, json_body=payload, timeout=120)
            resp.raise_for_status()
            body = resp.json()
            batch = body.get("users") if isinstance(body, dict) else []
            if not isinstance(batch, list):
                batch = []
            all_rows.extend(batch)

            total_results = self._total_results(body)
            if total_results is not None:
                if len(all_rows) >= total_results or not batch:
                    break
            elif not batch or len(batch) < count:
                break
            start_index += len(batch)
        return all_rows

    @staticmethod
    def user_ids_from_assigned_users(users: list[dict[str, Any]]) -> list[str]:
        seen: dict[str, None] = {}
        for u in users:
            uid = u.get("userId") or u.get("user_id")
            if uid:
                seen[str(uid)] = None
        return list(seen.keys())

    @staticmethod
    def role_snapshots_for_account_users(
        member_rows: list[dict[str, Any]],
        account_id: str,
        user_ids: list[str],
    ) -> list[dict[str, Any]]:
        # Grab current role from roleAssignations before we POST share.
        want = {str(x) for x in user_ids}
        aid = str(account_id)
        out: list[dict[str, Any]] = []
        for u in member_rows:
            uid = u.get("userId") or u.get("user_id")
            if not uid or str(uid) not in want:
                continue
            prev = ""
            for ra in u.get("roleAssignations") or []:
                if not isinstance(ra, dict):
                    continue
                if (
                    str(ra.get("resourceId")) == aid
                    and str(ra.get("resourceType") or "").lower() == "account"
                ):
                    prev = str(ra.get("role") or "")
                    break
            out.append(
                {
                    "user_id": str(uid),
                    "email": u.get("email"),
                    "first_name": u.get("firstName"),
                    "last_name": u.get("lastName"),
                    "previous_role": cerby_role_to_display_role(prev),
                }
            )
        return out

    def change_role(
        self, account_id: str, user_ids: list[str], app_role: str
    ) -> requests.Response:
        url = f"{self.api_base_url}accounts/share"
        payload = {
            "userIds": user_ids,
            "role": app_role,
            "accountId": account_id,
        }
        return self._request("POST", url, json_body=payload, timeout=120)

    def describe_account(self, account: dict[str, Any]) -> str:
        for key in ("name", "username", "email", "title", "id"):
            v = account.get(key)
            if v:
                return str(v)
        return json.dumps(account)[:80]
