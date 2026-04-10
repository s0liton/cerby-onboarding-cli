"""Persist synced account payloads to CSV or SQLite."""

from __future__ import annotations

import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _account_id(account: dict[str, Any]) -> str | None:
    return (
        account.get("id")
        or account.get("accountId")
        or account.get("account_id")
    )


def sync_accounts_sqlite(db_path: Path, accounts: list[dict[str, Any]]) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                account_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                synced_at TEXT NOT NULL
            )
            """
        )
        now = datetime.now(timezone.utc).isoformat()
        for acc in accounts:
            aid = _account_id(acc)
            if not aid:
                continue
            conn.execute(
                """
                INSERT OR REPLACE INTO accounts (account_id, payload, synced_at)
                VALUES (?, ?, ?)
                """,
                (str(aid), json.dumps(acc), now),
            )
        conn.commit()
    finally:
        conn.close()
    return len(accounts)


def sync_accounts_csv(csv_path: Path, accounts: list[dict[str, Any]]) -> int:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f, fieldnames=["account_id", "payload", "synced_at"], extrasaction="ignore"
        )
        w.writeheader()
        for acc in accounts:
            aid = _account_id(acc)
            if not aid:
                continue
            w.writerow(
                {
                    "account_id": str(aid),
                    "payload": json.dumps(acc, ensure_ascii=False),
                    "synced_at": now,
                }
            )
    return len(accounts)
