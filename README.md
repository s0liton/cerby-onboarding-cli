# Cerby Onboarding CLI

Bulk rotate passwords and change user roles on Cerby accounts after self-onboarding. The tool fetches accounts that match your filters, then rotates passwords and/or downgrades owners to collaborators.

**Requires:** Python 3.12+, and a Cerby user who is a **super administrator with all access mode**.

## Install

```bash
git clone <repository-url>
cd cerby-onboarding-cli
uv sync
uv run playwright install chromium
```

<details>
<summary>Install with pip instead of uv</summary>

```bash
python3.12 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e .
playwright install chromium
```

</details>

Run all commands below from the **repo root**.

## Interactive mode

Starts a step-by-step wizard. Choose **manual** (pick accounts once) or **automated** (poll for new accounts — good for short onboarding windows).

```bash
uv run cerby-onboarding          # same as `run`
uv run cerby-onboarding run \
  --workspace mycompany \
  --app-name "slack,zoom" \
  --account-role COLLABORATOR \
  --session-name production-onboarding
```

First run opens a browser to sign in. The token is saved to `assets/.cerby_session.json`.

**Work sessions** (`assets/work_sessions/`) are identified by a **unique session name** you choose at creation. They track which accounts were already acted on, so re-runs skip them. Names cannot be reused or overwritten.

At the end of a run you can export a JSON report. Logs go to `log/cerby-onboarding.log`.

## Service mode

For long-running, unattended operation (e.g. systemd). Configuration lives in `config/config.yaml`.

```bash
cp config/config.example.yaml config/config.yaml
# Edit: workspace, app_names, session_name, actions, poll_interval, etc.
uv run cerby-onboarding service --config config/config.yaml
```

**Authentication** — use either:

- A one-time `access_token` in `config/config.yaml` (removed from the file on first start), or
- An existing `assets/.cerby_session.json` from a prior interactive run

Create the work session interactively first (pick a unique **session name**), then set the same `session_name` in config.

**systemd:** adjust paths in `deploy/cerby-onboarding.service`, then enable the unit.

**Monitoring:** `log/cerby-onboarding.log` and `running_report.json` (updated while the service runs).

## Runtime files

| Path | Purpose |
|------|---------|
| `assets/.cerby_session.json` | API access token |
| `assets/work_sessions/` | Per-run progress (rotations, role changes) |
| `log/` | Application logs |
| `running_report.json` | Live service summary |
| `cerby_run_report_*.json` | Optional exports from interactive runs |

Set `CERBY_DATA_DIR` to change the project root for logs/reports. Set `CERBY_ASSETS_DIR` to relocate `assets/`.

## Help

```bash
uv run cerby-onboarding --help
uv run cerby-onboarding run --help
uv run cerby-onboarding service --help
```
