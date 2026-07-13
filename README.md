## Introduction

A tool that helps you onboard accounts into Cerby and get them into a proper security posture immediatley. When a user onboards their own credentials into Cerby, they gain "owner" access to those credentials, and STILL know their current password. You typically want to make sure the user does not have that level of permission to the account, and you want to change the password to something secure as soon as possible. This tool does both for you.

### Two modes of operation:

##### Manual mode

- You provide the accounts to act on via a CSV file.

##### Automated mode

- The tool acts like a listener on the Cerby API and processes accounts as they are added.

##### Service mode

- Same as Automated mode, but built to run as a liux service for long term automated actions.

## Install

**Requires:** Python 3.12+, and a Cerby API token with: Read Accounts, Write Accounts, Write Automation Jobs scopes.

```bash
git clone <repository-url>
cd cerby-onboarding-cli
uv sync
uv run playwright install chromium
```

Install with pip instead of uv

```bash
python3.12 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e .
playwright install chromium
```

## Manual and Automated Interactive mode

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

**Work session** — service mode uses `session_name` from config (default: `production`) and creates `assets/work_sessions/<name>.json` automatically on first start. Interactive mode still prompts for session names as before.

**systemd:** set `WorkingDirectory` to the project root and adjust paths in `deploy/cerby-onboarding.service`, then enable the unit. Service logs are written to `log/cerby-onboarding.log` and mirrored to stderr (visible in `journalctl`).

**Monitoring:** `log/cerby-onboarding.log`, `journalctl -u cerby-onboarding.service`, and `running_report.json` (updated while the service runs).

## Runtime files

| Path                         | Purpose                                    |
| ---------------------------- | ------------------------------------------ |
| `assets/.cerby_session.json` | API access token                           |
| `assets/work_sessions/`      | Per-run progress (rotations, role changes) |
| `log/`                       | Application logs                           |
| `running_report.json`        | Live service summary                       |
| `cerby_run_report_*.json`    | Optional exports from interactive runs     |

Set `CERBY_DATA_DIR` to change the project root for logs/reports. Set `CERBY_ASSETS_DIR` to relocate `assets/` (must be the `assets` directory itself, not `assets/work_sessions`).

Service mode pins the project root from the config file path (`config/config.yaml` → parent of `config/`), so asset paths do not depend on the process working directory.

## Help

```bash
uv run cerby-onboarding --help
uv run cerby-onboarding run --help
uv run cerby-onboarding service --help
```
