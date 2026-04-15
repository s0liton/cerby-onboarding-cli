# Cerby Onboarding CLI

CLI for syncing accounts from Cerby, rotating passwords, and bulk-updating user roles after self-onboarding (dashboard or browser extension). Sign-in uses a real browser; the tool stores only a short-lived API access token locally.

## Introduction

This tool helps you rotate passwords and change user roles for accounts in Cerby that were onboarded by end users. It can operate on accounts added from the Cerby dashboard or captured via the extension. When users add their own accounts or Cerby captures their credentials, the user is automatically given OWNER permissions to that account. This poses a security risk, since the user can modify the details of the account, share the account with others, and can reveal the password.

This tool allows you to quickly act on multiple accounts at once so that:

- The account password is rotated to a stronger value managed by Cerby.
- Users are not able to reveal the password or change sensitive account settings when given collaborator access.

## Features

- Rotate Cerby account passwords for one or more integration types.
- Change Cerby account assigned user's roles to "collaborator" or "owner".
- Interactive browser login (Local credentials or whichever IdP is connected to your Cerby Workspace).
- Work sessions — remember which accounts were already rotated or role-changed so you can safely re-run as more accounts are onboarded.
- Per-run or overall session reports.
- Automated Mode - Listens on an interval for new accounts of the specified types, and performs the actions you desire.
- Verbose logging.

## Requirements

- **Python** 3.12 or newer.
- **Cerby permissions** — the Cerby user whose session supplies the API token must be a **Super administrator** with **all access mode** enabled,

## Installation

Use **either** [uv](https://docs.astral.sh/uv/) **or** a normal **Python 3.12+ virtual environment with pip**.

### 1. Clone and enter the repository

```bash
git clone <repository-url>
cd <checkout-directory>
```

### 2a. Install with uv

1. Install uv (see the [uv install guide](https://docs.astral.sh/uv/getting-started/installation/)).
2. From the repo root, create the environment and install dependencies plus this project in editable form:

```bash
uv sync
```

1. Install the Chromium build used for browser sign-in:

```bash
uv run playwright install chromium
```

### 2b. Install with pip in a virtual environment

1. Create a venv with Python 3.12+:

```bash
python3.12 -m venv .venv
```

1. **Activate** the venv:

| Platform             | Command                      |
| -------------------- | ---------------------------- |
| macOS / Linux        | `source .venv/bin/activate`  |
| Windows (cmd.exe)    | `.venv\Scripts\activate.bat` |
| Windows (PowerShell) | `.venv\Scripts\Activate.ps1` |

1. Upgrade pip and install this project in editable mode:

```bash
python -m pip install --upgrade pip
python -m pip install -e .
```

1. With the venv **still activated**, install Chromium for Playwright:

```bash
playwright install chromium
```

## How to run

### If you use uv

```bash
# Interactive flow (default — same as the `run` subcommand)
uv run python main.py

# Same thing via the installed console script
uv run cerby-onboarding

# Help
uv run cerby-onboarding --help
uv run cerby-onboarding run --help
```

### If you use a pip virtual environment

**Activate** the venv, then run:

```bash
# Interactive flow (default)
python main.py

# Console script (after pip install -e .)
cerby-onboarding

# Explicit subcommand
python main.py run

# Help
cerby-onboarding --help
cerby-onboarding run --help
```

## Adding `cerby-onboarding` to your PATH (optional)

If you want to easily launch the tool using the `cerby-inboarding` command from your shell, add it to your PATH.

| OS            | Folder to add to `PATH` (replace `<repo>` with your checkout’s absolute path) |
| ------------- | ----------------------------------------------------------------------------- |
| macOS / Linux | `<repo>/.venv/bin`                                                            |
| Windows       | `<repo>\.venv\Scripts`                                                        |

### macOS and Linux

1. Open your shell's config (`~/.zshrc` for zsh, `~/.bashrc` for bash).
2. Append (edit the path to match your machine):

```bash
 export PATH="/absolute/path/to/your/checkout/.venv/bin:$PATH"
```

3. Reload the file (`source ~/.zshrc`, etc.) or open a new terminal.
4. Check: `cerby-onboarding --help`

### Windows

1. Open **Settings → System → About → Advanced system settings** (or search **“environment variables”**).
2. **Environment Variables…** → under _User variables_, select **Path** → **Edit** → **New**.
3. Add the **Scripts** folder, e.g. `C:\absolute\path\to\your\checkout\.venv\Scripts` (the folder, not the `.exe`).
4. Confirm with **OK**, then open a **new** Command Prompt or PowerShell.
5. Check: `cerby-onboarding --help`
