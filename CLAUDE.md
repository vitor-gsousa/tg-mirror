# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-service Telegram mirror bot: it listens on source chats (via Telethon) and forwards new messages to one destination chat, applying configurable URL/regex filters and code-based deduplication along the way. It ships with a small FastAPI admin web UI (password-protected) for configuration, live stats, log tailing, filter management, and ad-hoc read-only SQL against the state DB. Runs as a single container, typically on a self-hosted ARM device (Tanix TX2).

## Conventions

- **Bump the dashboard version marker on every code change.** `DASHBOARD_VERSION` and `DASHBOARD_DEPLOY_NOTE` (defaults in `mirror.py`, near the top `# ================= CONFIG =================` section) are shown in the admin UI footer (`templates/index.html`) as the only visible indicator of what's actually deployed on the device. When you change `mirror.py`, `repository.py`, or `services.py`, update `DASHBOARD_VERSION`'s default to the current date (`YYYY.MM.DD`) and `DASHBOARD_DEPLOY_NOTE`'s default to a short summary of the change — otherwise the dashboard keeps showing a stale version after deploy, making it impossible to confirm a fix actually shipped to the self-hosted runner.

## Commands

```bash
# Install deps (runtime + dev/test extras)
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Run all tests
pytest

# Run a single test
pytest tests/test_mirror_helpers.py::test_parse_cleanup_time_valid

# Run locally outside Docker (needs config/.env with required vars, see below)
python mirror.py

# Docker (production path)
docker compose build            # add --no-cache to rebuild from scratch
docker compose up -d
```

There is no lint config in the repo; don't assume flake8/ruff/black conventions beyond what the existing code already follows.

### Environment / running locally

`mirror.py` requires `API_ID`, `API_HASH`, `DEST_CHAT`, `SOURCE_CHATS`, `ADMIN_PASSWORD` to be set (raises `RuntimeError` at import time otherwise) — this is why `tests/test_mirror_helpers.py` sets dummy env vars via `os.environ.setdefault(...)` in its `mirror_module` fixture *before* importing `mirror`. Any new test that imports `mirror` must go through that same fixture rather than importing the module directly.

It looks for its env file at `config/.env` (local dev) or `/config/.env` (Docker), and its data dir (SQLite DB, stats.json, app.log) at `./data` or `/data`. See `config/.env.example` for the full variable list.

## Architecture

Three Python modules, split by responsibility:

- **`repository.py`** — `SQLiteRepository`: the only place that touches SQL. Wraps a shared `sqlite3.Connection`/`Cursor`/`threading.Lock` (WAL mode) and exposes typed methods for the `processed`, `channels`, `message_codes`, and `url_filters` tables, including the schema creation and inline migrations (`init_schema`, called once at startup). Every method takes the mutex — this repo is used from both the asyncio event loop thread (via `asyncio.to_thread`) and FastAPI's sync/threaded handlers, so all DB access must go through it rather than touching `conn`/`cur` directly.
- **`services.py`** — pure-ish business logic layer (stats aggregation, code extraction/normalization/deduplication, filter/query pass-through) that takes a `SQLiteRepository` instance as an explicit argument rather than importing global state. This is what `tests/test_mirror_helpers.py` and `mirror.py`'s thin wrapper functions actually delegate to.
- **`mirror.py`** — the entry point. Wires everything together: env/config loading, logging (rotating file + stream handler), the Telethon `TelegramClient`, the shared SQLite connection + `SQLiteRepository`, the FastAPI app (routes, session auth), and process lifecycle. Most module-level functions here (`is_processed`, `mark_processed`, `extract_codes`, etc.) are thin pass-throughs to `services.py`/`repository.py` kept for backward-compatible naming and are what tests patch via `monkeypatch.setattr(mirror_module, ...)`.

### Concurrency model

Three threads share process state:
1. **Main thread** — runs the FastAPI/uvicorn app (admin web UI, sync route handlers).
2. **Bot thread** (`run_bot`) — owns its own asyncio event loop running the Telethon client and the `handler()` message callback.
3. **Cleanup scheduler thread** (`cleanup_scheduler`) — polls every 60s and runs retention cleanup once per day at the configured `CLEANUP_TIME`.

Shared mutable state crossing these threads: the SQLite connection (guarded by `db_mutex`, enforced inside `SQLiteRepository`), the in-memory `stats` dict (guarded by asyncio `stats_lock`, only touched from the bot-thread event loop via `asyncio.to_thread`), and `SOURCE_CHATS` (mutated by the `/add-source-chat` route via `global`). `apply_filters` (which can make blocking HTTP calls to expand shortened URLs) runs off a dedicated `http_executor` ThreadPoolExecutor so it doesn't block the bot's event loop.

### Message handling flow (`handler()` in mirror.py)

1. Skip if `(chat_id, msg_id)` already in `processed` table.
2. Apply configured URL/regex filters to the message text (off-loop, via `http_executor`).
3. Extract & dedupe candidate codes from the filtered text using `DUP_CODE_REGEX`.
4. If any code was already seen in `message_codes`, mark the message processed and skip (no forward).
5. Otherwise mark the new codes as seen *before* forwarding (closes a race window between concurrent messages with the same code), forward the message/media to `DEST_CHAT`, and roll back (`delete_codes`) if forwarding fails.
6. Mark the message processed and increment the stats counter.

### Web UI

Session-cookie auth (`SessionMiddleware`, single shared `ADMIN_PASSWORD`, no per-user accounts) gates all page routes (`require_page_login`, redirects to `/login`) and JSON/API-style routes (`require_api_login`, 401s). Templates are Jinja2 (`templates/index.html`, `templates/login.html`), served from a hardcoded `/app/templates` path (Docker layout — see `Dockerfile`). The `/execute-query` endpoint only allows `SELECT` statements (`validate_readonly_query`), enforced in `services.py`.

## CI/CD

`.github/workflows/deploy.yml` builds and pushes a `linux/arm/v7` image to `ghcr.io` on every push to `main`, then deploys via a `self-hosted` runner that pulls and restarts the compose stack on the target device. There is no test/lint gate in this workflow — running `pytest` locally before pushing to `main` is on you.
