# mirror.py

import os
import json
import re
import sqlite3
import threading
import time
import signal
import secrets
import asyncio
import atexit
import requests
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, timezone
from typing import Any

from dotenv import load_dotenv, dotenv_values
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from asyncio import Lock

from fastapi import (
    FastAPI, Request, Form, Depends, HTTPException, status
)
from fastapi.responses import (
    RedirectResponse, HTMLResponse, PlainTextResponse, JSONResponse
)
from starlette.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

import uvicorn

from repository import SQLiteRepository
import services as app_services


# ================= CONFIG =================

# Support both Docker and local execution
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_config_env_path = os.path.join(SCRIPT_DIR, "config", ".env")
ENV_PATH = _config_env_path if os.path.exists(
    _config_env_path) else "/config/.env"
DATA_DIR = os.path.join(SCRIPT_DIR, "data") if not os.path.exists(
    "/data") else "/data"
STATS_PATH = f"{DATA_DIR}/stats.json"
DB_PATH = f"{DATA_DIR}/state.db"
LOG_PATH = f"{DATA_DIR}/app.log"

os.makedirs(DATA_DIR, exist_ok=True)

load_dotenv(ENV_PATH)

REQUIRED_VARS = ["API_ID", "API_HASH",
                 "DEST_CHAT", "SOURCE_CHATS", "ADMIN_PASSWORD"]

for var in REQUIRED_VARS:
    if not os.getenv(var):
        raise RuntimeError(f"Missing env var: {var}")


API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]

DEST_CHAT = int(os.environ["DEST_CHAT"])
SOURCE_CHATS = [
    int(x.strip())
    for x in os.environ["SOURCE_CHATS"].split(",")
    if x.strip()
]

ADMIN_PASSWORD = os.environ["ADMIN_PASSWORD"]

SESSION_STRING = os.environ.get("SESSION_STRING")
SESSION_NAME = os.environ.get("SESSION", "mirror")

WEB_PORT = int(os.getenv("WEB_PORT", "8000"))
CLEANUP_DAYS_DEFAULT = 30
CLEANUP_TIME_DEFAULT = "00:05"
DASHBOARD_VERSION = os.getenv("DASHBOARD_VERSION", "2026.07.02")
DASHBOARD_DEPLOY_NOTE = os.getenv(
    "DASHBOARD_DEPLOY_NOTE",
    "Refactor: repository.py + services.py"
)


# ================= LOGGING =================

logger = logging.getLogger("tg-mirror")
logger.setLevel(logging.INFO)

if not logger.handlers:
    _file_handler = RotatingFileHandler(
        LOG_PATH,
        maxBytes=2 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8"
    )
    _file_handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")
    )

    _stream_handler = logging.StreamHandler()
    _stream_handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")
    )

    logger.addHandler(_file_handler)
    logger.addHandler(_stream_handler)
logger.propagate = False

CODE_REGEX_DEFAULT = r"\b[A-Za-z0-9]{6,}\b"
CODE_REGEX = os.getenv("DUP_CODE_REGEX", CODE_REGEX_DEFAULT)
try:
    CODE_PATTERN = re.compile(CODE_REGEX)
except re.error as exc:
    logger.error("Invalid DUP_CODE_REGEX '%s': %s", CODE_REGEX, exc)
    CODE_PATTERN = None


# ================= LOCKS =================

db_mutex = threading.Lock()
stats_lock = Lock()


# ================= STATS =================

def load_stats() -> dict[str, Any]:
    """Load service metrics from disk.

    Returns:
        dict[str, Any]: Persisted stats. Falls back to default values when the
        stats file is missing or unreadable.
    """
    if not os.path.exists(STATS_PATH):
        return {"messages": 0, "status": "starting"}

    try:
        with open(STATS_PATH) as f:
            return json.load(f)
    except Exception:
        return {"messages": 0, "status": "reset"}


def save_stats(data: dict[str, Any]):
    """Persist service metrics atomically to disk.

    Args:
        data (dict[str, Any]): Stats payload to be saved.
    """
    with open(STATS_PATH, "w") as f:
        json.dump(data, f)
        f.flush()
        os.fsync(f.fileno())


stats = load_stats()
stats["status"] = "running"
save_stats(stats)
logger.info("Service started")


# ================= TELEGRAM =================

if SESSION_STRING:
    client = TelegramClient(
        StringSession(SESSION_STRING),
        API_ID,
        API_HASH
    )
else:
    client = TelegramClient(
        SESSION_NAME,
        API_ID,
        API_HASH
    )


# ================= DATABASE =================

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL;")

cur = conn.cursor()


repository = SQLiteRepository(conn, cur, db_mutex)


def utc_now_string() -> str:
    """Return the current UTC timestamp formatted for SQLite."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def init_db():
    """Create required tables and apply lightweight schema migrations.

    Initializes structures used for processed messages, channel labels,
    deduplication codes, and URL filters.
    """

    repository.init_schema()


init_db()


# ================= FASTAPI =================

app = FastAPI()

SESSION_SECRET = os.getenv("SESSION_SECRET", secrets.token_hex(32))
if os.getenv("SESSION_SECRET") is None:
    logger.warning("SESSION_SECRET not set, sessions will be lost on restart.")

app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="tg_mirror_session",
    max_age=3600 * 24 * 7  # 1 week
)

templates = Jinja2Templates(directory="/app/templates")


# ================= AUTH =================

@app.get("/login", response_class=HTMLResponse, tags=["auth"])
async def login_form(request: Request):
    """Render the login page or redirect authenticated users to home.

    Args:
        request (Request): Incoming HTTP request.

    Returns:
        RedirectResponse | HTMLResponse: Redirect to home when the user is
        already authenticated, otherwise the login template.
    """
    if request.session.get("authenticated"):
        return RedirectResponse(url="/")
    return templates.TemplateResponse(request, "login.html")


@app.post("/login", tags=["auth"])
async def handle_login(request: Request, password: str = Form(...)):
    """Authenticate a user session based on the admin password.

    Args:
        request (Request): Incoming HTTP request.
        password (str): Password submitted from the login form.

    Returns:
        RedirectResponse | HTMLResponse: Redirect to home on success or render
        the login template with an error message on failure.
    """
    if password == ADMIN_PASSWORD:
        request.session["authenticated"] = True
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    else:
        return templates.TemplateResponse(
            request, "login.html", {"error": "Password inválida"}
        )


@app.post("/logout", tags=["auth"])
async def handle_logout(request: Request):
    """Invalidate the current session and redirect to the login page.

    Args:
        request (Request): Incoming HTTP request.

    Returns:
        RedirectResponse: Redirect to the login route.
    """
    request.session.clear()
    return RedirectResponse(
        url="/login", status_code=status.HTTP_303_SEE_OTHER
    )


def require_page_login(request: Request):
    """Ensure page routes are accessed only by authenticated users.

    Args:
        request (Request): Incoming HTTP request.

    Raises:
        HTTPException: 307 redirect to the login page when unauthenticated.
    """
    if not request.session.get("authenticated"):
        raise HTTPException(
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
            headers={"Location": "/login"},
        )


def require_api_login(request: Request):
    """Ensure API routes are accessed only by authenticated users.

    Args:
        request (Request): Incoming HTTP request.

    Raises:
        HTTPException: 401 error when the session is not authenticated.
    """
    if not request.session.get("authenticated"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )


# ================= HELPERS =================

def get_stats() -> dict[str, Any]:
    """Read runtime metrics from disk with graceful fallback.

    Returns:
        dict[str, Any]: Stats payload or an error-safe default structure.
    """

    if not os.path.exists(STATS_PATH):
        return {"messages": 0, "status": "unknown"}

    try:
        with open(STATS_PATH) as f:
            return json.load(f)
    except Exception:
        return {"messages": 0, "status": "error"}


def save_env(data: dict[str, Any]):
    """Write environment configuration values to the .env file.

    Args:
        data (dict[str, Any]): Key-value pairs to persist.
    """

    with open(ENV_PATH, "w") as f:
        for k, v in data.items():
            if v is not None:
                f.write(f"{k}={v}\n")


def load_env_config() -> dict[str, Any]:
    """Load configuration values from the .env file.

    Returns:
        dict[str, Any]: Environment configuration mapping.
    """
    return dict(dotenv_values(ENV_PATH))


def save_env_config(config: dict[str, Any]):
    """Persist environment configuration and enforce required secrets.

    Args:
        config (dict[str, Any]): Environment configuration mapping.
    """
    config["ADMIN_PASSWORD"] = ADMIN_PASSWORD
    save_env(config)


def parse_chat_id_list(value: str) -> list[int]:
    """Parse a comma-separated list of chat IDs.

    Args:
        value (str): Raw chat ID list.

    Returns:
        list[int]: Parsed chat IDs.
    """
    return [
        int(chat_id.strip())
        for chat_id in value.split(",")
        if chat_id.strip()
    ]


def get_channel_stats() -> list[dict[str, Any]]:
    """Build per-channel message counters for dashboard rendering.

    Returns:
        list[dict[str, Any]]: Ordered channel stats including chat ID, label,
        and processed message count.
    """

    return app_services.get_channel_stats(repository, SOURCE_CHATS)


def get_filters() -> list[tuple[Any, ...]]:
    """Return URL filter rules sorted by their execution order.

    Returns:
        list[tuple[Any, ...]]: Filter tuples as
        (id, pattern, replacement).
    """
    return app_services.get_filters(repository)


def move_filter(filter_id: int, move_up: bool):
    """Move one filter up or down in execution order.

    Args:
        filter_id (int): Filter identifier.
        move_up (bool): True to move up, False to move down.
    """
    app_services.move_filter(repository, filter_id, move_up)


def _expand_url(session: requests.Session, url: str) -> str:
    """Resolve redirects for one URL and normalize Amazon product links.

    Args:
        session (requests.Session): Active HTTP session used for requests.
        url (str): URL to expand.

    Returns:
        str: Final URL after redirects. Returns the original URL when
        expansion fails.
    """
    try:
        resp = session.get(url, allow_redirects=True, timeout=10, stream=True)
        final_url = resp.url
        resp.close()

        # Only clean parameters if it is a product link (/dp/ or /gp/)
        if ("/dp/" in final_url or "/gp/" in final_url) and "?" in final_url:
            final_url = final_url.split("?")[0]

        return final_url
    except Exception as e:
        logger.error("Failed to expand %s: %s", url, e)
        return url


def apply_filters(text: str) -> str:
    """Apply configured URL and regex transformations to message text.

    Args:
        text (str): Original message text.

    Returns:
        str: Transformed message text.
    """
    if not text:
        return text

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    for _, pattern, replacement in get_filters():
        try:
            if replacement == "amz":
                urls = set(re.findall(pattern, text))
                if not urls:
                    continue

                with requests.Session() as session:
                    session.headers.update(headers)
                    for url in urls:
                        final_url = _expand_url(session, url)
                        if final_url != url:
                            text = text.replace(url, final_url)
                            logger.info("Expanded %s -> %s", url, final_url)
            else:
                old_text = text
                text = re.sub(pattern, replacement, text)
                if old_text != text:
                    logger.info("Filter matched: '%s'", pattern)
        except Exception as e:
            logger.error("Regex error '%s': %s", pattern, e)
    return text


def tail_file(path: str, max_lines: int = 200) -> str:
    """Read the tail of a text file.

    Args:
        path (str): File path to read.
        max_lines (int): Number of lines to return from the end.

    Returns:
        str: File tail content, or a user-friendly error message.
    """

    if not os.path.exists(path):
        return "No logs yet."

    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
            return "".join(lines[-max_lines:])
    except Exception as e:
        return f"Failed to read logs: {e}"


# ================= TELEGRAM HANDLER =================

def is_processed(chat_id: int, msg_id: int) -> bool:
    """Check whether a source message was already mirrored.

    Args:
        chat_id (int): Source chat identifier.
        msg_id (int): Source message identifier.

    Returns:
        bool: True when the message is already processed.
    """

    return app_services.is_processed(repository, chat_id, msg_id)


def mark_processed(chat_id: int, msg_id: int):
    """Persist a source message as processed.

    Args:
        chat_id (int): Source chat identifier.
        msg_id (int): Source message identifier.
    """

    app_services.mark_processed(repository, chat_id, msg_id)


def cleanup_processed(days: int) -> int:
    """Delete processed-message rows older than the configured retention.

    Args:
        days (int): Retention period in days.

    Returns:
        int: Number of deleted rows.
    """

    return app_services.cleanup_processed(repository, days)


def cleanup_code_cache() -> int:
    """Clear all cached deduplication codes.

    Returns:
        int: Number of deleted rows.
    """

    return app_services.cleanup_code_cache(repository)


def normalize_code(code: str) -> str:
    """Normalize a deduplication code for stable comparisons.

    Args:
        code (str): Raw extracted code.

    Returns:
        str: Trimmed uppercase code.
    """
    return app_services.normalize_code(code)


def extract_codes(text: str) -> list[str]:
    """Extract candidate deduplication codes from text.

    Args:
        text (str): Message content to inspect.

    Returns:
        list[str]: Extracted normalized codes, possibly with duplicates.
    """

    return app_services.extract_codes(text, CODE_PATTERN)


def find_existing_codes(codes: list[str]) -> set[str]:
    """Fetch codes that are already present in the dedup cache.

    Args:
        codes (list[str]): Codes to query.

    Returns:
        set[str]: Codes found in storage.
    """

    return app_services.find_existing_codes(repository, codes)


def mark_codes(codes: list[str]):
    """Store newly observed codes for future deduplication.

    Args:
        codes (list[str]): Codes to insert when absent.
    """

    app_services.mark_codes(repository, codes)


def deduplicate_codes(text: str) -> list[str]:
    """Extract and deduplicate normalized codes preserving order.

    Args:
        text (str): Message text after transformations.

    Returns:
        list[str]: Unique codes preserving first appearance order.
    """
    return list(dict.fromkeys(extract_codes(text)))


async def has_duplicate_codes(
    codes: list[str],
    chat_id: int,
    msg_id: int
) -> bool:
    """Check whether any extracted code was already seen.

    Args:
        codes (list[str]): Extracted deduplication codes.
        chat_id (int): Source chat identifier.
        msg_id (int): Source message identifier.

    Returns:
        bool: True when duplicate codes are found.
    """
    if not codes:
        return False

    existing_codes = await asyncio.to_thread(find_existing_codes, codes)
    if not existing_codes:
        return False

    logger.info(
        "[SKIP] Duplicate codes %s in %s:%s",
        ",".join(sorted(existing_codes)),
        chat_id,
        msg_id
    )
    return True


async def forward_event_message(msg: Any, text: str, msg_id: int) -> bool:
    """Forward an incoming message to the destination chat.

    Args:
        msg (Any): Telethon message object.
        text (str): Message text/caption to forward.
        msg_id (int): Source message identifier for logging.

    Returns:
        bool: True when forwarding succeeds.
    """
    try:
        if msg.media:
            await client.send_file(
                DEST_CHAT,
                msg.media,
                caption=text,
                silent=True
            )
        else:
            await client.send_message(
                DEST_CHAT,
                text,
                silent=True
            )
    except Exception as exc:
        logger.error("Error forwarding %s: %s", msg_id, exc)
        return False

    return True


async def increment_message_counter():
    """Increment mirrored-message counter and persist stats."""
    async with stats_lock:
        stats["messages"] += 1
        save_stats(stats)


async def handler(event):
    """Mirror new Telegram messages after filtering and deduplication.

    Args:
        event: Telethon event containing the incoming message.
    """

    chat_id = event.chat_id
    msg_id = event.id

    # Check duplicates
    if await asyncio.to_thread(is_processed, chat_id, msg_id):
        return

    msg = event.message
    text = await asyncio.to_thread(apply_filters, msg.raw_text or "")
    codes = deduplicate_codes(text)

    if await has_duplicate_codes(codes, chat_id, msg_id):
        await asyncio.to_thread(mark_processed, chat_id, msg_id)
        return

    if not await forward_event_message(msg, text, msg_id):
        return

    logger.info("[OK] Forwarded %s:%s", chat_id, msg_id)

    # Save processed
    await asyncio.to_thread(mark_processed, chat_id, msg_id)

    # Save codes
    if codes:
        await asyncio.to_thread(mark_codes, codes)

    # Update stats
    await increment_message_counter()


# ================= WEB ROUTES =================

@app.get("/", response_class=HTMLResponse)
def index(request: Request, _=Depends(require_page_login)):
    """Render the main admin dashboard.

    Args:
        request (Request): Incoming HTTP request.
        _ : Authentication dependency guard.

    Returns:
        HTMLResponse: Rendered dashboard page.
    """

    cfg = dotenv_values(ENV_PATH)
    stats_data = get_stats()
    channel_stats = get_channel_stats()
    filters = get_filters()

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "cfg": cfg,
            "stats": stats_data,
            "channel_stats": channel_stats,
            "filters": filters,
            "dashboard_version": DASHBOARD_VERSION,
            "dashboard_deploy_note": DASHBOARD_DEPLOY_NOTE,
            "dashboard_rendered_at": datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M UTC"
            )
        }
    )


@app.get("/health")
def health():
    """Return a minimal liveness payload for health checks.

    Returns:
        dict[str, str]: Service liveness status.
    """
    return {"status": "ok"}


@app.get("/logs", response_class=PlainTextResponse)
def logs(_=Depends(require_api_login), lines: int = 200):
    """Return the latest application log lines.

    Args:
        _ : Authentication dependency guard.
        lines (int): Requested number of tail lines.

    Returns:
        str: Log tail text limited to a safe range.
    """
    return tail_file(LOG_PATH, max_lines=min(max(lines, 20), 1000))


@app.post("/save")
def save(
    _=Depends(require_page_login),
    api_id: str = Form(...),
    api_hash: str = Form(...),
    session_string: str = Form(""),
    dest_chat: str = Form(...),
    source_chats: str = Form(...)
):
    """Persist core Telegram mirror configuration from the admin form.

    Args:
        _ : Authentication dependency guard.
        api_id (str): Telegram API ID.
        api_hash (str): Telegram API hash.
        session_string (str): Optional Telethon string session.
        dest_chat (str): Destination chat identifier.
        source_chats (str): Comma-separated source chat identifiers.

    Returns:
        RedirectResponse: Redirect to the configuration section.
    """

    env = load_env_config()

    env["API_ID"] = api_id
    env["API_HASH"] = api_hash
    env["SESSION_STRING"] = session_string
    env["DEST_CHAT"] = dest_chat
    env["SOURCE_CHATS"] = source_chats
    save_env_config(env)

    return RedirectResponse("/#config", status_code=303)


@app.post("/save-db")
def save_db(
    _=Depends(require_page_login),
    cleanup_days: str = Form("")
):
    """Persist database cleanup settings from the admin form.

    Args:
        _ : Authentication dependency guard.
        cleanup_days (str): Retention in days. Empty value removes override.

    Returns:
        RedirectResponse: Redirect to the database section.
    """

    env = load_env_config()

    if cleanup_days.strip():
        env["CLEANUP_DAYS"] = cleanup_days.strip()
    else:
        env.pop("CLEANUP_DAYS", None)

    save_env_config(env)

    return RedirectResponse("/#db", status_code=303)


@app.post("/save-dup-config")
def save_dup_config(
    _=Depends(require_page_login),
    dup_code_regex: str = Form("")
):
    """Persist duplicate-detection regex configuration.

    Args:
        _ : Authentication dependency guard.
        dup_code_regex (str): Regex used to extract deduplication codes.

    Returns:
        RedirectResponse: Redirect to filters section.
    """

    env = load_env_config()

    if dup_code_regex.strip():
        # Validate regex before saving
        try:
            re.compile(dup_code_regex.strip())
            env["DUP_CODE_REGEX"] = dup_code_regex.strip()
        except re.error:
            # If invalid regex, don't save and redirect back
            logger.warning(
                "Invalid regex pattern provided: %s", dup_code_regex)
            return RedirectResponse("/#filters", status_code=303)
    else:
        env.pop("DUP_CODE_REGEX", None)

    save_env_config(env)

    return RedirectResponse("/#filters", status_code=303)


@app.post("/restart")
def restart(_=Depends(require_page_login)):
    """Terminate the current process to trigger service restart.

    Args:
        _ : Authentication dependency guard.

    Returns:
        RedirectResponse: Redirect to the dashboard.
    """

    os.kill(os.getpid(), signal.SIGTERM)

    return RedirectResponse("/", status_code=303)


@app.post("/clear-db")
def clear_db(_=Depends(require_page_login)):
    """Delete the SQLite database file and redirect to database tab.

    Args:
        _ : Authentication dependency guard.

    Returns:
        RedirectResponse: Redirect to the database section.
    """

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        logger.info("Database cleared")

    return RedirectResponse("/#db", status_code=303)


def validate_readonly_query(query: str) -> str | None:
    """Validate whether a query is allowed in the admin SQL endpoint.

    Args:
        query (str): SQL query text.

    Returns:
        str | None: Error message when invalid, otherwise None.
    """
    return app_services.validate_readonly_query(query)


def run_select_query(query: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Execute a validated read-only SQL query.

    Args:
        query (str): SQL SELECT statement.

    Returns:
        tuple[list[str], list[tuple[Any, ...]]]: Columns and fetched rows.
    """
    return app_services.run_select_query(repository, query)


@app.post("/execute-query")
async def execute_query(
    request: Request,
    _=Depends(require_page_login)
):
    """Execute read-only SQL queries submitted from the admin UI.

    Args:
        request (Request): Incoming HTTP request with JSON payload.
        _ : Authentication dependency guard.

    Returns:
        JSONResponse: Query result set or structured error payload.
    """

    try:
        body = await request.json()
        query = body.get("query", "").strip()

        validation_error = validate_readonly_query(query)
        if validation_error:
            status_code = 403 if "SELECT" in validation_error else 400
            return JSONResponse(
                {"error": validation_error},
                status_code=status_code
            )

        columns, rows = run_select_query(query)

        return JSONResponse({
            "columns": columns,
            "rows": rows
        })

    except sqlite3.Error as e:
        logger.error("SQL error: %s", e)
        return JSONResponse(
            {"error": f"SQL Error: {str(e)}"},
            status_code=400
        )
    except Exception as e:
        logger.error("Query execution error: %s", e)
        return JSONResponse(
            {"error": f"Error: {str(e)}"},
            status_code=500
        )


@app.post("/add-source-chat")
def add_source_chat(
    _=Depends(require_page_login),
    chat_id: str = Form(...),
    name: str = Form("")
):
    """Add or update a source chat in configuration and label table.

    Args:
        _ : Authentication dependency guard.
        chat_id (str): Source chat ID.
        name (str): Optional display name for the chat.

    Returns:
        RedirectResponse: Redirect to configuration section.
    """

    try:
        chat_id_int = int(chat_id)
    except ValueError:
        return RedirectResponse("/#config", status_code=303)

    env = load_env_config()
    parsed = parse_chat_id_list(env.get("SOURCE_CHATS", "") or "")

    if chat_id_int not in parsed:
        parsed.append(chat_id_int)

    env["SOURCE_CHATS"] = ",".join(str(x) for x in parsed)
    save_env_config(env)

    repository.upsert_channel(chat_id_int, name.strip())

    global SOURCE_CHATS
    SOURCE_CHATS = parsed

    logger.info("Source chat added/updated: %s", chat_id_int)

    return RedirectResponse("/#config", status_code=303)


@app.post("/add-filter")
def add_filter(
    _=Depends(require_page_login),
    pattern: str = Form(...),
    replacement: str = Form("")
):
    """Create a new URL transformation filter.

    Args:
        _ : Authentication dependency guard.
        pattern (str): Regex pattern to match.
        replacement (str): Replacement value or strategy token.

    Returns:
        RedirectResponse: Redirect to filters section.
    """
    repository.add_filter(pattern, replacement)
    return RedirectResponse("/#filters", status_code=303)


@app.post("/update-filter")
def update_filter(
    _=Depends(require_page_login),
    filter_id: int = Form(...),
    pattern: str = Form(...),
    replacement: str = Form("")
):
    """Update an existing URL transformation filter.

    Args:
        _ : Authentication dependency guard.
        filter_id (int): Filter identifier.
        pattern (str): New regex pattern.
        replacement (str): New replacement value.

    Returns:
        RedirectResponse: Redirect to filters section.
    """
    repository.update_filter(filter_id, pattern, replacement)
    return RedirectResponse("/#filters", status_code=303)


@app.post("/delete-filter")
def delete_filter(filter_id: int = Form(...), _=Depends(require_page_login)):
    """Delete one URL transformation filter by ID.

    Args:
        filter_id (int): Filter identifier.
        _ : Authentication dependency guard.

    Returns:
        RedirectResponse: Redirect to filters section.
    """
    repository.delete_filter(filter_id)
    return RedirectResponse("/#filters", status_code=303)


@app.post("/move-filter-up")
def move_filter_up(filter_id: int = Form(...), _=Depends(require_page_login)):
    """Move a filter one position up in execution order.

    Args:
        filter_id (int): Filter identifier.
        _ : Authentication dependency guard.

    Returns:
        RedirectResponse: Redirect to filters section.
    """
    move_filter(filter_id, move_up=True)

    return RedirectResponse("/#filters", status_code=303)


@app.post("/move-filter-down")
def move_filter_down(
    filter_id: int = Form(...),
    _=Depends(require_page_login)
):
    """Move a filter one position down in execution order.

    Args:
        filter_id (int): Filter identifier.
        _ : Authentication dependency guard.

    Returns:
        RedirectResponse: Redirect to filters section.
    """
    move_filter(filter_id, move_up=False)

    return RedirectResponse("/#filters", status_code=303)


# ================= BOT THREAD =================

def run_bot():
    """Start the Telegram client loop in a dedicated thread."""

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def main():
        await client.connect()
        if not await client.is_user_authorized():
            await client.start()  # type: ignore
        client.add_event_handler(
            handler, events.NewMessage(chats=SOURCE_CHATS))
        await client.run_until_disconnected()  # type: ignore

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


def parse_cleanup_time(value: str) -> tuple[int, int]:
    """Parse a cleanup schedule time in HH:MM format.

    Args:
        value (str): Time string from configuration.

    Returns:
        tuple[int, int]: Parsed (hour, minute). Falls back to (0, 5) when
        input is invalid.
    """

    try:
        hour_str, minute_str = value.strip().split(":", 1)
        hour = int(hour_str)
        minute = int(minute_str)
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour, minute
    except Exception:
        pass

    return 0, 5


def parse_cleanup_days(value: str) -> int:
    """Parse cleanup retention days from configuration.

    Args:
        value (str): Raw configuration value.

    Returns:
        int: Parsed number of days, or default when invalid.
    """

    try:
        return int(value)
    except Exception:
        return CLEANUP_DAYS_DEFAULT


def seconds_until_next_run(hour: int, minute: int) -> int:
    """Compute delay in seconds until the next scheduled execution.

    Args:
        hour (int): Target hour in 24h format.
        minute (int): Target minute.

    Returns:
        int: Number of seconds until next run, with a 60-second minimum.
    """

    now = datetime.now()
    run_at = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if run_at <= now:
        run_at = run_at + timedelta(days=1)

    return max(60, int((run_at - now).total_seconds()))


def cleanup_scheduler():
    """Run periodic cleanup for processed messages and code cache."""

    while True:
        cfg = dotenv_values(ENV_PATH)
        time_value = cfg.get("CLEANUP_TIME") or CLEANUP_TIME_DEFAULT
        hour, minute = parse_cleanup_time(time_value)

        time.sleep(seconds_until_next_run(hour, minute))

        cfg = dotenv_values(ENV_PATH)
        days = parse_cleanup_days(
            cfg.get("CLEANUP_DAYS") or str(CLEANUP_DAYS_DEFAULT))

        if days > 0:
            removed = cleanup_processed(days)
            logger.info(
                "Cleanup removed %s rows older than %s days", removed, days)

        removed_codes = cleanup_code_cache()
        logger.info("Code cache cleanup removed %s rows", removed_codes)


# ================= SHUTDOWN =================

def shutdown():
    """Persist shutdown state and close database resources safely."""

    stats["status"] = "stopped"
    save_stats(stats)

    try:
        repository.close()
    except Exception:
        pass


atexit.register(shutdown)


# ================= MAIN =================

if __name__ == "__main__":

    threading.Thread(
        target=run_bot,
        daemon=True
    ).start()

    threading.Thread(
        target=cleanup_scheduler,
        daemon=True
    ).start()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=WEB_PORT
    )
