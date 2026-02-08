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
from fastapi.responses import RedirectResponse, HTMLResponse, PlainTextResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

import uvicorn


# ================= CONFIG =================

# Support both Docker and local execution
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, "config", ".env") if os.path.exists(os.path.join(SCRIPT_DIR, "config", ".env")) else "/config/.env"
DATA_DIR = os.path.join(SCRIPT_DIR, "data") if not os.path.exists("/data") else "/data"
STATS_PATH = f"{DATA_DIR}/stats.json"
DB_PATH = f"{DATA_DIR}/state.db"
LOG_PATH = f"{DATA_DIR}/app.log"

os.makedirs(DATA_DIR, exist_ok=True)

load_dotenv(ENV_PATH)

REQUIRED_VARS = ["API_ID", "API_HASH", "DEST_CHAT", "SOURCE_CHATS", "ADMIN_PASSWORD"]

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


# ================= LOGGING =================

logger = logging.getLogger("tg-mirror")
logger.setLevel(logging.INFO)

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
    """
    Loads application statistics from the JSON file.

    :return: A dictionary containing statistics or default values.
    """
    if not os.path.exists(STATS_PATH):
        return {"messages": 0, "status": "starting"}

    try:
        with open(STATS_PATH) as f:
            return json.load(f)
    except Exception:
        return {"messages": 0, "status": "reset"}


def save_stats(data: dict[str, Any]):
    """
    Saves application statistics to the JSON file safely.

    :param data: The statistics dictionary to save.
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


def utc_now_string() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def init_db():
    """
    Initializes the database tables and migrates schema if necessary.
    Creates tables for processed messages, channels, codes, and filters.
    """

    with db_mutex:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS processed (
            chat_id INTEGER,
            message_id INTEGER,
            created_at TEXT,
            PRIMARY KEY (chat_id, message_id)
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            chat_id INTEGER PRIMARY KEY,
            name TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS message_codes (
            code TEXT PRIMARY KEY,
            created_at TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS url_filters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern TEXT,
            replacement TEXT,
            sort_order INTEGER DEFAULT 0
        )
        """)

        # Add sort_order column if it doesn't exist
        filter_columns = [row[1] for row in cur.execute(
            "PRAGMA table_info(url_filters)"
        ).fetchall()]
        if "sort_order" not in filter_columns:
            cur.execute("ALTER TABLE url_filters ADD COLUMN sort_order INTEGER DEFAULT 0")
            # Initialize sort_order for existing rows
            cur.execute("""
                UPDATE url_filters 
                SET sort_order = id 
                WHERE sort_order IS NULL OR sort_order = 0
            """)

        columns = [row[1] for row in cur.execute(
            "PRAGMA table_info(processed)"
        ).fetchall()]
        if "created_at" not in columns:
            cur.execute("ALTER TABLE processed ADD COLUMN created_at TEXT")
            cur.execute(
                "UPDATE processed SET created_at = ? WHERE created_at IS NULL",
                (utc_now_string(),)
            )
        conn.commit()


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

templates = Jinja2Templates(directory="templates")


# ================= AUTH =================

@app.get("/login", response_class=HTMLResponse, tags=["auth"])
async def login_form(request: Request):
    if request.session.get("authenticated"):
        return RedirectResponse(url="/")
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login", tags=["auth"])
async def handle_login(request: Request, password: str = Form(...)):
    if password == ADMIN_PASSWORD:
        request.session["authenticated"] = True
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    else:
        return templates.TemplateResponse("login.html", {"request": request, "error": "Password inválida"})


@app.post("/logout", tags=["auth"])
async def handle_logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)


def require_page_login(request: Request):
    if not request.session.get("authenticated"):
        raise HTTPException(
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
            headers={"Location": "/login"},
        )


def require_api_login(request: Request):
    if not request.session.get("authenticated"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated"
        )


# ================= HELPERS =================

def get_stats() -> dict[str, Any]:
    """
    Retrieves current statistics safely.

    :return: A dictionary with current stats or error status.
    """

    if not os.path.exists(STATS_PATH):
        return {"messages": 0, "status": "unknown"}

    try:
        with open(STATS_PATH) as f:
            return json.load(f)
    except Exception:
        return {"messages": 0, "status": "error"}


def save_env(data: dict[str, Any]):
    """
    Writes configuration key-value pairs to the .env file.

    :param data: Dictionary containing environment variables to save.
    """

    with open(ENV_PATH, "w") as f:
        for k, v in data.items():
            if v is not None:
                f.write(f"{k}={v}\n")


def get_channel_stats() -> list[dict[str, Any]]:
    """
    Aggregates message counts per channel from the database.

    :return: A list of dictionaries containing chat_id, name, and message count.
    """

    labels = {}
    try:
        with db_mutex:
            rows = conn.execute("SELECT chat_id, name FROM channels").fetchall()
            labels = {row[0]: (row[1] or "") for row in rows}
    except Exception:
        labels = {}

    counts = {}
    try:
        with db_mutex:
            rows = conn.execute(
                "SELECT chat_id, COUNT(*) FROM processed GROUP BY chat_id"
            ).fetchall()
            counts = {row[0]: row[1] for row in rows}
    except Exception:
        counts = {}

    ordered = []
    for chat_id in SOURCE_CHATS:
        ordered.append({
            "chat_id": chat_id,
            "name": labels.get(chat_id, ""),
            "messages": counts.get(chat_id, 0)
        })

    for chat_id, msg_count in counts.items():
        if chat_id not in SOURCE_CHATS:
            ordered.append({
                "chat_id": chat_id,
                "name": labels.get(chat_id, ""),
                "messages": msg_count
            })

    return ordered


def get_filters() -> list[tuple[Any, ...]]:
    with db_mutex:
        return conn.execute(
            "SELECT id, pattern, replacement FROM url_filters ORDER BY sort_order, id"
        ).fetchall()

def _expand_url(session: requests.Session, url: str) -> str:
    """
    Helper to expand a single URL and clean Amazon parameters.

    :param session: The active requests Session.
    :param url: The URL to be expanded.
    :return: The final URL after following redirects and cleaning parameters.
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
    """
    Applies URL expansion and regex replacements to the message text.

    :param text: The input message text.
    :return: The processed text with expanded URLs and applied filters.
    """
    if not text:
        return text

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
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
    """
    Reads the last N lines of a file.

    :param path: Path to the file.
    :param max_lines: Maximum number of lines to read from the end.
    :return: The content of the last lines as a string.
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
    """
    Checks if a message has already been processed to prevent duplicates.

    :param chat_id: The source chat ID.
    :param msg_id: The message ID.
    :return: True if processed, False otherwise.
    """

    with db_mutex:
        cur.execute(
            "SELECT 1 FROM processed WHERE chat_id=? AND message_id=?",
            (chat_id, msg_id)
        )
        return cur.fetchone() is not None


def mark_processed(chat_id: int, msg_id: int):
    """
    Marks a message as processed in the database.

    :param chat_id: The source chat ID.
    :param msg_id: The message ID.
    """

    with db_mutex:
        cur.execute(
            "INSERT OR IGNORE INTO processed VALUES (?, ?, ?)",
            (chat_id, msg_id, utc_now_string())
        )
        conn.commit()


def cleanup_processed(days: int) -> int:
    """
    Removes processed message records older than the specified number of days.

    :param days: Retention period in days.
    :return: Number of rows deleted.
    """

    if days <= 0:
        return 0

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    with db_mutex:
        cur.execute(
            "DELETE FROM processed WHERE created_at IS NOT NULL AND created_at < ?",
            (cutoff,)
        )
        conn.commit()
        return cur.rowcount or 0


def cleanup_code_cache() -> int:
    """
    Clears the cache of deduplication codes.

    :return: Number of rows deleted.
    """

    with db_mutex:
        cur.execute("DELETE FROM message_codes")
        conn.commit()
        return cur.rowcount or 0


def normalize_code(code: str) -> str:
    return code.strip().upper()


def extract_codes(text: str) -> list[str]:
    """
    Extracts codes from text using the configured regex pattern.

    :param text: The text to search.
    :return: A list of unique codes found.
    """

    if not text or CODE_PATTERN is None:
        return []

    codes = []
    for match in CODE_PATTERN.finditer(text):
        # Use capture group if exists (e.g. (?:/dp/)([A-Z0-9]{10}) captures only the code)
        # Otherwise use full match
        if match.lastindex and match.lastindex > 0:
            codes.append(normalize_code(match.group(1)))
        else:
            codes.append(normalize_code(match.group(0)))
    
    return codes


def find_existing_codes(codes: list[str]) -> set[str]:
    """
    Checks which codes in the list already exist in the database.

    :param codes: List of codes to check.
    :return: A set containing the codes that were found in the database.
    """

    if not codes:
        return set()

    placeholders = ",".join("?" for _ in codes)
    query = f"SELECT code FROM message_codes WHERE code IN ({placeholders})"

    with db_mutex:
        rows = cur.execute(query, codes).fetchall()
        return {row[0] for row in rows}


def mark_codes(codes: list[str]):
    """
    Saves new codes to the database to prevent future duplicates.

    :param codes: List of codes to insert.
    """

    if not codes:
        return

    now = utc_now_string()
    rows = [(code, now) for code in codes]

    with db_mutex:
        cur.executemany(
            "INSERT OR IGNORE INTO message_codes (code, created_at) VALUES (?, ?)",
            rows
        )
        conn.commit()


async def handler(event):
    """
    Main message handler: filters, expands URLs, checks duplicates, and forwards.

    :param event: The Telethon event object containing the message.
    """

    chat_id = event.chat_id
    msg_id = event.id

    # Check duplicates
    if await asyncio.to_thread(is_processed, chat_id, msg_id):
        return

    msg = event.message

    raw_text = msg.raw_text or ""
    text = raw_text

    # Apply URL filters
    text = await asyncio.to_thread(apply_filters, text)

    # Extract codes AFTER expanding/cleaning URLs
    codes = list(dict.fromkeys(extract_codes(text)))

    if codes:
        existing_codes = await asyncio.to_thread(find_existing_codes, codes)
        if existing_codes:
            logger.info(
                "[SKIP] Duplicate codes %s in %s:%s",
                ",".join(sorted(existing_codes)),
                chat_id,
                msg_id
            )
            await asyncio.to_thread(mark_processed, chat_id, msg_id)
            return

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

    except Exception as e:
        logger.error("Error forwarding %s: %s", msg_id, e)
        return

    logger.info("[OK] Forwarded %s:%s", chat_id, msg_id)

    # Save processed
    await asyncio.to_thread(mark_processed, chat_id, msg_id)

    # Save codes
    if codes:
        await asyncio.to_thread(mark_codes, codes)

    # Update stats
    async with stats_lock:
        stats["messages"] += 1
        save_stats(stats)


# ================= WEB ROUTES =================

@app.get("/", response_class=HTMLResponse)
def index(request: Request, _ = Depends(require_page_login)):

    cfg = dotenv_values(ENV_PATH)
    stats_data = get_stats()
    channel_stats = get_channel_stats()
    filters = get_filters()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "cfg": cfg,
            "stats": stats_data,
            "channel_stats": channel_stats,
            "filters": filters
        }
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/logs", response_class=PlainTextResponse)
def logs(_ = Depends(require_api_login), lines: int = 200):
    return tail_file(LOG_PATH, max_lines=min(max(lines, 20), 1000))


@app.post("/save")
def save(
    _ = Depends(require_page_login),
    api_id: str = Form(...),
    api_hash: str = Form(...),
    session_string: str = Form(""),
    dest_chat: str = Form(...),
    source_chats: str = Form(...)
):

    env = dotenv_values(ENV_PATH)

    env["API_ID"] = api_id
    env["API_HASH"] = api_hash
    env["SESSION_STRING"] = session_string
    env["DEST_CHAT"] = dest_chat
    env["SOURCE_CHATS"] = source_chats
    env["ADMIN_PASSWORD"] = ADMIN_PASSWORD

    save_env(env)

    return RedirectResponse("/#config", status_code=303)


@app.post("/save-db")
def save_db(
    _ = Depends(require_page_login),
    cleanup_days: str = Form("")
):

    env = dotenv_values(ENV_PATH)

    if cleanup_days.strip():
        env["CLEANUP_DAYS"] = cleanup_days.strip()
    else:
        env.pop("CLEANUP_DAYS", None)

    env["ADMIN_PASSWORD"] = ADMIN_PASSWORD

    save_env(env)

    return RedirectResponse("/#db", status_code=303)


@app.post("/save-dup-config")
def save_dup_config(
    _ = Depends(require_page_login),
    dup_code_regex: str = Form("")
):

    env = dotenv_values(ENV_PATH)

    if dup_code_regex.strip():
        # Validate regex before saving
        try:
            re.compile(dup_code_regex.strip())
            env["DUP_CODE_REGEX"] = dup_code_regex.strip()
        except re.error:
            # If invalid regex, don't save and redirect back
            logger.warning("Invalid regex pattern provided: %s", dup_code_regex)
            return RedirectResponse("/#filters", status_code=303)
    else:
        env.pop("DUP_CODE_REGEX", None)

    env["ADMIN_PASSWORD"] = ADMIN_PASSWORD

    save_env(env)

    return RedirectResponse("/#filters", status_code=303)


@app.post("/restart")
def restart(_ = Depends(require_page_login)):

    os.kill(os.getpid(), signal.SIGTERM)

    return RedirectResponse("/", status_code=303)


@app.post("/clear-db")
def clear_db(_ = Depends(require_page_login)):

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        logger.info("Database cleared")

    return RedirectResponse("/#db", status_code=303)


@app.post("/execute-query")
async def execute_query(
    request: Request,
    _ = Depends(require_page_login)
):

    try:
        body = await request.json()
        query = body.get("query", "").strip()

        if not query:
            return JSONResponse(
                {"error": "Query cannot be empty"},
                status_code=400
            )

        # Security: only allow SELECT queries
        if not query.upper().startswith("SELECT"):
            return JSONResponse(
                {"error": "Only SELECT queries are allowed for safety"},
                status_code=403
            )

        with db_mutex:
            cursor = conn.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

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
    _ = Depends(require_page_login),
    chat_id: str = Form(...),
    name: str = Form("")
):

    try:
        chat_id_int = int(chat_id)
    except ValueError:
        return RedirectResponse("/#config", status_code=303)

    env = dotenv_values(ENV_PATH)

    current = env.get("SOURCE_CHATS", "") or ""
    parsed = [
        int(x.strip())
        for x in current.split(",")
        if x.strip()
    ]

    if chat_id_int not in parsed:
        parsed.append(chat_id_int)

    env["SOURCE_CHATS"] = ",".join(str(x) for x in parsed)
    env["ADMIN_PASSWORD"] = ADMIN_PASSWORD

    save_env(env)

    with db_mutex:
        conn.execute(
            "INSERT OR REPLACE INTO channels (chat_id, name) VALUES (?, ?)",
            (chat_id_int, name.strip())
        )
        conn.commit()

    global SOURCE_CHATS
    SOURCE_CHATS = parsed

    logger.info("Source chat added/updated: %s", chat_id_int)

    return RedirectResponse("/#config", status_code=303)


@app.post("/add-filter")
def add_filter(
    _ = Depends(require_page_login),
    pattern: str = Form(...),
    replacement: str = Form("")
):
    with db_mutex:
        # Get max sort_order and add 1
        max_order = cur.execute("SELECT COALESCE(MAX(sort_order), 0) FROM url_filters").fetchone()[0]
        cur.execute(
            "INSERT INTO url_filters (pattern, replacement, sort_order) VALUES (?, ?, ?)",
            (pattern, replacement, max_order + 1)
        )
        conn.commit()
    return RedirectResponse("/#filters", status_code=303)


@app.post("/update-filter")
def update_filter(
    _ = Depends(require_page_login),
    filter_id: int = Form(...),
    pattern: str = Form(...),
    replacement: str = Form("")
):
    with db_mutex:
        cur.execute(
            "UPDATE url_filters SET pattern=?, replacement=? WHERE id=?",
            (pattern, replacement, filter_id)
        )
        conn.commit()
    return RedirectResponse("/#filters", status_code=303)


@app.post("/delete-filter")
def delete_filter(filter_id: int = Form(...), _ = Depends(require_page_login)):
    with db_mutex:
        cur.execute("DELETE FROM url_filters WHERE id=?", (filter_id,))
        conn.commit()
    return RedirectResponse("/#filters", status_code=303)


@app.post("/move-filter-up")
def move_filter_up(filter_id: int = Form(...), _ = Depends(require_page_login)):
    with db_mutex:
        # Get current filter
        current = cur.execute(
            "SELECT id, sort_order FROM url_filters WHERE id=?", 
            (filter_id,)
        ).fetchone()
        
        if not current:
            return RedirectResponse("/#filters", status_code=303)
        
        current_id, current_order = current
        
        # Get previous filter
        previous = cur.execute(
            "SELECT id, sort_order FROM url_filters WHERE sort_order < ? ORDER BY sort_order DESC LIMIT 1",
            (current_order,)
        ).fetchone()
        
        if previous:
            prev_id, prev_order = previous
            # Swap sort_order
            cur.execute("UPDATE url_filters SET sort_order=? WHERE id=?", (prev_order, current_id))
            cur.execute("UPDATE url_filters SET sort_order=? WHERE id=?", (current_order, prev_id))
            conn.commit()
    
    return RedirectResponse("/#filters", status_code=303)


@app.post("/move-filter-down")
def move_filter_down(filter_id: int = Form(...), _ = Depends(require_page_login)):
    with db_mutex:
        # Get current filter
        current = cur.execute(
            "SELECT id, sort_order FROM url_filters WHERE id=?", 
            (filter_id,)
        ).fetchone()
        
        if not current:
            return RedirectResponse("/#filters", status_code=303)
        
        current_id, current_order = current
        
        # Get next filter
        next_filter = cur.execute(
            "SELECT id, sort_order FROM url_filters WHERE sort_order > ? ORDER BY sort_order ASC LIMIT 1",
            (current_order,)
        ).fetchone()
        
        if next_filter:
            next_id, next_order = next_filter
            # Swap sort_order
            cur.execute("UPDATE url_filters SET sort_order=? WHERE id=?", (next_order, current_id))
            cur.execute("UPDATE url_filters SET sort_order=? WHERE id=?", (current_order, next_id))
            conn.commit()
    
    return RedirectResponse("/#filters", status_code=303)


# ================= BOT THREAD =================

def run_bot():
    """
    Runs the Telegram client event loop.
    """

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def main():
        await client.connect()
        if not await client.is_user_authorized():
            await client.start()  # type: ignore
        client.add_event_handler(handler, events.NewMessage(chats=SOURCE_CHATS))
        await client.run_until_disconnected() # type: ignore

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


def parse_cleanup_time(value: str) -> tuple[int, int]:
    """
    Parses HH:MM string into hour and minute integers.

    :param value: Time string in "HH:MM" format.
    :return: Tuple (hour, minute). Defaults to (0, 5) on error.
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
    """
    Parses the cleanup days configuration value.

    :param value: String value from config.
    :return: Integer number of days, or default if invalid.
    """

    try:
        return int(value)
    except Exception:
        return CLEANUP_DAYS_DEFAULT


def seconds_until_next_run(hour: int, minute: int) -> int:
    """
    Calculates seconds remaining until the next scheduled time.

    :param hour: Scheduled hour (0-23).
    :param minute: Scheduled minute (0-59).
    :return: Seconds remaining.
    """

    now = datetime.now()
    run_at = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if run_at <= now:
        run_at = run_at + timedelta(days=1)

    return max(60, int((run_at - now).total_seconds()))


def cleanup_scheduler():
    """
    Background thread that runs daily cleanup tasks.
    """

    while True:
        cfg = dotenv_values(ENV_PATH)
        time_value = cfg.get("CLEANUP_TIME") or CLEANUP_TIME_DEFAULT
        hour, minute = parse_cleanup_time(time_value)

        time.sleep(seconds_until_next_run(hour, minute))

        cfg = dotenv_values(ENV_PATH)
        days = parse_cleanup_days(cfg.get("CLEANUP_DAYS") or str(CLEANUP_DAYS_DEFAULT))

        if days > 0:
            removed = cleanup_processed(days)
            logger.info("Cleanup removed %s rows older than %s days", removed, days)

        removed_codes = cleanup_code_cache()
        logger.info("Code cache cleanup removed %s rows", removed_codes)


# ================= SHUTDOWN =================

def shutdown():

    stats["status"] = "stopped"
    save_stats(stats)
    logger.info("Service stopped")

    try:
        conn.close()
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
