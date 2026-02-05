# mirror.py

import os
import json
import sqlite3
import threading
import time
import signal
import asyncio
import atexit
import requests
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv, dotenv_values
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from asyncio import Lock

from fastapi import (
    FastAPI, Request, Form, Depends,
    HTTPException, status
)
from fastapi.responses import RedirectResponse, HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials

import uvicorn


# ================= CONFIG =================

ENV_PATH = "/config/.env"
DATA_DIR = "/data"
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


# ================= LOCKS =================

db_mutex = threading.Lock()
stats_lock = Lock()


# ================= STATS =================

def load_stats():
    if not os.path.exists(STATS_PATH):
        return {"messages": 0, "status": "starting"}

    try:
        with open(STATS_PATH) as f:
            return json.load(f)
    except Exception:
        return {"messages": 0, "status": "reset"}


def save_stats(data):
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
templates = Jinja2Templates(directory="templates")
security = HTTPBasic()


# ================= AUTH =================

def auth(credentials: HTTPBasicCredentials = Depends(security)):

    if credentials.password != ADMIN_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Wrong password",
            headers={"WWW-Authenticate": "Basic"},
        )


# ================= HELPERS =================

def get_stats():

    if not os.path.exists(STATS_PATH):
        return {"messages": 0, "status": "unknown"}

    try:
        with open(STATS_PATH) as f:
            return json.load(f)
    except Exception:
        return {"messages": 0, "status": "error"}


def save_env(data: dict):

    with open(ENV_PATH, "w") as f:
        for k, v in data.items():
            if v is not None:
                f.write(f"{k}={v}\n")


def get_channel_stats():

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


def tail_file(path: str, max_lines: int = 200) -> str:

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

    with db_mutex:
        cur.execute(
            "SELECT 1 FROM processed WHERE chat_id=? AND message_id=?",
            (chat_id, msg_id)
        )
        return cur.fetchone() is not None


def mark_processed(chat_id: int, msg_id: int):

    with db_mutex:
        cur.execute(
            "INSERT OR IGNORE INTO processed VALUES (?, ?, ?)",
            (chat_id, msg_id, utc_now_string())
        )
        conn.commit()


def cleanup_processed(days: int) -> int:

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


async def handler(event):

    chat_id = event.chat_id
    msg_id = event.id

    # Check duplicates
    if await asyncio.to_thread(is_processed, chat_id, msg_id):
        return

    msg = event.message

    text = msg.raw_text or ""

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

    # Update stats
    async with stats_lock:
        stats["messages"] += 1
        save_stats(stats)


# ================= WEB ROUTES =================

@app.get("/", response_class=HTMLResponse)
def index(request: Request, user=Depends(auth)):

    cfg = dotenv_values(ENV_PATH)
    stats_data = get_stats()
    channel_stats = get_channel_stats()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "cfg": cfg,
            "stats": stats_data,
            "channel_stats": channel_stats
        }
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/logs", response_class=PlainTextResponse)
def logs(lines: int = 200, user=Depends(auth)):
    return tail_file(LOG_PATH, max_lines=min(max(lines, 20), 1000))


@app.post("/save")
def save(
    api_id: str = Form(...),
    api_hash: str = Form(...),
    session_string: str = Form(""),
    dest_chat: str = Form(...),
    source_chats: str = Form(...),
    user=Depends(auth)
):

    env = dotenv_values(ENV_PATH)

    env["API_ID"] = api_id
    env["API_HASH"] = api_hash
    env["SESSION_STRING"] = session_string
    env["DEST_CHAT"] = dest_chat
    env["SOURCE_CHATS"] = source_chats
    env["ADMIN_PASSWORD"] = ADMIN_PASSWORD

    save_env(env)

    return RedirectResponse("/", status_code=303)


@app.post("/save-db")
def save_db(
    cleanup_days: str = Form(""),
    user=Depends(auth)
):

    env = dotenv_values(ENV_PATH)

    if cleanup_days.strip():
        env["CLEANUP_DAYS"] = cleanup_days.strip()
    else:
        env.pop("CLEANUP_DAYS", None)

    env["ADMIN_PASSWORD"] = ADMIN_PASSWORD

    save_env(env)

    return RedirectResponse("/", status_code=303)


@app.post("/restart")
def restart(user=Depends(auth)):

    os.kill(os.getpid(), signal.SIGTERM)

    return RedirectResponse("/", status_code=303)


@app.post("/clear-db")
def clear_db(user=Depends(auth)):

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        logger.info("Database cleared")

    return RedirectResponse("/", status_code=303)


@app.post("/add-source-chat")
def add_source_chat(
    chat_id: str = Form(...),
    name: str = Form(""),
    user=Depends(auth)
):

    try:
        chat_id_int = int(chat_id)
    except ValueError:
        return RedirectResponse("/", status_code=303)

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

    return RedirectResponse("/", status_code=303)


# ================= BOT THREAD =================

def run_bot():

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def main():
        await client.start()
        client.add_event_handler(handler, events.NewMessage(chats=SOURCE_CHATS))
        await client.run_until_disconnected()

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


def parse_cleanup_time(value: str) -> tuple[int, int]:

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

    try:
        return int(value)
    except Exception:
        return CLEANUP_DAYS_DEFAULT


def seconds_until_next_run(hour: int, minute: int) -> int:

    now = datetime.now()
    run_at = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if run_at <= now:
        run_at = run_at + timedelta(days=1)

    return max(60, int((run_at - now).total_seconds()))


def cleanup_scheduler():

    while True:
        cfg = dotenv_values(ENV_PATH)
        days = parse_cleanup_days(cfg.get("CLEANUP_DAYS") or str(CLEANUP_DAYS_DEFAULT))
        time_value = cfg.get("CLEANUP_TIME", CLEANUP_TIME_DEFAULT)
        hour, minute = parse_cleanup_time(time_value)

        if days <= 0:
            time.sleep(300)
            continue

        time.sleep(seconds_until_next_run(hour, minute))

        cfg = dotenv_values(ENV_PATH)
        days = parse_cleanup_days(cfg.get("CLEANUP_DAYS") or str(CLEANUP_DAYS_DEFAULT))
        if days <= 0:
            continue

        removed = cleanup_processed(days)
        logger.info("Cleanup removed %s rows older than %s days", removed, days)


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
