# mirror.py

import os
import json
import sqlite3
import threading
import signal
import asyncio
import atexit
import requests

from dotenv import load_dotenv, dotenv_values
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from asyncio import Lock

from fastapi import (
    FastAPI, Request, Form, Depends,
    HTTPException, status
)
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials

import uvicorn


# ================= CONFIG =================

ENV_PATH = "/config/.env"
DATA_DIR = "/data"
STATS_PATH = f"{DATA_DIR}/stats.json"
DB_PATH = f"{DATA_DIR}/state.db"

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


# ================= LOCKS =================

db_lock = Lock()
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
cur.execute("""
CREATE TABLE IF NOT EXISTS processed (
    chat_id INTEGER,
    message_id INTEGER,
    PRIMARY KEY (chat_id, message_id)
)
""")
conn.commit()


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


# ================= TELEGRAM HANDLER =================

@client.on(events.NewMessage(chats=SOURCE_CHATS))
async def handler(event):

    chat_id = event.chat_id
    msg_id = event.id

    # Check duplicates
    async with db_lock:
        cur.execute(
            "SELECT 1 FROM processed WHERE chat_id=? AND message_id=?",
            (chat_id, msg_id)
        )
        if cur.fetchone():
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
        print(f"Error forwarding {msg_id}: {e}")
        return

    # Save processed
    async with db_lock:
        cur.execute(
            "INSERT OR IGNORE INTO processed VALUES (?, ?)",
            (chat_id, msg_id)
        )
        conn.commit()

    # Update stats
    async with stats_lock:
        stats["messages"] += 1
        save_stats(stats)


# ================= WEB ROUTES =================

@app.get("/", response_class=HTMLResponse)
def index(request: Request, user=Depends(auth)):

    cfg = dotenv_values(ENV_PATH)
    stats_data = get_stats()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "cfg": cfg,
            "stats": stats_data
        }
    )


@app.get("/health")
def health():
    return {"status": "ok"}


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


@app.post("/restart")
def restart(user=Depends(auth)):

    os.kill(os.getpid(), signal.SIGTERM)

    return RedirectResponse("/", status_code=303)


@app.post("/clear-db")
def clear_db(user=Depends(auth)):

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    return RedirectResponse("/", status_code=303)


# ================= BOT THREAD =================

def run_bot():

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def main():
        await client.start()
        await client.run_until_disconnected()

    loop.run_until_complete(main())
    loop.close()


# ================= SHUTDOWN =================

def shutdown():

    stats["status"] = "stopped"
    save_stats(stats)

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

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=WEB_PORT
    )
