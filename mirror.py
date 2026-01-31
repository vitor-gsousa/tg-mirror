import os
import json
import asyncio
import logging
import aiosqlite

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession


# ---------- LOG ----------

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("mirror")


# ---------- CONFIG ----------

load_dotenv("/config/.env")

DATA_DIR = "/data"
STATS_PATH = f"{DATA_DIR}/stats.json"
DB_PATH = f"{DATA_DIR}/state.db"

os.makedirs(DATA_DIR, exist_ok=True)


# ---------- STATS ----------

stats_lock = asyncio.Lock()


async def save_stats(data):
    async with stats_lock:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: json.dump(data, open(STATS_PATH, "w"))
        )


def load_stats():
    if not os.path.exists(STATS_PATH):
        return {"messages": 0, "status": "starting"}

    try:
        return json.load(open(STATS_PATH))
    except:
        return {"messages": 0, "status": "reset"}


stats = load_stats()


# ---------- TELEGRAM ----------

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]

SESSION_STRING = os.environ.get("SESSION_STRING")

DEST_CHAT = int(os.environ["DEST_CHAT"])
SOURCE_CHATS = [
    int(x.strip()) for x in os.environ["SOURCE_CHATS"].split(",")
]


if SESSION_STRING:
    client = TelegramClient(
        StringSession(SESSION_STRING),
        API_ID,
        API_HASH
    )
else:
    client = TelegramClient("mirror", API_ID, API_HASH)


# ---------- DB ----------

db = None


async def init_db():

    global db

    db = await aiosqlite.connect(DB_PATH)

    await db.execute("PRAGMA journal_mode=WAL;")

    await db.execute("""
        CREATE TABLE IF NOT EXISTS processed (
            chat_id INTEGER,
            message_id INTEGER,
            PRIMARY KEY (chat_id, message_id)
        )
    """)

    await db.commit()


# ---------- HANDLER ----------

@client.on(events.NewMessage(chats=SOURCE_CHATS))
async def handler(event):

    chat_id = event.chat_id
    msg_id = event.id

    async with db.execute(
        "SELECT 1 FROM processed WHERE chat_id=? AND message_id=?",
        (chat_id, msg_id)
    ) as cur:

        if await cur.fetchone():
            return

    try:

        # Copy (remove buttons, keep content)
        await event.message.copy_to(DEST_CHAT)

        await db.execute(
            "INSERT OR IGNORE INTO processed VALUES (?, ?)",
            (chat_id, msg_id)
        )

        await db.commit()

        async with stats_lock:
            stats["messages"] += 1

        await save_stats(stats)

        logger.info(f"Forwarded {msg_id}")

    except Exception as e:

        logger.error("Error forwarding", exc_info=True)


# ---------- MAIN ----------

async def main():

    await init_db()

    stats["status"] = "running"
    await save_stats(stats)

    logger.info("Bot started")

    await client.start()
    await client.run_until_disconnected()

    stats["status"] = "stopped"
    await save_stats(stats)


if __name__ == "__main__":
    asyncio.run(main())
