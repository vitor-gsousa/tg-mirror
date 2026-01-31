import os
import json
import sqlite3
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from asyncio import Lock
db_lock = Lock()

load_dotenv("/config/.env")

STATS_PATH = "/data/stats.json"

def save_stats(data):
    with open(STATS_PATH, "w") as f:
        json.dump(data, f)
        f.flush()
        os.fsync(f.fileno())

def load_stats():
    if not os.path.exists(STATS_PATH):
        return {"messages": 0, "status": "starting"}
    with open(STATS_PATH) as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {"messages": 0, "status": "reset"}

stats = load_stats()
stats["status"] = "running"
save_stats(stats)

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]

SESSION_STRING = os.environ.get("SESSION_STRING")
SESSION_NAME = os.environ.get("SESSION", "mirror")

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

DEST_CHAT = int(os.environ["DEST_CHAT"])
SOURCE_CHATS = [
    int(x.strip()) for x in os.environ["SOURCE_CHATS"].split(",")
]

# SQLite (persistence)
DB_PATH = "/data/state.db"
os.makedirs("/data", exist_ok=True)
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

@client.on(events.NewMessage(chats=SOURCE_CHATS))
async def handler(event):
    chat_id = event.chat_id
    msg_id = event.id

    async with db_lock:
        cur.execute(
            "SELECT 1 FROM processed WHERE chat_id=? AND message_id=?",
            (chat_id, msg_id)
        )
        if cur.fetchone():
            return

    msg = event.message

    text = msg.text or ""
    entities = msg.entities

    use_format = bool(entities)

    try:
        if msg.media:
            await client.send_file(
                DEST_CHAT,
                msg.media,
                caption=text,
                formatting_entities=entities if use_format else None,
                silent=True
            )
        else:
            await client.send_message(
                DEST_CHAT,
                text,
                formatting_entities=entities if use_format else None,
                silent=True
            )
    except Exception as e:
        print(f"Error forwarding message {msg_id} from chat {chat_id}: {e}")
        return

    async with db_lock:
        cur.execute(
            "INSERT OR IGNORE INTO processed VALUES (?, ?)",
            (chat_id, msg_id)
        )
        conn.commit()
    stats["messages"] += 1
    save_stats(stats)


# start
client.start()
client.run_until_disconnected()

stats["status"] = "stopped"
save_stats(stats)
