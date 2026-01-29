import os
import sqlite3
from telethon import TelegramClient, events
from dotenv import load_dotenv

load_dotenv("/config/.env")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION = os.environ.get("SESSION", "mirror")
DEST_CHAT = int(os.environ["DEST_CHAT"])
SOURCE_CHATS = [int(x.strip()) for x in os.environ["SOURCE_CHATS"].split(",")]

DB_PATH = "/data/state.db"

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS processed (
    chat_id INTEGER,
    message_id INTEGER,
    PRIMARY KEY (chat_id, message_id)
)
""")
conn.commit()

client = TelegramClient(SESSION, API_ID, API_HASH)

@client.on(events.NewMessage(chats=SOURCE_CHATS))
async def handler(event):
    chat_id, msg_id = event.chat_id, event.id
    cur.execute(
        "SELECT 1 FROM processed WHERE chat_id=? AND message_id=?",
        (chat_id, msg_id)
    )
    if cur.fetchone():
        return

    await client.forward_messages(DEST_CHAT, event.message, silent=True)

    cur.execute(
        "INSERT OR IGNORE INTO processed VALUES (?, ?)",
        (chat_id, msg_id)
    )
    conn.commit()

client.start()
client.run_until_disconnected()
