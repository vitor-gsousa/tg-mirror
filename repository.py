import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any


def _utc_now_string() -> str:
    """Return the current UTC timestamp formatted for SQLite."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class SQLiteRepository:
    """Encapsulate SQLite read/write operations behind a small API."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        cursor: sqlite3.Cursor,
        mutex: threading.Lock
    ):
        self._conn = connection
        self._cur = cursor
        self._mutex = mutex

    def init_schema(self):
        """Create required tables and apply lightweight migrations."""
        with self._mutex:
            self._cur.execute("""
            CREATE TABLE IF NOT EXISTS processed (
                chat_id INTEGER,
                message_id INTEGER,
                created_at TEXT,
                PRIMARY KEY (chat_id, message_id)
            )
            """)
            self._cur.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                chat_id INTEGER PRIMARY KEY,
                name TEXT
            )
            """)
            self._cur.execute("""
            CREATE TABLE IF NOT EXISTS message_codes (
                code TEXT PRIMARY KEY,
                created_at TEXT
            )
            """)
            self._cur.execute("""
            CREATE TABLE IF NOT EXISTS url_filters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern TEXT,
                replacement TEXT,
                sort_order INTEGER DEFAULT 0
            )
            """)

            filter_columns = [row[1] for row in self._cur.execute(
                "PRAGMA table_info(url_filters)"
            ).fetchall()]
            if "sort_order" not in filter_columns:
                self._cur.execute(
                    "ALTER TABLE url_filters ADD COLUMN sort_order "
                    "INTEGER DEFAULT 0"
                )
                self._cur.execute("""
                    UPDATE url_filters
                    SET sort_order = id
                    WHERE sort_order IS NULL OR sort_order = 0
                """)

            columns = [row[1] for row in self._cur.execute(
                "PRAGMA table_info(processed)"
            ).fetchall()]
            if "created_at" not in columns:
                self._cur.execute(
                    "ALTER TABLE processed ADD COLUMN created_at TEXT"
                )
                self._cur.execute(
                    "UPDATE processed SET created_at = ? "
                    "WHERE created_at IS NULL",
                    (_utc_now_string(),)
                )
            self._conn.commit()

    def get_channel_labels(self) -> dict[int, str]:
        """Return channel labels keyed by chat id."""
        with self._mutex:
            rows = self._conn.execute(
                "SELECT chat_id, name FROM channels"
            ).fetchall()
        return {row[0]: (row[1] or "") for row in rows}

    def get_processed_counts(self) -> dict[int, int]:
        """Return processed message counts grouped by source chat."""
        with self._mutex:
            rows = self._conn.execute(
                "SELECT chat_id, COUNT(*) FROM processed GROUP BY chat_id"
            ).fetchall()
        return {row[0]: row[1] for row in rows}

    def get_filters(self) -> list[tuple[Any, ...]]:
        """Return URL filters sorted by execution order."""
        with self._mutex:
            return self._conn.execute(
                "SELECT id, pattern, replacement FROM url_filters "
                "ORDER BY sort_order, id"
            ).fetchall()

    def add_filter(self, pattern: str, replacement: str):
        """Insert a new URL filter at the end of the ordering."""
        with self._mutex:
            max_order = self._cur.execute(
                "SELECT COALESCE(MAX(sort_order), 0) FROM url_filters"
            ).fetchone()[0]
            self._cur.execute(
                "INSERT INTO url_filters "
                "(pattern, replacement, sort_order) VALUES (?, ?, ?)",
                (pattern, replacement, max_order + 1)
            )
            self._conn.commit()

    def update_filter(self, filter_id: int, pattern: str, replacement: str):
        """Update pattern and replacement for an existing filter."""
        with self._mutex:
            self._cur.execute(
                "UPDATE url_filters SET pattern=?, replacement=? WHERE id=?",
                (pattern, replacement, filter_id)
            )
            self._conn.commit()

    def delete_filter(self, filter_id: int):
        """Delete one filter by identifier."""
        with self._mutex:
            self._cur.execute(
                "DELETE FROM url_filters WHERE id=?",
                (filter_id,)
            )
            self._conn.commit()

    def move_filter(self, filter_id: int, move_up: bool):
        """Move one filter up or down by swapping sort_order."""
        comparator = "<" if move_up else ">"
        order_by = "DESC" if move_up else "ASC"
        neighbor_query = (
            "SELECT id, sort_order FROM url_filters "
            f"WHERE sort_order {comparator} ? "
            f"ORDER BY sort_order {order_by} LIMIT 1"
        )

        with self._mutex:
            current = self._cur.execute(
                "SELECT id, sort_order FROM url_filters WHERE id=?",
                (filter_id,)
            ).fetchone()
            if not current:
                return

            current_id, current_order = current
            neighbor = self._cur.execute(
                neighbor_query,
                (current_order,)
            ).fetchone()
            if not neighbor:
                return

            neighbor_id, neighbor_order = neighbor
            self._cur.execute(
                "UPDATE url_filters SET sort_order=? WHERE id=?",
                (neighbor_order, current_id)
            )
            self._cur.execute(
                "UPDATE url_filters SET sort_order=? WHERE id=?",
                (current_order, neighbor_id)
            )
            self._conn.commit()

    def upsert_channel(self, chat_id: int, name: str):
        """Insert or update source channel metadata."""
        with self._mutex:
            self._conn.execute(
                "INSERT OR REPLACE INTO channels "
                "(chat_id, name) VALUES (?, ?)",
                (chat_id, name)
            )
            self._conn.commit()

    def is_processed(self, chat_id: int, msg_id: int) -> bool:
        """Check if a source message was already processed."""
        with self._mutex:
            self._cur.execute(
                "SELECT 1 FROM processed WHERE chat_id=? AND message_id=?",
                (chat_id, msg_id)
            )
            return self._cur.fetchone() is not None

    def mark_processed(self, chat_id: int, msg_id: int):
        """Mark one source message as processed."""
        with self._mutex:
            self._cur.execute(
                "INSERT OR IGNORE INTO processed VALUES (?, ?, ?)",
                (chat_id, msg_id, _utc_now_string())
            )
            self._conn.commit()

    def cleanup_processed(self, cutoff: str) -> int:
        """Delete processed rows older than cutoff timestamp."""
        with self._mutex:
            self._cur.execute(
                "DELETE FROM processed WHERE created_at IS NOT NULL AND "
                "created_at < ?",
                (cutoff,)
            )
            self._conn.commit()
            return self._cur.rowcount or 0

    def clear_code_cache(self) -> int:
        """Delete all deduplication code rows."""
        with self._mutex:
            self._cur.execute("DELETE FROM message_codes")
            self._conn.commit()
            return self._cur.rowcount or 0

    def find_existing_codes(self, codes: list[str]) -> set[str]:
        """Return which codes from input already exist in storage."""
        if not codes:
            return set()

        placeholders = ",".join("?" for _ in codes)
        query = (
            "SELECT code FROM message_codes "
            f"WHERE code IN ({placeholders})"
        )

        with self._mutex:
            rows = self._cur.execute(query, codes).fetchall()
            return {row[0] for row in rows}

    def mark_codes(self, codes: list[str]):
        """Persist deduplication codes ignoring existing entries."""
        if not codes:
            return

        now = _utc_now_string()
        rows = [(code, now) for code in codes]
        with self._mutex:
            self._cur.executemany(
                "INSERT OR IGNORE INTO message_codes "
                "(code, created_at) VALUES (?, ?)",
                rows
            )
            self._conn.commit()

    def execute_select(
        self,
        query: str
    ) -> tuple[list[str], list[tuple[Any, ...]]]:
        """Execute a validated read-only query and return columns and rows."""
        with self._mutex:
            cursor = self._conn.execute(query)
            rows = cursor.fetchall()
            columns = (
                [desc[0] for desc in cursor.description]
                if cursor.description else []
            )
        return columns, rows

    def close(self):
        """Close the underlying SQLite connection."""
        self._conn.close()
