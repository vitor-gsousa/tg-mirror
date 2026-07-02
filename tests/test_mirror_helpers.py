import asyncio
import importlib
import os
import sqlite3
import threading
from datetime import datetime

import pytest


@pytest.fixture(scope="module")
def mirror_module():
    os.environ.setdefault("API_ID", "12345")
    os.environ.setdefault("API_HASH", "dummy_hash")
    os.environ.setdefault("DEST_CHAT", "123")
    os.environ.setdefault("SOURCE_CHATS", "1,2")
    os.environ.setdefault("ADMIN_PASSWORD", "secret")
    os.environ.setdefault("SESSION", "test_session")

    return importlib.import_module("mirror")


def test_validate_readonly_query_rejects_empty(mirror_module):
    assert mirror_module.validate_readonly_query("") == "Query cannot be empty"


def test_validate_readonly_query_rejects_non_select(mirror_module):
    assert (
        mirror_module.validate_readonly_query("DELETE FROM users")
        == "Only SELECT queries are allowed for safety"
    )


def test_validate_readonly_query_accepts_select(mirror_module):
    assert (
        mirror_module.validate_readonly_query("SELECT * FROM processed")
        is None
    )


def test_parse_cleanup_time_valid(mirror_module):
    assert mirror_module.parse_cleanup_time("23:59") == (23, 59)


def test_parse_cleanup_time_invalid_returns_default(mirror_module):
    assert mirror_module.parse_cleanup_time("99:99") == (0, 5)


def test_parse_cleanup_days_valid(mirror_module):
    assert mirror_module.parse_cleanup_days("15") == 15


def test_parse_cleanup_days_invalid_returns_default(mirror_module):
    assert (
        mirror_module.parse_cleanup_days("invalid")
        == mirror_module.CLEANUP_DAYS_DEFAULT
    )


def test_seconds_until_next_run_same_day(mirror_module, monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 1, 1, 10, 0, 0, tzinfo=tz)

    monkeypatch.setattr(mirror_module, "datetime", FixedDateTime)
    assert mirror_module.seconds_until_next_run(10, 5) == 300


def test_seconds_until_next_run_next_day(mirror_module, monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 1, 1, 10, 0, 0, tzinfo=tz)

    monkeypatch.setattr(mirror_module, "datetime", FixedDateTime)
    assert mirror_module.seconds_until_next_run(9, 55) == 86100


def test_deduplicate_codes_preserves_order(mirror_module, monkeypatch):
    monkeypatch.setattr(
        mirror_module,
        "extract_codes",
        lambda _text: ["ABC123", "XYZ999", "ABC123", "TTT111", "XYZ999"]
    )
    assert mirror_module.deduplicate_codes("ignored") == [
        "ABC123",
        "XYZ999",
        "TTT111"
    ]


def test_run_select_query_returns_columns_and_rows(mirror_module, monkeypatch):
    test_conn = sqlite3.connect(":memory:")
    test_conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
    test_conn.execute("INSERT INTO items (id, name) VALUES (1, 'one')")
    test_conn.commit()

    test_repository = mirror_module.SQLiteRepository(
        test_conn,
        test_conn.cursor(),
        threading.Lock()
    )
    monkeypatch.setattr(mirror_module, "repository", test_repository)

    columns, rows = mirror_module.run_select_query(
        "SELECT id, name FROM items ORDER BY id"
    )
    assert columns == ["id", "name"]
    assert rows == [(1, "one")]


def test_has_duplicate_codes_returns_false_for_empty_list(mirror_module):
    result = asyncio.run(mirror_module.has_duplicate_codes([], 100, 200))
    assert result is False


def test_has_duplicate_codes_returns_true_when_match_exists(
    mirror_module,
    monkeypatch
):
    monkeypatch.setattr(
        mirror_module,
        "find_existing_codes",
        lambda _codes: {"DUP1"}
    )
    result = asyncio.run(mirror_module.has_duplicate_codes(["DUP1"], 100, 200))
    assert result is True


def test_increment_message_counter_updates_stats(mirror_module, monkeypatch):
    mirror_module.stats["messages"] = 0
    monkeypatch.setattr(mirror_module, "save_stats", lambda _data: None)

    asyncio.run(mirror_module.increment_message_counter())

    assert mirror_module.stats["messages"] == 1
