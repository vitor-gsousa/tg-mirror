import re
from datetime import datetime, timedelta, timezone
from typing import Any

from repository import SQLiteRepository


def get_channel_stats(
    repository: SQLiteRepository,
    source_chats: list[int]
) -> list[dict[str, Any]]:
    """Build ordered per-channel message counters for dashboard rendering."""
    labels: dict[int, str] = {}
    try:
        labels = repository.get_channel_labels()
    except Exception:
        labels = {}

    counts: dict[int, int] = {}
    try:
        counts = repository.get_processed_counts()
    except Exception:
        counts = {}

    ordered = []
    for chat_id in source_chats:
        ordered.append({
            "chat_id": chat_id,
            "name": labels.get(chat_id, ""),
            "messages": counts.get(chat_id, 0)
        })

    for chat_id, msg_count in counts.items():
        if chat_id not in source_chats:
            ordered.append({
                "chat_id": chat_id,
                "name": labels.get(chat_id, ""),
                "messages": msg_count
            })

    return ordered


def get_filters(repository: SQLiteRepository) -> list[tuple[Any, ...]]:
    """Return URL filter rules sorted by execution order."""
    return repository.get_filters()


def move_filter(repository: SQLiteRepository, filter_id: int, move_up: bool):
    """Move one filter up or down in execution order."""
    repository.move_filter(filter_id, move_up)


def is_processed(
    repository: SQLiteRepository,
    chat_id: int,
    msg_id: int
) -> bool:
    """Check whether a source message was already mirrored."""
    return repository.is_processed(chat_id, msg_id)


def mark_processed(repository: SQLiteRepository, chat_id: int, msg_id: int):
    """Persist one source message as processed."""
    repository.mark_processed(chat_id, msg_id)


def cleanup_processed(repository: SQLiteRepository, days: int) -> int:
    """Delete processed-message rows older than the configured retention."""
    if days <= 0:
        return 0

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return repository.cleanup_processed(cutoff)


def cleanup_code_cache(repository: SQLiteRepository) -> int:
    """Clear all cached deduplication codes."""
    return repository.clear_code_cache()


def normalize_code(code: str) -> str:
    """Normalize a deduplication code for stable comparisons."""
    return code.strip().upper()


def extract_codes(
    text: str,
    code_pattern: re.Pattern[str] | None
) -> list[str]:
    """Extract candidate deduplication codes from text."""
    if not text or code_pattern is None:
        return []

    codes = []
    for match in code_pattern.finditer(text):
        if match.lastindex and match.lastindex > 0:
            codes.append(normalize_code(match.group(1)))
        else:
            codes.append(normalize_code(match.group(0)))

    return codes


def find_existing_codes(
    repository: SQLiteRepository,
    codes: list[str]
) -> set[str]:
    """Fetch codes that are already present in the dedup cache."""
    return repository.find_existing_codes(codes)


def mark_codes(repository: SQLiteRepository, codes: list[str]):
    """Store newly observed codes for future deduplication."""
    repository.mark_codes(codes)


def deduplicate_codes(
    text: str,
    code_pattern: re.Pattern[str] | None
) -> list[str]:
    """Extract and deduplicate normalized codes preserving order."""
    return list(dict.fromkeys(extract_codes(text, code_pattern)))


def validate_readonly_query(query: str) -> str | None:
    """Validate whether a query is allowed in the admin SQL endpoint."""
    if not query:
        return "Query cannot be empty"
    if not query.upper().startswith("SELECT"):
        return "Only SELECT queries are allowed for safety"
    return None


def run_select_query(
    repository: SQLiteRepository,
    query: str
) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Execute a validated read-only SQL query."""
    return repository.execute_select(query)
