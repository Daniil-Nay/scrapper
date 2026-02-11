from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id INTEGER NOT NULL,
    channel_username TEXT,
    channel_title TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    post_url TEXT,
    post_datetime TEXT NOT NULL,
    message_text TEXT,
    views INTEGER,
    forwards INTEGER,
    UNIQUE(channel_id, message_id)
);

CREATE TABLE IF NOT EXISTS links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    link_type TEXT NOT NULL,
    UNIQUE(post_id, url),
    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id INTEGER NOT NULL,
    snapshot_date TEXT NOT NULL,
    total_reactions INTEGER NOT NULL,
    reactions_json TEXT NOT NULL,
    views INTEGER,
    forwards INTEGER,
    UNIQUE(post_id, snapshot_date),
    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
);
"""


def ensure_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA)
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(posts)").fetchall()
        }
        if "post_url" not in columns:
            conn.execute("ALTER TABLE posts ADD COLUMN post_url TEXT")
        conn.commit()


@contextmanager
def open_db(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        yield conn
    finally:
        conn.close()


def upsert_post(
    conn: sqlite3.Connection,
    *,
    channel_id: int,
    channel_username: str | None,
    channel_title: str,
    message_id: int,
    post_url: str | None,
    post_datetime: str,
    message_text: str | None,
    views: int | None,
    forwards: int | None,
) -> int:
    conn.execute(
        """
        INSERT INTO posts (
            channel_id, channel_username, channel_title, message_id,
            post_url, post_datetime, message_text, views, forwards
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel_id, message_id) DO UPDATE SET
            channel_username=excluded.channel_username,
            channel_title=excluded.channel_title,
            post_url=excluded.post_url,
            post_datetime=excluded.post_datetime,
            message_text=excluded.message_text,
            views=excluded.views,
            forwards=excluded.forwards
        """,
        (
            channel_id,
            channel_username,
            channel_title,
            message_id,
            post_url,
            post_datetime,
            message_text,
            views,
            forwards,
        ),
    )

    row = conn.execute(
        "SELECT id FROM posts WHERE channel_id = ? AND message_id = ?",
        (channel_id, message_id),
    ).fetchone()
    if row is None:
        raise RuntimeError("Failed to resolve post id after upsert.")
    return int(row["id"])


def replace_links(conn: sqlite3.Connection, post_id: int, links: list[tuple[str, str]]) -> None:
    conn.execute("DELETE FROM links WHERE post_id = ?", (post_id,))
    if not links:
        return
    conn.executemany(
        "INSERT OR IGNORE INTO links (post_id, url, link_type) VALUES (?, ?, ?)",
        [(post_id, url, link_type) for url, link_type in links],
    )


def upsert_snapshot(
    conn: sqlite3.Connection,
    *,
    post_id: int,
    snapshot_date: date,
    total_reactions: int,
    reactions: dict[str, int],
    views: int | None,
    forwards: int | None,
) -> None:
    conn.execute(
        """
        INSERT INTO snapshots (
            post_id, snapshot_date, total_reactions, reactions_json, views, forwards
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(post_id, snapshot_date) DO UPDATE SET
            total_reactions=excluded.total_reactions,
            reactions_json=excluded.reactions_json,
            views=excluded.views,
            forwards=excluded.forwards
        """,
        (
            post_id,
            snapshot_date.isoformat(),
            total_reactions,
            json.dumps(reactions, ensure_ascii=True, sort_keys=True),
            views,
            forwards,
        ),
    )
