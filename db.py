"""SQLite database setup, queue management, and upsert helpers."""

import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path

from config import DB_PATH


def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    """Return a connection to the SQLite database."""
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            slug TEXT PRIMARY KEY,
            taaft_url TEXT,
            pricing_label_raw TEXT,
            status TEXT DEFAULT 'pending',
            queued_at TEXT,
            scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS agents (
            slug TEXT PRIMARY KEY,
            name TEXT,
            taaft_url TEXT,
            external_url TEXT,
            description TEXT,
            pricing_model TEXT,
            is_agent_tagged BOOLEAN,
            is_agent_inferred BOOLEAN,
            agent_confidence_score INTEGER,
            task_categories TEXT,
            qa_content TEXT,
            saves_count INTEGER,
            rating REAL,
            rating_count INTEGER,
            scraped_at TEXT
        );
    """)
    conn.commit()


def upsert_job(conn: sqlite3.Connection, slug: str, taaft_url: str,
               pricing_label_raw: str, status: str = "pending") -> bool:
    """Insert or update a job. Returns True if a new job was inserted."""
    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        "SELECT status FROM jobs WHERE slug = ?", (slug,)
    )
    existing = cursor.fetchone()

    if existing is None:
        conn.execute(
            """INSERT INTO jobs (slug, taaft_url, pricing_label_raw, status, queued_at)
               VALUES (?, ?, ?, ?, ?)""",
            (slug, taaft_url, pricing_label_raw, status, now),
        )
        conn.commit()
        return True
    else:
        # Don't overwrite a 'done' status unless explicitly re-queuing
        if existing["status"] == "done" and status == "pending":
            return False
        conn.execute(
            """UPDATE jobs SET taaft_url = ?, pricing_label_raw = ?,
               queued_at = ? WHERE slug = ? AND status != 'done'""",
            (taaft_url, pricing_label_raw, now, slug),
        )
        conn.commit()
        return False


def mark_job(conn: sqlite3.Connection, slug: str, status: str) -> None:
    """Update job status and set scraped_at timestamp for terminal states."""
    now = datetime.now(timezone.utc).isoformat()
    if status in ("done", "failed", "skipped", "filtered"):
        conn.execute(
            "UPDATE jobs SET status = ?, scraped_at = ? WHERE slug = ?",
            (status, now, slug),
        )
    else:
        conn.execute(
            "UPDATE jobs SET status = ? WHERE slug = ?",
            (status, slug),
        )
    conn.commit()


def get_pending_jobs(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    """Fetch pending jobs up to limit."""
    cursor = conn.execute(
        "SELECT slug, taaft_url, pricing_label_raw FROM jobs WHERE status = 'pending' LIMIT ?",
        (limit,),
    )
    return cursor.fetchall()


def reset_failed_jobs(conn: sqlite3.Connection) -> int:
    """Reset all failed jobs back to pending. Returns count."""
    cursor = conn.execute(
        "UPDATE jobs SET status = 'pending' WHERE status = 'failed'"
    )
    conn.commit()
    return cursor.rowcount


def reset_all_for_refetch(conn: sqlite3.Connection) -> int:
    """Reset all done jobs back to pending for refetching. Returns count."""
    cursor = conn.execute(
        "UPDATE jobs SET status = 'pending' WHERE status = 'done'"
    )
    conn.commit()
    return cursor.rowcount


def upsert_agent(conn: sqlite3.Connection, data: dict) -> None:
    """Insert or replace an agent record."""
    # Serialize JSON fields
    task_categories = json.dumps(data.get("task_categories", []))
    qa_content = json.dumps(data.get("qa_content", []))

    conn.execute(
        """INSERT OR REPLACE INTO agents
           (slug, name, taaft_url, external_url, description, pricing_model,
            is_agent_tagged, is_agent_inferred, agent_confidence_score,
            task_categories, qa_content, saves_count, rating, rating_count, scraped_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            data["slug"],
            data.get("name"),
            data.get("taaft_url"),
            data.get("external_url"),
            data.get("description"),
            data.get("pricing_model"),
            data.get("is_agent_tagged", False),
            data.get("is_agent_inferred", False),
            data.get("agent_confidence_score", 0),
            task_categories,
            qa_content,
            data.get("saves_count"),
            data.get("rating"),
            data.get("rating_count"),
            data.get("scraped_at"),
        ),
    )
    conn.commit()


def get_stats(conn: sqlite3.Connection) -> dict:
    """Return queue and agent statistics."""
    stats = {}
    for status in ("pending", "done", "filtered", "failed", "skipped"):
        cursor = conn.execute(
            "SELECT COUNT(*) as cnt FROM jobs WHERE status = ?", (status,)
        )
        stats[f"jobs_{status}"] = cursor.fetchone()["cnt"]

    cursor = conn.execute("SELECT COUNT(*) as cnt FROM jobs")
    stats["jobs_total"] = cursor.fetchone()["cnt"]

    cursor = conn.execute("SELECT COUNT(*) as cnt FROM agents")
    stats["agents_total"] = cursor.fetchone()["cnt"]

    cursor = conn.execute(
        "SELECT COUNT(*) as cnt FROM agents WHERE is_agent_tagged = 1 OR is_agent_inferred = 1"
    )
    stats["agents_confirmed"] = cursor.fetchone()["cnt"]

    cursor = conn.execute(
        "SELECT COUNT(*) as cnt FROM agents WHERE is_agent_tagged = 0 AND is_agent_inferred = 0"
    )
    stats["agents_non_agent"] = cursor.fetchone()["cnt"]

    return stats


def get_all_agents(conn: sqlite3.Connection) -> list[dict]:
    """Return all agent records as a list of dicts with parsed JSON fields."""
    cursor = conn.execute("SELECT * FROM agents ORDER BY saves_count DESC NULLS LAST")
    rows = cursor.fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d["task_categories"] = json.loads(d["task_categories"]) if d["task_categories"] else []
        d["qa_content"] = json.loads(d["qa_content"]) if d["qa_content"] else []
        d["is_agent"] = bool(d["is_agent_tagged"] or d["is_agent_inferred"])
        result.append(d)
    return result
