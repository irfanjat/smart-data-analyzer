"""
logger.py — Audit Logging Module
Records every cleaning operation to SQLite and a text file.
"""

import sqlite3
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_PATH = BASE_DIR / "logs" / "cleaning_log.txt"


def log(conn: sqlite3.Connection, column: str, issue_type: str,
        rows_affected: int, action_taken: str, details: str = ""):
    """Write one entry to both the DB table and the text log file."""

    # 1. SQLite
    conn.execute("""
        INSERT INTO cleaning_log (column_name, issue_type, rows_affected, action_taken, details)
        VALUES (?, ?, ?, ?, ?)
    """, (column, issue_type, rows_affected, action_taken, details))

    # 2. Text file
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = (f"[{ts}] col={column:<20} issue={issue_type:<25} "
            f"rows={rows_affected:>5} | {action_taken} | {details}\n")
    with open(LOG_PATH, "a") as f:
        f.write(line)
