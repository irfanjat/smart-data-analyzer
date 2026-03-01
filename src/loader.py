"""
loader.py — Data Ingestion Module
Loads raw Netflix CSV into SQLite and generates an initial data profile.
Run: python src/loader.py
"""

import sqlite3
import pandas as pd
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "raw" / "netflix_titles.csv"
DB_PATH  = BASE_DIR / "database" / "data.db"


# ── 1. Load CSV ───────────────────────────────────────────────────────────────
def load_csv(path: Path) -> pd.DataFrame:
    print(f"\n📂 Loading CSV from: {path}")
    df = pd.read_csv(path, dtype=str)  # load everything as strings — preserve raw data
    print(f"   ✅ {len(df):,} rows | {len(df.columns)} columns loaded")
    return df


# ── 2. Create SQLite schema ───────────────────────────────────────────────────
def create_schema(conn: sqlite3.Connection):
    print("\n🗄️  Creating database schema...")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_titles (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id      TEXT,
            type         TEXT,
            title        TEXT,
            director     TEXT,
            cast         TEXT,
            country      TEXT,
            date_added   TEXT,
            release_year TEXT,
            rating       TEXT,
            duration     TEXT,
            listed_in    TEXT,
            description  TEXT,
            inserted_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS cleaned_titles (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id      TEXT,
            type         TEXT,
            title        TEXT,
            director     TEXT,
            cast         TEXT,
            country      TEXT,
            date_added   TEXT,
            release_year INTEGER,
            rating       TEXT,
            duration     TEXT,
            listed_in    TEXT,
            description  TEXT,
            cleaned_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS cleaning_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp     TEXT DEFAULT (datetime('now')),
            column_name   TEXT,
            issue_type    TEXT,
            rows_affected INTEGER,
            action_taken  TEXT,
            details       TEXT
        );
    """)
    print("   ✅ Tables created: raw_titles, cleaned_titles, cleaning_log")


# ── 3. Insert raw data ────────────────────────────────────────────────────────
def insert_raw_data(conn: sqlite3.Connection, df: pd.DataFrame):
    print("\n📥 Inserting raw data into database...")
    conn.execute("DELETE FROM raw_titles")
    df.to_sql("raw_titles", conn, if_exists="append", index=False, method="multi", chunksize=500)
    count = conn.execute("SELECT COUNT(*) FROM raw_titles").fetchone()[0]
    print(f"   ✅ {count:,} rows inserted into raw_titles")


# ── 4. Data profile report ────────────────────────────────────────────────────
def print_profile(df: pd.DataFrame):
    print("\n" + "═" * 55)
    print("  📊 INITIAL DATA PROFILE REPORT")
    print("═" * 55)
    print(f"  Total rows    : {len(df):,}")
    print(f"  Total columns : {len(df.columns)}")
    print()
    print(f"  {'Column':<20} {'Type':<10} {'Nulls':>6} {'Null %':>8}")
    print(f"  {'─'*20} {'─'*10} {'─'*6} {'─'*8}")
    for col in df.columns:
        nulls = df[col].isna().sum()
        pct   = (nulls / len(df)) * 100
        print(f"  {col:<20} {'str':<10} {nulls:>6,} {pct:>7.1f}%")
    print("═" * 55)
    print()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not CSV_PATH.exists():
        print(f"\n❌ CSV not found at: {CSV_PATH}")
        print("   Make sure netflix_titles.csv is in data/raw/")
        return

    df   = load_csv(CSV_PATH)
    print(f"\n🔌 Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        with conn:
            create_schema(conn)
            insert_raw_data(conn, df)
        print_profile(df)
        print("🎉 Ingestion complete! Run next: python src/analyzer.py\n")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
