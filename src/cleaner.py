"""
cleaner.py — Data Cleaning Pipeline
Fixes all issues found by analyzer.py, logs every operation, and writes
cleaned data to both cleaned_titles (SQLite) and data/cleaned/netflix_cleaned.csv.
Run: python src/cleaner.py
"""

import sqlite3
import pandas as pd
from pathlib import Path
from logger import log

BASE_DIR     = Path(__file__).resolve().parent.parent
DB_PATH      = BASE_DIR / "database" / "data.db"
CLEANED_PATH = BASE_DIR / "data" / "cleaned" / "netflix_cleaned.csv"
LOG_PATH     = BASE_DIR / "logs" / "cleaning_log.txt"


# ── Fix 1: Misplaced duration values in 'rating' column ──────────────────────
def fix_rating_duration_swap(df: pd.DataFrame, conn) -> pd.DataFrame:
    mask = df["rating"].str.match(r"^\d+ min$", na=False)
    affected = mask.sum()
    if affected:
        # Move the value to duration, then clear rating
        df.loc[mask, "duration"] = df.loc[mask, "rating"]
        df.loc[mask, "rating"]   = None
        log(conn, "rating/duration", "misplaced_value", affected,
            "moved duration string from rating → duration, set rating to null",
            f"values: {df.loc[mask, 'duration'].tolist()}")
        print(f"   ✅ Fixed {affected} rows where duration was in rating column")
    return df


# ── Fix 2: Fill missing values ────────────────────────────────────────────────
def fix_missing_values(df: pd.DataFrame, conn) -> pd.DataFrame:
    fills = {
        "director"  : "Unknown",
        "cast"      : "Unknown",
        "country"   : "Unknown",
        "date_added": "Unknown",
        "rating"    : "Unknown",
        "duration"  : "Unknown",
    }
    for col, fill_val in fills.items():
        nulls = df[col].isna().sum()
        if nulls:
            df[col] = df[col].fillna(fill_val)
            log(conn, col, "missing_value", int(nulls),
                f"filled with '{fill_val}'", f"{nulls} nulls replaced")
            print(f"   ✅ {col}: filled {nulls:,} nulls with '{fill_val}'")
    return df


# ── Fix 3: Strip whitespace from text columns ─────────────────────────────────
def fix_whitespace(df: pd.DataFrame, conn) -> pd.DataFrame:
    text_cols = ["title", "director", "cast", "country", "rating", "listed_in", "description"]
    for col in text_cols:
        if col not in df.columns:
            continue
        before   = df[col].copy()
        df[col]  = df[col].str.strip()
        affected = (before != df[col]).sum()
        if affected:
            log(conn, col, "whitespace", int(affected),
                "stripped leading/trailing whitespace", f"{affected} rows fixed")
            print(f"   ✅ {col}: stripped whitespace in {affected} row(s)")
    return df


# ── Fix 4: Standardise date_added format ─────────────────────────────────────
def fix_date_format(df: pd.DataFrame, conn) -> pd.DataFrame:
    mask    = df["date_added"] != "Unknown"
    parsed  = pd.to_datetime(df.loc[mask, "date_added"], errors="coerce")
    good    = parsed.notna().sum()
    df.loc[mask, "date_added"] = parsed.dt.strftime("%Y-%m-%d")
    log(conn, "date_added", "format_inconsistency", int(good),
        "standardised to YYYY-MM-DD", f"{good} dates reformatted")
    print(f"   ✅ date_added: standardised {good:,} dates to YYYY-MM-DD")
    return df


# ── Fix 5: Cast release_year to integer ──────────────────────────────────────
def fix_release_year(df: pd.DataFrame, conn) -> pd.DataFrame:
    before   = df["release_year"].copy()
    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce").astype("Int64")
    affected = (before != df["release_year"].astype(str)).sum()
    log(conn, "release_year", "type_cast", int(len(df)),
        "cast TEXT → Integer", "all rows converted")
    print(f"   ✅ release_year: cast to integer")
    return df


# ── Fix 6: Flag old-content outliers (keep rows, add note) ───────────────────
def flag_outliers(df: pd.DataFrame, conn) -> pd.DataFrame:
    old_mask = df["release_year"].notna() & (df["release_year"] < 2004)
    count    = old_mask.sum()
    # We keep these rows — they're real classic content, not data errors
    log(conn, "release_year", "outlier_flagged", int(count),
        "kept — legitimate classic content (pre-2004)",
        f"{count} titles with release_year < 2004")
    print(f"   ✅ release_year: {count:,} pre-2004 titles flagged (kept — valid data)")
    return df


# ── Write cleaned data ────────────────────────────────────────────────────────
def save_cleaned(df: pd.DataFrame, conn):
    cols = ["show_id","type","title","director","cast","country",
            "date_added","release_year","rating","duration","listed_in","description"]
    clean = df[cols].copy()

    # SQLite
    conn.execute("DELETE FROM cleaned_titles")
    clean.to_sql("cleaned_titles", conn, if_exists="append", index=False,
                 method="multi", chunksize=500)
    print(f"\n   ✅ {len(clean):,} rows written to cleaned_titles (SQLite)")

    # CSV
    clean.to_csv(CLEANED_PATH, index=False)
    print(f"   ✅ Cleaned CSV saved to: {CLEANED_PATH}")


# ── Summary ───────────────────────────────────────────────────────────────────
def print_summary(conn):
    rows = conn.execute("SELECT COUNT(*) FROM cleaning_log").fetchone()[0]
    ops  = conn.execute("""
        SELECT issue_type, SUM(rows_affected)
        FROM cleaning_log GROUP BY issue_type
    """).fetchall()

    print("\n" + "═" * 55)
    print("  📋 CLEANING SUMMARY")
    print("═" * 55)
    print(f"  Total operations logged : {rows}")
    print(f"\n  {'Issue Type':<30} {'Rows Fixed':>10}")
    print(f"  {'─'*30} {'─'*10}")
    for issue, count in ops:
        print(f"  {issue:<30} {count:>10,}")
    print("═" * 55)
    print("\n  🎉 Cleaning complete! Run next: streamlit run dashboard/app.py\n")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # Clear old log file
    LOG_PATH.parent.mkdir(exist_ok=True)
    LOG_PATH.write_text("")

    print("\n🔌 Connecting to database...")
    conn = sqlite3.connect(DB_PATH)
    df   = pd.read_sql("SELECT * FROM raw_titles", conn)
    print(f"   ✅ {len(df):,} rows loaded\n")

    print("🧹 Running cleaning pipeline...")
    try:
        with conn:
            df = fix_rating_duration_swap(df, conn)
            df = fix_missing_values(df, conn)
            df = fix_whitespace(df, conn)
            df = fix_date_format(df, conn)
            df = fix_release_year(df, conn)
            df = flag_outliers(df, conn)
            save_cleaned(df, conn)

        print_summary(conn)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
