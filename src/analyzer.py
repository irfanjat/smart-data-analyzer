"""
analyzer.py — Data Quality Assessment Module
Scans raw_titles and produces a full quality report.
Run: python src/analyzer.py
"""

import sqlite3
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "database" / "data.db"


# ── Load raw data from DB ─────────────────────────────────────────────────────
def load_raw(conn) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM raw_titles", conn)


# ── 1. Missing Values ─────────────────────────────────────────────────────────
def check_missing(df: pd.DataFrame) -> dict:
    results = {}
    for col in df.columns:
        if col in ("id", "inserted_at"):
            continue
        nulls = df[col].isna().sum()
        if nulls > 0:
            results[col] = {"nulls": int(nulls), "pct": round(nulls / len(df) * 100, 1)}
    return results


# ── 2. Duplicates ─────────────────────────────────────────────────────────────
def check_duplicates(df: pd.DataFrame) -> dict:
    exact     = df.duplicated(subset=["title", "type", "release_year"]).sum()
    show_id   = df.duplicated(subset=["show_id"]).sum()
    return {"exact_duplicates": int(exact), "duplicate_show_ids": int(show_id)}


# ── 3. Outliers (release_year) ────────────────────────────────────────────────
def check_outliers(df: pd.DataFrame) -> dict:
    years = pd.to_numeric(df["release_year"], errors="coerce").dropna()
    Q1, Q3 = years.quantile(0.25), years.quantile(0.75)
    IQR     = Q3 - Q1
    lower, upper = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR
    outliers = years[(years < lower) | (years > upper)]
    return {
        "column"  : "release_year",
        "Q1"      : int(Q1), "Q3": int(Q3),
        "lower_bound": int(lower), "upper_bound": int(upper),
        "outlier_count": len(outliers),
        "outlier_values": sorted(outliers.unique().tolist())
    }


# ── 4. Type Inconsistencies ───────────────────────────────────────────────────
def check_types(df: pd.DataFrame) -> dict:
    issues = {}
    # release_year should be numeric
    bad_years = df["release_year"].dropna()
    non_numeric = bad_years[pd.to_numeric(bad_years, errors="coerce").isna()]
    if len(non_numeric):
        issues["release_year"] = {"expected": "integer", "bad_rows": len(non_numeric), "samples": non_numeric.head(3).tolist()}

    # rating — check for values that look like durations (e.g. "74 min")
    valid_ratings = {"G","PG","PG-13","R","NC-17","TV-Y","TV-Y7","TV-Y7-FV","TV-G","TV-PG","TV-14","TV-MA","NR","UR"}
    bad_ratings = df["rating"].dropna()[~df["rating"].dropna().isin(valid_ratings)]
    if len(bad_ratings):
        issues["rating"] = {"expected": "valid rating code", "bad_rows": len(bad_ratings), "samples": bad_ratings.head(3).tolist()}

    return issues


# ── 5. Format Inconsistencies ─────────────────────────────────────────────────
def check_formats(df: pd.DataFrame) -> dict:
    issues = {}

    # date_added — look for non-standard formats
    date_col = df["date_added"].dropna().str.strip()
    def is_valid_date(d):
        try:
            pd.to_datetime(d)
            return True
        except:
            return False
    bad_dates = date_col[~date_col.apply(is_valid_date)]
    if len(bad_dates):
        issues["date_added"] = {"bad_rows": len(bad_dates), "samples": bad_dates.head(3).tolist()}

    # Whitespace issues
    for col in ["title", "director", "country", "rating"]:
        if col not in df.columns:
            continue
        has_ws = df[col].dropna()[df[col].dropna() != df[col].dropna().str.strip()]
        if len(has_ws):
            issues[f"{col}_whitespace"] = {"rows_with_leading_trailing_spaces": len(has_ws)}

    # Inconsistent casing in 'type'
    types = df["type"].dropna().unique().tolist()
    issues["type_values"] = {"unique_values": types}

    return issues


# ── Print report ──────────────────────────────────────────────────────────────
def print_report(missing, dupes, outliers, types, formats):
    print("\n" + "═" * 60)
    print("  🔍 DATA QUALITY ASSESSMENT REPORT")
    print("═" * 60)

    print("\n  1️⃣  MISSING VALUES")
    print(f"  {'Column':<20} {'Nulls':>6} {'%':>7}")
    print(f"  {'─'*20} {'─'*6} {'─'*7}")
    for col, info in missing.items():
        print(f"  {col:<20} {info['nulls']:>6,} {info['pct']:>6.1f}%")

    print("\n  2️⃣  DUPLICATES")
    print(f"  Exact duplicates (title+type+year) : {dupes['exact_duplicates']:,}")
    print(f"  Duplicate show_ids                 : {dupes['duplicate_show_ids']:,}")

    print("\n  3️⃣  OUTLIERS — release_year")
    print(f"  IQR range   : {outliers['lower_bound']} – {outliers['upper_bound']}")
    print(f"  Outliers    : {outliers['outlier_count']} rows")
    if outliers["outlier_values"]:
        print(f"  Values      : {outliers['outlier_values']}")

    print("\n  4️⃣  TYPE INCONSISTENCIES")
    if types:
        for col, info in types.items():
            print(f"  {col}: {info['bad_rows']} bad rows — expected {info['expected']}")
            print(f"    samples: {info['samples']}")
    else:
        print("  ✅ No type issues found")

    print("\n  5️⃣  FORMAT INCONSISTENCIES")
    for key, info in formats.items():
        print(f"  {key}: {info}")

    print("\n" + "═" * 60)
    print("  🎉 Assessment complete! Run next: python src/cleaner.py")
    print("═" * 60 + "\n")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("\n🔌 Connecting to database...")
    conn = sqlite3.connect(DB_PATH)
    df   = load_raw(conn)
    conn.close()
    print(f"   ✅ {len(df):,} rows loaded from raw_titles")

    print("\n⏳ Running quality checks...")
    missing  = check_missing(df)
    dupes    = check_duplicates(df)
    outliers = check_outliers(df)
    types    = check_types(df)
    formats  = check_formats(df)

    print_report(missing, dupes, outliers, types, formats)


if __name__ == "__main__":
    main()
