"""
app.py — Streamlit Dashboard
Run: streamlit run dashboard/app.py
"""

import sqlite3
import pandas as pd
import plotly.express as px
import streamlit as st
from pathlib import Path

BASE_DIR     = Path(__file__).resolve().parent.parent
DB_PATH      = BASE_DIR / "database" / "data.db"
CLEANED_PATH = BASE_DIR / "data" / "cleaned" / "netflix_cleaned.csv"

st.set_page_config(page_title="Smart Data Quality Analyzer", page_icon="🎬", layout="wide")

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    conn    = sqlite3.connect(DB_PATH)
    raw     = pd.read_sql("SELECT * FROM raw_titles",     conn)
    cleaned = pd.read_sql("SELECT * FROM cleaned_titles", conn)
    log     = pd.read_sql("SELECT * FROM cleaning_log",   conn)
    conn.close()
    return raw, cleaned, log

raw, cleaned, audit_log = load_data()

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("🎬 Netflix Data Analyzer")
page = st.sidebar.radio("Navigate", [
    "📊 Overview",
    "🔍 Missing Values",
    "📈 Distributions",
    "🗂️ Category Analysis",
    "📋 Cleaning Log",
    "💾 Export"
])

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Overview
# ═════════════════════════════════════════════════════════════════════════════
if page == "📊 Overview":
    st.title("📊 Data Quality Overview")
    st.caption("Before vs. After cleaning comparison")

    # Top metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Titles",     f"{len(cleaned):,}")
    col2.metric("Movies",           f"{(cleaned['type']=='Movie').sum():,}")
    col3.metric("TV Shows",         f"{(cleaned['type']=='TV Show').sum():,}")
    col4.metric("Cleaning Ops",     f"{len(audit_log):,}")

    st.divider()

    # Before / After null comparison
    st.subheader("Missing Values: Before vs. After")
    cols = ["director","cast","country","date_added","rating","duration"]
    before = [raw[c].isna().sum()   for c in cols]
    after  = [cleaned[c].isna().sum() for c in cols]

    compare_df = pd.DataFrame({"Column": cols*2,
                                "Nulls":  before + after,
                                "Stage":  ["Before"]*len(cols) + ["After"]*len(cols)})
    fig = px.bar(compare_df, x="Column", y="Nulls", color="Stage", barmode="group",
                 color_discrete_map={"Before":"#EF553B","After":"#00CC96"},
                 title="Null Count Before vs. After Cleaning")
    st.plotly_chart(fig, use_container_width=True)

    # Quality score
    st.subheader("Data Quality Score")
    total_cells  = len(raw) * len(cols)
    nulls_before = sum(before)
    nulls_after  = sum(after)
    score_before = round((1 - nulls_before / total_cells) * 100, 1)
    score_after  = round((1 - nulls_after  / total_cells) * 100, 1)
    c1, c2 = st.columns(2)
    c1.metric("Before Cleaning", f"{score_before}%",  delta=None)
    c2.metric("After Cleaning",  f"{score_after}%",   delta=f"+{score_after-score_before:.1f}%")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Missing Values Heatmap
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🔍 Missing Values":
    st.title("🔍 Missing Values Analysis")

    cols = ["director","cast","country","date_added","rating","duration"]
    null_pct = {c: round(raw[c].isna().mean()*100, 1) for c in cols}

    st.subheader("Null Percentage per Column (Raw Data)")
    fig = px.bar(x=list(null_pct.keys()), y=list(null_pct.values()),
                 labels={"x":"Column","y":"Null %"},
                 color=list(null_pct.values()),
                 color_continuous_scale="Reds",
                 title="Missing Value % by Column")
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

    # Heatmap sample
    st.subheader("Null Heatmap — First 200 Rows")
    sample = raw[cols].head(200).isna().astype(int)
    fig2 = px.imshow(sample.T, color_continuous_scale=["#00CC96","#EF553B"],
                     labels={"color":"Is Null"},
                     title="Red = Missing, Green = Present",
                     aspect="auto")
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Null Counts Table")
    table = pd.DataFrame({"Column": cols,
                           "Nulls (raw)": [raw[c].isna().sum() for c in cols],
                           "Null % (raw)": [f"{raw[c].isna().mean()*100:.1f}%" for c in cols],
                           "Nulls (cleaned)": [cleaned[c].isna().sum() for c in cols]})
    st.dataframe(table, use_container_width=True, hide_index=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Distributions
# ═════════════════════════════════════════════════════════════════════════════
elif page == "📈 Distributions":
    st.title("📈 Distributions")

    # Release year histogram
    st.subheader("Release Year Distribution")
    years = pd.to_numeric(cleaned["release_year"], errors="coerce").dropna()
    fig = px.histogram(years, nbins=40, title="Titles by Release Year",
                       labels={"value":"Release Year","count":"Count"},
                       color_discrete_sequence=["#636EFA"])
    st.plotly_chart(fig, use_container_width=True)

    # Box plot
    st.subheader("Release Year — Box Plot")
    fig2 = px.box(cleaned, x="type", y="release_year", color="type",
                  title="Release Year Spread by Type",
                  color_discrete_map={"Movie":"#EF553B","TV Show":"#00CC96"})
    st.plotly_chart(fig2, use_container_width=True)

    # Movies added per year
    st.subheader("Titles Added to Netflix Over Time")
    dated = cleaned[cleaned["date_added"] != "Unknown"].copy()
    dated["year_added"] = pd.to_datetime(dated["date_added"], errors="coerce").dt.year
    by_year = dated.groupby(["year_added","type"]).size().reset_index(name="count")
    fig3 = px.line(by_year, x="year_added", y="count", color="type",
                   title="New Titles Added per Year",
                   color_discrete_map={"Movie":"#EF553B","TV Show":"#00CC96"})
    st.plotly_chart(fig3, use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Category Analysis
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🗂️ Category Analysis":
    st.title("🗂️ Category Analysis")

    # Ratings
    st.subheader("Content Ratings")
    ratings = cleaned[cleaned["rating"] != "Unknown"]["rating"].value_counts().reset_index()
    ratings.columns = ["Rating","Count"]
    fig = px.bar(ratings, x="Rating", y="Count", color="Count",
                 color_continuous_scale="Blues", title="Titles by Rating")
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

    # Top countries
    st.subheader("Top 15 Countries")
    countries = (cleaned[cleaned["country"] != "Unknown"]["country"]
                 .str.split(",").explode().str.strip()
                 .value_counts().head(15).reset_index())
    countries.columns = ["Country","Count"]
    fig2 = px.bar(countries, x="Count", y="Country", orientation="h",
                  color="Count", color_continuous_scale="Teal",
                  title="Top 15 Countries by Title Count")
    fig2.update_layout(yaxis={"categoryorder":"total ascending"}, coloraxis_showscale=False)
    st.plotly_chart(fig2, use_container_width=True)

    # Top genres
    st.subheader("Top 15 Genres")
    genres = (cleaned["listed_in"].str.split(",").explode().str.strip()
              .value_counts().head(15).reset_index())
    genres.columns = ["Genre","Count"]
    fig3 = px.bar(genres, x="Count", y="Genre", orientation="h",
                  color="Count", color_continuous_scale="Purples",
                  title="Top 15 Genres")
    fig3.update_layout(yaxis={"categoryorder":"total ascending"}, coloraxis_showscale=False)
    st.plotly_chart(fig3, use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 5 — Cleaning Log
# ═════════════════════════════════════════════════════════════════════════════
elif page == "📋 Cleaning Log":
    st.title("📋 Cleaning Audit Log")
    st.caption("Every transformation recorded with full traceability")

    # Filter controls
    col1, col2 = st.columns(2)
    issue_filter  = col1.multiselect("Filter by Issue Type",
                                      options=audit_log["issue_type"].unique(),
                                      default=list(audit_log["issue_type"].unique()))
    column_filter = col2.multiselect("Filter by Column",
                                      options=audit_log["column_name"].unique(),
                                      default=list(audit_log["column_name"].unique()))

    filtered = audit_log[audit_log["issue_type"].isin(issue_filter) &
                          audit_log["column_name"].isin(column_filter)]

    st.dataframe(filtered[["timestamp","column_name","issue_type",
                             "rows_affected","action_taken","details"]],
                 use_container_width=True, hide_index=True)

    st.metric("Total rows affected", f"{filtered['rows_affected'].sum():,}")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE 6 — Export
# ═════════════════════════════════════════════════════════════════════════════
elif page == "💾 Export":
    st.title("💾 Export Cleaned Data")

    st.dataframe(cleaned.head(50), use_container_width=True)
    st.caption(f"Showing first 50 of {len(cleaned):,} rows")

    csv = cleaned.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download Full Cleaned CSV",
        data=csv,
        file_name="netflix_cleaned.csv",
        mime="text/csv"
    )
    st.success(f"✅ {len(cleaned):,} clean rows ready for download")
