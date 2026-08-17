"""
sbo/dashboard_app.py

MP CTV pacing dashboard -- DuckDB-backed version.

Instead of loading the full history CSV into pandas (which OOM-killed the
first version on this droplet's 961MB RAM), every view here is computed by
a DuckDB SQL query. The CSV is registered as a named view at startup with
explicit column types -- this avoids auto-detection issues that caused the
NoneType crash (DuckDB was inferring Run_Date as DATE, Streamlit returned
datetime.date objects from the slider, and the VARCHAR comparison silently
failed). All subsequent queries use FROM history, not read_csv_auto().

Run with: streamlit run dashboard_app.py --server.port 8502 --server.address 0.0.0.0
"""

import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="MP CTV Pacing Dashboard", layout="wide")

HISTORY_PATH = "dashboards/mp_ctv_pacing_history.csv.gz"


@st.cache_resource
def get_connection():
    """Connect to DuckDB and register the history CSV as a persistent view.
    Pinning column types at startup avoids repeated auto-detection and keeps
    Run_Date as VARCHAR so string date parameters always compare correctly."""
    con = duckdb.connect()
    con.execute(f"""
        CREATE OR REPLACE VIEW history AS
        SELECT * FROM read_csv(
            '{HISTORY_PATH}',
            compression = 'gzip',
            header = true,
            columns = {{
                'Run_Date':              'VARCHAR',
                'SF_Line_Item_ID':       'VARCHAR',
                'BW_Line_Item_ID':       'VARCHAR',
                'Publisher':             'VARCHAR',
                'Deal_ID':               'VARCHAR',
                'CPM_Bid':               'DOUBLE',
                'Floor_Price':           'DOUBLE',
                'Category':              'VARCHAR',
                'Pacing_Pct':            'DOUBLE',
                'Effective_Bid_Current': 'DOUBLE',
                'Effective_Bid_New':     'DOUBLE',
                'Decision_Reason':       'VARCHAR'
            }}
        )
    """)
    return con


con = get_connection()


def q(sql: str, params=None):
    """Execute SQL against the registered history view; return a DataFrame.
    Returns an empty DataFrame (never None) on failure so callers can safely
    check .empty without an additional None guard."""
    try:
        result = con.execute(sql, params or []).fetchdf()
        return result if result is not None else pd.DataFrame()
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=600)
def load_summary():
    row = con.execute(
        "SELECT COUNT(*), MIN(Run_Date), MAX(Run_Date), COUNT(DISTINCT Run_Date) FROM history"
    ).fetchone()
    return row


@st.cache_data(ttl=600)
def load_filter_options():
    dates = con.execute(
        "SELECT DISTINCT Run_Date FROM history ORDER BY Run_Date"
    ).fetchdf()["Run_Date"].tolist()
    categories = con.execute(
        "SELECT DISTINCT Category FROM history WHERE Category IS NOT NULL ORDER BY Category"
    ).fetchdf()["Category"].tolist()
    publishers = con.execute(
        "SELECT DISTINCT Publisher FROM history WHERE Publisher IS NOT NULL ORDER BY Publisher"
    ).fetchdf()["Publisher"].tolist()
    return dates, categories, publishers


def build_filter_sql(start_date, end_date, cats, pubs):
    """Returns (where_clause, params) for the shared filter set.
    str() on the date values ensures VARCHAR comparison even when Streamlit
    returns datetime.date objects from select_slider."""
    clauses = ["Run_Date BETWEEN ? AND ?"]
    params = [str(start_date), str(end_date)]
    if cats:
        placeholders = ",".join(["?"] * len(cats))
        clauses.append(f"Category IN ({placeholders})")
        params.extend(cats)
    if pubs:
        placeholders = ",".join(["?"] * len(pubs))
        clauses.append(f"Publisher IN ({placeholders})")
        params.extend(pubs)
    return " AND ".join(clauses), params


# ── Page header ──────────────────────────────────────────────────────────────

st.title("MP CTV Pacing Dashboard")
st.caption("DuckDB-backed -- queries the history file directly, never loads it all into memory")

with st.spinner("Reading file summary..."):
    total_rows, min_date, max_date, n_days = load_summary()

st.success(f"{total_rows:,} rows across {n_days} days  ({min_date} to {max_date})")

dates, categories, publishers = load_filter_options()

# ── Filters ──────────────────────────────────────────────────────────────────

col1, col2, col3 = st.columns(3)
with col1:
    date_range = st.select_slider(
        "Date range", options=dates, value=(dates[0], dates[-1])
    )
with col2:
    selected_cats = st.multiselect("Category filter (empty = all)", categories)
with col3:
    selected_pubs = st.multiselect("Publisher filter (empty = all)", publishers)

where_sql, params = build_filter_sql(
    date_range[0], date_range[1], selected_cats, selected_pubs
)

# ── Row count metric ─────────────────────────────────────────────────────────

count_df = q(f"SELECT COUNT(*) AS n FROM history WHERE {where_sql}", params)
row_count = int(count_df["n"].iloc[0]) if not count_df.empty else 0
st.metric("Rows in view", f"{row_count:,}")

# ── Volume + pacing charts ───────────────────────────────────────────────────

st.subheader("Row volume by day")
daily = q(
    f"""SELECT Run_Date,
               COUNT(*) AS rows,
               AVG(Pacing_Pct) AS avg_pacing_pct
        FROM history
        WHERE {where_sql}
        GROUP BY Run_Date
        ORDER BY Run_Date""",
    params,
)
if not daily.empty:
    st.bar_chart(daily.set_index("Run_Date")["rows"])

st.subheader("Average pacing % by day")
if not daily.empty:
    st.line_chart(daily.set_index("Run_Date")["avg_pacing_pct"])

# ── Decision reason breakdown ─────────────────────────────────────────────────

st.subheader("Decision reason breakdown by day (top 8)")
reason_daily = q(
    f"""SELECT Run_Date,
               regexp_extract(Decision_Reason, '^([A-Z_]+)', 1) AS Reason_Code,
               COUNT(*) AS n
        FROM history
        WHERE {where_sql}
        GROUP BY Run_Date, Reason_Code
        ORDER BY Run_Date""",
    params,
)
if not reason_daily.empty:
    top_reasons = (
        reason_daily.groupby("Reason_Code")["n"]
        .sum()
        .sort_values(ascending=False)
        .head(8)
        .index
    )
    pivot = (
        reason_daily[reason_daily["Reason_Code"].isin(top_reasons)]
        .pivot(index="Run_Date", columns="Reason_Code", values="n")
        .fillna(0)
    )
    st.bar_chart(pivot)

# ── Sample rows ───────────────────────────────────────────────────────────────

st.subheader("Sample rows (first 200 matching filters)")
sample = q(f"SELECT * FROM history WHERE {where_sql} LIMIT 200", params)
if not sample.empty:
    st.dataframe(sample, width="stretch")
