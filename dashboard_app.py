"""
sbo/dashboard_app.py

MP CTV pacing dashboard -- DuckDB-backed version.

Instead of loading the full history CSV into pandas (which OOM-killed the
first version on this droplet's 961MB RAM), every view here is computed by
a DuckDB SQL query that scans the file on disk and only pulls the small
aggregated result into memory. Row-level "sample rows" are LIMIT-ed at the
SQL layer, never loaded in full.

Run with: streamlit run sbo/dashboard_app.py --server.port 8502 --server.address 0.0.0.0
"""

import duckdb
import streamlit as st

st.set_page_config(page_title="MP CTV Pacing Dashboard", layout="wide")

HISTORY_PATH = "dashboards/mp_ctv_pacing_history.csv.gz"


@st.cache_resource
def get_connection():
    return duckdb.connect()


con = get_connection()


def q(sql: str, params=None):
    """Run a SQL query against the history file, return a DataFrame."""
    return con.execute(sql, params or []).fetchdf()


@st.cache_data(ttl=600)
def load_summary():
    row = con.execute(
        f"""SELECT COUNT(*), MIN(Run_Date), MAX(Run_Date), COUNT(DISTINCT Run_Date)
            FROM read_csv_auto('{HISTORY_PATH}')"""
    ).fetchone()
    return row


@st.cache_data(ttl=600)
def load_filter_options():
    dates = con.execute(
        f"SELECT DISTINCT Run_Date FROM read_csv_auto('{HISTORY_PATH}') ORDER BY Run_Date"
    ).fetchdf()["Run_Date"].tolist()
    categories = con.execute(
        f"""SELECT DISTINCT Category FROM read_csv_auto('{HISTORY_PATH}')
            WHERE Category IS NOT NULL ORDER BY Category"""
    ).fetchdf()["Category"].tolist()
    publishers = con.execute(
        f"""SELECT DISTINCT Publisher FROM read_csv_auto('{HISTORY_PATH}')
            WHERE Publisher IS NOT NULL ORDER BY Publisher"""
    ).fetchdf()["Publisher"].tolist()
    return dates, categories, publishers


def build_filter_sql(start_date, end_date, cats, pubs):
    """Returns (where_clause, params) for the shared filter set."""
    clauses = ["Run_Date BETWEEN ? AND ?"]
    params = [start_date, end_date]
    if cats:
        placeholders = ",".join(["?"] * len(cats))
        clauses.append(f"Category IN ({placeholders})")
        params.extend(cats)
    if pubs:
        placeholders = ",".join(["?"] * len(pubs))
        clauses.append(f"Publisher IN ({placeholders})")
        params.extend(pubs)
    return " AND ".join(clauses), params


st.title("MP CTV Pacing Dashboard")
st.caption("DuckDB-backed -- queries the history file directly, never loads it all into memory")

with st.spinner("Reading file summary..."):
    total_rows, min_date, max_date, n_days = load_summary()

st.success(f"{total_rows:,} rows across {n_days} days ({min_date} to {max_date})")

dates, categories, publishers = load_filter_options()

col1, col2, col3 = st.columns(3)
with col1:
    date_range = st.select_slider("Date range", options=dates, value=(dates[0], dates[-1]))
with col2:
    selected_cats = st.multiselect("Category filter (empty = all)", categories)
with col3:
    selected_pubs = st.multiselect("Publisher filter (empty = all)", publishers)

where_sql, params = build_filter_sql(date_range[0], date_range[1], selected_cats, selected_pubs)

row_count = q(
    f"SELECT COUNT(*) AS n FROM read_csv_auto('{HISTORY_PATH}') WHERE {where_sql}", params
)["n"].iloc[0]
st.metric("Rows in view", f"{row_count:,}")

st.subheader("Row volume by day")
daily = q(
    f"""SELECT Run_Date, COUNT(*) AS rows,
               AVG(TRY_CAST(Pacing_Pct AS DOUBLE)) AS avg_pacing_pct
        FROM read_csv_auto('{HISTORY_PATH}')
        WHERE {where_sql}
        GROUP BY Run_Date ORDER BY Run_Date""",
    params,
)
st.bar_chart(daily.set_index("Run_Date")["rows"])

st.subheader("Average pacing % by day")
st.line_chart(daily.set_index("Run_Date")["avg_pacing_pct"])

st.subheader("Decision reason breakdown by day (top 8)")
reason_daily = q(
    f"""SELECT Run_Date, regexp_extract(Decision_Reason, '^([A-Z_]+)', 1) AS Reason_Code,
               COUNT(*) AS n
        FROM read_csv_auto('{HISTORY_PATH}')
        WHERE {where_sql}
        GROUP BY Run_Date, Reason_Code
        ORDER BY Run_Date""",
    params,
)
top_reasons = (
    reason_daily.groupby("Reason_Code")["n"].sum().sort_values(ascending=False).head(8).index
)
pivot = reason_daily[reason_daily["Reason_Code"].isin(top_reasons)].pivot(
    index="Run_Date", columns="Reason_Code", values="n"
).fillna(0)
st.bar_chart(pivot)

st.subheader("Sample rows (first 200 matching filters)")
sample = q(
    f"SELECT * FROM read_csv_auto('{HISTORY_PATH}') WHERE {where_sql} LIMIT 200", params
)
st.dataframe(sample, width="stretch")
