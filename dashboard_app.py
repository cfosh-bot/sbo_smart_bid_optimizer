"""
sbo/dashboard_app.py

Standalone POC dashboard for the MP CTV pacing history CSV.
Run with: streamlit run sbo/dashboard_app.py --server.port 8502 --server.address 0.0.0.0
"""

import pandas as pd
import streamlit as st

st.set_page_config(page_title="MP CTV Pacing Dashboard", layout="wide")

HISTORY_PATH = "dashboards/mp_ctv_pacing_history.csv.gz"


@st.cache_data(ttl=600)
def load_data():
    df = pd.read_csv(HISTORY_PATH, compression="gzip", dtype=str)
    df["Pacing_Pct"] = pd.to_numeric(df["Pacing_Pct"], errors="coerce")
    df["CPM_Bid"] = pd.to_numeric(df["CPM_Bid"], errors="coerce")
    df["Effective_Bid_Current"] = pd.to_numeric(df["Effective_Bid_Current"], errors="coerce")
    df["Effective_Bid_New"] = pd.to_numeric(df["Effective_Bid_New"], errors="coerce")
    df["Reason_Code"] = df["Decision_Reason"].str.extract(r"^([A-Z_]+)")
    return df


st.title("MP CTV Pacing Dashboard")
st.caption("Proof of concept -- reading directly from mp_ctv_pacing_history.csv.gz")

with st.spinner("Loading history file..."):
    df = load_data()

st.success(f"Loaded {len(df):,} rows across {df['Run_Date'].nunique()} days.")

dates = sorted(df["Run_Date"].unique())
col1, col2, col3 = st.columns(3)
with col1:
    date_range = st.select_slider(
        "Date range", options=dates, value=(dates[0], dates[-1])
    )
with col2:
    categories = sorted(df["Category"].dropna().unique())
    selected_cats = st.multiselect("Category filter (empty = all)", categories)
with col3:
    publishers = sorted(df["Publisher"].dropna().unique())
    selected_pubs = st.multiselect("Publisher filter (empty = all)", publishers)

mask = (df["Run_Date"] >= date_range[0]) & (df["Run_Date"] <= date_range[1])
if selected_cats:
    mask &= df["Category"].isin(selected_cats)
if selected_pubs:
    mask &= df["Publisher"].isin(selected_pubs)
filtered = df[mask]

st.metric("Rows in view", f"{len(filtered):,}")

daily = filtered.groupby("Run_Date").agg(
    rows=("Reason_Code", "size"),
    avg_pacing_pct=("Pacing_Pct", "mean"),
).reset_index()

st.subheader("Row volume by day")
st.bar_chart(daily.set_index("Run_Date")["rows"])

st.subheader("Average pacing % by day")
st.line_chart(daily.set_index("Run_Date")["avg_pacing_pct"])

st.subheader("Decision reason breakdown by day")
reason_daily = (
    filtered.groupby(["Run_Date", "Reason_Code"]).size().unstack(fill_value=0)
)
top_reasons = filtered["Reason_Code"].value_counts().head(8).index
st.bar_chart(reason_daily[top_reasons])

st.subheader("Sample rows")
st.dataframe(filtered.head(200), use_container_width=True)
