"""
sbo/dashboard_app.py

MP CTV delivery dashboard -- DuckDB-backed, Altair small-multiples.

Everything is computed by DuckDB SQL against the history CSV registered as a
named view (never loaded fully into pandas -- this is what fixed the original
961MB-RAM droplet OOM crash). Altair (bundled with Streamlit, no extra
dependency) draws the per-entity daily charts so kill days can be marked and
hovered with the actual decision-reason text.

Performance notes:
  - Every stacked chart section fetches ALL of its *currently shown page's*
    entities' daily data in ONE batched query (grouped by entity + day), not
    one query per entity or per full entity set.
  - Rendering is paginated at 100 entities per page ("Load N more" button).
    An earlier version rendered every matching entity unconditionally --
    fine for a few dozen rows, but a single line item can carry hundreds of
    deals, and rendering hundreds of Altair charts in one page load is what
    made "click into a line item" hang.

Four views, chosen at the top:
  - Line Item View : one daily chart per line item (with that day's pacing %
                      labeled above the plotted points), click -> its deals
                      broken out by day (same pacing labels)
  - Publisher View : one daily chart per publisher, click -> the line items
                      delivering through it (still aggregate, not deal-level
                      -- too granular one level too soon), click one of those
                      -> deals for that (publisher, line item) pair
  - Deal View      : one daily chart per deal (aggregated across whatever line
                      items carry it), click -> that deal broken out by line item
  - Total          : whole-filtered-set daily trend + by-deal/by-line/
                      by-publisher totals tables

Two chart styles:
  - "aggregate" rows (line item / publisher / deal-view-top-level / total,
    all of which can span more than one (line item, deal) pair) hover-show
    that day's % of deals killed instead of a single reason.
  - "single-instance" rows (one specific (line item, deal) pair -- reached by
    drilling into one line item, one publisher's line item, or isolating one
    deal) mark killed days with a red X and hover-show the actual
    Decision_Reason text. The bid line is never hidden on a killed day --
    killed just means the bid was suppressed toward floor, not that there's
    no bid to show. Floor price is always shown (small grey text) wherever a
    deal is represented -- deal-level rows should never hide it.

Every view ends with a decision-reason-by-day stacked bar and a reason-code
glossary (definitions sourced from sbo/multiplier_engine_mp_ctv.py's own
comments and decision-reason text).

Today's Run_Date is always excluded -- the day is still accruing.

Run with: streamlit run dashboard_app.py --server.port 8502 --server.address 0.0.0.0
"""

import re
from datetime import date

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="MP CTV Delivery Dashboard", layout="wide")

HISTORY_PATH = "dashboards/mp_ctv_pacing_history.csv.gz"

KILL_REASON_CODES = [
    "PRICE_KILL",
    "PRICE_KILL_HOLD",
    "PRICE_KILL_HOLD_FALLBACK",
    "CAP_KILL",
    "CAT_CAP_KILL",
    "BACKFILL_ESTIMATED_KILL",
]
REASON_CODE_EXPR = "regexp_extract(Decision_Reason, '^([A-Z_]+)', 1)"
# Inlined as SQL literals, not bound `?` params -- KILL_REASON_CODES is a fixed,
# developer-controlled list (no injection risk). Binding it as `?` params and
# reusing that fragment more than once per query silently mis-binds in DuckDB
# (confirmed empirically while building the first version of this dashboard).
_KILL_LIST_SQL = ",".join(f"'{c}'" for c in KILL_REASON_CODES)
IS_KILLED_EXPR = f"({REASON_CODE_EXPR} IN ({_KILL_LIST_SQL}))"

# Sourced from sbo/multiplier_engine_mp_ctv.py's priority-tier comments and the
# actual Decision_Reason text seen in the data -- not guessed. Worth a sanity
# check against the pipeline if any of these look off.
REASON_CODE_GLOSSARY = {
    "BACKFILL_ESTIMATED_HOLD": "Not a real engine decision -- reconstructed after a day the pipeline never ran. Bid held at whatever was in effect that day.",
    "BACKFILL_ESTIMATED_KILL": "Not a real engine decision -- reconstructed after a day the pipeline never ran. Estimated killed because bid < floor x 0.95.",
    "PACE_HOLD_ONTARGET": "Pacing on target (95-105%) -- no bid adjustment",
    "PACE_UP_AGG": "Behind pace -- aggressive bid increase",
    "PACE_UP_MOD": "Behind pace -- moderate bid increase",
    "PACE_UP_CRITICAL": "Significantly behind pace -- maximum bid increase",
    "PACE_DOWN_AGG": "Ahead of pace -- aggressive bid decrease",
    "PACE_DOWN_MOD": "Ahead of pace -- moderate bid decrease",
    "PRICE_KILL": "Clearing price triggered a price-based kill -- bid suppressed toward floor",
    "PRICE_KILL_HOLD": "Remains price-killed from a prior day",
    "PRICE_KILL_HOLD_FALLBACK": "Price-kill hold, using fallback logic",
    "PRICE_UNKILL": "Reinstated from a price-kill -- clearing price recovered",
    "CAP_KILL": "Publisher share hard-cap kill",
    "CAP_THROTTLE": "Publisher share throttle (soft reduction, not a full kill)",
    "CAP_THROTTLE_HOLD": "Publisher share throttle held from a prior day",
    "CAT_CAP_KILL": "Category share hard-cap kill (537 lines only)",
    "CAT_CAP_THROTTLE": "Category share throttle (537 lines only)",
    "CAT_CAP_THROTTLE_HOLD": "Category share throttle held from a prior day",
    "FIRST_RUN": "First evaluation for this line/deal -- no history yet",
    "FIRST_RUN_SHORT": "First run, shortened evaluation window",
    "LINE_PAUSED": "Line item is paused -- no bid changes",
    "LINE_PAUSED_HOLDING": "Line paused, holding last bid",
    "LINE_RESUMED": "Line item resumed from a pause",
    "NO_CPM_BID": "No CPM bid configured for this line",
    "NO_PACING": "No pacing data available to calculate against",
    "PRE_FLIGHT_HOLD": "Flight hasn't started yet -- holding",
}


# ── connection ────────────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    con = duckdb.connect()
    con.execute(f"""
        CREATE OR REPLACE VIEW history AS
        SELECT * FROM read_csv(
            '{HISTORY_PATH}',
            compression = 'gzip',
            header = true,
            columns = {{
                'Run_Date':                 'VARCHAR',
                'SF_Line_Item_ID':          'VARCHAR',
                'BW_Line_Item_ID':          'VARCHAR',
                'Line_Item_Name':           'VARCHAR',
                'Publisher':                'VARCHAR',
                'Deal_ID':                  'VARCHAR',
                'CPM_Bid':                  'DOUBLE',
                'Floor_Price':              'DOUBLE',
                'Category':                 'VARCHAR',
                'Pacing_Pct':               'DOUBLE',
                'Effective_Bid_Current':    'DOUBLE',
                'Effective_Bid_New':        'DOUBLE',
                'Decision_Reason':          'VARCHAR',
                'Deal_Impressions_1Day':    'DOUBLE',
                'Deal_Spend_1Day_USD':      'DOUBLE',
                'Targets_537':              'VARCHAR',
                'Included_Deal_Lists':      'VARCHAR'
            }}
        )
    """)
    return con


con = get_connection()


@st.cache_data(ttl=600, show_spinner=False)
def q(sql: str, params=None) -> pd.DataFrame:
    """Every entity list, chart batch, and breakout table funnels through
    here, so caching at this single choke point is what makes flipping back
    to a view you already opened (same filters, same drill-down) instant
    instead of re-hitting DuckDB -- at the cost of results going stale for
    up to 10 minutes after the daily pipeline appends a new row."""
    try:
        result = con.execute(sql, params or []).fetchdf()
        return result if result is not None else pd.DataFrame()
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()


def safe_label(parts, fallback) -> str:
    """Join non-null, non-'None' parts with ' · '; fall back to a raw ID when
    nothing usable is available (e.g. Line_Item_Name not yet captured for any
    row before this dashboard's schema change)."""
    clean = [str(p) for p in parts if pd.notna(p) and str(p).strip() not in ("", "None", "nan")]
    return " · ".join(clean) if clean else str(fallback)


def safe_int(v) -> int:
    """int(v or 0) crashes on NaN -- NaN is truthy in Python, so `NaN or 0`
    evaluates to NaN, not 0. Confirmed as a real crash: any candidate deal
    with zero actual delivery (a real, common case -- Option A keeps them
    visible rather than dropping them) has NULL Impressions, which reached
    `int(nan)` and took the whole app down once no min-impressions filter
    was screening them out."""
    return int(v) if pd.notna(v) else 0


def safe_num(v) -> float:
    return float(v) if pd.notna(v) else 0.0


# ── filter option loaders ────────────────────────────────────────────────

@st.cache_data(ttl=600)
def load_filter_options(today_str: str):
    dates = q("SELECT DISTINCT Run_Date FROM history WHERE Run_Date < ? ORDER BY Run_Date",
              [today_str])["Run_Date"].tolist()
    categories = q("SELECT DISTINCT Category FROM history WHERE Category IS NOT NULL ORDER BY Category")["Category"].tolist()
    publishers = q("SELECT DISTINCT Publisher FROM history WHERE Publisher IS NOT NULL ORDER BY Publisher")["Publisher"].tolist()
    bw_ids = q("SELECT DISTINCT BW_Line_Item_ID FROM history WHERE BW_Line_Item_ID IS NOT NULL ORDER BY BW_Line_Item_ID")["BW_Line_Item_ID"].tolist()
    sf_ids = q("SELECT DISTINCT SF_Line_Item_ID FROM history WHERE SF_Line_Item_ID IS NOT NULL ORDER BY SF_Line_Item_ID")["SF_Line_Item_ID"].tolist()
    li_names = q("SELECT DISTINCT Line_Item_Name FROM history WHERE Line_Item_Name IS NOT NULL ORDER BY Line_Item_Name")["Line_Item_Name"].tolist()
    deal_ids = q("SELECT DISTINCT Deal_ID FROM history WHERE Deal_ID IS NOT NULL ORDER BY Deal_ID")["Deal_ID"].tolist()
    targets_537 = q("SELECT DISTINCT Targets_537 FROM history WHERE Targets_537 IS NOT NULL ORDER BY Targets_537")["Targets_537"].tolist()
    reason_codes = q(f"""
        SELECT DISTINCT {REASON_CODE_EXPR} AS Reason_Code FROM history
        WHERE Decision_Reason IS NOT NULL ORDER BY Reason_Code
    """)["Reason_Code"].tolist()
    return dates, categories, publishers, bw_ids, sf_ids, li_names, deal_ids, targets_537, reason_codes


def picker(label: str, options: list[str], key: str):
    """One widget: search-and-click like a normal multiselect, OR paste a
    comma/newline-separated list and press Enter -- either way you get a set
    of selected values. `accept_new_options` lets the box accept typed/pasted
    text that isn't in the pre-loaded option list; we then split any pasted
    entry on commas/newlines so one paste becomes many selections instead of
    one giant literal tag."""
    raw = st.multiselect(
        label, options, key=f"{key}_multi", accept_new_options=True,
        placeholder="Search & select, or paste comma/newline-separated IDs",
    )
    values = set()
    for item in raw:
        for part in re.split(r"[,\n]+", str(item)):
            part = part.strip()
            if part:
                values.add(part)
    return sorted(values)


# ── shared WHERE builder ─────────────────────────────────────────────────

FLOOR_OPERATORS = {">": ">", "<": "<", "=": "="}


def build_filter_sql(today_str, start_date, end_date, cats, pubs, bw_ids, sf_ids, li_names, deal_ids,
                      targets_537=None, reason_codes=None, floor_op=None, floor_val=None):
    clauses = ["Run_Date BETWEEN ? AND ?", "Run_Date < ?"]
    params = [str(start_date), str(end_date), today_str]

    def _in(col, values):
        if values:
            clauses.append(f"{col} IN ({','.join(['?'] * len(values))})")
            params.extend(values)

    _in("Category", cats)
    _in("Publisher", pubs)
    _in("BW_Line_Item_ID", bw_ids)
    _in("SF_Line_Item_ID", sf_ids)
    _in("Line_Item_Name", li_names)
    _in("Deal_ID", deal_ids)
    _in("Targets_537", targets_537)
    if reason_codes:
        clauses.append(f"{REASON_CODE_EXPR} IN ({','.join(['?'] * len(reason_codes))})")
        params.extend(reason_codes)
    if floor_op and floor_val is not None:
        if floor_op not in FLOOR_OPERATORS:
            raise ValueError(f"Unsupported floor price operator: {floor_op!r}")
        clauses.append(f"Floor_Price {FLOOR_OPERATORS[floor_op]} ?")
        params.append(floor_val)
    return " AND ".join(clauses), params


# ── aggregate metrics (multi-instance rows: LI / publisher / deal-view-top) ──

AGG_SELECT = f"""
    SUM(Deal_Impressions_1Day) AS Impressions,
    SUM(Deal_Spend_1Day_USD) AS Spend,
    CASE WHEN SUM(Deal_Impressions_1Day) > 0
         THEN SUM(Deal_Spend_1Day_USD) / SUM(Deal_Impressions_1Day) * 1000
         ELSE NULL END AS Actual_Clearing_CPM,
    AVG(Effective_Bid_Current) FILTER (
        WHERE Decision_Reason IS NOT NULL AND NOT {IS_KILLED_EXPR}
    ) AS Avg_Bid,
    100.0 * COUNT(*) FILTER (WHERE Decision_Reason IS NOT NULL AND {IS_KILLED_EXPR})
        / NULLIF(COUNT(*) FILTER (WHERE Decision_Reason IS NOT NULL), 0) AS Pct_Killed
"""


IMPRESSIONS_OPERATORS = [">=", ">", "<", "="]


def entity_summary(group_cols: list[str], where_sql: str, params: list, min_impressions: float,
                    extra_select: str = "", order_by: str = "Impressions DESC NULLS LAST",
                    impressions_op: str = ">="):
    """One row per entity (LI / publisher / deal), aggregated over the whole
    filtered date range -- feeds both the entity list and the breakout tables."""
    if impressions_op not in IMPRESSIONS_OPERATORS:
        raise ValueError(f"Unsupported impressions operator: {impressions_op!r}")
    group_sql = ", ".join(group_cols)
    # group_cols[0] is always the true identity column at every call site
    # (BW_Line_Item_ID / Publisher / Deal_ID). Excluding NULL there matters
    # beyond just tidiness: a NULL id later flows into an IN(...) parameter
    # list as a Python NaN (not the string "nan"), which makes DuckDB infer
    # the whole list as DOUBLE and try to cast the id column to a number --
    # confirmed by reproducing it with Publisher's real NULL rows.
    sql = f"""
        SELECT {group_sql}, {extra_select} {AGG_SELECT}
        FROM history
        WHERE {where_sql} AND {group_cols[0]} IS NOT NULL
        GROUP BY {group_sql}
        HAVING COALESCE(SUM(Deal_Impressions_1Day), 0) {impressions_op} ?
        ORDER BY {order_by}
    """
    df = q(sql, params + [min_impressions])
    if not df.empty and "Impressions" in df.columns:
        total = df["Impressions"].sum()
        df["Impression_Share_Pct"] = (df["Impressions"] / total * 100).round(2) if total else None
    return df


def daily_totals(where_sql: str, params: list) -> pd.DataFrame:
    """Whole-filtered-set daily trend -- no entity grouping at all. Feeds the
    Total view's headline chart."""
    sql = f"""
        SELECT Run_Date,
            SUM(Deal_Impressions_1Day) AS Impressions,
            CASE WHEN SUM(Deal_Impressions_1Day) > 0
                 THEN SUM(Deal_Spend_1Day_USD) / SUM(Deal_Impressions_1Day) * 1000
                 ELSE NULL END AS Actual_Clearing_CPM,
            AVG(Effective_Bid_Current) FILTER (
                WHERE Decision_Reason IS NOT NULL AND NOT {IS_KILLED_EXPR}
            ) AS Avg_Bid,
            100.0 * COUNT(*) FILTER (WHERE Decision_Reason IS NOT NULL AND {IS_KILLED_EXPR})
                / NULLIF(COUNT(*) FILTER (WHERE Decision_Reason IS NOT NULL), 0) AS Pct_Killed
        FROM history
        WHERE {where_sql}
        GROUP BY Run_Date
        ORDER BY Run_Date
    """
    return q(sql, params)


def daily_aggregate_batch(id_col: str, where_sql: str, params: list, entity_ids: list[str]) -> pd.DataFrame:
    """ALL entities' daily data in one query, grouped by (entity, day) --
    replaces a one-query-per-entity loop."""
    if not entity_ids:
        return pd.DataFrame()
    placeholders = ",".join(["?"] * len(entity_ids))
    sql = f"""
        SELECT {id_col} AS Entity_ID, Run_Date,
            SUM(Deal_Impressions_1Day) AS Impressions,
            CASE WHEN SUM(Deal_Impressions_1Day) > 0
                 THEN SUM(Deal_Spend_1Day_USD) / SUM(Deal_Impressions_1Day) * 1000
                 ELSE NULL END AS Actual_Clearing_CPM,
            AVG(Effective_Bid_Current) FILTER (
                WHERE Decision_Reason IS NOT NULL AND NOT {IS_KILLED_EXPR}
            ) AS Avg_Bid,
            AVG(Pacing_Pct) AS Pacing_Pct,
            100.0 * COUNT(*) FILTER (WHERE Decision_Reason IS NOT NULL AND {IS_KILLED_EXPR})
                / NULLIF(COUNT(*) FILTER (WHERE Decision_Reason IS NOT NULL), 0) AS Pct_Killed
        FROM history
        WHERE {where_sql} AND {id_col} IN ({placeholders})
        GROUP BY {id_col}, Run_Date
        ORDER BY {id_col}, Run_Date
    """
    df = q(sql, params + entity_ids)
    if not df.empty:
        df["Entity_ID"] = df["Entity_ID"].astype(str)
    return df


def li_true_cpm_batch(li_ids: list[str], date_only_where: str, date_only_params: list) -> dict[str, float]:
    """BW_Line_Item_ID -> that line item's own overall Actual_Clearing_CPM,
    across every publisher/deal/category -- ignoring whatever narrower
    Publisher/Deal/Category/Targets/Reason/Floor filter is currently active.
    Only the date range applies. Lets a line-item-level row show its true
    total alongside a CPM that's already scoped to one publisher or deal."""
    if not li_ids:
        return {}
    placeholders = ",".join(["?"] * len(li_ids))
    sql = f"""
        SELECT BW_Line_Item_ID,
            CASE WHEN SUM(Deal_Impressions_1Day) > 0
                 THEN SUM(Deal_Spend_1Day_USD) / SUM(Deal_Impressions_1Day) * 1000
                 ELSE NULL END AS Actual_Clearing_CPM
        FROM history
        WHERE {date_only_where} AND BW_Line_Item_ID IN ({placeholders})
        GROUP BY BW_Line_Item_ID
    """
    df = q(sql, date_only_params + li_ids)
    if df.empty:
        return {}
    return {
        str(r["BW_Line_Item_ID"]): r["Actual_Clearing_CPM"]
        for _, r in df.iterrows() if pd.notna(r["Actual_Clearing_CPM"])
    }


def single_instance_batch(where_sql: str, params: list, li_deal_pairs: list[tuple]) -> pd.DataFrame:
    """ALL (line item, deal) pairs in one query. The dedupe key (Run_Date,
    BW_Line_Item_ID, Deal_ID) guarantees at most one row per pair per day, so
    no aggregation is needed -- just a wide OR filter."""
    if not li_deal_pairs:
        return pd.DataFrame()
    clauses, p = [], []
    for li, deal in li_deal_pairs:
        clauses.append("(BW_Line_Item_ID = ? AND Deal_ID = ?)")
        p.extend([li, deal])
    sql = f"""
        SELECT BW_Line_Item_ID, Deal_ID, Run_Date, Deal_Impressions_1Day AS Impressions,
            Effective_Bid_Current AS Avg_Bid,
            CASE WHEN Deal_Impressions_1Day > 0
                 THEN Deal_Spend_1Day_USD / Deal_Impressions_1Day * 1000
                 ELSE NULL END AS Actual_Clearing_CPM,
            Decision_Reason, Pacing_Pct,
            {IS_KILLED_EXPR} AS Is_Killed
        FROM history
        WHERE {where_sql} AND ({" OR ".join(clauses)})
        ORDER BY BW_Line_Item_ID, Deal_ID, Run_Date
    """
    df = q(sql, params + p)
    if not df.empty:
        df["BW_Line_Item_ID"] = df["BW_Line_Item_ID"].astype(str)
        df["Deal_ID"] = df["Deal_ID"].astype(str)
    return df


# ── chart builders (Altair) ──────────────────────────────────────────────

def _date_axis():
    return alt.Axis(labelAngle=-45)


def _y_plan(daily_df: pd.DataFrame, value_cols: list[str], headroom: bool):
    """Explicit $5-interval gridlines with a pinned domain.

    Two bugs had to be fixed to get here: (1) tickMinStep only sets a *floor*
    on tick spacing -- Vega-Lite's own 'nice number' rounding still picked 20
    or 40 -- so tick values must be listed explicitly. (2) when a pacing-label
    layer (whose Y values run ~18% above the line data, to sit above the
    points) is combined with the line layer via `+`, Vega-Lite recomputes a
    *shared* domain across both layers and silently drops the explicit tick
    values that no longer span it. Pinning an identical `scale(domain=...)`
    on every layer -- lines, kill marks, and labels alike -- stops that
    recomputation from ever happening.
    """
    present = [c for c in value_cols if c in daily_df.columns]
    vmax = daily_df[present].max(skipna=True).max() if present else None
    if pd.isna(vmax) or vmax is None or vmax <= 0:
        return alt.Axis(title="$"), alt.Scale()
    raw_top = vmax * 1.2 if headroom else vmax  # match the label layer's 1.18x headroom
    top = int((raw_top // 5 + 2) * 5)
    axis = alt.Axis(title="$", values=list(range(0, top + 1, 5)), labelOverlap=False, labelFontSize=8)
    scale = alt.Scale(domain=[0, top])
    return axis, scale


def _pacing_label_layer(daily_df: pd.DataFrame, axis: alt.Axis, scale: alt.Scale) -> alt.Chart:
    """That day's pacing %, plotted as text directly above that day's bid/CPM
    points -- not a single summary number, one label per day.

    Passing the SAME `axis` object as the line layer (not just the same
    `scale`) turned out to matter: with only `scale` shared, Vega-Lite still
    treats the two layers' y-axis definitions as needing to be resolved (this
    layer has none), and it silently discarded the line layer's explicit
    $5-interval tick `values` in that resolution -- confirmed by reproducing
    it in an isolated single-chart test page outside this whole dashboard."""
    label_df = daily_df.copy()
    value_cols = [c for c in ("Avg_Bid", "Actual_Clearing_CPM") if c in label_df.columns]
    label_df["Label_Y"] = label_df[value_cols].max(axis=1, skipna=True) * 1.18
    label_df["Pacing_Label"] = label_df["Pacing_Pct"].apply(lambda v: f"{v:.0%}" if pd.notna(v) else "")
    return alt.Chart(label_df).mark_text(dy=-4, fontSize=10, color="#666").encode(
        x=alt.X("Run_Date:O", axis=_date_axis()),
        y=alt.Y("Label_Y:Q", axis=axis, scale=scale),
        text="Pacing_Label:N",
    )


def chart_aggregate(daily_df: pd.DataFrame, show_pacing_labels: bool = False, height: int = 130):
    """Two-line (Avg Bid / Actual Clearing CPM) daily chart. Hover shows that
    day's % of deals killed -- appropriate whenever the row can span more
    than one (line item, deal) pair."""
    if daily_df.empty:
        return None
    melted = daily_df.melt(
        id_vars=["Run_Date", "Pct_Killed"],
        value_vars=["Avg_Bid", "Actual_Clearing_CPM"],
        var_name="Metric", value_name="Value",
    )
    axis, scale = _y_plan(daily_df, ["Avg_Bid", "Actual_Clearing_CPM"], headroom=show_pacing_labels)
    lines = alt.Chart(melted).mark_line(point=True).encode(
        x=alt.X("Run_Date:O", title=None, axis=_date_axis()),
        y=alt.Y("Value:Q", title="$", axis=axis, scale=scale),
        color=alt.Color("Metric:N", legend=alt.Legend(orient="right", title=None)),
        tooltip=["Run_Date", "Metric", alt.Tooltip("Value:Q", format="$.2f"),
                 alt.Tooltip("Pct_Killed:Q", format=".1f", title="% killed that day")],
    ).properties(height=height)
    if show_pacing_labels and "Pacing_Pct" in daily_df.columns:
        return lines + _pacing_label_layer(daily_df, axis, scale)
    return lines


def chart_single_instance(daily_df: pd.DataFrame, show_pacing_labels: bool = False, height: int = 130):
    """Two-line chart for exactly one (line item, deal) pair. Killed days get
    a red X on the bid line; hovering any point (killed or not) shows the
    actual Decision_Reason text."""
    if daily_df.empty:
        return None
    melted = daily_df.melt(
        id_vars=["Run_Date", "Decision_Reason", "Is_Killed"],
        value_vars=["Avg_Bid", "Actual_Clearing_CPM"],
        var_name="Metric", value_name="Value",
    )
    axis, scale = _y_plan(daily_df, ["Avg_Bid", "Actual_Clearing_CPM"], headroom=show_pacing_labels)
    base = alt.Chart(melted).mark_line(point=True).encode(
        x=alt.X("Run_Date:O", title=None, axis=_date_axis()),
        y=alt.Y("Value:Q", title="$", axis=axis, scale=scale),
        color=alt.Color("Metric:N", legend=alt.Legend(orient="right", title=None)),
        tooltip=["Run_Date", "Metric", alt.Tooltip("Value:Q", format="$.2f"), "Decision_Reason"],
    ).properties(height=height)

    chart = base
    kills = daily_df[daily_df["Is_Killed"] == True]  # noqa: E712
    if not kills.empty:
        kill_marks = alt.Chart(kills).mark_point(shape="cross", size=140, color="red", strokeWidth=3).encode(
            x=alt.X("Run_Date:O", title=None, axis=_date_axis()),
            y=alt.Y("Avg_Bid:Q", scale=scale),
            tooltip=["Run_Date", alt.Tooltip("Avg_Bid:Q", format="$.2f", title="Bid (killed)"), "Decision_Reason"],
        )
        chart = chart + kill_marks
    if show_pacing_labels and "Pacing_Pct" in daily_df.columns:
        chart = chart + _pacing_label_layer(daily_df, axis, scale)
    return chart


# ── row renderers (paginated: 100 charts at a time, not all at once) ────
#
# An earlier version rendered every matching entity unconditionally. That
# works fine for a few dozen rows, but a single line item can carry hundreds
# of deals, and rendering hundreds of Altair charts in one page load is what
# made "click into a line item" hang. Capped back to a page size with a
# "Load more" button -- the batched SQL query (already fast) is now also
# scoped to just the page being shown, not the full entity set.

PAGE_SIZE = 100


def _cpm_text(ent) -> str:
    return f"Actual CPM ${ent['Actual_Clearing_CPM']:.2f}" if pd.notna(ent.get("Actual_Clearing_CPM")) else "Actual CPM N/A"


def _floor_text(ent) -> str:
    return f"Floor ${ent['Floor_Price']:.2f}" if pd.notna(ent.get("Floor_Price")) else ""


def _li_total_cpm_text(ent) -> str:
    """The line item's own overall actual clearing CPM (every publisher,
    every deal) -- distinct from a CPM already scoped to one publisher or
    one deal's slice of that same line item."""
    return (
        f"LI total actual CPM ${ent['LI_True_Actual_CPM']:.2f}"
        if pd.notna(ent.get("LI_True_Actual_CPM")) else ""
    )


def render_load_more(total: int, key_prefix: str):
    page_key = f"{key_prefix}_n"
    n = st.session_state.get(page_key, PAGE_SIZE)
    if n < total:
        st.caption(f"Showing {min(n, total)} of {total}")
        if st.button(f"Load {min(PAGE_SIZE, total - n)} more", key=f"{key_prefix}_more"):
            st.session_state[page_key] = n + PAGE_SIZE
            st.rerun()
    else:
        st.caption(f"All {total} loaded.")


def reset_pagination_if_changed(signature: str):
    """Clears every page-position counter when the filters/view/drill state
    changes, so a stale 'loaded 300 of 40' from a previous, broader view
    never lingers into a new, narrower one."""
    if st.session_state.get("_pg_sig") != signature:
        for k in [k for k in st.session_state if k.endswith("_n")]:
            del st.session_state[k]
        st.session_state["_pg_sig"] = signature


def render_aggregate_stack(entities: pd.DataFrame, id_col: str, key_prefix: str,
                            where_sql: str, params: list, show_pacing_labels: bool = False,
                            share_pct_label: bool = False):
    """Stacked list of aggregate-style charts (LI / publisher / deal-view-top).
    Returns the id clicked this run, or None."""
    if entities.empty:
        st.info("No rows match the current filters.")
        return None
    # Defensive: entity_summary's grouping should already guarantee one row
    # per id_col, but a duplicate here would crash st.button on a repeated
    # key, so belt-and-suspenders it.
    entities = entities.drop_duplicates(subset=[id_col])
    page_key = f"{key_prefix}_n"
    n = st.session_state.get(page_key, PAGE_SIZE)
    shown = entities.head(n)
    shown_ids = shown[id_col].dropna().astype(str).tolist()
    batch = daily_aggregate_batch(id_col, where_sql, params, shown_ids)

    clicked = None
    for _, ent in shown.iterrows():
        eid = str(ent[id_col])
        left, right = st.columns([1, 4])
        with left:
            imps_text = f"{safe_int(ent['Impressions']):,} imps"
            cpm_text = _cpm_text(ent)
            if id_col == "BW_Line_Item_ID":
                # The line item name can be very long (real examples run past
                # 100 characters) -- it's small grey text below the short ID,
                # not the heading, so it doesn't dominate the row.
                st.markdown(f"#### {eid}")
                name = ent.get("Line_Item_Name")
                if pd.notna(name):
                    st.caption(str(name))
                sf = ent.get("SF_Line_Item_ID")
                # Within a publisher's own line-item breakdown, share of that
                # publisher's impressions is more useful than a raw count --
                # the count alone doesn't say whether this LI is 2% or 80% of
                # what the publisher delivers.
                vol_text = (
                    f"{ent['Impression_Share_Pct']:.1f}% of impressions"
                    if share_pct_label and pd.notna(ent.get("Impression_Share_Pct"))
                    else imps_text
                )
                st.caption(" · ".join(x for x in [f"SF {sf}" if pd.notna(sf) else "", vol_text, cpm_text] if x))
                li_total = _li_total_cpm_text(ent)
                if li_total:
                    st.caption(li_total)
            elif id_col == "Publisher":
                st.markdown(f"#### {eid}")
                cat = ent.get("Category")
                st.caption(" · ".join(x for x in [str(cat) if pd.notna(cat) else "", imps_text, cpm_text] if x))
            elif id_col == "Deal_ID":
                st.markdown(f"#### {eid}")
                floor = _floor_text(ent)
                if floor:
                    st.caption(floor)
                pub = ent.get("Publisher")
                st.caption(" · ".join(x for x in [str(pub) if pd.notna(pub) else "", imps_text, cpm_text] if x))
            else:
                st.markdown(f"#### {eid}")
                st.caption(imps_text)
            if st.button("View →", key=f"{key_prefix}_{eid}"):
                clicked = eid
        with right:
            daily = batch[batch["Entity_ID"] == eid] if not batch.empty else pd.DataFrame()
            chart = chart_aggregate(daily, show_pacing_labels=show_pacing_labels)
            if chart is not None:
                st.altair_chart(chart, width='stretch', theme=None)
            else:
                st.caption("No daily delivery data for this entity in range.")
    render_load_more(len(entities), key_prefix)
    return clicked


def render_single_instance_stack(entities: pd.DataFrame, label_fn, where_sql: str, params: list,
                                  key_prefix: str, show_pacing_labels: bool = False,
                                  share_pct: bool = False):
    """Stacked list of single-(line item, deal)-instance charts, e.g. the deals
    under one line item/publisher, or the line items carrying one isolated deal.

    label_fn(ent) -> (big_text, [grey_caption_lines]). Every deal-representing
    context always includes a "Floor $X" line -- floor price should be visible
    wherever a deal shows up, not just in the breakout table below."""
    if entities.empty:
        st.info("No rows match the current filters.")
        return
    entities = entities.drop_duplicates(subset=["BW_Line_Item_ID", "Deal_ID"])
    page_key = f"{key_prefix}_n"
    n = st.session_state.get(page_key, PAGE_SIZE)
    shown = entities.head(n)
    pairs = list(zip(shown["BW_Line_Item_ID"].astype(str), shown["Deal_ID"].astype(str)))
    batch = single_instance_batch(where_sql, params, pairs)

    for _, ent in shown.iterrows():
        li_id, deal_id = str(ent["BW_Line_Item_ID"]), str(ent["Deal_ID"])
        left, right = st.columns([1, 4])
        with left:
            big, greys = label_fn(ent)
            st.markdown(f"#### {big}")
            for g in greys:
                if g:
                    st.caption(g)
            if share_pct and pd.notna(ent.get("Impression_Share_Pct")):
                st.caption(f"{ent['Impression_Share_Pct']:.1f}% of impressions")
            else:
                st.caption(f"{safe_int(ent['Impressions']):,} imps")
        with right:
            daily = batch[(batch["BW_Line_Item_ID"] == li_id) & (batch["Deal_ID"] == deal_id)] if not batch.empty else pd.DataFrame()
            chart = chart_single_instance(daily, show_pacing_labels=show_pacing_labels)
            if chart is not None:
                st.altair_chart(chart, width='stretch', theme=None)
            else:
                st.caption("No daily delivery data for this pair in range.")
    render_load_more(len(entities), key_prefix)


def render_reason_footer(where_sql: str, params: list):
    """Decision-reason-by-day stacked bar + reason code glossary -- shown at
    the bottom of every view."""
    st.divider()
    st.subheader("Decision reason breakdown by day")
    reason_daily = q(f"""
        SELECT Run_Date, {REASON_CODE_EXPR} AS Reason_Code, COUNT(*) AS n
        FROM history
        WHERE {where_sql} AND Decision_Reason IS NOT NULL
        GROUP BY Run_Date, Reason_Code
        ORDER BY Run_Date
    """, params)
    if not reason_daily.empty:
        top_reasons = (
            reason_daily.groupby("Reason_Code")["n"].sum().sort_values(ascending=False).head(8).index
        )
        pivot = (
            reason_daily[reason_daily["Reason_Code"].isin(top_reasons)]
            .pivot(index="Run_Date", columns="Reason_Code", values="n")
            .fillna(0)
        )
        st.bar_chart(pivot)
    else:
        st.caption("No decisioned rows in range.")

    st.caption("Reason code library")
    glossary_df = pd.DataFrame(REASON_CODE_GLOSSARY.items(), columns=["Code", "Meaning"])
    st.dataframe(glossary_df, width='stretch', hide_index=True)


# ── page setup ────────────────────────────────────────────────────────────

st.title("MP CTV Delivery Dashboard")

today_str = date.today().isoformat()
(dates, categories, publishers, bw_ids_all, sf_ids_all, li_names_all, deal_ids_all,
 targets_537_all, reason_codes_all) = load_filter_options(today_str)

if not dates:
    st.warning("No complete (non-today) days available yet.")
    st.stop()

st.subheader("Filters")
f1, f2 = st.columns(2)
with f1:
    date_range = st.select_slider("Date range", options=dates, value=(dates[0], dates[-1]))
with f2:
    i1, i2 = st.columns([1, 2])
    with i1:
        impressions_op = st.selectbox("Impressions", IMPRESSIONS_OPERATORS, index=0, label_visibility="visible")
    with i2:
        min_impressions = st.number_input(
            "Total impressions in range (per entity)", min_value=0, value=0, step=10_000,
            help="Defaults to 0 -- pagination (100 entities at a time) and query caching keep "
                 "broad views from overloading the droplet's 961MB RAM, so this is now just a "
                 "convenience filter, not a safety limit. Raise it to cut noise from tiny entities.",
        )

f3, f4 = st.columns(2)
with f3:
    selected_cats = st.multiselect("Category", categories)
with f4:
    selected_pubs = st.multiselect("Publisher", publishers)

f5, f6 = st.columns(2)
with f5:
    selected_targets_537 = st.multiselect("Targets 537 (marketplace / 537 / none)", targets_537_all)
with f6:
    selected_reason_codes = st.multiselect("Decision reason code", reason_codes_all)

f7, f8 = st.columns(2)
with f7:
    floor_op = st.selectbox("Floor price", ["No filter", ">", "<", "="], index=0)
with f8:
    floor_val = st.number_input("Floor price value ($)", min_value=0.0, value=0.0, step=0.5,
                                 disabled=(floor_op == "No filter"))

selected_bw = picker("BW Line Item ID", bw_ids_all, "bw")
selected_sf = picker("SF Line Item ID", sf_ids_all, "sf")
selected_names = picker("Line Item Name", li_names_all, "liname")
selected_deals = picker("Deal ID", deal_ids_all, "deal")

st.caption(
    "Every matching entity renders its own chart on these pages -- no page limit. "
    "If a view feels sluggish with very broad filters, narrow with min impressions "
    "or the ID/name filters above."
)

where_sql, params = build_filter_sql(
    today_str, date_range[0], date_range[1], selected_cats, selected_pubs,
    selected_bw, selected_sf, selected_names, selected_deals,
    targets_537=selected_targets_537, reason_codes=selected_reason_codes,
    floor_op=None if floor_op == "No filter" else floor_op,
    floor_val=floor_val if floor_op != "No filter" else None,
)

# Date range only -- no Publisher/Category/Deal/Targets/Reason/Floor narrowing.
# Used to show a line item's own true total clearing CPM (all publishers,
# all deals) alongside a context that's already scoped narrower than the
# whole line item, e.g. one publisher's or one deal's slice of it.
date_only_where = "Run_Date BETWEEN ? AND ? AND Run_Date < ?"
date_only_params = [str(date_range[0]), str(date_range[1]), today_str]

view = st.radio("View", ["Line Item View", "Publisher View", "Deal View", "Total"], horizontal=True)

for key in ("drill_li", "drill_pub", "drill_pub_li", "drill_deal"):
    if key not in st.session_state:
        st.session_state[key] = None

reset_pagination_if_changed(
    where_sql + str(params) + view + str((
        st.session_state.drill_li, st.session_state.drill_pub,
        st.session_state.drill_pub_li, st.session_state.drill_deal,
    ))
)

# ── header stats ─────────────────────────────────────────────────────────
# Scoped to whatever the user is currently drilled into -- otherwise these
# numbers stay pinned to the page-wide total even while looking at one
# publisher's or one deal's slice, which reads as broken.

if view == "Line Item View" and st.session_state.drill_li:
    scope_where = where_sql + " AND BW_Line_Item_ID = ?"
    scope_params = params + [st.session_state.drill_li]
elif view == "Publisher View" and st.session_state.drill_pub_li:
    scope_where = where_sql + " AND Publisher = ? AND BW_Line_Item_ID = ?"
    scope_params = params + [st.session_state.drill_pub, st.session_state.drill_pub_li]
elif view == "Publisher View" and st.session_state.drill_pub:
    scope_where = where_sql + " AND Publisher = ?"
    scope_params = params + [st.session_state.drill_pub]
elif view == "Deal View" and st.session_state.drill_deal:
    scope_where = where_sql + " AND Deal_ID = ?"
    scope_params = params + [st.session_state.drill_deal]
else:
    scope_where, scope_params = where_sql, params

overview = q(f"SELECT COUNT(*) AS rows, {AGG_SELECT} FROM history WHERE {scope_where}", scope_params)

m1, m2, m3, m4, m5 = st.columns(5)
if not overview.empty:
    row = overview.iloc[0]
    m1.metric("Impressions", f"{safe_int(row['Impressions']):,}")
    m2.metric("Spend", f"${safe_num(row['Spend']):,.0f}")
    m3.metric("Actual clearing CPM", f"${row['Actual_Clearing_CPM']:,.2f}" if pd.notna(row["Actual_Clearing_CPM"]) else "N/A")
    m4.metric("Avg bid (excl. killed)", f"${row['Avg_Bid']:,.2f}" if pd.notna(row["Avg_Bid"]) else "N/A")
    m5.metric("% of deals killed", f"{row['Pct_Killed']:.1f}%" if pd.notna(row["Pct_Killed"]) else "N/A")

st.divider()

# ══════════════════════════════════════════════════════════════════════════
# LINE ITEM VIEW
# ══════════════════════════════════════════════════════════════════════════

if view == "Line Item View":
    if not st.session_state.drill_li:
        st.subheader("Line items")
        li_df = entity_summary(
            ["BW_Line_Item_ID"], where_sql, params, min_impressions, impressions_op=impressions_op,
            extra_select="arg_max(SF_Line_Item_ID, Run_Date) AS SF_Line_Item_ID, "
                          "arg_max(Line_Item_Name, Run_Date) AS Line_Item_Name,",
        )
        # True total is only worth showing separately when a Publisher/Category/
        # Deal/etc filter up top has already narrowed this LI's own numbers --
        # cheap to always merge in, entity_summary's own CPM already matches it
        # when no such filter is active.
        li_df["LI_True_Actual_CPM"] = li_df["BW_Line_Item_ID"].astype(str).map(
            li_true_cpm_batch(li_df["BW_Line_Item_ID"].astype(str).tolist(), date_only_where, date_only_params)
        )
        clicked = render_aggregate_stack(li_df, "BW_Line_Item_ID", "li", where_sql, params, show_pacing_labels=True)
        if clicked:
            st.session_state.drill_li = clicked
            st.rerun()
    else:
        li_id = st.session_state.drill_li
        if st.button("⬅ Back to line items"):
            st.session_state.drill_li = None
            st.rerun()
        li_where = where_sql + " AND BW_Line_Item_ID = ?"
        li_params = params + [li_id]
        name_row = q("SELECT DISTINCT Line_Item_Name FROM history WHERE BW_Line_Item_ID = ? LIMIT 1", [li_id])
        li_name = safe_label([name_row.iloc[0]["Line_Item_Name"]] if not name_row.empty else [], li_id)
        st.subheader(f"Deals for line item {li_name} ({li_id})")

        deal_df = entity_summary(
            ["Deal_ID", "Publisher", "Category", "BW_Line_Item_ID"], li_where, li_params, min_impressions, impressions_op=impressions_op,
            extra_select="arg_max(Floor_Price, Run_Date) AS Floor_Price,",
        )
        render_single_instance_stack(
            deal_df,
            lambda ent: (
                ent["Deal_ID"],
                [
                    _floor_text(ent),
                    _cpm_text(ent),
                    f"{ent['Publisher']} · {ent['Category']}",
                ],
            ),
            where_sql, params, "li_deal", show_pacing_labels=True,
        )

        st.subheader("Breakout: impressions, bid & CPM by publisher / category / deal")
        st.dataframe(
            deal_df[["Publisher", "Category", "Deal_ID", "Floor_Price", "Impressions", "Impression_Share_Pct",
                     "Avg_Bid", "Actual_Clearing_CPM", "Pct_Killed"]],
            width='stretch', hide_index=True,
        )

# ══════════════════════════════════════════════════════════════════════════
# PUBLISHER VIEW
# ══════════════════════════════════════════════════════════════════════════

elif view == "Publisher View":
    if not st.session_state.drill_pub:
        # Level 1: publishers.
        st.subheader("Publishers")
        pub_df = entity_summary(
            ["Publisher"], where_sql, params, min_impressions, impressions_op=impressions_op,
            extra_select="arg_max(Category, Run_Date) AS Category,",
        )
        clicked = render_aggregate_stack(pub_df, "Publisher", "pub", where_sql, params)
        if clicked:
            st.session_state.drill_pub = clicked
            st.rerun()

        st.subheader("All publishers -- summary")
        st.dataframe(
            pub_df[["Publisher", "Category", "Impressions", "Impression_Share_Pct",
                    "Actual_Clearing_CPM", "Avg_Bid", "Pct_Killed"]],
            width='stretch', hide_index=True,
        )
    elif not st.session_state.drill_pub_li:
        # Level 2: line items delivering through this publisher -- NOT deals.
        # Breaking straight into every (line item, deal) pair here was too
        # granular to load; that detail is one more click away, scoped to a
        # specific line item, same as Line Item View's own drill-down.
        pub = st.session_state.drill_pub
        if st.button("⬅ Back to publishers"):
            st.session_state.drill_pub = None
            st.rerun()
        st.subheader(f"Line items delivering through {pub}")
        pub_where = where_sql + " AND Publisher = ?"
        pub_params = params + [pub]

        li_df = entity_summary(
            ["BW_Line_Item_ID"], pub_where, pub_params, min_impressions, impressions_op=impressions_op,
            extra_select="arg_max(SF_Line_Item_ID, Run_Date) AS SF_Line_Item_ID, "
                          "arg_max(Line_Item_Name, Run_Date) AS Line_Item_Name,",
        )
        # This CPM is scoped to just this publisher's slice of the line item --
        # merge in the line item's own true total (every publisher, every deal)
        # so it's visible right alongside it, not just the narrower number.
        li_df["LI_True_Actual_CPM"] = li_df["BW_Line_Item_ID"].astype(str).map(
            li_true_cpm_batch(li_df["BW_Line_Item_ID"].astype(str).tolist(), date_only_where, date_only_params)
        )
        clicked = render_aggregate_stack(li_df, "BW_Line_Item_ID", "pub_li", pub_where, pub_params,
                                          share_pct_label=True, show_pacing_labels=True)
        if clicked:
            st.session_state.drill_pub_li = clicked
            st.rerun()

        st.subheader(f"Line items delivering through {pub} -- summary")
        st.dataframe(
            li_df[["Line_Item_Name", "BW_Line_Item_ID", "SF_Line_Item_ID", "Impressions",
                   "Impression_Share_Pct", "Avg_Bid", "Actual_Clearing_CPM", "Pct_Killed"]],
            width='stretch', hide_index=True,
        )
    else:
        # Level 3: deals for this (publisher, line item) pair.
        pub = st.session_state.drill_pub
        li_id = st.session_state.drill_pub_li
        if st.button("⬅ Back to line items"):
            st.session_state.drill_pub_li = None
            st.rerun()
        pub_li_where = where_sql + " AND Publisher = ? AND BW_Line_Item_ID = ?"
        pub_li_params = params + [pub, li_id]
        name_row = q("SELECT DISTINCT Line_Item_Name FROM history WHERE BW_Line_Item_ID = ? LIMIT 1", [li_id])
        li_name = safe_label([name_row.iloc[0]["Line_Item_Name"]] if not name_row.empty else [], li_id)
        st.subheader(f"Deals for {li_name} ({li_id}) via {pub}")

        deal_df = entity_summary(
            ["Deal_ID", "Category", "BW_Line_Item_ID"], pub_li_where, pub_li_params, min_impressions, impressions_op=impressions_op,
            extra_select="arg_max(Floor_Price, Run_Date) AS Floor_Price,",
        )
        render_single_instance_stack(
            deal_df,
            lambda ent: (
                ent["Deal_ID"],
                [
                    _floor_text(ent),
                    _cpm_text(ent),
                    str(ent.get("Category") or ""),
                ],
            ),
            where_sql, params, "pub_li_deal", show_pacing_labels=True,
        )

        st.subheader("Breakout: impression share, avg bid, kill %, floor price by deal")
        st.dataframe(
            deal_df[["Deal_ID", "Category", "Floor_Price", "Impressions", "Impression_Share_Pct",
                     "Avg_Bid", "Actual_Clearing_CPM", "Pct_Killed"]],
            width='stretch', hide_index=True,
        )

# ══════════════════════════════════════════════════════════════════════════
# DEAL VIEW
# ══════════════════════════════════════════════════════════════════════════

elif view == "Deal View":
    if not st.session_state.drill_deal:
        st.subheader("Deals (aggregated across whatever line items carry them)")
        deal_df = entity_summary(
            ["Deal_ID"], where_sql, params, min_impressions, impressions_op=impressions_op,
            extra_select="arg_max(Publisher, Run_Date) AS Publisher, arg_max(Category, Run_Date) AS Category, arg_max(Floor_Price, Run_Date) AS Floor_Price,",
        )
        clicked = render_aggregate_stack(deal_df, "Deal_ID", "deal_top", where_sql, params)
        if clicked:
            st.session_state.drill_deal = clicked
            st.rerun()

        st.subheader("Breakout: impressions and avg bid by deal")
        st.dataframe(
            deal_df[["Deal_ID", "Publisher", "Category", "Floor_Price", "Impressions",
                     "Impression_Share_Pct", "Avg_Bid", "Actual_Clearing_CPM", "Pct_Killed"]],
            width='stretch', hide_index=True,
        )
    else:
        deal_id = st.session_state.drill_deal
        if st.button("⬅ Back to deals"):
            st.session_state.drill_deal = None
            st.rerun()
        st.subheader(f"Deal {deal_id} — broken out by line item")
        deal_where = where_sql + " AND Deal_ID = ?"
        deal_params = params + [deal_id]

        li_df = entity_summary(
            ["BW_Line_Item_ID"], deal_where, deal_params, min_impressions, impressions_op=impressions_op,
            extra_select="arg_max(SF_Line_Item_ID, Run_Date) AS SF_Line_Item_ID, "
                          "arg_max(Line_Item_Name, Run_Date) AS Line_Item_Name, "
                          "arg_max(Floor_Price, Run_Date) AS Floor_Price,",
        )
        li_df["Deal_ID"] = deal_id
        li_df["LI_True_Actual_CPM"] = li_df["BW_Line_Item_ID"].astype(str).map(
            li_true_cpm_batch(li_df["BW_Line_Item_ID"].astype(str).tolist(), date_only_where, date_only_params)
        )
        render_single_instance_stack(
            li_df,
            lambda ent: (
                ent["BW_Line_Item_ID"],
                [
                    str(ent["Line_Item_Name"]) if pd.notna(ent.get("Line_Item_Name")) else "",
                    _floor_text(ent),
                    _cpm_text(ent),
                    _li_total_cpm_text(ent),
                    f"SF {ent['SF_Line_Item_ID']}",
                ],
            ),
            where_sql, params, "deal_li",
            share_pct=True, show_pacing_labels=True,
        )

        st.subheader("Breakout: impression share by line item")
        st.dataframe(
            li_df[["Line_Item_Name", "BW_Line_Item_ID", "SF_Line_Item_ID", "Impressions",
                   "Impression_Share_Pct", "Avg_Bid", "Actual_Clearing_CPM", "Pct_Killed"]],
            width='stretch', hide_index=True,
        )

# ══════════════════════════════════════════════════════════════════════════
# TOTAL
# ══════════════════════════════════════════════════════════════════════════

else:
    st.subheader("Total: actual clearing CPM & avg bid by day, across all lines and deals")
    total_daily = daily_totals(where_sql, params)
    total_chart = chart_aggregate(total_daily, height=200)
    if total_chart is not None:
        st.altair_chart(total_chart, width='stretch', theme=None)
    else:
        st.info("No rows match the current filters.")

    st.subheader("Totals by line item")
    li_total = entity_summary(
        ["BW_Line_Item_ID"], where_sql, params, min_impressions, impressions_op=impressions_op,
        extra_select="arg_max(SF_Line_Item_ID, Run_Date) AS SF_Line_Item_ID, "
                      "arg_max(Line_Item_Name, Run_Date) AS Line_Item_Name,",
    )
    st.dataframe(
        li_total[["Line_Item_Name", "BW_Line_Item_ID", "SF_Line_Item_ID", "Impressions",
                  "Impression_Share_Pct", "Avg_Bid", "Actual_Clearing_CPM", "Pct_Killed"]],
        width='stretch', hide_index=True,
    )

    st.subheader("Totals by publisher")
    pub_total = entity_summary(
        ["Publisher"], where_sql, params, min_impressions, impressions_op=impressions_op, extra_select="arg_max(Category, Run_Date) AS Category,",
    )
    st.dataframe(
        pub_total[["Publisher", "Category", "Impressions", "Impression_Share_Pct",
                   "Avg_Bid", "Actual_Clearing_CPM", "Pct_Killed"]],
        width='stretch', hide_index=True,
    )

    st.subheader("Totals by deal")
    deal_total = entity_summary(
        ["Deal_ID"], where_sql, params, min_impressions, impressions_op=impressions_op,
        extra_select="arg_max(Publisher, Run_Date) AS Publisher, arg_max(Category, Run_Date) AS Category, arg_max(Floor_Price, Run_Date) AS Floor_Price,",
    )
    st.dataframe(
        deal_total[["Deal_ID", "Publisher", "Category", "Floor_Price", "Impressions",
                    "Impression_Share_Pct", "Avg_Bid", "Actual_Clearing_CPM", "Pct_Killed"]],
        width='stretch', hide_index=True,
    )

# Decision reason breakdown + glossary -- on every view, per request.
render_reason_footer(where_sql, params)
