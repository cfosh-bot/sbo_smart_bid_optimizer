"""Streamlit UI for Smart Bid Optimizer.

Run locally:
    streamlit run app.py

First launch will trigger the Google OAuth flow in your browser.

Layout:
    sidebar  — config picker (podcast / streaming / …), sheet ID, current user
    Run      — phase buttons, live log tail
    Review   — pre-push review of the latest run's Bid Optimizer (cols Z, AA, AC, AD)
    History  — recent run folders + metadata
    State    — persistent state files (do not delete)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from sbo.config_models import load_config
from sbo.run_storage import list_recent_runs

load_dotenv()

st.set_page_config(
    page_title="Smart Bid Optimizer",
    page_icon="📈",
    layout="wide",
)

# ── sidebar ───────────────────────────────────────────────────────────────

st.sidebar.title("SBO Control")

tactic = st.sidebar.selectbox(
    "Tactic",
    options=["podcast", "streaming", "marketplace_ctv", "select_ctv"],
    index=0,
)
config_path = Path(f"sbo/config/{tactic}.yaml")
if not config_path.exists():
    st.sidebar.error(f"Config not found: {config_path}")
    st.stop()

cfg = load_config(config_path)
st.sidebar.caption(f"Config: `{config_path}`")

env_var = f"SHEET_ID_{tactic.upper()}"
default_sheet_id = os.environ.get(env_var, "")
sheet_id = st.sidebar.text_input(
    "Google Sheet ID",
    value=default_sheet_id,
    help=f"Set {env_var} in .env to skip this step",
)

if not sheet_id:
    st.sidebar.warning(f"No sheet ID — set {env_var} in .env or paste above")

st.sidebar.divider()
st.sidebar.caption(f"User: `{os.environ.get('USER', 'unknown')}`")

# ── main ──────────────────────────────────────────────────────────────────

st.title("📈 Smart Bid Optimizer")
st.caption(f"Tactic: **{cfg.tactic}** • Modifier suffix: `{cfg.beeswax.modifier_suffix}`")

tab_run, tab_review, tab_history, tab_state = st.tabs(
    ["▶ Run", "🔍 Review", "📁 History", "💾 State"]
)


# ── Run tab ──────────────────────────────────────────────────────────────


def _run_phase(phase_key: str, fn_name: str, **kwargs):
    """Spin up a RunContext, invoke the phase fn, stream logs back to UI."""
    if not sheet_id:
        st.error("Set the Google Sheet ID in the sidebar first.")
        return
    from sbo import pipeline

    user = os.environ.get("USER", "unknown")
    lock_acquired = False

    with st.status(f"Running {fn_name}...", expanded=True) as status:
        ctx = None
        try:
            ctx = pipeline.build_context(
                config_path=str(config_path),
                sheet_id=sheet_id,
                phase=phase_key,
            )
            st.write(f"Run folder: `{ctx.run.path}`")

            # Soft lock — warn if someone else is running, but allow override
            current = ctx.sheets.lock_status()
            if current.get("status") == "running" and not current.get("_stale"):
                st.warning(
                    f"⚠️ **{current.get('running_user')}** is currently running "
                    f"`{current.get('phase')}` (started {current.get('started_at')}). "
                    f"Continuing anyway — but coordinate before pushing!"
                )
            lock_acquired = ctx.sheets.acquire_lock(user, fn_name, ctx.run.folder_name)
            getattr(pipeline, fn_name)(ctx, **kwargs)
            status.update(label=f"✅ {fn_name} complete", state="complete")
            log_path = ctx.run.path / "logs.txt"
            if log_path.exists():
                st.code(log_path.read_text()[-3000:], language="text")
        except Exception as e:
            status.update(label=f"❌ {fn_name} failed", state="error")
            st.exception(e)
        finally:
            if ctx is not None:
                if lock_acquired:
                    try:
                        ctx.sheets.release_lock()
                    except Exception:
                        pass
                pipeline.close_context(ctx)


with tab_run:
    st.subheader("Daily flow")
    st.caption(
        "Run a full pipeline, push approved multipliers, or just refresh pacing. "
        "Each phase writes a fresh folder under `runs/`."
    )

    cols = st.columns(3)
    with cols[0]:
        if st.button("🌅 Full Run", use_container_width=True, type="primary"):
            _run_phase("full", "run_full")
    with cols[1]:
        if st.button("📊 Pacing Only", use_container_width=True):
            _run_phase("pacing_only", "run_pacing_only")
    with cols[2]:
        push_dry = st.checkbox("dry-run push", value=False, key="push_dry")
        if st.button("🚀 Push Only", use_container_width=True):
            _run_phase("pushonly", "run_pushonly", dry_run=push_dry)

    st.divider()
    st.subheader("Migration phases")
    st.caption(
        "Less-frequent maintenance: onboard new lines, patch line items, or sync deal terms."
    )
    cols2 = st.columns(3)
    with cols2[0]:
        if st.button("Phase 1 — Create modifiers", use_container_width=True):
            _run_phase("phase1", "run_phase1")
    with cols2[1]:
        if st.button("Phase 2 — Patch line items", use_container_width=True):
            _run_phase("phase2", "run_phase2")
    with cols2[2]:
        new_only = st.checkbox("new lines only (col I)", value=False, key="p3_new_only")
        if st.button("Phase 3 — Sync deal terms", use_container_width=True):
            _run_phase("phase3", "run_phase3", new_only=new_only)


# ── Review tab — load latest run's Bid Optimizer + decisions ────────────


def _latest_run_for_phase(runs_dir: Path, tactic_name: str, phase: str) -> Path | None:
    """Pick the most recent successful run folder for this tactic + phase."""
    if not runs_dir.exists():
        return None
    candidates = []
    for p in runs_dir.iterdir():
        if not p.is_dir() or tactic_name not in p.name:
            continue
        meta = p / "00_run_metadata.json"
        if not meta.exists():
            continue
        try:
            data = json.loads(meta.read_text())
        except Exception:
            continue
        if data.get("phase") != phase:
            continue
        if data.get("status") not in ("success", "partial"):
            continue
        candidates.append(p)
    candidates.sort(key=lambda p: p.name, reverse=True)
    return candidates[0] if candidates else None


with tab_review:
    st.subheader("Bid Optimizer review")
    st.caption(
        "Pre-push review of the latest **full run**. Inspect decisions, then "
        "click **Push Only** on the Run tab to send these multipliers to Beeswax."
    )

    runs_dir = Path(os.environ.get("RUNS_DIR", "runs"))
    latest = _latest_run_for_phase(runs_dir, cfg.tactic, "full")
    if latest is None:
        st.info(
            "No completed full run found for this tactic. Run **Full Run** on the "
            "Run tab first."
        )
    else:
        st.caption(f"Showing: `{latest.name}`")
        bo_path = latest / "04_bid_optimizer.parquet"
        if not bo_path.exists():
            st.warning("Run folder is missing `04_bid_optimizer.parquet`.")
        else:
            df = pd.read_parquet(bo_path)
            kpi_cols = st.columns(5)
            kpi_cols[0].metric("Total deal terms", f"{len(df):,}")
            kpi_cols[1].metric("Unique LIs", f"{df['BW_Line_Item_ID'].nunique():,}")
            new_mult = pd.to_numeric(df.get("Calculated_New_Multiplier"), errors="coerce")
            curr_mult = pd.to_numeric(df.get("Current_Multiplier"), errors="coerce")
            changed = (new_mult - curr_mult).abs() > 0.001
            kpi_cols[2].metric("Will change", f"{int(changed.sum()):,}")
            held = (~changed) & new_mult.notna()
            kpi_cols[3].metric("Held", f"{int(held.sum()):,}")
            kpi_cols[4].metric("No decision", f"{int(new_mult.isna().sum()):,}")

            # Filter controls
            st.divider()
            f_cols = st.columns(4)
            reason_filter = f_cols[0].text_input(
                "Filter by reason code (substring)",
                placeholder="e.g. PACE_UP_CRITICAL",
                key="reason_filter",
            )
            bw_filter = f_cols[1].text_input(
                "Filter by BW LI ID",
                placeholder="e.g. 7553",
                key="bw_filter",
            )
            only_changed = f_cols[2].checkbox("Only changing rows", value=False)
            cap_rows = f_cols[3].number_input(
                "Show top N rows", min_value=50, max_value=5000, value=500, step=50,
            )

            view = df.copy()
            if reason_filter:
                view = view[
                    view.get("Decision_Reason", "").astype(str).str.contains(
                        reason_filter, case=False, na=False
                    )
                ]
            if bw_filter:
                view = view[
                    view["BW_Line_Item_ID"].astype(str).str.contains(bw_filter, na=False)
                ]
            if only_changed:
                v_new = pd.to_numeric(view["Calculated_New_Multiplier"], errors="coerce")
                v_curr = pd.to_numeric(view["Current_Multiplier"], errors="coerce")
                view = view[(v_new - v_curr).abs() > 0.001]

            display_cols = [
                "BW_Line_Item_ID", "Line_Item_Name", "Deal_ID", "Modifier_Deal_List",
                "CPM_Bid", "Pacing_Pct", "Days_Remaining",
                "Current_Multiplier", "Calculated_New_Multiplier",
                "Effective_Bid_Current", "Effective_Bid_New",
                "Decision_Reason",
            ]
            display_cols = [c for c in display_cols if c in view.columns]
            st.dataframe(
                view[display_cols].head(int(cap_rows)),
                use_container_width=True,
                hide_index=True,
            )
            st.caption(
                f"Showing {min(len(view), int(cap_rows)):,} of {len(view):,} filtered rows. "
                f"Underlying parquet: `{bo_path.resolve()}`"
            )


# ── History tab ──────────────────────────────────────────────────────────


with tab_history:
    st.subheader("Recent runs")
    runs_dir = Path(os.environ.get("RUNS_DIR", "runs"))
    recent = list_recent_runs(runs_dir, limit=20)
    if not recent:
        st.info(f"No runs yet under `{runs_dir}/`. Hit Run on the Run tab to get going.")
    else:
        for folder in recent:
            meta_path = folder / "00_run_metadata.json"
            label = folder.name
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text())
                    status_emoji = {
                        "success": "✅", "partial": "⚠️",
                        "failed": "❌", "running": "🟡",
                    }.get(meta.get("status", ""), "•")
                    label = f"{status_emoji} {folder.name}"
                except Exception:
                    pass
            with st.expander(label):
                if meta_path.exists():
                    st.code(meta_path.read_text(), language="json")
                files = sorted(p.name for p in folder.iterdir() if p.is_file())
                st.write("**Files:**", ", ".join(files))
                st.caption(f"Path: `{folder.resolve()}`")


# ── State tab ────────────────────────────────────────────────────────────


with tab_state:
    st.subheader("Persistent state")
    st.caption(
        "These files survive across runs. Deleting them will cause every line to "
        "Day-1-baseline on the next run — don't."
    )

    # ── One-time migration from Apps Script tabs ─────────────────────
    st.divider()
    st.markdown("##### 🔁 One-time: import Apps Script state")
    st.caption(
        "Run this **once** before your first Full Run to inherit Apps Script's "
        "Day-1/Day-2 logs, paused log, kill log, pacing history, and modifier map. "
        "Without this, every line will look like Day 1 to the engine even though "
        "Apps Script has been tracking it for weeks."
    )
    import_cols = st.columns([1, 1, 2])
    with import_cols[0]:
        overwrite_existing = st.checkbox(
            "overwrite existing state",
            value=False,
            help="Replace any non-empty Parquet files. Off by default — safer to skip.",
            key="state_import_overwrite",
        )
    with import_cols[1]:
        if st.button("Import state from sheet", use_container_width=True):
            if not sheet_id:
                st.error("Set the Google Sheet ID in the sidebar first.")
            else:
                from sbo.state_import import import_apps_script_state

                logs: list[str] = []

                def _ui_log(msg: str):
                    logs.append(str(msg))

                with st.status("Importing Apps Script state...", expanded=True) as status:
                    try:
                        counts = import_apps_script_state(
                            sheet_id=sheet_id,
                            state_dir=os.environ.get("STATE_DIR", "state"),
                            overwrite=overwrite_existing,
                            log=_ui_log,
                        )
                        st.code("\n".join(logs), language="text")
                        st.json(counts)
                        total = sum(counts.values())
                        if total > 0:
                            status.update(
                                label=f"✅ Imported {total:,} rows", state="complete",
                            )
                        else:
                            status.update(
                                label="⚠️ Nothing imported — state files already exist "
                                "(check 'overwrite' to replace).",
                                state="error",
                            )
                    except Exception as e:
                        status.update(label="❌ Import failed", state="error")
                        st.exception(e)

    # ── State file viewer ───────────────────────────────────────────
    st.divider()
    state_dir = Path(os.environ.get("STATE_DIR", "state"))
    if not state_dir.exists():
        st.info(f"State dir `{state_dir}/` doesn't exist yet — created on first run.")
    else:
        files = sorted(p for p in state_dir.iterdir() if p.suffix == ".parquet")
        if not files:
            st.info(
                "No state files yet — either run **Full Run** once OR use the import "
                "button above to bring over Apps Script history."
            )
        else:
            for f in files:
                with st.expander(f"{f.name} ({f.stat().st_size // 1024} KB)"):
                    try:
                        df = pd.read_parquet(f)
                        st.write(f"Rows: {len(df):,}  •  Cols: {list(df.columns)}")
                        st.dataframe(df.head(20), use_container_width=True)
                    except Exception as e:
                        st.error(f"Could not read: {e}")
