"""Apply EngineResult side-effects to persistent state.

The engine is a pure function — it just returns what changed. This module
takes those changes and writes them to the StateStore + paused_log.
Without this, the Day-1/Day-2 baseline tracking, pause detection, and
pacing history would all reset every run.

Called once at the end of pipeline.run_full, after the engine pass.

NOTE: all date/time fields are stored as ISO-format strings, not native
datetime, so Parquet's strict typing doesn't choke on mixed empty/datetime
columns (a `Resumed_Date` column may have some rows blank and others set).
"""

from __future__ import annotations

from datetime import datetime
from typing import List

import pandas as pd

from sbo.multiplier_engine import EngineResult
from sbo.state import StateStore


def _iso(ts: datetime) -> str:
    return ts.isoformat(timespec="seconds")


def apply_engine_state(
    state: StateStore, result: EngineResult, run_date: datetime,
) -> dict:
    """Write all engine side-effects to state. Returns a counts summary."""
    counts = {
        "first_run_added": 0, "second_run_added": 0,
        "pre_flight_resets": 0,
        "pauses_added": 0, "lines_resumed": 0,
        "kill_log_rows": 0, "pacing_history_rows": 0,
    }
    run_date_iso = _iso(run_date)

    # 1. Day 1 / Day 2 run logs (append new entries) ─────────────────────
    if result.new_first_run:
        first = state.load("first_run_log")
        new = pd.DataFrame([
            {"BW_Line_Item_ID": bw, "End_Date": str(ed), "First_Run_Date": run_date_iso}
            for bw, ed in result.new_first_run
        ])
        merged = pd.concat([first, new], ignore_index=True) if not first.empty else new
        merged["End_Date"] = merged["End_Date"].astype(str)
        merged["BW_Line_Item_ID"] = merged["BW_Line_Item_ID"].astype(str)
        state.save("first_run_log", merged)
        counts["first_run_added"] = len(new)

    if result.new_second_run:
        second = state.load("second_run_log")
        new = pd.DataFrame([
            {"BW_Line_Item_ID": bw, "End_Date": str(ed), "Second_Run_Date": run_date_iso}
            for bw, ed in result.new_second_run
        ])
        merged = pd.concat([second, new], ignore_index=True) if not second.empty else new
        merged["End_Date"] = merged["End_Date"].astype(str)
        merged["BW_Line_Item_ID"] = merged["BW_Line_Item_ID"].astype(str)
        state.save("second_run_log", merged)
        counts["second_run_added"] = len(new)

    # 2. Pre-flight resets (delete first/second-run entries to re-baseline) ─
    if result.pre_flight_resets:
        ids = set(result.pre_flight_resets)
        for log_name in ("first_run_log", "second_run_log"):
            df = state.load(log_name)
            if not df.empty:
                df = df[~df["BW_Line_Item_ID"].astype(str).isin(ids)]
                state.save(log_name, df.reset_index(drop=True))
        counts["pre_flight_resets"] = len(ids)

    # 3. New pauses (append to paused_log with empty Resumed_Date) ────────
    if result.new_pauses:
        paused = state.load("paused_log")
        new = pd.DataFrame([
            {
                "BW_Line_Item_ID": bw, "End_Date": ed,
                "Paused_Date": run_date_iso, "Resumed_Date": "",
            }
            for bw, ed in result.new_pauses
        ])
        merged = pd.concat([paused, new], ignore_index=True) if not paused.empty else new
        merged["End_Date"] = merged["End_Date"].astype(str)
        merged["BW_Line_Item_ID"] = merged["BW_Line_Item_ID"].astype(str)
        state.save("paused_log", merged)
        counts["pauses_added"] = len(new)

    # 3b. Paused multiplier snapshot (save per-deal held multipliers at pause time)
    if result.pause_snapshots:
        snap = state.load("paused_snapshot")
        new_snap = pd.DataFrame(result.pause_snapshots)
        merged_snap = pd.concat([snap, new_snap], ignore_index=True) if not snap.empty else new_snap
        state.save("paused_snapshot", merged_snap)

    # 4. Resumed lines (write Resumed_Date back into paused_log) ──────────
    if result.resumed_row_indices:
        paused = state.load("paused_log")
        if not paused.empty:
            # Force string dtype so we don't get mixed datetime/empty mismatch
            paused["Resumed_Date"] = paused["Resumed_Date"].fillna("").astype(str)
            valid_idx = [i for i in result.resumed_row_indices if i in paused.index]
            paused.loc[valid_idx, "Resumed_Date"] = run_date_iso
            state.save("paused_log", paused)
            counts["lines_resumed"] = len(valid_idx)

    # 5. Publisher kill log (append) ──────────────────────────────────────
    if result.kill_log_entries:
        kill = state.load("kill_log")
        new = pd.DataFrame(
            [
                # Stringify the Action_Date so parquet doesn't choke on mixed types
                [bw, action, _iso(ts) if isinstance(ts, datetime) else str(ts),
                 deal_mod, undo, end_date]
                for bw, action, ts, deal_mod, undo, end_date in result.kill_log_entries
            ],
            columns=[
                "BW_Line_Item_ID", "Action", "Action_Date",
                "Deal_ID_and_Modifier_Type", "Undo_Date", "End_Date",
            ],
        )
        merged = pd.concat([kill, new], ignore_index=True) if not kill.empty else new
        state.save("kill_log", merged)
        counts["kill_log_rows"] = len(new)

    # 6. Pacing history (append today's column to wide-format DF) ──────────
    if result.pacing_signals:
        pacing_history_update(state, result.pacing_signals, run_date)
        counts["pacing_history_rows"] = len(result.pacing_signals)

    return counts


def pacing_history_update(
    state: StateStore, signals: dict, run_date: datetime, max_runs: int = 4,
) -> None:
    """Append today's column to the wide-format pacing_history.

    Format: BW_Line_Item_ID | <date1> | <date2> | … | <today>
    Cells: 'OVER' (≥100%), 'UNDER' (<100%), '' (paused/no data).
    Keeps only the last `max_runs` date columns.
    """
    today_col = run_date.strftime("%Y-%m-%d")
    df = state.load_pacing_history(max_runs=max_runs)

    if df.empty:
        new_df = pd.DataFrame([
            {"BW_Line_Item_ID": bw, today_col: sig}
            for bw, sig in signals.items()
        ])
        state.save_pacing_history(new_df)
        return

    if today_col not in df.columns:
        df[today_col] = ""
    df = df.set_index("BW_Line_Item_ID")
    for bw, sig in signals.items():
        if bw not in df.index:
            df.loc[bw] = ""
        df.at[bw, today_col] = sig
    df = df.reset_index()

    # Trim to max_runs date columns (newest-rightmost preserved)
    date_cols: List[str] = [c for c in df.columns if c != "BW_Line_Item_ID"]
    if len(date_cols) > max_runs:
        keep = date_cols[-max_runs:]
        df = df[["BW_Line_Item_ID", *keep]]

    # Prune rows where all date columns are blank (line has no signal in last N runs)
    date_cols_kept = [c for c in df.columns if c != "BW_Line_Item_ID"]
    if date_cols_kept:
        has_any = df[date_cols_kept].apply(
            lambda row: any(str(v).strip() in ("OVER", "UNDER") for v in row), axis=1
        )
        pruned = (~has_any).sum()
        df = df[has_any].reset_index(drop=True)
        if pruned > 0:
            import logging
            logging.getLogger(__name__).info(f"Pacing History: pruned {pruned} all-blank LI rows")

    state.save_pacing_history(df)


def prune_state_at_run_start(state: StateStore, today: datetime) -> dict:
    """Run before the engine to drop expired run-log entries.

    Without this, ended campaigns linger forever and a reused BW LI ID
    would skip Day-1 baseline incorrectly.
    """
    first_pruned, second_pruned = state.prune_expired_run_logs(
        pd.Timestamp(today.date())
    )
    return {"first_pruned": first_pruned, "second_pruned": second_pruned}
