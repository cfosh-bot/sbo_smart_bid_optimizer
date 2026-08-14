"""Push calculated multipliers to Beeswax.

Port of `pushMultipliersToBeewax` (Apps Script Section 19). One GET + one
PUT per bid modifier (not per deal term), so a modifier with 50 changing
terms still costs only 2 API calls.

Inputs:
    bid_optimizer DataFrame with cols AA–AD populated (engine output).
    Only rows where `Calculated_New_Multiplier` differs from
    `Current_Multiplier` are pushed.

Output:
    PushResults dataclass + 06_push_results.parquet:
        BW_Line_Item_ID | Deal_ID | Bid_Modifier_ID
        Prev_Multiplier | New_Multiplier | Status | Error | Pushed_At

Safety:
    - `dry_run=True` skips all PUTs and just builds the result DataFrame
    - Per-modifier try/except — one bad modifier doesn't kill the run
    - "No matching terms in live modifier" → logged, not pushed
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from sbo.beeswax_client import BeeswaxClient, BeeswaxError
from sbo.config_models import EngineConfig
from sbo.run_storage import RunFolder
from sbo.utils import safe_float

PUSH_RESULT_COLUMNS = [
    "BW_Line_Item_ID",
    "Deal_ID",
    "Bid_Modifier_ID",
    "Prev_Multiplier",
    "New_Multiplier",
    "Status",
    "Error",
    "Pushed_At",
]


@dataclass
class PushSummary:
    updated: int = 0      # term writes that actually changed
    skipped: int = 0      # rows where new == current (no push needed)
    errors: int = 0       # term writes that failed
    modifiers_touched: int = 0
    modifiers_failed: int = 0
    error_log: List[Dict[str, Any]] = field(default_factory=list)


def push_multipliers_to_beeswax(
    bw: BeeswaxClient,
    cfg: EngineConfig,
    run: RunFolder,
    bid_optimizer: pd.DataFrame,
    *,
    dry_run: bool = False,
) -> tuple[pd.DataFrame, PushSummary]:
    """Apply Calculated_New_Multiplier values to Beeswax.

    Args:
        bw: authenticated client
        cfg: engine config
        run: per-run folder for results + raw API capture
        bid_optimizer: DataFrame with cols A–AD; only rows with non-empty
            `Calculated_New_Multiplier` are considered
        dry_run: if True, no PUT calls are made — useful for AM review

    Returns:
        (push_results_df, PushSummary). push_results_df is also saved to
        06_push_results.parquet inside the run folder.
    """
    run.log(f"=== push_multipliers start (dry_run={dry_run}) ===")
    bw.authenticate()

    summary = PushSummary()
    results: List[Dict[str, Any]] = []

    # 1. Filter to rows that actually need pushing
    work = _filter_changes(bid_optimizer, summary)
    if work.empty:
        run.log("No rows with multiplier changes — nothing to push.")
        df = pd.DataFrame(columns=PUSH_RESULT_COLUMNS)
        run.save_dataframe("06_push_results", df)
        run.save_csv("06_push_results", df)
        return df, summary

    # 2. Group by Bid_Modifier_ID — one GET + PUT per modifier
    grouped = work.groupby("Bid_Modifier_ID", sort=False)
    run.log(
        f"Push plan: {len(grouped):,} modifiers, "
        f"{len(work):,} term changes, {summary.skipped:,} unchanged"
    )

    now = datetime.now()
    for mod_id, group in grouped:
        mod_id = str(mod_id).strip()
        if not mod_id:
            for _, row in group.iterrows():
                results.append(_result_row(row, now, "❌ Skipped — no modifier ID", ""))
                summary.errors += 1
            continue

        # Map deal_id → new_multiplier for this modifier
        deal_to_new: Dict[str, float] = {}
        for _, row in group.iterrows():
            try:
                deal_to_new[str(row["Deal_ID"]).strip()] = float(row["Calculated_New_Multiplier"])
            except (TypeError, ValueError):
                continue

        try:
            updated_count = _push_one_modifier(
                bw=bw, mod_id=mod_id, deal_to_new=deal_to_new,
                run=run, dry_run=dry_run,
            )
        except BeeswaxError as e:
            run.log(f"❌ Modifier {mod_id} failed: {e}")
            summary.modifiers_failed += 1
            for _, row in group.iterrows():
                results.append(_result_row(
                    row, now, f"❌ Error: {str(e)[:120]}", str(e)
                ))
                summary.error_log.append({
                    "modifier_id": mod_id,
                    "bw_id": str(row.get("BW_Line_Item_ID", "")),
                    "deal_id": str(row.get("Deal_ID", "")),
                    "error": str(e),
                })
                summary.errors += 1
            continue

        summary.modifiers_touched += 1
        if updated_count == 0:
            for _, row in group.iterrows():
                results.append(_result_row(row, now, "⚠ No matching terms", ""))
        else:
            for _, row in group.iterrows():
                prev = _safe_float(row.get("Current_Multiplier"))
                new = _safe_float(row.get("Calculated_New_Multiplier"))
                if abs(new - prev) > 0.001:
                    msg = (
                        f"{'[DRY RUN] ' if dry_run else '✅ '}"
                        f"{prev:.3f} → {new:.3f}"
                    )
                    results.append(_result_row(row, now, msg, ""))
                    summary.updated += 1
                else:
                    results.append(_result_row(row, now, "⏭️ No change", ""))
                    summary.skipped += 1
        run.log(
            f"  Modifier {mod_id}: {updated_count} term(s) "
            f"{'would be ' if dry_run else ''}written"
        )

    # 3. Save results to run folder — Parquet (machine-readable, used internally)
    #    + CSV (human-readable, this is the file to open in Excel/Sheets)
    df = pd.DataFrame(results, columns=PUSH_RESULT_COLUMNS)
    run.save_dataframe("06_push_results", df)
    run.save_csv("06_push_results", df)

    run.log(
        f"=== push complete: updated={summary.updated} skipped={summary.skipped} "
        f"errors={summary.errors} | modifiers={summary.modifiers_touched} "
        f"failed={summary.modifiers_failed} ==="
    )
    return df, summary


# ── internals ─────────────────────────────────────────────────────────────


def _filter_changes(
    bid_optimizer: pd.DataFrame, summary: PushSummary
) -> pd.DataFrame:
    """Keep rows where Calculated_New_Multiplier differs from Current_Multiplier.

    SAFETY: rows with a missing Current_Multiplier are SKIPPED, not defaulted
    to 0. We can't safely diff against 'unknown' — defaulting would cause
    every such row to look like a change and force a blind PUT.
    """
    if bid_optimizer.empty:
        return bid_optimizer

    df = bid_optimizer.copy()
    df["_new_f"] = pd.to_numeric(df["Calculated_New_Multiplier"], errors="coerce")
    df["_curr_f"] = pd.to_numeric(df["Current_Multiplier"], errors="coerce")

    has_new = df["_new_f"].notna()
    has_curr = df["_curr_f"].notna()  # ← required: we never push without a known current
    has_modifier = df["Bid_Modifier_ID"].astype(str).str.strip().ne("")
    has_deal = df["Deal_ID"].astype(str).str.strip().ne("")
    valid = has_new & has_curr & has_modifier & has_deal
    summary.skipped += int((~valid).sum())

    df = df[valid].copy()
    # Only push rows that actually differ from current (within float epsilon)
    df["_changed"] = (df["_new_f"] - df["_curr_f"]).abs() > 0.001
    no_change = df[~df["_changed"]]
    summary.skipped += len(no_change)
    return df[df["_changed"]].drop(columns=["_new_f", "_curr_f", "_changed"])


def _push_one_modifier(
    *,
    bw: BeeswaxClient,
    mod_id: str,
    deal_to_new: Dict[str, float],
    run: RunFolder,
    dry_run: bool,
) -> int:
    """GET → mutate matching terms → PUT. Returns number of terms changed."""
    mod_obj = bw.get_bid_modifier(mod_id)
    # Capture raw GET for replay/QA
    try:
        run.save_beeswax_response(
            f"bid_modifier_{mod_id}_get", _truncated_json(mod_obj)
        )
    except Exception:
        pass

    terms = mod_obj.get("terms") or []
    changed = 0
    for term in terms:
        deal_id = str(term.get("value") or "").strip()
        if deal_id in deal_to_new:
            new_mult = f"{deal_to_new[deal_id]:.2f}"
            if str(term.get("multiplier")) != new_mult:
                term["multiplier"] = new_mult
                changed += 1

    if changed == 0:
        return 0
    if dry_run:
        return changed

    bw.update_bid_modifier(mod_id, mod_obj)
    return changed


def _result_row(row: pd.Series, ts: datetime, status: str, error: str) -> Dict[str, Any]:
    return {
        "BW_Line_Item_ID": str(row.get("BW_Line_Item_ID", "")),
        "Deal_ID": str(row.get("Deal_ID", "")),
        "Bid_Modifier_ID": str(row.get("Bid_Modifier_ID", "")),
        "Prev_Multiplier": _safe_float(row.get("Current_Multiplier")),
        "New_Multiplier": _safe_float(row.get("Calculated_New_Multiplier")),
        "Status": status,
        "Error": error,
        "Pushed_At": ts,
    }


_safe_float = safe_float  # backwards-compat alias


def _truncated_json(obj: Any, max_len: int = 5000) -> str:
    import json
    s = json.dumps(obj, default=str, indent=2)
    if len(s) > max_len:
        s = s[:max_len] + "\n... [truncated]"
    return s
