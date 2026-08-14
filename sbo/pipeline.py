"""Pipeline orchestrator.

Wires together: Beeswax client → run folder → state store → multiplier
engine → Sheets writes. Each phase is a top-level function that takes the
shared `RunContext` and is independently runnable from the Streamlit UI
or `python -m sbo.pipeline <phase>`.

Phases (matching the Apps Script 3-run-per-day schedule):
    full        — ATR rebuild + full report + pull modifiers + decide + push
    pacing_only — recalc pacing + decide + push (no ATR rebuild)
    pushonly    — push pre-calculated multipliers (after AM review)
    phase1      — create bid modifiers for new lines
    phase2      — patch line items with their new modifier IDs
    phase3      — sync modifier terms with current targeted deals
"""

from __future__ import annotations

from dotenv import load_dotenv

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Literal, Tuple

import pandas as pd
from dotenv import load_dotenv

from sbo.beeswax_client import BeeswaxClient, BeeswaxConfig
from sbo.bid_optimizer import pull_bid_modifiers
from sbo.config_models import EngineConfig, load_config
from sbo.full_report import build_full_report
from sbo.full_report_mp_ctv import build_full_report_mp_ctv
from sbo.full_report_select_ctv import build_full_report_select_ctv
from sbo.multiplier_engine import decide_multipliers
from sbo.multiplier_engine_mp_ctv import decide_multipliers_mp_ctv
from sbo.multiplier_engine_select_ctv import decide_multipliers_select_ctv
from sbo.pacing import calculate_pacing_from_bw
from sbo.pacing_select_ctv import calculate_pacing_select_ctv
from sbo.phases import (
    create_publisher_bid_modifiers,
    patch_line_item_bid_modifiers,
    update_bid_modifier_terms,
)
from sbo.phases_select_ctv import (
    create_publisher_bid_modifiers_select_ctv,
    update_bid_modifier_terms_select_ctv,
)
from sbo.price_kill_staging import build_price_kill_staging
from sbo.push import push_multipliers_to_beeswax
from sbo.run_storage import RunFolder
from sbo.sheets_io import SheetsIO
from sbo.state import StateStore
from sbo.state_apply import apply_engine_state, prune_state_at_run_start

Phase = Literal["full", "pacing_only", "pushonly", "phase1", "phase2", "phase3"]

DRIVE_SNAPSHOT_FOLDER_ID = "1vJLvhr5vdyyYtVqtV2G0RcY4ydDXxp4b"


@dataclass
class RunContext:
    cfg: EngineConfig
    sheets: SheetsIO
    state: StateStore
    bw: BeeswaxClient
    run: RunFolder


def build_context(
    config_path: str | Path,
    sheet_id: str,
    phase: Phase,
    runs_dir: Path | None = None,
    state_dir: Path | None = None,
) -> RunContext:
    """One-stop initializer. Loads config, opens sheet, creates run folder.

    Checks the local checkout against GitHub before doing anything else
    (see sbo/git_guard.py) — every invocation path (CLI, cron scripts,
    Streamlit) goes through this function, so the check runs exactly once,
    everywhere, without each entry point needing to remember to call it.

    The caller is responsible for `ctx.bw.close()` when done — easiest is
    `with closing(build_context(...)) as ctx:`. The Streamlit launcher in
    `app.py` handles this via try/finally.
    """
    from sbo.git_guard import check_git_version
    check_git_version()

    load_dotenv()
    cfg = load_config(config_path)
    runs_dir = runs_dir or Path(os.environ.get("RUNS_DIR", "runs"))
    state_dir = state_dir or Path(os.environ.get("STATE_DIR", "state")) / cfg.tactic

    sheets = SheetsIO(sheet_id, cfg.sheet_tabs.model_dump())
    state = StateStore(state_dir)
    load_dotenv()
    bw = BeeswaxClient(BeeswaxConfig.from_env())
    bw.__enter__()  # acquire underlying httpx.Client; bw.close() releases
    run = RunFolder(runs_dir, tactic=cfg.tactic, phase=phase)
    return RunContext(cfg=cfg, sheets=sheets, state=state, bw=bw, run=run)


def close_context(ctx: RunContext) -> None:
    """Tear down resources. Always call this when a run is done."""
    try:
        ctx.bw.close()
    except Exception:
        pass


# ── phase entry points ────────────────────────────────────────────────────


def _build_sub_tactic_shares_from_stats(
    publisher_stats: pd.DataFrame,
) -> Dict[Tuple[str, str], float]:
    """Pre-compute per-LI sub-tactic impression shares from publisher_stats.

    Result: {(bw_id, sub_tactic): share_fraction}
    e.g. {("12345", "streaming"): 0.75, ("12345", "podcast"): 0.25}

    Total Audio only — called when cfg.sub_tactic_share is configured.
    """
    from sbo.multiplier_engine import _parse_sub_tactic
    out: Dict[Tuple[str, str], float] = {}
    if publisher_stats.empty:
        return out
    for bw_id, group in publisher_stats.groupby("Line_Item_ID"):
        bw_id_str = str(bw_id)
        tactic_imps: Dict[str, float] = {}
        for _, row in group.iterrows():
            alt_id = str(row.get("Deal_Alternative_ID", "") or "")
            sub_tactic = _parse_sub_tactic(alt_id)
            if not sub_tactic:
                continue
            imps = float(row.get("Impressions", 0) or 0)
            tactic_imps[sub_tactic] = tactic_imps.get(sub_tactic, 0.0) + imps
        total = sum(tactic_imps.values())
        if total > 0:
            for sub_tactic, imps in tactic_imps.items():
                out[(bw_id_str, sub_tactic)] = round(imps / total, 4)
    return out


def run_full(ctx: RunContext) -> None:
    """Full daily run.

    Mirrors the Apps Script 3 AM cadence:
        1. ATR rebuild → 02_atr.parquet
        2. Build full report (Publisher Stats equivalent) → 03_publisher_stats.parquet
        3. Pull bid modifiers → join with Publisher Stats → 04_bid_optimizer.parquet
        4. Calculate pacing from BW (or Redshift) → join into Bid Optimizer
        5. Decide multipliers → 05_decisions.parquet
        6. Push to Beeswax → 06_push_results.parquet
        7. Write Bid Optimizer + Run Log to Sheet
    """
    ctx.run.log(f"=== Full run start (tactic={ctx.cfg.tactic}) ===")

    # Clear the Bid Optimizer tab immediately to free cell budget before any
    # heavy work or log writes happen — mirrors the Apps Script behavior.
    try:
        ctx.sheets.clear_tab("bid_optimizer")
        ctx.run.log("Bid Optimizer tab cleared at run start.")
    except Exception as e:
        ctx.run.log(f"Bid Optimizer clear skipped: {e}")

    # Snapshot the input tab so build_full_report has the BW LI list
    bw_settings = ctx.sheets.read_tab("beeswax_line_item_settings")
    ctx.run.save_input("beeswax_line_item_settings", bw_settings)
    ctx.run.log(f"Snapshotted Beeswax Line Item Settings: {len(bw_settings):,} rows")

    # Phase 1: build publisher stats (ATR + LI/TE + reports + modifier deals)
    if ctx.cfg.is_mp_ctv:
        artifacts = build_full_report_mp_ctv(bw=ctx.bw, cfg=ctx.cfg, run=ctx.run, state=ctx.state)
    elif ctx.cfg.is_select_ctv:
        artifacts = build_full_report_select_ctv(bw=ctx.bw, cfg=ctx.cfg, run=ctx.run, state=ctx.state)
    else:
        artifacts = build_full_report(bw=ctx.bw, cfg=ctx.cfg, run=ctx.run, state=ctx.state)
    ctx.run.log(
        f"Publisher stats ready: {len(artifacts.publisher_stats):,} rows"
    )

    # Phase 2: build the Bid Optimizer DataFrame (one row per LI × deal term)
    bid_optimizer = pull_bid_modifiers(
        bw=ctx.bw,
        cfg=ctx.cfg,
        run=ctx.run,
        state=ctx.state,
        publisher_stats=artifacts.publisher_stats,
        li_settings=artifacts.li_settings,
        last3_cpm=artifacts.last3_cpm,
        input_snapshot=bw_settings,
        deal_to_mod_type=artifacts.deal_to_mod_type,
        deal_to_sub_tactic=getattr(artifacts, "deal_to_sub_tactic", None),
    )
    ctx.run.save_dataframe("04_bid_optimizer", bid_optimizer)
    ctx.run.log(
        f"Bid Optimizer ready: {len(bid_optimizer):,} rows × {len(bid_optimizer.columns)} cols"
    )

    # Phase 3: pacing (Select CTV reads "Beeswax Select CTV" + a different
    # formula with no today-subtraction; everyone else reads SF Data Import)
    if ctx.cfg.is_select_ctv:
        beeswax_select_ctv = ctx.sheets.read_tab("beeswax_select_ctv", header_row=2)
        ctx.run.save_input("beeswax_select_ctv", beeswax_select_ctv)
        bid_optimizer, up_pacing_summary = calculate_pacing_select_ctv(
            bw=ctx.bw,
            cfg=ctx.cfg,
            run=ctx.run,
            bid_optimizer=bid_optimizer,
            input_snapshot=bw_settings,
            beeswax_select_ctv=beeswax_select_ctv,
        )
        if not up_pacing_summary.empty:
            try:
                ctx.sheets.write_tab("up_pacing", up_pacing_summary)
                ctx.run.log("UP Pacing summary written to sheet (AM review/comparison).")
            except Exception as e:
                ctx.run.log(f"UP Pacing summary write skipped: {e}")
    else:
        sf_data = ctx.sheets.read_tab("sf_data_import", header_row=2)
        ctx.run.save_input("sf_data_import", sf_data)
        bid_optimizer = calculate_pacing_from_bw(
            bw=ctx.bw,
            cfg=ctx.cfg,
            run=ctx.run,
            bid_optimizer=bid_optimizer,
            input_snapshot=bw_settings,
            sf_data_import=sf_data,
        )
    # Overwrite the 04_bid_optimizer parquet with pacing filled in
    ctx.run.save_dataframe("04_bid_optimizer", bid_optimizer)

    # Round Pacing_Pct to 2 decimal places: 0.9950 → 1.00 (on-pace).
    # Matches AppScript — affects engine decisions and pacing history signals.
    if "Pacing_Pct" in bid_optimizer.columns:
        bid_optimizer["Pacing_Pct"] = (
            pd.to_numeric(bid_optimizer["Pacing_Pct"], errors="coerce").round(2)
        )

    # Phase 4: decide multipliers (the engine — pure logic)
    from datetime import date as date_cls, datetime as datetime_cls
    now = datetime_cls.now()

    # Prune ended-campaign entries from run logs BEFORE loading them
    pruned = prune_state_at_run_start(ctx.state, now)
    if pruned["first_pruned"] or pruned["second_pruned"]:
        ctx.run.log(
            f"Pruned expired run-log entries: "
            f"first={pruned['first_pruned']}, second={pruned['second_pruned']}"
        )

    # Pull state inputs for the engine
    first_log = ctx.state.load("first_run_log")
    second_log = ctx.state.load("second_run_log")
    first_run_seen = set(first_log["BW_Line_Item_ID"].astype(str)) if not first_log.empty else set()
    second_run_seen = set(second_log["BW_Line_Item_ID"].astype(str)) if not second_log.empty else set()

    paused_log = ctx.state.load("paused_log")

    # Zero delivery from the run's last_day_imps snapshot
    # Built BEFORE paused_active so we can cross-validate.
    zero_delivery: set = set()
    last1_path = ctx.run.path / "last_day_imps.parquet"
    if last1_path.exists():
        l1 = pd.read_parquet(last1_path)
        zero_delivery = set(
            l1[l1["Had_Impressions_Yesterday"] == "N"]["BW_Line_Item_ID"].astype(str).tolist()
        )
        ctx.run.log(
            f"Zero delivery: {len(zero_delivery):,} LIs had no impressions yesterday."
        )
    else:
        ctx.run.log("last_day_imps.parquet not found — zero_delivery set is empty.")

    # paused_active maps bw_id → DataFrame index (NOT a 1-based row number).
    # GUARD: only treat a line as active-paused if it ALSO has zero delivery
    # confirmed by last_day_imps. This prevents corrupted paused_log state
    # (e.g. from failed prior runs) from incorrectly pausing delivering lines.
    paused_active: dict = {}
    if not paused_log.empty:
        active_mask = paused_log["Resumed_Date"].isna() | (paused_log["Resumed_Date"] == "")
        for idx in paused_log.index[active_mask]:
            bw_id = str(paused_log.at[idx, "BW_Line_Item_ID"]).strip()
            if bw_id in zero_delivery:
                paused_active[bw_id] = idx
    ctx.run.log(
        f"Paused active: {len(paused_active):,} LIs confirmed paused "
        f"(paused_log entries × zero delivery cross-check)."
    )

    pacing_history = ctx.state.load_pacing_history(max_runs=4)

    # Paused multiplier snapshot: deal-level held multipliers to restore on resume
    paused_snapshot = ctx.state.load("paused_snapshot")
    paused_snapshot_map: dict = {}
    if not paused_snapshot.empty:
        for _, snap_row in paused_snapshot.iterrows():
            key = (str(snap_row["BW_Line_Item_ID"]).strip(), str(snap_row["Deal_ID"]).strip())
            paused_snapshot_map[key] = float(snap_row["Held_Multiplier"])

    if ctx.cfg.is_mp_ctv:
        price_kill_df = build_price_kill_staging(
            optimizer_df=bid_optimizer,
            cfg=ctx.cfg,
            pacing_history=pacing_history,
            paused_active=set(paused_active.keys()),
        )
        ctx.run.log(f"Price kill staging: {len(price_kill_df):,} action rows")
        engine_result = decide_multipliers_mp_ctv(
            optimizer_df=bid_optimizer,
            cfg=ctx.cfg,
            pacing_history=pacing_history,
            first_run_seen=first_run_seen,
            second_run_seen=second_run_seen,
            paused_active=paused_active,
            zero_delivery=zero_delivery,
            run_date=now,
            paused_snapshot_map=paused_snapshot_map,
            price_kill_actions=price_kill_df,
        )
    elif ctx.cfg.is_select_ctv:
        engine_result = decide_multipliers_select_ctv(
            optimizer_df=bid_optimizer,
            cfg=ctx.cfg,
            pacing_history=pacing_history,
            first_run_seen=first_run_seen,
            second_run_seen=second_run_seen,
            paused_active=paused_active,
            zero_delivery=zero_delivery,
            run_date=now,
            paused_snapshot_map=paused_snapshot_map,
        )
    else:
        # Pre-compute sub-tactic impression shares (Total Audio only)
        sub_tactic_shares = (
            _build_sub_tactic_shares_from_stats(artifacts.publisher_stats)
            if ctx.cfg.sub_tactic_share else {}
        )
        engine_result = decide_multipliers(
            optimizer_df=bid_optimizer,
            cfg=ctx.cfg,
            pacing_history=pacing_history,
            first_run_seen=first_run_seen,
            second_run_seen=second_run_seen,
            paused_active=paused_active,
            zero_delivery=zero_delivery,
            run_date=now,
            paused_snapshot_map=paused_snapshot_map,
            sub_tactic_shares=sub_tactic_shares,
        )
    ctx.run.log(f"Engine ran: {len(engine_result.decisions):,} decisions")
    ctx.run.log(
        f"  new Day1: {len(engine_result.new_first_run)}, "
        f"new Day2: {len(engine_result.new_second_run)}, "
        f"new pauses: {len(engine_result.new_pauses)}, "
        f"resumed: {len(engine_result.resumed_row_indices)}, "
        f"pre-flight resets: {len(engine_result.pre_flight_resets)}, "
        f"kill log: {len(engine_result.kill_log_entries)}"
    )

    # Persist all engine side-effects to state — without this, every run treats
    # every line as Day 1 forever and pacing history never accumulates.
    state_counts = apply_engine_state(ctx.state, engine_result, now)
    ctx.run.log(f"State persisted: {state_counts}")

    # Apply decisions back to the Bid Optimizer DataFrame (cols AA–AD)
    decisions_df = pd.DataFrame([
        {
            "BW_Line_Item_ID": d.bw_id,
            "Deal_ID": d.deal_id,
            "Calculated_New_Multiplier": d.new_multiplier,
            "Effective_Bid_Current": d.effective_bid_current,
            "Effective_Bid_New": d.effective_bid_new,
            "Decision_Reason": (
                d.reason_text if d.reason_text.startswith(str(d.reason_code))
                else f"{d.reason_code} — {d.reason_text}"
            ),
        }
        for d in engine_result.decisions
    ])
    ctx.run.save_dataframe("05_decisions", decisions_df)

    # Merge decisions back into bid_optimizer
    bid_optimizer = bid_optimizer.drop(
        columns=["Calculated_New_Multiplier", "Effective_Bid_Current", "Effective_Bid_New", "Decision_Reason"],
        errors="ignore",
    ).merge(decisions_df, on=["BW_Line_Item_ID", "Deal_ID"], how="left")

    # Re-enforce schema column order after merge (merge appends joined cols to end)
    from sbo.bid_optimizer import (
        BID_OPTIMIZER_COLUMNS_MP_CTV, BID_OPTIMIZER_COLUMNS, BID_OPTIMIZER_COLUMNS_SELECT_CTV,
    )
    if ctx.cfg.is_mp_ctv:
        col_schema = BID_OPTIMIZER_COLUMNS_MP_CTV
    elif ctx.cfg.is_select_ctv:
        col_schema = BID_OPTIMIZER_COLUMNS_SELECT_CTV
    else:
        col_schema = BID_OPTIMIZER_COLUMNS
    schema_present = [c for c in col_schema if c in bid_optimizer.columns]
    extra_cols     = [c for c in bid_optimizer.columns if c not in col_schema]
    bid_optimizer  = bid_optimizer[schema_present + extra_cols]

    ctx.run.save_dataframe("04_bid_optimizer", bid_optimizer)

    # Phase 5: write Bid Optimizer to the AM-facing sheet for AM review
    # Strip internal columns (prefixed _) before writing to sheet
    sheet_df = bid_optimizer.loc[:, [c for c in bid_optimizer.columns if not c.startswith("_")]]
    ctx.sheets.write_tab("bid_optimizer", sheet_df)
    ctx.run.log(
        f"Wrote Bid Optimizer to sheet ({len(bid_optimizer):,} rows). "
        f"AM review cols AA–AD before push."
    )

    # Phase 6: write all state logs back to sheet so they stay in sync
    # First run log — pruned + updated
    ctx.sheets.write_tab("first_run_log", ctx.state.load("first_run_log"))
    # Second run log — pruned + updated
    ctx.sheets.write_tab("second_run_log", ctx.state.load("second_run_log"))
    # Paused log / snapshot — skipped for MP CTV until pause logic is
    # fully validated. Parquets remain the source of truth in the meantime.
    if not ctx.cfg.is_mp_ctv:
        ctx.sheets.write_tab("paused_log", ctx.state.load("paused_log"))
        ctx.sheets.write_tab("paused_snapshot", ctx.state.load("paused_snapshot"))
    # Kill log — append-only event log (Podcast/Streaming/Total Audio only)
    # MP CTV price-kill staging lives in run parquets, not a sheet tab
    if not ctx.cfg.is_mp_ctv:
        kill_log_full = ctx.state.load("kill_log")
        ctx.sheets.write_tab("kill_log", kill_log_full.tail(2_000))
    # Pacing history — wide-format OVER/UNDER signals
    ctx.sheets.write_tab("pacing_history", ctx.state.load_pacing_history(max_runs=100))

    # MP CTV only: write Deal CPM History and Publisher CPM History tabs
    if ctx.cfg.is_mp_ctv and ctx.cfg.sheet_tabs.deal_cpm_history:
        try:
            ctx.sheets.write_tab("deal_cpm_history", ctx.state.load("deal_cpm_history"))
            ctx.run.log("Deal CPM History written to sheet.")
        except Exception as e:
            ctx.run.log(f"Deal CPM History write skipped: {e}")
    if ctx.cfg.is_mp_ctv and ctx.cfg.sheet_tabs.publisher_cpm_history:
        try:
            ctx.sheets.write_tab("publisher_cpm_history", ctx.state.load("publisher_cpm_history"))
            ctx.run.log("Publisher CPM History written to sheet.")
        except Exception as e:
            ctx.run.log(f"Publisher CPM History write skipped: {e}")

    ctx.run.log("State logs written back to sheet.")

    ctx.run.mark_complete(
        "success",
        summary={
            "decisions": len(engine_result.decisions),
            "new_pauses": len(engine_result.new_pauses),
            "new_first_run": len(engine_result.new_first_run),
            "new_second_run": len(engine_result.new_second_run),
        },
    )
    ctx.run.log(
        "run_full ready for AM review. Click Push in Streamlit to invoke "
        "run_pushonly on this run folder."
    )


def run_pacing_only(ctx: RunContext) -> None:
    """Recalculate pacing without an ATR rebuild. Cheaper daily option."""
    raise NotImplementedError


def run_pushonly(ctx: RunContext, dry_run: bool = False) -> None:
    """Push the multipliers from the latest run's Bid Optimizer to Beeswax.

    Used after AM reviews cols AA–AD and clicks Push in Streamlit.
    Reads the Bid Optimizer from the most recent run folder, NOT the sheet
    (the sheet may have been modified since the engine ran).
    """
    ctx.run.log(f"=== run_pushonly start (dry_run={dry_run}) ===")
    src = _latest_run_artifact(
        ctx, "04_bid_optimizer.parquet", phase_filter="full"
    )
    if src is None:
        raise FileNotFoundError(
            "No prior successful 'full' run found with 04_bid_optimizer.parquet. "
            "Run a Full Run first and let it complete."
        )
    ctx.run.log(f"Pulling Bid Optimizer from prior run: {src.parent.name}")
    bid_optimizer = pd.read_parquet(src)
    ctx.run.save_dataframe("04_bid_optimizer", bid_optimizer)

    push_results, summary = push_multipliers_to_beeswax(
        bw=ctx.bw, cfg=ctx.cfg, run=ctx.run,
        bid_optimizer=bid_optimizer, dry_run=dry_run,
    )
    ctx.run.log(
        f"Push summary — updated:{summary.updated} skipped:{summary.skipped} "
        f"errors:{summary.errors} | modifiers touched:{summary.modifiers_touched} "
        f"failed:{summary.modifiers_failed}"
    )

    # Write Update_Status back to Bid Optimizer sheet tab
    if not push_results.empty and "Update_Status" in bid_optimizer.columns:
        status_map = dict(zip(
            zip(push_results["BW_Line_Item_ID"].astype(str),
                push_results["Deal_ID"].astype(str)),
            push_results["Status"],
        ))
        bid_optimizer = bid_optimizer.copy()
        bid_optimizer["Update_Status"] = bid_optimizer.apply(
            lambda r: status_map.get(
                (str(r["BW_Line_Item_ID"]), str(r["Deal_ID"])), ""
            ), axis=1,
        )
        # Write only the Update_Status column back — update col AC in sheet
        try:
            sheet_df = bid_optimizer.loc[
                :, [c for c in bid_optimizer.columns if not c.startswith("_")]
            ]
            ctx.sheets.write_tab("bid_optimizer", sheet_df)
            ctx.run.log("Update_Status written back to Bid Optimizer tab.")
        except Exception as e:
            ctx.run.log(f"Update_Status write-back skipped: {e}")

    ctx.run.mark_complete(
        "success" if summary.errors == 0 else "partial",
        summary={
            "updated": summary.updated, "skipped": summary.skipped,
            "errors": summary.errors,
            "modifiers_touched": summary.modifiers_touched,
            "modifiers_failed": summary.modifiers_failed,
            "dry_run": dry_run,
        },
    )

    # Save Drive snapshot after push — non-fatal if it fails. Podcast/Streaming/
    # Total Audio only: MP CTV's operational flow (unattended droplet cron,
    # narrower Drive OAuth scope) stays exactly as it is in SBO_Droplet_Pull —
    # no new side effects added to that path.
    if not ctx.cfg.is_mp_ctv:
        _save_drive_snapshot(ctx, bid_optimizer)


def _save_drive_snapshot(ctx: RunContext, bid_optimizer: pd.DataFrame) -> None:
    """Create a dated Google Sheet snapshot in the Beeswax Reports Drive folder.

    Non-fatal — logs a warning and returns if anything fails so push is unaffected.
    File name format: "YYYY-MM-DD - Podcast Smart Bid Optimizer Run"
    """
    from datetime import date
    try:
        from sbo.sheets_io import get_authorized_client
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        tactic_label = ctx.cfg.tactic.replace("_", " ").title()
        today = date.today().strftime("%Y-%m-%d")
        file_name = f"{today} - {tactic_label} Smart Bid Optimizer Run"

        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_authorized_user_file("credentials/token.json", scopes=scopes)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        client = get_authorized_client()

        # Create new spreadsheet
        new_ss = client.create(file_name)

        # Move it to the Beeswax Reports folder via Drive API
        import httpx
        token = creds.token
        file_id = new_ss.id
        httpx.patch(
            f"https://www.googleapis.com/drive/v3/files/{file_id}",
            headers={"Authorization": f"Bearer {token}"},
            params={"addParents": DRIVE_SNAPSHOT_FOLDER_ID, "removeParents": "root"},
        )

        # Write Bid Optimizer data to first sheet
        ws = new_ss.sheet1
        ws.update_title("Bid Optimizer Snapshot")
        import math
        def _clean_val(v):

            if isinstance(v, float) and math.isnan(v):
                return ""
            return str(v) if v is not None else ""
        rows = [bid_optimizer.columns.tolist()] + [
            [_clean_val(v) for v in row]
            for row in bid_optimizer.values.tolist()
        ]

        # Expand sheet to fit all rows before writing
        total_rows = len(rows)
        if total_rows > 1000:
            ws.add_rows(total_rows - 1000)

        # Write in chunks of 10,000 rows to avoid API limits
        chunk_size = 10_000
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            start_row = i + 1
            ws.update(range_name=f"A{start_row}", values=chunk)

        ctx.run.log(f"Drive snapshot saved: '{file_name}' (id={file_id})")
    except Exception as e:
        ctx.run.log(f"WARNING: Drive snapshot failed (non-fatal): {e}")


def _refresh_bw_line_item_settings(ctx: RunContext) -> pd.DataFrame:
    """Clear Beeswax Line Item Settings row 2+ and repopulate Col A & B
    from Line Item Import (col E = SF LI ID, col I = BW LI ID, where col K = 'Include').
    Returns the freshly written DataFrame.
    """
    # Read Line Item Import directly by tab name (not in logical tab map)
    ws = ctx.sheets._book.worksheet("Line Item Import")
    all_values = ws.get_all_values()

    if len(all_values) < 2:
        ctx.run.log("WARNING: Line Item Import tab is empty — skipping refresh")
        return ctx.sheets.read_tab("beeswax_line_item_settings")

    headers = all_values[0]
    rows = all_values[1:]

    # Col E = index 4 (SF LI ID), Col I = index 8 (BW LI ID), Col K = index 10 (Include filter)
    sf_idx, bw_idx, filter_idx = 4, 8, 10

    included = [
        r for r in rows
        if len(r) > filter_idx and r[filter_idx].strip().lower() == "include"
        and len(r) > bw_idx and r[bw_idx].strip()
    ]

    ctx.run.log(f"Line Item Import: {len(included):,} 'Include' rows found")

    # Read existing sheet to preserve headers and all columns
    existing = ctx.sheets.read_tab("beeswax_line_item_settings")
    if existing.empty:
        ctx.run.log("WARNING: Beeswax Line Item Settings has no headers — cannot refresh")
        return existing

    # Build fresh DataFrame — Col A & B populated, everything else blank
    sf_col = next((c for c in existing.columns if "sf" in c.lower() and "li" in c.lower()), existing.columns[0])
    bw_col = next((c for c in existing.columns if "bw" in c.lower() and "li" in c.lower()), existing.columns[1])

    fresh_rows = []
    for r in included:
        row = {c: "" for c in existing.columns}
        row[sf_col] = r[sf_idx].strip() if len(r) > sf_idx else ""
        row[bw_col] = r[bw_idx].strip() if len(r) > bw_idx else ""
        fresh_rows.append(row)

    fresh_df = pd.DataFrame(fresh_rows, columns=existing.columns)
    ctx.sheets.write_tab("beeswax_line_item_settings", fresh_df)
    ctx.run.log(f"Beeswax Line Item Settings refreshed: {len(fresh_df):,} rows written")
    return fresh_df


def _merge_phase_results_safely(
    ctx: RunContext,
    snapshot: pd.DataFrame,
    updated: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """Merge a phase's results back onto the LIVE sheet without ever
    touching columns A/B (SF LI ID / BW LI ID) and without assuming BW LI ID
    is unique in the sheet.

    Phases 1/2/3 all run `_normalize_input` -> process -> `_denormalize_input`
    on the exact snapshot they were handed, and never add, drop, or reorder
    rows. So `updated` has the same row count and order as `snapshot`, and
    the correct way to match a result back to a sheet row is ORIGINAL ROW
    POSITION — not BW LI ID, which can repeat. (Duplicate BW LI IDs are what
    caused the 'BW LI ID\\n...\\nName: ..., dtype: str' corruption: a pandas
    Series getting written into a cell whenever `.set_index(bw_col)` hit a
    duplicate key.)

    Safeguard: a row position is only trusted if the BW LI ID sitting there
    in the freshly re-read live sheet still matches the BW LI ID that was
    there when the snapshot was taken. If the sheet's shape or row order
    changed mid-run, that row is left completely untouched and logged as a
    warning instead of guessed at.

    Returns (fresh_sheet_with_result_cols_updated, result_cols, warnings).
    result_cols never includes A/B, so writing only result_cols back via
    write_columns() can never touch them — including any live formula there.
    """
    fresh = ctx.sheets.read_tab("beeswax_line_item_settings")
    bw_col = next(c for c in fresh.columns if "bw" in c.lower() and "li" in c.lower())
    sf_col = next(c for c in fresh.columns if "sf" in c.lower() and "li" in c.lower())
    result_cols = [c for c in fresh.columns if c not in (sf_col, bw_col)]

    warnings: list[str] = []
    n = min(len(fresh), len(snapshot), len(updated))
    if len(fresh) != len(snapshot) or len(fresh) != len(updated):
        warnings.append(
            f"Row count changed during phase run (snapshot={len(snapshot)}, "
            f"live={len(fresh)}, results={len(updated)}). Only the first "
            f"{n} rows were checked; write-back stopped there for safety."
        )

    out = fresh.copy()
    skipped = 0
    for i in range(n):
        snap_bw = str(snapshot.iloc[i][bw_col]).strip()
        live_bw = str(fresh.iloc[i][bw_col]).strip()
        if not snap_bw or snap_bw != live_bw:
            skipped += 1
            warnings.append(
                f"Row {i + 2}: skipped write-back — live BW LI ID ({live_bw!r}) "
                f"no longer matches what was there when the phase started "
                f"({snap_bw!r})."
            )
            continue
        for col in result_cols:
            if col in updated.columns:
                out.iat[i, out.columns.get_loc(col)] = updated.iloc[i][col]

    if skipped:
        warnings.append(f"{skipped} row(s) skipped out of {n} checked — see above.")

    return out, result_cols, warnings


def run_phase1(ctx: RunContext) -> None:
    """Create bid modifiers for lines without one (Section 10)."""
    # Total Audio / Select CTV: skip Line Item Import refresh — BW LI IDs are
    # pasted directly into Beeswax Line Item Settings. Podcast/Streaming/MP CTV
    # use Line Item Import.
    if ctx.cfg.tactic in ("total_audio", "select_ctv"):
        bw_settings = ctx.sheets.read_tab("beeswax_line_item_settings")
    else:
        bw_settings = _refresh_bw_line_item_settings(ctx)
    ctx.run.save_input("beeswax_line_item_settings", bw_settings)

    # Pricing lookup from latest run's publisher_stats (if available)
    publisher_stats = _latest_publisher_stats(ctx)

    if ctx.cfg.is_select_ctv:
        # Select CTV: flat 1.00x starting multiplier, no immediate patch —
        # see phases_select_ctv.py module docstring.
        updated, results, summary = create_publisher_bid_modifiers_select_ctv(
            bw=ctx.bw, cfg=ctx.cfg, run=ctx.run, state=ctx.state,
            input_snapshot=bw_settings,
        )
    else:
        updated, results, summary = create_publisher_bid_modifiers(
            bw=ctx.bw, cfg=ctx.cfg, run=ctx.run, state=ctx.state,
            input_snapshot=bw_settings, publisher_stats=publisher_stats,
        )
    # Merge by row position with a live BW-LI-ID safeguard, then write ONLY
    # the result columns (C:H etc.) — columns A/B (and any formula there)
    # are never touched.
    final, result_cols, warnings = _merge_phase_results_safely(ctx, bw_settings, updated)
    for w in warnings:
        ctx.run.log(f"WARNING (write-back): {w}")
    ctx.sheets.write_columns("beeswax_line_item_settings", final, columns=result_cols)
    ctx.run.mark_complete(
        "success" if summary.errors == 0 else "partial",
        summary={
            "created": summary.created, "skipped": summary.skipped,
            "errors": summary.errors,
        },
    )


def run_phase2(ctx: RunContext) -> None:
    """Patch line items with their newly created modifier IDs (Section 11)."""
    bw_settings = ctx.sheets.read_tab("beeswax_line_item_settings")
    ctx.run.save_input("beeswax_line_item_settings", bw_settings)

    updated, results, summary = patch_line_item_bid_modifiers(
        bw=ctx.bw, cfg=ctx.cfg, run=ctx.run, input_snapshot=bw_settings,
    )
    final, result_cols, warnings = _merge_phase_results_safely(ctx, bw_settings, updated)
    for w in warnings:
        ctx.run.log(f"WARNING (write-back): {w}")
    ctx.sheets.write_columns("beeswax_line_item_settings", final, columns=result_cols)
    ctx.run.mark_complete(
        "success" if summary.errors == 0 else "partial",
        summary={
            "patched": summary.patched, "skipped": summary.skipped,
            "errors": summary.errors,
        },
    )


def run_phase3(ctx: RunContext, new_only: bool = False) -> None:
    """Sync modifier terms with current targeted deals (Section 20)."""
    bw_settings = ctx.sheets.read_tab("beeswax_line_item_settings")
    ctx.run.save_input("beeswax_line_item_settings", bw_settings)
    publisher_stats = _latest_publisher_stats(ctx)

    if ctx.cfg.is_select_ctv:
        # Select CTV: new terms priced off floor x 1.20 (not smart_starting_mult's
        # 1.3) with a literal $45 fallback — see phases_select_ctv.py.
        updated, results, summary = update_bid_modifier_terms_select_ctv(
            bw=ctx.bw, cfg=ctx.cfg, run=ctx.run, state=ctx.state,
            input_snapshot=bw_settings, publisher_stats=publisher_stats,
            new_only=new_only,
        )
    else:
        updated, results, summary = update_bid_modifier_terms(
            bw=ctx.bw, cfg=ctx.cfg, run=ctx.run, state=ctx.state,
            input_snapshot=bw_settings, publisher_stats=publisher_stats,
            new_only=new_only,
        )
    final, result_cols, warnings = _merge_phase_results_safely(ctx, bw_settings, updated)
    for w in warnings:
        ctx.run.log(f"WARNING (write-back): {w}")
    ctx.sheets.write_columns("beeswax_line_item_settings", final, columns=result_cols)
    ctx.run.mark_complete(
        "success" if summary.errors == 0 else "partial",
        summary={
            "updated": summary.updated, "skipped": summary.skipped,
            "errors": summary.errors, "new_only": new_only,
        },
    )


def _latest_publisher_stats(ctx: RunContext):
    """Find publisher_stats from the most recent successful run for this tactic.

    For MP CTV: if no prior publisher_stats parquet exists, fall back to the
    deal_cpm_history state parquet so Phase 1/3 can price new terms from
    historical clearing CPM rather than falling all the way back to floor × 1.3.
    """
    artifact = _latest_run_artifact(ctx, "03_publisher_stats.parquet", phase_filter="full")
    if artifact is not None:
        ctx.run.log(f"Using publisher_stats from {artifact.parent.name}")
        ps = pd.read_parquet(artifact)
        # For MP CTV: backfill any deals with no global CPM from deal_cpm_history
        if ctx.cfg.is_mp_ctv:
            ps = _backfill_global_cpm_from_history(ps, ctx)
        return ps

    # No prior publisher_stats at all
    if ctx.cfg.is_mp_ctv:
        # Build a synthetic publisher_stats stub from deal_cpm_history so
        # Phase 1/3 can use historical clearing CPM for new term pricing
        stub = _synthetic_publisher_stats_from_history(ctx)
        if stub is not None and not stub.empty:
            ctx.run.log(
                f"No prior publisher_stats — using deal_cpm_history for pricing "
                f"({len(stub):,} deals)."
            )
            return stub

    ctx.run.log("No prior publisher_stats — Phase will use fallback pricing.")
    return None


def _backfill_global_cpm_from_history(
    ps: pd.DataFrame, ctx: RunContext
) -> pd.DataFrame:
    """For any deal in publisher_stats with no global CPM, fill from history."""
    try:
        hist = ctx.state.load("deal_cpm_history")
    except Exception:
        return ps
    if hist.empty or "Deal_ID" not in hist.columns:
        return ps
    hist_map = dict(zip(
        hist["Deal_ID"].astype(str).str.strip(),
        pd.to_numeric(hist["Global_Clearing_CPM"], errors="coerce").fillna(0),
    ))
    if "Deal_Global_Clearing_CPM" not in ps.columns:
        return ps
    mask = pd.to_numeric(ps["Deal_Global_Clearing_CPM"], errors="coerce").fillna(0) <= 0
    ps = ps.copy()
    ps.loc[mask, "Deal_Global_Clearing_CPM"] = (
        ps.loc[mask, "Deal_ID"].astype(str).str.strip().map(hist_map).fillna(0)
    )
    return ps


def _synthetic_publisher_stats_from_history(
    ctx: RunContext,
) -> pd.DataFrame | None:
    """Build a minimal publisher_stats stub from deal_cpm_history for Phase 1/3 pricing."""
    try:
        hist = ctx.state.load("deal_cpm_history")
    except Exception:
        return None
    if hist.empty or "Deal_ID" not in hist.columns:
        return None
    # Minimal columns that build_pricing_lookup reads
    stub = pd.DataFrame({
        "Deal_ID":                   hist["Deal_ID"].astype(str).str.strip(),
        "Deal_Global_Clearing_CPM":  pd.to_numeric(hist["Global_Clearing_CPM"], errors="coerce").fillna(0),
        "Floor_Price":               pd.to_numeric(hist.get("Floor_Price", 0), errors="coerce").fillna(0),
        "Line_Item_ID":              "",
        "CPM_Bid":                   0.0,
    })
    return stub[stub["Deal_Global_Clearing_CPM"] > 0]


def _latest_run_artifact(
    ctx: RunContext, filename: str, phase_filter: str | None = None
) -> Path | None:
    """Find the most recent successful run folder for this tactic that
    contains `filename`. Optionally restrict by phase name.

    Validates run_metadata.json status — only 'success'/'partial' qualify
    (so a half-finished run that wrote 04_bid_optimizer but errored before
    completing isn't picked up by run_pushonly).
    """
    runs_dir = ctx.run.path.parent
    if not runs_dir.exists():
        return None
    candidates = []
    for p in runs_dir.iterdir():
        if not p.is_dir() or p == ctx.run.path:
            continue
        if ctx.cfg.tactic not in p.name:
            continue
        if not (p / filename).exists():
            continue
        meta_path = p / "00_run_metadata.json"
        if not meta_path.exists():
            continue
        try:
            import json as _json
            meta = _json.loads(meta_path.read_text())
        except Exception:
            continue
        if meta.get("status") not in ("success", "partial"):
            continue
        if phase_filter and meta.get("phase") != phase_filter:
            continue
        candidates.append(p)
    candidates.sort(key=lambda p: p.name, reverse=True)
    return (candidates[0] / filename) if candidates else None


# ── CLI ───────────────────────────────────────────────────────────────────


def main() -> None:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument(
        "phase",
        choices=["full", "pacing_only", "pushonly", "phase1", "phase2", "phase3"],
    )
    p.add_argument("--config", default="sbo/config/podcast.yaml")
    p.add_argument("--sheet-id", default=None, help="Override env-var sheet ID")
    args = p.parse_args()

    tactic_key = Path(args.config).stem.upper()  # e.g. "total_audio" → "TOTAL_AUDIO"
    env_var = f"SHEET_ID_{tactic_key}"
    sheet_id = args.sheet_id or os.environ.get(env_var)
    if not sheet_id:
        raise SystemExit(f"Set {env_var} in .env or pass --sheet-id")

    from sbo.git_guard import GitGuardError
    try:
        ctx = build_context(args.config, sheet_id, args.phase)
    except GitGuardError as e:
        raise SystemExit(f"Stage {args.phase} BLOCKED — {e}")
    {
        "full": run_full,
        "pacing_only": run_pacing_only,
        "pushonly": run_pushonly,
        "phase1": run_phase1,
        "phase2": run_phase2,
        "phase3": run_phase3,
    }[args.phase](ctx)


if __name__ == "__main__":
    main()
