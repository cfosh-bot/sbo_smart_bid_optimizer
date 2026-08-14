"""Smart Bid multiplier decision engine — pure logic, fully testable.

Port of `calculateNewMultipliers` (Apps Script Section 17). Given a Bid
Optimizer DataFrame plus state inputs (run logs, pacing history, paused
log, zero-delivery set), produces a per-row multiplier + reason code.

No I/O. No API calls. No sheet writes. Hand it inputs, get outputs back.

Priority cascade (in order — first matching branch wins):
    CASE 0  PRE_FLIGHT_HOLD          zero delivery + ~0% pacing + >3d
    CASE 1  LINE_PAUSED_HOLDING       paused + still zero
    CASE 2  LINE_RESUMED              paused + delivery returned
    CASE 3  LINE_PAUSED               newly zero delivery
    PRI A   FIRST_RUN[_EOC_UNDER]     Day 1 baseline (with EOC fast-path)
    PRI B   DAY2_BASELINE[_*]         Day 2 baseline
    PRI 1   LAST_3_DAYS_HOLD/UNDER    days_rem ≤ 3
    PRI 2   PRIORITY_MODE_*           4 consecutive OVER days
    PRI 3   OTHER_*                   "Other" modifier category at ≥100%
    PRI 3.5 SUB_TACTIC_CAP_*         streaming/podcast share out of bounds
                                      (Total Audio only — skipped when
                                      cfg.sub_tactic_share is None)
                                      Suspended when pacing <75% at any time,
                                      or pacing <100% AND days_rem ≤7.
    PRI 4   PACE_*                    normal pacing engine (down/hold/up)
    PRI 5   NO_PACING                 fallback — hold current

Final clamp: category dollar cap (ceiling), kill multiplier (floor).

Sub-tactic share enforcement (Priority 3.5) — Total Audio only:
    Pre-computed per LI: streaming share % and podcast share % from
    Modifier_Impression_Share_Pct, grouped by sub-tactic via Alternative_ID.

    Per deal row:
        - If this deal's sub-tactic share is between throttle_entry×max and max:
          soft throttle — linearly taper multiplier toward floor×throttle_mult/CPM.
        - If this deal's sub-tactic share is at or above max:
          hard kill — floor×0.10/CPM (guarantees zero delivery).
        - If enforcement is suspended (pacing<75% or pacing<100% AND ≤7d):
          fall through to normal pacing, note in reason text.
        - If sub-tactic share is within bounds: no action, fall through.

    Known tradeoff: suppressing the over-represented sub-tactic may cause
    some pacing drag. This is accepted during normal operations. Enforcement
    is always suspended near EOC to prioritize delivery.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Literal, Optional, Set, Tuple

import pandas as pd

from sbo.config_models import EngineConfig

ReasonCode = str  # see set in multiplier_engine.REASON_CODES — kept open for new codes


@dataclass
class Decision:
    bw_id: str
    deal_id: str
    new_multiplier: float
    effective_bid_current: float
    effective_bid_new: float
    reason_code: ReasonCode
    reason_text: str


@dataclass
class EngineResult:
    """Everything the engine produces for one run.

    The orchestrator persists each of these to its proper destination:
    decisions → 05_decisions.parquet, run-log entries → state, etc.
    """

    decisions: List[Decision] = field(default_factory=list)
    new_first_run: List[Tuple[str, str]] = field(default_factory=list)  # (bw_id, end_date_str)
    new_second_run: List[Tuple[str, str]] = field(default_factory=list)
    pre_flight_resets: List[str] = field(default_factory=list)  # bw_ids to remove from first/second
    new_pauses: List[Tuple[str, str]] = field(default_factory=list)  # (bw_id, end_date_str)
    pause_snapshots: List[Dict] = field(default_factory=list)  # for SBO Paused Multiplier Snapshot
    resumed_row_indices: List[int] = field(default_factory=list)  # row indexes in paused_log to mark resumed
    kill_log_entries: List[List] = field(default_factory=list)  # rows for Publisher Kill Log
    pacing_signals: Dict[str, str] = field(default_factory=dict)  # bw_id → 'OVER'|'UNDER'|''


# ─────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────


def decide_multipliers(
    optimizer_df: pd.DataFrame,
    *,
    cfg: EngineConfig,
    pacing_history: pd.DataFrame,
    first_run_seen: Set[str],
    second_run_seen: Set[str],
    paused_active: Dict[str, int],
    zero_delivery: Set[str],
    run_date: date,
    paused_snapshot_map: Dict[Tuple[str, str], float] | None = None,
    sub_tactic_shares: Dict[Tuple[str, str], float] | None = None,
) -> EngineResult:
    """Apply the full cascade to every row. See module docstring for rules."""
    result = EngineResult()
    if optimizer_df.empty:
        return result

    # Pre-build per-LI deal price ranking (for continuous tier modifier)
    price_ranks = _build_price_ranks(optimizer_df)

    # Per-LI pacing-history signals (newest-first, up to 4)
    history_map = _build_history_map(pacing_history, max_runs=4)

    # Pre-build per-LI sub-tactic impression share map (Total Audio only)
    # Uses pre-computed shares from publisher_stats if provided (more accurate),
    # otherwise falls back to building from optimizer_df.
    if sub_tactic_shares is not None:
        _sub_tactic_shares = sub_tactic_shares
    else:
        _sub_tactic_shares = _build_sub_tactic_shares(optimizer_df) if cfg.sub_tactic_share else {}

    # Track per-LI events we've already logged (one log per LI, not per deal)
    seen_first: Set[str] = set()
    seen_second: Set[str] = set()
    seen_pause_write: Set[str] = set()
    seen_resume_write: Set[str] = set()
    seen_pre_flight: Set[str] = set()

    is_weekend_window = _is_weekend_window_est(run_date)

    for _, row in optimizer_df.iterrows():
        ctx = _RowContext.from_row(
            row, cfg, price_ranks, history_map,
            zero_delivery, paused_active, is_weekend_window,
            _sub_tactic_shares,
        )
        _decide_one(ctx, result, first_run_seen, second_run_seen,
                    seen_first, seen_second, seen_pause_write, seen_resume_write, seen_pre_flight,
                    paused_snapshot_map=paused_snapshot_map or {})
        # Pacing history signal for this LI (only the first row per LI matters)
        if ctx.bw_id and ctx.bw_id not in result.pacing_signals:
            if ctx.bw_id in zero_delivery or ctx.bw_id in paused_active:
                result.pacing_signals[ctx.bw_id] = ""
            elif ctx.pacing is None:
                result.pacing_signals[ctx.bw_id] = ""
            else:
                result.pacing_signals[ctx.bw_id] = "OVER" if ctx.pacing >= 1.0 else "UNDER"
    return result


# ─────────────────────────────────────────────────────────────────────────
# Per-row decision (the cascade)
# ─────────────────────────────────────────────────────────────────────────


@dataclass
class _RowContext:
    """All the inputs the cascade needs for one (LI × deal) row.

    Built once per row from the DataFrame + state inputs, then passed
    around so individual branches don't have to keep re-parsing the row.
    """
    bw_id: str
    deal_id: str
    end_date: str
    days_rem: Optional[int]
    pacing: Optional[float]
    cpm_bid: float
    floor: float
    deal_cpm_li: float
    deal_glob_cpm: float
    l3_cpm: float
    pub_share: float
    mod_type: str
    curr_mult: float
    is_zero_delivery: bool
    is_paused_active: bool
    paused_log_row_idx: Optional[int]  # paused_log DataFrame index (None if not paused)
    rank_frac: float
    history: List[str]  # newest-first; 'OVER'|'UNDER'
    is_weekend: bool
    alternative_id: str  # deal-level alternative_id for sub-tactic routing (Total Audio)
    sub_tactic_share_pct: float  # this deal's sub-tactic's impression share on this LI (0–1)
    cfg: EngineConfig

    @staticmethod
    def from_row(
        row: pd.Series,
        cfg: EngineConfig,
        price_ranks: Dict[Tuple[str, str], float],
        history_map: Dict[str, List[str]],
        zero_delivery: Set[str],
        paused_active: Dict[str, int],
        is_weekend: bool,
        sub_tactic_shares: Dict[Tuple[str, str], float] | None = None,
    ) -> "_RowContext":
        bw_id = str(row["BW_Line_Item_ID"]).strip()
        deal_id = str(row["Deal_ID"]).strip()
        days_rem_raw = row.get("Days_Remaining", "")
        days_rem = int(days_rem_raw) if str(days_rem_raw).strip() not in ("", "nan") else None
        pacing_raw = row.get("Pacing_Pct", "")
        pacing = _parse_pacing(pacing_raw)
        alt_id = str(row.get("Alternative_ID", "") or "")
        sub_tactic = str(row.get("Sub_Tactic", "") or "") or _parse_sub_tactic(alt_id)
        share_pct = (sub_tactic_shares or {}).get((bw_id, sub_tactic), 0.0)
        return _RowContext(
            bw_id=bw_id,
            deal_id=deal_id,
            end_date=str(row.get("End_Date", "") or ""),
            days_rem=days_rem,
            pacing=pacing,
            cpm_bid=_safe_float(row.get("CPM_Bid")),
            floor=_safe_float(row.get("Floor_Price")),
            deal_cpm_li=_safe_float(row.get("Deal_Clearing_CPM_On_LI")),
            deal_glob_cpm=_safe_float(row.get("Deal_Global_Clearing_CPM")),
            l3_cpm=_safe_float(row.get("Last_3_Days_Clearing_CPM")),
            pub_share=_safe_float(row.get("Pub_Impression_Share_Pct")),
            mod_type=str(row.get("Modifier_Deal_List", "") or ""),
            curr_mult=_safe_float(row.get("Current_Multiplier")) or 1.0,
            is_zero_delivery=bw_id in zero_delivery,
            is_paused_active=bw_id in paused_active,
            paused_log_row_idx=paused_active.get(bw_id),
            rank_frac=price_ranks.get((bw_id, deal_id), 0.5),
            history=history_map.get(bw_id, []),
            is_weekend=is_weekend,
            alternative_id=alt_id,
            sub_tactic_share_pct=share_pct,
            cfg=cfg,
        )


def _decide_one(
    ctx: _RowContext,
    result: EngineResult,
    first_run_seen: Set[str],
    second_run_seen: Set[str],
    seen_first: Set[str],
    seen_second: Set[str],
    seen_pause_write: Set[str],
    seen_resume_write: Set[str],
    seen_pre_flight: Set[str],
    paused_snapshot_map: Dict[Tuple[str, str], float] | None = None,
) -> None:
    """Apply the priority cascade for a single (LI × deal) row.

    Appends Decision (and any state-update events) to `result`.
    """
    # ── CASE 0: PRE_FLIGHT_HOLD — line exists but never delivered ────────
    pacing_for_preflight = ctx.pacing if ctx.pacing is not None else None
    is_pre_flight = (
        ctx.is_zero_delivery
        and pacing_for_preflight is not None
        and pacing_for_preflight < 0.001
        and (ctx.days_rem is None or ctx.days_rem > 3)
    )
    if is_pre_flight:
        if ctx.bw_id in first_run_seen and ctx.bw_id not in seen_pre_flight:
            seen_pre_flight.add(ctx.bw_id)
            result.pre_flight_resets.append(ctx.bw_id)
        pf_hold = 1.0
        if ctx.cpm_bid > 0 and ctx.floor > 0:
            base = ctx.deal_glob_cpm if ctx.deal_glob_cpm > 0 else ctx.floor
            pf_hold = round(min(
                ctx.cfg.hard_max.normal,
                max(0.01, (base * ctx.cfg.floor.max_floor_mult) / ctx.cpm_bid),
            ), 3)
        result.decisions.append(Decision(
            bw_id=ctx.bw_id, deal_id=ctx.deal_id, new_multiplier=pf_hold,
            effective_bid_current=0.0, effective_bid_new=0.0,
            reason_code="PRE_FLIGHT_HOLD",
            reason_text=(
                f"PRE_FLIGHT_HOLD — line has never delivered "
                f"(pacing ~0%, zero imps yesterday, "
                f"{ctx.days_rem if ctx.days_rem is not None else 'no end date'} days remaining). "
                f"Holding at Day 1 baseline {pf_hold}×."
            ),
        ))
        return

    # ── CASE 1: LINE_PAUSED_HOLDING — paused + still zero ────────────────
    # Frozen (not recomputed) while still paused: hold at whatever multiplier
    # was in effect the moment zero delivery was detected. (MP CTV behavior —
    # deliberately chosen over recomputing a floor-protection value each run
    # so a held line's multiplier doesn't drift while it isn't delivering.)
    if ctx.is_paused_active and ctx.is_zero_delivery:
        result.decisions.append(Decision(
            bw_id=ctx.bw_id, deal_id=ctx.deal_id, new_multiplier=ctx.curr_mult,
            effective_bid_current=round(ctx.cpm_bid * ctx.curr_mult, 2),
            effective_bid_new=round(ctx.cpm_bid * ctx.curr_mult, 2),
            reason_code="LINE_PAUSED_HOLDING",
            reason_text=(
                f"LINE_PAUSED_HOLDING — zero delivery confirmed again. "
                f"Holding at snapshotted multiplier {ctx.curr_mult:.3f}×."
            ),
        ))
        return

    # ── CASE 2: LINE_RESUMED — paused but delivery returned ──────────────
    if ctx.is_paused_active and not ctx.is_zero_delivery:
        if ctx.bw_id not in seen_resume_write:
            seen_resume_write.add(ctx.bw_id)
            row_idx = ctx.paused_log_row_idx
            if row_idx is not None and row_idx not in result.resumed_row_indices:
                result.resumed_row_indices.append(row_idx)
        if ctx.deal_cpm_li > 0 and ctx.cpm_bid > 0:
            floor_prot = round((ctx.deal_cpm_li * 1.15) / ctx.cpm_bid, 3)
            basis = (
                f"max(curr {ctx.curr_mult:.3f}×, dealCpmLi ${ctx.deal_cpm_li:.2f} × 1.15 / CPM "
                f"${ctx.cpm_bid:.2f})"
            )
        elif ctx.floor > 0 and ctx.cpm_bid > 0:
            floor_prot = round((ctx.floor * 1.05) / ctx.cpm_bid, 3)
            basis = f"max(curr {ctx.curr_mult:.3f}×, floor ${ctx.floor:.2f} × 1.05 / CPM ${ctx.cpm_bid:.2f})"
        else:
            floor_prot = 0.0
            basis = f"curr {ctx.curr_mult:.3f}× — no deal CPM or floor data"
        resume_mult = round(max(ctx.curr_mult, floor_prot), 2)
        result.decisions.append(Decision(
            bw_id=ctx.bw_id, deal_id=ctx.deal_id, new_multiplier=resume_mult,
            effective_bid_current=round(ctx.cpm_bid * ctx.curr_mult, 2),
            effective_bid_new=round(ctx.cpm_bid * resume_mult, 2),
            reason_code="LINE_RESUMED",
            reason_text=(
                f"LINE_RESUMED — delivery returned. "
                f"Restart multiplier {resume_mult:.3f}× ({basis})."
            ),
        ))
        return

    # ── CASE 3: LINE_PAUSED — newly detected zero delivery ────────────────
    if not ctx.is_paused_active and ctx.is_zero_delivery:
        cap_dollar = (
            ctx.cfg.category_max_bid["Spreaker"].max_at_below_75
            if "Spreaker" in ctx.cfg.category_max_bid else 14.0
        )
        if ctx.deal_cpm_li > 0 and ctx.cpm_bid > 0:
            floor_prot = round(min(
                cap_dollar / ctx.cpm_bid,
                (ctx.deal_cpm_li * 1.2) / ctx.cpm_bid,
            ), 3)
            basis = (
                f"max(curr {ctx.curr_mult:.3f}×, dealCpmLi ${ctx.deal_cpm_li:.2f} × 1.2 / CPM "
                f"${ctx.cpm_bid:.2f})"
            )
        elif ctx.floor > 0 and ctx.cpm_bid > 0:
            floor_prot = round((ctx.floor * 1.05) / ctx.cpm_bid, 3)
            basis = f"max(curr {ctx.curr_mult:.3f}×, floor ${ctx.floor:.2f} × 1.05 / CPM ${ctx.cpm_bid:.2f})"
        else:
            floor_prot = 0.0
            basis = f"curr {ctx.curr_mult:.3f}× — no deal CPM or floor data"
        held = round(max(ctx.curr_mult, floor_prot), 2)
        if ctx.bw_id not in seen_pause_write:
            seen_pause_write.add(ctx.bw_id)
            result.new_pauses.append((ctx.bw_id, ctx.end_date))
        result.pause_snapshots.append({
            "BW_Line_Item_ID": ctx.bw_id,
            "Deal_ID": ctx.deal_id,
            "Paused_Date": datetime.now(),
            "Held_Multiplier": held,
            "Basis": basis,
        })
        result.decisions.append(Decision(
            bw_id=ctx.bw_id, deal_id=ctx.deal_id, new_multiplier=held,
            effective_bid_current=round(ctx.cpm_bid * ctx.curr_mult, 2),
            effective_bid_new=round(ctx.cpm_bid * held, 2),
            reason_code="LINE_PAUSED",
            reason_text=(
                f"LINE_PAUSED — zero delivery detected. Held at {held:.3f}× ({basis})."
            ),
        ))
        return

    # ── Sanity guards (CPM / floor missing) ──────────────────────────────
    if ctx.cpm_bid <= 0:
        result.decisions.append(Decision(
            bw_id=ctx.bw_id, deal_id=ctx.deal_id, new_multiplier=0.0,
            effective_bid_current=0.0, effective_bid_new=0.0,
            reason_code="NO_CPM_BID",
            reason_text="No CPM bid data on line item.",
        ))
        return
    if ctx.floor <= 0.01:
        # Floor ≤ $0.01 (e.g. iHM O&O fixed price): hold at 1.0×
        result.decisions.append(Decision(
            bw_id=ctx.bw_id, deal_id=ctx.deal_id, new_multiplier=1.0,
            effective_bid_current=round(ctx.cpm_bid * 1.0, 2),
            effective_bid_new=round(ctx.cpm_bid * 1.0, 2),
            reason_code="FLOOR_TOO_LOW",
            reason_text="Floor $0.01 or missing — holding 1.0× (O&O fixed price).",
        ))
        return

    # ── Guardrail thresholds ─────────────────────────────────────────────
    f = ctx.cfg.floor
    hard_max = ctx.cfg.hard_max.severe if (ctx.pacing is not None and ctx.pacing < 0.75) else ctx.cfg.hard_max.normal
    base_mult = min((ctx.floor * f.max_floor_mult) / ctx.cpm_bid, ctx.cfg.hard_max.normal)
    norm_min = max((ctx.floor * f.norm_min_floor_mult) / ctx.cpm_bid, 0.01) if ctx.floor > 0.01 else 1.0
    throttle = (ctx.floor * f.throttle_mult) / ctx.cpm_bid
    kill_mult = (ctx.floor * f.kill_mult) / ctx.cpm_bid if ctx.floor > 0.01 else 1.0

    # ── History trend modifier ───────────────────────────────────────────
    good = sum(1 for h in ctx.history if h == "OVER" or h == "GOOD")
    under = sum(1 for h in ctx.history if h == "UNDER")
    sustained4_good = len(ctx.history) >= 4 and good == 4
    sustained4_under = len(ctx.history) >= 4 and under == 4
    mostly3_good = len(ctx.history) >= 3 and good >= 3
    mostly3_under = len(ctx.history) >= 3 and under >= 3
    if sustained4_good or sustained4_under:
        hist_trend = 1.4
    elif mostly3_good or mostly3_under:
        hist_trend = 1.2
    else:
        hist_trend = 1.0

    priority_mode_active = sustained4_good

    # ── Modifier category routing ────────────────────────────────────────
    _other_names = [ctx.cfg.modifier_other_name] if isinstance(ctx.cfg.modifier_other_name, str) else ctx.cfg.modifier_other_name
    is_other = ctx.mod_type in _other_names

    is_priority = ctx.mod_type in ctx.cfg.modifier_priority
    throttle_lvl = ctx.cfg.modifier_throttle_levels.get(ctx.mod_type, 0.0)

    # ── No-category fallback ceiling ─────────────────────────────────────
    no_cat_dollar = ctx.deal_glob_cpm * 1.7 if ctx.deal_glob_cpm > 0 else 0
    no_cat_mult = no_cat_dollar / ctx.cpm_bid if no_cat_dollar > 0 else base_mult

    # ── Sliding-scale targets (≤14d underpacing) and caps (>14d) ─────────
    is_eoc_window = ctx.days_rem is not None and ctx.days_rem <= 3
    is_mid_window = ctx.days_rem is not None and 3 < ctx.days_rem <= 14
    scale_target = (
        _sliding_scale_mult(ctx, norm_min)
        if (is_eoc_window or is_mid_window) else None
    )
    scale_cap = (
        _sliding_scale_mult(ctx, norm_min)
        if not (is_eoc_window or is_mid_window) else None
    )

    new_mult: float
    reason_code: str
    reason_text: str

    # ── PRIORITY A: FIRST_RUN ────────────────────────────────────────────
    if ctx.bw_id not in first_run_seen:
        if ctx.days_rem is not None and ctx.days_rem <= 3:
            if ctx.pacing is None or ctx.pacing >= 1.0:
                new_mult = 1.0 if ctx.floor <= 0.01 else round(base_mult, 2)
                reason_code = "FIRST_RUN"
                reason_text = (
                    f"Day 1 (EOC, on pace) — floor ${ctx.floor:.2f} × 1.2 / CPM "
                    f"${ctx.cpm_bid:.2f} = {new_mult}×. Day 2 not skipped."
                )
                if ctx.bw_id not in seen_first:
                    seen_first.add(ctx.bw_id)
                    result.new_first_run.append((ctx.bw_id, ctx.end_date))
            else:
                # Underpacing within EOC — jump to sliding scale, skip Day 2
                new_mult = scale_target if scale_target is not None else round(
                    min(no_cat_mult, max(norm_min, base_mult)), 2
                )
                reason_code = "FIRST_RUN_EOC_UNDER"
                reason_text = (
                    f"Day 1 (EOC, underpacing {round(ctx.pacing * 100)}%) — "
                    f"EOC sliding scale → {new_mult}×. Day 2 skipped."
                )
                if ctx.bw_id not in seen_first:
                    seen_first.add(ctx.bw_id)
                    result.new_first_run.append((ctx.bw_id, ctx.end_date))
                if ctx.bw_id not in seen_second:
                    seen_second.add(ctx.bw_id)
                    result.new_second_run.append((ctx.bw_id, ctx.end_date))
        else:
            if ctx.floor <= 0.01:
                new_mult = 1.0
                reason_code = "FIRST_RUN"
                reason_text = (
                    f"Day 1 baseline — floor ${ctx.floor:.2f} too low. Holding 1.0×."
                )
            else:
                new_mult = round(base_mult, 2)
                reason_code = "FIRST_RUN"
                reason_text = (
                    f"Day 1 baseline — floor ${ctx.floor:.2f} × 1.2 / CPM "
                    f"${ctx.cpm_bid:.2f} = {new_mult}×."
                )
            if ctx.bw_id not in seen_first:
                seen_first.add(ctx.bw_id)
                result.new_first_run.append((ctx.bw_id, ctx.end_date))

    # ── PRIORITY B: DAY 2 ────────────────────────────────────────────────
    elif ctx.bw_id not in second_run_seen and (
        ctx.days_rem is None or ctx.days_rem > 3 or ctx.pacing is None or ctx.pacing >= 1.0
    ):
        if ctx.days_rem is not None and ctx.days_rem <= 3 and (ctx.pacing is None or ctx.pacing >= 1.0):
            # EOC Day 2, on pace
            if ctx.deal_cpm_li > 0:
                new_mult = 1.0 if ctx.floor <= 0.01 else round((ctx.deal_cpm_li * 1.2) / ctx.cpm_bid, 2)
                reason_code = "DAY2_BASELINE"
                reason_text = (
                    f"Day 2 (EOC, on pace) — dealCpmLi ${ctx.deal_cpm_li:.2f} × 1.2 / CPM "
                    f"${ctx.cpm_bid:.2f} = {new_mult}×."
                )
            else:
                new_mult = 1.0 if ctx.floor <= 0.01 else round(base_mult, 2)
                reason_code = "DAY2_BASELINE_FALLBACK"
                reason_text = f"Day 2 (EOC, on pace, no deal CPM) — floor baseline {new_mult}×."
        elif is_other:
            new_mult = 1.0 if ctx.floor <= 0.01 else round(throttle, 2)
            reason_code = "DAY2_OTHER_THROTTLE"
            reason_text = (
                f"Day 2 — \"Other\" category. Starting throttled at 1.05× floor → {new_mult}×."
            )
        elif ctx.pacing is not None and ctx.pacing < 1.0 and scale_target is not None:
            new_mult = scale_target
            reason_code = "DAY2_BASELINE"
            reason_text = (
                f"Day 2 (underpacing {round(ctx.pacing * 100)}%, ≤14d) — "
                f"sliding scale target → ${new_mult * ctx.cpm_bid:.2f} eff bid → {new_mult}×."
            )
        else:
            new_mult = 1.0 if ctx.floor <= 0.01 else round(base_mult, 2)
            reason_code = "DAY2_BASELINE_FALLBACK"
            reason_text = (
                f"Day 2 — no deal CPM. Floor baseline: ${ctx.floor:.2f} × 1.2 / "
                f"${ctx.cpm_bid:.2f} = {new_mult}×."
            )
        if ctx.bw_id not in seen_second:
            seen_second.add(ctx.bw_id)
            result.new_second_run.append((ctx.bw_id, ctx.end_date))

    # ── PRIORITY 1: LAST_3_DAYS ──────────────────────────────────────────
    elif is_eoc_window:
        if ctx.pacing is None or ctx.pacing >= 1.0:
            new_mult = ctx.curr_mult
            reason_code = "LAST_3_DAYS_HOLD"
            reason_text = (
                f"Last 3 days, on pace ({round(ctx.pacing * 100) if ctx.pacing else '?'}%) — "
                f"holding {ctx.curr_mult:.3f}×. Days left: {ctx.days_rem}."
            )
        else:
            if scale_target is not None:
                new_mult = scale_target
            else:
                step = 0.10 if ctx.pacing >= 0.90 else (0.20 if ctx.pacing >= 0.75 else 0.30)
                new_mult = round(min(no_cat_mult, max(norm_min, ctx.curr_mult + step * hist_trend)), 2)
            reason_code = "LAST_3_DAYS_UNDER"
            reason_text = (
                f"Last 3 days, underpacing {round(ctx.pacing * 100)}% — "
                f"EOC sliding scale → ${new_mult * ctx.cpm_bid:.2f} eff bid → {new_mult}×. "
                f"Days left: {ctx.days_rem}."
            )

    # ── PRIORITY 2: PRIORITY_MODE (4 days good, ≥100% pacing) ─────────────
    elif priority_mode_active and ctx.pacing is not None and ctx.pacing >= 1.0:
        if is_other:
            t_mult = (ctx.floor * 1.10) / ctx.cpm_bid
            new_mult = round(min(ctx.curr_mult, t_mult), 2)
            reason_code = "PRIORITY_MODE_THROTTLE_OTHER"
            reason_text = f"Priority mode (4 days ≥100%). \"Other\" throttled at 1.10× floor → {new_mult}×."
            if ctx.curr_mult > t_mult + 0.001:
                result.kill_log_entries.append([
                    ctx.bw_id, "DEAL_THROTTLE", datetime.now(),
                    f"{ctx.deal_id} [Other - priority mode]", "", ctx.end_date,
                ])
        elif is_priority:
            new_mult = _priority_mode_step(ctx, "exempt", hist_trend, norm_min)
            reason_code = "PRIORITY_MODE_EXEMPT"
            reason_text = (
                f"Priority mode — iHM O&O/Min Guarantee exempt. "
                f"{ctx.curr_mult:.3f} → {new_mult:.3f}."
            )
        elif throttle_lvl > 0:
            t_mult = (ctx.floor * throttle_lvl) / ctx.cpm_bid
            new_mult = round(min(ctx.curr_mult, t_mult), 2)
            reason_code = "PRIORITY_MODE_THROTTLE"
            reason_text = (
                f"Priority mode. \"{ctx.mod_type}\" throttled at "
                f"{int(throttle_lvl * 100)}× floor → {new_mult}×."
            )
            if ctx.curr_mult > t_mult + 0.001:
                result.kill_log_entries.append([
                    ctx.bw_id, "DEAL_THROTTLE", datetime.now(),
                    f"{ctx.deal_id} [{ctx.mod_type} - priority mode]", "", ctx.end_date,
                ])
        else:
            new_mult = _priority_mode_step(ctx, "down", hist_trend, norm_min)
            reason_code = "PRIORITY_MODE_DOWN"
            reason_text = f"Priority mode. Down. {ctx.curr_mult:.3f} → {new_mult:.3f}."

    # ── PRIORITY 3: OTHER category at ≥100% pacing ───────────────────────
    elif is_other and ctx.pacing is not None and ctx.pacing >= 1.0:
        if is_eoc_window:
            # EOC: throttle to 1.10× floor, don't kill
            eoc_other_throttle = (ctx.floor * 1.10) / ctx.cpm_bid
            new_mult = round(min(ctx.curr_mult, eoc_other_throttle), 2)
            reason_code = "OTHER_EOC_THROTTLE"
            reason_text = (
                f"\"Other\" EOC throttle at 1.10× floor — pacing "
                f"{round(ctx.pacing * 100)}% (≤3d). → {new_mult}×."
            )
            if ctx.curr_mult > eoc_other_throttle + 0.001:
                result.kill_log_entries.append([
                    ctx.bw_id, "DEAL_THROTTLE", datetime.now(),
                    f"{ctx.deal_id} [Other - EOC]", "", ctx.end_date,
                ])
        else:
            normal_throttle = (ctx.floor * 1.10) / ctx.cpm_bid
            new_mult = round(min(ctx.curr_mult, normal_throttle), 2)
            reason_code = "OTHER_THROTTLE_NORMAL"
            reason_text = (
                f"\"Other\" throttled at 1.10× floor — pacing "
                f"{round(ctx.pacing * 100)}% (100–105%). → {new_mult}×."
            )
            if ctx.curr_mult > normal_throttle + 0.001:
                result.kill_log_entries.append([
                    ctx.bw_id, "DEAL_THROTTLE", datetime.now(),
                    f"{ctx.deal_id} [Other - normal ops]", "", ctx.end_date,
                ])

    # ── PRIORITY 3.5: SUB_TACTIC_CAP — Total Audio impression share ──────
    elif ctx.cfg.sub_tactic_share is not None and ctx.sub_tactic_share_pct > 0:
        sub_tactic = _parse_sub_tactic(ctx.alternative_id)
        bounds = getattr(ctx.cfg.sub_tactic_share, sub_tactic, None) if sub_tactic else None

        if bounds is not None:
            share = ctx.sub_tactic_share_pct  # already a fraction (0–1)
            cap_max = bounds.max
            throttle_entry = bounds.max * bounds.throttle_entry  # e.g. 0.90 × 0.80 = 0.72
            hard_kill_mult = (ctx.floor * 0.10) / ctx.cpm_bid if ctx.cpm_bid > 0 else 0.0
            throttle_floor = (ctx.floor * ctx.cfg.floor.throttle_mult) / ctx.cpm_bid if ctx.cpm_bid > 0 else 0.0

            # Suspension check: pacing <75% or (pacing <100% AND ≤7 days)
            suspended = (
                ctx.pacing is not None and ctx.pacing < 0.75
            ) or (
                ctx.pacing is not None and ctx.pacing < 1.0
                and ctx.days_rem is not None and ctx.days_rem <= 7
            )

            if suspended and share >= throttle_entry:
                # Enforcement suspended — fall through to normal pacing, append note
                new_mult, reason_code, reason_text = _normal_pacing(
                    ctx, norm_min, kill_mult, no_cat_mult, scale_cap, hist_trend, result,
                )
                reason_text += (
                    f" [SUB_TACTIC_CAP_SUSPENDED: {sub_tactic} share "
                    f"{round(share * 100, 1)}% — enforcement suspended "
                    f"({'<75%' if ctx.pacing is not None and ctx.pacing < 0.75 else 'underpacing ≤7d'})]"
                )

            elif share >= cap_max:
                # Hard kill — sub-tactic at or over cap
                new_mult = round(max(hard_kill_mult, 0.001), 3)
                reason_code = "SUB_TACTIC_CAP_KILL"
                reason_text = (
                    f"SUB_TACTIC_CAP_KILL — {sub_tactic} share "
                    f"{round(share * 100, 1)}% at/above {round(cap_max * 100)}% cap. "
                    f"Hard kill → {new_mult:.3f}×."
                )
                result.kill_log_entries.append([
                    ctx.bw_id, "SUB_TACTIC_CAP_KILL", datetime.now(),
                    f"{ctx.deal_id} [{sub_tactic} {round(share * 100, 1)}% ≥ {round(cap_max * 100)}% cap]",
                    "", ctx.end_date,
                ])

            elif share >= throttle_entry:
                # Soft throttle — linearly taper toward throttle floor
                # progress 0 = just entered throttle zone, 1 = at full cap
                progress = (share - throttle_entry) / (cap_max - throttle_entry)
                progress = max(0.0, min(1.0, progress))
                target = throttle_floor + (1 - progress) * (ctx.curr_mult - throttle_floor)
                new_mult = round(max(throttle_floor, min(ctx.curr_mult, target)), 3)
                reason_code = "SUB_TACTIC_CAP_THROTTLE"
                reason_text = (
                    f"SUB_TACTIC_CAP_THROTTLE — {sub_tactic} share "
                    f"{round(share * 100, 1)}% (throttle entry {round(throttle_entry * 100, 1)}%, "
                    f"cap {round(cap_max * 100)}%). "
                    f"Taper progress {round(progress * 100)}% → {new_mult:.3f}×."
                )
                if ctx.curr_mult > new_mult + 0.001:
                    result.kill_log_entries.append([
                        ctx.bw_id, "SUB_TACTIC_CAP_THROTTLE", datetime.now(),
                        f"{ctx.deal_id} [{sub_tactic} {round(share * 100, 1)}%]",
                        "", ctx.end_date,
                    ])
            else:
                # Share within bounds — fall through to normal pacing
                new_mult, reason_code, reason_text = _normal_pacing(
                    ctx, norm_min, kill_mult, no_cat_mult, scale_cap, hist_trend, result,
                )
        else:
            # No bounds configured for this sub-tactic — fall through
            new_mult, reason_code, reason_text = _normal_pacing(
                ctx, norm_min, kill_mult, no_cat_mult, scale_cap, hist_trend, result,
            )

    # ── PRIORITY 4: Normal pacing engine ─────────────────────────────────
    elif ctx.pacing is not None:
        new_mult, reason_code, reason_text = _normal_pacing(
            ctx, norm_min, kill_mult, no_cat_mult, scale_cap, hist_trend, result,
        )

    # ── PRIORITY 5: NO_PACING ────────────────────────────────────────────
    else:
        new_mult = ctx.curr_mult
        reason_code = "NO_PACING"
        reason_text = (
            f"No pacing data — holding {ctx.curr_mult:.3f}. Run Update Pacing first."
        )

    # ── Final clamp: category dollar cap ceiling, kill_mult floor ────────
    is_eoc_under = is_eoc_window and ctx.pacing is not None and ctx.pacing < 1.0
    cat_ceiling_dollar = _category_max_bid(ctx, is_eoc_under)
    if ctx.cpm_bid > 0 and cat_ceiling_dollar != math.inf:
        ceiling_mult = min(cat_ceiling_dollar / ctx.cpm_bid, hard_max)
    else:
        ceiling_mult = hard_max

    # Note: SUB_TACTIC_CAP_KILL uses floor×0.10 which is intentionally below
    # the normal kill_mult floor — don't clamp it back up for cap kills.
    if reason_code == "SUB_TACTIC_CAP_KILL":
        new_mult = round(min(ceiling_mult, new_mult), 2)
    else:
        new_mult = round(max(kill_mult, min(ceiling_mult, new_mult)), 2)

    if (
        ctx.cpm_bid > 0
        and cat_ceiling_dollar != math.inf
        and (ctx.cpm_bid * new_mult) >= cat_ceiling_dollar - 0.01
    ):
        eoc_tag = " (EOC)" if is_eoc_under else ""
        reason_text += (
            f" [CAT_MAX_BID: ${cat_ceiling_dollar:.2f} ceiling for "
            f"\"{ctx.mod_type}\" at {round((ctx.pacing or 0) * 100)}% pacing"
            f"{eoc_tag} → max mult {ceiling_mult:.2f}×]"
        )

    result.decisions.append(Decision(
        bw_id=ctx.bw_id, deal_id=ctx.deal_id, new_multiplier=new_mult,
        effective_bid_current=round(ctx.cpm_bid * ctx.curr_mult, 2),
        effective_bid_new=round(ctx.cpm_bid * new_mult, 2),
        reason_code=reason_code, reason_text=reason_text,
    ))


# ─────────────────────────────────────────────────────────────────────────
# Engine helpers
# ─────────────────────────────────────────────────────────────────────────


def _normal_pacing(
    ctx: _RowContext,
    norm_min: float,
    kill_mult: float,
    no_cat_mult: float,
    scale_cap: Optional[float],
    hist_trend: float,
    result: EngineResult,
) -> Tuple[float, str, str]:
    """PRIORITY 4 — normal pacing engine (down + hold + up + weekend)."""
    pacing = ctx.pacing or 0.0

    if pacing >= 1.0:
        # Overpacing
        if ctx.days_rem is not None and ctx.days_rem <= 7:
            # ≤7 days: 0.5× dampener on decreases
            if pacing < 1.05:
                return ctx.curr_mult, "PACE_HOLD_ONTARGET", (
                    f"Pacing {round(pacing * 100)}% (100–105%, ≤7d) — on target. "
                    f"Holding {ctx.curr_mult:.3f}."
                )
            base_down = 0.10 if pacing < 1.15 else (0.15 if pacing < 1.25 else 0.20)
            base_down *= hist_trend
            tier_down = _price_tier_mod(ctx.rank_frac, "down", ctx.cfg)
            total_down = min(base_down * tier_down * 0.5, ctx.cfg.max_single_day_down)
            new = round(max(norm_min, ctx.curr_mult - total_down), 2)
            code = "PACE_DOWN_MOD" if pacing < 1.15 else "PACE_DOWN_AGG"
            return new, code, (
                f"Pacing {round(pacing * 100)}% (≤7d, 0.5× dampener) — "
                f"down −{total_down:.3f}. {ctx.curr_mult:.3f} → {new:.3f}."
            )
        if pacing < 1.05:
            return ctx.curr_mult, "PACE_HOLD_ONTARGET", (
                f"Pacing {round(pacing * 100)}% (100–105%) — on target. No adjustment."
            )
        # >7d, >105% — normal decrease
        base_down = 0.10 if pacing < 1.15 else (0.15 if pacing < 1.25 else 0.20)
        base_down *= hist_trend
        tier_down = _price_tier_mod(ctx.rank_frac, "down", ctx.cfg)
        total_down = min(base_down * tier_down, ctx.cfg.max_single_day_down)
        new = round(max(norm_min, ctx.curr_mult - total_down), 2)
        code = "PACE_DOWN_MOD" if pacing < 1.15 else "PACE_DOWN_AGG"
        return new, code, (
            f"Pacing {round(pacing * 100)}% — down −{total_down:.3f} "
            f"(base {base_down:.3f} × tier {tier_down:.3f}). Trend: {hist_trend:.1f}×. "
            f"{ctx.curr_mult:.3f} → {new:.3f}."
        )

    # Underpacing
    is_eoc_window = ctx.days_rem is not None and ctx.days_rem <= 3
    if ctx.is_weekend and not is_eoc_window:
        return ctx.curr_mult, "PACE_HOLD_WEEKEND", (
            f"Pacing {round(pacing * 100)}% — weekend window (Sun/Mon). "
            f"Bid increase suppressed. Holding {ctx.curr_mult:.3f}."
        )

    is_mid_window = ctx.days_rem is not None and 3 < ctx.days_rem <= 14
    if is_mid_window:
        # Jump straight to sliding-scale target
        target = _sliding_scale_mult(ctx, norm_min)
        if target is not None:
            new = target
            code = "PACE_UP_MOD" if pacing >= 0.90 else (
                "PACE_UP_AGG" if pacing >= 0.75 else "PACE_UP_CRITICAL"
            )
            if ctx.curr_mult <= (ctx.floor * ctx.cfg.floor.kill_mult / ctx.cpm_bid) + 0.001:
                result.kill_log_entries.append([
                    ctx.bw_id, "DEAL_UNKILL", datetime.now(), ctx.deal_id, "", ctx.end_date,
                ])
            return new, code, (
                f"Pacing {round(pacing * 100)}% (≤14d) — sliding scale target "
                f"${new * ctx.cpm_bid:.2f} → {new}×. Trend: {hist_trend:.1f}×."
            )

    # >14d underpacing — incremental raise capped at sliding-scale or no_cat_mult
    severe = pacing < 0.75
    base_up = 0.10 if pacing >= 0.90 else (0.20 if pacing >= 0.75 else 0.30)
    base_up *= hist_trend
    direction = "up_severe" if severe else "up_normal"
    tier_up = _price_tier_mod(ctx.rank_frac, direction, ctx.cfg)
    total_up = base_up * tier_up
    inc_cap = scale_cap if scale_cap is not None else no_cat_mult
    new = round(min(inc_cap, max(norm_min, ctx.curr_mult + total_up)), 2)
    code = "PACE_UP_MOD" if pacing >= 0.90 else (
        "PACE_UP_AGG" if pacing >= 0.75 else "PACE_UP_CRITICAL"
    )
    return new, code, (
        f"Pacing {round(pacing * 100)}% (>14d) — incremental +{total_up:.3f} "
        f"(base {base_up:.3f} × tier {tier_up:.3f}). Cap ${inc_cap * ctx.cpm_bid:.2f}. "
        f"Trend: {hist_trend:.1f}×. {ctx.curr_mult:.3f} → {new:.3f}."
    )


def _priority_mode_step(
    ctx: _RowContext, kind: str, hist_trend: float, norm_min: float
) -> float:
    """Priority-mode down step (kind='exempt' or 'down')."""
    pacing = ctx.pacing or 0.0
    base_down = 0 if pacing < 1.05 else (
        0.10 if pacing < 1.15 else (0.15 if pacing < 1.25 else 0.20)
    )
    base_down *= hist_trend
    tier_down = _price_tier_mod(ctx.rank_frac, "down", ctx.cfg)
    day_mod = 0.5 if ctx.days_rem is not None and ctx.days_rem <= 7 else 1.0
    total_down = min(base_down * tier_down * day_mod, ctx.cfg.max_single_day_down)
    return round(max(norm_min, ctx.curr_mult - total_down), 2)


def _price_tier_mod(rank_frac: float, direction: str, cfg: EngineConfig) -> float:
    """Continuous price-tier modifier. rank_frac 0=cheapest, 1=most expensive."""
    pt = cfg.price_tier
    if direction == "down":
        return pt.down_min + rank_frac * (pt.down_max - pt.down_min)
    if direction == "up_severe":
        return pt.up_sev_max - rank_frac * (pt.up_sev_max - pt.up_sev_min)
    return pt.up_norm_max - rank_frac * (pt.up_norm_max - pt.up_norm_min)


def _category_max_bid(ctx: _RowContext, is_eoc_under: bool) -> float:
    """Interpolated dollar cap for the modifier category. inf if no category.

    Priority order:
        1. deal_max_bid override (deal-level, highest priority)
        2. category_max_bid_by_sub_tactic (streaming vs podcast, Total Audio only)
        3. category_max_bid (flat fallback, used by Podcast/Streaming and as fallback)
    """
    if not ctx.mod_type:
        return math.inf
    # iHM O&O is exempt from all dollar caps — held at 1.0× via floor rules
    if ctx.mod_type in ctx.cfg.modifier_priority and ctx.floor <= 0.01:
        return math.inf
    # Deal-level override takes precedence over everything
    deal_override = ctx.cfg.deal_max_bid.get(ctx.deal_id)
    if deal_override is not None:
        max_over = deal_override.max_at_over_100
        max_below = deal_override.max_at_below_75
    else:
        # Sub-tactic-aware caps (Total Audio) take precedence over flat caps
        if ctx.cfg.category_max_bid_by_sub_tactic:
            sub_tactic = _parse_sub_tactic(ctx.alternative_id)
            sub_caps = ctx.cfg.category_max_bid_by_sub_tactic.get(sub_tactic, {})
            cap = sub_caps.get(ctx.mod_type)
            if cap is None:
                # Fall back to flat category_max_bid
                cap = ctx.cfg.category_max_bid.get(ctx.mod_type) or ctx.cfg.category_max_bid.get("Spreaker")
        else:
            # Podcast/Streaming — use flat caps as before
            cap = ctx.cfg.category_max_bid.get(ctx.mod_type) or ctx.cfg.category_max_bid.get("Spreaker")
        if cap is None:
            return math.inf
        max_over = cap.max_at_over_100
        max_below = cap.max_at_below_75

    if ctx.pacing is None:
        return math.inf
    if is_eoc_under:
        p = max(0.0, min(1.0, ctx.pacing))
        return max_below + (1 - p) * ctx.cfg.cat_max_bid_eoc_bonus
    if ctx.pacing >= 1.0:
        return max_over
    p = max(0.0, ctx.pacing)
    return max_below + p * (max_over - max_below)


def _sliding_scale_mult(ctx: _RowContext, norm_min: float) -> Optional[float]:
    """Sliding-scale target multiplier; None if no category match."""
    if ctx.pacing is None or ctx.pacing >= 1.0 or ctx.cpm_bid <= 0:
        return None
    is_eoc = ctx.days_rem is not None and ctx.days_rem <= 3
    target_dollar = _category_max_bid(ctx, is_eoc)
    if target_dollar == math.inf:
        return None
    mult = target_dollar / ctx.cpm_bid
    return round(max(norm_min, mult), 2)


# ─────────────────────────────────────────────────────────────────────────
# Pre-loop helpers
# ─────────────────────────────────────────────────────────────────────────


def _build_sub_tactic_shares(
    optimizer_df: pd.DataFrame,
) -> Dict[Tuple[str, str], float]:
    """Per-LI sub-tactic impression share map.

    For each LI, sums Modifier_Impression_Share_Pct across all deals
    belonging to the same sub-tactic (streaming or podcast), derived from
    Alternative_ID. Returns fractions (0–1), not percentages.

    Result: {(bw_id, sub_tactic): share_fraction}
    e.g. {("12345", "streaming"): 0.75, ("12345", "podcast"): 0.25}

    Only called when cfg.sub_tactic_share is not None (Total Audio only).
    """
    out: Dict[Tuple[str, str], float] = {}
    if optimizer_df.empty:
        return out
    if "Alternative_ID" not in optimizer_df.columns or "Modifier_Impression_Share_Pct" not in optimizer_df.columns:
        return out

    for bw_id, group in optimizer_df.groupby("BW_Line_Item_ID"):
        bw_id_str = str(bw_id)
        tactic_shares: Dict[str, float] = {}
        for _, row in group.iterrows():
            alt_id = str(row.get("Alternative_ID", "") or "")
            sub_tactic = _parse_sub_tactic(alt_id)
            if not sub_tactic:
                continue
            share = _safe_float(row.get("Modifier_Impression_Share_Pct", 0))
            tactic_shares[sub_tactic] = tactic_shares.get(sub_tactic, 0.0) + share

        # Convert from percentage points to fraction
        total = sum(tactic_shares.values())
        for sub_tactic, share_pct in tactic_shares.items():
            # share_pct is already a percentage (e.g. 75.0 meaning 75%)
            # Store as fraction for comparison against bounds (0–1)
            out[(bw_id_str, sub_tactic)] = round(share_pct / 100.0, 4) if total > 0 else 0.0

    return out


def _build_price_ranks(
    optimizer_df: pd.DataFrame,
) -> Dict[Tuple[str, str], float]:
    """Per-LI deal rank by Last_3_Days_Clearing_CPM (0=cheapest, 1=most expensive).

    No-spend deals (L3 CPM is 0/empty) get the midpoint (0.5).
    """
    ranks: Dict[Tuple[str, str], float] = {}
    if optimizer_df.empty:
        return ranks
    for bw_id, group in optimizer_df.groupby("BW_Line_Item_ID"):
        deals: List[Tuple[str, float]] = []
        for _, row in group.iterrows():
            l3 = _safe_float(row.get("Last_3_Days_Clearing_CPM"))
            deals.append((str(row["Deal_ID"]).strip(), l3))
        active = sorted([d for d in deals if d[1] > 0], key=lambda x: x[1])
        active_n = len(active)
        for i, (deal_id, _) in enumerate(active):
            rank = i / (active_n - 1) if active_n > 1 else 0.5
            ranks[(str(bw_id), deal_id)] = rank
        for deal_id, l3 in deals:
            if l3 == 0 and (str(bw_id), deal_id) not in ranks:
                ranks[(str(bw_id), deal_id)] = 0.5
    return ranks


def _build_history_map(
    pacing_history: pd.DataFrame, max_runs: int
) -> Dict[str, List[str]]:
    """BW LI → newest-first list of last `max_runs` signals ('OVER'/'UNDER')."""
    out: Dict[str, List[str]] = {}
    if pacing_history.empty or "BW_Line_Item_ID" not in pacing_history.columns:
        return out
    date_cols = [c for c in pacing_history.columns if c != "BW_Line_Item_ID"]
    if not date_cols:
        return out
    for _, row in pacing_history.iterrows():
        bw = str(row["BW_Line_Item_ID"]).strip()
        if not bw:
            continue
        signals: List[str] = []
        for c in reversed(date_cols):  # newest is rightmost
            if len(signals) >= max_runs:
                break
            v = str(row[c]).strip()
            if v in ("OVER", "UNDER"):
                signals.append(v)
        if signals:
            out[bw] = signals
    return out


def _is_weekend_window_est(run_date: date) -> bool:
    """Sunday or Monday in US/Eastern time → weekend inventory guard active."""
    # ISO weekday: Monday=1, Sunday=7. JS: Sun=0, Mon=1.
    iso = pd.Timestamp(run_date).isoweekday()
    return iso == 7 or iso == 1  # Sun or Mon


# ─────────────────────────────────────────────────────────────────────────
# Small parsing helpers
# ─────────────────────────────────────────────────────────────────────────


def _parse_sub_tactic(alternative_id: str) -> str:
    """Extract 'streaming' or 'podcast' from alternative_id.

    Checks index 4 of the dash-separated string first (e.g.
    'RON-BBC-Triton-Fixed$7-Podcast-ALL POSITION-Minimum Guarantees'),
    then falls back to searching the full string. Returns '' if neither found.
    """
    if not alternative_id:
        return ""
    parts = alternative_id.split("-")
    if len(parts) > 4:
        candidate = parts[4].strip().lower()
        if candidate in ("streaming", "podcast"):
            return candidate
    lower = alternative_id.lower()
    if "podcast" in lower:
        return "podcast"
    if "streaming" in lower:
        return "streaming"
    return ""


def _safe_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v) if not math.isnan(v) else 0.0
    s = str(v).strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_pacing(v) -> Optional[float]:
    """Pacing comes as decimal (0.97) or percent (97). Normalize to decimal."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        if math.isnan(f):
            return None
    else:
        s = str(v).strip()
        if not s:
            return None
        try:
            f = float(s)
        except ValueError:
            return None
    if f > 9:
        f = f / 100
    return f