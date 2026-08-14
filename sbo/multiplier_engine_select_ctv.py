"""Smart Bid multiplier decision engine — Select CTV tactic.

Port of `calculateNewMultipliers` (Select CTV Apps Script Section 17,
2026-08-14 revision — the latest/authoritative one).

Select CTV has no content-category deal lists and a single fee-adjusted
floor per deal (Max_Floor == Min_Floor by construction upstream), so this
engine is simpler than MP CTV's: publisher-cap kill/throttle only, no
category cap, no price-kill staging, no sub-tactic share enforcement. It is
kept in its own file (rather than branching inside the shared engine) for
the same reason MP CTV's engine is separate — changes to one cannot
accidentally break another product's live bidding.

Priority cascade (in order — first matching branch wins):
    STEP 0  PRE_FLIGHT_HOLD     zero delivery + ~0% pacing + >3d remaining
    STEP 0  LINE_PAUSED_HOLDING paused + still zero — frozen, not recomputed
    STEP 0  (resumed)           paused + delivery returned — no dedicated
                                 reason code; marks the paused_log row
                                 resumed and falls through to the normal
                                 cascade below (Day 1 naturally re-fires,
                                 since a newly-detected pause already
                                 removed this BW LI from the first/second
                                 run logs)
    STEP 0  LINE_PAUSED         newly zero delivery — hold at curr_mult
                                 unchanged (no floor-protection recompute)
    PRI A   FIRST_RUN[_SHORT]   Day 1 baseline
    PRI B   DAY2_BASELINE[_FALLBACK] Day 2 — anchored to Pub_Clearing_CPM_On_LI
    PRI 1   LAST_3_DAYS_HOLD/UNDER days_rem <= 3
    PRI 2   CAP_KILL            publisher share hard kill (uniform 40% cap)
    PRI 3   CAP_THROTTLE        approaching publisher cap
    PRI 5   PACE_*/PACE_HOLD_MARGIN_TRIM  normal pacing + continuous price
                                 tier + on-target margin-health trim (added
                                 2026-08-14)
    PRI 6   NO_PACING           fallback — hold current

Final clamp: kill_mult floor, OPT_HARD_MAX_LAST3 (2.00) absolute ceiling —
applies to every row that reaches it (Step 0 branches return before it,
matching the Apps Script's `continue` inside that gating block).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from sbo.config_models import EngineConfig

# Re-use the Decision / EngineResult dataclasses and generic low-level
# helpers from the shared engine to keep the pipeline orchestrator
# interface identical and avoid re-deriving already-correct math.
from sbo.multiplier_engine import (
    Decision,
    EngineResult,
    _build_history_map,
    _build_price_ranks,
    _parse_pacing,
    _price_tier_mod,
    _safe_float,
)


# ─────────────────────────────────────────────────────────────────────────
# Public entry point (same signature shape as decide_multipliers)
# ─────────────────────────────────────────────────────────────────────────


def decide_multipliers_select_ctv(
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
) -> EngineResult:
    """Apply the full Select CTV cascade to every row."""
    result = EngineResult()
    if optimizer_df.empty:
        return result

    price_ranks = _build_price_ranks(optimizer_df)
    history_map = _build_history_map(pacing_history, max_runs=4)

    seen_first: Set[str] = set()
    seen_second: Set[str] = set()
    seen_pause_write: Set[str] = set()
    seen_resume_write: Set[str] = set()
    seen_pre_flight: Set[str] = set()

    for _, row in optimizer_df.iterrows():
        ctx = _SelectCtvRowContext.from_row(
            row, cfg, price_ranks, history_map, zero_delivery, paused_active,
        )
        _decide_one_select_ctv(
            ctx, result,
            first_run_seen, second_run_seen,
            seen_first, seen_second,
            seen_pause_write, seen_resume_write, seen_pre_flight,
        )
        if ctx.bw_id and ctx.bw_id not in result.pacing_signals:
            if ctx.bw_id in zero_delivery or ctx.bw_id in paused_active:
                result.pacing_signals[ctx.bw_id] = ""
            elif ctx.pacing is None:
                result.pacing_signals[ctx.bw_id] = ""
            else:
                result.pacing_signals[ctx.bw_id] = "OVER" if ctx.pacing >= 1.0 else "UNDER"

    return result


# ─────────────────────────────────────────────────────────────────────────
# Row context
# ─────────────────────────────────────────────────────────────────────────


@dataclass
class _SelectCtvRowContext:
    bw_id: str
    deal_id: str
    end_date: str
    days_rem: Optional[int]
    pacing: Optional[float]
    cpm_bid: float
    floor: float                # single fee-adjusted floor (Max_Floor == Min_Floor)
    deal_cpm_li: float          # Deal_Clearing_CPM_On_LI (2026-08-14 metric)
    pub_cpm_li: float           # Pub_Clearing_CPM_On_LI
    pub_glob_cpm: float         # Pub_Global_Clearing_CPM
    l3_cpm: float                # Last_3_Days_Clearing_CPM (line-level fallback)
    pub_share: float            # Pub_Impression_Share_Pct (0-100)
    curr_mult: float
    is_zero_delivery: bool
    is_paused_active: bool
    paused_log_row_idx: Optional[int]
    rank_frac: float
    history: List[str]
    cfg: EngineConfig

    @staticmethod
    def from_row(
        row: pd.Series,
        cfg: EngineConfig,
        price_ranks: Dict[Tuple[str, str], float],
        history_map: Dict[str, List[str]],
        zero_delivery: Set[str],
        paused_active: Dict[str, int],
    ) -> "_SelectCtvRowContext":
        bw_id = str(row["BW_Line_Item_ID"]).strip()
        deal_id = str(row["Deal_ID"]).strip()
        days_rem_raw = row.get("Days_Remaining", "")
        days_rem = int(days_rem_raw) if str(days_rem_raw).strip() not in ("", "nan") else None
        pacing = _parse_pacing(row.get("Pacing_Pct", ""))
        return _SelectCtvRowContext(
            bw_id=bw_id,
            deal_id=deal_id,
            end_date=str(row.get("End_Date", "") or ""),
            days_rem=days_rem,
            pacing=pacing,
            cpm_bid=_safe_float(row.get("CPM_Bid")),
            floor=_safe_float(row.get("Floor_Price")),
            deal_cpm_li=_safe_float(row.get("Deal_Clearing_CPM_On_LI")),
            pub_cpm_li=_safe_float(row.get("Pub_Clearing_CPM_On_LI")),
            pub_glob_cpm=_safe_float(row.get("Pub_Global_Clearing_CPM")),
            l3_cpm=_safe_float(row.get("Last_3_Days_Clearing_CPM")),
            pub_share=_safe_float(row.get("Pub_Impression_Share_Pct")),
            curr_mult=_safe_float(row.get("Current_Multiplier")) or 1.0,
            is_zero_delivery=bw_id in zero_delivery,
            is_paused_active=bw_id in paused_active,
            paused_log_row_idx=paused_active.get(bw_id),
            rank_frac=price_ranks.get((bw_id, deal_id), 0.5),
            history=history_map.get(bw_id, []),
            cfg=cfg,
        )


# ─────────────────────────────────────────────────────────────────────────
# Per-row decision cascade
# ─────────────────────────────────────────────────────────────────────────


def _decide_one_select_ctv(
    ctx: _SelectCtvRowContext,
    result: EngineResult,
    first_run_seen: Set[str],
    second_run_seen: Set[str],
    seen_first: Set[str],
    seen_second: Set[str],
    seen_pause_write: Set[str],
    seen_resume_write: Set[str],
    seen_pre_flight: Set[str],
) -> None:
    f = ctx.cfg.floor
    cfg = ctx.cfg

    # ── STEP 0: PRE_FLIGHT_HOLD ────────────────────────────────────────────
    is_pre_flight = (
        ctx.is_zero_delivery
        and ctx.pacing is not None
        and ctx.pacing < 0.001
        and (ctx.days_rem is None or ctx.days_rem > 3)
    )
    if is_pre_flight:
        if ctx.bw_id in first_run_seen and ctx.bw_id not in seen_pre_flight:
            seen_pre_flight.add(ctx.bw_id)
            result.pre_flight_resets.append(ctx.bw_id)
        pf_hold = 1.0
        if ctx.cpm_bid > 0:
            base = ctx.pub_glob_cpm if ctx.pub_glob_cpm > 0 else ctx.floor
            pf_hold = round(min(
                cfg.hard_max.normal, max(0.01, (base * f.max_floor_mult) / ctx.cpm_bid),
            ), 3)
        _append(result, ctx, pf_hold, "PRE_FLIGHT_HOLD",
                f"PRE_FLIGHT_HOLD — line has never delivered (pacing ~0%, zero imps "
                f"yesterday, {ctx.days_rem if ctx.days_rem is not None else 'no end date'} "
                f"days remaining). Holding at Day 1 baseline {pf_hold}×.")
        return

    # ── STEP 0: LINE_PAUSED_HOLDING — paused + still zero (frozen) ────────
    if ctx.is_paused_active and ctx.is_zero_delivery:
        _append(result, ctx, ctx.curr_mult, "LINE_PAUSED_HOLDING",
                f"LINE_PAUSED_HOLDING — zero delivery confirmed again. "
                f"Holding at {ctx.curr_mult:.3f}×.")
        return

    # ── STEP 0: resumed — mark resumed, fall through to normal cascade ────
    # (No dedicated LINE_RESUMED reason code in Select CTV — Day 1 baseline
    # naturally re-fires below since the newly-detected pause already
    # removed this BW LI from first_run_seen/second_run_seen.)
    if ctx.is_paused_active and not ctx.is_zero_delivery:
        if ctx.bw_id not in seen_resume_write:
            seen_resume_write.add(ctx.bw_id)
            row_idx = ctx.paused_log_row_idx
            if row_idx is not None and row_idx not in result.resumed_row_indices:
                result.resumed_row_indices.append(row_idx)
        # no `return` — continues into the guardrails / priority cascade below

    # ── STEP 0: LINE_PAUSED — newly detected zero delivery ────────────────
    elif not ctx.is_paused_active and ctx.is_zero_delivery:
        if ctx.bw_id not in seen_pause_write:
            seen_pause_write.add(ctx.bw_id)
            result.new_pauses.append((ctx.bw_id, ctx.end_date))
        # Remove from First/Second Run logs so Day 1 naturally re-fires on
        # resume (mirrors sboRemoveFromFirstAndSecondRunLog_). Re-uses the
        # same state_apply.py effect as PRE_FLIGHT_HOLD's reset list — both
        # mean "delete this BW LI from first_run_log and second_run_log."
        if ctx.bw_id not in seen_pre_flight:
            seen_pre_flight.add(ctx.bw_id)
            result.pre_flight_resets.append(ctx.bw_id)
        result.pause_snapshots.append({
            "BW_Line_Item_ID": ctx.bw_id,
            "Deal_ID": ctx.deal_id,
            "Paused_Date": datetime.now(),
            "Held_Multiplier": ctx.curr_mult,
            "Basis": "newly detected zero delivery — held unchanged",
        })
        _append(result, ctx, ctx.curr_mult, "LINE_PAUSED",
                f"LINE_PAUSED — zero delivery detected. Held at {ctx.curr_mult:.3f}× "
                f"(unchanged).")
        return

    # ── Sanity guard: no CPM bid or no floor data ─────────────────────────
    if ctx.cpm_bid <= 0 or ctx.floor <= 0:
        _append(result, ctx, 0.0, "NO_FLOOR_CPM_DATA",
                "No floor/CPM data on this deal — skipping.")
        return

    # ── Guardrail thresholds (floor already fee-adjusted) ─────────────────
    base_mult = min((ctx.floor * f.max_floor_mult) / ctx.cpm_bid, cfg.hard_max.normal)
    norm_min = max((ctx.floor * f.norm_min_floor_mult) / ctx.cpm_bid, 0.01)
    throttle_mult = (ctx.floor * f.throttle_mult) / ctx.cpm_bid
    kill_mult = (ctx.floor * f.kill_mult) / ctx.cpm_bid

    # ── History trend modifier ─────────────────────────────────────────────
    good = sum(1 for h in ctx.history if h in ("OVER", "GOOD"))
    under = sum(1 for h in ctx.history if h == "UNDER")
    n = len(ctx.history)
    sustained4 = n >= 4 and (good == 4 or under == 4)
    mostly3 = n >= 3 and (good >= 3 or under >= 3)
    hist_trend = 1.4 if sustained4 else (1.2 if mostly3 else 1.0)

    # ── Publisher cap — uniform 40% for every line (no 537 exception) ─────
    pub_cap = cfg.pub_cap_other
    pub_frac = ctx.pub_share / 100.0
    within_eoc_delivery_window = (
        ctx.pacing is not None and ctx.pacing < 1.0
        and ctx.days_rem is not None and ctx.days_rem <= 7
    )

    new_mult: Optional[float] = None
    reason_code: Optional[str] = None
    reason_text: Optional[str] = None

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY A — FIRST_RUN
    # ══════════════════════════════════════════════════════════════════
    if ctx.bw_id not in first_run_seen:
        if ctx.days_rem is not None and ctx.days_rem <= 3:
            new_mult = 1.0
            reason_code = "FIRST_RUN_SHORT"
            reason_text = (
                f"Day 1 — <=3 days remaining ({ctx.days_rem}d). Holding 1.0×. "
                f"Day 1 + Day 2 both marked done."
            )
            if ctx.bw_id not in seen_first:
                seen_first.add(ctx.bw_id)
                result.new_first_run.append((ctx.bw_id, ctx.end_date))
            if ctx.bw_id not in seen_second:
                seen_second.add(ctx.bw_id)
                result.new_second_run.append((ctx.bw_id, ctx.end_date))
        else:
            new_mult = round(base_mult, 3)
            reason_code = "FIRST_RUN"
            reason_text = (
                f"Day 1 baseline — floor ${ctx.floor:.2f} × {f.max_floor_mult} / "
                f"CPM ${ctx.cpm_bid:.2f} = {new_mult}×."
            )
            if ctx.bw_id not in seen_first:
                seen_first.add(ctx.bw_id)
                result.new_first_run.append((ctx.bw_id, ctx.end_date))

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY B — DAY 2
    # ══════════════════════════════════════════════════════════════════
    elif ctx.bw_id not in second_run_seen and (ctx.days_rem is None or ctx.days_rem > 3):
        if ctx.pub_cpm_li > 0:
            new_mult = round((ctx.pub_cpm_li * f.max_floor_mult) / ctx.cpm_bid, 3)
            reason_code = "DAY2_BASELINE"
            reason_text = (
                f"Day 2 — Pub_Clearing_CPM_On_LI ${ctx.pub_cpm_li:.2f} × "
                f"{f.max_floor_mult} / CPM ${ctx.cpm_bid:.2f} = {new_mult}×."
            )
        else:
            new_mult = round(base_mult, 3)
            reason_code = "DAY2_BASELINE_FALLBACK"
            reason_text = (
                f"Day 2 (no pub clearing CPM) — floor ${ctx.floor:.2f} × "
                f"{f.max_floor_mult} / CPM ${ctx.cpm_bid:.2f} = {new_mult}×."
            )
        if ctx.bw_id not in seen_second:
            seen_second.add(ctx.bw_id)
            result.new_second_run.append((ctx.bw_id, ctx.end_date))

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY 1 — LAST 3 DAYS
    # ══════════════════════════════════════════════════════════════════
    elif ctx.days_rem is not None and ctx.days_rem <= 3:
        if ctx.pacing is None or ctx.pacing >= 1.0:
            new_mult = ctx.curr_mult
            reason_code = "LAST_3_DAYS_HOLD"
            reason_text = (
                f"Last 3 days, on pace "
                f"({round(ctx.pacing * 100) if ctx.pacing is not None else '?'}%) — "
                f"holding {ctx.curr_mult:.3f}×. Days left: {ctx.days_rem}."
            )
        else:
            base_up = 0.10 if ctx.pacing >= 0.90 else (0.20 if ctx.pacing >= 0.75 else 0.30)
            eoc_boost = 1.10 if ctx.pacing >= 0.90 else (1.20 if ctx.pacing >= 0.75 else 1.30)
            base_up = base_up * eoc_boost * hist_trend
            direction = "up_severe" if ctx.pacing < 0.75 else "up_normal"
            tier_mod = _price_tier_mod(ctx.rank_frac, direction, cfg)
            total_up = base_up * tier_mod
            new_mult = ctx.curr_mult + total_up
            reason_code = "LAST_3_DAYS_UNDER"
            reason_text = (
                f"Last 3 days, underpacing {round(ctx.pacing * 100)}% — "
                f"raise +{total_up:.3f} (base {base_up:.3f} × tier {tier_mod:.3f}). "
                f"Days left: {ctx.days_rem}. {ctx.curr_mult:.3f} → {new_mult:.3f}."
            )

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY 2 — CAP_KILL (publisher over 40% cap)
    # ══════════════════════════════════════════════════════════════════
    elif pub_frac >= pub_cap and not within_eoc_delivery_window:
        new_mult = round(kill_mult, 3)
        reason_code = "CAP_KILL"
        reason_text = (
            f"Publisher over {pub_cap * 100:.0f}% impression-share cap "
            f"({ctx.pub_share:.1f}%) — hard kill at {new_mult:.3f}×."
        )

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY 3 — CAP_THROTTLE (approaching 40% cap)
    # ══════════════════════════════════════════════════════════════════
    elif pub_frac >= pub_cap * 0.8 and not within_eoc_delivery_window:
        cap_prog = max(0.0, min(1.0, (pub_frac - pub_cap * 0.8) / (pub_cap * 0.2)))
        new_mult = round(max(
            throttle_mult, ctx.curr_mult - (ctx.curr_mult - throttle_mult) * cap_prog,
        ), 3)
        reason_code = "CAP_THROTTLE"
        reason_text = (
            f"Approaching {pub_cap * 100:.0f}% publisher cap ({ctx.pub_share:.1f}%) — "
            f"soft throttle. Progress: {cap_prog * 100:.0f}%. "
            f"{ctx.curr_mult:.3f} → {new_mult:.3f}×."
        )

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY 5 — Pacing + continuous price-tier (+ margin-health trim)
    # ══════════════════════════════════════════════════════════════════
    elif ctx.pacing is not None:
        new_mult, reason_code, reason_text = _normal_pacing_select_ctv(
            ctx, norm_min, kill_mult, hist_trend,
        )

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY 6 — NO_PACING
    # ══════════════════════════════════════════════════════════════════
    else:
        new_mult = ctx.curr_mult
        reason_code = "NO_PACING"
        reason_text = f"No pacing data — holding {ctx.curr_mult:.3f}. Run Update Pacing first."

    # ── Final clamp: kill_mult floor, OPT_HARD_MAX_LAST3 (2.00) ceiling ───
    clamped_kill_floor = min(kill_mult, cfg.hard_max.last3)
    clamped = round(max(clamped_kill_floor, min(cfg.hard_max.last3, new_mult)), 3)

    # ── Kill/Unkill event logging ──────────────────────────────────────────
    is_kill = reason_code == "CAP_KILL"
    was_killed = ctx.curr_mult <= kill_mult + 0.001
    if is_kill and not was_killed:
        result.kill_log_entries.append([
            ctx.bw_id, "PUB_KILL", datetime.now(), ctx.deal_id, "", ctx.end_date,
        ])
    if (
        was_killed and not is_kill
        and reason_code is not None and "PACE_UP" in reason_code
        and ctx.days_rem is not None and 3 < ctx.days_rem <= 7
    ):
        result.kill_log_entries.append([
            ctx.bw_id, "PUB_UNKILL", datetime.now(), ctx.deal_id, "", ctx.end_date,
        ])

    _append(result, ctx, clamped, reason_code, reason_text)


# ─────────────────────────────────────────────────────────────────────────
# Priority 5 — normal pacing engine (+ on-target margin-health trim)
# ─────────────────────────────────────────────────────────────────────────


def _normal_pacing_select_ctv(
    ctx: _SelectCtvRowContext,
    norm_min: float,
    kill_mult: float,
    hist_trend: float,
) -> Tuple[float, str, str]:
    cfg = ctx.cfg
    pacing = ctx.pacing or 0.0

    if pacing >= 1.0:
        # ── Overpacing (down) ──────────────────────────────────────────
        if ctx.days_rem is not None and ctx.days_rem <= 7:
            if pacing < 1.05:
                return ctx.curr_mult, "PACE_HOLD_ONTARGET", (
                    f"Pacing {round(pacing * 100)}% (100-105%, <=7d) — on target. "
                    f"Holding {ctx.curr_mult:.3f}."
                )
            base_down = 0.10 if pacing < 1.15 else (0.15 if pacing < 1.25 else 0.20)
            base_down *= hist_trend
            tier_down = _price_tier_mod(ctx.rank_frac, "down", cfg)
            total_down = min(base_down * tier_down * 0.5, cfg.max_single_day_down)
            effective_norm_min = min(norm_min, ctx.curr_mult - 0.001)
            new = round(max(effective_norm_min, ctx.curr_mult - total_down), 3)
            code = "PACE_DOWN_MOD" if pacing < 1.15 else "PACE_DOWN_AGG"
            return new, code, (
                f"Pacing {round(pacing * 100)}% (<=7d, 0.5x dampener) — "
                f"down -{total_down:.3f}. {ctx.curr_mult:.3f} -> {new:.3f}."
            )

        # >7 days (or no end date)
        if pacing < 1.05:
            # 2026-08-14: on-target margin-health trim. Uses Deal_Clearing_CPM_On_LI
            # (line-specific, all-time from ATR), falling back to
            # Last_3_Days_Clearing_CPM (line-level blended) if unavailable.
            cpm_for_margin = ctx.deal_cpm_li if ctx.deal_cpm_li > 0 else ctx.l3_cpm
            margin = (
                (ctx.cpm_bid - cpm_for_margin) / ctx.cpm_bid
                if cpm_for_margin > 0 and ctx.cpm_bid > 0 else None
            )
            if margin is None:
                return ctx.curr_mult, "PACE_HOLD_ONTARGET", (
                    f"Pacing {round(pacing * 100)}% (100-105%) — within target band. "
                    f"No L3/deal CPM data, holding at {ctx.curr_mult:.3f}."
                )
            margin_step = cfg.margin_trim.healthy_step if margin >= cfg.margin_trim.healthy_margin_threshold else cfg.margin_trim.thin_step
            margin_trim = round(max(norm_min, ctx.curr_mult - margin_step), 3)
            if margin_trim >= ctx.curr_mult:
                return ctx.curr_mult, "PACE_HOLD_ONTARGET", (
                    f"Pacing {round(pacing * 100)}% (100-105%) — margin "
                    f"{margin * 100:.1f}%. Already at normMin floor, no trim possible. "
                    f"Holding at {ctx.curr_mult:.3f}."
                )
            src_label = (
                f"Deal CPM on LI ${ctx.deal_cpm_li:.2f}" if ctx.deal_cpm_li > 0
                else f"Line L3 CPM ${ctx.l3_cpm:.2f} [fallback]"
            )
            return margin_trim, "PACE_HOLD_MARGIN_TRIM", (
                f"Pacing {round(pacing * 100)}% (100-105%) — margin {margin * 100:.1f}% "
                f"({'>=6%, slow trim' if margin >= cfg.margin_trim.healthy_margin_threshold else '<6%, faster trim'}). "
                f"Bid down -{margin_step:.3f} ({src_label} vs bid ${ctx.cpm_bid:.2f}). "
                f"{ctx.curr_mult:.3f} -> {margin_trim:.3f}."
            )

        base_down = 0.10 if pacing < 1.15 else (0.15 if pacing < 1.25 else 0.20)
        base_down *= hist_trend
        tier_down = _price_tier_mod(ctx.rank_frac, "down", cfg)
        total_down = min(base_down * tier_down, cfg.max_single_day_down)
        effective_norm_min = min(norm_min, ctx.curr_mult - 0.001)
        new = round(max(effective_norm_min, ctx.curr_mult - total_down), 3)
        code = "PACE_DOWN_MOD" if pacing < 1.15 else "PACE_DOWN_AGG"
        return new, code, (
            f"Pacing {round(pacing * 100)}% — down -{total_down:.3f} "
            f"(base {base_down:.3f} x tier {tier_down:.3f}). Trend {hist_trend:.1f}x. "
            f"{ctx.curr_mult:.3f} -> {new:.3f}."
        )

    # ── Underpacing (up) ────────────────────────────────────────────────
    severe = pacing < 0.75
    time_tag = ""
    if ctx.days_rem is not None and ctx.days_rem <= 7:
        base_up = 0.10 if pacing >= 0.90 else (0.20 if pacing >= 0.75 else 0.30)
        eoc_boost = 1.10 if pacing >= 0.90 else (1.20 if pacing >= 0.75 else 1.30)
        base_up *= eoc_boost
        time_tag = "<=7d EOC"
    elif ctx.days_rem is not None and ctx.days_rem <= 14:
        base_up = 0.15 if pacing >= 0.90 else (0.25 if pacing >= 0.75 else 0.35)
        eoc_boost = 1.0
        time_tag = "<=14d"
    else:
        base_up = 0.10 if pacing >= 0.90 else (0.20 if pacing >= 0.75 else 0.30)
        eoc_boost = 1.0

    base_up *= hist_trend
    direction = "up_severe" if severe else "up_normal"
    tier_up = _price_tier_mod(ctx.rank_frac, direction, ctx.cfg)
    total_up = base_up * tier_up

    effective_floor = kill_mult
    if ctx.days_rem is not None and ctx.days_rem <= 7 and severe:
        effective_floor = 0.001
        time_tag += " [kill-override: severe pace]"

    effective_hard_max = ctx.cfg.hard_max.severe if severe else ctx.cfg.hard_max.normal
    new = round(min(effective_hard_max, max(effective_floor, ctx.curr_mult + total_up)), 3)
    code = "PACE_UP_MOD" if pacing >= 0.90 else ("PACE_UP_AGG" if pacing >= 0.75 else "PACE_UP_CRITICAL")
    return new, code, (
        f"Pacing {round(pacing * 100)}%"
        f"{', ' + str(ctx.days_rem) + 'd rem' if ctx.days_rem is not None else ''}"
        f"{' (' + time_tag + ')' if time_tag else ''}. "
        f"Raise +{total_up:.3f} (base {base_up:.3f} x tier {tier_up:.3f}). "
        f"Trend {hist_trend:.1f}x. {ctx.curr_mult:.3f} -> {new:.3f}."
    )


# ─────────────────────────────────────────────────────────────────────────
# Helper: append a Decision to the result
# ─────────────────────────────────────────────────────────────────────────


def _append(
    result: EngineResult,
    ctx: _SelectCtvRowContext,
    mult: float,
    code: str,
    text: str,
) -> None:
    result.decisions.append(Decision(
        bw_id=ctx.bw_id,
        deal_id=ctx.deal_id,
        new_multiplier=mult,
        effective_bid_current=round(ctx.cpm_bid * ctx.curr_mult, 2),
        effective_bid_new=round(ctx.cpm_bid * mult, 2),
        reason_code=code,
        reason_text=text,
    ))
