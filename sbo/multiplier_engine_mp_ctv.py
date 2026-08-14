"""Smart Bid multiplier decision engine — Marketplace CTV tactic.

Port of `calculateNewMultipliers` (MP CTV Apps Script Section 17).

Unlike the Podcast/Streaming engine, MP CTV operates on a per-deal
publisher/category share model rather than a modifier-category dollar cap
model.  The two engines are kept in separate files so changes to one
cannot accidentally break the other.

Priority cascade (in order — first matching branch wins):
    CASE 0  PRE_FLIGHT_HOLD          zero delivery + ~0% pacing + >3d rem
    CASE 1  LINE_PAUSED_HOLDING      paused + still zero
    CASE 2  LINE_RESUMED             paused + delivery returned
    CASE 3  LINE_PAUSED              newly zero delivery
    PRI A   FIRST_RUN[_SHORT]        Day 1 baseline
    PRI B   DAY2_BASELINE[_FALLBACK] Day 2 CPM targeting
    PRI 1   LAST_3_DAYS_HOLD/UNDER   days_rem ≤ 3
    PRI 1.5 PRICE_KILL/UNKILL/HOLD   price-based deal kill system
    PRI 2   CAP_KILL                 publisher share hard kill (floor × 0.10)
    PRI 3   CAP_THROTTLE[_HOLD]      approaching publisher cap
    PRI 4   CAT_CAP_KILL/THROTTLE    category share cap (537 lines only)
    PRI 5   PACE_*                   normal pacing + continuous price tier
    PRI 6   NO_PACING                fallback — hold current

Final clamp: hard_max ceiling, kill_mult floor.
CAP_KILL and CAT_CAP_KILL use a lower cap_kill floor (floor × 0.10).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from sbo.config_models import EngineConfig

# Re-use the Decision / EngineResult dataclasses from the existing engine
# to keep the pipeline orchestrator interface identical.
from sbo.multiplier_engine import (
    Decision,
    EngineResult,
    _build_history_map,
    _build_price_ranks,
    _is_weekend_window_est,
    _parse_pacing,
    _price_tier_mod,
    _safe_float,
)


# ─────────────────────────────────────────────────────────────────────────
# Public entry point  (same signature as decide_multipliers)
# ─────────────────────────────────────────────────────────────────────────


def decide_multipliers_mp_ctv(
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
    price_kill_actions: pd.DataFrame | None = None,
) -> EngineResult:
    """Apply the full MP CTV cascade to every row.

    price_kill_actions: optional DataFrame produced by
    build_price_kill_staging() with columns
    [BW_Line_Item_ID, Deal_ID, Action, Restart_Multiplier].
    If None, price-kill logic is skipped (Priority 1.5 never fires).
    """
    result = EngineResult()
    if optimizer_df.empty:
        return result

    price_ranks  = _build_price_ranks(optimizer_df)
    history_map  = _build_history_map(pacing_history, max_runs=4)

    # Build price-kill action lookup: (bw_id, deal_id) → {action, restart_mult}
    pk_map: Dict[Tuple[str, str], Dict] = {}
    if price_kill_actions is not None and not price_kill_actions.empty:
        for _, pkr in price_kill_actions.iterrows():
            key = (str(pkr["BW_Line_Item_ID"]).strip(), str(pkr["Deal_ID"]).strip())
            pk_map[key] = {
                "action": str(pkr.get("Action", "")),
                "restart_mult": _safe_float(pkr.get("Restart_Multiplier", 0)),
            }

    # Per-LI median CPM (for price-tier context in reason text)
    li_median_cpm = _build_li_median_cpm(optimizer_df)

    seen_first:        Set[str] = set()
    seen_second:       Set[str] = set()
    seen_pause_write:  Set[str] = set()
    seen_resume_write: Set[str] = set()
    seen_pre_flight:   Set[str] = set()

    is_weekend = _is_weekend_window_est(run_date)

    marketplace_set = set(cfg.marketplace_list_ids)

    for _, row in optimizer_df.iterrows():
        ctx = _MCTVRowContext.from_row(
            row, cfg, price_ranks, history_map,
            zero_delivery, paused_active, is_weekend, marketplace_set,
        )
        _decide_one_mp_ctv(
            ctx, result, pk_map, li_median_cpm,
            first_run_seen, second_run_seen,
            seen_first, seen_second,
            seen_pause_write, seen_resume_write, seen_pre_flight,
            paused_snapshot_map=paused_snapshot_map or {},
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
class _MCTVRowContext:
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
    pub_share: float        # Pub_Impression_Share_Pct (0–100)
    pub_name: str
    cat_share: float        # Category_Impression_Share_Pct (0–100)
    targets_537: bool       # True if line targets deal list 537
    targets_marketplace: bool  # True if at least one marketplace list is targeted
    curr_mult: float
    is_zero_delivery: bool
    is_paused_active: bool
    paused_log_row_idx: Optional[int]
    rank_frac: float
    history: List[str]
    is_weekend: bool
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
        marketplace_set: Set[str],
    ) -> "_MCTVRowContext":
        bw_id   = str(row["BW_Line_Item_ID"]).strip()
        deal_id = str(row["Deal_ID"]).strip()

        days_rem_raw = row.get("Days_Remaining", "")
        days_rem = int(days_rem_raw) if str(days_rem_raw).strip() not in ("", "nan") else None

        pacing = _parse_pacing(row.get("Pacing_Pct", ""))

        # Targets_537 column: value is the deal list ID string '537' when the
        # line targets it, or 'NONE' when it doesn't target any marketplace list.
        targets_raw = str(row.get("Targets_537", "") or "").strip()
        # Three-way value: "537" | "MARKETPLACE" | "NONE"
        targets_537        = targets_raw == "537"
        targets_marketplace = targets_raw in ("537", "MARKETPLACE")

        return _MCTVRowContext(
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
            pub_name=str(row.get("Publisher", "") or ""),
            cat_share=_safe_float(row.get("Category_Share_Pct")),
            targets_537=targets_537,
            targets_marketplace=targets_marketplace,
            curr_mult=_safe_float(row.get("Current_Multiplier")) or 1.0,
            is_zero_delivery=bw_id in zero_delivery,
            is_paused_active=bw_id in paused_active,
            paused_log_row_idx=paused_active.get(bw_id),
            rank_frac=price_ranks.get((bw_id, deal_id), 0.5),
            history=history_map.get(bw_id, []),
            is_weekend=is_weekend,
            cfg=cfg,
        )


# ─────────────────────────────────────────────────────────────────────────
# Per-row decision cascade
# ─────────────────────────────────────────────────────────────────────────


def _decide_one_mp_ctv(
    ctx: _MCTVRowContext,
    result: EngineResult,
    pk_map: Dict[Tuple[str, str], Dict],
    li_median_cpm: Dict[str, float],
    first_run_seen: Set[str],
    second_run_seen: Set[str],
    seen_first: Set[str],
    seen_second: Set[str],
    seen_pause_write: Set[str],
    seen_resume_write: Set[str],
    seen_pre_flight: Set[str],
    paused_snapshot_map: Dict[Tuple[str, str], float],
) -> None:
    f   = ctx.cfg.floor
    cfg = ctx.cfg

    # ── CASE 0: PRE_FLIGHT_HOLD ───────────────────────────────────────────
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
        if ctx.cpm_bid > 0 and ctx.floor > 0:
            base = ctx.deal_glob_cpm if ctx.deal_glob_cpm > 0 else ctx.floor
            pf_hold = round(min(
                cfg.hard_max.normal,
                max(0.01, (base * f.max_floor_mult) / ctx.cpm_bid),
            ), 3)
        _append(result, ctx, pf_hold, "PRE_FLIGHT_HOLD",
                f"PRE_FLIGHT_HOLD — never delivered (pacing ~0%, zero imps). Holding {pf_hold}×.")
        return

    # ── CASE 1: LINE_PAUSED_HOLDING ───────────────────────────────────────
    if ctx.is_paused_active and ctx.is_zero_delivery:
        snap_key = (ctx.bw_id, ctx.deal_id)
        held = paused_snapshot_map.get(snap_key, ctx.curr_mult)
        _append(result, ctx, held, "LINE_PAUSED_HOLDING",
                f"LINE_PAUSED_HOLDING — zero delivery confirmed. Holding {held:.3f}×.")
        return

    # ── CASE 2: LINE_RESUMED ──────────────────────────────────────────────
    if ctx.is_paused_active and not ctx.is_zero_delivery:
        if ctx.bw_id not in seen_resume_write:
            seen_resume_write.add(ctx.bw_id)
            row_idx = ctx.paused_log_row_idx
            if row_idx is not None and row_idx not in result.resumed_row_indices:
                result.resumed_row_indices.append(row_idx)
        snap_key = (ctx.bw_id, ctx.deal_id)
        snapped  = paused_snapshot_map.get(snap_key)
        if snapped is not None:
            resume_mult = round(snapped, 2)
        elif ctx.deal_cpm_li > 0 and ctx.cpm_bid > 0:
            resume_mult = round(
                min(cfg.hard_max.normal, (ctx.deal_cpm_li * 1.2) / ctx.cpm_bid), 2
            )
        else:
            resume_mult = ctx.curr_mult
        _append(result, ctx, resume_mult, "LINE_RESUMED",
                f"LINE_RESUMED — delivery returned. Restart {resume_mult:.3f}× "
                f"(dealCpmLi ${ctx.deal_cpm_li:.2f} × 1.2 / CPM bid ${ctx.cpm_bid:.2f}).")
        return

    # ── CASE 3: LINE_PAUSED (newly zero) ─────────────────────────────────
    if not ctx.is_paused_active and ctx.is_zero_delivery:
        floor_prot = ctx.curr_mult
        if ctx.deal_cpm_li > 0 and ctx.cpm_bid > 0:
            floor_prot = round(
                min(cfg.hard_max.normal, (ctx.deal_cpm_li * 1.2) / ctx.cpm_bid), 3
            )
        held = round(max(ctx.curr_mult, floor_prot), 2)
        if ctx.bw_id not in seen_pause_write:
            seen_pause_write.add(ctx.bw_id)
            result.new_pauses.append((ctx.bw_id, ctx.end_date))
        result.pause_snapshots.append({
            "BW_Line_Item_ID": ctx.bw_id,
            "Deal_ID": ctx.deal_id,
            "Paused_Date": datetime.now(),
            "Held_Multiplier": held,
            "Basis": f"max(curr {ctx.curr_mult:.3f}×, dealCpmLi ${ctx.deal_cpm_li:.2f} × 1.2 / CPM ${ctx.cpm_bid:.2f})",
        })
        _append(result, ctx, held, "LINE_PAUSED",
                f"LINE_PAUSED — zero delivery. Held at {held:.3f}×.")
        return

    # ── Sanity guard ──────────────────────────────────────────────────────
    if ctx.cpm_bid <= 0:
        _append(result, ctx, 0.0, "NO_CPM_BID", "No CPM bid data — skipping.")
        return

    # Missing floor: use l3_cpm * 1.25 as effective floor.
    # If l3_cpm is also missing, fall back to deal_cpm_li * 1.25.
    # This ensures the engine never bids $0 just because floor data is absent.
    effective_floor = ctx.floor
    if effective_floor <= 0:
        if ctx.l3_cpm > 0:
            effective_floor = round(ctx.l3_cpm * 1.25, 2)
        elif ctx.deal_cpm_li > 0:
            effective_floor = round(ctx.deal_cpm_li * 1.25, 2)
        else:
            _append(result, ctx, 0.0, "NO_FLOOR",
                    "No floor or deal CPM data — skipping.")
            return

    # ── Pre-compute thresholds ────────────────────────────────────────────
    # Use effective_floor (falls back to deal_cpm*1.25 when floor is missing)
    norm_min        = max((effective_floor * f.norm_min_floor_mult) / ctx.cpm_bid, 0.01)
    throttle_mult   = (effective_floor * f.throttle_mult) / ctx.cpm_bid
    kill_mult       = (effective_floor * f.kill_mult) / ctx.cpm_bid
    cap_kill_mult   = (effective_floor * 0.10) / ctx.cpm_bid  # publisher/category hard kill

    # Dynamic max multiplier for this deal (based on deal CPM history)
    dyn_cpm_ceil  = max(ctx.deal_cpm_li, ctx.deal_glob_cpm)
    dynamic_max   = (
        round(min(cfg.hard_max.severe, (dyn_cpm_ceil * 1.5) / ctx.cpm_bid), 3)
        if dyn_cpm_ceil > 0 else cfg.hard_max.severe
    )

    # History counts
    good  = sum(1 for h in ctx.history if h in ("OVER", "GOOD"))
    under = sum(1 for h in ctx.history if h == "UNDER")
    n     = len(ctx.history)
    sustained4_good  = n >= 4 and good  == 4
    sustained4_under = n >= 4 and under == 4
    mostly3_good     = n >= 3 and good  >= 3
    mostly3_under    = n >= 3 and under >= 3
    if sustained4_good or sustained4_under:
        hist_trend = 1.4
    elif mostly3_good or mostly3_under:
        hist_trend = 1.2
    else:
        hist_trend = 1.0

    pub_cap  = cfg.pub_cap_537 if ctx.targets_537 else cfg.pub_cap_other
    pub_frac = ctx.pub_share / 100.0
    cat_frac = ctx.cat_share / 100.0
    skip_caps = not ctx.targets_marketplace

    # Current effective bid — used to detect whether a deal is already price-killed
    eff_bid_curr = ctx.cpm_bid * ctx.curr_mult
    is_killed    = eff_bid_curr < ctx.floor * 0.9

    pk_key    = (ctx.bw_id, ctx.deal_id)
    pk_action = pk_map.get(pk_key)

    # Category kill eligibility (Priority 4)
    cat_kill_eligible = (
        ctx.targets_537
        and not skip_caps
        and (sustained4_good or mostly3_good)
        and not (ctx.days_rem is not None and ctx.days_rem <= 3)
        and not (ctx.pacing is not None and ctx.pacing < 1.0
                 and ctx.days_rem is not None and ctx.days_rem <= 7)
        and (ctx.pacing is None or ctx.pacing >= 1.0)
        and cat_frac >= cfg.cat_kill_over
        and (ctx.l3_cpm <= 0 or ctx.deal_cpm_li > ctx.l3_cpm * 1.2)
    )

    median_cpm = li_median_cpm.get(ctx.bw_id, ctx.l3_cpm)
    hist_tag   = "[" + ",".join(ctx.history) + "]"

    new_mult:    Optional[float] = None
    reason_code: Optional[str]  = None
    reason_text: Optional[str]  = None

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY A — Day 1
    # ══════════════════════════════════════════════════════════════════
    if ctx.bw_id not in first_run_seen:
        if ctx.days_rem is not None and ctx.days_rem <= 3:
            new_mult    = 1.0
            reason_code = "FIRST_RUN_SHORT"
            reason_text = (
                f"Day 1 — ≤3 days remaining ({ctx.days_rem}d). "
                f"Holding 1.0×. Day 1 + Day 2 both marked done."
            )
            if ctx.bw_id not in seen_first:
                seen_first.add(ctx.bw_id)
                result.new_first_run.append((ctx.bw_id, ctx.end_date))
            if ctx.bw_id not in seen_second:
                seen_second.add(ctx.bw_id)
                result.new_second_run.append((ctx.bw_id, ctx.end_date))
        else:
            d1_src_cpm  = ctx.deal_glob_cpm if ctx.deal_glob_cpm > 0 else ctx.floor
            d1_label    = ("deal global CPM" if ctx.deal_glob_cpm > 0 else "floor") + f" ${d1_src_cpm:.2f} × 1.3"
            raw_d1      = (d1_src_cpm * f.max_floor_mult) / ctx.cpm_bid
            new_mult    = round(min(cfg.hard_max.normal, max(0.01, raw_d1)), 3)
            reason_code = "FIRST_RUN"
            reason_text = (
                f"Day 1 baseline — {d1_label} / CPM bid ${ctx.cpm_bid:.2f} = {new_mult}×."
            )
            if ctx.bw_id not in seen_first:
                seen_first.add(ctx.bw_id)
                result.new_first_run.append((ctx.bw_id, ctx.end_date))

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY B — Day 2
    # ══════════════════════════════════════════════════════════════════
    elif ctx.bw_id not in second_run_seen and (ctx.days_rem is None or ctx.days_rem > 3):
        # Source CPM: deal_cpm_li first, then deal_glob_cpm, then floor fallback.
        # deal_cpm_li can be 0 even when the column is populated if the deal had
        # no impressions yesterday (spend/imps = 0 in the ATR aggregation).
        d2_cpm   = ctx.deal_cpm_li if ctx.deal_cpm_li > 0 else ctx.deal_glob_cpm
        if d2_cpm > 0:
            raw_d2   = (d2_cpm * 1.1) / ctx.cpm_bid
            new_mult = round(min(cfg.hard_max.normal, max(0.01, raw_d2)), 3)
            reason_code = "DAY2_BASELINE"
            src_label = (
                f"deal CPM on LI ${ctx.deal_cpm_li:.2f}"
                if ctx.deal_cpm_li > 0
                else f"deal global CPM ${ctx.deal_glob_cpm:.2f} (LI CPM=0 fallback)"
            )
            reason_text = (
                f"Day 2 — {src_label} × 1.1 / "
                f"CPM bid ${ctx.cpm_bid:.2f} = {new_mult}×."
            )
        else:
            raw_d2fb = (effective_floor * f.max_floor_mult) / ctx.cpm_bid
            new_mult = round(min(cfg.hard_max.normal, max(0.01, raw_d2fb)), 3)
            reason_code = "DAY2_BASELINE_FALLBACK"
            reason_text = (
                f"Day 2 (no deal CPM) — floor ${effective_floor:.2f} × 1.3 / "
                f"CPM bid ${ctx.cpm_bid:.2f} = {new_mult}×."
            )
        if ctx.bw_id not in seen_second:
            seen_second.add(ctx.bw_id)
            result.new_second_run.append((ctx.bw_id, ctx.end_date))

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY 1 — Last 3 days remaining
    # ══════════════════════════════════════════════════════════════════
    elif ctx.days_rem is not None and ctx.days_rem <= 3:
        if ctx.pacing is None or ctx.pacing >= 1.0:
            new_mult    = ctx.curr_mult
            reason_code = "LAST_3_DAYS_HOLD"
            reason_text = (
                f"Last 3 days, on pace ({round(ctx.pacing * 100) if ctx.pacing is not None else '?'}%) — "
                f"holding {ctx.curr_mult:.3f}×. No decreases. Days left: {ctx.days_rem}."
            )
        else:
            pacing = ctx.pacing
            base_up   = 0.10 if pacing >= 0.90 else (0.20 if pacing >= 0.75 else 0.30)
            eoc_boost = 1.10 if pacing >= 0.90 else (1.20 if pacing >= 0.75 else 1.30)
            base_up   = base_up * eoc_boost * hist_trend
            sev       = pacing < 0.75
            tier_mod  = _price_tier_mod(ctx.rank_frac, "up_severe" if sev else "up_normal", cfg)
            total_up  = base_up * tier_mod
            start     = norm_min if ctx.curr_mult <= throttle_mult + 0.001 else ctx.curr_mult
            new_mult  = round(min(dynamic_max, max(norm_min, start + total_up)), 3)
            reason_code = "LAST_3_DAYS_UNDER"
            reason_text = (
                f"Last 3 days, underpacing {round(pacing * 100)}% — "
                f"raise +{total_up:.3f} "
                f"(base {0.10 if pacing >= 0.90 else (0.20 if pacing >= 0.75 else 0.30):.2f} "
                f"× EOC {eoc_boost:.2f} × trend {hist_trend:.1f} × tier {tier_mod:.3f}). "
                f"Days left: {ctx.days_rem}. {ctx.curr_mult:.3f} → {new_mult:.3f}."
            )

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY 1.5a — Force-unkill: deal CPM ≤ L3 × 1.2 (bypasses cat check)
    # Does NOT fire if publisher is over its impression share cap — pub cap
    # kill (Priority 2) always takes precedence over price unkill.
    # Only ≤3 days remaining exempts the pub cap check.
    # ══════════════════════════════════════════════════════════════════
    elif (
        not skip_caps
        and pk_action is not None
        and pk_action["action"] == "PRICE_UNKILL"
        and ctx.l3_cpm > 0
        and ctx.deal_cpm_li > 0
        and ctx.deal_cpm_li <= ctx.l3_cpm * 1.2
        and not (
            pub_frac >= pub_cap
            and not (ctx.days_rem is not None and ctx.days_rem <= 3)
        )
    ):
        rm = pk_action["restart_mult"]
        new_mult    = round(min(cfg.hard_max.severe, max(0.01, rm if rm > 0 else norm_min)), 3)
        reason_code = "PRICE_UNKILL"
        reason_text = (
            f"PRICE_UNKILL (force) — deal CPM ${ctx.deal_cpm_li:.2f} ≤ "
            f"L3 avg × 1.2 (${ctx.l3_cpm * 1.2:.2f}). Bypasses cat-kill guard. "
            f"Restart {new_mult}× (eff bid ${ctx.cpm_bid * new_mult:.2f})."
        )

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY 1.5b — Normal price-kill/unkill/hold (not cat-kill eligible
    # and not approaching category cap threshold)
    # ══════════════════════════════════════════════════════════════════
    elif (
        not skip_caps
        and not cat_kill_eligible
        and pk_action is not None
        and not (
            pub_frac >= pub_cap
            and not (ctx.days_rem is not None and ctx.days_rem <= 3)
        )
        and not (
            ctx.targets_537
            and cat_frac >= cfg.cat_cap_over * 0.8
            and not (ctx.days_rem is not None and ctx.days_rem <= 3)
        )
    ):
        action = pk_action["action"]
        if action == "PRICE_UNKILL":
            rm       = pk_action["restart_mult"]
            new_mult = round(min(cfg.hard_max.severe, max(0.01, rm if rm > 0 else norm_min)), 3)
            reason_code = "PRICE_UNKILL"
            reason_text = (
                f"PRICE_UNKILL — deal {ctx.deal_id} CPM ${ctx.deal_cpm_li:.2f} "
                f"(LI avg ${ctx.l3_cpm:.2f}). Restart {new_mult}×. "
                f"Pacing {round(ctx.pacing * 100) if ctx.pacing is not None else 'n/a'}%."
            )
        elif action == "PRICE_KILL":
            new_mult    = round(kill_mult, 3)
            reason_code = "PRICE_KILL"
            reason_text = (
                f"PRICE_KILL — deal {ctx.deal_id} CPM ${ctx.deal_cpm_li:.2f} "
                f"(LI avg ${ctx.l3_cpm:.2f}). "
                f"Pacing {round(ctx.pacing * 100) if ctx.pacing is not None else 'n/a'}%."
            )
        elif action == "PRICE_KILL_HOLD":
            new_mult    = round(kill_mult, 3)
            reason_code = "PRICE_KILL_HOLD"
            reason_text = (
                f"PRICE_KILL_HOLD — deal {ctx.deal_id} remains price-killed. "
                f"CPM ${ctx.deal_cpm_li:.2f} (LI avg ${ctx.l3_cpm:.2f}). "
                f"Pacing {round(ctx.pacing * 100) if ctx.pacing is not None else 'n/a'}%."
            )
        else:
            new_mult    = ctx.curr_mult
            reason_code = "NO_PACING"
            reason_text = f"Unknown price-kill action '{action}' — holding {ctx.curr_mult:.3f}×."

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY 2 — Over publisher cap → hard kill
    # ══════════════════════════════════════════════════════════════════
    elif (
        not skip_caps
        and pub_frac >= pub_cap
        and not (ctx.days_rem is not None and ctx.days_rem <= 3)
    ):
        new_mult    = round(cap_kill_mult, 3)
        reason_code = "CAP_KILL"
        reason_text = (
            f"Publisher over {pub_cap * 100:.0f}% cap "
            f"({ctx.pub_share:.1f}%, pub: {ctx.pub_name}) — "
            f"hard kill (floor × 0.10 = {new_mult:.3f}×)."
        )

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY 3 — Approaching publisher cap → soft throttle
    # ══════════════════════════════════════════════════════════════════
    elif (
        not skip_caps
        and pub_frac >= pub_cap * 0.8
        and not (ctx.days_rem is not None and ctx.days_rem <= 3)
        and ctx.curr_mult > kill_mult * 1.05
        and not (ctx.l3_cpm > 0 and ctx.deal_cpm_li > 0 and ctx.deal_cpm_li <= ctx.l3_cpm * 1.2)
        and not cat_kill_eligible
    ):
        cap_prog = (pub_frac - pub_cap * 0.8) / (pub_cap * 0.2)
        if ctx.curr_mult <= throttle_mult:
            new_mult    = ctx.curr_mult
            reason_code = "CAP_THROTTLE_HOLD"
            reason_text = (
                f"Approaching pub cap ({ctx.pub_share:.1f}% of {pub_cap * 100:.0f}%) "
                f"— at/below throttle level, holding {ctx.curr_mult:.3f}×."
            )
        else:
            new_mult = round(max(
                throttle_mult,
                ctx.curr_mult - (ctx.curr_mult - throttle_mult) * cap_prog,
            ), 3)
            reason_code = "CAP_THROTTLE"
            reason_text = (
                f"Approaching pub cap ({ctx.pub_share:.1f}% of {pub_cap * 100:.0f}%) "
                f"— soft throttle. Progress: {cap_prog * 100:.0f}%. "
                f"{ctx.curr_mult:.3f} → {new_mult:.3f}×."
            )

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY 4 — Category cap (537-targeted lines only)
    # ══════════════════════════════════════════════════════════════════
    elif (
        not skip_caps
        and ctx.targets_537
        and not (ctx.days_rem is not None and ctx.days_rem <= 3)
        and not (ctx.pacing is not None and ctx.pacing < 1.0
                 and ctx.days_rem is not None and ctx.days_rem <= 7)
        and ctx.cat_share > 0
        and cat_frac >= (
            (cfg.cat_cap_under if ctx.pacing is not None and ctx.pacing < 1.0
             else cfg.cat_cap_over) * 0.8
        )
    ):
        cat_threshold = (
            cfg.cat_cap_under if ctx.pacing is not None and ctx.pacing < 1.0
            else cfg.cat_cap_over
        )
        throttle_entry = cat_threshold * 0.8
        above_l3_for_kill = ctx.l3_cpm <= 0 or ctx.deal_cpm_li > ctx.l3_cpm * 1.2
        # Kill fires when:
        # - All structural conditions met (537, not <=3 days, overpacing, above l3*1.2)
        # - Category share is above kill threshold (cfg.cat_kill_over = 30%)
        # Kill always takes priority over throttle per the rule:
        # any eligible kill > any throttle.
        cat_kill_cond = (
            cat_kill_eligible
            and cat_frac >= cfg.cat_kill_over
            and above_l3_for_kill
        )


        if cat_kill_cond:
            new_mult    = round(cap_kill_mult, 3)
            reason_code = "CAT_CAP_KILL"
            reason_text = (
                f"Category kill — "
                f"{'4/4' if sustained4_good else '3/4'} good history and "
                f"cat share {ctx.cat_share:.1f}% ≥ "
                f"{cfg.cat_kill_over * 100:.0f}% threshold. "
                f"Kill at floor × 0.10 = {new_mult:.3f}×."
            )
        elif cat_frac >= throttle_entry:
            cat_prog = min(1.0, (cat_frac - throttle_entry) / (cat_threshold - throttle_entry))
            # If overpacing and deal is at/below kill level — hold at kill level.
            # Cat cap throttle must never RAISE a killed deal when overpacing.
            if (ctx.pacing is None or ctx.pacing >= 1.0) and ctx.curr_mult <= kill_mult * 1.05:
                new_mult    = ctx.curr_mult
                reason_code = "PRICE_KILL_HOLD"
                reason_text = (
                    f"PRICE_KILL_HOLD — deal at kill level ({ctx.curr_mult:.3f}×), "
                    f"overpacing {round(ctx.pacing*100) if ctx.pacing else '?'}%. "
                    f"Cat cap ({ctx.cat_share:.1f}%) does not raise killed deals."
                )
            elif ctx.curr_mult <= norm_min:
                new_mult    = norm_min
                reason_code = "CAT_CAP_THROTTLE_HOLD"
                reason_text = (
                    f"Category approaching {cat_threshold * 100:.0f}% cap "
                    f"({ctx.cat_share:.1f}%) — at norm_min floor, holding {norm_min:.3f}×."
                )
            elif ctx.curr_mult <= throttle_mult:
                new_mult    = ctx.curr_mult
                reason_code = "CAT_CAP_THROTTLE_HOLD"
                reason_text = (
                    f"Category approaching {cat_threshold * 100:.0f}% cap "
                    f"({ctx.cat_share:.1f}%) — at/below throttle level, holding."
                )
            else:
                new_mult = round(max(
                    throttle_mult,
                    ctx.curr_mult - (ctx.curr_mult - throttle_mult) * cat_prog * 0.7,
                ), 3)
                reason_code = "CAT_CAP_THROTTLE"
                reason_text = (
                    f"Category approaching {cat_threshold * 100:.0f}% cap "
                    f"({ctx.cat_share:.1f}%) — throttling. "
                    f"Progress: {cat_prog * 100:.0f}%. "
                    f"{ctx.curr_mult:.3f} → {new_mult:.3f}×."
                )
        else:
            # Entry condition was met but neither kill nor throttle path hit —
            # fall through to Priority 5 by leaving new_mult as None
            pass

    # ══════════════════════════════════════════════════════════════════
    # PRIORITY 5 — Pacing + continuous price-tier
    # ══════════════════════════════════════════════════════════════════
    if new_mult is None:
        if ctx.pacing is None:
            # Priority 6
            new_mult    = ctx.curr_mult
            reason_code = "NO_PACING"
            reason_text = f"No pacing data — holding {ctx.curr_mult:.3f}×."
        else:
            new_mult, reason_code, reason_text = _normal_pacing_mp_ctv(
                ctx, norm_min, kill_mult, throttle_mult, dynamic_max,
                hist_trend, pk_map, cfg,
            )

    # ── Final clamp ───────────────────────────────────────────────────────
    # CAP_KILL / CAT_CAP_KILL use cap_kill_mult floor (floor × 0.10).
    # PRICE_KILL_HOLD_FALLBACK holds at curr_mult regardless.
    # All others floor at kill_mult (floor × 0.75).
    if reason_code == "PRICE_KILL_HOLD_FALLBACK":
        clamped = round(min(cfg.hard_max.severe, new_mult), 3)
    elif reason_code in ("CAP_KILL", "CAT_CAP_KILL"):
        clamped = round(max(cap_kill_mult, min(cfg.hard_max.severe, new_mult)), 3)
    else:
        clamped = round(max(kill_mult, min(cfg.hard_max.severe, new_mult)), 3)

    # ── Below-L3 protection (hard override) ──────────────────────────────
    # If deal_cpm_li < l3_cpm AND pub is not over its cap, this deal must
    # never sit at or below kill level regardless of any prior decision.
    # Raise to norm_min (floor × 1.1 / cpm_bid) as the minimum.
    # This rule is absolute — no scenario justifies killing a deal that
    # clears below the LI average and is not over the publisher cap.
    # Effective bid check: cpm_bid * multiplier vs floor price.
    # A deal is "effectively killed" if its bid is below floor regardless of
    # whether the multiplier itself is at the kill_mult threshold.
    eff_bid_after = ctx.cpm_bid * clamped
    is_below_l3_final = (
        ctx.l3_cpm > 0
        and ctx.deal_cpm_li > 0
        and ctx.deal_cpm_li <= ctx.l3_cpm * 1.2
        and not (pub_frac >= pub_cap and not (ctx.days_rem is not None and ctx.days_rem <= 3))
        and reason_code not in ("CAP_KILL", "CAT_CAP_KILL", "LINE_PAUSED",
                                "LINE_PAUSED_HOLDING", "LINE_RESUMED", "PRE_FLIGHT_HOLD")
    )
    if is_below_l3_final and eff_bid_after < effective_floor:
        protected = round(min(cfg.hard_max.normal, norm_min), 3)
        _append(
            result, ctx, protected, "BELOW_L3_PROTECTED",
            f"BELOW_L3_PROTECTED — deal CPM ${ctx.deal_cpm_li:.2f} ≤ LI avg×1.2 "
            f"${ctx.l3_cpm*1.2:.2f} and pub {ctx.pub_share:.1f}% < cap "
            f"{pub_cap*100:.0f}%. Raised from {clamped:.3f}× to norm_min "
            f"{protected:.3f}× (eff_floor ${effective_floor:.2f} × "
            f"{cfg.floor.norm_min_floor_mult} / CPM ${ctx.cpm_bid:.2f})."
            + (f" Prior decision: {reason_code}." if reason_code else ""),
        )
        return

    _append(
        result, ctx, clamped, reason_code,
        reason_text + (
            f" [clamped {new_mult:.3f} → {clamped:.3f}]"
            if abs(clamped - new_mult) > 0.0005 else ""
        ),
    )


# ─────────────────────────────────────────────────────────────────────────
# Priority 5 — normal pacing engine
# ─────────────────────────────────────────────────────────────────────────


def _normal_pacing_mp_ctv(
    ctx: _MCTVRowContext,
    norm_min: float,
    kill_mult: float,
    throttle_mult: float,
    dynamic_max: float,
    hist_trend: float,
    pk_map: Dict[Tuple[str, str], Dict],
    cfg: EngineConfig,
) -> Tuple[float, str, str]:
    """Priority 5 — pacing-driven multiplier with continuous price-tier modifier."""
    pacing = ctx.pacing or 0.0
    median_cpm = 0.0  # passed in context not available here; reason text will omit it

    if pacing >= 1.0:
        # ── Overpacing ────────────────────────────────────────────────
        if pacing < 1.05:
            return ctx.curr_mult, "PACE_HOLD_ONTARGET", (
                f"Pacing {round(pacing * 100)}% (100–105%) — on target, no adjustment."
            )

        base_down  = 0.10 if pacing < 1.15 else (0.15 if pacing < 1.25 else 0.20)
        base_down *= hist_trend
        tier_down  = _price_tier_mod(ctx.rank_frac, "down", cfg)
        day_mod    = 0.5 if ctx.days_rem is not None and ctx.days_rem <= 7 else 1.0
        total_down = min(base_down * tier_down * day_mod, cfg.max_single_day_down)

        # Determine effective floor (extended if any price-kills active on this LI)
        li_pk_keys    = [k for k in pk_map if k[0] == ctx.bw_id
                         and pk_map[k].get("action") in ("PRICE_KILL", "PRICE_KILL_HOLD")]
        has_pk_active = bool(li_pk_keys)
        mostly3_good  = len(ctx.history) >= 3 and sum(1 for h in ctx.history if h in ("OVER","GOOD")) >= 3
        extended_floor = (
            round(((ctx.floor * 1.07) / ctx.cpm_bid), 3)
            if has_pk_active and mostly3_good else norm_min
        )
        this_kill = round((ctx.floor * cfg.floor.kill_mult) / ctx.cpm_bid, 3)
        floor_to_use = (
            this_kill if ctx.curr_mult <= this_kill * 1.05 and extended_floor > this_kill
            else extended_floor
        )
        new = round(max(floor_to_use, ctx.curr_mult - total_down), 3)
        code = "PACE_DOWN_MOD" if pacing < 1.15 else "PACE_DOWN_AGG"
        return new, code, (
            f"Pacing {round(pacing * 100)}% — drift down −{total_down:.3f} "
            f"(base {base_down:.3f} × tier {tier_down:.3f}"
            f"{' × 0.5 [≤7d]' if day_mod < 1 else ''}, "
            f"cap {cfg.max_single_day_down}). "
            f"Rank {ctx.rank_frac:.2f}. Trend {hist_trend:.1f}× {ctx.history}. "
            f"{ctx.curr_mult:.3f} → {new:.3f}."
        )

    # ── Underpacing ───────────────────────────────────────────────────
    sev      = pacing < 0.75
    time_tag = ""
    if ctx.days_rem is not None and ctx.days_rem <= 7:
        base_up   = 0.10 if pacing >= 0.90 else (0.20 if pacing >= 0.75 else 0.30)
        eoc_boost = 1.10 if pacing >= 0.90 else (1.20 if pacing >= 0.75 else 1.30)
        base_up  *= eoc_boost
        time_tag  = "≤7d EOC"
    elif ctx.days_rem is not None and ctx.days_rem <= 14:
        base_up   = 0.15 if pacing >= 0.90 else (0.25 if pacing >= 0.75 else 0.35)
        eoc_boost = 1.0
        time_tag  = "≤14d"
    else:
        base_up   = 0.10 if pacing >= 0.90 else (0.20 if pacing >= 0.75 else 0.30)
        eoc_boost = 1.0

    base_up  *= hist_trend
    tier_up   = _price_tier_mod(ctx.rank_frac, "up_severe" if sev else "up_normal", cfg)
    total_up  = base_up * tier_up

    # Determine ceiling
    up_ceil  = (
        dynamic_max
        if sev or (ctx.days_rem is not None and ctx.days_rem <= 7)
        else cfg.hard_max.normal
    )

    # Floor: kill-override for severe underpace within 7 days
    eff_floor = kill_mult
    if ctx.days_rem is not None and ctx.days_rem <= 7 and sev:
        eff_floor = 0.001
        time_tag += " [kill-override: severe pace]"

    pk_key = (ctx.bw_id, ctx.deal_id)
    this_kill = round((ctx.floor * cfg.floor.kill_mult) / ctx.cpm_bid, 3)
    is_below_l3_p5 = (ctx.l3_cpm > 0 and ctx.deal_cpm_li > 0
                      and ctx.deal_cpm_li <= ctx.l3_cpm * 1.2)
    if ctx.curr_mult <= this_kill * 1.05 and pk_map.get(pk_key) is None and not is_below_l3_p5:
        return ctx.curr_mult, "PRICE_KILL_HOLD", (
            f"PRICE_KILL_HOLD — deal at kill level ({ctx.curr_mult:.3f}). "
            f"No staging unkill entry. Holding."
        )

    new  = round(min(up_ceil, max(eff_floor, ctx.curr_mult + total_up)), 3)
    code = "PACE_UP_MOD" if pacing >= 0.90 else ("PACE_UP_AGG" if pacing >= 0.75 else "PACE_UP_CRITICAL")
    return new, code, (
        f"Pacing {round(pacing * 100)}%"
        f"{', ' + str(ctx.days_rem) + 'd rem' if ctx.days_rem is not None else ''}"
        f"{' (' + time_tag + ')' if time_tag else ''}"
        f"{'[SEVERE]' if sev else ''}. "
        f"Raise +{total_up:.3f} "
        f"(base {base_up:.3f}"
        f"{' incl EOC ' + str(round(eoc_boost, 2)) + '×' if eoc_boost > 1 else ''}"
        f" × tier {tier_up:.3f}). "
        f"Rank {ctx.rank_frac:.2f}. Trend {hist_trend:.1f}× {ctx.history}. "
        f"{ctx.curr_mult:.3f} → {new:.3f}."
    )


# ─────────────────────────────────────────────────────────────────────────
# Pre-loop helper: per-LI median CPM
# ─────────────────────────────────────────────────────────────────────────


def _build_li_median_cpm(optimizer_df: pd.DataFrame) -> Dict[str, float]:
    """Per-LI median Last_3_Days_Clearing_CPM across all spend deals."""
    out: Dict[str, float] = {}
    if optimizer_df.empty:
        return out
    for bw_id, grp in optimizer_df.groupby("BW_Line_Item_ID"):
        cpms = sorted(
            _safe_float(r.get("Last_3_Days_Clearing_CPM"))
            for _, r in grp.iterrows()
            if _safe_float(r.get("Last_3_Days_Clearing_CPM")) > 0
        )
        if not cpms:
            continue
        n = len(cpms)
        mid = n // 2
        out[str(bw_id)] = cpms[mid] if n % 2 else (cpms[mid - 1] + cpms[mid]) / 2
    return out


# ─────────────────────────────────────────────────────────────────────────
# Helper: append a Decision to the result
# ─────────────────────────────────────────────────────────────────────────


def _append(
    result: EngineResult,
    ctx: _MCTVRowContext,
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
