"""MP CTV Price Kill Staging.

Pre-computes per-deal PRICE_KILL / PRICE_UNKILL / PRICE_KILL_HOLD actions
before the multiplier engine runs.

KILL TIERS (overpacing, >3 days remaining):
  4/4 GOOD streak  → kill 10 most expensive deals above L3×1.2
  ≥125% pacing     → kill 5 most expensive deals above L3×1.2
  ≥105% pacing     → kill 2 most expensive deals above L3×1.2
  Kill quota reduced by number of pub/cat cap kills firing this run.

UNKILL TIERS (priority order, first match wins):
  P1  4/4 UNDER                    → ALL kills, floor×1.20, no exemptions
  P2  3/4 UNDER                    → 20 deals,  floor×1.20, skip pub-over-cap
  P3a ≤3 days, <100%               → ALL kills, floor×1.20, no exemptions
  P3b ≤3 days, ≥100%  (EOC proact) → deals >1.3× LI avg, floor×1.20
  P4a ≤7 days, <90%                → ALL kills, floor×1.20, skip pub-over-cap
  P4b ≤7 days, 90-99%              → 20 deals,  floor×1.20, skip pub-over-cap
  P5a ≤14 days, <75%               → 30 deals,  floor×1.20, skip pub-over-cap
  P5b ≤14 days, 75-89%             → 16 deals,  floor×1.20, skip pub-over-cap
  P5c ≤14 days, 90-99%             → 8 deals,   floor×1.15, skip pub-over-cap
  P6a >14 days, <75%               → 15 deals,  floor×1.20, skip pub-over-cap
  P6b >14 days, 75-89%             → 8 deals,   floor×1.20, skip pub-over-cap
  P6c >14 days, 90-99%             → 4 deals,   floor×1.15, skip pub-over-cap
  Unkill quota reduced by force-unkills (below L3×1.2) firing this run.
  Unkill quota capped by number of currently killed deals.

KILL STATE: eff_bid < floor × 0.9  (cpm_bid × curr_mult < floor × 0.9)
NEVER KILL: deal_cpm_li ≤ l3_cpm × 1.2  (below/near LI average)
UNKILL ORDER: cheapest Deal_Clearing_CPM_On_LI first
KILL ORDER: most expensive Deal_Clearing_CPM_On_LI first
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from sbo.config_models import EngineConfig
from sbo.multiplier_engine import _safe_float


# ── Public entry point ────────────────────────────────────────────────────


def build_price_kill_staging(
    optimizer_df: pd.DataFrame,
    cfg: EngineConfig,
    pacing_history: pd.DataFrame,
    paused_active: Set[str],
) -> pd.DataFrame:
    """Compute PRICE_KILL / PRICE_UNKILL / PRICE_KILL_HOLD actions."""
    if optimizer_df.empty:
        return _empty_df()

    from sbo.multiplier_engine import _build_history_map, _parse_pacing

    history_map  = _build_history_map(pacing_history, max_runs=4)
    f            = cfg.floor
    action_rows: List[Dict] = []

    for bw_id, grp in optimizer_df.groupby("BW_Line_Item_ID"):
        bw_id = str(bw_id).strip()
        if bw_id in paused_active:
            continue

        # ── Per-LI scalars ────────────────────────────────────────────
        first_row    = grp.iloc[0]
        cpm_bid      = _safe_float(first_row.get("CPM_Bid"))
        floor_val    = _safe_float(first_row.get("Floor_Price"))
        pacing       = _parse_pacing(first_row.get("Pacing_Pct", ""))
        days_rem_raw = first_row.get("Days_Remaining", "")
        days_rem     = (
            int(days_rem_raw)
            if str(days_rem_raw).strip() not in ("", "nan") else None
        )

        if cpm_bid <= 0:
            continue

        # L3 CPM for this LI
        l3_cpm_vals = grp["Last_3_Days_Clearing_CPM"].dropna()
        l3_cpm = 0.0
        if not l3_cpm_vals.empty and (l3_cpm_vals > 0).any():
            l3_cpm = _safe_float(l3_cpm_vals[l3_cpm_vals > 0].max())

        # Use floor or fall back to l3_cpm * 1.25
        if floor_val <= 0:
            floor_val = round(l3_cpm * 1.25, 2) if l3_cpm > 0 else 0.0
        if floor_val <= 0:
            continue

        history          = history_map.get(bw_id, [])
        n                = len(history)
        good             = sum(1 for h in history if h in ("OVER", "GOOD"))
        under            = sum(1 for h in history if h == "UNDER")
        sustained4_good  = n >= 4 and good  == 4
        mostly3_good     = n >= 3 and good  >= 3
        sustained4_under = n >= 4 and under == 4
        mostly3_under    = n >= 3 and under >= 3

        kill_mult_val = (floor_val * f.kill_mult) / cpm_bid

        # ── Classify every deal ───────────────────────────────────────
        deals: List[Dict] = []
        for _, dr in grp.iterrows():
            deal_id         = str(dr["Deal_ID"]).strip()
            deal_cpm_li     = _safe_float(dr.get("Deal_Clearing_CPM_On_LI"))
            curr_mult       = _safe_float(dr.get("Current_Multiplier")) or 1.0
            pub_share       = _safe_float(dr.get("Pub_Impression_Share_Pct")) / 100.0
            cat_share_d     = _safe_float(dr.get("Category_Share_Pct")) / 100.0
            targets_537_val = str(dr.get("Targets_537", "") or "").strip()
            targets_537     = targets_537_val == "537"

            # Kill state: eff_bid < floor * 0.9 (AppScript definition)
            eff_bid   = cpm_bid * curr_mult
            is_killed = eff_bid < floor_val * 0.9

            pub_cap = cfg.pub_cap_537 if targets_537 else cfg.pub_cap_other

            is_pub_kill_eligible = (
                pub_share >= pub_cap
                and not (days_rem is not None and days_rem <= 3)
            )
            is_cat_kill_eligible = (
                targets_537
                and (sustained4_good or mostly3_good)
                and (pacing is None or pacing >= 1.0)
                and cat_share_d >= cfg.cat_kill_over
                and (l3_cpm <= 0 or deal_cpm_li > l3_cpm * 1.2)
                and not (days_rem is not None and days_rem <= 3)
                and not (pacing is not None and pacing < 1.0
                         and days_rem is not None and days_rem <= 7)
            )
            is_below_l3 = (
                l3_cpm > 0 and deal_cpm_li > 0 and deal_cpm_li <= l3_cpm * 1.2
            )

            deals.append({
                "deal_id":              deal_id,
                "deal_cpm_li":          deal_cpm_li,
                "curr_mult":            curr_mult,
                "eff_bid":              eff_bid,
                "is_killed":            is_killed,
                "is_pub_kill_eligible": is_pub_kill_eligible,
                "is_cat_kill_eligible": is_cat_kill_eligible,
                "is_below_l3":          is_below_l3,
                "pub_cap":              pub_cap,
                "pub_share":            pub_share,
            })

        if not deals:
            continue

        currently_killed     = [d for d in deals if d["is_killed"]]
        currently_not_killed = [d for d in deals if not d["is_killed"]]
        n_killed             = len(currently_killed)

        # Count other-rule kills/unkills firing this run (for quota adjustment)
        other_kills   = sum(1 for d in currently_not_killed
                            if d["is_pub_kill_eligible"] or d["is_cat_kill_eligible"])
        force_unkills = sum(1 for d in currently_killed
                            if d["is_below_l3"] and not d["is_pub_kill_eligible"])

        # ── Kill quota ────────────────────────────────────────────────
        raw_kill_quota = _kill_quota(
            pacing, days_rem, sustained4_good
        )
        kill_quota = max(0, raw_kill_quota - other_kills)

        # ── Unkill tier ───────────────────────────────────────────────
        unkill_count_max, unkill_restart_mult, skip_over_cap, no_exemptions, eoc_proactive = \
            _unkill_tier(pacing, days_rem, sustained4_under, mostly3_under, l3_cpm)

        # Reduce unkill count by force-unkills already happening
        if unkill_count_max is not None:
            unkill_count_max = max(0, min(unkill_count_max - force_unkills, n_killed))
        else:
            # None = ALL — cap at n_killed minus force-unkills
            unkill_count_max = max(0, n_killed - force_unkills)

        unkill_restart = round(
            (floor_val * unkill_restart_mult) / cpm_bid, 3
        )

        # ── Step 1: Force-unkill below-L3 killed deals ───────────────
        unkilled_set: Set[str] = set()
        for d in sorted(currently_killed, key=lambda x: x["deal_cpm_li"]):
            if d["is_below_l3"] and not d["is_pub_kill_eligible"]:
                restart = round(
                    min(cfg.hard_max.normal,
                        max(0.01, (floor_val * f.norm_min_floor_mult) / cpm_bid)),
                    3,
                )
                action_rows.append(_action(
                    bw_id, d, l3_cpm, "PRICE_UNKILL", restart,
                    f"Force-unkill: deal CPM ${d['deal_cpm_li']:.2f} "
                    f"≤ L3×1.2 (${l3_cpm*1.2:.2f}). Bypasses quota.",
                ))
                unkilled_set.add(d["deal_id"])

        # ── Step 2: EOC proactive unkill (≤3 days, on-pace) ──────────
        if eoc_proactive:
            for d in sorted(currently_killed, key=lambda x: x["deal_cpm_li"], reverse=True):
                if d["deal_id"] in unkilled_set:
                    continue
                if d["deal_cpm_li"] > l3_cpm * 1.3:
                    restart_eoc = round(
                        min(cfg.hard_max.normal,
                            (floor_val * unkill_restart_mult) / cpm_bid),
                        3,
                    )
                    action_rows.append(_action(
                        bw_id, d, l3_cpm, "PRICE_UNKILL", restart_eoc,
                        f"EOC proactive unkill: deal CPM ${d['deal_cpm_li']:.2f} "
                        f"> L3×1.3 (${l3_cpm*1.3:.2f}). ≤3 days, on-pace.",
                    ))
                    unkilled_set.add(d["deal_id"])

        # ── Step 3: Normal unkill ─────────────────────────────────────
        if unkill_count_max > 0 and not eoc_proactive:
            unkill_candidates = sorted(
                [d for d in currently_killed
                 if d["deal_id"] not in unkilled_set
                 and not d["is_below_l3"]
                 and not (skip_over_cap and d["is_pub_kill_eligible"])
                 and not d["is_cat_kill_eligible"]],
                key=lambda x: x["deal_cpm_li"],  # cheapest first
            )
            count = 0
            for d in unkill_candidates:
                if count >= unkill_count_max:
                    break
                # no_exemptions: skip_over_cap already handled in candidate filter
                action_rows.append(_action(
                    bw_id, d, l3_cpm, "PRICE_UNKILL", unkill_restart,
                    f"Unkill cheapest first (quota {unkill_count_max}, "
                    f"pacing {round(pacing*100) if pacing else '?'}%, "
                    f"days {days_rem}). "
                    f"Eff bid ${d['eff_bid']:.2f} < floor×0.9 "
                    f"(${floor_val*0.9:.2f}).",
                ))
                unkilled_set.add(d["deal_id"])
                count += 1

        # ── Step 4: Price kill ────────────────────────────────────────
        if kill_quota > 0 and pacing is not None and pacing >= 1.05:
            kill_candidates = sorted(
                [d for d in currently_not_killed
                 if not d["is_below_l3"]
                 and not d["is_pub_kill_eligible"]
                 and not d["is_cat_kill_eligible"]],
                key=lambda x: x["deal_cpm_li"],  # most expensive first
                reverse=True,
            )
            kills_fired = 0
            for d in kill_candidates:
                if kills_fired >= kill_quota:
                    break
                tier_label = (
                    "4/4 GOOD streak (quota 10)" if sustained4_good else
                    "≥125% pacing (quota 5)"     if pacing >= 1.25 else
                    "≥105% pacing (quota 2)"
                )
                action_rows.append(_action(
                    bw_id, d, l3_cpm, "PRICE_KILL", round(kill_mult_val, 3),
                    f"{tier_label} — raw_quota {raw_kill_quota}, "
                    f"adj_quota {kill_quota} (−{other_kills} other kills). "
                    f"Deal CPM ${d['deal_cpm_li']:.2f} > L3×1.2 "
                    f"(${l3_cpm*1.2:.2f}).",
                ))
                kills_fired += 1

        # ── Step 5: Hold — killed deals not being unkilled ───────────
        for d in currently_killed:
            if (
                d["deal_id"] not in unkilled_set
                and not d["is_pub_kill_eligible"]
                and not d["is_cat_kill_eligible"]
                and not d["is_below_l3"]
            ):
                action_rows.append(_action(
                    bw_id, d, l3_cpm, "PRICE_KILL_HOLD", round(kill_mult_val, 3),
                    f"HOLD — killed (eff bid ${d['eff_bid']:.2f} < "
                    f"floor×0.9 ${floor_val*0.9:.2f}). "
                    f"Unkill quota {unkill_count_max} exhausted or not applicable.",
                ))

    if not action_rows:
        return _empty_df()
    return pd.DataFrame(action_rows)


# ── Kill quota helper ─────────────────────────────────────────────────────


def _kill_quota(
    pacing: Optional[float],
    days_rem: Optional[int],
    sustained4_good: bool,
) -> int:
    """Raw kill quota before adjusting for other kills."""
    if days_rem is not None and days_rem <= 3:
        return 0  # Priority 1 handles last-3-days
    if pacing is None or pacing < 1.05:
        return 0
    if sustained4_good:
        return 10
    if pacing >= 1.25:
        return 5
    # 1.05 <= pacing < 1.25
    return 2


# ── Unkill tier helper ────────────────────────────────────────────────────


def _unkill_tier(
    pacing: Optional[float],
    days_rem: Optional[int],
    sustained4_under: bool,
    mostly3_under: bool,
    l3_cpm: float,
) -> Tuple[Optional[int], float, bool, bool, bool]:
    """
    Returns (count_max, restart_floor_mult, skip_over_cap, no_exemptions, eoc_proactive).
    count_max=None means ALL killed deals.
    """
    if pacing is None:
        return 0, 1.20, True, False, False

    # P1 — 4/4 UNDER: all kills, skip pub-over-cap
    if sustained4_under:
        return None, 1.20, True, False, False

    # P2 — 3/4 UNDER: 20 deals, skip pub-over-cap
    if mostly3_under and pacing < 1.0:
        return 20, 1.20, True, False, False

    # P3 — ≤3 days remaining
    if days_rem is not None and days_rem <= 3:
        if pacing < 1.0:
            return None, 1.20, False, True, False   # P3a: underpacing, all kills
        else:
            return None, 1.20, False, False, True   # P3b: on-pace EOC proactive

    # P4 — ≤7 days remaining
    if days_rem is not None and days_rem <= 7:
        if pacing < 0.90:
            return None, 1.20, True, False, False   # P4a: <90%, all kills
        elif pacing < 1.0:
            return 20, 1.20, True, False, False     # P4b: 90-99%, 20 deals

    # P5 — ≤14 days remaining
    if days_rem is not None and days_rem <= 14:
        if pacing < 0.75:
            return 30, 1.20, True, False, False     # P5a: <75%, 30 deals
        elif pacing < 0.90:
            return 16, 1.20, True, False, False     # P5b: 75-89%, 16 deals
        elif pacing < 1.0:
            return 8,  1.15, True, False, False     # P5c: 90-99%, 8 deals

    # P6 — >14 days remaining
    if pacing < 0.75:
        return 15, 1.20, True, False, False         # P6a: <75%, 15 deals
    elif pacing < 0.90:
        return 8,  1.20, True, False, False         # P6b: 75-89%, 8 deals
    elif pacing < 1.0:
        return 4,  1.15, True, False, False         # P6c: 90-99%, 4 deals

    # On-pace or overpacing with no unkill condition → no unkills
    return 0, 1.20, True, False, False


# ── Row builder ───────────────────────────────────────────────────────────


def _action(
    bw_id: str,
    d: Dict,
    l3_cpm: float,
    action: str,
    restart: float,
    reason: str,
) -> Dict:
    return {
        "BW_Line_Item_ID":    bw_id,
        "Deal_ID":            d["deal_id"],
        "Deal_CPM_LI":        d["deal_cpm_li"],
        "L3_CPM":             l3_cpm,
        "Action":             action,
        "Restart_Multiplier": restart,
        "Reason":             reason,
    }


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "BW_Line_Item_ID", "Deal_ID", "Deal_CPM_LI", "L3_CPM",
        "Action", "Restart_Multiplier", "Reason",
    ])
