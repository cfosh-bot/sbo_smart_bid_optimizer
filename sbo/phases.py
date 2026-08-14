"""Phases 1 / 2 / 3 — modifier creation, line item patching, term sync.

Less frequently run than the daily flow but critical for onboarding new
lines and keeping modifier terms in sync as targeting changes.

Phase 1 (create_publisher_bid_modifiers — Apps Script Section 10):
    For each input row WITHOUT an existing modifier ID:
      1. GET LI → check if it already has a modifier (then just record it)
      2. GET targeting expression → expand included deal_lists
      3. Calculate per-deal smart starting multiplier:
           floor ≤ $0.01           →  1.00× (O&O fixed price)
           globCpm > 0              →  (globCpm × 1.2) / cpm_bid
           floor > 0                →  (floor × 1.3) / cpm_bid
           else                     →  $11 / cpm_bid (fallback)
      4. POST /bid-modifiers with all deal terms
      5. Update input snapshot with new modifier ID

Phase 2 (patch_line_item_bid_modifiers — Apps Script Section 11):
    For each row with a modifier ID assigned but not yet patched:
      PATCH /line-items/{id}: bid_modifier_id, min_bid=0.01,
                              max_bid=min(cpm_bid × 2, 100)

Phase 3 (update_bid_modifier_terms — Apps Script Section 20):
    For each row with a modifier ID:
      1. GET LI → TE → currently targeted deal IDs
      2. GET modifier → existing terms
      3. Detect migration (old deal_id_list-style terms → wipe + rebuild)
      4. Append missing deals as new terms with smart multipliers
      5. PUT modifier

All three phases return (updated_input_df, results_df). The orchestrator
writes the updated input back to the AM-facing sheet.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd

from sbo.beeswax_client import BeeswaxClient, BeeswaxError
from sbo.config_models import EngineConfig
from sbo.run_storage import RunFolder
from sbo.state import StateStore
from sbo.utils import clean_id, safe_float


PHASE_RESULT_COLUMNS = ["BW_Line_Item_ID", "Action", "Status", "Detail", "Timestamp"]


@dataclass
class PhaseSummary:
    created: int = 0
    patched: int = 0
    updated: int = 0
    skipped: int = 0
    errors: int = 0
    error_log: List[Dict[str, Any]] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────
# Smart starting multiplier (used by Phase 1 + Phase 3)
# ─────────────────────────────────────────────────────────────────────────


def smart_starting_mult(
    cpm_bid: float,
    deal_glob_cpm: float,
    deal_floor: float,
    fallback_dollar: float,
) -> tuple[float, str]:
    """Return (multiplier, source_explanation).

    Pricing priority:
        floor ≤ $0.01 → 1.00× (O&O fixed price)
        globCpm > 0   → (globCpm × 1.2) / cpm_bid
        floor > 0     → (floor × 1.3) / cpm_bid
        fallback      → $11 / cpm_bid
    """
    if deal_floor and deal_floor <= 0.01:
        return 1.00, "floor $0.01 → 1.000×"
    if deal_glob_cpm > 0 and cpm_bid > 0:
        return round(max(0.01, (deal_glob_cpm * 1.2) / cpm_bid), 2), (
            f"globCPM ${deal_glob_cpm:.2f} × 1.2"
        )
    if deal_floor > 0 and cpm_bid > 0:
        return round(max(0.01, (deal_floor * 1.3) / cpm_bid), 2), (
            f"floor ${deal_floor:.2f} × 1.3"
        )
    if cpm_bid > 0:
        return round(fallback_dollar / cpm_bid, 2), f"${fallback_dollar} fallback"
    return 1.00, "no cpm_bid available"


# ─────────────────────────────────────────────────────────────────────────
# Pricing lookup from publisher_stats (or last-known state)
# ─────────────────────────────────────────────────────────────────────────


@dataclass
class DealPricingLookup:
    """Lookups extracted from publisher_stats for smart multiplier math."""
    glob_cpm: Dict[str, float] = field(default_factory=dict)   # deal_id → global CPM
    floor: Dict[str, float] = field(default_factory=dict)      # deal_id → raw floor price
    cpm_bid: Dict[str, float] = field(default_factory=dict)    # li_id → cpm_bid
    alternative_id: Dict[str, str] = field(default_factory=dict)  # deal_id → alternative_id


def build_pricing_lookup(publisher_stats: pd.DataFrame) -> DealPricingLookup:
    """Mirrors `sboBuildDealPricingLookup_` from the Apps Script."""
    out = DealPricingLookup()
    if publisher_stats.empty:
        return out
    for _, r in publisher_stats.iterrows():
        deal_id = str(r.get("Deal_ID", "")).strip()
        li_id = str(r.get("Line_Item_ID", "")).strip()
        if deal_id and deal_id not in out.glob_cpm:
            try:
                v = float(r.get("Deal_Global_Clearing_CPM") or 0)
                if v > 0:
                    out.glob_cpm[deal_id] = v
            except (TypeError, ValueError):
                pass
        if deal_id and deal_id not in out.floor:
            try:
                v = float(r.get("Floor_Price") or 0)
                if v > 0:
                    out.floor[deal_id] = v
            except (TypeError, ValueError):
                pass
        if li_id and li_id not in out.cpm_bid:
            try:
                v = float(r.get("CPM_Bid") or 0)
                if v > 0:
                    out.cpm_bid[li_id] = v
            except (TypeError, ValueError):
                pass
        if deal_id and deal_id not in out.alternative_id:
            v = str(r.get("Deal_Alternative_ID", "") or "").strip()
            if v:
                out.alternative_id[deal_id] = v
    return out


def _resolve_fallback_dollar(
    cfg: EngineConfig,
    deal_id: str,
    pricing: DealPricingLookup,
) -> float:
    """Resolve new_term_fallback_dollar to a float.

    For Podcast/Streaming (float config), returns the value directly.
    For Total Audio (dict config), parses sub-tactic from the deal's
    alternative_id. Falls back to the highest value if unrecognized.
    """
    fallback = cfg.new_term_fallback_dollar
    if isinstance(fallback, (float, int)):
        return float(fallback)
    # Dict — Total Audio sub-tactic-aware
    alt_id = pricing.alternative_id.get(deal_id, "")
    parts = alt_id.split("-")
    if len(parts) > 4:
        candidate = parts[4].strip().lower()
        if candidate in fallback:
            return float(fallback[candidate])
    # Fallback: search full string
    lower = alt_id.lower()
    if "podcast" in lower and "podcast" in fallback:
        return float(fallback["podcast"])
    if "streaming" in lower and "streaming" in fallback:
        return float(fallback["streaming"])
    return float(max(fallback.values()))


# ─────────────────────────────────────────────────────────────────────────
# Phase 1: create_publisher_bid_modifiers
# ─────────────────────────────────────────────────────────────────────────


def create_publisher_bid_modifiers(
    bw: BeeswaxClient,
    cfg: EngineConfig,
    run: RunFolder,
    state: StateStore,
    input_snapshot: pd.DataFrame,
    publisher_stats: pd.DataFrame | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, PhaseSummary]:
    """Create bid modifiers for input rows that don't have one yet.

    Returns (updated_input_df, results_df, summary).
    """
    run.log("=== Phase 1: create_publisher_bid_modifiers ===")
    bw.authenticate()
    pricing = build_pricing_lookup(publisher_stats) if publisher_stats is not None else DealPricingLookup()

    # Pre-fetch all list items so we can expand deal lists locally
    items_by_list = bw.fetch_all_list_items_by_list_id()
    run.log(f"Pre-fetched {len(items_by_list):,} deal lists")

    df = _normalize_input(input_snapshot)
    summary = PhaseSummary()
    results: List[Dict[str, Any]] = []
    now = datetime.now()

    # All rows with a BW LI ID — always verify against live Beeswax
    work = df[df["_bw_id"].ne("")].copy()
    run.log(f"Phase 1 work list: {len(work):,} rows")

    # ── Bulk pre-fetch all line items ────────────────────────────────────
    all_bw_ids = work["_bw_id"].tolist()
    li_map: Dict[str, Any] = {
        str(li["id"]): li
        for li in bw.fetch_line_items(all_bw_ids)
        if li.get("id") is not None
    }
    run.log(f"Pre-fetched {len(li_map):,} line items")

    # ── Bulk pre-fetch all targeting expressions ─────────────────────────
    te_ids = list({
        str(li_map[bw_id]["targeting_expression_id"])
        for bw_id in all_bw_ids
        if bw_id in li_map and li_map[bw_id].get("targeting_expression_id")
    })
    te_map: Dict[str, Any] = {
        str(te["id"]): te
        for te in bw.fetch_targeting_expressions(te_ids)
        if te.get("id") is not None
    }
    run.log(f"Pre-fetched {len(te_map):,} targeting expressions")

    for idx, row in work.iterrows():
        bw_id = row["_bw_id"]
        sf_id = row["_sf_id"]
        adv_name = row["_advertiser"]
        try:
            li = li_map.get(bw_id)
            if not li:
                results.append(_phase_result(bw_id, "create", "❌ Skipped", "LI not found", now))
                summary.skipped += 1
                continue

            # Always record whatever is live on the LI — keeps sheet fresh
            if li.get("bid_modifier_id"):
                existing_bm = str(li["bid_modifier_id"])
                df.at[idx, "_bm_id"] = existing_bm
                df.at[idx, "_patch_status"] = df.at[idx, "_patch_status"] or f"Patched (verified {now:%Y-%m-%d})"
                df.at[idx, "_create_status"] = f"Verified {now:%Y-%m-%d}"
                results.append(_phase_result(
                    bw_id, "create", "⏭️ Verified", f"BM {existing_bm} confirmed on LI", now,
                ))
                summary.skipped += 1
                continue

            te_id = li.get("targeting_expression_id")
            cpm_bid = _safe_float((li.get("bidding") or {}).get("values", {}).get("cpm_bid")) \
                or pricing.cpm_bid.get(bw_id, 0)
            if not te_id:
                results.append(_phase_result(bw_id, "create", "❌ Skipped", "No targeting_expression_id", now))
                summary.skipped += 1
                continue
            if cpm_bid <= 0:
                results.append(_phase_result(bw_id, "create", "❌ Skipped", "No cpm_bid", now))
                summary.skipped += 1
                continue

            # Expand TE → deal_id_list IDs → individual deal IDs
            te = te_map.get(str(te_id))
            if not te:
                results.append(_phase_result(bw_id, "create", "❌ Skipped", "TE not found", now))
                summary.skipped += 1
                continue
            list_ids = _included_list_ids(te)
            direct_ids = _included_direct_deal_ids(te)
            deal_ids = sorted(
                {d for lid in list_ids for d in items_by_list.get(lid, {})}
                | set(direct_ids)
            )
            if not deal_ids:
                results.append(_phase_result(bw_id, "create", "❌ Skipped", "No included deals or deal lists", now))
                summary.skipped += 1
                continue

            # Build per-deal terms with smart starting multipliers
            terms = []
            for d in deal_ids:
                mult, _src = smart_starting_mult(
                    cpm_bid=cpm_bid,
                    deal_glob_cpm=pricing.glob_cpm.get(d, 0),
                    deal_floor=pricing.floor.get(d, 0),
                    fallback_dollar=_resolve_fallback_dollar(cfg, d, pricing),
                )
                terms.append({
                    "comparator": "equals",
                    "value": d,
                    "multiplier": f"{mult:.2f}",
                    "override_multiplier": False,
                    "targeting_key": "deal_id",
                })

            modifier_name = (
                f"{sf_id}-{bw_id}-{adv_name}-{cfg.beeswax.modifier_suffix}"
            )
            payload = {
                "name": modifier_name,
                "account_id": cfg.beeswax.account_id,
                "advertiser_id": None,
                "active": True,
                "terms": terms,
            }
            created = bw.create_bid_modifier(payload)
            new_id = (created.get("results") or [{}])[0].get("id") or created.get("id")
            if not new_id:
                raise BeeswaxError("POST /bid-modifiers returned no ID")
            new_id = str(new_id)
            df.at[idx, "_bm_id"] = new_id
            df.at[idx, "_create_status"] = f"Created {now:%Y-%m-%d}"

            # Immediately patch the line item with the new modifier ID
            max_bid = min(round(cpm_bid * 2, 2), 100.0)
            try:
                bw.patch_line_item(bw_id, {
                    "bid_modifier_id": int(new_id),
                    "min_bid": 0.01,
                    "max_bid": max_bid,
                })
                df.at[idx, "_patch_status"] = f"Patched {now:%Y-%m-%d}"
                detail_msg = f"BM {new_id} with {len(terms)} term(s), patched max_bid=${max_bid:.2f}"
            except Exception as patch_err:
                detail_msg = f"BM {new_id} created but patch failed: {str(patch_err)[:100]}"

            results.append(_phase_result(
                bw_id, "create", "✅ Created", detail_msg, now,
            ))
            summary.created += 1

        except Exception as e:
            results.append(_phase_result(bw_id, "create", "❌ Error", str(e)[:200], now))
            summary.errors += 1
            summary.error_log.append({"bw_id": bw_id, "error": str(e)})

    results_df = pd.DataFrame(results, columns=PHASE_RESULT_COLUMNS)
    run.save_dataframe("phase1_results", results_df)
    run.log(
        f"Phase 1 done: created={summary.created} skipped={summary.skipped} "
        f"errors={summary.errors}"
    )
    _upsert_li_modifier_map(state, df)
    return _denormalize_input(df), results_df, summary


# ─────────────────────────────────────────────────────────────────────────
# Phase 2: patch_line_item_bid_modifiers
# ─────────────────────────────────────────────────────────────────────────


def patch_line_item_bid_modifiers(
    bw: BeeswaxClient,
    cfg: EngineConfig,
    run: RunFolder,
    input_snapshot: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, PhaseSummary]:
    """Apply bid_modifier_id + min/max_bid to each line item.

    max_bid = min(cpm_bid × 2, 100)
    min_bid = 0.01
    """
    run.log("=== Phase 2: patch_line_item_bid_modifiers ===")
    bw.authenticate()

    df = _normalize_input(input_snapshot)
    summary = PhaseSummary()
    results: List[Dict[str, Any]] = []
    now = datetime.now()

    work = df[df["_bw_id"].ne("")].copy()
    work = work[~work["_patch_status"].str.contains("Patched", na=False, regex=False)]
    run.log(f"Phase 2 work list: {len(work):,} rows")

    # ── Bulk pre-fetch all line items ────────────────────────────────────
    all_bw_ids = work["_bw_id"].tolist()
    li_map: Dict[str, Any] = {
        str(li["id"]): li
        for li in bw.fetch_line_items(all_bw_ids)
        if li.get("id") is not None
    }
    run.log(f"Pre-fetched {len(li_map):,} line items")

    for idx, row in work.iterrows():
        bw_id = row["_bw_id"]
        bm_id = row["_bm_id"]
        try:
            li = li_map.get(bw_id)
            if not li:
                results.append(_phase_result(bw_id, "patch", "❌ Skipped", "LI not found", now))
                summary.skipped += 1
                continue
            cpm_bid = _safe_float((li.get("bidding") or {}).get("values", {}).get("cpm_bid"))
            if cpm_bid <= 0:
                results.append(_phase_result(bw_id, "patch", "❌ Skipped", "No cpm_bid on LI", now))
                summary.skipped += 1
                continue
            max_bid = min(round(cpm_bid * 2, 2), 100.0)
            payload = {
                "bid_modifier_id": int(bm_id),
                "min_bid": 0.01,
                "max_bid": max_bid,
            }
            bw.patch_line_item(bw_id, payload)
            df.at[idx, "_patch_status"] = f"Patched {now:%Y-%m-%d}"
            results.append(_phase_result(
                bw_id, "patch", "✅ Patched",
                f"BM {bm_id}, max_bid=${max_bid:.2f}", now,
            ))
            summary.patched += 1
        except Exception as e:
            results.append(_phase_result(bw_id, "patch", "❌ Error", str(e)[:200], now))
            summary.errors += 1
            summary.error_log.append({"bw_id": bw_id, "error": str(e)})

    results_df = pd.DataFrame(results, columns=PHASE_RESULT_COLUMNS)
    run.save_dataframe("phase2_results", results_df)
    run.log(
        f"Phase 2 done: patched={summary.patched} skipped={summary.skipped} "
        f"errors={summary.errors}"
    )
    return _denormalize_input(df), results_df, summary


# ─────────────────────────────────────────────────────────────────────────
# Phase 3: update_bid_modifier_terms
# ─────────────────────────────────────────────────────────────────────────


def update_bid_modifier_terms(
    bw: BeeswaxClient,
    cfg: EngineConfig,
    run: RunFolder,
    state: StateStore,
    input_snapshot: pd.DataFrame,
    publisher_stats: pd.DataFrame | None = None,
    new_only: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, PhaseSummary]:
    """Sync each modifier's terms with the LI's current targeted deals.

    new_only=True: only process rows flagged in `_new_line` (col I = 'yes').
    Otherwise process every row that has a modifier ID.

    Migration: if existing modifier still has old `deal_id_list`-style terms,
    wipe all terms and rebuild from the current targeted deals.
    """
    run.log(f"=== Phase 3: update_bid_modifier_terms (new_only={new_only}) ===")
    bw.authenticate()
    pricing = build_pricing_lookup(publisher_stats) if publisher_stats is not None else DealPricingLookup()
    items_by_list = bw.fetch_all_list_items_by_list_id()

    df = _normalize_input(input_snapshot)
    summary = PhaseSummary()
    results: List[Dict[str, Any]] = []
    now = datetime.now()

    work = df[df["_bw_id"].ne("")].copy()
    if new_only:
        work = work[work["_new_line"].str.lower() == "yes"]
    run.log(f"Phase 3 work list: {len(work):,} rows")

    # ── Bulk pre-fetch all line items ────────────────────────────────────
    all_bw_ids = work["_bw_id"].tolist()
    li_map: Dict[str, Any] = {
        str(li["id"]): li
        for li in bw.fetch_line_items(all_bw_ids)
        if li.get("id") is not None
    }
    run.log(f"Pre-fetched {len(li_map):,} line items")

    # ── Bulk pre-fetch all targeting expressions ─────────────────────────
    te_ids = list({
        str(li_map[bw_id]["targeting_expression_id"])
        for bw_id in all_bw_ids
        if bw_id in li_map and li_map[bw_id].get("targeting_expression_id")
    })
    te_map: Dict[str, Any] = {
        str(te["id"]): te
        for te in bw.fetch_targeting_expressions(te_ids)
        if te.get("id") is not None
    }
    run.log(f"Pre-fetched {len(te_map):,} targeting expressions")

    for idx, row in work.iterrows():
        bw_id = row["_bw_id"]
        bm_id = row["_bm_id"]
        try:
            li = li_map.get(bw_id)
            if not li or not li.get("bid_modifier_id"):
                results.append(_phase_result(bw_id, "sync", "⏭️ Skipped", "No BM on live LI", now))
                summary.skipped += 1
                continue
            live_bm = str(li["bid_modifier_id"])
            if live_bm != bm_id:
                df.at[idx, "_bm_id"] = live_bm
                bm_id = live_bm
            te_id = li.get("targeting_expression_id")
            cpm_bid = _safe_float((li.get("bidding") or {}).get("values", {}).get("cpm_bid"))
            if not te_id or cpm_bid <= 0:
                results.append(_phase_result(bw_id, "sync", "❌ Skipped", "No TE or cpm_bid", now))
                summary.skipped += 1
                continue
            te = te_map.get(str(te_id))
            if not te:
                results.append(_phase_result(bw_id, "sync", "❌ Skipped", "TE not found", now))
                summary.skipped += 1
                continue
            list_ids = _included_list_ids(te)
            direct_ids = _included_direct_deal_ids(te)
            targeted_deal_ids = sorted(
                {d for lid in list_ids for d in items_by_list.get(lid, {})}
                | set(direct_ids)
            )
            if not targeted_deal_ids:
                results.append(_phase_result(bw_id, "sync", "❌ Skipped", "Targeted lists empty", now))
                summary.skipped += 1
                continue

            mod_obj = bw.get_bid_modifier(bm_id)

            # Detect migration: old deal_id_list-style terms present?
            existing_deal_terms: set[str] = set()
            has_old_list_terms = False
            for t in mod_obj.get("terms") or []:
                k = t.get("targeting_key") or ""
                if k == "deal_id_list":
                    has_old_list_terms = True
                elif k == "deal_id" and t.get("value") is not None:
                    existing_deal_terms.add(str(t["value"]))
            if has_old_list_terms:
                mod_obj["terms"] = []
                existing_deal_terms = set()

            new_term_deals = [
                d for d in targeted_deal_ids
                if d and d != "null" and d not in existing_deal_terms
            ]
            if not new_term_deals:
                df.at[idx, "_create_status"] = (
                    f"No changes ({len(targeted_deal_ids)} deals present) {now:%Y-%m-%d}"
                )
                results.append(_phase_result(
                    bw_id, "sync", "⏭️ No-op", "All targeted deals already terms", now,
                ))
                summary.skipped += 1
                continue

            for d in new_term_deals:
                mult, _src = smart_starting_mult(
                    cpm_bid=cpm_bid,
                    deal_glob_cpm=pricing.glob_cpm.get(d, 0),
                    deal_floor=pricing.floor.get(d, 0),
                    fallback_dollar=_resolve_fallback_dollar(cfg, d, pricing),
                )
                mod_obj["terms"].append({
                    "comparator": "equals",
                    "value": d,
                    "multiplier": f"{mult:.2f}",
                    "override_multiplier": False,
                    "targeting_key": "deal_id",
                })

            bw.update_bid_modifier(bm_id, mod_obj)

            action_word = "Migrated" if has_old_list_terms else "Added"
            df.at[idx, "_create_status"] = (
                f"{action_word} {len(new_term_deals)} term(s) {now:%Y-%m-%d}"
            )
            if new_only:
                df.at[idx, "_new_line"] = ""
            results.append(_phase_result(
                bw_id, "sync", f"✅ {action_word}",
                f"BM {bm_id} +{len(new_term_deals)} terms" + (" [migrated]" if has_old_list_terms else ""),
                now,
            ))
            summary.updated += 1
        except Exception as e:
            results.append(_phase_result(bw_id, "sync", "❌ Error", str(e)[:200], now))
            summary.errors += 1
            summary.error_log.append({"bw_id": bw_id, "error": str(e)})

    results_df = pd.DataFrame(results, columns=PHASE_RESULT_COLUMNS)
    run.save_dataframe("phase3_results", results_df)
    run.log(
        f"Phase 3 done: updated={summary.updated} skipped={summary.skipped} "
        f"errors={summary.errors}"
    )
    _upsert_li_modifier_map(state, df)
    return _denormalize_input(df), results_df, summary


# ─────────────────────────────────────────────────────────────────────────
# Helpers — input normalization, deal-list extraction, state upsert
# ─────────────────────────────────────────────────────────────────────────


_INPUT_COL_MAP = {
    "_sf_id":          ["sf li id", "sf_li_id"],
    "_bw_id":          ["bw li id", "bw_li_id"],
    "_advertiser":     ["advertiser"],
    "_bm_id":          ["bid modifier id"],
    "_create_status":  ["bid modifier created date"],
    "_patch_status":   ["bid modifier added date"],
    "_end_date":       ["end date"],
    "_new_line":       ["new line indicator - add yes", "new line indicator add yes"],
}


def _normalize_input(df: pd.DataFrame) -> pd.DataFrame:
    """Rename heterogeneous source columns to internal _-prefixed names."""
    out = df.copy()
    cols = {
        str(c).lower().strip(): c
        for c in out.columns
        if c is not None and str(c).strip()
    }
    for internal, candidates in _INPUT_COL_MAP.items():
        actual = next((cols[c] for c in candidates if c in cols), None)
        if actual is None:
            out[internal] = ""
        else:
            out[internal] = out[actual]
    # Coerce ID + status columns to clean strings (clean_id strips '.0' suffix
    # safely without eating real digits — `str.rstrip('.0')` would break '12340')
    for c in ("_sf_id", "_bw_id", "_bm_id"):
        out[c] = out[c].fillna("").map(clean_id)
    for c in ("_advertiser", "_create_status", "_patch_status", "_end_date", "_new_line"):
        out[c] = out[c].fillna("").astype(str).str.strip().replace({"nan": ""})
    return out


def _denormalize_input(df: pd.DataFrame) -> pd.DataFrame:
    """Write internal columns back to their original source-column names."""
    out = df.copy()
    cols = {
        str(c).lower().strip(): c
        for c in out.columns
        if c is not None and str(c).strip()
    }
    for internal, candidates in _INPUT_COL_MAP.items():
        actual = next((cols[c] for c in candidates if c in cols), None)
        if actual is not None and internal in out.columns:
            out[actual] = out[internal]
    return out.drop(columns=[c for c in _INPUT_COL_MAP if c in out.columns])


def _included_list_ids(te: Dict[str, Any]) -> List[str]:
    """Extract included deal_id_list IDs (both .all and .any buckets)."""
    modules = (te or {}).get("modules", {}) or {}
    app_site = modules.get("app_site", {}) or {}
    all_b = (app_site.get("all") or {}).get("deal_id_list", {}) or {}
    any_b = (app_site.get("any") or {}).get("deal_id_list", {}) or {}
    items = (all_b.get("any") or []) + (any_b.get("any") or [])
    return [str(it["value"]) for it in items if it.get("value") is not None]


def _included_direct_deal_ids(te: Dict[str, Any]) -> List[str]:
    """Extract directly targeted deal_id values (not via a list)."""
    modules = (te or {}).get("modules", {}) or {}
    app_site = modules.get("app_site", {}) or {}
    all_b = (app_site.get("all") or {}).get("deal_id", {}) or {}
    any_b = (app_site.get("any") or {}).get("deal_id", {}) or {}
    items = (all_b.get("any") or []) + (any_b.get("any") or [])
    return [str(it["value"]) for it in items if it.get("value") is not None]


def _phase_result(
    bw_id: str, action: str, status: str, detail: str, ts: datetime
) -> Dict[str, Any]:
    return {
        "BW_Line_Item_ID": bw_id,
        "Action": action,
        "Status": status,
        "Detail": detail,
        "Timestamp": ts,
    }


_safe_float = safe_float  # backwards-compat alias for any internal callers


def _upsert_li_modifier_map(state: StateStore, normalized_df: pd.DataFrame) -> None:
    """Persist BW LI ID → advertiser, modifier ID across runs."""
    if normalized_df.empty:
        return
    rows = []
    for _, r in normalized_df.iterrows():
        bw = r.get("_bw_id", "")
        if not bw:
            continue
        rows.append({
            "BW_Line_Item_ID": bw,
            "Advertiser_Name": r.get("_advertiser", ""),
            "Bid_Modifier_ID": r.get("_bm_id", ""),
        })
    if not rows:
        return
    new_df = pd.DataFrame(rows)
    existing = state.load("li_modifier_map")
    if not existing.empty:
        existing = existing[~existing["BW_Line_Item_ID"].isin(new_df["BW_Line_Item_ID"])]
        merged = pd.concat([existing, new_df], ignore_index=True)
    else:
        merged = new_df
    state.save("li_modifier_map", merged)
