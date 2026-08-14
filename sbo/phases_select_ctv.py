"""Phase 1 / Phase 3 — Select CTV bid-modifier creation and term sync.

Select CTV's provisioning phases use different starting-multiplier math
than the shared Podcast/Streaming/MP CTV `phases.py` (source: Select CTV
Apps Script Sections 10 and 20):

    Phase 1 (create): every new term starts at a FLAT 1.00× multiplier —
        no floor/CPM-based smart pricing at creation time. Also does NOT
        immediately patch the line item afterward (unlike the shared
        phases.py Phase 1) — patching is a fully separate manual step here,
        exactly as in the Apps Script.

    Phase 3 (add missing terms to an existing modifier): new deal terms use
        min(1.20, max(0.01, floor × 1.20 / cpm_bid)) when a floor is known,
        else a literal $45 / cpm_bid fallback — a different formula and a
        different (hardcoded) fallback dollar than Phase 1's flat 1.00× or
        the shared engine's smart_starting_mult.

Phase 2 (patch_line_item_bid_modifiers) is IDENTICAL business logic to the
shared implementation (bid_modifier_id + min_bid=0.01 + max_bid=min(cpm×2,100))
— Select CTV reuses `sbo.phases.patch_line_item_bid_modifiers` directly, no
dedicated version needed.

Kept in its own file (rather than branching inside the shared phases.py)
for the same reason MP CTV's engine is separate — a change to one product's
provisioning math cannot accidentally change another's.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd

from sbo.beeswax_client import BeeswaxClient, BeeswaxError
from sbo.config_models import EngineConfig
from sbo.phases import (
    PHASE_RESULT_COLUMNS,
    PhaseSummary,
    _denormalize_input,
    _included_direct_deal_ids,
    _included_list_ids,
    _normalize_input,
    _phase_result,
    _upsert_li_modifier_map,
    build_pricing_lookup,
)
from sbo.run_storage import RunFolder
from sbo.state import StateStore
from sbo.utils import safe_float

_safe_float = safe_float


# ─────────────────────────────────────────────────────────────────────────
# Phase 1: create_publisher_bid_modifiers_select_ctv
# ─────────────────────────────────────────────────────────────────────────


def create_publisher_bid_modifiers_select_ctv(
    bw: BeeswaxClient,
    cfg: EngineConfig,
    run: RunFolder,
    state: StateStore,
    input_snapshot: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, PhaseSummary]:
    """Create bid modifiers for Select CTV input rows without one yet.

    Every new term starts at a flat 1.00x multiplier — no smart pricing at
    creation time (that only happens later, in Phase 3, for terms ADDED to
    an already-existing modifier). Does not patch the line item afterward.
    """
    run.log("=== Phase 1 (Select CTV): create_publisher_bid_modifiers ===")
    bw.authenticate()

    items_by_list = bw.fetch_all_list_items_by_list_id()
    run.log(f"Pre-fetched {len(items_by_list):,} deal lists")

    df = _normalize_input(input_snapshot)
    summary = PhaseSummary()
    results: List[Dict[str, Any]] = []
    now = datetime.now()

    work = df[df["_bw_id"].ne("")].copy()
    run.log(f"Phase 1 work list: {len(work):,} rows")

    all_bw_ids = work["_bw_id"].tolist()
    li_map: Dict[str, Any] = {
        str(li["id"]): li
        for li in bw.fetch_line_items(all_bw_ids)
        if li.get("id") is not None
    }
    run.log(f"Pre-fetched {len(li_map):,} line items")

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

            if li.get("bid_modifier_id"):
                existing_bm = str(li["bid_modifier_id"])
                df.at[idx, "_bm_id"] = existing_bm
                df.at[idx, "_create_status"] = f"Existing (found on LI) {now:%Y-%m-%d}"
                results.append(_phase_result(
                    bw_id, "create", "⏭️ Existing", f"BM {existing_bm} found on LI", now,
                ))
                summary.skipped += 1
                continue

            te_id = li.get("targeting_expression_id")
            if not te_id:
                results.append(_phase_result(bw_id, "create", "❌ Skipped", "No targeting_expression_id", now))
                summary.skipped += 1
                continue

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
                results.append(_phase_result(bw_id, "create", "❌ Skipped", "No included deal lists", now))
                summary.skipped += 1
                continue

            # Every term starts flat at 1.00x — Select CTV-specific
            terms = [
                {
                    "comparator": "equals",
                    "value": d,
                    "multiplier": "1.00",
                    "override_multiplier": False,
                    "targeting_key": "deal_id",
                }
                for d in deal_ids
            ]

            modifier_name = f"{sf_id}-{bw_id}-{adv_name}-{cfg.beeswax.modifier_suffix}"
            payload = {
                "name": modifier_name,
                "account_id": cfg.beeswax.account_id,
                "bid_model_id": cfg.beeswax.bid_model_id,
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

            results.append(_phase_result(
                bw_id, "create", "✅ Created",
                f"BM {new_id} with {len(terms)} term(s) @ 1.00x", now,
            ))
            summary.created += 1

        except Exception as e:
            results.append(_phase_result(bw_id, "create", "❌ Error", str(e)[:200], now))
            summary.errors += 1
            summary.error_log.append({"bw_id": bw_id, "error": str(e)})

    results_df = pd.DataFrame(results, columns=PHASE_RESULT_COLUMNS)
    run.save_dataframe("phase1_results", results_df)
    run.log(
        f"Phase 1 (Select CTV) done: created={summary.created} skipped={summary.skipped} "
        f"errors={summary.errors}"
    )
    _upsert_li_modifier_map(state, df)
    return _denormalize_input(df), results_df, summary


# ─────────────────────────────────────────────────────────────────────────
# Phase 3: update_bid_modifier_terms_select_ctv
# ─────────────────────────────────────────────────────────────────────────


def update_bid_modifier_terms_select_ctv(
    bw: BeeswaxClient,
    cfg: EngineConfig,
    run: RunFolder,
    state: StateStore,
    input_snapshot: pd.DataFrame,
    publisher_stats: pd.DataFrame | None = None,
    new_only: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, PhaseSummary]:
    """Sync each modifier's terms with the LI's current targeted deals.

    New deal terms are priced at min(1.20, max(0.01, floor x 1.20 / cpm_bid))
    when a floor is known, else a literal $45 / cpm_bid fallback
    (cfg.new_term_fallback_dollar) — different from Phase 1's flat 1.00x.
    """
    run.log(f"=== Phase 3 (Select CTV): update_bid_modifier_terms (new_only={new_only}) ===")
    bw.authenticate()
    pricing = build_pricing_lookup(publisher_stats) if publisher_stats is not None else None
    floor_lookup: Dict[str, float] = pricing.floor if pricing is not None else {}
    cpm_bid_lookup: Dict[str, float] = pricing.cpm_bid if pricing is not None else {}
    items_by_list = bw.fetch_all_list_items_by_list_id()

    df = _normalize_input(input_snapshot)
    summary = PhaseSummary()
    results: List[Dict[str, Any]] = []
    now = datetime.now()

    work = df[df["_bw_id"].ne("")].copy()
    if new_only:
        work = work[work["_new_line"].str.lower() == "yes"]
    run.log(f"Phase 3 (Select CTV) work list: {len(work):,} rows")

    all_bw_ids = work["_bw_id"].tolist()
    li_map: Dict[str, Any] = {
        str(li["id"]): li
        for li in bw.fetch_line_items(all_bw_ids)
        if li.get("id") is not None
    }
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
    run.log(f"Pre-fetched {len(li_map):,} line items, {len(te_map):,} targeting expressions")

    fallback_dollar = float(cfg.new_term_fallback_dollar)

    for idx, row in work.iterrows():
        bw_id = row["_bw_id"]
        bm_id = row["_bm_id"]
        try:
            li = li_map.get(bw_id)
            if not li or not li.get("bid_modifier_id"):
                results.append(_phase_result(bw_id, "sync", "⏭️ Skipped", "No BM on live LI — run Phase 1 first", now))
                summary.skipped += 1
                continue
            live_bm = str(li["bid_modifier_id"])
            if live_bm != bm_id:
                df.at[idx, "_bm_id"] = live_bm
                bm_id = live_bm

            te_id = li.get("targeting_expression_id")
            live_cpm_bid = _safe_float((li.get("bidding") or {}).get("values", {}).get("cpm_bid")) \
                or cpm_bid_lookup.get(bw_id, 0)
            if not te_id or live_cpm_bid <= 0:
                results.append(_phase_result(bw_id, "sync", "❌ Skipped", "No TE or CPM bid", now))
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
                deal_floor = floor_lookup.get(d, 0)
                if deal_floor > 0:
                    start_mult = min(1.20, max(0.01, (deal_floor * cfg.floor.max_floor_mult) / live_cpm_bid))
                else:
                    start_mult = fallback_dollar / live_cpm_bid
                mod_obj["terms"].append({
                    "comparator": "equals",
                    "value": d,
                    "multiplier": f"{start_mult:.2f}",
                    "override_multiplier": False,
                    "targeting_key": "deal_id",
                })

            bw.update_bid_modifier(bm_id, mod_obj)

            action_word = "Migrated" if has_old_list_terms else "Added"
            df.at[idx, "_create_status"] = f"{action_word} {len(new_term_deals)} term(s) {now:%Y-%m-%d}"
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
        f"Phase 3 (Select CTV) done: updated={summary.updated} skipped={summary.skipped} "
        f"errors={summary.errors}"
    )
    _upsert_li_modifier_map(state, df)
    return _denormalize_input(df), results_df, summary
