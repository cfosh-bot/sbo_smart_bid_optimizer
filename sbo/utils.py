"""Small shared utilities — types/numerics/string coercion.

Consolidates helpers that were duplicated across modules. Keep this file
boring.
"""

from __future__ import annotations

import math
import re
from typing import Dict, List, Optional

import pandas as pd


_TRAILING_ZERO_DOT = re.compile(r"\.0+$")


def safe_float(v) -> float:
    """Permissive float coercion. NaN, None, '', non-numeric strings → 0.0.

    Use ONLY where 0.0 is a meaningful default (e.g. floor price). For
    fields where 'missing' must be distinguished from 0 (e.g. push diff
    calculations, pacing), use ``maybe_float`` instead.
    """
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return 0.0 if math.isnan(v) else float(v)
    s = str(v).strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def maybe_float(v) -> Optional[float]:
    """Strict float coercion. NaN, None, '' return None (not 0.0).

    Use for fields where 'missing' has different semantics than 0 — e.g.
    Pacing_Pct (no pacing data ≠ 0% pacing) and Current_Multiplier (a
    missing live value should not be treated as 'multiplier was 0').
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return None if math.isnan(v) else float(v)
    s = str(v).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def strip_trailing_zero_decimal(s: str) -> str:
    """Convert '12340.0' → '12340' WITHOUT eating real digits.

    `str.rstrip('.0')` is a footgun — strips any combination of '.' and '0'
    from the end, so '12340' → '1234'. Always use this helper.
    """
    return _TRAILING_ZERO_DOT.sub("", s)


def clean_id(v) -> str:
    """Coerce a sheet-cell value to a clean ID string.

    Removes float coercion artifacts ('1234.0' → '1234'), 'nan', and
    surrounding whitespace.
    """
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() == "nan" or s == "":
        return ""
    return strip_trailing_zero_decimal(s)


# ── Column-name normalization ────────────────────────────────────────────


def _norm_header(s) -> str:
    """Normalize a column header to snake_case for matching:
    'Line Item ID' → 'line_item_id', 'Deal.ID' → 'deal_id'."""
    return str(s).lower().replace(" ", "_").replace(".", "_").strip()


def normalize_columns(
    df: pd.DataFrame, aliases: Dict[str, List[str]]
) -> pd.DataFrame:
    """Rename columns to canonical names regardless of source casing.

    `aliases` maps canonical → list of possible source names (any-of match).
    Beeswax CSV reports use Title Case ('Line Item ID'); xlsx snapshots
    use the same; raw API JSON uses snake_case. This handles all three.

    Use this on EVERY DataFrame built from `BeeswaxClient.fetch_report`
    output before touching named columns — otherwise `df.get('impression',
    0)` returns the int default instead of a Series and `.fillna(0)` blows
    up. (See: that bug Casey hit on April 28.)
    """
    if df is None or df.empty:
        return df
    norm_to_actual = {_norm_header(c): c for c in df.columns}
    rename: Dict[str, str] = {}
    for canon, candidates in aliases.items():
        for cand in candidates:
            n = _norm_header(cand)
            if n in norm_to_actual and norm_to_actual[n] != canon:
                rename[norm_to_actual[n]] = canon
                break
    return df.rename(columns=rename) if rename else df


# Aliases for every Beeswax report we read.
# Add new entries here when we add a new report — don't inline aliases at
# the call site.

REPORT_ALIASES: Dict[str, Dict[str, List[str]]] = {
    "performance_agg": {
        "line_item_id": ["line_item_id", "Line Item ID"],
        "deal_id": ["deal_id", "Deal ID"],
        "alternative_id": ["alternative_id", "deal_alternative_id", "Deal Alternative ID"],
        "name": ["name", "deal_name", "Deal Name"],
        "impression": ["impression", "impressions", "Impressions", "Impression"],
        "media_spend_usd": ["media_spend_usd", "media_spend", "Media Spend USD", "Media Spend"],
        "cpm_usd": ["cpm_usd", "cpm", "CPM USD", "CPM"],
        "bid_shading_fee_usd": ["bid_shading_fee_usd", "Bid Shading Fee USD"],
    },
    "deal_agg": {
        "deal_id": ["deal_id", "Deal ID"],
        "floor_price": ["floor_price", "Floor Price"],
    },
}
