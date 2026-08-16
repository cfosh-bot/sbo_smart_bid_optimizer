"""Pydantic models that validate `sbo/config/*.yaml` at load time.

Catches typos / wrong types before the pipeline runs instead of silently
mis-applying a cap during the engine pass.
"""

from pathlib import Path
from typing import Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, Field


class BeeswaxBlock(BaseModel):
    account_id: int
    bid_model_id: Optional[int] = None
    modifier_suffix: str
    podcast_mod_prefix: Union[str, List[str]]
    pub_prefix: str
    cat_prefix: str


class SheetTabsBlock(BaseModel):
    beeswax_line_item_settings: str
    sf_data_import: str
    up_pacing: str
    bid_optimizer: str
    pipeline_state: str
    run_log: str
    pause_log: str
    reason_key: str
    first_run_log: str
    second_run_log: str
    paused_log: str
    paused_snapshot: str
    pacing_history: str
    kill_log: str
    deal_cpm_history: Optional[str] = None
    publisher_cpm_history: Optional[str] = None
    # Select CTV only — replaces sf_data_import as the pacing/end-date/goal
    # source. 2-header-row sheet (title row 1, columns row 2, data row 3+).
    beeswax_select_ctv: Optional[str] = None


class FloorBlock(BaseModel):
    max_floor_mult: float
    norm_min_floor_mult: float
    throttle_mult: float
    kill_mult: float

    # ── MP CTV-only Day 1 / Day 2 baseline multipliers (2026-08-16 redesign) ──
    # Optional so Podcast/Streaming/Total Audio/Select CTV YAMLs (which don't
    # set these) stay valid and keep using their own existing Day 1/2 logic.
    day1_floor_mult: Optional[float] = None
    day2_floor_mult: Optional[float] = None

    # Floor multiplier used ONLY by Phase 1 / Phase 3 (smart_starting_mult) when
    # bootstrapping a brand-new deal term. Deliberately separate from
    # max_floor_mult (which PRE_FLIGHT_HOLD still uses) so this can change per
    # product without touching PRE_FLIGHT_HOLD. Defaults to 1.30 to preserve
    # current behavior for every product that doesn't override it.
    new_term_floor_mult: float = 1.30


class HardMaxBlock(BaseModel):
    normal: float
    severe: float
    last3: float


class CategoryBidCap(BaseModel):
    max_at_over_100: float
    max_at_below_75: float


class DealBidCap(BaseModel):
    max_at_over_100: float
    max_at_below_75: float
    note: Optional[str] = None


class PriceTierBlock(BaseModel):
    down_min: float
    down_max: float
    up_norm_max: float
    up_norm_min: float
    up_sev_max: float
    up_sev_min: float


class ApiBlock(BaseModel):
    chunk_size: int
    report_batch_size: int
    atr_row_cap: int
    max_input_rows: int


class SubTacticShareBounds(BaseModel):
    min: float
    max: float
    throttle_entry: float  # fraction of max where throttle begins (e.g. 0.80)


class SubTacticShareBlock(BaseModel):
    streaming: SubTacticShareBounds
    podcast: SubTacticShareBounds


class MarginTrimBlock(BaseModel):
    """Select CTV only — the on-target (100-105% pacing, >7d) margin-health
    trim added 2026-08-14. Uses Deal_Clearing_CPM_On_LI (falls back to
    Last_3_Days_Clearing_CPM) vs CPM bid to slowly trim an on-pace line
    toward better clearing efficiency without disrupting pacing.
    """
    healthy_margin_threshold: float  # e.g. 0.06 — margin >= this is "healthy"
    healthy_step: float              # trim step when margin is healthy (slow burn)
    thin_step: float                 # trim step when margin is thin (faster correction)


class EngineConfig(BaseModel):
    """The full validated config — what the engine + pipeline consume."""

    tactic: str
    beeswax: BeeswaxBlock
    sheet_tabs: SheetTabsBlock
    floor: FloorBlock
    hard_max: HardMaxBlock
    cat_max_bid_eoc_bonus: float
    new_term_fallback_dollar: Union[float, Dict[str, float]]
    max_single_day_down: float
    modifier_priority: List[str]
    modifier_throttle_levels: Dict[str, float]
    modifier_other_name: Union[str, List[str]]
    category_max_bid: Dict[str, CategoryBidCap]
    deal_max_bid: Dict[str, DealBidCap] = Field(default_factory=dict)
    price_tier: PriceTierBlock
    floor_spread_warn: float
    floor_fee_mult: float
    api: ApiBlock
    schedule_notes: Optional[str] = None
    category_max_bid_by_sub_tactic: Optional[Dict[str, Dict[str, CategoryBidCap]]] = None
    sub_tactic_share: Optional[SubTacticShareBlock] = None

    # ── MP CTV-specific fields (optional so Podcast/Streaming/Total Audio YAMLs stay valid) ──
    pub_cap_537: float = 0.20
    pub_cap_other: float = 0.40
    cat_cap_over: float = 0.25
    cat_cap_under: float = 0.30
    cat_kill_over: float = 0.30
    marketplace_list_ids: List[str] = Field(default_factory=list)
    deal_537_id: str = "537"

    # ── Select CTV-specific fields (optional so other tactics' YAMLs stay valid) ──
    margin_trim: Optional[MarginTrimBlock] = None

    @property
    def is_mp_ctv(self) -> bool:
        """True when this config drives the Marketplace CTV tactic."""
        return self.tactic == "marketplace_ctv"

    @property
    def is_select_ctv(self) -> bool:
        """True when this config drives the Select CTV tactic."""
        return self.tactic == "select_ctv"


def load_config(path) -> EngineConfig:
    """Parse a YAML config file into a validated EngineConfig."""
    with open(path) as f:
        data = yaml.safe_load(f)
    return EngineConfig(**data)
