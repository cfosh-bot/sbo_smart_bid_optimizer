"""Beeswax API client.

Wraps the iHeartMedia Beeswax tenant (`iheartmedia.api.beeswax.com`) with:
  - cookie-cached auth (one /authenticate per process, not per call)
  - report submit + poll + binary-split-on-row-cap
  - paginated line-items / bid-modifiers / lists
  - typed exceptions so the orchestrator can decide retry vs fail

This is a stub. Wire up auth + reports first, then port helpers as we
port each Apps Script section.
"""

from __future__ import annotations

import csv
import io
import os
import time
from dataclasses import dataclass
from typing import Any, Iterable, Iterator

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


class BeeswaxError(Exception):
    """Base for all Beeswax errors."""


class BeeswaxAuthError(BeeswaxError):
    """Authentication failed (bad creds, expired session, rate-limited)."""


class BeeswaxRateLimitError(BeeswaxError):
    """429 / bandwidth-quota response from upstream."""


class BeeswaxReportTimeout(BeeswaxError):
    """Async report polling exceeded the timeout."""


@dataclass
class BeeswaxConfig:
    api_base: str
    legacy_list_base: str
    email: str
    password: str
    chunk_size: int = 40
    report_poll_interval_sec: int = 30
    report_max_wait_sec: int = 300

    @classmethod
    def from_env(cls) -> "BeeswaxConfig":
        return cls(
            api_base=os.environ["BEESWAX_API_BASE"],
            legacy_list_base=os.environ["BEESWAX_LEGACY_LIST_BASE"],
            email=os.environ["BEESWAX_EMAIL"],
            password=os.environ["BEESWAX_PASSWORD"],
        )


class BeeswaxClient:
    """Synchronous Beeswax client with persistent cookie session.

    Use as a context manager so the session is cleaned up:

        with BeeswaxClient(cfg) as bw:
            bw.authenticate()
            rows = bw.fetch_report({...}, label="ATR")
    """

    def __init__(self, cfg: BeeswaxConfig):
        self.cfg = cfg
        self._client: httpx.Client | None = None
        self._authenticated_at: float | None = None

    # ── lifecycle ─────────────────────────────────────────────

    def __enter__(self) -> "BeeswaxClient":
        self._client = httpx.Client(
    timeout=httpx.Timeout(connect=30.0, read=120.0, write=30.0, pool=30.0),
    follow_redirects=True
)
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    # ── auth ──────────────────────────────────────────────────

    @retry(
        retry=retry_if_exception_type(BeeswaxRateLimitError),
        wait=wait_exponential(multiplier=2, min=10, max=120),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def authenticate(self) -> None:
        """POST /authenticate. Cookies stick to the httpx client.

        Cached for the life of the process; call `re_authenticate()` to force.
        """
        assert self._client is not None
        if self._authenticated_at is not None:
            return  # already auth'd this process
        url = f"{self.cfg.api_base}/authenticate"
        resp = self._client.post(
            url,
            json={
                "email": self.cfg.email,
                "password": self.cfg.password,
                "keep_logged_in": True,
            },
        )
        if resp.status_code == 429 or "bandwidth quota" in resp.text.lower():
            raise BeeswaxRateLimitError(f"Auth rate-limited: {resp.text[:200]}")
        if resp.status_code != 200:
            raise BeeswaxAuthError(
                f"Auth failed: HTTP {resp.status_code} — {resp.text[:200]}"
            )
        self._authenticated_at = time.time()

    def re_authenticate(self) -> None:
        self._authenticated_at = None
        if self._client is not None:
            self._client.cookies.clear()
        self.authenticate()

    # ── reports ───────────────────────────────────────────────

    def submit_report(self, payload: dict[str, Any], label: str = "report") -> str:
        """Submit an async report query. Returns task_id."""
        assert self._client is not None
        resp = self._client.post(
            f"{self.cfg.api_base}/reporting/run-query", json=payload
        )
        if resp.status_code != 200:
            raise BeeswaxError(
                f"{label} submit failed: HTTP {resp.status_code} — {resp.text[:200]}"
            )
        data = resp.json()
        if not data.get("task_id"):
            raise BeeswaxError(f"{label}: no task_id returned")
        return str(data["task_id"])

    def poll_report(self, task_id: str) -> str:
        """Poll until report is ready. Returns CSV body."""
        assert self._client is not None
        elapsed = 0
        while elapsed < self.cfg.report_max_wait_sec:
            time.sleep(self.cfg.report_poll_interval_sec)
            elapsed += self.cfg.report_poll_interval_sec
            resp = self._client.get(
                f"{self.cfg.api_base}/reporting/async-results/{task_id}"
            )
            if resp.status_code == 204:
                continue
            if resp.status_code == 200:
                return resp.text
            raise BeeswaxError(
                f"Polling failed: HTTP {resp.status_code} — {resp.text[:200]}"
            )
        raise BeeswaxReportTimeout(f"Report {task_id} timed out after {elapsed}s")

    def fetch_report(
        self,
        payload: dict[str, Any],
        label: str = "report",
        row_cap: int = 30000,
    ) -> list[dict[str, Any]]:
        """Submit + poll + parse. Binary-splits on `row_cap` if hit.

        Mirrors `sboFetchReportWithSplit_` from the Apps Script.
        """
        task_id = self.submit_report(payload, label=label)
        csv_body = self.poll_report(task_id)
        rows = list(csv.DictReader(io.StringIO(csv_body)))

        if len(rows) < row_cap:
            return rows

        # Hit the cap — binary-split on line_item_id filter
        li_filter = payload.get("filters", {}).get("line_item_id", "")
        li_ids = [x for x in li_filter.split(",") if x.strip()]
        if len(li_ids) <= 1:
            return rows  # single LI hit cap; can't split further
        mid = len(li_ids) // 2
        left, right = li_ids[:mid], li_ids[mid:]
        time.sleep(1)
        left_payload = {
            **payload,
            "filters": {**payload["filters"], "line_item_id": ",".join(left)},
        }
        right_payload = {
            **payload,
            "filters": {**payload["filters"], "line_item_id": ",".join(right)},
        }
        return self.fetch_report(
            left_payload, label=f"{label}-L", row_cap=row_cap
        ) + self.fetch_report(right_payload, label=f"{label}-R", row_cap=row_cap)

    # ── line items ────────────────────────────────────────────

    def fetch_line_items(self, ids: Iterable[str]) -> list[dict[str, Any]]:
        """GET /line-items?id__in=... in chunks of `cfg.chunk_size`."""
        return list(self._fetch_paginated("line-items", ids))

    def fetch_bid_modifiers(self, ids: Iterable[str]) -> list[dict[str, Any]]:
        """GET /bid-modifiers?id__in=... in chunks."""
        return list(self._fetch_paginated("bid-modifiers", ids))

    def fetch_targeting_expressions(
        self, ids: Iterable[str]
    ) -> list[dict[str, Any]]:
        """GET /targeting-expressions?id__in=... in chunks."""
        return list(self._fetch_paginated("targeting-expressions", ids))

    def fetch_advertisers(self, ids: Iterable[str]) -> list[dict[str, Any]]:
        return list(self._fetch_paginated("advertisers", ids))

    def _fetch_paginated(
        self, endpoint: str, ids: Iterable[str]
    ) -> Iterator[dict[str, Any]]:
        assert self._client is not None
        all_ids = [str(i) for i in ids if str(i).strip()]
        for i in range(0, len(all_ids), self.cfg.chunk_size):
            chunk = all_ids[i : i + self.cfg.chunk_size]
            for attempt in range(3):
                try:
                    resp = self._client.get(
                        f"{self.cfg.api_base}/{endpoint}",
                        params={"id__in": ",".join(chunk)},
                    )
                    break
                except httpx.ReadTimeout:
                    if attempt == 2:
                        raise BeeswaxError(
                            f"GET /{endpoint} timed out after 3 attempts"
                        )
                    time.sleep(2 ** (attempt + 1))
            if resp.status_code != 200:
                raise BeeswaxError(
                    f"GET /{endpoint} HTTP {resp.status_code} — {resp.text[:200]}"
                )
            for r in resp.json().get("results", []):
                yield r
            if i + self.cfg.chunk_size < len(all_ids):
                time.sleep(0.3)  # gentle pacing — well under rate limit

    # ── modifier writes ───────────────────────────────────────

    def _is_transient(self, status_code: int) -> bool:
        """Server errors and rate limiting → worth retrying."""
        return status_code in (429, 500, 502, 503, 504)

    @retry(
        retry=retry_if_exception_type((BeeswaxRateLimitError, httpx.ReadTimeout)),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def update_bid_modifier(
        self, modifier_id: str, modifier_obj: dict[str, Any]
    ) -> dict[str, Any]:
        """PUT /bid-modifiers/{id}. Retries transient errors up to 3×."""
        assert self._client is not None
        resp = self._client.put(
            f"{self.cfg.api_base}/bid-modifiers/{modifier_id}", json=modifier_obj
        )
        if self._is_transient(resp.status_code):
            raise BeeswaxRateLimitError(
                f"PUT /bid-modifiers/{modifier_id} transient HTTP "
                f"{resp.status_code}: {resp.text[:200]}"
            )
        if resp.status_code not in (200, 201):
            raise BeeswaxError(
                f"PUT /bid-modifiers/{modifier_id} HTTP {resp.status_code} — "
                f"{resp.text[:200]}"
            )
        return resp.json()

    @retry(
        retry=retry_if_exception_type((BeeswaxRateLimitError, httpx.ReadTimeout)),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def get_bid_modifier(self, modifier_id: str) -> dict[str, Any]:
        assert self._client is not None
        resp = self._client.get(f"{self.cfg.api_base}/bid-modifiers/{modifier_id}")
        if resp.status_code != 200:
            raise BeeswaxError(
                f"GET /bid-modifiers/{modifier_id} HTTP {resp.status_code} — "
                f"{resp.text[:200]}"
            )
        return resp.json()

    def create_bid_modifier(self, payload: dict[str, Any]) -> dict[str, Any]:
        assert self._client is not None
        resp = self._client.post(f"{self.cfg.api_base}/bid-modifiers", json=payload)
        if resp.status_code not in (200, 201):
            raise BeeswaxError(
                f"POST /bid-modifiers HTTP {resp.status_code} — {resp.text[:200]}"
            )
        return resp.json()

    @retry(
        retry=retry_if_exception_type((BeeswaxRateLimitError, httpx.ReadTimeout)),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def patch_line_item(
        self, line_item_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        assert self._client is not None
        resp = self._client.patch(
            f"{self.cfg.api_base}/line-items/{line_item_id}", json=payload
        )
        if self._is_transient(resp.status_code):
            raise BeeswaxRateLimitError(
                f"PATCH /line-items/{line_item_id} transient HTTP "
                f"{resp.status_code}: {resp.text[:200]}"
            )
        if resp.status_code not in (200, 201):
            raise BeeswaxError(
                f"PATCH /line-items/{line_item_id} HTTP {resp.status_code} — "
                f"{resp.text[:200]}"
            )
        return resp.json()

    def close(self) -> None:
        """Close the underlying httpx session. Idempotent."""
        if self._client is not None:
            self._client.close()
            self._client = None

    # ── lists (legacy endpoint) ───────────────────────────────

    def fetch_all_lists(self) -> list[dict[str, Any]]:
        """GET /lists — paginated via `next` field."""
        assert self._client is not None
        out: list[dict[str, Any]] = []
        url: str | None = f"{self.cfg.api_base}/lists"
        while url:
            resp = self._client.get(url)
            if resp.status_code != 200:
                raise BeeswaxError(
                    f"GET /lists HTTP {resp.status_code} — {resp.text[:200]}"
                )
            data = resp.json()
            out.extend(data.get("results", []))
            url = data.get("next")
            if url:
                time.sleep(0.2)
        return out

    def fetch_all_list_items_by_list_id(
        self, page_size: int = 10000
    ) -> dict[str, dict[str, bool]]:
        """Legacy /list_item endpoint — returns map listId → {dealId: True}."""
        assert self._client is not None
        all_items: list[dict[str, Any]] = []
        offset = 0
        while True:
            resp = self._client.get(
                self.cfg.legacy_list_base,
                params={"rows": page_size, "offset": offset},
            )
            if resp.status_code != 200:
                raise BeeswaxError(
                    f"GET /list_item HTTP {resp.status_code} — {resp.text[:200]}"
                )
            data = resp.json()
            if not data.get("success"):
                raise BeeswaxError(f"list_item success=false at offset={offset}")
            page = data.get("payload", []) or []
            all_items.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
            time.sleep(0.5)

        by_list_id: dict[str, dict[str, bool]] = {}
        for item in all_items:
            list_id = item.get("list_id")
            deal_id = item.get("list_item")
            if not list_id or deal_id is None or str(deal_id) == "null":
                continue
            by_list_id.setdefault(str(list_id), {})[str(deal_id)] = True
        return by_list_id
