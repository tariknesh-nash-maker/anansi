# connectors/world_bank.py
# World Bank procurement via Finances One (DS00979 / RS00909)
# - Class + function APIs (back-compat with your aggregator)
# - Uses stable datacatalog API with top/skip paging
# - Soft filters: never zero out just because topics/dates are missing

from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timedelta
import os, time
import requests
import dateparser

F1_BASE = "https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice"
DATASET_ID = "DS00979"  # Procurement Notice
RESOURCE_ID = "RS00909"
UA = os.getenv("ANANSI_UA", "Mozilla/5.0 (compatible; anansi/1.0)")
HEADERS = {"User-Agent": UA, "Accept": "application/json"}

# ---------------- helpers ----------------

def _to_iso(s: str | None) -> str | None:
    if not s:
        return None
    dt = dateparser.parse(s, settings={"DATE_ORDER": "DMY", "PREFER_DAY_OF_MONTH": "first"})
    return dt.date().isoformat() if dt else None

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    return default if v is None else str(v).strip().lower() in ("1","true","yes")

def _prefer_or_fallback(preferred: List[Dict[str, Any]], fallback: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return preferred if preferred else fallback

def _fetch_slice(top: int, skip: int, debug: bool = False):
    # F1 returns {"count": <int>, "data": [ ... ]} always on success
    url = f"{F1_BASE}?datasetId={DATASET_ID}&resourceId={RESOURCE_ID}&type=json&top={top}&skip={skip}"
    if debug:
        print(f"[worldbank:F1] GET {url}")
    r = requests.get(url, headers=HEADERS, timeout=45)
    r.raise_for_status()
    return r.json() or {}

# ---------------- core impl ----------------

def _wb_fetch_impl(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    """
    Pull the most recent procurement notices from Finances One.
    Strategy:
      1) probe count with a tiny call (top=1) to know total rows
      2) read the newest 'slices' from the tail using skip = count - top
      3) filter gently by publication/deadline recency (if present)
    Env knobs (optional):
      WB_MAX_RESULTS (default 60)  -> how many items to return total
      WB_PAGES (default 1)         -> how many tail slices to fetch (each up to 1000)
      WB_DEBUG (0/1)               -> extra prints
      WB_REQUIRE_TOPIC_MATCH (0/1) + WB_TOPIC_LIST (pipe-separated) -> soft preference
    """
    debug = _env_bool("WB_DEBUG", False)
    max_results = _env_int("WB_MAX_RESULTS", 60)
    # Each slice can be up to 1000 (API limit). We keep it modest to be kind.
    slice_top_default = min( max_results, 500 )
    slice_top = _env_int("WB_F1_SLICE_TOP", slice_top_default)
    slice_top = max(1, min(slice_top, 1000))
    pages = max(1, min(_env_int("WB_PAGES", 1), 10))  # read that many tail slices

    # Probe count
    try:
        probe = _fetch_slice(top=1, skip=0, debug=debug)
        total = int(probe.get("count", 0))
        if debug:
            print(f"[worldbank:F1] total={total}")
    except Exception as e:
        if debug:
            print(f"[worldbank:F1] probe failed: {e}")
        return []

    items: List[Dict[str, Any]] = []
    collected = 0
    # Walk backward from the tail: last page, then previous, etc.
    for i in range(pages):
        if total <= 0:
            break
        top_now = min(slice_top, max_results - collected)
        if top_now <= 0:
            break
        skip = max(total - ((i + 1) * top_now), 0)
        try:
            chunk = _fetch_slice(top=top_now, skip=skip, debug=debug)
        except Exception as e:
            if debug:
                print(f"[worldbank:F1] slice {i} failed: {e}")
            time.sleep(0.8)
            continue

        rows = chunk.get("data") or []
        if debug:
            print(f"[worldbank:F1] slice {i} rows={len(rows)} skip={skip} top={top_now}")

        for d in rows:
            # Field names per dataset page:
            # bid_description, publication_date, deadline_date, country_name, url, ...
            title = (d.get("bid_description") or d.get("notice_type") or "").strip()
            if not title:
                continue
            url = d.get("url") or ""
            country = d.get("country_name") or ""
            pub_iso = _to_iso(d.get("publication_date"))
            deadline_iso = _to_iso(d.get("deadline_date"))

            # Soft recency: keep if either date is within 'days_back' (if present). If both missing, keep.
            since = (datetime.utcnow().date() - timedelta(days=days_back)).isoformat()
            keep = True
            if pub_iso:
                keep = keep and (pub_iso >= since)
            if deadline_iso:
                keep = keep and (deadline_iso >= since)
            # if both missing, keep remains True

            if not keep:
                continue

            items.append({
                "title": title,
                "source": "World Bank (F1)",
                "deadline": deadline_iso,
                "country": country,
                "topic": None,
                "url": url,
                "summary": f"{title} {country} pub:{pub_iso or ''}".lower(),
            })
            collected += 1
            if collected >= max_results:
                break
        if collected >= max_results:
            break

    # ---- Soft OGP & exclusions (never zero out) ----
    if ogp_only:
        try:
            from filters import ogp_relevant, is_excluded
            preferred = [it for it in items if ogp_relevant(f"{it['title']} {it.get('summary','')}")]
            items = _prefer_or_fallback(preferred, items)
            items = [it for it in items if not is_excluded(f"{it['title']} {it.get('summary','')}")]
        except Exception:
            pass

    # ---- Soft topic preference (env-driven) ----
    require_topic_match = _env_bool("WB_REQUIRE_TOPIC_MATCH", False)
    topic_raw = os.getenv("WB_TOPIC_LIST", "")
    topic_list = [t.strip().lower() for t in topic_raw.split("|") if t.strip()]
    if require_topic_match and topic_list:
        matched = []
        for it in items:
            text = f"{it.get('title','')} {it.get('summary','')}".lower()
            if any(t in text for t in topic_list):
                matched.append(it)
        items = _prefer_or_fallback(matched, items)

    return items

# ---------------- public APIs ----------------

class Connector:
    def fetch(self, days_back: int = 90):
        return _wb_fetch_impl(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _wb_fetch_impl(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
