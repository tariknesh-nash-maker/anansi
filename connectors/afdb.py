# -*- coding: utf-8 -*-
"""
AfDB — Calls for Proposals via SMApply (public pages)

Why: AfDB corporate/project-procurement HTML and RSS endpoints often block bots
or CI runners. The AfDB/ADF team publishes open Calls on SMApply, which is
accessible without auth for program overview pages.

Sources we discover:
  - Landing: https://afdb.smapply.org/  (scan for /prog/... links)
  - Known program slugs (kept extensible): CAW, ACCF, TSF, etc.

We parse titles + try to extract a deadline from visible text.
If no explicit deadline is present, we still return a normalized opportunity.

Usage:
  from connectors.afdb import fetch
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

LOG = logging.getLogger(__name__)

BASE = "https://afdb.smapply.org"
LANDING = f"{BASE}/"

HEADERS = {
    "User-Agent": "anansi/afdb (+https://github.com/tariknesh-nash-maker/anansi)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en,fr;q=0.9",
    "Connection": "close",
}

# Known public program pages (kept small; discovery will add more)
KNOWN_PROGRAMS = [
    "/prog/africa_climate_change_fund_fourth_call_for_proposals/",
    "/prog/adf_climate_action_window_mitigation_sub-window_call_for_proposals/",
    "/prog/adf_climate_action_window_technical_assistance_sub-window_call_for_proposals/",
    "/prog/african_development_fund_caw_adaptation_sub-window_2023/",
    "/prog/tsf-cfp-pillar3-2025/",
]

OGP_HINTS = [
    "governance", "inclusive institutions", "transparen", "accountab", "open data",
    "digital", "ict", "pfm", "public finance", "budget", "integrity", "anti-corruption",
    "justice", "rule of law", "civic", "participation", "citizen"
]

DEADLINE_RX = re.compile(
    r"(?:Deadline|Closing|Closes|Application deadline)\s*(?:date|time|:)?\s*"
    r"(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})",
    re.I
)

@dataclass
class Opportunity:
    id: str
    title: str
    donor: str
    url: str
    deadline: Optional[str]
    published_date: Optional[str]
    status: Optional[str]
    tags: List[str]
    country_scope: Optional[str]
    amount: Optional[str] = None
    currency: Optional[str] = None
    def to_dict(self) -> Dict: return asdict(self)

def _hash(*parts: str) -> str:
    return "afdb_" + hashlib.sha1("::".join([p for p in parts if p]).encode()).hexdigest()[:16]

def _classify(text: str) -> List[str]:
    t = (text or "").lower()
    tags = set()
    if any(k in t for k in ["digital","data","ict","open data"]): tags.add("ai_digital")
    if any(k in t for k in ["budget","public finance","pfm","audit"]): tags.add("budget")
    if any(k in t for k in ["transparen","accountab","anti-corruption","integrity","procurement"]): tags.add("anti_corruption")
    if any(k in t for k in ["civic","participation","citizen"]): tags.add("civic_participation")
    if any(k in t for k in ["justice","rule of law"]): tags.add("justice")
    if not tags: tags.add("governance")
    return sorted(tags)

def _get(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def _discover_program_links() -> List[str]:
    links: List[str] = []
    try:
        soup = _get(LANDING)
        for a in soup.select("a[href*='/prog/']"):
            href = a.get("href", "")
            if "/prog/" in href:
                links.append(urljoin(BASE, href))
    except Exception as e:
        LOG.warning("AfDB SMApply landing fetch failed: %s", e)
    # add known slugs (works even if not present on landing today)
    for slug in KNOWN_PROGRAMS:
        links.append(urljoin(BASE, slug))
    # de-dup & keep only same-host absolute URLs
    uniq = []
    seen = set()
    for u in links:
        if not u.startswith(BASE):
            continue
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq

def _parse_program(url: str, since: Optional[datetime]) -> Optional[Opportunity]:
    try:
        s = _get(url)
    except Exception as e:
        LOG.debug("AfDB SMApply detail failed %s: %s", url, e)
        return None

    title_el = s.select_one("h1, .program-title, .title")
    title = (title_el.get_text(" ", strip=True) if title_el else "").strip()
    if not title:
        # fallback: use <title>
        tt = s.select_one("title")
        title = (tt.get_text(" ", strip=True) if tt else "").strip()
    if not title:
        return None

    text = s.get_text(" ", strip=True)

    # Best-effort publication date (SMApply rarely shows a machine-readable date)
    pub = None
    # Some pages show a timeline; ignore if we can't parse a clear date.
    # (We rely on since_days to filter by deadline when possible.)

    # Deadline
    deadline = None
    m = DEADLINE_RX.search(text)
    if m:
        try:
            deadline = dateparser.parse(m.group(1)).date().isoformat()
        except Exception:
            deadline = None

    status = None
    try:
        if deadline and dateparser.parse(deadline).date() >= datetime.utcnow().date():
            status = "open"
    except Exception:
        pass

    # Filter out clearly non-governance calls if ogp_only will be used later;
    # here we always return and let fetch() filter.
    opp = Opportunity(
        id=_hash(title, url),
        title=title,
        donor="AfDB",
        url=url,
        deadline=deadline,
        published_date=pub,
        status=status,
        tags=_classify(title + " " + text),
        country_scope=None,
    )
    # since filter (only if we have a pub or deadline)
    if since:
        dd = None
        try:
            dd = dateparser.parse(opp.deadline).date() if opp.deadline else None
        except Exception:
            dd = None
        if dd and dd < since.date():
            return None
    return opp

def fetch(
    max_items: int = 60,
    since_days: Optional[int] = 365,
    ogp_only: bool = True,
    future_only: bool = True,           # <— new
) -> List[Dict]:
    since_dt = (datetime.now(timezone.utc) - timedelta(days=since_days)) if since_days else None
    out: List[Opportunity] = []
    today = datetime.now(timezone.utc).date()   # compare in UTC (deadlines are dates)

    for url in _discover_program_links():
        if len(out) >= max_items:
            break
        opp = _parse_program(url, since_dt)
        if not opp:
            continue

        # --- NEW: require a deadline strictly AFTER today
        if future_only:
            if not opp.deadline:
                continue
            try:
                d = dateparser.parse(opp.deadline).date()
            except Exception:
                continue
            if d <= today:                # strictly after today
                continue

        text_check = (opp.title + " " + " ".join(opp.tags)).lower()
        if ogp_only and not any(h in text_check for h in [h.lower() for h in OGP_HINTS]):
            continue

        # keep status consistent (optional)
        opp.status = "open"

        out.append(opp)
        if len(out) >= max_items:
            break

    # sort & dedup (unchanged) ...
    def sk(o: Opportunity):
        try:
            return (dateparser.parse(o.deadline).date() if o.deadline else datetime.max.date(), o.title)
        except Exception:
            return (datetime.max.date(), o.title)

    out.sort(key=sk)
    seen, uniq = set(), []
    for o in out:
        if o.url in seen: continue
        seen.add(o.url); uniq.append(o)
    return [o.to_dict() for o in uniq[:max_items]]
# --------------------------------------------------------------------------

class Connector:
    name = "afdb"
    def __init__(self, **kw): self.kwargs = kw
    def fetch(self, **kw) -> List[Dict]:
        return fetch(**{**self.kwargs, **kw})
