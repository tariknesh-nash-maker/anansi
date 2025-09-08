# -*- coding: utf-8 -*-
"""
Normalization pipeline for anansi opportunities.

Input  (from connectors): list[dict] with fields like:
  id, title, donor, url, deadline, published_date, status, tags (list[str]), country_scope, amount, currency

Output (normalized):
  id                stable sha1 hash (donor|url|title|deadline)
  title             cleaned text
  donor             canonical source (e.g., "World Bank", "UNDP", "EU (TED)", "AfDB", "AFD")
  url               canonical url
  deadline          YYYY-MM-DD or None
  published_date    YYYY-MM-DD or None
  status            "open"|"forthcoming"|"closed"|None   (computed if missing)
  themes            list[str] from the canonical taxonomy below
  country_scope     list[str] (split chips; cleaned)
  amount_min        float|None
  amount_max        float|None
  currency          str|None
  source_tags       original connector tags (for debugging)

Utility:
  - deduplicate by url first, then (donor,title,deadline)
  - optional filters: future_only, require_deadline
  - digest formatter for Slack/email lines
"""
from __future__ import annotations

import hashlib
import html
import logging
import re
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from dateutil import parser as dateparser

LOG = logging.getLogger(__name__)

# -------------------------
# Canonical taxonomy
# -------------------------
CANON_THEMES = [
    "Open Government",
    "Digital Governance",
    "Anti-Corruption",
    "Civic Space",
    "Fiscal Openness",
    "Justice and Rule of Law",
    "Gender and Inclusion",
    "Climate and Environment",
    "Media Freedom",
]

# map connector tag -> canonical theme (first match wins)
TAG_TO_THEME = {
    "ai_digital": "Digital Governance",
    "anti_corruption": "Anti-Corruption",
    "civic_participation": "Civic Space",
    "budget": "Fiscal Openness",
    "justice": "Justice and Rule of Law",
    "governance": "Open Government",
}

# fallback keyword map (title + scope text)
KW_TO_THEME = [
    (re.compile(r"\b(media|press|journalis[m|t]|broadcast)\b", re.I), "Media Freedom"),
    (re.compile(r"\b(gender|women|girls|GBV|SGBV)\b", re.I), "Gender and Inclusion"),
    (re.compile(r"\b(climate|environment|biodivers|resilienc|adaptation|mitigation)\b", re.I), "Climate and Environment"),
    (re.compile(r"\b(open data|digital|e-?gov|ICT|AI|data|cybersecurity)\b", re.I), "Digital Governance"),
    (re.compile(r"\b(anti-?corruption|integrity|illicit|brib|procuremen|transparen|accountab)\b", re.I), "Anti-Corruption"),
    (re.compile(r"\b(civic|participation|civil society|freedom of assembly|association)\b", re.I), "Civic Space"),
    (re.compile(r"\b(budget|public finance|PFM|audit|revenue|tax)\b", re.I), "Fiscal Openness"),
    (re.compile(r"\b(justice|rule of law|court|legal aid|ADR|judici)\b", re.I), "Justice and Rule of Law"),
    (re.compile(r"\b(governance|open government|accountable institution)\b", re.I), "Open Government"),
]

# obvious junk to drop from titles (fixes UNDP css leakage)
TITLE_JUNK_PATTERNS = [
    re.compile(r"\.css\b", re.I),
    re.compile(r"\.js\b", re.I),
    re.compile(r"site[-_ ]header", re.I),
    re.compile(r"^\s*(home|menu|language)\s*$", re.I),
]

WHITESPACE = re.compile(r"\s+")

def _sha1(*parts: str) -> str:
    base = "::".join([p.strip() for p in parts if p and isinstance(p, str)])
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]

def _to_iso(d: Optional[str]) -> Optional[str]:
    if not d:
        return None
    try:
        return dateparser.parse(d).date().isoformat()
    except Exception:
        return None

def _clean_title(t: str) -> str:
    if not t:
        return ""
    t = html.unescape(t)
    t = t.strip()
    # kill obvious junk titles
    for rx in TITLE_JUNK_PATTERNS:
        if rx.search(t):
            return ""
    # collapse whitespace & trim stray delimiters
    t = WHITESPACE.sub(" ", t)
    t = t.strip(" -|>:\u2013\u2014")
    return t

def _status_from_dates(deadline_iso: Optional[str]) -> Optional[str]:
    if not deadline_iso:
        return None
    try:
        today = datetime.now(timezone.utc).date()
        d = dateparser.parse(deadline_iso).date()
        if d > today:
            return "open"
        if d == today:
            return "open"  # treat today as open
        return "closed"
    except Exception:
        return None

def _themes_from(record: Dict) -> List[str]:
    # 1) connector tags
    tags = record.get("tags") or []
    for t in tags:
        th = TAG_TO_THEME.get(t)
        if th:
            return [th]
    # 2) keywords from title + scope
    text = f"{record.get('title','')} {record.get('country_scope','')}"
    for rx, th in KW_TO_THEME:
        if rx.search(text):
            return [th]
    # 3) default
    return ["Open Government"]

def _split_scope(val: Optional[str]) -> List[str]:
    if not val:
        return []
    # split by common separators
    parts = re.split(r"[;,/|]", val)
    out = []
    for p in parts:
        s = WHITESPACE.sub(" ", p).strip()
        if s:
            out.append(s)
    return out

def _norm_amount(text: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Best-effort amount normalization. Accept simple patterns like:
      "EUR 300000", "€300,000", "USD 1–3M", "USD 1,000,000 - 2,000,000"
    Returns (min, max, currency)
    """
    if not text:
        return None, None, None
    cur_match = re.search(r"\b(USD|EUR|GBP|MAD|CAD|AUD)\b|[€$£]", text)
    currency = None
    if cur_match:
        sym = cur_match.group(0)
        currency = {"€":"EUR", "$":"USD", "£":"GBP"}.get(sym, sym)

    # strip non-digits for rough parse
    nums = re.findall(r"[\d][\d,\.]*", text)
    if not nums:
        return None, None, currency
    vals = []
    for n in nums[:2]:
        try:
            vals.append(float(n.replace(",", "")))
        except Exception:
            continue
    if not vals:
        return None, None, currency
    if len(vals) == 1:
        return vals[0], vals[0], currency
    return min(vals), max(vals), currency

def normalize(
    records: Iterable[Dict],
    *,
    future_only: bool = False,
    require_deadline: bool = False,
    today_utc: Optional[datetime] = None,
) -> List[Dict]:
    """
    Normalize + dedupe + (optional) filter to future deadlines only.
    """
    today = (today_utc or datetime.now(timezone.utc)).date()
    seen_urls = set()
    seen_keys = set()
    normalized: List[Dict] = []

    for r in records:
        title = _clean_title(r.get("title") or "")
        if not title:
            # skip junk/malformed entries (fixes UNDP css leak)
            continue

        donor = (r.get("donor") or "").strip() or "Unknown"
        url = (r.get("url") or "").strip()
        if url:
            if url in seen_urls:
                continue
            seen_urls.add(url)

        deadline = _to_iso(r.get("deadline"))
        published = _to_iso(r.get("published_date"))

        # compute/override status if missing
        status = r.get("status") or _status_from_dates(deadline)

        # themes
        themes = _themes_from({"title": title, "country_scope": r.get("country_scope"), "tags": r.get("tags")})

        # country scope → list[str]
        scope_list = _split_scope(r.get("country_scope"))

        # amounts (best-effort)
        amin, amax, currency = _norm_amount(r.get("amount"))

        # filters
        if require_deadline and not deadline:
            continue
        if future_only and deadline:
            try:
                if dateparser.parse(deadline).date() <= today:
                    continue
            except Exception:
                # if deadline unparsable, drop
                continue

        # dedupe by (donor, norm title, deadline)
        key = (donor.lower(), title.lower(), deadline or "")
        if key in seen_keys:
            continue
        seen_keys.add(key)

        nid = r.get("id") or _sha1(donor, url or title, deadline or "")
        normalized.append({
            "id": nid,
            "title": title,
            "donor": donor,
            "url": url,
            "deadline": deadline,
            "published_date": published,
            "status": status,
            "themes": themes,
            "country_scope": scope_list,
            "amount_min": amin,
            "amount_max": amax,
            "currency": currency,
            "source_tags": r.get("tags") or [],
        })

    # sort: soonest deadline first, then newest publication
    def sort_key(x: Dict):
        try:
            d = dateparser.parse(x["deadline"]).date() if x["deadline"] else datetime.max.date()
        except Exception:
            d = datetime.max.date()
        try:
            p = dateparser.parse(x["published_date"]).date() if x["published_date"] else datetime.min.date()
        except Exception:
            p = datetime.min.date()
        return (d, -int(p.strftime("%s")))
    normalized.sort(key=sort_key)
    return normalized

# -------------------------
# Formatter for digests
# -------------------------
def format_digest_line(op: Dict) -> str:
    loc = " / ".join(op.get("country_scope") or []) or ""
    theme = (op.get("themes") or ["Open Government"])[0]
    dl = op.get("deadline") or "N/A"
    title = op.get("title") or "Untitled"
    donor = op.get("donor") or "Unknown"
    # If title already starts with a location like "Senegal —", don’t repeat loc
    show_loc = loc and not title.lower().startswith(loc.lower())
    prefix = f"{loc} — " if show_loc else ""
    return f"• {prefix}{title} ({donor}) — deadline: {dl} — {theme}"
