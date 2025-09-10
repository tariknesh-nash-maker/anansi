from __future__ import annotations
import re, html
from typing import List, Dict, Any
import requests
from bs4 import BeautifulSoup
from utils.date_parse import to_iso_date

HEADERS = {"User-Agent":"Mozilla/5.0 (compatible; anansi/1.0)"}

URLS = [
    # Open consultancy opportunities
    "https://www.afdb.org/en/projects-and-operations/procurement/consultancy",
    # Notices (EOIs/RFPs) – filter client-side by “Ongoing/Open” if present in DOM
    "https://www.afdb.org/en/projects-and-operations/procurement/notices",
]

def _parse_listing(url: str) -> List[Dict[str,Any]]:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    out: List[Dict[str,Any]] = []

    # Generic listing items
    for item in soup.select(".c-listing__item, article, li"):
        a = item.select_one("a[href]")
        if not a: 
            continue
        title = a.get_text(" ", strip=True)
        href = a.get("href","")
        link = href if href.startswith("http") else f"https://www.afdb.org{href}"

        blob = item.get_text(" ", strip=True)
        # try extracting “Deadline” if present
        deadline = None
        m = re.search(r"(deadline|closing\s*date|closing\s*on)\s*[:\-]?\s*(.+?)($|\s{2,}|\. )", blob, flags=re.I)
        if m:
            deadline = to_iso_date(m.group(2))

        # country occasionally in badges/labels
        country = ""
        badge = item.select_one(".c-badge, .o-badge, .badge")
        if badge:
            country = badge.get_text(" ", strip=True)

        out.append({
            "title": html.unescape(title),
            "source": "AfDB",
            "deadline": deadline,
            "country": country,
            "topic": None,
            "url": link,
            "summary": blob.lower(),
        })
    return out

class Connector:
    def fetch(self, days_back: int = 60) -> List[Dict[str,Any]]:
        out: List[Dict[str,Any]] = []
        for u in URLS:
            try:
                out.extend(_parse_listing(u))
            except Exception:
                continue
        # Filter obvious “Closed/Cancelled”
        filtered = []
        for it in out:
            s = it.get("summary","")
            if any(x in s for x in ["closed", "cancelled", "canceled"]):
                continue
            filtered.append(it)
        return filtered

# ---- Back-compat procedural API (for existing aggregator) ----
def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    """
    Backwards-compatible wrapper so old aggregator imports work:
      from connectors.X import fetch as fetch_X
    """
    items = Connector().fetch(days_back=since_days)
    if ogp_only:
        try:
            from filters import ogp_relevant
            filt = []
            for it in items:
                text = f"{it.get('title','')} {it.get('summary','')}"
                if ogp_relevant(text):
                    filt.append(it)
            items = filt
        except Exception:
            # If filters module not present, just return items
            pass
    return items

def accepted_args():
    # Preserve your logging of accepted args
    return ["ogp_only", "since_days"]
