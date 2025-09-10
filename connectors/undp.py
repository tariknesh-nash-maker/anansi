from __future__ import annotations
import re, html
from typing import List, Dict, Any, Iterable
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from utils.date_parse import to_iso_date

BASE = "https://procurement-notices.undp.org"
SEARCH = BASE + "/search.cfm?cur={page}"
HEADERS = {"User-Agent":"Mozilla/5.0 (compatible; anansi/1.0)"}

def _notice_ids_from_page(soup: BeautifulSoup) -> List[str]:
    ids = []
    # Results table: anchors to view_notice.cfm?notice_id=xxxxx
    for a in soup.select("a[href*='view_notice.cfm?notice_id=']"):
        m = re.search(r"notice_id=(\d+)", a.get("href",""))
        if m:
            ids.append(m.group(1))
    return list(dict.fromkeys(ids))  # dedupe preserve order

def _fetch_notice(nid: str) -> Dict[str, Any]:
    url = f"{BASE}/view_notice.cfm?notice_id={nid}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    # Title: try header first, fallback to og:title
    title = ""
    h = soup.select_one("h2, h1")
    if h:
        title = h.get_text(" ", strip=True)
    if not title:
        og = soup.select_one("meta[property='og:title']")
        if og and og.get("content"):
            title = og.get("content", "").strip()

    # Details are in key/value rows; normalize the label text
    details = {}
    for row in soup.select("div.notice-details div.row, div#content div.row"):
        label = row.select_one(".columns.small-4, .small-4")
        value = row.select_one(".columns.small-8, .small-8")
        if not label or not value:
            continue
        k = re.sub(r"\s+", " ", label.get_text(" ", strip=True)).strip(": ").lower()
        v = re.sub(r"\s+", " ", value.get_text(" ", strip=True))
        details[k] = v

    country = details.get("country", "") or details.get("project country", "")
    deadline_raw = details.get("deadline", "") or details.get("deadline (local time)", "")
    deadline = to_iso_date(deadline_raw)

    # Topic inference: light heuristic on title + description
    desc = details.get("procurement method", "") + " " + details.get("assignment description", "")
    summary = (title + " " + desc).lower()

    return {
        "title": title or f"UNDP Notice {nid}",
        "source": "UNDP",
        "deadline": deadline,
        "country": country,
        "topic": None,   # normalized later
        "url": url,
        "summary": summary,
    }

class Connector:
    def fetch(self, days_back: int = 90) -> List[Dict[str,Any]]:
        out: List[Dict[str,Any]] = []
        # Crawl first ~10 pages; site sorts by recency
        for page in range(1, 11):
            r = requests.get(SEARCH.format(page=page), headers=HEADERS, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            ids = _notice_ids_from_page(soup)
            if not ids:
                break
            # Sample a handful + fetch all ids
            for nid in ids:
                try:
                    out.append(_fetch_notice(nid))
                except Exception:
                    continue
        return out
