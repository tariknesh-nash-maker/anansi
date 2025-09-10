# connectors/afdb.py
from __future__ import annotations
from typing import List, Dict, Any, Set
from datetime import datetime, timedelta, timezone
import os, time, re, logging
import requests, feedparser
from bs4 import BeautifulSoup

UA = os.getenv("ANANSI_UA", "Mozilla/5.0 (compatible; anansi/1.0)")
HEADERS = {"User-Agent": UA}

SPN_RSS = "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/specific-procurement-notices-spns/rss.xml"
GPN_RSS = "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/general-procurement-notices-gpns/rss.xml"
RSS_FEEDS = [SPN_RSS, GPN_RSS]

# Fallback listing pages (server-rendered)
LISTING_PAGES = [
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/specific-procurement-notices-spns",
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/general-procurement-notices-gpns",
]

DEADLINE_RE = re.compile(r"(?:deadline|closing(?: date)?)\s*[:\-]?\s*([0-9]{1,2}\s+\w+\s+[0-9]{4})", re.I)

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    return default if v is None else str(v).strip().lower() in ("1","true","yes")

def _to_date_from_struct(tm) -> datetime | None:
    try:
        return datetime(*tm[:6], tzinfo=timezone.utc)
    except Exception:
        return None

def _parse_deadline(text: str) -> str | None:
    m = DEADLINE_RE.search(text or "")
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%d %B %Y").date().isoformat()
    except Exception:
        return None

def _rss_fetch(days_back: int, max_items: int, debug: bool) -> List[Dict[str, Any]]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).date()
    out: List[Dict[str, Any]] = []
    for url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            if debug:
                logging.info(f"[afdb] RSS url={url} status={getattr(feed,'status','?')} entries={len(feed.entries)}")
            for e in feed.entries:
                title = (getattr(e, "title", "") or "").strip()
                link  = (getattr(e, "link", "") or "").strip()
                if not title or not link:
                    continue
                pub_dt = None
                if getattr(e, "published_parsed", None):
                    pub_dt = _to_date_from_struct(e.published_parsed)
                elif getattr(e, "updated_parsed", None):
                    pub_dt = _to_date_from_struct(e.updated_parsed)
                if pub_dt and pub_dt.date() < cutoff:
                    continue
                summary = (getattr(e, "summary", "") or getattr(e, "description", "") or "")
                deadline = _parse_deadline(summary)
                out.append({
                    "title": title,
                    "source": "AfDB",
                    "deadline": deadline,
                    "country": "",
                    "topic": None,
                    "url": link,
                    "summary": (summary or title).lower(),
                })
                if len(out) >= max_items:
                    return out
        except Exception as ex:
            if debug:
                logging.warning(f"[afdb] RSS failed {url}: {ex}")
            continue
    return out

def _collect_listing_links(url: str, debug: bool) -> Set[str]:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    links: Set[str] = set()
    # Loosen selectors; keep within /procurement/ to avoid generic links
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        full = href if href.startswith("http") else f"https://www.afdb.org{href}"
        if "/procurement/" in full:
            links.add(full)
    if debug:
        logging.info(f"[afdb] listing {url} -> {len(links)} links")
    return links

def _parse_detail(url: str, debug: bool) -> Dict[str, Any] | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        title_tag = soup.select_one("h1, h2") or soup.select_one("title")
        title = (title_tag.get_text(" ", strip=True) if title_tag else "AfDB Notice").strip()
        text = soup.get_text(" ", strip=True)
        # deadline from labeled fields or whole page
        deadline = None
        for dt in soup.select("dt, strong, b"):
            lbl = dt.get_text(" ", strip=True).lower()
            if "dead" in lbl or "clos" in lbl:
                val = dt.find_next("dd")
                raw = val.get_text(" ", strip=True) if val else ""
                dl_try = _parse_deadline(f"deadline {raw}")
                if dl_try:
                    deadline = dl_try
                    break
        if not deadline:
            deadline = _parse_deadline(text)
        return {
            "title": title,
            "source": "AfDB",
            "deadline": deadline,
            "country": "",
            "topic": None,
            "url": url,
            "summary": text.lower()[:800],
        }
    except Exception as ex:
        if debug:
            logging.warning(f"[afdb] detail failed {url}: {ex}")
        return None

def _apply_filters(items: List[Dict[str, Any]], ogp_only: bool) -> List[Dict[str, Any]]:
    try:
        from filters import ogp_relevant, is_excluded
        items = [it for it in items if not is_excluded(f"{it.get('title','')} {it.get('summary','')}")]
        if ogp_only:
            preferred = [it for it in items if ogp_relevant(f"{it.get('title','')} {it.get('summary','')}")]
            items = preferred or items
    except Exception:
        pass
    return items

def _afdb_fetch(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    debug = _env_bool("AFDB_DEBUG", False)
    max_items = _env_int("AFDB_MAX", 40)

    # 1) RSS (fast path)
    items = _rss_fetch(days_back=days_back, max_items=max_items, debug=debug)
    if items:
        return _apply_filters(items, ogp_only)

    # 2) HTML fallback (robust, verbose)
    links: Set[str] = set()
    for lp in LISTING_PAGES:
        try:
            links |= _collect_listing_links(lp, debug=debug)
        except Exception as ex:
            if debug:
                logging.warning(f"[afdb] listing failed {lp}: {ex}")
            continue

    if debug:
        logging.info(f"[afdb] total candidate detail links: {len(links)}")

    out: List[Dict[str, Any]] = []
    for u in list(links)[: max_items * 2]:
        it = _parse_detail(u, debug=debug)
        if it:
            out.append(it)
        if len(out) >= max_items:
            break

    return _apply_filters(out, ogp_only)

class Connector:
    def fetch(self, days_back: int = 90):
        return _afdb_fetch(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _afdb_fetch(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
