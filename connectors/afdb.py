# connectors/afdb.py
from __future__ import annotations
from typing import List, Dict, Any, Set
from datetime import datetime, timedelta, timezone
import os, time, re, logging, requests, feedparser
from bs4 import BeautifulSoup
from utils.debug_utils import is_on, dump_text, dump_json, kv

UA = os.getenv("ANANSI_UA", "Mozilla/5.0 (compatible; anansi/1.0)")
HEADERS = {"User-Agent": UA}

SPN_RSS = "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/specific-procurement-notices-spns/rss.xml"
GPN_RSS = "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/general-procurement-notices-gpns/rss.xml"
RSS_FEEDS = [SPN_RSS, GPN_RSS]

LISTING_PAGES = [
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/specific-procurement-notices-spns",
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/general-procurement-notices-gpns",
]

DEADLINE_RE = re.compile(r"(?:deadline|closing(?: date)?)\s*[:\-]?\s*([0-9]{1,2}\s+\w+\s+[0-9]{4})", re.I)

def _env_int(name: str, default: int) -> int:
    try: return int(os.getenv(name, str(default)))
    except Exception: return default

def _to_date_from_struct(tm) -> datetime | None:
    try: return datetime(*tm[:6], tzinfo=timezone.utc)
    except Exception: return None

def _parse_deadline(text: str) -> str | None:
    m = DEADLINE_RE.search(text or "")
    if not m: return None
    try: return datetime.strptime(m.group(1), "%d %B %Y").date().isoformat()
    except Exception: return None

def _rss_fetch(days_back: int, max_items: int, verbose: bool) -> List[Dict[str, Any]]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).date()
    out: List[Dict[str, Any]] = []
    for url in RSS_FEEDS:
        feed = feedparser.parse(url)
        if verbose:
            kv("afdb:rss", url=url, status=getattr(feed,'status','?'), entries=len(feed.entries), bozo=getattr(feed,'bozo','?'))
            if getattr(feed, "bozo", 0):
                kv("afdb:rss_bozo", url=url, err=str(getattr(feed,"bozo_exception",""))[:200])
        for e in feed.entries:
            title = (getattr(e, "title", "") or "").strip()
            link  = (getattr(e, "link", "") or "").strip()
            if not title or not link:
                continue
            pub_dt = None
            if getattr(e, "published_parsed", None): pub_dt = _to_date_from_struct(e.published_parsed)
            elif getattr(e, "updated_parsed", None): pub_dt = _to_date_from_struct(e.updated_parsed)
            if pub_dt and pub_dt.date() < cutoff:
                continue
            summary = (getattr(e, "summary", "") or getattr(e, "description", "") or "")
            deadline = _parse_deadline(summary)
            out.append({
                "title": title, "source": "AfDB", "deadline": deadline,
                "country": "", "topic": None, "url": link,
                "summary": (summary or title).lower(),
            })
            if len(out) >= max_items:
                break
        if len(out) >= max_items:
            break
    if verbose:
        kv("afdb:rss_result", kept=len(out))
    return out

def _collect_listing_links(url: str, verbose: bool) -> Set[str]:
    r = requests.get(url, headers=HEADERS, timeout=30)
    if verbose:
        kv("afdb:listing_http", url=url, status=r.status_code, bytes=len(r.text or ""))
        dump_text("afdb-listing", r.text[:4000])
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    links: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href: continue
        full = href if href.startswith("http") else f"https://www.afdb.org{href}"
        if "/procurement/" in full and "/en/" in full:
            links.add(full.split("#")[0])
    if verbose:
        kv("afdb:links_found", url=url, links=len(links))
        dump_json("afdb-links", sorted(list(links))[:200])
    return links

def _parse_detail(url: str, verbose: bool) -> Dict[str, Any] | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if verbose:
            kv("afdb:detail_http", url=url, status=r.status_code, bytes=len(r.text or ""))
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        title_tag = soup.select_one("h1, h2") or soup.select_one("title")
        title = (title_tag.get_text(" ", strip=True) if title_tag else "AfDB Notice").strip()
        text = soup.get_text(" ", strip=True)
        deadline = None
        for dt in soup.select("dt, strong, b"):
            lbl = dt.get_text(" ", strip=True).lower()
            if "dead" in lbl or "clos" in lbl:
                val = dt.find_next("dd")
                raw = val.get_text(" ", strip=True) if val else ""
                dl_try = _parse_deadline(f"deadline {raw}")
                if dl_try: deadline = dl_try; break
        if not deadline:
            deadline = _parse_deadline(text)
        return {
            "title": title, "source": "AfDB", "deadline": deadline,
            "country": "", "topic": None, "url": url,
            "summary": text.lower()[:800],
        }
    except Exception as ex:
        if verbose:
            kv("afdb:detail_err", url=url, err=str(ex)[:200])
        return None

def _apply_filters(items: List[Dict[str, Any]], ogp_only: bool, verbose: bool) -> List[Dict[str, Any]]:
    raw = len(items)
    try:
        from filters import is_excluded
        items = [it for it in items if not is_excluded(f"{it.get('title','')} {it.get('summary','')}")]
    except Exception:
        pass
    after_ex = len(items)
    if ogp_only:
        try:
            from filters import ogp_relevant
            preferred = [it for it in items if ogp_relevant(f"{it.get('title','')} {it.get('summary','')}")]
            items = preferred or items
        except Exception:
            pass
    if verbose:
        kv("afdb:filter_counts", raw=raw, after_exclude=after_ex, returned=len(items))
    return items

def _afdb_fetch(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    # Verbose if env says so OR if we end up with 0 so we can see why
    env_verbose = is_on("AFDB_DEBUG", "DEBUG")
    max_items = _env_int("AFDB_MAX", 40)

    items = _rss_fetch(days_back=days_back, max_items=max_items, verbose=env_verbose)
    if items:
        return _apply_filters(items, ogp_only, env_verbose)

    # HTML fallback
    all_links: Set[str] = set()
    for lp in LISTING_PAGES:
        try:
            all_links |= _collect_listing_links(lp, verbose=env_verbose)
        except Exception as ex:
            print(f"[afdb] listing failed {lp}: {ex}")
            continue

    if not all_links and not env_verbose:
        # Force a one-time verbose attempt so you see what the page looks like
        for lp in LISTING_PAGES:
            try:
                _collect_listing_links(lp, verbose=True)
            except Exception:
                pass

    out: List[Dict[str, Any]] = []
    for u in list(all_links)[: max_items * 2]:
        it = _parse_detail(u, verbose=env_verbose)
        if it: out.append(it)
        if len(out) >= max_items:
            break

    if not out and not env_verbose:
        kv("afdb:empty", links=len(all_links), forcing_verbose=True)
        # Try parsing 2 detail pages verbosely for insight
        for u in list(all_links)[:2]:
            _parse_detail(u, verbose=True)

    return _apply_filters(out, ogp_only, env_verbose)

class Connector:
    def fetch(self, days_back: int = 90):
        return _afdb_fetch(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _afdb_fetch(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
