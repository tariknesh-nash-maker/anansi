# connectors/afdb.py
# African Development Bank – Project-related Procurement RSS (GPN + SPN)
# Feeds are public (Drupal views RSS). We parse the latest entries.
#
# Env:
#   AFDB_MAX (default 40)
#   AFDB_DEBUG (0/1)

from __future__ import annotations
from typing import List, Dict, Any
import os, time
import feedparser
from datetime import datetime, timedelta

# Confirmed RSS endpoints (project-related procurement)
FEEDS = [
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/specific-procurement-notices-spns/rss.xml",
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/general-procurement-notices-gpns/rss.xml",
]

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    return default if v is None else str(v).strip().lower() in ("1","true","yes")

def _to_iso_from_struct(tm) -> str | None:
    try:
        return datetime(*tm[:6]).date().isoformat()
    except Exception:
        return None

def _fetch_feed(url: str, debug: bool = False):
    if debug:
        print(f"[afdb] GET {url}")
    # feedparser handles redirects + gzip
    return feedparser.parse(url)

def _afdb_fetch(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    debug = _env_bool("AFDB_DEBUG", False)
    max_items = _env_int("AFDB_MAX", 40)
    since = datetime.utcnow().date() - timedelta(days=days_back)

    items: List[Dict[str, Any]] = []
    for url in FEEDS:
        try:
            fp = _fetch_feed(url, debug=debug)
        except Exception as e:
            if debug:
                print(f"[afdb] WARN feed error: {e}")
            continue
        for e in fp.entries[: max_items * 2]:
            title = (getattr(e, "title", "") or "").strip()
            link  = (getattr(e, "link", "") or "").strip()
            if not title or not link:
                continue

            # Use published/updated if provided
            pub_iso = None
            if getattr(e, "published_parsed", None):
                pub_iso = _to_iso_from_struct(e.published_parsed)
            elif getattr(e, "updated_parsed", None):
                pub_iso = _to_iso_from_struct(e.updated_parsed)

            # Keep recent items if we have a date; if not, keep anyway (permissive)
            keep = True
            if pub_iso:
                keep = pub_iso >= since.isoformat()
            if not keep:
                continue

            items.append({
                "title": title,
                "source": "AfDB",
                "deadline": None,            # not present in the feed
                "country": "",
                "topic": None,
                "url": link,
                "summary": (getattr(e, "summary", "") or title).lower(),
            })
            if len(items) >= max_items:
                break
        if len(items) >= max_items:
            break

    # Optional OGP/exclusion pass; never zero-out
    if ogp_only:
        try:
            from filters import ogp_relevant, is_excluded
            preferred = [it for it in items if ogp_relevant(f"{it['title']} {it.get('summary','')}")]
            items = preferred or items
            items = [it for it in items if not is_excluded(f"{it['title']} {it.get('summary','')}")]
        except Exception:
            pass

    return items

class Connector:
    def fetch(self, days_back: int = 90):
        return _afdb_fetch(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _afdb_fetch(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
