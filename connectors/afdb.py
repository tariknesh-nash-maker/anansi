# connectors/afdb.py
import os
import re
import time
import logging
from datetime import datetime, timedelta, timezone
import feedparser

# Official AfDB RSS endpoints (Drupal views export as rss.xml)
FEEDS = [
    # Project-related procurement:
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/specific-procurement-notices-spns/rss.xml",
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/general-procurement-notices-gpns/rss.xml",
    # Corporate procurement (Bank’s own tenders) – this one exposes multiple RSS paths; keep both forms:
    "https://www.afdb.org/en/about-us/corporate-procurement?format=rss",
    "https://www.afdb.org/en/corporate-procurement/news-and-events/rss",
]

DATE_RE = re.compile(r"(?:deadline|closing)\s*[:\-]?\s*(\d{1,2}\s+\w+\s+\d{4})", re.I)

def _parse_deadline(text: str) -> str | None:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(1), "%d %B %Y").date().isoformat()
        return dt
    except Exception:
        return None

def _topic(title: str, summary: str = "") -> str | None:
    s = f"{title} {summary}".lower()
    if any(k in s for k in ["audit", "public finance", "budget", "tax"]):
        return "Fiscal Openness"
    if any(k in s for k in ["data", "digital", "ict", "information system", "software"]):
        return "Digital Governance"
    if any(k in s for k in ["open data", "transparency", "citizen", "participation"]):
        return "Open Government"
    return None

def fetch(ogp_only: bool = True, since_days: int | None = None) -> list[dict]:
    debug = os.getenv("AFDB_DEBUG")
    cutoff_date = None
    if since_days:
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=since_days)).date()

    out: list[dict] = []
    for url in FEEDS:
        try:
            feed = feedparser.parse(url)
            if debug:
                logging.info(f"[afdb] feed={url} status={getattr(feed, 'status', '?')} entries={len(feed.entries)}")

            for e in feed.entries:
                title = (e.title or "").strip()
                link = e.link
                summary = getattr(e, "summary", "") or getattr(e, "description", "")
                # pick published date if available
                pub_dt = None
                if getattr(e, "published_parsed", None):
                    pub_dt = datetime.fromtimestamp(time.mktime(e.published_parsed), tz=timezone.utc).date()
                elif getattr(e, "updated_parsed", None):
                    pub_dt = datetime.fromtimestamp(time.mktime(e.updated_parsed), tz=timezone.utc).date()

                if cutoff_date and pub_dt and pub_dt < cutoff_date:
                    continue

                deadline = _parse_deadline(summary)
                topic = _topic(title, summary) if ogp_only else None
                if ogp_only and not topic:
                    continue

                out.append({
                    "source": "AfDB",
                    "country": None,  # country is often in the body, not consistently in the feed
                    "title": title,
                    "url": link,
                    "published": pub_dt.isoformat() if pub_dt else None,
                    "deadline": deadline,
                    "topic": topic,
                })
        except Exception as e:
            logging.warning(f"[afdb] feed failed url={url}: {e}")

    return out
