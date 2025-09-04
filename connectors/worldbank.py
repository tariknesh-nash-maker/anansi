# connectors/worldbank.py
# World Bank "Procurement Notices" (official API) — robust future-deadline filter with safe fallback.
#
# Primary: return notices with deadline >= today (auto-detects deadline field names).
# Fallback: if none, return recently *published* items (last 365 days), capped to 20, so Slack isn't empty.
#
# Optional env:
#   WB_DEBUG=1  -> print diagnostics in GitHub Actions logs

from __future__ import annotations
import os, hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests

API = "https://search.worldbank.org/api/procnotices"
TIMEOUT = 25
ROWS = 100          # per page
PAGES = 10          # widen the window (~1000 items)
DEBUG = os.getenv("WB_DEBUG", "0") == "1"

DATE_FMTS = ("%Y-%m-%d", "%d-%b-%Y", "%d %b %Y", "%m/%d/%Y", "%Y/%m/%d", "%Y.%m.%d")

def _parse_date(val: Optional[str]) -> Optional[datetime]:
    if not val:
        return None
    v = str(val).strip()
    for fmt in DATE_FMTS:
        try:
            return datetime.strptime(v, fmt)
        except Exception:
            continue
    return None

def _to_iso(val: Optional[str]) -> str:
    dt = _parse_date(val)
    return dt.date().isoformat() if dt else (val.strip() if val else "")

def _themes_from_text(text: str) -> List[str]:
    t = text.lower()
    tags: List[str] = []
    if any(k in t for k in ["ai", "algorithmic", "cybersecurity", "digital", "data protection"]):
        tags.append("ai_digital")
    if any(k in t for k in ["budget", "public finance", "fiscal", "open budget"]):
        tags.append("budget")
    if any(k in t for k in ["beneficial ownership", "procurement", "anti-corruption", "integrity", "aml", "cft"]):
        tags.append("anti_corruption")
    if any(k in t for k in ["parliament", "legislative", "assembly", "mp disclosure"]):
        tags.append("open_parliament")
    if any(k in t for k in ["climate", "adaptation", "resilience", "mrv", "just transition"]):
        tags.append("climate")
    out = []
    for th in tags:
        if th not in out:
            out.append(th)
    return out[:3]

def _infer_region(text: str, region0: str = "") -> str:
    tl = text.lower()
    if any(k in tl for k in ["mena", "middle east", "north africa", "maghreb", "arab"]):
        return "MENA"
    if "africa" in tl or any(k in tl for k in [
        "west africa","east africa","southern africa","sahel",
        "morocco","tunisia","algeria","egypt","jordan",
        "benin","cote d'ivoire","côte d’ivoire","senegal","ghana","liberia",
        "burkina faso","niger","mali","togo","mauritania","sierra leone"
    ]):
        return "Africa"
    return region0 or ""

def _sig(item: Dict[str, str]) -> str:
    base = f"{item.get('title','')}|{item.get('url','')}|{item.get('deadline','')}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

# ---------------- UPDATED: fetch page without 'fl' so we get all fields ----------------
def _fetch_page(start: int) -> Dict[str, Any] | List[Any]:
    # Request a broad slice; do NOT restrict fields so we can see actual keys.
    params = {
        "format": "json",
        "rows": ROWS,
        "start": start,
        # no 'fl' here on purpose
    }
    r = requests.get(API, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _detect_date_fields(obj: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Try official names first, then heuristics:
    Returns {'deadline': <raw>, 'published': <raw>} (strings or None).
    """
    # Preferred keys
    deadline_raw = obj.get("deadline") or obj.get("bid_deadline") or obj.get("closing_date")
    pub_raw = (obj.get("pub_date") or obj.get("publication_date") or
               obj.get("published_on") or obj.get("posting_date") or obj.get("post_date") or
               obj.get("issue_date") or obj.get("advertisement_date") or obj.get("last_update"))

    # Heuristic scan over keys
    if not deadline_raw:
        for k, v in obj.items():
            kl = k.lower()
            if ("dead" in kl or "clos" in kl or "submit" in kl) and isinstance(v, str):
                deadline_raw = v
                break
    if not pub_raw:
        for k, v in obj.items():
            kl = k.lower()
            if ("pub" in kl or "post" in kl or "issue" in kl or "advert" in kl or "update" in kl) and isinstance(v, str):
                pub_raw = v
                break
    return {"deadline": deadline_raw, "published": pub_raw}

# ---------------- UPDATED: normalize to adapt to whatever keys exist ----------------
def _normalize(n: Dict[str, Any]) -> Dict[str, str]:
    if DEBUG:
        print("[worldbank] normalize keys:", list(n.keys())[:15])

    title = (
        (n.get("title") or n.get("notice_title") or n.get("tender_title") or n.get("subject") or "")
        or str(n.get("project_id") or "")
    ).strip()

    url = (n.get("url") or n.get("notice_url") or n.get("source_url") or "").strip()
    desc = (n.get("description") or n.get("notice_description") or n.get("tender_description") or "").strip()
    country = (n.get("countryname") or n.get("country") or "").strip()
    region0 = (n.get("regionname") or n.get("region") or "").strip()

    dates = _detect_date_fields(n)
    deadline = _to_iso(dates["deadline"])
    pub_iso = _to_iso(dates["published"])

    text = " ".join([title, desc, country, region0])
    region = _infer_region(text, region0)
    themes = ",".join(_themes_from_text(text))

    return {
        "title": title or "World Bank procurement notice",
        "url": url,
        "deadline": deadline,
        "summary": desc[:500],
        "region": region,
        "themes": themes,
        "_pub": pub_iso  # for debugging only
    }

def _extract_rows(payload: Dict[str, Any] | List[Any]) -> List[Dict[str, Any]]:
    """
    Normalize payload shapes to a list of dicts.
    """
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    for key in ("procnotices", "procurements", "data", "rows"):
        block = payload.get(key)
        if isinstance(block, dict):
            return list(block.values())
        if isinstance(block, list):
            return [r for r in block if isinstance(r, dict)]
    return [payload] if isinstance(payload, dict) else []

def fetch() -> List[Dict[str, str]]:
    today = datetime.utcnow().date()
    recent_cutoff = datetime.utcnow() - timedelta(days=365)

    seen = set()
    future_items: List[Dict[str, str]] = []
    recent_pub_items: List[Dict[str, str]] = []

    total_rows = 0
    for i in range(PAGES):
        start = i * ROWS
        try:
            payload = _fetch_page(start)
        except Exception as e:
            if DEBUG:
                print(f"[worldbank] fetch page start={start} error: {e}")
            break

        rows = _extract_rows(payload)
        total_rows += len(rows)
        if DEBUG:
            typ = type(payload).__name__
            print(f"[worldbank] page {i+1}: payload type={typ}, rows={len(rows)}")
            if rows:
                print(f"[worldbank] sample keys: {sorted(list(rows[0].keys()))[:12]}")

        if not rows:
            break

        for raw in rows:
            item = _normalize(raw)
            s = _sig(item)
            if s in seen:
                continue
            seen.add(s)

            # Primary: keep if deadline >= today (when parseable)
            dl = _parse_date(item.get("deadline"))
            if dl and dl.date() >= today:
                future_items.append(item)
                continue

            # Fallback bucket: recently *published*
            pub_dt = _parse_date(item.get("_pub"))
            if pub_dt and pub_dt >= recent_cutoff:
                recent_pub_items.append(item)

    if DEBUG:
        print(f"[worldbank] scanned ~{total_rows} rows, future_deadlines={len(future_items)}, recent_pub={len(recent_pub_items)}")

    # Primary requirement
    if future_items:
        return future_items

    # Guaranteed non-empty fallback
    if recent_pub_items:
        return recent_pub_items[:20]

    # Last-resort fallback: return a few normalized items from page 1 so Slack isn't empty
    if DEBUG:
        print("[worldbank] no future or recent-published items found; returning first page raw-normalized.")
    try:
        payload = _fetch_page(0)
        rows = _extract_rows(payload)[:10]
        return [_normalize(r) for r in rows]
    except Exception:
        return []

if __name__ == "__main__":
    os.environ["WB_DEBUG"] = "1"
    items = fetch()
    print(f"Fetched {len(items)} items")
    for it in items[:5]:
        print("-", it["title"], "| deadline:", it["deadline"], "| pub:", it.get("_pub",""), "|", it["url"])
