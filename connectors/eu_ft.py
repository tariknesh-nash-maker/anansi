# connectors/eu_ft.py
import os
import logging
import requests
from datetime import datetime, timedelta, timezone

SESSION = requests.Session()
SESSION.headers.update({"Content-Type": "application/json"})

API_URL = "https://api.ted.europa.eu/v3/notices/search"

# small, *supported* field set per docs
TED_FIELDS = [
    "publication-number",
    "notice-title",
    "buyer-name",
    "place-of-performance",
    "cpv",
    "publication-date",
    "notice-type",
]

def _to_iso(d: str) -> str | None:
    # TED returns eForms publication date like 2024-02-13Z or 2024-02-13+01:00
    try:
        # normalize to date only
        if d.endswith("Z"):
            return datetime.fromisoformat(d.replace("Z", "+00:00")).date().isoformat()
        return datetime.fromisoformat(d).date().isoformat()
    except Exception:
        return None

def _map_topic(title: str, desc: str = "") -> str | None:
    text = f"{title} {desc}".lower()
    topics = {
        "Digital Governance": ["digital", "it ", "ict", "information system", "software", "data"],
        "Fiscal Openness": ["audit", "budget", "tax", "revenue", "procure", "fiscal"],
        "Open Government": ["open data", "transparency", "participation", "consultation", "citizen"],
        "Access to Information": ["access to information", "ati", "foi"],
        "Anti-Corruption": ["anti-corruption", "integrity", "fraud"],
        "Civic Space": ["civil society", "ngo", "advocacy", "association"],
        "Gender and Inclusion": ["gender", "women", "inclusion", "inclusive"],
        "Justice": ["court", "judicial", "justice", "rule of law"],
        "Media Freedom": ["media", "journalism", "press"],
        "Climate and Environment": ["climate", "environment", "waste", "renewable", "biodiversity"],
        "Public Participation": ["participation", "consult", "co-creation", "co creation"],
    }
    for label, kws in topics.items():
        if any(k in text for k in kws):
            return label
    return None

def _normalize_item(it):
    pubno = it.get("publication-number")
    url = f"https://ted.europa.eu/en/notice/-/detail/{pubno}" if pubno else None
    title = it.get("notice-title") or f"TED Notice {pubno}"
    pub_date = _to_iso(it.get("publication-date") or "")
    # TED Search API doesn’t expose a unified “submission deadline” field across all forms.
    # Leave deadline None; your Slack formatter already handles N/A.
    country = None
    pop = it.get("place-of-performance")
    if isinstance(pop, dict):
        country = pop.get("country") or pop.get("nuts")  # best-effort
    topic = _map_topic(title)
    return {
        "source": "EU (TED)",
        "country": country,
        "title": title,
        "url": url,
        "published": pub_date,
        "deadline": None,
        "topic": topic,
    }

def _search_active(page=1, limit=50, query: str | None = None):
    payload = {
        # Per docs: page-number pagination and ACTIVE scope. query can be omitted.
        "fields": TED_FIELDS,
        "page": page,
        "limit": limit,
        "scope": "ACTIVE",
        "checkQuerySyntax": False,
        "paginationMode": "PAGE_NUMBER",
    }
    if query:
        payload["query"] = query
    r = SESSION.post(API_URL, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch(ogp_only: bool = True, since_days: int | None = None) -> list[dict]:
    """
    Return normalized items from TED (EU). We keep the query broad (ACTIVE scope),
    then filter client-side by publication date + (optional) topic mapping.
    """
    debug = os.getenv("EUFT_DEBUG")
    try:
        out: list[dict] = []
        # Broad first page
        data = _search_active(page=1, limit=50)
        items = data.get("items") or []
        if debug:
            total = data.get("total")
            logging.info(f"[eu_ft] total={total} returned={len(items)}")

        for it in items:
            norm = _normalize_item(it)
            if not norm["title"] or not norm["url"]:
                continue

            # date window filter (client-side)
            if since_days:
                cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).date().isoformat()
                if norm["published"] and norm["published"] < cutoff:
                    continue

            if ogp_only and not norm["topic"]:
                # skip non-OGP-ish items if requested
                continue

            out.append(norm)

        # If we somehow got nothing (API changes or query optionality), fall back to a safe, valid example query
        # from the docs (Luxembourg place of performance), just to avoid returning empty results.
        if not out:
            if debug:
                logging.warning("[eu_ft] first attempt empty; falling back to LUX query")
            data2 = _search_active(page=1, limit=50, query="place-of-performance IN (LUX)")
            for it in data2.get("items") or []:
                norm = _normalize_item(it)
                if since_days:
                    cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).date().isoformat()
                    if norm["published"] and norm["published"] < cutoff:
                        continue
                if ogp_only and not norm["topic"]:
                    continue
                out.append(norm)

        return out
    except requests.HTTPError as e:
        logging.warning(f"[eu_ft] HTTP {e.response.status_code}: {e} | body={e.response.text[:200] if e.response is not None else ''}")
        return []
    except Exception as e:
        logging.warning(f"[eu_ft] failed: {e}")
        return []
