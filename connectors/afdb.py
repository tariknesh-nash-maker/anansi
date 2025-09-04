# Minimal AfDB connector (stub)
# Later: parse AfDB procurement/trust fund/grant pages that allow scraping.

from datetime import datetime, timedelta

def fetch() -> list[dict]:
    today = datetime.utcnow().date()
    return [
        {
            "title": "AfDB: Budget transparency & PFM strengthening (West Africa)",
            "url": "https://www.afdb.org/en/projects-and-operations/procurement",
            "deadline": (today + timedelta(days=50)).isoformat(),
            "summary": "Support PEFA-aligned fiscal transparency and citizen budget portals.",
            "region": "Africa",
            "themes": "budget"
        },
        {
            "title": "AfDB: Climate finance MRV pilot with local governments",
            "url": "https://www.afdb.org/en/projects-and-operations/procurement",
            "deadline": (today + timedelta(days=80)).isoformat(),
            "summary": "Monitoring, reporting, verification of climate funds with open data tools.",
            "region": "Africa",
            "themes": "climate"
        },
    ]
