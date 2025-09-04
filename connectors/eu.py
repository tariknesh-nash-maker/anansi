# Minimal EU Funding & Tenders connector (stub)
# Replace the list below with real parsed items later.

from datetime import datetime, timedelta

def fetch() -> list[dict]:
    today = datetime.utcnow().date()
    items = [
        {
            "title": "EU call: Digital governance pilot for Africa",
            "url": "https://ec.europa.eu/info/funding-tenders/opportunities/portal/…",
            "deadline": (today + timedelta(days=60)).isoformat(),
            "summary": "Support to digital governance and responsible AI in selected African countries.",
            "region": "Africa",
            "themes": "ai_digital"
        },
        {
            "title": "EU grant: Climate transparency & citizen budgets (MENA)",
            "url": "https://ec.europa.eu/info/funding-tenders/opportunities/portal/…",
            "deadline": (today + timedelta(days=45)).isoformat(),
            "summary": "Strengthen climate finance transparency and citizen budget tools in MENA.",
            "region": "MENA",
            "themes": "climate,budget"
        },
    ]
    return items
