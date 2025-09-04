# Minimal World Bank connector (stub)
# Later: parse WB procurement notices / grant calls (public listings).

from datetime import datetime, timedelta

def fetch() -> list[dict]:
    today = datetime.utcnow().date()
    return [
        {
            "title": "World Bank: Open Parliament & legislative transparency TA",
            "url": "https://projects.worldbank.org/en/projects-operations/procurement",
            "deadline": (today + timedelta(days=40)).isoformat(),
            "summary": "Technical assistance to strengthen parliamentary openness and citizen oversight.",
            "region": "Africa, MENA",
            "themes": "open_parliament"
        },
        {
            "title": "World Bank: Data protection & digital trust framework (Morocco)",
            "url": "https://projects.worldbank.org/en/projects-operations/procurement",
            "deadline": (today + timedelta(days=65)).isoformat(),
            "summary": "Support regulatory frameworks for data protection and trusted digital services.",
            "region": "Morocco",
            "themes": "ai_digital"
        },
    ]
