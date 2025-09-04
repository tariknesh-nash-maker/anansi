# Minimal UNDP connector (stub)
# Later you can query UNDP/UNGM listings and parse titles, links, deadlines.

from datetime import datetime, timedelta

def fetch() -> list[dict]:
    today = datetime.utcnow().date()
    return [
        {
            "title": "UNDP RFP: Anti-corruption & procurement transparency (Benin, Côte d’Ivoire)",
            "url": "https://www.undp.org/procurement/business/resources-for-bidders",
            "deadline": (today + timedelta(days=30)).isoformat(),
            "summary": "Technical assistance on integrity, beneficial ownership and e-procurement.",
            "region": "Africa",
            "themes": "anti_corruption"
        },
        {
            "title": "UNDP grant: CSO resilience and civic space support (Regional)",
            "url": "https://www.undp.org/procurement",
            "deadline": (today + timedelta(days=70)).isoformat(),
            "summary": "Core support to civil society networks to safeguard participation.",
            "region": "Africa, MENA",
            "themes": "cso_support"
        },
    ]
