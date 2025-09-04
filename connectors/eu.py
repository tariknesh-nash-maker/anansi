import re, requests
from datetime import datetime
from bs4 import BeautifulSoup

def fetch() -> list[dict]:
    # Example: query EU Funding & Tenders with keywords; for MVP, keep it simple
    url = "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities-topic-search"
    # For MVP, you might pre-curate a few RSS/JSON endpoints or static pages
    # HERE: stub with a tiny example list to show the shape
    items = []
    # ... implement real fetch/parse later ...
    # Each item:
    # items.append({"title": "...", "url": "...", "deadline": "2025-10-31", "source": "EU F&T"})
    return items

def tag_themes(text: str) -> list[str]:
    t = text.lower()
    tags = []
    if any(k in t for k in ["ai", "algorithmic", "digital", "cybersecurity", "data protection"]): tags.append("ai_digital")
    if any(k in t for k in ["budget", "fiscal", "public finance", "open budget"]): tags.append("budget")
    if any(k in t for k in ["corruption", "procurement", "integrity", "beneficial ownership"]): tags.append("anti_corruption")
    if any(k in t for k in ["climate", "adaptation", "just transition", "loss and damage"]): tags.append("climate")
    if any(k in t for k in ["parliament", "legislative", "mp disclosure"]): tags.append("open_parliament")
    if any(k in t for k in ["cso", "civic space", "advocacy"]): tags.append("cso_support")
    return tags[:3]
