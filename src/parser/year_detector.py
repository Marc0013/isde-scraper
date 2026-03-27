"""
Jaardetectie: bepaalt of een URL, bestandsnaam of tekst refereert aan 2024 of 2025.
"""

import re
from typing import Optional

TARGET_YEARS = {2024, 2025}

# Regex: 4-cijferige jaren in de range 2024-2025
YEAR_PATTERN = re.compile(r"\b(202[45])\b")


def detect_year_from_url(url: str) -> Optional[int]:
    """
    Zoek jaar in URL of bestandsnaam.
    Bijv. /productlijst-2024.pdf → 2024
    """
    matches = YEAR_PATTERN.findall(url)
    years = [int(y) for y in matches if int(y) in TARGET_YEARS]
    if len(years) == 1:
        return years[0]
    if len(years) > 1:
        # Meerdere jaren in URL: neem het eerste
        return years[0]
    return None


def detect_year_from_title(title: str) -> Optional[int]:
    """
    Zoek jaar in paginatitel of documenttitel.
    Bijv. "ISDE Subsidietabel 2025" → 2025
    """
    matches = YEAR_PATTERN.findall(title)
    years = [int(y) for y in matches if int(y) in TARGET_YEARS]
    if years:
        return years[0]
    return None


def detect_year_from_text(text: str) -> Optional[int]:
    """
    Zoek jaar in documenttekst (bijv. eerste pagina van PDF).
    Gebruik indicatieve zinnen als context.
    """
    # Zoek naar patronen als "subsidietabel 2024" of "geldig per ... 2024"
    context_patterns = [
        r"subsidietabel\s+(202[45])",
        r"geldig\s+(?:per|vanaf|in)\s+(?:\d+\s+\w+\s+)?(202[45])",
        r"versie\s+(202[45])",
        r"januari\s+(202[45])",
        r"aanvraagperiode\s+(202[45])",
        r"\b(202[45])\b",  # fallback: elk jaar in tekst
    ]

    for pattern in context_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            year = int(match.group(1))
            if year in TARGET_YEARS:
                return year

    return None


def detect_year(url: str = "", title: str = "", text: str = "") -> tuple[Optional[int], str]:
    """
    Combineer alle detectiemethoden.
    Geeft (jaar, methode) terug. methode is 'url', 'title', 'text', of 'onbekend'.
    """
    year = detect_year_from_url(url)
    if year:
        return year, "url"

    year = detect_year_from_title(title)
    if year:
        return year, "title"

    if text:
        year = detect_year_from_text(text)
        if year:
            return year, "text"

    return None, "onbekend"
