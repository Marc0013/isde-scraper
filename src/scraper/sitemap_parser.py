"""
Sitemap parser: haalt alle warmtepomp-detailpagina URLs op uit de RVO sitemap.
Dit is de meest directe en belastingloze manier om alle meldcodes te ontdekken:
  1 request voor de sitemap → 2.973 warmtepomp URLs direct beschikbaar.
"""

import re
import xml.etree.ElementTree as ET
from typing import Optional

import requests

from src.utils.logger import get_logger

logger = get_logger("sitemap_parser")

SITEMAP_URL = "https://www.rvo.nl/sitemap.xml"

WARMTEPOMP_PATH_PREFIX = "/meldcodes-warmtepompen/"

# Meldcode regex: bijv. ka31717 of KA31717 aan het begin van het slug
MELDCODE_FROM_SLUG = re.compile(r"^([a-zA-Z]{2}\d{5,})-", re.IGNORECASE)


def fetch_warmtepomp_urls(
    limit: Optional[int] = None,
    request_delay: float = 1.0,
) -> list[dict]:
    """
    Haal alle warmtepomp-detailpagina URLs op uit de RVO sitemap.

    Args:
        limit: maximaal aantal URLs teruggeven (None = alles)
        request_delay: wachttijd vóór request in seconden

    Geeft lijst van dicts terug:
      {url, meldcode_hint, slug}
    """
    import time
    time.sleep(request_delay)

    logger.info(f"Sitemap ophalen: {SITEMAP_URL}")
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; ISDE-subsidie-scraper/1.0; research)",
            "Accept-Language": "nl-NL,nl;q=0.9",
        }
        r = requests.get(SITEMAP_URL, headers=headers, timeout=30)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"Sitemap ophalen mislukt: {e}")
        return []

    # Parse XML
    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        logger.error(f"Sitemap XML parse fout: {e}")
        return []

    # Namespace-agnostisch zoeken naar <loc> elementen
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    all_locs = root.findall(".//sm:loc", ns)
    if not all_locs:
        # Probeer zonder namespace
        all_locs = root.findall(".//loc")

    logger.info(f"Sitemap totaal: {len(all_locs)} URLs")

    results = []
    for loc in all_locs:
        url = (loc.text or "").strip()
        if WARMTEPOMP_PATH_PREFIX in url:
            slug = url.split(WARMTEPOMP_PATH_PREFIX)[-1]
            meldcode_hint = _extract_meldcode_from_slug(slug)
            results.append({
                "url": url,
                "slug": slug,
                "meldcode_hint": meldcode_hint,
                "category": "warmtepomp",
                "source": "sitemap",
            })

    logger.info(f"Warmtepomp detailpagina's in sitemap: {len(results)}")

    if limit:
        results = results[:limit]
        logger.info(f"Beperkt tot {limit} URLs voor deze run")

    return results


def _extract_meldcode_from_slug(slug: str) -> Optional[str]:
    """
    Extraheer meldcode-hint uit URL-slug.
    Bijv. 'ka31717-heliotherm-pr13s-m-web' → 'KA31717'
    """
    m = MELDCODE_FROM_SLUG.match(slug)
    if m:
        return m.group(1).upper()
    return None
