"""
Crawler: ontdekt relevante pagina's en bestanden op rvo.nl.
- Begint bij startpagina's uit config
- Volgt alleen interne rvo.nl links
- Filtert op relevante onderwerpen
- Houdt bezochte URLs bij om dubbele scraping te voorkomen
"""

import json
from pathlib import Path
from collections import deque
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from src.scraper.domain_filter import filter_url, normalize_url, is_downloadable_file
from src.scraper.downloader import Downloader
from src.parser.year_detector import detect_year_from_url, detect_year_from_title
from src.utils.logger import get_logger
from src.utils.file_utils import load_json, save_json

logger = get_logger("crawler")

RELEVANT_KEYWORDS = [
    "isde", "isolatie", "warmtepomp", "meldcode",
    "meldcodelijst", "productlijst", "subsidietabel",
    "isolatiematerialen", "isolatiemaatregelen",
]
# "subsidie" en "woningeigenaar" bewust weggelaten: te breed,
# matcht vrijwel alle RVO-pagina's buiten ISDE-scope.

CATEGORY_KEYWORDS = {
    "isolatie": ["isolatie", "isolatiematerialen", "isolatiewaarde", "rd-waarde"],
    "warmtepomp": ["warmtepomp", "warmtepompen", "heatpump"],
}


class Crawler:
    def __init__(self, config: dict, start_urls: list[dict]):
        self.config = config
        self.start_urls = start_urls
        self.downloader = Downloader(config)
        self.visited_file = Path(config["paths"]["visited_urls_file"])
        self.visited: set[str] = self._load_visited()
        self.queue: deque = deque()
        self.discovered_files: list[dict] = []

    def run(self) -> list[dict]:
        """
        Start de crawler. Geeft lijst terug van ontdekte bestanden/pagina's.
        Elke entry bevat: url, file_type, category, detected_year, file_path, metadata
        """
        max_depth = 3
        max_pages = self.config.get("max_pages_per_run", 60)

        # Voeg startpunten toe aan queue — domeinfilter direct hier al toegepast
        for item in self.start_urls:
            url = normalize_url(item["url"])
            if not filter_url(url):
                logger.warning(f"Startpunt geblokkeerd (extern domein): {url}")
                continue
            if url not in self.visited:
                self.queue.append({
                    "url": url,
                    "category": item.get("category", "algemeen"),
                    "depth": 0,
                })

        processed = 0

        while self.queue:
            if processed >= max_pages:
                logger.warning(f"Max paginalimiet bereikt ({max_pages}). Crawl gestopt.")
                break

            item = self.queue.popleft()
            url = item["url"]
            depth = item["depth"]
            category = item["category"]

            if url in self.visited:
                continue
            if depth > max_depth:
                logger.debug(f"Max diepte bereikt, overgeslagen: {url}")
                continue

            self.visited.add(url)
            processed += 1
            logger.info(f"[{processed}/{max_pages}] Crawl (diepte {depth}): {url}")

            is_file, ext = is_downloadable_file(url)

            if is_file:
                self._handle_file(url, ext, category)
            else:
                new_links = self._handle_html_page(url, category, depth)
                for link_info in new_links:
                    if link_info["url"] not in self.visited:
                        self.queue.append(link_info)

            # Sla bezochte URLs periodiek op
            if processed % 10 == 0:
                self._save_visited()

        self._save_visited()
        logger.info(f"Crawler klaar. {processed} pagina's verwerkt, {len(self.discovered_files)} bestanden gevonden.")
        return self.discovered_files

    def _handle_html_page(self, url: str, category: str, depth: int) -> list[dict]:
        """
        Download en parseer een HTML-pagina.
        Geeft lijst van nieuwe links terug.
        """
        result = self.downloader.fetch_html(url)
        if result is None:
            return []

        html, metadata = result
        soup = BeautifulSoup(html, "lxml")

        # Paginatitel
        title = ""
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
        metadata["page_title"] = title

        # Jaardetectie
        year_url = detect_year_from_url(url)
        year_title = detect_year_from_title(title)
        detected_year = year_url or year_title
        metadata["detected_year"] = detected_year
        metadata["year_detection_method"] = "url" if year_url else ("title" if year_title else None)
        metadata["category"] = category

        # Pagina opnemen als relevant
        if self._is_relevant(url, title):
            self.discovered_files.append({
                "url": url,
                "file_type": "html",
                "category": category,
                "detected_year": detected_year,
                "file_path": metadata.get("file_path"),
                "metadata": metadata,
            })

        # Links verzamelen
        # Volgorde is bewust: domeinfilter EERST, dan pas relevantiecheck.
        # Externe links worden nooit aan de queue toegevoegd.
        new_links = []
        blocked_external = []

        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "").strip()
            if not href:
                continue

            abs_url = normalize_url(href, base_url=url)

            # Stap 1: domeinfilter — blokkeert alles buiten rvo.nl
            if not filter_url(abs_url, blocked_log=blocked_external):
                continue

            # Stap 2: relevantiefilter — alleen ISDE-gerelateerde pagina's
            link_text = a_tag.get_text(strip=True)
            link_category = self._detect_category(abs_url, link_text)
            if link_category is None and not self._is_relevant(abs_url, link_text):
                continue

            new_links.append({
                "url": abs_url,
                "category": link_category or category,
                "depth": depth + 1,
            })

        metadata["blocked_external_links"] = blocked_external
        return new_links

    def _handle_file(self, url: str, ext: str, category: str) -> None:
        """Download een bestand (PDF/Excel) en registreer het."""
        logger.info(f"Bestand gevonden: {url}")
        result = self.downloader.fetch_file(url, extension=ext)
        if result is None:
            return

        content, metadata = result

        # Jaardetectie uit URL en bestandsnaam
        year = detect_year_from_url(url)
        metadata["detected_year"] = year
        metadata["year_detection_method"] = "url" if year else None
        metadata["category"] = category

        self.discovered_files.append({
            "url": url,
            "file_type": ext,
            "category": category,
            "detected_year": year,
            "file_path": metadata.get("file_path"),
            "metadata": metadata,
            "content": content,
        })

    def _is_relevant(self, url: str, text: str = "") -> bool:
        """Controleert of URL of tekst relevant is voor ISDE."""
        combined = (url + " " + text).lower()
        return any(kw in combined for kw in RELEVANT_KEYWORDS)

    def _detect_category(self, url: str, text: str = "") -> Optional[str]:
        """Bepaal categorie op basis van URL en linktekst."""
        combined = (url + " " + text).lower()
        for cat, keywords in CATEGORY_KEYWORDS.items():
            if any(kw in combined for kw in keywords):
                return cat
        return None

    def _load_visited(self) -> set[str]:
        """Laad eerder bezochte URLs vanuit bestand."""
        data = load_json(self.visited_file)
        if isinstance(data, list):
            logger.info(f"{len(data)} eerder bezochte URLs geladen")
            return set(data)
        return set()

    def _save_visited(self) -> None:
        """Sla bezochte URLs op."""
        save_json(list(self.visited), self.visited_file)
