"""
HTML parser: extraheert tabellen en tekst uit opgeslagen HTML-pagina's.
Detecteert meldcodes, bedragen en technische waarden.
"""

import re
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

from src.parser.year_detector import detect_year
from src.utils.logger import get_logger
from src.utils.file_utils import now_iso

logger = get_logger("html_parser")

# Regex voor meldcodes (RVO gebruikt diverse formaten)
MELDCODE_PATTERN = re.compile(
    r"\b([A-Z]{2,6}[-_]?\d{4,10}|ISDE\d+|\d{6,10})\b"
)

# Regex voor geldbedragen in euro's
AMOUNT_PATTERN = re.compile(
    r"€?\s*(\d{1,5}(?:[.,]\d{3})*(?:[.,]\d{1,2})?)\s*(?:euro|EUR)?"
)

# Regex voor Rd-waarden (m²K/W)
RD_PATTERN = re.compile(r"(\d+[.,]\d+)\s*(?:m[²2]K/W|m2K/W)")


def parse_html_file(file_path: str, source_url: str, detected_year: Optional[int] = None) -> dict:
    """
    Parseer een opgeslagen HTML-bestand.
    Geeft parsed-model terug.
    """
    path = Path(file_path)
    if not path.exists():
        logger.error(f"HTML-bestand niet gevonden: {file_path}")
        return _empty_result(source_url, file_path)

    try:
        html = path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"Fout bij lezen HTML: {e}")
        return _empty_result(source_url, file_path)

    return parse_html_text(html, source_url, file_path, detected_year)


def parse_html_text(
    html: str, source_url: str, source_file: str = "", detected_year: Optional[int] = None
) -> dict:
    """
    Parseer HTML-tekst. Geeft parsed-model terug.
    """
    soup = BeautifulSoup(html, "lxml")
    warnings = []

    # Titel
    title = ""
    title_tag = soup.find("title")
    if title_tag:
        title = title_tag.get_text(strip=True)

    # Jaardetectie (verfijn indien nodig)
    if detected_year is None:
        body_text = soup.get_text(" ", strip=True)[:2000]
        detected_year, method = detect_year(url=source_url, title=title, text=body_text)
    else:
        method = "vooraf bepaald"

    # Categorie hint
    category = _detect_category(source_url, title, soup.get_text())

    # Tabellen extraheren
    tables = _extract_tables(soup, warnings)

    # Tekst blokken
    text_blocks = _extract_text_blocks(soup)

    # Meldcodes in hele pagina
    all_text = soup.get_text(" ", strip=True)
    meldcodes = list(set(MELDCODE_PATTERN.findall(all_text)))

    return {
        "source_url": source_url,
        "source_file": source_file,
        "detected_year": detected_year,
        "year_detection_method": method,
        "category": category,
        "page_title": title,
        "parsed_timestamp": now_iso(),
        "tables": tables,
        "raw_text_blocks": text_blocks[:10],  # max 10 blokken bewaren
        "meldcodes_found": meldcodes[:50],    # max 50
        "parse_warnings": warnings,
        "opmerking_extractie": "",
    }


def _extract_tables(soup: BeautifulSoup, warnings: list) -> list[dict]:
    """Extraheer alle tabellen als lijst van {headers, rows}."""
    tables = []
    for i, table in enumerate(soup.find_all("table")):
        headers = []
        rows = []

        # Headers
        header_row = table.find("tr")
        if header_row:
            headers = [
                th.get_text(strip=True)
                for th in header_row.find_all(["th", "td"])
            ]

        # Data rijen
        for tr in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if any(cells):  # skip lege rijen
                rows.append(cells)

        if headers or rows:
            tables.append({
                "table_index": i,
                "headers": headers,
                "rows": rows,
                "row_count": len(rows),
            })

    if not tables:
        warnings.append("geen HTML-tabellen gevonden")

    return tables


def _extract_text_blocks(soup: BeautifulSoup) -> list[str]:
    """Extraheer betekenisvolle tekstblokken (p, li, h2, h3)."""
    blocks = []
    for tag in soup.find_all(["p", "li", "h2", "h3", "h4"]):
        text = tag.get_text(strip=True)
        if len(text) > 20:
            blocks.append(text)
    return blocks


def _detect_category(url: str, title: str, text: str) -> Optional[str]:
    combined = (url + " " + title + " " + text[:500]).lower()
    if any(kw in combined for kw in ["isolati", "rd-waarde", "isolatiemateriaal"]):
        return "isolatie"
    if any(kw in combined for kw in ["warmtepomp", "heatpump", "koudemiddel"]):
        return "warmtepomp"
    return None


def _empty_result(source_url: str, source_file: str) -> dict:
    return {
        "source_url": source_url,
        "source_file": source_file,
        "detected_year": None,
        "year_detection_method": None,
        "category": None,
        "page_title": "",
        "parsed_timestamp": now_iso(),
        "tables": [],
        "raw_text_blocks": [],
        "meldcodes_found": [],
        "parse_warnings": ["bestand niet geladen"],
        "opmerking_extractie": "HTML-bestand kon niet worden geladen",
    }
