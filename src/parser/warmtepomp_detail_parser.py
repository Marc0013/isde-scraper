"""
Parser voor RVO warmtepomp-detailpagina's.

Paginastructuur (https://www.rvo.nl/meldcodes-warmtepompen/{slug}):
  - <h1>: meldcode + merk + model
  - <table>: rijen met <th> label en <td> waarde
    Labels: Type, Merk, Meldcode, Subsidiebedrag vanaf {datum}, Categorie,
            Subsidiabel Vermogen, Koudemiddel, Global Warming Potential (GWP)

Jaar-extractie: het label "Subsidiebedrag vanaf 01/01/2024" bevat de ingangsdatum.
Wij extraheren het jaar uit die datum en koppelen het bedrag aan dat jaar.
"""

import re
from typing import Optional

from bs4 import BeautifulSoup

from src.parser.year_detector import detect_year_from_url
from src.utils.logger import get_logger
from src.utils.file_utils import now_iso
from src.utils.validator import clean_amount, clean_float, clean_int

logger = get_logger("parser.warmtepomp_detail")

# Subsidiebedrag label: "Subsidiebedrag vanaf 01/01/2024 :"
SUBSIDIE_LABEL_PATTERN = re.compile(
    r"subsidiebedrag\s+(?:per|vanaf|tot|geldig)?\s*(?:(\d{1,2})[/-](\d{1,2})[/-](20[0-9]{2}))?",
    re.IGNORECASE,
)
YEAR_IN_LABEL = re.compile(r"\b(20[0-9]{2})\b")
VERMOGEN_PATTERN = re.compile(r"(\d+(?:[.,]\d+)?)\s*kW", re.IGNORECASE)
AMOUNT_CLEAN = re.compile(r"[^\d.,]")


def is_warmtepomp_detail_page(url: str, html: str = "") -> bool:
    """True als URL een warmtepomp-detailpagina is."""
    return "/meldcodes-warmtepompen/" in url


def parse_warmtepomp_detail(
    html: str,
    source_url: str,
    source_file: str = "",
) -> Optional[dict]:
    """
    Parseer een warmtepomp-detailpagina naar een genormaliseerd record.

    Geeft een dict terug met alle velden, of None als parsing volledig mislukt.
    De 'subsidiebedragen' key bevat een lijst van {jaar, bedrag} dicts,
    omdat een pagina meerdere tarieven (per jaar) kan bevatten.
    """
    soup = BeautifulSoup(html, "lxml")
    warnings = []

    # ── Titel ──
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""

    # ── Tabel lezen ──
    table = soup.find("table")
    if not table:
        logger.warning(f"Geen tabel gevonden op: {source_url}")
        return _empty_record(source_url, source_file, title, "geen tabel gevonden")

    fields = {}
    subsidiebedragen = []  # [{jaar, bedrag, label}]

    for row in table.find_all("tr"):
        th = row.find("th")
        td = row.find("td")
        if not th or not td:
            continue

        label = th.get_text(strip=True)
        value = td.get_text(strip=True)
        label_lower = label.lower()

        # Subsidiebedrag (met jaar in label)
        # Voorbeelden van labels:
        #   "Subsidiebedrag vanaf 01/01/2026 :"              → jaar=2026, open tarief
        #   "Subsidiebedrag vanaf 01/01/2025 tot en met 31/12/2025 :"  → jaar=2025
        #   "Subsidiebedrag vanaf 01/01/2024 tot en met 31/12/2024 :"  → jaar=2024
        if "subsidiebedrag" in label_lower:
            jaren_in_label = YEAR_IN_LABEL.findall(label)
            jaar_start = int(jaren_in_label[0]) if jaren_in_label else None
            jaar_eind = int(jaren_in_label[-1]) if len(jaren_in_label) > 1 else None
            bedrag = _parse_amount(value)
            subsidiebedragen.append({
                "jaar": jaar_start,
                "jaar_eind": jaar_eind,          # None = open tarief (huidig)
                "bedrag": bedrag,
                "label_origineel": label,
                "waarde_origineel": value,
            })
            continue

        # Overige velden
        if "type" in label_lower or "model" in label_lower:
            fields["model"] = value
        elif "merk" in label_lower:
            fields["fabrikant"] = value
        elif "meldcode" in label_lower:
            fields["meldcode"] = value
        elif "categorie" in label_lower:
            fields["categorie"] = value
        elif "vermogen" in label_lower:
            fields["vermogen_kw"] = _parse_vermogen(value)
            fields["vermogen_origineel"] = value
        elif "koudemiddel" in label_lower or "refrigerant" in label_lower:
            fields["naam_koudemiddel"] = value
        elif "global warming" in label_lower or "gwp" in label_lower:
            fields["gwp"] = clean_int(value)

    # ── Validatie ──
    if not fields.get("meldcode") and not fields.get("fabrikant"):
        warnings.append("geen meldcode of merk gevonden in tabel")

    # ── Jaar uit URL als extra fallback voor subsidiebedragen zonder jaar ──
    year_from_url = detect_year_from_url(source_url)
    for sb in subsidiebedragen:
        if sb["jaar"] is None and year_from_url:
            sb["jaar"] = year_from_url
            sb["jaar_bron"] = "url"

    # ── Confidence bepalen ──
    has_meldcode = bool(fields.get("meldcode"))
    has_bedrag = bool(subsidiebedragen)
    if has_meldcode and has_bedrag:
        confidence = 0.95
    elif has_meldcode:
        confidence = 0.80
    elif has_bedrag:
        confidence = 0.60
    else:
        confidence = 0.30

    return {
        "source_url": source_url,
        "source_file": source_file,
        "page_title": title,
        "parsed_timestamp": now_iso(),
        "category": "warmtepomp",

        # Kernvelden
        "meldcode": fields.get("meldcode"),
        "fabrikant": fields.get("fabrikant"),
        "model": fields.get("model"),
        "vermogen_kw": fields.get("vermogen_kw"),
        "vermogen_origineel": fields.get("vermogen_origineel"),
        "naam_koudemiddel": fields.get("naam_koudemiddel"),
        "gwp": fields.get("gwp"),
        "categorie": fields.get("categorie"),

        # Subsidiebedragen per jaar (lijst, kan meerdere jaren bevatten)
        "subsidiebedragen": subsidiebedragen,

        # Kwaliteit
        "confidence": confidence,
        "parse_warnings": warnings,
        "opmerking_extractie": "; ".join(warnings) if warnings else "",
    }


def extract_subsidie_voor_jaar(parsed: dict, jaar: int) -> Optional[float]:
    """
    Haal het subsidiebedrag op voor een specifiek jaar uit een parsed record.
    Geeft None terug als het jaar niet gevonden is.
    """
    for sb in parsed.get("subsidiebedragen", []):
        if sb.get("jaar") == jaar:
            return sb.get("bedrag")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Hulpfuncties
# ─────────────────────────────────────────────────────────────────────────────

def _extract_year_from_label(label: str) -> Optional[int]:
    """Extraheer jaar uit een subsidiebedrag-label."""
    match = YEAR_IN_LABEL.search(label)
    if match:
        return int(match.group(1))
    return None


def _parse_amount(value: str) -> Optional[float]:
    """
    Zet bedragstring om naar float.
    Houdt rekening met encoding-issues (€ kan als ? of ander teken binnenkomen).
    """
    if not value:
        return None
    # Verwijder niet-numerieke tekens behalve punt en komma
    cleaned = AMOUNT_CLEAN.sub("", value)
    # Nederlandse notatie: punt als duizendscheider
    if "." in cleaned and "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "." in cleaned and len(cleaned.split(".")[-1]) == 3:
        # 4.725 → 4725
        cleaned = cleaned.replace(".", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_vermogen(value: str) -> Optional[float]:
    """Extraheer vermogen in kW. Bijv. '12kW' → 12.0"""
    m = VERMOGEN_PATTERN.search(value)
    if m:
        return clean_float(m.group(1))
    # Probeer getal direct
    return clean_float(re.sub(r"[^\d.,]", "", value))


def _empty_record(source_url: str, source_file: str, title: str, reden: str) -> dict:
    return {
        "source_url": source_url,
        "source_file": source_file,
        "page_title": title,
        "parsed_timestamp": now_iso(),
        "category": "warmtepomp",
        "meldcode": None,
        "fabrikant": None,
        "model": None,
        "vermogen_kw": None,
        "vermogen_origineel": None,
        "naam_koudemiddel": None,
        "gwp": None,
        "categorie": None,
        "subsidiebedragen": [],
        "confidence": 0.0,
        "parse_warnings": [reden],
        "opmerking_extractie": reden,
    }
