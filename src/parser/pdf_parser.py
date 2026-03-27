"""
PDF parser: extraheert tabellen en tekst uit ISDE-productlijst PDFs.
Gebruikt pdfplumber voor tabelextractie.
Fallback: regex op ruwe tekst voor meldcodes en bedragen.
"""

import re
from pathlib import Path
from typing import Optional

try:
    import pdfplumber
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

from src.parser.year_detector import detect_year
from src.utils.logger import get_logger
from src.utils.file_utils import now_iso

logger = get_logger("pdf_parser")

MELDCODE_PATTERN = re.compile(
    r"\b([A-Z]{2,6}[-_]?\d{4,10}|ISDE\d+|\d{6,10})\b"
)
AMOUNT_PATTERN = re.compile(
    r"€?\s*(\d{1,5}(?:[.,]\d{3})*(?:[.,]\d{0,2})?)\s*(?:euro|EUR|,-)?",
    re.IGNORECASE,
)
RD_PATTERN = re.compile(r"(\d+[.,]\d+)\s*(?:m[²2]K/W|m2K/W)", re.IGNORECASE)
GWP_PATTERN = re.compile(r"GWP[:\s]*(\d+)", re.IGNORECASE)


def parse_pdf_file(
    file_path: str, source_url: str, detected_year: Optional[int] = None
) -> dict:
    """
    Parseer een PDF-bestand.
    Geeft parsed-model terug.
    """
    if not PDF_AVAILABLE:
        logger.error("pdfplumber niet geïnstalleerd. Voer uit: pip install pdfplumber")
        return _empty_result(source_url, file_path, "pdfplumber niet beschikbaar")

    path = Path(file_path)
    if not path.exists():
        logger.error(f"PDF-bestand niet gevonden: {file_path}")
        return _empty_result(source_url, file_path, "bestand niet gevonden")

    warnings = []
    tables = []
    text_blocks = []
    all_text = ""

    try:
        with pdfplumber.open(str(path)) as pdf:
            num_pages = len(pdf.pages)
            logger.info(f"PDF geopend: {path.name} ({num_pages} pagina's)")

            for page_num, page in enumerate(pdf.pages, 1):
                # Tekst extraheren
                page_text = page.extract_text() or ""
                if page_text:
                    text_blocks.append(f"[Pagina {page_num}] {page_text[:500]}")
                    all_text += page_text + "\n"

                # Tabellen extraheren
                page_tables = page.extract_tables()
                if page_tables:
                    for t_idx, raw_table in enumerate(page_tables):
                        if not raw_table:
                            continue
                        cleaned = _clean_table(raw_table)
                        if cleaned["rows"]:
                            cleaned["table_index"] = len(tables)
                            cleaned["page"] = page_num
                            tables.append(cleaned)

    except Exception as e:
        logger.error(f"Fout bij parsen PDF {path.name}: {e}")
        warnings.append(f"PDF parse fout: {str(e)}")

    # Jaardetectie
    if detected_year is None:
        detected_year, method = detect_year(
            url=source_url,
            title=path.stem,
            text=all_text[:3000]
        )
    else:
        method = "vooraf bepaald"

    # Categorie bepalen
    category = _detect_category(source_url, path.stem, all_text[:1000])

    # Meldcodes in tekst
    meldcodes = list(set(MELDCODE_PATTERN.findall(all_text)))

    if not tables:
        warnings.append("geen PDF-tabellen gevonden via pdfplumber")
        # Probeer regex-extractie als fallback
        regex_rows = _regex_extract_rows(all_text)
        if regex_rows:
            tables.append({
                "table_index": 0,
                "page": 0,
                "headers": ["raw_tekst"],
                "rows": [[r] for r in regex_rows],
                "row_count": len(regex_rows),
                "extractie_methode": "regex_fallback",
            })
            warnings.append(f"regex fallback: {len(regex_rows)} rijen gevonden")

    return {
        "source_url": source_url,
        "source_file": str(file_path),
        "detected_year": detected_year,
        "year_detection_method": method,
        "category": category,
        "page_title": path.stem,
        "parsed_timestamp": now_iso(),
        "tables": tables,
        "raw_text_blocks": text_blocks[:10],
        "meldcodes_found": meldcodes[:100],
        "parse_warnings": warnings,
        "opmerking_extractie": "",
    }


def _clean_table(raw_table: list) -> dict:
    """
    Schoon een pdfplumber-tabel op.
    Eerste rij wordt als header beschouwd als die geen None-cellen heeft.
    """
    if not raw_table:
        return {"headers": [], "rows": [], "row_count": 0}

    # Verwijder volledig lege rijen
    cleaned_rows = []
    for row in raw_table:
        cells = [str(c).strip() if c is not None else "" for c in row]
        if any(cells):
            cleaned_rows.append(cells)

    if not cleaned_rows:
        return {"headers": [], "rows": [], "row_count": 0}

    # Eerste rij als header als ze tekst bevatten
    first_row = cleaned_rows[0]
    if all(c and not c.replace(".", "").replace(",", "").isdigit() for c in first_row if c):
        headers = first_row
        data_rows = cleaned_rows[1:]
    else:
        headers = [f"kolom_{i}" for i in range(len(first_row))]
        data_rows = cleaned_rows

    return {
        "headers": headers,
        "rows": data_rows,
        "row_count": len(data_rows),
    }


def _regex_extract_rows(text: str) -> list[str]:
    """
    Fallback: zoek regels met meldcode-achtige patronen in ruwe tekst.
    """
    rows = []
    for line in text.split("\n"):
        line = line.strip()
        if MELDCODE_PATTERN.search(line) and len(line) > 10:
            rows.append(line)
    return rows[:200]  # max 200 rijen


def _detect_category(url: str, filename: str, text: str) -> Optional[str]:
    combined = (url + " " + filename + " " + text[:300]).lower()
    if any(kw in combined for kw in ["isolati", "rd-waarde", "isolatiemateriaal"]):
        return "isolatie"
    if any(kw in combined for kw in ["warmtepomp", "heatpump", "koudemiddel", "gwp"]):
        return "warmtepomp"
    return None


def _empty_result(source_url: str, source_file: str, reden: str = "") -> dict:
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
        "parse_warnings": [reden] if reden else [],
        "opmerking_extractie": reden,
    }
