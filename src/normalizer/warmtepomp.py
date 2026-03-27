"""
Normalizer voor warmtepompen.
Zet parsed tabellen om naar gestandaardiseerde warmtepomp-records per jaar.
"""

from typing import Optional

from src.utils.logger import get_logger
from src.utils.validator import validate_warmtepomp_record, clean_amount, clean_float, clean_int
from src.utils.file_utils import now_iso

logger = get_logger("normalizer.warmtepomp")

COLUMN_MAP = {
    "meldcode":         ["meldcode", "melding", "code", "nr", "nummer"],
    "fabrikant":        ["fabrikant", "merk", "merknaam", "leverancier", "brand"],
    "model":            ["model", "type", "productnaam", "naam", "product"],
    "vermogen_kw":      ["vermogen", "kw", "capaciteit", "nominaal vermogen"],
    "subsidiebedrag":   ["subsidie", "bedrag", "subsidiebedrag", "subsidie bedrag"],
    "naam_koudemiddel": ["koudemiddel", "refrigerant", "koelmiddel", "werkmedium"],
    "gwp":              ["gwp", "global warming", "aardopwarming"],
    "categorie":        ["categorie", "type", "producttype", "warmtepomptype"],
}


def normalize_warmtepomp(parsed_doc: dict) -> list[dict]:
    """
    Normaliseer een parsed document naar een lijst van warmtepomp-records.
    """
    records = []
    year = parsed_doc.get("detected_year")
    source_url = parsed_doc.get("source_url", "")
    source_file = parsed_doc.get("source_file", "")

    for table in parsed_doc.get("tables", []):
        headers = table.get("headers", [])
        rows = table.get("rows", [])

        if not headers or not rows:
            continue

        col_index = _map_columns(headers)
        if not col_index:
            logger.debug("Geen herkenbare warmtepomp-kolommen in tabel, overgeslagen")
            continue

        logger.info(f"Warmtepomp tabel: {len(rows)} rijen, kolommen: {list(col_index.keys())}")

        for row in rows:
            record = _row_to_record(row, col_index, year, source_url, source_file)
            if record:
                record = validate_warmtepomp_record(record)
                records.append(record)

    if not records:
        logger.warning(f"Geen warmtepomp-records gevonden in: {source_url}")

    return records


def _map_columns(headers: list[str]) -> dict[str, int]:
    col_index = {}
    for field, keywords in COLUMN_MAP.items():
        for i, header in enumerate(headers):
            header_lower = str(header).lower().strip()
            if any(kw in header_lower for kw in keywords):
                col_index[field] = i
                break
    return col_index


def _get_cell(row: list, index: Optional[int], default: str = "") -> str:
    if index is None or index >= len(row):
        return default
    return str(row[index]).strip() if row[index] is not None else default


def _row_to_record(
    row: list, col_index: dict, year: Optional[int],
    source_url: str, source_file: str
) -> Optional[dict]:
    meldcode = _get_cell(row, col_index.get("meldcode"))
    fabrikant = _get_cell(row, col_index.get("fabrikant"))
    model = _get_cell(row, col_index.get("model"))

    if not meldcode and not fabrikant and not model:
        return None

    if meldcode.lower() in ["meldcode", "code", "nr"]:
        return None

    record = {
        "jaar": year,
        "meldcode": meldcode or None,
        "fabrikant": fabrikant or None,
        "model": model or None,
        "vermogen_kw": clean_float(_get_cell(row, col_index.get("vermogen_kw"))),
        "subsidiebedrag": clean_amount(_get_cell(row, col_index.get("subsidiebedrag"))),
        "naam_koudemiddel": _get_cell(row, col_index.get("naam_koudemiddel")) or None,
        "gwp": clean_int(_get_cell(row, col_index.get("gwp"))),
        "categorie": _get_cell(row, col_index.get("categorie")) or None,
        "bron_url": source_url,
        "bron_bestand": _filename_from_path(source_file),
        "confidence": 0.85,
        "opmerking_extractie": "",
    }

    if record["meldcode"]:
        record["confidence"] = 0.95

    return record


def _filename_from_path(path: str) -> str:
    if not path:
        return ""
    return path.replace("\\", "/").split("/")[-1]
