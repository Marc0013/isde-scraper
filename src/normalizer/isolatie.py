"""
Normalizer voor isolatiematerialen.
Zet parsed tabellen om naar gestandaardiseerde isolatie-records per jaar.
"""

import re
from typing import Optional

from src.utils.logger import get_logger
from src.utils.validator import validate_isolatie_record, clean_amount, clean_float, clean_int
from src.utils.file_utils import now_iso

logger = get_logger("normalizer.isolatie")

# Kolomnamen die we herkennen (case-insensitive, partiele match)
COLUMN_MAP = {
    "meldcode":                 ["meldcode", "melding", "code", "nr", "nummer"],
    "fabrikant":                ["fabrikant", "merk", "merknaam", "leverancier", "brand"],
    "model":                    ["model", "type", "productnaam", "naam", "product"],
    "naam_materiaal":           ["materiaal", "omschrijving", "materiaalsoort", "isolatiemateriaal"],
    "min_waarde_rd":            ["rd", "rd-waarde", "warmteweerstand", "r-waarde"],
    "min_dikte_mm":             ["dikte", "mm", "minimale dikte"],
    "subsidiebedrag_enkel":     ["enkel", "enkele maatregel", "enkel maatregel", "bedrag enkel"],
    "subsidiebedrag_meerdere":  ["meerdere", "combinatie", "meerdere maatregelen", "bedrag meer"],
    "biobased_bonus":           ["biobased", "bio-based", "bonus", "biobased bonus"],
    "categorie":                ["categorie", "type maatregel", "maatregel"],
    "woning_type":              ["woning", "woningtype", "gebouwtype"],
}


def normalize_isolatie(parsed_doc: dict) -> list[dict]:
    """
    Normaliseer een parsed document naar een lijst van isolatie-records.
    Elk record vertegenwoordigt één product/meldcode voor een specifiek jaar.
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

        # Map kolomnamen naar posities
        col_index = _map_columns(headers)

        if not col_index:
            logger.debug(f"Geen herkenbare kolommen in tabel, overgeslagen")
            continue

        logger.info(f"Tabel verwerken: {len(rows)} rijen, kolommen: {list(col_index.keys())}")

        for row in rows:
            record = _row_to_record(row, col_index, year, source_url, source_file)
            if record:
                record = validate_isolatie_record(record)
                records.append(record)

    if not records:
        logger.warning(f"Geen isolatie-records gevonden in: {source_url}")

    return records


def _map_columns(headers: list[str]) -> dict[str, int]:
    """
    Koppel kolomnamen aan indices op basis van gedeeltelijke tekstmatch.
    Geeft {veldnaam: kolomindex} terug.
    """
    col_index = {}
    for field, keywords in COLUMN_MAP.items():
        for i, header in enumerate(headers):
            header_lower = str(header).lower().strip()
            if any(kw in header_lower for kw in keywords):
                col_index[field] = i
                break
    return col_index


def _get_cell(row: list, index: Optional[int], default: str = "") -> str:
    """Haal celwaarde op met bounds check."""
    if index is None or index >= len(row):
        return default
    return str(row[index]).strip() if row[index] is not None else default


def _row_to_record(
    row: list, col_index: dict, year: Optional[int],
    source_url: str, source_file: str
) -> Optional[dict]:
    """Zet één tabelrij om naar een genormaliseerd isolatie-record."""

    meldcode = _get_cell(row, col_index.get("meldcode"))
    fabrikant = _get_cell(row, col_index.get("fabrikant"))
    model = _get_cell(row, col_index.get("model"))

    # Skip rijen zonder enige zinvolle data
    if not meldcode and not fabrikant and not model:
        return None

    # Skip headerrijen die zijn meegenomen als datarij
    if meldcode.lower() in ["meldcode", "code", "nr"]:
        return None

    record = {
        "jaar": year,
        "meldcode": meldcode or None,
        "fabrikant": fabrikant or None,
        "model": model or None,
        "naam_materiaal": _get_cell(row, col_index.get("naam_materiaal")) or None,
        "min_waarde_rd": clean_float(_get_cell(row, col_index.get("min_waarde_rd"))),
        "min_dikte_mm": clean_int(_get_cell(row, col_index.get("min_dikte_mm"))),
        "subsidiebedrag_enkel": clean_amount(_get_cell(row, col_index.get("subsidiebedrag_enkel"))),
        "subsidiebedrag_meerdere": clean_amount(_get_cell(row, col_index.get("subsidiebedrag_meerdere"))),
        "biobased_bonus": clean_amount(_get_cell(row, col_index.get("biobased_bonus"))),
        "categorie": _get_cell(row, col_index.get("categorie")) or None,
        "woning_type": _get_cell(row, col_index.get("woning_type")) or None,
        "bron_url": source_url,
        "bron_bestand": _filename_from_path(source_file),
        "confidence": 0.85,
        "opmerking_extractie": "",
    }

    # Hogere confidence als meldcode aanwezig is
    if record["meldcode"]:
        record["confidence"] = 0.95

    return record


def _filename_from_path(path: str) -> str:
    """Extraheer bestandsnaam uit volledig pad."""
    if not path:
        return ""
    return path.replace("\\", "/").split("/")[-1]
