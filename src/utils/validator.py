"""
Validatie van genormaliseerde records.
Markeert ontbrekende velden en lage-confidence gevallen.
"""

from typing import Any


REQUIRED_ISOLATIE_FIELDS = ["meldcode", "fabrikant", "model", "naam_materiaal"]
REQUIRED_WARMTEPOMP_FIELDS = ["meldcode", "fabrikant", "model"]

CONFIDENCE_HIGH = 0.9
CONFIDENCE_MEDIUM = 0.65
CONFIDENCE_LOW = 0.3


def validate_isolatie_record(record: dict) -> dict:
    """
    Valideer een isolatie-record.
    Vult opmerkingen in en past confidence aan op basis van missende velden.
    """
    warnings = []

    for field in REQUIRED_ISOLATIE_FIELDS:
        if not record.get(field):
            warnings.append(f"ontbreekt: {field}")

    numeric_fields = ["min_waarde_rd", "subsidiebedrag_enkel", "subsidiebedrag_meerdere"]
    for field in numeric_fields:
        val = record.get(field)
        if val is None:
            warnings.append(f"numeriek veld leeg: {field}")
        elif not isinstance(val, (int, float)):
            warnings.append(f"onverwacht type voor {field}: {type(val).__name__}")

    if warnings:
        existing = record.get("opmerking_extractie", "")
        record["opmerking_extractie"] = "; ".join(filter(None, [existing] + warnings))
        # Verlaag confidence bij missende verplichte velden
        missing_required = [f for f in REQUIRED_ISOLATIE_FIELDS if not record.get(f)]
        if missing_required:
            record["confidence"] = min(record.get("confidence", 1.0), CONFIDENCE_LOW)
        else:
            record["confidence"] = min(record.get("confidence", 1.0), CONFIDENCE_MEDIUM)

    return record


def validate_warmtepomp_record(record: dict) -> dict:
    """
    Valideer een warmtepomp-record.
    """
    warnings = []

    for field in REQUIRED_WARMTEPOMP_FIELDS:
        if not record.get(field):
            warnings.append(f"ontbreekt: {field}")

    numeric_fields = ["vermogen_kw", "subsidiebedrag"]
    for field in numeric_fields:
        val = record.get(field)
        if val is None:
            warnings.append(f"numeriek veld leeg: {field}")

    if warnings:
        existing = record.get("opmerking_extractie", "")
        record["opmerking_extractie"] = "; ".join(filter(None, [existing] + warnings))
        missing_required = [f for f in REQUIRED_WARMTEPOMP_FIELDS if not record.get(f)]
        if missing_required:
            record["confidence"] = min(record.get("confidence", 1.0), CONFIDENCE_LOW)
        else:
            record["confidence"] = min(record.get("confidence", 1.0), CONFIDENCE_MEDIUM)

    return record


def clean_amount(value: Any) -> float | None:
    """
    Zet een bedragstring om naar float.
    Bijv. "€ 1.200,50" → 1200.50, "1200" → 1200.0
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    # Verwijder valutasymbolen en spaties
    text = text.replace("€", "").replace(" ", "").strip()
    # Nederlandse notatie: punt als duizendscheider, komma als decimaal
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def clean_float(value: Any) -> float | None:
    """Zet waarde om naar float, of None bij fout."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def clean_int(value: Any) -> int | None:
    """Zet waarde om naar int, of None bij fout."""
    f = clean_float(value)
    if f is None:
        return None
    return int(f)
