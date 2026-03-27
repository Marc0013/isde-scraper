"""
Merger: combineert genormaliseerde 2024- en 2025-datasets tot één vergelijkingstabel.
Koppelstrategie (prioriteit):
  1. Exacte meldcode match
  2. Fabrikant + model (case-insensitief)
  3. Fabrikant + model + categorie (looser)
Ongematchte records worden opgenomen met lage confidence en markering.
"""

from typing import Optional
from src.utils.logger import get_logger
from src.utils.file_utils import now_iso

logger = get_logger("merger")

TODAY = now_iso()[:10]  # alleen datum


# ─────────────────────────────────────────────────────────────────────────────
# Isolatie merger
# ─────────────────────────────────────────────────────────────────────────────

def merge_isolatie(records_2024: list[dict], records_2025: list[dict]) -> list[dict]:
    """
    Combineer isolatie-records van 2024 en 2025.
    Geeft één vergelijkingstabel terug.
    """
    logger.info(f"Isolatie merge: {len(records_2024)} records 2024, {len(records_2025)} records 2025")

    matched = []
    unmatched_2024 = list(records_2024)
    unmatched_2025 = list(records_2025)

    # Stap 1: match op meldcode
    matched, unmatched_2024, unmatched_2025 = _match_on_meldcode(
        unmatched_2024, unmatched_2025, _build_isolatie_comparison
    )

    # Stap 2: match op fabrikant + model (resterende)
    extra_matched, unmatched_2024, unmatched_2025 = _match_on_fabrikant_model(
        unmatched_2024, unmatched_2025, _build_isolatie_comparison
    )
    matched.extend(extra_matched)

    # Stap 3: ongematchte records opnemen (één kant)
    for r in unmatched_2024:
        matched.append(_build_isolatie_comparison(r, None, "alleen_2024"))
    for r in unmatched_2025:
        matched.append(_build_isolatie_comparison(None, r, "alleen_2025"))

    logger.info(
        f"Isolatie merge klaar: {len(matched)} vergelijkingsrecords "
        f"({len(unmatched_2024)} ongematch 2024, {len(unmatched_2025)} ongematch 2025 voor merge)"
    )
    return matched


def _build_isolatie_comparison(
    r2024: Optional[dict], r2025: Optional[dict], match_method: str = "meldcode"
) -> dict:
    """Bouw één vergelijkingsregel voor isolatie."""
    base = r2024 or r2025

    return {
        "meldcode": base.get("meldcode"),
        "fabrikant": base.get("fabrikant"),
        "model": base.get("model"),
        "naam_materiaal": base.get("naam_materiaal"),
        "min_waarde_rd": base.get("min_waarde_rd"),
        "min_dikte_mm": base.get("min_dikte_mm"),

        # 2024 kolommen
        "subsidiebedrag_enkel_2024": r2024.get("subsidiebedrag_enkel") if r2024 else None,
        "subsidiebedrag_meerdere_2024": r2024.get("subsidiebedrag_meerdere") if r2024 else None,
        "biobased_bonus_2024": r2024.get("biobased_bonus") if r2024 else None,

        # 2025 kolommen
        "subsidiebedrag_enkel_2025": r2025.get("subsidiebedrag_enkel") if r2025 else None,
        "subsidiebedrag_meerdere_2025": r2025.get("subsidiebedrag_meerdere") if r2025 else None,
        "biobased_bonus_2025": r2025.get("biobased_bonus") if r2025 else None,

        # Overig
        "categorie": base.get("categorie"),
        "woning_type": base.get("woning_type"),

        # Bronnen
        "bron_2024_url": r2024.get("bron_url") if r2024 else None,
        "bron_2025_url": r2025.get("bron_url") if r2025 else None,
        "bron_2024_bestand": r2024.get("bron_bestand") if r2024 else None,
        "bron_2025_bestand": r2025.get("bron_bestand") if r2025 else None,

        # Kwaliteit
        "confidence_2024": r2024.get("confidence") if r2024 else None,
        "confidence_2025": r2025.get("confidence") if r2025 else None,
        "match_methode": match_method,
        "datum_opgehaald": TODAY,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Warmtepomp merger
# ─────────────────────────────────────────────────────────────────────────────

def merge_warmtepomp(records_2024: list[dict], records_2025: list[dict]) -> list[dict]:
    """
    Combineer warmtepomp-records van 2024 en 2025.
    """
    logger.info(f"Warmtepomp merge: {len(records_2024)} records 2024, {len(records_2025)} records 2025")

    matched, unmatched_2024, unmatched_2025 = _match_on_meldcode(
        list(records_2024), list(records_2025), _build_warmtepomp_comparison
    )

    extra_matched, unmatched_2024, unmatched_2025 = _match_on_fabrikant_model(
        unmatched_2024, unmatched_2025, _build_warmtepomp_comparison
    )
    matched.extend(extra_matched)

    for r in unmatched_2024:
        matched.append(_build_warmtepomp_comparison(r, None, "alleen_2024"))
    for r in unmatched_2025:
        matched.append(_build_warmtepomp_comparison(None, r, "alleen_2025"))

    logger.info(f"Warmtepomp merge klaar: {len(matched)} vergelijkingsrecords")
    return matched


def _build_warmtepomp_comparison(
    r2024: Optional[dict], r2025: Optional[dict], match_method: str = "meldcode"
) -> dict:
    base = r2024 or r2025

    return {
        "meldcode": base.get("meldcode"),
        "fabrikant": base.get("fabrikant"),
        "model": base.get("model"),
        "vermogen_kw": base.get("vermogen_kw"),

        # Subsidies per jaar
        "subsidiebedrag_2024": r2024.get("subsidiebedrag") if r2024 else None,
        "subsidiebedrag_2025": r2025.get("subsidiebedrag") if r2025 else None,

        # Overig
        "naam_koudemiddel": base.get("naam_koudemiddel"),
        "gwp": base.get("gwp"),
        "categorie": base.get("categorie"),

        # Bronnen
        "bron_2024_url": r2024.get("bron_url") if r2024 else None,
        "bron_2025_url": r2025.get("bron_url") if r2025 else None,
        "bron_2024_bestand": r2024.get("bron_bestand") if r2024 else None,
        "bron_2025_bestand": r2025.get("bron_bestand") if r2025 else None,

        # Kwaliteit
        "confidence_2024": r2024.get("confidence") if r2024 else None,
        "confidence_2025": r2025.get("confidence") if r2025 else None,
        "match_methode": match_method,
        "datum_opgehaald": TODAY,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Generieke matchfuncties
# ─────────────────────────────────────────────────────────────────────────────

def _match_on_meldcode(
    list_2024: list[dict], list_2025: list[dict], build_fn
) -> tuple[list[dict], list[dict], list[dict]]:
    """Match records op exacte meldcode. Geeft (matched, rest_2024, rest_2025)."""
    index_2025 = {}
    for r in list_2025:
        mc = _normalize_key(r.get("meldcode"))
        if mc:
            index_2025[mc] = r

    matched = []
    remaining_2024 = []
    used_2025_keys = set()

    for r2024 in list_2024:
        mc = _normalize_key(r2024.get("meldcode"))
        if mc and mc in index_2025:
            r2025 = index_2025[mc]
            matched.append(build_fn(r2024, r2025, "meldcode"))
            used_2025_keys.add(mc)
        else:
            remaining_2024.append(r2024)

    remaining_2025 = [r for r in list_2025 if _normalize_key(r.get("meldcode")) not in used_2025_keys]

    return matched, remaining_2024, remaining_2025


def _match_on_fabrikant_model(
    list_2024: list[dict], list_2025: list[dict], build_fn
) -> tuple[list[dict], list[dict], list[dict]]:
    """Match records op fabrikant + model (case-insensitief). Geeft (matched, rest_2024, rest_2025)."""
    def key(r: dict) -> Optional[str]:
        fab = _normalize_key(r.get("fabrikant"))
        mod = _normalize_key(r.get("model"))
        if fab and mod:
            return f"{fab}|{mod}"
        return None

    index_2025 = {}
    for r in list_2025:
        k = key(r)
        if k:
            index_2025[k] = r

    matched = []
    remaining_2024 = []
    used_keys = set()

    for r2024 in list_2024:
        k = key(r2024)
        if k and k in index_2025:
            r2025 = index_2025[k]
            matched.append(build_fn(r2024, r2025, "fabrikant_model"))
            used_keys.add(k)
        else:
            remaining_2024.append(r2024)

    remaining_2025 = [r for r in list_2025 if key(r) not in used_keys]

    return matched, remaining_2024, remaining_2025


def _normalize_key(value: Optional[str]) -> Optional[str]:
    """Normaliseer een sleutelwaarde: lowercase, strip, verwijder spaties/leestekens."""
    if not value:
        return None
    import re
    normalized = str(value).lower().strip()
    normalized = re.sub(r"[\s\-_./]+", "", normalized)
    return normalized if normalized else None
