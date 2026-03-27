"""
ISDE Subsidie Scraper - Hoofdscript
Gebruik: python main.py [--skip-crawl] [--skip-parse] [--skip-normalize]

Stappen:
  1. Crawl RVO.nl voor ISDE-pagina's en bestanden
  2. Parseer HTML en PDF bestanden
  3. Normaliseer naar gestructureerde records per jaar
  4. Merge 2024 + 2025 tot vergelijkingstabel
"""

import sys
import json
import argparse
from pathlib import Path

import yaml

from src.utils.logger import setup_logger
from src.utils.file_utils import save_json, load_json, now_iso


def load_config() -> dict:
    """Laad settings.yaml en start_urls.yaml."""
    base = Path(__file__).parent

    with open(base / "config" / "settings.yaml", encoding="utf-8") as f:
        settings = yaml.safe_load(f)

    with open(base / "config" / "start_urls.yaml", encoding="utf-8") as f:
        urls_config = yaml.safe_load(f)

    # Zet paden relatief aan projectmap
    for key, val in settings.get("paths", {}).items():
        settings["paths"][key] = str(base / val)

    # Platte config voor downloader/crawler
    config = {
        **settings.get("scraper", {}),
        "paths": settings["paths"],
        "output": settings.get("output", {}),
    }
    # Output paden ook absoluut maken
    for key, val in config.get("output", {}).items():
        config["output"][key] = str(base / val)

    return config, urls_config.get("start_urls", [])


def step_crawl(config: dict, start_urls: list) -> list[dict]:
    """Stap 1: crawl RVO.nl en verzamel bestanden."""
    from src.scraper.crawler import Crawler
    logger.info("=" * 60)
    logger.info("STAP 1: CRAWLEN")
    logger.info("=" * 60)

    crawler = Crawler(config, start_urls)
    discovered = crawler.run()

    # Sla ontdekkingen op
    discovery_file = Path(config["paths"]["data_raw_metadata"]) / "discovered_files.json"
    # Sla alleen metadata op (geen bytes-content)
    safe_discovered = []
    for item in discovered:
        safe_item = {k: v for k, v in item.items() if k != "content"}
        safe_discovered.append(safe_item)
    save_json(safe_discovered, discovery_file)

    logger.info(f"Ontdekte bestanden opgeslagen: {discovery_file}")
    return discovered


def step_parse(config: dict, discovered: list[dict]) -> dict:
    """Stap 2: parseer alle ontdekte HTML/PDF bestanden."""
    from src.parser.html_parser import parse_html_file
    from src.parser.pdf_parser import parse_pdf_file

    logger.info("=" * 60)
    logger.info("STAP 2: PARSEN")
    logger.info("=" * 60)

    parsed_isolatie = []
    parsed_warmtepomp = []

    for item in discovered:
        url = item.get("url", "")
        file_type = item.get("file_type", "")
        file_path = item.get("file_path")
        category = item.get("category")
        detected_year = item.get("detected_year")

        if not file_path:
            continue

        logger.info(f"Parsen ({file_type}): {url}")

        if file_type == "html":
            parsed = parse_html_file(file_path, url, detected_year)
        elif file_type in ("pdf", "xlsx", "xls"):
            parsed = parse_pdf_file(file_path, url, detected_year)
        else:
            logger.debug(f"Onbekend bestandstype overgeslagen: {file_type}")
            continue

        # Categorie overnemen als parser het niet kon detecteren
        if not parsed.get("category") and category:
            parsed["category"] = category

        # Sla parsed resultaat op
        _save_parsed(parsed, config)

        if parsed.get("category") == "isolatie" or _likely_isolatie(parsed):
            parsed_isolatie.append(parsed)
        elif parsed.get("category") == "warmtepomp" or _likely_warmtepomp(parsed):
            parsed_warmtepomp.append(parsed)
        else:
            logger.debug(f"Categorie onduidelijk, niet opgenomen: {url}")

    logger.info(f"Geparseerde documenten: {len(parsed_isolatie)} isolatie, {len(parsed_warmtepomp)} warmtepomp")
    return {"isolatie": parsed_isolatie, "warmtepomp": parsed_warmtepomp}


def step_normalize(config: dict, parsed_data: dict) -> dict:
    """Stap 3: normaliseer naar records per jaar."""
    from src.normalizer.isolatie import normalize_isolatie
    from src.normalizer.warmtepomp import normalize_warmtepomp

    logger.info("=" * 60)
    logger.info("STAP 3: NORMALISEREN")
    logger.info("=" * 60)

    isolatie_2024, isolatie_2025 = [], []
    warmtepomp_2024, warmtepomp_2025 = [], []

    for doc in parsed_data["isolatie"]:
        records = normalize_isolatie(doc)
        for r in records:
            if r.get("jaar") == 2024:
                isolatie_2024.append(r)
            elif r.get("jaar") == 2025:
                isolatie_2025.append(r)
            else:
                logger.warning(f"Record zonder jaar overgeslagen: {r.get('meldcode')}")

    for doc in parsed_data["warmtepomp"]:
        records = normalize_warmtepomp(doc)
        for r in records:
            if r.get("jaar") == 2024:
                warmtepomp_2024.append(r)
            elif r.get("jaar") == 2025:
                warmtepomp_2025.append(r)
            else:
                logger.warning(f"Warmtepomp record zonder jaar: {r.get('meldcode')}")

    # Sla genormaliseerde data op
    base = Path(__file__).parent
    save_json(isolatie_2024, base / "data/normalized/2024/isolatie_2024.json")
    save_json(isolatie_2025, base / "data/normalized/2025/isolatie_2025.json")
    save_json(warmtepomp_2024, base / "data/normalized/2024/warmtepompen_2024.json")
    save_json(warmtepomp_2025, base / "data/normalized/2025/warmtepompen_2025.json")

    logger.info(
        f"Genormaliseerd: isolatie 2024={len(isolatie_2024)}, 2025={len(isolatie_2025)} | "
        f"warmtepomp 2024={len(warmtepomp_2024)}, 2025={len(warmtepomp_2025)}"
    )

    return {
        "isolatie_2024": isolatie_2024,
        "isolatie_2025": isolatie_2025,
        "warmtepomp_2024": warmtepomp_2024,
        "warmtepomp_2025": warmtepomp_2025,
    }


def step_merge(config: dict, normalized: dict) -> None:
    """Stap 4: merge 2024 + 2025 tot vergelijkingstabellen."""
    from src.merger.merger import merge_isolatie, merge_warmtepomp

    logger.info("=" * 60)
    logger.info("STAP 4: MERGEN")
    logger.info("=" * 60)

    comparison_isolatie = merge_isolatie(
        normalized["isolatie_2024"],
        normalized["isolatie_2025"]
    )
    comparison_warmtepomp = merge_warmtepomp(
        normalized["warmtepomp_2024"],
        normalized["warmtepomp_2025"]
    )

    base = Path(__file__).parent

    # JSON
    save_json(comparison_isolatie, base / "data/comparison/isde_isolatie_vergelijking.json")
    save_json(comparison_warmtepomp, base / "data/comparison/isde_warmtepompen_vergelijking.json")

    # CSV
    _save_csv(comparison_isolatie, base / "data/comparison/isde_isolatie_vergelijking.csv")
    _save_csv(comparison_warmtepomp, base / "data/comparison/isde_warmtepompen_vergelijking.csv")

    logger.info(f"Vergelijking opgeslagen: {len(comparison_isolatie)} isolatie, {len(comparison_warmtepomp)} warmtepomp records")
    logger.info(f"Bestanden in: data/comparison/")


def _save_csv(records: list[dict], path: Path) -> None:
    """Sla records op als CSV via pandas of handmatig."""
    if not records:
        logger.warning(f"Geen records voor CSV: {path.name}")
        return
    try:
        import pandas as pd
        df = pd.DataFrame(records)
        df.to_csv(str(path), index=False, encoding="utf-8-sig")
        logger.info(f"CSV opgeslagen: {path.name} ({len(df)} rijen)")
    except ImportError:
        # Handmatige CSV als pandas niet beschikbaar
        import csv
        path.parent.mkdir(parents=True, exist_ok=True)
        keys = list(records[0].keys())
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(records)
        logger.info(f"CSV opgeslagen (handmatig): {path.name} ({len(records)} rijen)")


def _save_parsed(parsed: dict, config: dict) -> None:
    """Sla parsed document op in de juiste map."""
    cat = parsed.get("category", "overig")
    if cat == "isolatie":
        out_dir = config["paths"]["data_parsed_isolatie"]
    elif cat == "warmtepomp":
        out_dir = config["paths"]["data_parsed_warmtepompen"]
    else:
        out_dir = str(Path(config["paths"]["data_parsed_isolatie"]).parent / "overig")

    from src.utils.file_utils import url_to_filename
    filename = url_to_filename(parsed.get("source_url", "unknown"), "json")
    save_json(parsed, Path(out_dir) / filename)


def _likely_isolatie(parsed: dict) -> bool:
    keywords = ["isolati", "rd-waarde", "isolatiemateriaal"]
    text = " ".join([
        parsed.get("page_title", ""),
        str(parsed.get("meldcodes_found", "")),
        " ".join(parsed.get("raw_text_blocks", [])[:3])
    ]).lower()
    return any(kw in text for kw in keywords)


def _likely_warmtepomp(parsed: dict) -> bool:
    keywords = ["warmtepomp", "koudemiddel", "gwp", "heatpump"]
    text = " ".join([
        parsed.get("page_title", ""),
        str(parsed.get("meldcodes_found", "")),
        " ".join(parsed.get("raw_text_blocks", [])[:3])
    ]).lower()
    return any(kw in text for kw in keywords)


def main():
    parser = argparse.ArgumentParser(description="ISDE Subsidie Scraper - RVO.nl")
    parser.add_argument("--skip-crawl", action="store_true", help="Sla crawlen over (gebruik bestaande raw data)")
    parser.add_argument("--skip-parse", action="store_true", help="Sla parsen over (gebruik bestaande parsed data)")
    parser.add_argument("--skip-normalize", action="store_true", help="Sla normaliseren over")
    parser.add_argument("--only-merge", action="store_true", help="Voer alleen de merge stap uit")
    args = parser.parse_args()

    config, start_urls = load_config()
    base = Path(__file__).parent

    # ── Stap 1: Crawlen ──
    if args.only_merge or args.skip_crawl:
        logger.info("Crawlen overgeslagen, laad bestaande ontdekkingen")
        discovery_file = Path(config["paths"]["data_raw_metadata"]) / "discovered_files.json"
        discovered = load_json(discovery_file) or []
    else:
        discovered = step_crawl(config, start_urls)

    # ── Stap 2: Parsen ──
    if args.only_merge or args.skip_parse:
        logger.info("Parsen overgeslagen, laad bestaande parsed data")
        parsed_data = _load_parsed_data(base)
    else:
        parsed_data = step_parse(config, discovered)

    # ── Stap 3: Normaliseren ──
    if args.only_merge or args.skip_normalize:
        logger.info("Normaliseren overgeslagen, laad bestaande normalized data")
        normalized = _load_normalized_data(base)
    else:
        normalized = step_normalize(config, parsed_data)

    # ── Stap 4: Mergen ──
    step_merge(config, normalized)

    logger.info("=" * 60)
    logger.info("KLAAR!")
    logger.info(f"Resultaten staan in: data/comparison/")
    logger.info("=" * 60)


def _load_parsed_data(base: Path) -> dict:
    """Laad alle parsed JSON bestanden vanuit parsed mappen."""
    def load_dir(path: Path) -> list[dict]:
        results = []
        if path.exists():
            for f in path.glob("*.json"):
                data = load_json(f)
                if data:
                    results.append(data)
        return results

    return {
        "isolatie": load_dir(base / "data/parsed/isolatie"),
        "warmtepomp": load_dir(base / "data/parsed/warmtepompen"),
    }


def _load_normalized_data(base: Path) -> dict:
    return {
        "isolatie_2024": load_json(base / "data/normalized/2024/isolatie_2024.json") or [],
        "isolatie_2025": load_json(base / "data/normalized/2025/isolatie_2025.json") or [],
        "warmtepomp_2024": load_json(base / "data/normalized/2024/warmtepompen_2024.json") or [],
        "warmtepomp_2025": load_json(base / "data/normalized/2025/warmtepompen_2025.json") or [],
    }


if __name__ == "__main__":
    # Logger opzetten vóór config laden
    logger = setup_logger("main", log_dir="logs")
    logger.info("ISDE Subsidie Scraper gestart")
    main()
