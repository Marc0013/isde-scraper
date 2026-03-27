"""
Test-run voor warmtepomp discovery en parsing.
Haalt een beperkt aantal detailpagina's op en toont de resultaten.

Gebruik:
    python test_warmtepomp.py [--limit N]  (standaard: 5 pagina's)
"""

import sys
import time
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.utils.logger import setup_logger
from src.utils.file_utils import save_json, now_iso
from src.scraper.sitemap_parser import fetch_warmtepomp_urls
from src.scraper.domain_filter import filter_url
from src.parser.warmtepomp_detail_parser import parse_warmtepomp_detail, extract_subsidie_voor_jaar

import requests

logger = setup_logger("test_warmtepomp", log_dir="logs")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ISDE-subsidie-scraper/1.0; research)",
    "Accept-Language": "nl-NL,nl;q=0.9",
}
DELAY = 1.5  # seconden tussen requests


def run_test(limit: int = 5):
    logger.info("=" * 60)
    logger.info(f"TEST-RUN WARMTEPOMPEN (limit={limit})")
    logger.info("=" * 60)

    # ── Stap 1: Discovery via sitemap ──
    logger.info("Stap 1: Warmtepomp URLs ophalen uit sitemap...")
    discovered = fetch_warmtepomp_urls(limit=limit)

    print(f"\n{'='*60}")
    print(f"DISCOVERY RESULTAAT")
    print(f"{'='*60}")
    print(f"Warmtepomp-bronnen gevonden: {len(discovered)}")
    for item in discovered:
        blocked = not filter_url(item["url"])
        status = "GEBLOKKEERD" if blocked else "OK"
        print(f"  [{status}] {item['url']}")
        print(f"         meldcode hint: {item.get('meldcode_hint', '?')}")

    # Domeincheck: alle URLs moeten rvo.nl zijn
    blocked = [d for d in discovered if not filter_url(d["url"])]
    if blocked:
        print(f"\nWAARSCHUWING: {len(blocked)} externe URLs geblokkeerd!")
    else:
        print(f"\nDomeincheck: alle {len(discovered)} URLs zijn rvo.nl [OK]")

    if not discovered:
        print("Geen URLs gevonden. Test gestopt.")
        return

    # ── Stap 2: Download en parseer elke detailpagina ──
    logger.info("Stap 2: Detailpagina's ophalen en parseren...")
    parsed_records = []
    failed = []

    for i, item in enumerate(discovered, 1):
        url = item["url"]
        if not filter_url(url):
            logger.warning(f"Overgeslagen (extern): {url}")
            continue

        logger.info(f"[{i}/{len(discovered)}] Ophalen: {url}")
        time.sleep(DELAY)

        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                logger.warning(f"HTTP {r.status_code}: {url}")
                failed.append({"url": url, "reden": f"HTTP {r.status_code}"})
                continue

            parsed = parse_warmtepomp_detail(
                html=r.text,
                source_url=url,
                source_file="",
            )

            if parsed:
                parsed_records.append(parsed)
                logger.info(
                    f"  OK: {parsed.get('meldcode')} | {parsed.get('fabrikant')} | "
                    f"{parsed.get('model')} | confidence={parsed.get('confidence')}"
                )
            else:
                failed.append({"url": url, "reden": "parse mislukt"})

        except Exception as e:
            logger.error(f"Fout bij {url}: {e}")
            failed.append({"url": url, "reden": str(e)})

    # ── Stap 3: Resultaten tonen ──
    print(f"\n{'='*60}")
    print(f"PARSE RESULTATEN")
    print(f"{'='*60}")
    print(f"Geparseerde records: {len(parsed_records)}")
    print(f"Mislukt:             {len(failed)}")
    print()

    for rec in parsed_records:
        print(f"  Meldcode:    {rec.get('meldcode', '?')}")
        print(f"  Merk:        {rec.get('fabrikant', '?')}")
        print(f"  Model:       {rec.get('model', '?')}")
        print(f"  Vermogen:    {rec.get('vermogen_kw', '?')} kW")
        print(f"  Koudemiddel: {rec.get('naam_koudemiddel', '?')}")
        print(f"  GWP:         {rec.get('gwp', '?')}")
        print(f"  Categorie:   {rec.get('categorie', '?')}")
        print(f"  Confidence:  {rec.get('confidence', '?')}")
        print(f"  Subsidiebedragen:")
        for sb in rec.get("subsidiebedragen", []):
            print(f"    jaar={sb.get('jaar', '?')}  bedrag=€{sb.get('bedrag', '?')}  [{sb.get('label_origineel', '')}]")
        if rec.get("parse_warnings"):
            print(f"  Waarschuwingen: {rec['parse_warnings']}")
        print()

    # ── Stap 4: Opslaan ──
    out_dir = Path("data/parsed/warmtepompen")
    out_dir.mkdir(parents=True, exist_ok=True)
    output_file = out_dir / "test_warmtepomp_records.json"
    save_json(parsed_records, output_file)

    summary_file = Path("data/parsed/warmtepompen") / "test_summary.json"
    save_json({
        "timestamp": now_iso(),
        "limit": limit,
        "discovered": len(discovered),
        "parsed_ok": len(parsed_records),
        "failed": len(failed),
        "failed_details": failed,
    }, summary_file)

    print(f"{'='*60}")
    print(f"AANGEMAAKT BESTANDEN")
    print(f"{'='*60}")
    print(f"  {output_file}  ({len(parsed_records)} records)")
    print(f"  {summary_file}")
    print()
    print(f"Logs: logs/isde_scraper_*.log")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5, help="Aantal detailpagina's om te testen")
    args = parser.parse_args()
    run_test(limit=args.limit)
