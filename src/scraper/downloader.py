"""
Downloader: haalt HTML en binaire bestanden (PDF/Excel) op van rvo.nl.
- Respecteert rate limiting
- Slaat op in raw mappen
- Slaat metadata op per download
"""

import time
from pathlib import Path
from typing import Optional

import requests

from src.utils.logger import get_logger
from src.utils.file_utils import save_raw_html, save_raw_binary, save_json, now_iso

logger = get_logger("downloader")

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ISDE-subsidie-scraper/1.0; research)",
    "Accept-Language": "nl-NL,nl;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class Downloader:
    def __init__(self, config: dict):
        self.delay = config.get("request_delay_seconds", 1.5)
        self.timeout = config.get("request_timeout_seconds", 30)
        self.max_retries = config.get("max_retries", 3)
        self.raw_html_dir = config["paths"]["data_raw_html"]
        self.raw_pdf_dir = config["paths"]["data_raw_pdf"]
        self.metadata_dir = config["paths"]["data_raw_metadata"]
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def fetch_html(self, url: str) -> Optional[tuple[str, dict]]:
        """
        Download een HTML-pagina.
        Geeft (html_text, metadata_dict) terug, of None bij fout.
        """
        response = self._fetch(url)
        if response is None:
            return None

        html = response.text
        file_path = save_raw_html(html, url, self.raw_html_dir)
        metadata = self._build_metadata(url, response, str(file_path), "html")
        self._save_metadata(metadata, url)
        logger.info(f"HTML opgeslagen: {file_path.name}")
        return html, metadata

    def fetch_file(self, url: str, extension: str = "pdf") -> Optional[tuple[bytes, dict]]:
        """
        Download een binair bestand (PDF, Excel).
        Geeft (bytes, metadata_dict) terug, of None bij fout.
        """
        response = self._fetch(url)
        if response is None:
            return None

        content = response.content
        file_path = save_raw_binary(content, url, self.raw_pdf_dir, extension)
        metadata = self._build_metadata(url, response, str(file_path), extension)
        metadata["file_size_bytes"] = len(content)
        self._save_metadata(metadata, url)
        logger.info(f"Bestand opgeslagen: {file_path.name} ({len(content)} bytes)")
        return content, metadata

    def _fetch(self, url: str) -> Optional[requests.Response]:
        """Interne fetch met retry en rate limiting."""
        time.sleep(self.delay)
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
                if response.status_code == 200:
                    return response
                elif response.status_code == 404:
                    logger.warning(f"404 Niet gevonden: {url}")
                    return None
                elif response.status_code == 429:
                    wait = 5 * attempt
                    logger.warning(f"Rate limited (429), wacht {wait}s: {url}")
                    time.sleep(wait)
                else:
                    logger.warning(f"HTTP {response.status_code} voor {url} (poging {attempt})")
                    if attempt < self.max_retries:
                        time.sleep(2 * attempt)
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout voor {url} (poging {attempt})")
                if attempt < self.max_retries:
                    time.sleep(2 * attempt)
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Verbindingsfout voor {url}: {e}")
                return None
            except Exception as e:
                logger.error(f"Onverwachte fout voor {url}: {e}")
                return None
        logger.error(f"Alle pogingen mislukt voor: {url}")
        return None

    def _build_metadata(
        self, url: str, response: requests.Response, file_path: str, file_type: str
    ) -> dict:
        return {
            "url": url,
            "domain": "rvo.nl",
            "file_type": file_type,
            "download_timestamp": now_iso(),
            "file_path": file_path,
            "file_size_bytes": len(response.content),
            "http_status": response.status_code,
            "content_type": response.headers.get("Content-Type", ""),
            "page_title": None,
            "source_page": None,
            "detected_year": None,
            "year_detection_method": None,
            "category": None,
            "blocked_external_links": [],
        }

    def _save_metadata(self, metadata: dict, url: str) -> None:
        from src.utils.file_utils import url_to_filename
        filename = url_to_filename(url, "json")
        path = Path(self.metadata_dir) / filename
        save_json(metadata, path)
