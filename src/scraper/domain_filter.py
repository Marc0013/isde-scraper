"""
Domeinfilter: alleen rvo.nl is toegestaan.
Alles buiten de allowlist wordt geblokkeerd en gelogd.
"""

from urllib.parse import urlparse
from typing import Optional
from src.utils.logger import get_logger

logger = get_logger("domain_filter")

ALLOWED_DOMAINS = {"rvo.nl", "www.rvo.nl"}

# Extensies die we nooit volgen (media, stijlen, scripts)
SKIP_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
    ".mp4", ".mp3", ".zip", ".tar", ".gz"
}


def is_allowed_domain(url: str) -> bool:
    """
    Controleert of een URL binnen de toegestane domeinen valt.
    Subdomains van rvo.nl (bijv. open.rvo.nl) worden ook toegestaan.
    """
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        # Verwijder poort indien aanwezig
        netloc = netloc.split(":")[0]
        return netloc in ALLOWED_DOMAINS or netloc.endswith(".rvo.nl")
    except Exception:
        return False


def should_skip_extension(url: str) -> bool:
    """True als URL een extensie heeft die we overslaan (afbeeldingen, etc.)."""
    path = urlparse(url).path.lower()
    for ext in SKIP_EXTENSIONS:
        if path.endswith(ext):
            return True
    return False


def filter_url(url: str, blocked_log: Optional[list] = None) -> bool:
    """
    Geeft True als URL verwerkt mag worden, False als geblokkeerd.
    Logt geblokkeerde externe links naar blocked_log (lijst) indien opgegeven.
    """
    if not url or not url.startswith("http"):
        return False

    if should_skip_extension(url):
        return False

    if not is_allowed_domain(url):
        logger.warning(f"GEBLOKKEERD (extern domein): {url}")
        if blocked_log is not None:
            blocked_log.append(url)
        return False

    return True


def normalize_url(url: str, base_url: str = "") -> str:
    """
    Zet relatieve URL om naar absoluut.
    Verwijdert fragment (#...) en normaliseert.
    """
    from urllib.parse import urljoin, urldefrag
    if base_url and not url.startswith("http"):
        url = urljoin(base_url, url)
    url, _ = urldefrag(url)
    return url.rstrip("/")


def is_downloadable_file(url: str) -> tuple[bool, str]:
    """
    Bepaal of URL een downloadbaar bestand is (PDF, Excel).
    Geeft (True, extensie) of (False, "") terug.
    """
    path = urlparse(url).path.lower()
    for ext in [".pdf", ".xlsx", ".xls", ".csv"]:
        if path.endswith(ext):
            return True, ext.lstrip(".")
    return False, ""
