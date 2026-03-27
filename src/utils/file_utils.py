"""
Bestandshulpfuncties: opslaan, laden, paden beheren.
"""

import json
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Any


def save_json(data: Any, path: str | Path) -> None:
    """Sla data op als JSON-bestand."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_json(path: str | Path) -> Any:
    """Laad JSON-bestand. Geeft None terug als bestand niet bestaat."""
    path = Path(path)
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def url_to_filename(url: str, extension: str = "") -> str:
    """
    Zet URL om naar een veilige bestandsnaam via MD5-hash.
    Behoudt een leesbaar prefix van het pad.
    """
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    # Leesbaar prefix: laatste deel van pad
    safe_part = url.split("//")[-1].replace("/", "_").replace("?", "_")[:60]
    safe_part = "".join(c for c in safe_part if c.isalnum() or c in "_-.")
    filename = f"{safe_part}_{url_hash}"
    if extension and not extension.startswith("."):
        extension = f".{extension}"
    return filename + extension


def save_raw_html(html: str, url: str, raw_html_dir: str) -> Path:
    """Sla ruwe HTML op. Geeft pad terug."""
    filename = url_to_filename(url, "html")
    path = Path(raw_html_dir) / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return path


def save_raw_binary(content: bytes, url: str, raw_dir: str, extension: str = "pdf") -> Path:
    """Sla binair bestand op (bijv. PDF)."""
    filename = url_to_filename(url, extension)
    path = Path(raw_dir) / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def now_iso() -> str:
    """Huidige tijd als ISO-8601 string."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
