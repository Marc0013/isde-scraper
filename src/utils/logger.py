"""
Logging utility voor de ISDE scraper.
Schrijft naar console én logbestand.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_logger(name: str, log_dir: str = "logs", level: str = "INFO") -> logging.Logger:
    """
    Maak een logger aan die schrijft naar console en naar een logbestand.

    Args:
        name: naam van de logger (bijv. 'scraper', 'parser')
        log_dir: map voor logbestanden
        level: logniveau ('DEBUG', 'INFO', 'WARNING', 'ERROR')
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    log_level = getattr(logging, level.upper(), logging.INFO)
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Voorkom dubbele handlers bij herinitialisatie
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Bestand handler (dagelijks bestand)
    today = datetime.now().strftime("%Y%m%d")
    log_file = Path(log_dir) / f"isde_scraper_{today}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """Haal bestaande logger op of maak nieuwe aan."""
    return logging.getLogger(name)
