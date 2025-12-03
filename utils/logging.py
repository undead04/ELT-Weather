import logging
import logging.handlers
from typing import Optional

from .config import LOG_DIR


def setup_logging(level: str = "INFO", max_bytes: int = 10 * 1024 * 1024, backup_count: int = 5):
    """Configure root logger with console + rotating file handler.

    File handler writes to LOG_DIR/root.log. Use get_logger(name) for domain-specific names.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_level = getattr(logging, level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(log_level)

    # Avoid adding handlers multiple times when called repeatedly
    if any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        return

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s - %(message)s')
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # Rotating file handler for general logs
    fh = logging.handlers.RotatingFileHandler(LOG_DIR / "root.log", maxBytes=max_bytes, backupCount=backup_count)
    fh.setLevel(log_level)
    fh.setFormatter(fmt)
    root.addHandler(fh)


def get_logger(name: Optional[str] = None, domain_file: Optional[str] = None, level: str = "INFO") -> logging.Logger:
    """Return a child logger. If domain_file is provided, add a rotating FileHandler writing to LOG_DIR/domain_file.

    Example: get_logger('scripts.extract', domain_file='weather.log')
    """
    if name is None:
        name = "app"

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Ensure base LOG_DIR exists
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    if domain_file:
        # Avoid attaching duplicate file handlers
        existing = [h for h in logger.handlers if isinstance(h, logging.handlers.RotatingFileHandler)]
        if not existing:
            fh = logging.handlers.RotatingFileHandler(LOG_DIR / domain_file, maxBytes=10 * 1024 * 1024, backupCount=5)
            fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s - %(message)s')
            fh.setFormatter(fmt)
            logger.addHandler(fh)

    return logger


__all__ = ["setup_logging", "get_logger"]
