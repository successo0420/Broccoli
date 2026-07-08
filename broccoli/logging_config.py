import logging
import sys
from typing import Optional


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure root logger with a console handler.

    Args:
        level: Logging level (e.g., logging.DEBUG, logging.INFO).
    """
    root = logging.getLogger()
    root.setLevel(level)

    # Remove existing handlers to avoid duplicates (useful when re‑configuring)
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    root.addHandler(handler)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger with the given name (typically __name__).

    If name is None, returns the root logger.
    """
    return logging.getLogger(name)


# Optional: pre‑configure with default INFO level (can be overridden later)
setup_logging(logging.INFO)
