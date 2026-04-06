import logging
import os
from datetime import datetime


def setup_betting_logger(
    log_dir: str | None = None,
    name: str = "live_betting",
    *,
    clear_file_on_start: bool = True,
) -> logging.Logger:
    """
    Console + file log under UI_webscraping/Betting/logs by default.
    If clear_file_on_start is True, today's log file is truncated when the process starts.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    base = log_dir or os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(base, exist_ok=True)
    path = os.path.join(base, f"{name}_{datetime.now().strftime('%Y%m%d')}.log")
    file_mode = "w" if clear_file_on_start else "a"
    fh = logging.FileHandler(path, mode=file_mode, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.debug("Log file: %s", os.path.abspath(path))
    return logger
