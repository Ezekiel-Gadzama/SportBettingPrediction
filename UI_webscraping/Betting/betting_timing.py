"""Small delays between UI actions — reduces burst timing that looks robotic."""

from __future__ import annotations

import random
import time


def random_human_pause(min_s: float = 0.3, max_s: float = 2.0) -> float:
    """Sleep a random duration in [min_s, max_s]; returns the chosen delay in seconds."""
    lo, hi = float(min_s), float(max_s)
    if hi < lo:
        lo, hi = hi, lo
    delay = random.uniform(lo, hi)
    time.sleep(delay)
    return delay
