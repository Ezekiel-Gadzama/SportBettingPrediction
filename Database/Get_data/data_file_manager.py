"""
Helpers for rotating large CSV scrapes under Database/Data into Database/Data/Done.

Per-thread file size budget = base_max_bytes / num_threads so total disk churn stays bounded
when running many concurrent scrapers.
"""
from __future__ import annotations

import os
import shutil
from datetime import datetime


def resolve_base_max_bytes(override: int | None) -> int:
    """Default 50 MiB total budget; override via arg or env SCRAPER_DATA_MAX_BYTES_BASE."""
    if override is not None and int(override) > 0:
        return int(override)
    raw = os.environ.get("SCRAPER_DATA_MAX_BYTES_BASE", str(50 * 1024 * 1024))
    try:
        return max(1024 * 1024, int(raw))  # at least 1 MiB base
    except ValueError:
        return 50 * 1024 * 1024


def effective_max_bytes_per_thread(base_max_bytes: int, num_threads: int) -> int:
    """More threads => smaller per-file threshold (shared aggregate budget)."""
    n = max(1, int(num_threads))
    per = int(base_max_bytes) // n
    return max(512 * 1024, per)  # floor 512 KiB per thread


def ensure_data_dirs(cwd: str | None = None) -> tuple[str, str]:
    root = cwd or os.getcwd()
    data_dir = os.path.join(root, "Database", "Data")
    done_dir = os.path.join(data_dir, "Done")
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(done_dir, exist_ok=True)
    return data_dir, done_dir


def new_stamped_csv_path(data_dir: str, sport_slug: str, thread_index: int) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    name = f"{sport_slug}_long_format_{int(thread_index)}_{ts}.csv"
    return os.path.join(data_dir, name)


def move_path_to_done_dir(src_path: str, done_dir: str) -> str:
    """Move src into done_dir; returns final destination path."""
    os.makedirs(done_dir, exist_ok=True)
    base = os.path.basename(src_path)
    dest = os.path.join(done_dir, base)
    if os.path.abspath(src_path) == os.path.abspath(dest):
        return dest
    n = 0
    stem, ext = os.path.splitext(base)
    while os.path.exists(dest):
        n += 1
        dest = os.path.join(done_dir, f"{stem}_dup{n}{ext}")
    shutil.move(src_path, dest)
    return dest


def rotate_oversized_csv_after_match(
    current_path: str,
    *,
    sport_slug: str,
    thread_index: int,
    data_dir: str,
    done_dir: str,
    base_max_bytes: int,
    num_threads: int,
) -> str:
    """
    After a match has fully ended: if current CSV is at or above the per-thread threshold,
    move it to Done and return a new stamped path under data_dir. Otherwise return current_path.
    """
    threshold = effective_max_bytes_per_thread(base_max_bytes, num_threads)
    if not current_path or not os.path.isfile(current_path):
        return current_path or new_stamped_csv_path(data_dir, sport_slug, thread_index)
    try:
        size = os.path.getsize(current_path)
    except OSError:
        return current_path
    if size < threshold:
        return current_path
    dest = move_path_to_done_dir(current_path, done_dir)
    print(
        f"[INFO] Data CSV size {size} bytes >= per-thread limit {threshold} "
        f"(base budget {base_max_bytes} / {max(1, int(num_threads))} threads); "
        f"moved to Done: {dest}"
    )
    new_path = new_stamped_csv_path(data_dir, sport_slug, thread_index)
    print(f"[INFO] New active CSV: {new_path}")
    return new_path
