from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path


class _Tee:
    def __init__(self, *streams):
        self._streams = streams

    def write(self, data):
        for s in self._streams:
            try:
                s.write(data)
                s.flush()
            except Exception:
                pass

    def flush(self):
        for s in self._streams:
            try:
                s.flush()
            except Exception:
                pass


def tee_stdout_stderr_to_log(script_file: str, *, log_dir: str | None = None) -> str:
    """
    Redirect stdout/stderr to <scriptname>.log (and keep console output).
    Returns absolute log path.
    """
    base = Path(log_dir) if log_dir else Path(script_file).resolve().parent
    base.mkdir(parents=True, exist_ok=True)
    log_path = base / f"{Path(script_file).stem}.log"
    fh = open(log_path, "a", encoding="utf-8")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fh.write(f"\n----- start {ts} pid={os.getpid()} -----\n")
    fh.flush()

    sys.stdout = _Tee(sys.__stdout__, fh)  # type: ignore[assignment]
    sys.stderr = _Tee(sys.__stderr__, fh)  # type: ignore[assignment]
    return str(log_path.resolve())

