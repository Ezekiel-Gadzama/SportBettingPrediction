from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any


@dataclass
class LiveListRow:
    """One football live-list row (live_list page) or legacy sidebar row."""

    home: str
    away: str
    minute: int | None
    time_raw: str
    home_goals: int
    away_goals: int
    game_id: str | None = None  # H1 / H2 / HT (from live list)
    league: str | None = None
    element: Any = None  # WebElement: match row to scroll/click (must be a field, not class attr)
    odd_home: float | None = None
    odd_draw: float | None = None
    odd_away: float | None = None

    @property
    def match_key(self) -> str:
        a, b = sorted([self.home.strip().lower(), self.away.strip().lower()])
        return f"{a}|{b}"

    def odd_for_1x2_selection(self, selection: str) -> float | None:
        sel = (selection or "").lower().strip()
        return {"home": self.odd_home, "draw": self.odd_draw, "away": self.odd_away}.get(sel)


_TIME_RE = re.compile(
    r"^(?P<m>\d+)(?:\+\d+)?'\s*(?P<phase>H[12]|HT)?",
    re.IGNORECASE,
)

_SCORE_RE = re.compile(r"^\s*(\d+)\s*:\s*(\d+)\s*$")


def parse_sidebar_time_minute(time_text: str) -> int | None:
    """
    Parse strings like \"89' H2\", \"45' HT\", \"11' H1\".
    Returns the displayed minute (e.g. 89), or None if not parsed.
    Halftime rows still parse as 45 — caller should filter by phase if needed.
    """
    t = (time_text or "").strip()
    if not t:
        return None
    m = _TIME_RE.match(t)
    if not m:
        return None
    try:
        return int(m.group("m"))
    except ValueError:
        return None


def is_ht_time(time_text: str) -> bool:
    return "HT" in (time_text or "").upper()


def is_halftime_game_id(game_id: str) -> bool:
    return (game_id or "").strip().upper() == "HT"


_CLOCK_MMSS = re.compile(r"^\s*(\d+)\s*:\s*(\d+)\s*$")


def parse_live_list_clock_minute(clock_text: str, game_id: str) -> int | None:
    """
    Live list shows cumulative clock e.g. \"70:54\" with game-id H2.
    Returns integer minute (70). Halftime (game-id HT) -> None.
    """
    if is_halftime_game_id(game_id):
        return None
    m = _CLOCK_MMSS.match((clock_text or "").strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def parse_odd_float(text: str) -> float | None:
    if not text:
        return None
    t = text.strip().replace(",", "")
    m = re.search(r"(\d+\.?\d*)", t)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def parse_score(score_text: str) -> tuple[int, int] | None:
    """
    Parse \"1:1\" / \"0:0\" from the score span.
    """
    if not score_text:
        return None
    m = _SCORE_RE.match(score_text.strip().replace("–", "-"))
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def minute_in_bet_window(
    minute: int | None,
    *,
    min_exclusive: float = 88.0,
    max_exclusive: float = 91.0,
) -> bool:
    """Match must satisfy min_exclusive < minute < max_exclusive (default → 89, 90)."""
    if minute is None:
        return False
    return min_exclusive < float(minute) < max_exclusive


def pick_1x2_selection(
    home_goals: int,
    away_goals: int,
    *,
    only_zero_zero: bool,
    only_draws: bool,
) -> str | None:
    """
    Returns 'home' | 'draw' | 'away' or None if this row should be skipped.
    only_zero_zero overrides only_draws.
    """
    if only_zero_zero:
        if home_goals == 0 and away_goals == 0:
            return "draw"
        return None
    if only_draws:
        if home_goals == away_goals:
            return "draw"
        return None
    if home_goals > away_goals:
        return "home"
    if home_goals < away_goals:
        return "away"
    return "draw"


def split_teams(vs_text: str) -> tuple[str, str] | None:
    """
    \"Real Oviedo vs Sevilla\" or \"A v B\" from sidebar / history.
    SportyBet bet history uses <span class=\"vs-text\">v</span> between names; Selenium
    often returns glued text like \"HomevAway\" (no spaces around v), which plain
    \" v \" splitting misses.
    """
    raw = (vs_text or "").replace("\n", " ").strip()
    if not raw:
        return None
    for sep in (" vs ", " VS ", " v ", " V "):
        if sep.lower() in raw.lower():
            parts = re.split(re.escape(sep), raw, maxsplit=1, flags=re.IGNORECASE)
            if len(parts) == 2:
                a, b = parts[0].strip(), parts[1].strip()
                if a and b:
                    return a, b
    # Glued "v" from history DOM: "Club Laguna SafvCentral SC PE"
    glued = re.split(r"(?<=\S)v(?=\S)", raw, maxsplit=1)
    if len(glued) == 2:
        a, b = glued[0].strip(), glued[1].strip()
        if a and b:
            return a, b
    return None


def teams_match_key(home_a: str, away_a: str, text_b: str) -> bool:
    """Compare stored teams to a history row like \"Team A v Team B\"."""
    parsed = split_teams(text_b)
    if not parsed:
        return False
    h2, a2 = parsed
    s1 = {home_a.strip().lower(), away_a.strip().lower()}
    s2 = {h2.strip().lower(), a2.strip().lower()}
    return s1 == s2


def _norm_team_name(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _team_name_similarity(x: str, y: str) -> float:
    """
    Similarity in [0, 1]. Uses SequenceMatcher; if the shorter normalized name appears
    as a substring of the longer (len >= 4), boost score so live-list vs history
    abbreviations (e.g. \"Bolivar\" vs \"Bolivar SC\") still match at high confidence.
    """
    x, y = _norm_team_name(x), _norm_team_name(y)
    if not x or not y:
        return 0.0
    if x == y:
        return 1.0
    base = SequenceMatcher(None, x, y).ratio()
    shorter, longer = (x, y) if len(x) <= len(y) else (y, x)
    if len(shorter) >= 4 and shorter in longer:
        return max(base, 0.92)
    return base


def teams_match_fuzzy(
    home_a: str,
    away_a: str,
    text_b: str,
    *,
    threshold: float = 0.85,
) -> bool:
    """
    Exact set match first, else both sides must reach ``threshold`` similarity for either
    (home,away)≈(h2,a2) or swapped — using difflib + substring boost for truncated names.
    """
    if teams_match_key(home_a, away_a, text_b):
        return True
    parsed = split_teams(text_b)
    if not parsed:
        return False
    h2, a2 = parsed[0].strip(), parsed[1].strip()
    ha, aa = home_a.strip(), away_a.strip()
    d_h = _team_name_similarity(ha, h2)
    d_a = _team_name_similarity(aa, a2)
    s_h = _team_name_similarity(ha, a2)
    s_a = _team_name_similarity(aa, h2)
    if d_h >= threshold and d_a >= threshold:
        return True
    if s_h >= threshold and s_a >= threshold:
        return True
    return False
