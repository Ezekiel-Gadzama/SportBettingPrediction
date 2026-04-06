from __future__ import annotations

import dataclasses
import logging
import time
from dataclasses import dataclass, field

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import sys
import os

# Project root
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from UI_webscraping.Login.login import SportyBetLoginBot
from UI_webscraping.Betting import betting_logger as _bet_log
from UI_webscraping.Betting import bet_slip_actions as slip
from UI_webscraping.Betting import bet_history as bh
from UI_webscraping.Betting.betting_timing import random_human_pause
from UI_webscraping.Betting.league_filter import LeagueFilter
from UI_webscraping.Betting.parsing import (
    LiveListRow,
    is_halftime_game_id,
    minute_in_bet_window,
    parse_live_list_clock_minute,
    parse_odd_float,
    pick_1x2_selection,
)
DEFAULT_LIVE_URL = "https://www.sportybet.com/ng/sport/football/live_list/"


@dataclass
class LiveBettingConfig:
    amount_to_use: float = 3000.0
    max_simultaneous_matches: int = 30
    # Full-time betting window (minutes)
    bet_fulltime: bool = True
    ft_min_minute_exclusive: float = 88.0
    ft_max_minute_exclusive: float = 91.0
    # Half-time betting (1st half) — only when game_id == H1
    bet_halftime: bool = False
    ht_min_minute_exclusive: float = 44.0
    ht_max_minute_exclusive: float = 46.0
    # If set, only bet when the chosen 1X2 odd is >= this (e.g. 1.25). If None, no minimum.
    minimum_odd: float | None = None
    # If set, reject odds above this (live prices can spike). If None, no upper cap.
    maximum_odd: float | None = None
    # Skip matches where |home_goals - away_goals| > this (1X2 is often removed when leading by 2+).
    max_abs_goal_diff_for_1x2: int = 1
    # Skip matches where (home_goals + away_goals) > this.
    ft_max_total_goals: int = 4
    ht_max_total_goals: int = 2
    # Exclude competitions (league names) that often have volatile stoppage-time goals.
    # Mapping is done via DeepSeek and cached to disk (normalized league -> matched excluded name or "NONE").
    excluded_competitions: list[str] = field(
        default_factory=lambda: [
            "Premier League (England)",
            "La Liga (Spain)",
            "UEFA Champions League",
            "FA Cup (England)",
            "FIFA World Cup Qualifiers",
            "NWSL (USA)",
            "Women's Super League (UK)",
            "UEFA Women's Champions League",
            "Women's EURO 2025",
        ]
    )
    league_cache_path: str = os.path.join(os.path.dirname(__file__), "league_filter_cache.json")
    deepseek_enabled: bool = True
    deepseek_base_url: str | None = None
    deepseek_model: str = "deepseek-chat"
    deepseek_timeout_s: float = 35.0
    only_bet_draws: bool = False
    only_bet_zero_zero_score: bool = False
    result_wait_seconds: float = 600.0
    cache_ttl_seconds: float = 1200.0
    bet_history_pages: int = 5
    # Min per-side similarity (0–1) when matching live-list names to bet history (abbreviations).
    bet_history_team_match_ratio: float = 0.85
    poll_sleep_seconds: float = 8.0
    live_url: str = DEFAULT_LIVE_URL


@dataclass
class PendingBet:
    home: str
    away: str
    booking_code: str | None
    stake: float
    potential_win: str | None
    placed_at: float
    check_after: float
    selection: str
    history_retries: int = 0


@dataclass
class BetCacheEntry:
    match_key: str
    expires_at: float


class LiveBettingBot(SportyBetLoginBot):
    """
    Live football 1X2 betting loop using SportyBetLoginBot for auth and driver.
    Single-threaded: one browser, one action sequence at a time.
    """

    def __init__(self, config: LiveBettingConfig | None = None):
        cfg = config or LiveBettingConfig()
        super().__init__(cfg.live_url)
        self.cfg = cfg
        self.log = _bet_log.setup_betting_logger(
            os.path.join(os.path.dirname(__file__), "logs"),
            name="live_betting",
            clear_file_on_start=True,
        )

        # Initial capital (fixed) — used for profit % calculations in logs
        self.initial_amount_to_use: float = float(cfg.amount_to_use)
        # Stake sizing: always estimated_balance / max_simultaneous_matches (starts equal to initial)
        self.estimated_balance: float = self.initial_amount_to_use
        # Tracked "real" balance for logging only: starts at initial; +profit on win, net loss on loss
        self.tracked_balance: float = self.initial_amount_to_use
        self.wins: int = 0
        self.losses: int = 0
        self.pending: list[PendingBet] = []
        # Successful bet recently placed — do not open same fixture again until TTL
        self._cache: list[BetCacheEntry] = []
        # Tried and failed (low odd, closed market, errors) — same TTL as _cache
        self._skipped_cache: list[BetCacheEntry] = []
        self._league_filter = LeagueFilter(
            cache_path=self.cfg.league_cache_path,
            exclude_list=self.cfg.excluded_competitions,
            deepseek_enabled=self.cfg.deepseek_enabled,
            deepseek_base_url=self.cfg.deepseek_base_url,
            deepseek_model=self.cfg.deepseek_model,
            deepseek_timeout_s=self.cfg.deepseek_timeout_s,
            logger=self.log,
        )
        self._league_filter.load()
        # Per-step counters (reset each try_place_bets call)
        self._league_filter_skipped_counts: dict[str, int] = {}

    # --- navigation ---

    def go_live(self) -> None:
        """
        Open the football live list. Avoids a full reload on every poll when the
        session is already on that URL — constant get() was racing the SPA and looked
        like endless refresh; it also often yielded 0 rows if read before render.
        """
        cur = (self.driver.current_url or "").lower()
        live = (self.cfg.live_url or "").strip()
        on_list = "sportybet.com" in cur and "football/live_list" in cur
        if on_list:
            self.log.debug("Already on football live list; skipping driver.get() reload")
            time.sleep(0.5)
        else:
            self.load_url(live)
            time.sleep(2.5)
        self._relogin_if_header_login_visible()
        self.ensure_football_sport_selected()
        self._scroll_live_list_for_lazy_rows()
        self._update_league_mapping_from_dom()

    def _update_league_mapping_from_dom(self) -> None:
        """Read all league rows currently on the page and ask DeepSeek to map unknown ones."""
        try:
            leagues = []
            for el in self.driver.find_elements(
                By.CSS_SELECTOR, "div.m-table-row.league-row .m-table-cell.league"
            ):
                try:
                    txt = (el.text or "").strip()
                    if txt:
                        leagues.append(txt)
                except Exception:
                    continue
            if leagues:
                self._league_filter.update_mappings(leagues)
        except Exception:
            self.log.debug("_update_league_mapping_from_dom", exc_info=True)

    def _relogin_if_header_login_visible(self) -> None:
        """If the site shows the header login bar (session expired), sign in again from .env."""
        try:
            if not self.is_header_login_form_visible():
                return
            self.log.warning(
                "[Session] Header login form visible (likely logged out) — re-authenticating."
            )
            if self.relogin_via_header():
                self.log.info("[Session] Re-login submitted; continuing on live list.")
        except ValueError as e:
            self.log.error("[Session] Cannot re-login: %s", e)
        except Exception as e:
            self.log.error("[Session] Re-login failed: %s", e)
            self.log.debug("_relogin_if_header_login_visible", exc_info=True)

    def _scroll_live_list_for_lazy_rows(self) -> None:
        """Nudge scroll so below-the-fold live rows (if any) mount in the DOM."""
        try:
            self.driver.execute_script(
                """
                var el = document.querySelector('div.m-main-mid') || document.querySelector('.m-overview') || document.body;
                if (el) { el.scrollTop = el.scrollHeight; }
                window.scrollTo(0, document.body.scrollHeight);
                """
            )
            time.sleep(0.6)
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.3)
        except Exception:
            self.log.debug("scroll live list", exc_info=True)

    def ensure_football_sport_selected(self) -> None:
        """On football/live_list, ensure overview sport 'Football' is selected."""
        try:
            overview = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.m-overview"))
            )
            items = overview.find_elements(By.CSS_SELECTOR, "div.sport-name-item")
            for item in items:
                try:
                    label = item.find_element(By.CSS_SELECTOR, "div.text").text.strip()
                except Exception:
                    continue
                if label != "Football":
                    continue
                cls = item.get_attribute("class") or ""
                if "active" not in cls:
                    self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", item)
                    time.sleep(0.2)
                    self.driver.execute_script("arguments[0].click();", item)
                    self.log.info("[Live list] Selected Football in the sport overview.")
                    time.sleep(2)
                else:
                    self.log.debug("Football sport already active in overview.")
                return
            self.log.warning("[Live list] Could not find the Football sport pill in the overview.")
        except Exception:
            self.log.warning(
                "[Live list] Could not use the sport overview (page still loading or layout changed)."
            )
            self.log.debug("ensure_football_sport_selected detail", exc_info=True)

    def collect_live_rows(self) -> list[LiveListRow]:
        rows: list[LiveListRow] = []
        skipped_ht = 0
        parse_errors = 0
        raw_dom = 0
        # Prefer any content row; some builds omit the football-row class.
        row_css_candidates = (
            "div.m-table-row.m-content-row.match-row.football-row",
            "div.m-table-row.m-content-row.match-row",
        )
        try:
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.m-table.match-table.live-table, div.m-overview")
                )
            )
            match_rows: list = []
            for css in row_css_candidates:
                found = self.driver.find_elements(By.CSS_SELECTOR, css)
                match_rows = [
                    e
                    for e in found
                    if "league-row" not in ((e.get_attribute("class") or "").lower())
                ]
                if match_rows:
                    break
            raw_dom = len(match_rows)

            def _row_label() -> str:
                try:
                    el = mr.find_element(By.CSS_SELECTOR, ".teams")
                    t = (el.get_attribute("title") or "").strip()
                    if t:
                        return t
                    h = el.find_element(By.CSS_SELECTOR, ".home-team").text.strip()
                    a = el.find_element(By.CSS_SELECTOR, ".away-team").text.strip()
                    return f"{h} vs {a}" if (h or a) else "(unknown teams)"
                except Exception:
                    return "(unknown teams)"

            def _fmt_ex_short(ex: BaseException) -> str:
                s = str(ex).strip()
                if not s:
                    return type(ex).__name__
                first = s.splitlines()[0]
                return first[:220] + ("…" if len(first) > 220 else "")

            for mr in match_rows:
                try:
                    league_name = None
                    try:
                        league_el = mr.find_element(
                            By.XPATH,
                            "preceding-sibling::div[contains(@class,'league-row')][1]//div[contains(@class,'league')]",
                        )
                        league_name = (league_el.text or "").strip()
                    except Exception:
                        league_name = None
                    left = mr.find_element(By.CSS_SELECTOR, ".left-team-cell")
                    # Layout: left-team-cell > left-arrow + left-team-table(time, teams, score)
                    try:
                        lt = left.find_element(By.CSS_SELECTOR, ".left-team-table")
                    except Exception:
                        lt = left
                    clock_els = lt.find_elements(By.CSS_SELECTOR, ".clock-time")
                    gid_els = lt.find_elements(By.CSS_SELECTOR, ".game-id")
                    if not clock_els or not gid_els:
                        parse_errors += 1
                        self.log.info(
                            "[Live list parse] %s | missing clock/game-id "
                            "(row updating, stale, or non-match row)",
                            _row_label(),
                        )
                        continue
                    clock = clock_els[0].text.strip()
                    game_id = gid_els[0].text.strip()
                    if is_halftime_game_id(game_id):
                        skipped_ht += 1
                        self.log.debug(
                            "[Live list] skip HT break: %s | clock=%s game_id=%s",
                            _row_label(),
                            clock,
                            game_id,
                        )
                        continue
                    home = lt.find_element(By.CSS_SELECTOR, ".home-team").text.strip()
                    away = lt.find_element(By.CSS_SELECTOR, ".away-team").text.strip()
                    score_items = lt.find_elements(By.CSS_SELECTOR, ".score .score-item")
                    if len(score_items) < 2:
                        parse_errors += 1
                        self.log.info(
                            "[Live list parse] %s | need 2 score cells, found %d",
                            f"{home} vs {away}",
                            len(score_items),
                        )
                        continue
                    try:
                        hg = int(score_items[0].text.strip())
                        ag = int(score_items[1].text.strip())
                    except ValueError:
                        parse_errors += 1
                        self.log.info(
                            "[Live list parse] %s | non-integer score: %r / %r",
                            f"{home} vs {away}",
                            score_items[0].text,
                            score_items[1].text,
                        )
                        continue
                    minute = parse_live_list_clock_minute(clock, game_id)
                    time_raw = f"{clock} {game_id}".strip()

                    odd_home = odd_draw = odd_away = None
                    market_cell = mr.find_element(By.CSS_SELECTOR, ".m-table-cell.market-cell")
                    # :scope > ... is unreliable on WebElement in Selenium; use direct-child XPath.
                    mkts = market_cell.find_elements(
                        By.XPATH, "./div[contains(@class,'m-market')]"
                    )
                    # "Next Goals" is the block with the goal-count dropdown (.af-select).
                    # Do not skip on .specifiers-select alone — some layouts put that class on
                    # 3-Way cells too, which made us skip every market and break parsing.
                    three_way = None
                    for mkt in mkts:
                        if mkt.find_elements(By.CSS_SELECTOR, ".af-select"):
                            continue
                        three_way = mkt
                        break
                    if three_way is None and mkts:
                        three_way = mkts[0]
                    if three_way is not None:
                        spans = three_way.find_elements(By.CSS_SELECTOR, "span.m-outcome-odds")
                        vals: list[float | None] = []
                        for sp in spans[:3]:
                            cls = sp.get_attribute("class") or ""
                            if "m-icon-lock" in cls:
                                vals.append(None)
                            else:
                                vals.append(parse_odd_float(sp.text))
                        if len(vals) >= 3:
                            odd_home, odd_draw, odd_away = vals[0], vals[1], vals[2]

                    rows.append(
                        LiveListRow(
                            home=home,
                            away=away,
                            league=league_name,
                            minute=minute,
                            time_raw=time_raw,
                            home_goals=hg,
                            away_goals=ag,
                            game_id=game_id,
                            element=mr,
                            odd_home=odd_home,
                            odd_draw=odd_draw,
                            odd_away=odd_away,
                        )
                    )
                except Exception as ex:
                    parse_errors += 1
                    self.log.info(
                        "[Live list parse] %s | %s: %s",
                        _row_label(),
                        type(ex).__name__,
                        _fmt_ex_short(ex),
                    )
                    self.log.debug("Live list parse row (stack)", exc_info=True)
                    continue
        except Exception:
            self.log.warning(
                "[Live list] Timed out or could not find the live table (layout/login/geo). "
                "Check you are logged in and the page shows live matches."
            )
            self.log.debug("collect_live_rows detail", exc_info=True)
        if not rows and raw_dom == 0:
            self.log.info(
                "[Live list] 0 match rows in DOM — table empty, blocked, or still loading. url=%s",
                self.driver.current_url,
            )
        elif not rows and raw_dom > 0:
            self.log.info(
                "[Live list] 0 usable rows (raw_dom=%d halftime_skipped=%d parse_errors=%d). "
                "halftime_skipped = rows at HT (half-time); not used for live minute window.",
                raw_dom,
                skipped_ht,
                parse_errors,
            )
        elif parse_errors > 0:
            self.log.info(
                "[Live list] summary: parsed=%d raw_dom=%d halftime_skipped=%d (HT rows) parse_errors=%d",
                len(rows),
                raw_dom,
                skipped_ht,
                parse_errors,
            )
        return rows

    def _sweep_cache(self) -> None:
        now = time.time()
        self._cache = [c for c in self._cache if c.expires_at > now]
        self._skipped_cache = [c for c in self._skipped_cache if c.expires_at > now]

    def _cache_has(self, key: str) -> bool:
        now = time.time()
        return any(c.match_key == key and c.expires_at > now for c in self._cache)

    def _skipped_cache_has(self, key: str) -> bool:
        now = time.time()
        return any(c.match_key == key and c.expires_at > now for c in self._skipped_cache)

    def _match_cache_blocked(self, key: str) -> bool:
        """Recently bet successfully or recently skipped (failed attempt)."""
        return self._cache_has(key) or self._skipped_cache_has(key)

    def _cache_add(self, key: str) -> None:
        self._cache.append(BetCacheEntry(key, time.time() + self.cfg.cache_ttl_seconds))

    def _skipped_cache_add(self, key: str, reason: str) -> None:
        self._skipped_cache.append(BetCacheEntry(key, time.time() + self.cfg.cache_ttl_seconds))
        self.log.info(
            "[Skip cache] %s — will not retry this fixture until cache TTL (same as bet cache).",
            reason,
        )

    def stake_per_match(self) -> float:
        """Always estimated_account_balance / max_simultaneous_matches (min stake 10)."""
        n = max(1, self.cfg.max_simultaneous_matches)
        return max(10.0, round(self.estimated_balance / n, 2))

    def eligible_rows(self, rows: list[LiveListRow]) -> list[LiveListRow]:
        out: list[LiveListRow] = []
        for r in rows:
            if r.league:
                ex, matched = self._league_filter.should_exclude(r.league)
                if ex:
                    try:
                        self._league_filter_skipped_counts[r.league] = (
                            self._league_filter_skipped_counts.get(r.league, 0) + 1
                        )
                    except Exception:
                        pass
                    self.log.debug(
                        "Skip %s vs %s: excluded competition league=%r (matched=%r)",
                        r.home,
                        r.away,
                        r.league,
                        matched,
                    )
                    continue
            if r.minute is None:
                continue
            # Decide whether this row is a Half-time (H1) bet or Full-time bet candidate
            bet_market = "1X2"
            bet_tag = "FT"
            bet_max_total_goals = int(self.cfg.ft_max_total_goals)
            if (
                self.cfg.bet_halftime
                and (r.game_id or "").strip().upper() == "H1"
                and minute_in_bet_window(
                    r.minute,
                    min_exclusive=self.cfg.ht_min_minute_exclusive,
                    max_exclusive=self.cfg.ht_max_minute_exclusive,
                )
            ):
                bet_market = "1st Half - 1X2"
                bet_tag = "HT"
                bet_max_total_goals = int(self.cfg.ht_max_total_goals)
            else:
                if not self.cfg.bet_fulltime:
                    continue
                if not minute_in_bet_window(
                    r.minute,
                    min_exclusive=self.cfg.ft_min_minute_exclusive,
                    max_exclusive=self.cfg.ft_max_minute_exclusive,
                ):
                    continue
            gap = abs(int(r.home_goals) - int(r.away_goals))
            if gap > int(self.cfg.max_abs_goal_diff_for_1x2):
                self.log.debug(
                    "Skip %s vs %s: score %d-%d (gap %d > max %d for 1X2)",
                    r.home,
                    r.away,
                    r.home_goals,
                    r.away_goals,
                    gap,
                    self.cfg.max_abs_goal_diff_for_1x2,
                )
                continue
            tg = int(r.home_goals) + int(r.away_goals)
            if tg > int(bet_max_total_goals):
                self.log.debug(
                    "Skip %s vs %s: total_goals=%d (score %d-%d) > %s_max_total_goals=%d",
                    r.home,
                    r.away,
                    tg,
                    r.home_goals,
                    r.away_goals,
                    bet_tag,
                    bet_max_total_goals,
                )
                continue
            sel = pick_1x2_selection(
                r.home_goals,
                r.away_goals,
                only_zero_zero=self.cfg.only_bet_zero_zero_score,
                only_draws=self.cfg.only_bet_draws,
            )
            if sel is None:
                continue
            bet_key = f"{r.match_key}|{bet_tag}"
            if self._match_cache_blocked(bet_key):
                self.log.debug("Cached (skip retry): %s vs %s", r.home, r.away)
                continue
            # attach per-row bet metadata via element attribute (keeps LiveListRow minimal)
            setattr(r, "_bet_market_name", bet_market)
            setattr(r, "_bet_tag", bet_tag)
            setattr(r, "_bet_cache_key", bet_key)
            out.append(r)
        # Prefer lower minute first (closer to end / window)
        out.sort(key=lambda x: x.minute or 0)
        return out

    def try_place_bets(self) -> int:
        placed = 0
        # Reset per-step league skip counters
        self._league_filter_skipped_counts = {}
        rows = self.collect_live_rows()
        candidates = self.eligible_rows(rows)
        self.log.info("Live rows=%s eligible=%s pending=%s/%s", len(rows), len(candidates), len(self.pending), self.cfg.max_simultaneous_matches)
        if self._league_filter_skipped_counts:
            top = sorted(
                self._league_filter_skipped_counts.items(), key=lambda kv: kv[1], reverse=True
            )
            # Keep log compact: show up to top 12 leagues
            summary = " | ".join([f"{k}={v}" for k, v in top[:12]])
            more = f" (+{len(top)-12} more)" if len(top) > 12 else ""
            self.log.info("[League filter] skipped_by_league: %s%s", summary, more)

        for row in candidates:
            if len(self.pending) >= self.cfg.max_simultaneous_matches:
                self.log.info("Max simultaneous matches reached; stop placing")
                break
            sel = pick_1x2_selection(
                row.home_goals,
                row.away_goals,
                only_zero_zero=self.cfg.only_bet_zero_zero_score,
                only_draws=self.cfg.only_bet_draws,
            )
            if sel is None:
                continue
            stake_amt = self.stake_per_match()
            self.log.info(
                "Attempt bet %s vs %s @ %s' score=%s:%s selection=%s stake=%.2f "
                "(estimated_bal %.2f / max_sim %d)",
                row.home,
                row.away,
                row.minute,
                row.home_goals,
                row.away_goals,
                sel,
                stake_amt,
                self.estimated_balance,
                self.cfg.max_simultaneous_matches,
            )
            bet_market = getattr(row, "_bet_market_name", "1X2")
            bet_key = getattr(row, "_bet_cache_key", row.match_key)
            ok = self._place_bet_sequence(row, sel, stake_amt, bet_market, bet_key)
            if ok:
                placed += 1
                self._cache_add(bet_key)
            random_human_pause(0.3, 2.0)
        return placed

    def _fail_bet_flow(self, row: LiveListRow, public_reason: str, *, cache_key: str) -> bool:
        """Log, add to skip cache, clear slip, return to live. Always returns False."""
        self._skipped_cache_add(cache_key, public_reason)
        try:
            slip.cancel_all_betslips(self.driver, self.log)
        except Exception:
            self.log.debug("cancel_all_betslips during cleanup", exc_info=True)
        self.go_live()
        return False

    @staticmethod
    def _norm_team(s: str) -> str:
        return " ".join((s or "").strip().lower().split())

    def _find_live_list_click_target(self, home: str, away: str):
        """
        Re-query the live list for this fixture (avoids stale row.element).
        Prefer clicking `.teams` — it often carries the router/handler; full row hits other markets.
        """
        h, a = self._norm_team(home), self._norm_team(away)
        for css in (
            "div.m-table-row.m-content-row.match-row div.left-team-table",
            "div.match-row.football-row div.left-team-table",
            "div.left-team-table",
        ):
            for lt in self.driver.find_elements(By.CSS_SELECTOR, css):
                try:
                    hn = self._norm_team(
                        lt.find_element(By.CSS_SELECTOR, ".home-team").text
                    )
                    an = self._norm_team(
                        lt.find_element(By.CSS_SELECTOR, ".away-team").text
                    )
                    if hn == h and an == a:
                        try:
                            return lt.find_element(By.CSS_SELECTOR, ".teams")
                        except Exception:
                            return lt
                    teams_el = lt.find_element(By.CSS_SELECTOR, ".teams")
                    title = (teams_el.get_attribute("title") or "").strip()
                    if title and " vs " in title:
                        parts = [self._norm_team(p) for p in title.split(" vs ", 1)]
                        if len(parts) == 2 and parts[0] == h and parts[1] == a:
                            return teams_el
                except Exception:
                    continue
        return None

    def _click_open_match(self, click_el) -> None:
        self.driver.execute_script(
            "arguments[0].scrollIntoView({block:'center'});", click_el
        )
        time.sleep(0.25)
        try:
            click_el.click()
        except Exception:
            try:
                ActionChains(self.driver).move_to_element(click_el).pause(0.1).click().perform()
            except Exception:
                self.driver.execute_script("arguments[0].click();", click_el)

    def _submit_stake_place_confirm_success(
        self, row: LiveListRow, selection: str, stake_amt: float
    ) -> str:
        """
        Returns 'ok' | 'abort' | 'retry'.
        'abort' = odd out of range on slip (Accept Changes / Place Bet) — do not toggle-retry.
        """
        def _fast_pause():
            # Keep some jitter, but much shorter than navigation delays.
            random_human_pause(0.12, 0.55)

        _fast_pause()
        if not slip.enter_stake_amount(self.driver, stake_amt, self.log):
            # Sometimes the match page gets stale/laggy; refresh once and retry.
            try:
                self.log.warning("[Recovery] Stake entry failed — refreshing match page once.")
                self.driver.refresh()
                time.sleep(1.2)
                market_name = getattr(row, "_bet_market_name", "1X2")
                slip.wait_for_match_detail_1x2(
                    self.driver, self.log, timeout=12, market_name=market_name
                )
                slip.click_1x2_outcome(
                    self.driver,
                    selection,
                    home_name=row.home,
                    away_name=row.away,
                    logger=self.log,
                    market_name=market_name,
                )
                _fast_pause()
                if not slip.enter_stake_amount(self.driver, stake_amt, self.log):
                    return "retry"
            except Exception:
                self.log.debug("[Recovery] stake refresh retry", exc_info=True)
                return "retry"

        _fast_pause()
        pf = slip.click_place_bet_with_accept_flow(
            self.driver,
            self.log,
            minimum_odd=self.cfg.minimum_odd,
            maximum_odd=self.cfg.maximum_odd,
            human_pause=_fast_pause,
        )
        if pf == "abort_range":
            return "abort"
        if pf != "placed":
            return "retry"
        _fast_pause()
        if not slip.click_confirm_if_present(self.driver, self.log, wait=5):
            self.log.error(
                "[Confirm] Payment confirmation did not appear — will retry if attempts left."
            )
            return "retry"
        _fast_pause()
        if not slip.wait_success_dialog(self.driver, self.log, timeout=5):
            return "retry"
        return "ok"

    def _finalize_successful_bet(
        self, row: LiveListRow, selection: str, stake_amt: float
    ) -> None:
        details = slip.read_success_dialog_details(self.driver, self.log)
        slip.click_success_ok(self.driver, self.log)
        now = time.time()
        self.pending.append(
            PendingBet(
                home=row.home,
                away=row.away,
                booking_code=details.get("booking_code"),
                stake=stake_amt,
                potential_win=details.get("potential_win"),
                placed_at=now,
                check_after=now + self.cfg.result_wait_seconds,
                selection=selection,
            )
        )
        self.log.info(
            "Bet recorded pending check at +%ss | booking=%s",
            self.cfg.result_wait_seconds,
            details.get("booking_code"),
        )
        self.go_live()

    def _place_bet_sequence(
        self,
        row: LiveListRow,
        selection: str,
        stake_amt: float,
        market_name: str,
        cache_key: str,
    ) -> bool:
        try:
            self._scroll_live_list_for_lazy_rows()
            random_human_pause()
            click_el = self._find_live_list_click_target(row.home, row.away)
            if click_el is None and row.element is not None:
                root = row.element
                try:
                    click_el = root.find_element(By.CSS_SELECTOR, ".teams")
                except Exception:
                    try:
                        click_el = root.find_element(By.CSS_SELECTOR, ".left-team-table")
                    except Exception:
                        click_el = root
            if click_el is None:
                return self._fail_bet_flow(
                    row,
                    "Could not find live list row to open match (re-query failed).",
                    cache_key=cache_key,
                )

            self._click_open_match(click_el)
            random_human_pause()

            if not slip.wait_for_match_detail_1x2(
                self.driver, self.log, timeout=12, market_name=market_name
            ):
                return self._fail_bet_flow(
                    row,
                    f"Match page did not load ({market_name} header not visible).",
                    cache_key=cache_key,
                )
            random_human_pause()

            odd_val = slip.read_1x2_selection_odd(
                self.driver,
                selection,
                home_name=row.home,
                away_name=row.away,
                logger=self.log,
                market_name=market_name,
            )
            if odd_val is None:
                return self._fail_bet_flow(
                    row,
                    f"Could not read odd for {row.home} vs {row.away} (market closed / suspended / page issue).",
                    cache_key=cache_key,
                )

            if not slip.odd_in_range(odd_val, self.cfg.minimum_odd, self.cfg.maximum_odd):
                self.log.warning(
                    "[Odd range] %.2f not in [min=%s max=%s] — skipping this fixture.",
                    odd_val,
                    self.cfg.minimum_odd if self.cfg.minimum_odd is not None else "—",
                    self.cfg.maximum_odd if self.cfg.maximum_odd is not None else "—",
                )
                return self._fail_bet_flow(
                    row,
                    f"Odd {odd_val:.2f} outside allowed range for {row.home} vs {row.away}.",
                    cache_key=cache_key,
                )

            self.log.info(
                "[Odd range] OK on match page: %.2f for %s vs %s (%s)",
                odd_val,
                row.home,
                row.away,
                market_name,
            )

            random_human_pause()
            if not slip.click_1x2_outcome(
                self.driver,
                selection,
                home_name=row.home,
                away_name=row.away,
                logger=self.log,
                market_name=market_name,
            ):
                return self._fail_bet_flow(
                    row,
                    f"Could not click 1X2 selection for {row.home} vs {row.away}.",
                    cache_key=cache_key,
                )
            random_human_pause()

            st = self._submit_stake_place_confirm_success(row, selection, stake_amt)
            if st == "abort":
                return self._fail_bet_flow(
                    row,
                    "Odd outside allowed range after price change on bet slip.",
                    cache_key=cache_key,
                )
            if st == "ok":
                self._finalize_successful_bet(row, selection, stake_amt)
                return True

            self.log.info(
                "[Bet] First submit incomplete — toggling 1X2 selection off/on and retrying once."
            )
            random_human_pause()
            if not slip.click_1x2_outcome(
                self.driver,
                selection,
                home_name=row.home,
                away_name=row.away,
                logger=self.log,
                market_name=market_name,
            ):
                return self._fail_bet_flow(
                    row,
                    f"Could not deselect 1X2 for {row.home} vs {row.away}.",
                    cache_key=cache_key,
                )
            random_human_pause()
            if not slip.click_1x2_outcome(
                self.driver,
                selection,
                home_name=row.home,
                away_name=row.away,
                logger=self.log,
                market_name=market_name,
            ):
                return self._fail_bet_flow(
                    row,
                    f"Could not reselect 1X2 for {row.home} vs {row.away}.",
                    cache_key=cache_key,
                )
            random_human_pause()

            st2 = self._submit_stake_place_confirm_success(row, selection, stake_amt)
            if st2 == "abort":
                return self._fail_bet_flow(
                    row,
                    "Odd outside allowed range after price change on bet slip (retry).",
                    cache_key=cache_key,
                )
            if st2 != "ok":
                return self._fail_bet_flow(
                    row,
                    f"Bet failed after retry (stake/place/confirm/success) for {row.home} vs {row.away}.",
                    cache_key=cache_key,
                )
            self._finalize_successful_bet(row, selection, stake_amt)
            return True
        except Exception:
            self.log.error(
                "[Bet flow] Unexpected error while betting on %s vs %s — skipping retries until cache TTL.",
                row.home,
                row.away,
            )
            self.log.debug("_place_bet_sequence detail", exc_info=True)
            try:
                slip.cancel_all_betslips(self.driver, self.log)
            except Exception:
                pass
            self._skipped_cache_add(
                cache_key,
                f"Unexpected error during bet flow ({row.home} vs {row.away}).",
            )
            self.go_live()
            return False

    def process_due_pending(self) -> None:
        now = time.time()
        due = [p for p in self.pending if p.check_after <= now]
        if not due:
            return
        p = due[0]
        self.pending.remove(p)
        self.log.info("Checking result for %s vs %s (placed %.0fs ago)", p.home, p.away, now - p.placed_at)

        try:
            info = bh.search_bet_history(
                self.driver,
                p.home,
                p.away,
                max_pages=self.cfg.bet_history_pages,
                match_ratio=self.cfg.bet_history_team_match_ratio,
                logger=self.log,
            )
        except Exception:
            self.log.error(
                "[Bet history] Could not load or scan bet history pages (network or page error). Will retry this bet later."
            )
            self.log.debug("search_bet_history detail", exc_info=True)
            p.check_after = now + 120
            self.pending.append(p)
            self.go_live()
            return

        if info is None:
            self.log.warning("Bet not found in history; re-queue in 180s")
            p.check_after = now + 180
            p.history_retries += 1
            if p.history_retries < 15:
                self.pending.append(p)
            self.go_live()
            return

        if info.status == "running":
            self.log.info("Still running — re-check in 120s")
            p.check_after = now + 120
            self.pending.append(p)
            self.go_live()
            return

        if info.status == "won":
            if info.stake is not None and info.total_return is not None:
                pnl = info.total_return - info.stake
                self.estimated_balance += pnl
                self.tracked_balance += pnl
                self.wins += 1
                self.log.info(
                    "WON net_pnl=%.2f estimated_balance=%.2f tracked_balance=%.2f wins=%d losses=%d",
                    pnl,
                    self.estimated_balance,
                    self.tracked_balance,
                    self.wins,
                    self.losses,
                )
            else:
                self.log.warning("Won but could not parse stake/return from history row")
        elif info.status == "lost":
            if info.stake is not None and info.total_return is not None:
                pnl = info.total_return - info.stake
            elif info.stake is not None:
                pnl = -float(info.stake)
            else:
                pnl = None
            if pnl is not None:
                self.tracked_balance += pnl
            self.losses += 1
            self.log.info(
                "LOST net_pnl=%s estimated_balance=%.2f (wins-only model) tracked_balance=%.2f wins=%d losses=%d",
                f"{pnl:.2f}" if pnl is not None else "n/a",
                self.estimated_balance,
                self.tracked_balance,
                self.wins,
                self.losses,
            )

        bh.read_header_balance(self.driver, self.log)
        self.go_live()

    def _profit_pct(self, current: float) -> float:
        """% change vs initial_amount_to_use."""
        if self.initial_amount_to_use <= 0:
            return 0.0
        return 100.0 * (current - self.initial_amount_to_use) / self.initial_amount_to_use

    def _win_rate_pct(self) -> float | None:
        """Wins as % of settled bets (wins + losses)."""
        settled = self.wins + self.losses
        if settled <= 0:
            return None
        return 100.0 * self.wins / settled

    @staticmethod
    def _fmt_signed_money(value: float) -> str:
        sign = "+" if value >= 0 else ""
        return f"{sign}{value:.2f}"

    def log_status(self, actual_balance: float | None = None) -> None:
        pct_tr = self._profit_pct(self.tracked_balance)
        pct_est = self._profit_pct(self.estimated_balance)
        profit_amt_est = self.estimated_balance - self.initial_amount_to_use
        profit_amt_tr = self.tracked_balance - self.initial_amount_to_use
        wr = self._win_rate_pct()
        wr_s = f"{wr:.2f}%" if wr is not None else "n/a"
        settled = self.wins + self.losses
        site = actual_balance if actual_balance is not None else "n/a"

        block = "\n".join(
            [
                "[STATUS] ------------------------------------------------------------------",
                f"  initial_capital          = {self.initial_amount_to_use:.2f}",
                f"  ft_window               = ({self.cfg.ft_min_minute_exclusive:.1f}, {self.cfg.ft_max_minute_exclusive:.1f})",
                f"  bet_fulltime            = {self.cfg.bet_fulltime}",
                f"  bet_halftime            = {self.cfg.bet_halftime}",
                f"  ht_window               = ({self.cfg.ht_min_minute_exclusive:.1f}, {self.cfg.ht_max_minute_exclusive:.1f})  (H1 only)",
                f"  minimum_odd              = {self.cfg.minimum_odd:.2f}"
                if self.cfg.minimum_odd is not None
                else "  minimum_odd              = (none — no lower bound)",
                f"  maximum_odd              = {self.cfg.maximum_odd:.2f}"
                if self.cfg.maximum_odd is not None
                else "  maximum_odd              = (none — no upper bound)",
                f"  max_abs_goal_diff_1x2  = {self.cfg.max_abs_goal_diff_for_1x2}  (skip if |H-A| > this)",
                f"  ft_max_total_goals     = {self.cfg.ft_max_total_goals}  (skip if H+A > this)",
                f"  ht_max_total_goals     = {self.cfg.ht_max_total_goals}  (skip if H+A > this)",
                f"  excluded_competitions  = {len(self.cfg.excluded_competitions)}  (DeepSeek mapping cached)",
                f"  deepseek_enabled       = {self.cfg.deepseek_enabled}  (model {self.cfg.deepseek_model})",
                f"  bet_history_match_ratio = {self.cfg.bet_history_team_match_ratio:.2f}  (fuzzy name match in history)",
                "  -- estimated (stake sizing: wins only; no deduction on loss) ------------",
                f"  estimated_balance      = {self.estimated_balance:.2f}",
                f"  total_profit_est       = {self._fmt_signed_money(profit_amt_est)}  (balance - initial)",
                f"  profit_pct_est         = {self._fmt_signed_money(pct_est)}%",
                "  -- tracked (logging balance: +win / -loss net) ---------------------------",
                f"  tracked_balance        = {self.tracked_balance:.2f}",
                f"  total_profit_tracked   = {self._fmt_signed_money(profit_amt_tr)}  (balance - initial)",
                f"  profit_pct_tracked     = {self._fmt_signed_money(pct_tr)}%",
                "  -- next stake ------------------------------------------------------------",
                f"  stake_next             = {self.stake_per_match():.2f}  (= estimated_balance / {self.cfg.max_simultaneous_matches})",
                "  -- settled record --------------------------------------------------------",
                f"  wins                   = {self.wins}",
                f"  losses                 = {self.losses}",
                f"  settled_total          = {settled}",
                f"  win_rate               = {wr_s}  (wins / settled)",
                "  -- pending / site --------------------------------------------------------",
                f"  pending_bets           = {len(self.pending)}",
                f"  site_balance           = {site}",
                "[STATUS] ------------------------------------------------------------------",
            ]
        )
        self.log.info("%s", block)

    def step(self) -> None:
        self._sweep_cache()
        self.go_live()
        bal = bh.read_header_balance(self.driver, self.log)
        self.log_status(actual_balance=bal)

        self.try_place_bets()
        self.process_due_pending()

        time.sleep(self.cfg.poll_sleep_seconds)

    def run_forever(self) -> None:
        self.log.info("Starting live betting bot | config=%s", dataclasses.asdict(self.cfg))
        self.login()
        time.sleep(4)
        try:
            while True:
                self.step()
        except KeyboardInterrupt:
            self.log.info("Interrupted by user")


def main():
    cfg = LiveBettingConfig(
        amount_to_use=3000.0,
        max_simultaneous_matches=30,
        only_bet_draws=False,
        only_bet_zero_zero_score=False,
    )
    bot = LiveBettingBot(cfg)
    bot.run_forever()


if __name__ == "__main__":
    main()
