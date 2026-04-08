from __future__ import annotations

import dataclasses
import logging
import math
import threading
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
    # Incremental staking (one bet at a time). When enabled, max_simultaneous_matches
    # becomes the maximum number of escalating trials within a "round".
    incremental: bool = False
    # Expected average odd used for stake sizing in incremental mode (must be > 1.0).
    average_odd: float = 1.17
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
    # Winning mode: track one match per thread, bet current leader (or draw if level).
    # When the bet is no longer winning, cash out and re-bet the new winning outcome with
    # stake sized to recover this match's net loss plus the original target profit.
    winning: bool = False
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
    sporty_phone: str | None = None
    sporty_password: str | None = None
    # Force a hard refresh (driver.get) periodically to reduce stale SPA state / session glitches.
    hard_refresh_seconds: float = 600.0
    # Shared-browser threading: when True, each thread's "betting turn" starts with a full
    # driver.get(live_url) even if already on live list (helps recover from silent logout).
    thread_turn_full_reload: bool = True
    # Global throttle for thread-turn reload (shared-browser mode): only reload if the last
    # reload was at least this many seconds ago.
    thread_turn_reload_min_interval_seconds: float = 300.0
    # Multi-threading: one browser (Selenium session) per thread; shared locks + caches below.
    num_threads: int = 1
    # Logging: override the default base name (live_betting / live_betting_tN).
    # If set, log files become "<log_base_name>.log" (or with _tN suffix when multi-threaded).
    log_base_name: str | None = None
    # If False, do not suffix log file with YYYYMMDD.
    log_include_date: bool = True


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
    inc_trial: int | None = None
    # Used to group multiple pending bets per match (winning mode).
    match_group_key: str | None = None
    # When we successfully click Cashout+Confirm for a specific pending bet, we can often read the
    # cashout return amount immediately from the cashout card. Store it here so winning-mode stake
    # sizing can treat the outstanding exposure as (stake - cashout_return) instead of full stake,
    # even before bet-history reflects the settlement.
    cashout_expected_return: float | None = None


@dataclass
class BetCacheEntry:
    match_key: str
    expires_at: float


@dataclass
class SharedBettingContext:
    """
    Shared across LiveBettingBot threads: one browser per thread, but
    - only one thread at a time may scan live list + place bet + return to live list
    - only one thread at a time may navigate bet history; results are cached for all threads
    - bet/skip fixture caches are shared so threads do not repeat failed/successful fixtures
    - balances are shared (same account) when using multiple threads
    """

    cfg: LiveBettingConfig
    # Global UI lock: Selenium driver is NOT thread-safe. With a shared browser,
    # *all* driver interactions must be serialized through this lock.
    ui_lock: threading.Lock = field(default_factory=threading.Lock)
    # One thread at a time may navigate bet history; results are cached for all threads.
    result_check_lock: threading.Lock = field(default_factory=threading.Lock)
    result_cache_lock: threading.Lock = field(default_factory=threading.Lock)
    cache_lock: threading.Lock = field(default_factory=threading.Lock)
    balance_lock: threading.Lock = field(default_factory=threading.Lock)
    league_lock: threading.Lock = field(default_factory=threading.Lock)
    driver_ready: threading.Event = field(default_factory=threading.Event)
    driver: object | None = None

    # Settled results keyed by bet_history.result_cache_key_with_stake(home, away, stake) (or base key).
    # Omit "running" for cache longevity.
    result_cache: dict[str, bh.SettledBetInfo] = field(default_factory=dict)
    _bet_cache: list[BetCacheEntry] = field(default_factory=list)
    _skipped_cache: list[BetCacheEntry] = field(default_factory=list)

    bots: list["LiveBettingBot"] = field(default_factory=list)

    estimated_balance: float = field(init=False)
    tracked_balance: float = field(init=False)
    wins: int = 0
    losses: int = 0
    cashout_settles: int = 0
    loss_pool: float = 0.0
    next_hard_refresh_at: float = field(default_factory=lambda: time.monotonic() + 600.0)
    last_thread_turn_reload_at: float = field(default_factory=time.monotonic)

    def __post_init__(self) -> None:
        self.estimated_balance = float(self.cfg.amount_to_use)
        self.tracked_balance = float(self.cfg.amount_to_use)
        self.next_hard_refresh_at = time.monotonic() + float(self.cfg.hard_refresh_seconds)
        self.last_thread_turn_reload_at = time.monotonic()

    def register(self, bot: "LiveBettingBot") -> None:
        self.bots.append(bot)

    def collect_due_pending_triples(self, now: float) -> list[tuple[str, str, float | None, str]]:
        """(home, away, stake, cache_key) for all threads' pending bets that are due, deduped by key."""
        seen: set[str] = set()
        out: list[tuple[str, str, float | None, str]] = []
        for bot in self.bots:
            for p in bot.pending:
                if p.check_after <= now:
                    k = bh.result_cache_key_with_stake(p.home, p.away, p.stake)
                    if k not in seen:
                        seen.add(k)
                        out.append((p.home, p.away, p.stake, k))
        return out


@dataclass
class WinningMatchState:
    """Per-thread state for winning-mode progression on a single match."""
    home: str
    away: str
    bet_tag: str  # "FT" or "HT"
    market_name: str
    profit_target: float  # desired net profit for this match when it finally wins
    net_before: float = 0.0  # cumulative net: returns - stakes (negative means loss)
    last_selection: str | None = None
    has_bet_before: bool = False
    # The live list is a dynamic SPA and our parser deliberately skips some transient rows
    # (e.g. HT break). Track consecutive "not found" cycles so we can tolerate short misses
    # without unlocking the match.
    missing_live_count: int = 0


class LiveBettingBot(SportyBetLoginBot):
    """
    Live football 1X2 betting loop using SportyBetLoginBot for auth and driver.
    Single-threaded by default; with SharedBettingContext + num_threads>1, one browser per thread
    with coordinated locks for live-list betting and bet-history checks.
    """

    def __init__(
        self,
        config: LiveBettingConfig | None = None,
        *,
        shared: SharedBettingContext | None = None,
        thread_id: int = 0,
        clear_log_on_start: bool = True,
    ):
        cfg = config or LiveBettingConfig()
        super().__init__(
            cfg.live_url,
            phone=cfg.sporty_phone,
            password=cfg.sporty_password,
        )
        self.cfg = cfg
        self.shared = shared
        self.thread_id = int(thread_id)
        base = (cfg.log_base_name or "").strip() or None
        if base:
            log_name = f"{base}_t{self.thread_id}" if shared is not None else base
        else:
            log_name = f"live_betting_t{self.thread_id}" if shared is not None else "live_betting"
        self.log = _bet_log.setup_betting_logger(
            os.path.join(os.path.dirname(__file__), "logs"),
            name=log_name,
            clear_file_on_start=clear_log_on_start and self.thread_id == 0,
            include_date=bool(cfg.log_include_date),
        )

        # Initial capital (fixed) — used for profit % calculations in logs
        self.initial_amount_to_use: float = float(cfg.amount_to_use)
        # Stake sizing: always estimated_balance / max_simultaneous_matches (starts equal to initial)
        # When `shared` is set, balances live on SharedBettingContext (same wallet).
        if shared is None:
            self.estimated_balance: float = self.initial_amount_to_use
            self.tracked_balance: float = self.initial_amount_to_use
        else:
            self.estimated_balance = float(shared.estimated_balance)
            self.tracked_balance = float(shared.tracked_balance)
        self.wins: int = 0
        self.losses: int = 0
        self.cashout_settles: int = 0
        self.loss_pool: float = 0.0
        self.pending: list[PendingBet] = []
        self._winning: WinningMatchState | None = None
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
        self._last_site_balance: float | None = None

        # Incremental staking state (one bet at a time, escalating by trial on loss)
        self._inc_round_base_stake: float | None = None
        self._inc_round_spent: float = 0.0
        self._inc_trial_next: int = 1

        if shared is not None:
            shared.register(self)
        self._next_hard_refresh_at: float = time.monotonic() + float(self.cfg.hard_refresh_seconds)

    def _effective_estimated_balance(self) -> float:
        if self.shared is not None:
            with self.shared.balance_lock:
                return float(self.shared.estimated_balance)
        return float(self.estimated_balance)

    def _effective_tracked_balance(self) -> float:
        if self.shared is not None:
            with self.shared.balance_lock:
                return float(self.shared.tracked_balance)
        return float(self.tracked_balance)

    def _loss_pool_value(self) -> float:
        if self.shared is not None:
            with self.shared.balance_lock:
                return float(self.shared.loss_pool)
        return float(self.loss_pool)

    def _loss_pool_add(self, amount: float) -> None:
        """Add positive amount to shared loss pool."""
        try:
            a = float(amount)
        except Exception:
            return
        if a <= 0:
            return
        if self.shared is not None:
            with self.shared.balance_lock:
                self.shared.loss_pool += a
        else:
            self.loss_pool += a

    def _loss_pool_repay(self, amount: float) -> float:
        """Repay as much as possible from pool; returns actual repaid."""
        try:
            a = float(amount)
        except Exception:
            return 0.0
        if a <= 0:
            return 0.0
        if self.shared is not None:
            with self.shared.balance_lock:
                rep = min(float(self.shared.loss_pool), a)
                self.shared.loss_pool -= rep
                return float(rep)
        rep = min(float(self.loss_pool), a)
        self.loss_pool -= rep
        return float(rep)

    # --- incremental staking ---

    def _inc_max_trials(self) -> int:
        return max(1, int(self.cfg.max_simultaneous_matches))

    def _inc_avg_odd(self) -> float:
        o = float(self.cfg.average_odd)
        if o <= 1.0:
            raise ValueError(f"average_odd must be > 1.0 (got {o})")
        return o

    @staticmethod
    def _inc_total_exposure_factor(avg_odd: float, trials: int) -> float:
        """
        Total exposure (sum of stakes) for incremental trials when base_stake=1.
        This lets us pick a safe base stake such that base * factor ~= bankroll.
        """
        o = float(avg_odd)
        n = max(1, int(trials))
        if o <= 1.0:
            return float("inf")
        profit_unit = o - 1.0  # with base=1, trial-1 profit target
        spent = 0.0
        for k in range(1, n + 1):
            if k == 1:
                stake_k = 1.0
            else:
                stake_k = (spent + profit_unit * k) / (o - 1.0)
            spent += stake_k
        return spent

    def _inc_reset_round(self) -> None:
        """Start a new round from trial 1 using current bankroll."""
        self._inc_round_spent = 0.0
        self._inc_trial_next = 1
        self._inc_round_base_stake = None

    def _inc_ensure_round_base(self) -> float:
        if self._inc_round_base_stake is not None:
            return float(self._inc_round_base_stake)
        avg_odd = self._inc_avg_odd()
        trials = self._inc_max_trials()
        factor = self._inc_total_exposure_factor(avg_odd, trials)
        bankroll = float(self._effective_estimated_balance())
        if self._last_site_balance is not None:
            bankroll = min(bankroll, float(math.floor(float(self._last_site_balance))))
        if factor <= 0 or factor == float("inf"):
            base = 10.0
        else:
            base = bankroll / factor
        base = max(10.0, round(base, 2))
        self._inc_round_base_stake = base
        # If bankroll is too small for min stake * factor, surface it in logs.
        if factor != float("inf") and base * factor > bankroll + 1e-6:
            self.log.warning(
                "[Incremental] Bankroll %.2f is below required exposure %.2f for %d trials at avg_odd=%.2f "
                "(min base stake=%.2f). Strategy may overexpose.",
                bankroll,
                base * factor,
                trials,
                avg_odd,
                base,
            )
        return float(self._inc_round_base_stake)

    def _inc_next_stake_amount(self) -> float:
        """
        Stake sizing:
        - trial 1: base stake
        - trial k>1: stake_k = (spent_so_far + (profit_unit * k)) / (avg_odd - 1)
          where profit_unit = base_stake * (avg_odd - 1)
        This yields net profit = profit_unit * k if the k-th bet wins.
        """
        base = self._inc_ensure_round_base()
        avg_odd = self._inc_avg_odd()
        k = int(self._inc_trial_next)
        if k <= 1:
            return float(base)
        profit_unit = base * (avg_odd - 1.0)
        stake_k = (float(self._inc_round_spent) + profit_unit * k) / (avg_odd - 1.0)
        return max(10.0, round(stake_k, 2))

    def _inc_next_stake_amount_for_current_odd(self, current_odd: float) -> float | None:
        """
        Trial 2+ sizing using the *current* odd right before placing the bet.
        Keeps the same profit target schedule (based on base stake + average_odd),
        but uses (current_odd - 1) in the denominator so the win is more likely to
        cover prior losses at the actual price.
        """
        try:
            o = float(current_odd)
        except Exception:
            return None
        if o <= 1.0:
            return None
        base = self._inc_ensure_round_base()
        k = int(self._inc_trial_next)
        if k <= 1:
            return float(base)
        # Profit target per trial is still anchored to the expected average odd.
        avg_odd = self._inc_avg_odd()
        profit_unit = base * (avg_odd - 1.0)
        stake_k = (float(self._inc_round_spent) + profit_unit * k) / (o - 1.0)
        return max(10.0, round(float(stake_k), 2))

    # --- navigation ---

    def _maybe_hard_refresh(self) -> None:
        """
        Periodic full reload to combat stale SPA state and force session re-check.
        In threaded mode this is coordinated by SharedBettingContext so only one thread does it.
        """
        interval = float(self.cfg.hard_refresh_seconds)
        if interval <= 0:
            return
        now = time.monotonic()

        if self.shared is not None:
            # Only safe to call this while holding shared.ui_lock.
            if now < float(self.shared.next_hard_refresh_at):
                return
            self.shared.next_hard_refresh_at = now + interval
        else:
            if now < float(self._next_hard_refresh_at):
                return
            self._next_hard_refresh_at = now + interval

        live = (self.cfg.live_url or "").strip()
        self.log.info("[Live list] Hard refresh now (interval %.0fs).", interval)
        try:
            self.load_url(live)
            time.sleep(2.0)
        except Exception:
            self.log.debug("hard refresh detail", exc_info=True)

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

    def _thread_turn_reload_live(self) -> None:
        """Full reload of live list at start of a thread turn (shared-browser mode)."""
        try:
            if not bool(self.cfg.thread_turn_full_reload):
                return
        except Exception:
            return
        # Throttle reloads globally in shared-browser mode.
        if self.shared is not None:
            try:
                min_interval = float(self.cfg.thread_turn_reload_min_interval_seconds)
            except Exception:
                min_interval = 300.0
            if min_interval < 0:
                min_interval = 0.0
            now = time.monotonic()
            last = float(self.shared.last_thread_turn_reload_at)
            if now - last < min_interval:
                return
            self.shared.last_thread_turn_reload_at = now
        live = (self.cfg.live_url or "").strip()
        if not live:
            return
        self.log.info("[Live list] Thread-turn full reload.")
        try:
            self.load_url(live)
            time.sleep(2.0)
        except Exception:
            self.log.debug("thread-turn reload detail", exc_info=True)

    def _update_league_mapping_from_dom(self) -> None:
        """Read all league rows currently on the page and ask DeepSeek to map unknown ones."""
        def _run() -> None:
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

        if self.shared is not None:
            with self.shared.league_lock:
                _run()
        else:
            _run()

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
        if self.shared is not None:
            with self.shared.cache_lock:
                self.shared._bet_cache = [c for c in self.shared._bet_cache if c.expires_at > now]
                self.shared._skipped_cache = [
                    c for c in self.shared._skipped_cache if c.expires_at > now
                ]
        else:
            self._cache = [c for c in self._cache if c.expires_at > now]
            self._skipped_cache = [c for c in self._skipped_cache if c.expires_at > now]

    def _cache_has(self, key: str) -> bool:
        now = time.time()
        if self.shared is not None:
            with self.shared.cache_lock:
                return any(
                    c.match_key == key and c.expires_at > now for c in self.shared._bet_cache
                )
        return any(c.match_key == key and c.expires_at > now for c in self._cache)

    def _skipped_cache_has(self, key: str) -> bool:
        now = time.time()
        if self.shared is not None:
            with self.shared.cache_lock:
                return any(
                    c.match_key == key and c.expires_at > now for c in self.shared._skipped_cache
                )
        return any(c.match_key == key and c.expires_at > now for c in self._skipped_cache)

    def _match_cache_blocked(self, key: str) -> bool:
        """Recently bet successfully or recently skipped (failed attempt)."""
        return self._cache_has(key) or self._skipped_cache_has(key)

    def _cache_add(self, key: str) -> None:
        ent = BetCacheEntry(key, time.time() + self.cfg.cache_ttl_seconds)
        if self.shared is not None:
            with self.shared.cache_lock:
                self.shared._bet_cache.append(ent)
        else:
            self._cache.append(ent)

    def _skipped_cache_add(self, key: str, reason: str) -> None:
        ent = BetCacheEntry(key, time.time() + self.cfg.cache_ttl_seconds)
        if self.shared is not None:
            with self.shared.cache_lock:
                self.shared._skipped_cache.append(ent)
        else:
            self._skipped_cache.append(ent)
        self.log.info(
            "[Skip cache] %s — will not retry this fixture until cache TTL (same as bet cache).",
            reason,
        )

    def stake_per_match(self) -> float:
        """
        Stake sizing:
        - default: estimated_balance / max_simultaneous_matches (min stake 10)
        - incremental: one bet at a time, escalating by trial on loss (uses average_odd)
        """
        if self.cfg.incremental:
            stake = self._inc_next_stake_amount()
        else:
            n = max(1, self.cfg.max_simultaneous_matches)
            stake = max(10.0, round(self._effective_estimated_balance() / n, 2))

        # Always cap stake by available header balance (rounded down).
        if self._last_site_balance is not None:
            cap = float(math.floor(float(self._last_site_balance)))
            stake = min(stake, cap)
        return max(0.0, round(float(stake), 2))

    @staticmethod
    def _winning_selection_from_score(hg: int, ag: int) -> str:
        if hg > ag:
            return "home"
        if ag > hg:
            return "away"
        return "draw"

    def _winning_compute_next_stake(self, current_odd: float, state: WinningMatchState) -> float | None:
        """
        Choose stake so that if the bet wins, net_after >= profit_target.
        net_after = net_before + stake*(odd-1)
        """
        try:
            o = float(current_odd)
        except Exception:
            return None
        if o <= 1.0:
            return None
        # Global loss sharing: each thread aims to recover its share of the pool
        # *in addition* to its own match profit target.
        threads = max(1, int(self.cfg.num_threads))
        loss_share = self._loss_pool_value() / float(threads)

        # Include outstanding exposure for this match group (pending stakes not yet reflected
        # in state.net_before because bet history settlement may lag).
        outstanding_loss = 0.0
        try:
            group = f"{state.home}|{state.away}|{state.bet_tag}"
            pending_src = (
                [p for b in self.shared.bots for p in b.pending] if self.shared is not None else list(self.pending)
            )
            for p in pending_src:
                try:
                    if (p.match_group_key or "") != group:
                        continue
                    exp_ret = float(p.cashout_expected_return) if p.cashout_expected_return is not None else None
                    if exp_ret is not None and exp_ret >= 0:
                        outstanding_loss += max(0.0, float(p.stake) - exp_ret)
                    else:
                        outstanding_loss += max(0.0, float(p.stake))
                except Exception:
                    continue
        except Exception:
            outstanding_loss = 0.0

        required = (
            float(state.profit_target)
            - float(state.net_before)
            + float(loss_share)
            + float(outstanding_loss)
        )
        if required < 0:
            required = 0.0
        stake = required / (o - 1.0) if (o - 1.0) > 0 else None
        if stake is None:
            return None
        stake = max(10.0, round(float(stake), 2))
        # Cap by header balance (rounded down)
        if self._last_site_balance is not None:
            cap = float(math.floor(float(self._last_site_balance)))
            stake = min(stake, cap)
        return float(stake)

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

        # Winning mode: each thread manages a single match until it ends.
        if self.cfg.winning:
            return self._winning_try_manage_match(rows, candidates)

        # Incremental mode: only one bet at a time (must wait for result).
        if self.cfg.incremental and self.pending:
            self.log.info(
                "[Incremental] Pending bet exists (%d). Waiting for result before placing next.",
                len(self.pending),
            )
            return 0

        for row in candidates:
            # Popups (e.g. "YOU WON") can appear anytime and block clicks.
            try:
                slip.dismiss_winning_popup(self.driver, self.log)
            except Exception:
                pass
            # Always check Cashout before each bet attempt (so cashout is not only once per step).
            try:
                wanted: dict[str, str] = {}
                if self.shared is not None:
                    for b in self.shared.bots:
                        for p in b.pending:
                            k1 = bh.result_cache_key(p.home, p.away)
                            k2 = bh.result_cache_key(p.away, p.home)
                            wanted[k1] = p.selection
                            wanted[k2] = p.selection
                else:
                    for p in self.pending:
                        k1 = bh.result_cache_key(p.home, p.away)
                        k2 = bh.result_cache_key(p.away, p.home)
                        wanted[k1] = p.selection
                        wanted[k2] = p.selection
                slip.cashout_scan_and_execute(
                    self.driver,
                    wanted,
                    logger=self.log,
                    max_pages=5,
                )
            except Exception:
                self.log.debug("cashout_scan_and_execute (per-bet) detail", exc_info=True)

            pending_cap = 1 if self.cfg.incremental else int(self.cfg.max_simultaneous_matches)
            if len(self.pending) >= pending_cap:
                self.log.info("Max pending bets reached; stop placing")
                break
            sel = pick_1x2_selection(
                row.home_goals,
                row.away_goals,
                only_zero_zero=self.cfg.only_bet_zero_zero_score,
                only_draws=self.cfg.only_bet_draws,
            )
            if sel is None:
                continue
            # Incremental mode: enforce max trial count (same field used as max_trials).
            if self.cfg.incremental and self._inc_trial_next > self._inc_max_trials():
                self.log.warning(
                    "[Incremental] Max trials exceeded (%d). Resetting round and not placing further bets this step.",
                    self._inc_max_trials(),
                )
                self._inc_reset_round()
                break

            stake_amt = self.stake_per_match()
            if stake_amt < 10.0:
                self.log.warning(
                    "Insufficient balance for min stake (stake_cap=%.2f). Skipping bet placement this step.",
                    stake_amt,
                )
                break
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
                self._effective_estimated_balance(),
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

    def _winning_try_manage_match(self, rows: list[LiveListRow], candidates: list[LiveListRow]) -> int:
        """
        Winning mode loop:
        - If no current match, pick one candidate and start it.
        - If current match is still live, ensure we have a bet on the current winning outcome.
          If the last selection is no longer winning, cashout and re-bet current winner.
        - If match is gone from live list, treat as ended; clear current and let next loop pick a new one.
        """
        # Winning mode constraint: do not start managing a new match while this thread still has
        # any unresolved pending bet(s). This prevents the bot from accumulating multiple pending
        # bets across different matches when a locked match temporarily disappears from parsing.
        try:
            if self.pending:
                self.log.info(
                    "[Winning] Pending exists (%d) — not selecting a new match this cycle.",
                    len(self.pending),
                )
                return 0
        except Exception:
            pass

        if self._winning is None:
            if not candidates:
                return 0
            r = candidates[0]
            bet_market = getattr(r, "_bet_market_name", "1X2")
            bet_tag = getattr(r, "_bet_tag", "FT")
            self._winning = WinningMatchState(
                home=r.home,
                away=r.away,
                bet_tag=str(bet_tag),
                market_name=str(bet_market),
                profit_target=0.0,
            )
            self.log.info(
                "[Winning] Locked match: %s vs %s tag=%s market=%s",
                r.home,
                r.away,
                bet_tag,
                bet_market,
            )

        st = self._winning
        if st is None:
            return 0

        # Find the match row (if missing, match ended).
        live_row = None
        for r in rows:
            try:
                if r.home == st.home and r.away == st.away:
                    live_row = r
                    break
            except Exception:
                continue
        if live_row is None:
            # If we already have a pending bet for this locked match, do NOT unlock immediately.
            # Common case: HT break rows are skipped by the parser (game_id == HT), or the row
            # is temporarily missing while the live list updates.
            try:
                pending_src = (
                    [p for b in self.shared.bots for p in b.pending] if self.shared is not None else list(self.pending)
                )
                has_pending_for_locked = any(
                    (p.home == st.home and p.away == st.away) or (p.home == st.away and p.away == st.home)
                    for p in pending_src
                )
            except Exception:
                has_pending_for_locked = False

            st.missing_live_count = int(getattr(st, "missing_live_count", 0)) + 1
            if has_pending_for_locked:
                self.log.info(
                    "[Winning] Locked match missing from parsed live list (pending exists); holding lock: %s vs %s (missing_count=%d)",
                    st.home,
                    st.away,
                    st.missing_live_count,
                )
                return 0

            self.log.info(
                "[Winning] Match ended (not in parsed live list): %s vs %s (missing_count=%d pending=%s)",
                st.home,
                st.away,
                st.missing_live_count,
                has_pending_for_locked,
            )
            self._winning = None
            return 0
        else:
            st.missing_live_count = 0

        sel = self._winning_selection_from_score(live_row.home_goals, live_row.away_goals)

        # Guard: if we already have a pending bet for this match + current winning selection,
        # do not place another identical bet (prevents repeated re-betting while score is unchanged).
        try:
            group_key = f"{live_row.match_key}|{st.bet_tag}"
            pending_src = (
                [p for b in self.shared.bots for p in b.pending] if self.shared is not None else list(self.pending)
            )
            already_pending = any(
                (p.match_group_key == group_key and (p.selection or "").strip().lower() == (sel or "").strip().lower())
                for p in pending_src
            )
            if already_pending:
                self.log.info(
                    "[Winning] Skip re-bet (already pending): %s vs %s sel=%s group=%s",
                    st.home,
                    st.away,
                    sel,
                    group_key,
                )
                st.last_selection = sel
                return 0
        except Exception:
            # If this check fails for any reason, fall back to previous behavior.
            self.log.debug("[Winning] pending dedupe check detail", exc_info=True)

        # If we have a previous selection and it is no longer winning, cash out.
        if st.last_selection is not None and st.last_selection != sel:
            try:
                # Disambiguate cashout by team + stake. If multiple cashouts exist for the same
                # teams (e.g. repeated bets), this prevents cashing out the "new" bet by mistake.
                wanted_stake = None
                try:
                    pending_src = (
                        [p for b in self.shared.bots for p in b.pending] if self.shared is not None else list(self.pending)
                    )
                    # Prefer the most recently placed pending bet for the *previous* selection.
                    cand = [
                        p
                        for p in pending_src
                        if (
                            (p.home == st.home and p.away == st.away) or (p.home == st.away and p.away == st.home)
                        )
                        and (p.selection or "").strip().lower() == (st.last_selection or "").strip().lower()
                    ]
                    if cand:
                        cand.sort(key=lambda x: float(x.placed_at or 0.0), reverse=True)
                        wanted_stake = float(cand[0].stake)
                except Exception:
                    wanted_stake = None

                wanted = {
                    bh.result_cache_key(st.home, st.away): (st.last_selection, wanted_stake),
                    bh.result_cache_key(st.away, st.home): (st.last_selection, wanted_stake),
                }
                self.log.info(
                    "[Winning] Selection changed %s -> %s for %s vs %s; attempting cashout.",
                    st.last_selection,
                    sel,
                    st.home,
                    st.away,
                )
                actions = slip.cashout_scan_and_execute_detailed(self.driver, wanted, logger=self.log, max_pages=5)
                # Mark expected returns on pending bets so stake sizing can account for (stake - return)
                # even before bet-history settles.
                if actions:
                    try:
                        pending_src2 = (
                            [p for b in self.shared.bots for p in b.pending] if self.shared is not None else list(self.pending)
                        )
                        for act in actions:
                            for p in pending_src2:
                                try:
                                    if (
                                        ((p.home == act.home and p.away == act.away) or (p.home == act.away and p.away == act.home))
                                        and abs(float(p.stake) - float(act.stake)) < 0.01
                                    ):
                                        p.cashout_expected_return = float(act.cashout_return)
                                except Exception:
                                    continue
                    except Exception:
                        pass
            except Exception:
                self.log.debug("[Winning] cashout attempt detail", exc_info=True)

        # Place/replace bet on current winning selection.
        bet_key = f"{live_row.match_key}|{st.bet_tag}"
        stake_guess = self.stake_per_match()
        ok = self._place_winning_bet_sequence(live_row, sel, stake_guess, st.market_name, bet_key, st)
        if ok:
            return 1
        return 0

    def _place_winning_bet_sequence(
        self,
        row: LiveListRow,
        selection: str,
        stake_amt: float,
        market_name: str,
        cache_key: str,
        state: WinningMatchState,
    ) -> bool:
        """
        Similar to _place_bet_sequence but:
        - selection is the current winning outcome
        - stake is computed from match ledger to recover net_before + profit_target
        - do not skip-cache failures after first successful bet on this match
        """
        try:
            try:
                slip.dismiss_winning_popup(self.driver, self.log)
            except Exception:
                pass
            self._scroll_live_list_for_lazy_rows()
            random_human_pause()
            click_el = self._find_live_list_click_target(row.home, row.away)
            if click_el is None:
                self.log.info(
                    "[Winning] Could not find click target for %s vs %s on live list (row may have updated).",
                    row.home,
                    row.away,
                )
                return False
            self._click_open_match(click_el)
            random_human_pause()
            if not slip.wait_for_match_detail_1x2(self.driver, self.log, timeout=12, market_name=market_name):
                self.log.info(
                    "[Winning] Match page did not show market header %s for %s vs %s.",
                    market_name,
                    row.home,
                    row.away,
                )
                return False

            odd_val = slip.read_1x2_selection_odd(
                self.driver,
                selection,
                home_name=row.home,
                away_name=row.away,
                logger=self.log,
                market_name=market_name,
            )
            if odd_val is None:
                self.log.info(
                    "[Winning] Could not read odd for %s vs %s selection=%s market=%s.",
                    row.home,
                    row.away,
                    selection,
                    market_name,
                )
                return False
            if not slip.odd_in_range(odd_val, self.cfg.minimum_odd, self.cfg.maximum_odd):
                self.log.info(
                    "[Winning] Odd %.2f outside range [min=%s max=%s] for %s vs %s.",
                    float(odd_val),
                    self.cfg.minimum_odd if self.cfg.minimum_odd is not None else "—",
                    self.cfg.maximum_odd if self.cfg.maximum_odd is not None else "—",
                    row.home,
                    row.away,
                )
                return False

            # Initialize per-match profit target on first bet.
            if not state.has_bet_before or state.profit_target <= 0:
                base = max(10.0, float(stake_amt))
                state.profit_target = max(1.0, round(base * (float(odd_val) - 1.0), 2))
                self.log.info(
                    "[Winning] Profit target set for %s vs %s: %.2f (base stake %.2f @ odd %.2f)",
                    state.home,
                    state.away,
                    state.profit_target,
                    base,
                    float(odd_val),
                )

            stake2 = self._winning_compute_next_stake(float(odd_val), state)
            if stake2 is None or float(stake2) < 10.0:
                self.log.info(
                    "[Winning] Stake compute failed for %s vs %s odd=%.2f net_before=%.2f profit_target=%.2f pool=%.2f.",
                    state.home,
                    state.away,
                    float(odd_val),
                    float(state.net_before),
                    float(state.profit_target),
                    self._loss_pool_value(),
                )
                return False

            self.log.info(
                "[Winning] Bet %s vs %s sel=%s odd=%.2f stake=%.2f net_before=%.2f target_profit=%.2f",
                state.home,
                state.away,
                selection,
                float(odd_val),
                float(stake2),
                float(state.net_before),
                float(state.profit_target),
            )

            if not slip.click_1x2_outcome(
                self.driver,
                selection,
                home_name=row.home,
                away_name=row.away,
                logger=self.log,
                market_name=market_name,
            ):
                self.log.info(
                    "[Winning] Failed to click outcome for %s vs %s selection=%s market=%s.",
                    row.home,
                    row.away,
                    selection,
                    market_name,
                )
                return False

            st = self._submit_stake_place_confirm_success(row, selection, float(stake2))
            if st != "ok":
                self.log.info(
                    "[Winning] Bet submit flow not ok=%s for %s vs %s selection=%s stake=%.2f.",
                    st,
                    row.home,
                    row.away,
                    selection,
                    float(stake2),
                )
                if not state.has_bet_before:
                    self._skipped_cache_add(cache_key, "Winning mode: initial bet flow failed.")
                try:
                    slip.cancel_all_betslips(self.driver, self.log)
                except Exception:
                    pass
                self.go_live()
                return False

            details = slip.read_success_dialog_details(self.driver, self.log)
            slip.click_success_ok(self.driver, self.log)
            now = time.time()
            self.pending.append(
                PendingBet(
                    home=row.home,
                    away=row.away,
                    booking_code=details.get("booking_code"),
                    stake=float(stake2),
                    potential_win=details.get("potential_win"),
                    placed_at=now,
                    check_after=now + self.cfg.result_wait_seconds,
                    selection=selection,
                    inc_trial=None,
                    match_group_key=f"{row.match_key}|{state.bet_tag}",
                )
            )
            state.last_selection = selection
            state.has_bet_before = True
            self.log.info(
                "[Winning] Pending added %s vs %s sel=%s stake=%.2f | pending_now=%d",
                row.home,
                row.away,
                selection,
                float(stake2),
                sum(len(b.pending) for b in self.shared.bots) if self.shared is not None else len(self.pending),
            )
            self.go_live()
            return True
        except Exception:
            self.log.debug("_place_winning_bet_sequence detail", exc_info=True)
            try:
                slip.cancel_all_betslips(self.driver, self.log)
            except Exception:
                pass
            self.go_live()
            return False

    def _fail_bet_flow(self, row: LiveListRow, public_reason: str, *, cache_key: str) -> bool:
        """Log, add to skip cache, clear slip, return to live. Always returns False."""
        # If logged out, do not cache this fixture as "failed bet" (it was a session issue).
        try:
            if self.is_header_login_form_visible():
                self.log.warning(
                    "[Session] Header login visible during bet flow — treating as logout issue; not skip-caching this fixture."
                )
                try:
                    self._relogin_if_header_login_visible()
                except Exception:
                    pass
                self.go_live()
                return False
        except Exception:
            pass

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
                inc_trial=(int(self._inc_trial_next) if self.cfg.incremental else None),
            )
        )
        if self.cfg.incremental:
            # Record actual stake used for this round (needed for next-trial sizing).
            self._inc_round_spent += float(stake_amt)
        try:
            pool_pending = (
                sum(len(b.pending) for b in self.shared.bots)
                if self.shared is not None
                else len(self.pending)
            )
            self.log.info(
                "[Pending] Added %s vs %s selection=%s | pending_now=%d",
                row.home,
                row.away,
                selection,
                pool_pending,
            )
        except Exception:
            pass
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
            try:
                slip.dismiss_winning_popup(self.driver, self.log)
            except Exception:
                pass
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

            # Incremental: for trial 2+, recompute stake using the *current* odd on this match page.
            if self.cfg.incremental and int(self._inc_trial_next) > 1:
                new_stake = self._inc_next_stake_amount_for_current_odd(float(odd_val))
                if new_stake is None:
                    return self._fail_bet_flow(
                        row,
                        f"Cannot compute incremental stake for odd {odd_val:.2f}.",
                        cache_key=cache_key,
                    )
                # Apply header balance cap (rounded down) to the newly computed stake.
                if self._last_site_balance is not None:
                    cap = float(math.floor(float(self._last_site_balance)))
                    new_stake = min(float(new_stake), cap)
                if float(new_stake) < 10.0:
                    return self._fail_bet_flow(
                        row,
                        f"Insufficient balance for computed incremental stake (cap {new_stake:.2f}).",
                        cache_key=cache_key,
                    )
                if abs(float(new_stake) - float(stake_amt)) > 0.009:
                    self.log.info(
                        "[Incremental] Trial %d stake recomputed using current odd %.2f: %.2f -> %.2f",
                        int(self._inc_trial_next),
                        float(odd_val),
                        float(stake_amt),
                        float(new_stake),
                    )
                stake_amt = float(new_stake)

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

            try:
                slip.dismiss_winning_popup(self.driver, self.log)
            except Exception:
                pass
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

    def _apply_settled_result(self, p: PendingBet, info: bh.SettledBetInfo) -> None:
        """Apply won/lost to balances (shared wallet if multi-threaded)."""
        # SportyBet sometimes reports cashouts as "Won" but with total_return < stake.
        # Treat that as a cashout settlement (not a win), and keep PnL based on return-stake.
        is_cashout_settle = (
            info.status == "won"
            and info.stake is not None
            and info.total_return is not None
            and float(info.total_return) < float(info.stake)
        )
        if is_cashout_settle:
            pnl = float(info.total_return) - float(info.stake)
            # Winning-mode ledger: cashout settle is a real net PnL for this match.
            try:
                if self.cfg.winning and self._winning is not None:
                    if self._winning.home == p.home and self._winning.away == p.away:
                        self._winning.net_before += float(pnl)
            except Exception:
                pass
            # Loss pool update: cashout settle with pnl<0 adds (stake - return) to pool.
            if pnl < 0:
                self._loss_pool_add(-float(pnl))
            # Apply to balances exactly as recorded (usually a smaller loss than full stake).
            if self.shared is not None:
                with self.shared.balance_lock:
                    self.shared.estimated_balance += pnl
                    self.shared.tracked_balance += pnl
                    self.shared.losses += 1
                    self.shared.cashout_settles += 1
            else:
                self.estimated_balance += pnl
                self.tracked_balance += pnl
            self.losses += 1
            self.cashout_settles += 1
            if self.cfg.incremental:
                # Cashout means the bet did not win; advance to the next trial.
                self._inc_trial_next = int(self._inc_trial_next) + 1
            self.log.info(
                "[Cashout settle] status=won-but-return<stake stake=%.2f return=%.2f pnl=%.2f "
                "estimated_balance=%.2f tracked_balance=%.2f wins=%d losses=%d cashout_settles=%d (thread=%s)",
                float(info.stake),
                float(info.total_return),
                pnl,
                self._effective_estimated_balance(),
                self._effective_tracked_balance(),
                self.wins,
                self.losses,
                self.cashout_settles,
                self.thread_id,
            )
            return

        if info.status == "won":
            if info.stake is not None and info.total_return is not None:
                pnl = info.total_return - info.stake
                # Repay global loss pool first from positive pnl.
                if pnl > 0:
                    rep = self._loss_pool_repay(float(pnl))
                    if rep > 0:
                        self.log.info(
                            "[Loss pool] Repaid %.2f (pool_now=%.2f) from win pnl=%.2f",
                            rep,
                            self._loss_pool_value(),
                            float(pnl),
                        )
                if self.shared is not None:
                    with self.shared.balance_lock:
                        self.shared.estimated_balance += pnl
                        self.shared.tracked_balance += pnl
                        self.shared.wins += 1
                else:
                    self.estimated_balance += pnl
                    self.tracked_balance += pnl
                self.wins += 1
                if self.cfg.incremental:
                    self._inc_reset_round()
                # Winning-mode ledger: apply net.
                try:
                    if self.cfg.winning and self._winning is not None:
                        if self._winning.home == p.home and self._winning.away == p.away:
                            self._winning.net_before += float(pnl)
                except Exception:
                    pass
                self.log.info(
                    "WON net_pnl=%.2f estimated_balance=%.2f tracked_balance=%.2f wins=%d losses=%d (thread=%s)",
                    pnl,
                    self._effective_estimated_balance(),
                    self._effective_tracked_balance(),
                    self.wins,
                    self.losses,
                    self.thread_id,
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
                if float(pnl) < 0:
                    self._loss_pool_add(-float(pnl))
                if self.shared is not None:
                    with self.shared.balance_lock:
                        self.shared.tracked_balance += pnl
                        if self.cfg.incremental:
                            self.shared.estimated_balance += pnl
                else:
                    self.tracked_balance += pnl
                    if self.cfg.incremental:
                        self.estimated_balance += pnl
            self.losses += 1
            if self.shared is not None:
                with self.shared.balance_lock:
                    self.shared.losses += 1
            if self.cfg.incremental:
                self._inc_trial_next = int(self._inc_trial_next) + 1
            # Winning-mode ledger: apply net.
            try:
                if self.cfg.winning and self._winning is not None:
                    if self._winning.home == p.home and self._winning.away == p.away:
                        if pnl is not None:
                            self._winning.net_before += float(pnl)
            except Exception:
                pass
            self.log.info(
                "LOST net_pnl=%s estimated_balance=%.2f tracked_balance=%.2f wins=%d losses=%d (thread=%s)",
                f"{pnl:.2f}" if pnl is not None else "n/a",
                self._effective_estimated_balance(),
                self._effective_tracked_balance(),
                self.wins,
                self.losses,
                self.thread_id,
            )

    def process_due_pending(self) -> None:
        now = time.time()
        due = [p for p in self.pending if p.check_after <= now]
        if not due:
            return
        p = due[0]
        self.pending.remove(p)
        key = bh.result_cache_key_with_stake(p.home, p.away, p.stake)
        self.log.info(
            "Checking result for %s vs %s (placed %.0fs ago) thread=%s",
            p.home,
            p.away,
            now - p.placed_at,
            self.thread_id,
        )

        # --- Multi-threaded (shared browser): caller must already hold shared.ui_lock ---
        if self.shared is not None:
            with self.shared.result_cache_lock:
                cached = self.shared.result_cache.pop(key, None)
            if cached is not None:
                self.log.info(
                    "[Result cache] Hit for %s vs %s (thread=%s)",
                    p.home,
                    p.away,
                    self.thread_id,
                )
                self._apply_settled_result(p, cached)
                bh.read_header_balance(self.driver, self.log)
                self.go_live()
                return

            # Only one thread should navigate bet history; everyone else waits.
            with self.shared.result_check_lock:
                with self.shared.result_cache_lock:
                    cached = self.shared.result_cache.pop(key, None)
                if cached is not None:
                    self.log.info(
                        "[Result cache] Hit after wait for %s vs %s (thread=%s)",
                        p.home,
                        p.away,
                        self.thread_id,
                    )
                    self._apply_settled_result(p, cached)
                    bh.read_header_balance(self.driver, self.log)
                    self.go_live()
                    return

                triples = self.shared.collect_due_pending_triples(now)
                keys = {t[3] for t in triples}
                if key not in keys:
                    triples = list(triples) + [(p.home, p.away, p.stake, key)]

                try:
                    results = bh.search_bet_history_for_pairs(
                        self.driver,
                        triples,
                        max_pages=self.cfg.bet_history_pages,
                        match_ratio=self.cfg.bet_history_team_match_ratio,
                        logger=self.log,
                    )
                except Exception:
                    self.log.error(
                        "[Bet history] Could not load or scan bet history pages (network or page error). Will retry this bet later."
                    )
                    self.log.debug("search_bet_history_for_pairs detail", exc_info=True)
                    p.check_after = now + 120
                    self.pending.append(p)
                    self.go_live()
                    return

                with self.shared.result_cache_lock:
                    for k2, inf in results.items():
                        if inf.status != "running":
                            self.shared.result_cache[k2] = inf

                info = results.get(key)
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

                with self.shared.result_cache_lock:
                    self.shared.result_cache.pop(key, None)
                self._apply_settled_result(p, info)
                bh.read_header_balance(self.driver, self.log)
                self.go_live()
                return

        # --- Single-threaded ---
        try:
            info = bh.search_bet_history(
                self.driver,
                p.home,
                p.away,
                max_pages=self.cfg.bet_history_pages,
                match_ratio=self.cfg.bet_history_team_match_ratio,
                wanted_stake=p.stake,
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

        self._apply_settled_result(p, info)

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
        est = self._effective_estimated_balance()
        trk = self._effective_tracked_balance()
        pct_tr = self._profit_pct(trk)
        pct_est = self._profit_pct(est)
        profit_amt_est = est - self.initial_amount_to_use
        profit_amt_tr = trk - self.initial_amount_to_use
        wr = self._win_rate_pct()
        wr_s = f"{wr:.2f}%" if wr is not None else "n/a"
        settled = self.wins + self.losses
        site = actual_balance if actual_balance is not None else "n/a"
        pending_pool = (
            sum(len(b.pending) for b in self.shared.bots)
            if self.shared is not None
            else len(self.pending)
        )

        block = "\n".join(
            [
                "[STATUS] ------------------------------------------------------------------",
                f"  thread_id                = {self.thread_id}"
                + (
                    f"  (pool wins/losses = {self.shared.wins}/{self.shared.losses} cashout_settles={self.shared.cashout_settles} loss_pool={self.shared.loss_pool:.2f})"
                    if self.shared is not None
                    else ""
                ),
                f"  initial_capital          = {self.initial_amount_to_use:.2f}",
                f"  incremental             = {self.cfg.incremental}"
                + (f"  (avg_odd={self.cfg.average_odd:.3f} max_trials={self._inc_max_trials()} next_trial={self._inc_trial_next} spent_round={self._inc_round_spent:.2f})" if self.cfg.incremental else ""),
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
                f"  estimated_balance      = {est:.2f}",
                f"  total_profit_est       = {self._fmt_signed_money(profit_amt_est)}  (balance - initial)",
                f"  profit_pct_est         = {self._fmt_signed_money(pct_est)}%",
                "  -- tracked (logging balance: +win / -loss net) ---------------------------",
                f"  tracked_balance        = {trk:.2f}",
                f"  total_profit_tracked   = {self._fmt_signed_money(profit_amt_tr)}  (balance - initial)",
                f"  profit_pct_tracked     = {self._fmt_signed_money(pct_tr)}%",
                "  -- next stake ------------------------------------------------------------",
                f"  stake_next             = {self.stake_per_match():.2f}"
                + (
                    "  (= incremental sizing)"
                    if self.cfg.incremental
                    else f"  (= estimated_balance / {self.cfg.max_simultaneous_matches})"
                ),
                "  -- settled record --------------------------------------------------------",
                f"  wins                   = {self.wins}",
                f"  losses                 = {self.losses}",
                f"  cashout_settles         = {self.cashout_settles}  (won but return < stake)",
                f"  loss_pool               = {self._loss_pool_value():.2f}  (shared loss to recover)",
                f"  settled_total          = {settled}",
                f"  win_rate               = {wr_s}  (wins / settled)",
                "  -- pending / site --------------------------------------------------------",
                f"  pending_bets           = {pending_pool}",
                f"  site_balance           = {site}",
                "[STATUS] ------------------------------------------------------------------",
            ]
        )
        self.log.info("%s", block)

    def step(self) -> None:
        if self.shared is not None:
            with self.shared.ui_lock:
                # Close any blocking popup that can appear asynchronously.
                try:
                    slip.dismiss_winning_popup(self.driver, self.log)
                except Exception:
                    pass
                # In shared-browser mode, all UI is serialized. Do result checks first so if
                # a thread needs bet-history navigation, everybody else waits until it's done.
                self.process_due_pending()
                self._sweep_cache()
                self._maybe_hard_refresh()
                # Always start this thread's betting turn from a freshly loaded live list
                # (better recovery if session expired or SPA got stale).
                self._thread_turn_reload_live()
                self.go_live()
                bal = bh.read_header_balance(self.driver, self.log)
                self._last_site_balance = bal
                self.log_status(actual_balance=bal)
                # Cashout scan on live list (before placing new bets).
                try:
                    try:
                        slip.dismiss_winning_popup(self.driver, self.log)
                    except Exception:
                        pass
                    self.log.info("[Cashout] Scan start (before betting).")
                    wanted: dict[str, str] = {}
                    if self.shared is not None:
                        for b in self.shared.bots:
                            for p in b.pending:
                                # Map both directions for robust matching.
                                k1 = bh.result_cache_key(p.home, p.away)
                                k2 = bh.result_cache_key(p.away, p.home)
                                wanted[k1] = p.selection
                                wanted[k2] = p.selection
                    else:
                        for p in self.pending:
                            k1 = bh.result_cache_key(p.home, p.away)
                            k2 = bh.result_cache_key(p.away, p.home)
                            wanted[k1] = p.selection
                            wanted[k2] = p.selection
                    c = slip.cashout_scan_and_execute(
                        self.driver,
                        wanted,
                        logger=self.log,
                        max_pages=5,
                    )
                    if c:
                        self.log.info("[Cashout] Completed: confirms=%d", c)
                    time.sleep(0.25)
                except Exception:
                    self.log.debug("cashout_scan_and_execute detail", exc_info=True)
                self.try_place_bets()
        else:
            # Single-threaded: OK to check results outside any global lock.
            try:
                slip.dismiss_winning_popup(self.driver, self.log)
            except Exception:
                pass
            self.process_due_pending()
            self._sweep_cache()
            self._maybe_hard_refresh()
            self.go_live()
            bal = bh.read_header_balance(self.driver, self.log)
            self._last_site_balance = bal
            self.log_status(actual_balance=bal)
            try:
                try:
                    slip.dismiss_winning_popup(self.driver, self.log)
                except Exception:
                    pass
                self.log.info("[Cashout] Scan start (before betting).")
                wanted: dict[str, str] = {}
                for p in self.pending:
                    k1 = bh.result_cache_key(p.home, p.away)
                    k2 = bh.result_cache_key(p.away, p.home)
                    wanted[k1] = p.selection
                    wanted[k2] = p.selection
                c = slip.cashout_scan_and_execute(
                    self.driver,
                    wanted,
                    logger=self.log,
                    max_pages=5,
                )
                if c:
                    self.log.info("[Cashout] Completed: confirms=%d", c)
                time.sleep(0.25)
            except Exception:
                self.log.debug("cashout_scan_and_execute detail", exc_info=True)
            self.try_place_bets()

        time.sleep(self.cfg.poll_sleep_seconds)

    def run_forever(self) -> None:
        cfgd = dataclasses.asdict(self.cfg)
        if "sporty_phone" in cfgd and cfgd.get("sporty_phone"):
            cfgd["sporty_phone"] = "***"
        if "sporty_password" in cfgd and cfgd.get("sporty_password"):
            cfgd["sporty_password"] = "***"
        self.log.info(
            "Starting live betting bot | thread=%s config=%s",
            self.thread_id,
            cfgd,
        )
        # Shared-browser mode: login is bootstrapped once in run_threaded_live_bots().
        if self.shared is None:
            self.login()
            time.sleep(4)
        try:
            while True:
                self.step()
        except KeyboardInterrupt:
            self.log.info("Interrupted by user")


def run_threaded_live_bots(cfg: LiveBettingConfig) -> None:
    """
    Run N parallel LiveBettingBot instances sharing ONE Chrome session.
    Uses SharedBettingContext for: live-list+bet serialization, bet-history batch cache,
    shared fixture bet/skip cache, shared wallet balances.
    """
    n = max(1, int(cfg.num_threads))
    if n <= 1:
        LiveBettingBot(cfg).run_forever()
        return

    shared = SharedBettingContext(cfg)
    threads: list[threading.Thread] = []

    # Bootstrap: create one bot to open the shared browser and login once.
    bootstrap = LiveBettingBot(cfg, shared=shared, thread_id=0, clear_log_on_start=True)
    with shared.ui_lock:
        bootstrap.login()
        shared.driver = bootstrap.driver
        shared.driver_ready.set()

    def worker(tid: int) -> None:
        # Each worker has its own incremental state + pending list, but shares the driver.
        bot = LiveBettingBot(
            cfg,
            shared=shared,
            thread_id=tid,
            clear_log_on_start=False,
        )
        shared.driver_ready.wait()
        bot.driver = shared.driver  # type: ignore[assignment]
        bot.logged_in = True
        bot.run_forever()

    for tid in range(n):
        t = threading.Thread(target=worker, args=(tid,), name=f"live-bet-{tid}", daemon=True)
        threads.append(t)
        t.start()
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        pass


def main():
    cfg = LiveBettingConfig(
        amount_to_use=3000.0,
        max_simultaneous_matches=30,
        only_bet_draws=False,
        only_bet_zero_zero_score=False,
    )
    run_threaded_live_bots(cfg)


if __name__ == "__main__":
    main()
