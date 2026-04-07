"""
Entry point: run from project root, e.g.
  python UI_webscraping/Betting/run_live_football_betting.py
"""
import os
import sys

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from UI_webscraping.Betting.live_betting_bot import LiveBettingConfig, run_threaded_live_bots


def main():
    cfg = LiveBettingConfig(
        # If these are set (non-empty), they override SPORTY_PHONE/SPORTY_PASSWORD from .env
        sporty_phone=None,      # e.g. "08012345678"
        sporty_password=None,   # e.g. "your_password"
        amount_to_use=10000,
        max_simultaneous_matches=3,
        num_threads=4,
        incremental=True,
        average_odd=1.17,
        ft_min_minute_exclusive=86,
        ft_max_minute_exclusive=92.0,
        bet_fulltime=True,
        bet_halftime=True,
        ht_min_minute_exclusive=38,
        ht_max_minute_exclusive=40.0,
        minimum_odd=1.07,
        maximum_odd=1.35,
        ft_max_total_goals=4,
        ht_max_total_goals=2,
        excluded_competitions=[
            "Premier League (England)",
            "La Liga (Spain)",
            "UEFA Champions League",
            "FA Cup (England)",
            "FIFA World Cup Qualifiers",
            "NWSL (USA)",
            "Serie A (Italy)",
            "Ligue 1 (France)",
            "Women's Super League (UK)",
            "UEFA Women's Champions League",
            "Women's EURO 2025",
            "Bundesliga (Germany)",
            "simulated reality",
            "india i-league",
            "india mumbai premier league",
            "india goa pro league",
            "Any india football league"
        ],
        deepseek_enabled=True,
        deepseek_model="deepseek-chat",
        deepseek_timeout_s=35.0,
        only_bet_draws=True,
        only_bet_zero_zero_score=False,
        result_wait_seconds=500.0,
        cache_ttl_seconds=1200.0,
        bet_history_pages=1,
        poll_sleep_seconds=8.0,
        live_url="https://www.sportybet.com/ng/sport/football/live_list/",
    )
    run_threaded_live_bots(cfg)


if __name__ == "__main__":
    main()
