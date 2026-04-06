"""
Entry point: run from project root, e.g.
  python UI_webscraping/Betting/run_live_football_betting.py
"""
import os
import sys

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from UI_webscraping.Betting.live_betting_bot import LiveBettingBot, LiveBettingConfig


def main():
    cfg = LiveBettingConfig(
        amount_to_use=100,
        max_simultaneous_matches=10,
        min_minute_exclusive=88.0,
        max_minute_exclusive=92.0,
        minimum_odd=1.07,
        maximum_odd=1.35,
        max_total_goals=4,
        exclude_srl=True,
        excluded_competitions=[
            "Premier League (England)",
            "La Liga (Spain)",
            "UEFA Champions League",
            "FA Cup (England)",
            "FIFA World Cup Qualifiers",
            "NWSL (USA)",
            "Women's Super League (UK)",
            "UEFA Women's Champions League",
            "Women's EURO 2025",
            "simulated reality"
        ],
        deepseek_enabled=True,
        deepseek_model="deepseek-chat",
        deepseek_timeout_s=35.0,
        only_bet_draws=True,
        only_bet_zero_zero_score=False,
        result_wait_seconds=500.0,
        cache_ttl_seconds=1200.0,
        bet_history_pages=5,
        poll_sleep_seconds=8.0,
        live_url="https://www.sportybet.com/ng/sport/football/live_list/",
    )
    bot = LiveBettingBot(cfg)
    bot.run_forever()


if __name__ == "__main__":
    main()
