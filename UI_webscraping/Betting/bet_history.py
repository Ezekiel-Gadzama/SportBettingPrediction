from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from UI_webscraping.Betting.parsing import teams_match_fuzzy

if TYPE_CHECKING:
    from selenium.webdriver.remote.webdriver import WebDriver


@dataclass
class SettledBetInfo:
    status: Literal["won", "lost", "running", "unknown"]
    stake: float | None
    total_return: float | None
    match_text: str


def result_cache_key(home: str, away: str) -> str:
    """Stable key for cross-thread result cache (normalized team names)."""
    h = " ".join((home or "").strip().lower().split())
    a = " ".join((away or "").strip().lower().split())
    return f"{h}|{a}"


def _parse_money(text: str) -> float | None:
    if not text:
        return None
    t = text.replace("NGN", "").replace(",", "").strip()
    try:
        return float(t)
    except ValueError:
        return None


def _parse_order_to_info(order, match_text: str, *, logger: logging.Logger | None) -> SettledBetInfo | None:
    """Extract status + stake/return from a bet-history order block."""
    log = logger or logging.getLogger(__name__)
    try:
        bar = order.find_element(By.CSS_SELECTOR, ".m-order-bar")
        bar_class = bar.get_attribute("class") or ""
        status_el = bar.find_elements(By.CSS_SELECTOR, ".m-order-status")
        raw_status = status_el[0].text.strip().lower() if status_el else ""
        data_key = status_el[0].get_attribute("data-cms-key") if status_el else ""

        if "won" in raw_status or data_key == "won" or "win" in bar_class:
            st: Literal["won", "lost", "running", "unknown"] = "won"
        elif "lost" in raw_status or data_key == "lost" or "gray" in bar_class:
            st = "lost"
        elif "running" in raw_status or data_key == "running":
            st = "running"
        else:
            st = "unknown"

        stake = None
        ret = None
        try:
            stake_el = order.find_element(By.CSS_SELECTOR, ".m-order-total-stake .stake.label")
            stake = _parse_money(stake_el.text)
        except Exception:
            pass
        try:
            ret_el = order.find_element(By.CSS_SELECTOR, ".m-order-total-return .stake.label")
            ret = _parse_money(ret_el.text)
        except Exception:
            pass

        log.info("Found order: %s status=%s stake=%s return=%s", match_text, st, stake, ret)
        return SettledBetInfo(status=st, stake=stake, total_return=ret, match_text=match_text)
    except Exception:
        return None


def find_bet_on_page(
    driver: WebDriver,
    home: str,
    away: str,
    *,
    booking_code: str | None = None,
    match_ratio: float = 0.85,
    logger: logging.Logger | None = None,
) -> SettledBetInfo | None:
    """
    Scan loaded bet history page for a single order matching the teams.
    """
    log = logger or logging.getLogger(__name__)
    orders = driver.find_elements(By.CSS_SELECTOR, ".m-order-list")
    for order in orders:
        try:
            vs_els = order.find_elements(By.CSS_SELECTOR, ".m-order-vs.label")
            matched_vs = None
            match_text = ""
            for vs_el in vs_els:
                match_text = (vs_el.text or vs_el.get_attribute("innerText") or "").strip()
                if not match_text:
                    continue
                if teams_match_fuzzy(
                    home, away, match_text, threshold=match_ratio
                ):
                    matched_vs = vs_el
                    break
            if matched_vs is None:
                continue
            info = _parse_order_to_info(order, match_text, logger=logger)
            if info:
                return info
        except Exception:
            continue
    return None


def find_bets_for_pairs_on_page(
    driver: WebDriver,
    triples: list[tuple[str, str, str]],
    *,
    match_ratio: float = 0.85,
    logger: logging.Logger | None = None,
) -> dict[str, SettledBetInfo]:
    """
    triples: list of (home, away, cache_key) to look for on the *already loaded* page.
    Returns cache_key -> SettledBetInfo for matches found (at most one order per key).
    """
    log = logger or logging.getLogger(__name__)
    results: dict[str, SettledBetInfo] = {}
    if not triples:
        return results
    wanted = {k for _, _, k in triples}
    orders = driver.find_elements(By.CSS_SELECTOR, ".m-order-list")
    for order in orders:
        try:
            vs_els = order.find_elements(By.CSS_SELECTOR, ".m-order-vs.label")
            for vs_el in vs_els:
                match_text = (vs_el.text or vs_el.get_attribute("innerText") or "").strip()
                if not match_text:
                    continue
                for home, away, key in triples:
                    if key in results:
                        continue
                    if not teams_match_fuzzy(
                        home, away, match_text, threshold=match_ratio
                    ):
                        continue
                    info = _parse_order_to_info(order, match_text, logger=log)
                    if info:
                        results[key] = info
                if wanted <= set(results.keys()):
                    return results
        except Exception:
            continue
    return results


def search_bet_history(
    driver: WebDriver,
    home: str,
    away: str,
    *,
    base_url: str = "https://www.sportybet.com/ng/my_accounts/bet_history/sport_bets",
    max_pages: int = 5,
    settle_delay_s: float = 2.0,
    match_ratio: float = 0.85,
    logger: logging.Logger | None = None,
) -> SettledBetInfo | None:
    log = logger or logging.getLogger(__name__)
    for page in range(1, max_pages + 1):
        url = f"{base_url}?page={page}"
        log.info("Opening bet history page %s", url)
        driver.get(url)
        time.sleep(settle_delay_s)
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".m-order-list, .m-order-wrapper"))
            )
        except Exception:
            log.warning("Bet history container slow/missing on page %s", page)

        hit = find_bet_on_page(
            driver, home, away, match_ratio=match_ratio, logger=logger
        )
        if hit:
            return hit
    return None


def search_bet_history_for_pairs(
    driver: WebDriver,
    triples: list[tuple[str, str, str]],
    *,
    base_url: str = "https://www.sportybet.com/ng/my_accounts/bet_history/sport_bets",
    max_pages: int = 5,
    settle_delay_s: float = 2.0,
    match_ratio: float = 0.85,
    logger: logging.Logger | None = None,
) -> dict[str, SettledBetInfo]:
    """
    Scan bet history pages once per page and match many fixtures per page.
    triples: (home, away, cache_key) where cache_key == result_cache_key(home, away).
    Returns mapping cache_key -> SettledBetInfo for keys found (may be partial).
    """
    log = logger or logging.getLogger(__name__)
    out: dict[str, SettledBetInfo] = {}
    if not triples:
        return out
    want = {k for _, _, k in triples}
    for page in range(1, max_pages + 1):
        if want <= set(out.keys()):
            break
        url = f"{base_url}?page={page}"
        log.info("[Bet history batch] Opening page %s", url)
        driver.get(url)
        time.sleep(settle_delay_s)
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".m-order-list, .m-order-wrapper"))
            )
        except Exception:
            log.warning("Bet history container slow/missing on page %s", page)

        page_hits = find_bets_for_pairs_on_page(
            driver, triples, match_ratio=match_ratio, logger=log
        )
        for k, info in page_hits.items():
            if k not in out:
                out[k] = info
    return out


def read_header_balance(driver: WebDriver, logger: logging.Logger | None = None) -> float | None:
    log = logger or logging.getLogger(__name__)
    try:
        el = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "j_balance")))
        txt = el.text.strip()
        m = re.search(r"([\d,]+\.?\d*)", txt.replace(",", ""))
        if m:
            v = float(m.group(1).replace(",", ""))
            log.debug("Balance from header: %s", v)
            return v
    except Exception as e:
        log.debug("read_header_balance: %s", e)
    return None
