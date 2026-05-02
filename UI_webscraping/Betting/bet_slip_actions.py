"""
Reusable bet slip interactions (SportyBet desktop).
Patterns align with Database/Get_data/football betting.py and user-provided HTML.
"""

from __future__ import annotations

import logging
import re
import time
from typing import TYPE_CHECKING, Literal
from dataclasses import dataclass

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.remote.webelement import WebElement

from UI_webscraping.Betting.betting_timing import random_human_pause

if TYPE_CHECKING:
    from selenium.webdriver.remote.webdriver import WebDriver


PlaceBetFlowResult = Literal["placed", "abort_range", "failed"]

# After stake entry: pause once, then up to N trials spaced by this interval while clicking Accept/Place until Confirm shows.
POST_STAKE_SETTLE_SECONDS = 0.35
BETSLIP_ACTION_MAX_TRIALS = 5
BETSLIP_TRIAL_INTERVAL_SECONDS = 0.45

# Cashout flow timings (similar to Place Bet).
CASHOUT_ACTION_MAX_TRIALS = 5
CASHOUT_TRIAL_INTERVAL_SECONDS = 0.45


def _norm_team(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _parse_score_text(text: str) -> tuple[int, int] | None:
    if not text:
        return None
    m = re.search(r"(\d+)\s*:\s*(\d+)", text.strip())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def _strip_minute_suffix(match_text: str) -> str:
    # "Vietnam vs Bangladesh(78' H2)" -> "Vietnam vs Bangladesh"
    t = (match_text or "").strip()
    if "(" in t:
        t = t.split("(", 1)[0].strip()
    return t


def _split_vs(match_text: str) -> tuple[str, str] | None:
    t = _strip_minute_suffix(match_text)
    if " vs " not in t:
        return None
    a, b = t.split(" vs ", 1)
    a, b = a.strip(), b.strip()
    if not a or not b:
        return None
    return a, b


def _parse_money_float(text: str) -> float | None:
    try:
        t = (text or "").strip().replace(",", "")
        m = re.search(r"(-?\d+(?:\.\d+)?)", t)
        if not m:
            return None
        return float(m.group(1))
    except Exception:
        return None


def _read_cashout_item_stake(item: WebElement) -> float | None:
    """
    Cashout cards show:
      <div class="m-col-title">Bet Stake</div>
      <div class="m-col-text">100.00</div>
    Read and parse it as float.
    """
    try:
        cols = item.find_elements(By.CSS_SELECTOR, "div.m-col")
        for c in cols:
            try:
                title = (c.find_element(By.CSS_SELECTOR, "div.m-col-title").text or "").strip().lower()
                if "bet stake" in title:
                    val = (c.find_element(By.CSS_SELECTOR, "div.m-col-text").text or "").strip()
                    return _parse_money_float(val)
            except Exception:
                continue
        return None
    except Exception:
        return None


def _read_cashout_item_full_cashout_amount(item: WebElement) -> float | None:
    """
    When expanded, the card often shows:
      <div class="cashout-title"><span>Full Cashout</span> <span>78.74</span></div>
    Parse the numeric amount.
    """
    try:
        spans = item.find_elements(By.CSS_SELECTOR, "div.cashout-title span")
        if not spans:
            return None
        # Prefer the last span as value.
        val = (spans[-1].text or "").strip()
        return _parse_money_float(val)
    except Exception:
        return None


@dataclass(frozen=True)
class CashoutAction:
    home: str
    away: str
    stake: float
    cashout_return: float


def click_betslip_tab(driver: WebDriver, tab_name: str, logger: logging.Logger | None = None) -> bool:
    """
    Click betslip sidebar tab ("Betslip" or "Cashout").
    """
    log = logger or logging.getLogger(__name__)
    name = (tab_name or "").strip().lower()
    if not name:
        return False
    try:
        # Primary: data-name attribute (most stable).
        dn = "Cashout" if name.startswith("cash") else "Betslip"
        els = driver.find_elements(
            By.CSS_SELECTOR, f"div.betslip-tabs .tabs-v2__tab[data-name='{dn}']"
        )
        tabs = els or driver.find_elements(By.CSS_SELECTOR, "div.betslip-tabs .tabs-v2__tab")
        for t in tabs:
            try:
                cls = (t.get_attribute("class") or "").lower()
                dname = (t.get_attribute("data-name") or "").strip().lower()
                txt = (t.text or t.get_attribute("innerText") or "").strip().lower()
                if (dname and dname == name) or (txt and name in txt) or (dname and name in dname):
                    # Even if already active, click once to ensure panel renders/focuses.
                    if "active" in cls:
                        try:
                            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", t)
                            driver.execute_script("arguments[0].click();", t)
                            time.sleep(0.2)
                        except Exception:
                            pass
                        log.info("[Betslip tab] %s already active (ensured by click).", dn)
                        if name.startswith("cash"):
                            time.sleep(5.0)
                        return True
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", t)
                    driver.execute_script("arguments[0].click();", t)
                    time.sleep(0.25)
                    log.info("[Betslip tab] Clicked %s.", dn)
                    if name.startswith("cash"):
                        time.sleep(5.0)
                    return True
            except Exception:
                continue
        log.warning("[Betslip tab] Could not find tab=%s", dn)
        return False
    except Exception:
        log.debug("click_betslip_tab detail", exc_info=True)
        return False


def dismiss_winning_popup(driver: WebDriver, logger: logging.Logger | None = None) -> bool:
    """
    Close the "YOU WON" pop dialog if it appears.
    This popup can block clicks and cause mis-detections in other flows.
    Returns True if a close click was performed.
    """
    log = logger or logging.getLogger(__name__)
    try:
        wraps = driver.find_elements(By.CSS_SELECTOR, "div.m-winning-wrapper")
        if not wraps:
            return False
        for w in wraps:
            try:
                if not w.is_displayed():
                    continue
            except Exception:
                continue

            # Prefer the close icon.
            try:
                close_icon = w.find_element(By.CSS_SELECTOR, "i.m-icon-close[data-action='close']")
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", close_icon)
                except Exception:
                    pass
                try:
                    close_icon.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", close_icon)
                time.sleep(0.15)
                log.info("[Popup] Closed YOU WON popup (icon).")
                return True
            except Exception:
                pass

            # Fallback: any close button in the wrapper.
            try:
                close_btns = w.find_elements(By.CSS_SELECTOR, "[data-action='close']")
                for b in close_btns:
                    try:
                        if not b.is_displayed():
                            continue
                    except Exception:
                        pass
                    try:
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
                    except Exception:
                        pass
                    try:
                        b.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", b)
                    time.sleep(0.15)
                    log.info("[Popup] Closed YOU WON popup (fallback).")
                    return True
            except Exception:
                pass
        return False
    except Exception:
        log.debug("dismiss_winning_popup detail", exc_info=True)
        return False


def _cashout_confirm_dialog(driver: WebDriver) -> WebElement | None:
    try:
        dlg = driver.find_elements(By.CSS_SELECTOR, "div.m-comfirm-wrapper")
        for d in dlg:
            try:
                if d.is_displayed():
                    return d
            except Exception:
                continue
        return None
    except Exception:
        return None


def _click_cashout_confirm(driver: WebDriver, logger: logging.Logger | None = None) -> bool:
    log = logger or logging.getLogger(__name__)
    dlg = _cashout_confirm_dialog(driver)
    if dlg is None:
        return False
    try:
        btn = dlg.find_element(By.CSS_SELECTOR, "button.confirm-sub")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        btn.click()
        log.info("[Cashout] Clicked Confirm.")
        return True
    except Exception:
        log.debug("_click_cashout_confirm", exc_info=True)
        return False


def _click_cashout_button_in_item(driver: WebDriver, item: WebElement) -> bool:
    try:
        btns = item.find_elements(By.CSS_SELECTOR, "div.m-btn-wrapper button")
        for b in btns:
            try:
                if not b.is_displayed():
                    continue
            except Exception:
                pass
            # Prefer data key, fall back to visible text.
            txt = (b.text or "").strip().lower()
            if "cashout" in txt:
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
                    b.click()
                except Exception:
                    # JS fallback
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
                    driver.execute_script("arguments[0].click();", b)
                return True
        return False
    except Exception:
        return False


def cashout_scan_and_execute(
    driver: WebDriver,
    wanted_selections: dict[str, str],
    *,
    logger: logging.Logger | None = None,
    max_pages: int = 5,
) -> int:
    """
    Scan the Cashout tab and cash out any wanted fixture where the selection is no longer "winning".
    wanted_selections maps fixture keys to selection: key can be either "home|away" or "away|home".
    Returns number of cashout confirms clicked.
    """
    log = logger or logging.getLogger(__name__)
    # Always switch to Cashout tab; even if we have nothing to act on, this keeps the UI in sync.
    if not click_betslip_tab(driver, "Cashout", log):
        return 0
    if not wanted_selections:
        log.info("[Cashout] No wanted selections; skipping scan.")
        return 0

    # Wait for Cashout panel/list to render (SPA sometimes needs a tick even after tab click).
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#j_betslip div[data-op='Cashout-panel'], #j_betslip div.m-cashout")
            )
        )
    except Exception:
        pass
    # Nudge scroll in the betslip wrapper to help lazy content mount.
    try:
        driver.execute_script(
            """
            var root = document.querySelector('#j_betslip');
            if (!root) return;
            root.scrollTop = root.scrollHeight;
            """
        )
        time.sleep(0.25)
    except Exception:
        pass

    confirms = 0
    scanned = 0
    matched = 0
    triggered = 0
    unfolded = 0
    clicked_cashout = 0
    for _ in range(max_pages):
        # Scope within betslip; match both <li.m-cashout-item> and any wrapper variants.
        items = driver.find_elements(
            By.CSS_SELECTOR,
            "#j_betslip ul.m-cashout-list > li.m-cashout-item, #j_betslip li.m-cashout-item, #j_betslip div.m-cashout-item",
        )
        log.info(
            "[Cashout] Page scan: items=%d wanted=%d",
            len(items),
            len(wanted_selections),
        )
        for item in items:
            try:
                # Extract match + score.
                match_el = item.find_elements(
                    By.CSS_SELECTOR, "div.m-bet-detail .m-text-min span, div.m-cashout-bet .m-text-min span"
                )
                match_txt = (match_el[-1].text if match_el else "").strip()
                score_el = item.find_elements(By.CSS_SELECTOR, "div.m-bet-detail .m-score span")
                score_txt = (score_el[0].text if score_el else "").strip()
                teams = _split_vs(match_txt)
                score = _parse_score_text(score_txt)
                if not teams or not score:
                    continue
                h, a = teams
                hg, ag = score
                scanned += 1
                key1 = f"{_norm_team(h)}|{_norm_team(a)}"
                key2 = f"{_norm_team(a)}|{_norm_team(h)}"
                sel = wanted_selections.get(key1) or wanted_selections.get(key2)
                if not sel:
                    continue
                sel = sel.strip().lower()
                matched += 1

                no_longer_winning = False
                if sel == "draw":
                    no_longer_winning = hg != ag
                elif sel == "home":
                    no_longer_winning = hg <= ag
                elif sel == "away":
                    no_longer_winning = ag <= hg
                if not no_longer_winning:
                    continue
                triggered += 1
                log.info(
                    "[Cashout] Trigger: %s vs %s sel=%s score=%d:%d (no_longer_winning=%s)",
                    h,
                    a,
                    sel,
                    hg,
                    ag,
                    no_longer_winning,
                )

                # Expand dropdown if folded.
                try:
                    unfold = item.find_elements(By.CSS_SELECTOR, "div.m-operation i.m-icon-unfold")
                    if unfold:
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", unfold[0])
                        driver.execute_script("arguments[0].click();", unfold[0])
                        time.sleep(0.25)
                        unfolded += 1
                except Exception:
                    pass

                # Try cashout + confirm with retries.
                for trial in range(CASHOUT_ACTION_MAX_TRIALS):
                    if _cashout_confirm_dialog(driver) is not None:
                        if _click_cashout_confirm(driver, log):
                            confirms += 1
                        time.sleep(CASHOUT_TRIAL_INTERVAL_SECONDS)
                        break

                    if _click_cashout_button_in_item(driver, item):
                        clicked_cashout += 1
                        log.info(
                            "[Cashout] Clicked Cashout for %s vs %s (sel=%s score=%d:%d) trial %d/%d",
                            h,
                            a,
                            sel,
                            hg,
                            ag,
                            trial + 1,
                            CASHOUT_ACTION_MAX_TRIALS,
                        )
                        time.sleep(CASHOUT_TRIAL_INTERVAL_SECONDS)
                        continue

                    time.sleep(CASHOUT_TRIAL_INTERVAL_SECONDS)
            except Exception:
                log.debug("cashout item scan", exc_info=True)
                continue

        # Next page if available (pagination: "2 / 6" + next icon).
        try:
            pg = driver.find_elements(By.CSS_SELECTOR, "#j_betslip div.m-pagination-wrapper, div.m-pagination-wrapper")
            if not pg:
                break
            pg_el = pg[0]
            # Read "cur / total" if present.
            cur_page = None
            total_page = None
            try:
                spans = pg_el.find_elements(By.CSS_SELECTOR, "span")
                cur_txt = ((spans[0].text if spans else "") or "").strip()
                total_sp = pg_el.find_elements(By.CSS_SELECTOR, "span.m-total")
                total_txt = ((total_sp[0].text if total_sp else "") or "").replace("/", "").strip()
                cur_page = int(cur_txt) if cur_txt.isdigit() else None
                total_page = int(total_txt) if total_txt.isdigit() else None
            except Exception:
                pass

            if cur_page is not None and total_page is not None and cur_page >= total_page:
                break

            nxt_icons = pg_el.find_elements(By.CSS_SELECTOR, "i.m-icon-next")
            if not nxt_icons:
                break
            nxt_icon = nxt_icons[0]
            # Prefer clicking the icon itself (as per DOM). Fall back to parent <li> if needed.
            click_targets = [nxt_icon]
            try:
                li = nxt_icon.find_element(By.XPATH, "./ancestor::li[1]")
                click_targets.append(li)
            except Exception:
                pass

            log.info(
                "[Cashout] Pagination: cur=%s total=%s -> clicking next",
                cur_page if cur_page is not None else "?",
                total_page if total_page is not None else "?",
            )

            # Retry click until the page number changes (or give up).
            changed = False
            for _trial in range(3):
                try:
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block:'center'});", nxt_icon
                    )
                except Exception:
                    pass
                clicked = False
                for target in click_targets:
                    try:
                        driver.execute_script(
                            "arguments[0].scrollIntoView({block:'center'});", target
                        )
                    except Exception:
                        pass
                    try:
                        # JS click is most reliable for this icon.
                        driver.execute_script("arguments[0].click();", target)
                        clicked = True
                        break
                    except Exception:
                        continue
                if not clicked:
                    time.sleep(0.4)
                    continue
                time.sleep(0.7)
                try:
                    # Re-read from DOM after click (SPA may re-render pagination node).
                    pg2 = driver.find_elements(By.CSS_SELECTOR, "#j_betslip div.m-pagination-wrapper, div.m-pagination-wrapper")
                    pg2_el = pg2[0] if pg2 else pg_el
                    spans2 = pg2_el.find_elements(By.CSS_SELECTOR, "span")
                    cur2_txt = ((spans2[0].text if spans2 else "") or "").strip()
                    cur2 = int(cur2_txt) if cur2_txt.isdigit() else None
                    if cur_page is None or (cur2 is not None and cur2 != cur_page):
                        changed = True
                        log.info(
                            "[Cashout] Pagination advanced: %s -> %s",
                            cur_page if cur_page is not None else "?",
                            cur2 if cur2 is not None else "?",
                        )
                        break
                except Exception:
                    # If we can't read it, still proceed once.
                    changed = True
                    break
            if not changed:
                log.warning("[Cashout] Pagination click did not advance; stopping pagination.")
                break
        except Exception:
            break

    log.info(
        "[Cashout] Summary: scanned=%d matched=%d triggered=%d unfolded=%d cashout_clicks=%d confirms=%d",
        scanned,
        matched,
        triggered,
        unfolded,
        clicked_cashout,
        confirms,
    )
    return confirms


def cashout_scan_and_execute_detailed(
    driver: WebDriver,
    wanted: dict[str, str | tuple[str, float | None]],
    *,
    logger: logging.Logger | None = None,
    max_pages: int = 5,
) -> list[CashoutAction]:
    """
    Like cashout_scan_and_execute, but can disambiguate by stake and returns details for
    each confirmed cashout so callers can update their ledgers.

    `wanted` maps fixture keys to:
      - selection str ("home"/"away"/"draw") OR
      - (selection, wanted_stake) where wanted_stake is the exact stake to match.
    """
    log = logger or logging.getLogger(__name__)
    if not click_betslip_tab(driver, "Cashout", log):
        return []
    if not wanted:
        log.info("[Cashout] No wanted selections; skipping scan.")
        return []

    # Wait for panel render.
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#j_betslip div[data-op='Cashout-panel'], #j_betslip div.m-cashout")
            )
        )
    except Exception:
        pass

    # Nudge scroll.
    try:
        driver.execute_script(
            """
            var root = document.querySelector('#j_betslip');
            if (!root) return;
            root.scrollTop = root.scrollHeight;
            """
        )
        time.sleep(0.25)
    except Exception:
        pass

    actions: list[CashoutAction] = []
    for _ in range(max_pages):
        items = driver.find_elements(
            By.CSS_SELECTOR,
            "#j_betslip ul.m-cashout-list > li.m-cashout-item, #j_betslip li.m-cashout-item, #j_betslip div.m-cashout-item",
        )
        log.info("[Cashout] Page scan: items=%d wanted=%d", len(items), len(wanted))
        for item in items:
            try:
                match_el = item.find_elements(
                    By.CSS_SELECTOR, "div.m-bet-detail .m-text-min span, div.m-cashout-bet .m-text-min span"
                )
                match_txt = (match_el[-1].text if match_el else "").strip()
                score_el = item.find_elements(By.CSS_SELECTOR, "div.m-bet-detail .m-score span")
                score_txt = (score_el[0].text if score_el else "").strip()
                teams = _split_vs(match_txt)
                score = _parse_score_text(score_txt)
                if not teams or not score:
                    continue
                h, a = teams
                hg, ag = score
                stake_val = _read_cashout_item_stake(item)

                key1 = f"{_norm_team(h)}|{_norm_team(a)}"
                key2 = f"{_norm_team(a)}|{_norm_team(h)}"
                raw = wanted.get(key1) or wanted.get(key2)
                if not raw:
                    continue
                if isinstance(raw, tuple):
                    sel = (raw[0] or "").strip().lower()
                    wanted_stake = raw[1]
                else:
                    sel = (raw or "").strip().lower()
                    wanted_stake = None

                if wanted_stake is not None and stake_val is not None:
                    # Stake in cashout cards can be displayed rounded (e.g. 155.93 -> 156.00).
                    if abs(float(stake_val) - float(wanted_stake)) > 1.0:
                        continue
                elif wanted_stake is not None and stake_val is None:
                    # Can't verify stake -> skip to avoid cashing out wrong card.
                    continue

                no_longer_winning = False
                if sel == "draw":
                    no_longer_winning = hg != ag
                elif sel == "home":
                    no_longer_winning = hg <= ag
                elif sel == "away":
                    no_longer_winning = ag <= hg
                if not no_longer_winning:
                    continue

                log.info(
                    "[Cashout] Trigger: %s vs %s sel=%s stake=%s score=%d:%d",
                    h,
                    a,
                    sel,
                    f"{stake_val:.2f}" if stake_val is not None else "?",
                    hg,
                    ag,
                )

                # Expand.
                try:
                    unfold = item.find_elements(By.CSS_SELECTOR, "div.m-operation i.m-icon-unfold")
                    if unfold:
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", unfold[0])
                        driver.execute_script("arguments[0].click();", unfold[0])
                        time.sleep(0.25)
                except Exception:
                    pass

                # Read cashout return offer (best-effort) from expanded card.
                offer = _read_cashout_item_full_cashout_amount(item)

                # Click cashout + confirm.
                confirmed = False
                for trial in range(CASHOUT_ACTION_MAX_TRIALS):
                    if _cashout_confirm_dialog(driver) is not None:
                        if _click_cashout_confirm(driver, log):
                            confirmed = True
                        time.sleep(CASHOUT_TRIAL_INTERVAL_SECONDS)
                        break
                    if _click_cashout_button_in_item(driver, item):
                        log.info(
                            "[Cashout] Clicked Cashout for %s vs %s (sel=%s stake=%s) trial %d/%d",
                            h,
                            a,
                            sel,
                            f"{stake_val:.2f}" if stake_val is not None else "?",
                            trial + 1,
                            CASHOUT_ACTION_MAX_TRIALS,
                        )
                        time.sleep(CASHOUT_TRIAL_INTERVAL_SECONDS)
                        continue
                    time.sleep(CASHOUT_TRIAL_INTERVAL_SECONDS)

                if confirmed and stake_val is not None and offer is not None:
                    actions.append(CashoutAction(home=h, away=a, stake=float(stake_val), cashout_return=float(offer)))
            except Exception:
                log.debug("cashout item scan", exc_info=True)
                continue

        # Pagination (reuse existing logic by delegating to the old function's block would be messy;
        # keep it simple here by copying the minimal next-page logic).
        try:
            pg = driver.find_elements(By.CSS_SELECTOR, "#j_betslip div.m-pagination-wrapper, div.m-pagination-wrapper")
            if not pg:
                break
            pg_el = pg[0]
            cur_page = None
            total_page = None
            try:
                spans = pg_el.find_elements(By.CSS_SELECTOR, "span")
                cur_txt = ((spans[0].text if spans else "") or "").strip()
                total_sp = pg_el.find_elements(By.CSS_SELECTOR, "span.m-total")
                total_txt = ((total_sp[0].text if total_sp else "") or "").replace("/", "").strip()
                cur_page = int(cur_txt) if cur_txt.isdigit() else None
                total_page = int(total_txt) if total_txt.isdigit() else None
            except Exception:
                pass
            if cur_page is not None and total_page is not None and cur_page >= total_page:
                break

            nxt_icons = pg_el.find_elements(By.CSS_SELECTOR, "i.m-icon-next")
            if not nxt_icons:
                break
            nxt_icon = nxt_icons[0]
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", nxt_icon)
            driver.execute_script("arguments[0].click();", nxt_icon)
            time.sleep(0.7)
        except Exception:
            break

    return actions


def odd_in_range(
    odd: float | None,
    minimum_odd: float | None,
    maximum_odd: float | None,
) -> bool:
    """None bounds mean no limit on that side."""
    if odd is None:
        return False
    if minimum_odd is not None and odd < float(minimum_odd):
        return False
    if maximum_odd is not None and odd > float(maximum_odd):
        return False
    return True


def find_market_header(driver: WebDriver, market_name: str = "1X2", timeout: int = 25):
    """
    Match detail page uses <span class="m-table-header-title">1X2</span>.
    Avoid //span[text()='1X2'] only — whitespace / hidden duplicates can break.
    """
    primary = (
        f"//span[contains(@class,'m-table-header-title')]"
        f"[normalize-space()='{market_name}']"
    )
    fallback = f"//span[normalize-space()='{market_name}']"
    last_exc: Exception | None = None
    for xp in (primary, fallback):
        try:
            return WebDriverWait(driver, min(timeout, 5)).until(
                EC.visibility_of_element_located((By.XPATH, xp))
            )
        except Exception as e:
            last_exc = e
    if last_exc:
        raise last_exc
    raise RuntimeError("find_market_header: unreachable")


def wait_for_match_detail_1x2(
    driver: WebDriver,
    logger: logging.Logger | None = None,
    timeout: int = 12,
    *,
    market_name: str = "1X2",
) -> bool:
    """After opening a fixture from the live list, wait until the target market header is visible."""
    log = logger or logging.getLogger(__name__)
    try:
        find_market_header(driver, market_name, timeout=timeout)
        return True
    except Exception as e:
        log.warning(
            "[Match page] %s header not visible after %ss (%s).",
            market_name,
            timeout,
            type(e).__name__,
        )
        return False


def _cell_label(cell: WebElement) -> str:
    try:
        return cell.find_element(By.CLASS_NAME, "m-table-cell-item").text.strip().lower()
    except Exception:
        return ""


def pick_1x2_target_cell(
    outcome_cells: list,
    selection: str,
    home_name: str | None,
    away_name: str | None,
) -> WebElement | None:
    sel = selection.lower().strip()
    if sel not in ("home", "draw", "away"):
        return None
    target = None
    for cell in outcome_cells:
        label = _cell_label(cell)
        if sel == "draw" and label in ("x", "draw"):
            target = cell
            break
        if sel == "home" and home_name and home_name.strip().lower() in label:
            target = cell
            break
        if sel == "away" and away_name and away_name.strip().lower() in label:
            target = cell
            break
        if sel == "home" and label in ("1", "home"):
            target = cell
            break
        if sel == "away" and label in ("2", "away"):
            target = cell
            break
    if target is None and len(outcome_cells) >= 3:
        idx = {"home": 0, "draw": 1, "away": 2}[sel]
        target = outcome_cells[idx]
    return target


def _parse_decimal_odd_text(text: str) -> float | None:
    t = (text or "").strip().replace(",", "")
    if not t or t in ("-", "—", "N/A", "n/a"):
        return None
    m = re.search(r"(\d+\.?\d*)", t)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def read_betslip_first_selection_odd(
    driver: WebDriver, logger: logging.Logger | None = None,
) -> float | None:
    """
    Current decimal odd shown in the open bet slip (e.g. .m-item-odds .m-text-main).
    Used when the slip asks for 'Accept Changes' after a price move.
    """
    log = logger or logging.getLogger(__name__)
    try:
        betslip = driver.find_element(By.CLASS_NAME, "m-betslips")
        for item in betslip.find_elements(By.CSS_SELECTOR, ".m-list .m-item"):
            try:
                odd_el = item.find_element(By.CSS_SELECTOR, ".m-item-odds .m-text-main")
                val = _parse_decimal_odd_text(odd_el.text)
                if val is not None:
                    return val
            except Exception:
                continue
        for el in betslip.find_elements(By.CSS_SELECTOR, ".m-item-odds .m-text-main"):
            try:
                val = _parse_decimal_odd_text(el.text)
                if val is not None:
                    return val
            except Exception:
                continue
    except Exception:
        log.debug("read_betslip_first_selection_odd", exc_info=True)
    return None


def _visible_accept_changes_btn(driver: WebDriver) -> WebElement | None:
    """Accept Changes control (visible; may still be animating — caller uses try_click)."""
    for el in driver.find_elements(
        By.XPATH,
        "//div[contains(@class,'m-betslips')]//button[.//span[@data-cms-key='accept_changes']]",
    ):
        try:
            if el.is_displayed():
                return el
        except Exception:
            continue
    return None


def _payment_confirm_button(driver: WebDriver) -> WebElement | None:
    """Payment confirmation step after Place Bet (PIN / confirm)."""
    for el in driver.find_elements(
        By.CSS_SELECTOR,
        "button[data-op='desktop-betslip-confirm-button']",
    ):
        try:
            if el.is_displayed():
                return el
        except Exception:
            continue
    return None


def _try_click_element(driver: WebDriver, el: WebElement) -> None:
    try:
        el.click()
    except Exception:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        except Exception:
            pass
        driver.execute_script("arguments[0].click();", el)


def _any_place_bet_button_visible(driver: WebDriver) -> WebElement | None:
    """Place Bet control is in the DOM and visible (may still be disabled)."""
    for el in driver.find_elements(
        By.CSS_SELECTOR,
        "button[data-op='desktop-betslip-place-bet-button']",
    ):
        try:
            if el.is_displayed():
                return el
        except Exception:
            continue
    return None


def click_place_bet_with_accept_flow(
    driver: WebDriver,
    logger: logging.Logger | None = None,
    *,
    minimum_odd: float | None = None,
    maximum_odd: float | None = None,
    human_pause=None,
    total_wait: float = 40.0,
) -> PlaceBetFlowResult:
    """
    After stake is filled: wait POST_STAKE_SETTLE_SECONDS, then up to BETSLIP_ACTION_MAX_TRIALS
    rounds (BETSLIP_TRIAL_INTERVAL_SECONDS apart). Each round: if Payment Confirm is visible, success;
    else if Accept Changes — validate slip odd then click; else if Place Bet — validate odd if readable
    then click (native + JS fallback). Success means Confirm is visible — caller then runs confirm click.
    """
    start = time.monotonic()
    log = logger or logging.getLogger(__name__)
    pause = human_pause or random_human_pause

    time.sleep(POST_STAKE_SETTLE_SECONDS)

    for trial in range(BETSLIP_ACTION_MAX_TRIALS):
        if time.monotonic() - start > float(total_wait):
            log.warning("[Betslip] Action loop timed out after %.1fs.", total_wait)
            return "failed"
        if _payment_confirm_button(driver) is not None:
            log.info(
                "[Betslip] Payment Confirm visible (trial %s/%s).",
                trial + 1,
                BETSLIP_ACTION_MAX_TRIALS,
            )
            return "placed"

        accept_btn = _visible_accept_changes_btn(driver)
        if accept_btn is not None:
            slip_odd = read_betslip_first_selection_odd(driver, log)
            if slip_odd is None:
                log.warning(
                    "[Betslip] Accept Changes visible but could not read odd from slip — aborting."
                )
                return "abort_range"
            if not odd_in_range(slip_odd, minimum_odd, maximum_odd):
                log.warning(
                    "[Betslip] Accept Changes: slip odd %.2f outside allowed range "
                    "[min=%s max=%s] — aborting.",
                    slip_odd,
                    minimum_odd if minimum_odd is not None else "—",
                    maximum_odd if maximum_odd is not None else "—",
                )
                return "abort_range"
            try:
                pause()
                _try_click_element(driver, accept_btn)
                log.info(
                    "[Betslip] Clicked Accept Changes (slip odd=%.2f, trial %s/%s).",
                    slip_odd,
                    trial + 1,
                    BETSLIP_ACTION_MAX_TRIALS,
                )
            except Exception:
                log.debug("Accept Changes click", exc_info=True)
            time.sleep(BETSLIP_TRIAL_INTERVAL_SECONDS)
            continue

        place_btn = _any_place_bet_button_visible(driver)
        if place_btn is not None:
            slip_odd = read_betslip_first_selection_odd(driver, log)
            if slip_odd is not None and not odd_in_range(
                slip_odd, minimum_odd, maximum_odd
            ):
                log.warning(
                    "[Betslip] Place Bet shown but slip odd %.2f outside allowed range — aborting.",
                    slip_odd,
                )
                return "abort_range"
            try:
                pause()
                _try_click_element(driver, place_btn)
                log.info(
                    "Clicked Place Bet (trial %s/%s).",
                    trial + 1,
                    BETSLIP_ACTION_MAX_TRIALS,
                )
            except Exception:
                log.debug("Place Bet click", exc_info=True)
            time.sleep(BETSLIP_TRIAL_INTERVAL_SECONDS)
            continue

        log.debug(
            "[Betslip] Trial %s/%s: no Accept / Place Bet yet.",
            trial + 1,
            BETSLIP_ACTION_MAX_TRIALS,
        )
        time.sleep(BETSLIP_TRIAL_INTERVAL_SECONDS)

    if _payment_confirm_button(driver) is not None:
        log.info("[Betslip] Payment Confirm visible after trial loop.")
        return "placed"

    log.error(
        "[Betslip] Payment Confirm did not appear after %s stake trials "
        "(Accept/Place clicked when shown).",
        BETSLIP_ACTION_MAX_TRIALS,
    )
    return "failed"


def get_1x2_wrapper_and_cells(
    driver: WebDriver,
    logger: logging.Logger | None = None,
    *,
    market_name: str = "1X2",
):
    """Locate market table by header title; returns (wrapper, outcome_cells) or None on failure."""
    log = logger or logging.getLogger(__name__)
    try:
        market_header = find_market_header(driver, market_name)
        wrapper = market_header.find_element(
            By.XPATH,
            ".//ancestor::div[contains(@class, 'm-table__wrapper')]",
        )
        # One row: Home | Draw | Away — take cells from the first m-outcome row only
        # (avoids mixing in other markets that also use m-outcome-style rows).
        outcome_rows = wrapper.find_elements(
            By.XPATH,
            ".//div[contains(@class,'m-table-row') and contains(@class,'m-outcome')]",
        )
        if not outcome_rows:
            log.error("[%s market] No m-outcome row under wrapper.", market_name)
            return None
        outcome_cells = outcome_rows[0].find_elements(
            By.CSS_SELECTOR, "div.m-table-cell--responsive"
        )
        if len(outcome_cells) < 3:
            log.error(
                "[%s market] Expected three outcomes; the market may be closed or the page layout changed.",
                market_name,
            )
            return None
        return wrapper, outcome_cells
    except Exception:
        log.error(
            "[%s market] Failed to load this market on the match page "
            "(timeout, or layout not as expected)."
            ,
            market_name,
        )
        log.debug("get_1x2_wrapper_and_cells detail", exc_info=True)
        return None


def read_1x2_selection_odd(
    driver: WebDriver,
    selection: str,
    *,
    home_name: str | None = None,
    away_name: str | None = None,
    logger: logging.Logger | None = None,
    market_name: str = "1X2",
) -> float | None:
    """
    Read the displayed decimal odd for home/draw/away without clicking.
    Returns None if the price is missing, locked, or unreadable.
    """
    log = logger or logging.getLogger(__name__)
    try:
        setup = get_1x2_wrapper_and_cells(driver, log, market_name=market_name)
        if not setup:
            return None
        _, outcome_cells = setup
        target = pick_1x2_target_cell(outcome_cells, selection, home_name, away_name)
        if target is None:
            log.error("[%s odd read] Could not map selection to an outcome column.", market_name)
            return None
        odd_spans = target.find_elements(
            By.XPATH,
            ".//span[contains(@class, 'm-table-cell-item') and contains(text(), '.')]",
        )
        if not odd_spans:
            odd_spans = target.find_elements(By.XPATH, ".//span[contains(@class, 'm-table-cell-item')]")
        if not odd_spans:
            log.error(
                "[%s odd read] No price shown for this selection (suspended or market closed).",
                market_name,
            )
            return None
        raw = odd_spans[0].text.strip()
        val = _parse_decimal_odd_text(raw)
        if val is None:
            log.error(
                "[%s odd read] Could not parse a numeric odd from the page (got %r).",
                market_name,
                raw[:24] if raw else "",
            )
        return val
    except Exception:
        log.error("[%s odd read] Unexpected error while reading the price.", market_name)
        log.debug("read_1x2_selection_odd detail", exc_info=True)
        return None


def click_1x2_outcome(
    driver: WebDriver,
    selection: str,
    *,
    home_name: str | None = None,
    away_name: str | None = None,
    logger: logging.Logger | None = None,
    market_name: str = "1X2",
) -> bool:
    """
    selection: 'home' | 'draw' | 'away'
    Tries label match (team names / 1 / x / 2) then falls back to column index 0,1,2.
    """
    log = logger or logging.getLogger(__name__)
    sel = selection.lower().strip()
    if sel not in ("home", "draw", "away"):
        log.error("[1X2 click] Invalid selection %r (expected home, draw, or away).", selection)
        return False
    try:
        setup = get_1x2_wrapper_and_cells(driver, log, market_name=market_name)
        if not setup:
            return False
        _, outcome_cells = setup
        target = pick_1x2_target_cell(outcome_cells, sel, home_name, away_name)
        if target is None:
            log.error("[%s click] Could not find the outcome cell to click.", market_name)
            return False

        odd_spans = target.find_elements(
            By.XPATH,
            ".//span[contains(@class, 'm-table-cell-item') and contains(text(), '.')]",
        )
        if not odd_spans:
            log.error("[%s click] No clickable price for this outcome (market may be closed).", market_name)
            return False
        odd_el = odd_spans[0]
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", odd_el)
        time.sleep(0.2)
        odd_el.click()
        log.info("Clicked %s %s (odd text=%s)", market_name, sel, odd_el.text.strip())
        return True
    except Exception:
        log.error(
            "[%s click] Failed to select the outcome on the bet slip (page not ready or market unavailable).",
            market_name,
        )
        log.debug("click_1x2_outcome detail", exc_info=True)
        return False


def enter_stake_amount(driver: WebDriver, amount: float, logger: logging.Logger | None = None) -> bool:
    log = logger or logging.getLogger(__name__)
    try:
        betslip = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CLASS_NAME, "m-betslips"))
        )
        stake_input = WebDriverWait(betslip, 5).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    ".//div[contains(@class, 'm-stake')]//div[@id='j_stake_0']//input[contains(@class,'m-input')]",
                )
            )
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", stake_input)
        stake_input.click()
        time.sleep(0.1)
        stake_input.send_keys(Keys.CONTROL + "a")
        stake_input.send_keys(Keys.DELETE)
        driver.execute_script("arguments[0].value = '';", stake_input)
        amt = max(10.0, float(amount))
        amt_str = str(int(round(amt))) if amt >= 10 else str(amt)
        stake_input.send_keys(amt_str)
        entered = driver.execute_script("return arguments[0].value", stake_input)
        log.info("Stake entered: requested=%s field=%s", amt_str, entered)
        return entered.replace(",", "") == amt_str.replace(",", "") or entered == amt_str
    except Exception:
        log.error(
            "[Stake input] Could not fill the stake field in the bet slip (betslip closed or input not found)."
        )
        log.debug("enter_stake_amount detail", exc_info=True)
        return False


def click_place_bet(driver: WebDriver, logger: logging.Logger | None = None) -> bool:
    """Legacy helper: place bet with no odd bounds (any price accepted on Accept Changes)."""
    return (
        click_place_bet_with_accept_flow(
            driver,
            logger,
            minimum_odd=None,
            maximum_odd=None,
            human_pause=None,
        )
        == "placed"
    )


def click_confirm_if_present(driver: WebDriver, logger: logging.Logger | None = None, wait: int = 15) -> bool:
    log = logger or logging.getLogger(__name__)
    try:
        btn = WebDriverWait(driver, min(wait, 5)).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-op='desktop-betslip-confirm-button']"))
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        btn.click()
        log.info("Clicked Confirm")
        return True
    except Exception:
        log.debug("Confirm button not shown yet or not clickable", exc_info=True)
        return False


def wait_success_dialog(
    driver: WebDriver, logger: logging.Logger | None = None, timeout: int = 60
) -> bool:
    log = logger or logging.getLogger(__name__)
    try:
        WebDriverWait(driver, min(timeout, 5)).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-op='desktop-betslip-success-dialog']"))
        )
        log.info("Success dialog visible")
        return True
    except Exception:
        log.error(
            "[Bet confirmation] Did not see the submission success dialog in time "
            "(bet may have failed or the site was slow)."
        )
        log.debug("wait_success_dialog detail", exc_info=True)
        return False


def read_success_dialog_details(driver: WebDriver, logger: logging.Logger | None = None) -> dict:
    log = logger or logging.getLogger(__name__)
    out: dict = {"booking_code": None, "potential_win": None, "stake": None}
    try:
        dlg = driver.find_element(By.CSS_SELECTOR, "div[data-op='desktop-betslip-success-dialog']")
        out["booking_code"] = dlg.find_element(By.CSS_SELECTOR, ".booking-code").text.strip()
        for li in dlg.find_elements(By.CSS_SELECTOR, "li.m-order-info"):
            try:
                label_el = li.find_element(By.CSS_SELECTOR, ".m-label")
                val_el = li.find_element(By.CSS_SELECTOR, ".m-value")
                key = (label_el.get_attribute("data-cms-key") or "").strip()
                lab_txt = (label_el.text or "").strip()
                val = val_el.text.strip()
                if key == "potential_win" or "Potential Win" in lab_txt:
                    out["potential_win"] = val
                if key == "total_stake" or "Total Stake" in lab_txt:
                    out["stake"] = val
            except Exception:
                continue
    except Exception as e:
        log.debug("read_success_dialog_details partial: %s", e)
    log.info("Success details: %s", out)
    return out


def click_success_ok(driver: WebDriver, logger: logging.Logger | None = None) -> bool:
    log = logger or logging.getLogger(__name__)
    try:
        dlg = driver.find_element(By.CSS_SELECTOR, "div[data-op='desktop-betslip-success-dialog']")
        btn = dlg.find_element(
            By.XPATH,
            ".//button[contains(@class,'af-button--primary') and .//span[contains(text(),'OK')]]",
        )
        btn.click()
        log.info("Clicked OK on success dialog")
        time.sleep(1)
        return True
    except Exception:
        log.error("[Success dialog] Could not press OK to close the confirmation popup.")
        log.debug("click_success_ok detail", exc_info=True)
        return False


def cancel_all_betslips(driver: WebDriver, logger: logging.Logger | None = None) -> None:
    log = logger or logging.getLogger(__name__)
    try:
        for icon in driver.find_elements(By.XPATH, "//i[contains(@class,'m-icon-delete')]"):
            try:
                icon.click()
                time.sleep(0.3)
            except Exception:
                pass
        log.info("Cleared betslip delete icons")
    except Exception as e:
        log.debug("cancel_all_betslips: %s", e)
