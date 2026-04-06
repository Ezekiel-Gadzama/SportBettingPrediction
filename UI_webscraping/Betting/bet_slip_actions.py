"""
Reusable bet slip interactions (SportyBet desktop).
Patterns align with Database/Get_data/football betting.py and user-provided HTML.
"""

from __future__ import annotations

import logging
import re
import time
from typing import TYPE_CHECKING, Literal

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
