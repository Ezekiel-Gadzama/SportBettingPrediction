from Login.login import SportyBetLoginBot
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import time
import sys
import os
import re
from datetime import datetime, timedelta, timezone
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Define Nigeria's timezone
NIGERIA_TZ = timezone(timedelta(hours=1))

class FindMatch(SportyBetLoginBot):
    def __init__(self, url):
        super().__init__(url)
        self.matches = []
        self.live_url = None

    def extract_matches(self):
        try:
            wait = WebDriverWait(self.driver, 20)
            wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "match-row")))

            match_elements = self.driver.find_elements(By.CLASS_NAME, "match-row")

            for match in match_elements:
                try:
                    time_text = match.find_element(By.CLASS_NAME, "clock-time").text.strip()    
                    match_hour_minute = datetime.strptime(time_text, "%H:%M").time()

                    # Get current date in Nigeria
                    nigeria_now = datetime.now(NIGERIA_TZ)
                    match_datetime = datetime.combine(nigeria_now.date(), match_hour_minute, tzinfo=NIGERIA_TZ)

                    # Handle past match times that are actually scheduled for the next day
                    if match_datetime < nigeria_now:
                        match_datetime += timedelta(days=1)

                    home_player = match.find_element(By.CLASS_NAME, "home-team").text.strip()
                    away_player = match.find_element(By.CLASS_NAME, "away-team").text.strip()

                    odds = match.find_elements(By.CLASS_NAME, "m-outcome-odds")
                    home_odd = odds[0].text.strip() if len(odds) > 0 else "N/A"
                    away_odd = odds[1].text.strip() if len(odds) > 1 else "N/A"

                    match_info = {
                        "home_player": home_player,
                        "away_player": away_player,
                        "start_time": match_datetime,
                        "home_odd": home_odd,
                        "away_odd": away_odd,
                        "element": match
                    }

                    self.matches.append(match_info)
                except Exception as inner_e:
                    print(f"[WARNING] Skipped a match due to error: ")
        except Exception as e:
            print(f"[ERROR] Failed to extract matches: ")


    def click_earliest_match(self):
        if not self.matches:
            print("[INFO] No matches found.")
            return None
        print(f"[INFO] Found {len(self.matches)} matches.")
        
        # Sort matches by start time and select the earliest one
        self.matches.sort(key=lambda x: x["start_time"])
        earliest_match = self.matches[0]
        self.matches = []  # Clear matches after clicking

        print(f"[INFO] Clicking earliest match: {earliest_match['home_player']} vs {earliest_match['away_player']} at {earliest_match['start_time'].strftime('%H:%M')}")

        try:
            self.driver.execute_script("arguments[0].scrollIntoView();", earliest_match['element'])
            teams_element = earliest_match['element'].find_element(By.CLASS_NAME, "teams")
            self.driver.execute_script("arguments[0].click();", teams_element)
            print("[INFO] Click successful.")
        except Exception as e:
            print(f"[ERROR] Failed to click match: {str(e)}")
            return None
        
        # Return match info for verification

        return {
            "home_player": earliest_match["home_player"],
            "away_player": earliest_match["away_player"],
            "start_time": earliest_match["start_time"].strftime("%Y-%m-%d %H:%M")
        }
    
    def insert_live_into_url(self, url):
        """
        Inserts 'live' into the SportyBet URL path after the 'sport/.../' segment.
        Example:
        https://www.sportybet.com/ng/sport/tableTennis/... ->
        https://www.sportybet.com/ng/sport/tableTennis/live/...
        """
        url_parts = url.split('/')
        if 'live' not in url_parts:
            try:
                sport_index = url_parts.index('sport')
                url_parts.insert(sport_index + 2, 'live')
                return '/'.join(url_parts)
            except ValueError:
                print("[WARN] 'sport' not found in the URL.")
        return url
    
    def wait_until_match_start(self, start_time_str):
        """Wait until 10 seconds before the match start time"""
        now = datetime.now()
        start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M")
        wait_seconds = (start_time - now).total_seconds() # 30 seconds before match start
        
        if wait_seconds > 0:
            print(f"[INFO] Waiting {wait_seconds:.1f} seconds until 30s before match start")
            time.sleep(wait_seconds)

    def verify_live_teams(self, match_info):
        """Verify team names on live match page"""
        try:
            wait = WebDriverWait(self.driver, 10)  # Wait up to 10 seconds
            title_element = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h4.m-tracker-title span"))
            )
            
            full_match_text = title_element.text.strip()
            print(f"[DEBUG] Found match title text: {full_match_text}")

            if " vs " in full_match_text:
                home_team_live, away_team_live = [team.strip() for team in full_match_text.split(" vs ")]

                if (home_team_live == match_info["home_player"] and 
                    away_team_live == match_info["away_player"]):
                    print("[INFO] Team names verified on live page")
                    return True

            print("[WARNING] Team names mismatch or not found on live page")

        except Exception as e:
            print(f"[ERROR] Failed to verify teams on live page: {str(e)}")

        return False
    
    def wait_for_game_start(self, timeout=300):
        """Wait until the date in the match header is replaced (i.e., game has started)"""
        print("[INFO] Waiting for game to start...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                # Get the container that holds the date and time
                status_element = self.driver.find_element(
                    By.CSS_SELECTOR,
                    ".sr-lmt-plus-scb__status"
                )
                # Extract all text inside
                status_text = status_element.text.strip()
                
                # Regex to match formats like "19 Apr" or "08 May"
                date_pattern = r"\b\d{1,2} [A-Za-z]{3}\b"
                
                # If it no longer matches a date, game has likely started
                if not re.search(date_pattern, status_text):
                    print("[INFO] Game has started.")
                    return True
            except Exception:
                pass  # Ignore temporary failures

            time.sleep(1)

        print("[WARNING] Game did not start within timeout period.")
        return False
    
    def set_time_filter_to_1h(self, timeout: int = 10):
        try:
            # Wait for the slider items to appear
            slider_items = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "m-slider-piecewise-item"))
            )

            # Loop through items and find the one labeled "1 h"
            for item in slider_items:
                try:
                    label = item.find_element(By.CLASS_NAME, "m-slider-piecewise-label")
                    if label.text.strip() == "1 h":
                        label.click()
                        print("[INFO] Time filter set to 1 h")
                        time.sleep(4)  # Allow time for the page to update
                        return True
                except Exception as e:
                    continue

            print("[WARNING] '1 h' filter not found among slider items.")
            return False

        except Exception as e:
            print(f"[ERROR] Failed to set time filter to 1 h: {e}")
            return False

    def run_FindMatch(self):
        self.login()
        print("[INFO] Logged in, scraping matches now...")
        time.sleep(5)  # Let the page load

        self.set_time_filter_to_1h()
        self.extract_matches()
        match_info = self.click_earliest_match()
        
        if not match_info:
            print("[WARNING] No valid match found, trying again")
            return self.run_FindMatch()

        # Wait until 30 seconds before match start
        self.wait_until_match_start(match_info["start_time"])
        
        # Get live URL
        self.live_url = self.insert_live_into_url(self.driver.current_url)
        self.driver.get(self.live_url)
        print(f"[INFO] Navigated to live URL: {self.live_url}")
        time.sleep(10)  # Allow page to load

        # Verify teams on live page
        if not self.verify_live_teams(match_info) or not self.wait_for_game_start():
            print("[INFO] Retrying to find a valid match...")
            return self.run_FindMatch()
        

        return match_info
