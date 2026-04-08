from Login.login import SportyBetLoginBot
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
import time
import sys
import os
import re
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from threading import Lock
from queue import Queue
import time

# SportyBet upcoming pages display time in the browser/server's local timezone.
# So we interpret HH:MM using the machine's local timezone and only roll to the next day
# when the time is clearly in the past (with a small grace window).
START_TIME_PAST_GRACE_SECONDS = 120  # tolerate up to 2 minutes of drift

# Global variables for match tracking
global_claimed_matches = set()  # Now stores match names instead of IDs
global_matches_lock = Lock()

class FindMatch(SportyBetLoginBot):
    def __init__(self, url):
        super().__init__(url)
        self.matches = []
        self.live_url = None

    def extract_matches(self):
        try:
            wait = WebDriverWait(self.driver, 20)
            # Wait for the main container to load
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "import-match")))

            # Get all league containers (match-league-wrap)
            league_containers = self.driver.find_elements(By.CLASS_NAME, "match-league-wrap")

            for league in league_containers:
                # Find all match tables inside each league
                match_tables = league.find_elements(By.CLASS_NAME, "match-table")

                for table in match_tables:
                    # Find all match rows inside each table
                    match_rows = table.find_elements(By.CLASS_NAME, "match-row")

                    for match in match_rows:
                        try:
                            # Skip if it's a date row (e.g., "30/04 Wednesday")
                            if "date-row" in match.get_attribute("class"):
                                continue

                            time_text = match.find_element(By.CLASS_NAME, "clock-time").text.strip()
                            match_hour_minute = datetime.strptime(time_text, "%H:%M").time()

                            # Interpret the displayed HH:MM in local machine timezone.
                            local_now = datetime.now().astimezone()
                            match_datetime = datetime.combine(
                                local_now.date(),
                                match_hour_minute,
                                tzinfo=local_now.tzinfo,
                            )

                            # Handle past match times (next day)
                            if (local_now - match_datetime).total_seconds() > START_TIME_PAST_GRACE_SECONDS:
                                match_datetime += timedelta(days=1)

                            home_player = match.find_element(By.CLASS_NAME, "home-team").text.strip()
                            away_player = match.find_element(By.CLASS_NAME, "away-team").text.strip()

                            # Unique match identifier
                            match_identifier = f"{home_player} vs {away_player} | {match_datetime.strftime('%Y-%m-%d %H:%M')}"

                            # Get odds (home, draw, away)
                            odds = match.find_elements(By.CLASS_NAME, "m-outcome-odds")
                            home_odd = odds[0].text.strip() if len(odds) > 0 else "N/A"
                            draw_odd = odds[1].text.strip() if len(odds) > 1 else "N/A"
                            away_odd = odds[2].text.strip() if len(odds) > 2 else "N/A"

                            # Get over 2.5 odds (from the second market)
                            market_cells = match.find_elements(By.CLASS_NAME, "market-cell")
                            over_2_5_odd = 0
                            
                            if len(market_cells) > 0:
                                over_markets = market_cells[0].find_elements(By.CLASS_NAME, "m-market")
                                if len(over_markets) > 1:
                                    # Check if this is actually the over/under market (specifier = 2.5)
                                    specifier_element = over_markets[1].find_element(By.CLASS_NAME, "af-select-input")
                                    if specifier_element and "2.5" in specifier_element.text.strip():
                                        over_odds = over_markets[1].find_elements(By.CLASS_NAME, "m-outcome-odds")
                                        if len(over_odds) >= 2:
                                            try:
                                                over_2_5_odd = float(over_odds[0].text.strip()) 
                                            except (ValueError, IndexError):
                                                over_2_5_odd = 0

                            # Only add match if over 2.5 odds >= 2
                            if over_2_5_odd >= 2 and over_2_5_odd <= 2.4:
                                match_info = {
                                    "home_player": home_player,
                                    "away_player": away_player,
                                    "start_time": match_datetime,
                                    "home_odd": home_odd,
                                    "draw_odd": draw_odd,
                                    "away_odd": away_odd,
                                    "over_2_5_odd": over_2_5_odd,
                                    "element": match,
                                    "match_identifier": match_identifier,
                                }

                                self.matches.append(match_info)
                        except Exception as inner_e:
                            print(f"[WARNING] Skipped a match due to error: {inner_e}")

            # Sort matches by start time
            self.matches.sort(key=lambda x: x["start_time"])

        except Exception as e:
            print(f"[ERROR] Failed to extract matches: {e}")


    def extract_match_id_from_url(self, url):
        """Extract numeric match ID from URL"""
        try:
            # Example URL: https://www.sportybet.com/ng/sport/tableTennis/sr:match:12345678
            match = re.search(r'sr:match:(\d+)', url)
            return match.group(1) if match else None
        except Exception as e:
            print(f"[WARNING] Failed to extract ID from URL: {url} - ")
            return None

    def get_next_unique_match(self):
        """Thread-safe method to get next available match using match identifiers"""
        with global_matches_lock:
            for match in self.matches:
                if match["match_identifier"] not in global_claimed_matches:
                    global_claimed_matches.add(match["match_identifier"])
                    return match
            return None

    def click_earliest_match(self):
        match = self.get_next_unique_match()
        if not match:
            print("[INFO] No unclaimed matches available.")
            return None

        print(f"[INFO] Claiming match: {match['match_identifier']}")

        try:
            # Scroll to center of viewport for better click reliability
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", 
                match['element']
            )
            time.sleep(0.5)  # Allow scroll to complete
            
            teams_element = match['element'].find_element(By.CLASS_NAME, "teams")
            self.driver.execute_script("arguments[0].click();", teams_element)
            print("[INFO] Click successful.")
            
            # Now that we've clicked, we can get the match ID from the URL
            current_url = self.driver.current_url
            match_id = self.extract_match_id_from_url(current_url)
            if "simulated" in current_url.lower():
                print(f"[INFO] Skipping simulated match: {match['match_url']}")
                return None
            
            if match_id:
                print(f"[INFO] Found match ID: {match_id}")
                return {
                    "home_player": match["home_player"],
                    "away_player": match["away_player"],
                    "start_time": match["start_time"].strftime("%Y-%m-%d %H:%M"),
                    "match_id": match_id,
                    "match_url": current_url,
                    "match_identifier": match["match_identifier"]  # Include the identifier
                }
            else:
                print("[WARNING] Could not extract match ID from URL")
                return None
                
        except Exception as e:
            print(f"[ERROR] Failed to click match: {str(e)}")
            # with global_matches_lock:
            #     global_claimed_matches.discard(match["match_identifier"])
            return None

    def verify_live_teams(self, match_info, max_attempts=100, refresh_interval=4):
        """Continuously refresh and verify teams for up to 300 seconds"""
        count = 0
        attempt = 0
        
        while count < max_attempts:
            count += 1
            attempt += 1
            print(f"[INFO] Verification attempt {attempt} and count ({count} elapsed)")
            
            try:
                # Refresh the page every 3 attempts
                if attempt % 3 == 0:
                    self.driver.refresh()
                    time.sleep(2)  # Allow page to load after refresh
                
                wait = WebDriverWait(self.driver, 10)
                title_element = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "h4.m-tracker-title span")))
                
                full_match_text = title_element.text.strip()
                if " vs " in full_match_text:
                    home_team_live, away_team_live = [team.strip() for team in full_match_text.split(" vs ")]
                    if (home_team_live == match_info["home_player"] and 
                        away_team_live == match_info["away_player"]):
                        print("[INFO] Team names verified on live page")
                        return True

            except Exception as e:
                print(f"[WARNING] Verification attempt failed: ")
            
            time.sleep(refresh_interval)

        print("[ERROR] Failed to verify teams within timeout period")
        return False

    def wait_for_game_start(self, timeout=100, refresh_interval=4):
        """Wait until game starts with periodic refreshing.
        Works for both real and virtual football matches."""
        print("[INFO] Waiting for game to start with refreshing...")
        count = 0
        attempt = 0
        
        while count < timeout:
            count += 1
            attempt += 1
            print(f"[INFO] wait_for_game_start attempt {attempt}, {count*refresh_interval} seconds elapsed")
            
            try:
                # Refresh every 3 attempts
                if attempt % 3 == 0:
                    self.driver.refresh()
                    time.sleep(5)  # Extra time after refresh

                # First try virtual football detection
                try:
                    time_element = self.driver.find_element(By.CLASS_NAME, "time")
                    time_text = time_element.text.strip()
                    if '|' in time_text:  # Virtual football format found
                        period, clock = [part.strip() for part in time_text.split('|')]
                        if ':' in clock:  # Valid time format
                            print("[INFO] Virtual game has started!")
                            return True
                except:
                    pass  # Not virtual football, try real football check
                
                # Real football detection (original logic)
                status_element = self.driver.find_element(
                    By.CSS_SELECTOR, ".sr-lmt-plus-scb__status")
                status_text = status_element.text.strip()
                
                date_pattern = r"\b\d{1,2} [A-Za-z]{3}\b"
                if not re.search(date_pattern, status_text):
                    print("[INFO] Real game has started!")
                    return True
                
            except Exception as e:
                print(f"[WARNING] Game start check failed: ")
            
            time.sleep(refresh_interval)

        print("[ERROR] Game did not start within timeout period")
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
                        time.sleep(10)  # Allow time for the page to update
                        return True
                except Exception as e:
                    continue

            print("[WARNING] '1 h' filter not found among slider items.")
            return False

        except Exception as e:
            print(f"[ERROR] Failed to set time filter to 1 h: ")
            return False
        
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
                print("URL found")
                return '/'.join(url_parts)
            except ValueError:
                print("[WARN] 'sport' not found in the URL.")
        return url
    
    def wait_until_match_start(self, start_time_str):
        """Wait until 120 seconds before the match start time"""
        now = datetime.now()
        start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M")
        wait_seconds = (start_time - now).total_seconds() - 120 # 30 seconds before match start
        
        if wait_seconds > 0:
            print(f"[INFO] Waiting {wait_seconds:.1f} seconds until 120s before match start")
            time.sleep(wait_seconds)

    def run_FindMatch(self):
        self.matches = []
        self.login()
        print("[INFO] Logged in, scraping matches now...")
        time.sleep(10)  # Let the page load

        self.set_time_filter_to_1h()
        self.extract_matches()
        print(f"lenght of matches: {len(self.matches)}")
        match_info = self.click_earliest_match()
        
        if not match_info:
            print("[WARNING] No valid match found, trying again")
            return self.run_FindMatch()

        # Wait until 30 seconds before match start
        self.wait_until_match_start(match_info["start_time"])
        
        # Get live URL
        self.live_url = self.insert_live_into_url(self.driver.current_url)
        try:
            self.driver.get(self.live_url)
            print("Opened live url")
        except:
            return self.run_FindMatch()

        print(f"[INFO] Navigated to live URL: {self.live_url}")
        time.sleep(10)  # Allow page to load

        # Verify teams on live page
        if not self.verify_live_teams(match_info) or not self.wait_for_game_start():
            print("[INFO] Retrying to find a valid match...")
            with global_matches_lock:
                global_claimed_matches.discard(match_info["match_identifier"])
            return self.run_FindMatch()
        return match_info