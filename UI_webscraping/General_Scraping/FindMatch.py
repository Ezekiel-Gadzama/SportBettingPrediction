from Login.login import SportyBetLoginBot
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import time
import sys
import os
from datetime import datetime, timedelta, timezone
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Define Nigeria's timezone
NIGERIA_TZ = timezone(timedelta(hours=1))

class FindMatch(SportyBetLoginBot):
    def __init__(self, url):
        super().__init__(url)
        self.matches = []

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
                    print(f"[WARNING] Skipped a match due to error: {inner_e}")
        except Exception as e:
            print(f"[ERROR] Failed to extract matches: {e}")


    def click_earliest_match(self):
        if not self.matches:
            print("[INFO] No matches found.")
            return None

        self.matches.sort(key=lambda x: x["start_time"])
        earliest_match = self.matches[0]

        print(f"[INFO] Clicking earliest match: {earliest_match['home_player']} vs {earliest_match['away_player']} at {earliest_match['start_time'].strftime('%H:%M')}")

        self.driver.execute_script("arguments[0].scrollIntoView();", earliest_match['element'])

        try:
            teams_element = earliest_match['element'].find_element(By.CLASS_NAME, "teams")
            self.driver.execute_script("arguments[0].click();", teams_element)
            print("[INFO] Click successful.")
        except Exception as e:
            print(f"[ERROR] Failed to click match: {e}")

        return {
            "home_player": earliest_match["home_player"],
            "away_player": earliest_match["away_player"],
            "start_time": earliest_match["start_time"].strftime("%Y-%m-%d %H:%M")
        }
    
    def insert_live_into_url(self, url):
        """
        Inserts 'live' into the SportyBet URL path after the 'sport' segment.
        Example:
        https://www.sportybet.com/ng/sport/tableTennis/... ->
        https://www.sportybet.com/ng/sport/live/tableTennis/...
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


    def run_FindMatch(self):
        self.login()
        print("[INFO] Logged in, scraping matches now...")

        time.sleep(5)  # Let the page load

        self.extract_matches()
        match_info = self.click_earliest_match()
        current_url = self.driver.current_url

        self.url = self.insert_live_into_url(current_url)
        print(f"[INFO] Updated URL: {self.url}")
        self.driver.get(self.url)
        return match_info

