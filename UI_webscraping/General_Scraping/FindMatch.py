from Login.login import SportyBetLoginBot
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import time
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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
                    match_time = datetime.strptime(time_text, "%H:%M")

                    home_player = match.find_element(By.CLASS_NAME, "home-team").text.strip()
                    away_player = match.find_element(By.CLASS_NAME, "away-team").text.strip()

                    odds = match.find_elements(By.CLASS_NAME, "m-outcome-odds")
                    home_odd = odds[0].text.strip() if len(odds) > 0 else "N/A"
                    away_odd = odds[1].text.strip() if len(odds) > 1 else "N/A"

                    match_info = {
                        "home_player": home_player,
                        "away_player": away_player,
                        "start_time": match_time,
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
            return

        # Sort matches by time (earliest first)
        self.matches.sort(key=lambda x: x["start_time"])
        earliest_match = self.matches[0]

        print(f"[INFO] Clicking earliest match: {earliest_match['home_player']} vs {earliest_match['away_player']} at {earliest_match['start_time'].strftime('%H:%M')}")

        # Scroll into view
        self.driver.execute_script("arguments[0].scrollIntoView();", earliest_match['element'])

        try:
            # Try clicking a more specific inner element (like .teams)
            teams_element = earliest_match['element'].find_element(By.CLASS_NAME, "teams")
            self.driver.execute_script("arguments[0].click();", teams_element)
            print("[INFO] Click successful.")
        except Exception as e:
            print(f"[ERROR] Failed to click match: {e}")


    def run_FindMatch(self):
        self.login()
        print("[INFO] Logged in, scraping matches now...")

        time.sleep(5)  # Let the page load

        self.extract_matches()
        self.click_earliest_match()

        # Optional: Print match data for verification
        for match in self.matches:
            print(match)

