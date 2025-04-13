import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import csv
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from UI_webscraping.General_Scraping.FindMatch import FindMatch
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class MarketScraper(FindMatch):
    def __init__(self, url, markets_to_scrape):
        super().__init__(url)
        self.scraped_data = {}
        self.markets_to_scrape = markets_to_scrape
        self.lock = threading.Lock()  # for thread-safe WebDriver access

    def wait_for_market(self, market_name):
        """Wait for a specific market section to appear (e.g., 'Winner')."""
        wait = WebDriverWait(self.driver, 1)
        return wait.until(
            EC.presence_of_element_located((By.XPATH, f"//span[text()='{market_name}']"))
        )

    def extract_market_odds(self, market_name):
        """General method to extract odds for a given market."""
        try:
            with self.lock:  # thread-safe access to the driver
                print(f"[INFO] Searching for '{market_name}' market...")
                market_header = self.wait_for_market(market_name)

                wrapper = market_header.find_element(
                    By.XPATH, "./ancestor::div[contains(@class, 'm-table__wrapper')]"
                )
                outcome_rows = wrapper.find_elements(By.CLASS_NAME, "m-outcome")

            market_odds = []
            for row in outcome_rows:
                cells = row.find_elements(By.CLASS_NAME, "m-table-cell")
                for cell in cells:
                    items = cell.find_elements(By.CLASS_NAME, "m-table-cell-item")
                    if len(items) >= 2:
                        label = items[0].text.strip()
                        odd = items[1].text.strip()
                        market_odds.append((label, odd))

            self.scraped_data[market_name] = market_odds
            print(f"[INFO] Market: {market_name}")
            for label, odd in market_odds:
                print(f"  {label}: {odd}")

        except Exception as e:
            print(f"[ERROR] Failed to extract {market_name} market: {e}")


    def extract_multiple_markets(self, market_list):
        """Extract markets in parallel using threads."""
        with ThreadPoolExecutor(max_workers=len(market_list)) as executor:
            futures = [executor.submit(self.extract_market_odds, market) for market in market_list]
            for future in as_completed(futures):
                try:
                    future.result()  # Raise any exceptions
                except Exception as e:
                    print(f"[THREAD ERROR] {e}")


    def run(self, live_data_csv, duration=5):
        match_info = self.run_FindMatch()
        print("[INFO] Logged in and match clicked.")
        time.sleep(duration)

        # Base info (match details)
        base_row = {
            "Team A": match_info["home_player"],
            "Team B": match_info["away_player"],
            "Start Time": match_info["start_time"],
        }

        start_time = time.time()
        collected_data = {}  # this will be the evolving dictionary of all columns

        while True:
            current_time = int(time.time() - start_time)
            print(f"[INFO] Scraping at +{current_time}s")

            self.scraped_data.clear()
            self.extract_multiple_markets(self.markets_to_scrape)

            # Add scraped values with dynamic keys (e.g., winner_home_0s, etc.)
            for market, outcomes in self.scraped_data.items():
                for label, odd in outcomes:
                    if not label:  # skip empty labels
                        continue
                    key = f"{market.lower().replace(' ', '_')}_{label.lower().replace(' ', '_')}_{current_time}"
                    collected_data[key] = odd

            # Combine match info and all collected market data into one row
            full_row = {**base_row, **collected_data}

            # Write to CSV each second
            file_exists = os.path.isfile(live_data_csv)
            with open(live_data_csv, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=full_row.keys())
                writer.writeheader()
                writer.writerow(full_row)

            print(f"[INFO] Wrote updated data at +{current_time}s")





