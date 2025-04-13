import sys
import os
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from UI_webscraping.General_Scraping.FindMatch import FindMatch


class MarketScraper(FindMatch):
    def __init__(self, url, markets_to_scrape):
        super().__init__(url)
        self.scraped_data = {}
        self.markets_to_scrape = markets_to_scrape

    def wait_for_market(self, market_name):
        """Wait for a specific market section to appear (e.g., 'Winner')."""
        wait = WebDriverWait(self.driver, 20)
        return wait.until(
            EC.presence_of_element_located((By.XPATH, f"//span[text()='{market_name}']"))
        )

    def extract_market_odds(self, market_name):
        """General method to extract odds for a given market."""
        try:
            print(f"[INFO] Searching for '{market_name}' market...")
            market_header = self.wait_for_market(market_name)

            # Find the wrapper containing the market data
            wrapper = market_header.find_element(
                By.XPATH, "./ancestor::div[contains(@class, 'm-table__wrapper')]"
            )

            # Find the outcome row(s)
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
        """Loop through and extract all desired markets."""
        for market_name in market_list:
            self.extract_market_odds(market_name)
            time.sleep(1)  # Slight delay to avoid being too aggressive

    def run(self):
        self.run_FindMatch()
        print("[INFO] Logged in and match clicked.")
        time.sleep(5)  # Wait for market page to load

        print("[INFO] Extracting markets...")
        self.extract_multiple_markets(self.markets_to_scrape)


