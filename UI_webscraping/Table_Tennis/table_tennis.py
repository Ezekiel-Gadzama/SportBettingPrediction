import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from UI_webscraping.General_Scraping.scrape_data import MarketScraper


if __name__ == "__main__":
    url = "https://www.sportybet.com/ng/sport/tableTennis/upcoming?time=0"

    # List all markets you want to scrape
    markets = ["Winner", "1st game - winner", "Correct Score"]
    scraper = MarketScraper(url, markets)
    scraper.run()
