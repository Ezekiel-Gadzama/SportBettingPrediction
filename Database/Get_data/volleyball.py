import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from UI_webscraping.General_Scraping.scrape_data import MarketScraper
from Database.Get_data.Zsport_markets import SPORT_MARKETS

if __name__ == "__main__":
    sport = "volleyball"
    url = f"https://www.sportybet.com/ng/sport/{sport}/upcoming?time=0"
    markets = SPORT_MARKETS[sport]
    scraper = MarketScraper(url, markets)
    scraper.run(sport_name=sport)
