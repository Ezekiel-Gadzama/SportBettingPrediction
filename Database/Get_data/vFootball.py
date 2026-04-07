import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from UI_webscraping.General_Scraping.scrape_data import MarketScraper
from Database.Get_data.Zsport_markets import SPORT_MARKETS
from concurrent.futures import ThreadPoolExecutor, as_completed

def main():
    sport = "vFootball"
    url = f"https://www.sportybet.com/ng/sport/{sport}/upcoming?time=0"
    markets = SPORT_MARKETS[sport]
    
    num_threads = 10  # Number of concurrent browsers/scrapers
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            print(f"[MAIN] Starting scraper {i+1}")
            scraper = MarketScraper(url, markets)
            scraper.thread_index = int(i)
            futures.append(executor.submit(scraper.run, sport)) # Changed this line
        
        # Wait for all threads to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"[MAIN] Thread error: {e}")

if __name__ == "__main__":  # Fixed this line (was "main" in quotes)
    main()
