import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from concurrent.futures import ThreadPoolExecutor, as_completed
from UI_webscraping.General_Scraping.scrape_data import MarketScraper
from Database.Get_data.Zsport_markets import SPORT_MARKETS
from Database.Get_data.script_logging import tee_stdout_stderr_to_log

if __name__ == "__main__":
    tee_stdout_stderr_to_log(__file__)
    sport = "badminton"
    url = f"https://www.sportybet.com/ng/sport/{sport}/upcoming?time=0"
    markets = SPORT_MARKETS[sport]
    num_threads = 10  # Number of concurrent browsers/scrapers

    def _worker(idx: int):
        s = MarketScraper(url, markets)
        s.thread_index = int(idx)
        s.run(sport_name=sport)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(_worker, i) for i in range(num_threads)]
        for f in as_completed(futures):
            f.result()
