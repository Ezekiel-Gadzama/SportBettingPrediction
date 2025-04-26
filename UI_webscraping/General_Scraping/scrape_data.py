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
        self.scraped_odd = {}
        self.markets_to_scrape = markets_to_scrape
        self.lock = threading.Lock()  # for thread-safe WebDriver access

    def wait_for_market(self, market_name):
        """Wait for a specific market section to appear (e.g., 'Winner')."""
        wait = WebDriverWait(self.driver, 1)
        return wait.until(
            EC.presence_of_element_located((By.XPATH, f"//span[text()='{market_name}']"))
        )
    
    def find_market_without_waiting(self, market_name):
        """Try to find a specific market section (e.g., 'Winner') without waiting."""
        return self.driver.find_element(By.XPATH, f"//span[text()='{market_name}']")

    
    def extract_scores(self, sport_name: str, current_time: int) -> dict:
        try:
            if sport_name == "tableTennis":
                return self.extract_table_tennis_scores(current_time)
            elif sport_name == "football":
                return self.extract_football_scores(current_time)
            elif sport_name == "vFootball":
                return self.extract_football_scores(current_time)
            # Add more sports as needed
            else:
                print(f"[WARNING] No score extractor implemented for: {sport_name}")
                return {}
        except Exception as e:
            print(f"[ERROR] Failed to extract scores for {sport_name}: ")
            return {}
        
    def extract_table_tennis_scores(self, current_time: int) -> dict:
        scores = {}

        try:
            # Find the scoreboard container
            wrapper = self.driver.find_element(By.CLASS_NAME, "sr-lmt-plus__wrapper")

            # Locate the total score column (where "T" is the title and the scores follow)
            total_score_column = wrapper.find_element(By.CLASS_NAME, "sr-lmt-plus-pd-gen__col-total")
            score_cells = total_score_column.find_elements(By.CLASS_NAME, "sr-lmt-plus-pd-scr__cell")

            if len(score_cells) >= 2:
                try:
                    # Extract home and away scores
                    home_score = int(score_cells[0].text)
                    away_score = int(score_cells[1].text)
                except ValueError:
                    home_score = "N/A"
                    away_score = "N/A"

                scores["match_score_home"] = home_score
                scores["match_score_away"] = away_score
            else:
                scores["match_score_home"] = "N/A"
                scores["match_score_away"] = "N/A"

            # Get set scores
            set_count = 1
            try:
                set_columns = wrapper.find_elements(By.CLASS_NAME, "sr-lmt-plus-pd-gen__col")
                for col in set_columns:
                    try:
                        cells = col.find_elements(By.CLASS_NAME, "sr-lmt-plus-pd-scr__cell")
                        if len(cells) >= 2:
                            home_set_text = cells[0].text.strip()
                            away_set_text = cells[1].text.strip()

                            try:
                                scores[f"set_{set_count}_home_point"] = int(home_set_text)
                            except ValueError:
                                scores[f"set_{set_count}_home_point"] = "N/A"

                            try:
                                scores[f"set_{set_count}_away_point"] = int(away_set_text)
                            except ValueError:
                                scores[f"set_{set_count}_away_point"] = "N/A"

                            set_count += 1
                            if set_count > 5:
                                break
                    except Exception as e:
                        print(f"[WARNING] Failed to extract set {set_count}: ")
                        continue
            except Exception as e:
                print(f"[ERROR] extracting set scores: ")

            # Pad with N/A if less than 5 sets were found
            while set_count <= 5:
                scores[f"set_{set_count}_home_point"] = "N/A"
                scores[f"set_{set_count}_away_point"] = "N/A"
                set_count += 1

        except Exception as e:
            print(f"[ERROR] extract_table_tennis_scores: ")

        return scores
    
    def extract_football_scores(self, current_time: int) -> dict:
        """Extracts scores from both real and virtual football matches"""
        scores = {"match_score_home": "N/A", "match_score_away": "N/A"}
        
        try:
            # First try virtual football format
            try:
                score_element = self.driver.find_element(By.CLASS_NAME, "score")
                score_text = score_element.text.strip()
                if ':' in score_text:
                    home_score, away_score = score_text.split(':')
                    scores["match_score_home"] = int(home_score.strip())
                    scores["match_score_away"] = int(away_score.strip())
                    return scores
            except:
                pass  # Not virtual football, try real football format
            
            # Real football format
            score_container = self.driver.find_element(By.CLASS_NAME, "sr-lmt-plus-scb__result")
            home_score_element = score_container.find_element(By.CLASS_NAME, "srm-team1")
            away_score_element = score_container.find_element(By.CLASS_NAME, "srm-team2")
            
            scores["match_score_home"] = int(home_score_element.text.strip())
            scores["match_score_away"] = int(away_score_element.text.strip())
            
        except Exception as e:
            print(f"[ERROR] extract_football_scores: {str(e)}")
        
        return scores



    def extract_market_odds(self, market_name):
        """Extract and convert odds for a given market to implied probabilities, while also storing raw odds."""
        try:
            with self.lock:  # thread-safe access to the driver
                print(f"[INFO] Searching for '{market_name}' market...")
                market_header = self.find_market_without_waiting(market_name)

                wrapper = market_header.find_element(
                    By.XPATH, "./ancestor::div[contains(@class, 'm-table__wrapper')]"
                )
                outcome_rows = wrapper.find_elements(By.CLASS_NAME, "m-outcome")

            market_odds = []
            raw_odds = []  # To store the raw odds before conversion
            
            for row in outcome_rows:
                cells = row.find_elements(By.CLASS_NAME, "m-table-cell")
                for cell in cells:
                    items = cell.find_elements(By.CLASS_NAME, "m-table-cell-item")
                    if len(items) >= 2:
                        label = items[0].text.strip()
                        odd_str = items[1].text.strip()
                        try:
                            odd = float(odd_str)
                            market_odds.append((label, odd))
                            raw_odds.append((label, odd))  # Store raw odds
                            
                            # Store home/away/draw odds for betting logic
                            if "home" in label.lower():
                                self.current_home_odd = odd
                            elif "away" in label.lower():
                                self.current_away_odd = odd
                            elif "draw" in label.lower():
                                self.current_draw_odd = odd
                                
                        except ValueError:
                            print(f"[WARNING] Skipping invalid odd: {odd_str}")
                            market_odds.append((label, "N/A"))
                            raw_odds.append((label, "N/A"))

            # Store raw odds in scraped_odd
            self.scraped_odd[market_name] = raw_odds

            # Calculate implied probabilities and normalize
            total_inverse = sum(1 / odd for _, odd in market_odds if isinstance(odd, (int, float)) and odd > 0)
            market_probs = []
            for label, odd in market_odds:
                if isinstance(odd, (int, float)) and odd > 0:
                    implied_prob = (1 / odd) / total_inverse
                    market_probs.append((label, round(implied_prob, 4)))  # Rounded for readability
                else:
                    market_probs.append((label, odd))  # Keep "N/A" values

            self.scraped_data[market_name] = market_probs
            print(f"[INFO] Market: {market_name}")
            for label, prob in market_probs:
                if isinstance(prob, (int, float)):
                    print(f"  {label}: {prob:.2%}")  # Display as percentage for easier reading
                else:
                    print(f"  {label}: {prob}")  # For "N/A" values

        except Exception as e:
            print(f"[ERROR] Failed to extract {market_name} market: {str(e)}")


    def extract_data(self, market_list, sport_name, current_time):
        """Extract markets and scores in parallel using threads."""
        futures = []
        with ThreadPoolExecutor(max_workers=len(market_list) + 1) as executor:
            # Submit market scraping tasks
            futures.extend(executor.submit(self.extract_market_odds, market) for market in market_list)
            # Submit score extraction as an additional task
            futures.append(executor.submit(self.extract_scores, sport_name, current_time))

            results = {}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if isinstance(result, dict):
                        results.update(result)
                except Exception as e:
                    print(f"[THREAD ERROR] {e}")
        
        return results
    
    def has_data_changed(self, current_data, previous_data):
        """Compare current data with previous data, ignoring timestamp."""
        if previous_data is None:
            return True
            
        # Compare all fields except timestamp
        keys_to_compare = [k for k in current_data.keys() if k != "Timestamp"]
        for key in keys_to_compare:
            if key not in previous_data or current_data[key] != previous_data[key]:
                return True
        return False
    
    def is_match_ended(self):
        """Check if match has ended - works for both real and virtual football"""
        try:
            # First try virtual football format
            try:
                time_element = self.driver.find_element(By.CLASS_NAME, "time")
                time_text = time_element.text.strip().upper()
                if "ENDED" in time_text:
                    print('Virtual football match ended')
                    return True
                
                # Also check the match-info-title in virtual football
                try:
                    match_info = self.driver.find_element(By.CLASS_NAME, "match-info-title")
                    if "MATCH ENDED" in match_info.text.strip().upper():
                        print('Virtual football match ended through alternative')
                        return True
                except:
                    pass
                
            except:
                pass  # Not virtual football, try real football format
            
            # Real football format check
            try:
                status_div = self.driver.find_element(By.CLASS_NAME, "sr-lmt-plus-scb__status")
                ended_div = status_div.find_element(By.CLASS_NAME, "srm-is-uppercase")
                if "ended" in ended_div.text.strip().lower():
                    print('real football match ended')
                    return True
                else:
                    return False
            except:
                pass
                
            # Additional check for "Ended" in any visible text
            page_text = self.driver.page_source.upper()
            if "ENDED" in page_text or "MATCH ENDED" in page_text:
                return True
                
        except Exception as e:
            print(f"[ERROR] is_match_ended failed: {str(e)}")
        
        return False
    
    def _write_complete_row(self, fieldnames: list, row_data: dict, file_path: str):
        """
        Writes a complete row of data to the specified CSV file.
        Automatically adds new columns if they are not already in the CSV.
        """
        file_exists = os.path.isfile(file_path)

        # If file exists, read existing header
        existing_fieldnames = None
        if file_exists:
            with open(file_path, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                existing_fieldnames = reader.fieldnames or []

        # Handle case where existing_fieldnames is None
        if existing_fieldnames is None:
            existing_fieldnames = []

        # Detect and add new columns if any
        new_columns = [col for col in fieldnames if col not in existing_fieldnames]
        all_columns = existing_fieldnames + [col for col in fieldnames if col not in existing_fieldnames]

        if new_columns:
            print(f"[INFO] New columns detected: {set(new_columns)}")

            # Read old data
            old_data = []
            if file_exists:
                with open(file_path, mode='r', newline='', encoding='utf-8') as file:
                    reader = csv.DictReader(file)
                    old_data = list(reader)

            # Write new file with updated headers
            with open(file_path, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=all_columns)
                writer.writeheader()

                # Rewrite old data
                for row in old_data:
                    # Fill missing new fields with "N/A"
                    for col in new_columns:
                        row[col] = "N/A"
                    writer.writerow(row)

        # Write the new row
        with open(file_path, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=all_columns)
            # Ensure all required fields are in the row
            complete_row = {col: row_data.get(col, "N/A") for col in all_columns}
            writer.writerow(complete_row)


    def run(self, sport_name: str):
        # Create folders and paths
        folder = os.path.join("Database", "Data")
        os.makedirs(folder, exist_ok=True)
        long_path = os.path.join(folder, f"{sport_name.lower().replace(' ', '_')}_long_format.csv")
        print(f"[INFO] Long format CSV path: {long_path}")
        print("Current working directory:", os.getcwd())
        print(f"Writing to file: {os.path.abspath(long_path)}")

        while True:
            self.matches = []
            # Match setup - get new match info
            match_info = self.run_FindMatch()
            print("[INFO] Logged in and match clicked.")
            match_id = self.extract_match_id_from_url(self.driver.current_url)

            # Base row with match information - now includes match_finished
            base_row = {
                "Match ID": match_id,
                "Team A": match_info["home_player"],
                "Team B": match_info["away_player"],
                "Start Time": match_info["start_time"],
                "match_finished": False
            }

            start_time = time.time()
            collected_data = {}
            previous_data = None
            refresh_count = 0
            same_data_count = 0
            MAX_REFRESHES = 3
            WRITE_THRESHOLD = 10
            all_columns_seen = set(base_row.keys()) | {"Timestamp"}
            match_ended = False

            fieldnames = list(base_row.keys()) + ["Timestamp"]

            while not match_ended or not self.is_match_ended():
                loop_start = time.time()
                current_time = int(loop_start - start_time)

                current_match_status = self.is_match_ended()
                if current_match_status and not match_ended:
                    print(f"[INFO] Match {match_id} has ended. Processing final data...")
                    match_ended = True
                    base_row["match_finished"] = True
                    print(f"[INFO] Scraping at +{current_time}s (Match ended: {match_ended})")

                self.scraped_data.clear()
                scores_and_odds = self.extract_data(self.markets_to_scrape, sport_name, current_time)

                market_odds_missing = len(self.scraped_data) == 0

                if market_odds_missing and not match_ended:
                    print("[WARNING] Market odds missing - attempting refresh")
                    if refresh_count < MAX_REFRESHES:
                        self.driver.refresh()
                        time.sleep(3)
                        refresh_count += 1
                        continue
                    else:
                        print("[WARNING] Max refreshes reached without finding market odds")
                        refresh_count = 0
                else:
                    refresh_count = 0

                current_data = {}
                for market, outcomes in self.scraped_data.items():
                    for label, odd in outcomes:
                        if not label:
                            continue
                        key = f"{market.lower().replace(' ', '_')}_{label.lower().replace(' ', '_')}"
                        current_data[key] = odd

                for key, val in scores_and_odds.items():
                    current_data[key] = val

                new_columns = set(current_data.keys()) - all_columns_seen
                if new_columns:
                    print(f"[INFO] New columns detected: {new_columns}")
                    all_columns_seen.update(new_columns)
                    fieldnames.extend(col for col in sorted(new_columns) if col not in fieldnames)

                if self.has_data_changed(current_data, previous_data) or (match_ended and previous_data is None):
                    print(f"[INFO] Data changed at +{current_time}s - writing to file")
                    same_data_count = 0
                    refresh_count = 0

                    collected_data = current_data.copy()
                    previous_data = current_data.copy()

                elif same_data_count >= WRITE_THRESHOLD:
                    print(f"[INFO] Same data for {same_data_count} times — writing again and refreshing once")
                    collected_data = previous_data.copy() if previous_data else {}

                    if refresh_count < MAX_REFRESHES:
                        self.driver.refresh()
                        time.sleep(3)
                        refresh_count += 1
                        same_data_count = 0
                    else:
                        print("[WARNING] Max refreshes reached due to stagnant data")
                        refresh_count = 0

                else:
                    same_data_count += 1
                    print(f"[INFO] Data unchanged ({same_data_count} times in a row)")
                    collected_data = previous_data.copy() if previous_data else {}

                complete_row = {**base_row, "Timestamp": current_time}
                for col in all_columns_seen:
                    if col not in complete_row:
                        complete_row[col] = collected_data.get(col, "N/A")
                self._write_complete_row(list(all_columns_seen), complete_row, long_path)

                if not match_ended:
                    elapsed = time.time() - loop_start
                    if elapsed < 1:
                        time.sleep(1 - elapsed)
                else:
                    if current_match_status:
                        break

            print(f"[INFO] Finished processing match {match_id}. Moving to the next match...")


