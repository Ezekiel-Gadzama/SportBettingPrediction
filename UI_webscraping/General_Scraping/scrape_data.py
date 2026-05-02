import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import csv
import sys
import os

# Project root so `Database.Get_data.*` imports work when this module is loaded first.
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from Database.Get_data import data_file_manager as _dfm
from UI_webscraping.General_Scraping.FindMatch import FindMatch
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import hashlib

class MarketScraper(FindMatch):
    def __init__(self, url, markets_to_scrape):
        super().__init__(url)
        self.scraped_data = {}
        self.scraped_odd = {}
        self.markets_to_scrape = markets_to_scrape
        self.lock = threading.Lock()  # for thread-safe WebDriver access
        self.thread_index = 0

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
            elif sport_name == "eFootball":
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
            print(f"[ERROR] extract_football_scores: ")
        
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
            print(f"[ERROR] Failed to extract {market_name} market: ")


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
        """Check if match has ended - works for real football, virtual football, and eFootball"""
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
                pass  # Not virtual football, try other formats
            
            # Real football and eFootball format check
            try:
                status_div = self.driver.find_element(By.CLASS_NAME, "sr-lmt-plus-scb__status")
                
                # Check for eFootball/real football ended status
                ended_divs = status_div.find_elements(By.XPATH, "./div")
                if ended_divs:
                    # First check the first div (contains "Ended" in eFootball)
                    if "ended" in ended_divs[0].text.strip().lower():
                        print('eFootball/real football match ended')
                        return True
                    
                    # Also check all divs in case the structure varies
                    for div in ended_divs:
                        if "ended" in div.text.strip().lower():
                            print('eFootball/real football match ended (alternative check)')
                            return True
            except:
                pass
                
            # Additional check for HT/FT indicator in eFootball/real football
            try:
                result_period = self.driver.find_element(By.CLASS_NAME, "sr-lmt-plus-scb__result-period")
                if "FT" in result_period.text.strip().upper():
                    print('Match ended (FT indicator found)')
                    return True
            except:
                pass
                
        except Exception as e:
            print(f"[ERROR] is_match_ended failed: {e}")
        
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


    def generate_team_id(self, team_name, digits=15):
        """Generate a consistent numeric-only ID with a default of 15 digits.
        
        Args:
            team_name (str): Name of the team.
            digits (int): Desired length of the numeric ID (default: 15).
        
        Returns:
            str: A fixed-length numeric ID (padded with leading zeros if needed).
        """
        normalized_name = team_name.strip().lower().encode('utf-8')
        hash_obj = hashlib.sha256(normalized_name)
        hex_digest = hash_obj.hexdigest()  # 64-character hex string
        
        # Take the first N hex chars (15 chars = 60 bits, enough for 15-digit decimal)
        hex_subset = hex_digest[:digits]
        
        # Convert hex to integer, then to a zero-padded string
        numeric_id = str(int(hex_subset, 16)).zfill(digits)  # Key fix: use base=16
        
        # Ensure exact length (trim rightmost digits if over, though unlikely)
        return numeric_id[-digits:] if len(numeric_id) > digits else numeric_id
    
    
    def get_football_match_time_in_seconds(self):
        """
        Extracts and returns the current match time in minutes from the match status element.
        Returns 0 if the time cannot be determined or if the match hasn't started.
        """
        try:
            # Find the match status container
            status_container = self.driver.find_element(
                By.CLASS_NAME, "sr-lmt-plus-scb__status"
            )
            
            # Find the clock element
            clock_element = status_container.find_element(
                By.CLASS_NAME, "sr-lmt-plus-scb__clock"
            )
            time_text = clock_element.text.strip()
            
            # Handle different time formats (e.g., "84:37" or "45:00+2:00")
            if ':' in time_text:
                # Split minutes and seconds
                minutes, seconds = time_text.split(':', 1)
                minutes = int(minutes)
                
                # # Handle injury time (e.g., "45:00+2:00")
                # if '+' in seconds:
                #     seconds, injury_time = seconds.split('+', 1)
                #     injury_minutes = injury_time.split(':', 1)[0] if ':' in injury_time else injury_time
                #     return int(minutes) + int(injury_minutes)
                
                return int(minutes * 60) + seconds
            
            return 0  # Default if time format is unexpected
        
        except Exception as e:
            print(f"[ERROR] Could not determine match time: ")
            return 0
        
    def get_vFootball_match_time_in_seconds(self):
        """
        Extracts and returns the current match time in seconds from the match status element.
        Handles formats like "1st | 01:16" or "2nd | 45:00+2:00".
        Returns 0 if the time cannot be determined or if the match hasn't started.
        """
        try:
            # Find the time element in the new structure
            time_element = self.driver.find_element(By.CLASS_NAME, "time")
            time_text = time_element.text.strip()
            
            # Extract the period and time (e.g., "1st | 01:16" -> "01:16")
            if '|' in time_text:
                period, match_time = [part.strip() for part in time_text.split('|')]
            else:
                match_time = time_text
            
            # Handle different time formats (e.g., "01:16" or "45:00+2:00")
            if ':' in match_time:
                # Split minutes and seconds
                minutes_part = match_time.split(':', 1)[0]
                minutes = int(minutes_part) if minutes_part.isdigit() else 0
                
                # Handle injury time if present (e.g., "45:00+2:00")
                if '+' in match_time:
                    regular_time, injury_time = match_time.split('+', 1)
                    minutes, seconds = regular_time.split(':', 1)
                    injury_minutes = injury_time.split(':', 1)[0] if ':' in injury_time else injury_time
                    total_minutes = int(minutes) + int(injury_minutes)
                    return total_minutes * 60  # Return in seconds
                else:
                    # Normal time format (MM:SS)
                    minutes, seconds = match_time.split(':', 1)
                    return (int(minutes) * 60) + int(seconds)
            
            return 0  # Default if time format is unexpected
        
        except Exception as e:
            print(f"[ERROR] Could not determine match time:")
            return 0
        
    def get_efootball_match_time_in_seconds(self):
        """
        Extracts and returns the current match time in seconds from the eFootball match status element.
        Handles formats like "1st | 16:00".
        Returns 0 if the time cannot be determined or if the match hasn't started.
        """
        try:
            # Find the status element that contains the match time
            status_element = self.driver.find_element(By.CLASS_NAME, "sr-lmt-plus-scb__status")
            
            # Get all the child divs within the status element
            status_parts = status_element.find_elements(By.XPATH, "./div")
            
            # The time is in the third div (index 2) based on the HTML structure
            if len(status_parts) >= 3:
                time_text = status_parts[2].text.strip()  # e.g., "16:00"
                
                # Extract period from the first div (e.g., "1st")
                period = status_parts[0].text.strip().lower() if len(status_parts) > 0 else ""
                
                # Handle time format (MM:SS)
                if ':' in time_text:
                    minutes, seconds = time_text.split(':', 1)
                    total_seconds = (int(minutes) * 60) + int(seconds)
                    
                    # Adjust for period (1st half starts at 0, 2nd half at 45*60)
                    if '2nd' in period:
                        total_seconds += 45 * 60  # Add 45 minutes for second half
                    elif '3rd' in period:  # Some eFootball matches might have extra periods
                        total_seconds += 90 * 60
                    elif '4th' in period:
                        total_seconds += 105 * 60
                    
                    return total_seconds
                
            return 0  # Default if time format is unexpected
        
        except Exception as e:
            print(f"[ERROR] Could not determine eFootball match time: {e}")
            return 0
        
    def click_match_tracker(self):
        """
        Clicks the 'Match Tracker' tab in the match interface.
        Uses multiple locator strategies for reliability.
        """
        try:
            # Wait for the Match Tracker tab to be clickable
            match_tracker_tab = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//li[@data-cms-key='match_tracker' and contains(@class, 'm-nav-item')]")
                )
            )
            match_tracker_tab.click()
            print("[INFO] Successfully clicked on Match Tracker")
            return True
        except Exception as e:
            print(f"[ERROR] Could not click Match Tracker:")
            return False
        
    def extract_player_name(self,full_team_name):
        """
        Extracts player name from strings like "Netherlands (Thomas)" -> returns "Thomas"
        If no parentheses found, returns the original string
        """
        if '(' in full_team_name and ')' in full_team_name:
            start = full_team_name.find('(') + 1
            end = full_team_name.find(')')
            return full_team_name[start:end]
        return full_team_name

    def run(
        self,
        sport_name: str,
        num_threads: int = 1,
        max_file_bytes_base: int | None = None,
    ):
        """
        Scrapes matches into Database/Data CSVs named with date/time to avoid collisions.

        num_threads: same value as ThreadPoolExecutor max_workers — used to shrink per-file
        size limit (base budget / threads) so many parallel writers do not each grow huge files.

        max_file_bytes_base: total byte budget before splitting across threads; default from env
        SCRAPER_DATA_MAX_BYTES_BASE or 50 MiB. Rotation happens only after the current match ends.
        """
        data_dir, done_dir = _dfm.ensure_data_dirs()
        sport_slug = sport_name.lower().replace(" ", "_")
        base_max = _dfm.resolve_base_max_bytes(max_file_bytes_base)
        eff_thr = _dfm.effective_max_bytes_per_thread(base_max, num_threads)
        print(
            f"[INFO] Data CSV rotation: base_max={base_max} B, num_threads={num_threads}, "
            f"per-thread max before move (after match ends)={eff_thr} B"
        )
        long_path = _dfm.new_stamped_csv_path(data_dir, sport_slug, int(self.thread_index))
        print(f"[INFO] Long format CSV path: {long_path}")
        print("Current working directory:", os.getcwd())
        print(f"Writing to file: {os.path.abspath(long_path)}")

        while True:
            self.matches = []
            # Match setup - get new match info
            match_info = self.run_FindMatch()
            print("[INFO] Logged in and match clicked.")
            match_id = self.extract_match_id_from_url(self.driver.current_url)

            # Generate team IDs
            team_a_id = self.generate_team_id(match_info["home_player"])
            team_b_id = self.generate_team_id(match_info["away_player"])
            self.click_match_tracker()

            # Base row with match information - now includes team IDs
            base_row = {
                "Match ID": match_id,
                "Team A": match_info["home_player"],
                "Team B": match_info["away_player"],
                "Team A ID": team_a_id,
                "Team B ID": team_b_id,
                "Start Time": match_info["start_time"],
                "match_finished": False
            }

            if(sport_name == "eFootball"):
                team_a_player_id = self.generate_team_id(self.extract_player_name(match_info["home_player"]))
                team_b_player_id = self.generate_team_id(self.extract_player_name(match_info["away_player"]))
                print("sport is eFootball")

                # Base row with match information - now includes team IDs
                base_row = {
                    "Match ID": match_id,
                    "Team A": match_info["home_player"],
                    "Team B": match_info["away_player"],
                    "Team A ID": team_a_id,
                    "Team B ID": team_b_id,
                    "Team A Player ID": team_a_player_id,
                    "Team B Player ID": team_b_player_id,
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
            match_time = 0
            while (not match_ended or not self.is_match_ended()):
                if (match_time <= 0):
                    match_time = self.get_efootball_match_time_in_seconds()
                    if match_time <= 0 : continue
                
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

            long_path = _dfm.rotate_oversized_csv_after_match(
                long_path,
                sport_slug=sport_slug,
                thread_index=int(self.thread_index),
                data_dir=data_dir,
                done_dir=done_dir,
                base_max_bytes=base_max,
                num_threads=int(num_threads),
            )

            print(f"[INFO] Finished processing match {match_id}. Moving to the next match...")


