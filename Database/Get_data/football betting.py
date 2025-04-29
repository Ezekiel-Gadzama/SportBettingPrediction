import sys
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import csv
from selenium.webdriver.common.action_chains import ActionChains
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from UI_webscraping.General_Scraping.scrape_data import MarketScraper as BaseMarketScraper
from Database.Get_data.Zsport_markets import SPORT_MARKETS
from selenium.webdriver.common.keys import Keys
from UI_webscraping.General_Scraping.FindMatch import global_claimed_matches

########################################################################################
# Custom class to write to both file and terminal
import threading

class Tee:
    def __init__(self, filename):
        self.file = open(filename, 'w')
        self.stdout = sys.stdout
        self.lock = threading.Lock()  # Add a lock for thread safety

    def write(self, message):
        with self.lock:  # Protect file operations with a lock
            self.file.write(message)
            self.file.flush()  # Ensure immediate write
            self.stdout.write(message)

    def flush(self):
        with self.lock:
            self.file.flush()
            self.stdout.flush()

    def close(self):
        with self.lock:
            self.file.close()
        sys.stdout = self.stdout

# Create an instance of the Tee class
tee = Tee('table tennis output.log')
print("Current working directory:", os.getcwd())

# Redirect stdout to the Tee instance
sys.stdout = tee
sys.stderr = tee  # Optionally redirect stderr as well

########################################################################################
Total_profit = 0
sharedLoss = []
class CustomMarketScraper(BaseMarketScraper):
    def __init__(self, url, markets_to_scrape, account_balance, divide):
        """
        Initialize the custom scraper with URL and markets to scrape.
        
        Args:
            url (str): The URL to scrape
            markets_to_scrape (list): List of markets to scrape
        """
        super().__init__(url, markets_to_scrape)
        self.original_account_balance = account_balance
        self.reset(account_balance, divide)  # Initialize all values using reset method
        self.list_of_stakes = []  # Starting stake
        self.list_of_odds = []
        self.profit_made = 0
        self.market = markets_to_scrape[0]

    def reset(self, account_balance, divide):
        """
        Reset all tracking variables to their initial values.
        """
        self.divide = divide
        self.current_home_odd = None
        self.current_away_odd = None
        self.current_draw_odd = None
        self.skip = True
        self.current_stake = int(max(round(account_balance / self.divide), 10))
        self.account_balance = account_balance
        self.expected_profit = 0
        self.per_profit_value = int(round(self.current_stake * 1))
        self.match_time = 0
        self.will_bet_on_this_match = False
        self.home_score = 0
        self.away_score = 0
        self.last_bet_was_on = None
        self.current_odd = 0
        self.previous_total_score = 0
        self.bet_count = 0
        self.home_stakes = []
        self.home_odds = []
        self.away_stakes = []
        self.away_odds = []
        self.draw_stakes = []
        self.draw_odds = []
        self.loss = 0
        self.track_loss_add = []
        self.external_loss = 0
        self.there_is_arbitrage = False
        self.right_bet = None
        self.right_odd = None
        self.left_bet = None
        self.left_odd = None
        self.left_stake = None
        self.right_stake = None
        self.is_arbitrage_bet_completed = False

    def get_match_time_in_minutes(self):
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
                
                return int(minutes)
            
            return 0  # Default if time format is unexpected
        
        except Exception as e:
            print(f"[ERROR] Could not determine match time: ")
            return 0
        
    def get_virtual_match_time_in_minutes(self):
        """
        Extracts and returns the current virtual football match time in minutes.
        Returns 0 if the time cannot be determined.
        
        Handles formats like: 
        - "1st | 15:45" → returns 15
        - "2nd | 53:01" → returns 53 (not 45+53)
        """
        try:
            # Find the time element
            time_element = self.driver.find_element(By.CLASS_NAME, "time")
            time_text = time_element.text.strip()
            
            # Example formats: "1st | 15:45" or "2nd | 53:01"
            if '|' in time_text:
                period, clock = [part.strip() for part in time_text.split('|')]
                
                # Extract minutes:seconds
                if ':' in clock:
                    minutes, seconds = clock.split(':')
                    return int(minutes)  # Just return the minutes part
            
            return 0  # Default if format is unexpected
        
        except Exception as e:
            print(f"[ERROR] Could not determine virtual match time: {str(e)}")
            return 0
        
    def distribute_loss(self):
        global sharedLoss
        Loss = []
        remaining_loss = sum(sharedLoss)
        
        # Calculate how many full 12-unit chunks we can distribute
        max_elements = min(10, remaining_loss // 10) if remaining_loss >= 10 else 0
        
        # Distribute the maximum possible equal amounts (≥12)
        if max_elements > 0:
            base_amount = remaining_loss // max_elements
            remainder = remaining_loss % max_elements
            
            # Distribute base amounts
            Loss = [base_amount] * max_elements
            
            # Distribute remainder
            for i in range(remainder):
                Loss[i] += 1
        else:
            # Handle cases where loss is less than 12*10=120
            if remaining_loss > 0:
                Loss.append(remaining_loss)
        
        # Ensure no element is 0 and handle the 10-element max constraint
        Loss = [x for x in sharedLoss if x > 0]
        
        # If we have more than 10 elements (only possible when loss > 120)
        if len(Loss) > 10:
            # Re-distribute to exactly 10 elements with higher values
            base_amount = remaining_loss // 10
            remainder = remaining_loss % 10
            Loss = [base_amount] * 10
            for i in range(remainder):
                Loss[i] += 1
        sharedLoss = Loss
        print(f"Shared Loss: {sharedLoss}")
        
    def run(self, sport_name: str):
        # Create folders and paths
        print("This is the run method called")
        folder = os.path.join("Database", "Data")
        os.makedirs(folder, exist_ok=True)
        long_path = os.path.join(folder, f"{sport_name.lower().replace(' ', '_')}arbitrage_format.csv")
        print(f"[INFO] Arbitrage format CSV path: {long_path}")
        print("Current working directory:", os.getcwd())
        print(f"Writing to file: {os.path.abspath(long_path)}")

        while True:
            global Total_profit, sharedLoss
            Total_profit += self.per_profit_value
            self.account_balance = self.original_account_balance + Total_profit
            if self.loss > 0:
                sharedLoss.append(self.loss)  # Add to the global list
                self.distribute_loss()  # Redistribute
            if len(sharedLoss) > 0:
                self.external_loss = sharedLoss.pop()

            self.reset(self.account_balance, self.divide)
            # Match setup - get new match info
            try:
                match_info = self.run_FindMatch()
            except:
                print("Error happend in finding match in run method")
                continue

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
            refresh_count = 0
            MAX_REFRESHES = 3
            all_columns_seen = set(base_row.keys()) | {"Timestamp"}
            all_odds_columns_seen = set()  # For raw odds columns
            match_ended = False

            fieldnames = list(base_row.keys()) + ["Timestamp"]
            count = 0
            while not match_ended or not self.is_match_ended():
                self.skip = True 
                loop_start = time.time()
                current_time = int(loop_start - start_time)

                current_match_status = self.is_match_ended()
                if current_match_status and not match_ended:
                    print(f"[INFO] Match {match_id} has ended. Processing final data...")
                    match_ended = True
                    base_row["match_finished"] = True
                    print(f"[INFO] Scraping at +{current_time}s (Match ended: {match_ended})")

                self.scraped_data.clear()
                self.scraped_odd.clear()  # Clear previous odds data
                scores_and_odds = self.extract_data(self.markets_to_scrape, sport_name, current_time)

                market_odds_missing = len(self.scraped_data) == 0

                if market_odds_missing and not match_ended:
                    print("[WARNING] Market odds missing - attempting refresh")
                    if refresh_count < MAX_REFRESHES:
                        self.driver.refresh()
                        time.sleep(4)
                        self.cancel_bet_slip()
                        refresh_count += 1
                        continue
                    else:
                        print("[WARNING] Max refreshes reached without finding market odds")
                        refresh_count = 0
                else:
                    refresh_count = 0

                current_data = {}
                current_odds = {}  # For raw odds data
                
                # Process probabilities (existing functionality)
                for market, outcomes in self.scraped_data.items():
                    for label, odd in outcomes:
                        if not label:
                            continue
                        key = f"{market.lower().replace(' ', '_')}_{label.lower().replace(' ', '_')}"
                        current_data[key] = odd
                        

                # Process raw odds data
                for market, outcomes in self.scraped_odd.items():
                    for label, odd in outcomes:
                        if not label:
                            continue
                        odds_key = f"{market.lower().replace(' ', '_')}_{label.lower().replace(' ', '_')}_odd"
                        current_odds[odds_key] = odd
                        
                        # Store home and away odds separately for betting logic
                        
                        try:
                            print(f"Label is {label.lower()}")
                            if "home" in label.lower():
                                print(f"Home Odd is {odd}")
                                self.current_home_odd = float(odd)
                            elif "away" in label.lower():
                                print(f"Away Odd is {odd}")
                                self.current_away_odd = float(odd)
                            else:
                                print(f"Draw Odd is {odd}")
                                self.current_draw_odd = float(odd)

                        except:
                            print("Error: could not convert string to float:")
                            self.driver.refresh()
                            time.sleep(4)
                            continue

                # Add scores to both data and odds
                for key, val in scores_and_odds.items():
                    current_data[key] = val
                    current_odds[key] = val

                # Handle new columns for probabilities
                new_columns = set(current_data.keys()) - all_columns_seen
                if new_columns:
                    print(f"[INFO] New probability columns detected: {new_columns}")
                    all_columns_seen.update(new_columns)
                    fieldnames.extend(col for col in sorted(new_columns) if col not in fieldnames)

                # Handle new columns for raw odds
                new_odds_columns = set(current_odds.keys()) - all_odds_columns_seen
                if new_odds_columns:
                    print(f"[INFO] New odds columns detected: {new_odds_columns}")
                    all_odds_columns_seen.update(new_odds_columns)
                    fieldnames.extend(col for col in sorted(new_odds_columns) if col not in fieldnames)

                # Then your existing betting logic:
                # Inside your run method, add this after getting the scores:
                try:
                    home_score = int(scores_and_odds.get('match_score_home', 0))
                    away_score = int(scores_and_odds.get('match_score_away', 0))
                    if home_score == "N/A" or away_score == "N/A":
                        print(f"Home score is {home_score}  and away score is {away_score}")
                        print("Skipping for now")
                        self.driver.refresh()
                        time.sleep(5)
                        continue
                except:
                    print(f"Home score seems N/A  and away score seems N/A")
                    print("Skipping for now")
                    self.driver.refresh()
                    time.sleep(5)
                    continue
                # Calculate current set number based on sum of scores
                total_score = home_score + away_score
                self.home_score = home_score
                self.away_score = away_score
                print(f"Total score: {total_score} , Home score: {self.home_score} , Away Score: {self.away_score}")
                self.match_time = self.get_virtual_match_time_in_minutes()
                # Then your existing betting logic:
                # # and (home_score >= 2 and away_score >= 2)
                # (((self.match_time >= 60 and total_score < 3) or self.will_bet_on_this_match) and (total_score > self.previous_total_score or not self.will_bet_on_this_match) and self.bet_count < 2)
                if (total_score > self.previous_total_score):
                        self.driver.refresh()
                        time.sleep(2)

                if self.will_bet_on_this_match:
                    self.there_is_arbitrage = self.is_arbitrage()

                if self.is_arbitrage_bet_completed:
                    print("Arbitrage is already completed")
                    continue

                if total_score > self.previous_total_score or not self.will_bet_on_this_match or self.there_is_arbitrage:
                    print(f"match time: {self.match_time} total score: {total_score} will be on match: {self.will_bet_on_this_match} previous total score: {self.previous_total_score} bet count: {self.bet_count}")
                    print(f"Lenght of stake: {len(self.list_of_stakes)} and stakes are: {self.list_of_stakes} ")
                    self.evaluate_and_place_bet()
                    if self.skip:
                        count += 1
                        if (count > 10):
                            print("count greater than 10")
                            count = 0
                            self.driver.refresh()
                            time.sleep(2)
                            self.cancel_bet_slip()
                        continue
                    count = 0
                # elif (home_score + away_score) > 0 and not self.will_bet_on_this_match:
                #     print("Break out of this match because they are scoring goals")
                #     break

            # checking if bet won:
            if ((self.last_bet_was_on == "Home" and self.home_score > self.away_score) or (self.last_bet_was_on == "Away" and self.away_score > self.home_score) or (self.last_bet_was_on == "Draw" and self.away_score == self.home_score)) and (not self.there_is_arbitrage or self.is_arbitrage_bet_completed):
                print(f"It won the match {self.live_url}")
                profit = (sum(self.list_of_stakes) - (self.list_of_stakes[-1] * self.current_odd))
                self.profit_made += profit
                print(f"Profit is {profit} and Total profit is {self.profit_made} for this thread")
                self.list_of_stakes = []
                self.list_of_odds = []
                self.external_loss = 0
            else:
                self.loss = self.current_stake + self.external_loss
                print("self.loss = self.current_stake")

            print(f"[INFO] Finished processing match {match_id}. Moving to the next match...")


    def compute_stakes_iterative(self, remaining_stake, left_odds, left_stakes, current_left_odd, 
                                right_odds, right_stakes, current_right_odd, tolerance=1e-6, max_iter=100):
        """
        Generalized method to compute stake1 and stake2 for any two opposing bets.
        """
        # Start with an initial guess (split remaining_stake equally)
        stake1 = remaining_stake / 2
        stake2 = remaining_stake / 2

        for _ in range(max_iter):
            # Compute left_odd and right_odd with current stakes
            left_odd = (sum(odd * stake for odd, stake in zip(left_odds, left_stakes)) + current_left_odd) / (sum(left_stakes) + stake1)
            right_odd = (sum(odd * stake for odd, stake in zip(right_odds, right_stakes)) + current_right_odd) / (sum(right_stakes) + stake2)
            self.left_odd = left_odd
            self.right_odd = right_odd

            # Update stakes to satisfy stake1 * left_odd = stake2 * right_odd
            new_stake1 = (remaining_stake * right_odd) / (left_odd + right_odd)
            new_stake2 = remaining_stake - new_stake1

            # Check for convergence
            if abs(new_stake1 - stake1) < tolerance and abs(new_stake2 - stake2) < tolerance:
                print(f"stakes are: {new_stake1} and {new_stake2} and they give {new_stake1*left_odd} == {new_stake2 * right_odd} ")
                return new_stake1, new_stake2

            stake1, stake2 = new_stake1, new_stake2

        print(f"Just returning best estimate stakes are: {new_stake1} and {new_stake2} and they give {new_stake1*left_odd} == {new_stake2 * right_odd} ")
        return stake1, stake2  # Return best estimate after max_iter

    def is_arbitrage(self):
        """
        Checks for arbitrage opportunities based on the last bet placed.
        """
        if self.last_bet_was_on == "Home":
            stake = sum(self.home_stakes)
            odd1 = sum(odd * stake for odd, stake in zip(self.home_odds, self.home_stakes)) / stake
            expected_odd2 = 1 - (1/odd1)
            remaining_stake = (stake * odd1) / expected_odd2  # Fixed: using total stake instead of stake1

            # stake1, stake2 = self.compute_stakes_iterative(
            #     remaining_stake=remaining_stake,
            #     left_odds=self.draw_odds,
            #     left_stakes=self.draw_stakes,
            #     current_left_odd=self.current_draw_odd,
            #     right_odds=self.away_odds,
            #     right_stakes=self.away_stakes,
            #     current_right_odd=self.current_away_odd
            # )

            a = (sum(odd * stake for odd, stake in zip(self.draw_odds, self.draw_stakes)) + self.current_draw_odd)
            b = sum(self.draw_stakes)

            c = (sum(odd * stake for odd, stake in zip(self.away_odds, self.away_stakes)) + self.current_away_odd)
            d = sum(self.away_stakes)

            (c / (d + stake2)) * stake2 =  (a / (b  + stake1)) * stake1
            stake1 + stake2 = remaining_stake

            
            self.left_odd =  a / (b  + stake1)
            self.right_odd =  c / (d + stake2)


            self.left_bet = "Draw"
            self.right_bet = "Away"

        elif self.last_bet_was_on == "Away":
            stake = sum(self.away_stakes)
            odd1 = sum(odd * stake for odd, stake in zip(self.away_odds, self.away_stakes)) / stake
            expected_odd2 = 1 - (1/odd1)
            remaining_stake = (stake * odd1) / expected_odd2

            stake1, stake2 = self.compute_stakes_iterative(
                remaining_stake=remaining_stake,
                left_odds=self.home_odds,
                left_stakes=self.home_stakes,
                current_left_odd=self.current_home_odd,
                right_odds=self.draw_odds,
                right_stakes=self.draw_stakes,
                current_right_odd=self.current_draw_odd
            )

            self.left_odd = (sum(odd * stake for odd, stake in zip(self.home_odds, self.home_stakes)) + self.current_home_odd) / (sum(self.home_stakes) + stake1)
            self.right_odd = (sum(odd * stake for odd, stake in zip(self.draw_odds, self.draw_stakes)) + self.current_draw_odd) / (sum(self.draw_stakes) + stake2)
            self.left_bet = "Home"
            self.right_bet = "Draw"

        elif self.last_bet_was_on == "Draw":
            stake = sum(self.draw_stakes)
            odd1 = sum(odd * stake for odd, stake in zip(self.draw_odds, self.draw_stakes)) / stake
            expected_odd2 = 1 - (1/odd1)
            remaining_stake = (stake * odd1) / expected_odd2

            stake1, stake2 = self.compute_stakes_iterative(
                remaining_stake=remaining_stake,
                left_odds=self.home_odds,
                left_stakes=self.home_stakes,
                current_left_odd=self.current_home_odd,
                right_odds=self.away_odds,
                right_stakes=self.away_stakes,
                current_right_odd=self.current_away_odd
            )

            self.left_odd = (sum(odd * stake for odd, stake in zip(self.home_odds, self.home_stakes)) + self.current_home_odd) / (sum(self.home_stakes) + stake1)
            self.right_odd = (sum(odd * stake for odd, stake in zip(self.away_odds, self.away_stakes)) + self.current_away_odd) / (sum(self.away_stakes) + stake2)
            self.left_bet = "Home"
            self.right_bet = "Away"
        
        if odd1 == 0:
            print("Error: Last odd cannot be 0")
            return False

        print(f"Last bet was {self.last_bet_was_on}: {odd1:.2f} | Left odd: {self.left_odd:.2f} | Right odd: {self.right_odd:.2f}")
        
        # Calculate arbitrage percentage
        percentage_profit = (1/odd1) + (1/self.left_odd) + (1/self.right_odd)
        profit_percentage = (1 - percentage_profit) * 100
        
        # Define acceptable profit threshold (5% in this case)
        required_profit = 0.8  # 5% profit
        
        if percentage_profit < required_profit:
            print(f"Arbitrage opportunity found: {profit_percentage:.2f}% profit")
            
            # Calculate stakes for arbitrage
            total_investment = sum(self.home_stakes + self.away_stakes + self.draw_stakes)
            
            if self.left_bet and self.right_bet:
                # Calculate stakes to ensure equal payout regardless of outcome
                left_payout = total_investment / (percentage_profit * self.left_odd)
                right_payout = total_investment / (percentage_profit * self.right_odd)
                
                self.left_stake = left_payout / self.left_odd
                self.right_stake = right_payout / self.right_odd
                
                print(f"Bet {self.left_stake:.2f} on {self.left_bet} and {self.right_stake:.2f} on {self.right_bet}")
            
            return True
        else:
            print(f"No arbitrage (profit: {profit_percentage:.2f}%)")
            return False


    def calculate_stake(self, target_team):
        if target_team == "Home":
            self.current_odd = self.current_home_odd
        elif target_team == "Away":
            self.current_odd = self.current_away_odd
        else:
            self.current_odd = self.current_draw_odd


        odd_gains = 0
        print(f"Home stake {self.home_stakes} , away stake: {self.away_stakes} , draw stake {self.draw_stakes}")
        print(f"Home odd {self.home_odds} , away odd: {self.away_odds} , draw odd {self.draw_odds}")
        if target_team == "Home":
            print(f"it entered as target team == {target_team}")
            # Home team logic
            for i in range(len(self.home_stakes)):
                odd_gains += self.home_stakes[i] * (self.home_odds[i] - 1)
            loss = ((sum(self.away_stakes) + sum(self.draw_stakes) - odd_gains) + self.per_profit_value + self.external_loss)
            self.current_stake = max(
                 loss / (self.current_odd - 1),
                10
            )
        elif target_team == "Away":
            print(f"it entered as target team == {target_team}")
            # Away team logic
            for i in range(len(self.away_stakes)):
                odd_gains += self.away_stakes[i] * (self.away_odds[i] - 1)
            loss = ((sum(self.home_stakes) + sum(self.draw_stakes) - odd_gains) + self.per_profit_value + self.external_loss)
            self.current_stake = max(
                 loss / (self.current_odd - 1),
                10
            )
        elif target_team == "Draw":
            print(f"it entered as target team == {target_team}")
            # Draw logic
            for i in range(len(self.draw_stakes)):
                odd_gains += self.draw_stakes[i] * (self.draw_odds[i] - 1)
            loss = ((sum(self.home_stakes) + sum(self.away_stakes) - odd_gains) + self.per_profit_value + self.external_loss)
            self.current_stake = max(
                 loss / (self.current_odd - 1),
                10
            )
        else:
            print(f"Something is fucking wrong")

        if(self.there_is_arbitrage):  
            self.current_stake = self.left_stake if self.left_bet is not None else self.right_stake
            print(f"It has assigned arbitrage staked of {self.current_stake}")

            
        
        print(f"stake after calculating: {self.current_stake}")
    
        
            
    def evaluate_and_place_bet(self):
        """
        Evaluate the current odds and place a bet if conditions are met.
        """
        if self.current_home_odd is None or self.current_away_odd is None or self.current_home_odd == "N/A" or self.current_away_odd == "N/A":
            print("[BET] No valid odds to evaluate")
            return
            
        print(f"[BET] Current odds - Home: {self.current_home_odd}, Draw: {self.current_draw_odd} Away: {self.current_away_odd}")
        if not self.there_is_arbitrage:
            if self.home_score == self.away_score:
                target_team = "Draw"
                self.current_odd = self.current_draw_odd
            elif self.home_score > self.away_score:
                target_team = "Home"
                self.current_odd = self.current_home_odd
            else:
                target_team = "Away"
                self.current_odd = self.current_away_odd

            if self.last_bet_was_on == target_team:
                print(f"It is the same last match : {self.last_bet_was_on}")
                return
        else:
            if self.left_bet is not None:
                target_team = self.left_bet
            else:
                target_team = self.right_bet




            print("Inside life")

        # if(self.current_odd < 1.3):
        #     return
        
        self.calculate_stake(target_team)
        print(f"Now current stake is {self.current_stake}")
        
        # Click on the better odd
        if self.click_on_odd(target_team):
            # Handle the bet slip
            self.handle_bet_slip(target_team)
            if self.skip:
                print("Skipping the rest")
                return
        else:
            print("[BET] Failed to click on the odd")
        
    
    def click_on_odd(self, team):
        """
        Click on the odd for the specified team (home, away, or draw).
        
        Args:
            team (str): 'home', 'away', or 'draw'
            
        Returns:
            bool: True if click was successful, False otherwise
        """
        try:
            # Find the market header first
            market_header = self.find_market_without_waiting(self.market)
            
            # Get the wrapper containing all outcomes
            wrapper = market_header.find_element(
                By.XPATH, 
                ".//ancestor::div[contains(@class, 'm-table__wrapper')]"
            )
            
            # Find all outcome cells
            outcome_cells = wrapper.find_elements(
                By.XPATH,
                ".//div[contains(@class, 'm-outcome')]//div[contains(@class, 'm-table-cell--responsive')]"
            )
            
            # Determine which cell to click based on team
            target_cell = None
            for cell in outcome_cells:
                label = cell.find_element(By.CLASS_NAME, "m-table-cell-item").text.strip().lower()
                if team.lower() in label:
                    target_cell = cell
                    break
            
            if not target_cell:
                print(f"[BET ERROR] Could not find odds for {team} team")
                return False
            
            # Find the odd element within the target cell
            odd_elements = target_cell.find_elements(
                By.XPATH, 
                ".//span[contains(@class, 'm-table-cell-item') and contains(text(), '.')]"
            )
            
            if not odd_elements:
                print(f"[BET ERROR] Could not find odd value for {team} team")
                return False
            
            odd_element = odd_elements[0]
            print(f"[BET] Clicking on {team} odd: {odd_element.text}")
            odd_element.click()
            return True
            
        except Exception as e:
            print(f"[BET ERROR] Failed to click on {team} odd: ")
            return False
    
    def check_bet_success(self):
        """
        Check if the bet was successful by looking for the success dialog.
        Returns True if successful and prints bet details, False otherwise.
        """
        try:
            # Wait for success dialog to appear
            success_dialog = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'm-dialog-suc')]"))
            )
            
            # Wait for and extract bet details
            stake = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, 
                    "//li[@class='m-order-info']/div[@class='m-label' and contains(text(), 'Total Stake')]/following-sibling::div"))
            ).text.strip()
            
            potential_win = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH,
                    "//li[@class='m-order-info']/div[@class='m-label' and contains(text(), 'Potential Win')]/following-sibling::div"))
            ).text.strip()
            
            booking_code = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH,
                    "//div[contains(@class, 'booking-code')]"))
            ).text.strip()
            
            print("\n[BET SUCCESS] Bet placed successfully!")
            print(f"Stake: {stake}")
            print(f"Potential Win: {potential_win}")
            print(f"Booking Code: {booking_code}\n")
            
            # Close the success dialog
            try:
                close_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH,
                        "//button[contains(@class, 'af-button--primary') and .//span[contains(text(), 'OK')]]"))
                )
                close_button.click()
            except:
                print("couldn't close button")
            
            return True
            
        except Exception as e:
            print(f"[BET] No success dialog found - bet may have failed: ")
            return False
        

    def enter_stake(self, amount=100):
        """
        Enters the specified stake amount into the bet slip input field.
        
        Args:
            amount (int/float): The amount to stake (default: 100)
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            print("[BET] Attempting to enter stake amount...")
            
            # Wait for the betslip container to be present
            betslip_container = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "m-betslips"))
            )
            
            # More specific XPath to locate the stake input based on the actual structure
            stake_input = WebDriverWait(betslip_container, 15).until(
                EC.presence_of_element_located((By.XPATH, 
                    ".//div[contains(@class, 'm-stake')]//div[@class='m-value']//div[@id='j_stake_0']//input[@class='m-input fs-exclude']"))
            )
            
            # Scroll the element into view
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", stake_input)
            
            # Click and clear using multiple methods
            for _ in range(3):  # Try clicking multiple times
                try:
                    stake_input.click()
                    break
                except:
                    time.sleep(0.5)
            
            # Clear existing value thoroughly
            stake_input.send_keys(Keys.CONTROL + 'a')
            stake_input.send_keys(Keys.DELETE)
            self.driver.execute_script("arguments[0].value = '';", stake_input)
            
            # Send keys slowly with validation
            if amount < 10:
                print("amount is lesser than 10 so setting to 10")
                amount = max(amount, 10)

            amount_str = str(int(round(amount)))
            stake_input.send_keys(amount_str)
            print("Finished entering stake")
            # Verify value
            # Method 1: Get value via JavaScript
            entered_value = self.driver.execute_script("return arguments[0].value", stake_input)
            print(f"JavaScript value: {entered_value}")
            if entered_value == amount_str:
                print(f"[BET] Successfully entered stake amount: {amount}")
                return True
            
            # If verification failed, 
            print(f"[BET WARNING] Value mismatch Expected: {amount}, Got: {entered_value}")
            return False
            
        except Exception as e:
            print(f"[BET ERROR] Stake entry failed: ")
            self.driver.save_screenshot("stake_error.png")
            return False
        
    def click_on_bet_and_accept(self):
        
        """Simple version using only the provided full XPath"""
        try:
            print("[BET] Attempting to placebet or accept bet button...")
            bet = WebDriverWait(self.driver, 1).until(
                EC.element_to_be_clickable((By.XPATH,
                    "/html/body/div[1]/div[2]/div[2]/div/div[2]/div/aside/div[1]/div[2]/div[2]/div/div[3]/div/div[5]/button"))
            )
            
            # Scroll and click with JavaScript
            self.driver.execute_script("arguments[0].scrollIntoView(true);", bet)
            self.driver.execute_script("arguments[0].click();", bet)
            
            print("[BET] Successfully clicked placebet or accept bet")
            return True
            
        except Exception as e:
            print(f"[BET ERROR] Failed to click placebet or accept bet: ")
            return False 
    
    def click_confirm_button(self):
        """Simple version using only the provided full XPath"""
        try:
            print("[BET] Attempting to click Confirm button...")
            confirm_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                    "/html/body/div[1]/div[2]/div[2]/div/div[2]/div/aside/div[1]/div[2]/div[2]/div/div[3]/div/div[6]/div/div[2]/button[2]"))
            )
            
            # Scroll and click with JavaScript
            self.driver.execute_script("arguments[0].scrollIntoView(true);", confirm_button)
            self.driver.execute_script("arguments[0].click();", confirm_button)
            
            print("[BET] Successfully clicked Confirm button")
            return True
            
        except Exception as e:
            print(f"[BET ERROR] Failed to click Confirm button: ")
            return False 
    
    def handle_bet_slip(self,target_team):
        """
        Handle the bet slip - either accept changes or cancel based on odds comparison.
        
        Args:
            current_odd (float): The current odd being bet on
            team (str): 'home' or 'away'
            history (list): List of previous odds for this team
        """
        try:
            self.enter_stake(self.current_stake)
            # Wait for bet slip to appear
            max_attempts = 5
            attempt = 0
            
            while attempt < max_attempts:
                attempt += 1
                self.click_on_bet_and_accept()
                self.click_on_bet_and_accept()
                
                # Get the current odd from the bet slip to verify it hasn't changed
                try:
                    slip_odd_element = self.driver.find_element(
                            By.XPATH,
                            "//div[contains(@class, 'm-item-odds')]//span[contains(@class, 'm-text-main')]"
                        )
                    slip_odd = float(slip_odd_element.text.strip())
                    print(f"[BET] Bet slip odd: {slip_odd}")

                    if slip_odd < self.current_odd:
                        print("Got to this point")
                        if not self.is_arbitrage() and self.there_is_arbitrage:
                            print(f"No more arbitrage since slip odd changed to {slip_odd}")
                            self.there_is_arbitrage = False
                            return
                        
                        self.calculate_stake(target_team)
                        return self.handle_bet_slip(target_team, slip_odd)

                    # Verify the odd is still good
                    if not self.click_confirm_button():
                        continue
            
                    # Check if bet was successful
                    if self.check_bet_success():
                        # Append the new stake to list (whether it's arbitrage or normal bet)
                        self.will_bet_on_this_match = True
                        self.list_of_stakes.append(int(round(self.current_stake)))
                        self.list_of_odds.append(slip_odd)
                        print(f"successfully bet {self.list_of_stakes[-1]} on {target_team}")
                        self.last_bet_was_on = target_team
                        self.skip = False
                        self.previous_total_score = self.home_score + self.away_score
                        self.bet_count += 1
                        if self.left_bet is None and self.there_is_arbitrage:
                            self.right_bet = None
                            self.is_arbitrage_bet_completed = True
                            print("Arbitrage is completed")
                        elif self.there_is_arbitrage:
                            print(f"Finish betting on left bet {self.left_bet}")

                        self.left_bet = None
                        if target_team == "Home":
                            self.home_stakes.append(self.list_of_stakes[-1])
                            self.home_odds.append(slip_odd)
                        elif target_team == "Away":
                            self.away_stakes.append(self.list_of_stakes[-1])
                            self.away_odds.append(slip_odd)
                        elif target_team == "Draw":
                            self.draw_stakes.append(self.list_of_stakes[-1])
                            self.draw_odds.append(slip_odd)

                    else:
                        self.expected_profit = 0
                    self.cancel_bet_slip()
                    
                    return

                except Exception as e:
                    print(f"[BET ERROR] Could not verify bet slip odd: ")
            
            # If we get here, we should cancel the bet
            print("Maximum attempt reached to bet")
            self.cancel_bet_slip()
            
        except Exception as e:
            print(f"[BET ERROR] Error handling bet slip: ")
            self.cancel_bet_slip()
    
    def cancel_bet_slip(self):
        """
        Cancel all bet slips by clicking all delete buttons.
        """
        try:
            # Find all delete buttons
            delete_buttons = self.driver.find_elements(
                By.XPATH,
                "//i[contains(@class, 'm-icon-delete')]"
            )
            
            if not delete_buttons:
                print("[BET] No bet slips to cancel")
                return
                
            print(f"[BET] Cancelling {len(delete_buttons)} bet slip(s)")
            
            # Click each delete button
            for delete_button in delete_buttons:
                try:
                    delete_button.click()
                    time.sleep(0.5)  # Small delay to avoid rapid clicks
                except Exception as e:
                    print(f"[BET ERROR] Failed to click a delete button: ")
                    
            print("[BET] Successfully cancelled all bet slips")
            
        except Exception as e:
            print(f"[BET ERROR] Failed to find delete buttons: ")

def main():
    sport = "vFootball"
    url = f"https://www.sportybet.com/ng/sport/{sport}/upcoming?time=0"
    markets = ["1X2"]
    account_balance = 20000
    divide = account_balance / 10
    num_threads = 10  # Number of concurrent browsers/scrapers
    scraper = CustomMarketScraper(url, markets,account_balance, divide)
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            print(f"[MAIN] Starting scraper {i+1}")
            scraper = CustomMarketScraper(url, markets,account_balance, divide)
            futures.append(executor.submit(scraper.run, sport)) # Changed this line
        # Wait for all threads to complete
        for future in as_completed(futures):
            print("Completed future")
            try:
                future.result()
            except Exception as e:
                print(f"[MAIN] Thread error: {e}")

if __name__ == "__main__":  # Fixed this line (was "main" in quotes)
    main()