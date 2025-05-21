import pandas as pd
from collections import defaultdict

def find_arbitrage_opportunities(csv_file, interval=15, threshold=0.2):
    """
    Find arbitrage opportunities in 1x2 betting market.
    
    Args:
        csv_file (str): Path to the CSV file
        interval (int): How many previous rows to compare (default 1)
        threshold (float): Minimum odds difference to consider as significant shift
        
    Returns:
        Prints top 10 times with largest odds shifts where score was unchanged,
        showing both compared timestamps and the two rows.
    """
    # Read CSV file
    df = pd.read_csv(csv_file)
    
    # Convert probabilities to odds
    for col in ['1x2_home', '1x2_draw', '1x2_away']:
        df[col] = 1 / df[col]
    
    # Track matches separately
    matches = defaultdict(list)
    for _, row in df.iterrows():
        matches[row['Match ID']].append(row.to_dict())
    
    # Store significant shifts (avoid duplicates)
    significant_shifts = []
    seen_shifts = set()  # To avoid duplicate entries
    
    for match_id, match_data in matches.items():
        for i in range(interval, len(match_data)):
            current = match_data[i]
            previous = match_data[i - interval]
            
            # Skip if match ended or score changed
            if previous['match_finished'] or current['match_finished']:
                continue
                
            if (current['match_score_home'] != previous['match_score_home'] or 
                current['match_score_away'] != previous['match_score_away']):
                continue
            
            # Calculate odds differences
            home_diff = abs(current['1x2_home'] - previous['1x2_home'])
            draw_diff = abs(current['1x2_draw'] - previous['1x2_draw'])
            away_diff = abs(current['1x2_away'] - previous['1x2_away'])
            
            max_diff = max(home_diff, draw_diff, away_diff)
            
            if max_diff >= threshold:
                # Create a unique key to avoid duplicates
                shift_key = (
                    match_id,
                    previous['Timestamp'],
                    current['Timestamp'],
                    round(previous['1x2_home'], 2),
                    round(current['1x2_home'], 2),
                    round(previous['1x2_draw'], 2),
                    round(current['1x2_draw'], 2),
                    round(previous['1x2_away'], 2),
                    round(current['1x2_away'], 2),
                )
                
                if shift_key not in seen_shifts:
                    seen_shifts.add(shift_key)
                    significant_shifts.append({
                        'match_id': match_id,
                        'prev_timestamp': previous['Timestamp'],
                        'curr_timestamp': current['Timestamp'],
                        'score_home': current['match_score_home'],
                        'score_away': current['match_score_away'],
                        'home_odds_prev': previous['1x2_home'],
                        'home_odds_curr': current['1x2_home'],
                        'draw_odds_prev': previous['1x2_draw'],
                        'draw_odds_curr': current['1x2_draw'],
                        'away_odds_prev': previous['1x2_away'],
                        'away_odds_curr': current['1x2_away'],
                        'difference': max_diff
                    })
    
    # Sort by largest differences
    significant_shifts.sort(key=lambda x: x['difference'], reverse=True)
    
    print("\nTop 10 largest odds shifts with unchanged score:")
    print("="*70)
    for i, shift in enumerate(significant_shifts[:10], 1):
        print(f"{i}. Match ID: {shift['match_id']}")
        print(f"   Score: {shift['score_home']}-{shift['score_away']}")
        print(f"   Timestamp: {shift['prev_timestamp']} and {shift['curr_timestamp']}")
        print(f"   Home odds: {shift['home_odds_prev']:.2f} -> {shift['home_odds_curr']:.2f}")
        print(f"   Draw odds: {shift['draw_odds_prev']:.2f} -> {shift['draw_odds_curr']:.2f}")
        print(f"   Away odds: {shift['away_odds_prev']:.2f} -> {shift['away_odds_curr']:.2f}")
        print(f"   Max difference: {shift['difference']:.2f}")
        print("-"*70)


import csv
import sys
import os
# Usage
if __name__ == "__main__":
    sport_name = "vFootball"
    folder = os.path.join("Database", "Data")
    os.makedirs(folder, exist_ok=True)
    long_path = os.path.join(folder, f"{sport_name.lower().replace(' ', '_')}_long_format_{0}.csv")
    find_arbitrage_opportunities(long_path)