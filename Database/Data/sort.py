import pandas as pd
from pathlib import Path
import os
from datetime import datetime

def clean_and_reorder_csv(input_file, output_file=None):
    """
    Processes a CSV file containing table tennis betting data to:
    1. Group by match ID
    2. Ensure chronological order within each match
    3. Remove any duplicate entries
    4. Handle missing/NaN values
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str, optional): Path for cleaned output file. 
                                     If None, overwrites input file.
    """
    # Read the raw data
    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"Error: File {input_file} not found")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # Basic data validation
    if df.empty:
        print("Error: Empty dataframe")
        return
    
    required_columns = {'Match ID', 'Team A', 'Team B', 'Timestamp'}
    if not required_columns.issubset(df.columns):
        print(f"Error: Missing required columns. Needed: {required_columns}")
        return

    # Convert timestamp to datetime if it's not already
    if 'Start Time' in df.columns:
        try:
            df['Start Time'] = pd.to_datetime(df['Start Time'])
        except Exception as e:
            print(f"Warning: Could not convert 'Start Time' to datetime: {e}")

    # Clean data - fill NAs, convert types
    df = df.fillna('N/A')
    numeric_cols = df.select_dtypes(include=['float', 'int']).columns
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Sort by Match ID then Timestamp (ascending)
    df_sorted = df.sort_values(by=['Match ID', 'Timestamp'], 
                              ascending=[True, True])

    # Remove duplicates (same Match ID + Timestamp)
    df_clean = df_sorted.drop_duplicates(subset=['Match ID', 'Timestamp'], 
                                        keep='last')

    # Additional validation - check for timestamp ordering
    def validate_timestamps(group):
        timestamps = group['Timestamp'].values
        if not all(timestamps[i] <= timestamps[i+1] for i in range(len(timestamps)-1)):
            print(f"Warning: Non-monotonic timestamps in match {group.name}")
        return group

    df_clean = df_clean.groupby('Match ID', group_keys=False).apply(validate_timestamps)

    # Save the cleaned data
    output_path = output_file if output_file else input_file
    df_clean.to_csv(output_path, index=False)
    print(f"Successfully processed data. Saved to {output_path}")

    return df_clean

if __name__ == "__main__":
    # Example usage
    input_csv = "Database/Data/tabletennis_long_format.csv"
    output_csv = "Database/Data/tabletennis_clean.csv"
    
    # Create backup of original file
    if os.path.exists(input_csv):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"Database/Data/tabletennis_long_format_backup_{timestamp}.csv"
        os.rename(input_csv, backup_file)
        print(f"Created backup: {backup_file}")
    
    # Process the file
    clean_and_reorder_csv(backup_file if 'backup_file' in locals() else input_csv, 
                        output_csv)