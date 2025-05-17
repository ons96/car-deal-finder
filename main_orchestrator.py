import pandas as pd
from pathlib import Path
import sys
import os

# Add src directory to Python path to allow importing modules from there
# This assumes main_orchestrator.py is in the project root, and modules are in ./src/
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root / 'src'))

try:
    from process_facebook_data import parse_facebook_csv
    from data_processor import VehicleDataProcessor
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Please ensure 'facebook_parser.py' and 'data_processor.py' are in the 'src' directory,")
    print("and that the script is run from the project root directory, or adjust PYTHONPATH.")
    sys.exit(1)

# --- Configuration ---
DATA_DIR = project_root / "data"
FACEBOOK_CSV_PATTERN = "facebook-*.csv"  # Pattern to find Facebook CSV files
OUTPUT_CSV_PATH = DATA_DIR / "output.csv"
# Path to the reliability data for VehicleDataProcessor (relative to project root)
# Adjust if your reliability data path is different or defined elsewhere for VehicleDataProcessor
RELIABILITY_DATA_PATH = DATA_DIR / "chart_data_filtered.csv" 

# Columns that indicate a listing in output.csv has been fully processed
# If these are populated, we assume TCO etc., has been calculated.
# (Make sure these columns are actually produced by your VehicleDataProcessor)
FULLY_PROCESSED_INDICATOR_COLUMNS = ['deal_score', 'avg_annual_tco', 'tco_cost_per_km'] 

def load_processed_urls_and_details(output_csv_path: Path):
    """
    Loads URLs and key details from the existing output CSV to check for already processed listings.
    Returns a dictionary mapping URLs to a boolean indicating if they seem fully processed.
    """
    processed_details = {}
    if output_csv_path.exists() and os.path.getsize(output_csv_path) > 0:
        try:
            df_existing = pd.read_csv(output_csv_path)
            if 'url' not in df_existing.columns:
                print(f"Warning: 'url' column not found in {output_csv_path}. Cannot check for existing listings.")
                return processed_details
            
            # Check if any of the indicator columns exist
            existing_indicator_cols = [col for col in FULLY_PROCESSED_INDICATOR_COLUMNS if col in df_existing.columns]

            for _, row in df_existing.iterrows():
                url = row['url']
                if pd.isna(url):
                    continue
                
                # Assume not fully processed unless proven otherwise
                is_fully_processed = False 
                if existing_indicator_cols: # Only check if indicator columns are present
                    # Check if all *available* indicator columns are non-empty for this row
                    # This means if a car has values for deal_score and avg_annual_tco (and cost_per_km if present), it's processed.
                    is_fully_processed = all(pd.notna(row.get(col)) for col in existing_indicator_cols)
                
                processed_details[url] = is_fully_processed
            print(f"Loaded {len(processed_details)} URLs from {output_csv_path}. {sum(processed_details.values())} seem fully processed.")
        except pd.errors.EmptyDataError:
            print(f"Info: {output_csv_path} is empty. No existing listings to check.")
        except Exception as e:
            print(f"Error reading {output_csv_path}: {e}. Assuming no existing listings.")
    return processed_details

def main():
    print(f"Starting main orchestrator...")
    print(f"Looking for Facebook CSVs in: {DATA_DIR} matching '{FACEBOOK_CSV_PATTERN}'")
    print(f"Output will be managed in: {OUTPUT_CSV_PATH}")

    facebook_csv_files = list(DATA_DIR.glob(FACEBOOK_CSV_PATTERN))
    if not facebook_csv_files:
        print("No Facebook CSV files found. Exiting.")
        return

    print(f"Found Facebook CSV files: {[f.name for f in facebook_csv_files]}")

    # Load details of already processed listings from output.csv
    already_processed_details = load_processed_urls_and_details(OUTPUT_CSV_PATH)

    all_new_unprocessed_listings = []
    processed_file_count = 0

    for fb_csv_path in facebook_csv_files:
        print(f"\nProcessing Facebook file: {fb_csv_path.name}...")
        # The parser now returns a list of dicts
        parsed_listings = parse_facebook_csv(fb_csv_path)

        if not parsed_listings:
            print(f"No listings parsed from {fb_csv_path.name}. Skipping.")
            continue

        current_file_new_listings = []
        for listing in parsed_listings:
            listing_url = listing.get('url')
            if not listing_url:
                # print(f"Skipping a listing from {fb_csv_path.name} due to missing URL.")
                continue
            
            # Check if this URL is in output.csv and if it was fully processed
            if already_processed_details.get(listing_url, False): # True if URL exists AND fully_processed
                # print(f"Skipping already fully processed listing (URL): {listing_url}")
                continue
            # If URL exists but not fully_processed (False), or doesn't exist (None), it will be processed.
            
            current_file_new_listings.append(listing)
        
        if current_file_new_listings:
            print(f"Found {len(current_file_new_listings)} new/unprocessed listings in {fb_csv_path.name}.")
            all_new_unprocessed_listings.extend(current_file_new_listings)
            processed_file_count += 1
        else:
            print(f"No new or unprocessed listings to add from {fb_csv_path.name}.")

    if not all_new_unprocessed_listings:
        if processed_file_count > 0: # Files were processed, but all listings were duplicates/already handled
             print("\nAll listings from the Facebook CSV file(s) were already processed or filtered out. No new data to add to output.csv.")
        else: # No files had any processable listings to begin with
            print("\nNo new listings found in any Facebook CSV files to process. Output.csv remains unchanged.")
        return

    print(f"\nTotal of {len(all_new_unprocessed_listings)} new/unprocessed listings gathered from {processed_file_count} Facebook file(s) to be processed further.")

    # Initialize the VehicleDataProcessor
    # Ensure reliability_data_path is correct for your setup
    if not RELIABILITY_DATA_PATH.exists():
        print(f"CRITICAL ERROR: Reliability data file not found at {RELIABILITY_DATA_PATH}. Cannot initialize VehicleDataProcessor.")
        print("Please check the RELIABILITY_DATA_PATH variable in main_orchestrator.py.")
        # Fallback: create an empty DataFrame or an empty processor if you want to proceed with warnings.
        # For now, exiting might be safer if this data is critical.
        # As a temporary measure, we can try to proceed if the approved_vehicles_reliability.csv exists and is the primary source.
        # The VehicleDataProcessor itself has fallbacks if chart_data_filtered.csv is missing.
        print("Attempting to proceed, VehicleDataProcessor may issue warnings if chart_data_filtered.csv is essential and missing.")

    # You might want to make tax_rate, insurance, province configurable (e.g., from .env or args)
    processor = VehicleDataProcessor(reliability_data_path=str(RELIABILITY_DATA_PATH)) 

    print("\nProcessing new listings with VehicleDataProcessor (calculating TCO, deal scores, etc.)...")
    # The process_car_listings method expects a list of dictionaries
    df_processed_new_listings = processor.process_car_listings(all_new_unprocessed_listings)

    if df_processed_new_listings.empty:
        print("No listings survived the VehicleDataProcessor's filters and TCO calculation. Nothing to export.")
        return

    print(f"\nSuccessfully processed {len(df_processed_new_listings)} new listings through VehicleDataProcessor.")
    print("Exporting to CSV (appending, removing stale, de-duplicating)...")
    
    # The export_to_csv method in VehicleDataProcessor handles merging with existing output.csv,
    # removing duplicates (based on URL, keeping latest), and aging out old entries.
    final_output_path = processor.export_to_csv(df_processed_new_listings, str(OUTPUT_CSV_PATH))
    
    print(f"\nOrchestration complete. Results saved to {final_output_path}")

if __name__ == "__main__":
    main() 