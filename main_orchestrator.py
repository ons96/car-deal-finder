import pandas as pd
from pathlib import Path
import sys
import os
import argparse

# Add src directory to Python path to allow importing modules from there
# This assumes main_orchestrator.py is in the project root, and modules are in ./src/
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root / 'src'))

try:
    from process_facebook_data import parse_facebook_csv
    from data_processor import VehicleDataProcessor
    # Assuming scrapers are in src.scrapers and __init__.py exports them
    from scrapers import AutoTraderScraper, CarGurusScraper
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Please ensure all required modules (parsers, processors, scrapers) are in the 'src' directory or subdirectories,")
    print("and that the script is run from the project root directory, or adjust PYTHONPATH.")
    sys.exit(1)

# --- Configuration ---
DATA_DIR = project_root / "data"
FACEBOOK_CSV_PATTERN = "facebook-*.csv"  # Pattern to find Facebook CSV files
OUTPUT_CSV_PATH = DATA_DIR / "output.csv"
# Path to the reliability data for VehicleDataProcessor (relative to project root)
# Adjust if your reliability data path is different or defined elsewhere for VehicleDataProcessor
RELIABILITY_DATA_PATH = DATA_DIR / "chart_data_filtered.csv" 

# New configurations for scrapers
DEFAULT_POSTAL_CODE = "L6M3S7" # Default postal code for scrapers
DEFAULT_SCRAPE_LIMIT = 100     # Default limit for each web scraper (can be overridden by CLI)
# Max price and search radius will use defaults defined within the scrapers for now,
# unless explicitly passed or made configurable via CLI later.

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

def main(args):
    print(f"Starting main orchestrator...")
    # print(f"Looking for Facebook CSVs in: {DATA_DIR} matching '{FACEBOOK_CSV_PATTERN}'")
    # print(f"Web scrapers will use postal code: {args.postal_code} and limit: {args.limit_per_scraper}")
    # print(f"Output will be managed in: {OUTPUT_CSV_PATH}")

    # --- Initialize VehicleDataProcessor first to get access to approved vehicles list ---
    # This also ensures critical data like approved_vehicles_reliability.csv is loaded upfront.
    if not (project_root / "data" / "approved_vehicles_reliability.csv").exists():
        print(f"CRITICAL ERROR: Primary approved vehicles data file 'approved_vehicles_reliability.csv' not found in {DATA_DIR}.")
        print("This file is essential for filtering. Please ensure it exists. Exiting.")
        sys.exit(1)
        
    if not RELIABILITY_DATA_PATH.exists():
        print(f"Warning: OLD reliability data file (chart_data_filtered.csv) not found at {RELIABILITY_DATA_PATH}.")
        print("VehicleDataProcessor will proceed, but some fallback reliability scores might be affected if primary approved list doesn't cover all vehicles.")

    processor = VehicleDataProcessor(reliability_data_path=str(RELIABILITY_DATA_PATH))
    
    # Check if processor loaded approved vehicles successfully
    if not hasattr(processor, 'approved_vehicles_data') or not processor.approved_vehicles_data:
        print("CRITICAL ERROR: VehicleDataProcessor did not load any approved vehicles data. Scraping and processing cannot continue effectively. Exiting.")
        sys.exit(1)

    approved_vehicles_list_for_scrapers = []
    for item in processor.approved_vehicles_data:
        if 'Make_lc' in item and 'Model_norm' in item and 'Year' in item:
            approved_vehicles_list_for_scrapers.append(
                (item['Make_lc'], item['Model_norm'], item['Year'])
            )
    print(f"Extracted {len(approved_vehicles_list_for_scrapers)} approved vehicle criteria for scrapers from processor.")
    if not approved_vehicles_list_for_scrapers:
        print("Warning: No approved vehicle criteria extracted for scrapers. They will not filter by make/model/year.")

    # --- Load details of already processed listings from output.csv ---
    already_processed_details = load_processed_urls_and_details(OUTPUT_CSV_PATH)
    
    all_raw_listings = [] # This will hold data from all sources before final filtering

    # --- Run Web Scrapers ---
    print(f"\n--- Running Web Scrapers (Limit: {args.limit_per_scraper}, Postal Code: {args.postal_code}) ---")

    # Initialize and run CarGurusScraper
    try:
        print("\nInitializing CarGurus Scraper...")
        # Using scraper's internal defaults for max_price, search_radius if not specified here
        # approved_vehicles_list is now passed for pre-filtering
        cargurus_scraper = CarGurusScraper(postal_code=args.postal_code, approved_vehicles_list=approved_vehicles_list_for_scrapers)
        print("Scraping CarGurus.ca...")
        cargurus_data = cargurus_scraper.scrape(limit=args.limit_per_scraper)
        if cargurus_data:
            print(f"CarGurus scraper found {len(cargurus_data)} listings.")
            all_raw_listings.extend(cargurus_data)
        else:
            print("CarGurus scraper returned no data.")
    except Exception as e_cg:
        print(f"Error during CarGurus scraping: {e_cg}")

    # Initialize and run AutoTraderScraper
    try:
        print("\nInitializing AutoTrader Scraper...")
        # Using scraper's internal defaults for max_price, search_radius if not specified here
        # approved_vehicles_list is now passed for pre-filtering
        autotrader_scraper = AutoTraderScraper(postal_code=args.postal_code, approved_vehicles_list=approved_vehicles_list_for_scrapers)
        print("Scraping AutoTrader.ca...")
        autotrader_data = autotrader_scraper.scrape(limit=args.limit_per_scraper)
        if autotrader_data:
            print(f"AutoTrader scraper found {len(autotrader_data)} listings.")
            all_raw_listings.extend(autotrader_data)
        else:
            print("AutoTrader scraper returned no data.")
    except Exception as e_at:
        print(f"Error during AutoTrader scraping: {e_at}")
        
    # --- Process Facebook CSV Data ---
    print(f"\n--- Processing Facebook CSVs ---")
    print(f"Looking for Facebook CSVs in: {DATA_DIR} matching '{FACEBOOK_CSV_PATTERN}'")
    facebook_csv_files = list(DATA_DIR.glob(FACEBOOK_CSV_PATTERN))
    
    if not facebook_csv_files:
        print("No Facebook CSV files found.")
    else:
        print(f"Found Facebook CSV files: {[f.name for f in facebook_csv_files]}")
        fb_listings_count = 0
        for fb_csv_path in facebook_csv_files:
            print(f"Processing Facebook file: {fb_csv_path.name}...")
            parsed_listings = parse_facebook_csv(fb_csv_path)
            if parsed_listings:
                fb_listings_count += len(parsed_listings)
                all_raw_listings.extend(parsed_listings)
            else:
                print(f"No listings parsed from {fb_csv_path.name}.")
        print(f"Found {fb_listings_count} total listings from Facebook CSVs.")

    if not all_raw_listings:
        print("\nNo listings gathered from any source (Web Scrapers or Facebook CSVs). Exiting.")
        return

    print(f"\nTotal of {len(all_raw_listings)} raw listings gathered from all sources before de-duplication against output.csv.")

    # --- Filter all_raw_listings against already_processed_details ---
    final_unprocessed_listings = []
    for listing in all_raw_listings:
        listing_url = listing.get('url')
        if not listing_url:
            # print(f"Skipping a listing due to missing URL: {str(listing)[:100]}") # Avoid printing huge dicts
            continue
        
        if already_processed_details.get(listing_url, False): # True if URL exists AND fully_processed
            # print(f"Skipping already fully processed listing (URL): {listing_url}")
            continue
        final_unprocessed_listings.append(listing)

    if not final_unprocessed_listings:
        print("\nAll gathered listings were already processed or duplicates. No new data to add to output.csv.")
        return
        
    print(f"\nProceeding to process {len(final_unprocessed_listings)} new/unprocessed listings with VehicleDataProcessor.")

    # --- Process combined and filtered listings ---
    # The VehicleDataProcessor.process_car_listings method expects a list of dictionaries
    df_processed_listings = processor.process_car_listings(final_unprocessed_listings)

    if df_processed_listings.empty:
        print("No listings survived the VehicleDataProcessor's filters and TCO calculation. Nothing to export.")
        return

    print(f"\nSuccessfully processed {len(df_processed_listings)} listings through VehicleDataProcessor.")
    print("Exporting to CSV (appending, removing stale, de-duplicating)...\n")
    
    final_output_path = processor.export_to_csv(df_processed_listings, str(OUTPUT_CSV_PATH))
    
    print(f"Orchestration complete. Results saved to {final_output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Car Deal Finder Orchestrator: Scrapes and processes car listings.")
    parser.add_argument(
        "--postal_code",
        type=str,
        default=DEFAULT_POSTAL_CODE,
        help=f"Postal code for web scrapers (default: {DEFAULT_POSTAL_CODE})."
    )
    parser.add_argument(
        "--limit_per_scraper",
        type=int,
        default=DEFAULT_SCRAPE_LIMIT,
        help=f"Maximum number of listings to fetch per web scraper (default: {DEFAULT_SCRAPE_LIMIT}). Set high for extensive scraping."
    )
    # Placeholder for future arguments like --max_price, --radius, etc.
    
    parsed_args = parser.parse_args()
    main(parsed_args) 