import pandas as pd
from pathlib import Path
import sys
import os
import argparse
import asyncio
import csv
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

# Add src directory to Python path to allow importing modules from there
# This assumes main_orchestrator.py is in the project root, and modules are in ./src/
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root / 'src'))

try:
    from process_facebook_data import parse_facebook_csv
    from data_processor import VehicleDataProcessor
    # Assuming scrapers are in src.scrapers and __init__.py exports them
    from scrapers import AutoTraderScraper, CarGurusScraper
    from src.processors.approved_vehicles_processor import ApprovedVehiclesProcessor
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

async def main(args):
    """Main function to orchestrate the scraping process."""
    print("\n" + "="*80)
    print("STARTING CAR DEAL FINDER ORCHESTRATOR")
    print("="*80 + "\n")
    
    # Initialize approved vehicles processor
    processor = ApprovedVehiclesProcessor()
    processor.load_approved_vehicles()
    print(f"Successfully loaded {len(processor.approved_vehicles)} records from {processor.csv_path} and created {len(processor.approved_vehicles_by_make_model)} unique make/model pairs for approval.")
    
    # Get approved vehicles list for scrapers
    approved_vehicles_list = processor.get_approved_vehicles_list()
    print(f"Extracted {len(approved_vehicles_list)} approved vehicle criteria for scrapers from processor.")
    
    # Initialize scrapers with approved vehicles list
    scrapers = [
        AutoTraderScraper(postal_code=args.postal_code, approved_vehicles_list=approved_vehicles_list),
        CarGurusScraper(postal_code=args.postal_code, approved_vehicles_list=approved_vehicles_list)
    ]
    
    # Load existing URLs from output.csv
    existing_urls = set()
    if os.path.exists(args.output):
        with open(args.output, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('url'):
                    existing_urls.add(row['url'])
        print(f"Loaded {len(existing_urls)} URLs from {args.output}. {len(existing_urls)} seem fully processed.")
    
    # Scrape from each source
    all_listings = []
    for scraper in scrapers:
        try:
            print(f"\nScraping from {scraper.name}...")
            listings = await scraper.scrape(limit=args.limit)
            print(f"Found {len(listings)} listings from {scraper.name}")
            all_listings.extend(listings)
        except Exception as e:
            print(f"Error scraping {scraper.name}: {str(e)}")
    
    print(f"\nTotal of {len(all_listings)} raw listings gathered from all sources before de-duplication against {args.output}.")
    
    # Filter out duplicates
    new_listings = [listing for listing in all_listings if listing['url'] not in existing_urls]
    
    if not new_listings:
        print("All gathered listings were already processed or duplicates. No new data to add to output.csv.")
        return
    
    # Append new listings to output.csv
    fieldnames = ['url', 'title', 'year', 'make', 'model', 'price', 'mileage', 'body_type', 'source']
    file_exists = os.path.exists(args.output)
    
    with open(args.output, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_listings)
    
    print(f"Added {len(new_listings)} new listings to {args.output}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape car listings from multiple sources')
    parser.add_argument('--postal-code', type=str, default="L6M3S7", help='Postal code for location-based search')
    parser.add_argument('--limit', type=int, default=100, help='Maximum number of listings to scrape per source')
    parser.add_argument('--output', type=str, default='data/output.csv', help='Output CSV file path')
    
    args = parser.parse_args()
    asyncio.run(main(args)) 