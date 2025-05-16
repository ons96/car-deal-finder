import os
import argparse
from pathlib import Path
import pandas as pd
import shutil
import random # Added for playwright scraper if it uses it

# Import scrapers
# from src.scrapers.autotrader_scraper import AutoTraderScraper # REMOVE Selenium version
from src.scrapers.autotrader_scraper_playwright import AutoTraderPlaywrightScraper # ADD Playwright version
from src.scrapers.cargurus_scraper import CarGurusScraper
from src.scrapers.facebook_scraper import FacebookMarketplaceScraper # Selenium based
from src.scrapers.facebook_scraper_playwright import FacebookMarketplacePlaywrightScraper # ADDED

# Import data processor
from src.data_processor import VehicleDataProcessor

def main():
    """Main function to run the car deal finder."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Find the best used car deals")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of listings to scrape per site")
    parser.add_argument("--output", type=str, default="data/output.csv", help="Path to save output CSV")
    parser.add_argument("--method", type=str, default="playwright", choices=["selenium", "playwright", "crawl4ai", "scrapinggraph"], 
                        help="Scraping method to use for Facebook (defaults to playwright)")
    parser.add_argument("--sites", type=str, default="all", 
                        help="Sites to scrape (comma-separated: autotrader,cargurus,facebook,all)")
    
    args = parser.parse_args()
    
    # Set up paths
    base_dir = Path(__file__).parent.parent
    # Use the user-specified reliability file name
    reliability_data_path = base_dir / "data" / "approved_vehicles_reliability.csv"
    output_path = base_dir / args.output
    
    # Check if reliability data exists
    if not reliability_data_path.exists():
        print(f"Warning: Reliability data not found at {reliability_data_path}")
        # Attempt to locate it with the old name as a fallback, or from other locations
        # This part of the logic can be simplified if we strictly use the new name
        # For now, keeping the fallback logic but primary target is 'approved_vehicles_reliability.csv'
        print("Looking for data in alternative locations or with old name 'chart_data_filtered.csv'...")
        alternate_path_old_name = base_dir / "data" / "chart_data_filtered.csv"
        alternate_path_dashboard = base_dir / "dashboard-light_scraper" / "approved_vehicles_reliability.csv"
        alternate_path_dashboard_old_name = base_dir / "dashboard-light_scraper" / "chart_data_filtered.csv"
        
        # Check for the new name in dashboard-light_scraper first
        if alternate_path_dashboard.exists():
            reliability_data_path = alternate_path_dashboard
            print(f"Found reliability data at {reliability_data_path}")
        elif alternate_path_old_name.exists(): # Check for old name in data/
            reliability_data_path = alternate_path_old_name
            print(f"Found reliability data with old name at {reliability_data_path}")
        elif alternate_path_dashboard_old_name.exists(): # Check for old name in dashboard-light_scraper
            reliability_data_path = alternate_path_dashboard_old_name
            print(f"Found reliability data with old name at {reliability_data_path}")
        else:
            # Try to copy from an absolute path if nothing else works (using the new name)
            try:
                source_path_abs = Path("C:/Users/owens/Coding Projects/dashboard-light_scraper/approved_vehicles_reliability.csv")
                if not source_path_abs.exists(): # If new name not found, try old absolute name
                    source_path_abs = Path("C:/Users/owens/Coding Projects/dashboard-light_scraper/chart_data_filtered.csv")

                target_dir_abs = base_dir / "data"
                target_dir_abs.mkdir(exist_ok=True)
                target_path_abs = target_dir_abs / "approved_vehicles_reliability.csv" # Save with new name
                
                if source_path_abs.exists():
                    shutil.copy(source_path_abs, target_path_abs)
                    reliability_data_path = target_path_abs
                    print(f"Copied reliability data from {source_path_abs} to {reliability_data_path}")
                else:
                    print(f"Source data not found at {source_path_abs} or its variants.")
                    print("Please provide 'approved_vehicles_reliability.csv' manually in the 'data' folder.")
                    return
            except Exception as e:
                print(f"Error copying reliability data: {str(e)}")
                return
    
    # Initialize data processor
    data_processor = VehicleDataProcessor(reliability_data_path)
    
    # Determine which sites to scrape
    sites_to_scrape = args.sites.lower().split(',')
    if 'all' in sites_to_scrape:
        sites_to_scrape = ['autotrader', 'cargurus', 'facebook']
    
    # Initialize scrapers based on the chosen method
    scrapers = []
    
    # For Facebook, we select based on the scraping method
    facebook_scraper_class = None # Use class variable to hold the chosen scraper class
    if args.method == 'selenium':
        facebook_scraper_class = FacebookMarketplaceScraper
    elif args.method == 'playwright':
        facebook_scraper_class = FacebookMarketplacePlaywrightScraper
    elif args.method == 'crawl4ai':
        try:
            from src.scrapers.facebook_scraper_crawl4ai import FacebookMarketplaceCrawl4AIScraper
            facebook_scraper_class = FacebookMarketplaceCrawl4AIScraper
        except ImportError:
            print("Crawl4AI scraper selected but not available. Falling back to Playwright for Facebook.")
            facebook_scraper_class = FacebookMarketplacePlaywrightScraper # Fallback to Playwright
    # Default logic
    else:
        if args.method not in ['selenium', 'playwright', 'crawl4ai', 'scrapinggraph']:
             print(f"Warning: Unknown method '{args.method}' specified. Defaulting to Playwright for Facebook.")
        facebook_scraper_class = FacebookMarketplacePlaywrightScraper # Default to Playwright
    
    # Add selected scrapers
    if 'autotrader' in sites_to_scrape:
        scrapers.append(AutoTraderPlaywrightScraper()) # USE PLAYWRIGHT VERSION
    
    if 'cargurus' in sites_to_scrape:
        scrapers.append(CarGurusScraper()) # This is still Selenium-based, can be updated later
    
    if 'facebook' in sites_to_scrape:
        if facebook_scraper_class:
            scrapers.append(facebook_scraper_class())
        else:
            print("Error: No Facebook scraper class was selected. Check --method argument.")

    # Scrape listings
    all_listings = []
    
    for scraper in scrapers:
        print(f"\nUsing {scraper.name} with {type(scraper).__name__} scraper")
        listings = scraper.scrape(args.limit)
        all_listings.extend(listings)
    
    # Process listings
    if all_listings:
        print("\nProcessing listings...")
        results_df = data_processor.process_car_listings(all_listings)
        
        # Export results
        if not results_df.empty:
            output_file = data_processor.export_to_csv(results_df, output_path)
            print(f"\nResults exported to {output_file}")
            print(f"Found {len(results_df)} deals")
            
            # Display top 5 deals
            if len(results_df) > 0:
                print("\nTop 5 Best Deals:")
                top_deals = results_df.head(5)
                
                for i, (_, deal) in enumerate(top_deals.iterrows(), 1):
                    print(f"{i}. {deal['year']} {deal['make']} {deal['model']}")
                    print(f"   Price: ${deal['price']:.2f}, Mileage: {deal['mileage']:.0f} km")
                    print(f"   Composite Score: {deal['composite_score']:.2f}")
                    print(f"   Deal Score: {deal['deal_score']:.2f}")
                    print(f"   URL: {deal['url']}")
                    print()
        else:
            print("No valid listings found after processing")
    else:
        print("No listings found to process")

if __name__ == "__main__":
    main() 