import os
import argparse
from pathlib import Path
import pandas as pd
import shutil

# Import scrapers
from src.scrapers.autotrader_scraper import AutoTraderScraper
from src.scrapers.cargurus_scraper import CarGurusScraper
from src.scrapers.facebook_scraper import FacebookMarketplaceScraper
from src.scrapers.facebook_scraper_pyppeteer import FacebookMarketplacePyppeteerScraper
from src.scrapers.facebook_scraper_crawl4ai import FacebookMarketplaceCrawl4AIScraper

# Import data processor
from src.data_processor import VehicleDataProcessor

def main():
    """Main function to run the car deal finder."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Find the best used car deals")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of listings to scrape per site")
    parser.add_argument("--output", type=str, default="data/output.csv", help="Path to save output CSV")
    parser.add_argument("--method", type=str, default="crawl4ai", choices=["selenium", "pyppeteer", "crawl4ai", "scrapinggraph"], 
                        help="Scraping method to use (defaults to crawl4ai)")
    parser.add_argument("--sites", type=str, default="all", 
                        help="Sites to scrape (comma-separated: autotrader,cargurus,facebook,all)")
    
    args = parser.parse_args()
    
    # Set up paths
    base_dir = Path(__file__).parent.parent
    reliability_data_path = base_dir / "dashboard-light_scraper" / "chart_data_filtered.csv"
    output_path = base_dir / args.output
    
    # Check if reliability data exists
    if not reliability_data_path.exists():
        print(f"Warning: Reliability data not found at {reliability_data_path}")
        print("Looking for data in the current directory...")
        reliability_data_path = base_dir / "data" / "chart_data_filtered.csv"
        
        # If still not found, copy from source project if possible
        if not reliability_data_path.exists():
            try:
                source_path = Path("C:/Users/owens/Coding Projects/dashboard-light_scraper/chart_data_filtered.csv")
                target_dir = base_dir / "data"
                target_dir.mkdir(exist_ok=True)
                
                if source_path.exists():
                    shutil.copy(source_path, reliability_data_path)
                    print(f"Copied reliability data to {reliability_data_path}")
                else:
                    print(f"Source data not found at {source_path}")
                    print("Please provide reliability data manually")
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
    facebook_scraper = None
    if args.method == 'selenium':
        facebook_scraper = FacebookMarketplaceScraper
    elif args.method == 'pyppeteer':
        facebook_scraper = FacebookMarketplacePyppeteerScraper
    elif args.method == 'crawl4ai':
        facebook_scraper = FacebookMarketplaceCrawl4AIScraper
    # Default to the best option if not specified or for unusual values
    else:
        facebook_scraper = FacebookMarketplaceCrawl4AIScraper
    
    # Add selected scrapers
    if 'autotrader' in sites_to_scrape:
        scrapers.append(AutoTraderScraper())
    
    if 'cargurus' in sites_to_scrape:
        scrapers.append(CarGurusScraper())
    
    if 'facebook' in sites_to_scrape:
        scrapers.append(facebook_scraper())
    
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
                    print(f"   QIR Rate: {deal['qir_rate']}, Defect Rate: {deal['defect_rate']}")
                    print(f"   Deal Score: {deal['deal_score']:.2f}")
                    print(f"   URL: {deal['url']}")
                    print()
        else:
            print("No valid listings found after processing")
    else:
        print("No listings found to process")

if __name__ == "__main__":
    main() 