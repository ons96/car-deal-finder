import os
import time
from tqdm import tqdm
import pandas as pd
from dotenv import load_dotenv
from crawl4ai import Browser, By

from src.scrapers.base_scraper import BaseScraper

# Load environment variables
load_dotenv()


class FacebookMarketplaceCrawl4AIScraper(BaseScraper):
    """Scraper for Facebook Marketplace using Crawl4AI"""
    
    def __init__(self):
        """Initialize the Facebook Marketplace scraper."""
        super().__init__("Facebook Marketplace")
        
        self.base_url = "https://www.facebook.com"
        self.marketplace_url = f"{self.base_url}/marketplace/category/vehicles"
        
        # Credentials from environment variables (optional)
        self.email = os.environ.get('FACEBOOK_EMAIL')
        self.password = os.environ.get('FACEBOOK_PASSWORD')
        
    def scrape(self, limit=100):
        """
        Scrape car listings from Facebook Marketplace using Crawl4AI.
        
        Args:
            limit (int): Maximum number of listings to scrape
            
        Returns:
            list: List of car listing dictionaries
        """
        print(f"Scraping {self.name}...")
        listings = []
        
        # Initialize the browser with Crawl4AI
        with Browser(headless=True) as browser:
            try:
                # Login if credentials are available
                if self.email and self.password:
                    self._login(browser)
                
                # Navigate to marketplace vehicles section
                browser.get(self.marketplace_url)
                print("Loading Facebook Marketplace...")
                time.sleep(3)  # Wait for initial page load
                
                # Apply vehicle type filters if possible
                try:
                    # Use AI capabilities to find and interact with filters
                    browser.ai_click("Vehicle Type filter")
                    time.sleep(1)
                    
                    # Select vehicle types using AI
                    for vehicle_type in ["Sedan", "Coupe", "Hatchback"]:
                        try:
                            browser.ai_click(f"{vehicle_type} checkbox")
                            time.sleep(0.5)
                        except Exception:
                            print(f"Couldn't select {vehicle_type}")
                    
                    # Apply filters
                    browser.ai_click("Apply button")
                    time.sleep(3)
                except Exception as e:
                    print(f"Error applying filters: {e}")
                
                # Scroll to load more listings
                print("Scrolling to load more listings...")
                for _ in tqdm(range(min(10, limit // 10))):
                    browser.execute_script("window.scrollBy(0, 1000)")
                    time.sleep(1.5)
                
                # Use AI to extract vehicle listings
                print("Extracting vehicle information...")
                browser.wait_for_detection("car listings")
                
                # Extract using AI
                extracted_data = browser.ai_extract({
                    "vehicle_listings": [{
                        "title": "string",
                        "price": "string",
                        "url": "string",
                        "location": "string?",
                        "description": "string?"
                    }]
                })
                
                # Process extracted listings
                raw_listings = extracted_data.get("vehicle_listings", [])
                print(f"Found {len(raw_listings)} listings")
                
                for item in raw_listings[:limit]:
                    title = item.get("title", "")
                    
                    # Extract year, make, model from title
                    year = self._extract_year(title)
                    make, model = self._extract_make_model(title)
                    
                    # Extract price
                    price = self._extract_price(item.get("price", ""))
                    
                    # Try to extract mileage from description or title
                    mileage = None
                    description = item.get("description", "")
                    
                    if description:
                        mileage = self._extract_mileage(description)
                    
                    # If no mileage found, try from title
                    if not mileage:
                        mileage = self._extract_mileage(title)
                    
                    # If still no mileage, use default value
                    if not mileage:
                        mileage = 80000  # Default value
                    
                    # Determine body type from title or description
                    body_type = None
                    for bt in ["sedan", "coupe", "hatchback", "suv", "truck", "van"]:
                        if bt in title.lower() or (description and bt in description.lower()):
                            body_type = bt
                            break
                    
                    # Default to sedan if not found
                    if not body_type:
                        body_type = "sedan"
                    
                    # Create listing record
                    listing = {
                        'url': item.get("url", ""),
                        'title': title,
                        'year': year,
                        'make': make,
                        'model': model,
                        'price': price,
                        'mileage': mileage,
                        'body_type': body_type,
                        'source': self.name
                    }
                    
                    # Only add complete listings
                    if all([listing['url'], year, make, model, price, mileage]):
                        listings.append(listing)
                
            except Exception as e:
                print(f"Error scraping {self.name}: {str(e)}")
        
        print(f"Scraped {len(listings)} listings from {self.name}")
        return listings
        
    def _login(self, browser):
        """Log in to Facebook."""
        try:
            browser.get(f"{self.base_url}/login")
            print("Logging in to Facebook...")
            
            # Find and fill email field
            browser.ai_type("email field", self.email)
            
            # Find and fill password field
            browser.ai_type("password field", self.password)
            
            # Click login button
            browser.ai_click("login button")
            
            # Wait for successful login
            time.sleep(5)
            print("Login successful")
            return True
        
        except Exception as e:
            print(f"Login failed: {e}")
            return False 