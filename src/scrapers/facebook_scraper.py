import requests
import os
import json
from dotenv import load_dotenv
import time
from tqdm import tqdm

from src.scrapers.base_scraper import BaseScraper

# Load environment variables
load_dotenv()


class FacebookMarketplaceScraper(BaseScraper):
    """Scraper for Facebook Marketplace using ScrapingGraph AI"""
    
    def __init__(self):
        """Initialize the Facebook Marketplace scraper."""
        super().__init__("Facebook Marketplace")
        self.api_key = os.environ.get('SCRAPINGGRAPH_API_KEY')
        if not self.api_key:
            print("Warning: ScrapingGraph API key not found. Please set SCRAPINGGRAPH_API_KEY in .env file")
        
        self.base_url = "https://www.facebook.com"
        self.marketplace_url = f"{self.base_url}/marketplace/category/vehicles"
        
    def scrape(self, limit=100):
        """
        Scrape car listings from Facebook Marketplace using ScrapingGraph AI.
        
        Args:
            limit (int): Maximum number of listings to scrape
            
        Returns:
            list: List of car listing dictionaries
        """
        print(f"Scraping {self.name}...")
        
        if not self.api_key:
            print("Error: ScrapingGraph API key is required")
            return []
        
        listings = []
        
        try:
            # Configure the ScrapingGraph API request
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            # Define the data extraction schema
            extraction_data = {
                "url": self.marketplace_url,
                "instructions": "Extract used car listings from Facebook Marketplace. Focus on sedans, coupes, and hatchbacks.",
                "schema": {
                    "listings": {
                        "type": "array",
                        "description": "List of car listings",
                        "items": {
                            "type": "object",
                            "properties": {
                                "title": {
                                    "type": "string",
                                    "description": "Title of the listing"
                                },
                                "price": {
                                    "type": "string",
                                    "description": "Price of the vehicle including currency symbol"
                                },
                                "url": {
                                    "type": "string",
                                    "description": "URL to the full listing"
                                },
                                "location": {
                                    "type": "string",
                                    "description": "Location of the vehicle"
                                },
                                "mileage": {
                                    "type": "string",
                                    "description": "Mileage of the vehicle if available"
                                },
                                "description": {
                                    "type": "string",
                                    "description": "Any additional description text"
                                }
                            },
                            "required": ["title", "price", "url"]
                        }
                    }
                },
                "pagination": {
                    "strategy": "scroll",
                    "scroll_count": min(10, limit // 10),
                    "scroll_delay": 1000
                },
                "browser": {
                    "cookies": {
                        "accept_all": True
                    },
                    "size": {
                        "width": 1920,
                        "height": 1080
                    }
                }
            }
            
            # Make the API request
            api_url = "https://api.scrapinggraph.ai/scrape"  # Example endpoint
            response = requests.post(api_url, headers=headers, json=extraction_data)
            
            if response.status_code != 200:
                print(f"Error calling ScrapingGraph API: {response.status_code} - {response.text}")
                return []
            
            # Parse the results
            result = response.json()
            raw_listings = result.get("data", {}).get("listings", [])
            
            # Process each listing
            for item in raw_listings[:limit]:
                title = item.get("title", "")
                
                # Extract year, make, model from title
                year = self._extract_year(title)
                make, model = self._extract_make_model(title)
                
                # Extract price
                price = self._extract_price(item.get("price", ""))
                
                # Extract mileage if available, or use default
                mileage = None
                mileage_text = item.get("mileage", "")
                if mileage_text:
                    mileage = self._extract_mileage(mileage_text)
                
                # If no mileage found, check the description
                if not mileage:
                    description = item.get("description", "")
                    mileage = self._extract_mileage(description)
                
                # If still no mileage, use default value
                if not mileage:
                    mileage = 80000  # Default value
                
                # Determine body type from title or description
                body_type = None
                description = item.get("description", "").lower()
                
                for bt in ["sedan", "coupe", "hatchback", "suv", "truck", "van"]:
                    if bt in title.lower() or bt in description:
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