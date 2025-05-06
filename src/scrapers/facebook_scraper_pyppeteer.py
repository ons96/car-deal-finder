import asyncio
import pyppeteer
import os
import time
from dotenv import load_dotenv
from tqdm import tqdm

from src.scrapers.base_scraper import BaseScraper

# Load environment variables
load_dotenv()


class FacebookMarketplacePyppeteerScraper(BaseScraper):
    """Scraper for Facebook Marketplace using Pyppeteer"""
    
    def __init__(self):
        """Initialize the Facebook Marketplace scraper."""
        super().__init__("Facebook Marketplace")
        
        self.base_url = "https://www.facebook.com"
        self.marketplace_url = f"{self.base_url}/marketplace/category/vehicles"
        
        # Credentials from environment variables (optional)
        self.email = os.environ.get('FACEBOOK_EMAIL')
        self.password = os.environ.get('FACEBOOK_PASSWORD')
        
    async def _setup_browser(self):
        """Setup and return a Pyppeteer browser."""
        browser = await pyppeteer.launch({
            'headless': True,  # Set to False to see the browser
            'args': [
                '--no-sandbox', 
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--disable-gpu',
                '--window-size=1920,1080',
            ]
        })
        
        page = await browser.newPage()
        
        # Set user agent
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        
        # Set viewport
        await page.setViewport({'width': 1920, 'height': 1080})
        
        return browser, page
        
    async def _login(self, page):
        """Log in to Facebook (optional, but may improve results)."""
        if not self.email or not self.password:
            print("Facebook credentials not provided, skipping login")
            return False
            
        try:
            await page.goto(f"{self.base_url}/login", {'waitUntil': 'networkidle0'})
            
            # Accept cookies if prompt appears
            try:
                cookie_button = await page.waitForSelector("button[data-testid='cookie-policy-manage-dialog-accept-button']", {'timeout': 5000})
                if cookie_button:
                    await cookie_button.click()
                    await page.waitForTimeout(1000)
            except Exception:
                pass
            
            # Enter email
            await page.type("input#email", self.email)
            
            # Enter password
            await page.type("input#pass", self.password)
            
            # Click login button
            await page.click("button[name='login']")
            
            # Wait for login to complete
            await page.waitForSelector("div[role='banner']", {'timeout': 15000})
            
            print("Successfully logged in to Facebook")
            return True
            
        except Exception as e:
            print(f"Error logging in to Facebook: {str(e)}")
            return False
    
    async def _scrape_async(self, limit=100):
        """Asynchronous scraping method."""
        listings = []
        
        try:
            browser, page = await self._setup_browser()
            
            # Try to login (optional)
            await self._login(page)
            
            # Navigate to marketplace vehicles category
            await page.goto(self.marketplace_url, {'waitUntil': 'networkidle0', 'timeout': 60000})
            
            # Wait for listings to load
            await page.waitForSelector("div[aria-label='Collection of Marketplace items']", {'timeout': 20000})
            
            # Apply filters for vehicle type (sedan, coupe, hatchback)
            try:
                # Open vehicle type filter
                vehicle_type_button = await page.waitForXPath("//span[text()='Vehicle Type']", {'timeout': 10000})
                if vehicle_type_button:
                    await vehicle_type_button.click()
                    await page.waitForTimeout(1000)
                    
                    # Select desired vehicle types
                    for vehicle_type in ["Sedan", "Coupe", "Hatchback"]:
                        try:
                            checkbox = await page.waitForXPath(f"//span[text()='{vehicle_type}']", {'timeout': 5000})
                            if checkbox:
                                await checkbox.click()
                                await page.waitForTimeout(500)
                        except Exception:
                            continue
                    
                    # Apply filters
                    apply_button = await page.waitForXPath("//span[text()='Apply']", {'timeout': 5000})
                    if apply_button:
                        await apply_button.click()
                        await page.waitForTimeout(3000)
            except Exception as e:
                print(f"Couldn't apply vehicle type filters: {str(e)}")
            
            # Scroll to load more listings
            listing_container = await page.querySelector("div[aria-label='Collection of Marketplace items']")
            
            for _ in tqdm(range(min(10, limit // 10)), desc="Scrolling for listings"):
                await page.evaluate("""
                    (container) => {
                        container.scrollTop = container.scrollHeight;
                    }
                """, listing_container)
                await page.waitForTimeout(1500)
            
            # Extract all listing items
            listing_elements = await page.querySelectorAll("div[data-testid='marketplace_feed_item']")
            print(f"Found {len(listing_elements)} listing elements")
            
            # Process listings
            for element in listing_elements[:limit]:
                try:
                    # Extract listing link
                    link_element = await element.querySelector("a")
                    url = await page.evaluate("(element) => element.href", link_element)
                    
                    # Extract title (which often includes year, make, and model)
                    title_element = await element.querySelector("span.x1lliihq")
                    title = await page.evaluate("(element) => element.textContent", title_element)
                    
                    # Extract price
                    price_element = await element.querySelector("span.x193iq5w")
                    price_text = await page.evaluate("(element) => element.textContent", price_element)
                    price = self._extract_price(price_text)
                    
                    # Extract year, make, model from title
                    year = self._extract_year(title)
                    make, model = self._extract_make_model(title)
                    
                    # Extract additional info from subtitle (may contain mileage)
                    subtitle = ""
                    try:
                        subtitle_element = await element.querySelector("span.x1a1fy89")
                        if subtitle_element:
                            subtitle = await page.evaluate("(element) => element.textContent", subtitle_element)
                            # Try to extract mileage from subtitle
                            mileage = self._extract_mileage(subtitle)
                    except Exception:
                        mileage = None
                    
                    # If no mileage in subtitle, we'll set a default
                    if not mileage:
                        # Can't get mileage directly from listing preview
                        # We'd need to click each listing for details
                        mileage = 80000  # Default/placeholder value
                    
                    # Body type might be in the title or subtitle
                    body_type = None
                    for bt in ["sedan", "coupe", "hatchback", "suv", "truck", "van"]:
                        if bt in title.lower() or (subtitle and bt in subtitle.lower()):
                            body_type = bt
                            break
                    
                    # Default to sedan if not found
                    if not body_type:
                        body_type = "sedan"
                    
                    # Create listing record
                    listing = {
                        'url': url,
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
                    if all([url, year, make, model, price, mileage]):
                        listings.append(listing)
                
                except Exception as e:
                    print(f"Error extracting listing: {str(e)}")
                    continue
                
        except Exception as e:
            print(f"Error scraping {self.name}: {str(e)}")
        
        finally:
            if 'browser' in locals():
                await browser.close()
                
        return listings
    
    def scrape(self, limit=100):
        """
        Scrape car listings from Facebook Marketplace.
        
        Args:
            limit (int): Maximum number of listings to scrape
            
        Returns:
            list: List of car listing dictionaries
        """
        print(f"Scraping {self.name}...")
        
        # Run the async scraping function in an event loop
        loop = asyncio.get_event_loop()
        listings = loop.run_until_complete(self._scrape_async(limit))
        
        print(f"Scraped {len(listings)} listings from {self.name}")
        return listings 