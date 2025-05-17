from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import time
from tqdm import tqdm
import json

from src.scrapers.base_scraper import BaseScraper


class CarGurusScraper(BaseScraper):
    """Scraper for CarGurus.ca"""
    
    # Constants for scraper configuration
    MAX_PRICE = 20000
    SEARCH_RADIUS_KM = 250
    SEARCH_RADIUS_MILES = 155 # Approx 250km, CarGurus might use miles
    # Confirmed CarGurus uses entitySelectingHelper.selectedEntity= for make/model and specific year params
    # However, to avoid complexity of mapping all approved models to CarGurus entity IDs and managing multiple queries,
    # we will filter by price/location in URL, then filter by approved make/model/year in memory post-scrape.

    def __init__(self, postal_code="L6M3S7", approved_vehicles_list=None):
        """Initialize the CarGurus scraper."""
        super().__init__("CarGurus.ca")
        self.base_url = "https://www.cargurus.ca"
        self.postal_code = postal_code.replace(" ", "") # Ensure no spaces
        self.approved_vehicles = approved_vehicles_list if approved_vehicles_list else []

        # Dynamically build the search URL
        # Parameters: zip, minPrice, maxPrice, distance (likely miles)
        # sortType=DEAL_SCORE&sortDir=ASC is their default for good deals
        # inventorySearchWidgetType=AUTO is standard
        # shopByTypes=NEAR_BY seems relevant for distance based search
        self.search_url = (
            f"{self.base_url}/Cars/searchResults.action?"
            f"zip={self.postal_code}&inventorySearchWidgetType=AUTO&sortDir=ASC&sortType=DEAL_SCORE"
            f"&shopByTypes=NEAR_BY&minPrice=500&maxPrice={self.MAX_PRICE}&distance={self.SEARCH_RADIUS_MILES}"
        )
        
    def _setup_driver(self):
        """Setup and return a Selenium WebDriver."""
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        # To run non-headless for debugging, comment out the line above and uncomment the line below
        # chrome_options.headless = False
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={self.headers['User-Agent']}")
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        print("CarGurus WebDriver setup complete.")
        return driver
        
    def scrape(self, limit=100):
        """
        Scrape car listings from CarGurus.ca by parsing JSON data.
        
        Args:
            limit (int): Maximum number of listings to scrape
            
        Returns:
            list: List of car listing dictionaries
        """
        print(f"Scraping {self.name} (expecting JSON response)...")
        listings = []
        
        driver = None  # Initialize driver to None for the finally block
        try:
            driver = self._setup_driver()
            driver.get(self.search_url)
            
            # Wait a bit for the page (JSON) to fully load
            # This might be adjusted based on observation
            time.sleep(5) 
            
            page_content = driver.page_source
            
            # The actual JSON content might be embedded within <pre> tags or similar
            # Try to find JSON within <pre> tags first
            try:
                pre_tag_content = driver.find_element(By.TAG_NAME, "pre").text
                data = json.loads(pre_tag_content)
                print("Successfully parsed JSON from <pre> tag.")
            except (NoSuchElementException, json.JSONDecodeError):
                # If no <pre> tag or it doesn't contain valid JSON, try the whole page source
                try:
                    data = json.loads(page_content)
                    print("Successfully parsed JSON from full page source.")
                except json.JSONDecodeError as e:
                    print(f"Failed to decode JSON from page source. Error: {e}")
                    print(f"Page content (first 500 chars): {page_content[:500]}")
                    # Save the problematic content for debugging
                    with open("cargurus_error_page.html", "w", encoding="utf-8") as f:
                        f.write(page_content)
                    print("Saved non-JSON page content to cargurus_error_page.html")
                    return listings # Exit if JSON parsing fails

            # --- JSON Parsing Logic ---
            # This part is speculative and depends on the actual JSON structure.
            # Common root keys for listings could be 'listings', 'results', 'data', 'searchResult', 'items', etc.
            # We will try a few common ones or look for a list of dictionaries.
            
            json_listings = []
            if isinstance(data, list): # If the root is a list of listings
                json_listings = data
            elif isinstance(data, dict):
                # Try common keys that might hold the list of listings
                possible_list_keys = ['listings', 'results', 'data', 'items', 'vehicles', 'cars', 'searchResponse', 'searchResult', 'dataFeed']
                for key in possible_list_keys:
                    if key in data and isinstance(data[key], list):
                        json_listings = data[key]
                        print(f"Found listings under key: '{key}'")
                        break
                    # Sometimes listings are nested, e.g., data['results']['listings']
                    elif key in data and isinstance(data[key], dict):
                        for sub_key in possible_list_keys: # Check nested keys
                            if sub_key in data[key] and isinstance(data[key][sub_key], list):
                                json_listings = data[key][sub_key]
                                print(f"Found listings under nested key: '{key}.{sub_key}'")
                                break
                        if json_listings: break
                
                if not json_listings and 'searchResults' in data and 'listings' in data['searchResults']: # Specific common pattern
                     json_listings = data['searchResults']['listings']
                     print("Found listings under 'searchResults.listings'")

            if not json_listings:
                print("Could not find a list of listings in the parsed JSON.")
                print(f"JSON data (first 500 chars if dict, or type): {str(data)[:500] if isinstance(data, dict) else type(data)}")
                # Save the JSON structure for inspection if listings not found
                with open("cargurus_parsed_data.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)
                print("Saved parsed JSON data to cargurus_parsed_data.json for inspection.")
                return listings

            print(f"Found {len(json_listings)} items in JSON data. Processing up to limit ({limit})...")

            for item in tqdm(json_listings, desc="Processing JSON items"):
                if len(listings) >= limit:
                    break
                
                try:
                    # Extract data - field names based on debugged JSON structure
                    title = item.get('listingTitle', '')
                    year = item.get('carYear')
                    make = item.get('makeName', '')
                    model = item.get('modelName', '')
                    price = item.get('price') # This is a float
                    
                    # Mileage: prefer the direct numeric field if available
                    mileage_val = item.get('mileage') 
                    if mileage_val is None and 'unitMileage' in item and isinstance(item['unitMileage'], dict):
                        mileage_val = item['unitMileage'].get('value')
                    mileage = int(mileage_val) if mileage_val is not None else None
                    
                    # Construct URL using the item ID
                    item_id = item.get('id')
                    url = None
                    if item_id:
                        # This URL structure is a common one for CarGurus, might need adjustment if it doesn't lead to a user-viewable page.
                        # Using the viewPrintableDeal.action variant as it was in the previous attempt.
                        url = f"{self.base_url}/Cars/inventorylisting/viewPrintableDeal.action?inventoryListing={item_id}&sourceContext=carGurusHomePageModel"
                    
                    body_type_str = item.get('bodyTypeName', '')
                    body_type = None
                    # Standardize body type
                    bt_lower = body_type_str.lower()
                    if "sedan" in bt_lower: body_type = "sedan"
                    elif "coupe" in bt_lower: body_type = "coupe"
                    elif "hatchback" in bt_lower: body_type = "hatchback"
                    elif "suv" in bt_lower or "sport utility" in bt_lower : body_type = "suv"
                    elif "truck" in bt_lower: body_type = "truck"
                    elif "van" in bt_lower: body_type = "van"
                    elif "minivan" in bt_lower: body_type = "van"
                    else: body_type = "sedan" # Default if not matched

                    # --- Apply Make/Model/Year Filter ---
                    is_approved = False
                    if self.approved_vehicles:
                        scraped_make_lc = str(make).lower().strip()
                        scraped_model_norm = str(model).lower().replace('-', ' ').strip()
                        scraped_year_int = int(year) if year is not None else 0

                        for approved_make, approved_model, approved_year in self.approved_vehicles:
                            if (scraped_make_lc == approved_make and 
                                scraped_year_int == approved_year and 
                                scraped_model_norm.startswith(approved_model)):
                                is_approved = True
                                break
                    else:
                        is_approved = True # If no approved list, don't filter

                    if not is_approved:
                        # print(f"Skipping unapproved vehicle: {year} {make} {model}")
                        continue
                    # --- End Filter ---

                    # Basic validation
                    if not all([url, year, make, model, price is not None, mileage is not None]):
                        print(f"Skipping item due to missing core data after mapping: Year-{year}, Make-{make}, Model-{model}, Price-{price}, Mileage-{mileage}, URL-{url}. Original Title: '{title}'")
                        # print(f"  Full item dump: {json.dumps(item, indent=2)}") # For deeper debugging if needed
                        continue

                    listing_data = {
                        'url': url,
                        'title': title,
                        'year': int(year) if year is not None else None,
                        'make': str(make),
                        'model': str(model),
                        'price': float(price) if price is not None else None,
                        'mileage': int(mileage) if mileage is not None else None,
                        'body_type': body_type,
                        'source': self.name
                    }
                    listings.append(listing_data)
                
                except Exception as e:
                    print(f"Error processing a JSON item: {e}. Item: {str(item)[:200]}...") 
                    continue
            
        except Exception as e:
            print(f"Major error in CarGurus JSON scraping process: {str(e)}")
            # If driver is active and an error occurs, try to save page source for inspection
            if driver and 'page_content' not in locals(): # If page_content wasn't captured before error
                try:
                    error_page_source = driver.page_source
                    with open("cargurus_error_page_at_exception.html", "w", encoding="utf-8") as f:
                        f.write(error_page_source)
                    print("Saved page source at point of major error to cargurus_error_page_at_exception.html")
                except Exception as e_save:
                    print(f"Could not save page source during error handling: {e_save}")
        
        finally:
            if driver:
                driver.quit()
                
        print(f"Scraped {len(listings)} listings from {self.name} using JSON parsing.")
        return listings 