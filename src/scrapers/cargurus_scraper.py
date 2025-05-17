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
import random # Added for random delays

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

    # Constants for retry mechanism
    MAX_RETRIES = 3
    INITIAL_RETRY_DELAY_S = 10
    MAX_RETRY_DELAY_S = 60

    def __init__(self, postal_code="L6M3S7", approved_vehicles_list=None):
        """Initialize the CarGurus scraper."""
        super().__init__("CarGurus.ca")
        self.base_url = "https://www.cargurus.ca"
        self.postal_code = postal_code.replace(" ", "") # Ensure no spaces
        self.approved_vehicles = approved_vehicles_list if approved_vehicles_list else []

        # Dynamically build the search URL
        # Old URL structure:
        # f"{self.base_url}/Cars/searchResults.action?"
        # f"zip={self.postal_code}&inventorySearchWidgetType=AUTO&sortDir=ASC&sortType=DEAL_SCORE"
        # f"&shopByTypes=NEAR_BY&minPrice=500&maxPrice={self.MAX_PRICE}&distance={self.SEARCH_RADIUS_MILES}"
        
        # New URL structure based on user's finding for potentially more listings:
        # Parameters like minPrice, maxPrice, distance are retained.
        # sourceContext is from user's URL. entitySelectingHelper.selectedEntity is kept empty as per original scraper comments.
        self.search_url = (
            f"{self.base_url}/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action?"
            f"sourceContext=carGurusHomePageModel&zip={self.postal_code.lower()}"
            f"&minPrice=500&maxPrice={self.MAX_PRICE}&distance={self.SEARCH_RADIUS_MILES}"
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
        Handles retries for network issues/blocks and detects CAPTCHAs.
        
        Args:
            limit (int): Maximum number of listings to scrape
            
        Returns:
            list: List of car listing dictionaries
        """
        print(f"Scraping {self.name} (expecting JSON response)...")
        listings = []
        retries = 0
        
        while retries <= self.MAX_RETRIES:
            driver = None # Initialize driver to None for each attempt
            try:
                driver = self._setup_driver()
                driver.get(self.search_url)
                time.sleep(random.uniform(5, 10)) # Randomize initial wait
                
                page_content = driver.page_source.lower() # Lowercase for easier keyword matching

                # CAPTCHA detection
                captcha_keywords = ["captcha", "verify you are human", "recaptcha", "are you a robot"]
                if any(keyword in page_content for keyword in captcha_keywords):
                    print(f"CAPTCHA detected on {self.name}. Stopping scrape for this site.")
                    if driver:
                        with open(f"cargurus_captcha_page_{time.strftime('%Y%m%d_%H%M%S')}.html", "w", encoding="utf-8") as f:
                            f.write(driver.page_source) # Save original case page
                        print("Saved CAPTCHA page content.")
                    break # Exit retry loop and return collected listings

                data = None
                try:
                    # Try to find JSON within <pre> tags first
                    pre_element = driver.find_element(By.TAG_NAME, "pre")
                    if pre_element:
                        pre_tag_content = pre_element.text
                        if pre_tag_content.strip(): # Ensure pre_tag_content is not empty
                            data = json.loads(pre_tag_content)
                            print("Successfully parsed JSON from <pre> tag.")
                        else:
                            print("Found <pre> tag, but it was empty.")
                    else: # Should not happen if find_element doesn't raise exception
                        print("No <pre> tag found by Selenium (unexpected).")
                        
                except (NoSuchElementException, json.JSONDecodeError) as e_pre:
                    print(f"Could not parse JSON from <pre> tag (Error: {e_pre}). Trying full page source.")
                    # If no <pre> tag or it doesn't contain valid JSON, try the whole page source
                    try:
                        # Use the original page_content for JSON parsing, not the lowercased one
                        data = json.loads(driver.page_source) 
                        print("Successfully parsed JSON from full page source.")
                    except json.JSONDecodeError as e_full:
                        print(f"Failed to decode JSON from page source. Error: {e_full}")
                        
                        # Block/Rate Limit detection (if JSON parsing fails)
                        block_keywords = ["access denied", "blocked", "too many requests", "rate limit"]
                        if any(keyword in page_content for keyword in block_keywords):
                            print(f"Potential block/rate limit detected on {self.name} based on keywords.")
                            raise ConnectionError("Block/Rate limit detected") # Will be caught by outer try's ConnectionError
                        
                        print(f"Page content (first 500 chars): {driver.page_source[:500]}")
                        with open(f"cargurus_error_page_{time.strftime('%Y%m%d_%H%M%S')}.html", "w", encoding="utf-8") as f:
                            f.write(driver.page_source)
                        print("Saved non-JSON page content to error file.")
                        if retries >= self.MAX_RETRIES:
                            print("Max retries reached for JSON parsing failure.")
                            break # Exit retry loop
                        else:
                            # This path (JSON error not identified as block) might not need a long retry.
                            # However, keeping consistent retry logic for now.
                            raise ConnectionError("JSON decode error, attempting retry.")


                if data is None: # If data is still None after trying both methods
                    print("Failed to extract JSON data from page using any method.")
                    # This could be a block if the page is not what's expected.
                    if len(driver.page_source) < 500: # Arbitrary small size, might indicate an error page
                        print(f"Page source is very small ({len(driver.page_source)} bytes), might be an error/empty page.")
                    
                    # Consider this a retryable scenario
                    raise ConnectionError("No JSON data extracted, attempting retry.")

                # --- JSON Parsing Logic ---
                json_listings = []
                if isinstance(data, list):
                    json_listings = data
                elif isinstance(data, dict):
                    possible_list_keys = ['listings', 'results', 'data', 'items', 'vehicles', 'cars', 'searchResponse', 'searchResult', 'dataFeed']
                    for key in possible_list_keys:
                        if key in data and isinstance(data[key], list):
                            json_listings = data[key]
                            print(f"Found listings under key: '{key}'")
                            break
                        elif key in data and isinstance(data[key], dict):
                            for sub_key in possible_list_keys:
                                if sub_key in data[key] and isinstance(data[key][sub_key], list):
                                    json_listings = data[key][sub_key]
                                    print(f"Found listings under nested key: '{key}.{sub_key}'")
                                    break
                            if json_listings: break
                    if not json_listings and 'searchResults' in data and 'listings' in data['searchResults']:
                        json_listings = data['searchResults']['listings']
                        print("Found listings under 'searchResults.listings'")

                if not json_listings:
                    print("Could not find a list of listings in the parsed JSON.")
                    print(f"JSON data (first 500 chars if dict, or type): {str(data)[:500] if isinstance(data, dict) else type(data)}")
                    with open(f"cargurus_parsed_data_{time.strftime('%Y%m%d_%H%M%S')}.json", "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=4)
                    print("Saved parsed JSON data for inspection.")
                    # This might also be a reason to retry, as the page structure might have temporarily changed or been an error page.
                    raise ConnectionError("Listings not found in JSON, attempting retry.")

                print(f"Found {len(json_listings)} items in JSON data. Processing up to limit ({limit})...")
                print(f"--> Total raw listings from CarGurus source before any local filtering: {len(json_listings)}")

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
                    
                    except Exception as e_item: # Renamed 'e' to 'e_item' to avoid conflict if 'e' is used in outer scope
                        print(f"Error processing a JSON item: {e_item}. Item: {str(item)[:200]}...") 
                        # Continue to the next item even if one fails
                        continue
                
                print(f"Successfully scraped {len(listings)} listings on this attempt.")
                break # Successful scrape, exit retry loop

            except (TimeoutException, ConnectionError, NoSuchElementException) as e: # Catch more specific errors for retrying
                print(f"Error during scrape attempt {retries + 1}/{self.MAX_RETRIES + 1}: {str(e)}")
                retries += 1
                if retries <= self.MAX_RETRIES:
                    # Exponential backoff with jitter
                    delay = min(self.MAX_RETRY_DELAY_S, self.INITIAL_RETRY_DELAY_S * (2 ** (retries - 1)))
                    jitter = delay * 0.2 * random.random() # Add up to 20% jitter
                    actual_delay = delay + jitter
                    print(f"Retrying in {actual_delay:.2f} seconds...")
                    time.sleep(actual_delay)
                else:
                    print(f"Max retries reached for {self.name}. Moving on.")
                    # Save page source if it was a connection or parsing issue on last attempt
                    if driver and 'page_content' not in locals() or (isinstance(e, ConnectionError) or isinstance(e, NoSuchElementException)):
                         try:
                            error_page_source = driver.page_source
                            with open(f"cargurus_final_error_page_{time.strftime('%Y%m%d_%H%M%S')}.html", "w", encoding="utf-8") as f:
                                f.write(error_page_source)
                            print("Saved page source at point of final error.")
                         except Exception as e_save:
                            print(f"Could not save page source during final error handling: {e_save}")
                    break # Exit retry loop
            
            except Exception as e:
                print(f"Major unexpected error in CarGurus JSON scraping process: {str(e)}")
                if driver:
                    try:
                        error_page_source = driver.page_source
                        with open(f"cargurus_major_error_page_{time.strftime('%Y%m%d_%H%M%S')}.html", "w", encoding="utf-8") as f:
                            f.write(error_page_source)
                        print("Saved page source at point of major unexpected error.")
                    except Exception as e_save:
                        print(f"Could not save page source during major error handling: {e_save}")
                break # Exit retry loop for major unexpected errors
        
            finally:
                if driver:
                    driver.quit()
                    print(f"WebDriver quit for attempt {retries + 1} if it was active.") # Clarified message
                
        print(f"Scraped a total of {len(listings)} listings from {self.name} after all attempts.")
        return listings 