import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
from tqdm import tqdm
import random
import json

from src.scrapers.base_scraper import BaseScraper


class AutoTraderScraper(BaseScraper):
    """Scraper for AutoTrader.ca"""
    
    # Constants for scraper configuration (can be overridden by constructor params)
    DEFAULT_MAX_PRICE = 20000
    DEFAULT_SEARCH_RADIUS_KM = 250 # Autotrader's 'prx' likely uses km

    # Constants for retry mechanism
    MAX_RETRIES = 3
    INITIAL_RETRY_DELAY_S = 10
    MAX_RETRY_DELAY_S = 60
    
    def __init__(self, postal_code="L6M3S7", max_price=None, search_radius_km=None, approved_vehicles_list=None):
        """Initialize the AutoTrader scraper with dynamic search parameters."""
        super().__init__("AutoTrader.ca")
        self.base_url = "https://www.autotrader.ca"
        
        self.postal_code = postal_code.replace(" ", "") # Ensure no spaces
        self.max_price = max_price if max_price is not None else self.DEFAULT_MAX_PRICE
        self.search_radius_km = search_radius_km if search_radius_km is not None else self.DEFAULT_SEARCH_RADIUS_KM
        self.approved_vehicles = approved_vehicles_list if approved_vehicles_list else []

        # Construct the search URL dynamically
        # Common parameters:
        # rcp=100 (results per page)
        # rcs=0 (results offset/skip for pagination)
        # srt=9 (sort: 9 seems to be a common default, possibly "best match" or "date")
        # yRng=2010%2C2023 (Year range - keeping existing default for now, can be parameterized later if needed)
        # prx={search_radius_km} (proximity/distance)
        # loc={postal_code} (postal code)
        # sts=Used (status)
        # priceFrom, priceTo (speculative price parameters)
        # hprc=True (likely "has price")
        # wcp=True (unknown, keeping from original)
        # inMarket=advancedSearch (type of search)
        # Default vehicle types from original: /cars/sedan-coupe-hatchback/on/ (Ontario, specific types)
        # For broader search, might remove /sedan-coupe-hatchback and /on
        
        # Using a more general path for cars, assuming 'on' (Ontario) is not always desired.
        # Province filter can be added if 'loc' with postal code isn't sufficient or if it's a required field.
        # The original URL had "/on/" after "/cars/vehicle_types/", which implies provincial filtering.
        # For now, let's make it general and rely on postal code + radius.
        self.search_url = (
            f"{self.base_url}/cars/"
            f"?rcp=100&rcs=0&srt=9&yRng=2010%2C2023"
            f"&prx={self.search_radius_km}&loc={self.postal_code}"
            f"&priceFrom=500&priceTo={self.max_price}"
            f"&sts=Used&inMarket=advancedSearch&hprc=True&wcp=True"
        )
        print(f"AutoTrader Scraper initialized with URL: {self.search_url}")

    def _setup_driver(self):
        """Setup and return a Selenium WebDriver using undetected-chromedriver."""
        chrome_options = Options()
        chrome_options.add_argument("--headless") # For undetected-chromedriver, sometimes removing --headless or using new headless (options.headless = "new") is needed if issues arise.
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={self.headers['User-Agent']}")
        
        # driver = webdriver.Chrome(service=service, options=chrome_options) # Old way
        driver = uc.Chrome(options=chrome_options)
        return driver
        
    def scrape(self, limit=100):
        """
        Scrape car listings from AutoTrader.ca.
        Includes retry logic, CAPTCHA/block detection, and approved vehicle filtering.
        
        Args:
            limit (int): Maximum number of listings to scrape
            
        Returns:
            list: List of car listing dictionaries
        """
        print(f"Scraping {self.name}...")
        listings = []
        driver = None
        retries = 0

        while retries <= self.MAX_RETRIES:
            driver = None # Initialize driver to None for each attempt
            try:
                driver = self._setup_driver()
                print(f"Attempting to load URL: {self.search_url}")
                driver.get(self.search_url)
                time.sleep(random.uniform(3, 7)) # Wait for initial page load/redirects

                # Basic CAPTCHA/Block detection (more can be added)
                page_title_lower = driver.title.lower()
                page_source_lower = driver.page_source.lower()

                captcha_keywords = ["captcha", "verify you are human", "are you a robot", "security check"]
                block_keywords = ["access denied", "blocked", "forbidden", "site unavailable"]

                if any(keyword in page_title_lower for keyword in captcha_keywords) or \
                   any(keyword in page_source_lower for keyword in captcha_keywords):
                    print(f"CAPTCHA detected on {self.name}. Page title: {driver.title}. Stopping scrape for this site.")
                    if driver:
                        filepath = f"autotrader_captcha_page_{time.strftime('%Y%m%d_%H%M%S')}.html"
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(driver.page_source)
                        print(f"Saved CAPTCHA page content to {filepath}")
                    break # Exit retry loop immediately

                if any(keyword in page_title_lower for keyword in block_keywords) or \
                   any(keyword in page_source_lower for keyword in block_keywords):
                    print(f"Block detected on {self.name} (title: {driver.title}). Raising ConnectionError for retry.")
                    raise ConnectionError("Block/Rate limit detected based on page content")

                # Wait for listings to load - main indicator of a successful page
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.result-item"))
                )
                print("Main listing container detected.")

                # Get total pages to scrape (or estimate)
                # This logic might need adjustment based on how Autotrader displays total results
                # and if the limit is reached before processing all potential pages.
                # For now, we'll try to get all results up to the 'limit'.
                
                # Pagination loop
                current_page = 1
                processed_this_attempt = 0
                while True: # Loop for pages
                    print(f"Scraping page {current_page}...")
                    listing_elements = driver.find_elements(By.CSS_SELECTOR, "div.result-item")
                    if not listing_elements and current_page == 1:
                        print("No listings found on the first page. This might be a soft block or an issue with search criteria.")
                        # Potentially save page source for debugging
                        filepath = f"autotrader_no_listings_page_{time.strftime('%Y%m%d_%H%M%S')}.html"
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(driver.page_source)
                        print(f"Saved no-listings page content to {filepath}")
                        # Decide if this is a retryable offense or means no results
                        # For now, let's assume it might be temporary and let retry logic handle it if it leads to an exception.
                        # If it doesn't throw an exception here, the outer loop might just finish with 0 results.
                        # To force a retry, we could raise ConnectionError here.
                        # raise ConnectionError("No listings found on first page, might be a soft block.")
                        break # Break page loop, will then break retry loop if no listings added

                    print(f"Found {len(listing_elements)} elements on page {current_page}.")

                    for element_idx, element in enumerate(listing_elements):
                        if len(listings) >= limit:
                            print(f"Reached scrape limit of {limit} listings.")
                            break # Break item loop
                        
                        try:
                            url_element = element.find_element(By.CSS_SELECTOR, "a.link-overlay")
                            url = url_element.get_attribute("href")
                            title = element.find_element(By.CSS_SELECTOR, "h2.title").text.strip()
                            year = self._extract_year(title) # Assumes helper methods exist
                            make, model = self._extract_make_model(title) # Assumes helper methods exist

                            price_str_element = element.find_element(By.CSS_SELECTOR, "span.price-amount")
                            price = self._extract_price(price_str_element.text) if price_str_element else None
                            
                            mileage_str_element = element.find_element(By.CSS_SELECTOR, "span.kms") # Check if class is "kms" or similar
                            mileage = self._extract_mileage(mileage_str_element.text) if mileage_str_element else None
                            
                            # Extract body type (example, needs refinement based on actual HTML)
                            body_type = "unknown" # Default
                            try:
                                specs_list = element.find_elements(By.CSS_SELECTOR, "div.ad-specs li") # Example selector
                                for spec in specs_list:
                                    spec_text = spec.text.lower()
                                    if "sedan" in spec_text: body_type = "sedan"; break
                                    if "coupe" in spec_text: body_type = "coupe"; break
                                    if "hatchback" in spec_text: body_type = "hatchback"; break
                                    if "suv" in spec_text: body_type = "suv"; break
                                    if "truck" in spec_text: body_type = "truck"; break
                                    if "van" in spec_text or "minivan" in spec_text: body_type = "van"; break
                            except NoSuchElementException:
                                pass # Body type not found or different structure

                            # --- Apply Make/Model/Year Filter (if approved_vehicles is provided) ---
                            is_approved = False
                            if self.approved_vehicles:
                                if year and make and model: # Ensure we have data to filter on
                                    scraped_make_lc = str(make).lower().strip()
                                    scraped_model_norm = str(model).lower().replace('-', ' ').strip()
                                    scraped_year_int = int(year) # Assuming _extract_year returns int or convertible

                                    for approved_make, approved_model, approved_year_filter in self.approved_vehicles:
                                        if (scraped_make_lc == approved_make and
                                            scraped_year_int == approved_year_filter and
                                            scraped_model_norm.startswith(approved_model)):
                                            is_approved = True
                                            break
                                else: # Not enough info to check against approved list, treat as not approved if filtering is active
                                    is_approved = False
                            else:
                                is_approved = True # No filter list, so all are "approved"

                            if not is_approved:
                                # print(f"Skipping unapproved vehicle: {year} {make} {model}")
                                continue
                            # --- End Filter ---

                            if not all([url, year, make, model, price is not None, mileage is not None]):
                                print(f"Skipping item due to missing core data after extraction: Title='{title}', URL='{url}'")
                                continue

                            listing_data = {
                                'url': url, 'title': title, 'year': year, 'make': make, 'model': model,
                                'price': price, 'mileage': mileage, 'body_type': body_type, 'source': self.name
                            }
                            listings.append(listing_data)
                            processed_this_attempt +=1
                        
                        except Exception as e_item:
                            print(f"Error extracting details for one listing on page {current_page}, item {element_idx + 1}: {str(e_item)}")
                            # Potentially log the specific item's outerHTML for debugging if extraction fails often
                            # try:
                            #     print(f"Problematic element HTML: {element.get_attribute('outerHTML')[:500]}")
                            # except: pass
                            continue # Process next item

                    if len(listings) >= limit:
                        break # Break page loop

                    # Attempt to go to the next page
                    try:
                        # Look for a "Next" button - selector might need verification
                        next_button_candidates = driver.find_elements(By.CSS_SELECTOR, "a.page-direction-control.page-direction-control-right, a.next-page-link, button.next-page")
                        next_button = None
                        for btn in next_button_candidates:
                            if btn.is_displayed() and btn.is_enabled():
                                next_button = btn
                                break
                        
                        if next_button:
                            print(f"Found next page button. Text: '{next_button.text[:30]}'. Clicking...")
                            driver.execute_script("arguments[0].click();", next_button) # JS click can be more robust
                            time.sleep(random.uniform(3, 6)) # Wait for page to load
                            # Crucial: Wait for new content to appear to confirm page change
                            WebDriverWait(driver, 20).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div.result-item")) 
                                # Consider a more specific check if result-item is too generic or appears before full load
                            )
                            print("New page content loaded.")
                            current_page += 1
                        else:
                            print("No 'Next' button found or it's not interactable. Assuming end of results.")
                            break # Break page loop
                    except (NoSuchElementException, TimeoutException) as e_page:
                        print(f"Could not navigate to next page or content did not load: {str(e_page)}. Assuming end of results for this attempt.")
                        break # Break page loop
                    except Exception as e_next_page_general:
                        print(f"Unexpected error during next page navigation: {e_next_page_general}")
                        # Save page source for debugging
                        filepath = f"autotrader_nextpage_error_{time.strftime('%Y%m%d_%H%M%S')}.html"
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(driver.page_source)
                        print(f"Saved page content at next page error to {filepath}")
                        break # Break page loop

                if processed_this_attempt > 0 or not listings: # if we processed some, or if we have no listings and broke early from page 1
                    print(f"Finished scraping attempt {retries + 1}. Listings collected in this attempt: {processed_this_attempt}. Total: {len(listings)}")
                
                if len(listings) >= limit: # If limit reached, successful overall
                    print(f"Scraping limit ({limit}) reached for {self.name}.")
                    break # Exit retry loop

                if current_page > 1 and processed_this_attempt == 0 and len(listing_elements) > 0:
                    print("Processed multiple pages but last page had elements yet no new listings were added (likely all filtered out). Considering this a completed run for current filters.")
                    break # Exit retry loop - successfully scraped all available matching items

                if not listings and current_page == 1 and not listing_elements: # No results found at all
                    print("No listings found matching criteria after first page load. Ending.")
                    break

                # If we are here, it means we didn't reach the limit, and there might be more pages or it was an issue.
                # If no listings were processed in this attempt but the loop didn't break due to 'no next page', it might be an issue.
                # However, the retry logic handles broad exceptions. If we successfully processed all pages, this loop will also break.
                break # Default break from retry loop if everything in try block completed without specific error for retry.


            except (TimeoutException, ConnectionError) as e_retry:
                print(f"A retryable error occurred on attempt {retries + 1}/{self.MAX_RETRIES + 1} for {self.name}: {str(e_retry)}")
                retries += 1
                if driver: # Save page source on retryable error
                    filepath = f"autotrader_retry_error_page_{retries}_{time.strftime('%Y%m%d_%H%M%S')}.html"
                    try:
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(driver.page_source)
                        print(f"Saved page source at retryable error to {filepath}")
                    except Exception as e_save:
                        print(f"Could not save page source during retry handling: {e_save}")

                if retries <= self.MAX_RETRIES:
                    delay = min(self.MAX_RETRY_DELAY_S, self.INITIAL_RETRY_DELAY_S * (2 ** (retries - 1)))
                    jitter = delay * 0.2 * random.random()
                    actual_delay = delay + jitter
                    print(f"Retrying in {actual_delay:.2f} seconds...")
                    if driver: driver.quit(); driver = None # Ensure driver is closed before sleep
                    time.sleep(actual_delay)
                else:
                    print(f"Max retries reached for {self.name}. Moving on.")
                    break # Exit retry loop
            
            except Exception as e_major:
                print(f"Major unexpected error in {self.name} scraping process: {str(e_major)}")
                if driver:
                    filepath = f"autotrader_major_error_page_{time.strftime('%Y%m%d_%H%M%S')}.html"
                    try:
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(driver.page_source)
                        print(f"Saved page source at major error to {filepath}")
                    except Exception as e_save:
                        print(f"Could not save page source during major error handling: {e_save}")
                break # Exit retry loop for major errors
            
            finally:
                if driver:
                    driver.quit()
                    # print(f"WebDriver quit for AutoTrader attempt {retries + (1 if retries < self.MAX_RETRIES else 0)} if active.")

        print(f"Scraped a total of {len(listings)} listings from {self.name} after all attempts.")
        return listings

    # Helper methods (previously defined, ensure they are present and correct)
    def _extract_year(self, title):
        """Extracts year from title string. Assumes year is a 4-digit number."""
        import re
        match = re.search(r'\b(\d{4})\b', title)
        return int(match.group(1)) if match else None

    def _extract_make_model(self, title):
        """Extracts make and model from title string. This is a simplistic example."""
        if not title: return None, None
        year = self._extract_year(title)
        if year:
            title_no_year = title.replace(str(year), "").strip()
            parts = title_no_year.split()
            if len(parts) >= 2:
                # This is very basic, real make/model extraction is complex
                # Consider using a list of known makes to improve accuracy
                make = parts[0] 
                model = " ".join(parts[1:])
                return make, model
        return None, None # Fallback

    def _extract_price(self, price_text):
        """Extracts numeric price from price string (e.g., '$15,000')."""
        if not price_text: return None
        import re
        match = re.search(r'[,\d]+', price_text.replace('$', '').replace(',', ''))
        return float(match.group(0)) if match else None

    def _extract_mileage(self, mileage_text):
        """Extracts numeric mileage from mileage string (e.g., '100,000 km')."""
        if not mileage_text: return None
        import re
        match = re.search(r'[,\d]+', mileage_text.replace(',', '').lower().replace('km', '').strip())
        return int(match.group(0)) if match else None 