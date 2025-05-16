import asyncio
from playwright.async_api import async_playwright
import os
import time
from dotenv import load_dotenv
from tqdm import tqdm
import re
import random

from src.scrapers.base_scraper import BaseScraper

load_dotenv()

class AutoTraderPlaywrightScraper(BaseScraper):
    """Scraper for AutoTrader.ca using Playwright"""
    
    def __init__(self):
        super().__init__("AutoTrader.ca (Playwright)")
        self.base_url = "https://www.autotrader.ca"
        self.search_url = (
            f"{self.base_url}/cars/on/oakville/"
            "?rcp=100&rcs=0&srt=39&yRng=1996%2C&pRng=%2C20000&prx=100&prv=Ontario&loc=L6M%203S7&hprc=True&wcp=True&sts=New-Used&inMarket=advancedSearch"
        )
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }

    async def _scrape_async(self, limit=100):
        listings = []
        print(f"Scraping {self.name} from {self.search_url}...")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=self.headers['User-Agent'])
            page = await context.new_page()
            
            try:
                await page.goto(self.search_url, wait_until='domcontentloaded', timeout=60000)
                listing_card_selector = "div.result-item"
                await page.wait_for_selector(listing_card_selector, timeout=30000)

                current_page_num = 1
                results_per_page = 100 
                max_pages_to_scrape = (limit // results_per_page) + 2 

                with tqdm(total=limit, desc=f"Scraping {self.name}") as pbar:
                    while len(listings) < limit and current_page_num <= max_pages_to_scrape:
                        if current_page_num > 1:
                            print(f"Navigating to page {current_page_num}...")
                            next_page_button_selector = "a.page-direction-control.page-direction-control-right"
                            next_button = page.locator(next_page_button_selector).first
                            if await next_button.count() > 0 and await next_button.is_enabled():
                                await next_button.click()
                                await page.wait_for_selector(listing_card_selector, timeout=20000)
                                await page.wait_for_timeout(random.randint(1500,3000))
                            else:
                                print("Next page button not found or not enabled. Ending pagination.")
                                break
                        
                        item_elements = await page.locator(listing_card_selector).all()
                        if not item_elements and current_page_num == 1:
                            print("No listing items found on the first page. Check selectors or page content.")
                            break

                        for element_handle in item_elements:
                            if len(listings) >= limit:
                                break
                            raw_title_text = "ERROR_READING_TITLE"
                            raw_price_text = "ERROR_READING_PRICE"
                            raw_mileage_text = "ERROR_READING_MILEAGE"
                            extracted_url = "ERROR_READING_URL"
                            try:
                                url = None
                                url_element_locator = element_handle.locator("a.inner-link").first
                                try:
                                    # Wait for the specific URL element to be visible before trying to get attribute
                                    await url_element_locator.wait_for(state='visible', timeout=5000) # Check visibility
                                    # If wait_for succeeded, the element should exist and be visible.
                                    url = await url_element_locator.get_attribute("href", timeout=5000) 
                                    extracted_url = url # Store for printing
                                except Exception as e_url_detail:
                                    print(f"DEBUG: URL element not found/visible or error getting attribute: {e_url_detail} for item starting with: {(await element_handle.text_content(timeout=1000))[:50]}")
                                    extracted_url = f"ERROR: {e_url_detail}"
                                
                                if url and not url.startswith('http'):
                                    url = self.base_url + url
                                    extracted_url = url # Update URL if modified
                                
                                if not url:
                                    print(f"DEBUG: Skipping item due to missing URL (URL after checks: {extracted_url})")
                                    continue 

                                # Title extraction
                                title = ""
                                title_element_locator = element_handle.locator("h2.h2-title span.result-title").first
                                try:
                                    await title_element_locator.wait_for(state='visible', timeout=5000) # Increase timeout slightly
                                    title_text_content = await title_element_locator.text_content()
                                    raw_title_text = title_text_content # Store raw text for debug
                                    title = (title_text_content or "").strip()
                                except Exception as e_title:
                                    print(f"DEBUG: Title element not found/visible or error: {e_title}")
                                    raw_title_text = f"ERROR: {e_title}"

                                # Price extraction
                                price_text = ""
                                try:
                                    price_element = element_handle.locator("span.price-amount")
                                    await price_element.wait_for(state='visible', timeout=3000) # Add wait for price
                                    price_text_content = await price_element.text_content()
                                    raw_price_text = price_text_content # Store raw text for debug
                                    price_text = price_text_content
                                except Exception as e_price:
                                    # Price might legitimately not be displayed, only print if error
                                    if "Timeout" in str(e_price):
                                        print(f"DEBUG: Price element not found/visible or error: {e_price}")
                                    raw_price_text = f"ERROR_OR_MISSING: {e_price}"
                                
                                # Mileage extraction
                                mileage_text = ""
                                try:
                                    mileage_element = element_handle.locator("div.kms span.odometer-proximity")
                                    await mileage_element.wait_for(state='visible', timeout=3000) # Add wait for mileage
                                    mileage_text_content = await mileage_element.text_content()
                                    raw_mileage_text = mileage_text_content # Store raw text for debug
                                    mileage_text = mileage_text_content
                                except Exception as e_mileage:
                                    # Mileage might legitimately not be displayed
                                    if "Timeout" in str(e_mileage):
                                        print(f"DEBUG: Mileage element not found/visible or error: {e_mileage}")
                                    raw_mileage_text = f"ERROR_OR_MISSING: {e_mileage}"

                                # --- Debug Print --- 
                                print(f"DEBUG Attempting Extraction:")
                                print(f"  - URL: {extracted_url}")
                                print(f"  - Raw Title: '{raw_title_text}'")
                                print(f"  - Raw Price: '{raw_price_text}'")
                                print(f"  - Raw Mileage: '{raw_mileage_text}'")
                                # --- End Debug Print ---

                                # Call extraction functions
                                year = self._extract_year(title) # Note: uses parsed title variable
                                # make, model = self._extract_make_model(title) # OLD: uses parsed title variable
                                
                                # New: Extract make and model from URL
                                make = ""
                                model = ""
                                if url:
                                    match = re.search(r'/a/([^/]+)/([^/]+)/', url)
                                    if match:
                                        make_from_url = match.group(1).replace('%20', ' ').strip()
                                        model_from_url = match.group(2).replace('%20', ' ').strip()
                                        
                                        # Basic validation or cleaning (can be expanded)
                                        if make_from_url and model_from_url:
                                            make = make_from_url
                                            model = model_from_url
                                        else: # Fallback or log error if parts are missing
                                            print(f"DEBUG: Could not parse make/model from URL: {url}. Falling back to title.")
                                            make, model = self._extract_make_model(title) # Fallback
                                    else:
                                        print(f"DEBUG: Regex did not match make/model in URL: {url}. Falling back to title.")
                                        make, model = self._extract_make_model(title) # Fallback
                                else:
                                    print(f"DEBUG: URL is None, cannot extract make/model from it. Falling back to title.")
                                    make, model = self._extract_make_model(title) # Fallback

                                price = self._extract_price(price_text) # Note: uses price_text variable
                                mileage = self._extract_mileage(mileage_text) # Note: uses mileage_text variable

                                # --- Debug Print After Parsing --- 
                                print(f"DEBUG Parsed Values:")
                                print(f"  - Year: {year} (from '{title}')")
                                print(f"  - Make: {make} (from '{title}')")
                                print(f"  - Model: {model} (from '{title}')")
                                print(f"  - Price: {price} (from '{price_text}')")
                                print(f"  - Mileage: {mileage} (from '{mileage_text}')")
                                # --- End Debug Print After Parsing ---

                                body_type = ""
                                try:
                                    specs_elements = await element_handle.locator("div.ad-specs li").all()
                                    for spec_el in specs_elements:
                                        spec_text = (await spec_el.text_content() or "").lower()
                                        if any(bt in spec_text for bt in ["sedan", "coupe", "hatchback", "suv", "truck", "van"]):
                                            body_type = next((bt for bt in ["sedan", "coupe", "hatchback", "suv", "truck", "van"] if bt in spec_text), "")
                                            break
                                except Exception:
                                    pass
                                if not body_type: body_type = "sedan" # Default

                                if all([url, year, make, model, price is not None, mileage is not None]):
                                    listings.append({
                                        'url': url,
                                        'title': title,
                                        'year': year,
                                        'make': make,
                                        'model': model,
                                        'price': price,
                                        'mileage': mileage,
                                        'body_type': body_type,
                                        'source': self.name
                                    })
                                    pbar.update(1)
                                else:
                                    # print(f"Skipping incomplete listing: Title '{title}', Price '{price}', Mileage '{mileage}'")
                                    pass

                            except Exception as e_item:
                                print(f"Outer error processing an AutoTrader item: {e_item}")
                                continue
                        
                        if len(listings) >= limit:
                            break
                        current_page_num += 1

            except Exception as e_main:
                print(f"Error scraping {self.name}: {e_main}")
            finally:
                await browser.close()
        
        print(f"Scraped {len(listings)} listings from {self.name}")
        return listings

    def scrape(self, limit=100):
        # Synchronous wrapper for the async scraping method
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import nest_asyncio
                nest_asyncio.apply()
                return loop.run_until_complete(self._scrape_async(limit))
            else:
                return asyncio.run(self._scrape_async(limit))
        except RuntimeError: # No event loop
             return asyncio.run(self._scrape_async(limit))

# For testing the scraper directly (optional)
async def _test_scraper():
    scraper = AutoTraderPlaywrightScraper()
    results = await scraper._scrape_async(limit=5) 
    for item in results:
        print(item)

if __name__ == '__main__':
    asyncio.run(_test_scraper()) 