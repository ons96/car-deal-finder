import asyncio
from playwright.async_api import async_playwright
import os
import time
from dotenv import load_dotenv
from tqdm import tqdm
import re # Ensure re is imported for helper methods
import random # ADDED IMPORT FOR random.randint

from src.scrapers.base_scraper import BaseScraper

# Load environment variables
load_dotenv()


class FacebookMarketplacePlaywrightScraper(BaseScraper):
    """Scraper for Facebook Marketplace using Playwright"""
    
    def __init__(self):
        """Initialize the Facebook Marketplace scraper."""
        super().__init__("Facebook Marketplace (Playwright)")
        
        self.base_url = "https://www.facebook.com"
        # Refined URL parameters based on research
        # Adding location parameters for Oakville, ON
        self.marketplace_url = (
            f"{self.base_url}/marketplace/vehicles/"
            f"?minPrice=0&maxPrice=20000&topLevelVehicleType=car_truck"
            f"&city=Oakville&region=ON&postalCode=L6M3S7"
        )
        
        # Credentials from environment variables
        self.email = os.environ.get('FACEBOOK_EMAIL')
        self.password = os.environ.get('FACEBOOK_PASSWORD')

        if not self.email or not self.password:
            print("WARNING: FACEBOOK_EMAIL or FACEBOOK_PASSWORD not found in environment variables.")
            print("Facebook Marketplace scraping requires login and will likely fail without credentials.")
            print("Please ensure they are set in your .env file.")
        
    async def _login(self, page):
        """Log in to Facebook (mandatory for scraping)."""
        if not self.email or not self.password:
            print("Facebook credentials not provided in .env file. Login is required. Aborting login attempt.")
            return False
            
        try:
            await page.goto(f"{self.base_url}/login", wait_until='networkidle')
            
            # Accept cookies if prompt appears
            # Playwright uses locators. This is an example, might need adjustment.
            cookie_button_selector = "button[data-testid='cookie-policy-manage-dialog-accept-button']"
            try:
                cookie_button = page.locator(cookie_button_selector).first # .first to avoid strict mode error if multiple
                if await cookie_button.is_visible(timeout=5000):
                    await cookie_button.click()
                    await page.wait_for_timeout(1000) # مشابه page.waitForTimeout
            except Exception:
                # print("Cookie button not found or timed out.")
                pass
            
            await page.fill("input#email", self.email)
            await page.fill("input#pass", self.password)
            await page.click("button[name='login']")
            
            # Wait for login to complete by looking for a known element after login
            await page.wait_for_selector("div[role='banner']", timeout=15000) # Example selector
            
            print("Successfully logged in to Facebook with Playwright")
            return True
            
        except Exception as e:
            print(f"Error logging in to Facebook with Playwright: {str(e)}")
            return False
    
    async def _apply_vehicle_filters(self, page):
        """Attempts to apply vehicle type filters (Sedan, Coupe, Hatchback)."""
        print("Attempting to apply vehicle type filters via UI (Note: URL parameters also attempted)...")
        # This function may be simplified or removed if URL parameters are effective.
        # For now, keeping it but acknowledging URL parameters are primary for price/type.
        try:
            # Try to find and click a general "Filters" button first if it exists
            # This selector is a guess; actual text/role might differ.
            # Using a timeout to not hang if it's not immediately present.
            filter_button_texts = ["Filters", "Filter"]
            for fb_text in filter_button_texts:
                filters_button = page.locator(f"button:has-text('{fb_text}'):visible").first
                if await filters_button.count() > 0 and await filters_button.is_visible(timeout=5000):
                    print(f"Clicking '{fb_text}' button...")
                    await filters_button.click()
                    await page.wait_for_timeout(1500) # Wait for filter panel
                    break
            else: # If no generic filter button found, try to find vehicle type directly
                pass # Continue to look for vehicle type section

            # Try to find a "Vehicle Type" filter section/button
            # This is also a guess; common pattern is a clickable div or span
            vehicle_type_texts = ["Vehicle type", "Vehicle Type", "Body style", "Body Style"]
            found_vehicle_type_section = False
            for vt_text in vehicle_type_texts:
                vehicle_type_filter_header = page.locator(f"div[role='button']:has-text('{vt_text}'):visible, span:has-text('{vt_text}'):visible").first
                if await vehicle_type_filter_header.count() > 0 and await vehicle_type_filter_header.is_visible(timeout=5000):
                    print(f"Found '{vt_text}' filter section. Clicking (with force=True)...")
                    await vehicle_type_filter_header.click(force=True)
                    await page.wait_for_timeout(2000) # Wait for options to appear
                    found_vehicle_type_section = True
                    break
            
            if not found_vehicle_type_section:
                print("Could not find or open Vehicle Type filter section via UI. Relying on URL parameters.")
                return

            # Select Sedan, Coupe, Hatchback - these texts need to be exact or use robust selectors
            vehicle_types_to_select = ["Sedan", "Coupe", "Hatchback"]
            for v_type in vehicle_types_to_select:
                try:
                    # Try locating by text within a clickable role or a label associated with a checkbox
                    # This is a common pattern: a span with the text, and a checkbox nearby or as a parent.
                    # We are looking for a checkbox. If the text itself is clickable and sets the filter, that's also fine.
                    type_checkbox_label = page.locator(f"label:has-text('{v_type}'), span:has-text('{v_type}')").locator("xpath=./ancestor-or-self::*[@role='checkbox' or .//input[@type='checkbox'] or @role='button']").first
                    
                    # Fallback: simple text locator if the above is too complex or fails
                    if not await type_checkbox_label.count():
                         type_checkbox_label = page.locator(f"*:has-text('{v_type}'):visible").locator("xpath=./ancestor-or-self::*[@role='checkbox' or .//input[@type='checkbox'] or @role='button']").last

                    if await type_checkbox_label.count() > 0:
                        print(f"Attempting to select filter: {v_type}")
                        await type_checkbox_label.click() # Or .check() if it's an input[type=checkbox]
                        await page.wait_for_timeout(500) # Brief pause after click
                    else:
                        print(f"Could not find filter option for: {v_type}")
                except Exception as e_filter_item:
                    print(f"Error trying to select filter '{v_type}': {e_filter_item}")
            
            # Try to find and click an "Apply" or "Done" button for the filters
            apply_button_texts = ["Apply", "Done", "Show results", "Update"]
            applied_filters = False
            for ab_text in apply_button_texts:
                apply_button = page.locator(f"button:has-text('{ab_text}'):visible, [role='button']:has-text('{ab_text}'):visible").first
                if await apply_button.count() > 0 and await apply_button.is_visible(timeout=3000):
                    print(f"Clicking '{ab_text}' button to apply filters...")
                    await apply_button.click()
                    await page.wait_for_timeout(3000) # Wait for page to reload/update
                    applied_filters = True
                    break
            if not applied_filters:
                print("Could not find an Apply/Done button for filters. Filters might apply automatically or this step failed.")

        except Exception as e:
            print(f"Error applying vehicle filters: {e}")

    async def _scrape_async(self, limit=100):
        """Asynchronous scraping method using Playwright."""
        listings = []
        
        if not self.email or not self.password:
            print("Facebook credentials not available. Cannot proceed with Facebook Marketplace scraping as login is mandatory.")
            return listings

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False) # Run non-headless for FB debugging
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = await context.new_page()
            
            try:
                # --- Perform Login ---
                print("Attempting to log in to Facebook...")
                logged_in = await self._login(page)
                if not logged_in:
                    print("Facebook login failed or was skipped due to missing credentials. Aborting scrape for Facebook Marketplace.")
                    await browser.close()
                    return listings
                # --- End Login ---

                await page.goto(self.marketplace_url, wait_until='networkidle', timeout=60000)
                
                # --- Attempt to close login popup (should not be needed if login is successful, but kept as a failsafe) ---
                try:
                    # Look for a dialog box that might be the login popup
                    # Common selectors for popups/dialogs and close buttons
                    popup_dialog_selector = "div[role='dialog']"
                    # Try to find a close button within the dialog. Common aria-labels are "Close", "Dismiss", "Not now"
                    # This specific selector targets an aria-label "Close" button which is common.
                    close_button_selectors = [
                        f"{popup_dialog_selector} div[aria-label='Close']",
                        f"{popup_dialog_selector} button[aria-label='Close']",
                        f"{popup_dialog_selector} div[aria-label='Not now']", # Another common label for dismissing login prompts
                        f"{popup_dialog_selector} button[aria-label='Not now']",
                        f"{popup_dialog_selector} i[class*='close']", # Icon based close
                         # A more generic one if the above fail, targeting an X symbol if visually present, BE CAREFUL with this one
                        # f"{popup_dialog_selector} :text-matches('^[Xx]$', 'i')" 
                    ]
                    
                    close_button_found_and_clicked = False
                    for cb_selector in close_button_selectors:
                        close_button = page.locator(cb_selector).first
                        if await close_button.is_visible(timeout=5000): # Short timeout to find it
                            print(f"Login popup detected. Attempting to close with selector: {cb_selector}")
                            await close_button.click(timeout=5000)
                            await page.wait_for_timeout(1500) # Wait for popup to disappear
                            print("Clicked close on login popup.")
                            close_button_found_and_clicked = True
                            break # Popup closed, no need to try other selectors
                    if not close_button_found_and_clicked:
                        print("Login popup not detected or close button not found with tried selectors.")
                except Exception as e_popup:
                    print(f"Info: No login popup found or error while trying to close it: {e_popup}")
                # --- End attempt to close login popup ---
                
                # Attempt to apply filters via UI as a secondary measure or for other filters
                # await self._apply_vehicle_filters(page) # Temporarily disable to test URL params first
                print("Skipping direct UI filter application to test URL parameters first for FB Marketplace.")
                
                await page.wait_for_selector("div[aria-label*='Marketplace']", timeout=30000)

                # Scroll to load more listings
                # This is a common pattern, might need adjustment based on FB's actual scroll container
                for i in tqdm(range(min(10, limit // 20 + 1)), desc="Scrolling for listings"): # limit // 20 as items per scroll
                    await page.mouse.wheel(0, 15000) # Scroll down
                    await page.wait_for_timeout(2000 + random.randint(500, 1500)) # Wait for content to load

                # Extract all listing items - Selector needs to be verified for current Facebook Marketplace
                # This is a placeholder selector. It's CRITICAL to get this right.
                # Common strategy: find a stable parent element for each listing.
                listing_elements_selector = "a[href*='/marketplace/item/']" # Example
                
                item_elements = await page.locator(listing_elements_selector).all()
                
                print(f"Found {len(item_elements)} potential listing elements with Playwright.")

                for element_handle in item_elements[:limit]:
                    url = "N/A"
                    title_text = "N/A_INIT"
                    price_text = "N/A_INIT"
                    try:
                        url = await element_handle.get_attribute('href')
                        if url and not url.startswith('http'):
                            url = self.base_url + url
                        
                        # Try to find title within the card
                        title_locator_candidates = [
                            "div[role='heading'] span", # If title is in a heading
                            "span[dir='auto']:not(:has(span))", # Spans with text directly, not containing other spans (simplistic)
                            "div > span[data-lexical-text='true']" # A more direct child span
                        ]
                        for tl_candidate_selector in title_locator_candidates:
                            title_elements = await element_handle.locator(tl_candidate_selector).all_text_contents()
                            if title_elements:
                                title_text = " ".join([t.strip() for t in title_elements if t.strip()]).split('\n')[0]
                                if title_text: break
                        
                        if not title_text: # Fallback to broader lexical text search if specific ones fail
                            title_candidates = await element_handle.locator("span[data-lexical-text='true']").all_text_contents()
                            if title_candidates:
                                title_text = " ".join([t.strip() for t in title_candidates if t.strip()]).split('\n')[0]

                        # Try to find price within the card
                        price_locator_candidates = [
                            "div:has-text('$') > span[dir='auto']", # Price often in a div then span
                            "span:has-text('$')" # Simpler span with price
                        ]
                        for pl_candidate_selector in price_locator_candidates:
                            price_elements = await element_handle.locator(pl_candidate_selector).all_text_contents()
                            if price_elements:
                                price_text = " ".join([p.strip() for p in price_elements if p.strip() and '$' in p]).split('\n')[0]
                                if price_text: break

                        if not price_text: # Fallback to the original generic price locator
                            price_element_fallback = element_handle.locator("*:has-text('$')").first
                            if await price_element_fallback.count() > 0:
                                price_text = (await price_element_fallback.text_content() or "").strip()
                        
                        # --- Permanent Debug print for extracted card details ---
                        print(f"FB_CARD_EXTRACT: URL: {url}, Raw Title: '{title_text}', Raw Price: '{price_text}'")
                        # --- End Debug Print ---

                        if not title_text or not price_text or title_text == "N/A_INIT" or price_text == "N/A_INIT":
                            print(f"FB_DEBUG: Card details incomplete or not found. URL: {url}, Title: '{title_text}', Price: '{price_text}'")
                            # Fallthrough, will likely be skipped by `all()` check later if essential data missing
                            pass 

                        year = self._extract_year(title_text)
                        make, model = self._extract_make_model(title_text)
                        price = self._extract_price(price_text)
                        mileage = self._extract_mileage(title_text) # Mileage rarely on card title
                        if not mileage: mileage = 80000 # Default

                        body_type = "sedan" # Placeholder, determine from title or details
                        for bt_candidate in ["sedan", "coupe", "hatchback", "suv", "truck", "van"]:
                            if bt_candidate in title_text.lower():
                                body_type = bt_candidate
                                break
                        
                        if all([url, year, make, model, price]):
                            listings.append({
                                'url': url,
                                'title': title_text,
                                'year': year,
                                'make': make,
                                'model': model,
                                'price': price,
                                'mileage': mileage,
                                'body_type': body_type,
                                'source': self.name
                            })
                        if len(listings) >= limit:
                            break
                            
                    except Exception as e:
                        url_for_error = "N/A"
                        try: # Try to get URL for context even in error
                            if 'element_handle' in locals() and hasattr(element_handle, 'get_attribute'):
                                url_for_error = await element_handle.get_attribute('href') or "N/A"
                                if url_for_error and not url_for_error.startswith('http') and self.base_url:
                                    url_for_error = self.base_url + url_for_error
                        except: pass
                        print(f"FB_ITEM_ERROR: Error processing one listing element: {e}. URL hint: {url_for_error[:100]}")
                        continue
                        
            except Exception as e:
                print(f"Error scraping {self.name} with Playwright: {str(e)}")
            finally:
                await browser.close()
        
        print(f"Scraped {len(listings)} listings from {self.name} using Playwright")
        return listings

    def scrape(self, limit=100):
        """Synchronous wrapper for the async scraping method."""
        # To run from synchronous code, we need an event loop manager
        # If running in Jupyter or an environment with an existing loop, use nest_asyncio
        # For simple scripts, asyncio.run should work.
        try:
            # Check if an event loop is already running
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If running in an environment like Jupyter that has its own loop
                import nest_asyncio
                nest_asyncio.apply()
                return loop.run_until_complete(self._scrape_async(limit))
            else:
                return asyncio.run(self._scrape_async(limit))
        except RuntimeError: # No event loop
             return asyncio.run(self._scrape_async(limit))

# For testing the scraper directly (optional)
async def _test_scraper():
    import random # Added for scroll delay
    scraper = FacebookMarketplacePlaywrightScraper()
    # Provide credentials if you want to test login
    # scraper.email = "your_fb_email"
    # scraper.password = "your_fb_password"
    results = await scraper._scrape_async(limit=10) 
    for item in results:
        print(item)

if __name__ == '__main__':
    # This allows testing this specific scraper
    # Note: Running asyncio code directly like this can sometimes have issues
    # depending on the environment.
    # Consider using `uv run python your_script.py` if direct run has issues.
    asyncio.run(_test_scraper()) 