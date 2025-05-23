import playwright.async_api as pw_async
from bs4 import BeautifulSoup
import time
from tqdm import tqdm
import random
import json
from curl_cffi import requests as curl_requests

from src.scrapers.base_scraper import BaseScraper


class CarGurusScraper(BaseScraper):
    """Scraper for CarGurus.ca"""
    
    # Constants for scraper configuration
    MAX_PRICE = 20000
    SEARCH_RADIUS_KM = 250
    SEARCH_RADIUS_MILES = 155 # Approx 250km, CarGurus might use miles

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

        # Build the search URL
        self.search_url = (
            f"{self.base_url}/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action?"
            f"sourceContext=carGurusHomePageModel&zip={self.postal_code.lower()}"
            f"&minPrice=500&maxPrice={self.MAX_PRICE}&distance={self.SEARCH_RADIUS_MILES}"
        )

    async def _setup_playwright_page(self, playwright: pw_async.Playwright):
        """Setup and return a Playwright browser page and context with enhanced anti-detection measures."""
        # Launch browser with additional arguments to appear more like a real browser
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-site-isolation-trials',
                '--disable-web-security',
                '--disable-features=BlockInsecurePrivateNetworkRequests',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu',
                '--window-size=1920,1080',
            ]
        )
        
        # Create a more realistic browser context
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            device_scale_factor=1,
            has_touch=False,
            is_mobile=False,
            locale='en-CA',
            timezone_id='America/Toronto',
            geolocation={'latitude': 43.6532, 'longitude': -79.3832},  # Toronto coordinates
            permissions=['geolocation'],
            java_script_enabled=True,
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-CA,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
            }
        )

        # Add stealth scripts to avoid detection
        page = await context.new_page()
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-CA', 'en']
            });
            window.chrome = {
                runtime: {}
            };
        """)

        return browser, context, page

    async def scrape(self, limit=100):
        """
        Scrape car listings from CarGurus.ca using Playwright with enhanced anti-detection.
        """
        print(f"Scraping {self.name} with enhanced Playwright configuration...")
        listings = []
        
        retries = 0
        while retries <= self.MAX_RETRIES:
            browser = None
            context = None 
            page = None
            playwright_instance = None
            trace_path = f"cargurus_playwright_trace_attempt_{retries + 1}.zip"
            tracing_started_this_attempt = False
            
            try:
                playwright_instance = await pw_async.async_playwright().start()
                browser, context, page = await self._setup_playwright_page(playwright_instance)
                
                print(f"Starting Playwright trace for attempt {retries + 1}")
                await context.tracing.start(screenshots=True, snapshots=True, sources=True)
                tracing_started_this_attempt = True

                # Add random delays between actions to appear more human-like
                def random_delay():
                    time.sleep(random.uniform(2, 5))

                print(f"Attempting to load URL: {self.search_url}")
                await page.goto(self.search_url, timeout=60000, wait_until="networkidle")
                random_delay()

                # Add a longer delay after page load to ensure dynamic content is loaded
                print("Waiting for dynamic content to load...")
                await page.wait_for_timeout(5000)  # 5 second delay

                # Enhanced CAPTCHA detection
                captcha_indicators = [
                    "div[class*='captcha']",
                    "div[class*='challenge']",
                    "div[class*='security-check']",
                    "div[class*='bot-detection']",
                    "div[class*='incapsula']",
                    "div[class*='cloudflare']",
                    "iframe[src*='captcha']",
                    "iframe[src*='challenge']",
                    "iframe[src*='security']",
                    "iframe[src*='incapsula']",
                    "iframe[src*='cloudflare']",
                    "form[action*='captcha']",
                    "form[action*='challenge']",
                    "form[action*='security']",
                    "form[action*='incapsula']",
                    "form[action*='cloudflare']"
                ]

                for indicator in captcha_indicators:
                    if await page.query_selector(indicator):
                        print(f"CAPTCHA detected with indicator: {indicator}")
                        filepath = f"cargurus_captcha_page_{time.strftime('%Y%m%d_%H%M%S')}.html"
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(await page.content())
                        raise ConnectionError("CAPTCHA detected")

                # Enhanced listing selectors for CarGurus
                listing_selectors = [
                    # Primary selectors (most specific)
                    "div[data-test='listing-card']",
                    "div[data-test='inventory-listing']",
                    "div[data-test='vehicle-card']",
                    "div[data-test='car-listing']",
                    # Class-based selectors (more general)
                    "div[class*='listing-card']",
                    "div[class*='listing-item']",
                    "div[class*='result-item']",
                    "div[class*='car-listing']",
                    "div[class*='listing']",
                    "div[class*='car-card']",
                    "div[class*='vehicle-card']",
                    "div[class*='inventory-listing']",
                    "div[class*='inventory-item']",
                    # Additional selectors based on CarGurus structure
                    "div[class*='cg-listing']",
                    "div[class*='cg-card']",
                    "div[class*='cg-vehicle']",
                    "div[class*='cg-inventory']",
                    "div[class*='cg-result']",
                    "div[class*='cg-item']",
                    # Fallback selectors
                    "div[class*='listing-container'] > div",
                    "div[class*='results-container'] > div",
                    "div[class*='inventory-container'] > div",
                    "div[class*='vehicle-container'] > div"
                ]

                # Try to find the listing container with enhanced debugging
                listing_container = None
                for selector in listing_selectors:
                    try:
                        print(f"Trying selector: {selector}")
                        listing_container = await page.wait_for_selector(selector, timeout=10000)
                        if listing_container:
                            print(f"Found listing container with selector: {selector}")
                            # Verify we can find actual listings within this container
                            listings_found = await page.query_selector_all(f"{selector}")
                            if listings_found:
                                print(f"Verified {len(listings_found)} listings found with selector: {selector}")
                                break
                            else:
                                print(f"Selector {selector} found container but no listings within it")
                                listing_container = None
                    except Exception as e:
                        print(f"Selector {selector} failed: {str(e)}")
                        continue

                if not listing_container:
                    # Save the page content for debugging
                    filepath = f"cargurus_no_listings_page_{time.strftime('%Y%m%d_%H%M%S')}.html"
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(await page.content())
                    print(f"Saved page content to {filepath} for debugging")
                    raise ConnectionError("No listing container found")

                # Process listings with enhanced selectors
                current_page = 1
                while len(listings) < limit:
                    # Extract listings from current page with enhanced selectors
                    listing_elements = await page.query_selector_all(
                        "div[data-test='listing-card'], "
                        "div[data-test='inventory-listing'], "
                        "div[data-test='vehicle-card'], "
                        "div[data-test='car-listing'], "
                        "div[class*='listing-card'], "
                        "div[class*='listing-item'], "
                        "div[class*='result-item'], "
                        "div[class*='car-listing'], "
                        "div[class*='listing'], "
                        "div[class*='car-card'], "
                        "div[class*='vehicle-card'], "
                        "div[class*='inventory-listing'], "
                        "div[class*='inventory-item'], "
                        "div[class*='cg-listing'], "
                        "div[class*='cg-card'], "
                        "div[class*='cg-vehicle'], "
                        "div[class*='cg-inventory'], "
                        "div[class*='cg-result'], "
                        "div[class*='cg-item']"
                    )
                    
                    if not listing_elements:
                        print("No more listings found")
                        # Save the page content for debugging
                        filepath = f"cargurus_no_listings_page_{time.strftime('%Y%m%d_%H%M%S')}.html"
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(await page.content())
                        print(f"Saved page content to {filepath} for debugging")
                        break

                    print(f"Found {len(listing_elements)} listings on page {current_page}")

                    for element in listing_elements:
                        try:
                            # Extract data using more robust selectors
                            title_element = await element.query_selector(
                                "h3, h4, .title, [class*='title'], "
                                "[data-test*='title'], "
                                "[class*='vehicle-title'], "
                                "[class*='car-title']"
                            )
                            title = await title_element.text_content() if title_element else None
                            if title:
                                title = title.strip()
                            
                            price_text_element = await element.query_selector(
                                "[class*='price'], "
                                "[data-test*='price'], "
                                "[class*='listing-price'], "
                                "[class*='vehicle-price']"
                            )
                            price_text = await price_text_element.text_content() if price_text_element else None
                            if price_text:
                                price_text = price_text.strip()
                            price = self._extract_price(price_text) if price_text else None
                            
                            # Extract year, make, model from title
                            year = self._extract_year(title) if title else None
                            make, model = self._extract_make_model(title) if title else (None, None)
                            
                            # Extract mileage with more robust selectors
                            mileage_text_element = await element.query_selector(
                                "[class*='mileage'], "
                                "[data-test*='mileage'], "
                                "[class*='listing-mileage'], "
                                "[class*='vehicle-mileage'], "
                                "[class*='odometer']"
                            )
                            mileage_text = await mileage_text_element.text_content() if mileage_text_element else None
                            if mileage_text:
                                mileage_text = mileage_text.strip()
                            mileage = self._extract_mileage(mileage_text) if mileage_text else None
                            
                            # Extract URL with more robust selectors
                            url_element = await element.query_selector(
                                "a[href*='/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action'], "
                                "a[href*='/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action'], "
                                "a[href*='/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action'], "
                                "a[class*='listing-link'], "
                                "a[class*='vehicle-link']"
                            )
                            url = await url_element.get_attribute("href") if url_element else None
                            if url and not url.startswith("http"):
                                url = self.base_url + url

                            # Extract body type with more robust selectors
                            body_type = "unknown"
                            body_type_text_element = await element.query_selector(
                                "[class*='body-type'], "
                                "[data-test*='body-type'], "
                                "[class*='vehicle-type'], "
                                "[class*='car-type']"
                            )
                            body_type_text = await body_type_text_element.text_content() if body_type_text_element else None
                            if body_type_text:
                                body_type_text = body_type_text.strip().lower()
                                if "sedan" in body_type_text: body_type = "sedan"
                                elif "coupe" in body_type_text: body_type = "coupe"
                                elif "hatchback" in body_type_text: body_type = "hatchback"
                                elif "suv" in body_type_text: body_type = "suv"
                                elif "truck" in body_type_text: body_type = "truck"
                                elif "van" in body_type_text: body_type = "van"

                            # Apply approved vehicles filter
                            is_approved = False
                            if self.approved_vehicles:
                                if year and make and model:
                                    scraped_make_lc = str(make).lower().strip()
                                    scraped_model_norm = str(model).lower().replace('-', ' ').strip()
                                    scraped_year_int = int(year)

                                    for approved_make, approved_model, approved_year in self.approved_vehicles:
                                        if (scraped_make_lc == approved_make and
                                            scraped_year_int == approved_year and
                                            scraped_model_norm.startswith(approved_model)):
                                            is_approved = True
                                            break
                            else:
                                is_approved = True

                            if not is_approved:
                                continue

                            if not all([url, year, make, model, price is not None, mileage is not None]):
                                print(f"Skipping item due to missing core data: Title='{title}', URL='{url}'")
                                continue

                            listing_data = {
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
                            listings.append(listing_data)
                        
                            if len(listings) >= limit:
                                break

                        except Exception as e:
                            print(f"Error processing listing: {str(e)}")
                            continue
                    
                    if len(listings) >= limit:
                        break

                    # Try to go to next page
                    try:
                        next_button = await page.query_selector("button[aria-label*='Next'], a[aria-label*='Next'], [class*='next']")
                        if next_button and await next_button.is_visible():
                            await next_button.click()
                            random_delay()
                            current_page += 1
                        else:
                            print("No more pages available")
                            break
                    except Exception as e:
                        print(f"Error navigating to next page: {str(e)}")
                        break

                print(f"Finished scraping {len(listings)} listings")
                break

            except (pw_async.TimeoutError, ConnectionError) as e:
                print(f"Error during scrape attempt {retries + 1}/{self.MAX_RETRIES + 1}: {str(e)}")
                if tracing_started_this_attempt and context:
                    await context.tracing.stop(path=trace_path)
                    print(f"Saved trace to {trace_path}")
                
                retries += 1
                if retries <= self.MAX_RETRIES:
                    delay = min(self.MAX_RETRY_DELAY_S, self.INITIAL_RETRY_DELAY_S * (2 ** (retries - 1)))
                    jitter = delay * 0.2 * random.random()
                    actual_delay = delay + jitter
                    print(f"Retrying in {actual_delay:.2f} seconds...")
                    time.sleep(actual_delay)
                else:
                    print(f"Max retries reached for {self.name}")
                    break
            
            except Exception as e:
                print(f"Unexpected error: {str(e)}")
                if tracing_started_this_attempt and context:
                    await context.tracing.stop(path=trace_path)
                    print(f"Saved trace to {trace_path}")
                break
        
            finally:
                if tracing_started_this_attempt and context:
                    await context.tracing.stop(path=trace_path)
                if browser:
                    await browser.close()
                if playwright_instance:
                    await playwright_instance.stop()

        print(f"Scraped a total of {len(listings)} listings from {self.name}")
        return listings 

    def _extract_make_model(self, title):
        """Extract make and model from title string."""
        if not title:
            return None, None
        year = self._extract_year(title)
        if year:
            title_no_year = title.replace(str(year), "").strip()
            parts = title_no_year.split()
            if len(parts) >= 2:
                make = parts[0]
                model = " ".join(parts[1:])
                return make, model
        return None, None

    def _extract_mileage(self, mileage_text):
        """Extract numeric mileage from mileage string."""
        if not mileage_text:
            return None
        import re
        match = re.search(r'[,\d]+', mileage_text.replace(',', '').lower().replace('km', '').strip())
        return int(match.group(0)) if match else None
