import playwright.async_api as pw_async
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
from tqdm import tqdm
import random
import json
from curl_cffi import requests as curl_requests
from bs4 import BeautifulSoup

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
                '--disable-extensions',
                '--disable-component-extensions-with-background-pages',
                '--disable-default-apps',
                '--mute-audio',
                '--no-default-browser-check',
                '--no-experiments',
                '--disable-features=site-per-process',
                '--disable-features=TranslateUI',
                '--disable-features=BlinkGenPropertyTrees',
                '--disable-features=InterestCohort',
                '--disable-features=UserAgentClientHint',
                '--disable-features=NetworkService',
                '--disable-features=NetworkServiceInProcess',
                '--disable-features=NetworkServiceInProcess2',
                '--disable-features=NetworkServiceInProcess3',
                '--disable-features=NetworkServiceInProcess4',
                '--disable-features=NetworkServiceInProcess5',
                '--disable-features=NetworkServiceInProcess6',
                '--disable-features=NetworkServiceInProcess7',
                '--disable-features=NetworkServiceInProcess8',
                '--disable-features=NetworkServiceInProcess9',
                '--disable-features=NetworkServiceInProcess10',
                '--disable-features=NetworkServiceInProcess11',
                '--disable-features=NetworkServiceInProcess12',
                '--disable-features=NetworkServiceInProcess13',
                '--disable-features=NetworkServiceInProcess14',
                '--disable-features=NetworkServiceInProcess15',
                '--disable-features=NetworkServiceInProcess16',
                '--disable-features=NetworkServiceInProcess17',
                '--disable-features=NetworkServiceInProcess18',
                '--disable-features=NetworkServiceInProcess19',
                '--disable-features=NetworkServiceInProcess20',
            ]
        )
        
        # Create a more realistic browser context with additional settings
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
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-CA,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
                'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'DNT': '1',
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
            Object.defineProperty(navigator, 'platform', {
                get: () => 'Win32'
            });
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 8
            });
            Object.defineProperty(navigator, 'deviceMemory', {
                get: () => 8
            });
            Object.defineProperty(navigator, 'maxTouchPoints', {
                get: () => 0
            });
            Object.defineProperty(navigator, 'vendor', {
                get: () => 'Google Inc.'
            });
            Object.defineProperty(navigator, 'appVersion', {
                get: () => '5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
            });
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };
            Object.defineProperty(window, 'chrome', {
                get: () => ({
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                    app: {}
                })
            });
        """)

        # Set more realistic viewport and user agent
        await page.set_viewport_size({"width": 1920, "height": 1080})
        await page.set_extra_http_headers({
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "DNT": "1"
        })

        # Add random mouse movements
        await page.mouse.move(random.randint(0, 1920), random.randint(0, 1080))
        await page.mouse.move(random.randint(0, 1920), random.randint(0, 1080))

        # Add random scroll behavior
        await page.evaluate("""
            window.scrollTo({
                top: Math.floor(Math.random() * 100),
                behavior: 'smooth'
            });
        """)

        return browser, context, page

    async def _handle_incapsula_challenge(self, page):
        """Handle Incapsula security challenge if present."""
        try:
            # Check for Incapsula iframe
            incapsula_iframe = await page.query_selector('iframe#main-iframe')
            if incapsula_iframe:
                print("Detected Incapsula security check, attempting to handle...")
                
                # Wait for the iframe to load
                await page.wait_for_timeout(5000)  # Wait for initial load
                
                # Get the iframe content
                frame_content = await incapsula_iframe.content_frame()
                if frame_content:
                    # Wait for any potential challenge elements
                    await frame_content.wait_for_timeout(3000)
                    
                    # Check for common challenge elements
                    challenge_elements = [
                        'input[type="text"]',
                        'input[type="checkbox"]',
                        'button[type="submit"]',
                        'div[class*="challenge"]',
                        'div[class*="security"]'
                    ]
                    
                    for selector in challenge_elements:
                        element = await frame_content.query_selector(selector)
                        if element:
                            print(f"Found challenge element: {selector}")
                            # Wait a bit before interacting
                            await page.wait_for_timeout(2000)
                            
                            if selector == 'input[type="text"]':
                                # If it's a text input, we might need to solve a CAPTCHA
                                print("Text input detected - might be a CAPTCHA")
                                return False
                            elif selector == 'input[type="checkbox"]':
                                # If it's a checkbox, try to click it
                                await element.click()
                                await page.wait_for_timeout(2000)
                            elif selector == 'button[type="submit"]':
                                # If it's a submit button, try to click it
                                await element.click()
                                await page.wait_for_timeout(5000)
                
                # Wait for potential redirect
                await page.wait_for_timeout(5000)
                
                # Check if we're still on the challenge page
                if await page.query_selector('iframe#main-iframe'):
                    print("Still on Incapsula challenge page after handling attempt")
                    return False
                
                print("Successfully handled Incapsula challenge")
                return True
            
            return True  # No Incapsula challenge found
            
        except Exception as e:
            print(f"Error handling Incapsula challenge: {str(e)}")
            return False

    async def scrape(self, limit=100):
        """
        Scrape car listings from AutoTrader.ca using Playwright with enhanced anti-detection.
        """
        print(f"Scraping {self.name} with enhanced Playwright configuration...")
        listings = []
        
        retries = 0
        while retries <= self.MAX_RETRIES:
            browser = None
            context = None 
            page = None
            playwright_instance = None
            trace_path = f"autotrader_playwright_trace_attempt_{retries + 1}.zip"
            tracing_started_this_attempt = False
            
            try:
                playwright_instance = await pw_async.async_playwright().start()
                browser, context, page = await self._setup_playwright_page(playwright_instance)
                
                print(f"Starting Playwright trace for attempt {retries + 1}")
                await context.tracing.start(screenshots=True, snapshots=True, sources=True)
                tracing_started_this_attempt = True

                # Simulate human-like interactions to improve stealth
                await page.mouse.move(random.randint(100, 1820), random.randint(100, 980))
                await page.wait_for_timeout(random.randint(500, 1500))
                await page.evaluate("window.scrollBy(0, Math.floor(Math.random()*300 + 100))")
                await page.wait_for_timeout(random.randint(500, 1500))

                # Add random delays between actions to appear more human-like
                def random_delay():
                    time.sleep(random.uniform(2, 5))

                print(f"Attempting to load URL: {self.search_url}")
                await page.goto(self.search_url, timeout=60000, wait_until="networkidle")
                random_delay()

                # Add a longer delay after page load
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
                        filepath = f"autotrader_captcha_page_{time.strftime('%Y%m%d_%H%M%S')}.html"
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(await page.content())
                        raise ConnectionError("CAPTCHA detected")

                # Handle Incapsula challenge if present
                if not await self._handle_incapsula_challenge(page):
                    print("Failed to handle Incapsula challenge")
                    raise ConnectionError("Failed to handle Incapsula challenge")

                # Handle cookie consent with more robust selectors
                cookie_selectors = [
                    'button:has-text("Accept All")',
                    'button:has-text("Allow All")',
                    'button:has-text("Accept cookies")',
                    'button:has-text("I accept")',
                    'button:has-text("Agree and Proceed")',
                    'button[aria-label*="cookie"]',
                    'button[aria-label*="Cookie"]',
                    'button[data-test*="cookie"]',
                    'button[data-test*="Cookie"]',
                    '//button[contains(text(), "Accept")]',
                    '//button[contains(text(), "Allow")]',
                    '//button[contains(text(), "Agree")]'
                ]

                for selector in cookie_selectors:
                    try:
                        cookie_button = await page.query_selector(selector)
                        if cookie_button and await cookie_button.is_visible():
                            print(f"Found cookie button with selector: {selector}")
                            await cookie_button.click(timeout=5000)
                            random_delay()
                            break
                    except Exception as e:
                        continue

                # Wait for listings with more robust selectors
                listing_selectors = [
                    # Primary selectors
                    "div[data-test='result-item']",
                    "div.result-item",
                    "div[data-test='listing-card']",
                    "div.listing-card",
                    # Class-based selectors
                    "div[class*='result-item']",
                    "div[class*='listing-item']",
                    "div[class*='listing-card']",
                    "div[class*='vehicle-card']",
                    "div[class*='ad-listing']",
                    "div[class*='listing-container'] > div",
                    # Fallback selectors
                    "section.listing-section div.listing-item",
                    "ul.listings > li",
                ]

                listing_container = None
                for selector in listing_selectors:
                    try:
                        print(f"Trying AutoTrader selector: {selector}")
                        listing_container = await page.wait_for_selector(selector, timeout=10000)
                        if listing_container:
                            # Verify we can find actual listings within this container
                            found = await page.query_selector_all(selector)
                            if found:
                                print(f"AutoTrader selector succeeded: {selector}, found {len(found)} items")
                                break
                            else:
                                print(f"Selector {selector} found container but no items inside")
                                listing_container = None
                    except Exception as e:
                        print(f"AutoTrader selector {selector} failed: {e}")
                        continue

                if not listing_container:
                    # Save page snapshot for debugging
                    filepath = f"autotrader_no_listings_page_{time.strftime('%Y%m%d_%H%M%S')}.html"
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(await page.content())
                    print(f"Saved AutoTrader no-listings snapshot to {filepath}")
                    raise ConnectionError("No listing container found")

                current_page_num = 1
                processed_this_attempt = 0
                while True: # Loop for pages
                    # Navigate to specific page using URL offset
                    offset = (current_page_num - 1) * 100  # matches rcp=100 results per page
                    page_url = self.search_url.replace("rcs=0", f"rcs={offset}")
                    print(f"Playwright: Loading page {current_page_num}, offset={offset}: {page_url}")
                    await page.goto(page_url, timeout=60000, wait_until="domcontentloaded")
                    # Ensure elements are loaded
                    await page.wait_for_selector("div.result-item", timeout=20000, state="visible")
                    time.sleep(random.uniform(1,3))  # give it a moment
                    
                    listing_elements = await page.query_selector_all("div.result-item")
                    if not listing_elements and current_page_num == 1:
                        print("No listings found on the first page with Playwright. This might be a soft block or an issue with search criteria.")
                        filepath = f"autotrader_playwright_no_listings_page_{time.strftime('%Y%m%d_%H%M%S')}.html"
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(await page.content())
                        print(f"Saved Playwright no-listings page content to {filepath}")
                        break 

                    print(f"Found {len(listing_elements)} elements on page {current_page_num} with Playwright.")

                    for element_idx, element_handle in enumerate(listing_elements):
                        if (element_idx + 1) % 20 == 0: 
                            print(f"Processing item {element_idx + 1} of {len(listing_elements)} on page {current_page_num}...")

                        if len(listings) >= limit:
                            print(f"Reached scrape limit of {limit} listings.")
                            break 
                        
                        try:
                            # Get outerHTML of the listing item once
                            item_html = await element_handle.evaluate("element => element.outerHTML")
                            item_soup = BeautifulSoup(item_html, 'html.parser')

                            url_element = item_soup.find("a", class_="link-overlay")
                            url = url_element['href'] if url_element and url_element.has_attr('href') else None
                            if url and not url.startswith("http"):
                                url = self.base_url + url

                            title_element = item_soup.find("h2", class_="title")
                            title = title_element.get_text(strip=True) if title_element else "N/A"
                            
                            year = self._extract_year(title)
                            make, model = self._extract_make_model(title)

                            price_str_element = item_soup.find("span", class_="price-amount")
                            price = self._extract_price(price_str_element.get_text(strip=True)) if price_str_element else None
                            
                            mileage_str_element = item_soup.find("span", class_="kms") 
                            mileage = self._extract_mileage(mileage_str_element.get_text(strip=True)) if mileage_str_element else None
                            
                            body_type = "unknown"
                            try:
                                specs_list_items = item_soup.select("div.ad-specs li") # BeautifulSoup select
                                for spec_item in specs_list_items:
                                    spec_text = spec_item.get_text(strip=True).lower()
                                    if "sedan" in spec_text: body_type = "sedan"; break
                                    if "coupe" in spec_text: body_type = "coupe"; break
                                    if "hatchback" in spec_text: body_type = "hatchback"; break
                                    if "suv" in spec_text: body_type = "suv"; break
                                    if "truck" in spec_text: body_type = "truck"; break
                                    if "van" in spec_text or "minivan" in spec_text: body_type = "van"; break
                            except Exception: 
                                pass

                            is_approved = False
                            if self.approved_vehicles:
                                if year and make and model:
                                    scraped_make_lc = str(make).lower().strip()
                                    scraped_model_norm = str(model).lower().replace('-', ' ').strip()
                                    scraped_year_int = int(year)

                                    for approved_make, approved_model, approved_year_filter in self.approved_vehicles:
                                        if (scraped_make_lc == approved_make and
                                            scraped_year_int == approved_year_filter and
                                            scraped_model_norm.startswith(approved_model)):
                                            is_approved = True
                                            break
                                else:
                                    is_approved = False
                            else:
                                is_approved = True

                            if not is_approved:
                                continue

                            if not all([url, year, make, model, price is not None, mileage is not None]):
                                print(f"Skipping item due to missing core data after Playwright extraction: Title='{title}', URL='{url}'")
                                continue

                            listing_data = {
                                'url': url, 'title': title, 'year': year, 'make': make, 'model': model,
                                'price': price, 'mileage': mileage, 'body_type': body_type, 'source': self.name
                            }
                            listings.append(listing_data)
                            processed_this_attempt +=1
                        
                        except Exception as e_item:
                            print(f"Error extracting details for one listing (BS4 parse) on page {current_page_num}, item {element_idx + 1}: {str(e_item)}")
                            # Optionally log item_html if parsing fails often
                            # print(f"Problematic item HTML: {item_html[:500]}")
                            continue 

                    if len(listings) >= limit:
                        break 

                    # After processing all items on this page, check if we should continue
                    if len(listing_elements) < 100:
                        print(f"Last page reached at page {current_page_num} (only {len(listing_elements)} items).")
                        break
                    current_page_num += 1

                if processed_this_attempt > 0 or not listings:
                    print(f"Finished Playwright attempt {retries + 1}. Listings collected: {processed_this_attempt}. Total: {len(listings)}")
                
                if len(listings) >= limit:
                    print(f"Scraping limit ({limit}) reached for {self.name} with Playwright.")
                    break 

                if current_page_num > 1 and processed_this_attempt == 0 and len(listing_elements) > 0:
                    print("Processed multiple pages with Playwright but last page had elements yet no new listings (all filtered). Run complete.")
                    break

                if not listings and current_page_num == 1 and not listing_elements:
                    print("No listings found matching criteria on first page (Playwright). Ending.")
                    break
                
                break # Successful attempt, break retry loop

            except (pw_async.TimeoutError, ConnectionError) as e_retry_pw: # Playwright TimeoutError is a common one for retry
                print(f"A Playwright retryable error occurred on attempt {retries + 1}/{self.MAX_RETRIES + 1} for {self.name}: {str(e_retry_pw)}")
                if tracing_started_this_attempt and context:
                    await context.tracing.stop(path = trace_path)
                    tracing_started_this_attempt = False # Mark as stopped
                    print(f"Playwright trace saved to {trace_path} due to retryable error.")
                retries += 1
                if page: # Save page source on retryable error
                    filepath = f"autotrader_playwright_retry_error_page_{retries}_{time.strftime('%Y%m%d_%H%M%S')}.html"
                    try:
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(await page.content())
                        print(f"Saved page source at Playwright retryable error to {filepath}")
                    except Exception as e_save:
                        print(f"Could not save page source during Playwright retry handling: {e_save}")

                if retries <= self.MAX_RETRIES:
                    delay = min(self.MAX_RETRY_DELAY_S, self.INITIAL_RETRY_DELAY_S * (2 ** (retries - 1)))
                    jitter = delay * 0.2 * random.random()
                    actual_delay = delay + jitter
                    print(f"Retrying with Playwright in {actual_delay:.2f} seconds...")
                    # Close browser and playwright instance before sleep
                    if browser: await browser.close()
                    if playwright_instance: await playwright_instance.stop()
                    browser, context, page, playwright_instance = None, None, None, None
                    time.sleep(actual_delay)
                else:
                    print(f"Max retries reached for {self.name} with Playwright. Moving on.")
                    break 
            
            except Exception as e_major_pw:
                print(f"Major unexpected error in {self.name} Playwright scraping process: {str(e_major_pw)}")
                if tracing_started_this_attempt and context:
                    await context.tracing.stop(path = trace_path)
                    tracing_started_this_attempt = False # Mark as stopped
                    print(f"Playwright trace saved to {trace_path} due to major error.")
                if page:
                    filepath = f"autotrader_playwright_major_error_page_{time.strftime('%Y%m%d_%H%M%S')}.html"
                    try:
                        with open(filepath, "w", encoding="utf-8") as f:
                            f.write(await page.content())
                        print(f"Saved page source at Playwright major error to {filepath}")
                    except Exception as e_save:
                        print(f"Could not save page source during Playwright major error handling: {e_save}")
                break 
            
            finally:
                if tracing_started_this_attempt and context: # If tracing was started and not explicitly stopped
                    print(f"Stopping trace for attempt {retries + (1 if retries < self.MAX_RETRIES and not listings else 0)} as part of finally block.")
                    await context.tracing.stop(path = trace_path)
                    print(f"Trace saved to {trace_path} at the end of the attempt (finally block).")
                if browser:
                    await browser.close()
                if playwright_instance:
                    await playwright_instance.stop()
                # print(f"Playwright browser and instance stopped for AutoTrader attempt.")

        print(f"Scraped a total of {len(listings)} listings from {self.name} using Playwright after all attempts.")
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