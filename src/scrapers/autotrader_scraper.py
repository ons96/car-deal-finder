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

from src.scrapers.base_scraper import BaseScraper


class AutoTraderScraper(BaseScraper):
    """Scraper for AutoTrader.ca"""
    
    def __init__(self):
        """Initialize the AutoTrader scraper."""
        super().__init__("AutoTrader.ca")
        self.base_url = "https://www.autotrader.ca"
        self.search_url = (
            f"{self.base_url}/cars/sedan-coupe-hatchback/on"
            "?rcp=100&rcs=0&srt=9&yRng=2010%2C2023&prx=-2&hprc=True&wcp=True&loc=L6G+3H7&sts=Used&inMarket=advancedSearch"
        )
        
    def _setup_driver(self):
        """Setup and return a Selenium WebDriver."""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={self.headers['User-Agent']}")
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
        
    def scrape(self, limit=100):
        """
        Scrape car listings from AutoTrader.ca.
        
        Args:
            limit (int): Maximum number of listings to scrape
            
        Returns:
            list: List of car listing dictionaries
        """
        print(f"Scraping {self.name}...")
        listings = []
        
        try:
            driver = self._setup_driver()
            driver.get(self.search_url)
            
            # Wait for listings to load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.result-item"))
            )
            
            # Get total pages to scrape
            total_results_text = driver.find_element(By.CSS_SELECTOR, "span.results-text").text
            total_results = int(total_results_text.split()[0].replace(',', ''))
            total_pages = min(total_results // 100 + 1, (limit // 100) + 1)
            
            for page in tqdm(range(1, total_pages + 1), desc="Scraping pages"):
                # Extract listings from current page
                listing_elements = driver.find_elements(By.CSS_SELECTOR, "div.result-item")
                
                for element in listing_elements:
                    if len(listings) >= limit:
                        break
                        
                    try:
                        # Extract data
                        url_element = element.find_element(By.CSS_SELECTOR, "a.link-overlay")
                        url = url_element.get_attribute("href")
                        
                        title = element.find_element(By.CSS_SELECTOR, "h2.title").text.strip()
                        
                        # Extract year, make, model from title
                        year = self._extract_year(title)
                        make, model = self._extract_make_model(title)
                        
                        # Price
                        try:
                            price_element = element.find_element(By.CSS_SELECTOR, "span.price-amount")
                            price = self._extract_price(price_element.text)
                        except NoSuchElementException:
                            price = None
                        
                        # Mileage
                        try:
                            mileage_element = element.find_element(By.CSS_SELECTOR, "span.kms")
                            mileage = self._extract_mileage(mileage_element.text)
                        except NoSuchElementException:
                            mileage = None
                        
                        # Body type
                        try:
                            specs_list = element.find_elements(By.CSS_SELECTOR, "div.ad-specs li")
                            body_type = None
                            for spec in specs_list:
                                spec_text = spec.text.lower()
                                if any(bt in spec_text for bt in ["sedan", "coupe", "hatchback", "suv", "truck", "van"]):
                                    body_type = next((bt for bt in ["sedan", "coupe", "hatchback", "suv", "truck", "van"] if bt in spec_text), None)
                                    break
                        except NoSuchElementException:
                            body_type = None
                        
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
                
                if len(listings) >= limit or page == total_pages:
                    break
                
                # Go to next page
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, "a.page-direction-control.page-direction-control-right")
                    next_button.click()
                    
                    # Wait for new page to load
                    time.sleep(2)
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.result-item"))
                    )
                except (NoSuchElementException, TimeoutException):
                    break
                    
        except Exception as e:
            print(f"Error scraping {self.name}: {str(e)}")
        
        finally:
            if 'driver' in locals():
                driver.quit()
                
        print(f"Scraped {len(listings)} listings from {self.name}")
        return listings 