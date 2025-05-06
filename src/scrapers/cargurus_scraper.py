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


class CarGurusScraper(BaseScraper):
    """Scraper for CarGurus.ca"""
    
    def __init__(self):
        """Initialize the CarGurus scraper."""
        super().__init__("CarGurus.ca")
        self.base_url = "https://www.cargurus.ca"
        self.search_url = (
            f"{self.base_url}/Cars/searchResults.action"
            "?zip=L6G3H7&inventorySearchWidgetType=AUTO&sortDir=ASC&sortType=DEAL_SCORE"
            "&shopByTypes=NEAR_BY&bodyTypeGroup=bg8&maxMileage=150000&minPrice=500&maxPrice=100000"
            "&seriesArray=5,35,54,7,15,36,53,9,10,16,32,33,49,57,29&vehicleStyles=SEDAN,COUPE,HATCHBACK"
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
        Scrape car listings from CarGurus.ca.
        
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
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.ft-listing"))
            )
            
            # Check for cookie consent popup and accept it if present
            try:
                consent_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button#onetrust-accept-btn-handler"))
                )
                consent_button.click()
                time.sleep(2)
            except TimeoutException:
                pass
            
            # Determine how many pages to scrape
            try:
                total_results_text = driver.find_element(By.CSS_SELECTOR, "span.sCjvBe").text
                total_results = int(total_results_text.replace(',', '').replace('Results', '').strip())
                total_pages = min(total_results // 15 + 1, (limit // 15) + 1)
            except (NoSuchElementException, ValueError):
                total_pages = (limit // 15) + 1
            
            for page in tqdm(range(1, total_pages + 1), desc="Scraping pages"):
                # Extract listings from current page
                listing_elements = driver.find_elements(By.CSS_SELECTOR, "div.ft-listing")
                
                for element in listing_elements:
                    if len(listings) >= limit:
                        break
                        
                    try:
                        # Extract data
                        url_element = element.find_element(By.CSS_SELECTOR, "a.ft-listings__list-item")
                        url = url_element.get_attribute("href")
                        
                        # Title (may contain year, make, model)
                        title_element = element.find_element(By.CSS_SELECTOR, "h4.ft-listing__title")
                        title = title_element.text.strip()
                        
                        # Extract year, make, model from title
                        year = self._extract_year(title)
                        make, model = self._extract_make_model(title)
                        
                        # Price
                        try:
                            price_element = element.find_element(By.CSS_SELECTOR, "span.ft-listing__price")
                            price = self._extract_price(price_element.text)
                        except NoSuchElementException:
                            price = None
                        
                        # Mileage
                        try:
                            mileage_element = element.find_element(By.CSS_SELECTOR, "p.ft-listing__key-specs")
                            mileage = self._extract_mileage(mileage_element.text)
                        except NoSuchElementException:
                            mileage = None
                        
                        # Body type - try to infer from URL or title, or element class
                        body_type = None
                        for bt in ["sedan", "coupe", "hatchback", "suv", "truck", "van"]:
                            if bt in url.lower() or bt in title.lower():
                                body_type = bt
                                break
                        
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
                    next_button = driver.find_element(By.CSS_SELECTOR, "a[aria-label='Next page']")
                    driver.execute_script("arguments[0].scrollIntoView();", next_button)
                    next_button.click()
                    
                    # Wait for new page to load
                    time.sleep(3)
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.ft-listing"))
                    )
                except (NoSuchElementException, TimeoutException) as e:
                    print(f"Couldn't navigate to next page: {str(e)}")
                    break
                    
        except Exception as e:
            print(f"Error scraping {self.name}: {str(e)}")
        
        finally:
            if 'driver' in locals():
                driver.quit()
                
        print(f"Scraped {len(listings)} listings from {self.name}")
        return listings 