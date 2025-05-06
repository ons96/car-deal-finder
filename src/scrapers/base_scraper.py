import requests
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
import re
import time
import random
from tqdm import tqdm

class BaseScraper(ABC):
    """Base class for all car listing scrapers."""
    
    def __init__(self, name):
        """
        Initialize the scraper.
        
        Args:
            name (str): Name of the scraper for identification
        """
        self.name = name
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'DNT': '1',  # Do Not Track
        }
    
    @abstractmethod
    def scrape(self, limit=100):
        """
        Scrape car listings.
        
        Args:
            limit (int): Maximum number of listings to scrape
            
        Returns:
            list: List of car listing dictionaries
        """
        pass
    
    def _extract_price(self, price_text):
        """Extract numerical price from text."""
        if not price_text:
            return None
            
        # Remove currency symbols and commas
        price_str = re.sub(r'[^\d.]', '', price_text)
        
        try:
            return float(price_str)
        except (ValueError, TypeError):
            return None
    
    def _extract_year(self, title_text):
        """Extract year from title text."""
        if not title_text:
            return None
            
        # Look for 4-digit year between 1990 and 2025
        year_match = re.search(r'\b(19[9][0-9]|20[0-2][0-9])\b', title_text)
        
        if year_match:
            return int(year_match.group(1))
        
        return None
    
    def _extract_mileage(self, mileage_text):
        """Extract numerical mileage from text."""
        if not mileage_text:
            return None
            
        # Extract numeric value
        mileage_str = re.sub(r'[^\d.]', '', mileage_text)
        
        try:
            mileage = float(mileage_str)
            
            # Convert miles to km if necessary (rough estimation)
            if 'mile' in mileage_text.lower() and 'km' not in mileage_text.lower():
                mileage *= 1.60934
                
            return mileage
        except (ValueError, TypeError):
            return None
    
    def _extract_make_model(self, title_text):
        """
        Extract make and model from title text.
        This is a simple heuristic and might need refinement.
        """
        if not title_text:
            return None, None
            
        # Common car makes
        common_makes = [
            'toyota', 'honda', 'ford', 'chevrolet', 'chevy', 'nissan', 'hyundai', 
            'kia', 'mazda', 'subaru', 'volkswagen', 'vw', 'audi', 'bmw', 'mercedes', 
            'benz', 'lexus', 'acura', 'infiniti', 'jeep', 'dodge', 'chrysler', 'ram',
            'buick', 'cadillac', 'gmc', 'lincoln', 'volvo', 'porsche', 'jaguar',
            'land rover', 'range rover', 'mini', 'fiat', 'alfa romeo', 'mitsubishi',
            'suzuki', 'tesla'
        ]
        
        # Clean up and lowercase the title
        title_clean = re.sub(r'[^\w\s]', ' ', title_text.lower())
        title_words = title_clean.split()
        
        # Try to find make
        make = None
        for word in title_words:
            if word in common_makes:
                make = word
                break
                
        # Special cases for two-word makes
        if not make:
            for i in range(len(title_words)-1):
                two_words = f"{title_words[i]} {title_words[i+1]}"
                if two_words in common_makes:
                    make = two_words
                    break
        
        # If no make found, return None
        if not make:
            return None, None
            
        # Try to extract model
        # This is more complex and depends on the make
        # For now, we'll use a simple approach that takes the word after the make
        make_index = None
        
        # Find the index of the make in the title words
        for i, word in enumerate(title_words):
            if word == make:
                make_index = i
                break
        
        # If make is two words, find the index of the second word
        if not make_index and ' ' in make:
            for i in range(len(title_words)-1):
                two_words = f"{title_words[i]} {title_words[i+1]}"
                if two_words == make:
                    make_index = i + 1
                    break
        
        model = None
        if make_index is not None and make_index + 1 < len(title_words):
            model = title_words[make_index + 1]
            
            # Check if the next few words are part of the model (like "Accord Sport")
            if make_index + 2 < len(title_words):
                next_word = title_words[make_index + 2]
                if next_word not in ['for', 'with', 'in', 'at', 'near', 'from', 'sale']:
                    model += ' ' + next_word
        
        return make, model
    
    def _random_delay(self, min_seconds=1, max_seconds=3):
        """Add a random delay to avoid being blocked."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    def _get_soup(self, url):
        """Get BeautifulSoup object from URL."""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'lxml')
        except (requests.RequestException, Exception) as e:
            print(f"Error fetching {url}: {str(e)}")
            return None 