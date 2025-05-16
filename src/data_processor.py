import pandas as pd
import numpy as np
import os
from pathlib import Path
import datetime

# Define the path to the NRCan data relative to this script's location
# Assumes data dir is ../data relative to src/
FUEL_DATA_PATH = Path(__file__).parent.parent / "data" / "MY2015-2024 Fuel Consumption Ratings.csv"

# --- Constants for TCO Calculation ---
AVG_ANNUAL_MILEAGE_KM = 15000
AVG_VEHICLE_LIFESPAN_KM = 300000
# Default tax rate (Ontario HST) - Can be made configurable
DEFAULT_TAX_RATE = 0.13
# Default average annual insurance cost (Highly variable!) - Can be made configurable
DEFAULT_ANNUAL_INSURANCE_COST = 1800

# --- New/Modified Constants for TCO ---
CBB_API_KEY_PLACEHOLDER = "YOUR_CBB_API_KEY"  # Store securely, e.g., in .env
DEFAULT_PROVINCE = "ON" # Example, can be made configurable
AVG_OWNERSHIP_YEARS = 5 # Typical ownership period for TCO calculation

# Mock provincial fuel prices (cents/L) - to be replaced by actual data source integration (StatCan/NRCAN)
MOCK_PROVINCIAL_FUEL_PRICES = {
    "AB": 1.45, "BC": 1.65, "MB": 1.50, "NB": 1.55, "NL": 1.60,
    "NS": 1.58, "NT": 1.70, "NU": 2.00, "ON": 1.52, "PE": 1.56,
    "QC": 1.62, "SK": 1.48, "YT": 1.65,
}
DEFAULT_FUEL_PRICE_PER_LITRE = MOCK_PROVINCIAL_FUEL_PRICES[DEFAULT_PROVINCE] # Default if province not found

# Base maintenance cost per KM - Adjusted by make factor and age/mileage
BASE_MAINTENANCE_COST_PER_KM = 0.08

# Old Depreciation rates (will be superseded by CBB mock for TCO)
# DEPRECIATION_RATE_YEAR_1 = 0.25
# DEPRECIATION_RATE_YEARS_2_5 = 0.15
# DEPRECIATION_RATE_YEARS_6_PLUS = 0.10

# Maintenance cost factors by make (Lower factor = cheaper maintenance)
# Based loosely on relative rankings from CarEdge/YourMechanic
# Default is 1.0
MAKE_MAINTENANCE_FACTORS = {
    'toyota': 0.8, 'lexus': 0.85, 'scion': 0.85, # Generally low
    'honda': 0.9, 'acura': 0.95, 'mazda': 0.9, 'mitsubishi': 0.9, # Relatively low
    'hyundai': 1.0, 'kia': 1.0, 'nissan': 1.0, 'infiniti': 1.1, 'subaru': 1.0, # Average
    'ford': 1.05, 'lincoln': 1.15, 'gm': 1.05, 'chevrolet': 1.05, 'buick': 1.05, 'cadillac': 1.2, # Domestic avg/higher
    'chrysler': 1.1, 'dodge': 1.1, 'ram': 1.1, 'jeep': 1.1, # Stellantis avg/higher
    'volkswagen': 1.15, 'audi': 1.3, # German avg/higher
    'bmw': 1.5, 'mercedes-benz': 1.5, 'mini': 1.2, # German luxury (high)
    'volvo': 1.3, 'jaguar': 1.4, 'land rover': 1.6 # Other Euro luxury (high/v.high)
    # Add more makes as needed
}

class VehicleDataProcessor:
    def __init__(self, reliability_data_path, tax_rate=DEFAULT_TAX_RATE,
                 annual_insurance_cost=DEFAULT_ANNUAL_INSURANCE_COST,
                 province=DEFAULT_PROVINCE):
        """
        Initialize the data processor with reliability data and fuel consumption data.

        Args:
            reliability_data_path (str): Path to the CSV containing the *OLD* reliability data (chart_data_filtered.csv)
            tax_rate (float): Purchase tax rate.
            annual_insurance_cost (float): Estimated average annual insurance cost.
            province (str): Default province code for fuel price lookups (e.g., "ON").
        """
        # Load the primary approved vehicles and their reliability data
        self.approved_vehicles_data = []
        approved_vehicles_path = Path(__file__).parent.parent / "data" / "approved_vehicles_reliability.csv"
        try:
            # Explicitly specify dtype for Year to avoid mixed type warnings / issues
            approved_df = pd.read_csv(approved_vehicles_path, dtype={'Year': 'Int64'}) # Use Int64 to handle potential NA as pandas integer
            # Ensure required columns exist
            required_cols = ['Make', 'Model', 'Year', 'QIRRate', 'DefectRate']
            if not all(col in approved_df.columns for col in required_cols):
                raise ValueError(f"Approved vehicles CSV is missing one or more required columns: {required_cols}")

            # Normalize make and model for consistent matching later
            approved_df['Make_lc'] = approved_df['Make'].str.lower().str.strip()
            approved_df['Model_norm'] = approved_df['Model'].str.lower().str.replace('-', ' ', regex=False).str.strip()
            # Convert Year to integer, coercing errors to NaT which will then be dropped
            approved_df['Year'] = pd.to_numeric(approved_df['Year'], errors='coerce')
            approved_df.dropna(subset=['Make_lc', 'Model_norm', 'Year'], inplace=True) # Drop rows where key info is missing
            approved_df['Year'] = approved_df['Year'].astype(int)
            # Normalize composite score (should be float between 0 and 1)
            if 'Composite score' in approved_df.columns:
                approved_df['CompositeScore'] = pd.to_numeric(approved_df['Composite score'], errors='coerce').fillna(0) * 100
            else:
                approved_df['CompositeScore'] = 0

            self.approved_vehicles_data = approved_df[['Make_lc', 'Model_norm', 'Year', 'CompositeScore']].to_dict('records')
            # Create a set of (make, model) tuples for quick lookup
            self.approved_make_model_set = set()
            if 'Make_lc' in approved_df.columns and 'Model_norm' in approved_df.columns:
                self.approved_make_model_set = set(zip(approved_df['Make_lc'], approved_df['Model_norm']))
            
            print(f"Successfully loaded {len(self.approved_vehicles_data)} records from {approved_vehicles_path} and created {len(self.approved_make_model_set)} unique make/model pairs for approval.")
        except FileNotFoundError:
            print(f"CRITICAL ERROR: Approved vehicles data not found at {approved_vehicles_path}. No vehicles will be processed.")
        except ValueError as ve:
            print(f"CRITICAL ERROR: Value error processing approved vehicles CSV: {ve}. No vehicles will be processed.")
        except Exception as e:
            print(f"CRITICAL ERROR: Failed to load or process approved vehicles data from {approved_vehicles_path}: {e}. No vehicles will be processed.")
        
        # Load OLD reliability data (chart_data_filtered.csv) - this might become supplementary or be removed
        # For now, keep it, but its QIR/DefectRate will be overridden by approved_vehicles_data if a match is found
        try:
            self.reliability_data = pd.read_csv(reliability_data_path)
            self.qir_rate_dict = self._convert_to_lookup_dict('QIRRate') # Used for non-approved or as fallback
            self.defect_rate_dict = self._convert_to_lookup_dict('DefectRate') # Used for non-approved or as fallback
        except Exception as e:
            print(f"Warning: Could not load or process the old reliability data from {reliability_data_path}: {e}")
            self.reliability_data = pd.DataFrame() # Empty DataFrame
            self.qir_rate_dict = {}
            self.defect_rate_dict = {}
        
        # Load Fuel Consumption Data
        self.fuel_data = self._load_fuel_data()
        # Create a lookup dict for fuel data: {(make, model, year): combined_l_100km}
        self.fuel_lookup = self._create_fuel_lookup()
        
        # Store TCO constants/parameters
        self.avg_annual_mileage = AVG_ANNUAL_MILEAGE_KM
        self.tax_rate = tax_rate
        self.estimated_annual_insurance = annual_insurance_cost
        self.province = province
        self.cbb_api_key = CBB_API_KEY_PLACEHOLDER
        self.avg_vehicle_lifespan = AVG_VEHICLE_LIFESPAN_KM
        
        # Cost estimates
        self.maintenance_cost_per_km = {
            'luxury': 0.15,  # Higher for luxury brands
            'mid': 0.10,     # Mid-tier brands
            'economy': 0.07  # Economy brands
        }
        
        # Brand classifications
        self.brand_tiers = {
            'luxury': ['audi', 'bmw', 'mercedes', 'lexus', 'acura', 'infiniti', 'cadillac', 'lincoln', 'volvo', 'jaguar', 'land rover', 'porsche'],
            'mid': ['toyota', 'honda', 'mazda', 'subaru', 'volkswagen', 'hyundai', 'kia', 'nissan', 'ford', 'chevrolet', 'buick', 'chrysler', 'gmc', 'ram', 'jeep'], # Added common mid-tiers
            'economy': ['mitsubishi', 'suzuki', 'fiat', 'smart', 'mini']
        }
        
        # Fuel efficiency estimates (L/100km) - these are approximations
        self.fuel_efficiency = {
            'sedan': 8.0,
            'coupe': 8.5,
            'hatchback': 7.5,
            'suv': 10.0,
            'truck': 12.0,
            'van': 11.0
        }

    def _load_fuel_data(self):
        """Load and preprocess the NRCan fuel consumption data."""
        try:
            # Adjust column names based on the actual CSV header
            # Using simplified English names assumed from the file name
            fuel_df = pd.read_csv(
                FUEL_DATA_PATH,
                usecols=['Model year', 'Make', 'Model', 'Combined (L/100 km)'],
                encoding='utf-8-sig' # Try utf-8-sig for potential BOM
            )
            # Rename columns for consistency
            fuel_df.columns = ['year', 'make', 'model', 'combined_l_100km']
            # Convert to lowercase for easier matching
            fuel_df['make'] = fuel_df['make'].str.lower()
            fuel_df['model'] = fuel_df['model'].str.lower()
            # Remove rows with missing consumption data
            fuel_df.dropna(subset=['combined_l_100km'], inplace=True)
            # Convert year to int
            fuel_df['year'] = fuel_df['year'].astype(int)
            return fuel_df
        except FileNotFoundError:
            print(f"Warning: Fuel consumption data not found at {FUEL_DATA_PATH}. Fuel costs will use default estimates.")
            return None
        except Exception as e:
            print(f"Warning: Error loading fuel consumption data: {e}. Fuel costs will use default estimates.")
            return None

    def _create_fuel_lookup(self):
        """Create a lookup dictionary from the fuel data DataFrame."""
        if self.fuel_data is None:
            return {}

        lookup = {}
        # Handle potential multiple entries per make/model/year (e.g., different engines)
        # We'll average them for simplicity
        grouped = self.fuel_data.groupby(['make', 'model', 'year'])['combined_l_100km'].mean()
        for index, value in grouped.items():
            make, model, year = index
            lookup[(make, model, year)] = value
        return lookup

    def _get_fuel_consumption(self, make, model, year):
        """Get fuel consumption (L/100km) for a specific vehicle, with fallbacks."""
        make = make.lower()
        model = model.lower()
        year = int(year)
        key = (make, model, year)

        # 1. Direct match
        if key in self.fuel_lookup:
            return self.fuel_lookup[key]

        # 2. Fallback: Average for make/model across available years
        if self.fuel_data is not None:
            model_avg = self.fuel_data[
                (self.fuel_data['make'] == make) &
                (self.fuel_data['model'] == model)
            ]['combined_l_100km'].mean()
            if not pd.isna(model_avg):
                # print(f"Warning: No exact year match for {make} {model} {year}. Using model average: {model_avg:.2f} L/100km")
                return model_avg

        # 3. Fallback: Average for make across all models/years
        if self.fuel_data is not None:
            make_avg = self.fuel_data[self.fuel_data['make'] == make]['combined_l_100km'].mean()
            if not pd.isna(make_avg):
                # print(f"Warning: No model match for {make} {model} {year}. Using make average: {make_avg:.2f} L/100km")
                return make_avg

        # 4. Fallback: Broad default (e.g., overall average or a fixed guess)
        # print(f"Warning: No fuel data found for {make} {model} {year}. Using default: 9.0 L/100km")
        return 9.0 # General fallback guess

    def _convert_to_lookup_dict(self, chart_type):
        """Convert reliability data to a nested dictionary for easy lookup."""
        filtered_data = self.reliability_data[self.reliability_data['ChartType'] == chart_type]
        result = {}
        
        for _, row in filtered_data.iterrows():
            # Check for NaN in 'Year' before attempting conversion
            if pd.isna(row['Year']):
                # print(f"Warning: Skipping row due to missing or invalid year: {row.to_dict()}")
                continue  # Skip this row

            make = row['Make'].lower()
            model = row['Model'].lower()
            try:
                year = int(row['Year'])
            except ValueError:
                # print(f"Warning: Skipping row due to non-integer year after NaN check (should be rare): {row.to_dict()}")
                continue # Skip if year is not a valid integer after all

            value = row['Value']
            
            if make not in result:
                result[make] = {}
                
            if model not in result[make]:
                result[make][model] = {}
                
            result[make][model][year] = value
            
        return result
    
    def get_reliability_scores(self, make, model, year):
        """
        Get QIRRate and DefectRate for a vehicle.
        
        Returns:
            tuple: (QIRRate, DefectRate) or (None, None) if not found
        """
        make = make.lower()
        model = model.lower()
        year = int(year)
        
        qir_rate = None
        defect_rate = None
        
        # Try to get scores directly
        if make in self.qir_rate_dict:
            if model in self.qir_rate_dict[make]:
                if year in self.qir_rate_dict[make][model]:
                    qir_rate = self.qir_rate_dict[make][model][year]
                    
        if make in self.defect_rate_dict:
            if model in self.defect_rate_dict[make]:
                if year in self.defect_rate_dict[make][model]:
                    defect_rate = self.defect_rate_dict[make][model][year]
        
        # If not found, try to estimate from nearby years
        if qir_rate is None and make in self.qir_rate_dict and model in self.qir_rate_dict[make]:
            years = list(self.qir_rate_dict[make][model].keys())
            if years:
                closest_year = min(years, key=lambda y: abs(y - year))
                if abs(closest_year - year) <= 2:  # Only use if within 2 years
                    qir_rate = self.qir_rate_dict[make][model][closest_year]
        
        if defect_rate is None and make in self.defect_rate_dict and model in self.defect_rate_dict[make]:
            years = list(self.defect_rate_dict[make][model].keys())
            if years:
                closest_year = min(years, key=lambda y: abs(y - year))
                if abs(closest_year - year) <= 2:  # Only use if within 2 years
                    defect_rate = self.defect_rate_dict[make][model][closest_year]
        
        return qir_rate, defect_rate
    
    def calculate_remaining_lifespan(self, mileage):
        """Calculate estimated remaining lifespan in km."""
        return max(0, self.avg_vehicle_lifespan - float(mileage)) # Ensure mileage is float

    def _get_make_maintenance_factor(self, make):
        return MAKE_MAINTENANCE_FACTORS.get(make.lower(), 1.0) # Default to 1.0 if make not listed

    def _get_provincial_fuel_price(self, province_code):
        """
        Get average fuel price for a given province.
        In a real application, this would fetch data from Statistics Canada, NRCAN,
        or a fuel price API.
        """
        return MOCK_PROVINCIAL_FUEL_PRICES.get(province_code.upper(), DEFAULT_FUEL_PRICE_PER_LITRE)

    def _get_cbb_depreciation_data(self, make, model, year, mileage, listing_price):
        """
        Placeholder for Canadian Black Book (CBB) API call to get depreciation data.
        In a real scenario, this would make an HTTP request to CBB API using self.cbb_api_key.
        Requires vehicle VIN or precise make/model/year/trim, mileage.

        Args:
            make (str): Vehicle make.
            model (str): Vehicle model.
            year (int): Vehicle year.
            mileage (int): Current vehicle mileage.
            listing_price (float): The listed purchase price of the vehicle.

        Returns:
            tuple: (estimated_current_market_value, estimated_future_residual_value)
                   - estimated_current_market_value (float): CBB's assessment of current value.
                     For this mock, we'll assume it's close to listing_price.
                   - estimated_future_residual_value (float): CBB's projection of value after
                     AVG_OWNERSHIP_YEARS.
        """
        # This is a very rough mock. CBB data would be much more accurate and granular.
        # print(f"Mock CBB: Fetching depreciation for {year} {make} {model} with {mileage}km, listed at ${listing_price:.2f}")
        
        current_vehicle_age = max(0, datetime.datetime.now().year - year)
        
        # For mock simplicity, assume CBB's current valuation is the listing price.
        # A real API might return a slightly different current market value.
        estimated_current_market_value = float(listing_price)

        # Mock future residual value calculation after AVG_OWNERSHIP_YEARS.
        # This uses a highly simplified declining balance for mock purposes.
        # Real CBB residual values are based on extensive market data and modeling.
        future_value = estimated_current_market_value
        
        # Simulate year-by-year depreciation for AVG_OWNERSHIP_YEARS
        for i in range(AVG_OWNERSHIP_YEARS):
            age_at_future_year = current_vehicle_age + i
            # Example: Higher depreciation for first few years of vehicle's life, then tapers.
            if age_at_future_year < 1:
                dep_rate = 0.20  # 20% in first year of life
            elif age_at_future_year < 3:
                dep_rate = 0.15  # 15% for years 2-3 of life
            elif age_at_future_year < 6:
                dep_rate = 0.12  # 12% for years 4-6 of life
            else:
                dep_rate = 0.10  # 10% for older years
            
            future_value *= (1 - dep_rate)

        estimated_future_residual_value = max(0, future_value) # Value shouldn't go below zero
        
        # print(f"Mock CBB: Est. Current Value: ${estimated_current_market_value:.2f}, Est. Future Residual ({AVG_OWNERSHIP_YEARS} yrs): ${estimated_future_residual_value:.2f}")
        return estimated_current_market_value, estimated_future_residual_value

    def get_maintenance_cost(self, make, mileage, age_years, for_annual_mileage=AVG_ANNUAL_MILEAGE_KM):
        """
        Calculates estimated maintenance cost for a given number of kilometers (e.g., one year of driving).
        This is a simplified version focusing on calculating cost for a period based on current state.
        """
        base_maint_factor = self._get_make_maintenance_factor(make.lower())
        # Apply aging and mileage factors. These are indicative and can be refined.
        age_factor = 1 + (age_years / 10)  # Example: cost increases 10% for each year of age from baseline
        mileage_factor = 1 + (mileage / 150000) # Example: cost increases relative to 150k km
        
        adjusted_maint_cost_per_km = base_maint_factor * BASE_MAINTENANCE_COST_PER_KM * age_factor * mileage_factor
        return adjusted_maint_cost_per_km * for_annual_mileage

    def calculate_tco(self, listing_price, make, model, year, mileage, province_code=None):
        """
        Calculates the estimated Total Cost of Ownership over AVG_OWNERSHIP_YEARS.

        Args:
            listing_price (float): The asking price of the car.
            make (str): Vehicle make.
            model (str): Vehicle model.
            year (int): Vehicle manufacturing year.
            mileage (int): Current mileage of the vehicle.
            province_code (str, optional): Province code for specific fuel prices. 
                                         Defaults to instance's default province.

        Returns:
            dict: A dictionary containing various TCO components and totals.
        """
        details = {}
        try:
            listing_price = float(listing_price)
            year = int(year)
            mileage = int(mileage)
        except ValueError:
            # Handle cases where conversion might fail for some inputs
            # print("Error: Invalid numeric value for price, year, or mileage in TCO calc.")
            return {"error": "Invalid numeric input for TCO calculation", "avg_annual_tco_plus_tax": float('inf')} # Return high TCO on error
            
        current_car_age_years = max(0, datetime.datetime.now().year - year)

        # 1. Purchase Tax (applied to listing price)
        purchase_tax_cost = listing_price * self.tax_rate
        details['purchase_tax'] = purchase_tax_cost

        # 2. Depreciation over AVG_OWNERSHIP_YEARS
        _cbb_current_market_val, cbb_future_residual_val = self._get_cbb_depreciation_data(
            make, model, year, mileage, listing_price
        )
        total_depreciation = listing_price - cbb_future_residual_val
        details['total_depreciation_over_period'] = total_depreciation
        details['avg_annual_depreciation'] = total_depreciation / AVG_OWNERSHIP_YEARS if AVG_OWNERSHIP_YEARS > 0 else total_depreciation
        details['estimated_resale_value_after_period'] = cbb_future_residual_val

        # 3. Fuel Costs over AVG_OWNERSHIP_YEARS
        fuel_consumption_l_100km = self._get_fuel_consumption(make, model, year)
        
        actual_province = province_code if province_code else self.province
        current_fuel_price_per_litre = self._get_provincial_fuel_price(actual_province)
        
        annual_fuel_cost = (self.avg_annual_mileage / 100) * fuel_consumption_l_100km * current_fuel_price_per_litre
        total_fuel_cost_over_period = annual_fuel_cost * AVG_OWNERSHIP_YEARS
        details['total_fuel_cost_over_period'] = total_fuel_cost_over_period
        details['avg_annual_fuel_cost'] = annual_fuel_cost
        details['fuel_price_used_per_l'] = current_fuel_price_per_litre
        details['fuel_consumption_l_100km_used'] = fuel_consumption_l_100km

        # 4. Maintenance Costs over AVG_OWNERSHIP_YEARS
        total_maintenance_cost_over_period = 0
        simulated_mileage = mileage
        simulated_age = current_car_age_years

        for _ in range(AVG_OWNERSHIP_YEARS):
            annual_maint_cost = self.get_maintenance_cost(make, simulated_mileage, simulated_age)
            total_maintenance_cost_over_period += annual_maint_cost
            simulated_mileage += self.avg_annual_mileage
            simulated_age += 1
            
        details['total_maintenance_over_period'] = total_maintenance_cost_over_period
        details['avg_annual_maintenance'] = total_maintenance_cost_over_period / AVG_OWNERSHIP_YEARS if AVG_OWNERSHIP_YEARS > 0 else total_maintenance_cost_over_period

        # 5. Insurance Costs over AVG_OWNERSHIP_YEARS
        total_insurance_cost_over_period = self.estimated_annual_insurance * AVG_OWNERSHIP_YEARS
        details['total_insurance_over_period'] = total_insurance_cost_over_period
        details['avg_annual_insurance'] = self.estimated_annual_insurance

        # Calculate Total TCO
        tco_sum_over_period = (
            purchase_tax_cost + 
            total_depreciation + 
            total_fuel_cost_over_period + 
            total_maintenance_cost_over_period + 
            total_insurance_cost_over_period
        )
        details['listing_price'] = listing_price
        details['total_tco_plus_tax_over_period'] = tco_sum_over_period
        details['avg_annual_tco_plus_tax'] = tco_sum_over_period / AVG_OWNERSHIP_YEARS if AVG_OWNERSHIP_YEARS > 0 else tco_sum_over_period
        
        details['tco_calculation_years'] = AVG_OWNERSHIP_YEARS
        
        qir, defect = self.get_reliability_scores(make, model, year)
        details['reliability_qir'] = qir
        details['reliability_defect_rate'] = defect

        # Calculate and add remaining lifespan and cost per km
        details['remaining_lifespan_km'] = self.calculate_remaining_lifespan(mileage)
        avg_tco = details.get('avg_annual_tco_plus_tax')
        if self.avg_annual_mileage > 0 and avg_tco is not None and not pd.isna(avg_tco):
            details['cost_per_km'] = avg_tco / self.avg_annual_mileage
        else:
            details['cost_per_km'] = float('inf')

        return details

    def calculate_deal_score(self, car_data):
        """
        Calculate a score for each car deal based on TCO per km and composite reliability score.
        TCO per km is the main factor, composite score is secondary.
        """
        # Get TCO sub-dictionary and extract cost_per_km
        tco_data = car_data.get('tco_details', {})
        cost_per_km = tco_data.get('cost_per_km', float('inf'))
        # Composite score (should be 0-100)
        composite_score = car_data.get('composite_score', 0)

        # Cost per km (lower is better)
        # Normalize: Assume good is < $0.30/km, bad is > $1.00/km
        cost_range = 1.00 - 0.30
        normalized_cost = max(0, min(1, (cost_per_km - 0.30) / cost_range))
        cost_score = 100 * (1 - normalized_cost)

        # Composite score is already 0-100
        composite_score_norm = max(0, min(100, composite_score))

        # Weights
        weights = {
            'cost': 0.70,
            'composite': 0.30
        }

        weighted_score = (
            cost_score * weights['cost'] +
            composite_score_norm * weights['composite']
        )
        return round(weighted_score, 2)

    def process_car_listings(self, listings):
        """
        Process a list of scraped car listings, calculate TCO, deal scores, and filter.

        Args:
            listings (list): List of dictionaries, where each dict is a scraped car listing.

        Returns:
            pandas.DataFrame: Processed and scored car listings, sorted by deal score.
        """
        processed_cars = []
        if not self.approved_make_model_set and not self.approved_vehicles_data:
            print("CRITICAL: No approved vehicles loaded. Cannot process listings against approval list.")
            # Depending on desired behavior, could return empty DF or process without approval filter
            # For now, let's assume we want to strictly filter if the file was intended to be used.
            # If the approved_vehicles_reliability.csv was optional, this behavior would change.

        for i, car_data in enumerate(listings):
            # Ensure basic fields are present
            if not all(k in car_data for k in ['make', 'model', 'year', 'price', 'mileage', 'url']):
                print(f"Skipping car due to missing essential fields: {car_data.get('title', 'N/A')}")
                continue

            # --- Price Filter (Under $20,000) ---
            price = car_data.get('price')
            if price is None or not isinstance(price, (int, float)) or price >= 20000:
                # print(f"DEBUG: Skipping car {car_data.get('make')} {car_data.get('model')} due to price: {price}")
                continue

            # --- Approved Vehicle Filter ---
            # Normalize make and model from scraped data for matching
            scraped_make_lc = str(car_data.get('make', '')).lower().strip()
            scraped_model_norm = str(car_data.get('model', '')).lower().replace('-', ' ').strip()
            
            # Define scraped_year from car_data before it's used for matching
            try:
                scraped_year = int(car_data.get('year'))
            except (ValueError, TypeError):
                print(f"DEBUG: Skipping car due to invalid or missing year: {car_data.get('title', 'N/A')}")
                continue

            if not self.approved_make_model_set: # If set is empty (e.g. file load failed but we didn't exit)
                 print(f"DEBUG: Approved make/model set is empty. Cannot filter {scraped_make_lc} {scraped_model_norm}. Processing depends on error handling strategy.")
                 # To strictly enforce, we might 'continue' here. For now, let it pass if set is empty due to load failure.
                 # This assumes if the file is truly missing and critical, earlier checks in __init__ or main would halt.
            elif (scraped_make_lc, scraped_model_norm) not in self.approved_make_model_set:
                # print(f"DEBUG: Skipping car {scraped_make_lc} {scraped_model_norm} as it's not in the approved list.")
                continue
            
            # --- Proceed with processing if filters passed ---
            # print(f"DP_DEBUG Pre-TCO: Make={car_data.get('make')}, Model={car_data.get('model')}, Year={scraped_year}, Price={price}, Mileage={car_data.get('mileage')}")

            try:
                # --- Match against approved vehicles list (using Make, Model, Year) ---
                matched_approved_vehicle_data = None
                for approved_vehicle in self.approved_vehicles_data:
                    if (scraped_make_lc == approved_vehicle['Make_lc'] and 
                        scraped_year == approved_vehicle['Year'] and 
                        scraped_model_norm.startswith(approved_vehicle['Model_norm'])):
                        matched_approved_vehicle_data = approved_vehicle
                        break # Found a match
                
                if not matched_approved_vehicle_data:
                    # print(f"Skipping unmatched vehicle: {scraped_make_lc} {scraped_model_full} {scraped_year}")
                    continue # Not in the approved list
                
                # --- Vehicle is approved, proceed with processing ---
                # print(f"Processing approved vehicle: {scraped_make_lc} {scraped_model_full} {scraped_year}")

                price_val = car_data.get('price')
                mileage_val = car_data.get('mileage')

                if isinstance(price_val, (int, float)):
                    price = float(price_val)
                else:
                    price_str = str(price_val if price_val is not None else '0').replace('$', '').replace(',', '').replace(' ', '')
                    price = float(price_str) if price_str else 0.0
                
                if isinstance(mileage_val, (int, float)):
                    mileage = int(mileage_val) # Mileage should be int
                else:
                    mileage_str = str(mileage_val if mileage_val is not None else '0').replace('km', '').replace(',', '').replace(' ', '')
                    mileage = int(mileage_str) if mileage_str else 0

                make = scraped_make_lc
                model_name = scraped_model_norm
                year = scraped_year # Use the validated year
                
                # --- Debug Print in DataProcessor --- 
                print(f"DP_DEBUG Pre-TCO: Make={make}, Model={model_name}, Year={year}, Price={price}, Mileage={mileage}")
                # --- End Debug Print ---

                url = car_data.get('url', '')
                # Create a more robust listing_id, e.g. from URL or a hash of details
                # Ensure price_str is defined if used in listing_id when price is already float
                current_price_str_for_id = str(price) if isinstance(price, (int, float)) else price_str
                listing_id = car_data.get('id', f"{make}_{model_name}_{year}_{current_price_str_for_id}_{i}")

                if year == 0 or price == 0.0:
                    # print(f"Skipping car due to missing/invalid year or price: {listing_id}")
                    processed_cars.append({
                        'id': listing_id, 'url': url, 'make': make, 'model': model_name, 
                        'year': year, 'price': price, 'mileage': mileage, 'composite_score': 0,
                        'scraped_date': datetime.date.today().isoformat(),
                        'tco_details': {'error': 'Missing year/price'}, 
                        'avg_annual_tco': np.nan, 'estimated_resale_5yr': np.nan,
                        'deal_score': np.nan, 'error_processing': 'Missing year/price'
                    })
                    continue
                
                tco_details = self.calculate_tco(price, make, model_name, year, mileage, province_code=self.province)

                # Get CompositeScore from matched_approved_vehicle_data
                composite_score = matched_approved_vehicle_data.get('CompositeScore', 0)

                car_processed_data = {
                    'id': listing_id,
                    'url': url,
                    'make': make, # This is already lowercased from earlier
                    'model': model_name, # This is already lowercased from earlier
                    'year': year,
                    'price': price,
                    'mileage': mileage,
                    'composite_score': composite_score, # Add composite score
                    'scraped_date': datetime.date.today().isoformat(),
                    'tco_details': tco_details, 
                    'avg_annual_tco': tco_details.get('avg_annual_tco_plus_tax'),
                    'estimated_resale_after_period': tco_details.get('estimated_resale_value_after_period')
                }
                
                deal_score = self.calculate_deal_score(car_processed_data) # Pass the dict with tco_details
                car_processed_data['deal_score'] = deal_score
                
                # --- Debug Print in DataProcessor Post-Processing ---
                print(f"DP_DEBUG Post-Proc: Appending to processed_cars: {car_processed_data}")
                # --- End Debug Print ---

                processed_cars.append(car_processed_data)

            except Exception as e:
                print(f"Error processing car: {car_data}. Error: {e}")
                error_listing_id = car_data.get('id', f"error_idx_{i}")
                processed_cars.append({
                    'id': error_listing_id, 'url': car_data.get('url', ''), 'make': car_data.get('make', 'Error'), 
                    'model': car_data.get('model', 'Error'), 'year': car_data.get('year', 0), 'price': 0, 
                    'mileage': 0, 'composite_score': 0,
                    'scraped_date': datetime.date.today().isoformat(),
                    'tco_details': {'error': str(e)}, 'avg_annual_tco': np.nan, 
                    'estimated_resale_after_period': np.nan,
                    'deal_score': np.nan, 'error_processing': str(e)
                })

        df = pd.DataFrame(processed_cars)
        
        # Expand TCO details into separate columns if the column exists and is not empty
        if not df.empty and 'tco_details' in df.columns and df['tco_details'].apply(lambda x: isinstance(x, dict)).any():
            # Filter out rows where tco_details might not be a dict (e.g. error cases)
            valid_tco_details = df[df['tco_details'].apply(lambda x: isinstance(x, dict))]['tco_details'].apply(pd.Series)
            if not valid_tco_details.empty:
                valid_tco_details = valid_tco_details.add_prefix('tco_')
                # Align indices for concatenation
                df = df.join(valid_tco_details)
            df = df.drop(columns=['tco_details'], errors='ignore') # Drop original tco_details column
            
        # Define desired column order
        if not df.empty:
            # Start with the main identifiers and the newly prioritized group
            desired_order = ['id', 'url', 'deal_score', 'avg_annual_tco']
            # Add 'tco_cost_per_km' if it exists (it should come from tco_details)
            if 'tco_cost_per_km' in df.columns:
                desired_order.insert(3, 'tco_cost_per_km') # Insert after deal_score
            else: # If tco_cost_per_km is missing, ensure avg_annual_tco is next to deal_score correctly
                pass # avg_annual_tco is already correctly positioned if tco_cost_per_km isn't there

            # Add other primary car details
            primary_details = ['make', 'model', 'year', 'price', 'mileage', 'composite_score', 'estimated_resale_after_period']
            for col in primary_details:
                if col in df.columns and col not in desired_order:
                    desired_order.append(col)
            
            # Add all remaining columns, ensuring no duplicates and all are included
            remaining_columns = [col for col in df.columns if col not in desired_order]
            desired_order.extend(remaining_columns)
            
            # Filter desired_order to only include columns actually present in df to prevent KeyErrors
            desired_order = [col for col in desired_order if col in df.columns]
            
            df = df.reindex(columns=desired_order)
            
        return df

    def export_to_csv(self, df_new_listings, output_path):
        """
        Export results to CSV, appending to existing data and removing stale entries.
        
        Args:
            df_new_listings (pd.DataFrame): DataFrame of newly scraped and processed listings.
            output_path (str): Path to save CSV.
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Ensure scraped_date is in new listings (it should be added by process_car_listings)
        if 'scraped_date' not in df_new_listings.columns and not df_new_listings.empty:
            # This should not happen if process_car_listings is working correctly
            print("Warning: 'scraped_date' column missing in new listings. Adding current date.")
            df_new_listings['scraped_date'] = datetime.date.today().isoformat()
        elif df_new_listings.empty and 'scraped_date' not in df_new_listings.columns:
            # If df_new_listings is empty, ensure it has the column for consistency if we try to concat later
            df_new_listings = pd.DataFrame(columns=df_new_listings.columns.tolist() + ['scraped_date'])

        df_final_export = pd.DataFrame()

        if output_file.exists() and os.path.getsize(output_file) > 0:
            try:
                df_existing = pd.read_csv(output_file)
                print(f"Read {len(df_existing)} existing listings from {output_file}")

                # Ensure 'scraped_date' and 'url' columns exist in existing data
                if 'scraped_date' not in df_existing.columns:
                    print(f"Warning: 'scraped_date' column missing in {output_file}. Old entries without it cannot be aged out by date.")
                    # To prevent errors, we might fill it with a very old date or handle differently
                    # For now, if it's missing, those rows won't be filtered by date.
                if 'url' not in df_existing.columns:
                     print(f"Warning: 'url' column missing in {output_file}. Cannot reliably merge with new data. Overwriting.")
                     df_existing = pd.DataFrame() # Treat as if no valid existing data

                if not df_existing.empty and 'scraped_date' in df_existing.columns and 'url' in df_existing.columns:
                    # Convert scraped_date to datetime for comparison, coercing errors
                    df_existing['scraped_date_dt'] = pd.to_datetime(df_existing['scraped_date'], errors='coerce')
                    
                    # Identify stale listings (older than 7 days)
                    seven_days_ago = pd.to_datetime(datetime.date.today() - datetime.timedelta(days=7))
                    is_stale_by_date = df_existing['scraped_date_dt'] < seven_days_ago

                    # Get set of URLs from the newly scraped listings for quick lookup
                    newly_scraped_urls = set(df_new_listings['url'].unique())
                    
                    # Filter existing data:
                    # Keep if NOT stale by date OR if its URL is in the newly scraped data (i.e., it was found again)
                    # Also keep if scraped_date_dt is NaT (meaning original date was invalid, can't determine staleness by date, keep for now)
                    condition_to_keep = (~is_stale_by_date) | (df_existing['url'].isin(newly_scraped_urls)) | (df_existing['scraped_date_dt'].isna())
                    df_to_keep_from_existing = df_existing[condition_to_keep].copy()
                    df_to_keep_from_existing.drop(columns=['scraped_date_dt'], inplace=True, errors='ignore')
                    print(f"Keeping {len(df_to_keep_from_existing)} listings from existing data after staleness/refresh check.")
                    
                    # Concatenate filtered old data with new data
                    df_final_export = pd.concat([df_to_keep_from_existing, df_new_listings], ignore_index=True)
                else: # df_existing was empty or missing crucial columns
                    df_final_export = df_new_listings.copy()

            except pd.errors.EmptyDataError:
                print(f"Info: {output_file} is empty. Starting fresh.")
                df_final_export = df_new_listings.copy()
            except Exception as e:
                print(f"Error reading or processing existing {output_file}: {e}. Overwriting with new listings.")
                df_final_export = df_new_listings.copy()
        else:
            print(f"No existing {output_file} found or it is empty. Saving new listings.")
            df_final_export = df_new_listings.copy()

        if df_final_export.empty:
            print(f"No data to export to {output_path} after processing existing and new listings.")
            # Create an empty file or a file with headers if desired, to ensure output.csv exists
            # To be consistent, use the columns of df_new_listings if it had any, or a default set
            cols = df_new_listings.columns if not df_new_listings.empty else ['id', 'url', 'deal_score', 'avg_annual_tco', 'make', 'model', 'year', 'price', 'mileage', 'composite_score', 'scraped_date']
            pd.DataFrame(columns=cols).to_csv(output_file, index=False)
            return str(output_file)

        # Remove duplicates based on URL, keeping the entry from the new scrape if there's an overlap
        # (or the one with the latest scraped_date if multiple old entries for same URL survived)
        if 'url' in df_final_export.columns:
            if 'scraped_date' in df_final_export.columns:
                df_final_export.sort_values(by=['url', 'scraped_date'], ascending=[True, True], inplace=True)
                df_final_export.drop_duplicates(subset=['url'], keep='last', inplace=True)
            else: # Fallback if scraped_date somehow isn't there for sorting, just keep one
                df_final_export.drop_duplicates(subset=['url'], keep='last', inplace=True)
        
        # Sort by deal_score before final export
        if 'deal_score' in df_final_export.columns:
            df_final_export.sort_values(by='deal_score', ascending=False, inplace=True)

        # Monetary formatting (copied from original, apply to df_final_export)
        monetary_cols = ['price', 'avg_annual_tco', 'estimated_resale_after_period']
        for col in df_final_export.columns:
            if col.startswith('tco_') and (df_final_export[col].dtype == 'float64' or df_final_export[col].dtype == 'int64'):
                if not any(keyword in col for keyword in ['rate', 'years', 'count', 'l_100km', 'lifespan', 'per_km']):
                    monetary_cols.append(col)
        
        df_copy_for_export = df_final_export.copy()
        for col in monetary_cols:
            if col in df_copy_for_export.columns:
                df_copy_for_export[col] = pd.to_numeric(df_copy_for_export[col], errors='coerce')
                df_copy_for_export[col] = df_copy_for_export[col].apply(lambda x: f"${x:.2f}" if pd.notnull(x) else 'N/A')
            
        df_copy_for_export.to_csv(output_file, index=False)
        print(f"Exported {len(df_final_export)} listings to {output_file}")
        return str(output_file) 