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
# Default average fuel price ($/L) - Ideally updated or configurable
DEFAULT_FUEL_PRICE = 1.50
# Base maintenance cost per KM - Adjusted by make factor and age/mileage
BASE_MAINTENANCE_COST_PER_KM = 0.08 # Lowered base as factors will increase it

# Depreciation rates (example - can be refined)
DEPRECIATION_RATE_YEAR_1 = 0.25
DEPRECIATION_RATE_YEARS_2_5 = 0.15
DEPRECIATION_RATE_YEARS_6_PLUS = 0.10

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
                 fuel_price=DEFAULT_FUEL_PRICE):
        """
        Initialize the data processor with reliability data and fuel consumption data.

        Args:
            reliability_data_path (str): Path to the CSV containing reliability data
            tax_rate (float): Purchase tax rate.
            annual_insurance_cost (float): Estimated average annual insurance cost.
            fuel_price (float): Estimated average fuel price per litre.
        """
        self.reliability_data = pd.read_csv(reliability_data_path)
        # Convert reliability data to easier lookup format
        self.qir_rate_dict = self._convert_to_lookup_dict('QIRRate')
        self.defect_rate_dict = self._convert_to_lookup_dict('DefectRate')
        
        # Load Fuel Consumption Data
        self.fuel_data = self._load_fuel_data()
        # Create a lookup dict for fuel data: {(make, model, year): combined_l_100km}
        self.fuel_lookup = self._create_fuel_lookup()
        
        # Store TCO constants/parameters
        self.avg_annual_mileage = AVG_ANNUAL_MILEAGE_KM
        self.avg_vehicle_lifespan = AVG_VEHICLE_LIFESPAN_KM
        self.tax_rate = tax_rate
        self.estimated_annual_insurance = annual_insurance_cost
        self.fuel_price = fuel_price
        
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
            make = row['Make'].lower()
            model = row['Model'].lower()
            year = int(row['Year'])
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
        """Get the maintenance cost factor for a given make."""
        return MAKE_MAINTENANCE_FACTORS.get(make.lower(), 1.0) # Default to 1.0 if make not found

    def _calculate_depreciation_cost(self, purchase_price, age_years, remaining_lifespan_km):
        """
        Estimate the total depreciation cost over the remaining lifespan.
        This is a simplified estimation based on average annual rates.
        """
        if remaining_lifespan_km <= 0:
            return 0

        remaining_years = remaining_lifespan_km / self.avg_annual_mileage
        estimated_current_value = purchase_price # Start with purchase price for estimation base

        # Apply depreciation year by year to estimate current value (rough)
        for i in range(int(age_years)):
            year = i + 1
            if year == 1:
                rate = DEPRECIATION_RATE_YEAR_1
            elif 2 <= year <= 5:
                rate = DEPRECIATION_RATE_YEARS_2_5
            else:
                rate = DEPRECIATION_RATE_YEARS_6_PLUS
            estimated_current_value *= (1 - rate)

        # Now estimate future depreciation from this point
        total_future_depreciation = 0
        value_at_start_of_future = estimated_current_value
        for i in range(int(np.ceil(remaining_years))):
             # Age at the start of the future year i
            current_age_in_future = age_years + i
            if current_age_in_future < 1: # Should not happen if age_years >= 0
                 rate = DEPRECIATION_RATE_YEAR_1
            elif 1 <= current_age_in_future < 5:
                 rate = DEPRECIATION_RATE_YEARS_2_5
            else: # 5+ years old
                 rate = DEPRECIATION_RATE_YEARS_6_PLUS

            # Calculate depreciation for this future year
            depreciation_this_year = value_at_start_of_future * rate
            total_future_depreciation += depreciation_this_year
            value_at_start_of_future -= depreciation_this_year # Update value for next year's calculation

        # Ensure depreciation doesn't exceed the estimated current value
        return min(total_future_depreciation, estimated_current_value)

    def get_maintenance_cost(self, make, mileage, age_years, remaining_lifespan_km):
        """Calculate estimated maintenance cost for remaining lifespan."""
        if remaining_lifespan_km <= 0:
            return 0

        make_factor = self._get_make_maintenance_factor(make)
        base_cost = BASE_MAINTENANCE_COST_PER_KM * remaining_lifespan_km

        # Apply make factor
        cost = base_cost * make_factor

        # Apply age/mileage penalty (example: +10% if > 8 years old, +10% if > 150k km)
        age_penalty = 1.1 if age_years > 8 else 1.0
        mileage_penalty = 1.1 if mileage > 150000 else 1.0

        # Apply penalties (use max penalty to avoid compounding too much)
        cost *= max(age_penalty, mileage_penalty)

        return cost

    def calculate_tco(self, price, make, model, year, mileage):
        """
        Calculate estimated total cost of ownership over the remaining lifespan.

        Args:
            price (float): Listing price
            make (str): Vehicle make
            model (str): Vehicle model
            year (int): Vehicle year
            mileage (float): Current mileage in km

        Returns:
            dict: TCO breakdown or None if essential info missing
        """
        try:
            price = float(price)
            mileage = float(mileage)
            year = int(year)
            make = make.lower()
            model = model.lower()

            if price <= 0 or year <= 0 or make == 'unknown' or model == 'unknown':
                 print(f"Warning: Skipping TCO calculation due to missing info for {year} {make} {model}")
                 return None

            current_year = datetime.datetime.now().year
            age_years = max(0, current_year - year) # Vehicle age in years

            # Calculate purchase cost with tax
            purchase_cost = price * (1 + self.tax_rate)

            # Estimate remaining lifespan
            remaining_lifespan_km = self.calculate_remaining_lifespan(mileage)
            if remaining_lifespan_km <= 0:
                 # If no remaining lifespan, future costs are zero, TCO is just purchase cost
                 return {
                     'purchase_cost': purchase_cost,
                     'maintenance_cost': 0,
                     'fuel_cost': 0,
                     'insurance_cost': 0,
                     'depreciation_cost': 0, # No future depreciation if not driven
                     'fuel_consumption_l_100km': self._get_fuel_consumption(make, model, year),
                     'total_cost': purchase_cost, # TCO is just the initial cost
                     'cost_per_km': float('inf'), # Avoid division by zero
                     'remaining_lifespan_km': 0
                 }

            remaining_lifespan_years = remaining_lifespan_km / self.avg_annual_mileage

            # --- Calculate Cost Components over Remaining Lifespan ---

            # 1. Maintenance Cost
            maintenance_cost = self.get_maintenance_cost(make, mileage, age_years, remaining_lifespan_km)

            # 2. Fuel Cost
            fuel_consumption_l_100km = self._get_fuel_consumption(make, model, year)
            fuel_consumption_l_km = fuel_consumption_l_100km / 100
            total_fuel_litres = fuel_consumption_l_km * remaining_lifespan_km
            fuel_cost = total_fuel_litres * self.fuel_price

            # 3. Insurance Cost
            insurance_cost = self.estimated_annual_insurance * remaining_lifespan_years

            # 4. Depreciation Cost (Estimated future depreciation)
            # Pass purchase_price as the base for estimation
            depreciation_cost = self._calculate_depreciation_cost(price, age_years, remaining_lifespan_km)

            # --- Calculate Total Cost and Cost per KM ---
            # TCO = Purchase + Maintenance + Fuel + Insurance + Depreciation (all over remaining life)
            # NOTE: Purchase cost *is* part of the TCO from the moment you buy it.
            # The other costs accrue over the remaining life.
            total_operational_cost = maintenance_cost + fuel_cost + insurance_cost + depreciation_cost
            total_cost = purchase_cost + total_operational_cost # Total outlay including purchase

            cost_per_km = total_operational_cost / remaining_lifespan_km if remaining_lifespan_km > 0 else float('inf')
            # Alternatively, cost per km could include purchase price: total_cost / remaining_lifespan_km

            return {
                'purchase_cost': purchase_cost,
                'maintenance_cost': maintenance_cost,
                'fuel_cost': fuel_cost,
                'insurance_cost': insurance_cost,
                'depreciation_cost': depreciation_cost, # Estimated future depreciation
                'fuel_consumption_l_100km': fuel_consumption_l_100km,
                # Removed resale_value, TCO sums costs, doesn't subtract final value
                'total_cost': total_cost, # Includes purchase price
                'total_operational_cost': total_operational_cost, # Excludes purchase price
                'cost_per_km': cost_per_km, # Based on operational costs
                'remaining_lifespan_km': remaining_lifespan_km
            }
        except Exception as e:
            print(f"Error calculating TCO for {year} {make} {model} (Price: {price}, Mileage: {mileage}): {e}")
            return None

    def calculate_deal_score(self, car_data):
        """
        Calculate a score for each car deal based on TCO and reliability.
        (Weights might need tuning based on new TCO components)
        """
        # Extract relevant metrics
        qir_rate = car_data.get('qir_rate') # Keep using qir_rate if available
        defect_rate = car_data.get('defect_rate') # Keep using defect_rate if available
        # Use the operational cost per km for scoring cost-effectiveness
        cost_per_km = car_data.get('tco', {}).get('cost_per_km', float('inf'))
        remaining_lifespan_km = car_data.get('tco', {}).get('remaining_lifespan_km', 0)

        # --- Scoring Logic (adjust as needed) ---

        # Skip if essential data missing or unreliable
        if qir_rate is None or qir_rate < 50: # Minimum acceptable QIR
             # print(f"Skipping score: Low/missing QIR for {car_data.get('make')} {car_data.get('model')}")
             return 0
        # Allow calculation even if defect rate is missing, but penalize high defect rates
        if defect_rate is not None and defect_rate > 20: # Maximum acceptable Defect Rate (adjust threshold)
             # print(f"Skipping score: High DefectRate for {car_data.get('make')} {car_data.get('model')}")
             return 0
        if cost_per_km == float('inf') or remaining_lifespan_km <= 0:
             # print(f"Skipping score: Invalid TCO for {car_data.get('make')} {car_data.get('model')}")
             return 0

        # Calculate individual scores (0-100 scale for each)
        qir_score = min(100, max(0, qir_rate)) # Higher QIRRate is better

        # Lower DefectRate is better (inverse scoring, handle None)
        # If defect_rate is None, maybe assign a neutral score (e.g., 50) or use an average? Let's assign 75 (slightly positive)
        defect_score = 100 - min(100, max(0, defect_rate * 5)) if defect_rate is not None else 75

        # Cost per km (lower is better)
        # Normalize: Assume good is < $0.30/km, bad is > $1.00/km (operational cost)
        cost_range = 1.00 - 0.30
        normalized_cost = max(0, min(1, (cost_per_km - 0.30) / cost_range))
        cost_score = 100 * (1 - normalized_cost)

        # Remaining lifespan (higher is better)
        # Normalize to 0-100 scale (0-300,000 km)
        lifespan_score = min(100, max(0, remaining_lifespan_km / (self.avg_vehicle_lifespan / 100))) # Normalize based on avg lifespan

        # Weights for different factors (Adjust these based on importance)
        weights = {
            'qir': 0.30,
            'defect': 0.15, # Reduced weight slightly as it might be None
            'cost': 0.35,   # Increased weight for cost-effectiveness
            'lifespan': 0.20
        }

        # Calculate weighted score
        weighted_score = (
            qir_score * weights['qir'] +
            defect_score * weights['defect'] +
            cost_score * weights['cost'] +
            lifespan_score * weights['lifespan']
        )

        # print(f"Scores for {car_data.get('make')} {car_data.get('model')}: QIR={qir_score:.1f}, Defect={defect_score:.1f}, Cost={cost_score:.1f}, Life={lifespan_score:.1f} -> Weighted: {weighted_score:.2f}")

        return round(weighted_score, 2)

    def process_car_listings(self, listings):
        """
        Process a list of car listings, calculate TCO and reliability scores.

        Args:
            listings (list): List of dictionaries, each representing a car

        Returns:
            pandas.DataFrame: DataFrame with processed data and scores
        """
        processed_data = []
        for car in listings:
            try:
                # Ensure essential fields exist and are convertible
                price = float(car.get('price', 0))
                mileage_str = car.get('mileage')
                mileage = float(mileage_str) if mileage_str else self.avg_vehicle_lifespan # Default high if missing
                make = car.get('make', 'unknown').lower()
                model = car.get('model', 'unknown').lower()
                year_str = car.get('year')
                year = int(year_str) if year_str else 0

                # Basic validation
                if not all([price > 0, year > 1980, # Basic sanity check for year
                             make != 'unknown', model != 'unknown',
                             isinstance(mileage, (int, float)) and mileage >= 0]):
                    # print(f"Skipping listing due to invalid basic info: {car.get('title', 'N/A')}")
                    continue

                # Get reliability scores
                qir_rate, defect_rate = self.get_reliability_scores(make, model, year)

                # Calculate TCO (returns None if it fails)
                tco_metrics = self.calculate_tco(price, make, model, year, mileage)

                # Store results only if TCO calculation was successful
                if tco_metrics is not None:
                    processed_car = car.copy() # Use original case from listing for output
                    processed_car['qir_rate'] = qir_rate
                    processed_car['defect_rate'] = defect_rate
                    processed_car['tco'] = tco_metrics # Keep TCO as a dict for now
                    processed_car['deal_score'] = self.calculate_deal_score(processed_car) # Calculate score based on processed data
                    processed_data.append(processed_car)
                # else:
                    # print(f"Skipping listing due to TCO calculation failure: {car.get('title', 'N/A')}")


            except (ValueError, TypeError) as e:
                print(f"Error processing car data type for: {car.get('title', 'N/A')} - {e}")
                continue
            except Exception as e:
                print(f"Unexpected error processing car: {car.get('title', 'N/A')} - {e}")
                continue

        # Convert to DataFrame
        if not processed_data:
             print("Warning: No listings could be processed.")
             return pd.DataFrame() # Return empty DataFrame if nothing was processed

        df = pd.DataFrame(processed_data)

        # Expand TCO dictionary into separate columns for easier analysis/sorting
        if 'tco' in df.columns:
            # Normalize the 'tco' column safely
            try:
                # Filter out rows where 'tco' is None or not a dict before normalizing
                valid_tco = df['tco'].apply(lambda x: isinstance(x, dict))
                if valid_tco.any():
                     tco_df = pd.json_normalize(df.loc[valid_tco, 'tco']).add_prefix('tco_')
                     # Align index for concatenation
                     tco_df.index = df[valid_tco].index
                     df = pd.concat([df.drop(columns=['tco']), tco_df], axis=1)
                else:
                     df = df.drop(columns=['tco']) # Drop if no valid TCO dicts exist
            except Exception as e:
                print(f"Error expanding TCO data: {e}")
                # Decide how to handle - maybe drop TCO column or leave as dict
                # For now, let's drop it if normalization fails
                if 'tco' in df.columns:
                    df = df.drop(columns=['tco'])


        return df

    def export_to_csv(self, df, output_path):
        """
        Export results to CSV.
        
        Args:
            df (pd.DataFrame): Results DataFrame
            output_path (str): Path to save CSV
        """
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Export to CSV
        df.to_csv(output_path, index=False)
        
        return output_path 