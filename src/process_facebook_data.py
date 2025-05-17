import csv
import re
import os
import datetime

# Get the absolute path of the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define file paths relative to the script's directory
# facebook_csv_path = os.path.join(script_dir, "../data/facebook-2025-05-16.csv") # No longer needed here
approved_vehicles_csv_path = os.path.join(script_dir, "../data/approved_vehicles_reliability.csv")
output_csv_path = os.path.join(script_dir, "../data/output.csv") # This might also be part of a different workflow now

# --- Helper Functions ---
def parse_title(title_str):
    if not title_str or not isinstance(title_str, str):
        return None, None, None
    
    # Regex to find a 4-digit year
    year_match = re.search(r'\b(\d{4})\b', title_str)
    year = int(year_match.group(1)) if year_match else None
    
    # Remaining string after attempting to extract year
    remaining_title = title_str.replace(year_match.group(0), '').strip() if year_match else title_str
    
    # Tentative: Split the rest by space, assume first word is Make, rest is Model
    # This is a simplification and might need refinement
    parts = remaining_title.split()
    make = None
    model = None

    if not year: # If no year, very hard to determine make/model reliably from title alone
        # try to see if first word is a common make even without year
        # This helps with titles like "Honda Civic" without a year explicitly stated
        potential_makes = [
            "Acura", "Audi", "BMW", "Buick", "Cadillac", "Chevrolet", "Chrysler", "Dodge", "Ford", "GMC", 
            "Honda", "Hyundai", "Infiniti", "Jaguar", "Jeep", "Kia", "Land Rover", "Lexus", "Lincoln", 
            "Mazda", "Mercedes-Benz", "MINI", "Mitsubishi", "Nissan", "Porsche", "Ram", "Scion", 
            "Subaru", "Suzuki", "Tesla", "Toyota", "Volkswagen", "Volvo"
        ]
        if parts and parts[0] in potential_makes:
            make = parts[0]
            model = " ".join(parts[1:]) if len(parts) > 1 else None
        elif len(parts) >= 2: # if not a known make, but has at least two words
             # take first word as make and second as model
            make = parts[0]
            model = parts[1]

    elif parts:
        # Common makes to help with parsing
        # Order matters for multi-word makes (e.g., "Land Rover" before "Rover")
        known_makes_map = {
            "Land Rover": "Land Rover",
            "Mercedes-Benz": "Mercedes-Benz",
            # Single word makes
            "Acura": "Acura", "Alfa Romeo": "Alfa Romeo", "Aston Martin": "Aston Martin",
            "Audi": "Audi", "Bentley": "Bentley", "BMW": "BMW", "Buick": "Buick",
            "Cadillac": "Cadillac", "Chevrolet": "Chevrolet", "Chrysler": "Chrysler",
            "Dodge": "Dodge", "Ferrari": "Ferrari", "Fiat": "Fiat", "Ford": "Ford",
            "Genesis": "Genesis", "GMC": "GMC", "Honda": "Honda", "Hyundai": "Hyundai",
            "Infiniti": "Infiniti", "Jaguar": "Jaguar", "Jeep": "Jeep", "Kia": "Kia",
            "Lamborghini": "Lamborghini", "Lexus": "Lexus", "Lincoln": "Lincoln",
            "Lotus": "Lotus", "Maserati": "Maserati", "Mazda": "Mazda",
            "McLaren": "McLaren", "MINI": "MINI", "Mitsubishi": "Mitsubishi",
            "Nissan": "Nissan", "Polestar": "Polestar", "Pontiac": "Pontiac", "Porsche": "Porsche",
            "Ram": "Ram", "Rivian": "Rivian", "Rolls-Royce": "Rolls-Royce", "Saab": "Saab",
            "Saturn": "Saturn", "Scion": "Scion", "Smart": "Smart", "Subaru": "Subaru",
            "Suzuki": "Suzuki", "Tesla": "Tesla", "Toyota": "Toyota",
            "Volkswagen": "Volkswagen", "Volvo": "Volvo"
        }

        # Check for two-word makes first
        if len(parts) >= 2 and f"{parts[0]} {parts[1]}" in known_makes_map:
            make = known_makes_map[f"{parts[0]} {parts[1]}"]
            model = " ".join(parts[2:])
        elif parts[0] in known_makes_map:
            make = known_makes_map[parts[0]]
            model = " ".join(parts[1:])
        else: # If make not in known_makes, assume first word is make, rest is model
            make = parts[0]
            model = " ".join(parts[1:])

    if model == "": model = None # Handle cases where model becomes empty string

    return year, make, model

def parse_price(price_str):
    if not price_str or not isinstance(price_str, str):
        return None
    price_str = price_str.lower()
    if "free" in price_str:
        return 0 # Or handle as a special case, like None or -1
    # Remove "CA$", "$", commas, and any other non-numeric characters except decimal point
    cleaned_price = re.sub(r'[^\d.]', '', price_str)
    try:
        return float(cleaned_price)
    except ValueError:
        return None

def parse_mileage(mileage_str):
    if not mileage_str or not isinstance(mileage_str, str):
        return None
    mileage_str = mileage_str.lower().replace(',', '') # Remove commas
    
    km_match = re.search(r'(\d+\.?\d*|\d+)\s*k\s*km', mileage_str) # e.g., 100k km, 100 k km, 100.5k km
    if km_match:
        return float(km_match.group(1)) * 1000
        
    km_direct_match = re.search(r'(\d+\.?\d*|\d+)\s*km', mileage_str) # e.g. 100km, 100 km, 100000 km
    if km_direct_match:
        val = float(km_direct_match.group(1))
        # if it looks like it was already in full numbers (e.g. > 3000), don't multiply by 1000
        return val if val > 3000 else val * 1000 

    miles_match = re.search(r'(\d+\.?\d*|\d+)\s*k\s*miles', mileage_str) # e.g. 100k miles
    if miles_match:
        return float(miles_match.group(1)) * 1000 * 1.60934
        
    miles_direct_match = re.search(r'(\d+\.?\d*|\d+)\s*miles', mileage_str) # e.g. 100 miles, 100000 miles
    if miles_direct_match:
        val = float(miles_direct_match.group(1))
        val_km = val if val > 3000 else val * 1000 # if it looks like it was already in full numbers, don't multiply by 1000
        return val_km * 1.60934

    # Match numbers that might just be mileage without units, assume km if no other unit found
    # Be careful with this, as it might pick up other numbers if mileage format is very messy.
    # This should be a last resort.
    just_number_match = re.search(r'^(\d+\.?\d*|\d+)$', mileage_str) # e.g. 150000
    if just_number_match and just_number_match.group(1):
        val = float(just_number_match.group(1))
        # Heuristic: if it's a large number, assume it's already in km or miles (convert if needed)
        # if it's small (e.g. < 500), assume it was "k" that was missed.
        if val < 500: # e.g., "150" likely meant 150k
             return val * 1000 # Assume km
        return val # Assume km

    return None

# --- Load Approved Vehicles --- (Make, Model, Year)
approved_vehicles = set()
try:
    with open(approved_vehicles_csv_path, mode='r', encoding='utf-8-sig') as f_approved:
        reader = csv.reader(f_approved)
        header_approved = next(reader) # Skip header
        make_col_idx = header_approved.index('Make')
        model_col_idx = header_approved.index('Model')
        year_col_idx = header_approved.index('Year')
        for row in reader:
            try:
                make = row[make_col_idx].strip().lower()
                model = row[model_col_idx].strip().lower()
                year = int(row[year_col_idx].strip())
                approved_vehicles.add((make, model, year))
                # Add variations for model (e.g. mazda3 vs mazda 3)
                approved_vehicles.add((make, model.replace(" ", ""), year))
                approved_vehicles.add((make, model.replace("-", ""), year))
                approved_vehicles.add((make, model.replace(" ", "-"), year))

            except (ValueError, IndexError):
                # print(f"Skipping bad row in approved_vehicles: {row}")
                continue
except FileNotFoundError:
    print(f"Error: Approved vehicles file not found at {approved_vehicles_csv_path}")
    # exit()
except Exception as e:
    print(f"Error reading approved_vehicles: {e}")
    # exit()

# --- Process Facebook Data --- #
# processed_listings = [] # This will be initialized in the function

non_vehicle_keywords = [
    "parts", "wanted", "buy cars", "cash for cars", "scrap", "tires", "rims", "engine", 
    "transmission", "battery", "wrecking", "salvage", "repair", "service", "mechanic",
    "desk", "cowl", "enclosed mobility", "scooter", "trades or offer", "we buy cars", "cash cash cash",
    "any car and truck"
]

# def get_parsed_facebook_listings(): # Old function name
def parse_facebook_csv(input_csv_path): # New function name and parameter
    local_processed_listings = [] # Use a local list
    try:
        # with open(facebook_csv_path, mode='r', encoding='utf-8') as f_facebook: # Old hardcoded path
        with open(input_csv_path, mode='r', encoding='utf-8') as f_facebook: # Use function argument
            reader = csv.reader(f_facebook)
            header_fb = next(reader) # Skip header
            # Attempt to dynamically find column indices, default if not found or error
            try:
                url_idx = header_fb.index('Link') # Common name for URL
                price_idx = header_fb.index('Price') 
                title_idx = header_fb.index('Title')
                loc_idx = header_fb.index('Location')
                mileage_idx = header_fb.index('Mileage')
                # For alternative price, check if 'Alternate Price' or similar exists
                alt_price_idx = header_fb.index('Alternate Price') if 'Alternate Price' in header_fb else -1 
            except ValueError:
                # Fallback to default indices if specific headers are not found
                print("Warning: Could not find all expected headers (Link, Price, Title, Location, Mileage). Using default column indices [0,2,3,4,5]. This may lead to incorrect parsing.")
                url_idx, price_idx, title_idx, loc_idx, mileage_idx = 0, 2, 3, 4, 5
                alt_price_idx = 6 if len(header_fb) > 6 else -1


            for row_num, row in enumerate(reader):
                try:
                    # Defensive access to row elements
                    title_str = row[title_idx].strip() if len(row) > title_idx and row[title_idx] else None
                    price_str = row[price_idx].strip() if len(row) > price_idx and row[price_idx] else None
                    mileage_str = row[mileage_idx].strip() if len(row) > mileage_idx and row[mileage_idx] else None
                    url = row[url_idx].strip() if len(row) > url_idx and row[url_idx] else None
                    location = row[loc_idx].strip() if len(row) > loc_idx and row[loc_idx] else None
                    
                    alt_price_str = None
                    if alt_price_idx != -1 and len(row) > alt_price_idx and row[alt_price_idx] and row[alt_price_idx].strip():
                        alt_price_str = row[alt_price_idx].strip()

                    if not title_str: 
                        continue

                    if any(keyword in title_str.lower() for keyword in non_vehicle_keywords):
                        continue

                    year, make, model = parse_title(title_str)
                    if not year or not make or not model:
                        continue
                    
                    make_lower = make.lower()
                    model_lower = model.lower()

                    approved_key = (make_lower, model_lower, year)
                    approved_key_no_space = (make_lower, model_lower.replace(" ", ""), year)
                    approved_key_no_dash = (make_lower, model_lower.replace("-", ""), year)
                    approved_key_dash_instead_of_space = (make_lower, model_lower.replace(" ", "-"), year)
                    
                    if not (
                        approved_key in approved_vehicles or 
                        approved_key_no_space in approved_vehicles or 
                        approved_key_no_dash in approved_vehicles or
                        approved_key_dash_instead_of_space in approved_vehicles
                    ):
                        continue

                    mileage_km = parse_mileage(mileage_str)
                    if mileage_km is None and "enclosed mobility" not in title_str.lower() and "scooter" not in title_str.lower():
                        mileage_km = -1 
                    elif mileage_km is not None:
                        mileage_km = int(mileage_km)

                    price = parse_price(price_str)
                    if price is None and alt_price_str:
                        price = parse_price(alt_price_str)
                    
                    if price == 0: # Skip free listings
                        continue
                    
                    # Stricter filter for very cheap vehicles that are likely not cars or are problematic
                    if price is not None and price < 1000: # Increased threshold slightly
                        current_year_for_calc = datetime.datetime.now().year 
                        # If it's very new (e.g. < 5 years old) and < $1000, it's suspicious
                        is_suspiciously_new = year is not None and (current_year_for_calc - year) < 5
                        # If it's very low mileage (e.g. < 50000km) and < $1000, also suspicious
                        is_suspiciously_low_mileage = mileage_km is not None and mileage_km != -1 and mileage_km < 50000
                        
                        if is_suspiciously_new or is_suspiciously_low_mileage:
                            # print(f"Skipping suspiciously cheap listing: {title_str} Price: {price} Year: {year} Mileage: {mileage_km}")
                            continue 

                    if price is None: # Skip if price couldn't be parsed at all
                        continue
                    
                    # Ensure values are appropriate before adding
                    # Example: ensure year is plausible, mileage isn't excessively high for the price etc.
                    # For now, this is basic, can be expanded.
                    if year and year < 1980: # Skip very old cars unless specifically desired
                        continue

                    listing_details = {
                        "title": title_str,
                        "price": price,
                        "year": year,
                        "make": make,
                        "model": model,
                        "mileage": mileage_km if mileage_km is not None else -1, # Use -1 for unknown after filtering
                        "location": location,
                        "url": url,
                        "source_file": os.path.basename(input_csv_path), # Add source file
                        "scraped_date": datetime.date.today().isoformat() # Add scraped date
                    }
                    local_processed_listings.append(listing_details)
                
                except Exception as e:
                    # print(f"Error processing row {row_num+2} in {os.path.basename(input_csv_path)}: {row}. Error: {e}")
                    continue
        
    except FileNotFoundError:
        print(f"Error: Facebook data file not found at {input_csv_path}")
    except Exception as e:
        print(f"Error reading or processing Facebook data from {input_csv_path}: {e}")
        
    return local_processed_listings

# It's generally good practice not to have executable code at the module level
# like direct calls to process data or print statements unless it's for specific debugging
# or if this script is intended to be run standalone for a specific purpose.
# The main_orchestrator.py should be the one calling parse_facebook_csv.

# Example of how it might be called (for testing, normally not here):
# if __name__ == '__main__':
#     # This is just an example, provide a real path for testing
#     test_csv_path = os.path.join(script_dir, \"../data/facebook-2025-05-16.csv\") 
#     if os.path.exists(test_csv_path):
#         listings = parse_facebook_csv(test_csv_path)
#         print(f"Processed {len(listings)} listings from {test_csv_path}:")
#         for listing in listings[:5]: # Print first 5
#             print(listing)
#     else:
#         print(f"Test CSV {test_csv_path} not found.")


if __name__ == '__main__':
    print("Starting Facebook data processing...")
    # Check if dependent files exist before processing
    # if not os.path.exists(facebook_csv_path):
    #     print(f"CRITICAL: Facebook input file not found: {facebook_csv_path}")
    if not os.path.exists(approved_vehicles_csv_path):
        print(f"CRITICAL: Approved vehicles file not found: {approved_vehicles_csv_path}")
    else:
        print(f"Loading approved vehicles from: {approved_vehicles_csv_path}")
        # Note: approved_vehicles set is loaded globally when script is imported/run.
        # If this script is only run as main, the global loading is fine.
        # If imported, ensure approved_vehicles is loaded before get_parsed_facebook_listings is called.
        if not approved_vehicles: # Check if it was loaded successfully
            print("Warning: Approved vehicles set is empty. Filtering by approval might not work as expected.")

        # facebook_listings = get_parsed_facebook_listings()
        # if facebook_listings:
        #     print(f"Successfully parsed {len(facebook_listings)} Facebook listings.")
        #     # For testing, print the first few listings:
        #     # for i, item in enumerate(facebook_listings[:3]):
        #     #     print(f"Listing {i+1}: {item}")
        # else:
        #     print("No Facebook listings were parsed.")

    print("Facebook processing script finished.")
    print(f"To use this data, call parse_facebook_csv() from another script.")
    print(f"Ensure '{os.path.basename(approved_vehicles_csv_path)}' is in the '../data/' directory relative to this script for it to function.") 