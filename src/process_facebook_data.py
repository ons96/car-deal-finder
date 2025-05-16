import csv
import re
import os

# Define file paths
facebook_csv_path = "car-deal-finder/data/facebook-2025-05-16.csv"
approved_vehicles_csv_path = "car-deal-finder/data/approved_vehicles_reliability.csv"
output_csv_path = "car-deal-finder/data/output.csv"

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
    with open(approved_vehicles_csv_path, mode='r', encoding='utf-8') as f_approved:
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
processed_listings = []

non_vehicle_keywords = [
    "parts", "wanted", "buy cars", "cash for cars", "scrap", "tires", "rims", "engine", 
    "transmission", "battery", "wrecking", "salvage", "repair", "service", "mechanic",
    "desk", "cowl", "enclosed mobility", "scooter", "trades or offer", "we buy cars", "cash cash cash",
    "any car and truck"
]


try:
    with open(facebook_csv_path, mode='r', encoding='utf-8') as f_facebook:
        reader = csv.reader(f_facebook)
        header_fb = next(reader) # Skip header
        # Based on user's info: 0:URL, 1:Img, 2:Price, 3:Title, 4:Location, 5:Mileage, 6:AltPrice(opt)
        url_idx, price_idx, title_idx, loc_idx, mileage_idx = 0, 2, 3, 4, 5
        alt_price_idx = 6 if len(header_fb) > 6 else -1 # Check if alt price col exists

        for row in reader:
            try:
                title_str = row[title_idx].strip() if len(row) > title_idx else None
                price_str = row[price_idx].strip() if len(row) > price_idx else None
                mileage_str = row[mileage_idx].strip() if len(row) > mileage_idx else None
                url = row[url_idx].strip() if len(row) > url_idx else None
                location = row[loc_idx].strip() if len(row) > loc_idx else None
                
                alt_price_str = None
                if alt_price_idx != -1 and len(row) > alt_price_idx and row[alt_price_idx].strip():
                    alt_price_str = row[alt_price_idx].strip()

                # 1. Filter out non-vehicle/junk listings by title
                if any(keyword in title_str.lower() for keyword in non_vehicle_keywords):
                    # print(f"Filtered (junk title): {title_str}")
                    continue

                # 2. Parse title for Year, Make, Model
                year, make, model = parse_title(title_str)
                if not year or not make or not model:
                    # print(f"Filtered (could not parse year/make/model): {title_str}")
                    continue
                
                # Normalize for comparison
                make_lower = make.lower()
                model_lower = model.lower()

                # 3. Filter against approved vehicles
                # Check various model string permutations for better matching
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
                    # print(f"Filtered (not approved): {year} {make} {model}")
                    continue

                # 4. Parse and convert mileage
                mileage_km = parse_mileage(mileage_str)
                if mileage_km is None and "enclosed mobility" not in title_str.lower() and "scooter" not in title_str.lower(): # some valid items have no mileage
                    # print(f"Filtered (bad mileage): {title_str} -> {mileage_str}")
                    # continue # Keep if mileage is missing, but flag it or handle later if needed.
                    mileage_km = -1 # Represent missing mileage

                # 5. Parse price, handle "Free" and low prices
                price = parse_price(price_str)
                if price is None and alt_price_str:
                    price = parse_price(alt_price_str)
                
                if price == 0: # "Free" listings
                    # print(f"Filtered (Free price): {title_str}")
                    continue
                
                if price is not None and price < 2000: 
                    # Heuristic: if car is relatively new-ish or low mileage, a price < $2000 is suspicious
                    # This is a rough filter. More sophisticated logic might be needed.
                    current_year_for_calc = 2024 # Assuming current year for age calculation
                    is_newish = year is not None and (current_year_for_calc - year) < 10 # Less than 10 years old
                    is_low_mileage = mileage_km is not None and mileage_km != -1 and mileage_km < 150000
                    if is_newish or is_low_mileage:
                        # print(f"Flagged (potentially monthly payment?): {price} for {year} {make} {model} with {mileage_km}km")
                        # For now, let's filter them out. Or could set price to -1 or a flag.
                        continue 

                if price is None:
                    # print(f"Filtered (bad/missing price): {title_str} -> {price_str}")
                    continue

                processed_listings.append([
                    year, make.strip(), model.strip(), price, 
                    int(mileage_km) if mileage_km is not None and mileage_km != -1 else '', # Store as int or empty
                    location, url, "Facebook Marketplace"
                ])
            except Exception as e:
                # print(f"Skipping row due to error: {row} -> {e}")
                continue
except FileNotFoundError:
    print(f"Error: Facebook CSV file not found at {facebook_csv_path}")
    # exit()
except Exception as e:
    print(f"Error processing Facebook CSV: {e}")
    # exit()

# --- Write to Output CSV --- #
if processed_listings:
    file_exists = os.path.isfile(output_csv_path)
    try:
        with open(output_csv_path, mode='a', newline='', encoding='utf-8') as f_output:
            writer = csv.writer(f_output)
            if not file_exists or os.path.getsize(output_csv_path) == 0:
                writer.writerow(['Year', 'Make', 'Model', 'Price', 'Mileage (km)', 'Location', 'URL', 'Source']) # Header
            writer.writerows(processed_listings)
        print(f"Successfully processed {len(processed_listings)} listings and appended to {output_csv_path}")
    except Exception as e:
        print(f"Error writing to output CSV: {e}")
else:
    print("No listings processed to write to output.")

if __name__ == '__main__':
    # You can add test calls here if you run the script directly
    # print(parse_title("2010 Honda civic"))
    # print(parse_mileage("200K km"))
    # print(parse_price("CA$1,234"))
    print("Script finished. If run directly, this does nothing other than define functions.")
    print(f"To process data, ensure '{facebook_csv_path}' and '{approved_vehicles_csv_path}' exist.")
    print(f"Output will be appended to '{output_csv_path}'.") 