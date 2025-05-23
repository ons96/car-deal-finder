"""Processors package for car deal finder."""

import pandas as pd
from pathlib import Path
import os

class ApprovedVehiclesProcessor:
    """Processor for handling approved vehicles data."""
    
    def __init__(self):
        """Initialize the processor with paths to data files."""
        self.project_root = Path(__file__).resolve().parent.parent.parent
        self.data_dir = self.project_root / "data"
        self.csv_path = self.data_dir / "approved_vehicles_reliability.csv"
        self.approved_vehicles = []
        self.approved_vehicles_by_make_model = set()
        self.unique_make_model_pairs = set()
    
    def load_approved_vehicles(self):
        """Load approved vehicles from CSV file."""
        if not self.csv_path.exists():
            print(f"Warning: Approved vehicles file not found at {self.csv_path}")
            return False
        
        try:
            df = pd.read_csv(self.csv_path)
            # Only include vehicles where Filter is TRUE
            df = df[df['Filter'] == True]
            self.approved_vehicles = df.to_dict('records')
            
            # Create a set of unique make/model pairs for quick lookup
            for vehicle in self.approved_vehicles:
                if 'Make_lc' in vehicle and 'Model_norm' in vehicle:
                    self.approved_vehicles_by_make_model.add(
                        (vehicle['Make_lc'], vehicle['Model_norm'])
                    )
            self.unique_make_model_pairs = set(f"{row['Make']} {row['Model']}" for row in self.approved_vehicles)
            print(f"Successfully loaded {len(self.approved_vehicles)} records from {self.csv_path} and created {len(self.unique_make_model_pairs)} unique make/model pairs for approval.")
            return True
        except Exception as e:
            print(f"Error loading approved vehicles: {e}")
            self.approved_vehicles = []
            self.approved_vehicles_by_make_model = set()
            return False
    
    def get_approved_vehicles_list(self):
        """Get list of approved vehicles in format expected by scrapers."""
        return [{"make": row['Make'], "model": row['Model']} for row in self.approved_vehicles] 