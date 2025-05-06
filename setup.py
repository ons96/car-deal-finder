import os
import platform
import subprocess
import shutil
from pathlib import Path

def main():
    """Set up the car deal finder project."""
    print("Setting up Car Deal Finder...")
    
    # Create necessary directories
    for directory in ["data", "logs"]:
        Path(directory).mkdir(exist_ok=True)
        print(f"Created directory: {directory}")
    
    # Create .env file from template if it doesn't exist
    env_file = Path(".env")
    if not env_file.exists():
        template_file = Path("template.env")
        if template_file.exists():
            shutil.copy(template_file, env_file)
            print("Created .env file from template")
        else:
            print("Warning: template.env not found. Please create .env manually.")
    
    # Check for reliability data
    reliability_file = Path("data/chart_data_filtered.csv")
    if not reliability_file.exists():
        source_path = Path(os.path.expanduser("~/Coding Projects/dashboard-light_scraper/chart_data_filtered.csv"))
        if source_path.exists():
            shutil.copy(source_path, reliability_file)
            print(f"Copied reliability data to {reliability_file}")
        else:
            print(f"Warning: Reliability data not found at {source_path}")
            print("You'll need to provide this file manually in the data directory")
    
    # Install dependencies
    try:
        # Check if uv is installed
        try:
            subprocess.run(["uv", "--version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            has_uv = True
        except (subprocess.SubprocessError, FileNotFoundError):
            has_uv = False
        
        if has_uv:
            print("Installing dependencies with uv (faster)...")
            subprocess.run(["uv", "pip", "install", "-r", "requirements.txt"], check=True)
        else:
            print("Installing dependencies with pip...")
            subprocess.run(["pip", "install", "-r", "requirements.txt"], check=True)
        
        print("Dependencies installed successfully")
    except Exception as e:
        print(f"Error installing dependencies: {e}")
        print("Please install dependencies manually with: pip install -r requirements.txt")
    
    print("\nSetup complete! Run the program with: python src/main.py")
    print("For more options, see README.md")

if __name__ == "__main__":
    main() 