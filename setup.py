import os
import platform
import subprocess
import shutil
from pathlib import Path
import sys

# Define the expected relative path for data files from the project root
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
RELIABILITY_DATA_FILENAME = "chart_data_filtered.csv"
FUEL_DATA_FILENAME = "MY2015-2024 Fuel Consumption Ratings.csv"
FUEL_DATA_URL = "https://oee.nrcan.gc.ca/databases/fcr-rcf/MY2015-2024%20Fuel%20Consumption%20Ratings.csv"
ENV_TEMPLATE_FILENAME = "template.env"
ENV_FILENAME = ".env"
REQUIREMENTS_FILENAME = "requirements.txt"

def run_command(command, shell=False, check=True):
    """Helper to run a subprocess command and capture output."""
    try:
        print(f"Running command: {' '.join(command) if isinstance(command, list) else command}")
        process = subprocess.run(command, shell=shell, check=check, capture_output=True, text=True)
        if process.stdout:
            print(process.stdout)
        if process.stderr:
            print(f"Stderr: {process.stderr}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {' '.join(e.cmd) if isinstance(e.cmd, list) else e.cmd}")
        print(f"Return code: {e.returncode}")
        if e.stdout:
            print(f"Stdout: {e.stdout}")
        if e.stderr:
            print(f"Stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print(f"Error: Command '{command[0] if isinstance(command, list) else command.split()[0]}' not found. Please ensure it's installed and in your PATH.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False

def main():
    """Set up the car deal finder project environment and dependencies."""
    print("--- Starting Car Deal Finder Setup ---")

    # 1. Create necessary directories
    print("\nStep 1: Creating required directories...")
    for directory in [DATA_DIR, LOGS_DIR]:
        try:
            directory.mkdir(exist_ok=True)
            print(f"  Successfully ensured directory exists: {directory}")
        except Exception as e:
            print(f"  Error creating directory {directory}: {e}")
            print("  Please check your permissions or create it manually.")
            # Decide if this is a fatal error or if we can continue
    
    # 2. Create .env file from template if it doesn't exist
    print(f"\nStep 2: Checking for {ENV_FILENAME} file...")
    env_file = PROJECT_ROOT / ENV_FILENAME
    template_file = PROJECT_ROOT / ENV_TEMPLATE_FILENAME
    if not env_file.exists():
        if template_file.exists():
            try:
                shutil.copy(template_file, env_file)
                print(f"  Successfully created {ENV_FILENAME} from {ENV_TEMPLATE_FILENAME}.")
                print(f"  ACTION REQUIRED: Please open '{ENV_FILENAME}' and add your Facebook credentials if you plan to scrape Facebook Marketplace.")
            except Exception as e:
                print(f"  Error copying {template_file} to {env_file}: {e}")
        else:
            print(f"  Warning: {ENV_TEMPLATE_FILENAME} not found. Cannot create {ENV_FILENAME} automatically.")
            print(f"  If you need to scrape Facebook Marketplace, please create a '{ENV_FILENAME}' file manually with 'FACEBOOK_EMAIL=your_email' and 'FACEBOOK_PASSWORD=your_password'.")
    else:
        print(f"  {ENV_FILENAME} already exists. Skipping creation.")

    # 3. Check for reliability data
    print(f"\nStep 3: Checking for reliability data ({RELIABILITY_DATA_FILENAME})...")
    reliability_file_path = DATA_DIR / RELIABILITY_DATA_FILENAME
    if reliability_file_path.exists() and reliability_file_path.is_file():
        print(f"  Found: {reliability_file_path}")
    else:
        print(f"  ERROR: Reliability data file '{RELIABILITY_DATA_FILENAME}' not found in '{DATA_DIR}'.")
        print("  This file is essential for the application and should be part of the repository.")
        print("  Please ensure you have a complete clone of the project or restore this file.")
        # This is a critical file, could exit here if desired
        # sys.exit("Setup aborted: Missing critical reliability data file.")

    # 4. Check for fuel consumption data (and download if missing)
    print(f"\nStep 4: Checking for fuel consumption data ({FUEL_DATA_FILENAME})...")
    fuel_file_path = DATA_DIR / FUEL_DATA_FILENAME
    if fuel_file_path.exists() and fuel_file_path.is_file():
        print(f"  Found: {fuel_file_path}")
    else:
        print(f"  Fuel data file '{FUEL_DATA_FILENAME}' not found in '{DATA_DIR}'. Attempting to download...")
        download_success = False
        # Try with curl
        if platform.system() == "Windows":
            # On Windows, curl might need to be invoked via cmd if not directly in PowerShell path for subprocess easily
            # Using '-o' for output file with curl
            cmd_curl = ['curl', '-L', FUEL_DATA_URL, '-o', str(fuel_file_path)]
        else:
            cmd_curl = ['curl', '-L', FUEL_DATA_URL, '-o', str(fuel_file_path)]

        print(f"  Attempting download with: {' '.join(cmd_curl)}")
        if run_command(cmd_curl, shell=False, check=False): # check=False to handle errors manually
            if fuel_file_path.exists() and fuel_file_path.stat().st_size > 100: # Basic check for non-empty file
                 print(f"  Successfully downloaded {FUEL_DATA_FILENAME} to {DATA_DIR}")
                 download_success = True
            else:
                 print(f"  Download command seemed to run, but {FUEL_DATA_FILENAME} is missing or empty in {DATA_DIR}.")
                 if fuel_file_path.exists(): fuel_file_path.unlink(missing_ok=True) # Clean up empty file
        
        if not download_success:
            print(f"  ERROR: Failed to download {FUEL_DATA_FILENAME}.")
            print(f"  You may need to download it manually from '{FUEL_DATA_URL}'")
            print(f"  and place it in the '{DATA_DIR}' directory.")
            # This is also critical, could exit
            # sys.exit("Setup aborted: Missing critical fuel data file.")

    # 5. Install dependencies
    print("\nStep 5: Installing Python dependencies...")
    print("  IMPORTANT: This script assumes you have ALREADY created and ACTIVATED a Python virtual environment.")
    print(f"  Dependencies will be installed from '{REQUIREMENTS_FILENAME}'.")
    
    requirements_path = PROJECT_ROOT / REQUIREMENTS_FILENAME
    if not requirements_path.exists():
        print(f"  ERROR: '{REQUIREMENTS_FILENAME}' not found in the project root ({PROJECT_ROOT}).")
        print("  Cannot install dependencies. Please ensure the file exists.")
        # sys.exit("Setup aborted: Missing requirements.txt file.")
    else:
        has_uv = False
        try:
            # Check if uv is installed (more robust check)
            uv_check_process = subprocess.run(["uv", "--version"], capture_output=True, text=True, check=True)
            print(f"  Found uv version: {uv_check_process.stdout.strip()}")
            has_uv = True
        except (subprocess.SubprocessError, FileNotFoundError):
            print("  uv (Python package installer) not found or not in PATH.")
            has_uv = False
        
        install_command = []
        if has_uv:
            print("  Attempting to install dependencies with uv (typically faster)...")
            install_command = ["uv", "pip", "install", "-r", str(requirements_path)]
        else:
            print("  uv not found. Attempting to install dependencies with pip...")
            # Construct pip path relative to current Python interpreter to ensure it's from the venv
            python_executable = sys.executable
            pip_executable = str(Path(python_executable).parent / 'pip')
            if platform.system() == "Windows":
                 pip_executable = str(Path(python_executable).parent / 'pip.exe')

            install_command = [pip_executable, "install", "-r", str(requirements_path)]

        if run_command(install_command, shell=False):
            print("  Python dependencies installed successfully.")
        else:
            print("  ERROR: Failed to install Python dependencies.")
            print(f"  Please try installing them manually after activating your virtual environment: {' '.join(install_command)}")
            # sys.exit("Setup aborted: Failed to install dependencies.")

    print("\n--- Car Deal Finder Setup Complete! ---")
    print("\nNext Steps:")
    print("1. If you haven't already, ensure your virtual environment is activated.")
    print(f"2. If you plan to scrape Facebook, ensure your credentials are in the '{ENV_FILENAME}' file.")
    print("3. To run the application, use: python src/main.py")
    print("   For more options and details, please see README.md")

if __name__ == "__main__":
    main() 