# Car Deal Finder

A Python application that scrapes used car listings from various websites and ranks them based on value, reliability, and total cost of ownership.

## Features

- Scrapes used car listings from multiple sources:
  - Facebook Marketplace
  - CarGurus.ca
  - AutoTrader.ca
  - (and potentially others)
- Filters cars based on reliability metrics (QIRRate and DefectRate)
- Calculates an estimated Total Cost of Ownership (TCO) including fuel, maintenance, insurance, and depreciation.
- Ranks deals by overall value score.
- Exports results to CSV for easy analysis
- Multiple scraping methods supported (AI-powered, async, or traditional)

## Prerequisites

- Python 3.8+
- An internet connection.
- `curl` command-line utility (for automatically downloading fuel data if missing). Usually pre-installed on Linux/macOS. Windows users might need to install it or ensure it's in their PATH, or download the fuel data manually if setup fails at that step.

## Project Setup

Follow these steps to get the Car Deal Finder up and running:

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/ons96/car-deal-finder.git
    cd car-deal-finder
    ```

2.  **Create and Activate a Virtual Environment:**
    It's highly recommended to use a virtual environment to manage project dependencies. This isolates the project's packages from your global Python installation.
    ```bash
    # Using Python's built-in venv module
    python -m venv .venv 
    ```
    Then activate it:
    -   On Windows (Command Prompt/PowerShell):
        ```bash
        .venv\Scripts\activate
        ```
    -   On macOS and Linux (bash/zsh):
        ```bash
        source .venv/bin/activate
        ```
    *Alternatively, if you use `uv` (a fast Python package installer and resolver):*
    ```bash
    uv venv  # This creates and can also activate the venv, or use the activate scripts above.
    # Ensure it's activated before proceeding.
    ```

3.  **Run the Setup Script:**
    Once your virtual environment is activated, run the `setup.py` script. This will:
    - Create necessary directories (`data/`, `logs/`).
    - Copy `template.env` to `.env` if it doesn't exist (for your credentials).
    - Check for required data files. It will attempt to download the fuel consumption data if it's missing.
    - Install all required Python packages from `requirements.txt` into your active virtual environment using `uv` (if available) or `pip`.
    ```bash
    python setup.py
    ```
    Carefully read any messages produced by the setup script, especially if it reports errors or missing files.

4.  **Configure Credentials (Optional but Recommended for Facebook):**
    Open the `.env` file (created in the previous step) in a text editor. If you plan to scrape Facebook Marketplace, enter your Facebook email and password:
    ```env
    FACEBOOK_EMAIL=your_facebook_email@example.com
    FACEBOOK_PASSWORD=your_facebook_password
    ```
    Saving this file will allow the scrapers that require login (like some Facebook methods) to function correctly. The `.env` file is ignored by Git, so your credentials will remain local.

## Running the Application

Once setup is complete and your virtual environment is active, you can run the main application:

```bash
python src/main.py
```
This will use the default settings (Crawl4AI scraping method, all sites, default listing limits) to find and analyze car deals. Results will be saved to `data/output.csv` by default.

### Command-Line Options

You can customize the scraping process using various command-line arguments:

```bash
python src/main.py --method [crawl4ai|pyppeteer|selenium] --sites [autotrader,cargurus,facebook] --limit 100 --output data/my_custom_results.csv
```

-   `--method`: Choose the scraping method. 
    -   `crawl4ai` (Default): AI-powered, generally robust.
    -   `pyppeteer`: Asynchronous, good for speed and modern sites.
    -   `selenium`: Traditional browser automation, can be more stable for complex JavaScript sites but might be slower.
-   `--sites`: Comma-separated list of sites to scrape (e.g., `autotrader,cargurus,facebook`). Defaults to all supported sites.
-   `--limit`: Maximum number of listings to attempt to scrape per site (e.g., `50`). Default is `100`.
-   `--output`: Specify the path for the output CSV file (e.g., `data/custom_output.csv`). Default is `data/output.csv`.

*(Refer to the old README section for details on `--config` if you re-implement that)*

## Data Files

The `data/` directory contains essential input files:
- `chart_data_filtered.csv`: Vehicle reliability statistics.
- `MY2015-2024 Fuel Consumption Ratings.csv`: Fuel consumption data from NRCan.

These files are included in the repository. The setup script will attempt to download the fuel consumption data if it's missing.
Output CSVs (like `output.csv`) will also be saved in the `data/` directory.

## `logs/` Directory

A `logs/` directory will be created by the `setup.py` script. While not currently used extensively by the application for runtime logging, it's available if logging features are expanded.

## Scraping Methods Overview

| Method      | Description                          | Pros/Cons                                  |
|-------------|--------------------------------------|--------------------------------------------|
| `crawl4ai`  | **DEFAULT** AI-powered web scraping  | Best reliability, no API keys needed.      |
| `pyppeteer` | Async Puppeteer-based scraping       | Fast, good for modern sites.               |
| `selenium`  | Traditional browser automation       | Widely supported, can handle complex JS.   |
| `scrapinggraph` | External AI service (Not fully implemented) | Potentially powerful but requires API key. |

## Configuration

The application filters cars based on:
- QIRRate >= 80 (higher is better)
- DefectRate <= 15 (lower is better)
- Preference for sedans, coupes, and select hatchbacks

## License

MIT

## Running in the Cloud (No Local Installation)

If you have limited local resources, don't want to install software, or prefer a cloud-based environment, here are some free options to run this project:

**General Workflow for Cloud IDEs:**
1.  Open the project in the chosen cloud environment (usually by importing the GitHub repository).
2.  Open a terminal within the cloud IDE.
3.  Follow the "Project Setup" steps:
    *   Create and activate a virtual environment (`python -m venv .venv` then activate it).
    *   Run `python setup.py` to install dependencies and prepare files.
    *   Configure `.env` if needed (especially for Facebook scraping).
4.  Run the application: `python src/main.py --method crawl4ai --limit 50` (adjust arguments as needed). The `crawl4ai` method is generally recommended for free tiers due to potentially lower resource usage.
5.  Download your output CSV from the cloud environment's file explorer.

**Recommended Free Platforms:**

1.  **GitHub Codespaces:**
    *   **Details:** Provides a full VS Code experience in your browser, directly integrated with this GitHub repository. Offers a generous free tier of core-hours per month for personal accounts.
    *   **Setup:**
        1.  Go to the main page of this GitHub repository (`https://github.com/ons96/car-deal-finder`).
        2.  Click the green `<> Code` button.
        3.  Go to the "Codespaces" tab.
        4.  Click "Create codespace on master" (or your main branch).
        5.  Once loaded, use the integrated terminal to follow the general cloud workflow above.

2.  **Gitpod:**
    *   **Details:** Similar to Codespaces, offering a VS Code-like cloud IDE with a free tier.
    *   **Setup:**
        1.  Prefix the GitHub repository URL in your browser: `gitpod.io#https://github.com/ons96/car-deal-finder`
        2.  Log in with your GitHub account.
        3.  Once the workspace loads, use the integrated terminal and follow the general cloud workflow.

3.  **Google Cloud Shell:**
    *   **Details:** Provides a free Linux virtual machine instance with 5GB of persistent home directory and full terminal access. Python is usually pre-installed.
    *   **Setup:**
        1.  Go to [https://shell.cloud.google.com/](https://shell.cloud.google.com/) and log in with your Google account.
        2.  In the terminal, clone the repository: `git clone https://github.com/ons96/car-deal-finder.git`
        3.  `cd car-deal-finder`
        4.  Follow the general cloud workflow (create venv, activate, `python setup.py`, run `main.py`).

4.  **Kaggle Kernels (Notebooks):**
    *   **Details:** Offers free access to powerful compute resources (CPU, sometimes GPU) in a Jupyter Notebook environment. Good for running Python scripts.
    *   **Setup:**
        1.  Go to [Kaggle](https://www.kaggle.com/) and sign in/create an account.
        2.  Create a new Notebook.
        3.  In a code cell, clone the repository: `!git clone https://github.com/ons96/car-deal-finder.git`
        4.  Change directory: `!cd car-deal-finder && ...` (subsequent commands need to be aware of this path or run with `!cd car-deal-finder && python setup.py`).
        5.  Run setup: `!cd car-deal-finder && python -m venv .venv && source .venv/bin/activate && python setup.py` (you might need to adapt venv activation for notebooks or install directly).
        6.  Run the main script: `!cd car-deal-finder && .venv/bin/python src/main.py --method crawl4ai --limit 30`
        7.  You can manage files and download outputs from the Kaggle interface.

5.  **Replit:**
    *   **Details:** Easy-to-use online IDE. Import from GitHub, and it tries to set up the environment.
    *   **Setup:**
        1.  Go to [Replit](https://replit.com/) and sign in/create an account.
        2.  Click "Create Repl" or "Import Repo" and provide the GitHub URL: `https://github.com/ons96/car-deal-finder`.
        3.  Once imported, use the "Shell" tab for terminal commands.
        4.  You might need to ensure a virtual environment is used or packages are installed correctly (Replit sometimes auto-detects `requirements.txt`). Run `python setup.py` to be sure.
        5.  Run the application: `python src/main.py --method crawl4ai --limit 30`.

6.  **Alwaysdata.com:**
    *   **Details:** Offers a free tier with SSH access, Python support, and a small amount of storage.
    *   **Setup:**
        1.  Sign up for a free account at [Alwaysdata](https://www.alwaysdata.com/).
        2.  Use their file manager or SSH to upload/clone the repository.
        3.  Connect via SSH.
        4.  Follow the general cloud workflow (create venv, activate, `python setup.py`, run `main.py`).
    *   **Note:** Free tier resources (CPU, RAM, storage) are limited.

**Important Considerations for Cloud Environments:**
*   **Resource Limits:** Free tiers always have limitations on CPU, RAM, storage, and runtime. For intensive scraping (especially with Selenium/Pyppeteer), you might hit these limits. Start with smaller `--limit` values.
*   **Browser Automation Drivers:** If you intend to use `selenium` or `pyppeteer` methods, you might need to install the appropriate web browser (e.g., Chrome/Chromium) and its corresponding driver (e.g., chromedriver) in the cloud environment's terminal if they are not pre-installed. This can sometimes be tricky. The `crawl4ai` method is generally preferred for these environments as it doesn't rely on local browser automation.
*   **Output Files:** Ensure you know how to access and download any generated CSV files from the cloud environment. 