# Car Deal Finder

A Python application that scrapes used car listings from various websites and ranks them based on value, reliability, and total cost of ownership.

## Features

- Scrapes used car listings from multiple sources:
  - Facebook Marketplace
  - CarGurus.ca
  - AutoTrader.ca
  - (and potentially others)
- Filters cars based on reliability metrics (QIRRate and DefectRate)
- Calculates total cost of ownership
- Ranks deals by overall value
- Exports results to CSV for easy analysis
- Multiple scraping methods supported (AI-powered, async, or traditional)

## Prerequisites

- Python 3.8+
- Internet connection

## Installation

1. Clone the repository
2. Install dependencies using uv:
   ```
   uv venv
   uv pip install -r requirements.txt
   ```
3. Copy `template.env` to `.env` and add optional Facebook credentials for better results

## Usage

### Basic usage
```
python src/main.py
```

This will use Crawl4AI (default, best method) to scrape all sites and process the data.

### Advanced options

```
python src/main.py --method [crawl4ai|pyppeteer|selenium] --sites [autotrader,cargurus,facebook] --limit 200
```

#### Parameters:
- `--method`: Choose the scraping method (default: crawl4ai)
- `--sites`: Comma-separated list of sites to scrape (default: all)
- `--limit`: Maximum listings to scrape per site (default: 100)
- `--output`: Path to save results (default: data/output.csv)

## Scraping Methods

| Method      | Description                          | Pros/Cons                        |
|-------------|--------------------------------------|----------------------------------|
| crawl4ai    | **DEFAULT** AI-powered web scraping  | Best reliability, no API keys    |
| pyppeteer   | Async Puppeteer-based scraping       | Fast, clean, good alternative    |
| selenium    | Traditional browser automation       | Widely supported, more verbose   |
| scrapinggraph | External AI service                | Powerful but requires API key    |

## Configuration

The application filters cars based on:
- QIRRate >= 80 (higher is better)
- DefectRate <= 15 (lower is better)
- Preference for sedans, coupes, and select hatchbacks

## License

MIT 