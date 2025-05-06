# Using Pyppeteer for Web Scraping

This project offers multiple web scraping methods, including [Pyppeteer](https://github.com/pyppeteer/pyppeteer), a Python port of the Puppeteer JavaScript library.

## Advantages of Pyppeteer over Selenium

1. **Asynchronous API**: Pyppeteer uses async/await for better performance
2. **No WebDriver**: No need to download and manage WebDriver binaries
3. **Modern**: Uses modern Chrome/Chromium browser automation
4. **Lightweight**: Generally less resource-intensive than Selenium

## Using Pyppeteer in this Project

To use the Pyppeteer implementation:

```bash
python src/main.py --method pyppeteer --sites facebook
```

This will use Pyppeteer for the Facebook Marketplace scraper.

## Configuration

The Pyppeteer implementation supports optional Facebook login for better results:

1. Copy `template.env` to `.env`
2. Add your Facebook credentials (optional)
3. Run the scraper

## Troubleshooting

If you encounter issues with Pyppeteer:

1. **Browser Visibility**: You can set `headless: False` in the `_setup_browser` method to see the browser in action and debug
2. **Chromium Download**: On first run, Pyppeteer may need to download Chromium (this is automatic)
3. **Selectors**: If Facebook changes its UI, you may need to update the CSS selectors in the scraper class

## Comparing Methods

| Method    | Pros                           | Cons                          |
|-----------|--------------------------------|-------------------------------|
| Selenium  | Mature, well-documented        | Requires webdriver, slow      |
| Pyppeteer | Async, no webdriver, cleaner   | Less mature ecosystem         |
| AI-based  | No selectors, more resilient   | External service, API key     |

Choose the method that best fits your needs and constraints. 