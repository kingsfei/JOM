import os
import requests
from pyppeteer import launch
from dotenv import load_dotenv
import logging
import ast
logging.basicConfig(level=logging.INFO)

load_dotenv()

# SEC requires a descriptive User-Agent including your email. Prefer HEADERS from
# .env (as documented in README) but fall back to SEC_USER_AGENT or a default.
_headers_env = os.getenv("HEADERS")
_default_user_agent = os.getenv("SEC_USER_AGENT") or "sec-edgar-downloader (contact@example.com)"

if _headers_env:
    try:
        HEADERS = ast.literal_eval(_headers_env)
    except (ValueError, SyntaxError):
        # Treat the value as a plain user agent string if parsing fails.
        HEADERS = {'User-Agent': _headers_env}
else:
    HEADERS = {'User-Agent': _default_user_agent}

# Path to system-installed Google Chrome, update this path if running on another system
CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

def is_url_valid(url: str) -> bool:
    """Check if the URL is accessible (status code 200)."""
    try:
        response = requests.head(url, headers=HEADERS, allow_redirects=True, timeout=10)
        if response.status_code != 200:
            # Some SEC endpoints reject HEAD; try a lightweight GET before failing.
            with requests.get(
                url,
                headers=HEADERS,
                allow_redirects=True,
                timeout=10,
                stream=True,
            ) as resp_get:
                return resp_get.status_code == 200
        return True
    except Exception:
        return False


async def html_to_pdf(browser, url: str, output_file: str):
    """
    Use an existing browser instance to convert a SEC filing URL to PDF.
    Validates the URL before downloading.
    """
    if not is_url_valid(url):
        logging.warning(f"Invalid or inaccessible URL: {url}")
        return

    page = await browser.newPage()
    try:
        await page.setUserAgent(HEADERS['User-Agent'])
        await page.goto(url, {"waitUntil": "networkidle2", "timeout": 60000})
        await page.pdf({"path": output_file, "format": "A4", "printBackground": True})
        logging.info(f"PDF saved: {output_file}")
    except Exception as e:
        logging.error(f"Failed to generate PDF from {url}: {e}")
    finally:
        await page.close()
