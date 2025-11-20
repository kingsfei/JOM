import os
import asyncio
import pandas as pd
from pyppeteer import launch

from sec_utils import (
    load_company_indices,
    fetch_target_company_cik,
    get_recent_10k_filings_url,
    fetch_html_url_from_filing,
)

import logging

logging.basicConfig(level=logging.INFO)

COMPANIES = ["Apple", "Meta", "Alphabet", "Amazon", "Netflix", "Goldman Sachs"]
OUTPUT_DIR = "10k_html"
CONCURRENT_PAGES = 3
START_YEAR = 2014
END_YEAR = 2024


async def fetch_html(browser, url):
    """Open the HTML page and return page content as string."""
    page = await browser.newPage()
    try:
        await page.goto(url, timeout=120000, waitUntil="networkidle2")
        content = await page.content()
        return content
    except Exception as e:
        logging.error(f"Error fetching HTML from {url}: {e}")
        return None
    finally:
        await page.close()


async def save_html_file(browser, url, output_file):
    """Fetch the HTML text and save to file."""
    html = await fetch_html(browser, url)
    if html:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html)
        logging.info(f"Saved HTML → {output_file}")
    else:
        logging.warning(f"Failed to fetch HTML for {output_file}")


async def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = load_company_indices()
    if df.empty:
        logging.error("Could not load SEC company indices.")
        return

    browser = await launch(headless=True, args=["--no-sandbox"])
    semaphore = asyncio.Semaphore(CONCURRENT_PAGES)

    tasks = []

    for company in COMPANIES:
        cik = fetch_target_company_cik(df, company)
        if not cik:
            logging.warning(f"CIK not found for {company}")
            continue

        filings = get_recent_10k_filings_url(cik, start_year=START_YEAR, end_year=END_YEAR)
        if filings.empty:
            logging.warning(f"No 10-K filings found for {company}")
            continue

        for _, filing in filings.iterrows():
            url = fetch_html_url_from_filing(filing)
            accession = filing["accessionNumber"].replace("-", "")
            filing_date = filing.get("filingDate")
            year_label = "unknown"
            if pd.notna(filing_date):
                year_label = str(filing_date.year)

            output_file = os.path.join(
                OUTPUT_DIR,
                f"{company}_10K_{year_label}_{accession}.html"
            )

            async def sem_task(url=url, output_file=output_file):
                async with semaphore:
                    await save_html_file(browser, url, output_file)

            tasks.append(sem_task())

    if tasks:
        await asyncio.gather(*tasks)

    try:
        await browser.close()
    except Exception as e:
        logging.error(f"Error closing browser: {e}")


if __name__ == "__main__":
    asyncio.run(main())
