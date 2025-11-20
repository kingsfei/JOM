# SEC 10-K PDF Downloader

This project fetches the latest **10-K filings** from the SEC EDGAR database for the following companies : "Apple", "Meta", "Alphabet", "Amazon", "Netflix", "Goldman Sachs" and converts them into PDF format. It demonstrates **asynchronous processing, SEC API integration, and unit testing**.

---

## **Project Structure**
```
sec-edgar/
├─ main.py            # Main script to fetch and download PDFs
├─ pdf_utils.py       # Async PDF generation and SEC URL validation
├─ sec_utils.py       # SEC API helpers: load indices, fetch CIK, get filings, build URLs
├─ tests/             # Unit tests for utilities
│  ├─ __init__.py
│  ├─ test_pdf_utils.py
│  └─ test_sec_utils.py
└─ 10k_pdfs/          # Output folder for downloaded PDFs (created automatically)

```
## Requirements:
- Google Chrome installed on your system.

- Required Python packages are listed in requirements.txt

#### Installation Steps:
1. Clone the respository:
```
git clone https://github.com/prajna-mothi/sec-edgar.git
cd sec-edgar
```
2. Create and activate virtual environment:
```
python -m venv venv
source venv/bin/activate       # macOS/Linux
venv\Scripts\activate          # Windows
```
3. Install dependencies:
```
pip install -r requirements.txt
```
Ensure CHROME_PATH in .env points to your Chrome executable, change the path if necessary, here is the example:
```
CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
```
Set SEC User-Agent for Headers in .env:
```
HEADERS = {
    'User-Agent': 'Your Name (your.email@example.com)'
}
```
---

## Usage

1. Set the companies you want in main.py and HEADERS:
```
COMPANIES = ["Apple", "Meta", "Alphabet", "Amazon", "Netflix", "Goldman Sachs"]
```
2. Run the script:
```bash
python main.py
```
3. PDFs will be saved in 10k_pdfs/. Each file corresponds to a single 10-K filing.

## Testing

Unit tests ensure your code works correctly without hitting the real SEC API:
```bash
pytest -v tests/
```

- Async PDF generation is mocked.

- SEC API calls are mocked to simulate responses.

- All core functionality (CIK resolution, 10-K URL generation, URL validation) is tested.


## **Script Details**

### 1. `main.py`
- The main script to:
  - Load SEC company indices.
  - Resolve company names to CIKs.
  - Fetch all recent 10-K filings.
  - Generate PDFs asynchronously for each filing.
- Features concurrency control to avoid opening too many Chrome pages at once.

### 2. `pdf_utils.py`
- Contains:
  - `html_to_pdf(browser, url, output_file)` — async function to convert SEC HTML filings to PDF using a single Chrome instance.
  - `is_url_valid(url)` — checks if a SEC filing URL is accessible before attempting PDF generation.
- Handles errors and prints success/failure messages.

### 3. `sec_utils.py`
- Contains SEC helper functions:
  - `load_company_indices()` — fetches `company_tickers.json` from SEC and returns a DataFrame.
  - `fetch_target_company_cik(df, company_name)` — resolves a company name to a zero-padded 10-digit CIK (exact, boundary, or fuzzy match).
  - `get_recent_10k_filings_url(cik, delay)` — fetches recent filings for a CIK and filters 10-K forms.
  - `fetch_html_url_from_filing(filing)` — builds the SEC Archive URL for a given filing.

### 4. `tests/`
- Unit tests for all utility functions:
  - `test_pdf_utils.py` — tests `is_url_valid` and mocks `html_to_pdf`.
  - `test_sec_utils.py` — tests CIK resolution, URL construction, and mocked SEC API calls.
- Uses `pytest` and `pytest-asyncio` for async test support.

### 5. `10k_pdfs/`
- Output folder created automatically when running `main.py`.
- Stores all downloaded PDFs using the format:  
  `CompanyName_10K_accessionNumber.pdf` to avoid collisions.

---