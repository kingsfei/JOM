import os
import requests
import pandas as pd
import difflib
import re
import time
from dotenv import load_dotenv
import logging
import ast

logging.basicConfig(level=logging.INFO)
load_dotenv()

# Share the same flexible header loading logic as pdf_utils.
_headers_env = os.getenv("HEADERS")
_default_user_agent = os.getenv("SEC_USER_AGENT") or "sec-edgar-downloader (contact@example.com)"

if _headers_env:
    try:
        HEADERS = ast.literal_eval(_headers_env)
    except (ValueError, SyntaxError):
        HEADERS = {'User-Agent': _headers_env}
else:
    HEADERS = {'User-Agent': _default_user_agent}


def _get_json_with_retry(url: str) -> dict:
    """Request JSON from SEC with basic retry on rate limiting."""
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 429:
        time.sleep(1)
        return _get_json_with_retry(url)
    response.raise_for_status()
    return response.json()


def _build_filings_df(filings_dict: dict | None) -> pd.DataFrame:
    """Convert the SEC 'recent' filings dict into a DataFrame."""
    if not filings_dict:
        return pd.DataFrame()
    try:
        return pd.DataFrame.from_dict(filings_dict)
    except (ValueError, TypeError):
        return pd.DataFrame()


def _file_overlaps_years(file_info: dict, start_year: int | None, end_year: int | None) -> bool:
    """Return True if the SEC archival file overlaps the desired year range."""
    if not (start_year or end_year):
        return True
    filing_from = file_info.get("filingFrom")
    filing_to = file_info.get("filingTo")
    try:
        start = int(filing_from[:4]) if filing_from else None
        end = int(filing_to[:4]) if filing_to else None
    except (TypeError, ValueError):
        return True  # if we can't parse, err on fetching
    if start_year and end and end < start_year:
        return False
    if end_year and start and start > end_year:
        return False
    return True
def load_company_indices() -> pd.DataFrame:
    """Fetch company_tickers.json and return DataFrame with cik_str (10-digit)."""
    try:
        response = requests.get('https://www.sec.gov/files/company_tickers.json', headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame.from_dict(data, orient='index')
        df['title'] = df['title'].astype(str).str.strip()
        df['cik_str'] = df['cik_str'].apply(lambda x: str(x).zfill(10))
        return df
    except Exception as e:
        logging.error(f"Error loading company indices: {e}")
        return pd.DataFrame(columns=['cik_str', 'ticker', 'title'])

def _similarity(a, b):
    return difflib.SequenceMatcher(None, a, b).ratio()

def fetch_target_company_cik(df: pd.DataFrame, company_name: str, threshold=0.6) -> str | None:
    """
    Given a DataFrame of company indices and a company name, return the CIK against the company name.
    """
    try:
        company_lower = company_name.strip().lower()
        if df.empty:
            return None

        
        df = df.copy()
        df['title_lower'] = df['title'].str.lower()

        # First, try exact match
        exact_match = df[df['title_lower'] == company_lower]
        if not exact_match.empty:
            return exact_match.iloc[0]['cik_str']

        # Next, try boundary match
        pattern = rf"\b{re.escape(company_lower)}\b"
        boundary_match = df[df['title_lower'].str.contains(pattern, regex=True, na=False)]
        if not boundary_match.empty:
            return boundary_match.iloc[0]['cik_str']

        # Next, try fuzzy matching
        candidates = df['title_lower'].tolist()
        fuzzy_matches = difflib.get_close_matches(company_lower, candidates, n=5, cutoff=threshold)
        if fuzzy_matches:
            best_match = max(fuzzy_matches, key=lambda x: _similarity(company_lower, x))
            match_row = df[df['title_lower'] == best_match]
            if not match_row.empty:
                return match_row.iloc[0]['cik_str']

        # not found
        return None
    except Exception as e:
        logging.error(f"Error fetching CIK for {company_name}: {e}")
        return None

def get_recent_10k_filings_url(
    cik: str,
    delay: float = 0.2,
    start_year: int | None = None,
    end_year: int | None = None,
) -> pd.DataFrame:
    """
    Fetch filings for a company by CIK and return 10-K forms (optionally filtered by year range).
    Uses the SEC submissions API plus archival files to cover historical years.
    """
    submissions_api_url = f'https://data.sec.gov/submissions/CIK{cik}.json'
    data = _get_json_with_retry(submissions_api_url)

    frames = []
    frames.append(_build_filings_df(data.get('filings', {}).get('recent')))

    files_meta = data.get('filings', {}).get('files', [])
    if files_meta and (start_year or end_year):
        for file_info in files_meta:
            if not _file_overlaps_years(file_info, start_year, end_year):
                continue
            file_name = file_info.get('name')
            if not file_name:
                continue
            file_url = f"https://data.sec.gov/submissions/{file_name}"
            archive_data = _get_json_with_retry(file_url)
            archive_recent = archive_data.get('filings', {}).get('recent')
            if not archive_recent:
                archive_recent = archive_data.get('recent')
            frames.append(_build_filings_df(archive_recent))

    filings = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if filings.empty:
        return pd.DataFrame()

    filings_10k = filings[filings['form'] == '10-K'].copy()
    if filings_10k.empty:
        return pd.DataFrame()

    if 'accessionNumber' in filings_10k.columns:
        filings_10k = filings_10k.drop_duplicates(subset='accessionNumber')

    if 'filingDate' in filings_10k.columns:
        filings_10k['filingDate'] = pd.to_datetime(filings_10k['filingDate'], errors='coerce')
        if start_year or end_year:
            mask = filings_10k['filingDate'].notna()
            if start_year:
                mask &= filings_10k['filingDate'].dt.year >= start_year
            if end_year:
                mask &= filings_10k['filingDate'].dt.year <= end_year
            filings_10k = filings_10k[mask]
            if filings_10k.empty:
                return pd.DataFrame()

    filings_10k['cik'] = cik

    # to avoid hitting rate limits
    time.sleep(delay)
    return filings_10k

def fetch_html_url_from_filing(filing: pd.Series) -> str:
    """
    Given a filing Series (row), construct and return the full HTML URL.
    """
    base_url = "https://www.sec.gov/Archives/edgar/data"
    accession_number = filing['accessionNumber'].replace("-", "") 
    primary_doc = filing['primaryDocument']
    full_url = f"{base_url}/{filing['cik']}/{accession_number}/{primary_doc}"
    logging.info(f"Constructed URL: {full_url}")
    return full_url
