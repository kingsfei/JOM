import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from sec_utils import fetch_target_company_cik, fetch_html_url_from_filing, get_recent_10k_filings_url

# Exact match
def test_fetch_target_company_cik_exact():
    df = pd.DataFrame({
        "cik_str": ["0000320193"],
        "ticker": ["AAPL"],
        "title": ["Apple Inc"]
    })
    cik = fetch_target_company_cik(df, "Apple Inc")
    assert cik == "0000320193"

# Fuzzy match
def test_fetch_target_company_cik_fuzzy():
    df = pd.DataFrame({
        "cik_str": ["0000320193"],
        "ticker": ["AAPL"],
        "title": ["Apple Inc"]
    })
    cik = fetch_target_company_cik(df, "Aple Inc")  # typo
    assert cik == "0000320193"

# HTML URL construction
def test_fetch_html_url_from_filing():
    filing = {
        "cik": "0000320193",
        "accessionNumber": "000032019324000123",
        "primaryDocument": "aapl-20240928.htm"
    }
    url = fetch_html_url_from_filing(filing)
    expected = "https://www.sec.gov/Archives/edgar/data/0000320193/000032019324000123/aapl-20240928.htm"
    assert url == expected


@patch("sec_utils.requests.get")
def test_get_recent_10k_filings_url_basic(mock_get):
    data = {
        "filings": {
            "recent": {
                "form": ["10-K", "10-Q"],
                "accessionNumber": ["000032019324000123", "000032019324000124"],
                "primaryDocument": ["aapl-20240928.htm", "aapl-20240628.htm"],
                "filingDate": ["2024-10-28", "2024-07-01"],
            },
            "files": [],
        }
    }

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = data
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    df = get_recent_10k_filings_url("0000320193", delay=0, start_year=2023, end_year=2024)
    assert len(df) == 1
    assert df.iloc[0]['accessionNumber'] == "000032019324000123"
    assert mock_get.call_count == 1


@patch("sec_utils.requests.get")
@patch("time.sleep", return_value=None)
def test_get_recent_10k_filings_url_handles_429(mock_sleep, mock_get):
    data = {
        "filings": {
            "recent": {
                "form": ["10-K"],
                "accessionNumber": ["000032019324000123"],
                "primaryDocument": ["aapl-20240928.htm"],
                "filingDate": ["2024-10-28"],
            },
            "files": [],
        }
    }

    resp_429 = MagicMock()
    resp_429.status_code = 429

    resp_ok = MagicMock()
    resp_ok.status_code = 200
    resp_ok.json.return_value = data
    resp_ok.raise_for_status.return_value = None

    mock_get.side_effect = [resp_429, resp_ok]

    df = get_recent_10k_filings_url("0000320193", delay=0, start_year=2024, end_year=2024)
    assert len(df) == 1
    assert mock_sleep.called
    assert mock_get.call_count == 2


@patch("sec_utils.requests.get")
def test_get_recent_10k_filings_year_filter(mock_get):
    recent_data = {
        "filings": {
            "recent": {
                "form": ["10-K"],
                "accessionNumber": ["0001"],
                "primaryDocument": ["doc1.htm"],
                "filingDate": ["2013-05-01"],
            },
            "files": [
                {
                    "name": "CIK0000123456-2015.json",
                    "filingFrom": "2014-01-01",
                    "filingTo": "2015-12-31",
                }
            ],
        }
    }

    archive_data = {
        "filings": {
            "recent": {
                "form": ["10-K", "10-Q"],
                "accessionNumber": ["0002", "0003"],
                "primaryDocument": ["doc2.htm", "doc3.htm"],
                "filingDate": ["2015-03-01", "2015-06-01"],
            }
        }
    }

    recent_response = MagicMock()
    recent_response.status_code = 200
    recent_response.json.return_value = recent_data
    recent_response.raise_for_status.return_value = None

    archive_response = MagicMock()
    archive_response.status_code = 200
    archive_response.json.return_value = archive_data
    archive_response.raise_for_status.return_value = None

    def side_effect(url, headers=None, **kwargs):
        if url.endswith("CIK0000123456.json"):
            return recent_response
        return archive_response

    mock_get.side_effect = side_effect

    df = get_recent_10k_filings_url("0000123456", start_year=2014, end_year=2015, delay=0)
    assert len(df) == 1
    assert df.iloc[0]["accessionNumber"] == "0002"
