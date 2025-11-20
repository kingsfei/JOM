import pytest
from unittest.mock import AsyncMock, patch

from pdf_utils import is_url_valid, html_to_pdf
def test_is_url_valid():
    assert is_url_valid("https://www.sec.gov") is True
    assert is_url_valid("https://invalid.url.test") is False

@pytest.mark.asyncio
@patch("pdf_utils.launch")
async def test_html_to_pdf_mocked(launch_mock, tmp_path):
    mock_page = AsyncMock()
    mock_browser = AsyncMock()
    mock_browser.newPage.return_value = mock_page
    launch_mock.return_value = mock_browser

    url = "https://www.sec.gov/fake_filing"
    output_file = tmp_path / "output.pdf"

    with patch("pdf_utils.is_url_valid", return_value=True):
        await html_to_pdf(mock_browser, url, output_file)

    mock_browser.newPage.assert_called_once()
    mock_page.setUserAgent.assert_called_once()
    mock_page.goto.assert_called_once_with(url, {"waitUntil": "networkidle2", "timeout": 60000})
    mock_page.pdf.assert_called_once_with({"path": output_file, "format": "A4", "printBackground": True})
    mock_page.close.assert_called_once()
