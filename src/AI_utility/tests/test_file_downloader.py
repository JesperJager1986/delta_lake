import pytest
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path
from src.AI_utility.src.download.FileDownLoader import FileDownloader

@pytest.fixture
def downloader():
    return FileDownloader("https://example.com/test.csv")


def test_init(downloader):
    """Test initialization of FileDownloader."""
    assert downloader.path == "https://example.com/test.csv"
    assert downloader.store_file == Path("download/test.csv")


@patch("os.makedirs")
def test_create_folder(mock_makedirs, downloader):
    """Test folder creation."""
    downloader.create_folder()
    mock_makedirs.assert_called_once_with(Path("download").absolute(), exist_ok=True)


@patch("requests.get")
@patch("builtins.open", new_callable=mock_open)
def test_download_success(mock_open, mock_requests_get, downloader):
    """Test successful download."""
    mock_requests_get.return_value = MagicMock(status_code=200, content=b"Fake Data")

    result = downloader.download()

    assert result is True
    mock_requests_get.assert_called_once_with("https://example.com/test.csv")
    mock_open.assert_called_once_with(Path("download/test.csv"), "wb")


@patch("requests.get")
def test_download_failure(mock_requests_get, downloader):
    """Test failed download (404)."""
    mock_requests_get.return_value = MagicMock(status_code=404)

    result = downloader.download()

    assert result is False
