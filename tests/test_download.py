import pytest
import requests

from extractor.download import download_day, FAMILIES, USER_AGENT, DownloadError


@pytest.mark.integration
@pytest.mark.parametrize("family", list(FAMILIES.keys()))
def test_download_day_smoke(family):
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    data = download_day(session, family, "2026-08-25")
    assert data, "expected non-empty bytes"
    assert b"\n" in data or b"," in data, "expected CSV-like content"
