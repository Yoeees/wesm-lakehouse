import base64
import io
import time
import zipfile

import requests

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)


class DownloadError(Exception):
    def __init__(self, message, url=None):
        super().__init__(message)
        self.message = message
        self.url = url


# family -> (listing slug, server folder, file prefix, zipped?)
FAMILIES = {
    "prices": ("dipc-energy-results-final", "DIPCEF", "DIPCEF", True),
    "demand": ("rtd-regional-summaries", "RTDREG", "RTDREG", False),
    "generation": ("rtd-regional-summaries", "RTDREG", "RTDREG", False),
}


BASE_SERVER_PATH = "/var/www/html/wp-content/uploads/downloads/data"


def _build_url(listing: str, server_path: str) -> str:
    full = f"{BASE_SERVER_PATH}/{server_path}"
    encoded = base64.b64encode(full.encode()).decode()
    return f"https://www.iemop.ph/market-data/{listing}/?md_file={encoded}"


def _request_with_retry(session, url, retries=3, backoff=2.0):
    last_exc = None
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.content
            if resp.status_code == 404:
                return None
            last_exc = DownloadError(f"HTTP {resp.status_code} for {url}", url)
        except requests.RequestException as exc:
            last_exc = DownloadError(str(exc), url)
        time.sleep(backoff * (2 ** attempt))
    raise last_exc


def _fetch_family(session, listing, server_path, zipped):
    raw = _request_with_retry(session, _build_url(listing, server_path))
    if raw is None:
        return None
    if not zipped:
        return raw
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            names = [n for n in zf.namelist() if not n.endswith("/")]
            if not names:
                raise DownloadError(f"zip has no files: {server_path}")
            parts = []
            for name in names:
                if name.lower().endswith(".csv"):
                    parts.append(zf.read(name))
            if not parts:
                raise DownloadError(f"zip has no csv: {server_path}")
            return b"\n".join(parts)
    except zipfile.BadZipFile as exc:
        raise DownloadError(f"bad zip for {server_path}: {exc}", server_path)


def _hourly_server_paths(folder, prefix, yyyymmdd):
    paths = []
    for hh in range(24):
        for mm in (0,):
            paths.append(f"{folder}/{prefix}_{yyyymmdd}{hh:02d}{mm:02d}.zip")
    return paths


def download_day(session, family: str, trading_date: str) -> bytes:
    yyyymmdd = trading_date.replace("-", "")
    if family not in FAMILIES:
        raise DownloadError(f"unknown family: {family}")
    listing, folder, prefix, zipped = FAMILIES[family]

    if family == "prices":
        all_lines: list[bytes] = []
        header_line = None
        for path in _hourly_server_paths(folder, prefix, yyyymmdd):
            raw = _fetch_family(session, listing, path, True)
            if raw is None:
                continue
            lines = raw.split(b"\n", 1)
            if len(lines) == 2:
                h, body = lines
                if header_line is None:
                    header_line = h
                all_lines.append(body)
        if header_line is None:
            raise DownloadError(f"no prices files found for {trading_date}")
        return header_line + b"\n" + b"\n".join(all_lines)

    server_path = f"{folder}/{prefix}_{yyyymmdd}.csv"
    raw = _fetch_family(session, listing, server_path, False)
    if raw is None:
        raise DownloadError(f"no {family} file found for {trading_date}")
    return raw

