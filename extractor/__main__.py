import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

from .download import FAMILIES, USER_AGENT, DownloadError, download_day
from .manifest import build_manifest
from .parse import parse_demand, parse_generation, parse_prices
from .watermark import save_watermark

LANDING = Path(__file__).resolve().parent.parent / "landing"
WATERMARK = LANDING / ".watermark.json"
MANIFEST = LANDING / "manifest.json"


def _session():
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _already_downloaded(landing_dir: Path, family: str, yyyymmdd: str) -> bool:
    for p in landing_dir.glob(f"wesm/{family}/*"):
        if yyyymmdd in p.name:
            return True
    return False


def _ext(zipped: bool) -> str:
    return ".csv"


def _process_family(session, family, trading_date, manifest, watermarks):
    yyyymmdd = trading_date.replace("-", "")
    listing, folder, prefix, zipped = FAMILIES[family]
    family_dir = LANDING / "wesm" / family
    family_dir.mkdir(parents=True, exist_ok=True)

    if _already_downloaded(LANDING, family, yyyymmdd):
        print(f"[skip] {family} {trading_date} already present")
        return

    raw = download_day(session, family, trading_date)

    if family == "prices":
        rows = parse_prices(raw)
    elif family == "demand":
        rows = parse_demand(raw)
    else:
        rows = parse_generation(raw)

    out_file = family_dir / f"{prefix}_{yyyymmdd}{_ext(zipped)}"
    mode = "wb"
    out_file.write_bytes(raw)
    manifest.append(build_manifest(rows, out_file.name))
    watermarks.add(trading_date)
    print(f"[ok] {family} {trading_date}: {len(rows)} rows -> {out_file.name}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="extractor", description="WESM extractor")
    parser.add_argument("--date", help="trading date YYYY-MM-DD (default: yesterday)")
    args = parser.parse_args(argv)

    trading_date = args.date or (date.today() - timedelta(days=1)).isoformat()

    manifest = []
    if MANIFEST.exists():
        try:
            manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            manifest = []

    watermarks = set()
    session = _session()
    for family in FAMILIES:
        try:
            _process_family(session, family, trading_date, manifest, watermarks)
        except DownloadError as exc:
            print(f"[warn] {family}: {exc}", file=sys.stderr)

    if watermarks:
        save_watermark(str(WATERMARK), max(watermarks))

    MANIFEST.write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
