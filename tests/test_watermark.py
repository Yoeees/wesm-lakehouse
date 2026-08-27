import json
from pathlib import Path

from extractor.watermark import load_watermark, save_watermark
from extractor.manifest import build_manifest


def test_watermark_roundtrip(tmp_path: Path):
    path = tmp_path / ".watermark.json"
    assert load_watermark(str(path)) is None

    save_watermark(str(path), "2026-08-25")
    assert load_watermark(str(path)) == "2026-08-25"

    payload = json.loads(path.read_text())
    assert payload["last_date"] == "2026-08-25"


def test_watermark_overwrites(tmp_path: Path):
    path = tmp_path / ".watermark.json"
    save_watermark(str(path), "2026-08-25")
    save_watermark(str(path), "2026-08-26")
    assert load_watermark(str(path)) == "2026-08-26"


def test_manifest_counts_rows():
    rows = [
        {"trading_date": "2026-08-25", "grid": "LUZON"},
        {"trading_date": "2026-08-25", "grid": "VISAYAS"},
        {"trading_date": "2026-08-25", "grid": "MINDANAO"},
    ]
    manifest = build_manifest(rows, "RTDREG_20260825.csv")
    assert manifest["file_name"] == "RTDREG_20260825.csv"
    assert manifest["trading_date"] == "2026-08-25"
    assert manifest["row_count"] == 3
