import csv
import io
from datetime import datetime
from decimal import Decimal


class ParseError(Exception):
    def __init__(self, message, row_context=None):
        super().__init__(message)
        self.message = message
        self.row_context = row_context


def _parse_ts(value: str) -> str:
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    raise ParseError(f"cannot parse timestamp: {value!r}", value)


def _parse_decimal(value: str):
    value = value.strip()
    if value == "":
        return None
    try:
        return Decimal(value)
    except Exception:
        raise ParseError(f"cannot parse number: {value!r}", value)


_REGION_MAP = {
    "CLUZ": "LUZON",
    "CVIS": "VISAYAS",
    "CMIN": "MINDANAO",
    "LUZON": "LUZON",
    "VISAYAS": "VISAYAS",
    "MINDANAO": "MINDANAO",
}


def _map_grid(raw: str) -> str:
    return _REGION_MAP.get(raw.strip().upper(), raw.strip().upper())


def _row_to_canonical_price(rec: dict) -> dict:
    ts = rec.get("TIME_INTERVAL", "").strip()
    parsed = _parse_ts(ts)
    trading_date = parsed[:10]
    resource = rec.get("RESOURCE_NAME", "").strip()

    return {
        "trading_date": trading_date,
        "interval_start": parsed,
        "pricing_node": resource.upper(),
        "grid": _map_grid(rec.get("REGION_NAME", "")),
        "lmp_php_per_mwh": _parse_decimal(rec.get("LMP", "")),
        "demand_mw": None,
        "plant_id": resource.upper(),
        "fuel_type": None,
        "generation_mw": _parse_decimal(rec.get("SCHED_MW", "")),
    }


def _read_csv(raw: bytes):
    text = io.StringIO(raw.decode("utf-8"))
    return csv.DictReader(text)


def _skip_row(rec: dict) -> bool:
    ts = (rec.get("TIME_INTERVAL") or "").strip()
    if not ts or ts.upper() in ("EOF", "SUM"):
        return True
    if (rec.get("RUN_TIME") or "").strip().upper() in ("EOF", "SUM"):
        return True
    if not any(str(v).strip() for v in rec.values() if v is not None):
        return True
    return False


def _row_to_canonical_rtd(rec: dict, *, demand: bool) -> dict:
    ts = rec.get("TIME_INTERVAL", "").strip()
    parsed = _parse_ts(ts)
    trading_date = parsed[:10]
    if demand:
        value = _parse_decimal(rec.get("MKT_REQT", ""))
    else:
        value = _parse_decimal(rec.get("GENERATION", ""))
    return {
        "trading_date": trading_date,
        "interval_start": parsed,
        "pricing_node": None,
        "grid": _map_grid(rec.get("REGION_NAME", "")),
        "lmp_php_per_mwh": None,
        "demand_mw": value if demand else None,
        "plant_id": None,
        "fuel_type": None,
        "generation_mw": value if not demand else None,
    }


def _parse_rtd(raw: bytes, *, demand: bool) -> list[dict]:
    rows = []
    for rec in _read_csv(raw):
        if _skip_row(rec):
            continue
        if rec.get("COMMODITY_TYPE", "").strip() != "En":
            continue
        rows.append(_row_to_canonical_rtd(rec, demand=demand))
    return rows


def parse_demand(raw: bytes) -> list[dict]:
    return _parse_rtd(raw, demand=True)


def parse_generation(raw: bytes) -> list[dict]:
    return _parse_rtd(raw, demand=False)


def parse_prices(raw: bytes) -> list[dict]:
    text = io.StringIO(raw.decode("utf-8"))
    reader = csv.DictReader(text)
    rows = []
    for rec in reader:
        ts = rec.get("TIME_INTERVAL", "").strip()
        if not ts or ts.upper() in ("EOF", "SUM"):
            continue
        if not any(str(v).strip() for v in rec.values()):
            continue
        rows.append(_row_to_canonical_price(rec))
    return rows
