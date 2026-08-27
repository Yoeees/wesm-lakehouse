import pytest
from pathlib import Path

from extractor.parse import parse_prices, parse_demand, parse_generation, ParseError

FIXTURES = Path(__file__).parent / "fixtures"

EXPECTED_KEYS = {
    "trading_date",
    "interval_start",
    "pricing_node",
    "grid",
    "lmp_php_per_mwh",
    "demand_mw",
    "plant_id",
    "fuel_type",
    "generation_mw",
}


def test_parse_prices_canonical_fields():
    raw = (FIXTURES / "DIPCEF_202607260000.csv").read_bytes()
    rows = parse_prices(raw)
    assert rows, "expected a non-empty parse result"
    first = rows[0]
    assert set(first.keys()) == EXPECTED_KEYS


def test_parse_prices_malformed_row_raises():
    malformed = (
        b"TIME_INTERVAL,REGION_NAME,RESOURCE_NAME,PRICING_FLAG,LMP,SCHED_MW,LMP_SMP,LMP_LOSS,LMP_CONGESTION,\n"
        b"NOT_A_TIMESTAMP,LUZON,01ACNPC_G01,OK,100.0,1.0,0,0,0,\n"
    )
    with pytest.raises(ParseError):
        parse_prices(malformed)


def test_parse_demand_canonical_fields():
    raw = (FIXTURES / "RTDREG_sample.csv").read_bytes()
    rows = parse_demand(raw)
    assert rows, "expected a non-empty parse result"
    first = rows[0]
    assert set(first.keys()) == EXPECTED_KEYS
    assert first["grid"] in {"LUZON", "VISAYAS", "MINDANAO"}
    assert first["demand_mw"] is not None
    assert first["generation_mw"] is None


def test_parse_demand_filters_en_rows_only():
    raw = (FIXTURES / "RTDREG_sample.csv").read_bytes()
    rows = parse_demand(raw)
    assert all(r["demand_mw"] == 0 or r["demand_mw"] is not None for r in rows)
    grids = {r["grid"] for r in rows}
    assert "LUZON" in grids and "VISAYAS" in grids and "MINDANAO" in grids


def test_parse_generation_canonical_fields():
    raw = (FIXTURES / "RTDREG_sample.csv").read_bytes()
    rows = parse_generation(raw)
    assert rows, "expected a non-empty parse result"
    first = rows[0]
    assert set(first.keys()) == EXPECTED_KEYS
    assert first["generation_mw"] is not None
    assert first["demand_mw"] is None


def test_parse_generation_nonnegative():
    raw = (FIXTURES / "RTDREG_sample.csv").read_bytes()
    rows = parse_generation(raw)
    assert all(r["generation_mw"] >= 0 for r in rows)
