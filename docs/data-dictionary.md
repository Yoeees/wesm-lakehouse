# WESM/IEMOP Data Dictionary

**Project:** WESM Lakehouse (see `docs/superpowers/specs/`)
**Discovered:** 2026-08-26 by manual DevTools capture + Playwright-assisted inventory
**Sources verified live:** yes — samples in `landing/samples/`

---

## 1. Access mechanism (applies to ALL families)

Every file is served as a plain HTTP GET — no login, no cookies:

```
GET https://www.iemop.ph/market-data/<listing>/?md_file=<base64>
```

The `md_file` parameter is the **Base64-encoded full server path** of the file,
e.g. `L3Zhci93d3cvaHRtbC93cC1jb250ZW50L3VwbG9hZHMvZG93bmxvYWRzL2RhdGEvTVBSRVNFUlZFL01QX1JFU0VSVkVfMjAyNjA4MjUuY3N2` decodes to
`/var/www/html/wp-content/uploads/downloads/data/MPRESERVE/MP_RESERVE_20260825.csv`.

**URL construction recipe (extractor core):**

```python
BASE = "/var/www/html/wp-content/uploads/downloads/data"
server_path = f"{BASE}/{FAMILY_FOLDER}/{FILE_PREFIX}_{yyyymmdd}.csv"   # or _yyyymmddhhmm.zip
url = (f"https://www.iemop.ph/market-data/{listing}/"
       f"?md_file={base64.b64encode(server_path.encode()).decode()}")
```

- Send a browser-like `User-Agent` header (server runs Apache/PHP; don't test its defaults).
- Responses use `Content-Type: application/octet-stream` even for CSVs — never trust MIME; validate bytes.
- ⚠️ Fragility note: this hardcodes the site's internal WordPress folder layout (`/var/www/html/wp-content/uploads/downloads/data/...`). If IEMOP reorganizes, all URLs break — extractor must fail loudly and this dictionary gets updated first.

## 2. Source inventory

| # | Family | Listing page slug | Server folder / file pattern | Cadence | Freshness | Sample size |
|---|--------|-------------------|------------------------------|---------|-----------|-------------|
| F1 | Regional summaries (demand/supply) | `rtd-regional-summaries` | `RTDREG/RTDREG_<yyyymmdd>.csv` | 1/day | previous day | ~339 KB, ~4,300 rows/day |
| F2 | Energy results FINAL (LMP + schedules) | `dipc-energy-results-final` | `DIPCEF/DIPCEF_<yyyymmddhhmm>.zip` | 24 zips/day (hourly) | **~30-day lag** (settlement finalization) | 156 KB zip → 1.15 MB CSV/hour |
| F3 | Reserve market clearing price | `rtd-reserve-market-clearing-price` | `MPRESERVE/MP_RESERVE_<yyyymmdd>.csv` | 1/day | recent | ~261 KB, ~3,350 rows/day |
| F4 | RTD prices & schedules (preliminary) | `rtd-prices-and-schedules` | `RTD/RTD_<yyyymmddhhmm>.zip` | 24 zips/day | real-time | not yet sampled |
| F5 | Reserve results final | `dipc-reserve-results-final` | `DIPCRF/DIPCRF_<yyyymmddhhmm>.zip` | hourly | lagged | 10 KB zip, contents not yet opened |

**Core pipeline scope:** F1 + F2 + F3. (F4 exists to demonstrate the preliminary-vs-final revision story; F5 optional.)

## 3. Family schemas → canonical fields

### F1 `RTDREG_<yyyymmdd>.csv` — regional summary

Delimiter `,`, header row 1, trailing empty column exists (watch for stray null field).

| Published column | Canonical field | Notes |
|---|---|---|
| `RUN_TIME` | `trading_date` | Format `M/D/YYYY` |
| `TIME_INTERVAL` | `interval_start` | Format `M/D/YYYY h:mm:ss AM/PM`, PH local |
| `REGION_NAME` | `grid` | Codes `CLUZ/CVIS/CMIN` → map to `LUZON/VISAYAS/MINDANAO` |
| `COMMODITY_TYPE` | *(filter)* | Keep rows where value = `En` only (see §4) |
| `MKT_REQT` | `demand_mw` | Market requirement = demand to serve |
| `GENERATION` | `generation_mw` | Region-total scheduled generation |
| `LOAD_BID`, `LOAD_CURTAILED`, `LOSSES`, `MKT_IMPORT`, `MKT_EXPORT` | *(retained, not canonical)* | Needed for balance validation rule V1 |

### F2 `DIPCEF_<yyyymmddhhmm>.zip` — energy results final (per hour)

ZIP contains one CSV (~12 five-minute intervals × all resources).

| Published column | Canonical field | Notes |
|---|---|---|
| `TIME_INTERVAL` | `interval_start` | Format `M/D/YYYY H:mm` (24h), PH local |
| `REGION_NAME` | `grid` | Already `LUZON/VISAYAS/MINDANAO` — no remap! |
| `RESOURCE_NAME` | `pricing_node` and `plant_id` | Resource-level granularity; `_G*` suffix = generator, `_L*` = load (negative schedule) |
| `PRICING_FLAG` | *(validation)* | `OK` expected; anything else → quarantine candidate |
| `LMP` | `lmp_php_per_mwh` | Locational marginal price, PHP/MWh |
| `SCHED_MW` | `generation_mw` | Negative for load resources |
| `LMP_SMP`, `LMP_LOSS`, `LMP_CONGESTION` | *(retained)* | LMP decomposition; feeds rule V2 |

### F3 `MP_RESERVE_<yyyymmdd>.csv` — reserve clearing prices

| Published column | Canonical field | Notes |
|---|---|---|
| `RUN_TIME` / `TIME_INTERVAL` | `trading_date` / `interval_start` | Same formats as F1 |
| `REGION_NAME` | `grid` | `CLUZ/CVIS/CMIN` map |
| `RESOURCE_NAME` | `plant_id` | |
| `RESOURCE_TYPE` | *(retained)* | `G` observed |
| `COMMODITY_TYPE` | *(retained)* | `Dr/Fr/Rd/Ru` only (no `En`) |
| `MARGINAL_PRICE` | *(reserve price)* | PHP/MWh; observed up to ~25,000 |

## 4. Code tables

### Regions

| Code | Grid |
|---|---|
| `CLUZ` / `LUZON` | Luzon |
| `CVIS` / `VISAYAS` | Visayas |
| `CMIN` / `MINDANAO` | Mindanao |

### Commodity types (verified against IEMOP/WESM protocol docs)

| Code | Product | Meaning |
|---|---|---|
| `En` | Energy | Delivered electricity — the only type used for demand/generation series |
| `Ru` | Regulation Up | Standby capacity ramping output UP to correct frequency drift |
| `Rd` | Regulation Down | Standby capacity ramping output DOWN |
| `Fr` | Contingency (frequency) reserve | Covers sudden loss of largest unit/line; sized ≈ largest single contingency per grid |
| `Dr` | Dispatchable Reserve | Synchronizable within ~15 min of System Operator instruction |

## 5. Verified validation rules (silver-layer checks)

- **V1 — Energy balance identity** (F1, `En` rows):
  `GENERATION + MKT_IMPORT == MKT_REQT + LOSSES + MKT_EXPORT` within ±0.05 MW.
  Verified on sample: Visayas 1578.30+529.54 = 2086.76+21.08+0 ✓
- **V2 — LMP decomposition** (F2): `|LMP − (LMP_SMP + LMP_LOSS + LMP_CONGESTION)| ≤ tolerance`.
  Verified: 6716.5061 vs 6570.638+145.8682+0 = 6716.5062 ✓
- **V3 — Regulation sizing** (F1): `Ru/Rd` requirements ≈ 2% of that grid's energy demand (WESM Protocol §4.1.2).
  Verified across all three grids ✓
- **V4 — Price sanity**: flag (not drop) `LMP < 0` (negative price intervals are real and analytically interesting) and `LMP > offer cap`.

## 6. Known quirks & risks

1. Locale-formatted timestamps (`M/D/YYYY`, AM/PM) — parse with explicit formats only.
2. Sloppy numeric formatting observed (`00` instead of `0` in congestion column) — cast, don't trust strings.
3. ZIP-wrapped families (F2/F4/F5) — extraction step needed before parsing.
4. F2's ~30-day publication lag vs F4's real-time availability ⇒ the same interval legitimately appears twice with different values over time. This is *the* justification for bronze-append-only + silver MERGE design.
5. Fuel types are NOT present in any sampled family. Generation-mix-by-fuel mart needs a join source — candidates: `registered-capacity-generation` or `rtd-generation-offers` listings. **Open question O1.**
6. F5 zip contents not yet inspected (**open question O2**) — low priority, outside core scope.

## 7. Pending discovery item

- **Spike threshold calibration** (spec Task 1 Step 5): pull ~30 days of F2 hourly LMPs, compute per-grid mean/σ, confirm "mean+2σ" flags a sane number of intervals. Not yet run.
