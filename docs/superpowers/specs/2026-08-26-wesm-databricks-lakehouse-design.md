# WESM Lakehouse — Databricks Portfolio Project (Design Spec)

**Date:** 2026-08-26
**Status:** Approved by user
**Budget:** PHP 0 / USD 0 — runs entirely on Databricks Free Edition
**Pace:** ~10 hrs/week, ~6 weeks core + optional stretch

---

## 1. Problem statement

The Philippine Wholesale Electricity Spot Market (WESM) publishes hourly market
results: locational marginal prices per pricing node, system demand, and
generation mix by plant type. This data drives electricity bills, but it is
published as raw files with no analytical layer. Anyone asking "when and why do
prices spike?" must assemble the answer themselves.

This project builds a lakehouse that ingests WESM publications incrementally,
conforms them into an auditable Delta Lake star schema, and serves price-spike,
demand, and generation-mix analytics through a Databricks SQL dashboard.

## 2. Goals and non-goals

### Goals

1. Demonstrate the full Databricks lakehouse stack: Auto Loader, Delta Lake
   (MERGE, time travel), medallion architecture, Unity Catalog, Workflows,
   SQL dashboards.
2. Produce a genuinely useful analytical artifact: price-spike explorer,
   demand/generation trends for the PH grids (Luzon, Visayas, Mindanao).
3. Exhibit production hygiene: idempotency, quarantine of bad records,
   row-count reconciliation, tests, fail-loud alerting.

### Non-goals (YAGNI)

- No true streaming/Kafka simulation — Auto Loader's incremental file ingest
  covers the "streaming-shaped" talking points honestly.
- No ML forecasting in the core build (optional stretch week only).
- No multi-cloud portability layers; we target Databricks-native features on
  purpose (that is the portfolio point).
- No dbt on Databricks in the core build; transformations are PySpark/SQL so
  Spark fundamentals stay visible.

## 3. Environment and constraints

Databricks Free Edition (replaced Community Edition in 2025). Verified limits
(docs.databricks.com, July 2026):

| Constraint | Impact on design |
|---|---|
| Serverless compute only | Notebooks + Jobs compute is serverless; no cluster tuning demos |
| Outbound internet restricted to trusted domains | Extraction runs **locally** and lands files in a UC Volume |
| 1 SQL warehouse (2X-Small) | Dashboard queries are modest aggregates; fine |
| Max 5 concurrent job tasks | Single sequential workflow; no fan-out needed |
| No Scala/R | PySpark + SQL only |

LinkedIn identity verification can unlock outbound internet; if the user opts
in later, extraction may move into a workspace notebook. The local-extractor
design remains valid either way.

**Honest framing (goes in README):** in production, extraction would be a cloud
function or ADF dropping files into ADLS/S3. The local extractor mirrors that
shape (file drop → landing zone) with a different trigger.

## 4. Architecture

```
[WESM portal]
     |  (1) local Python extractor
     |      - downloads daily market-result files (hourly LMPs, demand, generation mix)
     |      - idempotent per trading_date via a local watermark file
     |      - writes a manifest (file -> trading_date -> row count) next to each landing
     v
UC Volume  (landing zone: /Volumes/<catalog>/<schema>/raw_landing/wesm/)
     |
     |  (2) Auto Loader (cloudFiles)
     |      - incremental, checkpointed, schema-evolution mode
     v
BRONZE  - append-only Delta tables, one per source file family
        - original values + metadata columns (_ingest_ts, _file_name, trading_date)
        - bronze rows are never updated or deleted
     |
     |  (3) PySpark transforms
     |      - cast types, standardize node/grid identifiers
     |      - deduplicate on natural key
     |      - MERGE upserts to absorb retroactively revised settlement figures
     |      - invalid rows diverted to quarantine table with reason + raw payload
     v
SILVER  - clean, conformed Delta tables
     |
     |  (4) aggregations + dimensional modeling
     v
GOLD    - fact_market_interval (grain: pricing_node x trading_interval)
        - dim_grid, dim_date
        - marts: daily avg price per grid, price-spike flags,
          demand vs generation-mix over time
     |
     v
(5) Databricks SQL Dashboard
    - price trends per grid, price-spike explorer, generation mix over time,
      quarantine/health tiles

Orchestration: one Databricks Workflow job (bronze -> silver -> gold -> checks),
scheduled daily; tolerant of missed days because Auto Loader picks up whatever landed.
Governance: Unity Catalog schemas bronze/silver/gold + managed Volume.
```

## 5. Data model

Unity Catalog layout:

```
<workspace catalog>/
├── bronze.wesm_prices_raw       -- hourly LMPs as landed
├── bronze.wesm_demand_raw       -- system demand as landed
├── bronze.wesm_generation_raw   -- generation mix as landed
├── silver.prices                -- typed, deduped, revision-merged LMPs
├── silver.demand                -- typed, conformed demand series
├── silver.generation            -- typed plant/fuel-level output
├── silver.quarantine            -- rejected rows + reason + raw payload
├── gold.fact_market_interval    -- grain: pricing_node x trading_interval
├── gold.dim_grid                -- Luzon / Visayas / Mindanao (+ node rollups)
├── gold.dim_date
├── gold.mart_daily_price        -- daily avg/min/max per grid
├── gold.mart_price_spikes       -- intervals above spike threshold
│                                    (grid's trailing 30-day mean + 2 std dev;
│                                    final formula confirmed against real data in week 1)
└── gold.mart_generation_mix     -- fuel-share over time per grid
```

Natural keys: `(pricing_node, trading_interval)` for prices; `(grid, interval)`
for demand; `(plant_id, interval)` for generation. Silver MERGE keys match these.

Revision handling: when WESM republishes past intervals (settlement disputes),
MERGE updates silver in place while bronze retains the original rows — Delta
time travel provides the audit trail ("what did they originally say?").

## 6. Error handling

- **Extractor:** retries with exponential backoff; skips dates already
  downloaded (watermark); manifest written beside every landing.
- **Schema drift:** Auto Loader schema evolution captures new columns in
  bronze without breaking downstream; if an expected column disappears, the
  silver task fails loudly.
- **Bad records:** never dropped silently — quarantined with reason codes;
  dashboard exposes a quarantine count tile.
- **Job failures:** 1 automatic retry, then email notification; every task logs
  row counts in/out.

## 7. Testing strategy

| Layer | Test | Runs where |
|---|---|---|
| Extractor parsing | pytest against fixture files (malformed rows, missing columns, duplicate dates) | local + GitHub Actions CI |
| Silver logic | unit tests on small DataFrames: dedupe correctness, MERGE idempotency (run twice == run once) | local + CI |
| Pipeline quality | post-transform checks: row-count reconciliation file→bronze→silver, null-rate thresholds, sanity ranges (e.g., flag negative prices rather than crash) | in-job |
| Gold | SQL expectation checks: grain uniqueness, referential integrity to dims, no future-dated intervals | in-job |

## 8. Repository layout

```
wesm-lakehouse/
├── extractor/          # local Python: requests-based download + Volume upload
├── notebooks/          # bronze / silver / gold Databricks notebooks
├── jobs/               # workflow definition (job-as-code JSON)
├── tests/              # pytest suite
├── docs/               # architecture diagram, data dictionary, WESM source notes
└── README.md           # problem -> architecture -> findings, recruiter-readable
```

Databricks Asset Bundles are a stretch item (infrastructure-as-code signal),
not required for the core build.

## 9. Build phases (~10 hrs/wk)

| Week | Phase | Deliverable |
|---|---|---|
| 1 | Discovery + setup | Verified list of free WESM file URLs/formats; data dictionary; Free Edition workspace with UC schemas + Volume; PySpark ramp drill. **Checkpoint:** if free WESM downloads prove gated/incomplete, pivot source (Open-Meteo weather x PH energy context, or EIA international) before any pipeline code exists |
| 2 | Extract + bronze | Extractor end-to-end into Volume; Auto Loader bronze notebook ingesting incrementally |
| 3 | Silver | Typed/deduped tables, revision MERGE, quarantine, quality checks passing |
| 4 | Gold | Star-schema facts/dims/marts built |
| 5 | Orchestrate + serve | Scheduled Workflow job; SQL dashboard; README with diagram + sample findings |
| 6 | Polish | All tests green, screenshots/GIF demo, resume bullet drafted. Stretch if ahead: MLflow demand forecast OR Lakeflow/DLT reimplementation comparison |

## 10. Success criteria

1. `docker`-free, $0-cost pipeline running on Free Edition for at least two
   consecutive scheduled weeks without manual repair.
2. Re-running any stage twice produces identical table contents
   (demonstrable idempotency).
3. A deliberately corrupted input file ends up quarantined with a reason code
   and does not corrupt silver/gold (demonstrable fail-safe).
4. Dashboard answers "which grids/intervals spiked this month?" in two clicks.
5. A stranger can follow the README from clone-to-dashboard in under an hour.

## 11. Interview narratives this project supplies

- Incremental ingestion design: watermarks, checkpoints, exactly-once semantics
- Late/revised data: MERGE upserts + time-travel audit
- Data quality: reconciliation counts, quarantine patterns, fail-loud alerts
- Cost awareness: serverless quotas, 2X-Small warehouse, why medallion beats
  one-big-table
- Domain credibility: energy & utilities is a top Databricks vertical
