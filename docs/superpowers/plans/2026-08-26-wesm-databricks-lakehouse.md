# WESM Lakehouse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **⚠️ MODE OVERRIDE — HUMAN-IN-LOOP TUTORING:** Per explicit user contract, the *user* performs every step. The agent guides, explains, reviews user-written code, and answers questions — but writes no project code unless the user explicitly asks. Agent-authored artifacts are limited to this plan plus docs/scaffolding the user requests.

**Goal:** Build a free, end-to-end Databricks lakehouse (medallion architecture) over Philippine WESM electricity market data: local extraction → UC Volume landing → Auto Loader bronze → MERGE-based silver → star-schema gold → scheduled Workflow job → SQL dashboard.

**Architecture:** Batch medallion lakehouse on Databricks Free Edition (serverless). Extraction runs locally (Free Edition gates outbound internet) and lands files into a Unity Catalog Volume; Auto Loader incrementally ingests into append-only bronze Delta tables; silver cleans/conforms and absorbs revised settlements via MERGE with a quarantine table for rejects; gold serves star-schema marts to a native SQL dashboard. One Workflows job orchestrates bronze→silver→gold→checks daily.

**Tech Stack:** Databricks Free Edition (serverless notebooks/Jobs, Unity Catalog, Volumes, Auto Loader, Delta Lake, SQL warehouse, dashboards), Python 3.11+ (requests, pytest), PySpark + Spark SQL, GitHub Actions CI.

**Spec:** `docs/superpowers/specs/2026-08-26-wesm-databricks-lakehouse-design.md`

## Global Constraints

- Cost: $0 — Databricks Free Edition only; no credit-card trials, no paid services.
- Compute: serverless-only; no cluster configuration anywhere.
- Languages: PySpark and Spark SQL only (Free Edition has no Scala/R).
- Idempotency: re-running ANY stage twice must produce identical table contents.
- Bronze is append-only; originals never updated/deleted.
- Bad records are quarantined (`silver.quarantine` with `reason_code`), never silently dropped.
- Fail loud: quality check failures must fail the job; email notification configured.
- Naming: snake_case; schemas `bronze`, `silver`, `gold`; volume `raw_landing`.
- Local machine: Windows; all local commands PowerShell-compatible.
- Commits: conventional-commit style (`feat:`, `test:`, `docs:`, `chore:`).
- Tutor mode: user types/executes everything; agent reviews and quizzes at every ⭐ CHECKPOINT.

## Canonical data contract (decided upfront, contract-first)

All parsers map whatever WESM actually publishes onto these canonical fields (locked now so downstream tasks stay valid regardless of discovery outcome):

```
trading_date      date          -- PH trading date (Asia/Manila)
interval_start    timestamp     -- settlement/dispatch interval start (PH local)
pricing_node      string        -- WESM nodal identifier, uppercased/trimmed
grid              string        -- LUZON | VISAYAS | MINDANAO
lmp_php_per_mwh   decimal(12,4) -- locational marginal price
demand_mw         decimal(12,4)
plant_id          string
fuel_type         string        -- COAL|NATGAS|OIL|HYDRO|GEO|SOLAR|WIND|BIOMASS|OTHER
generation_mw     decimal(12,4)
```

Parser signature (all three families):

```python
def parse_<family>(raw: bytes) -> list[dict]:
    """Return dicts whose keys are exactly the canonical fields relevant to <family>.
    Raises ParseError(message, row_context) on structurally broken input."""
```

---

## WEEK 1 — Discovery, environment, ramp

### Task 1: Source discovery & data dictionary

**Files:**
- Create: `wesm-lakehouse/docs/data-dictionary.md`

**Produces:** verified download URL pattern(s) for hourly LMP, demand, generation files; confirmed formats; data dictionary mapping published columns → canonical fields; spike threshold sanity-checked against ≥30 days of real prices.

- [ ] **Step 1:** Browse WESM/PEMC public market-results pages. Locate where hourly price (LMP per node), system demand, and generation mix are published. Note format: CSV, XLSX, or JSON endpoint.
  - Technique: browser DevTools → Network tab → filter Fetch/XHR while clicking download links; a direct file URL beats HTML scraping.
- [ ] **Step 2:** Download ONE day of each file family manually. Record per file: delimiter, header row index, encoding, timestamp format, how pricing nodes/grids are encoded.
- [ ] **Step 3:** Write `docs/data-dictionary.md`: one table per family — published column → canonical field (or `IGNORED`) → notes.
- [ ] **Step 4:** Confirm node→grid mapping rule or lookup table; record it in the dictionary (gold.dim_grid depends on it).
- [ ] **Step 5:** From ~30 days of prices, eyeball distribution per grid (min/mean/max); validate spike definition (trailing 30-day mean + 2σ). Adjust spec threshold if it flags nothing/everything.
- [ ] **Step 6:** ⭐ CHECKPOINT with agent: present findings; agent validates dictionary covers every canonical field before any code exists. **Fallback trigger:** if downloads prove gated/incomplete, invoke spec §9 week-1 pivot (Open-Meteo weather × PH energy context, or EIA international) BEFORE proceeding.

### Task 2: Databricks Free Edition workspace + Unity Catalog layout

**Files:** none (cloud setup)

**Consumes:** nothing. **Produces:** workspace catalog (record its actual name!) containing schemas `bronze`, `silver`, `gold`; Volume `<catalog>.bronze.raw_landing`; verified upload path `/Volumes/<catalog>/bronze/raw_landing/wesm/<family>/`.

- [ ] **Step 1:** Sign up at https://www.databricks.com/learn/free-edition. (Optional, recommended: LinkedIn verification to unlock outbound internet later.)
- [ ] **Step 2:** Explore Catalog Explorer; identify your workspace catalog name; write it down — every script references it.
- [ ] **Step 3:** In a scratch notebook run:
  ```sql
  CREATE SCHEMA IF NOT EXISTS <catalog>.bronze;
  CREATE SCHEMA IF NOT EXISTS <catalog>.silver;
  CREATE SCHEMA IF NOT EXISTS <catalog>.gold;
  ```
- [ ] **Step 4:** Create the volume:
  ```sql
  CREATE VOLUME IF NOT EXISTS <catalog>.bronze.raw_landing;
  ```
- [ ] **Step 5:** Verify in Catalog Explorer that `/Volumes/<catalog>/bronze/raw_landing/` exists and accepts a manual upload (upload any small .csv, then delete it).

### Task 3: PySpark ramp drill (in-workspace, zero setup)

**Files:** Create: Databricks notebook `ramp_drill` (throwaway).

**Why in-workspace:** local PySpark on Windows needs JAVA_HOME/winutils wrangling — unnecessary here, because all unit-tested logic (parsers, validators) is deliberately pure-Python; Spark code runs free on serverless notebooks.

**Produces:** fluency with the 8 Spark operations this project uses everywhere.

- [ ] **Step 1:** Create notebook; attach serverless compute.
- [ ] **Step 2:** Drill exercises, one cell each, self-check by printing results:
  1. `spark.createDataFrame` from list of dicts; print schema.
  2. `select`, `filter`, `withColumn` (add computed column).
  3. Casting: string `"1234.56"` → `DecimalType(12,4)`; string → `to_timestamp(col, "format")`.
  4. `groupBy("grid").agg(avg, min, max)` on made-up prices.
  5. Window function: trailing rolling average ordered by time, partitioned by grid.
  6. `join`: 10-row dimension DF joined to facts; observe inner vs left join difference.
  7. Dedupe semantics: sort-by-latest + `dropDuplicates(["key"])`.
  8. Write small DF as Delta to `<catalog>.bronze.ramp_test`; read back; `DROP TABLE`.
- [ ] **Step 3:** ⭐ CHECKPOINT with agent: explain what lazy evaluation means and where your drill showed it mattering.

---

## WEEK 2 — Repo, extractor, bronze

### Task 4: Repo scaffold + CI

**Files:**
- Create: `wesm-lakehouse/` repo root — `README.md` (stub), `.gitignore`, `requirements-dev.txt`, `extractor/__init__.py`, `tests/__init__.py`, `.github/workflows/ci.yml`

**Produces:** git repo pushed to GitHub; CI green on empty test suite.

- [ ] **Step 1:** Create folder `wesm-lakehouse` (ideally outside OneDrive sync — file locking bites git; staying inside is acceptable, note the risk).
- [ ] **Step 2:** `.gitignore`: `__pycache__/`, `.venv/`, `landing/`, `.watermark.json`, `.env`.
- [ ] **Step 3:** `python -m venv .venv`; activate (PowerShell: `.venv\Scripts\Activate.ps1`); `pip install requests pytest`; freeze to `requirements-dev.txt`.
- [ ] **Step 4:** Smoke test `tests/test_smoke.py`: `def test_ci(): assert True`.
- [ ] **Step 5:** `.github/workflows/ci.yml`: on push/PR → ubuntu-latest → setup-python 3.11 → `pip install -r requirements-dev.txt` → `pytest -v`.
- [ ] **Step 6:** `git init`; commit `chore: scaffold`; create public GitHub repo; push.
- [ ] **Step 7:** Verify CI green on GitHub Actions tab.

### Task 5: Extractor — parse, download, watermark, manifest (TDD)

**Files:**
- Create: `extractor/parse.py`, `extractor/download.py`, `extractor/__main__.py`, `tests/test_parse.py`, `tests/test_watermark.py`, `tests/fixtures/sample_<family>.<ext>` (from Task 1 downloads)
- URL pattern + format specifics come from `docs/data-dictionary.md`

**Consumes:** canonical contract (top of plan). **Produces:** `parse_prices/demand/generation(raw: bytes) -> list[dict]`; `ParseError(msg, context)`; `DownloadError`; `load_watermark(path) -> str|None`, `save_watermark(path, date_str)`; `build_manifest(rows, file_name) -> dict` with keys `(file_name, trading_date, row_count)`.

- [ ] **Step 1 (failing test):** `tests/test_parse.py::test_parse_prices_canonical_fields` — feed fixture bytes from your Task 1 sample; assert non-empty list, exact canonical price-family keys on every dict, first row equals hand-verified values you read from the file yourself.
  - Parsers are pure-Python (stdlib `csv`, or `openpyxl` if XLSX) so tests need no Spark. If XLSX, add `openpyxl` to requirements.
- [ ] **Step 2:** `pytest -v` → FAIL (ImportError).
- [ ] **Step 3:** Implement `parse_prices` minimally to pass.
- [ ] **Step 4:** `pytest -v` → PASS.
- [ ] **Step 5:** Same red-green cycle for: `test_parse_prices_malformed_row_raises` (corrupt a fixture row → expect `ParseError`), then both behaviors for demand and generation parsers.
- [ ] **Step 6:** Watermark + manifest tests (`test_watermark_roundtrip`, `test_manifest_counts_rows`) then implementations.
- [ ] **Step 7:** `download_day(session, family, trading_date) -> bytes`: hits Task 1 URL pattern; ×3 retries exponential backoff; raises `DownloadError` after final failure. (Network path reviewed by agent, not unit-tested.)
- [ ] **Step 8:** CLI: `python -m extractor --date YYYY-MM-DD` downloads all three families to `landing/wesm/<family>/<yyyymmdd>.<ext>`, updates `.watermark.json`, appends manifest row per file. Running twice same date = second run skips (verify manually).
- [ ] **Step 9:** Full suite PASS; commit `feat: extractor with parse/download/watermark`; push; CI green.

### Task 6: Landing zone + bronze Auto Loader

**Files:**
- Create: Databricks notebook `01_bronze_ingest` + mirrored copy in repo `notebooks/01_bronze_ingest.py`

**Consumes:** Volume (Task 2), landed files (Task 5). **Produces:** tables `bronze.wesm_prices_raw|wesm_demand_raw|wesm_generation_raw` with `_ingest_ts`, `_file_name`; checkpoints under `/Volumes/<catalog>/bronze/raw_landing/_checkpoints/<family>/`.

- [ ] **Step 1:** Files into Volume — baseline: Catalog Explorer → Volume → **Upload to this volume** (UI). Optional automation: install Databricks CLI (`winget install Databricks.CLI` or pip), `databricks auth login --profile FREE`, then `databricks fs cp <local> "/Volumes/<catalog>/bronze/raw_landing/wesm/prices/"`. UI upload is fully sufficient for the core build.
- [ ] **Step 2:** Auto Loader pattern (prices shown; adapt format per data dictionary):
  ```python
  df = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "<fmt>")
        .option("cloudFiles.schemaLocation", chkpt + "/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load("/Volumes/<catalog>/bronze/raw_landing/wesm/prices/"))
  bronze = (df.withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_file_name", F.col("_metadata.file_name")))
  q = (bronze.writeStream
       .option("checkpointLocation", chkpt)
       .trigger(availableNow=True)
       .toTable("<catalog>.bronze.wesm_prices_raw"))
  q.awaitTermination()
  ```
- [ ] **Step 3:** First run creates table; verify count == manifest row count.
- [ ] **Step 4:** Incremental proof: upload day 2 → rerun → count grows by exactly that file's rows. Run again with NO new files → zero new rows (idempotency evidence).
- [ ] **Step 5:** Repeat demand + generation (own checkpoint dirs).
- [ ] **Step 6:** ⭐ CHECKPOINT: explain what the checkpoint does and what happens if deleted — answer first, then verify by experiment.
- [ ] **Step 7:** Mirror notebook into repo; commit `feat: bronze autoloader ingest`.

---

## WEEK 3 — Silver

### Task 7: silver.prices — validate, quarantine, MERGE revisions

**Files:**
- Create: `notebooks/02_silver_prices` (+ repo mirror), `silver_logic/validate.py`, `tests/test_validate.py`

**Consumes:** `bronze.wesm_prices_raw`; canonical contract. **Produces:** `transform_prices(df) -> (valid_df, quarantine_df)` pure function; table `silver.prices` keyed `(pricing_node, interval_start)`; table `silver.quarantine(family, natural_key, reason_code, _quarantine_ts, payload)`.

- [ ] **Step 1 (failing tests):** `test_transform_flags_null_key`, `test_transform_flags_price_out_of_range` (≤ −10000 or > 100000 PHP/MWh), `test_transform_casts_types`, `test_transform_valid_rows_pass`.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement `silver_logic/validate.py` — pure function over an in-memory DF, no IO inside (that is what makes it unit-testable).
- [ ] **Step 4:** Run → PASS. Commit `feat: price validation logic`.
- [ ] **Step 5:** Notebook wires bronze → transform → MERGE:
  ```sql
  MERGE INTO silver.prices t
  USING updates s
    ON t.pricing_node = s.pricing_node AND t.interval_start = s.interval_start
  WHEN MATCHED AND s._ingest_ts > t._ingest_ts THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
  ```
  Carry `_ingest_ts` through transforms so revision recency compares correctly.
- [ ] **Step 6:** Revision drill: upload same trading-day file with ONE price changed → rerun bronze (both rows preserved) + silver MERGE → silver shows new value, bronze holds both. Screenshot + run `DESCRIBE HISTORY silver.prices`. This is your time-travel interview story.
- [ ] **Step 7:** Quarantine drill: upload corrupted copy → broken row lands in `silver.quarantine` with reason; rest of silver unchanged.
- [ ] **Step 8:** Idempotency drill: rerun silver twice → identical counts. ⭐ CHECKPOINT: why is MERGE safe here but plain `.write.mode("append")` would not be?
- [ ] **Step 9:** Commit `feat: silver prices with merge revisions and quarantine`.

### Task 8: silver.demand + silver.generation

**Files:**
- Create: `notebooks/03_silver_demand_gen`, `silver_logic/validate_demand_gen.py` + tests

**Consumes:** Task 7 pattern. **Produces:** `silver.demand` keyed `(grid, interval_start)`; `silver.generation` keyed `(plant_id, interval_start)`; fuel standardization map (unmapped → OTHER + quarantine reason `UNKNOWN_FUEL`); shared quarantine table distinguished by `family`.

Validations for this family: null grid/plant_id → `NULL_KEY`; negative demand/generation → `NEGATIVE_VALUE`; fuel map unit test covering every mapping entry.

- [ ] **Steps 1–4:** Same TDD cycle as Task 7 with the validations above.
- [ ] **Steps 5–8:** Notebooks; revision drill (demand figures DO get revised); idempotency drill.
- [ ] **Step 9:** Commit `feat: silver demand and generation`.

---

## WEEK 4 — Gold

### Task 9: Dimensions

**Files:**
- Create: `notebooks/04_gold_dims`

**Consumes:** silver tables; node→grid rule from data dictionary. **Produces:** `gold.dim_grid(grid, node_count, region_notes)` from DISTINCT silver nodes via the discovered mapping — unmapped nodes → quarantine (`UNMAPPED_NODE`), never guessed; `gold.dim_date(date, year, month, day_of_week, is_weekend)` spanning min→max trading_date + 90 days ahead.

- [ ] **Step 1:** Build node→grid mapping as a clearly-marked config cell documenting its provenance.
- [ ] **Step 2:** Generate dims; referential completeness check: zero silver pricing nodes missing from dim_grid.
- [ ] **Step 3:** Dims rebuild via `CREATE OR REPLACE TABLE` — full rebuild is correct for dims; be ready to explain why vs facts at checkpoint.
- [ ] **Step 4:** ⭐ CHECKPOINT + commit `feat: gold dimensions`.

### Task 10: Fact & marts

**Files:**
- Create: `notebooks/05_gold_fact_marts`

**Consumes:** silver + dims. **Produces:**
- `gold.fact_market_interval` grain `(pricing_node, interval_start)`: LMP, grid_key, date_key, `_ingest_ts`; built with the Task 7 MERGE pattern (facts get revisions too).
- `gold.mart_daily_price`: per grid×date — avg/min/max LMP, interval_count.
- `gold.mart_price_spikes`: intervals where LMP > per-grid trailing 30-day mean + 2σ (window: partition by grid, order by date, RANGE 30 PRECEDING). Decide with agent: spike flag on fact vs separate mart (storage vs query-simplicity trade-off).
- `gold.mart_generation_mix`: per grid×date×fuel — total MWh + share% (share sums to 1.0 ± 0.001 per grid-date).
- `gold.mart_quarantine_health`: quarantine counts by family/reason/day.

- [ ] **Step 1:** Fact build + reconciliation assert: fact count == distinct silver keys.
- [ ] **Step 2:** Spike mart; chart it in-notebook — flagged days should match reality (summer peaks, outages). Contradiction ⇒ wrong threshold ⇒ revisit Task 1 Step 5.
- [ ] **Step 3:** Mix + health marts + share-sum assert.
- [ ] **Step 4:** Idempotency drills on all four tables.
- [ ] **Step 5:** ⭐ CHECKPOINT: state the fact grain and why marts exist separately. Commit `feat: gold fact and analytical marts`.

---

## WEEK 5 — Orchestration + serving

### Task 11: Workflow job — scheduled + alerting

**Files:**
- Create: `jobs/workflow.json` (exported job-as-code)

**Consumes:** notebooks 01→05. **Produces:** daily job `wesm_daily`: bronze(×3) → silver(prices, demand/gen) → gold(dims, fact, marts) → quality_checks; retry=1; email notification on failure.

- [ ] **Step 1:** Jobs UI: create job chaining existing notebook tasks with dependencies; retry=1; email notifications set.
- [ ] **Step 2:** Quality-checks notebook: reconciliation queries (manifest↔bronze↔silver counts; orphan fact↔dim keys; share-sum checks) that raise on breach — a failing assert fails the task; that IS the alert path.
- [ ] **Step 3:** Manual "Run now" → all green. Then break on purpose (rename a volume path) → confirm failure email arrives → fix back.
- [ ] **Step 4:** Schedule daily at an hour after WESM's typical publication time (noted in Task 1).
- [ ] **Step 5:** Export job JSON into repo (`jobs/workflow.json`); commit `feat: scheduled workflow with quality gates`.

### Task 12: SQL dashboard + README

**Files:**
- Create: Databricks SQL dashboard (workspace); full `README.md`

**Consumes:** gold marts. **Produces:** recruiter-facing dashboard + README satisfying spec §10 criterion 5 (stranger clone-to-dashboard <1 hour).

Dashboard tiles: LMP trend per grid (30-day window, parameterized); spike explorer table + count tile; generation-mix stacked area over time; quarantine health tile.

README sections: problem statement → architecture diagram (mermaid) → data dictionary link → setup steps → sample findings (2–3 real insights with charts) → honest limitations (local extractor rationale).

- [ ] **Step 1:** Build dashboard queries against marts only (never silver/bronze in tiles — know why).
- [ ] **Step 2:** Write README; include architecture diagram and the revision/time-travel screenshot from Task 7.
- [ ] **Step 3:** ⭐ CHECKPOINT: agent reads README cold and flags anything a recruiter wouldn't follow. Commit `docs: readme and dashboard`.

---

## WEEK 6 — Polish, drills, stretch

### Task 13: Resilience drills (spec §10 criteria 2 & 3)

- [ ] **Step 1:** Idempotency drill: run entire workflow twice end-to-end; capture counts before/after — identical. Screenshot for README/interviews.
- [ ] **Step 2:** Corruption drill: drop deliberately malformed file into landing → next run quarantines it, pipeline stays green, health tile reflects it. Screenshot.
- [ ] **Step 3:** Soak: let the daily schedule run ≥2 consecutive weeks post-plan; note any manual interventions (goal: zero).
- [ ] **Commit:** `test: resilience drill evidence`.

### Task 14: Portfolio packaging + stretch decision

- [ ] **Step 1:** Screenshots/GIF of dashboard + workflow graph into README.
- [ ] **Step 2:** Draft resume bullet with agent review, e.g.: "Built a $0-cost Databricks lakehouse (Auto Loader, Delta MERGE, medallion architecture, Unity Catalog) ingesting Philippine electricity market data with quarantine-based data quality and scheduled Workflows."
- [ ] **Step 3:** ⭐ CHECKPOINT: final teach-back — user explains the whole flow source→dashboard unprompted; agent pokes two weak spots.
- [ ] **Step 4 (stretch, only if ahead):** pick ONE — (a) MLflow demand forecast model tracked against gold marts, or (b) re-implement silver+gold as Lakeflow Declarative Pipelines and write a comparison blog post. Do not attempt both.

---

## Verification (maps to spec §10 success criteria)

| Criterion | Evidence produced by |
|---|---|
| $0 cost, runs unattended 2+ weeks | Task 13 Step 3 soak log |
| Rerun-twice idempotency | Tasks 6/7/8 drills + Task 13 Step 1 screenshots |
| Corrupted input quarantined safely | Task 13 Step 2 |
| Dashboard answers spike question in 2 clicks | Task 12 tile design |
| Stranger follows README < 1 hr | Task 12 Step 3 cold read |

## Execution

Human-in-loop tutoring mode: begin at Week 1, Task 1 together in this session — the agent walks the user through each step, reviews outputs at ⭐ CHECKPOINTs, and never writes project code unless explicitly asked.
