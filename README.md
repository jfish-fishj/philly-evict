# Philly Evictions Pipeline

This repository contains the data processing and analysis pipeline for Philadelphia eviction research.

Additional documentation:
- **Data products codebook**: [`docs/data-products.md`](docs/data-products.md) — column-level reference for all major data products
- Eviction-InfoUSA linkage: [`docs/evict-infousa-linkage.md`](docs/evict-infousa-linkage.md)
- Race imputation: [`docs/race-imputation.md`](docs/race-imputation.md)
- Gender imputation: [`docs/gender-imputation.md`](docs/gender-imputation.md)
- Occupancy and shares: [`docs/occupancy-and-shares.md`](docs/occupancy-and-shares.md)
- BLP estimation: [`docs/blp-estimation.md`](docs/blp-estimation.md)

---

## Quick Start

### Prerequisites

1. **R 4.0+** with packages: `data.table`, `dplyr`, `sf`, `stringr`, `yaml`, `targets`
2. **Raw data** placed in `data/inputs/` (see `config.yml` for expected structure)
3. **Config file**: Copy `config.example.yml` to `config.yml` and adjust paths if needed

### Running the Pipeline

**Option A: Using `{targets}` (recommended)**

```r
# From the repo root
library(targets)
tar_make()        # Run all targets
tar_visnetwork()  # View dependency graph
```

**Option B: Running scripts manually**

```bash
# From repo root, run in order:
Rscript r/clean-eviction-addresses.R
Rscript r/clean-parcel-addresses.R
Rscript r/clean-license-addresses.R
# ... then make scripts, merge scripts, analysis
```

### Occupancy rate and unit imputation fixes (2026-02-16)

Occupancy rates in `parcel_occupancy_panel` were systematically too low (0.55-0.70 weighted mean for large buildings). Three fixes in `make-occupancy-vars.r`:

1. **Soft ceiling on `total_units`**: Capped at 1.5× `max_households` (from InfoUSA) to prevent over-imputation from BG raking. Applied in both 2010 snapshot and final panel.
2. **Occupancy interpolation**: Replaced forced-zero policy for missing `renter_occ` with within-PID LOCF/NOCB carry-forward and BG-level fallback (restricted to PIDs with rental evidence). Reduced missing from ~2M to ~11K rows.
3. **Scale factor guard**: Logs warning if any bin's occupancy scaling factor exceeds 1.5.

New QA outputs for InfoUSA coverage diagnostics:
- `output/qa/infousa_unmatched_high_hh_addresses.csv` — high-household addresses with no parcel match
- `output/qa/infousa_oversubscribed_addresses.csv` — matched PIDs where households >> imputed units

See `docs/occupancy-and-shares.md` and `issues/occupancy-vars.txt` for details.

### Earlier occupancy and BLP updates (2026-02-15)

- `make-occupancy-vars.r` now imputes units at address-cluster level (`n_sn_ss_c`) to reduce condo parcel-split undercount.
- Added household-informed floors for `structure_bin` and final `total_units` to prevent implausible tiny-unit assignments in high-household parcels.
- Added QA outputs:
  - `output/qa/units_vs_max_households_worst_offenders.csv`
  - `output/qa/rtt_price_per_unit_offenders.csv`
- `make-analytic-sample.R` now provides explicit BLP share columns:
  - `share_zip_all_bins` (zip-year markets)
  - `share_zip_bin` (zip-year-bin markets)
- `python/pyblp_estimation.py` uses a top-level share-mode toggle (`zip_all_bins` vs `zip_bin`); legacy `share_zip` is not used for estimation.

---

## Script Workflow

The pipeline follows a strict **clean → crosswalk → aggregate → analyze** order.
Each stage depends on outputs from the previous stage.

### Stage 1: Cleaning (`clean-*`)

Clean raw data, standardize addresses, and prepare for linking.

| Script | Inputs | Outputs | Notes |
|--------|--------|---------|-------|
| `clean-eviction-addresses.R` | `evictions_summary_table`, `evictions_property_addresses` | `evictions_clean.csv` | Canonicalizes addresses, parses docket numbers |
| `clean-parcel-addresses.R` | `opa_gdb` or `opa_shp` | `parcels_clean.csv` | Standardizes OPA parcel addresses |
| `clean-license-addresses.R` | `business_licenses` | `licenses_clean.csv` | Cleans rental license addresses |
| `clean-altos-addresses.R` | `altos_metro_raw` | `altos_clean.csv` | Cleans listing addresses |
| `clean-infousa-addresses.R` | `infousa_raw` | `infousa_clean.csv` | Cleans business directory addresses |
| `clean-eviction-names.R` | `evictions_party_names` | `eviction_names_clean.csv` | Standardizes plaintiff/defendant names |

**Run order:** These scripts are independent and can run in parallel.

### Stage 2: Crosswalks (`merge-*` and spatial `make-*`)

Link cleaned datasets together via address matching and spatial joins.
Despite the naming, `merge-*` scripts produce crosswalks, not merged panels.

| Script | Inputs | Outputs | Notes |
|--------|--------|---------|-------|
| `make_bldg_pid_xwalk.r` | `philly_parcels_sf`, `building_footprints_shp` | `building_pid_xwalk.csv`, `parcel_building_summary.csv` | Spatial join: building footprints → parcels |
| `make-address-parcel-xwalk.R` | `parcels_clean`, `evictions_clean`, `altos_clean`, `infousa_clean` | `xwalk_evictions_to_parcel.csv`, `xwalk_altos_to_parcel.csv`, etc. | Fuzzy address matching to parcels |
| `merge-altos-parcels.R` | `altos_clean`, `parcels_clean` | `altos_parcel_xwalk.csv` | Links listings → parcels |
| `merge-infousa-parcels.R` | `infousa_clean`, `parcels_clean` | `xwalk_infousa_to_parcel.csv` | Links businesses → parcels |

**Dependencies:** Requires Stage 1 clean outputs.

### Stage 3: Aggregation (`make-*` panels)

Build analysis-ready panels by aggregating linked data to PID × time.

| Script | Inputs | Outputs | Primary Key |
|--------|--------|---------|-------------|
| `make-rent-panel.R` | `parcels_clean`, `licenses_clean`, `building_pid_xwalk`, crosswalks | `ever_rentals_panel.csv`, `ever_rentals_pid.csv` | PID |
| `make-occupancy-vars.r` | `ever_rentals_panel`, `infousa_clean` | `parcel_occupancy_panel.csv` | PID × year |
| `make-evict-aggs.R` | `evictions_clean`, `xwalk_evictions_to_parcel` | `evict_pid_year.csv`, `evict_zip_year.csv` | PID × year |
| `make-building-data.R` | `parcels_clean`, `evict_pid_year` | `bldg_panel.csv` | PID × year_quarter |
| `make-analytic-sample.R` | `parcel_occupancy_panel`, `evict_pid_year`, `ever_rentals_panel` | `analytic_sample.csv`, `bldg_panel_blp.csv` | PID × year |

**Dependencies:** Requires Stage 1 clean outputs and Stage 2 crosswalks.

### Stage 3b: Demographic Imputation

Impute race and gender for eviction defendants using cleaned names and geography.

| Script | Inputs | Outputs | Notes |
|--------|--------|---------|-------|
| `impute-race-preflight.R` | `evictions_party_names`, `evict_address_xwalk_case`, `parcel_occupancy_panel`, `philly_bg_shp` | `output/qa/race_imputation_preflight_*.csv` | Parse names, link defendants to geographies, assign impute eligibility |
| `build-tract-race-priors.R` | Census ACS data | `xwalks/bg_renter_race_priors_*.csv` | Build block-group-level race priors from ACS renter data |
| `impute-race.R` | preflight person file, BG race priors, name probability files | `output/qa/race_imputed_*.csv` | BISG race imputation via `wru::predict_race`; block-group priors with low-N tract shrinkage |
| `impute-gender.R` | preflight person file | `output/qa/gender_imputed_*.csv` | Gender imputation via `gender::gender` (SSA baby names 1932--2012) |
| `race-imputation-diagnostics.R` | race imputed files | `output/qa/` diagnostic CSVs | Benchmark comparisons and coverage diagnostics |

**Run order:** Preflight first, then race priors, then race and gender imputation can run in parallel.

See [`docs/race-imputation.md`](docs/race-imputation.md) and [`docs/gender-imputation.md`](docs/gender-imputation.md) for methodology details.

**Dependencies:** Requires Stage 1 clean outputs, Stage 2 crosswalks, and Stage 3 occupancy panel.

### Stage 4: Analysis

Run regressions, generate figures, produce tables.

| Script | Inputs | Outputs | Notes |
|--------|--------|---------|-------|
| `retaliatory-evictions.r` | `analytic_sample.csv` | `output/` tables/figures | Main eviction analysis |
| `price-regs.R` | `analytic_sample.csv`, `altos_*` panels | `output/` tables/figures | Price regulation analysis |
| `altos-price-regs.R` | `altos_*` panels | `output/` tables/figures | Listing price analysis |
| `change-filing-patterns.R` | `evict_pid_year.csv` | `output/` tables/figures | Filing pattern analysis |

**Key rule:** Analysis scripts read from `processed/` and write only to `output/`.

---

## Execution Dependency Graph

```
                    ┌─────────────────────────────────────────┐
                    │           STAGE 1: CLEAN                │
                    │  (all clean-* scripts, run in parallel) │
                    └─────────────────────────────────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    ▼                  ▼                  ▼
            evictions_clean     parcels_clean      licenses_clean
            altos_clean         infousa_clean
                    │                  │                  │
                    └──────────────────┼──────────────────┘
                                       │
                    ┌─────────────────────────────────────────┐
                    │        STAGE 2: CROSSWALKS              │
                    │  merge-*, make_bldg_pid_xwalk,          │
                    │  make-address-parcel-xwalk              │
                    └─────────────────────────────────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    ▼                  ▼                  ▼
          building_pid_xwalk   xwalk_evictions     altos_parcel_xwalk
          xwalk_infousa        xwalk_altos
                    │                  │                  │
                    └──────────────────┼──────────────────┘
                                       │
                    ┌─────────────────────────────────────────┐
                    │        STAGE 3: AGGREGATION             │
                    │   make-rent-panel, make-occupancy-vars  │
                    │   make-evict-aggs, make-analytic-sample │
                    └─────────────────────────────────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    ▼                  ▼                  ▼
          ever_rentals_panel   evict_pid_year      parcel_occupancy
          bldg_panel_blp       analytic_sample
                    │                  │                  │
                    └──────────────────┼──────────────────┘
                                       │
                    ┌─────────────────────────────────────────┐
                    │     STAGE 3b: DEMOGRAPHIC IMPUTATION    │
                    │   impute-race-preflight → impute-race   │
                    │                        → impute-gender  │
                    └─────────────────────────────────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    ▼                  ▼                  ▼
          race_imputed_*      gender_imputed_*     diagnostics
                    │                  │                  │
                    └──────────────────┼──────────────────┘
                                       │
                    ┌─────────────────────────────────────────┐
                    │          STAGE 4: ANALYSIS              │
                    │   retaliatory-evictions, price-regs     │
                    └─────────────────────────────────────────┘
                                       │
                                       ▼
                               output/ (tables, figures)
```

---

## Full Pipeline Example

```bash
# 1. Set up config
cp config.example.yml config.yml
# Edit config.yml to set your data paths

# 2. Run cleaning (can be parallelized)
Rscript r/clean-eviction-addresses.R &
Rscript r/clean-parcel-addresses.R &
Rscript r/clean-license-addresses.R &
Rscript r/clean-altos-addresses.R &
Rscript r/clean-infousa-addresses.R &
wait

# 3. Build crosswalks (link datasets)
Rscript r/make_bldg_pid_xwalk.r
Rscript r/make-address-parcel-xwalk.R
Rscript r/merge-altos-parcels.R
Rscript r/merge-infousa-parcels.R

# 4. Aggregate to panels
Rscript r/make-rent-panel.R
Rscript r/make-occupancy-vars.r
Rscript r/make-evict-aggs.R
Rscript r/make-building-data.R
Rscript r/make-rental-building-data.R
Rscript r/make-analytic-sample.R

# 5. Demographic imputation
Rscript r/build-tract-race-priors.R
Rscript r/impute-race-preflight.R
Rscript r/impute-race.R &
Rscript r/impute-gender.R --sample-n=NA &
wait
Rscript r/race-imputation-diagnostics.R

# 6. Run analysis
Rscript r/retaliatory-evictions.r
Rscript r/price-regs.R

# 7. BLP demand estimation
/opt/anaconda3/envs/pyblp-env/bin/python python/pyblp_estimation.py
/opt/anaconda3/envs/pyblp-env/bin/python python/pyblp_estimation.py --iv full
/opt/anaconda3/envs/pyblp-env/bin/python python/pyblp_estimation.py --iv pid_year
/opt/anaconda3/envs/pyblp-env/bin/python python/pyblp_estimation.py --iv pid
```

---

## Logs and QA

All pipeline scripts write logs to `output/logs/`:
- `clean-eviction-addresses.log`
- `make-rent-panel.log`
- `make_bldg_pid_xwalk.log`
- etc.

Check logs for:
- Row counts before/after each step
- Key uniqueness assertions
- Merge diagnostics
- Warnings about dropped observations

---

## Script taxonomy (important)

The R scripts in `r/` fall into **five categories**.  
This is intentional and should be preserved.

### 1) `clean-*` — raw → standardized

**Purpose**
- Ingest raw administrative or commercial data
- Standardize addresses, names, identifiers, and dates
- Perform *no* aggregation beyond what is required for cleaning

**Typical inputs**
- Eviction summary tables / dockets
- OPA parcel data (GDB / shapefile)
- Business or rental license data
- Listing data (Altos, etc.)

**Typical outputs**
- `processed/*_clean.csv`

**Expectations**
- Deterministic address cleaning
- Explicit handling of units, suffixes, and street normalization
- No joins across datasets unless the purpose is cleaning (e.g. geocoding)

**Examples**
- `clean-eviction-addresses.R`
- `clean-parcel-addresses.R`
- `clean-license-addresses.R`
- `clean-altos-addresses.R`
- `clean-infousa-addresses.R`

---

### 2) `make-*` — cleaned → panels / aggregates

**Purpose**
- Build time panels, counts, rates, and indicators
- Collapse cleaned micro data into analysis-ready units

**Typical tasks**
- Construct PID × year (or year-quarter) panels
- Build "ever rental" indicators from license histories
- Aggregate filings to parcel, building, or ZIP level

**Typical outputs**
- `processed/*_panel.csv`

**Expectations**
- Explicit primary keys
- Clear time units (year vs year-quarter)
- No fuzzy matching at this stage
- Stable column naming

**Examples**
- `make-rent-panel.R`
- `make-occupancy-vars.r`
- `make-evict-aggs.R`
- `make-building-data.R`
- `make-analytic-sample.R`

---

### 3) `merge-*` — link datasets

**Purpose**
- Link previously cleaned or constructed datasets
- Produce crosswalks and merged analytic panels

**Typical tasks**
- Evictions ↔ parcels
- Licenses ↔ parcels
- Listings ↔ parcels

**Typical outputs**
- `processed/xwalk_*.csv`

**Expectations**
- Joins must be explicit and checked
- Many-to-many merges require aggregation or crosswalks
- Matching logic should be reproducible and logged
- Never silently drop unmatched observations

**Examples**
- `merge-altos-parcels.R`
- `merge-infousa-parcels.R`

---

### 4) `analysis/` — estimation & figures

**Purpose**
- Regressions, event studies, and descriptive analysis
- These scripts answer *specific* research questions

**Key rule**
> Analysis scripts must **only read from `processed/`**  
> and **only write to `output/`**

**Examples**
- `retaliatory-evictions.r`
- `price-regs.R`
- `altos-price-regs.R`
- `change-filing-patterns.R`
- `address-history-analysis.R`

---

### 5) `scratch/` — exploratory work

**Purpose**
- One-off exploration
- Prototyping
- Debugging

**Rules**
- Not part of the pipeline
- No guarantees of reproducibility
- Should not be depended on by `targets`

---

### 6) Misc / utility scripts

**Purpose**
- One-time data fetching or setup tasks
- Scripts that generate pipeline inputs (not intermediate products)

**Key rule**
> These scripts write to `data/inputs/` (not `processed/`).
> They are run once to bootstrap required input files and are not part of the regular pipeline DAG.

| Script | Outputs | Notes |
|--------|---------|-------|
| `build-acs-files.r` | `census/tenure_bg_{2000,2010}.csv`, `census/uis_tr_{2000,2010}.csv`, `census/bg_{2000,2010}.csv`, `census/hu_blocks_{2000,2010}.csv`, `shapefiles/philly_bg.shp` | Fetches decennial census + ACS data via `tidycensus` and block-group shapefile via `tigris`. Requires a Census API key. |

---

## Canonical data products (contract)

These outputs are the **backbone** of the repo.  
Analyses should be written to depend on these.

Recommended stable products:

- `processed/evictions_clean.csv`
- `processed/parcels_clean.csv`
- `processed/licenses_clean.csv`
- `processed/ever_rentals_panel.csv`
- `processed/parcel_occupancy_panel.csv`
- `processed/analytic_sample.csv`
- `processed/bldg_panel_blp.csv`

For each product, the pipeline should enforce:
- primary key(s)
- date coverage
- required columns
- no duplicate rows on keys

---

## Configuration & paths

All paths are controlled via `config.yml` (or env vars).

**Never** introduce:
- `~/Desktop/...`
- `/Users/...`
- machine-specific absolute paths

Use helpers from `R/config.R`, e.g.:

```r
cfg <- read_config()
fread(p_in(cfg, cfg$inputs$evictions_summary))
