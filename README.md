# Philly Evictions Pipeline

This repository contains the data processing and analysis pipeline for Philadelphia eviction research.

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

---

## Script Workflow

The pipeline follows a strict **clean → make → merge → analyze** order.
Each stage depends on outputs from the previous stage.

### Stage 1: Cleaning (`clean-*`)

Clean raw data, standardize addresses, and prepare for linking.

| Script | Inputs | Outputs | Notes |
|--------|--------|---------|-------|
| `clean-eviction-addresses.R` | `evictions_summary_table`, `evictions_property_addresses` | `evictions_clean.csv` | Canonicalizes addresses, parses docket numbers |
| `clean-parcel-addresses.R` | `opa_gdb` or `opa_shp` | `parcels_clean.csv` | Standardizes OPA parcel addresses |
| `clean-license-addresses.R` | `business_licenses` | `licenses_clean.csv` | Cleans rental license addresses |
| `clean-altos-addresses.R` | `altos_metro_raw` | `altos_clean.csv` | Cleans listing addresses |
| `clean-corelogic-addresses.R` | `corelogic_philly_raw` | `corelogic_clean.csv` | Cleans deed/mortgage addresses |
| `clean-infousa-addresses.R` | `infousa_raw` | `infousa_clean.csv` | Cleans business directory addresses |
| `clean-eviction-names.R` | `evictions_party_names` | `eviction_names_clean.csv` | Standardizes plaintiff/defendant names |

**Run order:** These scripts are independent and can run in parallel.

### Stage 2: Building Crosswalks (`make-*` spatial)

Build spatial crosswalks linking addresses to parcels.

| Script | Inputs | Outputs | Notes |
|--------|--------|---------|-------|
| `make_bldg_pid_xwalk.r` | `philly_parcels_sf`, `building_footprints_shp` | `building_pid_xwalk.csv`, `parcel_building_summary.csv` | Maps building footprints to parcels |
| `make-address-parcel-xwalk.R` | `parcels_clean`, `evictions_clean`, `altos_clean`, `infousa_clean` | `xwalk_evictions_to_parcel.csv`, `xwalk_altos_to_parcel.csv`, etc. | Fuzzy address matching |

**Dependencies:** Requires Stage 1 clean outputs.

### Stage 3: Panel Construction (`make-*`)

Build analysis-ready panels at various aggregation levels.

| Script | Inputs | Outputs | Primary Key |
|--------|--------|---------|-------------|
| `make-rent-panel.R` | `parcels_clean`, `licenses_clean`, `building_pid_xwalk` | `ever_rentals_panel.csv`, `ever_rentals_pid.csv` | PID |
| `make-occupancy-vars.r` | `ever_rentals_panel`, `infousa_clean` | `parcel_occupancy_panel.csv` | PID × year |
| `make-evict-aggs.R` | `evictions_clean`, `xwalk_evictions_to_parcel` | `evict_pid_year.csv`, `evict_zip_year.csv` | PID × year |
| `make-building-data.R` | `parcels_clean`, `evict_pid_year` | `bldg_panel.csv` | PID × year_quarter |
| `make-hexagons.r` | `philly_parcels_sf`, hex shapefiles | hex panel shapefiles | hex_id |
| `assign-evictions-crimes-homes-to-hexagon.r` | hex panels, `evictions_clean`, `crime` | `hex_panel_res{N}.csv` | hex_id × year_quarter |

**Dependencies:** Requires Stage 1 clean outputs and Stage 2 crosswalks.

### Stage 4: Merging Datasets (`merge-*`)

Link cleaned datasets and produce merged analytic files.

| Script | Inputs | Outputs | Notes |
|--------|--------|---------|-------|
| `merge-evictions-rental-listings.R` | `evictions_clean`, `altos_clean`, crosswalks | merged eviction-listing panel | Links evictions to rental listings |
| `merge-altos-rental-listings.R` | `altos_clean`, `parcels_clean` | `altos_parcel_xwalk.csv` | Links listings to parcels |
| `merge-altos-corelogic.R` | `altos_clean`, `corelogic_clean` | `altos_corelogic_xwalk.csv` | Links listings to deeds |
| `merge-infousa-parcels.R` | `infousa_clean`, `parcels_clean` | `xwalk_infousa_to_parcel.csv` | Links businesses to parcels |

**Dependencies:** Requires Stages 1-3 outputs.

### Stage 5: Analytic Sample (`make-analytic-sample.R`)

Combines all panels into the final analysis-ready dataset.

| Script | Inputs | Outputs | Primary Key |
|--------|--------|---------|-------------|
| `make-analytic-sample.R` | `parcel_occupancy_panel`, `evict_pid_year`, `bldg_panel` | `analytic_sample.csv` | PID × year |

**Dependencies:** Requires Stages 1-4 outputs.

### Stage 6: Analysis (`analysis/` or `r/*.R` analysis scripts)

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
            altos_clean         corelogic_clean    infousa_clean
                    │                  │                  │
                    └──────────────────┼──────────────────┘
                                       │
                    ┌─────────────────────────────────────────┐
                    │      STAGE 2: SPATIAL CROSSWALKS        │
                    │  make_bldg_pid_xwalk, make-address-xwalk│
                    └─────────────────────────────────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    ▼                  ▼                  ▼
          building_pid_xwalk   xwalk_evictions     xwalk_altos
                    │                  │                  │
                    └──────────────────┼──────────────────┘
                                       │
                    ┌─────────────────────────────────────────┐
                    │         STAGE 3: MAKE PANELS            │
                    │   make-rent-panel, make-occupancy-vars  │
                    │   make-evict-aggs, make-building-data   │
                    └─────────────────────────────────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    ▼                  ▼                  ▼
          ever_rentals_panel   evict_pid_year      bldg_panel
          parcel_occupancy     evict_zip_year
                    │                  │                  │
                    └──────────────────┼──────────────────┘
                                       │
                    ┌─────────────────────────────────────────┐
                    │           STAGE 4: MERGE                │
                    │         (merge-* scripts)               │
                    └─────────────────────────────────────────┘
                                       │
                                       ▼
                    ┌─────────────────────────────────────────┐
                    │     STAGE 5: ANALYTIC SAMPLE            │
                    │       make-analytic-sample.R            │
                    └─────────────────────────────────────────┘
                                       │
                                       ▼
                              analytic_sample.csv
                                       │
                    ┌─────────────────────────────────────────┐
                    │          STAGE 6: ANALYSIS              │
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
wait

# 3. Build spatial crosswalks
Rscript r/make_bldg_pid_xwalk.r
Rscript r/make-address-parcel-xwalk.R

# 4. Build panels
Rscript r/make-rent-panel.R
Rscript r/make-occupancy-vars.r
Rscript r/make-evict-aggs.R
Rscript r/make-building-data.R

# 5. Merge datasets
Rscript r/merge-altos-rental-listings.R
Rscript r/merge-evictions-rental-listings.R

# 6. Build analytic sample
Rscript r/make-analytic-sample.R

# 7. Run analysis
Rscript r/retaliatory-evictions.r
Rscript r/price-regs.R
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
- Listing data (Altos, CoreLogic, etc.)

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
- `clean-corelogic-addresses.R`

---

### 2) `make-*` — cleaned → panels / aggregates

**Purpose**
- Build time panels, counts, rates, and indicators
- Collapse cleaned micro data into analysis-ready units

**Typical tasks**
- Construct PID × year (or year-quarter) panels
- Build “ever rental” indicators from license histories
- Aggregate filings to parcel, building, ZIP, or hexagon level
- Create spatial panels (H3 hexagons and rings)

**Typical outputs**
- `processed/*_panel.csv`
- `processed/*_hex_res{RES}.csv`

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
- `make-hexagons.r`
- `assign-evictions-crimes-homes-to-hexagon.r`

---

### 3) `merge-*` — link datasets

**Purpose**
- Link previously cleaned or constructed datasets
- Produce crosswalks and merged analytic panels

**Typical tasks**
- Evictions ↔ parcels
- Licenses ↔ parcels
- Listings ↔ parcels
- Altos ↔ CoreLogic

**Typical outputs**
- `processed/xwalk_*.csv`
- `processed/*_merged.csv`

**Expectations**
- Joins must be explicit and checked
- Many-to-many merges require aggregation or crosswalks
- Matching logic should be reproducible and logged
- Never silently drop unmatched observations

**Examples**
- `merge-evictions-rental-listings.R`
- `merge-altos-rental-listings.R`
- `merge-altos-corelogic.R`
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

## Canonical data products (contract)

These outputs are the **backbone** of the repo.  
Analyses should be written to depend on these.

Recommended stable products:

- `processed/evictions_clean.csv`
- `processed/parcels_clean.csv`
- `processed/licenses_clean.csv`
- `processed/ever_rentals_panel.csv`
- `processed/parcel_occupancy_panel.csv`
- `processed/parcel_events_panel_pid_year.csv`
- `processed/analytic_sample.csv`
- `processed/bldg_panel_blp.csv`
- `processed/hex_panel_res{RES}.csv`

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
