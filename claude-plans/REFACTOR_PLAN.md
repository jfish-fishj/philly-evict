# Refactoring Plan: Philly Evictions Pipeline

## Executive Summary

This pipeline needs to transition from a "works on my machine" state to **publication-ready reproducibility**. The main issues are:

1. **Hardcoded paths everywhere** (e.g., `~/Desktop/data/philly-evict/...`)
2. **Flat data folder** with raw inputs mixed with derived products
3. **Config infrastructure exists but isn't used** by any scripts
4. **No logging or assertions** in data processing steps
5. **Inconsistent paradigms** (some scripts mix tidyverse + data.table)

---

## Current State Audit

### Data Folder (`/data/`)

```
data/
├── [RAW FILES - ~15 GB scattered at root]
│   ├── assessments.csv (424 MB)
│   ├── business_licenses_clean.csv (254 MB)
│   ├── evict_address_cleaned.csv (957 MB)
│   ├── infousa_address_cleaned.csv (5.2 GB)
│   ├── parcel_address_cleaned.csv (332 MB)
│   ├── philly-crime.csv (985 MB)
│   ├── philly-infousa.csv (3.7 GB)
│   ├── opa_properties_public.* (GDB + shapefiles)
│   ├── altos_*.csv
│   ├── philly_altos_*.csv
│   └── ... (many more)
├── census/           # Census data (bg, tenure, uis)
├── open-data/        # Open data portal downloads
├── phila-lt-data/    # Landlord-tenant court data (raw)
├── philly_hex_res*/  # Hex grid shapefiles
└── processed/        # Derived products (28 files, 23 GB)
```

**Problems:**
- Raw vs. intermediate vs. clean files not distinguished
- Some "cleaned" files at root level (e.g., `evict_address_cleaned.csv`)
- No clear provenance or versioning
- External data mixed with project outputs

### R Scripts (`/R/`)

| Category | Scripts | Config Usage | Hardcoded Paths |
|----------|---------|--------------|-----------------|
| clean-* | 7 files | None | Yes |
| make-* | 8 files | None | Yes |
| merge-* | 5 files | None | Yes |
| analysis | 4 files | None | Yes |
| helpers | 3 files | N/A | N/A |

**Example of current hardcoded paths (from `clean-eviction-addresses.R`):**
```r
philly_evict = fread("~/Desktop/data/philly-evict/phila-lt-data/summary-table.txt")
fwrite(philly_evict_adds_sample_m, "/Users/joefish/Desktop/data/philly-evict/evict_address_cleaned.csv")
```

---

## Proposed Data Folder Structure

```
data/
├── inputs/                          # RAW external data (read-only)
│   ├── evictions/
│   │   └── phila-lt-data/           # LT court exports (move from current location)
│   ├── parcels/
│   │   └── opa_properties_public.*  # OPA parcel data
│   ├── licenses/
│   │   └── business_licenses.csv
│   ├── listings/
│   │   ├── altos_*.csv
│   │   └── corelogic/
│   ├── census/                      # (move from data/census/)
│   ├── infousa/
│   │   └── philly-infousa.csv
│   ├── crime/
│   │   └── philly-crime.csv
│   ├── assessments/
│   │   └── assessments.csv
│   └── shapefiles/
│       ├── philly_bg.*
│       └── philly_hex_res*/
│
├── processed/                       # DERIVED products (pipeline outputs)
│   ├── clean/                       # Output of clean-* scripts
│   │   ├── evictions_clean.csv
│   │   ├── parcels_clean.csv
│   │   ├── licenses_clean.csv
│   │   ├── altos_clean.csv
│   │   └── infousa_clean.csv
│   ├── panels/                      # Output of make-* scripts
│   │   ├── ever_rentals_panel.csv
│   │   ├── parcel_occupancy_panel.csv
│   │   ├── evict_pid_year.csv
│   │   └── hex_panel_res*.csv
│   ├── xwalks/                      # Output of merge-* scripts
│   │   ├── evict_parcel_xwalk.csv
│   │   └── altos_parcel_xwalk.csv
│   └── analytic/                    # Final analysis-ready datasets
│       ├── analytic_sample.csv
│       └── bldg_panel_blp.csv
│
├── tmp/                             # Scratch files (safe to delete)
│
└── README.md                        # Data dictionary & provenance
```

---

## Phase 1: Data Folder Reorganization

### Step 1.1: Create directory structure
```bash
mkdir -p data/inputs/{evictions,parcels,licenses,listings,census,infousa,crime,assessments,shapefiles}
mkdir -p data/processed/{clean,panels,xwalks,analytic}
mkdir -p data/tmp
mkdir -p output/logs
```

### Step 1.2: Move raw inputs
| Current Location | New Location |
|------------------|--------------|
| `data/phila-lt-data/` | `data/inputs/evictions/phila-lt-data/` |
| `data/opa_properties_public.*` | `data/inputs/parcels/` |
| `data/business_licenses_clean.csv` | `data/inputs/licenses/` |
| `data/altos_*.csv` | `data/inputs/listings/` |
| `data/census/` | `data/inputs/census/` |
| `data/philly-infousa.csv` | `data/inputs/infousa/` |
| `data/philly-crime.csv` | `data/inputs/crime/` |
| `data/assessments.csv` | `data/inputs/assessments/` |
| `data/philly_bg.*` | `data/inputs/shapefiles/` |
| `data/philly_hex_res*/` | `data/inputs/shapefiles/` |

### Step 1.3: Move derived products
| Current Location | New Location |
|------------------|--------------|
| `data/evict_address_cleaned.csv` | `data/processed/clean/evictions_clean.csv` |
| `data/parcel_address_cleaned.csv` | `data/processed/clean/parcels_clean.csv` |
| `data/processed/ever_rentals_panel.csv` | `data/processed/panels/` |
| `data/processed/analytic_sample.csv` | `data/processed/analytic/` |
| ... etc. | |

### Step 1.4: Update config.yml
```yaml
paths:
  repo_root: "."
  input_dir: "data/inputs"
  processed_dir: "data/processed"
  output_dir: "output"
  tmp_dir: "data/tmp"

inputs:
  # Evictions
  evictions_summary_table: "evictions/phila-lt-data/summary-table.txt"
  evictions_party_names: "evictions/phila-lt-data/party-names-addresses.txt"
  evictions_docket_entries: "evictions/phila-lt-data/docket-entries.txt"

  # Parcels
  opa_gdb: "parcels/opa_properties_public.gdb"

  # Licenses
  business_licenses: "licenses/business_licenses.csv"

  # Listings
  altos_bedrooms: "listings/altos_year_bedrooms_philly.csv"
  altos_building: "listings/altos_year_building_philly.csv"

  # Census
  philly_bg_shp: "shapefiles/philly_bg.shp"
  tenure_bg_2010: "census/tenure_bg_2010.csv"

  # InfoUSA
  infousa_raw: "infousa/philly-infousa.csv"

  # Assessments
  assessments: "assessments/assessments.csv"

  # Crime
  crime: "crime/philly-crime.csv"

products:
  # Clean outputs
  evictions_clean: "clean/evictions_clean.csv"
  parcels_clean: "clean/parcels_clean.csv"
  licenses_clean: "clean/licenses_clean.csv"

  # Panels
  ever_rentals_panel: "panels/ever_rentals_panel.csv"
  parcel_occupancy_panel: "panels/parcel_occupancy_panel.csv"

  # Analytic
  analytic_sample: "analytic/analytic_sample.csv"
  bldg_panel_blp: "analytic/bldg_panel_blp.csv"
```

---

## Phase 2: Config Integration

### Step 2.1: Update `R/config.R` (minor enhancements)

Add product path helpers:
```r
# In addition to existing p_in(), p_proc(), p_out()
p_product <- function(cfg, key) {
  rel <- cfg$products[[key]]
  if (is.null(rel)) stop("Unknown product key: ", key)
  p_proc(cfg, rel)
}
```

### Step 2.2: Script template (new pattern)

Every script should follow this structure:
```r
## ============================================================
## clean-eviction-addresses.R
## ============================================================
## Inputs:  cfg$inputs$evictions_summary_table
## Outputs: cfg$products$evictions_clean
## Primary key: pm.uid (unique eviction filing ID)
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
})

source("R/config.R")
source("R/helper-functions.R")

# ---- Config & logging ----
cfg <- read_config()
log_file <- p_out(cfg, "logs", "clean-eviction-addresses.log")

logf("Starting clean-eviction-addresses.R", log_file = log_file)
logf("Input: ", p_in(cfg, cfg$inputs$evictions_summary_table), log_file = log_file)

# ---- Load data ----
philly_evict <- fread(p_in(cfg, cfg$inputs$evictions_summary_table))
logf("Loaded ", nrow(philly_evict), " rows", log_file = log_file)

# ---- Processing ----
# ... (existing logic)

# ---- Assertions ----
assert_unique(result, "pm.uid", "evictions_clean")
assert_has_cols(result, c("pm.uid", "n_sn_ss_c", "year", "pm.house", "pm.street"))

# ---- Write output ----
out_path <- p_product(cfg, "evictions_clean")
fwrite(result, out_path)
logf("Wrote ", nrow(result), " rows to ", out_path, log_file = log_file)
logf("Done.", log_file = log_file)
```

---

## Phase 3: Script Standardization (Priority Order)

### Tier 1: Core cleaning scripts (must fix first)
| Script | Input | Output | Status |
|--------|-------|--------|--------|
| `clean-eviction-addresses.R` | LT data | `evictions_clean.csv` | Hardcoded |
| `clean-parcel-addresses.R` | OPA GDB | `parcels_clean.csv` | Hardcoded |
| `clean-license-addresses.R` | Licenses | `licenses_clean.csv` | Hardcoded |

### Tier 2: Panel construction
| Script | Inputs | Output | Status |
|--------|--------|--------|--------|
| `make-rent-panel.R` | Cleaned data | `ever_rentals_panel.csv` | Hardcoded |
| `make-occupancy-vars.R` | Cleaned + InfoUSA | `parcel_occupancy_panel.csv` | TBD |
| `make-evict-aggs.R` | Cleaned evictions | `evict_pid_year.csv` | Hardcoded |

### Tier 3: Merge & analytic
| Script | Inputs | Output | Status |
|--------|--------|--------|--------|
| `merge-evictions-rental-listings.R` | Cleaned data | Xwalk files | Hardcoded |
| `make-analytic-sample.R` | All panels | `analytic_sample.csv` | Hardcoded |

### Tier 4: Analysis scripts (read-only from processed)
| Script | Notes |
|--------|-------|
| `price-regs.R` | Must only read from processed/, write to output/ |
| `altos-price-regs.R` | Same |
| `change-filing-patterns.R` | Same |

---

## Phase 4: Pipeline Orchestration

### Step 4.1: Update `_targets.R`

Currently uses `run_rscript()` which shells out. Once scripts are refactored:

```r
# Move from:
tar_target(
  clean_evictions_log,
  run_rscript("r/clean-eviction-addresses.R", ...),
  ...
)

# To function-based targets:
tar_target(
  evictions_clean,
  clean_evictions(cfg),
  format = "file"
)
```

### Step 4.2: Add validation targets

```r
tar_target(
  validate_evictions_clean,
  {
    dt <- fread(evictions_clean)
    assert_unique(dt, "pm.uid")
    assert_has_cols(dt, c("pm.uid", "n_sn_ss_c", "year"))
    stopifnot(nrow(dt) > 1e6)  # expected minimum
    TRUE
  }
)
```

---

## Checklist: What Constitutes "Done"

### Per-script checklist:
- [ ] Reads all paths from `cfg <- read_config()`
- [ ] Uses `p_in()`, `p_proc()`, `p_out()` for all file operations
- [ ] Has `assert_unique()` before and after joins
- [ ] Has `logf()` calls for start, key counts, and end
- [ ] No hardcoded paths (grep for `Desktop`, `/Users/`, `~`)
- [ ] Consistent paradigm (either data.table or tidyverse, not mixed)
- [ ] Documented inputs/outputs in header comment

### Pipeline checklist:
- [ ] `config.yml` exists and covers all inputs/products
- [ ] All scripts run via `Rscript R/script.R` from repo root
- [ ] `tar_make()` completes without error
- [ ] All logs written to `output/logs/`
- [ ] No files written outside `data/processed/` or `output/`

---

## Risk Areas / Watch Out For

1. **Join explosions**: Scripts like `make-rent-panel.R` do many merges. Add row count logging before/after each join.

2. **Address cleaning determinism**: The `postmastr` workflow in `clean-eviction-addresses.R` may have non-deterministic tie-breaking. Document or fix.

3. **Large files**: InfoUSA is 5+ GB. Consider `sample_mode` in config for development.

4. **External dependencies**: Some scripts may rely on data outside the repo. Audit all `fread()` calls.

5. **Analysis scripts writing to processed/**: Per CLAUDE.md rules, analysis must only read processed/, write to output/.

---

## Execution Order

1. **Phase 1** (Data reorg): ~2-3 hours of file moving
2. **Phase 2** (Config): ~1 hour to update config.R and config.yml
3. **Phase 3** (Scripts): ~1-2 hours per script, start with Tier 1
4. **Phase 4** (Targets): Once Tier 1-2 scripts done, wire up targets

Start with `clean-eviction-addresses.R` as the template, then apply the pattern to other scripts.
