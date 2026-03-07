# Transfer-Eviction Event Study: Implementation Plan

## Context

The existing `r/analyze-rtt-transfers.R` has a simple descriptive event study (Section 4c) that computes raw mean filing rates t-3 to t+3 around ownership transfers, stratified by buyer type (corporate/individual) and sheriff's deed status. It produces Table 11 and Figure 6.

This plan designs a proper regression-based event study with building fixed effects, heterogeneity by buyer type and portfolio size, and a landlord entity linkage pipeline. The goal is to answer: **do eviction rates change when a property changes hands, and does it depend on who buys it?**

Primary focus: larger apartment buildings (5+ units), with full sample as baseline.

---

## Deliverables

Two new scripts:

1. **`r/build-owner-linkage.R`** — Owner entity standardization and linkage (Phase 1: exact match on standardized names; Phase 2 deferred: fuzzy matching, mailing address)
2. **`r/analyze-transfer-evictions.R`** — Regression event study using the linkage output

---

## Part 1: Owner Linkage (`r/build-owner-linkage.R`)

### 1a. Name standardization

For each grantee name in `rtt_clean`:

1. `toupper()` + `trimws()`
2. Remove business suffix words using a curated list (LLC, INC, CORP, CORPORATION, LP, L.P., PARTNERSHIP, TRUST, HOLDINGS, MANAGEMENT, DEVELOPMENT, COMPANY, CO, ENTERPRISES, ASSOCIATES, ASSOC, GROUP, PROPERTIES, REALTY, REAL ESTATE, etc.) — similar to `make_business_short_name()` in `helper-functions.R` but **not using `clean_name()`** which has the documented bare-`|` bug (line 299 of analyze-rtt-transfers.R)
3. Remove trailing punctuation (periods, commas, dashes)
4. Collapse multiple whitespace to single space
5. Remove leading "THE"
6. Strip trailing Roman numerals (I, II, III, IV, V) — these are often LLC series identifiers, not person suffixes
7. Result: `owner_std` — the standardized name used for exact matching

For person names (non-corporate, per `business_regex`):
- Same uppercasing/trimming
- Remove titles (JR, SR, MR, MRS, DR) and suffixes (II, III, IV, ESQ)
- Remove middle initials
- Result: `owner_std` for persons

Store both `grantee_upper` (raw cleaned) and `owner_std` (shortened) so we can trace what was combined.

### 1b. Exact matching + transitive closure

After standardization, group all transfers sharing the same `owner_std` into an `owner_group_id`. Then handle **chain matches** (A matches B, B matches C → all get same group):

1. Build an edge list: `(owner_std_i, owner_std_j)` where `i` and `j` share the same raw `grantee_upper` variant or the same `owner_std`
2. Actually simpler: since we're doing exact match on `owner_std`, all names that map to the same `owner_std` are automatically in the same group. The transitive closure issue arises if we later add fuzzy matching. For now, **`owner_group_id` = unique ID assigned per distinct `owner_std`**.
3. But we still need to track the mapping: output a crosswalk `(grantee_upper, owner_std, owner_group_id, is_corp)` so every raw name maps to its group.

### 1c. Portfolio construction

For each `owner_group_id`, compute:
- `n_properties_total`: count of distinct PIDs ever acquired
- `n_properties_at_acquisition`: for each transfer, count of distinct PIDs acquired by this owner up to and including this transfer year
- `portfolio_bin`: Single-purchase / 2-4 / 5-9 / 10+

### 1d. Quality checks

**Automated QA (logged + written to output):**
- Top 50 owner groups by `n_properties_total` — write to CSV with all raw name variants, check that names look right
- Distribution of portfolio bins (how many single-purchase vs repeat buyers)
- Flag any `owner_std` that is very short (< 3 chars) or very generic ("PROPERTIES", "MANAGEMENT") — these may be over-merged
- Flag any raw name that maps to multiple `owner_std` values (shouldn't happen with deterministic standardization, but check)

**Random sample QA:**
- Draw 50 random owner groups with 2+ properties, write to CSV with all raw names and PIDs for manual inspection
- Draw 50 random single-purchase owners for comparison

### 1e. Output

| File | Description |
|------|-------------|
| `data/processed/xwalks/owner_linkage_xwalk.csv` | Crosswalk: `grantee_upper`, `owner_std`, `owner_group_id`, `is_corp` |
| `data/processed/xwalks/owner_portfolio.csv` | Portfolio: `owner_group_id`, `owner_std`, `n_properties_total`, first/last acquisition year |
| `output/qa/owner_linkage_top50.csv` | Top 50 owners by portfolio size with all raw name variants |
| `output/qa/owner_linkage_random_sample.csv` | Random sample for manual review |
| `output/qa/owner_linkage_qa.txt` | QA summary stats |
| `output/logs/build-owner-linkage.log` | Log file |

Config keys to add: `owner_linkage_xwalk`, `owner_portfolio` under `products:`.

### 1f. Deferred (Phase 2)

- Fuzzy matching on `owner_std` (Jaro-Winkler, deterministic tie-breaking per CLAUDE.md 3B)
- Mailing address as secondary match variable
- Transitive closure across fuzzy edges (union-find algorithm)
- Entity resolution for common LLC patterns (e.g., "123 MAIN ST" as both an LLC name and an address)

---

## Part 2: Event Study Analysis (`r/analyze-transfer-evictions.R`)

### Section 0: Setup & Data Loading

Following the pattern from `analyze-rtt-transfers.R` lines 1-80:

- Source `r/config.R`, `r/helper-functions.R`
- Load: `rtt_clean`, `bldg_panel_blp`, `owner_linkage_xwalk`, `owner_portfolio`
- `normalize_pid()` both datasets
- Output: `p_out(cfg, "transfer_evictions")`, figures to `figs/`
- Log file: `output/logs/analyze-transfer-evictions.log`

### Section 1: Build Transfer Event Panel

1. **Collapse RTT to (PID, year) level.** Multiple transfers in same PID-year: keep the one with highest `total_consideration`. Log how many are collapsed.

2. **Merge owner linkage.** Join `owner_linkage_xwalk` on `grantee_upper` to get `owner_group_id`, `is_corp`. Join `owner_portfolio` to get `n_properties_total`, `portfolio_bin`.

3. **Expand to event-time panel t-3 to t+5.** Vectorized via `CJ()`:
   ```r
   event_grid <- transfers[, .(event_time = -3L:5L), by = .(PID, transfer_year = year)]
   event_grid[, year := transfer_year + event_time]
   ```

4. **Merge building outcomes.** From `bldg_panel_blp`: `num_filings`, `total_units`, `total_violations`, `total_complaints`, `med_rent`, `GEOID`, `building_type`, `num_units_bin`.

5. **Compute fresh filing rate:** `filing_rate := num_filings / total_units` (not the EB-smoothed version — we need actual annual rates).

6. **Handle overlapping event windows.** Flag buildings with multiple transfers where windows overlap. Keep all (stacked) for main analysis; first-transfer-only as robustness.

7. **Define subsamples:**
   - Full sample
   - Large buildings: `total_units >= 5` (or `num_units_bin %in% c("5-9", "10-19", "20-49", "50+")`)
   - Exclude sheriff's deeds

8. **Assertions:** `assert_unique()` on `(PID, transfer_year, event_time)`, log coverage rates.

### Section 2: Descriptive Event Study Plots (raw means)

Before regressions, visualize raw patterns:

| Figure | Description |
|--------|-------------|
| `rtt_eviction_event_study_raw.png` | Mean filing rate by event time, full sample, with 95% CI |
| `rtt_eviction_event_study_raw_by_buyer.png` | Corporate vs Individual buyer |
| `rtt_eviction_event_study_raw_by_portfolio.png` | By portfolio bin |
| `rtt_eviction_event_study_raw_by_size.png` | Large (5+ units) vs small buildings |

All use `theme_philly_evict()`, `geom_line() + geom_ribbon()`, reference line at t=0.

### Section 3: Baseline Regression Event Study

Using `fixest::feols` with `i(event_time, ref = -1)`:

| Spec | Formula | Sample | FE |
|------|---------|--------|----|
| (1) | `filing_rate ~ i(event_time, ref=-1) \| PID + year` | Full | Building + Year |
| (2) | Same | Full | Building + GEOID x Year |
| (3) | Same as (1) | `total_units >= 5` | Building + Year |
| (4) | Same as (2) | `total_units >= 5` | Building + GEOID x Year |

Cluster SEs at PID level throughout.

**Output:**
- `transfer_evictions_baseline_coefs.csv` — tidy coefficients
- `rtt_eviction_event_study_baseline.png` — coefficient plot (point + errorbar)
- `transfer_evictions_baseline_etable.tex` — LaTeX via `etable()`

### Section 4: Heterogeneity by Buyer Type

Two approaches:

**A) Interaction model** (tests significance of difference):
```r
feols(filing_rate ~ i(event_time, buyer_is_corp, ref = -1) | PID + year, ...)
```

**B) Split-sample models** (clean coefficient plots per group):
- Corporate buyers only
- Individual buyers only

Run both for full sample and large-buildings subsample.

**Output:**
- `transfer_evictions_buyer_type_coefs.csv`
- `rtt_eviction_event_study_by_buyer.png`
- `transfer_evictions_buyer_type_etable.tex`

### Section 5: Heterogeneity by Portfolio Size

Split-sample models by `portfolio_bin`:
- Single-purchase buyers
- 2-4 properties
- 5-9 properties
- 10+ properties

Log cell counts per `(portfolio_bin, event_time)` and flag small cells.

**Output:**
- `transfer_evictions_portfolio_coefs.csv`
- `rtt_eviction_event_study_by_portfolio.png`
- `transfer_evictions_portfolio_etable.tex`

### Section 6: Robustness

| Check | Modification |
|-------|-------------|
| No sheriff's deeds | Drop `is_sheriff_deed == TRUE` |
| First transfer only | Keep earliest transfer per PID |
| Count outcome | `num_filings` instead of rate |
| Violations outcome | `total_violations` as outcome |
| Pre-COVID only | `year <= 2019` |

Each produces coefficients stacked into `transfer_evictions_robustness_coefs.csv` with a `spec` column.

Figure: faceted robustness comparison plot.

### Section 7: Descriptives and QA

- Transfer sample descriptives by buyer type and portfolio bin
- Pre-transfer balance table: compare pre-period filing rates, violations, rent, units across buyer groups
- Coverage QA: fraction of event-time cells with non-missing outcomes by event_time
- Write to `transfer_evictions_descriptives.csv`, `transfer_evictions_balance.csv`, `transfer_evictions_qa.txt`

---

## Implementation Order

1. **First: `r/build-owner-linkage.R`** — name standardization, exact matching, portfolio construction, QA outputs. This is the data construction prerequisite.
2. **Second: `r/analyze-transfer-evictions.R`** — event study analysis consuming the linkage output.
3. **Add config keys** for new data products (`owner_linkage_xwalk`, `owner_portfolio`).
4. **Update `docs/data-products.md`** with new product schemas.

---

## Verification

1. Run `r/build-owner-linkage.R` — check log, inspect `owner_linkage_top50.csv` and random sample for name quality
2. Run `r/analyze-transfer-evictions.R` — check log for row counts, coverage rates, small-cell warnings
3. Inspect baseline event study plot: pre-trends should be roughly flat (parallel trends)
4. Compare raw-means plots to regression plots — should tell similar story
5. Check that portfolio heterogeneity results are driven by substantive differences, not small cells

---

## Key Files

| File | Role |
|------|------|
| `r/analyze-rtt-transfers.R` | Template for data loading, buyer classification, event panel, `extract_feols_coefs()` |
| `r/helper-functions.R` | `business_regex` (line 487), `make_business_short_name()`, `theme_philly_evict()`, `logf()` |
| `r/config.R` | `read_config()`, `p_product()`, `p_out()` |
| `config.yml` | Add new product keys |
| `data/processed/clean/rtt_clean.csv` | Transfer data input |
| `data/processed/analytic/bldg_panel_blp.csv` | Building panel input |
