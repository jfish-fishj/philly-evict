# Quarterly Transfer-Eviction Event Study

**Script:** `r/analyze-transfer-evictions-quarterly.R`
**Writeup:** `writeups/transfer-evictions-quarterly.qmd`
**Output directory:** `output/transfer_evictions/`
**Prerequisite:** `r/build-owner-linkage.R` (produces owner crosswalk and portfolio data)

---

## Motivation

The annual event study (`analyze-transfer-evictions.R`) shows a suspicious jump in filing rates from t=-2 to t=-1 (the year before transfer). Monthly investigation of raw filing dates reveals the true pattern: **filings decline in the 3--6 months before transfer and bottom at the transfer date**, then recover afterward. Annual binning creates artifacts because t=-1 mixes the elevated pre-decline period with the declining period, depending on when in the year the transfer occurs.

This quarterly analysis uses exact transfer dates and day-level filing dates to correctly identify the timing of filing rate changes around ownership transfers.

---

## Input Data

All paths resolved via `config.yml` through `read_config()`.

| Dataset | Config key | Key columns used |
|---------|-----------|-----------------|
| `rtt_clean` | `rtt_clean` | `PID`, `display_date` (exact transfer date), `grantees`, `total_consideration`, `document_type`, `year` |
| `evictions_clean` | `evictions_clean` | `n_sn_ss_c` (case ID), `d_filing` (day-level filing date) |
| `evict_address_xwalk` | `evict_address_xwalk` | Links eviction case IDs to PIDs; restricted to unique parcel matches (`n_parcel_matches == 1`) |
| `bldg_panel_blp` | `bldg_panel_blp` | `PID`, `year`, `total_units`, `num_filings`, `GEOID`, `num_units_bin`, `ever_rental_any_year_ever`, `filing_rate_eb_pre_covid` |
| `owner_linkage_xwalk` | `owner_linkage_xwalk` | `grantee_upper` -> `owner_group_id`, `is_corp` |
| `owner_portfolio` | `owner_portfolio` | `owner_group_id` -> `n_properties_total`, `portfolio_bin` |

---

## Data Construction Pipeline

### Section 0: Loading

Loads all six input datasets. Normalizes PIDs to 9-character zero-padded strings.

### Section 1: Quarterly eviction counts

1. Parse `d_filing` from `evictions_clean` to extract filing year, month, quarter
2. Link to PIDs via `evict_address_xwalk` (inner join on case ID, restricted to unique-parcel matches)
3. **Pre-COVID restriction**: drop all filings after 2019 (`filing_year <= 2019`)
4. Aggregate to PID x quarter cells: `ev_q[PID, year_quarter] -> num_filings_q`

### Section 2: Building the event panel

This is the core merge pipeline. Steps:

**Step 2a: Collapse RTT to PID-quarter level.**
Multiple transfers of the same PID in the same quarter are collapsed to one row (keeping highest `total_consideration`). This avoids double-counting.

**Step 2b: Merge owner linkage.**
- Join `owner_linkage_xwalk` on `grantee_upper` to get `owner_group_id` and `is_corp`
- Join `owner_portfolio` on `owner_group_id` to get `n_properties_total` and `portfolio_bin`
- Match rate is ~100% (all grantees appear in the crosswalk)

**Step 2c: Compute pre-acquisition temporal filing rate.**
For each transfer of PID_i by owner_j (transfer year = Y):
1. Identify all OTHER PIDs that owner_j acquired (via `owner_group_id`)
2. From `bldg_panel_blp`, compute `mean(num_filings / total_units)` for those other PIDs in years strictly < Y
3. This is `acq_rate` -- the acquirer's revealed filing behavior before this acquisition

Implementation uses a vectorized cross-join approach:
- Build `bldg_annual`: PID x year annual filing rates from bldg_panel
- Build `owner_pids_temporal`: transfers with owner info
- Cross-join owners to their other PIDs
- Merge with `bldg_annual` for pre-transfer years only
- Aggregate to (owner_group_id, PID, transfer_year_quarter) level

Classify acquirers into four bins using median-split on `acq_rate`:
- **High-filer portfolio**: `acq_rate > median` (among acquirers with rate > 0)
- **Low-filer portfolio**: `0 < acq_rate <= median`
- **Non-filer (has portfolio)**: `acq_rate == 0` but has other properties
- **Single-purchase**: no other matched properties (n_other == 0)

**Step 2d: Expand to event-time panel.**
Each transfer is expanded to 33 quarters: q in {-12, ..., +20} (3 years before, 5 years after). Calendar dates for each relative quarter are computed by adding `q * 91 days` to the actual transfer date, then mapping to YYYY-QN format.

**Step 2e: Merge outcomes.**
- Merge `ev_q` (quarterly filing counts) on `(PID, year_quarter)`. Missing = 0 filings.
- Merge `bldg_panel_blp` columns (`total_units`, `GEOID`, etc.) on `(PID, year)`.
- Compute `filing_rate_q = num_filings_q / total_units` where total_units available.

**Step 2f: Merge transfer characteristics.**
Transfer-level variables (`buyer_type`, `is_corp`, `is_sheriff_deed`, `portfolio_bin`, etc.) and acquirer classification (`acq_filer_bin`, `acq_rate`, `n_other`) are merged onto the event panel.

**Step 2g: Construct derived variables.**
- `size_bin`: 1 unit / 2-4 / 5-9 / 10+ (from `total_units`)
- `known_rental`: TRUE if `ever_rental_any_year_ever == 1` or `filing_rate_eb_pre_covid > 0`
- `large_building`: `total_units >= 5`
- `has_overlap`: PID has multiple transfers with overlapping event windows

### Merge diagram

```
rtt_clean (675K rows)
  |-- collapse to PID-quarter --> transfers (661K)
  |-- merge owner_linkage_xwalk on grantee_upper --> +owner_group_id, is_corp
  |-- merge owner_portfolio on owner_group_id --> +n_properties_total, portfolio_bin
  |-- cross-join for pre-acq filing rate --> +acq_rate, acq_filer_bin
  |-- expand q={-12..+20} --> event_grid (21.8M rows)
        |-- merge ev_q on (PID, year_quarter) --> +num_filings_q
        |-- merge bldg_panel_blp on (PID, year) --> +total_units, GEOID, etc.
        |-- merge transfer_chars on (PID, transfer_year_quarter) --> +buyer_type, etc.
        |-- merge acq_lookup on (PID, transfer_year_quarter) --> +acq_filer_bin, etc.
        --> event_panel (21.8M rows, 352K PIDs x 33 quarters)
```

### Key counts (typical run)

| Quantity | Count |
|----------|-------|
| RTT transfers (PID-quarter) | 661,376 |
| Unique PIDs | 352,770 |
| Eviction filings linked to PID (pre-COVID) | 468,233 |
| Event panel rows | 21,825,408 |
| total_units coverage | ~35% |
| PIDs with multiple transfers | 181,089 (51%) |
| Known rentals | 77,801 (12%) |

---

## Specifications

All regressions use `fixest::feols()` with standard errors clustered at the PID level. The outcome is `filing_rate_q = num_filings_q / total_units` (quarterly filing rate per unit). Reference period is q = -4 (one year before transfer), chosen because the pre-transfer dip begins around q = -1.

### Section 4: Baseline event study (8 specs)

| Spec | Sample | FE | Weights |
|------|--------|----|---------|
| 1 | Full | PID + yq | None |
| 2 | Full | PID + yq | total_units |
| 3 | Full | PID + GEOID x yq | None |
| 4 | 5+ units | PID + yq | None |
| 5 | 5+ units | PID + yq | total_units |
| 6 | Known rental 5+ | PID + yq | None |
| 7 | 2+ units | PID + yq | None |
| 8 | Known rental (all) | PID + yq | None |

All 8 specs are plotted on a single faceted figure (`rtt_eviction_quarterly_baseline.png`).

### Section 5: Building size heterogeneity

Separate regressions for each size bin (1 unit, 2-4, 5-9, 10+), all with PID + yq FE. Faceted plot.

### Section 6: Buyer type heterogeneity

Split-sample corporate vs individual for both 5+ and 2+ unit buildings. Four specs total, faceted.

### Section 7: Acquirer filing rate heterogeneity

Separate regressions for each acquirer bin (High-filer, Low-filer, Non-filer, Single-purchase). Default sample is **2+ unit buildings** (drops single-family to avoid vacancy artifacts). Also run for 5+ units.

### Section 8: Portfolio size heterogeneity

Separate regressions by portfolio bin (Single-purchase, 2-4, 5-9, 10+). Default is 2+ units. Also run for 5+ units.

---

## Key Findings

### 1. The annual t=-1 artifact is explained

Filing rates peak at q=-2, then drop sharply at q=-1 and bottom at q=0. The annual analysis bins these together with the elevated pre-decline period, creating a misleading jump.

### 2. The pre-transfer dip is a small-building vacancy effect

For single-unit buildings, the pre-transfer dip is very deep (consistent with vacancy during sale). For 10+ unit buildings, filing rates are stable through the transition. This confirms the dip reflects mechanical vacancy, not an operational pattern.

### 3. Post-transfer filing rate increase is real and persistent

After the transfer quarter, filing rates recover and stabilize at approximately 35% above the pre-transfer baseline (q=+4 onward). This appears across building sizes and is robust to GEOID x yq FE.

### 4. High-filer acquirers drive the post-transfer increase

The ~6% of transfers going to high-filer portfolio acquirers show the largest post-transfer filing increases. Single-purchase acquirers show smaller or no increases. This is consistent with experienced landlords implementing their established eviction practices at newly acquired properties.

### 5. Sorting into high-eviction properties

High-filer portfolio acquirers disproportionately buy already-high-evicting properties (14.7% of above-median property transfers vs 10.2% of below-median), consistent with a market for high-eviction buildings.

### 6. Corporate buyers show larger effects (for 5+ units)

Among 5+ unit buildings, corporate buyers show larger post-transfer filing rate increases than individual buyers.

### 7. Single-purchase corporate investigation

About 81K transfers are classified as "single-purchase" but have corporate grantees. Investigation of a random sample of 1,000 shows:
- ~55% have their PID in the building panel
- ~51% of grantees appear on >1 PID in RTT (they DO have multiple deeds, just under different names or not linked by the owner standardization)
- ~52% of owner groups have >1 PID in the transfers table
- Median of 2 deeds in RTT per grantee, but mean of ~100 (heavily right-skewed)

This suggests the "single-purchase" classification is conservative -- many of these corporate buyers likely have larger portfolios that aren't captured by exact name matching.

---

## Output Files

### Coefficient CSVs

| File | Contents |
|------|----------|
| `transfer_evictions_quarterly_baseline_coefs.csv` | 8 baseline specs |
| `transfer_evictions_quarterly_size_coefs.csv` | 4 size-bin specs |
| `transfer_evictions_quarterly_buyer_coefs.csv` | 4 buyer-type specs |
| `transfer_evictions_quarterly_acq_filer_coefs.csv` | Acquirer filing rate specs (2+ units) |
| `transfer_evictions_quarterly_acq_filer_5plus_coefs.csv` | Acquirer filing rate specs (5+ units) |
| `transfer_evictions_quarterly_portfolio_coefs.csv` | Portfolio size specs (2+ units) |
| `transfer_evictions_quarterly_portfolio_5plus_coefs.csv` | Portfolio size specs (5+ units) |

All coefficient CSVs have columns: `term`, `estimate`, `std_error`, `t_value`, `p_value`, `spec`, `n`, `r2`.

### Descriptive CSVs

| File | Contents |
|------|----------|
| `transfer_evictions_quarterly_raw_means.csv` | Raw means by q_relative (all buildings) |
| `transfer_evictions_quarterly_raw_means_binary.csv` | Raw means, 1-unit vs multi-unit |
| `transfer_evictions_quarterly_acq_bin_stats.csv` | Acquirer bin stats at q=0 (2+ and 5+) |
| `transfer_evictions_quarterly_acq_descriptives.csv` | Full acquirer descriptives |
| `transfer_evictions_quarterly_portfolio_bin_stats.csv` | Portfolio bin stats at q=0 |
| `transfer_evictions_quarterly_transfer_rate_by_evict_class.csv` | Transfer rates by property eviction class |
| `transfer_evictions_quarterly_buyer_by_prop_evict_class.csv` | Buyer characteristics by property class |
| `transfer_evictions_quarterly_prop_x_acq_crosstab.csv` | Property class x acquirer type cross-tab |
| `transfer_evictions_quarterly_corp_by_filer_bin.csv` | Corporate/non-corporate within each filing bin |
| `transfer_evictions_quarterly_sp_corp_investigation.csv` | Single-purchase corporate sample investigation summary |
| `transfer_evictions_quarterly_sp_corp_sample_detail.csv` | Individual records from SP corp sample |

### Figures

| File | Contents |
|------|----------|
| `rtt_eviction_quarterly_raw_profile.png` | Raw means, full sample |
| `rtt_eviction_quarterly_raw_1v_multi.png` | Raw means, 1-unit vs multi-unit |
| `rtt_eviction_quarterly_by_size_4way.png` | Raw means by 4-way size split |
| `rtt_eviction_quarterly_baseline.png` | Baseline event study (8 specs, faceted) |
| `rtt_eviction_quarterly_by_size.png` | Size heterogeneity (faceted) |
| `rtt_eviction_quarterly_by_buyer.png` | Buyer type heterogeneity (faceted) |
| `rtt_eviction_quarterly_by_acq_filer.png` | Acquirer filing rate (2+ units, faceted) |
| `rtt_eviction_quarterly_by_acq_filer_5plus.png` | Acquirer filing rate (5+ units, faceted) |
| `rtt_eviction_quarterly_by_portfolio.png` | Portfolio size (2+ units, faceted) |
| `rtt_eviction_quarterly_by_portfolio_5plus.png` | Portfolio size (5+ units, faceted) |

### Other

| File | Contents |
|------|----------|
| `transfer_evictions_quarterly_qa.txt` | QA summary (sample sizes, coverage, classification details) |
| `output/logs/analyze-transfer-evictions-quarterly.log` | Full run log |

---

## Design Decisions

### Why q=-4 as reference period (not q=-1)?

The pre-transfer dip begins around q=-1, making it a poor reference point. Using q=-4 (one year before transfer) provides a stable baseline in the pre-trend region.

### Why 2+ units as the default heterogeneity sample?

Single-family buildings dominate the full sample (~82% of transfers) and exhibit a large vacancy artifact (deep pre-transfer dip). Restricting to 2+ units removes this confound while retaining smaller multi-family properties. The 5+ unit subsample is also reported but has much smaller N.

### Why pre-acquisition temporal filing rates (not LOO EB)?

The original implementation used leave-one-out (LOO) empirical Bayes smoothed pre-COVID filing rates (`filing_rate_eb_pre_covid`) to classify acquirers. This was replaced with actual pre-acquisition temporal rates because:
1. **No look-ahead bias**: only uses filing data from years strictly before the transfer
2. **Time-varying**: captures the acquirer's behavior at the time of acquisition, not a lifetime average
3. **Simpler**: avoids the EB smoothing machinery
4. **Similar results**: the temporal and LOO classifications produce nearly identical event study patterns

### Why cluster at PID (not owner)?

The building is the unit of treatment (the transfer happens to a specific PID). PID clustering accounts for serial correlation in filing rates within a building. Owner-level clustering would also be valid but would be more conservative and harder to justify theoretically.

### Pre-COVID restriction

All filing rate outcomes use filings through 2019Q4 only. The pandemic disrupted court operations and filing patterns, making post-2019 data unreliable for measuring landlord eviction behavior.
