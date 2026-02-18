# Occupancy and Share Construction Pipeline

## Overview

This document describes how rental occupancy and BLP market shares are constructed, from raw InfoUSA/Census data through to the estimation-ready `bldg_panel_blp`.

## 1. `make-occupancy-vars.r` — Occupancy Panel

### Inputs
- **InfoUSA address-level data**: `num_households`, `num_homeowners` by PID-year
- **Census 2010 BG-raked estimates**: `renters_2010` (building-level renter counts calibrated to 2010 Census block group totals; see Section 1a below)
- **Parcel attributes**: `year_built`, `num_units_base`, `total_units_2010`

### `total_units`
- For buildings built <= 2010: uses `total_units_2010` (selectively BG-raked stock measure)
- For buildings built > 2010: uses `num_units_base` (imputed from model), floored at 1

Safeguards to control unit counts:
- Unit imputation is done at **address cluster level** (`n_sn_ss_c`) to avoid condo parcel-split bias from PID-level labels.
- Address-level model includes `log_mkt_value_addr` and household-aware lower bounds.
- `structure_bin` gets a household-informed floor (e.g., high-household parcels cannot stay in `u_1_attached`).
- **Structure bin refinement** (added 2026-02-17): After `num_units_base` is computed, `structure_bin` is overridden using actual imputed unit counts. This fixes two problems: (a) all TWINs were mapped to `u_2_units` regardless of actual size (many are single-family or converted to 3-4 units), and (b) no building type mapped to `u_5_9_units` (27K Census units were unassigned). ROW and DETACHED keep their Census structural categories (u_1_attached, u_1_detached) regardless of unit count, since Census classifies by structure type. Result: 98K PIDs reassigned, u_5_9 now has 21K units (was 0), u_2 ratio improved 1.76→1.32, BG correlations improved across all bins.
- After raking/caps, final panel `total_units` is re-floored using conservative household rules.
- **Selective raking** (added 2026-02-17): Small-building bins (`u_1_detached`, `u_1_attached`, `u_2_units`) are NOT raked to Census BG targets — they keep their imputed seed units. These are individual structures; raking gave them fractional units (e.g., 112 ROW homes sharing 329 Census slots → 0.9 units each). For larger bins (`u_3_4_units` through `u_50plus_units`), raking only occurs when the pipeline's imputed units cover ≥70% of the Census bin target, preventing extreme up-scaling from poor coverage. BG-level total HU raking (former step 6b) is removed entirely.
- **Soft ceiling** (added 2026-02-16): If InfoUSA ever observed `max_households >= 5` at a PID, `total_units` is capped at `1.5 × max_households`. Applied in both the 2010 snapshot and the final panel. The 1.5× slack accounts for InfoUSA undercoverage. This prevents extreme over-imputation (e.g., raking assigning 300 units to a building where InfoUSA has only ever seen 2 households).
- **Suspect commercial filter** (added 2026-02-17): Parcels with `num_units_base >= 20`, `max_households <= 2` or NA, ambiguous building type (COMMERCIAL/OTHER/GROUP_QUARTERS/MULTI), and NO rental evidence are removed before unit raking. These are likely hotels, institutional buildings, or commercial properties with residential building codes. 734 parcels removed.

### `renters_2010` — Census BG-raked renter allocation (Section 7 of `make-occupancy-vars.r`)

For years <= 2010, renter counts are derived from Census block group totals, not InfoUSA. The allocation works as follows:

1. **Propensity scoring**: A logistic model estimates each building's probability of being rental (`p_rent`) using building_type, size, and rental evidence signals. Buildings with a license get `p_rent = 1`; buildings with `p_rent < 0.25` get zeroed out.

2. **Raw allocation**: `renters_raw = units_raked × 0.91 × p_rent` (where 0.91 is the 2010 Philadelphia renter occupancy prior).

3. **Scale to BG total**: `renters_scaled = renters_raw × (renter_occ_bg / sum(renters_raw))` within each GEOID, so building-level renters sum to the Census BG renter count.

4. **Cap at units**: `renters_final = pmin(renters_scaled, units_raked)`. No re-normalization to BG totals — we accept that building-level renters may sum to less than the Census BG renter count. The gap is absorbed into the outside good. (Prior to 2026-02-17, this step re-normalized to hit BG totals, which inflated 75K rows above their unit limits in 686/1,325 BGs.)

5. **Collapse to PID**: Sum across buildings within each PID, round to integer.

### `renter_occ` (renter-occupied units)

**Key assumption**: For rental properties, all InfoUSA households are renters. We do NOT subtract `num_homeowners` because the InfoUSA owner/renter status field is unreliable. Properties are already identified as rentals via licenses, Apartments.com, or eviction filings, so this assumption is reasonable.

Assignment logic:
- **year <= 2010 AND year_built <= 2010**: `renters_2010` (Census BG-raked)
- **All other cases** (post-2010 years OR post-2010 construction): `min(num_households, total_units)` from InfoUSA
- When `num_households` is NA, `renter_occ` is imputed (see below)
- Floored at 0
- **Hard clamp** (added 2026-02-17): After all imputation, `renter_occ` is clamped to `<= total_units`. See "BG renter re-normalization issue" below for why this is necessary.

**Occupancy imputation** (added 2026-02-16, replaces the old forced-zero policy):

When `renter_occ` is NA after the initial assignment (typically because InfoUSA has no data for that PID-year), two fallback mechanisms fill it in:

1. **Within-PID LOCF/NOCB** (Step A): If the PID has InfoUSA data in *any* year, the observed occupancy rate is carried forward/backward to fill gaps. This is the dominant mechanism (~2M rows filled). The interpolation works on occupancy rate (renter_occ/total_units), not raw counts, to handle unit changes correctly.

2. **BG-level fallback** (Step B): If the PID has *never* appeared in InfoUSA AND has rental evidence (license, Altos listing, or eviction filing), it receives the BG × structure_bin × year weighted-average occupancy rate from PIDs that do have data. PIDs with no InfoUSA data AND no rental evidence are left as NA (treated as 0) — these are likely not actually rentals.

This reduced missing `renter_occ` from ~2M rows to ~11K (99.5% filled).

### `occupancy_rate`
```
occupancy_rate = min(1, renter_occ / total_units)   [when both > 0]
```
This is a **rental** occupancy rate (renter-occupied / total stock).

Any remaining NA values in `renter_occ` (after imputation) are treated as 0 in the numerator. After the imputation fixes, this affects < 0.1% of rows.

### `rental_occupancy_rate_scaled`
Rescaled so the unit-weighted mean in 2010 equals 0.9 per `num_units_bin`, among rental PIDs only:
```
scale_factor = 0.9 / weighted_mean_occ_2010[bin]
rental_occupancy_rate_scaled = occupancy_rate * scale_factor
```
Clamped to [0, 1]. Each bin gets its own factor. After the 2026-02-16 fixes, scale factors are ~1.1 for bins 1/2-5/6-20 (healthy), and ~1.6-1.7 for bins 21-50/51+ (elevated due to structural InfoUSA undercoverage in large buildings). A log warning fires if any factor exceeds 1.5.

### Output
- `parcel_occupancy_panel` (key: PID × year)

## 2. `make-analytic-sample.R` — Shares

### `occupied_units`
Direct use of `renter_occ`, bounded:
```r
occupied_units = pmin(total_units, pmax(0, renter_occ))
```
This is the **numerator** for BLP shares.

### Market Definition
- `zip-year` markets: one market per `pm.zip × year`
- `zip-year-bin` markets: one market per `pm.zip × year × num_units_bin`

`python/pyblp_estimation.py` now toggles share specification at the top of the script:
- `zip_all_bins` => uses `share_zip_all_bins` with `zip-year` markets
- `zip_bin` => uses `share_zip_bin` with `zip-year-bin` markets

`share_zip` legacy is intentionally removed from BLP usage.

### `share_zip_all_bins` (BLP-ready, sums <= 1)
```
share_zip_all_bins = occupied_units / sum(total_units)  [by zip-year]
```
- **Denominator** = `total_units_market` = sum of `total_units` across all buildings in the zip-year
- **Outside good** = vacancy + non-sample units
- Shares sum to < 1 within each market; the gap is the outside good share

### `share_zip_bin` (BLP-ready within bin, sums <= 1)
Same as `share_zip_all_bins` but within (zip-year, num_units_bin) groups:
```
share_zip_bin = occupied_units / sum(total_units)  [by zip-year-bin]
```

### `market_share_units` (sums to 1, NOT for BLP)
```
market_share_units = occupied_units / sum(occupied_units)  [by zip-year]
```
Used for HHI and owner concentration calculations. Sums to 1 — NOT suitable for BLP.

### `share_units_zip_unit` (legacy concentration helper, sums to 1 within bin)
Same as `market_share_units` but within (zip-year, bin). Used for concentration measures.

### BLP Instruments
- Sum of competitor characteristics within market (BLP-style)
- Built via `make_blp_instruments()` function
- Key continuous vars: `total_units`, `filing_rate_eb_pre_covid`, `log_market_value`

### Output
- `bldg_panel_blp` (key: PID × year, with shares, instruments, market IDs)

## 3. Interpretation: Outside Good

With `sum(total_units)` as the denominator:
- If a building has 100 units and 80 renters, its share contribution is 80/N (not 80/M where M < N)
- The "missing" share (1 − sum of inside shares) is the outside good

The outside good now includes three components:
1. **Vacant units** in observed buildings (units that could attract renters if conditions change)
2. **Buildings our pipeline didn't capture** — Census counted them but they are not in our parcel/address match (due to residential filter exclusions, unmatchable parcels, BG boundary misalignment, or the selective raking decisions above)
3. **Buildings correctly excluded** — commercial, institutional, or suspect commercial parcels

This is a conceptual shift from Census-matched totals: `sum(total_units)` is the pipeline's **observed rental stock**, not a Census-calibrated count. The share formula is unchanged (`occupied_units / sum(total_units)` by zip-year), but we are honest about what we actually observe rather than forcing totals to match Census.

Expected inside share sums per market: 0.5–0.8 (healthy)

## 4. QA and Remaining Limitations (as of 2026-02-17)

### QA outputs from `make-occupancy-vars.r`
- `output/qa/units_vs_max_households_worst_offenders.csv`
- `output/qa/rtt_price_per_unit_offenders.csv`
- `output/qa/infousa_unmatched_high_hh_addresses.csv` — InfoUSA addresses with >=5 households but no parcel match (1,635 addresses, 37K unmatched households)
- `output/qa/infousa_oversubscribed_addresses.csv` — PIDs where max_hh >= 10 and ratio > 2× vs total_units (1,336 PIDs, median ratio 10:1)
- `output/qa/occupancy_by_building_type_year.csv` — weighted mean occupancy by building_type × year
- `output/qa/suspect_commercial_parcels.csv` — 734 parcels flagged as suspect commercial (large buildings, no InfoUSA, no rental evidence) (added 2026-02-17)
- `output/qa/renter_overalloc_bg_diagnostic.csv` — BG-level summary of 686 BGs with renter overallocation (added 2026-02-17)
- `output/qa/renter_overalloc_bldg_diagnostic.csv` — building-level detail for overallocated buildings (added 2026-02-17)
- `output/qa/step5_clamped_rows_diagnostic.csv` — top 5,000 clamped panel rows by excess (added 2026-02-17)
- Log metrics include:
  - `corr(total_units, max_households)`
  - counts for `total_units <= 2 & max_households > 10`
  - counts for `total_units > 10 & max_households <= 2`
  - RTT counts for `price_per_unit > 2e6` with small-unit flags
  - Fix 2 diagnostic: missing renter_occ breakdown by unit bin and building_type
  - Fix 2 diagnostic: PIDs with no InfoUSA ever, by size and rental evidence
  - Scale factor guard: warns if any bin's factor > 1.5

### What improved (2026-02-16, 2026-02-17)
- Occupancy rates now 0.88-0.91 (scaled) across all bins for 2015-2019, vs 0.55-0.70 before.
- ~2M missing renter_occ rows filled via LOCF/NOCB interpolation + BG fallback (99.5%).
- Unit over-imputation constrained via 1.5× max_households ceiling.
- Step B (BG fallback) restricted to PIDs with rental evidence — no longer creates phantom occupancy for non-rentals.
- Large condo undercount cases resolved.
- 734 suspect commercial parcels removed (large buildings with no InfoUSA + no rental evidence).
- Hotel detection improved (single-column match), GROUP_QUARTERS type added for boarding/dormitory.
- 86 new InfoUSA fuzzy house-number matches via Tier 5 crosswalk.
- Hard clamp: renter_occ <= total_units enforced after all imputation (75K rows affected, now near-zero after fixes below).
- Selective raking: small bins skip raking (ROW homes get 1 unit, not fractional); large bins only raked at ≥70% coverage.
- Renter re-normalization removed: `renters_final = pmin(renters_scaled, units_raked)`, no BG re-scaling.
- BG-level total HU raking (step 6b) removed: no longer re-distorts small-building unit counts.

### Known remaining issues

**~~BG renter re-normalization inflates renter_occ above total_units~~** (RESOLVED 2026-02-17): Removed the re-normalization step entirely. `renters_final` is now simply `pmin(renters_scaled, units_raked)` — buildings cannot exceed their unit limits. BG renter totals may fall short of Census counts; the gap is absorbed by the outside good. The downstream hard clamp (`renter_occ <= total_units`) should now be a near-no-op.

**~~Small buildings get fractional units from raking~~** (RESOLVED 2026-02-17): Small-building bins (`u_1_detached`, `u_1_attached`, `u_2_units`) are no longer raked. ROW homes now keep their imputed 1 unit instead of being assigned ~0.9 units from Census bin sharing. Larger bins are only raked when pipeline coverage ≥70% of Census target.

**Large-building admin parcels with tiny max_hh**: Some parcels classified as HIGHRISE/MIDRISE have 100-300 imputed units but only 2-4 max_households. These are likely admin parcels, condo master records, or commercial buildings with residential codes. LOCF carries their near-zero occupancy forward, dragging down the 21-50 and 51+ bin raw rates. Scale factors for these bins remain ~1.6-1.7 to compensate. The new suspect_commercial filter (2026-02-17) removes 734 of the worst offenders (num_units >= 20, max_hh <= 2, no rental evidence).

**OTHER/COMMERCIAL type dead weight**: 15K OTHER and 5K COMMERCIAL PIDs in the panel. 95% are u_1_attached (row homes with non-standard building codes). ~10K have neither rental evidence nor InfoUSA households in a given year — they entered the panel from signals in other years but contribute little.

**Address mismatches**: 1,635 InfoUSA addresses with >=5 households have no parcel match at all. 1,336 matched PIDs are "oversubscribed" (households >> units). Both patterns suggest address mismatches where residents report a slightly different address than the parcel record (e.g., "133 Main St" vs "123 Main St"). The QA outputs allow cross-referencing unmatched addresses with nearby oversubscribed PIDs in the same zip. A Tier 5 fuzzy house-number match (±10, same street/zip, unique match only) was added to the crosswalk (2026-02-17), recovering 86 additional addresses.

### Within-PID share variation is minimal
- Variance decomposition: ~98% between-PID, ~2% within-PID
- InfoUSA `num_households` varies year-to-year but the variation is small relative to the market denominator
- ~40% of PIDs have zero within-PID occupancy variation (InfoUSA reports same count each year)
- Within-PID occupancy rate std is ~0.077 on average, but this translates to tiny share movements because shares = occ / sum(total_units) where sum(total_units) ~ 20-30k per market

### Estimation consequences
- The current nested logit with PID + GEOID x year FE is degenerate: rho hits upper bound (0.99), price coefficient is insignificant and wrong-signed
- The previous specification (with the frozen-share bugs) happened to work because the bugs introduced a particular cross-sectional structure that PyBLP could fit
- Possible paths forward: coarser products, dropping PID FE, bin-level markets, alternative share sources

### `renter_occ` level shift at 2010/2011
- Pre-2010: total renters from Census BG allocation (now lower than previous ~286k since re-normalization was removed)
- Post-2010: ~444-480k (InfoUSA `num_households`)
- The jump reflects the switch from Census-allocated renter counts to InfoUSA total households. InfoUSA counts are higher because they include all occupants in identified rental properties, not just the Census-constrained renter subset.

## 5. Data Flow

```
InfoUSA + Census 2010
       |
       v
make-occupancy-vars.r
  -> parcel_occupancy_panel (PID × year: total_units, renter_occ, occupancy_rate)
       |
       v
make-analytic-sample.R
  -> merges with parcels_clean, ever_rentals_panel, events, assessments
  -> computes occupied_units, shares, instruments
  -> bldg_panel_blp
       |
       v
pyblp_estimation.py (or share-diagnostics.R)
  -> loads bldg_panel_blp, applies filters, estimates BLP
```
