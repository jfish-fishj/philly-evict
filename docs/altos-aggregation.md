# Altos Aggregation and Standardization (Phase 1)

This note documents the `r/make-altos-aggs.R` and `r/make-rent-panel.R` Altos rent aggregation logic introduced for composition-robust rent measures.

The active linkage path is:

- `r/make-address-parcel-xwalk.R` -> `xwalk_altos_to_parcel`
- `r/make-altos-aggs.R` -> Altos PID-year panels

The older `r/merge-altos-parcels.R` script and its listing-level crosswalk outputs are retired and kept in `graveyard/` only for archival reference.

## Scope

- `r/make-altos-aggs.R`:
  - de-stales Altos listing rows
  - writes legacy Altos aggregates (`altos_year_building`, `altos_year_bedrooms`)
  - writes Phase 1 cell product: `altos_pid_year_bedbin`
- `r/make-rent-panel.R`:
  - builds PID-year Altos metrics used downstream
  - computes composition-robust Altos levels and overlap-based changes

## Bed bins (Phase 1)

Listings are mapped to bedroom bins using `beds_imp` (Altos bedrooms + text parsing fallback):

- `studio` = 0 bedrooms
- `1br` = 1 bedroom
- `2br` = 2 bedrooms
- `3plus` = 3+ bedrooms

Rows without a mappable bedroom bin are excluded from the cell-based standardized measures, but they can still affect legacy/raw building-level pooling.

## New intermediate product: `altos_pid_year_bedbin`

Primary key: `PID x year x bed_bin`

Built from de-staled Altos listing rows. Each row summarizes a parcel-year-bedroom-bin cell:

- `med_rent_cell`
- `n_listing_rows_cell`
- QC fields (`mean`, `p25`, `p75`, unique prices, price changes)

This product is the input for Phase 1 standardization in `r/make-rent-panel.R`.

## Fixed weights for standardized Altos rent levels

Phase 1 uses fixed bedroom-share weights by coarse parcel `building_type` class (`mix_class`):

- `small_apartment`
- `mid_apartment`
- `large_apartment`

`mix_class` is derived from parcel `building_type` (see `standardize_building_type()` in `r/helper-functions.R` and `altos_bed_mix_class()`).

### Weight source

Preferred:
- AHS Table 0 CSV (`cfg$inputs$ahs_table0_bed_mix`)

Fallback:
- built-in default weights in `default_altos_bed_mix_weights()`

The loader logs which source is used.

### AHS parsing details

The parser reads the `Bedrooms` section of AHS Table 0 and constructs bin shares by AHS structure columns:

- `small_apartment` = `2 to 4 Units`
- `mid_apartment` = `5 to 9 Units` + `10 to 19 Units`
- `large_apartment` = `20 to 49 Units` + `50 or more`

Bedroom rows are collapsed to Phase 1 bins:

- `studio = None`
- `1br = 1`
- `2br = 2`
- `3plus = 3 + 4 or more`

## Phase 1 Altos level vs change logic (important)

### Standardized Altos level: `med_rent_altos_std_fixed`

Phase 1 computes a standardized Altos level whenever at least one valid bedroom-bin cell is observed in a PID-year.

- Weights are renormalized over observed bins.
- `altos_share_weight_observed` is retained as a diagnostic coverage metric.

This means low coverage does **not** invalidate the level by itself in Phase 1.

### Coverage diagnostic: `altos_share_weight_observed`

Definition:
- sum of fixed bedroom-bin weights observed in a PID-year (`0..1`)

Current `MIN_COVERAGE` (`0.60`) is used for diagnostics/logging and for Phase 2 bridge overlap sample restrictions, but **not** as a hard gate for `med_rent_altos_std_fixed`.

### Composition-robust Altos YoY change: `dlog_rent_altos_overlap`

This is the main protection against fake rent changes from bedroom composition drift.

Definition:
- use only bedroom bins observed in both `t` and `t-1`
- weight bin-specific log changes with fixed weights renormalized over overlap bins

This measure **does** use a hard overlap threshold (`MIN_OVERLAP`) because changes are where composition shifts are most problematic.

## Why many buildings have low level coverage

Most PID-years have only one observed Altos bedroom bin. That is usually acceptable for a level estimate (we use the observed bin), but it implies low `altos_share_weight_observed` under broad AHS-based bedroom distributions.

This is expected and is one reason `MIN_COVERAGE` is diagnostic-only for levels in Phase 1.
