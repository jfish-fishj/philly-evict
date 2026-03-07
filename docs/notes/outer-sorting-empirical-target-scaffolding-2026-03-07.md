# Outer Sorting Empirical Target Scaffolding (2026-03-07)

## What changed

- Added renter-poverty geography build scripts:
  - `r/build-renter-poverty-geo.R`
  - `r/make-renter-poverty-geo.R`
- Added empirical outer-sorting target build scripts:
  - `r/build-outer-sorting-empirical-moments.R`
  - `r/make-outer-sorting-empirical-moments.R`
- Added recovery scripts:
  - `r/outer-sorting-recovery.R`
  - `r/run-outer-sorting-recovery.R`
- Wired new processed products in config:
  - `bg_renter_poverty_share`
  - `tract_renter_poverty_share`
  - `outer_sorting_empirical_moments`
- Added config blocks for:
  - `run.renter_poverty_geo`
  - `run.outer_sorting.empirical`
  - `run.outer_sorting.recovery`
- Updated `README.md` and `docs/data-products.md`.

## Design choices

- Empirical base panel is `bldg_panel_blp`, not `analytic_sample`.
- Neighborhood FE for the empirical moments are tract-level (`substr(GEOID, 1, 11)`).
- Building composition proxy is block-group renter poverty share.
- Default empirical outcome mapping:
  - filings = `num_filings`
  - complaints = `total_complaints`
  - maintenance = `total_permits`
  - units exposure = `total_units`
- Default empirical target window is pooled `2017-2019`.

## Poverty-product caveat

- The preferred poverty input is a raw file with explicit renter-household counts:
  - `renter_hh_total`
  - `renter_hh_poverty`
- If no input file is supplied, the script falls back to ACS `B17019`.
- `B17019` is a renter-family poverty proxy, not a full renter-household poverty table, so the product records `measure_source` to make that distinction explicit.

## Verification

- Smoke-tested `r/make-renter-poverty-geo.R` on toy block-group input under a temp config.
- Smoke-tested `r/make-outer-sorting-empirical-moments.R` on toy `bldg_panel_blp` + poverty inputs under a temp config.
- Smoke-tested `r/run-outer-sorting-recovery.R` with `n_rep = 2` under a temp config.
- Attempted a real ACS fetch with:
  - `Rscript r/make-renter-poverty-geo.R`
- Result:
  - failed in this environment because the Census API is not reachable (`Could not resolve host: api.census.gov`).

## What you should run locally

- Run:
  - `Rscript r/make-renter-poverty-geo.R`
- If you already have a raw block-group poverty file with explicit renter counts, run:
  - `Rscript r/make-renter-poverty-geo.R --input=PATH_TO_FILE --input-geography=block_group`

## Next steps

- Build the real poverty geography product locally.
- Then run:
  - `Rscript r/make-outer-sorting-empirical-moments.R`
- After that, point `run.outer_sorting.calibration.target_product_key` to `outer_sorting_empirical_moments` for the first real-data calibration pass.
