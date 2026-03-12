## Outer Sorting InfoUSA Composition Proxy

Date: 2026-03-09

### What changed

- Added a new PID-year product, `infousa_building_income_proxy_panel`, built from `infousa_clean` plus `xwalk_infousa_to_parcel`.
- The default empirical composition proxy is now `infousa_find_low_income_share`, defined as the share of linked primary households with `find_div_1000 <= 30`.
- Updated `build-outer-sorting-empirical-moments.R` to use the building-level InfoUSA proxy instead of the block-group poverty product.
- Removed tract fallback logic from the empirical target path; missing PID-year composition proxy rows are dropped.

### Why

- The outer-sorting empirical target should use a building-level composition proxy when possible.
- `find_div_1000` looked usable in the earlier QA pass: no missingness, reasonable range, and meaningful negative correlation with tract/block-group poverty rates.

### Verification

- Ran `Rscript r/make-infousa-building-income-proxy.R`
- Ran `Rscript r/make-outer-sorting-empirical-moments.R`
- Ran `Rscript r/run-calibrate-outer-sorting.R`
- Ran `quarto render writeups/outer-sorting-baseline.qmd`

### Key outputs

- `data/processed/analytic/infousa_building_income_proxy_panel.csv`
- `data/processed/targets/outer_sorting_empirical_moments.csv`
- `output/outer_sorting/outer_sorting_calibration_optimization_summary.csv`
- `writeups/outer-sorting-baseline.pdf`

### Notes

- The proxy builder now restricts to the empirical target window (`2011-2019`) and aggregates households to address-year before joining to parcels.
- The simulator itself is unchanged; the main effect here is on empirical targets and the resulting calibration fit.
