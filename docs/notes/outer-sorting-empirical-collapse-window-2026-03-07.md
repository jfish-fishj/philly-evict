# Outer Sorting Empirical Collapse Window (2026-03-07)

- Changed the default empirical target window from a single-year `2015` cross section to `2011-2019`.
- Updated `r/build-outer-sorting-empirical-moments.R` so the empirical PID-level collapse now:
  - drops buildings with `year_built > year_to`
  - respects post-`year_from` entry via `year_built`
  - annualizes PID-level counts over observed active years
  - constructs rates using total unit-years over the active window
- Added `year_built_col` to the empirical settings block in `config.yml` / `config.example.yml`.
- This keeps the empirical target closer to a stationary average-period object while preserving correct exposure for later-built buildings.
