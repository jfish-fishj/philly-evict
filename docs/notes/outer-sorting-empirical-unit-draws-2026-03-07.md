# Outer Sorting Empirical Unit Draws (2026-03-07)

- Replaced the default outer-sorting unit draw with empirical resampling from the PID-level average annual `total_units` distribution in `bldg_panel_blp`.
- The draw pool uses the same active-year logic as the empirical target build:
  - default window `2011-2019`
  - drop rows with `year_built > year_to`
  - compute `avg_annual_units = sum(total_units) / observed_years` by PID
- Kept the old discrete unit-bin draw only as an explicit fallback mode (`units_draw_mode: "discrete_bins"`).
- Verified the new simulated unit distribution against the empirical PID-level distribution:
  - empirical mean units: `2.065`
  - simulated mean units: `2.082`
  - empirical p50/p90/p95/p99: `1 / 2 / 4 / 20`
  - simulated p50/p90/p95/p99: `1 / 2 / 3 / 21`
- Updated memo language to make unit-weighting explicit for means and to describe the empirical unit draw.
- `Rscript r/run-outer-sorting-sim.R` completed successfully under the new unit draw.
- A fresh `r/run-calibrate-outer-sorting.R` run was started under the new unit draw and was still running at note time; do not treat the previous memo calibration tables as updated until that run finishes and the memo is re-rendered.
