# Outer Sorting Baseline Calibration Run (2026-03-06)

## What changed

- Added calibration module and thin runner:
  - `r/calibrate-outer-sorting.R`
  - `r/run-calibrate-outer-sorting.R`
- Added calibration settings to config:
  - `run.outer_sorting.calibration.*` in `config.yml` and `config.example.yml`
- Added README run command:
  - `Rscript r/run-calibrate-outer-sorting.R`

## Calibration setup

- Target moments source: `products.outer_sorting_sim_moments`
- Calibrated parameters: `Dalph`, `bC_s`, `bF_theta`
- Objective: weighted quadratic distance over configured moments in `run.outer_sorting.calibration.moment_cols`
- Optimizer: `optim(..., method = "Nelder-Mead")` with a Dalph grid start

## Run and outputs

- Command run: `Rscript r/run-calibrate-outer-sorting.R`
- Output directory: `output/outer_sorting/`
- Files written:
  - `outer_sorting_calibration_best_params.csv`
  - `outer_sorting_calibration_moment_comparison.csv`
  - `outer_sorting_calibration_optimization_trace.csv`
  - `outer_sorting_calibration_optimization_summary.csv`
  - `outer_sorting_calibration_best_diagnostics.csv`
  - `outer_sorting_calibration_best_moments.csv`
  - `outer_sorting_calibration_best_panel.csv`

## Verification notes

- Calibration run completed successfully.
- Convergence code: `0` (local optimization skipped because starting objective was below tolerance).
- Objective at solution: ~`4.29e-31`.
- Best calibrated parameters remained at baseline values (`Dalph=6`, `bC_s=0.35`, `bF_theta=0.70`), as expected when calibrating to moments generated from the same baseline model.
