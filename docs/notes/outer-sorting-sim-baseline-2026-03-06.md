# Outer Sorting Baseline Simulation Implementation (2026-03-06)

## What changed

- Added modular outer-sorting simulation functions:
  - `r/build-outer-sorting-params.R`
  - `r/simulate-outer-sorting.R`
  - `r/outer-sorting-moments.R`
- Added thin wrapper runner:
  - `r/run-outer-sorting-sim.R`
- Wired config for simulation outputs and run settings:
  - `config.yml`
  - `config.example.yml`
- Updated canonical docs for new product contracts:
  - `docs/data-products.md`
  - `README.md` (manual run command and model-doc pointer)

## Why

- Implement the finalized baseline normalization from `codex-plans/codex_outer_sorting_dgp_spec.txt` and `latex/sorting_model_outer_dgp.tex`.
- Make the model immediately runnable as a reproducible pipeline product using config-driven paths, no hardcoded locations.
- Establish first-pass calibration moments and fixed-point diagnostics as stable artifacts.

## Product contracts added

- `products.outer_sorting_sim_panel` (`data/processed/sim/outer_sorting_sim_panel.csv`)
  - Primary key: `b`
  - One row per simulated building with equilibrium share `s`, expected rates (`mbar`, `cbar`, `fbar`), and realized counts/rates (`M`, `C`, `F`).
- `products.outer_sorting_sim_moments` (`data/processed/sim/outer_sorting_sim_moments.csv`)
  - Single-row moments table for calibration.
- `products.outer_sorting_sim_diagnostics` (`data/processed/sim/outer_sorting_sim_diagnostics.csv`)
  - Single-row convergence and mass-constraint diagnostics.

## Verification plan

- Run `Rscript r/run-outer-sorting-sim.R`.
- Check:
  - log file created at `output/logs/run-outer-sorting-sim.log`
  - panel key uniqueness (`b`)
  - final citywide mass residual near zero
  - moments and diagnostics outputs written to configured product paths

## Open next steps

- Add a calibration wrapper (`simulate + objective + optimizer`) using the moment product.
- Optionally add this script as a `{targets}` stage after current analysis targets.
