# Outer Sorting Review Fixes (2026-03-06)

## Scope

Applied follow-up fixes from code review to the baseline outer sorting simulation + calibration code.

## Bugs fixed

- `r/outer-sorting-moments.R`:
  - Maintenance discipline moment now uses observed complaint rate `c_rate`:
    - `M ~ log1p(c_rate) | nhood` (with offset `log(units)`).
  - High-filing indicator now uses a fixed absolute threshold (`high_filing_threshold`) instead of a simulated within-sample percentile.
  - Added output column `high_filing_threshold` and kept `filing_p85` as a backward-compatible alias.

## Moderate issues fixed

- `r/simulate-outer-sorting.R`:
  - Added explicit non-convergence warning (default behavior).
  - Recompute `mbar/cbar/fbar` after final citywide-mass projection of `s` to avoid stale-rate mismatch.
- `r/calibrate-outer-sorting.R`:
  - Suppress convergence warnings inside objective evaluations and add a penalty when an evaluation does not converge.

## Minor improvements applied

- Calibration defaults:
  - Increased default Nelder-Mead `maxit` from `40` to `300`.
  - Added explicit log warning when target moments are simulation-generated (`outer_sorting_sim_moments`) rather than empirical.
  - Removed redundant objective evaluation in the no-`Dalph` branch.
- `r/build-outer-sorting-params.R`:
  - Added warning when `units_bin_probs` is materially off-sum and gets renormalized.
- Config:
  - Added `run.outer_sorting.high_filing_threshold` and `run.outer_sorting.calibration.high_filing_threshold` in `config.yml` and `config.example.yml`.

## Verification

- `Rscript r/run-outer-sorting-sim.R`
- `Rscript r/run-calibrate-outer-sorting.R`

Both scripts completed successfully after fixes.
