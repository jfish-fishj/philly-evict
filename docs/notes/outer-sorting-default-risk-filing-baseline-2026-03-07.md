# Outer Sorting Default-Risk Filing Baseline (2026-03-07)

## What changed

- Replaced the direct filing-on-composition term with a default-risk filing block:
  - `delta_bar_b = s_b * delta_H + (1 - s_b) * delta_L`
  - `log fbar_b = aF + bF_theta * 1{theta_b = B} + bF_delta * delta_bar_b`
- Dropped `bF_s_hard` from the simulator/config baseline and introduced `bF_delta`.
- Expanded the first-pass calibration parameter list to include `bF_delta`.
- Updated the Quarto memo to match the new filing equation, shrink the parameter table with `\tiny`, and replace the old scatter with by-type histograms.
- Updated the spec/model docs and product docs to reflect the new filing block.

## Why

- This keeps the baseline close to the previous reduced-form implementation while making the tenant-side default anchors (`delta_L`, `delta_H`) active structural inputs.
- It also reduces direct confounding between sorting strength and the filing equation by routing composition through average default risk.

## Verification

- Ran:
  - `Rscript r/run-outer-sorting-sim.R`
  - `Rscript r/run-calibrate-outer-sorting.R`
  - `quarto render writeups/outer-sorting-baseline.qmd`
- Results:
  - simulation converged in 31 iterations
  - final mass residual was effectively zero
  - self-calibration check returned convergence code `0` and objective `0`
  - memo rendered successfully to `writeups/outer-sorting-baseline.pdf`

## Notes

- The first calibration run after the patch was launched in parallel with the simulator and therefore read stale target moments. Calibration was rerun sequentially after the simulator completed; the final outputs on disk are the correct ones.

## Next steps

- Replace self-generated target moments with empirical target moments.
- Decide whether the default-risk filing block is sufficient or whether filings need additional structure once empirical targets are in place.
