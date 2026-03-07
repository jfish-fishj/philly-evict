# BLP `total_units` inflation along the pipeline (2026-03-07)

This issue flags a likely denominator problem in the BLP path: `total_units` in `bldg_panel_blp` appears to get inflated somewhere upstream.

Most likely source scripts:

- `r/make-occupancy-vars.r`
- `r/make-analytic-sample.R`

## Symptom

When the outer-sorting empirical target is built using `bldg_panel_blp` and filing rates are defined as filings per unit-year, the implied filing-rate target is lower than expected.

In the current build:

- unit-weighted mean filing rate is about `0.039`
- unweighted mean filing rate is about `0.041`

If the filing rate is instead defined at a **per-household** level, the result appears to line up with the expected magnitude. That points to an overstated `total_units` denominator rather than a filing-count numerator problem.

## Why this matters

- It pushes down `filing_rate = num_filings / total_units`
- It can mechanically attenuate empirical target moments that use per-unit filing rates
- It can distort calibration if we treat the current `total_units` denominator as correct

## Current interpretation

This was **not material for the current outer-sorting run**, so no pipeline change was made here.

But it should be investigated before treating per-unit filing-rate levels in `bldg_panel_blp` as settled.

## Suggested check

Trace `total_units` from:

1. `r/make-occupancy-vars.r`
2. `r/make-analytic-sample.R`
3. final `bldg_panel_blp`

and compare:

- per-unit filing rates
- per-household filing rates
- raw household counts vs imputed unit counts

The main question is whether `total_units` is being overstated in the BLP output relative to the building/household objects actually used elsewhere in the project.
