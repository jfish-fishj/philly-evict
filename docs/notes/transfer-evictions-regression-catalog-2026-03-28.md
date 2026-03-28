# Transfer Evictions Regression Catalog (2026-03-28)

## What changed

- Added a first-pass annual weighted stacked DiD filing-rate block to `r/analyze-transfer-evictions-unified.R`.
- Added a first-pass streamlined annual output block in `r/analyze-transfer-evictions-unified.R` for the regression families we actually want to keep.
- Gated the legacy annual output surface behind `cfg$run$transfer_evictions_run_legacy_annual`, which defaults to `FALSE`.
- Updated `writeups/transfer-evictions.qmd` so it now reads only the streamlined annual outputs plus the weighted stacked filing-rate robustness.
- The new stacked outputs are:
  - `output/transfer_evictions/annual_baseline_stacked_weighted_coefs.csv`
  - `output/transfer_evictions/annual_baseline_stacked_weighted_qweights.csv`
  - `output/transfer_evictions/annual_baseline_stacked_weighted_etable.tex`
  - `output/transfer_evictions/figs/rtt_annual_baseline_stacked_weighted.png`
- The streamlined annual outputs now include:
  - `annual_filing_core_*`
  - `annual_filing_acq_2plus_*`
  - `annual_occupancy_2plus_*`
  - `annual_pct_*_2plus_*`
  - `annual_permit_rate_2plus_*`
  - `annual_complaint_rate_2plus_*`
  - `annual_rent_core_*`
  - `annual_rent_safe_core_*`
  - `annual_rental_evidence_1unit_*`
  - `annual_rental_evidence_2plus_*`
- The stacked block is intentionally narrow:
  - annual only
  - filing rate only
  - weighted linear only
  - baseline full sample and `2+` sample only

## Why

- The repo already had a large annual event-study surface in `r/analyze-transfer-evictions-unified.R`, but no weighted stacked implementation in the production script.
- Earlier ad hoc stacked checks lived in `scratch/benchmark-rental-stock-acs.R`, which is not the right place for a main robustness path.
- The goal here was to add one production-grade stacked robustness for the main filing outcome without silently changing the existing baseline event-study outputs.

## Weighted stacked design used here

- Source sample:
  - `full_panel_main` in `r/analyze-transfer-evictions-unified.R`
  - treated = single-transfer PIDs
  - controls = never-transferred PIDs
- Window:
  - trimmed annual `-3:3`
  - reference period `-2`
- Fixed effects:
  - `pid_cohort + yr_cohort`
- Outcome:
  - `filing_rate`
- Weights:
  - regression weight = `q_weight * total_units`
  - treated rows get `q_weight = 1`
  - control rows in cohort `a` get:
    - `(cohort treated PID share) / (cohort control PID share)`
- QA artifact:
  - `annual_baseline_stacked_weighted_qweights.csv` records cohort-specific treated/control counts and the corrective control weight

This is a first-pass corrective-weight implementation intended to move the production stacked spec closer to Wing, Freedman, and Hollingsworth (2024). It is a robustness path, not yet a wholesale replacement for the generic annual event study.

## Current regression families in `r/analyze-transfer-evictions-unified.R`

There are now two annual layers in the script:

- streamlined annual core outputs, which are the default production path and match the current writeup surface
- legacy annual override outputs, which remain available only if `cfg$run$transfer_evictions_run_legacy_annual` is set to `TRUE`

### 1. Annual full-panel linear event studies

- Core form:
  - `feols(outcome ~ event_time dummies | PID + year, ...)`
- Sample:
  - single-transfer treated PIDs
  - never-transferred controls
- Outputs:
  - baseline full / full weighted / size-restricted / rental-stock-restricted
  - buyer type
  - building size
  - acquirer filing bin
  - portfolio size
  - portfolio units
  - tract-year and neighborhood-gentrification variants

Recommendation:
- Keep as the main specification family for filing-rate outcomes.

### 2. Annual Poisson event studies

- Core form:
  - `fepois(num_filings ~ event_time dummies | PID + year, offset = log(total_units))`
- Currently produced for:
  - baseline
  - buyer type
  - acquirer filing bin
  - portfolio size

Recommendation:
- Keep as robustness for filing outcomes.
- Treat as a functional-form check, not the main headline estimator.

### 3. Annual weighted stacked linear event study

- Newly added in this iteration.
- Current scope:
  - baseline full weighted
  - baseline `2+` weighted

Recommendation:
- Keep as a robustness path for the main annual filing-rate results.
- Expand to selected heterogeneity panels only after deciding which subgroup definitions are substantively worth keeping.

### 4. Quarterly filing event studies

- Same general structure as the annual design, but with `PID + yq` and quarterly event time.

Recommendation:
- Keep as timing appendix / dynamics robustness.
- Do not treat the quarterly path as the headline design over the annual full-panel results.

### 5. Filing heterogeneity by acquirer filer bin

- Current production definition uses `compute_loo_filing_type()`:
  - full-period leave-one-out unit-years
  - high vs low filing threshold at `0.05`
- This is coherent as a landlord-type descriptor, but for filing outcomes it is partly outcome-defined.

Recommendation:
- Keep in the script, but do not treat it as the cleanest causal heterogeneity split for filing outcomes.
- Prefer describing these as composition/landlord-type heterogeneity, not as a clean pre-treatment sorter.
- If filer-bin heterogeneity becomes central, add a pre-transfer-only classification alongside the current full-period LOO version instead of silently replacing it.

### 6. Rent event studies

- The script currently runs:
  - unified rent
  - rent robustness
  - safe rent-change variants

Recommendation:
- Keep only as secondary/diagnostic outputs for now.
- Do not use unified rent levels as a headline outcome until the source-switching / bridge issue is resolved.
- If rent remains in the main writeup, favor same-source change outcomes over unified levels.

### 7. Scratch stacked DiD path

- `scratch/benchmark-rental-stock-acs.R`

Recommendation:
- Do not treat as the production source of truth.
- Keep only as exploratory scaffolding.

## What looks worth keeping versus de-emphasizing

Keep:

- Annual full-panel filing-rate event study with `PID + year` FE
- Annual generic Poisson filing-count robustness with unit offset
- Annual weighted stacked linear filing-rate robustness
- Quarterly filing event study as timing robustness
- Buyer-type and coarse size heterogeneity

De-emphasize or treat as descriptive:

- Filing heterogeneity defined by full-period filing behavior when the outcome is filings
- Stacked Poisson event studies
- Unified rent level event studies
- Scratch-script stacked results

## Verification

- Syntax check:
  - `Rscript -e "parse(file='r/analyze-transfer-evictions-unified.R')"`
- Production run:
  - `Rscript r/analyze-transfer-evictions-unified.R --frequency annual`
- Writeup render:
  - `quarto render writeups/transfer-evictions.qmd`

Key new outputs observed during the run:

- `stacked_full_wtd`
  - `event_time::-3 = -0.000529`
  - `event_time::1 = +0.015097`
  - `event_time::2 = +0.019517`
  - `event_time::3 = +0.019883`
- `stacked_2plus_wtd`
  - `event_time::-3 = -0.000891`
  - `event_time::1 = +0.009587`
  - `event_time::2 = +0.012382`
  - `event_time::3 = +0.013685`

These pre-periods are much flatter than the earlier ad hoc unweighted stacked checks, which is the main reason to keep developing the weighted stacked path instead of the old scratch version.

## Next steps

- Decide whether to extend the weighted stacked path beyond the baseline filing regressions.
- If filer-bin heterogeneity remains central for filing outcomes, add a parallel pre-transfer-only classification instead of replacing the current full-period LOO bins.
- Leave stacked Poisson out of the main production path unless it becomes necessary for a specific appendix robustness.
