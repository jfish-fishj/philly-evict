# Comparative Statics -> Empirics Implementation Checklist (2026-02-27)

Source recap: `codex-chats/comparative-statics-and-codex-recap.txt`

## Objective
Implement a reproducible analysis block that maps the conceptual decomposition
`E[C_bt] ~ Lambda_bt * pi_bt` into existing repo outputs, with annual composition
specs as primary and quarterly/event dynamics as mechanism support.

## Scope guardrails
- Keep canonical variable definitions unchanged unless explicitly noted below.
- Treat decomposition as conceptual, not point-identified.
- Keep EB filing intensity as EB (no zero-filer override columns in this plan).
- Exclude ownership-unsafe condo/common-address rows in ownership-sensitive transfer analyses.

## Phase 0: Data/column readiness (preflight)

### 0.1 Verify required columns exist in `bldg_panel_blp`
Script: `r/make-analytic-sample.R`

Checklist:
- [ ] `infousa_pct_black`
- [ ] `num_households`
- [ ] `rental_ownership_unsafe_any`
- [ ] `infousa_demog_provenance`
- [ ] `filing_rate_eb_pre_covid`
- [ ] `total_severe_complaints` (or current severe complaint measure used in main specs)
- [ ] lagged maintenance and violation stock variables used by complaint suppression ladder

### 0.2 Filing intensity handling
Script: `r/make-analytic-sample.R`

Checklist:
- [ ] Keep `filing_rate_eb_pre_covid` unchanged as the main filing-intensity measure.
- [ ] Do not add zero-filer override columns in this branch.

## Phase 1: Primary annual complaint-composition block

Script: `r/retaliatory-evictions.r`
Writeup: `writeups/retaliatory-evictions.qmd`

Checklist:
- [ ] Keep annual PPML severe-complaint ladder as primary block
- [ ] Add/confirm explicit sample restriction for InfoUSA composition specs:
  - `year <= 2022`
  - `!is.na(infousa_pct_black)`
- [ ] Add sensitivity toggle excluding 2020
- [ ] Keep maintenance and severe-violation lag controls as mechanism proxies (not "fully exogenous quality controls")

Outputs:
- [ ] Table: annual ladder with fixed sample notes
- [ ] Table: sample coverage by year (rows and unit-weighted)
- [ ] QA CSV: sample composition and missingness by year and unit bin

## Phase 2: Regime-heterogeneity block (central test)

Script: `r/retaliatory-evictions.r`
Writeup: `writeups/retaliatory-evictions.qmd`

Checklist:
- [ ] Add `pct_black * high_filing` interaction specs
- [ ] Add filing-regime bins/spline interactions with `pct_black`
- [ ] Report implied `pct_black` slope by filing regime
- [ ] Plot slope-by-regime with confidence intervals

Outputs:
- [ ] `output/.../regime_heterogeneity_table.*`
- [ ] `output/.../regime_heterogeneity_slopes.csv`
- [ ] `figs/.../pct_black_slope_by_filing_regime.png`

## Phase 3: Conversion-margin tests (lagged issue proxies)

Script: `r/retaliatory-evictions.r`

Core spec family:
`C_bt ~ V_b,t-1:t-L + V_b,t-1:t-L * high_filing_b + FE`

Checklist:
- [ ] Build lag windows for severe violations/investigations proxy
- [ ] Estimate baseline positive issue-signal effects
- [ ] Estimate interaction effects with high-filing regime
- [ ] Add placebo leads of issue proxies
- [ ] Add alternative high-filing cutoff sensitivity

Outputs:
- [ ] Table: conversion-margin interactions
- [ ] Figure: marginal complaint response by regime
- [ ] QA: lead-placebo coefficients

## Phase 4: Maintenance dynamics / mechanism support

Scripts:
- `r/retaliatory-evictions.r`
- (if needed) `r/analyze-transfer-evictions-unified.R` for event-time support visuals

Checklist:
- [ ] Keep maintenance lead/lag/event-style descriptive block
- [ ] Explicitly label as mechanism evidence (reactive/endogenous), not causal quality-control closure
- [ ] Keep distinction between issue signals and maintenance response clear in writeup text

## Phase 5: Transfer/ownership analysis safety filters

Scripts:
- `r/analyze-transfer-evictions-unified.R`
- `r/analyze-rtt-transfers.R`
- `r/analyze-transfer-evictions.R`
- `r/analyze-transfer-evictions-quarterly.R`

Checklist:
- [x] Exclude `rental_ownership_unsafe_any == TRUE` from `bldg_panel_blp` analysis sample
- [x] Log excluded rows/PIDs/units and post-filter counts
- [ ] Re-run annual and quarterly unified transfer scripts and archive updated logs

## Phase 6: Theory-to-empirics mapping table (paper-facing)

Writeup target: `writeups/retaliatory-evictions.qmd`

Checklist:
- [ ] Add one table mapping each regression family to channel interpretation:
  - latent issue severity / maintenance
  - complaint conversion / deterrence
  - sorting / exposure to filing regime
  - retaliation intensity
- [ ] Add one-line identifying assumptions / limitations per row

## QA gates before sign-off

### Gate A: Coverage and sample integrity
- [ ] Unit-weighted coverage tables for `infousa_pct_black` and outcomes by year
- [ ] Confirm 2023-2024 not used in InfoUSA composition specs
- [ ] Confirm 2020 sensitivity run reported

### Gate B: Join/key integrity
- [ ] Assert `PID x year` uniqueness after all merges in analytic sample build
- [ ] No silent join expansion in new regression prep tables

### Gate C: Robustness package
- [ ] Alternate filing-regime cutoffs
- [ ] Placebo leads in conversion-margin specs
- [ ] Ownership-unsafe excluded vs included sensitivity (transfer analyses)

## Run order (minimal)
1. `Rscript --vanilla r/make-analytic-sample.R`
2. `Rscript --vanilla r/retaliatory-evictions.r`
3. `Rscript --vanilla r/analyze-transfer-evictions-unified.R --frequency annual`
4. `Rscript --vanilla r/analyze-transfer-evictions-unified.R --frequency quarterly`
5. `Rscript --vanilla r/analyze-rtt-transfers.R`

## Decision points to lock before final tables
- Whether `2020` is excluded in the main specification or only sensitivity.
- Preferred high-filing threshold(s) in the main text.

## Definition log (for reproducibility)
- `filing_rate_eb_pre_covid`: EB-smoothed pre-COVID filing intensity (unchanged).
- `rental_ownership_unsafe_any`: exclusion flag for ownership-sensitive analyses.
