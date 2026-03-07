# EB Filing-Rate Memo (2026-03-04)

This memo records how the building-level Empirical Bayes (EB) filing rate is constructed in the production pipeline, so analysis scripts can reference one canonical definition.

## Source of truth

- Script: `r/make-analytic-sample.R`
- Section: `6b) FILINGS INTENSITY: RAW RATES + EMPIRICAL BAYES (POOLED ACROSS YEARS)`
- Key lines: around `839-985` in the current script.

## Model

For building `i` and year `t`:

- `y_it` = eviction filings (`num_filings`)
- `E_it` = exposure (occupied units, `exposure_occ`)

Assumption:

- `y_it | lambda_i ~ Poisson(E_it * lambda_i)`
- `lambda_i ~ Gamma(alpha, beta)`

Pre-COVID pooled sufficient statistics:

- `Y_i = sum_t y_it` for `t <= 2019`
- `E_i = sum_t E_it` for `t <= 2019`

Posterior mean used as EB filing intensity:

- `E[lambda_i | Y_i, E_i] = (alpha + Y_i) / (beta + E_i)`

## Prior estimation and fallback

- City-wide Gamma prior (`alpha_city`, `beta_city`) estimated by method-of-moments from pooled `(Y_i, E_i)`.
- ZIP-level priors are estimated when ZIP support is sufficient.
- ZIP priors fall back to city prior when thin (`min_bldgs_per_zip`, `min_total_E_zip` checks in script).

Produced columns include:

- `filing_rate_eb_city_pre_covid`
- `filing_rate_eb_zip_pre_covid`
- `filing_rate_eb` (defaulted to city prior version)
- `filing_rate_eb_pre_covid` (analysis-facing EB pre-COVID measure)

## Retaliatory-evictions usage decision (this iteration)

- Main maintenance regressions in `r/retaliatory-evictions.r` now use `filing_rate_eb_pre_covid`.
- Raw long-run filing rate (`filing_rate_longrun_pre2019`) is retained for appendix robustness only.
- Filing bins used in retaliatory maintenance models are:
  - `[0,1)`, `[1,5)`, `[5,10)`, `[10,20)`, `[20,inf)`
- High-filing indicator cutoff is `>= 15%`.

## Address-history usage decision (this iteration)

- Main filing regressions in `r/address-history-analysis.R` now use EB (`filing_rate_eb_pre_covid`).
- Raw long-run filing-rate regressions (`filing_rate_longrun_pre2019`) are exported as appendix robustness tables.
- Filing bins used in inflow/outflow and trajectory conditioning are:
  - `[0,1)`, `[1,5)`, `[5,10)`, `[10,20)`, `[20,inf)`
- High-filing indicator cutoff is `>= 15%` (for persistence and trajectory high-filer flags).
- Trajectory destination-level filing models also include a high-filing-dummy specification
  (middle building `>= 15%`) alongside bin specifications.
- LOO tract filing-rate trajectory models are split into EB (main) and raw (appendix) variants,
  with both bin and high-filing-dummy specifications.
