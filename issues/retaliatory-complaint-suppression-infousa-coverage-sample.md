# Retaliatory complaint suppression: InfoUSA coverage and `% Black` sample selection (2026-02-25)

This issue tracks a likely sample-definition problem in the severe-complaint PPML complaint-suppression regressions in `r/retaliatory-evictions.r`.

## Problem summary

The current `% Black` complaint-suppression ladder uses a tenant-composition completeness restriction (`tenant_comp_missing == 0`) in the severe PPML block.

That is likely too strict for `% Black`-only regressions because it requires other demographic fields (e.g., female / intersection shares) to be present, not just `infousa_pct_black`.

At the same time, InfoUSA demographic coverage in `bldg_panel_blp` is materially incomplete and varies sharply by year.

## Why this matters

- The complaint-suppression demographic regressions run on a selected subset of the analytic PID universe.
- Unit-mass coverage drops materially once `% Black` coverage restrictions are imposed.
- `2023-2024` have effectively zero InfoUSA demographic-share coverage, so `% Black` regressions should not silently include those years.
- `2020` looks anomalous (race fields relatively available while other demographic-quality metrics degrade), which can distort `tenant_comp_missing`-based restrictions.

## Evidence observed in current iteration

- In `bldg_panel_blp`, `infousa_pct_black` coverage is only ~56-65% of PID-year rows (and unit mass) in many years.
- `2020` anomaly:
  - race coverage improves
  - other demographic fields degrade sharply
  - `infousa_share_persons_demog_ok` values often collapse (many zeros)
- `2023-2024`: InfoUSA demographic-share fields (including `infousa_pct_black`) are effectively all missing.

## Proposed follow-up (do not patch silently)

For `% Black` complaint-suppression regressions in `r/retaliatory-evictions.r`:

1. Change the sample rule from `tenant_comp_missing == 0` to `!is.na(infousa_pct_black)` (plus spec-specific missingness controls such as `log_total_area` / tract FE requirements).
2. Restrict demographic complaint-suppression specs to years with usable InfoUSA coverage (likely `<= 2022`).
3. Add a `2020` sensitivity exclusion and report the coefficient differences.
4. Add an explicit coverage QA table/plot (year-by-year row share and unit-mass share with non-missing `infousa_pct_black`) to regression outputs/QMD.

## Guardrail

Any change here affects regression sample definitions and coefficient interpretation. Implement as an explicit new spec / documented change, not a silent replacement.
