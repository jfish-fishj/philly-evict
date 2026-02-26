# Retaliatory Complaint Suppression Iteration Notes (2026-02-26)

This note summarizes the complaint-suppression iteration work that was implemented in `r/retaliatory-evictions.r` and surfaced in `writeups/retaliatory-evictions.qmd`.

## What changed

- Added a severe-complaint PPML complaint-suppression ladder (weighted by units, clustered by `PID`) in `r/retaliatory-evictions.r`.
- Rewired the complaint-suppression section in `writeups/retaliatory-evictions.qmd` to report the severe PPML ladder instead of the legacy LPM suppression results.
- Added complaint-only mode defaults in `r/retaliatory-evictions.r` to speed iteration by skipping non-suppression blocks and the legacy suppression LPM/long-run suppression blocks.
- Added a fourth complaint-suppression regression that controls for a lagged EWMA stock of severe violations (hazardous + unsafe), per unit.

## Severe complaint PPML ladder (current)

The ladder currently estimated in `r/retaliatory-evictions.r` is:

1. `total_severe_complaints ~ infousa_pct_black | year`
2. Reg 1 + building / neighborhood controls and FE (`log_total_area`, tract FE, building type, unit bin, year-built decade, stories bin, quality FE)
3. Reg 2 + lagged maintenance stock per unit (`maint_stock_lag_rate_sev3`)
4. Reg 3 + lagged severe-violations stock per unit (`severe_viol_stock_lag_rate_sev3`)

Both stock variables use an EWMA with `lambda = 0.9`, lagged one quarter.

## Key empirical result from this iteration

- In the analytic-sample PID universe used by `r/retaliatory-evictions.r`, the lagged maintenance-stock coefficient remains strongly positive in the severe-complaint PPML specs.
- Adding the lagged severe-violations stock proxy (Reg 4) also yields a strongly positive coefficient and does not materially move the `infousa_pct_black` coefficient.

This was part of the motivation for pausing interpretation and auditing demographic coverage.

## InfoUSA coverage issue (main blocker)

The bigger issue appears to be InfoUSA demographic coverage / sample selection rather than PPML implementation bugs.

Observed diagnostics (from `bldg_panel_blp` coverage checks):

- `infousa_pct_black` is only non-missing for roughly ~56-65% of PID-year rows (and unit mass) in many years.
- `2020` is anomalous: race fields look more available, but other demographic coverage/quality metrics degrade sharply.
- `2023-2024` have effectively zero coverage for InfoUSA demographic share fields (`infousa_pct_black`, `infousa_pct_white`, `infousa_pct_female`, etc.).

Implication:

- Complaint-suppression demographic regressions are running on a selected subset of the analytic PID universe.
- For `% Black`-only regressions, using `tenant_comp_missing == 0` is stricter than necessary and should likely be revisited.

## Immediate next steps (queued)

- Audit upstream InfoUSA demographics product coverage (`infousa_building_demographics_panel` / `bldg_panel_blp`) before interpreting demographic coefficients.
- Consider restricting demographic complaint-suppression regressions to years with usable coverage (likely `<= 2022`).
- Add a `2020` sensitivity exclusion.
- Consider sampling on `!is.na(infousa_pct_black)` (not full `tenant_comp_missing == 0`) for `% Black`-only regressions.
