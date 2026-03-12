# Codex chat: implement regressions implied by the two-type stochastic-issues equilibrium

**Goal:** Implement a regression suite that maps the equilibrium objects to data.

Model implications to operationalize:
- Conditional on the issue environment (or maintenance), high-risk tenants complain less ⇒ buildings with higher risky share (`mu_risky`) have fewer complaints.
- Unconditionally, the sign of complaints vs risky share is ambiguous because maintenance is endogenous.
- A sharp diagnostic: the marginal complaint response to issue signals is weaker in high-risk-share buildings (`mu_risky × issue_signal` < 0).

---

## Data products (PID × period panels)

Work at `PID × period` (period = month or quarter; parameterize). Build and write these panels to Parquet:

1) `complaints_panel.parquet`
- Keys: `PID`, `period`
- Vars: `complaints_total`
- If possible: `complaints_severe`, `complaints_nonsevere` (based on category / substantiation / severity)

2) `issue_signal_panel.parquet` (credible issue proxies)
- Keys: `PID`, `period`
- Vars: `li_violations`, `li_investigations`
- Optional severe flags: `unsafe_building`, `imminently_dangerous` if available
- Use `log1p_*` transforms in regression script, not in raw panel.

3) `permits_panel.parquet` (maintenance proxy)
- Keys: `PID`, `period`
- Vars: `permits_total`
- Optional: `permits_repair` (subset to repair-relevant permit types)

4) `evictions_panel.parquet`
- Keys: `PID`, `period`
- Vars: `evictions_filed` (count or indicator)

5) `composition_panel.parquet` (InfoUSA composition / risky share)
- Keys: `PID`, `period`
- Vars: `mu_risky` (share of risky tenants), plus `n_households` if available
- Define “risky” using an observable proxy (e.g., low-income band, high turnover, etc.). Document the rule in code comments and keep it consistent across time.

Merge these to create `analysis_panel.parquet` with all vars.

**Engineering requirements**
- Use `data.table` throughout.
- Make a balanced time grid per PID where feasible (explicitly create missing PID×period rows).
- Do not replace missing leads/lags with zero. Use `NA` outside feasible windows.
- Log merge rates and missingness.

Outputs go to: `output/regressions/2026-02-24/`.

---

## Regression Family 1: Complaint suppression conditional on issue environment

### Spec A (baseline)
OLS on `log1p`:
- `log1p(complaints_total) ~ mu_risky + log1p(li_violations) + log1p(li_investigations) | PID + period_fe`
- Cluster: `PID`

Expected sign: coefficient on `mu_risky` <= 0.

### Spec B (interaction: sharp diagnostic)
- `log1p(complaints_total) ~ mu_risky * log1p(li_violations) + mu_risky * log1p(li_investigations) | PID + period_fe`

Expected sign: interaction terms negative (issue-signal response weaker when `mu_risky` is higher).

### Robustness
- Replace outcome with `complaints_severe` where available.
- Run `fepois` for count outcomes:
  - `complaints_total ~ ... | PID + period_fe` with `fepois` and cluster `PID`.

Deliver:
- LaTeX tables via `fixest::etable`
- Coef plots of `mu_risky` and interactions
- A marginal effects plot: effect of issue signal at `mu_risky` quantiles (e.g., p10, p50, p90)

---

## Regression Family 2: Maintenance / permits response and weakened discipline

### Spec C (levels)
- `log1p(permits_total) ~ mu_risky + log1p(li_violations_lag1) + log1p(li_investigations_lag1) | PID + period_fe`

### Spec D (interaction)
- `log1p(permits_total) ~ mu_risky * log1p(li_violations_lag1) + mu_risky * log1p(li_investigations_lag1) | PID + period_fe`

Expected sign: interactions negative (permit response attenuated in high-risk-share buildings).

### Horizon variants
Create helper to build future sums:
- `permits_fwd_H = sum_{h=1..H} permits_total[t+h]` for H in {2,4}
Run same specs with `log1p(permits_fwd_H)` as outcome.

---

## Regression Family 3: Eviction/retaliation dynamics around complaint events (building-level)

Define an event:
- `t0` = first period with `complaints_severe > 0` after a clean pre-window (no severe complaints for `K_pre` periods)
- `event_time = t - t0` in [-K_pre, +K_post]

Estimate event study:
- `evictions_filed ~ i(event_time, ref=-1) | PID + period_fe`
and heterogeneity:
- `evictions_filed ~ i(event_time, ref=-1) * mu_risky | PID + period_fe`

Requirements:
- Do not fill missing event_time with zeros.
- Provide CLI args: `--K_pre`, `--K_post`, `--balanced_window`.

Deliver:
- Event-study plots (baseline and interacted)
- LaTeX table of key post coefficients

---

## Deliverables (files to create)

1) `tasks/build_panels/build_analysis_panel.R`
- Reads raw sources, creates the 5 panels, merges into `analysis_panel.parquet`.

2) `tasks/run_regressions/run_complaint_regs.R`
3) `tasks/run_regressions/run_permit_regs.R`
4) `tasks/run_regressions/run_event_study_regs.R`

5) `tasks/run_all_regressions.R`
- CLI: `--period=month|quarter`, `--outdir=...`, `--K_pre=...`, `--K_post=...`, `--balanced_window=...`
- Calls all three regression scripts and writes outputs to `output/regressions/2026-02-24/`.

---

## Notes
- Conditioning on `permits_total` directly can be a bad control (endogenous response). Prefer conditioning on credible issue proxies (violations/investigations).
- Use `log1p()` consistently for skewed counts in OLS; use `fepois` as robustness.

---

## Implementation notes (2026-02-25 / 2026-02-26)

Standalone note for this iteration: `docs/notes/retaliatory-complaint-suppression-2026-02-26.md`

Status update from current iteration (repo-style implementation, not the original `tasks/` layout above):

- Added a severe-complaint PPML complaint-suppression ladder inside `r/retaliatory-evictions.r` and rewired the complaint-suppression section of `writeups/retaliatory-evictions.qmd` to report it.
- Ladder now has 4 specs (weighted by units, clustered by PID):
  1. `total_severe_complaints ~ infousa_pct_black | year`
  2. `+ log(total_area) + tract + building type + unit-bin + year-built decade + stories bin + quality FE`
  3. `+ lagged maintenance stock / unit` (EWMA over repair permits: electrical, plumbing, mechanical, fire suppression; default `lambda = 0.9`)
  4. `+ lagged severe-violations stock / unit` (EWMA over `hazardous + unsafe` violations; same `lambda`)
- Added a severe-ladder sample/missingness table (`pct_black` / tenant-comp / area / tract coverage) to the QMD.
- Added `only_complaint_suppression=TRUE` mode (default in current branch) so `r/retaliatory-evictions.r` skips distributed-lag / LP-DiD / targeting blocks and also skips the legacy suppression LPM / long-run suppression blocks. This makes iteration much faster while preserving existing outputs for other modes.
- Added a scratch-only standalone script for debugging the severe ladder (`scratch/complaint-suppression-severe-ppml-scratch.R`) with inline prep steps (not intended for commit to main analysis pipeline).

Key empirical diagnostics discovered while iterating:

- The positive maintenance-stock coefficient in the severe PPML ladder is not a coding bug in the PPML fit; it persists in the analytic-sample PID universe and remains positive after adding the lagged severe-violations stock proxy in Reg 4.
- The larger issue is **InfoUSA coverage / sample selection**:
  - In `bldg_panel_blp`, `infousa_pct_black` is only non-missing for roughly ~56-65% of PID-year rows in most years.
  - `2020` is an anomaly: race fields become more available, but other demographic fields degrade sharply (e.g., many `infousa_share_persons_demog_ok` values are zero and `infousa_num_households` often equals 1).
  - `2023-2024` have effectively **zero** coverage for the InfoUSA demographic share fields (`infousa_pct_black`, `infousa_pct_white`, `infousa_pct_female`, etc.).
- Consequence: unit-mass coverage for `% Black` regression samples falls materially relative to the `bldg_panel_blp` total, and drops to zero in 2023-2024.

Immediate next step (before interpreting demographic coefficients further):

- Audit the upstream InfoUSA demographics product (`infousa_building_demographics_panel` / `bldg_panel_blp` merge behavior) and decide whether the complaint-suppression demographic specs should:
  - cap years at `<= 2022`
  - treat `2020` as a sensitivity exclusion
  - sample on `!is.na(infousa_pct_black)` (rather than full `tenant_comp_missing == 0`) for `% Black`-only regressions

---

## Condo / Common-Address Follow-up (2026-02-26)

- New plan note: `codex-plans/condo_common_address_linking_plan_2026-02-26.md`
- New issue note: `issues/infousa-condo-common-address-linking-and-unit-imputation.md`
- Summary:
  - Distinguish `ambiguous_match_condo_groupable` vs `unmatched_address` across `infousa`, `altos`, and `evictions`
  - Keep parcel `PID` immutable
  - Add condo/property-level `link_id` + `anchor_pid` + provenance / ownership-unsafe flags
  - Use flags to exclude condo-group-linked rows from ownership/transfer analyses while preserving coverage for tenant composition analyses

---

## Implementation Checklist (2026-02-27)

- Added standalone implementation checklist note:
  - `docs/notes/comparative-statics-implementation-checklist-2026-02-27.md`
- Purpose:
  - Converts `codex-chats/comparative-statics-and-codex-recap.txt` into script-level phases,
    QA gates, run order, and decision points.
  - Keeps EB filing intensity unchanged as the active path.
  - Updated per user direction: removed the `no_filings_pre_covid` implementation track from the checklist.

---

## Crosswalk QC Iteration (2026-03-03)

- Standalone note: `docs/notes/crosswalk-qc-2026-03-03.md`
- Focused writeup: `docs/notes/crosswalk-qc-mean-pct-black-2026-03-03.md`
- Script: `r/cross-walk-qc.R`
- Outputs: `output/qa/crosswalk_qc_*` and `output/logs/cross-walk-qc.log`
- Purpose: quantify 2006-2019 eviction coverage dropoff from unique parcel match to InfoUSA-linked PID, and test whether dropped cases are more concentrated in higher-Black tracts.

---

## Retaliatory EB Memo + Filing-Bin Update (2026-03-04)

- New memo note: `docs/notes/eb-filing-rate-memo-2026-03-04.md`
- Script updates:
  - `r/retaliatory-evictions.r` now uses EB pre-COVID filing rate as default for maintenance models.
  - Added EB filing bins: `[0,1)`, `[1,5)`, `[5,10)`, `[10,20)`, `[20,inf)`.
  - High-filing dummy cutoff set to `>= 15%`.
  - Raw filing-rate maintenance regressions exported separately for appendix output.
- Writeup updates:
  - `writeups/retaliatory-evictions.qmd` includes an EB construction memo near the top.
  - Raw filing-rate maintenance table moved to an appendix section.

---

## Address-History EB-First + Appendix Raw Update (2026-03-04)

---

## Outer Sorting Approximation + InfoUSA Income Proxy (2026-03-09)

- Standalone note:
  - `docs/notes/outer-sorting-final-projection-and-infousa-income-proxy-2026-03-09.md`
- Issue:
  - `issues/outer-sorting-final-projection-approximation.md`
- Code changes:
  - `r/build-outer-sorting-empirical-moments.R`
    - removed tract poverty fallback; empirical outer-sorting target now uses BG composition proxy only
  - `r/qa-infousa-income-fields.R`
    - added QA script for modeled InfoUSA income / wealth fields against BG / tract poverty proxies
- Verification:
  - reran `r/make-outer-sorting-empirical-moments.R`
  - ran `r/qa-infousa-income-fields.R`
- Current read:
  - BG-only empirical target drops `700` building-year rows from `8` BGs
  - `find_div_1000` looks like the cleanest first-pass InfoUSA income proxy for future building-level composition work
  - the simulator’s post-projection rate refresh remains a documented approximation kept for computational reasons

- Shared memo note updated:
  - `docs/notes/eb-filing-rate-memo-2026-03-04.md`
- Script updates:
  - `r/address-history-analysis.R` now treats EB (`filing_rate_eb_pre_covid`) as main filing measure in persistence and trajectory filing regressions.
  - Filing bins standardized to `[0,1)`, `[1,5)`, `[5,10)`, `[10,20)`, `[20,inf)` (removed "No filings" split).
  - High-filing cutoff set to `>= 15%`.
  - Raw filing-rate persistence/trajectory tables exported separately for appendix robustness.
- Writeup updates:
  - `writeups/address-history-analysis.qmd` includes EB construction memo near the top.
  - Main text now reports EB filing regressions; raw filing regressions moved to appendix section.

---

## Address-History High-Dummy + LOO Split + Violations Outcomes (2026-03-04)

- Script updates (`r/address-history-analysis.R`):
  - Added destination-level filing specs replacing middle-bin regressors with a high-filing dummy (`>= 15%`), for EB (main) and raw (appendix).
  - Added LOO tract filing-rate splits:
    - EB LOO outcomes in main tables
    - raw LOO outcomes in appendix
    - each with both bin and high-filing-dummy variants.
  - Added complaints-analog outcome families:
    - total violations per unit (delta + destination tables)
    - severe violations per unit (delta + destination tables).
  - Expanded fixest dictionary labels so FE/control rows print human-readable names.
- Writeup updates (`writeups/address-history-analysis.qmd`):
  - Added sections/tables for filing high-dummy destination specs.
  - Added EB LOO main + raw LOO appendix tables (bins + high-dummy).
  - Added total/severe violations outcome tables.

---

## Outer Sorting Baseline Simulation Wiring (2026-03-06)

- Added runnable outer sorting simulation modules:
  - `r/build-outer-sorting-params.R`
  - `r/simulate-outer-sorting.R`
  - `r/outer-sorting-moments.R`
  - `r/run-outer-sorting-sim.R`
- Wired config contracts:
  - `products.outer_sorting_sim_panel`
  - `products.outer_sorting_sim_moments`
  - `products.outer_sorting_sim_diagnostics`
  - `run.outer_sorting.*` parameter block in both `config.yml` and `config.example.yml`
- Updated canonical docs:
  - `docs/data-products.md` entries for all three simulation products
  - `README.md` manual run command and DGP-spec pointer
- Iteration note:
  - `docs/notes/outer-sorting-sim-baseline-2026-03-06.md`

---

## Outer Sorting Baseline Calibration Wiring + Run (2026-03-06)

- Added calibration module and runner:
  - `r/calibrate-outer-sorting.R`
  - `r/run-calibrate-outer-sorting.R`
- Added calibration config block:
  - `run.outer_sorting.calibration.*` in `config.yml` and `config.example.yml`
- Added README manual command:
  - `Rscript r/run-calibrate-outer-sorting.R`
- Executed calibration:
  - convergence code `0` (fast-exit: starting objective below tolerance)
  - outputs written to `output/outer_sorting/`
- Iteration note:
  - `docs/notes/outer-sorting-calibration-baseline-2026-03-06.md`

---

## Outer Sorting Review Fixes (2026-03-06)

- Fixed reported bugs/issues:
  - maintenance moment now uses observed `c_rate` (not latent `cbar`)
  - high-filing indicator uses fixed absolute threshold (no longer simulated percentile)
  - explicit non-convergence handling/warning
  - recompute expected rates after final citywide-mass projection
- Calibration robustness updates:
  - default `maxit` increased to 300
  - self-target warning when using `outer_sorting_sim_moments`
  - non-converged objective evaluations now receive penalty
- Config/docs updates:
  - added `run.outer_sorting.high_filing_threshold`
  - added `run.outer_sorting.calibration.high_filing_threshold`
  - updated product contract docs for fixed threshold + `c_rate` maintenance moment
- Iteration note:
  - `docs/notes/outer-sorting-review-fixes-2026-03-06.md`

---

## Outer Sorting First-Pass Calibration Design Reset (2026-03-06)

- Anchored tenant-side parameters:
  - `mu = 0.30`
  - `delta_L = 0.01`
  - `delta_H = 0.50`
- Kept `b_Fs_hard` fixed in the first pass.
- Expanded default calibration parameter list to:
  - `Dalph`
  - `p_bad_shape1`, `p_bad_shape2`
  - `aF`, `bF_theta`
  - `aC`, `bC_theta`, `bC_s`, `bC_M`
  - `aM`, `bM_theta`, `bM_C`
- Replaced the prior compact moment set with the requested 12-moment first-pass target set:
  - three mean rates
  - three zero shares
  - two sorting moments
  - two complaint-gradient moments
  - one maintenance-discipline moment
  - one filing-dispersion moment
- Note:
  - `comp-statics-annual-race` does not currently export a direct filing-on-composition slope, so `b_Fs_hard` remains a fixed placeholder config value.
- Iteration note:
  - `docs/notes/outer-sorting-first-pass-calibration-design-2026-03-06.md`

---

## Outer Sorting Baseline Memo (2026-03-07)

- Added a Quarto writeup:
  - `writeups/outer-sorting-baseline.qmd`
- The memo summarizes:
  - equilibrium setup and parameterization
  - baseline fixed and calibrated parameters
  - convergence diagnostics from the current simulation run
  - resulting equilibrium moments, type-level summaries, and figures
- Render fixes included:
  - explicit project-root path resolution from `writeups/`
  - deterministic figure sampling
  - removal of duplicated raw-LaTeX table labels
- Produced rendered artifact:
  - `writeups/outer-sorting-baseline.pdf`
- Iteration note:
  - `docs/notes/outer-sorting-baseline-memo-2026-03-07.md`

---

## Outer Sorting Default-Risk Filing Baseline (2026-03-07)

- Replaced direct filing-on-composition with:
  - `delta_bar_b = s_b * delta_H + (1 - s_b) * delta_L`
  - `log fbar_b = aF + bF_theta * 1{theta_b=B} + bF_delta * delta_bar_b`
- Removed `bF_s_hard` from the active baseline config and added calibrated `bF_delta`.
- Updated:
  - simulator/config/calibration code
  - `latex/sorting_model_outer_dgp.tex`
  - `codex-plans/codex_outer_sorting_dgp_spec.txt`
  - `writeups/outer-sorting-baseline.qmd`
- Presentation updates in the memo:
  - parameter table shrunk with `\tiny`
  - old scatter replaced with by-type histograms
- Verified with:
  - `Rscript r/run-outer-sorting-sim.R`
  - `Rscript r/run-calibrate-outer-sorting.R`
  - `quarto render writeups/outer-sorting-baseline.qmd`
- Iteration note:
  - `docs/notes/outer-sorting-default-risk-filing-baseline-2026-03-07.md`

---

## Outer Sorting Empirical Target Scaffolding (2026-03-07)

- Added renter-poverty geography builders:
  - `r/build-renter-poverty-geo.R`
  - `r/make-renter-poverty-geo.R`
- Added empirical target builder:
  - `r/build-outer-sorting-empirical-moments.R`
  - `r/make-outer-sorting-empirical-moments.R`
- Added recovery runner:
  - `r/outer-sorting-recovery.R`
  - `r/run-outer-sorting-recovery.R`
- New processed products:
  - `bg_renter_poverty_share`
  - `tract_renter_poverty_share`
  - `outer_sorting_empirical_moments`
- Empirical design:
  - base panel = `bldg_panel_blp`
  - composition proxy = block-group renter poverty share
  - neighborhood FE = tract
  - default pooled target window = `2017-2019`
- Verification:
  - toy smoke tests passed for poverty build, empirical moments, and a small recovery run
  - real ACS fetch failed in this environment because Census API access is blocked
- Local command to run:
  - `Rscript r/make-renter-poverty-geo.R`
- Iteration note:
  - `docs/notes/outer-sorting-empirical-target-scaffolding-2026-03-07.md`

---

## Outer Sorting Empirical Collapse Window (2026-03-07)

- Updated the empirical target collapse in `r/build-outer-sorting-empirical-moments.R`.
- Default empirical window is now `2011-2019`.
- PID-level empirical rows now:
  - drop buildings with `year_built > year_to`
  - use observed active years within the window for annualization
  - compute per-unit-year rates from total counts over total unit-years
- Config updated with `run.outer_sorting.empirical.year_built_col = "year_built"`.
- Iteration note:
  - `docs/notes/outer-sorting-empirical-collapse-window-2026-03-07.md`

---

## Outer Sorting Memo Empirical Calibration Section (2026-03-07)

- Updated `writeups/outer-sorting-baseline.qmd` to report the current empirical calibration run.
- Added memo tables for:
  - calibration run summary
  - baseline vs calibrated parameters
  - largest calibration moment misses
- Updated memo interpretation text so it no longer describes the current calibration as a self-calibration check.
- Verified with:
  - `quarto render writeups/outer-sorting-baseline.qmd`
- Iteration note:
  - `docs/notes/outer-sorting-memo-empirical-calibration-section-2026-03-07.md`

---

## Outer Sorting Empirical Unit Draws (2026-03-07)

- Replaced the default simulated unit draw with empirical resampling from PID-level average annual `total_units` in `bldg_panel_blp`.
- Added config controls:
  - `units_draw_mode`
  - `units_empirical_product_key`
  - `units_empirical_year_from`
  - `units_empirical_year_to`
  - `units_empirical_year_built_col`
- Kept the old discrete-bin unit draw as a fallback mode only.
- Updated memo language to state that mean rates are unit-weighted and zero shares are building shares.
- Verified the new unit distribution:
  - empirical mean units `2.065`
  - simulated mean units `2.082`
- Verified with:
  - `Rscript r/run-outer-sorting-sim.R`
- Iteration note:
  - `docs/notes/outer-sorting-empirical-unit-draws-2026-03-07.md`

---

## Outer Sorting Memo Empirical Neighborhood Comparisons (2026-03-07)

- Updated `writeups/outer-sorting-baseline.qmd` to add empirical vs simulated neighborhood comparisons.
- Added memo artifacts for:
  - empirical filing-rate check
  - empirical vs simulated neighborhood distribution summary
  - neighborhood filing-rate distribution comparison
  - neighborhood poverty-share distribution comparison
- Confirmed the current empirical filing-rate target is about `0.039` unit-weighted and `0.041` unweighted under the present per-unit-year definition.
- Verified with:
  - `quarto render writeups/outer-sorting-baseline.qmd`
- Iteration note:
  - `docs/notes/outer-sorting-memo-empirical-neighborhood-comparisons-2026-03-07.md`

---

## Outer Sorting InfoUSA Composition Proxy (2026-03-09)

- Added `r/build-infousa-building-income-proxy.R` and `r/make-infousa-building-income-proxy.R`.
- New product:
  - `infousa_building_income_proxy_panel`
- The default empirical composition proxy is now building-level:
  - `infousa_find_low_income_share`
  - defined as the share of linked primary households with `find_div_1000 <= 30`
- Updated `r/build-outer-sorting-empirical-moments.R` to use the new PID-year proxy product and household-count weights when collapsing to one PID row over `2011-2019`.
- Verified with:
  - `Rscript r/make-infousa-building-income-proxy.R`
  - `Rscript r/make-outer-sorting-empirical-moments.R`
  - `Rscript r/run-calibrate-outer-sorting.R`
  - `quarto render writeups/outer-sorting-baseline.qmd`
- Iteration note:
  - `docs/notes/outer-sorting-infousa-composition-proxy-2026-03-09.md`
