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
