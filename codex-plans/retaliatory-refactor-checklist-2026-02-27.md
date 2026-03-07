# Retaliatory Refactor Checklist (2026-02-27)

Goal: make `r/retaliatory-evictions.r` a single-purpose analysis script (retaliatory timing), and remove complaint suppression from it entirely.

Progress note (2026-02-27):
- Completed: core refactor of `r/retaliatory-evictions.r` to dist-lag + bandwidth only.
- Completed: `writeups/retaliatory-evictions.qmd` rewritten to the same narrowed scope.
- Completed: `writeups/comp-statics-annual-race.qmd` annotated as canonical complaint-suppression location.
- Pending: config/README/docs cleanup for retired retaliatory outputs and legacy flags.

## Scope decisions (keep / move / delete)

| Current block in `r/retaliatory-evictions.r` | Decision | Destination |
|---|---|---|
| Panel assembly (PID-period panel, filings merge, complaint flags, leads/lags, bandwidth tables) | Keep | `r/retaliatory-evictions.r` |
| Distributed-lag models (main + `% Black` split) | Keep | `r/retaliatory-evictions.r` |
| LP-DiD block | Move | `r/retaliatory-lpdid.R` (separate optional script) |
| Same-period PID FE + tenant-augmented models | Delete from this workflow | remove (can recover from git history if needed) |
| Tract FE same-period variants | Delete from this workflow | remove |
| Retaliatory targeting (period-level + building-year + interaction slopes) | Delete from this workflow | remove |
| Complaint suppression severe PPML ladder + missingness + legacy suppression blocks | Delete entirely from retaliatory | use `r/comp-statics-annual-race.R` instead |
| Large output writer for legacy artifacts | Prune | keep only core retaliatory outputs |

## Phase 1: Lock output contract for retaliatory script

- [ ] Define required outputs for `r/retaliatory-evictions.r` (only):
  - `retaliatory_*_distlag_coefficients.csv`
  - `retaliatory_*_distlag_coefficients.png`
  - `retaliatory_*_distlag_coefficients_black_split.csv`
  - `retaliatory_*_distlag_coefficients_black_split.png`
  - `retaliatory_*_bandwidth_summary.csv`
  - `retaliatory_*_bandwidth_status_counts.csv`
  - `retaliatory_*_qa.txt`
- [ ] Define optional output for separate script:
  - `retaliatory_*_lpdid_coefficients.csv` (+ plot/table if kept)
- [ ] Explicitly retire legacy retaliatory outputs (same-period, targeting, suppression, long-run, interaction slope tables).

## Phase 2: Refactor `r/retaliatory-evictions.r` to one analysis

- [ ] Remove model blocks unrelated to dist-lag retaliation timing:
  - same-period models
  - tract FE variants
  - targeting blocks
  - complaint suppression blocks
- [ ] Keep fail-fast schema assertions for required columns.
- [ ] Keep panel QA needed for interpreting dist-lag and bandwidth summaries.
- [ ] Keep script-level logging and deterministic behavior.
- [ ] Simplify CLI/config flags by removing unused legacy toggles.

## Phase 3: Create/clean companion scripts

- [ ] Create `r/retaliatory-lpdid.R` (if LP-DiD is still desired), reusing panel-prep helpers.
- [ ] Ensure complaint suppression lives only in:
  - `r/comp-statics-annual-race.R`
- [ ] Do not duplicate suppression estimators in retaliatory scripts.

## Phase 4: QMD alignment (required)

- [ ] Edit `writeups/retaliatory-evictions.qmd`:
  - remove chunks/sections for same-period, tract FE, targeting, building-year targeting, and all suppression tables/figures
  - keep dist-lag, `% Black` heterogeneity dist-lag, and bandwidth diagnostics
  - if LP-DiD moved out, either remove LP-DiD section or read from new script outputs only
  - update narrative so this writeup is explicitly “retaliatory timing evidence”
- [ ] Edit `writeups/comp-statics-annual-race.qmd`:
  - ensure complaint suppression interpretation is centered there
  - include any missingness/coverage discussion previously shown in retaliatory QMD
  - add cross-reference note: suppression moved from retaliatory workflow

## Phase 5: Config + docs cleanup

- [ ] Update `config.yml` and `config.example.yml`:
  - remove/deprecate `retaliatory_run_legacy_targeting`
  - remove/deprecate `retaliatory_run_legacy_suppression`
  - keep only flags used by retained retaliatory analysis
- [ ] Update `README.md` analysis section and run commands to reflect the split:
  - `retaliatory-evictions.r` = timing/dist-lag
  - `comp-statics-annual-race.R` = complaint suppression
- [ ] Update `docs/data-products.md` output mappings if filenames/contracts changed.

## Phase 6: Verification checklist

- [ ] Run:
  - `Rscript --vanilla r/retaliatory-evictions.r`
  - `Rscript --vanilla r/comp-statics-annual-race.R`
- [ ] Render:
  - `quarto render writeups/retaliatory-evictions.qmd`
  - `quarto render writeups/comp-statics-annual-race.qmd`
- [ ] Confirm no retaliatory suppression artifacts are produced.
- [ ] Confirm both QMDs render with no missing-file warnings for removed sections.
- [ ] Spot-check that core dist-lag coefficients and suppression ladder coefficients match pre-refactor behavior (allowing only expected differences from code deletion/scope tightening).
