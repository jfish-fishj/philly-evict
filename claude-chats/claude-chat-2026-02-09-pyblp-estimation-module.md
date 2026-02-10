# Claude Chat - 2026-02-09 - PyBLP Estimation Module & Share Fix

## Summary

Created a structured, config-driven PyBLP nested logit estimation module replacing the scratch `nested-logit.py`. Diagnosed and fixed a critical market share construction bug in `make-analytic-sample.R` that caused shares to sum to exactly 1.0 within markets, eliminating the outside good and producing nonsensical estimates (positive price coefficient, rho at boundary, positive own-price elasticities).

## The Share Bug

### Problem
`share_units_zip_unit` was computed as `occupied_units / sum(occupied_units)` within each `(market_id, num_units_bin)` cell. This sums to exactly 1.0 by construction — no room for an outside good.

When the estimation script defined markets as `(zip, year, num_units_bin)` and used these shares, PyBLP saw 100% inside share in every market. The model had no outside option, so:
- **rho hit its upper bound (0.99)** — nesting param not identified
- **Price coefficient was positive (+0.04, insignificant)** — endogeneity unresolved
- **Own-price elasticities were +23** — wrong sign entirely
- **Diversion ratios were all zero** — overflow in NL share computation

### Root Cause
The share denominator (`total_occupied_units_bin`) was the sum of the same `occupied_units` that appeared in the numerator. Within the estimation market (zip × year × bin), every product was in both the numerator and denominator → shares summed to 1.

### Fix
1. **New share variable `share_zip`**: `occupied_units / sum(total_units)` by zip-year. Uses `total_units` (full housing stock, including vacant) as denominator. Shares mechanically sum to ≤ 1 (vacancy creates outside good). After filtering to apartments with >5 units, inside shares average ~11% — the ~89% outside good is single-family homes, small buildings, vacant units, etc.

2. **New share variable `share_zip_bin`**: Same logic but denominator = `sum(total_units)` within `(zip, year, num_units_bin)`.

3. **Market definition changed**: zip × year (dropped `num_units_bin` from market). The unit-size bin is a product characteristic, not a market dimension.

4. **Assertions added**: shares must sum ≤ 1 within each market; all shares in [0, 1].

### Results Comparison

| Metric | Before (broken) | After (fixed) |
|---|---|---|
| Price coefficient | +0.043 (p=0.92) | **-0.244 (p<0.001)** |
| rho | 0.990 (boundary) | **0.156 (SE=0.046)** |
| Own-price elasticities | +23.3 (wrong sign) | **-1.96** |
| Inside share sums | 1.00 | mean=0.11, max=0.50 |
| Diversion to outside | 0.00 | 0.80 |

## Files Created

### `python/config_helpers.py`
Shared config loading module extracted from `scrape-apartments.py`. Functions: `load_config()`, `p_product()`, `p_input()`, `p_output()`, `p_tmp()`. Mirrors R's `r/config.R` semantics.

### `python/pyblp_estimation.py`
Structured estimation module replacing `nested-logit.py`:

- **`EstimationConfig` dataclass** — All tunable parameters with defaults:
  - Sample restrictions: `year_range`, `min_units`, `max_units`, `building_type_regex`, etc.
  - Market/product/firm column definitions (`shares_col` defaults to `share_zip`)
  - Nest definition: threshold column, threshold value, labels
  - Formulation: `x1_formula`, `absorb_formula`
  - Solver: `rho_init`, `solve_kwargs`

- **Pipeline functions** (each logs row counts):
  - `load_and_prepare_data()` — Load from config, construct IDs, compute shares on-the-fly if missing
  - `build_estimation_sample()` — Apply all filters with per-step logging
  - `build_instruments()` — Construct `demand_instruments{k}` columns
  - `define_formulation()` / `estimate_model()` — PyBLP Problem + solve
  - `compute_post_estimation()` — Elasticities, diversions, nest swaps
  - `print_summary()` — Console summary with coefficients, elasticities by bin, diversion ratios
  - `export_tables()` — LaTeX output to `output/pyblp_summaries/`
  - `main()` — Orchestrator, returns dict with all objects for interactive use

- **Preserved utility functions** (cleaned up):
  - `residualize_against_fixed_effects()` — multi-way FE residualizer
  - `instrument_relevance_after_fe()` / `report_instrument_checks()` — first-stage diagnostics
  - `nl_shares_from_delta_single_level()` — manual NL share computation
  - `percent_swap_when_bumping_nests()` — counterfactual nest bump analysis
  - `diversion_ratios_for_target_nest()` — diversion ratio computation

### `.claude/commands/pyblp.md`
Slash command (`/pyblp`) with:
- Full `bldg_panel_blp` column reference
- PyBLP API quick reference (Formulation, Problem, solve, post-estimation)
- `EstimationConfig` parameter reference table
- Diagnostic guidance (weak instruments, convergence, elasticity sanity, nesting parameter)
- Typical interactive workflow examples

## Files Modified

### `python/scrape-apartments.py`
Replaced inline config functions with `from config_helpers import load_config, p_output, p_product`.

### `r/make-analytic-sample.R`
Section 7 (Market Shares & HHI):
- Added `total_units_market = sum(total_units)` by zip-year (all housing stock, incl. vacant)
- Added `share_zip = occupied_units / total_units_market` (BLP-ready: sums ≤ 1)
- Added `total_units_bin = sum(total_units)` by (zip-year, bin)
- Added `share_zip_bin = occupied_units / total_units_bin`
- Kept legacy `market_share_units` and `share_units_zip_unit` for backward compat
- Added assertion block: share sums ≤ 1 within markets, all values in [0, 1]
- Updated `required_cols` to include `share_zip`, `share_zip_bin`

## Key Design Decisions

- **`share_zip` as default**: Uses total housing stock as denominator. After filtering to apartments, inside shares are ~11% — large, well-defined outside good.
- **On-the-fly share computation**: If `share_zip` is missing from data (R script not re-run), the Python script computes it from `occupied_units` and `total_units`. No need to re-run the full R pipeline to test.
- **Market = zip × year**: Simpler than zip × year × bin. Unit-size bin is a product characteristic (used for elasticity breakdowns), not a market dimension.
- **Config-driven**: Every tunable parameter is a named field in `EstimationConfig`. No magic numbers buried in procedural code.

## IV Estimation Investigation

### Motivation
Following Calder-Wang (2024), attempted to instrument for prices using tax assessment changes, with property (PID) fixed effects to absorb time-invariant unobserved quality and GEOID×year FE to absorb market-time shocks.

### Instruments Tested
- `change_log_taxable_value` — own-property change in log taxable building value
- `z_sum_otherfirm_change_taxable_value` — sum of tax changes for other-firm properties in same market
- `z_sum_otherfirm_change_taxable_value_per_unit` — same, per unit
- `nest_sizes` — count of products per nest-market

### First-Stage Results by FE Structure

| FE Structure | F-stat | Assessment |
|---|---|---|
| None | 94.0 | Strong (cross-sectional) |
| PID only | 12.2 | Passes Staiger-Stock |
| Year only | 7.1 | Borderline |
| PID + Year | 6.3 | Borderline |
| GEOID + Year | 1.5 | Weak |
| GEOID×Year | 1.2 | Weak |
| **PID + GEOID×Year** | **0.03** | **Dead** |

### Root Cause of Weak Instruments

After absorbing PID + GEOID×Year, only **6.3% of rent variation** survives. The tax-value instruments capture:
- Cross-sectional variation (which building is expensive) — absorbed by PID FE
- Geographic-by-time variation (which neighborhood is appreciating) — absorbed by GEOID×year FE

The remaining idiosyncratic within-building-within-market-year rent variation has **near-zero correlation** (r ≈ 0.002) with tax assessment changes. Tax assessments simply don't predict building-specific rent deviations from the neighborhood-year trend.

The instruments do retain ~20% of their own variance after demeaning, so it's not that tax changes lack within-variation — it's that this variation is orthogonal to rent residuals.

### Conclusion
The baseline (no-IV) nested logit is the appropriate specification for now. IV with these instruments requires FE structures too coarse to be credible (PID only, no time controls). Possible future directions:
- Different instruments (permit activity, renovation timing, utility costs)
- Longer differencing to amplify within-building signal
- Cross-market instruments leveraging landlord portfolio exposure

### IV Configuration Added
The script now supports three IV specs via `--iv [full|pid_year|pid]`:
- `--iv full`: PID + GEOID×year (Calder-Wang spec, weak instruments)
- `--iv pid_year`: PID + year (additive, borderline F)
- `--iv pid`: PID only (strongest first stage, no time controls)
- Default (no `--iv`): no-IV nested logit (recommended)

## Environment Note

PyBLP estimation requires the `pyblp-env` conda environment:
```bash
conda activate pyblp-env
python python/pyblp_estimation.py        # baseline (recommended)
python python/pyblp_estimation.py --iv   # IV with PID + year FE
```
The base anaconda env has a NumPy 1.x/2.x incompatibility that prevents pandas from loading.
