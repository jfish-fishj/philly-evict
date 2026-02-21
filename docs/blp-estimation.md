# BLP Estimation Pipeline

## Overview

`python/pyblp_estimation.py` estimates a nested logit demand model for Philadelphia rental housing using the PyBLP library. The model treats buildings as "products" and zip-years as "markets".

## `EstimationConfig`

All estimation parameters are controlled via the `EstimationConfig` dataclass:

| Parameter | Default | Description |
|---|---|---|
| `shares_col` | `"share_zip"` | Which share column to use |
| `market_cols` | `["pm.zip", "year"]` | Columns defining markets |
| `firm_col` | `"owner_mailing_clean"` | Firm identifier |
| `product_col` | `"PID"` | Product identifier |
| `nest_threshold_col` | `"filing_rate_eb_pre_covid"` | Column for nest assignment |
| `nest_threshold` | `0.1` | Threshold for high/low nest |
| `year_range` | `(2011, 2019)` | Estimation sample years |
| `min_units` / `max_units` | `6` / `500` | Unit count filters |
| `max_annual_rent_change` | `0.2` | Max annualized log-rent change |

## Pipeline Steps

### 1. `load_and_prepare_data()`

1. Loads `bldg_panel_blp` CSV
2. Creates `num_units_bin` if missing
3. **Computes shares on-the-fly** from full raw data (before filtering):
   - `shares = occupied_units / sum(total_units)` by market group
   - Denominator = `total_units` (vacancy as outside good)
4. Creates `market_ids`, `firm_ids`, `product_ids`, `nesting_ids`
5. Applies filters: year range, unit count, building type, rent change, missing values
6. Drops markets with share sums >= 1

### 2. `build_formulation()`

Constructs PyBLP `Formulation` objects:
- **X1** (linear characteristics): intercept + observed product characteristics
- **X2** (random coefficients): price
- **X3** (supply): not used (demand-only)

### 3. `build_problem()` / `solve_problem()`

Sets up and solves the PyBLP `Problem`:
- Absorbs fixed effects (PID, GEOID×year)
- Uses nesting structure for nested logit
- Starting value for rho (nesting parameter) = 0.5

### 4. Post-estimation

- Computes own-price elasticities
- Computes diversion ratios (within-nest, to outside)
- Generates summary tables (LaTeX)

## Share Construction

Shares are computed from the **full** raw dataset before any sample filters:
```python
df[denom_name] = df.groupby(grp)["total_units"].transform("sum")
df["shares"] = df["occupied_units"] / df[denom_name]
```

This ensures the denominator reflects the full market, not just the estimation sample.

## Fixed Effects

| Spec | Description |
|---|---|
| PID | Building fixed effects (absorbs time-invariant building characteristics) |
| GEOID×year | Census tract × year (absorbs neighborhood-year shocks) |

## IV Investigation (Findings)

Tax assessment instruments (`change_log_taxable_value`, `z_sum_otherfirm_*`) are **dead** under PID + GEOID×year FE:
- First-stage F-statistic: 0.03
- Only 6.3% of rent variation survives demeaning
- Instruments capture cross-sectional/geographic-time variation, not within-building-within-market-year variation

**Pre-share-fix baseline** (with frozen `renter_occ` and `sum(renter_occ)` denominator): price = -0.24***, rho = 0.156, own elasticities ~ -2. These results were obtained under buggy share definitions that happened to work for the estimator.

**Post-share-fix** (2026-02-12, with corrected `renter_occ` and `sum(total_units)` denominator): rho hits upper bound (0.99), price coefficient = +0.034 (insignificant, wrong sign). The model is degenerate because within-PID share variation is ~2% of total — not enough for the FE structure. See `claude-chats/claude-chat-2026-02-12-option-a-share-fix.md` for full writeup.

## CLI Usage

```bash
# Baseline (no IV)
/opt/anaconda3/envs/pyblp-env/bin/python python/pyblp_estimation.py

# With IV specs
/opt/anaconda3/envs/pyblp-env/bin/python python/pyblp_estimation.py --iv full
/opt/anaconda3/envs/pyblp-env/bin/python python/pyblp_estimation.py --iv pid_year
/opt/anaconda3/envs/pyblp-env/bin/python python/pyblp_estimation.py --iv pid
```

## Key Assumptions and Limitations

1. **All-renters assumption**: InfoUSA `num_households` treated as renter count for identified rental properties
2. **Static building stock**: `total_units` does not change over time for pre-2010 buildings
3. **No supply-side**: Demand-only estimation (no pricing equation)
4. **Nesting by eviction rate**: Binary high/low nesting based on pre-COVID filing rate >= 0.1
5. **Missing data**: Buildings with NA `renter_occ` are dropped (honest about coverage gaps)
6. **Within-PID share variation**: ~98% of share variance is between-PID (building size). Only ~2% is within-PID over time. This is a fundamental data limitation — InfoUSA occupancy counts don't vary enough year-to-year to generate meaningful share dynamics at the building level.

## Estimation Status (2026-02-12)

The current share definitions are **definitionally correct** but the building-level nested logit with PID + GEOID×year FE does not converge to economically meaningful parameters. Possible paths forward:
- Coarser product definitions (aggregate buildings to owner×zip×bin)
- Relaxed FE (drop PID FE, use only GEOID×year)
- Bin-level markets (`share_zip_bin` with zip×year×bin as market)
- Alternative demand quantity proxies (listing counts, applications)
- Aggregated time windows (2-3 year panels)
