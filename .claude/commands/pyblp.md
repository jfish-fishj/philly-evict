# PyBLP Estimation Helper

You are helping the user iterate on their nested logit demand estimation using PyBLP.
The estimation code lives in `python/pyblp_estimation.py` with an `EstimationConfig` dataclass
controlling all tunable parameters.

User's question: $ARGUMENTS

---

## Quick Reference: `bldg_panel_blp` Columns

**Identifiers:**
- `PID` — parcel ID (product_ids)
- `pm.zip` — ZIP code
- `owner_grp` — owner group (firm_ids)
- `year` — year
- `census_tract` — census tract

**Price / Share:**
- `log_med_rent` — log median rent (→ `prices`)
- `share_units_zip_unit` — market share by units in zip (→ `shares`)
- `total_market_share_units_bins` — total market share by unit-size bin

**Building characteristics:**
- `num_units_imp` / `total_units` — number of units (legacy / current)
- `num_units_bins` — binned unit count category
- `num_units_zip` — total units in the ZIP
- `total_area` — total building area (sq ft)
- `year_built` — year built
- `building_code_description_new_fixed` — building type (Apartment, Coop, Mixed Use, etc.)
- `quality_grade` — quality grade (A+, A, A-, B+, B, etc.)
- `market_value` — assessed market value
- `total_tax` — total tax assessment
- `change_log_taxable_value` — year-over-year change in log taxable value

**Eviction / Filing:**
- `filing_rate` — eviction filing rate
- `filing_rate_preCOVID` — pre-COVID filing rate (used for nesting threshold)
- `num_filings` — number of filings

**Instruments (constructed in pipeline):**
- `demand_instruments0` — prices (slot for IV)
- `demand_instruments1` — nest sizes
- `z_cnt_1to3km`, `z_mean_1to3km_log_taxable_value`, etc. — spatial instruments (if present)

**Other:**
- `adj_occ_rate` — adjusted occupancy rate
- `num_households` — number of households
- `source` — data source flag

---

## PyBLP API Quick Reference

### Formulation
```python
import pyblp as blp
# Linear characteristics (no random coefficients in simple/nested logit)
X1 = blp.Formulation("0 + prices + log_market_value + log_total_area + year_built",
                      absorb="C(census_tract)*C(year) + C(building_code_description_new_fixed)")
```
- `0 +` suppresses the intercept (absorbed by FEs)
- `C(var)` creates categorical dummies
- `C(a)*C(b)` creates the full interaction
- Multiple absorb terms separated by ` + `

### Problem & Solve
```python
problem = blp.Problem(X1, df)  # df must have: market_ids, firm_ids, product_ids, prices, shares, nesting_ids, demand_instruments{k}
results = problem.solve(rho=0.2)  # rho = nesting parameter initial value
```

### Post-Estimation
```python
elasticities = results.compute_elasticities()       # J×J per market
diversions = results.compute_diversion_ratios()      # J×J per market
own_means = results.extract_diagonal_means(elasticities)  # mean own-elasticity per market

# Key attributes:
results.beta       # linear parameter estimates
results.beta_se    # standard errors
results.rho        # estimated nesting parameter
results.delta      # mean utilities (J×1)
results.xi         # demand unobservables
```

---

## `EstimationConfig` Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `year_range` | `(2011, 2019)` | Inclusive year window |
| `min_units` | `5` | Drop buildings with fewer units |
| `max_units` | `500` | Drop buildings with more units |
| `min_units_zip` | `100` | Min total units in ZIP |
| `building_type_regex` | `"Apartment\|Coop\|Mixed Use"` | Regex filter on building_code_description_new_fixed |
| `max_abs_change_log_taxable_value` | `1.25` | Max year-over-year tax value change |
| `max_annualized_change_log_rent` | `0.2` | Max annualized rent change |
| `require_log_market_value_positive` | `True` | Require log(market_value) > 0 |
| `min_obs_per_pid` | `1` | Min observations per product |
| `market_cols` | `("pm.zip", "year", "num_units_bins")` | Columns defining market IDs |
| `firm_col` | `"owner_grp"` | Column for firm IDs |
| `product_col` | `"PID"` | Column for product IDs |
| `nest_threshold_col` | `"filing_rate_preCOVID"` | Column for nest assignment |
| `nest_threshold` | `0.1` | Threshold: >= this → first nest label |
| `nest_labels` | `("high", "low")` | (above_threshold, below_threshold) |
| `x1_formula` | `"0 + prices + ..."` | PyBLP Formulation string |
| `absorb_formula` | `"C(census_tract)*C(year) + ..."` | Absorbed FEs |
| `rho_init` | `0.2` | Initial nesting parameter |
| `diversion_target_nest` | `"low"` | Nest to bump in post-estimation |
| `diversion_pct_increase` | `0.10` | Price increase for counterfactuals |

---

## Diagnostic Guidance

### Weak Instruments
- Check `demand_instruments0` correlation with endogenous `prices` after absorbing FE
- Use `report_instrument_checks(est_df, "prices", ["demand_instruments0"], fe_cols=["market_ids"])`
- F-stat < 10 → weak instruments; consider adding spatial instruments (`z_cnt_1to3km`, etc.)

### Convergence Issues
- If `solve()` doesn't converge: try different `rho_init` (0.1, 0.3, 0.5)
- Check `results.converged` — if False, results are unreliable
- Reduce sample complexity: fewer FE interactions, simpler formulation

### Elasticity Sanity Checks
- Own-price elasticities should be **negative** (price up → demand down)
- Typical range for rental housing: -0.3 to -2.0
- If all near zero: check that prices have sufficient within-market variation
- If implausibly large (< -5): possible share measurement issues

### Nesting Parameter (rho)
- `rho = 0` → standard logit (no within-nest correlation)
- `rho → 1` → products within a nest are perfect substitutes
- Typical reasonable values: 0.1–0.7
- If `rho` hits boundary (0 or ~1): nesting may not be identified

### Common Errors
- "shares do not sum to less than 1" → filter markets with total shares >= 1
- NaN in instruments → check for zero-valued variables before log transforms
- "singular matrix" → perfect collinearity in FE + covariates; simplify absorb formula

---

## Typical Workflow

```python
from pyblp_estimation import EstimationConfig, main

# 1) Run with defaults
out = main()

# 2) Inspect results
print(out["results"])
print(out["post_est"]["own_elasticities"].mean())

# 3) Iterate — change config
cfg2 = EstimationConfig(year_range=(2016, 2019), min_units=10, rho_init=0.3)
out2 = main(est_cfg=cfg2)

# 4) Compare
print(f"Baseline rho: {float(out['results'].rho):.3f}")
print(f"New rho: {float(out2['results'].rho):.3f}")

# 5) First-stage diagnostics
from pyblp_estimation import report_instrument_checks
report_instrument_checks(out["est_df"], "prices", ["demand_instruments0"],
                         fe_cols=["market_ids"])
```

---

When answering, always:
1. Read `python/pyblp_estimation.py` to understand current code state
2. Reference specific `EstimationConfig` fields when suggesting parameter changes
3. Show concrete code snippets the user can run
4. Flag any sample restriction changes that might affect identification
