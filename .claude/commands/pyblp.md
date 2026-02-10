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
- `owner_1` — owner name (firm_ids)
- `year` — year
- `GEOID` — census tract GEOID

**Price / Share:**
- `log_med_rent` — log median rent (→ `prices`)
- `share_zip` — product share of total housing stock in zip-year (BLP-ready, sums ≤ 1)
- `share_zip_bin` — product share of housing stock in zip-year-bin
- `share_units_zip_unit` — legacy share (sums to 1, NOT for BLP)

**Building characteristics:**
- `total_units` — number of units (current); `num_units_imp` (legacy fallback)
- `num_units_bin` — binned unit count category
- `num_units_zip` — total units in the ZIP
- `total_area` — total building area (sq ft)
- `year_built` — year built
- `building_code_description_new` — building type (APARTMENTS - BLT AS RESID, APTS - HIGH RISE, etc.)
- `quality_grade` — quality grade (A+, A, A-, B+, B, etc.)
- `market_value` — assessed market value
- `total_tax` — total tax assessment
- `change_log_taxable_value` — year-over-year change in log taxable value

**Eviction / Filing:**
- `filing_rate` — eviction filing rate
- `filing_rate_eb_pre_covid` — pre-COVID filing rate (empirical Bayes; used for nesting threshold)
- `num_filings` — number of filings

**Tax / Cost Instruments:**
- `change_log_taxable_value` — own-property change in log taxable building value
- `z_sum_otherfirm_change_taxable_value` — sum of tax changes for other-firm properties
- `z_sum_otherfirm_change_taxable_value_per_unit` — same, per unit
- Other `z_sum_*`, `z_cnt_*`, `z_mean_*` columns (spatial instruments)

**Other:**
- `adj_occ_rate` — adjusted occupancy rate
- `num_households` — number of households
- `source` — data source flag

---

## PyBLP API Quick Reference

### Formulation
```python
import pyblp as blp
X1 = blp.Formulation("0 + prices + log_market_value + log_total_area + year_built",
                      absorb="C(GEOID)*C(year) + C(building_code_description_new)")
```
- `0 +` suppresses the intercept (absorbed by FEs)
- `C(var)` creates categorical dummies
- `C(a)*C(b)` creates the full interaction
- Multiple absorb terms separated by ` + `

### Problem & Solve
```python
problem = blp.Problem(X1, df)
results = problem.solve(rho=0.2)  # rho = nesting parameter initial value
```
Required df columns: `market_ids`, `firm_ids`, `product_ids`, `prices`, `shares`, `nesting_ids`, `demand_instruments{k}`

### Post-Estimation
```python
elasticities = results.compute_elasticities()       # J×J per market
diversions = results.compute_diversion_ratios()      # J×J per market
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
| `building_type_regex` | `"Apartment\|Apt\|Coop\|Condo\|Mixed"` | Regex filter on building type (case-insensitive) |
| `max_abs_change_log_taxable_value` | `1.25` | Max year-over-year tax value change |
| `max_annualized_change_log_rent` | `0.2` | Max annualized rent change |
| `min_obs_per_pid` | `1` | Min observations per product (set ≥2 for IV with PID FE) |
| `market_cols` | `("pm.zip", "year")` | Columns defining market IDs |
| `firm_col` | `"owner_1"` | Column for firm IDs |
| `product_col` | `"PID"` | Column for product IDs |
| `shares_col` | `"share_zip"` | Share variable (must sum < 1 per market) |
| `nest_threshold_col` | `"filing_rate_eb_pre_covid"` | Column for nest assignment |
| `nest_threshold` | `0.1` | Threshold: >= this → first nest label |
| `nest_labels` | `("high", "low")` | (above_threshold, below_threshold) |
| `x1_formula` | `"0 + prices + ..."` | PyBLP Formulation string |
| `absorb_formula` | `"C(GEOID)*C(year) + ..."` | Absorbed FEs |
| `excluded_instruments` | `()` | Columns for IV (empty = no IV) |
| `include_nest_size_instrument` | `True` | Add nest sizes as instrument |
| `rho_init` | `0.2` | Initial nesting parameter |
| `output_subdir` | `"pyblp_summaries"` | Subdir in output/ for LaTeX tables |

---

## Pre-built Configurations

```python
from pyblp_estimation import EstimationConfig, iv_config, iv_config_pid_year, iv_config_pid, main

# Baseline (recommended) — no IV, GEOID*year + building type FE
out = main()

# IV with PID + GEOID*year (WEAK instruments, F≈0.03)
out = main(est_cfg=iv_config())

# IV with PID + year (borderline, F≈6)
out = main(est_cfg=iv_config_pid_year())

# IV with PID only (strongest first stage, F≈12, but no time controls)
out = main(est_cfg=iv_config_pid())
```

CLI: `python python/pyblp_estimation.py --iv [full|pid_year|pid]`

---

## Diagnostic Guidance

### IV / Weak Instruments
Tax assessment instruments (`change_log_taxable_value`, `z_sum_otherfirm_*`) are **weak under PID + GEOID×year FE** (F=0.03). The instruments capture cross-sectional and geographic-time variation, which is exactly what the FEs absorb. Within-building, within-market-year correlation between rent and tax changes is ≈0.002.

**IV is only feasible with coarser FE**: PID only (F≈12) or PID + year (F≈6). The baseline no-IV spec is currently the best option.

### Convergence Issues
- Try different `rho_init` (0.1, 0.3, 0.5)
- Check `results.converged`
- Singular weighting matrix → collinearity in instruments or too many FE
- `log_total_area` is time-invariant → collinear with PID FE (drop from IV specs)

### Elasticity Sanity Checks
- Own-price elasticities should be **negative** (typical: -0.3 to -3.0)
- If positive: wrong-sign price coefficient (endogeneity or share issue)
- If near zero: insufficient within-market price variation
- Baseline results: mean ≈ -1.96

### Nesting Parameter (rho)
- `rho = 0` → standard logit; `rho → 1` → perfect substitutes within nest
- Typical: 0.1–0.7; baseline estimate: 0.156
- If hits boundary (0 or ~0.99): nesting not identified (check shares!)

### Share Issues
- Shares MUST sum < 1 per market (outside good required)
- `share_zip` uses total housing stock denominator — safe
- If shares sum to 1.0: using wrong share variable or wrong market definition

---

## Typical Workflow

```python
from pyblp_estimation import EstimationConfig, main

# 1) Run baseline
out = main()

# 2) Inspect
out["results"].beta          # coefficients
out["post_est"]["own_elasticities"].mean()  # mean own elasticity

# 3) Iterate
cfg2 = EstimationConfig(year_range=(2016, 2019), min_units=10)
out2 = main(est_cfg=cfg2)

# 4) First-stage diagnostics (for IV)
from pyblp_estimation import report_instrument_checks
report_instrument_checks(out["est_df"], "prices",
    ["demand_instruments0", "demand_instruments1"],
    fe_cols=["PID", "year"])
```

---

When answering, always:
1. Read `python/pyblp_estimation.py` to understand current code state
2. Reference specific `EstimationConfig` fields when suggesting parameter changes
3. Show concrete code snippets the user can run
4. Flag any sample restriction changes that might affect identification
