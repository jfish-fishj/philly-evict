# Complaint-Contingent Filing Hazard Score (Module A)

## Overview

`r/retaliatory-evictions.r` — Module A — constructs a **PID-level behavioral
marker** for complaint-contingent eviction filing escalation. The core question
is: does a building's eviction filing probability spike specifically in quarters
when it also receives a severe code complaint, relative to its own baseline
in complaint-free quarters?

**This is not a legal proof of retaliation.** It is a complaint-contingent
legal escalation marker — a signal that a landlord tends to file evictions
when under regulatory pressure from tenants, not just at a general background
rate.

The module runs only when `time_unit = "quarter"` (the default). Monthly
panels are not supported for this module.

### Script location

```
r/retaliatory-evictions.r   (Module A, ~lines 721–1083)
```

### Primary output

```
output/retaliatory-evictions/retaliatory_quarterly_pid_excess_hazard_score.csv
```

---

## Conceptual design

### The identification strategy

Standard eviction analysis compares high-filing landlords to low-filing
landlords. This conflates two things:

1. Landlords who have genuinely difficult tenant situations (high non-payment,
   high turnover) and therefore file often regardless of complaint activity.
2. Landlords who file specifically *because* a tenant filed a complaint — i.e.,
   who use the eviction system as a disciplinary tool.

Module A uses **within-PID variation over time** to separate these. For each
building, it compares the filing rate in quarters when a severe complaint
arrives to the filing rate in quarters when no severe complaint is nearby.
A building that files at a high baseline rate scores low; a building that
files disproportionately *in* complaint quarters scores high.

### Why "hazard" but not a true hazard model?

"Hazard" refers loosely to the per-quarter probability of an eviction filing
given complaint exposure. This is a linear probability model (LPM), not a
continuous-time survival or Cox model. The quarterly binning is the estimation
unit; no sub-quarterly timing information is used.

---

## Step A1 — Window status variables

For each PID-quarter, a mutually exclusive state is assigned:

| State | Condition |
|---|---|
| `same_q_severe` | `filed_severe == 1` in that quarter |
| `near_q_severe_not_same` | A severe complaint in the adjacent quarter (lag 1 or lead 1), but not the same quarter |
| `no_severe_nearby` | No severe complaint within ±`BW_Q` quarters (the **baseline**) |

The bandwidth `BW_Q = 1` (one quarter on each side) is fixed and parameterized
near the top of the module. Only bandwidth = 1 is currently tested.

**Severe complaints** are the priority signal: heat, fire, and property
maintenance complaints. A parallel version using all complaint types
(`any_window_status`) is also constructed but is secondary.

### Constants

| Constant | Default | Role |
|---|---|---|
| `BW_Q` | `1` | Quarter bandwidth for window construction |
| `MIN_SAME_Q_SEVERE` | `2` | Min same-q severe quarters for `score_ok_same_q` flag |
| `MIN_BASE_Q` | `8` | Min baseline quarters for `score_ok_same_q` flag |
| `MIN_NEAR_Q_SEVERE` | `2` | Min adjacent severe quarters for `score_ok_near_q` flag |
| `SHRINK_N0_SAME` | `5` | EB shrinkage reliability constant |
| `MIN_BIN_N_FOR_SHRINK` | `20` | Min PIDs per bin to use bin mean; else falls back to global |

---

## Step A2 — LPM fixed-effects model (aggregate estimate)

### Specification

```
filed_eviction_{it} = α_i + α_t
  + β_same  · 1[same_q_severe_{it}]
  + β_near  · 1[near_q_severe_not_same_{it}]
  + ε_{it}
```

- **Outcome**: `filed_eviction` (binary: any filing in PID-quarter)
- **Reference**: `no_severe_nearby` quarters
- **FE**: PID (`α_i`) + period (`α_t`, i.e., `period_fe`)
- **Cluster**: by PID
- **Sample**: pre-COVID (`year <= pre_covid_end`, default 2019)
- **Estimator**: `feols()` (LPM); Poisson robustness via `fepois()`

This is analogous to the distributed-lag model run elsewhere in the script,
but collapses the lead/lag sequence into a single categorical window indicator.
`β_same` estimates the average across-building complaint-quarter filing
elevation relative to each building's own non-complaint baseline.

### Outputs

| File | Contents |
|---|---|
| `retaliatory_quarterly_hazard_window_lpm_severe_coefficients.csv` | LPM coefficients (β_same, β_near) with SE, p-value, N |
| `retaliatory_quarterly_hazard_window_pois_severe_coefficients.csv` | Poisson robustness (log-linear, same spec) |
| `retaliatory_quarterly_hazard_window_lpm_any_coefficients.csv` | LPM for all-complaint window (parallel) |

### Typical results (Philadelphia, 2011–2019, quarterly)

From the 2026-03-02 run:

| Term | Estimate | Interpretation |
|---|---|---|
| `same_q_severe` | ~+0.04–0.06 | Filing probability is ~4–6 pp higher in same-quarter severe complaint periods, within-building |
| `near_q_severe_not_same` | ~+0.02–0.03 | Smaller elevation in adjacent quarters |

Note that 144,945 PIDs are absorbed by within-PID FE (singletons or all-zero
outcome), leaving N ≈ 1.76M observations for Poisson.

---

## Step A3 — PID-level excess score construction

### Filing rates by window status

For each PID, on the pre-COVID sample:

```
filing_rate_same_q_severe  = filings_same_severe / n_same_q_severe
filing_rate_near_q_severe  = filings_near_severe / n_near_q_severe
filing_rate_base           = filings_base        / n_base_q
```

All rates are `NA` (not `NaN` or `Inf`) when the denominator is zero.

### Raw excess scores

Three scores are computed:

| Score | Formula | Interpretation |
|---|---|---|
| `excess_same_q_vs_base` | `filing_rate_same_q_severe − filing_rate_base` | **Primary.** Filing elevation in same-quarter vs. own baseline |
| `excess_near_q_vs_base` | `filing_rate_near_q_severe − filing_rate_base` | Adjacent-quarter elevation vs. baseline |
| `excess_same_q_vs_near` | `filing_rate_same_q_severe − filing_rate_near_q_severe` | Concentration of spike in the same quarter vs. broader conflict window |

### Score usability flags

Small sample sizes produce unreliable rates. Two flags identify PIDs with
enough observations to be interpretable:

```r
score_ok_same_q  :=  n_same_q_severe >= 2  &  n_base_q >= 8
score_ok_near_q  :=  n_same_q_severe >= 2  &  n_near_q_severe >= 2
```

In the 2026-03-02 run: **22,537 of 193,930 PIDs** pass `score_ok_same_q`
(11.6%). The remaining ~171K PIDs either had no severe complaint quarters at
all (score = `NA`) or had too few quarters to be reliable.

### Score distribution (score_ok PIDs, Philadelphia 2011–2019)

| Percentile | `excess_same_q_vs_base` |
|---|---|
| p10 | −0.053 |
| p25 | 0.000 |
| p50 | 0.000 |
| p75 | +0.023 |
| p90 | +0.463 |
| p95 | +0.500 |
| p99 | +0.872 |

The distribution is **sharply zero-inflated**: the median score is exactly 0
(filing rate in complaint quarters equals baseline), and the right tail is
long. Meaningful complaint-contingent elevation is concentrated in roughly the
top 10% of score_ok PIDs.

---

## Step A4 — Bin-specific EB shrinkage

### The problem with raw scores

Raw rates for small buildings are extremely noisy. A 1-unit building that
happened to file in all 3 of its severe complaint quarters has
`filing_rate_same_q_severe = 1.0`, but this is based on 3 Bernoulli trials.
Without shrinkage, these buildings dominate any ranking.

### James-Stein style shrinkage

The EB score shrinks each building's raw excess toward a **group mean** with
a reliability weight that increases with observed sample size:

```
w_same  =  n_same_q_severe / (n_same_q_severe + N0)

excess_same_q_vs_base_eb  =  w_same × excess_same_q_vs_base
                           + (1 − w_same) × μ_bin
```

where `N0 = SHRINK_N0_SAME = 5` and `μ_bin` is the mean excess for all
finite-score PIDs in the same `num_units_bin`.

**Weight examples** (N0 = 5):

| n_same_q_severe | w (weight on raw score) | Shrinkage toward group mean |
|---|---|---|
| 2 | 0.29 | 71% |
| 3 | 0.38 | 62% |
| 5 | 0.50 | 50% |
| 8 | 0.62 | 38% |
| 13 | 0.72 | 28% |
| 20 | 0.80 | 20% |

### Why bin-specific rather than global?

The mean excess differs substantially across building size bins (2026-03-02
run):

| `num_units_bin` | n (finite score PIDs) | Mean excess |
|---|---|---|
| 1 unit | 39,508 | 0.083 |
| 2–5 units | 14,979 | 0.085 |
| 6–20 units | 1,393 | 0.027 |
| 21–50 units | 637 | 0.032 |
| 51+ units | 420 | 0.045 |

Small buildings have much higher mean excess than large buildings, partly
because their per-quarter filing rates are noisier (zero-one variance is
maximized at 50% probability). Shrinking a 24-unit building toward the global
mean (0.082) overstates its expected score relative to its true peer group;
shrinking it toward the 21–50 bin mean (0.032) is a much better prior.

If a bin has fewer than `MIN_BIN_N_FOR_SHRINK = 20` finite-score PIDs, it
falls back to the global mean.

### Effect of bin shrinkage on rankings

Bin-specific shrinkage re-orders the top of the ranking because large buildings
that were being shrunk toward an inappropriately high global mean get pulled
down, while buildings that genuinely spike in complaint quarters relative to
their own peer group move up. The correlation of the EB score with raw filing
rate dropped from **r = 0.131** (global shrinkage) to **r = 0.099** (bin
shrinkage), confirming that the bin-specific version captures more complaint-
contingent signal and less general filing intensity.

---

## Output file schema

`retaliatory_quarterly_pid_excess_hazard_score.csv` — one row per PID.

### Counts and filing rates

| Column | Description |
|---|---|
| `PID` | 9-digit padded parcel ID |
| `n_total_q` | Total PID-quarter observations in pre-COVID panel |
| `n_same_q_severe` | Quarters with same-quarter severe complaint |
| `n_near_q_severe` | Quarters with adjacent-quarter (not same) severe complaint |
| `n_base_q` | Baseline quarters (no nearby severe complaint) |
| `filings_same_severe` | Eviction filings in same-q severe quarters |
| `filings_near_severe` | Eviction filings in near-q severe quarters |
| `filings_base` | Eviction filings in baseline quarters |
| `filing_rate_same_q_severe` | `filings_same_severe / n_same_q_severe` (NA if denom=0) |
| `filing_rate_near_q_severe` | `filings_near_severe / n_near_q_severe` (NA if denom=0) |
| `filing_rate_base` | `filings_base / n_base_q` (NA if denom=0) |
| `filing_rate_raw` | Unconditional filing rate across all quarters (correlation diagnostic) |

### Scores

| Column | Description |
|---|---|
| `excess_same_q_vs_base` | Raw score: same-q rate minus baseline rate |
| `excess_near_q_vs_base` | Near-q rate minus baseline rate |
| `excess_same_q_vs_near` | Same-q rate minus near-q rate (spike concentration) |
| `excess_same_q_vs_base_eb` | **Primary score.** EB-shrunk toward bin mean |
| `w_same_shrink` | Shrinkage reliability weight `n/(n+N0)` |
| `mu_bin` | Bin-specific shrinkage target used |
| `n_bin` | Number of finite-score PIDs in the bin (for `mu_bin`) |

### Usability flags

| Column | Description |
|---|---|
| `score_ok_same_q` | `n_same_q_severe >= 2 & n_base_q >= 8` |
| `score_ok_near_q` | `n_same_q_severe >= 2 & n_near_q_severe >= 2` |

### Any-complaint parallel

| Column | Description |
|---|---|
| `n_same_q_any` | Same-quarter periods with any complaint |
| `filing_rate_same_q_any` | Filing rate in same-q any-complaint periods |
| `excess_same_q_any_vs_base` | Raw excess for any-complaint window |

### Building metadata (joined at export)

| Column | Source |
|---|---|
| `owner_1` | `bldg_panel_blp.owner_1` |
| `building_code_description_new` | `bldg_panel_blp.building_code_description_new` |
| `year_built` | `bldg_panel_blp.year_built` |
| `total_units` | `bldg_panel_blp.total_units` |
| `num_units_bin` | `bldg_panel_blp.num_units_bin` |
| `n_sn_ss_c` | First matched address from `evict_address_xwalk` (unique-parcel matches only) |

---

## Interpretation guidance

### What the score captures

A high `excess_same_q_vs_base_eb` means a building files evictions
disproportionately in quarters when it is also receiving severe code complaints,
relative to its own baseline filing behavior. This is consistent with
complaint-triggered escalation — a landlord who uses the eviction system as
a response to tenant complaints rather than (or in addition to) genuine non-
payment.

The score is agnostic about *why* this co-occurrence happens. Some possible
mechanisms:

1. **Direct retaliation**: Landlord files against the complaining tenant.
2. **Portfolio-level response**: A complaint triggers a sweep of non-payment
   cases building-wide.
3. **Reverse causation**: Tenant files complaint *because* they received an
   eviction notice (common in Philadelphia where complaints sometimes precede
   court appearances).
4. **Confounding**: Periods of building distress generate both complaints and
   filings for independent reasons.

The distributed-lag model (`filed_severe → filed_eviction` with leads and
lags) provides complementary evidence on timing; leads (future complaints
predicting current filings) would undercut the reverse-causation story.

### What the score does NOT capture

- **Legal retaliation** in the statutory sense. Proving retaliation requires
  identifying the specific tenant who complained and the case filed against
  them, which is a case-level linkage problem (see the `one_month_back_rent`
  module later in the script).
- **Intent**. The score is behavioral, not causal.
- **Buildings with no complaint history**. PIDs with zero severe complaint
  quarters have `excess_same_q_vs_base = NA`. They are not scored at all.

### Recommended usage

For ranking or flagging purposes, restrict to `score_ok_same_q == TRUE` and
sort by `excess_same_q_vs_base_eb`. The primary score is robust to small-
sample noise from the EB shrinkage; the raw score `excess_same_q_vs_base`
is useful for transparency but should not be used for ranking on its own.

The correlation of `excess_same_q_vs_base_eb` with raw filing rate is
**r ≈ 0.10** — low enough that the score is not simply a noisy version of
overall filing intensity. High-excess buildings are genuinely distinctive in
their complaint-quarter behavior.

---

## Known limitations and future work

### 1. Zero-inflation

Three-quarters of `score_ok` PIDs have `excess_same_q_vs_base = 0`, meaning
they file at the same rate in complaint quarters as in baseline quarters. This
is probably real (most landlords do not escalate), but it means distributional
summaries like means are hard to interpret. Quantile-based summaries (p90+) or
binary flags (`excess > threshold`) are more useful.

### 2. N0 calibration

`SHRINK_N0_SAME = 5` is a pragmatic first-pass choice. A proper empirical
Bayes estimator would estimate N0 from the data (via marginal likelihood
maximization or method of moments). With N0=5, a building needs ~10 complaint
quarters to have 2/3 of its weight on the raw score. This is conservative for
large buildings (which may accumulate 10+ complaint quarters over 9 years) but
aggressive shrinkage for the median building (which has 2–4 complaint quarters).

### 3. Bandwidth fixed at ±1 quarter

Only `BW_Q = 1` is tested. A ±2 quarter window would capture slower landlord
responses but would also reduce the specificity of the "same-quarter" category
(fewer baseline quarters per PID). This is a design choice worth revisiting if
results are sensitive.

### 4. No cross-building normalization within owner

The score is PID-level. An owner with 10 buildings, each with moderate excess
scores, is treated the same as 10 unrelated single-building landlords. Owner-
level aggregation (e.g., portfolio-weighted mean excess) is a natural next step
for identifying repeat-player landlords.
