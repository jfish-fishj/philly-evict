# LOO Filing Type: `compute_loo_filing_type()`

## What it computes

`compute_loo_filing_type()` (defined in `r/helper-functions.R`) classifies each
building (PID) into one of four landlord-type groups based on the **leave-one-out
(LOO)** filing rate of its owning conglomerate:

| Group | Condition |
|---|---|
| Solo | No other buildings in the conglomerate |
| Small portfolio | Other buildings present, but fewer than `loo_min_unit_years` LOO unit-years |
| Low-evicting portfolio | LOO unit-years ≥ threshold AND LOO rate < `loo_filing_threshold` |
| High-evicting portfolio | LOO unit-years ≥ threshold AND LOO rate ≥ `loo_filing_threshold` |

The LOO rate for a focal PID is:

```
loo_rate = (conglomerate_total_filings - pid_filings) /
           (conglomerate_total_units   - pid_units)
```

computed over the rows of `pid_yr_dt` supplied by the caller.

## Function signature

```r
compute_loo_filing_type(
  pid_yr_dt,                    # data.table: PID, year, conglomerate_id,
                                #   num_filings, total_units
  loo_min_unit_years = 90L,     # minimum LOO unit-years for Low/High split
  loo_filing_threshold = 0.05   # filings per unit-year threshold
)
```

Returns a data.table with one row per **(PID, conglomerate_id)** pair:
`PID, conglomerate_id, loo_units_cong, loo_rate_cong, portfolio_evict_group_cong`.

## Time-window choices

The function is **time-window agnostic** — the caller filters `pid_yr_dt` before
passing it. Two design choices apply across the codebase:

### Full-period (structural classification)

**Used by**: `r/retaliatory-evictions.r` (spell-level maintenance regressions)

Pass the full 2011–2019 panel. Landlord type is a fixed structural attribute; a
full-period average minimises measurement noise. The `loo_min_unit_years = 90`
default (10 units × 9 years) is calibrated for this window.

```r
loo_type_dt <- compute_loo_filing_type(
  pid_yr_dt            = blp_cong[, .(PID, year, conglomerate_id,
                                      num_filings, total_units)],
  loo_min_unit_years   = loo_min_units * 9L,
  loo_filing_threshold = loo_filing_threshold
)
spell_dt <- merge(spell_dt, loo_type_dt, by = c("PID", "conglomerate_id"), all.x = TRUE)
```

### Pre-acquisition (causal event-study designs)

**Used by**: `r/analyze-transfer-evictions-unified.R` and
`r/analyze-nhood-portfolio-transitions.R` — *not yet refactored to use this helper*.

These scripts need to classify the **acquirer** using only years before the
acquisition, so the outcome (post-transfer filing rate) doesn't contaminate the
treatment indicator. Each transfer has its own `transfer_year`, meaning the
pre-acquisition window varies per PID.

**Why the helper is not currently used here**: `compute_loo_filing_type()` aggregates
all rows in `pid_yr_dt` into a single set of conglomerate totals. With a per-PID
pre-acquisition filter (`year < transfer_year`), different conglomerate members
would contribute different year ranges — making the LOO aggregation inconsistent
across members. The existing cross-join approach in those scripts correctly handles
per-transfer-year windows and is left unchanged.

A future refactor could extend the helper with a `focal_pid_filter` argument that
applies the time restriction only to the focal PID, leaving other-member totals
computed over the full panel — but this is deferred.

## Unit-years threshold convention

`loo_min_unit_years` is always expressed in **unit-years** (units × years), not
units alone. The default of 90 = `loo_min_units * 9` reflects 10 units observed
over the full 9-year window (2011–2019). Scripts that pass a shorter window should
either accept that more spells will be classified as "Small portfolio," or reduce
the threshold proportionally.

## Config parameters

Both `loo_filing_threshold` and `loo_min_units` are config-driven via
`cfg$run$retaliatory_loo_filing_threshold` (default 0.05) and
`cfg$run$retaliatory_loo_min_portfolio_units` (default 10). The same defaults apply
in the transfer-evictions scripts via `ACQ_HIGH_FILER_THRESHOLD` and
`ACQ_SMALL_PORTFOLIO_MIN_OTHER_UNITS`.
