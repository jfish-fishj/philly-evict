# Issue: InfoUSA race imputation — name_lookup_failed overlap with BLP sample

**Status**: Known issue, deferred. Not worth fixing given small overall impact.

## Background

`impute-race-infousa.R` produces 446,320 `name_lookup_failed` person-rows
(2.5% of 18.1M). The root cause is WRU warning:

> "The following locations in the voter.file are not available in the census data."

These are BG geoids (mostly `PA-101-...`) that exist in the InfoUSA/parcel
data but are absent from our custom BG priors file
(`bg_renter_race_priors_2013.csv`, 1,336 BGs). The absent BGs are likely:
- BGs slightly outside Philadelphia county proper
- BGs suppressed in 2013 ACS 5-year due to very low population

## BLP-sample overlap (checked 2026-02-28)

| Metric | Value |
|--------|-------|
| BLP unique PIDs | 193,918 |
| BLP PIDs with any name_lookup_failed | 7,070 (3.6%) |
| Person-rows failed at BLP PIDs | 25,452 / 6,849K (**0.37%**) |
| Affected PID-years | 22,055 |
| Median failed share per affected PID-year | 40% |
| PID-years with 100% failure (fully dark) | 5,463 |

## Why deferred

The 0.37% overall person-row miss rate is negligible for building-level race
panel means. The 5,463 fully-dark BLP PID-years are a mild concern (those
buildings contribute nothing to the race panel), but fixing requires non-trivial
additional logic (name-only or tract-level fallback for unmatched BG codes) and
the spatial concentration of the affected BGs is likely limited.

## Potential fix (if revisited)

For `name_lookup_failed` rows that have a valid BG geoid but whose geoid is
absent from the priors file: fall back to a tract-level WRU prediction using
tract-aggregated priors, similar to the block→BG fallback already implemented
in `impute-race.R`.
