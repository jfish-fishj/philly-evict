# Crosswalk QC Iteration Notes (2026-03-03)

This note documents the new crosswalk quality-control checks added for eviction coverage and dropped-case composition.

## What changed

- Added a new script: `r/cross-walk-qc.R`.
- The script builds a case-level funnel for filing years 2006-2019:
  - total evictions
  - unique-parcel-matched evictions (`num_parcels_matched == 1`)
  - unique-parcel cases dropped because the PID is not in InfoUSA
  - unique-parcel cases with both InfoUSA and non-missing `filing_rate_eb_pre_covid`
- Added dropped-vs-matched tract composition checks among unique-parcel cases:
  - tract `% Black` (from BG priors aggregated to tract)
  - share in high-Black tracts (`tract_p_black >= 0.50`)
  - share above the case-level median tract `% Black`
- Added a simple LPM diagnostic:
  - `dropped_no_infousa ~ tract_p_black + year FE`
- Added join/key safety assertions and row-count invariants around all major merges.

## Why

To quantify whether eviction coverage gaps in the InfoUSA crosswalk are systematically associated with geography and tract racial composition, not just overall match rates.

## Files written by the script

- `output/qa/crosswalk_qc_funnel_2006_2019.csv`
- `output/qa/crosswalk_qc_funnel_pooled_2006_2019.csv`
- `output/qa/crosswalk_qc_dropped_characteristics_2006_2019.csv`
- `output/qa/crosswalk_qc_dropped_by_year_2006_2019.csv`
- `output/qa/crosswalk_qc_dropped_vs_matched_lpm_2006_2019.csv`
- `output/qa/crosswalk_qc_summary_2006_2019.txt`
- `output/logs/cross-walk-qc.log`

## Key pooled results (2006-2019)

From `output/qa/crosswalk_qc_funnel_pooled_2006_2019.csv`:

- Total evictions: `313,380`
- Unique parcel-matched evictions: `269,987` (`86.15%`)
- Dropped due to no InfoUSA PID (among unique parcel-matched): `19,580` (`7.25%`)
- Kept with both InfoUSA + eviction rate (among unique parcel-matched): `238,835` (`88.46%`)

From `output/qa/crosswalk_qc_dropped_characteristics_2006_2019.csv` (unique parcel-matched cases only):

- Mean tract `% Black`:
  - dropped (no InfoUSA): `0.594`
  - matched to InfoUSA: `0.556`
- Share with tract `% Black >= 0.50`:
  - dropped: `0.541`
  - matched: `0.562`
- Share above case-level median tract `% Black`:
  - dropped: `0.525`
  - matched: `0.502`

From `output/qa/crosswalk_qc_dropped_vs_matched_lpm_2006_2019.csv`:

- Coefficient on `tract_p_black`: `0.0223` (SE `0.0015`, p `< 0.001`)
- Interpretation: higher tract `% Black` is positively associated with being dropped from InfoUSA among unique parcel-matched cases, conditional on year FE.

## What was run

```bash
Rscript r/cross-walk-qc.R
```

## Notes / caveats

- `id == "Unknown"` rows are excluded from the xwalk side before the `id` join, because `Unknown` is not a stable key and creates non-unique collisions.
- Tract `% Black` is based on BG priors aggregated to tract using an unweighted BG mean.
