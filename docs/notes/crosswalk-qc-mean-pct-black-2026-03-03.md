# Crosswalk QC: Mean % Black by `dropped_no_infousa` (2006-2019)

This note is the focused writeup for the requested metric: mean tract `% Black` by `dropped_no_infousa` status among eviction cases with a unique parcel match.

## Source files

- `output/qa/crosswalk_qc_mean_pct_black_by_dropped_no_infousa_2006_2019.csv`
- `output/qa/crosswalk_qc_mean_pct_black_by_dropped_no_infousa_year_2006_2019.csv`

## Pooled 2006-2019

| dropped_no_infousa | N cases | Mean % Black | Median % Black |
|---|---:|---:|---:|
| dropped (TRUE) | 19,580 | 59.45 | 75.20 |
| matched (FALSE) | 250,407 | 55.55 | 58.19 |

Difference in mean `% Black` (dropped - matched): **+3.90 percentage points**.

## By year (difference in mean % Black)

- Range of annual differences (dropped - matched): **+0.94 pp to +7.12 pp**
- Average annual difference: **+3.87 pp**

Largest annual gaps are in:

- 2010: +7.12 pp
- 2015: +6.92 pp
- 2016: +5.70 pp

## Run command

```bash
Rscript r/cross-walk-qc.R
```

## Interpretation

Within the unique-parcel eviction sample, cases dropped due to no InfoUSA PID are, on average, located in higher-Black tracts than cases retained in InfoUSA.
