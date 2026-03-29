# Transfer Evictions Filing Cap / Sample Reporting Update (2026-03-28)

## What changed

- Added a filing-rate cap of `0.75` filings per unit-year in the transfer-evictions annual pipeline.
- Applied the cap both to the annual filing-rate outcome and to the leave-one-out filing-rate inputs used for acquirer bin classification.
- Expanded the transfer-evictions writeup to report:
  - the weighted stacked estimating equation
  - the role of `q_weight` and `total_units`
  - explicit acquirer-bin rules
  - filing sample-size tables for the main filing grid and the `2+` acquirer split

## Why

- A small number of PID-years had implausibly large filing rates, especially in small buildings, and these tails were inflating noise in the high-filer bin.
- The weighted stacked filing results are now the main specification, so the PDF needs to define the estimator and sample construction clearly.

## Products / outputs affected

- **Product:** `output/transfer_evictions/annual_acq_cutoffs.csv`
- **Definition notes:** now includes `filing_rate_cap` in addition to the existing LOO unit-year and high-filer thresholds.
- **Version/date:** 2026-03-28

## Verification

- `Rscript -e "parse(file='r/helper-functions.R'); parse(file='r/analyze-transfer-evictions-unified.R')"`
- `Rscript r/analyze-transfer-evictions-unified.R --frequency annual`
- `quarto render writeups/transfer-evictions.qmd`

## Notes

- After capping the filing tail, the weighted stacked `2+` high-filer treated sample fell from the earlier exploratory count to `320` treated transfers, consistent with some extreme-rate acquirers moving out of the high-filer bin.
- The writeup now reports treated transfers, control PIDs, stacked rows, and trimmed cohorts directly from the regression outputs.
