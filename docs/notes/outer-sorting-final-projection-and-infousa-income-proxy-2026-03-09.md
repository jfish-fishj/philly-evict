# Outer Sorting Final Projection + InfoUSA Income Proxy

## What changed

- Removed the tract fallback from `r/build-outer-sorting-empirical-moments.R`; the empirical outer-sorting target is now **block-group only** for the composition proxy.
- Added `r/qa-infousa-income-fields.R` to assess the modeled InfoUSA income / wealth fields against documented ranges and geography-level poverty proxies.
- Documented the final-post-projection approximation in the outer-sorting simulator as an intentional computational shortcut to keep for now.

## Why

- Mixing block-group and tract composition measures inside the empirical target weakens interpretation of the within-neighborhood sorting moments.
- We need a quick empirical read on whether InfoUSA modeled income fields are usable as building-level low-income proxies.
- The simulator’s final exact mass projection changes `s` after the main fixed-point loop; this should be documented because the rates are refreshed once, not fully re-solved.

## What was run

- `Rscript r/make-outer-sorting-empirical-moments.R`
- `Rscript r/qa-infousa-income-fields.R`

## Verified

- BG-only empirical target rebuild succeeds.
- Current BG-only drop count:
  - `700` building-year rows
  - `8` block groups
- InfoUSA QA outputs written:
  - `output/qa/infousa_income_field_summary.csv`
  - `output/qa/infousa_income_field_correlations.csv`
  - `output/qa/infousa_income_field_codebook_excerpt.txt`

## InfoUSA income-field takeaway

For future building-level composition work, `find_div_1000` looks like the cleanest first-pass proxy:

- 0% missing in the InfoUSA file
- documented bounded range with no out-of-range values in the QA
- meaningful negative correlation with BG / tract poverty proxies

`ppi_div_1000` is also usable. `wealth_finder_score` is weaker but still informative. `estmtd_home_val_div_1000` is much messier because a large share of zeros fall outside the documented support.

## Open follow-up

- Build a building-level InfoUSA income proxy using `find_div_1000` or `ppi_div_1000`, likely restricted to likely renters / primary households.
- If needed, add a verification mode to quantify how much the final-post-projection shortcut moves simulated moments.
