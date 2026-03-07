# Outer Sorting Memo Empirical Neighborhood Comparisons (2026-03-07)

- Updated `writeups/outer-sorting-baseline.qmd` to add empirical-vs-simulated neighborhood comparisons.
- Added:
  - empirical filing-rate check table
  - empirical vs simulated neighborhood distribution summary table
  - histogram comparison of neighborhood mean filing rates
  - histogram comparison of neighborhood mean poverty shares
- The memo now states explicitly that:
  - `mean_f_rate`, `mean_m_rate`, and `mean_c_rate` are unit-weighted means
  - zero shares are unweighted building shares
- Verified the current empirical filing-rate target under the present per-unit-year definition:
  - unit-weighted mean filing rate: `0.039`
  - unweighted mean filing rate: `0.041`
- Rendered successfully with:
  - `quarto render writeups/outer-sorting-baseline.qmd`
