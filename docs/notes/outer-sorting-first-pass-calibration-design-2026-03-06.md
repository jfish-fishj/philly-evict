# Outer Sorting First-Pass Calibration Design (2026-03-06)

## Design choices implemented

- Fixed `mu = 0.30` as the citywide high-risk / precarious tenant share.
- Added anchored tenant default probabilities:
  - `delta_L = 0.01`
  - `delta_H = 0.50`
- Kept `b_Fs_hard` fixed in the first pass.
- Expanded the first-pass calibration parameter set to:
  - `Dalph`
  - `p_bad_shape1`, `p_bad_shape2`
  - `aF`, `bF_theta`
  - `aC`, `bC_theta`, `bC_s`, `bC_M`
  - `aM`, `bM_theta`, `bM_C`

## Compact target set implemented

The default calibration objective now uses 12 moments:

1. `mean_f_rate`
2. `mean_m_rate`
3. `mean_c_rate`
4. `zero_share_f`
5. `zero_share_m`
6. `zero_share_c`
7. `sorting_coef_high_filing_nhood`
8. `sorting_coef_high_bin_nhood`
9. `beta_ppml_s`
10. `beta_ppml_s_x_high_filing`
11. `beta_maint_on_log1p_c_rate`
12. `filing_resid_sd_nhood_fe`

## Reduced-form filing slope note

The current `comp-statics-annual-race` writeup and exported tables do not contain a standalone filing-on-composition regression estimate. They report complaint regressions with filing controls. Because of that, `b_Fs_hard` remains a fixed config value (`0.55`) and is documented as a placeholder until a direct filing reduced form is exported.
