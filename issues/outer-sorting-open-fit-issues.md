# Outer Sorting: Open Fit Issues And Likely Causes

Date: 2026-03-09

## Current status

The outer-sorting simulator is now closer to the empirical object than the earlier versions:

- simulated outcomes are annualized over a multi-year exposure window rather than drawn as one-shot annual counts
- building units and year built are empirically resampled
- complaints load on composition only for bad landlords
- filings use a single-intensity Poisson block rather than the earlier extensive/intensive patch
- maintenance uses a direct baseline rate `m0`

These changes materially improved the maintenance fit. The remaining problems are no longer primarily about maintenance mean levels. They are now mostly about neighborhood composition dispersion, complaint/filing levels, and the geography structure of the model.

## Main empirical mismatches still showing up

Using the latest calibration outputs in `output/outer_sorting/`:

1. Neighborhood low-income-share dispersion is still too small.
   - `nhood_mean_s_p90_p10` is the largest miss.
   - The model does not generate enough between-neighborhood dispersion in mean composition.

2. The maintenance-on-complaints slope is still too weak.
   - `beta_maint_on_c_rate` remains materially below the empirical target.
   - This is improved relative to earlier runs, but it is still one of the dominant misses.

3. Filing and complaint levels are still too high in many calibrated fits.
   - `mean_f_rate` and `mean_c_rate` remain above the empirical targets.
   - Once filing and complaint zero shares were removed from the objective, the optimizer still tended to overshoot these means.

4. The model can fit some maintenance tail moments only by distorting other objects.
   - When `zero_share_m` was removed from the objective, the optimizer let `mean_m_rate` drift far above the empirical target again.
   - That indicates `zero_share_m` was acting as a useful regularizer even in the annualized setup.

## Likely causes

### 1. Geography confounding: `p_n` and `eta_n` are both doing neighborhood work

This is the most important structural issue now.

- `p_n` controls the neighborhood prevalence of bad landlords.
- `eta_n` controls neighborhood sorting pressure directly.

Both are neighborhood-level shifters, and both affect neighborhood filing patterns and neighborhood composition patterns. That means:

- `p_n` can generate filing geography and indirectly generate share geography
- `eta_n` can generate share geography directly and indirectly affect filing geography through equilibrium sorting

If both are left flexible, bad-landlord geography and tenant-share geography are mechanically entangled. This weakens identification and makes it hard to know whether the model is matching neighborhood composition using:

- landlord geography, or
- a pure sorting shifter

### 2. Complaint and maintenance remain simultaneously determined

The model still has:

- maintenance responding to complaints
- complaints responding to maintenance

That same-period feedback can attenuate the reduced-form maintenance-on-complaints moment even when the structural maintenance response is positive. The annualized multi-year simulator fixed the worst comparison problem, but simultaneity is still likely compressing the slope.

### 3. Complaint and filing intensities are still being asked to do too much

The model uses filing rates as the sorting signal. That means the filing block still carries two jobs:

- generate realistic filing outcomes
- generate the signal variation that drives equilibrium sorting

When the model tries to create more sorting-relevant variation, it can overshoot filing and complaint levels. This is less severe than before, but it is still present.

### 4. Building observables help, but do not solve omitted heterogeneity

Adding year-built controls was useful and moved coefficients in sensible directions. But building age alone is not enough to absorb all omitted building heterogeneity relevant for:

- complaint intensity
- maintenance intensity
- complaint suppression vs true disorder

The remaining candidate missing ingredient is persistent building-level heterogeneity.

## What we learned from recent changes

1. Switching the simulated panel to multi-year annualized counts/rates was necessary.
   - Before that, the model was being compared to the wrong empirical object.
   - Positive maintenance-rate moments looked artificially bad because the simulation was too discrete.

2. Removing filing and complaint zero shares from the objective was reasonable.
   - Once both empirical and simulated objects are multi-year annualized averages, those zeros are less economically central.

3. Removing maintenance zero share from the objective was not helpful.
   - Fit worsened, and maintenance means drifted back up.
   - So `zero_share_m` should probably stay unless the maintenance block is restructured again.

## Recommended next steps

1. Restore `zero_share_m` in the calibration objective.

2. Separate the jobs of neighborhood bad-type geography and neighborhood sorting geography.
   - `p_n` should be disciplined by filing-side neighborhood moments.
   - `eta_n` should be disciplined by neighborhood composition-dispersion moments.
   - They should not both float as generic geography shifters without separate discipline.

3. Decide whether the maintenance block should stay simultaneous or move to a partially predetermined complaint signal.
   - If the goal is to match a stronger maintenance-on-complaints moment, timing may need to change.

4. Consider adding persistent building heterogeneity only after the geography problem is clarified.
   - Otherwise it will become a third flexible residual term on top of `p_n` and `eta_n`.

## Files most relevant to this issue

- `r/simulate-outer-sorting.R`
- `r/calibrate-outer-sorting.R`
- `r/build-outer-sorting-empirical-moments.R`
- `r/outer-sorting-moments.R`
- `latex/sorting_model_outer_dgp.tex`
