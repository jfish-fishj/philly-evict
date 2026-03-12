# Outer Sorting Final-Projection Approximation

## Summary

`r/simulate-outer-sorting.R` currently does **not** fully re-solve the maintenance / complaint expected-rate subsystem after the final exact citywide-mass projection of `s`.

That is intentional for now and should be treated as a documented computational approximation, not a silent equilibrium claim.

## Current behavior

The simulator:

1. iterates on `s` and `cbar` until the fixed-point update is within tolerance,
2. computes final expected rates at that converged iterate,
3. projects `s` one more time using `xi_final` so the citywide mass constraint holds exactly,
4. recomputes `mbar`, `cbar`, and `fbar` once in forward order at the projected `s`,
5. draws counts from those refreshed rates.

## Why this is only approximate

After the final projection, `mbar` and `cbar` are not fully re-solved as a conditional fixed point:

- `mbar` depends on `cbar`
- `cbar` depends on `mbar` and `s`

So the reported rates after the final projection are a coherent one-pass refresh, but not necessarily the exact fixed point associated with the final `s`.

## Why we are keeping it for now

The main benefit is computational:

- calibration already calls the simulator hundreds of times
- a full post-projection re-solve would add another inner fixed-point problem inside each evaluation
- under current tolerances, the final projection is usually small, so the numerical mismatch should also be small

## When this would matter more

This approximation matters more if:

- `tol` / `mass_tol_abs` are loosened,
- the final `s` projection is nontrivial,
- feedback parameters in the complaint / maintenance block become larger,
- calibration is sensitive to very small moment changes.

## Recommended future check

Add an optional verification mode that:

1. re-solves the maintenance / complaint subsystem after the final `s` projection,
2. compares the refreshed rates and moments to the current shortcut,
3. logs the differences.

If the differences are negligible, the shortcut is harmless. If not, tighten the implementation.
