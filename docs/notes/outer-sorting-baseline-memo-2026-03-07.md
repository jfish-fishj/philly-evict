# Outer Sorting Baseline Memo (2026-03-07)

## What changed

- Added a new Quarto memo at `writeups/outer-sorting-baseline.qmd`.
- The memo documents:
  - the outer sorting equilibrium setup
  - the baseline parameter values currently in `config.yml`
  - convergence diagnostics from the current simulation run
  - the resulting equilibrium moments, type-level summaries, and plots
- The document writes supporting CSV and LaTeX tables to `output/outer_sorting/tables/` during render.

## Why

- The simulation and first-pass calibration were already wired, but there was no rendered memo summarizing the current baseline in one place.
- This writeup gives a stable artifact for checking assumptions, baseline values, convergence behavior, and the shape of the simulated equilibrium before moving on to tighter calibration work.

## Verification

- Rendered with:
  - `quarto render writeups/outer-sorting-baseline.qmd`
- Render completed successfully and produced:
  - `writeups/outer-sorting-baseline.pdf`
- The QMD was patched to:
  - resolve project paths correctly when rendered from `writeups/`
  - make the sampled sorting figure deterministic with a fixed seed
  - remove duplicated raw-LaTeX table labels from generated table files

## Notes

- The memo uses the current simulation outputs and calibration outputs already on disk.
- It reports the anchored tenant-side interpretation values (`mu`, `delta_L`, `delta_H`) and explicitly notes that the default anchors are not yet active structural inputs to the reduced-form fixed-point equations.

## Next steps

- If this becomes a recurring artifact, add a thin render wrapper under `r/` or a Make/targets entry so the memo is regenerated alongside simulation and calibration runs.
