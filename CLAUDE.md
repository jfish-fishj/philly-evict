
---

## CLAUDE.md (draft)

```markdown
# CLAUDE.md — Project Guardrails (Philly Evictions)

This file is the rules of engagement for editing this repo.
If a generic R style guide conflicts with this file, **this file wins**.

## 1) Primary objective
Make the pipeline **portable, reproducible, and data-product oriented**:
- no hardcoded paths
- stable intermediate outputs (“products”)
- modular functions where appropriate
- minimal scientific / definitional churn unless explicitly requested

## 2) Non-negotiable rules
1) **Correctness > style churn.**
2) **Do not silently change definitions, sample restrictions, or merge logic.**
   - If you change joins/filters/variable construction, document it and add checks.
3) **No hardcoded paths.**
   - All reads/writes must come from `cfg` (from `config.yml`) or environment variables.
   - Never introduce `/Users/...`, `~/Desktop/...`, etc.
4) **Keep paradigms consistent inside a file.**
   - If a script is `data.table`-heavy, keep it `data.table`.
   - If tidyverse, keep tidyverse.
   - Avoid mixing unless there’s a strong reason.
5) **Analysis scripts must not write to processed products.**
   - analysis reads from `processed/` and writes to `output/` only.

## 3) Required safety checks for any meaningful refactor
If you change anything in cleaning, make, or merge steps:

### A) Key assertions
Before and after each join:
- assert uniqueness of intended keys
- assert expected row counts (or expected upper bounds)
- explicitly handle many-to-many merges (aggregate first or create a crosswalk)

### B) Determinism
- No random sampling without a fixed seed (and document it).
- Any fuzzy matching must have deterministic tie-breaking.

### C) Output contracts (data products)
Every product written to `processed/` must have:
- documented primary key(s)
- stable column names
- a schema version (or at least a note in README)
- consistent date/time units (year vs year_quarter)

### D) Logging
Every pipeline step should:
- log start/end + input paths + output path(s)
- log row counts and key uniqueness checks
- write logs to `output/logs/`

## 4) Preferred refactor pattern
1) Move logic into `R/` functions:
   - `clean_evictions(cfg)`, `clean_parcels(cfg)`, `clean_licenses(cfg)`
   - `make_ever_rentals_panel(cfg)`, `make_occupancy_panel(cfg)`
   - `make_analytic_sample(cfg)`
2) Keep `r/*.R` as thin wrappers:
   - parse config
   - call one function
   - write outputs
3) Add `_targets.R` to orchestrate:
   - targets should depend on file products, not hidden globals

## 5) “Do not touch unless asked”
- Any substantive modeling choices in:
  - retaliatory evictions regressions
  - price regulation analyses
  - definition of filing rates / high-filer thresholds
…should remain unchanged unless the user explicitly requests a scientific change.

## 6) When uncertain
- Prefer adding a *new* artifact over overwriting.
- Prefer a small, reversible refactor (one step at a time).
- If you suspect a join explosion or key mismatch, stop and add assertions first.
