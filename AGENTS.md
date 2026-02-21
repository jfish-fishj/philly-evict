# AGENTS.md — Project Guardrails (Philly Evictions)

This file defines the rules of engagement for **any agent** (Codex CLI, ChatGPT, etc.) editing this repo.
If a generic style guide conflicts with this file, **this file wins**.

## Read-first map
Before making changes, skim in this order:
1) `README.md` (project overview + how to run)
2) `docs/` (design notes / decisions / data contracts)
3) This file (`AGENTS.md`) for guardrails and workflow

## Primary objective
Make the pipeline **portable, reproducible, and data-product oriented**:
- no hardcoded paths
- stable intermediate outputs (“products”)
- modular functions where appropriate
- minimal scientific / definitional churn unless explicitly requested

## Non-negotiable rules
1) **Correctness > style churn.**
2) **Do not silently change definitions, sample restrictions, or merge logic.**
   - If you change joins/filters/variable construction: document it and add checks.
3) **No hardcoded paths.**
   - All reads/writes must come from `cfg` (from `config.yml`) or environment variables.
   - Never introduce `/Users/...`, `~/Desktop/...`, etc.
4) **Keep paradigms consistent inside a file.**
   - If a script is `data.table`-heavy, keep it `data.table`.
   - If tidyverse, keep tidyverse.
   - Avoid mixing unless there’s a strong reason.
5) **Analysis scripts must not write to processed products.**
   - analysis reads from `processed/` and writes to `output/` only.

## Required safety checks for any meaningful refactor
If you change anything in cleaning, make, or merge steps:

### A) Key assertions (joins/merges)
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
- a schema version (or at least a note in README/docs)
- consistent date/time units (year vs year_quarter, etc.)

### D) Logging
Every pipeline step should:
- log start/end + input paths + output path(s)
- log row counts and key uniqueness checks
- write logs to `output/logs/`

## Preferred refactor pattern
1) Move logic into `R/` functions:
   - `clean_evictions(cfg)`, `clean_parcels(cfg)`, `clean_licenses(cfg)`
   - `make_ever_rentals_panel(cfg)`, `make_occupancy_panel(cfg)`
   - `make_analytic_sample(cfg)`
2) Keep `r/*.R` as thin wrappers:
   - parse config
   - call one function
   - write outputs
3) If using a workflow orchestrator (e.g., `{targets}`), ensure:
   - targets depend on file products, not hidden globals

## Do not touch unless explicitly asked
Any substantive modeling choices in:
- retaliatory evictions regressions
- price regulation analyses
- definition of filing rates / high-filer thresholds
…should remain unchanged unless the user explicitly requests a scientific change.

## When uncertain
- Prefer adding a *new* artifact over overwriting.
- Prefer a small, reversible refactor (one step at a time).
- If you suspect a join explosion or key mismatch, stop and add assertions first.

## What to include in PR / change summary
For non-trivial changes, include:
- what changed (1–5 bullets)
- why it changed
- how you verified (commands run + key checks)
- any schema/contract changes to products
