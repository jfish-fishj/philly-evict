---
name: repo-onboard
description: "Fast, consistent repo understanding before editing: map files, entrypoints, and run commands."
---

## When to use
Use this at the start of a session or when you’re unsure where a behavior lives.

## Steps
1) Read in repo order: `README.md`, relevant `docs/` pages, then `AGENTS.md`.
2) Identify:
   - main entrypoints (scripts, CLIs, Makefile/targets)
   - core data products in `processed/` and their keys
   - required input schema for each major script (which columns are mandatory and should hard-fail if missing)
   - where configs live (`config.yml`, env vars)
   - which scripts are pipeline steps vs analysis-only (`output/` writers)
3) Output a short “repo map”:
   - directories + what they contain
   - top 5 workflows / commands
   - “danger zones” (joins, definitions, products)
   - quick notes on verification commands (`rg`, tests, `tar_make`, key scripts)

## Deliverable format
- 10–20 bullet repo map
- 3–5 suggested next actions (if the user asked vaguely)
- include exact file paths and 1-line purpose for each key entrypoint
