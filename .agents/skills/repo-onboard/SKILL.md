name: repo-onboard
description: Fast, consistent repo understanding before editing: map files, entrypoints, and run commands.

## When to use
Use this at the start of a session or when you’re unsure where a behavior lives.

## Steps
1) Read: `AGENTS.md`, `README.md`, and relevant `docs/` pages.
2) Identify:
   - main entrypoints (scripts, CLIs, Makefile/targets)
   - core data products in `processed/` and their keys
   - where configs live (`config.yml`, env vars)
3) Output a short “repo map”:
   - directories + what they contain
   - top 5 workflows / commands
   - “danger zones” (joins, definitions, products)

## Deliverable format
- 10–20 bullet repo map
- 3–5 suggested next actions (if the user asked vaguely)

