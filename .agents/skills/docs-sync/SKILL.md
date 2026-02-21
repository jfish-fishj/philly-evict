name: docs-sync
description: Keep docs and data-product contracts synchronized with code changes.

## When to use
Any time a change affects:
- products in `processed/` (schema, keys, meaning)
- config semantics
- a major workflow command

## Steps
1) Identify impacted products:
   - file path(s)
   - primary key(s)
   - columns added/removed/renamed
2) Update docs:
   - add a short note to the relevant `docs/` page (or create one)
   - update README if run commands change
3) Add a contract check (preferred):
   - schema assertion (columns/types)
   - key uniqueness
   - “small invariants” (e.g., totals, ranges)

## Suggested doc snippet format
- **Product:** processed/...
- **Keys:** ...
- **Definition notes:** ...
- **Version/date:** ...

