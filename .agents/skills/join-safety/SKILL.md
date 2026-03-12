---
name: join-safety
description: Prevent join explosions and silent definition changes by adding key + rowcount assertions around merges.
---

## When to use
Any time you change a join, filter, crosswalk, aggregation, or key definition.

## Checklist
- State intended keys (left + right).
- Assert uniqueness on the side(s) expected to be unique.
- If many-to-many is possible: aggregate first OR build an explicit crosswalk.
- Log before/after row counts and the share of unmatched keys.
- Add a regression test or a small invariant check (e.g., totals within tolerance).
- Treat required schema columns as mandatory: if missing, `stop(...)` with a clear error.

## Banned anti-pattern for required columns
- Do not use `intersect(required_cols, names(dt))` + `if (length(...)) { ... } else { fallback }`.
- Do not silently substitute broad fallback columns (e.g., `total_violations`) when specific required columns are missing.

## R helpers (preferred pattern)
Add (or reuse) tiny helpers in `R/`:
- `assert_unique(dt, keys, label)`
- `assert_row_change(n_before, n_after, upper_mult, label)`
- `summarize_join(left, right, keys, label)`

## Output to logs
Always log:
- input paths
- key uniqueness results
- row counts
- unmatched rates

## “Stop and ask” triggers
If any of these happen, stop and ask the user:
- row count increases by > 5–10% unexpectedly
- uniqueness fails where it “should” hold
- unmatched share rises substantially
