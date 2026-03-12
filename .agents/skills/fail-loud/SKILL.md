---
name: fail-loud
description: Fail loudly on missing data, stale inputs, or schema violations. Never silently degrade or substitute fallbacks for required fields.
---

## Core principle

A silent fallback is a hidden bug. When required data is missing or stale, the correct response is a loud, informative error — not a `"---"` placeholder, a skipped section, an `NA` fill, or a conditional block that quietly omits output. The user should never be left wondering why a table column is blank or a section is absent.

## Anti-patterns (always forbidden for required data)

### R pipeline scripts
```r
# BAD — silently fills missing required column
if (!"col" %in% names(dt)) dt[, col := NA_real_]

# BAD — silently skips required processing
if (nrow(section_data) > 0) { do_something(section_data) }

# BAD — see also join-safety skill
intersect(required_cols, names(dt))  # then fallback branch
```

### QMD / reports / notebooks
```r
# BAD — stale CSV renders silently with dashes
if (!"mean_units_owned" %in% names(desc)) desc[, mean_units_owned := NA_real_]

# BAD — silently omits a section, no indication to the user
if (nrow(desc_port_cong) > 0) { kbl(...) }
```

## Correct pattern

### R pipeline scripts
```r
# Required column → hard stop with message telling user what to do
assert_has_cols(dt, c("mean_units_owned"), "filing_decomp_descriptives_by_landlord.csv")

# Required file → hard stop
if (!file.exists(p_product(cfg, "xwalk_pid_conglomerate")))
  stop("xwalk_pid_conglomerate not found — run build-ownership-panel.R first")
```

### QMD / reports / notebooks
```r
# Required column — fail with clear instruction
if (!"mean_units_owned" %in% names(desc))
  stop("Column 'mean_units_owned' missing — re-run analyze-filing-decomposition.R")

# Required model output
stopifnot("M5 model missing from R² table — re-run R script" = "M5" %in% r2$model)
```

## When optional / conditional rendering IS acceptable

Only when the feature is explicitly opt-in (behind a config flag, a separate optional data
product, or a clearly documented extension). Even then:

- Log a visible warning, not a silent skip.
- Label the skipped section in the output (e.g. `"B4: Skipped — race data not available"`).
- Never make the output look complete when it isn't.

```r
# ACCEPTABLE — optional data product, clearly logged
has_race_cases <- file.exists(p_product(cfg, "race_imputed_case_sample"))
if (!has_race_cases) {
  logf("WARNING: race_imputed_case_sample not found; B4 will be skipped", log_file = log_file)
}
```

## Rule of thumb

> If removing the fallback would cause a visible failure, the fallback is hiding a real problem.
> Remove the fallback and let the failure be visible.
