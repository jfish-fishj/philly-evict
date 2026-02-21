---
name: table-output
description: Create and present analysis tables consistently. Use when writing R analysis outputs with coefficients or summary statistics; prefer LaTeX table artifacts on disk and present key numeric results as terminal tables in responses.
---

## When to use
- Produce model results (coefficients, marginal effects, event-study/LP estimates).
- Produce group summaries (means, shares, counts, rates).
- Update an analysis script that currently writes only free-form text.

## Rules
- Write a machine-readable table to disk (`.csv`) and a publication table to disk (`.tex`).
- Prefer LaTeX output in R scripts (`fixest::etable`, `knitr::kable`, or `gt`).
- Keep output paths config-driven (`p_out(cfg, ...)`), never hardcoded.
- Include key metadata columns when relevant: sample, spec, outcome, reference group, FE/cluster notes.
- Keep column names stable and explicit (`estimate`, `std_error`, `p_value`, `n_obs`).

## R patterns
- Prefer `fixest::etable(..., tex = TRUE)` for regression tables.
- Prefer a paired coefficients CSV extracted from `summary(model)$coeftable`.
- Use `knitr::kable(..., format = "latex", booktabs = TRUE)` or `gt::as_latex()` for non-regression tables.

```r
# Regression table (LaTeX)
tex_lines <- capture.output(fixest::etable(list(m1, m2), se.below = TRUE, digits = 3, tex = TRUE))
writeLines(tex_lines, con = p_out(cfg, "tables", "model_main.tex"))

# Coefficient CSV
ct <- data.table::as.data.table(summary(m1)$coeftable, keep.rownames = "term")
data.table::setnames(ct, c("Estimate", "Std. Error", "Pr(>|t|)"), c("estimate", "std_error", "p_value"))
data.table::fwrite(ct, p_out(cfg, "tables", "model_main_coefficients.csv"))
```

## Terminal presentation
- Present key results as a compact Markdown table in terminal responses.
- Show at minimum: term/group, estimate, standard error, p-value, and sample/spec label.
- State reference category directly above or below the table.
- Round for readability (typically 3-4 decimals) while preserving sign and significance.
