# Race Imputation Pipeline

## Overview

This document summarizes the current race-imputation pipeline, the intended target behavior, benchmark checks, and open issues.

Primary scripts:
- `r/impute-race-preflight.R`
- `r/impute-race.R`
- `r/race-imputation-diagnostics.R`

Primary outputs:
- preflight person/case QA files in `output/qa/`
- imputed person/case files in `output/qa/`
- diagnostics reports and CSVs in `output/qa/`

## Goal

Produce defendant-level and case-level race probabilities for Philadelphia eviction cases using:
- cleaned defendant names,
- geography-linked demographic priors (now block-group preferred),
- WRU/BISG posterior inference,
- explicit QA coverage and benchmark diagnostics.

The research target is to recover plausible composition patterns in filed evictions (including high Black shares in likely high-filing geographies) while maintaining reproducible, auditable processing.

## Current Pipeline Design

### 1) Preflight (`r/impute-race-preflight.R`)

Responsibilities:
- keep defendants only for analysis imputation
- parse and clean names
- remove occupant/junk tokens and business entities
- compute `impute_status` and parsing status
- flag counterclaim-linked defendant rows (`counterclaim_party`)
- attach geography fields (`GEOID`, tract derivations)

Key status concept:
- `is_analysis_defendant == TRUE` excludes rows flagged as counterclaim-linked defendants.

### 2) Imputation (`r/impute-race.R`)

Responsibilities:
- consume preflight person rows
- use WRU with tract-coerced priors
- dedupe inference at unique name-geo level
- merge posteriors back to person rows
- produce case-level aggregation and Lodermeier-style case class robustness output

Current case-level robustness classifier:
- threshold: `0.8`
- white if `Pr(all tenants white) >= 0.8`
- minority if `Pr(any tenant minority) >= 0.8`
- otherwise unclassified (missing or uncertain)

### 3) Diagnostics (`r/race-imputation-diagnostics.R`)

Responsibilities:
- benchmark Black prior shares under different weighting schemes
- tract concentration diagnostics
- missingness diagnostics and bounds
- report-ready QA summaries in `output/qa/`

## Benchmarking Work Completed

Most recent benchmark refresh compares paper-style case-level race composition versus current pipeline outputs under similar standards.

Files written:
- `output/qa/lodermeier_benchmark_compare_2006_2019.csv`
- `output/qa/lodermeier_benchmark_coverage_2006_2019.csv`
- `output/qa/lodermeier_renter_benchmark_compare_2010.csv`
- `output/qa/lodermeier_sensitivity_defendant1_vs_all_2006_2019.csv`

High-level findings from that refresh:
- Black share among classified cases is below the paper benchmark by about 5-6 pp.
- White share among classified cases is also lower than benchmark.
- Hispanic/Asian/Other-Mixed shares are higher than benchmark.
- Unclassified share is lower than the benchmark table used in that contrast.
- Restricting to `Defendant #1` only shifts results slightly but does not eliminate the Black-share gap.

## QA Artifacts Added for Inspection

Name-level QA extract including priors and posteriors:
- `output/qa/race_name_parse_prob_bgqa_sample10k_parsev2.csv`

This file includes:
- original and parsed name fields
- final posterior probabilities (`p_white`, `p_black`, `p_hispanic`, `p_asian`, `p_other`)
- BG prior shares used for context (`bg_p_*_prior`)

## What We Are Trying To Do Next

1. Close the benchmark gap while preserving defensible rules.
2. Verify whether composition differences are driven by:
- sample-frame differences (years/case universe),
- classification denominator differences,
- geography prior vintage/definition mismatches,
- residual name-parser edge cases,
- handling of `Defendant #2` and counterclaim-linked records.
3. Add stricter side-by-side comparability tables for final reporting.

## Open Risks

- Benchmark mismatch may reflect definitional drift rather than model error.
- Multi-race breakdown (Hispanic/Asian/Other-Mixed) appears materially higher than paper target.
- Some benchmark percentages currently come from extracted notes and should be re-verified against the exact paper table/version before publication use.

## Repro Notes

Run core scripts from repo root:
- `Rscript r/impute-race-preflight.R ...`
- `Rscript r/impute-race.R ...`
- `Rscript r/race-imputation-diagnostics.R ...`

All generated QA artifacts for this workflow are written to `output/qa/`.
