# Eviction-InfoUSA Household Linkage

## Overview

This document describes how eviction cases are linked to InfoUSA households in `r/link-evictions-infousa-names.R`.

The matching unit is:
- eviction case (`evict_id`) on the eviction side
- household (`familyid`) on the InfoUSA side

Person-level matches are generated first, then rolled up to one household per case.

## Inputs

Primary inputs:
- `products.race_imputed_person_sample` (eviction defendants/person rows)
- `products.infousa_link_people` (InfoUSA person rows with name + one-line address + parsed `pm.*`)
- `products.evictions_clean` (used to backfill key eviction fields when needed)
- `products.parcel_occupancy_panel` (building-size bins)

Primary outputs:
- `products.evict_infousa_hh_matches`
- `products.evict_infousa_hh_unmatched`
- `products.evict_infousa_candidate_stats`

## Eligibility and normalization

Eviction rows are filtered to:
- filing year `2006-2019`
- non-commercial (`commercial == 0`)
- person rows only (entities/non-person rows dropped)

Both sides:
- names are normalized
- address key prefers parsed `pm.*` composition; `n_sn_ss_c` is fallback
- rows with missing usable `address_norm` are excluded from matching

When year blocking is off (default), InfoUSA is collapsed to one row per `familyid + person_num`.

## Matching flow

### 1) Direct deterministic passes

Run first and remove matched eviction persons from candidate generation:
- exact `address_norm + first_name + last_name`
- exact `street_name + first_name + last_name`

### 2) Candidate generation tiers

Tier usage is controlled by run flags/CLI:
- Tier A (optional): PID blocking (`run.use_pid_in_blocking`)
- Tier B (default main tier): tract/bg + street block (`run.use_bg_in_blocking` toggles bg vs tract)
- Tier C (optional ZIP fallback): zip + street block (`run.use_zip_in_blocking`)

Street blocking keys:
- exact street block (`street_block`)
- optional fuzzy street block (`street_block_fuzzy`, default on)

Current behavior:
- Tier C is used as a fallback for eviction rows missing tract.

### 3) Candidate cap and name gate

Per-eviction-person candidate cap:
- `run.candidate_cap_k` or `--candidate-cap-k`

Fuzzy name gate (applied before fastLink/scoring):
- first-name score >= `PHILLY_NAME_MIN_FIRST_SIM` (default `0.80`)
- last-name score >= `PHILLY_NAME_MIN_LAST_SIM` (default `0.88`)

## Scoring

### Proxy score

Base `p_match` proxy combines:
- last-name similarity
- first-name similarity
- street-name similarity
- exact street-number match

### fastLink scoring

`fastLink` is used when available with fields:
- `first_name`, `last_name`, `street_number`, `street_name`

FastLink run rules:
- only address-eligible rows
- only KxK blocks (`n_ev > 1` and `n_inf > 1`)
- 1xK and Kx1 blocks keep proxy scores
- block extraction is keyed (`setkey` + `J`) for speed

Runtime controls:
- `PHILLY_FASTLINK_MAX_BLOCK_ROWS`
- `PHILLY_FASTLINK_MIN_BLOCK_ROWS`
- `PHILLY_FASTLINK_BLOCK_TIMEOUT_SEC`
- `run.fastlink_chunk_evict_n` / `--fastlink-chunk-evict-n`

## Acceptance rules

`apply_size_thresholds()` applies size-dependent acceptance thresholds.

Current `p_match` thresholds:

| `bldg_size_bin` | `p_match` min |
|---|---|
| `3-4` | `0.76` |
| `5-9` | `0.77` |
| `10-19` | `0.78` |
| `20-49` | `0.79` |
| `50+` / unknown | `0.80` |

Current margin (`top2_margin`) thresholds:

| `bldg_size_bin` | margin min |
|---|---|
| `3-4` | `0.02` |
| `5-9` | `0.03` |
| `10-19` | `0.04` |
| `20-49` | `0.05` |
| `50+` / unknown | `0.06` |

Then deterministic one-to-one resolution is applied:
- one best InfoUSA person per eviction person
- one best household per eviction case
- household collisions across cases are deterministically broken

## Sample mode

For fast development runs:
- enable `run.sample_mode` or `--sample-mode=true`
- prebuilt sample inputs from `r/make-link-evictions-infousa-sample.R`:
  - `products.evict_infousa_sample_evict_people`
  - `products.evict_infousa_sample_infousa_people`
- optional additional case downsample:
  - `run.sample_evict_cases_n` / `--sample-evict-cases-n`
- optional InfoUSA geographic restriction in sample:
  - `run.restrict_inf_to_evict_tracts` / `--restrict-inf-to-evict-tracts`

## QA artifacts

Primary QA outputs:
- `output/qa/evict_infousa_name_link/review_highconf.csv`
- `output/qa/evict_infousa_name_link/review_grayzone.csv`
- `output/qa/evict_infousa_name_link/evict_infousa_*.csv`
- `data/processed/xwalks/evict_infousa_sample_manifest.csv`

Additional diagnostics:
- `output/qa/evict_infousa_unmatched_zip_name_fuzzy_*.csv`
- `output/qa/evict_infousa_unmatched_street_names*_1000_cases.csv`

## Notes

- Person-level direct match counts can exceed final case matches because final outputs are case-level after household resolution.
- See `issues/evict-infousa-linkage-low-match-rate.md` for current low-recall hypotheses and evidence.
