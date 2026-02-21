# Codex Plan: Link Eviction Defendant Names ↔ InfoUSA Person Names (fastLink + guardrails)

## Goal
Link eviction *cases* (households) to InfoUSA *households* with high precision, high QC, and reasonable recall.
- A 60% match rate is very good only if precision remains high.
- Be intentionally conservative in large buildings and more permissive in small buildings.
- **Conceptual unit of matching**: we match eviction cases to *households* (familyid), not to individual people. If any person named in an eviction case matches any person in an InfoUSA household, the case is linked to that household. The output xwalk is keyed on `(evict_id, familyid)`.

## HARD RULE #1: Start on a random sample first
Before any full run, implement `sample_mode` and require it in dev.
1) Do a lightweight pre-join first to derive strata (`bldg_size_bin`, neighborhood/geography).
2) Sample N eviction person rows (e.g., N = 50k), stratified on those strata.
3) Run the full pipeline on the sample.
4) Review QC outputs (especially false-positive risk indicators) before scaling up.

Implementation requirements:
- Single config toggle controls this: `cfg$run$sample_mode`.
- Sampling is reproducible: `set.seed(cfg$run$seed)`.
- Persist sampled IDs to a product key (for reproducibility/audit).

---

## Inputs (two tables)

### Eviction people table (one row per defendant-person)
Required columns:
- `evict_id`: case/docket id (the household-level eviction identifier)
- `party_type`: defendant/plaintiff (matching focuses on defendants)
- `filing_dt` or `filing_yr`
- `first_name`, `last_name` (cleaned)
- `n_sn_ss_c` (canonical cleaned address)

**ID construction and deduplication** (REVISED):
- Build a person-level matching key `evict_person_id` from `(id, first_name, last_name)` where `id` is the case/docket id from cleaned eviction data. This is robust because it uses the existing stable `id` column rather than a fragile composite of name+address+year.
- Multiple defendants per case are expected and each is a separate matching candidate. The same person can also appear across multiple cases (multiple filings at the same address) — each `(id, first_name, last_name)` combination is a distinct matching row.
- Before matching, deduplicate: if the same `(id, first_name, last_name)` appears multiple times, keep one row. Log how many duplicates are dropped.
- `assert_unique(evict_person_id)` after dedup (i.e., `assert_unique(id, first_name, last_name)`). If not unique, stop and write duplicate diagnostics to `output/qa/`.
- After matching, the output rolls up from person-level matches to case→household: if any defendant on case `id` matches a person in `familyid`, the case is linked to that household.

Optional (strongly recommended):
- `PID` (if already crosswalked)
- gender (if available/imputed)
- unit/apartment info (if available)

### InfoUSA people table (one row per person-year)
Required columns:
- `familyid`: household identifier (primary key component)
- `person_num`: person within household
- `year`
- `first_name`, `last_name` (cleaned)

**ID and filtering** (REVISED):
- Preferred ID: `(familyid, person_num)` (year-agnostic person ID). Keep `year` as context, but do not include it in the primary person key by default.
- **Drop all rows with `familyid = NA`** before matching. These are ~1% of records and cannot be linked to households. Log the drop count.
- `individual_id` exists but has significant missingness; do not use it as primary key. It can serve as a supplementary disambiguation field if needed.
- Preserve parsed address components (`pm.house`, `pm.preDir`, `pm.street`, `pm.streetSuf`, `pm.sufDir`) for blocking/scoring fields; use `n_sn_ss_c` as a fallback.

Optional (strongly recommended):
- `PID` (parcel-linked)
- gender
- `head_of_household`
- `bg_geoid` (or tract)
- `locationid`
- cleaned address key (note: canonicalization differs from eviction side; see Tier B notes)

---

## Core design: candidate generation (blocking) then scoring
### Why
fastLink is useful for scoring but unsafe with broad candidate sets.
We block hard, cap candidates, and log all caps/drops.

### Year alignment
Default behavior is year-agnostic blocking:
- match candidates without a year window
- collapse InfoUSA to one row per `(familyid, person_num)` before blocking
Optional year-aware mode can be re-enabled if needed:
- `run.use_year_in_blocking: true`
- `run.year_window: [0]` (or wider, if explicitly desired)

---

## Step 0: Pre-filters & normalization (both sides)
1) Standardize names:
- uppercase, remove punctuation, collapse whitespace
- strip suffixes/honorifics (JR/SR/III/MR/DR, etc.)

2) Drop non-person/entity rows (especially eviction side):
- if `is_entity` exists, drop `TRUE`
- also drop names containing business/non-person tokens (LLC/INC/TRUST/PROPERTIES/MANAGEMENT/HOUSING AUTHORITY/etc.)

3) Create helper fields:
- `last_initial`
- `fn_initial`
- `last_name_len`
- optional `common_lastname_flag` from InfoUSA surname frequency (see config knob `common_surname_quantile`)

---

## Step 1: Attach building size/type (for thresholds)
Attach `bldg_size_bin` for each eviction person row.

Preferred:
- if eviction rows have `PID`, join to parcel/year panel for `num_units_bin`, `structure_bin`, building type.

Fallback:
- if no `PID`, use address→PID crosswalk first, then join parcel/year.

If neither is possible:
- assign `"unknown_size"` and apply stricter acceptance thresholds.

Standard bins:
- `bldg_size_bin` in `{ "1", "2", "3-4", "5-9", "10-19", "20-49", "50+" }`
- optional `structure_bin` (row/single-family vs multi)

---

## Step 2: Candidate generation (blocking tiers with caps)
Create candidate pairs in tiers; later tiers only for still-unmatched eviction rows.

### Tier A (highest precision): `PID + exact last_name`
Block on:
- same `PID`
- exact `last_name`
- optional `year_link` filter only when year-aware mode is enabled

### Tier B: `street name + exact last_name`
For rows without PID match, or as a supplementary tier:
- block on street name tokens (shared between both sides) + street number
- exact `last_name`
- optional `year_link` filter only when year-aware mode is enabled

**Address canonicalization note**: The eviction side (`clean-eviction-addresses.R`) and InfoUSA side (`clean-infousa-addresses.R`) use *different* canonicalization pipelines and do NOT produce identical `n_sn_ss_c` keys for the same physical address. This is a known divergence. For Tier B, do NOT require exact `n_sn_ss_c` equality. Instead, block on parsed street components (street number + street name tokens) which are more robust to canonicalization differences. Alternatively, if both sides have been routed through `make-address-parcel-xwalk.R` and have `PID`, prefer Tier A.

### Tier C (last resort): `micro-geo + street tokens + exact last_name`
Only when no PID/address-key path works:
- block on `bg_geoid` (or tract) and street number/name tokens
- optional `year_link` filter only when year-aware mode is enabled
- exact `last_name`

### Candidate explosion cap (mandatory guardrail)
For each eviction row, if candidate count > `K` (e.g., 50):
- tighten using `fn_initial`, then `last_initial`, then first-name similarity prefilter
- if still > `K`, no auto-match in that tier; set `too_many_candidates = TRUE`

---

## Mandatory join assertions (every tier)
Before each join/block:
- assert uniqueness of intended keys on both sides
- log row counts and unique key counts

After each join/block:
- log candidate row count and per-eviction candidate distribution
- assert no unexpected blow-up versus defined upper bounds
- explicitly flag many-to-many situations and how they are handled

All checks/logs go to `output/logs/` and QC artifacts in `output/qa/`.

---

## Step 3: Name similarity logic
No middle-name matching.

### First-name score
`fn_match_score`:
- `1.00` exact
- `0.95` nickname dictionary match
- otherwise Jaro-Winkler similarity in `[0,1]`

Nickname dictionary:
- maintained as a config-managed input key (no hardcoded path)
- versioned and auditable
- **Source must be pinned before implementation** — options include: `humaniformat` R package, Census Bureau nickname lists, or a custom curated CSV. Pick one, document it, and add it as a versioned input under `cfg$inputs$nickname_dict`.

### Last name
- default exact match in all tiers
- optional tiny fuzziness only with `PID` and very small buildings (off by default)

### Gender check (diagnostic only, not used in scoring by default)
- Record `gender_agreement` for all matched pairs (TRUE/FALSE/NA)
- Do NOT use gender as a blocking, scoring, or acceptance criterion initially
- If QC reveals unacceptable false-positive rates, gender can be promoted to a tiebreaker or acceptance filter — but only after reviewing the evidence

---

## Step 4: Scoring and decision rules (size-dependent)
Two modes:
1) deterministic acceptance for very small buildings
2) fastLink scoring for medium/large buildings

### 4.1 Deterministic acceptance (small buildings)
If `bldg_size_bin %in% c("1", "2")` or structure indicates row/single-family:
Accept only if all hold:
- exact address key (`PID` or `n_sn_ss_c`)
- exact `last_name`
- first name exact or nickname or `JW >= 0.90`
- unique qualifying candidate (otherwise `ambiguous_flag = TRUE`)
- (record `gender_agreement` for QC but do not require it for acceptance)

### 4.2 fastLink / probabilistic scoring (medium/large buildings)

**REVISED**: fastLink must be given sufficient fields to produce meaningful posteriors. Running it on only `first_name` + `last_name` is insufficient — the model needs more variation to discriminate.

fastLink inputs (all available fields from blocking):
- `first_name` (string, Jaro-Winkler)
- `last_name` (string, Jaro-Winkler — even though blocked on exact match, fastLink uses the similarity weight internally)
- `street_number` (numeric/exact)
- `street_name` (string, Jaro-Winkler)

Do NOT include gender in the initial fastLink specification — only add it as a matching field if false-positive rates in QC are unacceptably high.

If address components are not available in the candidate pair (e.g., Tier C with only geo blocking), fall back to deterministic JW thresholds on first name rather than running fastLink with just two name fields.

Thresholds:
- size 3-4: `p_match >= 0.92` and `(best - second_best) >= 0.05`
- size 5-9: `p_match >= 0.95` and `(best - second_best) >= 0.07`
- size 10-19: `p_match >= 0.97` and `(best - second_best) >= 0.10`
- size 20-49: `p_match >= 0.985` and `(best - second_best) >= 0.12`
- size 50+: `p_match >= 0.99` and `(best - second_best) >= 0.15`
- optional relaxation `-0.01` only for size 3-9 with agreeing gender

Gray zone:
- near-threshold rows get `needs_review = TRUE`; no auto-link

### One-to-one constraint with deterministic tie-breaking
Enforce `1 eviction case -> 1 InfoUSA household`.
- Multiple defendant names on the same `evict_id` can each match to different `person_num` values within the same `familyid` — this is fine and expected (e.g., two named tenants on a lease both appear in InfoUSA for the same household).
- The constraint is: one `evict_id` maps to at most one `familyid`. If different defendants on the same case match to *different* familyids, flag as `ambiguous_household = TRUE` and do not auto-link.

If collisions occur (same `familyid` claimed by multiple `evict_id`s), resolve deterministically by:
1) higher tier priority (`A > B > C`)
2) higher `p_match`
3) larger `(best - second_best)`
4) higher `fn_match_score`
5) smaller `abs(inf_year - filing_yr)`
6) stable lexical tie-break (`evict_id`, then `familyid`)

---

## Step 5: Output products and contracts
All write paths come from `cfg` keys (no hardcoded paths).

Processed products:
1) **matches product** (e.g., `cfg$products$evict_infousa_hh_matches`)
- columns: `evict_id`, `familyid`, `match_tier`, `matched_person_nums` (list of person_nums that matched), `bldg_size_bin`, `year_link_used`, `p_match` (best), `fn_match_score` (best), `gender_agreement`, `ambiguous_flag`, `ambiguous_household`, `needs_review`, `schema_version`
- **primary key: `evict_id`** (one case → one household)

2) **unmatched product** (e.g., `cfg$products$evict_infousa_hh_unmatched`)
- primary key: `evict_id`

3) **candidate stats product** (e.g., `cfg$products$evict_infousa_candidate_stats`)
- one row per `evict_id`; includes `num_candidates`, `was_capped`, `tier_attempted`, `n_defendants_on_case`, `n_defendants_matched`

QA outputs (`output/qa/`):
- `review_highconf.csv`
- `review_grayzone.csv`
- sampled ID manifest used in sample mode

Contract requirements:
- stable column names
- documented primary key(s)
- `schema_version` field or explicit schema note
- documented in `docs/data-products.md`

---

## Step 6: QC / guardrail reporting (every run)
### Match rates
- overall
- by tier (A/B/C)
- by building size bin
- by neighborhood/geography
- by surname frequency bucket

### False-positive risk indicators
- ambiguous share (`top1-top2` small)
- high match rates in 20+ unit buildings without strong first-name evidence
- matches concentrated in common surnames
- geography-specific selection/collapse in match rates
- many-to-one collisions before final resolution
- `ambiguous_household` rate (multiple familyids matched per case)

### Score distributions
- `p_match` histograms by size bin
- `fn_match_score` histograms by tier

### Validation against known links (ADDED)
- Build a small manual validation set of confirmed matches. This must be hand-curated — e.g., cases where the eviction address + defendant name + filing year exactly match a unique InfoUSA person at the same PID, verified by eye.
- Use this as a held-out truth set to benchmark precision and recall before scaling.
- Target: ~200-500 manually verified pairs, stratified by building size.
- This is a one-time setup cost; persist as a versioned input under `cfg$inputs$evict_infousa_validation_set`.

---

## Implementation outline (R/data.table, repo-convention aligned)
Code organization:
- `r/lib/link_evictions_infousa_utils.R`
  - normalize_name(), load_nickname_dict(), first_name_score(), gender_compatible()
- `r/lib/link_evictions_infousa_candidates.R`
  - make_year_links(), build_blocks_tier_A/B/C(), enforce_candidate_caps()
- `r/lib/link_evictions_infousa_scoring.R`
  - run_fastlink_on_pairs(), apply_size_thresholds(), resolve_one_to_one_deterministic()
- `r/lib/link_evictions_infousa_qc.R`
  - qc_tables(), qc_plots(), write_review_samples()
- thin wrapper script: `r/link-evictions-infousa-names.R`
  - read config
  - orchestrate tiers/scoring/QC
  - write products + logs

Optional orchestration:
- add a dedicated target in `_targets.R` with explicit upstream dependencies.

### Default knobs (config)
- `run.sample_mode: TRUE` (dev default)
- `run.sample_n: 50000`
- `run.seed: 123`
- `run.use_year_in_blocking: false`
- `run.year_window: [0]`
- `run.candidate_cap_k: 50`
- `run.fastlink_chunk_evict_n: 100` (dev iteration)
- `run.common_surname_quantile: 0.995` — used to flag common surnames for extra scrutiny in QC reporting (Step 6) and optional candidate tightening in large buildings

---

## Notes / philosophy
- In rowhomes / 1-2 unit buildings, address + surname can be strong evidence; still require uniqueness.
- In large buildings, address + surname alone is weak; require stronger posterior evidence and margin.
- Prefer unmatched rows over silent false positives.
- **Match cases to households, not people to people.** Any person-level match on a case is evidence that the household was involved; the output xwalk is `(evict_id, familyid)`.

---

## Revision log
- **v2 (2026-02-19)**: Incorporated review feedback:
  1. Person-level ID built from `(id, first_name, last_name)` using existing docket `id` column. Dedup within case. Each defendant is a separate matching candidate; output rolls up to case→household.
  2. InfoUSA: drop NA familyids; prefer `(familyid, person_num)` as person ID (year-agnostic by default), with year used as optional context.
  3. Year blocking is off by default (`run.use_year_in_blocking = false`); InfoUSA person ID defaults to `(familyid, person_num)`.
  4. Tier B: documented that eviction and InfoUSA canonicalization pipelines differ; block on street components, not exact `n_sn_ss_c`.
  5. fastLink: require address components alongside names; fall back to deterministic JW if address not available. Gender excluded from scoring by default — only add if false-positive rates warrant it.
  6. Nickname dictionary: must be pinned to a specific source before implementation.
  7. One-to-one constraint redefined at case→household level; multiple defendants per case matching same familyid is expected.
  8. Added manual validation set requirement for precision/recall benchmarking.
  9. Clarified `common_surname_quantile` usage in QC reporting.
