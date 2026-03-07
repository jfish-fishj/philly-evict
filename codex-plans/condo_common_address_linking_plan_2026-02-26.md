# Condo/Common-Address Linking Plan (2026-02-26)

## Purpose

Plan a cohesive fix for condo/common-address linkage failures that affect:

- InfoUSA household coverage (`num_households`)
- InfoUSA race/demographics coverage (`infousa_pct_black`)
- likely also Altos and evictions address-linked coverage

This plan is intended to preserve parcel semantics (`PID`) while improving address-link coverage where the real problem is **many valid parcel candidates**, not true address absence.

## What We Learned (Current Iteration)

1. Rerunning the race/gender/demographics chain fixed a large amount of `infousa_pct_black` missingness.
   - This was mostly stale/misaligned race-linkage output.
   - Example fixed: `8201 HENRY AVE` now has `infousa_pct_black` for `2006-2022`.

2. Some major condo/common-address examples still fail to appear in `bldg_panel_blp` despite existing in InfoUSA and the refreshed building demographics panel.
   - `2001 HAMILTON ST`
   - `226 W RITTENHOUSE SQ`
   - `3900 CITY AVE` (currently unmatched in xwalk)

3. Condo/common-address problems are distinct from true unmatched addresses.

## Core Distinction (Must Be Preserved in QA)

Condo-group logic only fixes **address ambiguity**, not true unmatched addresses.

For each address-linked source (`infousa`, `altos`, `evictions`), classify xwalk outcomes:

- `unique_match`
- `ambiguous_match_condo_groupable`
- `ambiguous_match_noncondo`
- `unmatched_address`

This is critical because otherwise we will over-attribute residual coverage problems to condos.

## Recommended Design

### Keep `PID` immutable

Do not redefine parcel `PID` globally.

Reason:
- `PID` is the parcel key used by parcel metadata, assessments, events, ownership, and panel joins.
- Replacing condo unit PIDs with a single “reference PID” repo-wide will silently change semantics and create hard-to-debug downstream errors.

### Add a second link entity for address-linked sources

Introduce a condo/property-level link key for ambiguous common addresses:

- `condo_group_id`
- `link_id` (source-specific use; e.g., `infousa_link_id`)
- `link_type` (`parcel` vs `condo_group`)
- `anchor_pid` (deterministic compatibility parcel)
- `ownership_unsafe` flag (for condo/common-address-derived linkage)

Recommended rule:
- Keep parcel-level products keyed by `PID`
- Use `link_id` only for address-source linkage and source-derived aggregations
- Carry provenance flags downstream

### Anchor PID (how to use it)

Use a deterministic `anchor_pid` only as a compatibility shim for address-source outputs, not as a replacement `PID`.

Good use:
- xwalk outputs store `anchor_pid`
- downstream address-derived variables can be mapped back to a parcel row for compatibility

Bad use:
- rewriting `PID` in parcel/event/ownership products

## Proposed Phases

## Phase 1: Condo Grouping + Xwalk Outcome Taxonomy (No downstream behavior changes yet)

### 1A. Build condo parcel grouping product

New product (proposed):
- `data/processed/xwalks/condo_parcel_group_membership.csv`

Columns (proposed):
- `PID`
- `is_condo`
- `condo_group_id`
- `anchor_pid`
- `group_size_pids`
- `group_n_parcel_addresses`
- `group_key_version`

Grouping inputs:
- `is_condo` from `r/helper-functions.R` / `standardize_building_type()`
- parcel normalized address keys (including `n_sn_ss_c`)
- deterministic connected-component grouping among condo parcels sharing common-address keys

### 1B. Extend address xwalk outputs with source-agnostic status fields

For each address xwalk (`infousa`, `altos`, `evictions`):
- add `xwalk_status`
- add candidate diagnostics (`n_candidates`, `n_condo_candidates`, `candidate_share_condo`)
- add `link_id`, `link_type`, `anchor_pid`

This allows direct QA of:
- “how much is ambiguous condo-groupable?”
- “how much is truly unmatched?”

## Phase 2: Source-Specific Behavior Changes (Address-derived aggregates use `link_id`)

### 2A. InfoUSA occupancy and demographics

- `num_households` and person-level demographics are aggregated by `link_id x year`
- parcel-level panels retain `PID` but carry:
  - `link_type`
  - `anchor_pid`
  - `ownership_unsafe`

Important:
- Do not copy condo-group building HH counts onto every condo parcel.
- Race shares (`pct_black`) are safer to carry at group level than HH/unit totals.

### 2B. Altos and evictions

Same ambiguity problem likely applies.

Goal:
- distinguish `ambiguous_match_condo_groupable` from `unmatched_address`
- optionally aggregate source signals by `link_id` where appropriate

## Phase 3: Downstream Analysis Integration

### Ownership-sensitive analyses

Use flags to exclude rows where parcel-level ownership attribution is unreliable.

Example policy:
- keep condo-group-linked rows for tenant composition / coverage analyses
- drop `ownership_unsafe == TRUE` rows in transfer/ownership analyses

This matches the intended tradeoff:
- better coverage for demographics/occupancy
- explicit exclusions for ownership inference

## Scripts With Behavior Changes (Inventory)

This section identifies scripts whose outputs/logic would change under the condo/common-address plan.

### A. Required behavior changes (Phase 1 + Phase 2 core)

1. `r/clean-parcel-addresses.R`
   - likely producer of parcel-side normalized address keys used to build condo groups
   - may add/write condo-group helper fields or a new grouping product input

2. `r/make-address-parcel-xwalk.R`
   - core address xwalk logic for `infousa`, `altos`, `evictions`
   - deterministic winner logic changes for ambiguous condo/common-address cases
   - outputs gain `xwalk_status`, `link_id`, `link_type`, `anchor_pid`, candidate diagnostics

3. `r/make-occupancy-vars.r`
   - InfoUSA HH linkage/aggregation behavior changes from parcel-only to parcel-or-condo-group link
   - coverage QA and provenance flags change
   - must avoid parcel-level overassignment of condo-group HH counts

4. `r/impute-race-infousa.R`
   - person-to-entity linkage changes from parcel-only PID link to condo-aware `link_id`
   - output provenance likely changes (`link_type`, `anchor_pid`, `ownership_unsafe`)

5. `r/make-building-demographics.R`
   - demographics aggregation changes from `PID x year` only to condo-aware link entity aggregation + parcel compatibility mapping
   - provenance fields/flags need to be retained

6. `r/make-analytic-sample.R`
   - merge behavior for InfoUSA-derived variables changes (parcel-native vs condo-group-derived provenance)
   - must propagate exclusion/provenance flags into `bldg_panel_blp`

### B. Very likely behavior changes (source-specific downstreams)

7. `r/make-altos-aggs.R`
   - currently aggregates Altos by `PID`
   - condo/common-address xwalk statuses and `link_id` would change grouping behavior and coverage diagnostics
   - likely needs provenance flags if using condo-group linkage

8. `r/make-evict-aggs.R`
   - uses `xwalk_evictions_to_parcel`; behavior changes if evictions xwalk gains condo-group linkage
   - aggregation key and QA need to reflect `xwalk_status`

9. `r/make-rent-panel.R`
   - consumes Altos + evict aggregates and license data at `PID x year`
   - behavior changes if condo-aware link outputs are incorporated (especially coverage / evidence logic / flags)
   - may initially only consume new flags (without using condo HH allocation)

### C. Legacy xwalk/QA scripts that may need behavior changes (if we keep them in active use)

10. `r/merge-infousa-parcels.R`
   - if retained for QA or fallback occupancy path, should emit the same ambiguity taxonomy and condo-group diagnostics
   - especially important while both legacy and new xwalk products coexist

11. `r/merge-evictions-parcels.R`
   - if legacy eviction xwalks remain in use for any pipeline paths, same issue as above

### D. Ownership/transfer analysis scripts (behavior changes if we implement flagged exclusions)

These may not need changes in Phase 1, but **will** change behavior if we adopt `ownership_unsafe` exclusions.

12. `r/analyze-transfer-evictions.R`
13. `r/analyze-transfer-evictions-unified.R`
14. `r/analyze-transfer-evictions-quarterly.R`
15. `r/analyze-rtt-transfers.R`

Expected change:
- explicitly drop or separately tabulate `ownership_unsafe == TRUE` rows

### E. Scripts likely *not* required to change initially (but may be touched later)

1. `r/impute-gender-infousa.R`
   - can likely remain unchanged if race output carries new link/provenance fields and `make-building-demographics.R` uses race-side fields after merge
   - may be updated later for symmetry/provenance passthrough

2. Pure analysis scripts unrelated to ownership or InfoUSA linkage
   - no immediate behavior change expected

## Product / Config Changes (Non-script but required)

Likely required:
- `config.yml` product keys for new condo-group mapping and/or condo-aware xwalk outputs
- maybe docs/data-product note once schema/contracts stabilize

## Guardrails for Implementation

1. Join safety:
- assert uniqueness of `condo_group_id`, `link_id`, and compatibility mappings
- log row counts before/after merges
- explicitly handle one-to-many parcel-to-group propagation

2. Determinism:
- deterministic group ID creation
- deterministic anchor PID selection
- deterministic tie-breaking in ambiguous xwalk cases

3. Provenance:
- every source-derived value merged into parcel panels should indicate:
  - parcel-native vs condo-group
  - ownership-safe vs ownership-unsafe
  - xwalk outcome status

4. No silent PID semantic changes:
- parcel `PID` remains parcel `PID`

## Immediate Next Step (Recommended)

Prototype Phase 1 only:

1. Build `condo_parcel_group_membership` (scratch/product)
2. Add xwalk outcome taxonomy + condo-group diagnostics in `r/make-address-parcel-xwalk.R`
3. Quantify for `infousa`, `altos`, `evictions`:
   - `% unique`
   - `% ambiguous_condo_groupable`
   - `% ambiguous_noncondo`
   - `% unmatched`

This gives the payoff estimate before changing occupancy/demographics behavior.
