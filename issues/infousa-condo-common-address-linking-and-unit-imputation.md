# InfoUSA condo/common-address linking and condo-aware unit imputation (2026-02-26)

This issue tracks a structural problem in how we link InfoUSA addresses to parcels when the address is a condo/common building address (rather than a unit-specific parcel address).

The problem shows up in both:

- `num_households` coverage / occupancy imputation (especially midsize buildings)
- `infousa_pct_black` coverage in `bldg_panel_blp` (after race/gender imputation)

The same underlying xwalk failure mode likely affects other address-linked sources too (especially Altos and evictions): an address exists, but the xwalk sees many parcel candidates.

## Problem summary

We currently treat `PID` (parcel) as the primary building identifier through most of the pipeline, and most InfoUSA joins assume a single winning parcel per `n_sn_ss_c` address key.

That breaks down for condo/common-address cases:

- InfoUSA often records a building/common address (`n_sn_ss_c`) with many households
- OPA parcels may be condo units or condo/common-element parcels
- license rows can indicate a rental unit in a condo building, but unit counts / parcel identity are not "whole-building" quantities
- xwalk tie-breaking can either:
  - choose one arbitrary parcel (losing siblings), or
  - fail to match due to large ambiguous candidate sets

This creates two linked issues:

1. **Coverage loss**: real residential InfoUSA addresses do not propagate cleanly into parcel-level panels.
2. **Unit-imputation risk**: if we naively attach building-level households to a condo unit parcel, we can badly misstate occupancy / unit counts.

## Important distinction: ambiguous match vs true unmatched

A condo/common-address solution will only fix a subset of "missing" records.

We should separate xwalk outcomes (for `infousa`, `altos`, and evictions address xwalks) into:

1. `unique_match`
   - address exists and maps cleanly to one parcel
2. `ambiguous_match_condo_groupable`
   - address exists, maps to many parcels, and the candidate set looks like a condo/common-address cluster
   - this is the main target for condo-group linking
3. `ambiguous_match_noncondo`
   - address exists, maps to many parcels, but not a condo/common-address cluster
   - condo logic will not fully solve this
4. `unmatched_address`
   - address does not match parcel candidates under current normalization/xwalk rules
   - condo-group logic will not solve this

This prevents us from attributing all coverage gaps to condo handling.

## Evidence from current iteration

### What the rerun fixed (important contrast)

Rerunning:

- `r/impute-gender-infousa.R`
- `r/impute-race-infousa.R`
- `r/make-building-demographics.R`
- `r/make-analytic-sample.R`

recovered a large amount of `infousa_pct_black` coverage. This shows a lot of missingness was stale race/PID linkage, not true InfoUSA absence.

Example:

- `8201 HENRY AVE` (`PID 881093500`) now has `infousa_pct_black` for `2006-2022` after rerun.

### Condo/common-address residual problem remains

Examples manually identified and confirmed in products:

- `2001 HAMILTON ST` (`PID 888000799`) appears in InfoUSA and the refreshed InfoUSA building demographics panel, but is **not** in `ever_rentals_pid` / `bldg_panel_blp`.
- `226 W RITTENHOUSE SQ` (`PID 888080958`) same pattern.
- `3900 CITY AVE` has large InfoUSA household counts but remains unmatched in the current InfoUSA xwalk.

This is consistent with condo/common-address / parcel-identity mismatch, not just missing InfoUSA.

We should expect the same pattern in Altos / evictions where a source address is a building/common address and parcel-level xwalk resolution becomes non-unique.

## Why this matters

- It disproportionately affects midsize buildings (e.g., `6-20` units), where coverage is already weakest.
- It contaminates interpretation of "InfoUSA undercoverage" by mixing:
  - true InfoUSA missingness
  - xwalk ambiguity
  - condo/common-address representation mismatch
- It can produce invalid unit imputation if building-level households are assigned to a single condo parcel.

## Design goals

1. **Do not overwrite parcel identity (`PID`) globally.**
2. **Handle condo/common-address records as building/property-level entities where needed.**
3. **Keep joins deterministic and auditable.**
4. **Allow incremental rollout without rewriting the entire pipeline at once.**

## Recommended approach (phased)

### Core idea: add a condo/property link entity; keep `PID` immutable

Use `PID` for parcel-level products as today, but introduce a second identifier for InfoUSA linking in condo/common-address cases.

Proposed fields/products:

- `condo_group_id`: stable building/property-level ID for condo/common-address clusters
- `infousa_link_id`: ID used for InfoUSA linkage
  - non-condo: `infousa_link_id == PID`
  - condo/common-address: `infousa_link_id == condo_group_id`
- `infousa_link_type`: `parcel` vs `condo_group`
- `infousa_anchor_pid`: deterministic representative parcel for compatibility (see below)
- (optionally generalized later) `link_anchor_pid` for source-agnostic address-link outputs

### Why not replace all condo PIDs with a "reference PID" everywhere?

Using a single reference PID across all scripts is tempting, but risky:

- silently changes parcel semantics
- can break ownership / assessments / events joins
- makes interpretation of `PID` inconsistent across products
- increases chance of hidden double-counting or dropped parcel-level records

A reference PID is useful as a **compatibility shim** for InfoUSA-derived variables, but should not replace parcel `PID` as the canonical key.

## Proposed implementation plan

### Phase 1 (low-risk, high-value): add condo grouping + link metadata

#### 1) Create condo group membership product

Build a new product (e.g., `processed/xwalks/condo_parcel_group_membership.csv`) with one row per parcel:

- `PID`
- `is_condo` (from `standardize_building_type()` / `r/helper-functions.R`)
- `condo_group_id` (if condo/common-address grouped)
- `infousa_anchor_pid` (deterministic representative PID)
- group diagnostics (`group_size_pids`, `group_n_addresses`, etc.)

Suggested grouping rule (initial version):

- start with parcels flagged `is_condo == TRUE`
- group condo parcels using parcel-side normalized address keys (including `n_sn_ss_c`)
- allow connected components across shared condo/common address keys
- deterministic ID + deterministic anchor selection (e.g., smallest normalized `PID`)

Note: this is a building-address grouping for InfoUSA linkage, not a claim that all grouped units share ownership.

#### 2) Extend InfoUSA xwalk outputs with condo-aware link fields

Update `r/make-address-parcel-xwalk.R` (and QA in `r/merge-infousa-parcels.R` if needed) so each source address winner includes:

- original winning parcel (`parcel_number`)
- `infousa_link_id`
- `infousa_link_type`
- `infousa_anchor_pid`
- `is_condo_parcel`
- `condo_group_id`

For non-condo parcels, these collapse to the current parcel behavior.

For condo/common-address winners with many condo candidates, route to `condo_group_id` rather than forcing a single parcel winner.

### Phase 1b (same pattern across sources): xwalk outcome diagnostics

For each address xwalk product (`infousa`, `altos`, `evictions` where applicable), persist:

- `xwalk_status` (`unique_match`, `ambiguous_match_condo_groupable`, `ambiguous_match_noncondo`, `unmatched_address`)
- `n_candidates`
- `n_condo_candidates`
- `candidate_share_condo`
- `link_id` (parcel or condo_group)
- `anchor_pid` (deterministic representative parcel for compatibility)

This makes it easy to quantify how much condo-group logic helps versus how much missingness is truly unmatched address coverage.

### Phase 2 (targeted propagation): use link entity for InfoUSA-derived variables

#### 3) Occupancy (`r/make-occupancy-vars.r`)

Current issue:

- `num_households` is treated as parcel-level.

Condo-aware approach:

- aggregate InfoUSA households at `infousa_link_id x year`
- write a condo-aware InfoUSA HH panel (new artifact)
- keep parcel-level outputs but add flags:
  - `infousa_from_condo_group`
  - `infousa_link_type`
  - `infousa_anchor_pid`

Important guardrail:

- do **not** naively copy full building HH counts onto every condo parcel in a group

Short-term acceptable behavior:

- use condo-group linkage for coverage diagnostics and race shares
- defer parcel-level HH allocation for condos until an explicit allocation rule is chosen

This same caution applies to Altos-derived building-level signals when the source address is a common condo address rather than a unit parcel.

#### 4) Race / demographics (`r/impute-race-infousa.R`, `r/make-building-demographics.R`)

Race shares are easier than HH counts because they are composition measures.

Condo-aware approach:

- link person rows to `infousa_link_id` (not only parcel `PID`)
- aggregate demographics at `infousa_link_id x year`
- retain a parcel compatibility mapping via `infousa_anchor_pid`
- add flags for whether a parcel-level value comes from a condo-group aggregate

This should recover many condo/common-address `infousa_pct_black` missings without inventing parcel-level HH counts.

### Phase 3 (downstream integration): BLP / analytic panels

#### 5) `r/make-analytic-sample.R` integration

Do not replace `PID`.

Instead merge InfoUSA-derived variables with explicit provenance:

- parcel-native InfoUSA link (`parcel`)
- condo-group-derived InfoUSA link (`condo_group`)

Add columns/flags like:

- `infousa_link_type`
- `infousa_from_condo_group`
- `infousa_anchor_pid`
- `condo_link_ownership_unsafe` (or equivalent)

This lets analysis scripts:

- include condo-group-derived race shares where appropriate
- exclude condo-derived HH/unit quantities where they are not conceptually valid
- drop condo-group-linked rows from ownership-sensitive analyses (e.g., transfer analysis) when parcel ownership attribution is unreliable

## Compatibility option (if we need a quick bridge)

If short-term code churn is too high, use the anchor PID approach **only for InfoUSA linkage outputs**:

- all condo/common-address InfoUSA rows map to `infousa_anchor_pid`
- `PID` remains unchanged in parcel products
- downstream scripts read `infousa_anchor_pid` explicitly for InfoUSA merges

This captures most coverage gains while avoiding a repo-wide redefinition of `PID`.

## Analysis tradeoff (explicit)

Assigning a source address to a condo/common building entity improves coverage, but weakens parcel-level ownership interpretability.

That tradeoff is acceptable if we preserve flags and use analysis-specific exclusions:

- keep condo-group-linked rows for tenant composition / coverage analyses
- exclude (or separately analyze) condo-group-linked rows in ownership / transfer analyses

## Script touchpoints (likely)

- `r/helper-functions.R`
  - reuse `is_condo` from `standardize_building_type()`
- `r/clean-parcel-addresses.R`
  - good place to materialize condo grouping inputs / address keys
- `r/make-address-parcel-xwalk.R`
  - add condo-aware link target fields and tie logic
- `r/merge-infousa-parcels.R`
  - QA summaries for condo/common-address ambiguity
- `r/make-occupancy-vars.r`
  - use `infousa_link_id` for HH aggregation; add condo provenance flags
- `r/impute-race-infousa.R`
  - link persons using condo-aware InfoUSA target
- `r/make-building-demographics.R`
  - support entity-level aggregation + parcel compatibility mapping
- `r/make-analytic-sample.R`
  - merge and preserve provenance (`parcel` vs `condo_group`)

## QA / acceptance criteria

1. Known examples behave as expected:
   - `8201 HENRY AVE` remains fixed after reruns
   - `2001 HAMILTON ST` and `226 W RITTENHOUSE SQ` appear in condo-aware InfoUSA-linked outputs
   - `3900 CITY AVE` is either matched to condo/common group or clearly classified as unresolved

2. Join safety checks:
   - explicit uniqueness assertions on new keys (`condo_group_id`, `infousa_link_id`)
   - no row explosions in `PID x year` panels

3. Coverage diagnostics:
   - report change in InfoUSA HH coverage and `infousa_pct_black` coverage by:
     - year
     - `num_units_bin`
     - `is_condo`
     - `building_type`
   - and by xwalk outcome (`unique_match`, `ambiguous_match_condo_groupable`, `unmatched_address`, etc.)

4. Cross-source xwalk QA:
   - run the same xwalk outcome summary for `infousa`, `altos`, and `evictions`
   - verify condo-group logic increases coverage mainly through `ambiguous_match_condo_groupable`, not `unmatched_address`

5. Provenance:
   - every InfoUSA-derived merge into parcel panels indicates whether it came from parcel vs condo-group link

## Open questions

1. What is the right parcel-level allocation rule (if any) for condo-group `num_households`?
   - proportional to assessed unit count?
   - equal split across condo unit parcels?
   - no parcel allocation (group-only HH metrics)?

2. How should condo-group-derived variables interact with ownership analyses?
   - likely separate handling / flags needed

3. Should condo grouping be limited to `is_condo == TRUE`, or expanded to other common-address complexes where parcel coding is nonres but InfoUSA households are clearly residential?

## Guardrail

This is a merge-key / entity-definition change. Do not silently replace `PID` semantics.

Implement with:

- new IDs / mapping products
- explicit provenance flags
- before/after coverage QA
- documented downstream behavior changes
