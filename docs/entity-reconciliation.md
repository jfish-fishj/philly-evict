# Entity Reconciliation Pipeline

**Scripts:** `r/build-owner-linkage.R` → `r/build-ownership-panel.R`
**Outputs:** `data/processed/xwalks/owner_linkage_xwalk.csv` and several downstream crosswalks

---

## Overview

The goal is to build a clean PID × year ownership panel where properties owned by the same
real-world entity (operating under multiple legal names or LLC variants) are linked together
into a *conglomerate*. The pipeline runs in two scripts and three phases:

| Phase | Where | What |
|-------|-------|------|
| **Phase 1**: exact name normalization | `build-owner-linkage.R` | Standardize names → assign `owner_group_id` |
| **Phase 2**: OPA mailing-address graph | `build-ownership-panel.R` | Link entities sharing a mailing address (within parcel data only) |
| **Phase 3**: RTT-OPA transitive edges | `build-ownership-panel.R` | One-shot link: RTT last grantee ↔ OPA 2024 owner |

---

## Phase 1 — Name Standardization (`build-owner-linkage.R`)

### Input
`rtt_clean` — all deed transfers from `process-rtt-data.R`.

### Name cleaning (`standardize_corp_name`)
Applied to corporate and trust names. Steps in order:
1. Strip legal entity suffixes: `LLC`, `INC`, `CORP`, `LP`, `LLP`, `LTD`, `CO`, `COMPANY`
2. Normalize bank/trust name fragments:
   - `U S` → `US`
   - `TRUS` or `TR` at end of name → `TRUST` (preserves trust designation rather than silently dropping it)
   - Strip trailing `FA` (federal association) and `N A` (national association)
3. Strip trailing punctuation (`.`, `,`, `-`)
4. Strip leading `THE`
5. Strip trailing Roman numerals (LLC series: `I`, `II`, `III`, etc.)
6. Strip trailing digits

### Entity classification

| Flag | Condition | Grouping in Phase 1 |
|------|-----------|---------------------|
| `is_financial_intermediary` | Name matches bank/GSE/servicer regex (applied to `owner_std`) | Corp-style (name only) |
| `is_corp` & `!is_trust` | Name matches `business_regex` (LLC, INC, etc.) | Corp-style (name only) |
| `is_trust` | Name contains `TRUST` or `TRUSTS` post-normalization | **Person-style (name + PID)** |
| neither | Person name | Person-style (name + PID) |

**Why trusts are person-style:** Trust names are often used at registered-agent addresses (law
firms, financial advisors) shared with dozens of unrelated trusts. Treating trusts as
corporations (grouping by name alone across all PIDs) caused spurious merges in Phase 2.
Each trust entity is now PID-specific, so a mailing-address match between two trust entities
implies a genuine shared address — not just a shared trustee firm.

### Output: `owner_linkage_xwalk.csv`
Primary key: `(grantee_upper, PID)`.
- Corp/FI rows: `PID = NA` (one row per distinct upper-case grantee name)
- Person/trust rows: `PID` = actual parcel ID

Key columns: `owner_group_id`, `owner_std`, `is_corp`, `is_trust`, `is_financial_intermediary`

---

## Phase 2 — OPA Mailing-Address Name+Address Matching (`build-ownership-panel.R`, iterative)

### Design
For each pair of entities sharing the same mailing address, check whether they are the
same entity by name:

- **Corps** (`is_corp=TRUE`, `is_trust=FALSE`): match on `(business_short_name, mailing_norm)`,
  where `business_short_name` strips common descriptor words (PROPERTIES, MANAGEMENT, REALTY,
  HOLDINGS, DEVELOPMENT, INVESTMENTS, ASSOCIATES, ENTERPRISES, VENTURES, GROUP, etc.) from
  `entity_name`. Only match if the short name has ≥ 4 characters.
  - Example: "ABC PROPERTIES LLC" and "ABC MANAGEMENT INC" both reduce to "ABC" → merged if
    they share a mailing address.
- **Persons / trusts** (`is_corp=FALSE` or `is_trust=TRUE`): match on `(entity_name, mailing_norm)` —
  exact (standardized) name + same address. Two people named "SMITH JOHN" at the same address
  merge; "SMITH JOHN" and "JONES MARY" at the same address do not.

The igraph connected-component algorithm then finds transitive merges (A=B and B=C → A=B=C)
across the direct entity-entity pairs.

The loop runs to convergence: in each round, after entities merge, their mailing-address
profiles are pooled (each entity in a conglomerate inherits all members' addresses). This
enables full transitive closure within the parcel data.

**Critically, only OPA mailing addresses (`opa_mailing_raw`) are used throughout all rounds.**
RTT-only entities (that appeared in transfers but not in the 2024 parcel file) have no OPA
mailing record and cannot contribute addresses — preventing contamination of the OPA graph
by RTT-sourced names.

### Why name+address (not just address)?
Pure address matching (any two entities at the same address → merged) caused the City of
Philadelphia, which owns thousands of parcels each with a unique OPA mailing address, to
absorb 10,000+ unrelated entities in a single round. The name requirement prevents this:
CITY OF PHILADELPHIA will only merge with another entity whose business_short_name is
"CITY OF PHILADELPHIA" — a rare false positive.

### Registered-agent filter
Any mailing address linked to more than `entity_linkage_ra_threshold` (default 300) distinct
entities is dropped before matching. This catches registered-agent addresses (law firms,
management companies) that appear on thousands of unrelated properties.

### Conservative baseline snapshot
Before the iterative loop, a *single round with no expansion* is run and its conglomerate size
distribution is logged and written to `output/qa/ownership_conservative_top_conglomerates.csv`.
Compare this with the full iterative output to assess how much chain-expansion matters.

### Tuning parameters
| Config key | Default | Effect |
|---|---|---|
| `run.entity_linkage_max_rounds` | 5 | Max Phase 2 rounds before forcing stop |
| `run.entity_linkage_ra_threshold` | 300 | Addresses with > N entities are excluded |
| `run.entity_linkage_diag_threshold` | 20 | Conglomerates > N members get bridge-address diagnostics |

---

## Phase 3 — RTT-OPA Transitive Edges (one shot)

After Phase 2 converges, add one set of edges: for each PID where the **last RTT grantee**
(from transfer records through 2024) differs from the **current OPA 2024 owner**, *and*
the two entities are plausibly the same entity under different name spellings, link those
two conglomerates. This closes gaps like "ABC LLC" (OPA) vs "ABC INC" (RTT), or "GULLE
JEAN PAUL" (OPA) vs "GULLE JEAN P" (RTT).

**Phase 3 name-similarity gate** (applied before creating any edge):
- **Corps**: both sides must have the same `business_short_name` (≥4 chars)
- **Persons/trusts**: both sides must share ≥1 token of ≥5 characters in `entity_name`
- **Cross-type** (corp ↔ person): no edge

**Why the gate is necessary**: Government entities (City of Philadelphia, PHA, Redevelopment
Authority) acquired hundreds of parcels via tax sale and eminent domain — those acquisitions
appear in RTT. The City subsequently conveyed these parcels to CDCs, developers, and
homebuyers through tax-exempt transfers not captured in RTT. Without the gate, Phase 3 would
link every such recipient to the City (OPA owner ≠ RTT last grantee), creating a 1,000+
entity mega-conglomerate. The name gate drops these since "K&A INVESTMENTS" shares no
5-character tokens with "CITY OF PHILADELPHIA".

**Phase 3 runs exactly once and does NOT trigger another Phase 2 pass.** The merged entities
from Phase 3 do not get their OPA addresses pooled back into Phase 2.

Rule: `if A=B=C in OPA (Phase 2) and C≈D in RTT (name-similar), then A=B=C=D — stop there.`

---

## Diagnostics

### Bridge-address log (`output/qa/ownership_large_conglomerate_bridge_addresses.csv`)
For every conglomerate with more than `entity_linkage_diag_threshold` members, lists the
mailing addresses at which multiple conglomerate members were originally registered. The
`n_entities_at_addr` column shows how many members shared each address — a high value
(combined with a short `business_short_name`) indicates the merge was driven by name+address
matching and is likely intentional. A shared address with unrelated short names would not
trigger a merge under the new design.

### Conservative baseline (`output/qa/ownership_conservative_top_conglomerates.csv`)
Top 20 conglomerates from the one-round, no-expansion Phase 2 baseline. Comparing this to
`ownership_top_conglomerates_2015.csv` (full iterative result) reveals how much of the
merging comes from chain expansion vs. direct address sharing.

### Owner linkage QA (`output/qa/owner_linkage_qa.txt`)
Counts of corp / trust / person entity groups, short-name warnings, generic-name warnings,
and top financial intermediaries.

---

## Output Data Products

| Product (config key) | Primary key | Description |
|---|---|---|
| `owner_linkage_xwalk` | `(grantee_upper, PID)` | Phase 1: raw name → entity_id |
| `xwalk_entity_conglomerate` | `entity_id` | Entity → conglomerate; `link_method`, `consolidation_round` |
| `xwalk_pid_entity` | `(PID, year)` | Rolling ownership: entity-level, with time-varying portfolio size |
| `xwalk_pid_conglomerate` | `(PID, year)` | Rolling ownership: conglomerate-level |
| `name_lookup` | `name_raw` | All observed names → entity_id + conglomerate_id |
| `owner_portfolio` | `conglomerate_id` | Total portfolio size, `portfolio_bin`, first/last acquisition year |

### `link_method` values in `xwalk_entity_conglomerate`
- `exact_norm` — singleton; never linked to another entity
- `mailing_address` — merged via Phase 2 OPA mailing-address graph
- `rtt_transitive` — merged via Phase 3 RTT-OPA transitive edge

---

## Known Limitations

1. **Person linkage is conservative by design.** Two records with the same person name at
   different PIDs are treated as separate entities (Phase 1). Phase 2 can re-merge them if
   they share a mailing address in the OPA data, but no name-fuzzy matching is done.

2. **No RTT address data.** Transfer records do not contain mailing addresses, so RTT-only
   entities (not in OPA 2024) can only be linked transitively via Phase 3.

3. **OPA is a 2024 cross-section.** Properties that changed hands after the OPA snapshot
   will have mismatched last-RTT-grantee vs. OPA-owner, creating Phase 3 edges that may
   represent genuine ownership changes rather than entity aliases.

4. **RA threshold is a blunt instrument.** Lowering it reduces mega-conglomerates but
   also discards real shared-address signals for smaller landlords that happen to use
   the same management-company address.
