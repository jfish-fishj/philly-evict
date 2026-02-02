# Oracle-based address canonicalization (Step 5b) — Implementation guide for Claude

## Goal
Increase parcel match rates **without increasing false positives** by adding a **post-parse canonicalization step** (“Step 5b”) to each address-cleaning pipeline. The step:

1. standardizes `pm.street` (e.g., MLK/JFK/MT)  
2. normalizes + repairs `pm.streetSuf`  
3. repairs “stuck directionals” (e.g., `echurch`)  
4. applies “boulevard precedence” (boulevard wins even if suffix parsed differently)  
5. validates all modifications against a **parcel oracle**: `(pm.street, pm.streetSuf)` must exist in parcel address universe

This pattern was implemented for **evictions** and should be replicated across other sources (Altos/listings, licenses/permits/complaints, InfoUSA/address history), with **source-specific alias files** and **tight validation**.

---

## Inputs and config
### Required
- `inputs.philly_parcel_address_counts`  
  Path to parcel street×suffix counts CSV with columns: `pm.street`, `pm.streetSuf`, `N`.

### Recommended (source-specific)
- `inputs.street_suffix_aliases_<source>`  
  Path to suffix alias mapping CSV with columns: `from,to,note`.

Examples:
```yaml
inputs:
  philly_parcel_address_counts: "clean/philly-parcel-address-counts.csv"
  street_suffix_aliases_evictions: "config/aliases/evictions_suffix_aliases.csv"
  street_suffix_aliases_altos: "config/aliases/altos_suffix_aliases.csv"
  street_suffix_aliases_licenses: "config/aliases/licenses_suffix_aliases.csv"
  street_suffix_aliases_infousa: "config/aliases/infousa_suffix_aliases.csv"
```

**Why source-specific?**  
Evictions often contain unique noise (instruction text, truncation, glued tokens). Alias files should be derived from QA for each source.

---

## What we implemented for evictions (to replicate)
### Where Step 5b runs
Run Step 5b **after postmastr parsing** (so `pm.*` columns exist) and **before** you build merge keys (`n_sn_ss_c`, etc.) and run merge tiers.

### Step 5b responsibilities
1. **Normalize parsed columns**
   - Lowercase/squish: `pm.preDir`, `pm.sufDir`, `pm.street`, `pm.streetSuf`.
2. **Street name standardization (source-agnostic helper)**
   - MLK variants → `martin luther king`
   - JFK variants → `john f kennedy`
   - `mount` → `mt`
   - `saint` → `st` (street-name token; not the suffix field)
   - common misspellings: e.g., `boulvard` → `boulevard`
3. **Suffix alias normalization (source-specific config)**
   - Normalize known variants: `blv/boul/boulevard/...` → `blvd`, `av/avenue` → `ave`, etc.
4. **Oracle-validated fixes**  
   Each fix only commits if the resulting `(pm.street, pm.streetSuf)` exists in the **parcel oracle**.

#### Fix rule sequence (evictions v2)
- **Boulevard precedence (override allowed)**  
  If `pm.street` ends with `blv/blvd/boulevard/boulv/...`:
  - remove that token from `pm.street`
  - set `pm.streetSuf := "blvd"`
  - **commit only if** `(street_base, blvd)` exists in parcel oracle  
  Rationale: boulevard token in the street string should “win” even if suffix was parsed as `rd/st`.

- **Stuck directional by street membership** (`echurch → e + church`)  
  If:
  - `pm.preDir` empty  
  - `pm.street` begins with `^[nsew]`  
  - `pm.street` **not** in parcel street list  
  - remainder street **is** in parcel street list  
  - and `(remainder, pm.streetSuf)` exists in parcel oracle  
  → set `pm.preDir` and update `pm.street`.

- **Known parse artifact repair** (`est/rst → st`)  
  For `pm.streetSuf ∈ {est,rst}`:
  - reattach leading letter to street: `spruc + est → spruce + st`
  - commit only if oracle validates `(new_street, st)`.

- **Split trailing suffix token from street** (token extraction)  
  If `pm.street` ends with a suffix-like token:
  - treat last token as candidate suffix (apply alias map)
  - base street = `pm.street` without token
  - commit only if oracle validates `(base, cand_suffix)`  
  Note: this is powerful; keep it oracle-validated and consider enabling/disabling per source depending on QA.

5. **Audit fields + QA outputs**
   - Add fields: `fix_tag`, `fix_changed`, `fix_parcelN`
   - Output:
     - `fix_tag_counts.csv`
     - `oracle_valid_rate_before_after.csv`
     - `changed_rows_sample.csv`  
   under `p_out(cfg, "qa", "<source>_address_canonicalize")`.

---

## Make Step 5b reusable
### Preferred approach
Implement a single helper function in `address_utils.R` and call it from each cleaning script:

```r
dt <- canonicalize_parsed_addresses(
  dt,
  cfg,
  source = "evictions",        # or altos/licenses/infousa
  oracle_city = "philly",
  export_qa = TRUE
)
```

### Function inputs
- `dt`: a `data.table` with parsed `pm.*` columns
- `cfg`: provides:
  - parcel oracle path
  - suffix alias path for the given `source`

### Function outputs
- updated `dt` (canonicalized `pm.*`)
- fix metadata fields (for QA and debugging)

---

## Managing alias files
### Keep suffix aliases in config, not in code
Suffix alias lists are generated from QA and evolve as you find new artifacts. Storing them in config:
- avoids code churn
- makes changes auditable
- allows different alias sets per source or city

### Keep stable street-name rules in code
Street-name standardization (MLK/JFK/MT/SAINT, etc.) is stable and should be applied everywhere the same way. Put this in helper code (`standardize_street_name_vec()`).

### Add a QA workflow to generate alias candidates
For each source, after a run:
1. produce a table of suffix tokens associated with poor match rates
2. export a candidate mapping table (e.g., `qa/<source>/suffix_alias_candidates.csv`)
3. manually promote entries into `config/aliases/<source>_suffix_aliases.csv`

---

## Source-specific notes
### Evictions
- messy strings; parser artifacts common
- Step 5b repairs provide meaningful gains
- alias mapping is often necessary

### Rental listings (Altos/MLS)
- addresses often cleaner
- bigger issue may be **matching to the correct building** (multi-unit complexes)
- Step 5b still helps, but also improve merge tie-breaking/candidate scoring downstream

### Licenses / permits / complaints
- typically clean but may include “rear/front/unit/corner” phrases
- keep Step 5b conservative; boulevard precedence + directional unstick are generally safe
- enable split-trailing-token rule only if QA shows benefit

### InfoUSA / address history
- more PO boxes, suites, commercial patterns
- consider filtering non-parcel-mappable strings early
- Step 5b helps, but strong screening is often required

---

## Practical checklist per script
1. Ensure parsed fields exist (`pm.street`, `pm.streetSuf`, `pm.preDir`, `pm.sufDir`, `pm.house`)
2. Load parcel oracle once per run (street×suffix counts + street list)
3. Load suffix alias map for this `source` (if provided)
4. Standardize `pm.street` via shared helper
5. Normalize `pm.streetSuf` via alias helper
6. Run fix rules in order (boulevard precedence → directional unstick → artifact repairs → token split), always oracle-validated
7. Export QA summaries + samples
8. Proceed to merge keys and tiered merges

---

## Safety principle
Any transformation that changes `pm.street` or `pm.streetSuf` must be **validated** against the parcel oracle. This is what prevents recall improvements from turning into false positives.
