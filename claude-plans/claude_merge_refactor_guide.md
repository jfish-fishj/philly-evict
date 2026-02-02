# Philly parcels merge + address QA refactor (Guide for Claude)

This note describes a **refactor/validation pass** to make the Philly pipeline more robust when combining:

1) eviction filings  
2) rental listings  
3) address history data  
4) building complaints / permits  

onto Philly **parcels**, with goals:

- **Maximize matches** for each source (recall)
- **Minimize false positives** (precision)
- Produce **standardized QA artifacts** so it’s easy to see *why* matches fail (e.g., `fourth` vs `4th`) and iterate on parsers.

---

## 0. Summary of the change

### What to implement
1. Create shared helpers:
   - `r/lib/address_utils.R` (parsing + normalization; shared across all sources)
   - `r/lib/qa_utils.R` (merge diagnostics; exportable QA tables; sampling utilities)

2. Update cleaning scripts (`clean-*.R`) to use shared helpers and to output:
   - `addr_building_key` (building-level)  
   - `addr_unit_key` (unit-level)  
   - standardized `n_sn_ss_c`

3. Update merge scripts (e.g., `merge-evictions-rental-listings.R`) to:
   - call `merge_summary_stats()` **at every tier**
   - write QA CSVs to a consistent location
   - generate **unmatched diagnostics** (flags + suggested fixes + random samples)

---

## 1. Address normalization helpers (address_utils.R)

### Create: `r/lib/address_utils.R`

Move duplicated functions into this file (currently repeated across scripts):
- `parse_unit()`
- `parse_unit_extra()`
- `parse_range()`
- `parse_letter()`
- `ord_words_to_nums()` (if you still want word→number handling; see below)

Add new shared functions:

### 1.1 `standardize_street_name(x)`
Encapsulate all street-level normalization you use across parcels/evictions/licenses/InfoUSA.

Minimum set:
- lowercase, `str_squish`
- standardize ordinals:
  - `\b1\b` → `1st`, `\b2\b` → `2nd`, `\b3\b` → `3rd`, else `N` → `Nth`
- common Philly fixes you already have:
  - `berkeley` → `berkley`
  - `\bmt\b` → `mount`
  - `\bst\b` → `saint` (be careful: this is only for *street name tokens*; avoid clobbering suffix “st” for “street”)
  - `\bmc\b` normalization (if used)
- option: accept `mode = c("strict","philly")` and keep the extra Philly-specific rules under `"philly"`.

### 1.2 `normalize_unit(x)`
Return a stable unit token suitable for keys:
- uppercase
- strip punctuation except alphanumerics
- map variants:
  - `APARTMENT`, `APT`, `UNIT`, `#` → `APT`
  - `FL`, `FLOOR` → `FL`
  - `STE`, `SUITE` → `STE`
- keep final as e.g. `APT 3B`, `STE 120`, `FL 2`

### 1.3 `make_n_sn_ss_c(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir)`
Canonical construction of your join key:
- `tolower`, `str_squish`
- replace `NA` with `""`
- return `NA` if empty

### 1.4 `make_addr_keys(dt)`
Utility that adds:
- `addr_building_key := n_sn_ss_c`
- `addr_unit_key := paste(n_sn_ss_c, normalize_unit(pm.unit), sep = " | ")` (if unit exists)

---

## 2. QA + merge diagnostics helpers (qa_utils.R)

### Create: `r/lib/qa_utils.R`

This file should standardize the “ugly code blocks” used to summarize merges (the block in `merge-evictions-rental-listings.R` appears ~3–4 times).

### 2.1 `merge_summary_stats(dt, addr_key="n_sn_ss_c", num_pids_col="num_pids", street_col="pm.street", top_n=20)`
Returns a list of **data.tables**:
- `dist_num_pids`: distribution of `num_pids` (unique by address)
- `matched_vs_unmatched`: share unmatched (`num_pids==0`)
- `top_unmatched_streets`: top streets among unmatched
- `top_unmatched_addresses`: top address keys among unmatched

Include optional args:
- `restrict_to_unique_addr = TRUE` (default): stats computed on `unique(dt, by=addr_key)`
- `label = "tier_name"`: stored in each output table for easy stacking across tiers

### 2.2 `write_merge_summary(stats, out_dir, prefix)`
Write each table as CSV with stable names:
- `{prefix}_dist_num_pids.csv`
- `{prefix}_matched_vs_unmatched.csv`
- `{prefix}_top_unmatched_streets.csv`
- `{prefix}_top_unmatched_addresses.csv`

Use an output directory like:
- `p_out(cfg, "qa", "merge-evict-rentals")` or `p_out(cfg,"qa")/merge-evict-rentals/`

### 2.3 `sample_unmatched(dt, addr_key="n_sn_ss_c", num_pids_col="num_pids", n=200, strata=c("street","none"))`
Return a random sample of unmatched addresses suitable for inspection.

Recommended approach:
- sample at **unique address** level
- allow a `strata="street"` mode that:
  - samples a set of streets
  - then samples addresses within those streets
This helps detect systematic parser issues on certain streets / naming patterns.

Write samples to:
- `{prefix}_unmatched_sample.csv`

---

## 3. Unmatched diagnostics: flags + suggested fixes (core request)

In merge scripts, we want more than “how many unmatched”; we want **why** they’re unmatched.

### Create: `diagnose_unmatched_addresses()`

Signature idea:
```r
diagnose_unmatched_addresses <- function(
  src_addys,         # unique source addresses (e.g., philly_evict_addys)
  parcel_addys,      # unique parcel addresses (philly_parcel_addys)
  addr_key = "n_sn_ss_c",
  street_col = "pm.street",
  house_col = "pm.house",
  top_n = 50
)
```

Inputs should be **unique-by-address** tables containing parsed components (`pm.*`) and `n_sn_ss_c`.

#### 3.1 Produce “shadow keys” for hypothesis testing
For each unmatched address, compute alternate keys and test membership in `parcel_addys[[addr_key]]`:

- **Ordinal mismatch flags**  
  Goal: catch `FOURTH` vs `4TH`, `TWENTY SECOND` vs `22ND`, etc.
  - `key_ord_numeric`: convert word ordinals to numeric (`fourth`→`4th`)
  - `key_ord_word`: convert numeric ordinals to word form (optional; less necessary)
  - `flag_ordinal_fixable = key_ord_numeric %in% parcels`

- **Suffix / direction mismatch flags**
  - `key_no_predir`: drop `pm.preDir`
  - `key_no_sufdir`: drop `pm.sufDir`
  - `key_no_suffix`: drop `pm.streetSuf`
  - flags if any of these exist in parcels

- **Zip mismatch flag**
  - Construct `key_no_zip` variant (for tiers that include zip)
  - flag if `key_no_zip` exists in parcels

- **Street not in parcels**
  - `flag_street_not_in_parcels = !(pm.street %in% parcel_addys$pm.street)`

- **House number missing / malformed**
  - `flag_missing_house = is.na(pm.house) | pm.house==""`
  - `flag_house_range = str_detect(pm.house, "-")` (if you keep range in house token)

Each flag should be a boolean column.

#### 3.2 Suggested “reason” field
Add a single categorical `unmatched_reason` chosen with priority order, e.g.:

1. `street_not_in_parcels`
2. `ordinal_word_numeric_mismatch`
3. `predir_or_sufdir_mismatch`
4. `suffix_mismatch`
5. `zip_mismatch`
6. `missing_house`
7. `other`

This gives immediate signal like “a lot of failures are ordinals”.

#### 3.3 Output: counts + top examples
Write:
- `unmatched_reason_counts.csv`
- `unmatched_fixable_examples.csv` (top N by each reason)
- `unmatched_street_counts.csv` (top streets among unmatched, plus flags share)

#### 3.4 Output: random unmatched sample
Write:
- `unmatched_random_sample.csv` including:
  - raw address (if you have it)
  - parsed components
  - `n_sn_ss_c`
  - flags + `unmatched_reason`
  - any “shadow keys” that *would* match parcels

This directly supports your workflow of eyeballing whether failures are “normal” or parser-related.

---

## 4. How to modify `merge-evictions-rental-listings.R` (concrete edits)

### 4.1 At top: source utilities
Add:
```r
source("r/lib/address_utils.R")
source("r/lib/qa_utils.R")
```

### 4.2 After each tier merge, replace ad hoc blocks
Right after each tier block where you compute:
```r
setDT(tier_dt)
tier_dt[, num_pids := uniqueN(PID, na.rm=TRUE), by=n_sn_ss_c]
```

Add:
```r
stats <- merge_summary_stats(tier_dt, addr_key="n_sn_ss_c", num_pids_col="num_pids", street_col="pm.street")
write_merge_summary(stats, out_dir = p_out(cfg,"qa","merge-evict-rentals"), prefix = paste0("tier_", unique(tier_dt$merge)))
```

This standardizes your repeated diagnostics blocks.

### 4.3 Diagnose unmatched after the final tier set
After `matched_ids` is built (or after `xwalk` is built), construct:

- `src_unique <- unique(philly_evict_addys, by="n_sn_ss_c")`
- `parc_unique <- unique(philly_parcel_addys, by="n_sn_ss_c")`

Then:
```r
unmatched <- src_unique[!n_sn_ss_c %in% xwalk_unique$n_sn_ss_c & !n_sn_ss_c %in% xwalk_non_unique$n_sn_ss_c]
diag <- diagnose_unmatched_addresses(unmatched, parc_unique)
fwrite(diag$reason_counts, p_out(cfg,"qa","merge-evict-rentals","unmatched_reason_counts.csv"))
fwrite(diag$examples,       p_out(cfg,"qa","merge-evict-rentals","unmatched_fixable_examples.csv"))
fwrite(diag$sample,         p_out(cfg,"qa","merge-evict-rentals","unmatched_random_sample.csv"))
```

### 4.4 (Optional but recommended) “fixable key” rerun hook
If a big share of unmatched are `ordinal_word_numeric_mismatch`, add a controlled second pass:

- Create `n_sn_ss_c_alt` for *only* unmatched addresses using ordinal normalization
- attempt the strictest tier merge on `n_sn_ss_c_alt`
- **only accept** if the resulting PID match is unique

This boosts recall without compromising precision, and is completely auditable.

---

## 5. Notes on correctness issues in the current merge script (to address while refactoring)

### 5.1 Lon/lat convention
In `merge-evictions-rental-listings.R` you set:
```r
mutate(geocode_y = lng, geocode_x = lat)
```
This is a common footgun: in `sf`, `coords = c(x,y)` expects `x=lon`, `y=lat`.

Recommended convention everywhere:
- store as `lon`, `lat` (or `longitude`, `latitude`)
- when calling `st_as_sf(coords=c("longitude","latitude"), crs=4326)`

Do **not** `st_set_crs(st_crs(parcel_sf_subset))` on raw lon/lat; explicitly set `4326` first, then transform.

---

## 6. Deliverables Claude should include in the PR

1) New files:
- `r/lib/address_utils.R`
- `r/lib/qa_utils.R`

2) Modified scripts:
- all `clean-*.R` scripts use shared normalization + output `addr_building_key` / `addr_unit_key`
- merge scripts call `merge_summary_stats()` and export QA CSVs

3) New QA outputs (written each run):
- tier summaries (`dist_num_pids`, etc.)
- unmatched diagnostics (`unmatched_reason_counts`, `unmatched_fixable_examples`, `unmatched_random_sample`)

4) Smoke tests / assertions:
- `validate_address_table()` called after each clean script
- merge scripts log tier match rates and confirm non-overlap of matched IDs across tiers (as you already do with `sum(matched_* %in% matched_*)`)

---

## 7. What this gives you

- A stable, reusable address-cleaning layer (no drift between sources)
- Merge QA artifacts that make it obvious *why* addresses failed to merge
- A fast iteration loop: change parser → rerun → see reason counts + examples + random sample
- Higher match rates without silently increasing false positives (unique-only acceptance + auditable fix flags)

