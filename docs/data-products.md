# Data Products Codebook

Reference for all major data products in the pipeline. Each entry documents the config key, producing script, primary key, row count, and column descriptions. Column descriptions are organized by category, with notes on which analysis scripts consume each product.

> **Audience**: LLMs, agents, and humans working with this codebase.

---

## Table of Contents

1. [bldg_panel_blp](#bldg_panel_blp) — Full building panel for BLP estimation
2. [analytic_sample](#analytic_sample) — Filtered estimation sample
3. [infousa_race_imputed_person](#infousa_race_imputed_person) — InfoUSA person-level race imputation
4. [infousa_building_race_panel](#infousa_building_race_panel) — Building-level racial composition
5. [race_imputed_person_sample](#race_imputed_person_sample) — Eviction defendant race (person-level)
6. [race_imputed_case_sample](#race_imputed_case_sample) — Eviction defendant race (case-level)
7. [gender_imputed_case_sample](#gender_imputed_case_sample) — Eviction defendant gender (case-level)
8. [parcel_occupancy_panel](#parcel_occupancy_panel) — Parcel occupancy panel
9. [ever_rentals_panel](#ever_rentals_panel) — Rental activity panel
10. [nhpd_clean](#nhpd_clean) — Cleaned National Housing Preservation Database properties
11. [nhpd_address_agg / nhpd_parcel_xwalk / nhpd_parcel_xwalk_property](#nhpd_xwalk_products) — NHPD-to-parcel crosswalk products
12. [altos_pid_year_bedbin](#altos_pid_year_bedbin) — Altos PID-year-bedroom-bin cell panel
13. [building_data](#building_data) — Building permits, violations, complaints
14. [evictions_clean](#evictions_clean) — Cleaned eviction case data
15. [evict_address_xwalk / evict_address_xwalk_case](#evict_address_xwalk) — Eviction-to-parcel crosswalks
16. [evict_infousa_hh_matches / evict_infousa_hh_unmatched / evict_infousa_candidate_stats](#evict_infousa_household_linkage) — Eviction case-to-InfoUSA household linkage products
17. [bg_renter_poverty_share / tract_renter_poverty_share](#renter_poverty_geography_products) — Geography-level renter poverty products
18. [infousa_building_income_proxy_panel](#infousa_building_income_proxy_panel) — PID-year InfoUSA low-income proxy
19. [outer_sorting_empirical_moments](#outer_sorting_empirical_moments) — Empirical outer-sorting calibration target moments
20. [outer_sorting_sim_panel / outer_sorting_sim_moments / outer_sorting_sim_diagnostics](#outer_sorting_simulation_products) — Simulated outer-sorting baseline products

---

## bldg_panel_blp

| Field | Value |
|-------|-------|
| **Config key** | `products.bldg_panel_blp` |
| **Path** | `data/processed/analytic/bldg_panel_blp.csv` |
| **Producing script** | `r/make-analytic-sample.R` |
| **Primary key** | `PID` x `year` |
| **Rows** | 3,560,446 |
| **Columns** | 274 |
| **Years** | 2006-2024 |

The main analysis panel. Merges parcel occupancy, rental activity, eviction filings, building characteristics, assessments, and BLP market share instruments. Every effective-rental parcel-year in the occupancy panel, including parcels identified through NHPD and owner-based PHA rules and excluding university-owned housing from the effective rental universe.

### Consumed by

- `r/price-regs-eb.R` — filing rate regressions
- `r/retaliatory-evictions.r` — distributed lag + LP-DiD (joins on PID)
- `r/retaliatory-evictions-race.R` / `r/retaliatory-evictions-gender.R` — demographic analysis
- `python/pyblp_estimation.py` — BLP nested logit demand estimation

### Identifiers

| Column | Type | Description |
|--------|------|-------------|
| `PID` | character | Parcel ID (OPA parcel_number). Primary key part 1. |
| `year` | integer | Calendar year. Primary key part 2. |
| `GEOID` | character | Census block group GEOID (12-digit: state+county+tract+BG). From spatial join of parcel centroid to BG shapefile. |
| `market_id` | character | `{zip}_{year}_{num_units_bin}` — zip x year x unit-bin market for within-bin shares. |
| `market_id_zip_year` | character | `{zip}_{year}` — zip x year market for BLP estimation. |

### Parcel characteristics

| Column | Type | Description |
|--------|------|-------------|
| `owner_1` | character | Primary owner name (from `parcels_clean`). |
| `pm.zip` | character | Mailing ZIP, underscore-prefixed (e.g., `_19104`). Coalesced: ever_rentals_panel -> parcels_clean fallback. |
| `building_code` | character | OPA building code (e.g., `O50`, `M10`). |
| `building_code_description` | character | OPA building code description. |
| `building_code_description_new` | character | Standardized building type description (from `standardize_building_type()`). |
| `building_code_description_new_fixed` | character | Further-cleaned building type. Priority for filtering: `_new_fixed` > `_new` > `building_type`. |
| `building_type` | character | Coded building type (e.g., `MIDRISE_MULTI`, `ROW`). NOT descriptive — use `building_code_description_new` for labels. |
| `is_condo` | logical | Condo flag from building type standardization. |
| `category_code` | integer | OPA category code. |
| `category_code_description` | character | OPA category description. |
| `total_area` | numeric | Total building area (sq ft). |
| `total_livable_area` | numeric | Livable area (sq ft). |
| `number_of_bedrooms` | integer | Bedrooms per unit (OPA). |
| `number_of_bathrooms` | integer | Bathrooms per unit (OPA). |
| `number_stories` | numeric | Number of stories. |
| `num_stories_bin` | character | Binned stories (e.g., `1`, `2`, `3+`). |
| `quality_grade` | character | OPA quality grade (A+ through F). |
| `year_built` | integer | Year built (OPA). |
| `year_blt_decade` | character | Year built binned by decade. |
| `source` | character | Rental evidence source flags. |
| `structure_bin` | character | Structure type bin (e.g., `u_1_detached`, `u_2`, `u_3_4`, `u_5_9`, `u_10_19`, `u_20_49`, `u_50_plus`). |

### Occupancy & units

| Column | Type | Description |
|--------|------|-------------|
| `total_units` | numeric | Imputed total housing units (capped at 1.5x max_households). Central unit measure for BLP. |
| `renter_occ` | numeric | Estimated renter-occupied units. LOCF/NOCB interpolated, BG fallback for PIDs with no InfoUSA. |
| `occupancy_rate` | numeric | `renter_occ / total_units`. |
| `rental_occupancy_rate_scaled` | numeric | Occupancy rate scaled to 0.9 mean in 2010 per structure bin. |
| `occupied_units` | numeric | `pmin(total_units, pmax(0, renter_occ))`. Used for share computation. |
| `num_units_bin` | character | Unit count bin (e.g., `1`, `2`, `3-4`, `5-9`, `10-19`, `20-49`, `50+`). Used for nesting in BLP. |

### Rent & listings

| Column | Type | Description |
|--------|------|-------------|
| `med_rent_altos` | numeric | Legacy Altos PID-year rent alias (Phase 1 points to `med_rent_altos_raw`). |
| `med_rent_altos_raw` | numeric | Phase 1 raw Altos PID-year rent (listing-row median from `altos_year_building`). `med_rent_altos` is currently an alias for backward compatibility. |
| `med_rent_altos_std_fixed` | numeric | Phase 1 composition-robust Altos PID-year rent using fixed bedroom-bin weights by parcel `building_type` class, renormalized over observed bins. Computed whenever at least one bedroom bin is observed; coverage is tracked diagnostically via `altos_share_weight_observed`. |
| `med_rent_eviction` | numeric | Alias for `med_eviction_rent` in Phase 2 rent harmonization code. |
| `med_rent_eviction_std` | numeric | Eviction claimed rent calibrated onto the Altos standardized scale via a median log-ratio bridge (zip-year fallback hierarchy). |
| `med_rent` | numeric | Unified rent contract (Phase 2): priority = `med_rent_altos_std_fixed` -> `med_rent_altos_raw` -> `med_rent_eviction_std` -> `med_rent_eviction`. |
| `log_med_rent` | numeric | `log(med_rent)`. Main price variable after Phase 2 unified rent construction. |
| `n_altos_listings` | integer | Number of Altos listing rows at PID in year (Phase 1 semantic fix; previously a bedroom-cell count in `make-rent-panel.R`). |
| `n_altos_listing_rows` | integer | Explicit Altos listing-row count at PID in year (same value as `n_altos_listings` in Phase 1). |
| `n_altos_cells` | integer | Number of observed Altos bedroom bins (`studio`, `1br`, `2br`, `3plus`) in PID-year. |
| `altos_share_weight_observed` | numeric | Sum of fixed bedroom-bin weights observed in PID-year (0-1). Phase 1 diagnostic coverage metric for standardized Altos levels (not a hard inclusion rule for `med_rent_altos_std_fixed`). |
| `altos_overlap_weight_prev` | numeric | Sum of fixed bedroom-bin weights observed in both year `t` and `t-1` for PID. Coverage diagnostic for overlap-based Altos rent growth. |
| `altos_cell_overlap_prev` | integer | Count of bedroom bins overlapping between year `t` and `t-1` for PID. |
| `dlog_rent_altos_overlap` | numeric | Phase 1 composition-robust Altos YoY log rent change using only bedroom bins observed in both `t` and `t-1`, weighted by fixed bedroom shares (requires minimum overlap coverage). |
| `dlog_rent_altos_raw_yoy` | numeric | Diagnostic-only raw Altos YoY log change from `med_rent_altos_raw`; composition-sensitive. |
| `share_altos_rows_with_bed_bin` | numeric | Share of Altos listing rows in PID-year that have a mappable bedroom bin (0-1). |
| `evict_bridge_adj_log` | numeric | Log adjustment applied to `med_rent_eviction` to create `med_rent_eviction_std` (bridge level chosen by fallback hierarchy). |
| `evict_bridge_n` | integer | Number of overlap observations used at the selected bridge level. |
| `evict_bridge_level_used` | character | Bridge fallback level used for eviction standardization: `zip_year`, `zip`, `city_year`, `city`, `pooled`, or `none`. |
| `rent_source` | character | Source chosen for unified `med_rent`: `altos_std_fixed`, `altos_raw`, `eviction_std`, `eviction`, or `missing`. |
| `rent_source_std` | logical | TRUE when `rent_source` is standardized (`altos_std_fixed` or `eviction_std`). |
| `rent_source_switch_prev` | logical | TRUE if unified rent source changes relative to the prior adjacent year (`t-1`) for the same PID. |
| `year_gap_rent_prev` | integer | Gap to prior observed PID-year row used for source-change and YoY diagnostics. |
| `year_gap_rent_prev_same_source` | integer | Gap to the prior observed PID-year row with the same `rent_source` (used for annualized safe rent changes). |
| `dlog_med_rent_safe` | numeric | Source-safe adjacent-year log rent change. Refuses source mixing and uses `dlog_rent_altos_overlap` for `altos_std_fixed`. Preferred rent-change measure. |
| `dlog_med_rent_safe_ann` | numeric | Source-safe annualized log rent change using prior same-source endpoint. For adjacent `altos_std_fixed` rows that pass overlap checks, equals `dlog_rent_altos_overlap`; for non-adjacent gaps, uses endpoint levels divided by gap. |
| `dlog_med_rent_naive` | numeric | Diagnostic adjacent-year log change from unified `med_rent` without source protections. Not recommended for analysis. |

### Eviction filings

| Column | Type | Description |
|--------|------|-------------|
| `num_filings` | integer | Total eviction filings at PID in year. |
| `num_filings_with_ha` | integer | Filings with housing authority involvement. |
| `filing_rate` | numeric | `num_filings / total_units`. |
| `filing_rate_eb` | numeric | Empirical Bayes shrunk filing rate (city prior). |
| `filing_rate_eb_zip` | numeric | EB filing rate (ZIP prior). |
| `filing_rate_eb_pre_covid` | numeric | EB filing rate pooled across pre-COVID years (city prior). Key analysis variable. |
| `filing_rate_eb_zip_pre_covid` | numeric | EB filing rate pooled pre-COVID (ZIP prior). |
| `filing_rate_eb_city_pre_covid` | numeric | Alias; same as `filing_rate_eb_pre_covid`. |
| `filing_rate_eb_asinh` | numeric | `asinh(filing_rate_eb)`. |
| `filing_rate_longrun` | numeric | Long-run mean filing rate (all years). |
| `filing_rate_longrun_pre2019` | numeric | Long-run mean filing rate (pre-2019). |
| `total_filings_pre2019` | integer | Total filings at PID before 2019. |

### Market shares (BLP)

| Column | Type | Description |
|--------|------|-------------|
| `share_zip` | numeric | `occupied_units / sum(total_units)` within zip-year. BLP-ready; sums <= 1. Vacancy = outside good. |
| `share_zip_all_bins` | numeric | Alias for `share_zip`. Used by `pyblp_estimation.py` in `zip_all_bins` share mode. |
| `share_zip_bin` | numeric | Same as `share_zip` but within zip-year-bin. |
| `market_share_units` | numeric | `occupied_units / sum(occupied_units)` within zip-year. Sums to 1. NOT for BLP — legacy. |
| `share_units_zip_unit` | numeric | Same concept, different naming. Legacy. |
| `hhi_zip_year` | numeric | Herfindahl index by owner within zip-year. |

### BLP instruments

All instruments computed within `market_id_zip_year` (zip x year) by `make_blp_instruments()` in `make-analytic-sample.R`. For each continuous variable `v` below, three instrument columns are created: `z_sum_others_v` (sum at other-owner products), `z_sum_samefirm_v` (sum at same-owner other products), `z_sum_otherfirm_v` (sum at other-owner products — equivalent to `z_sum_others_v`). Count variables also produce `z_cnt_market`, `z_cnt_firm`, `z_cnt_others`, `z_cnt_samefirm`, `z_cnt_otherfirm`.

Continuous variables used (`blp_cont_vars` in `make-analytic-sample.R`):

| Variable | Description |
|----------|-------------|
| `total_units` | Total housing units. |
| `log_med_rent` | Log unified rent. |
| `hazardous_violation_count` | Hazardous code violations. |
| `total_violations` | Total code violations. |
| `total_severe_violations` | Severe violations (HAZARDOUS or IMMINENTLY DANGEROUS). |
| `total_investigations` | Total L&I case investigations. |
| `total_severe_investigations` | Severe investigations (`investigationtype`-based; pre-2020 only — see regime note in `building_data`). |
| `building_permit_count` | Building permits. |
| `general_permit_count` | General permits. |
| `mechanical_permit_count` | Mechanical permits. |
| `zoning_permit_count` | Zoning permits. |
| `plumbing_permit_count` | Plumbing permits. |
| `taxable_value` | OPA taxable value. |
| `change_taxable_value` | Year-over-year change in taxable value. |
| `log_taxable_value` | Log taxable value. |
| `change_taxable_value_per_unit` | Per-unit change in taxable value. |
| `log_taxable_value_per_unit` | Log per-unit taxable value. |

### Assessments

| Column | Type | Description |
|--------|------|-------------|
| `market_value` | numeric | OPA assessed market value. |
| `taxable_land` | numeric | Assessed taxable land value. |
| `taxable_building` | numeric | Assessed taxable building value. |
| `exempt_land` | numeric | Exempt land value. |
| `exempt_building` | numeric | Exempt building value. |
| `log_market_value` | numeric | `log(market_value)`. |
| `change_log_taxable_value` | numeric | Year-over-year change in log taxable value. Used as potential IV (weak under FE). |

### Violations, complaints & investigations (from building_data merge)

| Column | Type | Description |
|--------|------|-------------|
| `total_permits` | integer | Total permits at PID in year. |
| `building_permit_count` | integer | Building permits. |
| `electrical_permit_count` | integer | Electrical permits. |
| `plumbing_permit_count` | integer | Plumbing permits. |
| `total_complaints` | integer | Total L&I complaints at PID in year. |
| `total_severe_complaints` | integer | Severe complaints (Heat, Fire, Drainage, Property Maintenance). |
| `heat_complaint_count` | integer | Heat-related complaints. |
| `fire_complaint_count` | integer | Fire-related complaints. |
| `drainage_complaint_count` | integer | Drainage/plumbing complaints. |
| `structural_deficiency_complaint_count` | integer | Structural deficiency complaints. |
| `complaint_*_count` | integer | Other individual complaint type counts. |
| `total_violations` | integer | Total code violations at PID in year. |
| `total_severe_violations` | integer | Severe violations (`caseprioritydesc` ∈ HAZARDOUS, IMMINENTLY DANGEROUS). |
| `hazardous_violation_count` | integer | Hazardous violations. |
| `imminently_dangerous_violation_count` | integer | Imminently dangerous violations. |
| `violation_*_count` | integer | Other individual violation type counts. |
| `total_investigations` | integer | Total L&I case investigations at PID in year. |
| `total_severe_investigations` | integer | Severe investigations (`investigationtype`-based; pre-2020 reliable only — see `building_data` regime note). |
| `investigation_*_count` | integer | Individual investigation type counts. |

### Rental flags

| Column | Type | Description |
|--------|------|-------------|
| `rental_from_altos` | logical | PID appears in Altos listings. |
| `rental_from_license` | logical | PID has rental license. |
| `rental_from_evict` | logical | PID has eviction filing. |
| `rental_from_nhpd` | logical | PID is identified as rental in the current year through a uniquely matched NHPD property. |
| `rental_from_owner` | logical | PID is pulled into the effective rental universe via owner-name rules (currently PHA owner matching). |
| `ever_rental_nhpd` | logical | PID has any uniquely matched NHPD rental evidence across years. |
| `nhpd_property_count` | integer | Number of uniquely matched NHPD properties on the parcel. |
| `nhpd_total_units` | numeric | Sum of NHPD-reported units across uniquely matched properties on the parcel. |
| `is_pha_owner` | logical | Parcel owner matches the PHA owner-name regex. |
| `is_university_owned` | logical | Parcel owner matches the curated university-owner regex. |
| `include_in_rental_universe` | logical | Final effective-rental-universe flag after adding PHA parcels and excluding university-owned housing. |
| `ever_rental_any_year` | logical | Any effective rental evidence in any year. |
| `intensity_*` | numeric | Rental intensity scores by source and period. |

---

## analytic_sample

| Field | Value |
|-------|-------|
| **Config key** | `products.analytic_sample` |
| **Path** | `data/processed/analytic/analytic_sample.csv` |
| **Producing script** | `r/make-analytic-sample.R` |
| **Primary key** | `PID` x `year` |
| **Rows** | 303,964 |
| **Columns** | 274 (same as bldg_panel_blp) |

Subset of `bldg_panel_blp` filtered to rows with non-missing rent and units, plus market quality filters. This is the estimation sample for BLP and price regulation regressions.

### Filter criteria (from make-analytic-sample.R)

- `!is.na(log_med_rent)` — must have a usable rent measure
- `!is.na(market_share_units)` — must have a valid within-market occupied-unit share
- `!is.na(total_units)` — must have units
- Market has `>= 5` products and `>= 2` distinct owners within `market_id`

### Consumed by

- `python/pyblp_estimation.py` — primary estimation sample
- `r/price-regs-eb.R` — filing rate regressions
- `r/retaliatory-evictions.r` — joins for parcel characteristics

---

## infousa_race_imputed_person

| Field | Value |
|-------|-------|
| **Config key** | `products.infousa_race_imputed_person` |
| **Path** | `data/processed/analytic/infousa_race_imputed_person.csv` |
| **Producing script** | `r/impute-race-infousa.R` |
| **Primary key** | `familyid` x `person_num` x `year` |
| **Rows** | ~141,295 (sample_mode) |
| **Columns** | 29 |

Person-level race imputation for InfoUSA household members using WRU BISG (Bayesian Improved Surname Geocoding). Each row is one person in one household-year.

### Consumed by

- Aggregated to `infousa_building_race_panel` for building-level racial composition
- Potential direct use in neighborhood composition analysis

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `familyid` | character | InfoUSA family/household ID. |
| `family_id` | character | Alias for familyid. |
| `locationid` | character | InfoUSA location ID. |
| `individual_id` | character | InfoUSA individual ID. |
| `person_num` | integer | Person number within household (1-5). |
| `head_of_household` | logical | TRUE if person_num == 1. |
| `year` | integer | Calendar year. |
| `PID` | character | Linked parcel ID (via n_sn_ss_c -> xwalk_infousa_to_parcel). NA if unlinked. |
| `bg_geoid` | character | Block group GEOID used for imputation. Coalesced: InfoUSA census cols (primary) -> parcel-linked (fallback). |
| `geo_source` | character | Source of bg_geoid: `"parcel_linked"`, `"infousa_census"`, or `"missing"`. |
| `first_name` | character | First name. |
| `last_name` | character | Last name (surname). |
| `middle_name` | character | Middle name/initial. |
| `ethnicity_code` | character | InfoUSA vendor ethnicity code (2-char, e.g., `A7`=Chinese, `VN`=Vietnamese, `W4`=British). |
| `gender` | character | InfoUSA gender field. |
| `age` | integer | Age or age range. |
| `p_white` | numeric | Posterior probability of White. |
| `p_black` | numeric | Posterior probability of Black. |
| `p_hispanic` | numeric | Posterior probability of Hispanic. |
| `p_asian` | numeric | Posterior probability of Asian. |
| `p_other` | numeric | Posterior probability of Other race. |
| `race_hat` | character | Argmax predicted race (`white`, `black`, `hispanic`, `asian`, `other`). |
| `race_hat_p` | numeric | Probability of predicted race (confidence). |
| `race_entropy` | numeric | Shannon entropy of race probabilities (higher = more uncertain). |
| `race_impute_method` | character | `"wru_firstname+surname+block_group"` or `"wru_surname+block_group"`. |
| `race_impute_status` | character | `"ok"`, `"name_lookup_failed"`, or `"no_geoid"`. |
| `in_eviction_data` | logical | PID appears in eviction address crosswalk. |
| `is_rental_pid` | logical | PID appears in ever_rentals_panel. |
| `impute_status` | character | Overall status: `"ok"` or `"no_geoid"`. |

---

## infousa_building_race_panel

| Field | Value |
|-------|-------|
| **Config key** | `products.infousa_building_race_panel` |
| **Path** | `data/processed/analytic/infousa_building_race_panel.csv` |
| **Producing script** | `r/impute-race-infousa.R` |
| **Primary key** | `PID` x `year` |
| **Rows** | ~74,612 |
| **Columns** | 12 |

Building-level racial composition aggregated from person-level WRU predictions. Probability-weighted (uses posterior probabilities, not argmax).

### Consumed by

- Potential use in neighborhood segregation analysis
- Can be joined to `bldg_panel_blp` on PID x year

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `PID` | character | Parcel ID. |
| `year` | integer | Calendar year. |
| `num_households` | integer | Number of distinct households at PID in year. |
| `num_persons` | integer | Number of persons with race predictions. |
| `pct_white` | numeric | Probability-weighted share White. |
| `pct_black` | numeric | Probability-weighted share Black. |
| `pct_hispanic` | numeric | Probability-weighted share Hispanic. |
| `pct_asian` | numeric | Probability-weighted share Asian. |
| `pct_other` | numeric | Probability-weighted share Other. |
| `mean_entropy` | numeric | Mean race prediction entropy across persons (uncertainty measure). |
| `n_head_of_hh` | integer | Number of heads of household. |
| `bg_geoid` | character | Block group GEOID (modal across persons at PID-year). |

---

## race_imputed_person_sample

| Field | Value |
|-------|-------|
| **Config key** | N/A (written to `output/qa/`) |
| **Path** | `output/qa/race_imputed_person_sample.csv` |
| **Producing script** | `r/impute-race.R` |
| **Primary key** | `person_row_id` (or `id` x `party_type` x `person_num`) |
| **Rows** | ~915,524 |
| **Columns** | 51 |

Person-level race imputation for eviction defendants/plaintiffs. Each row is one parsed name from a court filing.

### Key columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | character | Eviction case docket number. |
| `party_type` | character | `"defendant"` or `"plaintiff"`. |
| `person_num` | integer | Person number within party list. |
| `first_name`, `last_name` | character | Parsed name components. |
| `is_entity` | logical | TRUE if name is a business/entity (not imputable). |
| `p_white`..`p_other` | numeric | WRU posterior race probabilities. |
| `race_hat` | character | Argmax predicted race. |
| `race_hat_p` | numeric | Confidence of prediction. |
| `race_entropy` | numeric | Shannon entropy. |
| `race_impute_method` | character | WRU method used. |
| `race_impute_status` | character | `"ok"`, `"is_entity"`, `"name_lookup_failed"`, etc. |
| `bg_geoid` | character | Block group used for BISG. |
| `geo_source` | character | How GEOID was obtained. |
| `lodermeier_*` | various | Lodermeier case classification fields (corporate landlord indicators). |

---

## race_imputed_case_sample

| Field | Value |
|-------|-------|
| **Config key** | N/A (written to `output/qa/`) |
| **Path** | `output/qa/race_imputed_case_sample.csv` |
| **Producing script** | `r/impute-race.R` |
| **Primary key** | `id` (case docket number) |
| **Rows** | ~403,262 |
| **Columns** | 29 |

Case-level race aggregation. Defendant race probabilities averaged across persons within each case.

### Consumed by

- `r/retaliatory-evictions-race.R` — racial disparity analysis

### Key columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | character | Case docket number. Primary key. |
| `case_impute_status` | character | `"ok"`, `"all_entities"`, `"no_imputable_defendants"`, etc. |
| `case_p_white`..`case_p_other` | numeric | Mean defendant race probabilities. |
| `case_race_hat` | character | Argmax case-level race. |
| `case_race_group_l80` | character | Race group at 80% threshold (`"black"`, `"white"`, `"hispanic"`, `"mixed"`). Used in regressions. |
| `case_race_group_l60` | character | Race group at 60% threshold. |
| `n_defendants` | integer | Number of defendant persons on case. |
| `n_imputed_defendants` | integer | Number successfully imputed. |
| `mean_defendant_entropy` | numeric | Mean entropy across defendants. |

---

## gender_imputed_case_sample

| Field | Value |
|-------|-------|
| **Config key** | N/A (written to `output/qa/`) |
| **Path** | `output/qa/gender_imputed_case_sample.csv` |
| **Producing script** | `r/impute-gender.R` |
| **Primary key** | `id` (case docket number) |
| **Rows** | ~403,262 |
| **Columns** | 12 |

Case-level gender aggregation from SSA baby name gender imputation.

### Consumed by

- `r/retaliatory-evictions-gender.R` — gender disparity analysis

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | character | Case docket number. Primary key. |
| `case_gender_impute_status` | character | `"ok"`, `"all_entities"`, etc. |
| `case_p_female` | numeric | Mean probability female across defendants. |
| `case_p_male` | numeric | Mean probability male across defendants. |
| `case_gender_hat` | character | Argmax gender. |
| `case_gender_group_l80` | character | Gender group at 80% threshold. |
| `case_gender_group_l60` | character | Gender group at 60% threshold. |
| `n_defendants` | integer | Number of defendants. |
| `n_imputed_defendants` | integer | Number with gender predictions. |
| `mean_defendant_p_female` | numeric | Same as case_p_female (alias). |
| `frac_imputed` | numeric | Fraction of defendants successfully imputed. |
| `case_gender_entropy` | numeric | Gender prediction entropy. |

---

## parcel_occupancy_panel

| Field | Value |
|-------|-------|
| **Config key** | `products.parcel_occupancy_panel` |
| **Path** | `data/processed/panels/parcel_occupancy_panel_with_ever_rentals.csv` |
| **Producing script** | `r/make-occupancy-vars.r` |
| **Primary key** | `PID` x `year` |
| **Rows** | 9,241,087 |
| **Columns** | 36 |
| **Years** | 2006-2024 |

All parcels (not just rentals) crossed with all years. Contains imputed unit counts and occupancy measures, plus current-year rental evidence flags used downstream. The rental-only companion output is `data/processed/panels/parcel_occupancy_panel_rentals_only.csv`.

### Consumed by

- `r/make-analytic-sample.R` — base panel for bldg_panel_blp
- `r/impute-race-preflight.R` — GEOID lookup for eviction defendants
- `r/impute-race-infousa.R` — parcel-linked GEOID crosscheck

### Key columns

| Column | Type | Description |
|--------|------|-------------|
| `PID` | character | Parcel ID. |
| `year` | integer | Calendar year. |
| `GEOID` | character | Block group GEOID from spatial join. |
| `total_units` | numeric | Imputed housing units (capped at 1.5x max_households). |
| `renter_occ` | numeric | Estimated renter-occupied units. |
| `occupancy_rate` | numeric | `renter_occ / total_units`. |
| `rental_occupancy_rate_scaled` | numeric | Scaled occupancy rate (0.9 mean in 2010 per bin). |
| `structure_bin` | character | Structure type bin. |
| `num_units_base` | numeric | Pre-cap unit count. |
| `max_households` | numeric | Maximum InfoUSA households ever observed. |
| `ever_rental_any_year` | logical | Any effective rental evidence in any year. |
| `rental_from_altos` | logical | Current-year Altos rental evidence after rental-universe exclusions. |
| `rental_from_license` | logical | Current-year rental-license evidence after rental-universe exclusions. |
| `rental_from_evict` | logical | Current-year eviction-based rental evidence after rental-universe exclusions. |
| `rental_from_nhpd` | logical | Current-year NHPD rental evidence after unique NHPD-to-parcel matching. |
| `rental_from_owner` | logical | Owner-based rental evidence in the current year. |
| `num_households_observed_raw` | numeric | Parcel-observed household count before sibling-link redistribution. |
| `num_households_link_alloc` | numeric | Household count allocated from the shared InfoUSA link group. |
| `num_households_from_link_alloc` | logical | TRUE when the final household count used link-group redistribution. |

---

## ever_rentals_panel

| Field | Value |
|-------|-------|
| **Config key** | `products.ever_rentals_panel` |
| **Path** | `data/processed/panels/ever_rentals_panel.csv` |
| **Producing script** | `r/make-rent-panel.R` |
| **Primary key** | `PID` x `year` |
| **Rows** | 3,770,246 |
| **Columns** | 62 |
| **Years** | 2006-2024 |

Candidate rental panel: PIDs with any rental evidence or qualifying owner-based rental evidence crossed with years. Rental identification now combines licenses, Altos, evictions, uniquely matched NHPD properties, and PHA owner-name rules, while retaining excluded university-owned parcels for audit via `include_in_rental_universe == FALSE`. Contains rent levels, filing counts, owner flags, NHPD preservation flags, and rental intensity measures.

### Consumed by

- `r/make-analytic-sample.R` — rent and filing data for bldg_panel_blp
- `r/make-occupancy-vars.r` — rental flags
- `r/impute-race-infousa.R` — rental PID filter

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `PID` | character | Parcel ID. |
| `year` | integer | Calendar year. |
| `med_rent_altos` | numeric | Legacy Altos PID-year rent alias (Phase 1 points to `med_rent_altos_raw`). |
| `med_rent_altos_raw` | numeric | Phase 1 raw Altos PID-year rent (listing-row median from `altos_year_building`). |
| `med_rent_altos_std_fixed` | numeric | Phase 1 composition-robust Altos PID-year rent using fixed bedroom-bin weights by parcel `building_type` class. Computed whenever at least one bedroom bin is observed; `altos_share_weight_observed` remains a diagnostic coverage metric. |
| `dlog_rent_altos_overlap` | numeric | Phase 1 overlap-based composition-robust Altos YoY log change. Uses only bedroom bins observed in both `t` and `t-1`. |
| `dlog_rent_altos_raw_yoy` | numeric | Diagnostic-only raw Altos YoY log change (composition-sensitive). |
| `n_altos_listings` | integer | Number of Altos listing rows (Phase 1 semantic fix; legacy name retained). |
| `n_altos_listing_rows` | integer | Explicit Altos listing-row count. Same value as `n_altos_listings` in Phase 1. |
| `n_altos_cells` | integer | Number of observed Altos bedroom bins in PID-year. |
| `n_altos_listing_rows_with_bed_bin` | integer | Altos listing rows in PID-year with a mappable bedroom bin (subset of `n_altos_listing_rows`). |
| `share_altos_rows_with_bed_bin` | numeric | Share of Altos listing rows with a mappable bedroom bin. |
| `altos_share_weight_observed` | numeric | Sum of fixed bedroom-bin weights observed in PID-year (Phase 1 diagnostic coverage metric for standardized Altos levels). |
| `altos_overlap_weight_prev` | numeric | Sum of fixed bedroom-bin weights overlapping between `t` and `t-1` (coverage diagnostic for overlap growth). |
| `altos_cell_overlap_prev` | integer | Number of overlapping bedroom bins between `t` and `t-1`. |
| `mix_class` | character | Coarse building-type-based bedroom mix class used to assign fixed Phase 1 Altos weights (`small_apartment`, `mid_apartment`, `large_apartment`). |
| `year_gap_altos_prev` | integer | Gap in years to previous observed Altos PID-year. `dlog_rent_altos_raw_yoy` is only defined when this equals 1. |
| `lic_num_units` | integer | Units from rental license. |
| `num_filings` | integer | Eviction filings in year. |
| `num_filings_with_ha` | integer | Filings with housing authority. |
| `med_eviction_rent` | numeric | Median rent claimed in eviction filings. |
| `pm.zip` | character | Mailing ZIP (underscore-prefixed). |
| `owner_1` | character | Primary owner name from parcels. |
| `is_pha_owner` | logical | Parcel owner matches the PHA owner-name regex. |
| `is_university_owned` | logical | Parcel owner matches the curated university-owner regex. |
| `ever_rental_nhpd` | logical | Parcel has any uniquely matched NHPD property. |
| `nhpd_property_count` | integer | Number of uniquely matched NHPD properties on the parcel. |
| `nhpd_total_units` | numeric | Sum of NHPD-reported units across those properties. |
| `nhpd_any_active` | logical | At least one uniquely matched NHPD property is `Active`. |
| `nhpd_any_inconclusive` | logical | At least one uniquely matched NHPD property is `Inconclusive`. |
| `ever_rental_owner` | logical | Any owner-based rental evidence across years. |
| `include_in_rental_universe` | logical | Final effective-rental-universe flag after owner-based inclusions/exclusions. |
| `rental_from_altos_raw` | logical | Raw Altos listing evidence before owner-based exclusion rules. |
| `rental_from_license_raw` | logical | Raw license evidence before owner-based exclusion rules. |
| `rental_from_evict_raw` | logical | Raw eviction evidence before owner-based exclusion rules. |
| `rental_from_nhpd_raw` | logical | Raw NHPD evidence before owner-based exclusion rules. |
| `rental_from_altos` | logical | Effective Altos listing evidence used downstream. |
| `rental_from_license` | logical | Effective rental license evidence used downstream. |
| `rental_from_evict` | logical | Effective eviction evidence used downstream. |
| `rental_from_nhpd` | logical | Effective NHPD evidence used downstream. |
| `rental_from_owner` | logical | Owner-based rental evidence used downstream (currently PHA owner matching). |
| `ever_rental_any_year` | logical | Any effective rental evidence in any year. |
| `intensity_license_y` | numeric | License-based rental intensity. |
| `intensity_altos_y` | numeric | Altos-based rental intensity. |
| `intensity_evict_y` | numeric | Eviction-based rental intensity. |
| `intensity_evict_ha_y` | numeric | Housing authority eviction intensity. |
| `intensity_total_y` | numeric | Combined rental intensity. |
| `intensity_total_y_recent` | numeric | Recent-period rental intensity. |

---

## nhpd_clean

| Field | Value |
|-------|-------|
| **Config key** | `products.nhpd_clean` |
| **Path** | `data/processed/clean/nhpd_clean.csv` |
| **Producing script** | `r/clean-nhpd-addresses.R` |
| **Primary key** | `nhpd_property_id` |
| **Rows** | 461 |
| **Columns** | 300 |

Cleaned National Housing Preservation Database property-level file for Philadelphia. Built from the NHPD "Active and Inconclusive Properties" workbook, filtered to `State == "PA"`, `City == "Philadelphia"`, and `PropertyStatus %in% c("Active", "Inconclusive")`, then standardized with the same Postmastr-based address cleaning flow used for Altos and other sources.

### Consumed by

- `r/merge-nhpd-parcels.R` — parcel crosswalk construction

### Key columns

| Column | Type | Description |
|--------|------|-------------|
| `nhpd_property_id` | character | NHPD property identifier (`NHPDPropertyID`). Primary key. |
| `PropertyName` | character | Property name from the NHPD workbook. |
| `PropertyAddress` | character | Raw property address from the NHPD workbook. |
| `PropertyStatus` | character | NHPD status, retained here as `Active` or `Inconclusive`. |
| `TotalUnits` | numeric | Total NHPD-reported units at the property. |
| `Owner` | character | Owner name from NHPD. |
| `pm.house` | character | Parsed house number. |
| `pm.street` | character | Parsed street name. |
| `pm.streetSuf` | character | Parsed street suffix. |
| `pm.zip` | character | Parsed ZIP code, underscore-prefixed. |
| `n_sn_ss_c` | character | Canonicalized normalized address string used in crosswalk joins. |
| `GEOID.longitude` | numeric | Longitude from the source workbook. |
| `GEOID.latitude` | numeric | Latitude from the source workbook. |
| `num_properties_at_address` | integer | Number of NHPD properties sharing the cleaned address. |
| `address_total_units` | numeric | Sum of `TotalUnits` across all NHPD properties at the cleaned address. |

---

## nhpd_xwalk_products

| Field | Value |
|-------|-------|
| **Config keys** | `products.nhpd_address_agg`, `products.nhpd_parcel_xwalk`, `products.nhpd_parcel_xwalk_property` |
| **Paths** | `data/processed/xwalks/nhpd_address_agg.csv`; `data/processed/xwalks/nhpd_parcel_xwalk.csv`; `data/processed/xwalks/nhpd_parcel_xwalk_property.csv` |
| **Producing script** | `r/merge-nhpd-parcels.R` |
| **Primary keys** | `n_sn_ss_c` (`nhpd_address_agg`); `PID` x `n_sn_ss_c` (`nhpd_parcel_xwalk`); `nhpd_property_id` x `PID` (`nhpd_parcel_xwalk_property`) |
| **Rows** | 461; 472; 472 |
| **Columns** | 12; 6; 8 |

NHPD-to-parcel crosswalk products. Matching is deterministic and tiered: normalized address match on `pm.house + pm.street + pm.streetSuf`, then parcel-polygon spatial join, then a nearest-parcel fallback within 50 feet for residual cases. In the current Philadelphia build, 428 of 461 NHPD properties (90.7%) matched uniquely, mapping to 422 unique parcels.

### Consumed by

- `r/make-rent-panel.R` — NHPD-based rental identification
- `r/make-occupancy-vars.r` — NHPD rental flags in occupancy products
- `r/make-analytic-sample.R` — downstream analytic rental-source fields

### nhpd_address_agg

One row per cleaned NHPD address, aggregated before parcel matching.

| Column | Type | Description |
|--------|------|-------------|
| `n_sn_ss_c` | character | Canonical cleaned address key. Primary key. |
| `nhpd_property_count` | integer | Number of NHPD properties at the address. |
| `nhpd_total_units` | numeric | Sum of NHPD `TotalUnits` at the address. |
| `property_status_mode` | character | Modal NHPD status at the address. |
| `GEOID.longitude` | numeric | Address longitude used for spatial fallback. |
| `GEOID.latitude` | numeric | Address latitude used for spatial fallback. |

### nhpd_parcel_xwalk

Address-level parcel crosswalk, analogous to the Altos parcel crosswalk.

| Column | Type | Description |
|--------|------|-------------|
| `PID` | character | Matched parcel ID. |
| `n_sn_ss_c` | character | Cleaned NHPD address key. |
| `merge` | character | Match tier: `num_st_sfx`, `spatial`, `nearest_50ft`, or `not_merged`. |
| `unique` | logical | TRUE when the cleaned address matched exactly one parcel. |
| `num_parcels_matched` | integer | Number of parcels linked to the address. |
| `num_addys_matched` | integer | Number of NHPD addresses linked to the parcel in this xwalk. |

### nhpd_parcel_xwalk_property

Property-level parcel crosswalk retaining one row per NHPD property x matched parcel.

| Column | Type | Description |
|--------|------|-------------|
| `nhpd_property_id` | character | NHPD property identifier. |
| `PID` | character | Matched parcel ID. |
| `n_sn_ss_c` | character | Cleaned address key. |
| `merge` | character | Match tier inherited from `nhpd_parcel_xwalk`. |
| `unique` | logical | TRUE when the underlying address matched one parcel. |
| `matched_unique` | logical | TRUE when the NHPD property is carried forward as a unique parcel match in downstream rental identification. |
| `num_parcels_matched` | integer | Number of parcels linked to the property address. |
| `num_addys_matched` | integer | Number of cleaned NHPD addresses linked to the parcel. |

---

## altos_pid_year_bedbin

| Field | Value |
|-------|-------|
| **Config key** | `products.altos_pid_year_bedbin` |
| **Path** | `data/processed/panels/altos_pid_year_bedbin.csv` |
| **Producing script** | `r/make-altos-aggs.R` |
| **Primary key** | `PID` x `year` x `bed_bin` |
| **Conceptual unit** | Altos listing-price cell (parcel-year-bedroom-bin) |

Phase 1 Altos composition standardization input product. Each row is a parcel-year-bedroom-bin cell built directly from de-staled Altos listing rows.

### Key columns

| Column | Type | Description |
|--------|------|-------------|
| `PID` | character | Parcel ID (OPA parcel number). |
| `year` | integer | Calendar year. |
| `bed_bin` | character | Bedroom bin: `studio`, `1br`, `2br`, `3plus`. |
| `med_rent_cell` | numeric | Median listing rent among Altos rows in this PID-year-bedroom-bin cell. |
| `mean_rent_cell` | numeric | Mean listing rent in cell. |
| `p25_rent_cell` | numeric | 25th percentile listing rent in cell (QC). |
| `p75_rent_cell` | numeric | 75th percentile listing rent in cell (QC). |
| `n_listing_rows_cell` | integer | Number of Altos listing rows in the cell. |
| `n_price_points_cell` | integer | Number of unique list prices observed in the cell-year. |
| `n_price_changes_cell` | integer | Count of within-year observed price changes across listings in the cell (QC summary). |

---

## building_data

| Field | Value |
|-------|-------|
| **Config key** | `products.building_data` (year), `products.building_data_rental_year` / `_quarter` / `_month` (parquet) |
| **Path** | `data/processed/panels/building_data_year.csv` (year), `.parquet` variants for quarter/month |
| **Producing script** | `r/make-building-data.R` (year CSV), `r/make-rental-building-data.R` (rental parquet) |
| **Primary key** | `parcel_number` x `period` |
| **Rows** | ~18,687,008 (year CSV, all parcels) |
| **Columns** | 38 |

Building-level permits, violations, complaints, and investigations. The year CSV covers all parcels; the parquet files are rental-only at finer time granularity.

### Coverage notes (complaints/permits)

Empirical coverage check run on 2026-02-27 (from `building_data_rental_quarter`):

- `total_complaints` and `total_permits` are structurally non-missing across all years in the rental-quarter panel.
- Positive activity is effectively zero in 2005-2006, then appears from 2007 onward.
- For retaliatory-evictions analysis defaults, the active window is `2011-2019`.

| Year | Share of PID-quarter rows with `total_complaints > 0` | Share with `total_permits > 0` |
|------|---------------------------------------------------------|----------------------------------|
| 2005 | 0.0000 | 0.0000 |
| 2006 | 0.0000 | 0.0000 |
| 2007 | 0.0158 | 0.0134 |
| 2011 | 0.0237 | 0.0132 |
| 2019 | 0.0332 | 0.0182 |
| 2022 | 0.0264 | 0.0138 |

### Consumed by

- `r/retaliatory-evictions.r` — uses `building_data_rental_month` or `_quarter` (complaints as treatment)
- `r/retaliatory-evictions-race.R` / `r/retaliatory-evictions-gender.R` — uses `building_data_rental_quarter`

### Key columns

| Column | Type | Description |
|--------|------|-------------|
| `parcel_number` | character | OPA parcel number (= PID). |
| `period` | character | Time period (year, quarter `YYYY-Q#`, or month `YYYY-MM`). |
| `year` | integer | Derived calendar year. |
| `total_permits` | integer | Total permits issued. |
| `building_permit_count` | integer | Building permits specifically. |
| `electrical_permit_count` | integer | Electrical permits. |
| `plumbing_permit_count` | integer | Plumbing permits. |
| `total_complaints` | integer | Total L&I complaints filed. Key treatment variable for retaliatory evictions. |
| `total_severe_complaints` | integer | Severe/hazardous complaints subset. |
| `complaint_*_count` | integer | Individual complaint type counts (e.g., `complaint_fire_count`, `complaint_plumbing_count`, etc.). |
| `total_violations` | integer | Total code violations. |
| `total_severe_violations` | integer | Severe violations (HAZARDOUS or IMMINENTLY DANGEROUS `caseprioritydesc`). |
| `hazardous_violation_count` | integer | Hazardous violations specifically. |
| `violation_*_count` | integer | Individual violation type counts. |
| `total_investigations` | integer | Total L&I case investigations. |
| `total_severe_investigations` | integer | Severe investigations (code enforcement, lead, hazmat; classified via `investigationtype`). **Structurally zero for 2021+ due to data regime change — see note below.** |
| `investigation_*_count` | integer | Individual investigation type counts. |

### Investigation data regime note

The L&I case investigations dataset underwent a classification regime change around 2020:

- **Pre-2020**: `investigationtype` carries the meaningful type codes (e.g., `HCEU INSP`, `PRECOURT`, `CI HAZMAT`). `casepriority` is blank for 100% of rows.
- **2020+**: `casepriority` is populated (0% blank); `investigationtype` codes change and no longer map cleanly to the pre-2020 severe categories.

`total_severe_investigations` (and the typed `investigation_*_count` columns) are built from `investigationtype` and are therefore **reliable for pre-2020 years only**. Post-2020 values are structurally zero or near-zero and should not be used. There is no clean mapping between the two regimes.

---

## evictions_clean

| Field | Value |
|-------|-------|
| **Config key** | `products.evictions_clean` |
| **Path** | `data/processed/clean/evictions_clean.csv` |
| **Producing script** | `r/clean-eviction-addresses.R` |
| **Primary key** | `id` (docket number) |
| **Rows** | ~796,440 |
| **Columns** | 145 |

Cleaned eviction case data with standardized addresses, filing details, party information, outcomes, and dates.

### Consumed by

- `r/make-evict-aggs.R` — aggregation to PID-year
- `r/impute-race-preflight.R` — defendant name parsing
- `r/retaliatory-evictions.r` — case-level filing details
- `r/retaliatory-evictions-race.R` / `r/retaliatory-evictions-gender.R`

### Key columns (selected from 145)

| Column | Type | Description |
|--------|------|-------------|
| `id` | character | Docket number. Primary key. |
| `filing_dt` | Date | Filing date. |
| `d_filing` | Date | Alias for filing_dt used by retaliatory-evictions scripts. |
| `filing_yr` | integer | Filing year. |
| `n_sn_ss_c` | character | Canonical address string (number + street name + suffix + city). Join key to xwalks. |
| `commercial` | logical/integer | Commercial case flag. Analysis scripts filter to `commercial == 0`. | # note: do not rely on this column
| `n_defendants_total` | integer | Total defendants on case. |
| `disp_*` | various | Disposition/outcome fields. |
| `claim_*` | various | Claim amount fields. |
| `plaintiff_*` / `defendant_*` | character | Party name fields. |

---

## evict_address_xwalk

| Field | Value |
|-------|-------|
| **Config key** | `products.evict_address_xwalk` / `products.evict_address_xwalk_case` |
| **Path** | `data/processed/xwalks/philly_evict_address_agg_xwalk.csv` / `..._xwalk_case.csv` |
| **Producing script** | `r/make-address-parcel-xwalk.R` |
| **Primary key** | `n_sn_ss_c` (address-level) / `id` (case-level) |

Links eviction filing addresses to parcel IDs.

### evict_address_xwalk (address-level, 7 columns)

| Column | Type | Description |
|--------|------|-------------|
| `PID` | character | Matched parcel ID. |
| `n_sn_ss_c` | character | Canonical address string. |
| `merge` | character | Match method (e.g., `"exact"`, `"fuzzy"`). |
| `unique` | logical | Whether match is unique. |
| `num_merges` | integer | Number of match attempts. |
| `num_parcels_matched` | integer | Number of parcels matched to this address. |
| `num_addys_matched` | integer | Number of addresses matched to the parcel. |

### evict_address_xwalk_case (case-level, 8 columns)

Same columns plus `id` (docket number), enabling direct case-to-PID joins.

---

## evict_infousa_household_linkage

| Field | Value |
|-------|-------|
| **Config key(s)** | `products.evict_infousa_hh_matches`, `products.evict_infousa_hh_unmatched`, `products.evict_infousa_candidate_stats` |
| **Path(s)** | `data/processed/xwalks/evict_infousa_hh_matches.csv`, `.../evict_infousa_hh_unmatched.csv`, `.../evict_infousa_candidate_stats.csv` |
| **Producing script** | `r/link-evictions-infousa-names.R` |
| **Primary key(s)** | `evict_id` (matches/unmatched/candidate_stats) |
| **Conceptual unit** | Eviction case (household) linked to InfoUSA household (`familyid`) |

These products link eviction cases to InfoUSA households using person-level name matching plus case-level deterministic resolution. Multiple defendant names can match multiple InfoUSA persons within one `familyid`; final output enforces at most one `familyid` per `evict_id`.

Methodology note:
- Full linkage workflow details (blocking, scoring, thresholds, sample mode, and runtime controls) are documented in `docs/evict-infousa-linkage.md`.

### evict_infousa_hh_matches

One auto-accepted household link per eviction case.

| Column | Type | Description |
|--------|------|-------------|
| `evict_id` | character | Eviction case docket ID. |
| `familyid` | character | Matched InfoUSA household ID. |
| `matched_person_nums` | character | Comma-separated InfoUSA `person_num` values matched for this case-household pair. |
| `match_tier` | character | Candidate tier used (`A`, `B`, `C`). |
| `bldg_size_bin` | character | Building size bin used for thresholds. |
| `year_link_used` | integer | Chosen year offset (`inf_year - filing_yr`) for best match; `0` when year-agnostic blocking is used. |
| `p_match` | numeric | Best score among matched persons in the case-household pair. |
| `fn_match_score` | numeric | Best first-name similarity score in the pair. |
| `top2_margin` | numeric | Best-vs-second score gap from person-level ranking. |
| `gender_agreement` | logical | Diagnostic only; not used in base acceptance rules. |
| `ambiguous_flag` | logical | TRUE if score separation is weak. |
| `ambiguous_household` | logical | TRUE if case had competing household candidates pre-resolution. |
| `needs_review` | logical | Review flag from threshold logic. |
| `schema_version` | character | Product schema/version tag. |

### evict_infousa_hh_unmatched

One row per eviction case that did not receive an auto-accepted household link.

| Column | Type | Description |
|--------|------|-------------|
| `evict_id` | character | Eviction case docket ID. |
| `bldg_size_bin` | character | Building size bin. |
| `bg_geoid` | character | Case geography used for QC stratification. |
| `unmatched_reason` | character | Reason code (e.g., `no_match`, `ambiguous_household`). |

### evict_infousa_candidate_stats

Case-level diagnostics on candidate volume and match coverage.

| Column | Type | Description |
|--------|------|-------------|
| `evict_id` | character | Eviction case docket ID. |
| `n_defendants_on_case` | integer | Number of unique defendant person-rows used in linkage. |
| `n_defendants_matched` | integer | Number of defendant person-rows that received accepted person-level links. |
| `num_candidates` | integer | Total raw candidate pairs across all defendants in the case. |
| `num_candidates_capped` | integer | Candidate count after per-defendant cap. |
| `was_capped` | logical | TRUE if any defendant on case exceeded cap `K`. |
| `tier_attempted` | character | Comma-separated tiers attempted (`A,B,C`). |

### Related QA artifacts

- `output/qa/evict_infousa_name_link/review_highconf.csv`
- `output/qa/evict_infousa_name_link/review_grayzone.csv`
- `output/qa/evict_infousa_name_link/evict_infousa_*.csv`
- `data/processed/xwalks/evict_infousa_sample_manifest.csv`

---

## renter_poverty_geography_products

| Field | Value |
|-------|-------|
| **Config keys** | `products.bg_renter_poverty_share`, `products.tract_renter_poverty_share` |
| **Paths** | `data/processed/xwalks/bg_renter_poverty_share.csv`, `data/processed/xwalks/tract_renter_poverty_share.csv` |
| **Producing script** | `r/make-renter-poverty-geo.R` |
| **Primary keys** | BG product: `bg_geoid`; tract product: `tract_geoid` |
| **Rows** | One per block group / tract in the build geography |

Geography-level poverty products for outer-sorting empirical targets. If a raw input with explicit renter household counts is supplied, the script uses those counts directly. If no input is supplied, the script first tries ACS `B17019` renter-family counts; if that route returns structurally empty counts, it falls back to ACS `C17002` overall poverty counts and computes an overall poverty share. The product records the path taken in `measure_source`.

### bg_renter_poverty_share

| Column | Type | Description |
|--------|------|-------------|
| `bg_geoid` | character | Census block-group GEOID. |
| `renter_hh_total` | numeric | Total renter households or renter families in the source measure. `NA` when the fallback is a direct share-only ACS measure. |
| `renter_hh_poverty` | numeric | Renter households or renter families below poverty in the source measure. `NA` when the fallback is a direct share-only ACS measure. |
| `renter_poverty_share` | numeric | `renter_hh_poverty / renter_hh_total` when renter counts are available; otherwise an overall ACS poverty-share proxy from `C17002`. |
| `acs_year` | integer | ACS year used for the source build. |
| `measure_source` | character | Source flag, e.g. `input_explicit_household_counts`, `acs_b17019_renter_family_proxy`, or `acs_c17002_overall_poverty_share`. |

### tract_renter_poverty_share

| Column | Type | Description |
|--------|------|-------------|
| `tract_geoid` | character | Census tract GEOID. |
| `renter_hh_total` | numeric | Tract-level sum of renter households/families from BG counts. `NA` when the fallback is a direct share-only ACS measure. |
| `renter_hh_poverty` | numeric | Tract-level sum of renter households/families below poverty from BG counts. `NA` when the fallback is a direct share-only ACS measure. |
| `renter_poverty_share` | numeric | `renter_hh_poverty / renter_hh_total` when renter counts are available; otherwise an overall ACS poverty-share proxy from `C17002`. |
| `acs_year` | integer | ACS year used for the source build. |
| `measure_source` | character | Source flag inherited from the BG build. |

---

## outer_sorting_empirical_moments

| Field | Value |
|-------|-------|
| **Config key** | `products.outer_sorting_empirical_moments` |
| **Path** | `data/processed/targets/outer_sorting_empirical_moments.csv` |
| **Producing script** | `r/make-outer-sorting-empirical-moments.R` |
| **Primary key** | Single-row product |
| **Rows** | 1 |

Empirical calibration moment vector for the outer-sorting model, built from `bldg_panel_blp` plus a PID-year InfoUSA composition proxy. The script collapses `bldg_panel_blp` over the configured year window to one row per PID, uses tract FE (`substr(GEOID, 1, 11)`) as the neighborhood definition, and uses the configured building-level low-income-share proxy as the empirical analogue of composition `s`. Counts are annualized over each building's active years in the window, with rate construction based on total unit-years. Buildings with `year_built > year_to` are dropped; buildings built after `year_from` start exposure at `year_built`. Rows with missing building-level composition proxy are dropped from the empirical target sample.

Additional metadata columns beyond the simulated moment schema:

| Column | Type | Description |
|--------|------|-------------|
| `year_from` | integer | First year included in the pooled empirical target window. |
| `year_to` | integer | Last year included in the pooled empirical target window. |
| `composition_product_key` | character | Config product key used for the building-level composition input. |
| `composition_proxy_col` | character | Column from the composition product used as the empirical analogue of `s`. |
| `composition_measure_source` | character | Source flag from the composition product. |

Outcome mapping in the default empirical build:

| Simulator concept | Empirical default |
|-------------------|-------------------|
| Filings | `num_filings` |
| Complaints | `total_complaints` |
| Maintenance | `total_permits` |
| Units exposure | `total_units`, aggregated to PID-level unit-years and annualized back to average annual units |
| Composition share `s` | PID-year InfoUSA low-income share (`infousa_find_low_income_share` by default) |
| Neighborhood FE | tract `substr(GEOID, 1, 11)` |

Composition-proxy merge:

| Stage | Source |
|-------|--------|
| Building composition proxy | `infousa_building_income_proxy_panel` |
| Missing PID-year proxy | dropped from empirical target sample |

Empirical collapse semantics in the default build:

| PID-level quantity | Construction |
|--------------------|--------------|
| Active years | observed PID-years within `year_from:year_to`, after dropping rows with `year_built > year_to` |
| Unit-years | `sum(total_units)` across active years |
| Annualized counts | `sum(outcome_count) / active_years` |
| Per-unit-year rates | `sum(outcome_count) / sum(total_units)` |

---

## infousa_building_income_proxy_panel

| Field | Value |
|-------|-------|
| **Config key** | `products.infousa_building_income_proxy_panel` |
| **Path** | `data/processed/analytic/infousa_building_income_proxy_panel.csv` |
| **Producing script** | `r/make-infousa-building-income-proxy.R` |
| **Primary key** | `PID` x `year` |
| **Rows** | ~3.6M for the default 2011-2019 window |

PID-year InfoUSA income proxy panel used by the outer-sorting empirical target path. The builder starts from `infousa_clean`, keeps primary-family rows, deduplicates to one row per `familyid` x `year`, aggregates to address-year, links addresses to parcels via `xwalk_infousa_to_parcel`, drops address keys that map to more than one PID, and then aggregates to PID-year. The default composition proxy is the share of linked households with `find_div_1000 <= 30`.

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `PID` | character | Parcel ID. Primary key part 1. |
| `year` | integer | Calendar year. Primary key part 2. |
| `infousa_num_households_income_proxy` | integer | Number of linked primary-household observations contributing to the PID-year proxy. |
| `infousa_find_mean_k` | numeric | Mean `find_div_1000` across linked households at the PID-year. |
| `infousa_find_low_income_share` | numeric | Share of linked households with `find_div_1000` at or below the configured threshold. Default threshold is `30`. |
| `infousa_income_proxy_any_ownership_unsafe` | logical | TRUE if any contributing address row in the parcel xwalk was flagged `ownership_unsafe`. |
| `infousa_income_proxy_xwalk_status_mode` | character | Modal xwalk status across contributing address-year rows. |
| `infousa_income_proxy_source` | character | Source label for the proxy definition. |
| `infousa_income_proxy_threshold_k` | numeric | Income threshold in `$000s` used to define the low-income share. |

---

## outer_sorting_simulation_products

| Field | Value |
|-------|-------|
| **Config keys** | `products.outer_sorting_sim_panel`, `products.outer_sorting_sim_moments`, `products.outer_sorting_sim_diagnostics` |
| **Paths** | `data/processed/sim/outer_sorting_sim_panel.csv`, `data/processed/sim/outer_sorting_sim_moments.csv`, `data/processed/sim/outer_sorting_sim_diagnostics.csv` |
| **Producing script** | `r/run-outer-sorting-sim.R` |
| **Primary keys** | panel: `b`; moments: single-row product; diagnostics: single-row product |
| **Rows** | panel: `B` from `run.outer_sorting.B`; moments: 1; diagnostics: 1 |

Simulation outputs from the stationary baseline outer sorting DGP in `latex/sorting_model_outer_dgp.tex`. The current baseline uses signal `x_b = fbar_b`, where filings follow a single-intensity Poisson block driven by average tenant default risk `\bar\delta_b = s_b \delta_H + (1 - s_b)\delta_L`, complaints depend on composition only for bad landlords, and the sorting update includes a neighborhood shifter `eta_sort`. The maintenance and complaint blocks also include empirically drawn building-age indicators based on year built. By default, simulated building units and year-built observables are drawn by resampling the empirical PID-level BLP distribution over the configured unit-draw window. Realized simulation outcomes are then generated as total counts over each building's active years in the configured simulation window and annualized back to average annual counts and per-unit-year rates; the older discrete-bin unit draw remains available only as an explicit fallback mode.

### outer_sorting_sim_panel

One row per simulated building.

| Column | Type | Description |
|--------|------|-------------|
| `b` | integer | Simulated building ID. Primary key. |
| `nhood` | integer | Simulated neighborhood index. |
| `theta` | character | Landlord/building type label (`G` or `B`). |
| `theta_bad` | integer | Type indicator (`1` for bad type, `0` for good type). |
| `units` | numeric | Simulated building units. Default build draws by empirical resampling from PID-level average annual `total_units`; legacy discrete-bin draws are only used if explicitly configured. |
| `year_built` | integer | Empirically resampled year built from the BLP-based draw pool; may be missing for some simulated buildings if the source year built is missing. |
| `old_bldg` | integer | Indicator for `year_built <= run.outer_sorting.old_bldg_cutoff_year`. |
| `new_bldg` | integer | Indicator for `year_built >= run.outer_sorting.new_bldg_cutoff_year`. |
| `first_year_active` | integer | First simulated active year, equal to `max(run.outer_sorting.sim_year_from, year_built)` when `year_built` is observed and `run.outer_sorting.sim_year_from` otherwise. |
| `exposure_years` | integer | Number of active simulated years used to generate annualized outcomes. |
| `unit_years` | numeric | Simulated unit-years, `units * exposure_years`. |
| `eta_sort` | numeric | Neighborhood sorting shifter drawn once per neighborhood and shared across buildings in that neighborhood. |
| `s` | numeric | Equilibrium share of group-1 tenants in the building. |
| `mbar`, `cbar`, `fbar` | numeric | Expected maintenance/complaint/filing rates per unit. |
| `x` | numeric | Sorting signal index. Baseline: `x = fbar`. |
| `M_total`, `C_total`, `F_total` | integer | Total maintenance/complaint/filing counts drawn over the building's active simulated years. |
| `M`, `C`, `F` | numeric | Average annual maintenance/complaint/filing counts (`total / exposure_years`). |
| `m_rate`, `c_rate`, `f_rate` | numeric | Annualized realized per-unit-year rates (`total / unit_years`). |

### outer_sorting_sim_moments

Single-row calibration moment summary.

| Column | Type | Description |
|--------|------|-------------|
| `n_buildings` | integer | Number of simulated buildings used to compute moments. |
| `s_weighted_mean` | numeric | Unit-weighted mean of `s`; target should align with configured `mu`. |
| `mean_f_rate` | numeric | Unit-weighted mean realized filing rate. |
| `mean_m_rate` | numeric | Unit-weighted mean realized maintenance rate. |
| `mean_c_rate` | numeric | Unit-weighted mean realized complaint rate. |
| `mean_m_rate_pos` | numeric | Mean realized maintenance rate among buildings with positive maintenance counts (`M > 0`). |
| `m_rate_pos_p50` | numeric | Median realized maintenance rate among buildings with positive maintenance counts. |
| `m_rate_pos_p90` | numeric | 90th percentile realized maintenance rate among buildings with positive maintenance counts. |
| `zero_share_f` | numeric | Share of buildings with zero filings (`F = 0`). |
| `zero_share_m` | numeric | Share of buildings with zero maintenance counts (`M = 0`). |
| `zero_share_c` | numeric | Share of buildings with zero complaints (`C = 0`). |
| `share_high_filing` | numeric | Share of buildings above the fixed high-filing threshold (`f_rate >= high_filing_threshold`). |
| `high_filing_threshold` | numeric | Fixed absolute filing-rate cutoff used to construct `high_filing` moments (set in config). |
| `filing_p85` | numeric | Backward-compatible alias for `high_filing_threshold` in the current baseline implementation. |
| `sorting_coef_high_filing_nhood` | numeric | Weighted within-neighborhood OLS slope from `s ~ high_filing | nhood`. |
| `sorting_coef_high_bin_nhood` | numeric | Weighted within-neighborhood OLS coefficient on the top filing bin relative to the low filing bin. |
| `sorting_gap_high_minus_low` | numeric | Backward-compatible alias of `sorting_coef_high_filing_nhood`. |
| `beta_ppml_s` | numeric | PPML coefficient on continuous composition share `s` in the complaint regression with neighborhood FE and unit offset. |
| `beta_ppml_high_filing` | numeric | PPML coefficient on `high_filing`. |
| `beta_ppml_s_x_high_filing` | numeric | PPML coefficient on `s × high_filing`. |
| `beta_ppml_above_mean_s` | numeric | Backward-compatible alias of `beta_ppml_s`. |
| `beta_ppml_interaction` | numeric | Backward-compatible alias of `beta_ppml_s_x_high_filing`. |
| `filing_resid_sd_nhood_fe` | numeric | SD of filing-rate residuals after neighborhood demeaning. |
| `nhood_mean_s_sd` | numeric | SD of neighborhood mean group-1 share. |
| `nhood_mean_s_p90_p10` | numeric | 90-10 spread of neighborhood mean group-1 share. |
| `nhood_mean_f_rate_sd` | numeric | SD of neighborhood mean filing rates. |
| `nhood_mean_f_rate_p90_p10` | numeric | 90-10 spread of neighborhood mean filing rates. |
| `beta_maint_on_c_rate` | numeric | Maintenance-discipline PPML coefficient on raw `c_rate` with neighborhood FE and unit offset. |
| `beta_maint_on_log1p_c_rate` | numeric | Backward-compatible alias of `beta_maint_on_c_rate`. |
| `beta_maint_on_log1p_cbar` | numeric | Backward-compatible alias of `beta_maint_on_c_rate`. |

### outer_sorting_sim_diagnostics

Single-row fixed-point convergence diagnostics.

| Column | Type | Description |
|--------|------|-------------|
| `converged` | logical | TRUE when tolerance conditions were met before `max_iter`. |
| `iters` | integer | Iteration count at stop. |
| `max_diff` | numeric | Final `max_b |s^{k+1} - s^k|`. |
| `sim_year_from` | integer | First year in the configured simulation window used for annualization. |
| `sim_year_to` | integer | Last year in the configured simulation window used for annualization. |
| `sim_n_years` | integer | Length of the configured simulation window. |
| `mass_resid_before_final_proj` | numeric | Citywide mass residual before final exact projection step. |
| `mass_resid_final` | numeric | Citywide mass residual after final projection. |
| `xi_last_iter` | numeric | Last intercept value from iterative updates. |
| `xi_final` | numeric | Final intercept used in exact mass-constrained projection. |
| `mu_target` | numeric | Configured citywide target share (`mu`). |
| `s_weighted_mean` | numeric | Unit-weighted mean `s` in final panel. |

---

## Notes for Consumers

### Joining products together

Most joins use `PID` x `year`:
```r
# Join building race to analytic sample
merge(bldg_panel_blp, infousa_building_race_panel, by = c("PID", "year"), all.x = TRUE)

# Join case-level demographics to eviction data
merge(evictions_clean, race_imputed_case_sample, by = "id", all.x = TRUE)
```

### BLP-specific notes

- Use `share_zip` (not `market_share_units`) for BLP estimation — it sums <= 1, with vacancy as the outside good.
- Market definition for BLP is zip x year (`market_id_zip_year`), NOT zip x year x bin.
- `num_units_bin` is the nesting variable for nested logit.
- BLP instruments (`z_sum_others_*`, `z_sum_samefirm_*`, `z_sum_otherfirm_*`) are weak under PID + GEOID x year FE. The baseline no-IV nested logit works well.

### Common gotchas

- `pm.zip` is underscore-prefixed (e.g., `_19104`), not numeric.
- `building_type` has coded values (e.g., `MIDRISE_MULTI`); use `building_code_description_new` for human-readable labels.
- `parcel_number` and `PID` are the same field with different names across products.
- Race/gender output files live in `output/qa/`, not `data/processed/`, despite being proper data products.
- `building_data_rental_*` parquet files are rental-only subsets; the year CSV has all parcels.
- `retaliatory-evictions-race.R` and `retaliatory-evictions-gender.R` have been consolidated into `retaliatory-evictions-demographics.R`.

---

## Cross-Reference: Which Analysis Scripts Read Which Products

| Data Product | Analysis Scripts |
|---|---|
| `analytic_sample` | `price-regs-eb.R`, `retaliatory-evictions.r` (PID filter only) |
| `bldg_panel_blp` | `retaliatory-evictions-demographics.R` (controls), `pyblp_estimation.py` |
| `building_data_rental_month/quarter` | `retaliatory-evictions.r`, `retaliatory-evictions-demographics.R` |
| `evictions_clean` | `retaliatory-evictions.r`, `retaliatory-evictions-demographics.R` |
| `evict_address_xwalk` | `retaliatory-evictions.r`, `retaliatory-evictions-demographics.R` |
| `evict_infousa_hh_matches` | linkage QC and downstream case-level demographic enrichment (optional) |
| `evict_infousa_hh_unmatched` | linkage QA and gap diagnostics |
| `evict_infousa_candidate_stats` | linkage QA and candidate-cap diagnostics |
| `race_imputed_case_sample` | `retaliatory-evictions-demographics.R` |
| `gender_imputed_case_sample` | `retaliatory-evictions-demographics.R` |
| `infousa_race_imputed_person` | `retaliatory-evictions-demographics.R` (optional) |
