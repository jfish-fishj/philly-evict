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
10. [building_data](#building_data) — Building permits, violations, complaints
11. [evictions_clean](#evictions_clean) — Cleaned eviction case data
12. [evict_address_xwalk / evict_address_xwalk_case](#evict_address_xwalk) — Eviction-to-parcel crosswalks
13. [evict_infousa_hh_matches / evict_infousa_hh_unmatched / evict_infousa_candidate_stats](#evict_infousa_household_linkage) — Eviction case-to-InfoUSA household linkage products

---

## bldg_panel_blp

| Field | Value |
|-------|-------|
| **Config key** | `products.bldg_panel_blp` |
| **Path** | `data/processed/analytic/bldg_panel_blp.csv` |
| **Producing script** | `r/make-analytic-sample.R` |
| **Primary key** | `PID` x `year` |
| **Rows** | ~3,585,124 |
| **Columns** | 208 |
| **Years** | 2006-2022 |

The main analysis panel. Merges parcel occupancy, rental activity, eviction filings, building characteristics, assessments, and BLP market share instruments. Every rental parcel-year in the occupancy panel.

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
| `med_rent_altos` | numeric | Median monthly rent from Altos listings (PID-year level). |
| `log_med_rent` | numeric | `log(med_rent_altos)`. Main price variable. |
| `n_altos_listings` | integer | Number of Altos listings at PID in year. |

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

All instruments computed within `market_id_zip_year` (zip x year).

| Column | Type | Description |
|--------|------|-------------|
| `z_sum_others_total_units` | numeric | Sum of `total_units` at other firms in market. |
| `z_sum_others_total_area` | numeric | Sum of `total_area` at other firms. |
| `z_sum_others_total_livable_area` | numeric | Sum of `total_livable_area` at other firms. |
| `z_sum_others_number_of_bedrooms` | numeric | Sum of bedrooms at other firms. |
| `z_sum_others_number_of_bathrooms` | numeric | Sum of bathrooms at other firms. |
| `z_sum_others_number_stories` | numeric | Sum of stories at other firms. |
| `z_sum_samefirm_*` | numeric | Same-firm versions of above (sum across own firm's other products). |
| `z_sum_otherfirm_*` | numeric | Other-firm versions (equivalent to `z_sum_others_*`). |

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

### Violations & complaints (from building_data merge)

| Column | Type | Description |
|--------|------|-------------|
| `imminently_dangerous_violation_count` | integer | Imminently dangerous code violations. |
| `unsafe_violation_count` | integer | Unsafe code violations. |
| `hazardous_violation_count` | integer | Hazardous code violations. |
| `heat_complaint_count` | integer | Heat-related complaints. |
| `fire_complaint_count` | integer | Fire-related complaints. |
| `structural_deficiency_complaint_count` | integer | Structural deficiency complaints. |
| `drainage_complaint_count` | integer | Drainage/plumbing complaints. |
| `total_complaints` | integer | Total complaints at PID in year. |
| `total_violations` | integer | Total violations at PID in year. |
| `total_permits` | integer | Total permits at PID in year. |

### Rental flags

| Column | Type | Description |
|--------|------|-------------|
| `rental_from_altos` | logical | PID appears in Altos listings. |
| `rental_from_license` | logical | PID has rental license. |
| `rental_from_evict` | logical | PID has eviction filing. |
| `ever_rental_any_year` | logical | Any rental evidence in any year. |
| `intensity_*` | numeric | Rental intensity scores by source and period. |

---

## analytic_sample

| Field | Value |
|-------|-------|
| **Config key** | `products.analytic_sample` |
| **Path** | `data/processed/analytic/analytic_sample.csv` |
| **Producing script** | `r/make-analytic-sample.R` |
| **Primary key** | `PID` x `year` |
| **Rows** | ~270,345 |
| **Columns** | 208 (same as bldg_panel_blp) |

Subset of `bldg_panel_blp` filtered to rows with non-missing rent, shares, and units, plus market quality filters. This is the estimation sample for BLP and price regulation regressions.

### Filter criteria (from make-analytic-sample.R)

- `!is.na(med_rent_altos)` — must have observed rent
- `!is.na(share_zip)` and `share_zip > 0` — must have valid market share
- `total_units >= 1` — must have units
- Market has >= 5 products (buildings) in the zip-year

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
| **Rows** | ~9,190,439 |
| **Columns** | 26 |
| **Years** | 2006-2022 |

All parcels (not just rentals) crossed with all years. Contains imputed unit counts and occupancy measures.

### Consumed by

- `r/make-analytic-sample.R` — base panel for bldg_panel_blp
- `r/impute-race-preflight.R` — GEOID lookup for eviction defendants
- `r/impute-race-infousa.R` — parcel-linked GEOID crosscheck

### Key columns

| Column | Type | Description |
|--------|------|-------------|
| `PID` | character | Parcel ID. |
| `year` | integer | Calendar year. |
| `parcel_number` | character | OPA parcel number (= PID). |
| `GEOID` | character | Block group GEOID from spatial join. |
| `total_units` | numeric | Imputed housing units (capped at 1.5x max_households). |
| `renter_occ` | numeric | Estimated renter-occupied units. |
| `occupancy_rate` | numeric | `renter_occ / total_units`. |
| `rental_occupancy_rate_scaled` | numeric | Scaled occupancy rate (0.9 mean in 2010 per bin). |
| `structure_bin` | character | Structure type bin. |
| `num_units_base` | numeric | Pre-cap unit count. |
| `max_households` | numeric | Maximum InfoUSA households ever observed. |
| `has_hh_data` | logical | Whether PID has any InfoUSA household data. |
| `ever_rental` | logical | Any rental evidence. |

---

## ever_rentals_panel

| Field | Value |
|-------|-------|
| **Config key** | `products.ever_rentals_panel` |
| **Path** | `data/processed/panels/ever_rentals_panel.csv` |
| **Producing script** | `r/make-rent-panel.R` |
| **Primary key** | `PID` x `year` |
| **Rows** | ~3,666,886 |
| **Columns** | 19 |

Rental-only panel: PIDs with any rental evidence (license, listing, or eviction) crossed with years. Contains rent levels, filing counts, and rental intensity measures.

### Consumed by

- `r/make-analytic-sample.R` — rent and filing data for bldg_panel_blp
- `r/make-occupancy-vars.r` — rental flags
- `r/impute-race-infousa.R` — rental PID filter

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `PID` | character | Parcel ID. |
| `year` | integer | Calendar year. |
| `med_rent_altos` | numeric | Median Altos listing rent. |
| `n_altos_listings` | integer | Number of Altos listings. |
| `lic_num_units` | integer | Units from rental license. |
| `num_filings` | integer | Eviction filings in year. |
| `num_filings_with_ha` | integer | Filings with housing authority. |
| `med_eviction_rent` | numeric | Median rent claimed in eviction filings. |
| `pm.zip` | character | Mailing ZIP (underscore-prefixed). |
| `rental_from_altos` | logical | Has Altos listing evidence. |
| `rental_from_license` | logical | Has rental license evidence. |
| `rental_from_evict` | logical | Has eviction filing evidence. |
| `ever_rental_any_year` | logical | Any rental evidence in any year. |
| `intensity_license_y` | numeric | License-based rental intensity. |
| `intensity_altos_y` | numeric | Altos-based rental intensity. |
| `intensity_evict_y` | numeric | Eviction-based rental intensity. |
| `intensity_evict_ha_y` | numeric | Housing authority eviction intensity. |
| `intensity_total_y` | numeric | Combined rental intensity. |
| `intensity_total_y_recent` | numeric | Recent-period rental intensity. |

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
| `hazardous_violation_count` | integer | Hazardous violations specifically. |
| `violation_*_count` | integer | Individual violation type counts. |
| `total_investigations` | integer | Total L&I case investigations. |
| `investigation_*_count` | integer | Individual investigation type counts. |

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
