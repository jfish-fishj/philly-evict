# ============================================================
# Philly: parcel-level unit & occupancy imputation with 2010 BG constraints
# - Rakes parcel units to match 2010 BG units-by-structure & total renters
# - Parcels built after 2010 are EXCLUDED from 2010 constraint weights
# - Keeps your InfoUSA-derived signals; adds constrained adjustments
# ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(sf)
  library(tidycensus)
  library(fixest)
  library(stringr)
})

`%||%` <- function(a,b) if (is.null(a) || is.na(a)) b else a

# -------------------------------
# 0) LOAD INPUTS (your file paths)
# -------------------------------
philly_infousa_dt <- fread("~/Desktop/data/philly-evict/infousa_address_cleaned.csv")
philly_bldgs      <- fread('~/Desktop/data/philly-evict/processed/parcel_building_summary_2024.csv')
philly_parcels    <- fread("~/Desktop/data/philly-evict/processed/parcel_building_2024.csv")
info_usa_xwalk    <- fread("~/Desktop/data/philly-evict/philly_infousa_dt_address_agg_xwalk.csv")
philly_rent_df    <- fread("~/Desktop/data/philly-evict/processed/bldg_panel.csv")
philly_rentals    <- fread("~/Desktop/data/philly-evict/processed/license_long_min.csv")
tenure_bg_2010    <- fread('/Users/joefish/Desktop/data/philly-evict/census/tenure_bg_2010.csv')
uis_tr_2010       <- fread("~/Desktop/data/philly-evict/census/uis_tr_2010.csv")
philly_evict_xwalk     <- fread("~/Desktop/data/philly-evict/philly_evict_address_agg_xwalk.csv")
philly_evict = fread("/Users/joefish/Desktop/data/philly-evict/evict_address_cleaned.csv")
# If you *haven't* built these yet, keep your previous code to fetch them.
# Expected columns:
# tenure_bg_2010: GEOID (BG), occ_total, renter_occ
# uis_tr_2010:   tract_geoid, u_1_detached,...,u_50plus_units, u_mobile_home, u_boat_rv_other, hu_total
stopifnot(exists("tenure_bg_2010"), exists("uis_tr_2010"))

# Normalize key columns
setDT(philly_infousa_dt)
setDT(philly_parcels)
setDT(info_usa_xwalk)
setDT(philly_rent_df)
setDT(philly_rentals)

philly_infousa_dt[, obs_id := .I]
philly_infousa_dt[, family_id := coalesce(family_id, familyid)]
philly_parcels[, PID := as.numeric(PID)]
philly_rentals[, PID := as.numeric(PID)]
philly_rent_df[,  PID := as.numeric(PID)]

# --------------------------------
# 1) BUILD INFOUSA x PARCEL MERGES
# --------------------------------
# do cuts to match census categories 1;2;3-4;5-9p;10-19;20-49;50+
philly_rentals[, num_units_cut := cut(
  numberofunits, breaks = c(-Inf,1,2,4,9,19,49,Inf),
  labels = c("1","2","3-4","5-9","10-19","20-49","50+")
)]

# Eviction aggregates (PID-level)
philly_evict_df <- philly_rent_df[, .(
  filing_rate = mean(filing_rate, na.rm = TRUE),
  total_filings = sum(num_filings, na.rm = TRUE)
), by = PID]

# Keep InfoUSA rows that match exactly one parcel
xwalk_1 <- info_usa_xwalk[num_parcels_matched == 1 & !is.na(PID)]

# Attach parcel PID to InfoUSA
infousa_m <- merge(
  philly_infousa_dt,
  xwalk_1[, .(n_sn_ss_c, PID)],
  by = "n_sn_ss_c",
  all.x = TRUE
)

# Homeowner flag from InfoUSA
infousa_m[, home_owner := owner_renter_status %in% c(7,8,9)]

# Merge eviction features
infousa_m <- merge(infousa_m, philly_evict_df, by = "PID", all.x = TRUE)

# -------------------------------------------
# 2) PARCEL FEATURES & INITIAL UNIT IMPUTATION
# -------------------------------------------
# story imputations by building code group
philly_parcels[, num_stories_mean := mean(number_stories, na.rm = TRUE), by = building_code_description_new_fixed]
philly_parcels[, num_stories_imp  := coalesce(number_stories, num_stories_mean)]

# merge on bldg agg
philly_bldgs_agg = philly_bldgs[,list(
  num_bldgs = .N,
  total_sq_ft = sum(square_ft,na.rm=T),
  mean_height = mean(max_hgt,na.rm = T)

), by = .(PID)]

philly_parcels = merge(
  philly_parcels,
  philly_bldgs_agg,
  by = "PID", all.x = TRUE
)
philly_parcels[,.N, by = is.na(num_bldgs)][order(is.na)]
# fill in mean num bldgs by building code group
philly_parcels[, num_bldgs_mean := mean(num_bldgs, na.rm = TRUE), by = building_code_description_new_fixed]
philly_parcels[, num_bldgs_imp  := coalesce(num_bldgs, num_bldgs_mean)]

# HOUSEHOLD COUNTS by parcel-year (InfoUSA)
infousa_m[, num_households := .N, by = .(PID, year)]
infousa_m[, last_name_cat  := .GRP, by = last_name_1]
infousa_m[, num_years_in_sample := .N, by = family_id]
infousa_m[, first_year_in_sample := min(year) == year, by = family_id]
infousa_m[, first_year_at_parcel := min(year) == year, by = .(family_id, PID)]
infousa_m[, last_year_at_parcel  := max(year) == year, by = .(family_id, PID)]
infousa_m[, num_households_g1    := sum(num_years_in_sample > 1), by = .(PID, year)]
infousa_m[, max_households       := max(num_households, na.rm = TRUE), by = PID]

# Merge parcel attributes onto InfoUSA rows (for modeling)
infousa_m <- merge(
  infousa_m[, !grepl("^parcels", names(infousa_m)), with = FALSE],
  philly_parcels,
  by = "PID", all.x = TRUE, suffixes = c("", "_parcels")
)

# Prepare data for unit-model
parcel_agg_for_model <- merge(
  philly_parcels,
  infousa_m[, .(
    max_households = max(num_households, na.rm=TRUE),
    median_households = median(num_households, na.rm=TRUE),
    mean_households = mean(num_households, na.rm=TRUE)
  ), by = PID],
  by = "PID", all.x = TRUE
)
# parcel_agg_for_model[,max_households := replace_na(max_households, 0)]
# parcel_agg_for_model[,median_households := replace_na(median_households, 0)]
# parcel_agg_for_model[,mean_households   := replace_na(mean_households, 0)]

# Drop suspicious reported units (only for small buildings, big disagreement)
parcel_agg_for_model[, num_units_fixed := fifelse(
  !is.na(num_units) & !is.na(max_households) &
    (abs(num_units - max_households) > 100) & num_units <= 20,
  NA_integer_, num_units
)]
parcel_agg_for_model[,year_built_decade := floor(year_built / 10) * 10]
# Fit units model
units_model <- feols(
  num_units_fixed ~ total_area + I(total_area^2) + total_livable_area + I(total_livable_area^2) +
    median_households + I(median_households^2) + num_bldgs_imp + I(num_bldgs_imp^2) +
    #mean_height + I(mean_height^2) +
    num_stories_imp + I(num_stories_imp^2) +
    quality_grade_fixed + building_code_description_new_fixed + year_built_decade,
  data = parcel_agg_for_model, combine.quick = FALSE
)
summary(units_model)
# predict model; merge back onto info_usa_m
parcel_agg_for_model[, num_units_pred_new := predict(units_model, newdata = parcel_agg_for_model)]

parcel_agg_for_model[,list(mean(is.na(num_bldgs_imp)),.N), by = building_code_description_new_fixed]
parcel_agg_for_model[,list(mean(is.na(median_households)),.N), by = building_code_description_new_fixed]
parcel_agg_for_model[,list(mean(is.na(num_units_pred_new)),.N), by = building_code_description_new_fixed]

# Choose imputed units
parcel_agg_for_model[, num_units_imp := fifelse(is.na(num_units), num_units_pred_new, round(num_units))]
parcel_agg_for_model[, num_units_imp := fifelse(num_units_imp <= 0, 1, num_units_imp)]
# Harmonize extreme gaps
parcel_agg_for_model[, num_units_imp := fifelse(
  !is.na(num_units) &
    abs(num_units_pred_new - num_units) > 50 &
    abs(max_households - num_units) > 50,
  round(num_units_pred_new/2), num_units_imp
)]

parcel_agg_for_model[, num_units_imp := fifelse((num_units_imp - max_households) > 100, NA_integer_, num_units_imp)]
parcel_agg_for_model[, num_units_imp := fifelse((num_units_imp / pmax(1, max_households)) > 3, NA_integer_, num_units_imp)]

parcel_agg_for_model[,.N, by = is.na(num_units_imp)][order(is.na)]

parcel_agg_for_model[,mean(is.na(num_units_imp)), by = building_code_description_new_fixed]

(parcel_agg_for_model[num_bldgs_mean > 0,list(mean(num_units_imp / num_bldgs_mean,na.rm=T),
                                              median(num_units_imp / num_bldgs_mean,na.rm=T),
                                              max((num_units_imp / num_bldgs_mean),na.rm=T),
                                              .N,
                                              mean(num_bldgs,na.rm=T)),
                      by = building_code_description_new_fixed])
parcel_agg_for_model[,.N, by = is.na(num_bldgs_imp)][order(is.na)]

# -------------------------------------------
# 3) 2010 BG TARGETS: STRUCTURE & RENTER COUNTS
# -------------------------------------------
tenure_bg_2010 <- as.data.table(tenure_bg_2010)
tenure_bg_2010[, tract_geoid := str_sub(GEOID, 1, 11)]
bg_w <- tenure_bg_2010[, .(GEOID, tract_geoid, occ_total, renter_occ)]
bg_w[, tract_occ := sum(occ_total, na.rm=TRUE), by = tract_geoid]
bg_w[, w_bg := fifelse(tract_occ > 0, occ_total / tract_occ, 0)]
bg_w[,tract_geoid := as.numeric(tract_geoid)]
uis_tr_2010_dt <- as.data.table(uis_tr_2010)
uis_long <- melt(
  uis_tr_2010_dt,
  id.vars = "tract_geoid",
  measure.vars = patterns("^u_"),
  variable.name = "struct_var",
  value.name = "tract_units_k"
)


bg_targets_wide <- merge(bg_w, uis_tr_2010_dt, by = "tract_geoid", all.x = TRUE) %>%
  select(-matches("M$"))
bg_targets_long <- melt(
  bg_targets_wide,
  #id.vars = "tract_geoid",
  measure.vars = patterns("^u_"),
  variable.name = "struct_var",
  value.name = "tract_units_k"
)
bg_targets_long[, bg_units_k := w_bg * tract_units_k]

bg_targets <- dcast(
  bg_targets_long[, .(GEOID, struct_var, bg_units_k)],
  GEOID ~ struct_var, value.var = "bg_units_k", fun.aggregate = sum
)
bg_targets <- merge(bg_targets, bg_w[, .(GEOID, renter_occ, occ_total)], by = "GEOID", all.x = TRUE)
bg_targets[, hu_bg_target := rowSums(.SD, na.rm = TRUE), .SDcols = patterns("^u_")]

# ---------------------------------------------------
# 4) MAP PARCELS TO STRUCTURE BINS (B25024 categories)
# ---------------------------------------------------

parcel_agg_for_model[,building_code_description_new_fixed := str_to_lower(building_code_description_new_fixed)]
parcel_agg_for_model[, structure_bin := case_when(
  str_detect(building_code_description_new_fixed, regex("single|detached", ignore_case=TRUE)) ~ "u_1_detached",
  str_detect(building_code_description_new_fixed, regex("row|town|attached|condo", ignore_case=TRUE)) ~ "u_1_attached",
  str_detect(building_code_description_new_fixed, regex("twin|old style", ignore_case=TRUE)) ~ "u_2_units",
  str_detect(building_code_description_new_fixed, regex("3|4 unit|triplex|quad", ignore_case=TRUE)) ~ "u_3_4_units",
  str_detect(building_code_description_new_fixed, regex("apartments other", ignore_case=TRUE)) ~ "u_5_9_units",
  str_detect(building_code_description_new_fixed, regex("garden|low rise", ignore_case=TRUE)) ~ "u_10_19_units",
  str_detect(building_code_description_new_fixed, regex("mid rise", ignore_case=TRUE)) ~ "u_20_49_units",
  str_detect(building_code_description_new_fixed, regex("50\\+|high rise|tower", ignore_case=TRUE)) | (num_stories_imp >= 6) ~ "u_50plus_units",
  str_detect(building_code_description_new_fixed, regex("other", ignore_case=TRUE)) | (num_stories_imp >= 6) ~ "u_50plus_units",
  TRUE ~ NA_character_
)]
parcel_agg_for_model[building_code_description_new_fixed=="other",structure_bin := case_when(
  num_units_imp <= 1 & num_bldgs_imp == 1 ~ "u_1_attached",
  num_units_imp <= 2 & num_bldgs_imp == 1 ~ "u_2_units",
  num_units_imp <= 4 & num_bldgs_imp == 1 ~ "u_3_4_units",
  num_units_imp <= 9 & num_bldgs_imp == 1 ~ "u_5_9_units",
  num_units_imp <= 19 & num_bldgs_imp == 1 ~ "u_10_19_units",
  num_units_imp <= 49 & num_bldgs_imp == 1 ~ "u_20_49_units",
  num_units_imp >= 50 | num_bldgs_imp > 1 ~ "u_50plus_units",
)]
parcel_agg_for_model[,sum(num_units_imp,na.rm=T), by=(structure_bin)]
parcel_agg_for_model[,sum(num_units_imp,na.rm=T), by=.(building_code_description_new_fixed,structure_bin)]
parcel_agg_for_model[,sum(num_units_imp,na.rm=T)]
#
# # make building agg (handles parcels w/ multiple bldgs)
parcel_building = merge(
  parcel_agg_for_model[!is.na(structure_bin)],
  philly_bldgs[, .(PID, max_hgt, square_ft)],
  by = "PID", all.x = TRUE
)

# divide units evenly amongst buildings
parcel_building[,num_units_per_bldg := num_units_imp / .N, by = PID]
# divide hhs evenly amongst buildings
parcel_building[,max_households_per_bldg := fifelse(!is.na(max_households), max_households / .N, NA_real_), by = PID]
# --------------------------------------------
# 5) BUILD PARCEL TABLE (one row per PID, 2010)
#     and EXCLUDE post-2010 parcels from weights
# --------------------------------------------
# Build a per-PID record including 2010 BG & year_built
# parcel_base <- infousa_m[, .(
#   PID,
#   GEOID = first(GEOID),
#   year_built = first(year_built),
#   num_units_imp = first(num_units_imp),
#   num_households = first(num_households),
#   max_households = first(max_households),
#   total_filings = first(total_filings),
#   filing_rate   = first(filing_rate),
#   mean_occupancy_rate = mean(1.0, na.rm = TRUE) # placeholder if you keep your time-varying calc
# ), by = PID]

parcel_base <- parcel_building

# ---- Year cutoff logic:
# Treat parcels built AFTER 2010 as "post-2010" -> they should NOT take 2010 BG weight mass.
parcel_base[, post2010 := !is.na(year_built) & year_built > 2010]

# Join BG targets
parcel_bg <- merge(parcel_base, bg_targets, by = "GEOID", all.x = F)

# --------------------------------------------
# 6) RAKING UNITS BY STRUCTURE WITH YEAR CUTOFF
# --------------------------------------------
scale_to_target <- function(x, target, floor_vec=NULL, cap_vec=NULL) {
  if (all(is.na(x)) || is.na(target)) return(x)
  s <- sum(x, na.rm = TRUE)
  if (s == 0) return(x)
  y <- x * (target / s)
  if (!is.null(floor_vec)) y <- pmax(y, floor_vec)
  if (!is.null(cap_vec))   y <- pmin(y, cap_vec)
  s2 <- sum(y, na.rm = TRUE)
  if (s2 > 0 && !is.na(target)) y <- y * (target / s2)
  y
}

# Floors/caps
parcel_bg[, floor_units := pmax(1, as.numeric(max_households_per_bldg))]
parcel_bg[, cap_units   := pmax(floor_units, num_units_per_bldg * 2)]

# Bins we will rake
bins <- grep("^u_", names(bg_targets), value = TRUE)

# For each BG & bin:
#   • Compute weights using ONLY parcels with post2010==FALSE (built ≤ 2010)
#   • Assign raked units to those parcels to meet the bin target
#   • Parcels built after 2010 keep their original num_units_imp (don’t consume 2010 target mass)
parcel_bg[, num_units_raked := NA_real_]

for (b in bins) {
  # target (per BG)
  # raking group: parcels built ≤ 2010 with this structure bin
  # non-raking group: post-2010 OR unmatched structure -> keep baseline
  parcel_bg[
    structure_bin == b & post2010 == FALSE,
    num_units_raked := scale_to_target(
      x = num_units_per_bldg,
      target = unique(get(b)),
      floor_vec = floor_units,
      cap_vec   = cap_units
    ),
    by = GEOID
  ]
}

# For parcels not in raking sets: keep baseline (num_units_imp)
parcel_bg[is.na(num_units_raked), num_units_raked := num_units_per_bldg]

# Optional second pass to match BG total HU among ≤2010 parcels while leaving post-2010 untouched
parcel_bg[, by_bg_sum_2010 := sum(num_units_raked[post2010 == FALSE], na.rm=TRUE), by = GEOID]
parcel_bg[
  post2010 == FALSE & !is.na(hu_bg_target) & by_bg_sum_2010 > 0,
  num_units_raked := num_units_raked * (hu_bg_target / by_bg_sum_2010),
  by = GEOID
]
parcel_bg[,.N, by = is.na(floor_units)]
parcel_bg[,.N, by = is.na(cap_units)]
parcel_bg[,.N, by = .(is.na(num_units_raked) | is.na(floor_units) | is.na(cap_units))]
parcel_bg[post2010==F,.N, by = .(is.na(num_units_raked) | is.na(floor_units) | is.na(cap_units))]
# Final units with bounds
parcel_bg[, num_units_final := pmin(pmax(num_units_raked, floor_units), cap_units)]
View(parcel_bg[is.na(num_units_final) | num_units_final <= 0, .(PID, GEOID, year_built, post2010, num_units_per_bldg,num_units_imp,
                                                                num_units_raked, floor_units, cap_units, num_units_final)])

# ---------------------------------------------------
# 7) ALLOCATE 2010 RENTER TOTALS ACROSS PARCELS (≤2010)
#     Post-2010 parcels get renters = 0 in 2010 snap
# ---------------------------------------------------
# Simple propensity using licenses, size, evictions
parcel_bg[, has_license := as.integer(PID %in% philly_rentals$PID)]
parcel_bg[, big_multi   := as.integer(num_units_final >= 5)]
parcel_bg[, rental_flag  := as.integer((PID %in% philly_rentals$PID))]

parcel_bg[, p_raw := 0.4*has_license + 0.3*big_multi + 0.3*rental_flag]
parcel_bg[, p_raw := fifelse(is.na(p_raw), 0, p_raw)]
parcel_bg[, p     := pmax(0.05, pmin(0.95, p_raw))]

# Prior occupancy level (if you keep your historical calc, merge it; here use neutral)
parcel_bg[, occ_prior := 0.9]

# Only parcels built ≤2010 participate in the 2010 renter constraint
parcel_bg[, renters_raw := fifelse(post2010 == FALSE, num_units_final * occ_prior * p, 0)]
parcel_bg[, sum_raw := sum(renters_raw, na.rm=TRUE), by = GEOID]
parcel_bg[, renters_scaled := fifelse(sum_raw > 0 & !is.na(renters_raw), renters_raw * (renter_occ / sum_raw), 0)]

# Floors/caps & renormalize to renter_occ
parcel_bg[, renter_floor := replace_na(mean_households,0)]  # if you trust owner flags, subtract owners
parcel_bg[, renter_cap   := replace_na(num_units_final,0)]

parcel_bg[, renters_final := pmin(pmax(renters_scaled, renter_floor), renter_cap)]
parcel_bg[, sum_final := sum(renters_final, na.rm=TRUE), by = GEOID]
parcel_bg[, renters_final := fifelse(sum_final > 0, renters_final * (renter_occ / sum_final), renters_final)]

# Derived 2010 occupancy rate (clip)
parcel_bg[, occupancy_rate_2010 := fifelse(num_units_final > 0, renters_final / num_units_final, NA_real_)]
parcel_bg[, occupancy_rate_2010 := pmin(pmax(occupancy_rate_2010, 0.25), 1.00)]

# ---------------------------------------------
# 8) MERGE BACK TO YOUR PANEL & EXPORT SNAPSHOT
# ---------------------------------------------
# Keep constrained 2010 snapshot per parcel
parcel_keep_2010 <- parcel_bg[, .(
  PID, GEOID, year_built, post2010,
  num_units_final_2010 = round(num_units_final),
  renters_final_2010   = round(renters_final),
  occupancy_rate_2010
)]

# If you want to carry these as anchors into your panel:
#  - For years <= 2010, prefer constrained values (for ≤2010 parcels)
#  - For years > 2010, you can blend to your time-varying series

# Build your annual panel grid (as you had)
parcel_grid <- CJ(
  year = 2006:2023,
  PID  = sort(unique(infousa_m$PID))
)


panel <- merge(parcel_grid, parcel_static, by = "PID", all.x = TRUE)
panel <- merge(panel, parcel_keep_2010, by = c("PID","GEOID","year_built"), all.x = TRUE)

# Your InfoUSA-based yearly outcomes
yearly_infousa <- infousa_m[, .(
  num_households        = first(num_households),
  num_households_g1     = first(num_households_g1),
  num_homeowners        = sum(home_owner, na.rm=TRUE),
  max_households        = first(max_households),
  total_filings         = first(total_filings),
  filing_rate           = first(filing_rate),
  n_sn_ss_c             = first(n_sn_ss_c)
), by = .(PID, year)]

panel <- merge(panel, yearly_infousa, by = c("PID","year"), all.x = TRUE)

# Apply year_built validity
panel <- panel[year >= year_built]

# Use constrained units for years <= 2010 on ≤2010 parcels; else fallback to imputed
panel[, num_units_imp_final :=
        fifelse(!is.na(num_units_final_2010) & year <= 2010 & (is.na(year_built) | year_built <= 2010),
                num_units_final_2010,
                num_units_imp)]

# Define renters series:
#   For years <= 2010 (≤2010 parcels), use constrained renters; after 2010, keep your original logic or extrapolate.
panel[, renters_imputed :=
        fifelse(!is.na(renters_final_2010) & year <= 2010 & (is.na(year_built) | year_built <= 2010),
                renters_final_2010,
                pmin(num_households %||% 0, num_units_imp_final %||% 0))
]

# Occupancy rate
panel[, occupancy_rate := fifelse(num_units_imp_final > 0, renters_imputed / num_units_imp_final, NA_real_)]
panel[, occupancy_rate := pmin(pmax(occupancy_rate, 0.25), 1.00)]

# Optional trims/diagnostics like in your script
panel[, parcel_exists := year > year_built]
panel[, old_parcel    := year > (year_built + 5)]
panel[, num_units_cuts := cut(
  num_units_imp_final,
  breaks = c(-Inf,2,5,10,20,50,Inf),
  labels = c("1-2","3-5","6-10","11-20","21-50","50+"),
  include.lowest = TRUE
)]

# -----------------
# 9) EXPORT RESULTS
# -----------------
fwrite(panel, "~/Desktop/data/philly-evict/processed/infousa_parcel_occupancy_vars_constrained.csv")

# -------------
# 10) LOGGING
# -------------
message("Rows in panel: ", nrow(panel))
message("Parcels (unique): ", uniqueN(panel$PID))
message("Parcels post-2010 (excluded from 2010 weights): ", panel[!is.na(year_built) & year_built > 2010, uniqueN(PID)])
message("Share of BGs with targets: ", round(mean(!is.na(panel$GEOID) & panel$GEOID %in% bg_targets$GEOID), 3))
