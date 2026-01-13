# ============================================================
# Philly: occupancy / renters with 2010 BG constraints
#  - Reuses units from ever-rentals script (parcel_units)
#  - Rakes those units to match 2010 BG structure counts & renters
#  - Builds parcel-year occupancy panel, informed by ever-rentals
# ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(sf)
  library(tidycensus)
  library(stringr)
})

`%||%` <- function(a, b) if (is.null(a) || all(is.na(a))) b else a
normalize_pid <- function(x) stringr::str_pad(as.character(x), 9, "left", "0")

scale_to_target <- function(x, target, floor_vec = NULL, cap_vec = NULL) {
  # Simple proportional scaling with optional floor/cap and re-normalization
  if (all(is.na(x)) || is.na(target)) return(x)
  s <- sum(x, na.rm = TRUE)
  if (s <= 0 || is.na(s)) return(x)
  y <- x * (target / s)
  if (!is.null(floor_vec)) y <- pmax(y, floor_vec)
  if (!is.null(cap_vec))   y <- pmin(y, cap_vec)
  s2 <- sum(y, na.rm = TRUE)
  if (s2 > 0 && !is.na(target)) y <- y * (target / s2)
  y
}

# -------------------------------
# 0) FILE PATHS & INPUTS
#    (edit to your actual locations)
# -------------------------------
ROOT      <- "~/Desktop/data/philly-evict"
PROC      <- file.path(ROOT, "processed")
CENSUSDIR <- file.path(ROOT, "census")

path_infousa        <- file.path(ROOT,  "infousa_address_cleaned.csv")
path_parcels_2024   <- file.path(PROC,  "parcel_building_2024.csv")
path_bldg_summary   <- file.path(PROC,  "parcel_building_summary_2024.csv")
path_info_xwalk     <- file.path(ROOT,  "philly_infousa_dt_address_agg_xwalk.csv")

path_tenure_bg_2010 <- file.path(CENSUSDIR, "tenure_bg_2010.csv")
path_uis_tr_2010    <- file.path(CENSUSDIR, "uis_tr_2010.csv")

## Outputs from ever-rentals script
path_ever_panel     <- file.path(PROC, "ever_rentals_panel.csv")
path_parcel_units   <- file.path(PROC, "ever_rental_parcel_units.csv")
# (Optional) if you exported PID-level summary; not strictly needed:
# path_ever_pid    <- file.path(PROC, "ever_rentals_pid.csv")

# -------------------------------
# 1) LOAD DATA
# -------------------------------
philly_infousa_dt <- fread(path_infousa)
philly_bldgs      <- fread(path_bldg_summary)
philly_parcels    <- fread(path_parcels_2024)
info_usa_xwalk    <- fread(path_info_xwalk)

tenure_bg_2010    <- fread(path_tenure_bg_2010)
uis_tr_2010       <- fread(path_uis_tr_2010)

ever_rentals_panel <- fread(path_ever_panel)
parcel_units       <- fread(path_parcel_units)

setDT(philly_infousa_dt)
setDT(philly_parcels)
setDT(philly_bldgs)
setDT(info_usa_xwalk)
setDT(tenure_bg_2010)
setDT(uis_tr_2010)
setDT(ever_rentals_panel)
setDT(parcel_units)

# Normalize PIDs to 9-char strings everywhere
philly_parcels[,  PID := normalize_pid(PID)]
philly_bldgs[,    PID := normalize_pid(PID)]
info_usa_xwalk[,  PID := normalize_pid(PID)]
ever_rentals_panel[, PID := normalize_pid(PID)]
parcel_units[,    PID := normalize_pid(PID)]

philly_infousa_dt[, obs_id := .I]

# -------------------------------
# 2) INFOUSA × PARCEL MERGES
# -------------------------------
xwalk_1 <- info_usa_xwalk[num_parcels_matched == 1 & !is.na(PID)]

infousa_m <- merge(
  philly_infousa_dt,
  xwalk_1[, .(n_sn_ss_c, PID)],
  by    = "n_sn_ss_c",
  all.x = TRUE
)

# Basic household / tenure summaries
infousa_m[, home_owner := owner_renter_status %in% c(7, 8, 9)]

infousa_m[, num_households := .N, by = .(PID, year)]
infousa_m[, num_years_in_sample := .N, by = family_id]
infousa_m[, first_year_in_sample := min(year) == year, by = family_id]
infousa_m[, first_year_at_parcel := min(year) == year, by = .(family_id, PID)]
infousa_m[, last_year_at_parcel  := max(year) == year, by = .(family_id, PID)]
infousa_m[, num_households_g1    := sum(num_years_in_sample > 1), by = .(PID, year)]
infousa_m[, max_households       := max(num_households, na.rm = TRUE), by = PID]

# Parcel-level household stats for occupancy logic
hh_agg <- infousa_m[!is.na(PID),
                    .(
                      max_households    = max(num_households, na.rm = TRUE),
                      median_households = median(num_households, na.rm = TRUE),
                      mean_households   = mean(num_households, na.rm = TRUE)
                    ),
                    by = PID
]

# -------------------------------
# 3) PARCEL FEATURES + UNITS (from ever-rentals)
# -------------------------------
# Building-level aggregates
philly_bldgs_agg <- philly_bldgs[
  ,
  .(
    num_bldgs   = .N,
    total_sq_ft = sum(square_ft, na.rm = TRUE),
    mean_height = mean(max_hgt,   na.rm = TRUE)
  ),
  by = PID
]

philly_parcels <- merge(
  philly_parcels,
  philly_bldgs_agg,
  by    = "PID",
  all.x = TRUE
)

# Story imputations by building code group
philly_parcels[
  ,
  num_stories_mean := mean(number_stories, na.rm = TRUE),
  by = building_code_description_new_fixed
]
philly_parcels[
  ,
  num_stories_imp := fifelse(
    !is.na(number_stories),
    number_stories,
    num_stories_mean
  )
]

# Impute number of buildings if missing
philly_parcels[
  ,
  num_bldgs_mean := mean(num_bldgs, na.rm = TRUE),
  by = building_code_description_new_fixed
]
philly_parcels[
  ,
  num_bldgs_imp := fifelse(
    !is.na(num_bldgs),
    num_bldgs,
    num_bldgs_mean
  )
]

# Merge household stats
parcel_agg <- merge(
  philly_parcels,
  hh_agg,
  by    = "PID",
  all.x = TRUE
)

# Merge units from ever-rentals (canonical units; no re-imputation here)
parcel_units[, PID := normalize_pid(PID)]

parcel_agg <- merge(
  parcel_agg,
  parcel_units[, .(PID, num_units_imp_base = num_units_imp,
                   num_units_raw, num_units_pred,
                   max_households_units = max_households,
                   median_households_units = median_households)],
  by    = "PID",
  all.x = TRUE
)

# For occupancy logic, use num_units_imp_base as starting units
parcel_agg[, num_units_imp := num_units_imp_base]

# Year built decade (for diagnostics; not used for re-imputation)
parcel_agg[, year_built_decade := floor(year_built / 10) * 10]

# -------------------------------
# 4) 2010 BG TARGETS: UNITS & RENTERS
# -------------------------------
tenure_bg_2010[, tract_geoid := str_sub(GEOID, 1, 11)]

bg_w <- tenure_bg_2010[
  ,
  .(GEOID, tract_geoid, occ_total, renter_occ)
]

bg_w[
  ,
  tract_occ := sum(occ_total, na.rm = TRUE),
  by = tract_geoid
]
bg_w[
  ,
  w_bg := fifelse(tract_occ > 0, occ_total / tract_occ, 0)
]
bg_w[, tract_geoid := as.numeric(tract_geoid)]

uis_tr_2010_dt <- as.data.table(uis_tr_2010)

# Wide BG targets: units by structure + renter totals
bg_targets_wide <- merge(
  bg_w,
  uis_tr_2010_dt,
  by    = "tract_geoid",
  all.x = TRUE
) %>%
  select(-matches("M$"))  # drop margins of error cols if present

bg_targets_long <- melt(
  bg_targets_wide,
  id.vars      = c("GEOID", "tract_geoid", "w_bg", "occ_total", "renter_occ"),
  measure.vars = patterns("^u_"),
  variable.name = "struct_var",
  value.name    = "tract_units_k"
)

bg_targets_long[
  ,
  bg_units_k := w_bg * tract_units_k
]

bg_targets <- dcast(
  bg_targets_long[, .(GEOID, struct_var, bg_units_k)],
  GEOID ~ struct_var,
  value.var     = "bg_units_k",
  fun.aggregate = sum
)

bg_targets <- merge(
  bg_targets,
  bg_w[, .(GEOID, renter_occ, occ_total)],
  by    = "GEOID",
  all.x = TRUE
)

bg_targets[
  ,
  hu_bg_target := rowSums(.SD, na.rm = TRUE),
  .SDcols = patterns("^u_")
]

# -------------------------------
# 5) STRUCTURE BINS & PARCEL-BUILDING TABLE
# -------------------------------
parcel_agg[
  ,
  building_code_description_new_fixed :=
    stringr::str_to_lower(building_code_description_new_fixed)
]

parcel_agg[
  ,
  structure_bin := case_when(
    str_detect(building_code_description_new_fixed, "single|detached") ~ "u_1_detached",
    str_detect(building_code_description_new_fixed, "row|town|attached|condo") ~ "u_1_attached",
    str_detect(building_code_description_new_fixed, "twin|old style") ~ "u_2_units",
    str_detect(building_code_description_new_fixed, "3|4 unit|triplex|quad") ~ "u_3_4_units",
    str_detect(building_code_description_new_fixed, "apartments other") ~ "u_5_9_units",
    str_detect(building_code_description_new_fixed, "garden|low rise") ~ "u_10_19_units",
    str_detect(building_code_description_new_fixed, "mid rise") ~ "u_20_49_units",
    str_detect(building_code_description_new_fixed, "50\\+|high rise|tower") |
      num_stories_imp >= 6 ~ "u_50plus_units",
    str_detect(building_code_description_new_fixed, "other") ~ NA_character_,
    TRUE ~ NA_character_
  )
]

# For "other", back out from units/bldgs
parcel_agg[
  building_code_description_new_fixed == "other",
  structure_bin := case_when(
    num_units_imp <= 1 & num_bldgs_imp == 1 ~ "u_1_attached",
    num_units_imp <= 2 & num_bldgs_imp == 1 ~ "u_2_units",
    num_units_imp <= 4 & num_bldgs_imp == 1 ~ "u_3_4_units",
    num_units_imp <= 9 & num_bldgs_imp == 1 ~ "u_5_9_units",
    num_units_imp <= 19 & num_bldgs_imp == 1 ~ "u_10_19_units",
    num_units_imp <= 49 & num_bldgs_imp == 1 ~ "u_20_49_units",
    num_units_imp >= 50 | num_bldgs_imp > 1  ~ "u_50plus_units"
  )
]

# Parcel-building table (replicate parcel info across buildings)
parcel_building <- merge(
  parcel_agg[!is.na(structure_bin)],
  philly_bldgs[, .(PID, max_hgt, square_ft)],
  by    = "PID",
  all.x = TRUE
)

parcel_building[
  ,
  num_units_per_bldg := num_units_imp / .N,
  by = PID
]
parcel_building[
  ,
  max_households_per_bldg :=
    fifelse(!is.na(max_households),
            max_households / .N,
            NA_real_),
  by = PID
]

# -------------------------------
# 6) RAKING UNITS BY BG & STRUCTURE (≤2010)
# -------------------------------
# Use existing units, but constrain to 2010 BG totals

parcel_building[
  ,
  post2010 := !is.na(year_built) & year_built > 2010
]

parcel_bg <- merge(
  parcel_building,
  bg_targets,
  by    = "GEOID",
  all.x = TRUE
)

parcel_bg[
  ,
  floor_units := pmax(1, as.numeric(max_households_per_bldg))
]
parcel_bg[
  ,
  cap_units   := pmax(floor_units, num_units_per_bldg * 2)
]

bins <- grep("^u_", names(bg_targets), value = TRUE)
parcel_bg[, num_units_raked := NA_real_]

for (b in bins) {
  parcel_bg[
    structure_bin == b & post2010 == FALSE,
    num_units_raked := scale_to_target(
      x         = num_units_per_bldg,
      target    = unique(get(b)),
      floor_vec = floor_units,
      cap_vec   = cap_units
    ),
    by = GEOID
  ]
}

parcel_bg[is.na(num_units_raked), num_units_raked := num_units_per_bldg]

parcel_bg[
  ,
  by_bg_sum_2010 := sum(num_units_raked[post2010 == FALSE], na.rm = TRUE),
  by = GEOID
]

parcel_bg[
  post2010 == FALSE & !is.na(hu_bg_target) & by_bg_sum_2010 > 0,
  num_units_raked := num_units_raked * (hu_bg_target / by_bg_sum_2010),
  by = GEOID
]

parcel_bg[
  ,
  num_units_final := pmin(pmax(num_units_raked, floor_units), cap_units)
]

# -------------------------------
# 7) USE EVER-RENTALS TO ALLOCATE 2010 RENTERS
# -------------------------------
# PID-level rental evidence from ever_rentals_panel
ever_pid_stats <- ever_rentals_panel[
  ,
  .(
    ever_rental_any      = as.integer(any(ever_rental_any_year, na.rm = TRUE)),
    ever_rental_altos    = as.integer(any(rental_from_altos,   na.rm = TRUE)),
    ever_rental_license  = as.integer(any(rental_from_license, na.rm = TRUE)),
    ever_rental_evict    = as.integer(any(rental_from_evict,   na.rm = TRUE))
  ),
  by = PID
]

parcel_bg <- merge(
  parcel_bg,
  ever_pid_stats,
  by    = "PID",
  all.x = TRUE
)

parcel_bg[is.na(ever_rental_any),     ever_rental_any     := 0L]
parcel_bg[is.na(ever_rental_altos),   ever_rental_altos   := 0L]
parcel_bg[is.na(ever_rental_license), ever_rental_license := 0L]
parcel_bg[is.na(ever_rental_evict),   ever_rental_evict   := 0L]

parcel_bg[
  ,
  has_license := ever_rental_license
]
parcel_bg[
  ,
  big_multi := as.integer(num_units_final >= 5)
]

# Basic propensity: license + size + "any rental trace"
parcel_bg[
  ,
  p_raw := 0.4 * has_license + 0.3 * big_multi + 0.3 * ever_rental_any
]
parcel_bg[
  ,
  p_raw := fifelse(is.na(p_raw), 0, p_raw)
]
parcel_bg[
  ,
  p := pmax(0.05, pmin(0.95, p_raw))
]

# Prior occupancy level (can be updated later)
parcel_bg[, occ_prior := 0.9]

# Only parcels built ≤2010 participate in 2010 renter constraint
parcel_bg[
  ,
  renters_raw := fifelse(
    post2010 == FALSE,
    num_units_final * occ_prior * p,
    0
  )
]
parcel_bg[
  ,
  sum_raw := sum(renters_raw, na.rm = TRUE),
  by = GEOID
]

parcel_bg[
  ,
  renters_scaled := fifelse(
    sum_raw > 0 & !is.na(renters_raw),
    renters_raw * (renter_occ / sum_raw),
    0
  )
]

parcel_bg[
  ,
  renter_floor := max_households_per_bldg %||% 0
]
parcel_bg[
  ,
  renter_cap   := num_units_final %||% 0
]

parcel_bg[
  ,
  renters_final := pmin(pmax(renters_scaled, renter_floor), renter_cap)
]
parcel_bg[
  ,
  sum_final := sum(renters_final, na.rm = TRUE),
  by = GEOID
]
parcel_bg[
  ,
  renters_final := fifelse(
    sum_final > 0,
    renters_final * (renter_occ / sum_final),
    renters_final
  )
]

parcel_bg[
  ,
  occupancy_rate_2010 := fifelse(
    num_units_final > 0,
    renters_final / num_units_final,
    NA_real_
  )
]
parcel_bg[
  ,
  occupancy_rate_2010 := pmin(pmax(occupancy_rate_2010, 0.25), 1.00)
]

# Collapse back to one row per PID (2010 snapshot)
parcel_keep_2010 <- parcel_bg[
  ,
  .(

    GEOID = first(GEOID),
    year_built = first(year_built),
    post2010 = first(post2010),
    num_units_final_2010 = round(sum(num_units_final, na.rm = TRUE)),
    renters_final_2010   = round(sum(renters_final, na.rm = TRUE)),
    occupancy_rate_2010  = first(occupancy_rate_2010)
  ), by = PID
]

# -------------------------------
# 8) BUILD PARCEL-YEAR PANEL
# -------------------------------
# Static parcel attributes for panel
parcel_static <- parcel_agg[
  ,
  .(
    PID,
    GEOID,
    year_built,
    num_units_imp_base = num_units_imp,
    max_households,
    median_households,
    mean_households,
    structure_bin
  )
]

# Yearly InfoUSA summary
yearly_infousa <- infousa_m[!is.na(PID),
                            .(
                              num_households    = first(num_households),
                              num_households_g1 = first(num_households_g1),
                              num_homeowners    = sum(home_owner, na.rm = TRUE),
                              max_households    = first(max_households)
                            ),
                            by = .(PID, year)
]

# Yearly rental panel info (subset of columns)
ever_panel_year <- ever_rentals_panel[
  ,
  .(
    PID, year,
    ever_rental_any_year,
    rental_from_altos,
    rental_from_license,
    rental_from_evict,
    num_filings
  )
]

all_pids  <- sort(unique(c(parcel_static$PID, yearly_infousa$PID, ever_panel_year$PID)))
years_seq <- sort(unique(c(yearly_infousa$year, ever_panel_year$year)))

parcel_grid <- CJ(
  PID  = all_pids,
  year = years_seq
)

panel <- merge(parcel_grid, parcel_static, by = "PID", all.x = TRUE)
panel <- merge(panel, parcel_keep_2010,
               by = c("PID", "GEOID", "year_built"), all.x = TRUE)
panel <- merge(panel, yearly_infousa,
               by = c("PID", "year"), all.x = TRUE)
panel <- merge(panel, ever_panel_year,
               by = c("PID", "year"), all.x = TRUE)

# Respect year_built: drop pre-construction years
panel <- panel[is.na(year_built) | year >= year_built]

# Units used in panel:
#  - For ≤2010 & pre-2010 parcels: BG-constrained num_units_final_2010
#  - Otherwise: base units from ever-rentals
panel[
  ,
  num_units_imp_final := fifelse(
    !is.na(num_units_final_2010) &
      year <= 2010 &
      (is.na(year_built) | year_built <= 2010),
    num_units_final_2010,
    num_units_imp_base
  )
]

# Renters imputed:
#  - For ≤2010 & pre-2010 parcels: constrained renters_final_2010
#  - After 2010: min(households, units), using InfoUSA where available
panel[
  ,
  renters_imputed := fifelse(
    !is.na(renters_final_2010) &
      year <= 2010 &
      (is.na(year_built) | year_built <= 2010),
    renters_final_2010,
    pmin(num_households %||% 0, num_units_imp_final %||% 0)
  )
]

panel[
  ,
  occupancy_rate := fifelse(
    num_units_imp_final > 0,
    renters_imputed / num_units_imp_final,
    NA_real_
  )
]
panel[
  ,
  occupancy_rate := pmin(pmax(occupancy_rate, 0.25), 1.00)
]

panel[
  ,
  num_units_cuts := cut(
    num_units_imp_final,
    breaks = c(-Inf, 2, 5, 10, 20, 50, Inf),
    labels = c("1-2", "3-5", "6-10", "11-20", "21-50", "50+"),
    include.lowest = TRUE
  )
]

# -------------------------------
# 9) EXPORT
# -------------------------------
out_path_all  <- file.path(PROC, "parcel_occupancy_panel_with_ever_rentals.csv")
out_path_rent <- file.path(PROC, "parcel_occupancy_panel_rentals_only.csv")

fwrite(panel, out_path_all)

fwrite(
  panel[ever_rental_any_year == TRUE | rental_from_license == TRUE | rental_from_altos == TRUE],
  out_path_rent
)

message("[occupancy] Rows in panel: ", nrow(panel))
message("[occupancy] Parcels (unique): ", uniqueN(panel$PID))
message("[occupancy] Parcels with any rental evidence: ",
        panel[ever_rental_any_year == TRUE | rental_from_license == TRUE | rental_from_altos == TRUE, uniqueN(PID)])
message("[occupancy] Done.")
