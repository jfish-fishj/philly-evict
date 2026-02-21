## ============================================================
## make-occupancy-vars.r  (FULL REWRITE)
## ============================================================
## Goal:
##   Build a parcel-year panel with:
##     - total_units (denominator based on *all residential parcels*)
##     - renter_occ (allocated to match 2010 BG renter totals for ≤2010 stock)
##     - occupancy_rate (renter_occ / total_units; keep name but interpret carefully)
##
## Core changes vs old version:
##   1) Units are imputed for the *full residential universe* (philly_parcels),
##      not “ever rentals only”.
##   2) building_type/is_condo are constructed using helper-functions.R
##      (standardize_building_type + extract_stories_from_code).
##   3) Unit raking uses a regression-seeded weight (estimated coefficients),
##      then rakes to 2010 BG×structure_bin totals and BG HU totals.
##      *No InfoUSA-based floors* (removes inflation mechanism).
##   4) Renter allocation uses rental evidence + building_type/size signals,
##      constrained to 2010 BG renter totals. *No InfoUSA floors*.
##
## Inputs (unchanged where possible):
##   - cfg$inputs$infousa_cleaned
##   - cfg$products$parcel_building_2024
##   - cfg$products$parcel_building_summary
##   - cfg$products$infousa_address_xwalk
##   - cfg$inputs$tenure_bg_2010
##   - cfg$inputs$uis_tr_2010
##   - cfg$products$ever_rentals_panel
##   - cfg$products$ever_rental_parcel_units  (used only as *labels* where present)
##
## Outputs:
##   - cfg$products$parcel_occupancy_panel
##   - cfg$products$parcel_occupancy_rentals_only
##
## Primary key: (PID, year)
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(fixest)
})

# ---- Load config + helpers ----
source("r/config.R")
source("r/helper-functions.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "make-occupancy-vars.log")

logf("=== Starting make-occupancy-vars.r (rewrite) ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

`%||%` <- function(a, b) if (is.null(a) || all(is.na(a))) b else a
normalize_pid <- function(x) stringr::str_pad(as.character(x), 9, "left", "0")

# -----------------------------
# small utilities
# -----------------------------
safe_log <- function(x) log(pmax(1, as.numeric(x)))
safe_log1p <- function(x) log1p(pmax(0, as.numeric(x)))

scale_to_target_simple <- function(x, target) {
  # proportional scaling; no floors/caps
  if (is.na(target)) return(x)
  s <- sum(x, na.rm = TRUE)
  if (!is.finite(s) || s <= 0) return(x)
  x * (as.numeric(target) / s)
}

largest_remainder_round <- function(x, target_sum) {
  x <- pmax(0, as.numeric(x))
  base <- floor(x)
  rem <- x - base
  need <- as.integer(round(target_sum) - sum(base, na.rm = TRUE))
  if (!is.finite(need) || need == 0L) return(as.integer(base))

  ord <- order(rem, decreasing = (need > 0))
  k <- min(abs(need), length(ord))
  idx <- ord[seq_len(k)]
  base[idx] <- base[idx] + sign(need)
  base[base < 0] <- 0
  as.integer(base)
}

# Conservative, non-inflationary caps (optional).
# We do NOT floor upward anywhere.
cap_units_building <- function(structure_bin, x) {
  cap <- fcase(
    structure_bin %in% c("u_1_detached", "u_1_attached"), 6L,
    structure_bin == "u_2_units", 10L,
    structure_bin == "u_3_4_units", 16L,
    structure_bin == "u_5_9_units", 30L,
    structure_bin == "u_10_19_units", 50L,
    structure_bin == "u_20_49_units", 150L,
    structure_bin == "u_50plus_units", 800L,
    default = 200L
  )
  pmin(as.numeric(x), cap)
}

# Order bins so we can apply "minimum plausible bin" floors.
structure_bin_rank <- function(structure_bin) {
  fcase(
    structure_bin %in% c("u_1_detached", "u_1_attached"), 1L,
    structure_bin == "u_2_units", 2L,
    structure_bin == "u_3_4_units", 3L,
    structure_bin == "u_5_9_units", 4L,
    structure_bin == "u_10_19_units", 5L,
    structure_bin == "u_20_49_units", 6L,
    structure_bin == "u_50plus_units", 7L,
    default = NA_integer_
  )
}

# Translate observed household scale into a conservative minimum structure bin.
hh_to_structure_bin <- function(max_hh) {
  fcase(
    !is.na(max_hh) & max_hh >= 50, "u_50plus_units",
    !is.na(max_hh) & max_hh >= 20, "u_20_49_units",
    !is.na(max_hh) & max_hh >= 10, "u_10_19_units",
    !is.na(max_hh) & max_hh >= 5, "u_5_9_units",
    !is.na(max_hh) & max_hh >= 3, "u_3_4_units",
    !is.na(max_hh) & max_hh >= 2, "u_2_units",
    default = NA_character_
  )
}

# -------------------------------
# 1) LOAD DATA
# -------------------------------
logf("Loading input data...", log_file = log_file)

infousa_path <- p_product(cfg, "infousa_clean")
if (!file.exists(infousa_path)) {
  infousa_path <- p_input(cfg, "infousa_cleaned")
}
logf("  InfoUSA source: ", infousa_path, log_file = log_file)
philly_infousa_dt  <- fread(infousa_path)
philly_bldgs       <- fread(p_product(cfg, "parcel_building_summary"))  # building rows: PID, max_hgt, square_ft
philly_parcels     <- fread(p_product(cfg, "parcels_clean"))     # parcel rows
info_usa_xwalk     <- fread(p_product(cfg, "infousa_address_xwalk"))

tenure_bg_2010     <- fread(p_input(cfg, "tenure_bg_2010"))
uis_tr_2010        <- fread(p_input(cfg, "uis_tr_2010"))

ever_rentals_panel <- fread(p_product(cfg, "ever_rentals_panel"))
ever_rentals_pid <- fread(p_product(cfg, "ever_rentals_pid"))

setDT(philly_infousa_dt); setDT(philly_bldgs); setDT(philly_parcels); setDT(info_usa_xwalk)
setDT(tenure_bg_2010); setDT(uis_tr_2010); setDT(ever_rentals_panel)

# Normalize PIDs
philly_parcels[, PID := normalize_pid(parcel_number)]
philly_bldgs[,   PID := normalize_pid(PID)]
info_usa_xwalk[, PID := normalize_pid(PID)]
ever_rentals_panel[, PID := normalize_pid(PID)]
ever_rentals_pid[, PID := normalize_pid(PID)]
philly_infousa_dt[, obs_id := .I]

logf("  InfoUSA: ", nrow(philly_infousa_dt), " rows", log_file = log_file)
logf("  Parcels: ", nrow(philly_parcels), " rows", log_file = log_file)
logf("  Buildings: ", nrow(philly_bldgs), " rows", log_file = log_file)
logf("  Ever rentals panel: ", nrow(ever_rentals_panel), " rows", log_file = log_file)

# -------------------------------
# 2) INFOUSA × PARCEL MERGES (for panel only; not floors)
# -------------------------------
logf("Merging InfoUSA with parcels...", log_file = log_file)

xwalk_1 <- info_usa_xwalk[num_parcels_matched == 1 & !is.na(PID)]
logf("  xwalk_1 (unique matches): ", nrow(xwalk_1), " rows", log_file = log_file)

infousa_m <- merge(
  philly_infousa_dt,
  xwalk_1[, .(n_sn_ss_c, PID)],
  by = "n_sn_ss_c",
  all.x = TRUE
)

infousa_m[, home_owner := owner_renter_status %in% c(7, 8, 9)]
infousa_m[, num_households := .N, by = .(PID, year)]
infousa_m[, num_years_in_sample := .N, by = family_id]
infousa_m[, first_year_in_sample := min(year) == year, by = family_id]
infousa_m[, first_year_at_parcel := min(year) == year, by = .(family_id, PID)]
infousa_m[, last_year_at_parcel  := max(year) == year, by = .(family_id, PID)]
infousa_m[, num_households_g1    := sum(num_years_in_sample > 1), by = .(PID, year)]
infousa_m[, max_households       := max(num_households, na.rm = TRUE), by = PID]

# Raw InfoUSA completion for one-year interior family gaps:
# if a family is observed at PID in t and t+2, add t+1 as a high-confidence
# coverage-gap row. This uses raw family-year continuity instead of occupancy-rate
# imputation and is deterministic by construction.
infousa_family_obs <- unique(
  infousa_m[!is.na(PID) & !is.na(family_id) & !is.na(year),
            .(PID, family_id, year)],
  by = c("PID", "family_id", "year")
)
assert_unique(infousa_family_obs, c("PID", "family_id", "year"), "infousa_family_obs")
setorder(infousa_family_obs, PID, family_id, year)

infousa_family_gapfill_1y <- infousa_family_obs[
  ,
  {
    next_year <- data.table::shift(year, type = "lead")
    gap_idx <- which(!is.na(next_year) & (next_year - year) == 2L)
    if (length(gap_idx) == 0L) NULL else .(year = year[gap_idx] + 1L)
  },
  by = .(PID, family_id)
]
if (nrow(infousa_family_gapfill_1y) == 0L) {
  infousa_family_gapfill_1y <- infousa_family_obs[0, .(PID, family_id, year)]
} else {
  setorder(infousa_family_gapfill_1y, PID, family_id, year)
}
assert_unique(infousa_family_gapfill_1y, c("PID", "family_id", "year"), "infousa_family_gapfill_1y")

infousa_gapfill_pid_year <- infousa_family_gapfill_1y[
  ,
  .(num_households_raw_gapfill_1y = .N),
  by = .(PID, year)
]
logf(
  "  [raw InfoUSA gap-fill] Added ",
  nrow(infousa_family_gapfill_1y),
  " family-year rows across ",
  infousa_gapfill_pid_year[, .N],
  " PID-years (1-year interior gaps only)",
  log_file = log_file
)

hh_agg <- infousa_m[!is.na(PID),
                    .(
                      max_households    = max(num_households, na.rm = TRUE),
                      median_households = median(num_households, na.rm = TRUE),
                      mean_households   = mean(num_households, na.rm = TRUE)
                    ),
                    by = PID
]

# -------------------------------
# 3) PARCEL FEATURES (ALL RESIDENTIAL) + building_type from helpers
# -------------------------------
logf("Building parcel features + building_type...", log_file = log_file)

# building aggregates (parcel-level)
philly_bldgs_agg <- philly_bldgs[
  ,
  .(
    num_bldgs   = .N,
    total_sq_ft = sum(square_ft, na.rm = TRUE),
    mean_height = mean(max_hgt, na.rm = TRUE)
  ),
  by = PID
]

philly_parcels <- merge(philly_parcels, philly_bldgs_agg, by = "PID", all.x = TRUE)

# Ensure stories input for standardize_building_type
if (!"stories_from_code" %in% names(philly_parcels) &&
    "building_code_description" %in% names(philly_parcels)) {
  philly_parcels[, stories_from_code := extract_stories_from_code(building_code_description)]
}

# Fill number_stories if missing using stories_from_code
if ("number_stories" %in% names(philly_parcels)) {
  philly_parcels[, number_stories := fifelse(!is.na(number_stories), number_stories, stories_from_code)]
} else {
  philly_parcels[, number_stories := stories_from_code]
}

# Construct building_type + is_condo via helper (preferred)
if (!"building_type" %in% names(philly_parcels) &&
    all(c("building_code_description", "building_code_description_new") %in% names(philly_parcels))) {

  bldg_result <- standardize_building_type(
    bldg_code_desc     = philly_parcels$building_code_description,
    bldg_code_desc_new = philly_parcels$building_code_description_new,
    num_bldgs   = if ("num_bldgs" %in% names(philly_parcels)) philly_parcels$num_bldgs else NA_integer_,
    num_stories = philly_parcels$number_stories
  )
  philly_parcels[, building_type := bldg_result$building_type]
  philly_parcels[, is_condo := bldg_result$is_condo]
}

# Impute stories by building_type (not fixed legacy column)
impute_group_col <- if ("building_type" %in% names(philly_parcels)) "building_type" else "building_code_description_new_fixed"

philly_parcels[, num_stories_mean := mean(number_stories, na.rm = TRUE), by = c(impute_group_col)]
philly_parcels[, num_stories_imp  := fifelse(!is.na(number_stories), number_stories, num_stories_mean)]

philly_parcels[, num_bldgs_mean := mean(num_bldgs, na.rm = TRUE), by = c(impute_group_col)]
philly_parcels[, num_bldgs_imp  := fifelse(!is.na(num_bldgs), num_bldgs, num_bldgs_mean)]
philly_parcels[is.na(num_bldgs_imp), num_bldgs_imp := 1]

# Merge household stats (used later for panel, and optionally as a *weak* regressor; not floors)
parcel_agg <- merge(philly_parcels, hh_agg, by = "PID", all.x = TRUE)

# Year built decade
if ("year_built" %in% names(parcel_agg)) {
  parcel_agg[, year_built_decade := floor(as.numeric(year_built) / 10) * 10]
} else {
  parcel_agg[, year_built := NA_integer_]
  parcel_agg[, year_built_decade := NA_integer_]
}

# make GEOID 2010 for philly_parcels
# make coordinates from SHAPE; format is -75.1497752930359|39.9264602244216

philly_parcels[,parcel_latitude := as.numeric(str_extract(SHAPE, "(?<=\\|)[0-9\\.-]+"))]
philly_parcels[,parcel_longitude := as.numeric(str_extract(SHAPE, "[-]?[0-9\\.-]+(?=\\|)"))]
parcel_coords = philly_parcels[!is.na(parcel_latitude),.(PID, parcel_latitude, parcel_longitude)] |>
  unique() |>
  sf::st_as_sf(coords = c("parcel_longitude", "parcel_latitude"), crs = 4326)

bg_sf = sf::st_read(p_input(cfg, "philly_bg_shp"), quiet=TRUE) |>
  sf::st_transform(crs = 4326) |>
  sf::st_make_valid() |>
  dplyr::mutate(GEOID = str_sub(GEO_ID, -12, -1)) |>
  dplyr::select(GEOID)

parcel_bg_join = sf::st_join(parcel_coords, bg_sf, left=TRUE, join = sf::st_within)
parcel_bg_join_dt = as.data.table(parcel_bg_join)[, .(PID, GEOID=GEOID)]
# join back to parcel_agg
parcel_agg = merge(parcel_agg, parcel_bg_join_dt, by = "PID", all.x = TRUE)
# -------------------------------
# 4) 2010 BG TARGETS: UNITS & RENTERS (same logic)
# -------------------------------
logf("Building 2010 BG targets...", log_file = log_file)

tenure_bg_2010[, tract_geoid := str_sub(GEOID, 1, 11)]

bg_w <- tenure_bg_2010[, .(GEOID, tract_geoid, occ_total, renter_occ)]
bg_w[, tract_occ := sum(occ_total, na.rm = TRUE), by = tract_geoid]
bg_w[, w_bg := fifelse(tract_occ > 0, occ_total / tract_occ, 0)]
bg_w[, tract_geoid := as.numeric(tract_geoid)]

uis_tr_2010_dt <- as.data.table(uis_tr_2010)
uis_tr_2010_dt[, tract_geoid := as.numeric(tract_geoid)]
bg_targets_wide <- merge(bg_w, uis_tr_2010_dt, by = "tract_geoid", all.x = TRUE)
# drop MOE columns if present
bg_targets_wide <- bg_targets_wide[, !grepl("M$", names(bg_targets_wide)), with = FALSE]

bg_targets_long <- melt(
  bg_targets_wide,
  id.vars       = c("GEOID", "tract_geoid", "w_bg", "occ_total", "renter_occ"),
  measure.vars  = patterns("^u_"),
  variable.name = "struct_var",
  value.name    = "tract_units_k"
)
bg_targets_long[, bg_units_k := w_bg * tract_units_k]

bg_targets <- dcast(
  bg_targets_long[, .(GEOID, struct_var, bg_units_k)],
  GEOID ~ struct_var,
  value.var = "bg_units_k",
  fun.aggregate = sum
)

bg_targets <- merge(bg_targets, bg_w[, .(GEOID, renter_occ, occ_total)], by = "GEOID", all.x = TRUE)
bg_targets[, hu_bg_target := rowSums(.SD, na.rm = TRUE), .SDcols = patterns("^u_")]

# -------------------------------
# 5) STRUCTURE BIN from standardized building_type (no dependence on units labels)
#    + allow height/stories to refine multi-rise categories
# -------------------------------
logf("Assigning structure_bin...", log_file = log_file)

# initial map from building_type
parcel_agg[, structure_bin := fcase(
  building_type == "DETACHED", "u_1_detached",
  building_type == "ROW", "u_1_attached",
  building_type == "TWIN", "u_2_units",
  building_type == "SMALL_MULTI_2_4", "u_3_4_units",
  building_type == "LOWRISE_MULTI", "u_10_19_units",
  building_type == "MULTI_BLDG_COMPLEX", "u_10_19_units",
  building_type == "MIDRISE_MULTI", "u_20_49_units",
  building_type == "HIGHRISE_MULTI", "u_50plus_units",
  default = NA_character_
)]

parcel_agg = merge(
  parcel_agg,
  ever_rentals_pid[,.(PID, ever_rental_altos, ever_rental_license,
                      ever_rental_evict, ever_rental_any, intensity_total_sum,
                      intensity_altos_sum, intensity_license_sum, intensity_evict_sum,
                      years_any_evidence, years_altos, years_license, years_evict
                      )],
  by = "PID",
  all.x = TRUE
)

# replace NAs with 0 for ever_rental indicators
for (nm in c("ever_rental_altos","ever_rental_license","ever_rental_evict","ever_rental_any")) {
  parcel_agg[is.na(get(nm)), (nm) := 0L]
}

# flag for matched InfoUSA.
# Intentional design choice: requiring InfoUSA presence for "definitely residential"
# parcels reduces risk that large non-residential parcels (e.g., hotels/admin
# parcels) enter the residential denominator with implausible unit counts.
parcel_agg[,in_infousa := PID %in% info_usa_xwalk$PID]

parcel_agg[, is_def_res := (building_type %in% c(
  "ROW","TWIN","DETACHED",
  "SMALL_MULTI_2_4",
  "LOWRISE_MULTI","MIDRISE_MULTI","HIGHRISE_MULTI",
  "MULTI_BLDG_COMPLEX"
) & in_infousa)]

parcel_agg[, is_ambig := building_type %in% c("OTHER","COMMERCIAL","GROUP_QUARTERS") |
             (building_type %in% c(
               "ROW","TWIN","DETACHED",
               "SMALL_MULTI_2_4",
               "LOWRISE_MULTI","MIDRISE_MULTI","HIGHRISE_MULTI",
               "MULTI_BLDG_COMPLEX"
             ) & !in_infousa)
             ]

parcel_agg[, has_hh      := !is.na(max_households) & max_households >= 1]

parcel_agg[, has_rental_evidence := (ever_rental_license == 1L) | (ever_rental_altos == 1L) | (ever_rental_evict == 1L) ]

# residential universe flag
parcel_agg[, in_res_universe := is_def_res |
      (is_ambig & ( has_hh | has_rental_evidence))
]

# (optional) tag why an ambiguous parcel was kept (very useful for QA)
parcel_agg[is_ambig==T, keep_reason := case_when(
  has_rental_evidence == TRUE ~ "rental_evidence",
  has_hh == TRUE             ~ "households",
  TRUE                       ~ "other"
)]

# If missing, try to infer from stories (NOT from InfoUSA, NOT from prior units file)


parcel_agg[is.na(structure_bin) & in_res_universe, structure_bin := case_when(
  !is.na(num_bldgs_imp) & num_bldgs_imp >= 50~ "u_50plus_units",
  !is.na(num_bldgs_imp) & num_bldgs_imp >= 20~ "u_20_49_units",
  !is.na(num_bldgs_imp) & num_bldgs_imp >= 10~ "u_10_19_units",
  TRUE~ "u_1_attached"
)]

# Why this step:
# "OTHER/COMMERCIAL" parcels with real household activity can be mis-binned as
# tiny structures (e.g., u_1_attached), and later cap/raking then forces implausibly
# low unit counts. We enforce a household-informed minimum bin before raking.
parcel_agg[, structure_bin_hh_floor := hh_to_structure_bin(max_households)]
parcel_agg[, structure_bin_rank_cur := structure_bin_rank(structure_bin)]
parcel_agg[, structure_bin_rank_hh := structure_bin_rank(structure_bin_hh_floor)]

parcel_agg[
  !is.na(structure_bin_rank_hh) &
    !is.na(structure_bin_rank_cur) &
    max_households >= 10 &
    structure_bin_rank_hh > structure_bin_rank_cur,
  structure_bin := structure_bin_hh_floor
]

logf(
  "  [structure_bin hh floor] overrides=",
  parcel_agg[
    !is.na(structure_bin_rank_hh) &
      !is.na(structure_bin_rank_cur) &
      max_households >= 10 &
      structure_bin_rank_hh > structure_bin_rank_cur,
    .N
  ],
  log_file = log_file
)

parcel_agg[, `:=`(
  structure_bin_hh_floor = NULL,
  structure_bin_rank_cur = NULL,
  structure_bin_rank_hh = NULL
)]

# -------------------------------
# 6) UNITS: regression-seeded weights on ALL RESIDENTIAL parcels, then rake to 2010 targets
# -------------------------------
logf("Imputing + raking units on full residential universe...", log_file = log_file)


# build a training dataset where labels exist
# Important: DO NOT include InfoUSA household counts as strong predictors (coverage drift).
# We include them only as a weak control (optional) but you can drop them entirely.
parcel_agg[, log_total_sqft := safe_log(total_sq_ft)]
parcel_agg[, log_livable   := safe_log(total_livable_area)]
parcel_agg[, log_land      := safe_log(total_area)]
parcel_agg[, log_mkt_value := safe_log(market_value)]
parcel_agg[, log_bldgs     := safe_log1p(num_bldgs_imp)]
parcel_agg[, log_stories   := safe_log1p(num_stories_imp)]
parcel_agg[, hh_med        := as.numeric(median_households)]
parcel_agg[!is.finite(hh_med), hh_med := NA_real_]

# Merge unit labels where available
parcel_agg <- merge(
  parcel_agg,
  ever_rentals_pid[, .(PID, num_units_label = mode_num_units)],
  by = "PID",
  all.x = TRUE
)

# Address-level id for unit imputation:
# Condo parcels are often split into many 1-unit parcel rows at the same address.
# Modeling units at PID-level treats these rows as independent "single-family" labels
# and biases imputation downward for large buildings. We therefore model at address
# level (n_sn_ss_c) and distribute predictions back to PIDs.
parcel_agg[, addr_id := fifelse(!is.na(n_sn_ss_c) & n_sn_ss_c != "", n_sn_ss_c, paste0("PID_", PID))]

addr_dt <- parcel_agg[
  ,
  .(
    log_total_sqft_addr = safe_log(sum(total_sq_ft, na.rm = TRUE)),
    log_livable_addr    = safe_log(sum(total_livable_area, na.rm = TRUE)),
    log_land_addr       = safe_log(sum(total_area, na.rm = TRUE)),
    log_mkt_value_addr  = safe_log(sum(market_value, na.rm = TRUE)),
    log_bldgs_addr      = safe_log1p(sum(num_bldgs_imp, na.rm = TRUE)),
    log_stories_addr    = safe_log1p(mean(num_stories_imp, na.rm = TRUE)),
    is_condo_addr       = as.integer(any(is_condo %in% TRUE, na.rm = TRUE)),
    building_type_addr  = names(sort(table(building_type), decreasing = TRUE))[1],
    year_built_decade_addr = suppressWarnings(min(year_built_decade, na.rm = TRUE)),
    n_pid_addr          = uniqueN(PID),
    n_label_addr        = sum(!is.na(num_units_label)),
    label_sum_addr      = sum(num_units_label, na.rm = TRUE),
    label_mean_addr     = ifelse(sum(!is.na(num_units_label)) > 0, mean(num_units_label, na.rm = TRUE), NA_real_),
    max_hh_addr         = {
      hh_tmp <- max_households[is.finite(max_households)]
      if (length(hh_tmp) == 0L) NA_real_ else as.numeric(max(hh_tmp))
    }
  ),
  by = addr_id
]

addr_dt[!is.finite(year_built_decade_addr), year_built_decade_addr := NA_real_]
addr_dt[!is.finite(max_hh_addr), max_hh_addr := NA_real_]
addr_dt[, year_built_decade_addr := as.numeric(year_built_decade_addr)]
addr_dt[n_label_addr == 0, `:=`(label_sum_addr = NA_real_, label_mean_addr = NA_real_)]

train_addr <- addr_dt[!is.na(label_sum_addr)]

# Use fepois for positive count-ish outcome at address level.
units_model <- fepois(
  label_sum_addr ~
    log_total_sqft_addr + I(log_total_sqft_addr^2) +
    log_livable_addr   + I(log_livable_addr^2) +
    log_land_addr +
    log_mkt_value_addr +
    log_bldgs_addr +
    log_stories_addr +
    is_condo_addr +
    building_type_addr +
    year_built_decade_addr,
  data = train_addr
)

logf("  [units_model] fitted on ", nrow(train_addr), " labeled addresses", log_file = log_file)

# drop down to residential parcels
parcel_agg = parcel_agg[in_res_universe ==T]


# Predict address-level unit totals then distribute back to PIDs.
addr_dt[, num_units_seed_addr := predict(units_model, newdata = addr_dt, type = "response")]
addr_dt[!is.finite(num_units_seed_addr) | is.na(num_units_seed_addr) | num_units_seed_addr <= 0, num_units_seed_addr := 1e-6]

# Guardrail for condo-style address splits:
# if an address has strong condo-like structure (many PID rows) and substantial
# observed households, avoid implausible seed totals (e.g., 1-2 units for a large
# tower) by enforcing a household-informed lower bound.
addr_dt[, condo_split_addr := is_condo_addr == 1L | n_pid_addr >= 10]
addr_dt[
  condo_split_addr == TRUE & !is.na(max_hh_addr) & max_hh_addr >= 10,
  num_units_seed_addr := pmax(num_units_seed_addr, as.numeric(max_hh_addr))
]

# Broader household lower bounds (not condo-only):
# very large observed household counts should not coexist with tiny unit totals.
addr_dt[
  !is.na(max_hh_addr) & max_hh_addr >= 20,
  num_units_seed_addr := pmax(num_units_seed_addr, 0.75 * as.numeric(max_hh_addr))
]
addr_dt[
  !is.na(max_hh_addr) & max_hh_addr >= 50 & num_units_seed_addr < 10,
  num_units_seed_addr := pmax(num_units_seed_addr, as.numeric(max_hh_addr))
]

parcel_agg <- merge(
  parcel_agg,
  addr_dt[, .(addr_id, num_units_seed_addr, n_pid_addr, n_label_addr, label_mean_addr, is_condo_addr, max_hh_addr, condo_split_addr)],
  by = "addr_id",
  all.x = TRUE
)

parcel_agg[, pid_w := fifelse(
  is.finite(total_livable_area) & total_livable_area > 0, total_livable_area,
  fifelse(is.finite(total_sq_ft) & total_sq_ft > 0, total_sq_ft, 1)
)]
parcel_agg[, pid_w_sum := sum(pid_w, na.rm = TRUE), by = addr_id]
parcel_agg[, num_units_seed := num_units_seed_addr * (pid_w / pid_w_sum)]
parcel_agg[!is.finite(num_units_seed) | is.na(num_units_seed) | num_units_seed <= 0, num_units_seed := 1e-6]

# use label when present; otherwise prediction
parcel_agg[, num_units_base := fifelse(!is.na(num_units_label), num_units_label, num_units_seed)]
parcel_agg[, num_units_base := pmax(1, num_units_base)]

# where num_units_label = 1 but num_units_seed > 10, use num_units_seed (likely a mislabeled single-family with strong multi-family signal)
parcel_agg[num_units_label == 1 & num_units_seed > 10, num_units_base := num_units_seed]

# Condo-like address override:
# If an address has many PID rows and condo signal, PID-level labels of 1 are
# typically parcel splits rather than true building unit totals. In these cases
# keep address-level distributed seeds instead of PID labels.
parcel_agg[, condo_like_addr := is_condo_addr == 1L & n_pid_addr >= 10 & n_label_addr >= 5 & label_mean_addr <= 1.5]
parcel_agg[condo_like_addr == TRUE, num_units_base := num_units_seed]

# Additional undercount guardrail:
# if an address is condo-split and has many observed households, do not allow
# final parcel-level unit totals to imply fewer units than those households.
parcel_agg[
  condo_split_addr == TRUE & !is.na(max_hh_addr) & max_hh_addr >= 10,
  num_units_base := pmax(num_units_base, as.numeric(max_hh_addr))
]
parcel_agg[
  !is.na(max_hh_addr) & max_hh_addr >= 20,
  num_units_base := pmax(num_units_base, 0.75 * as.numeric(max_hh_addr))
]
parcel_agg[
  !is.na(max_hh_addr) & max_hh_addr >= 50 & num_units_base < 10,
  num_units_base := pmax(num_units_base, as.numeric(max_hh_addr))
]

logf(
  "  [condo_like_addr override] addresses=",
  parcel_agg[condo_like_addr == TRUE, uniqueN(addr_id)],
  ", PIDs=",
  parcel_agg[condo_like_addr == TRUE, uniqueN(PID)],
  log_file = log_file
)

# Step 3: Flag suspect commercial parcels.
# Large buildings with near-zero InfoUSA presence that could be hotels,
# commercial, or institutional rather than residential.
# Placed here because num_units_base is now available.
parcel_agg[, suspect_commercial := (
  num_units_base >= 20 &
  (is.na(max_households) | max_households <= 2) &
  building_type %in% c("COMMERCIAL", "OTHER", "GROUP_QUARTERS",
                        "MIDRISE_MULTI", "HIGHRISE_MULTI",
                        "LOWRISE_MULTI", "MULTI_BLDG_COMPLEX") &
  # Exception: keep if parcel has eviction filings or Altos listings
  !has_rental_evidence
)]

n_suspect <- parcel_agg[suspect_commercial == TRUE, .N]
logf("  [Step 3] Suspect commercial parcels flagged: ", n_suspect, log_file = log_file)

# Write QA output for spot-checking
suspect_qa <- parcel_agg[suspect_commercial == TRUE, .(
  PID, building_type, num_units_base, max_households,
  ever_rental_altos, ever_rental_license, ever_rental_evict,
  has_rental_evidence, structure_bin, n_sn_ss_c
)]
suspect_qa_out <- p_out(cfg, "qa", "suspect_commercial_parcels.csv")
fwrite(suspect_qa, suspect_qa_out)
logf("  Wrote suspect commercial QA: ", nrow(suspect_qa), " rows to ", suspect_qa_out,
     log_file = log_file)

# Remove suspect commercial parcels from the panel
parcel_agg <- parcel_agg[suspect_commercial == FALSE]
logf("  [Step 3] Removed ", n_suspect, " suspect commercial parcels from res universe",
     log_file = log_file)

# -------------------------------
# 5b) Refine structure_bin using num_units_base
# -------------------------------
# The initial building_type → structure_bin mapping has two problems:
#   1. No building type maps to u_5_9_units (27K Census units unassigned)
#   2. TWIN → u_2_units regardless of actual unit count (many are 1-unit or 3-4)
# Now that num_units_base is available, override structure_bin for mismatches.
# This preserves the building_type label but corrects the Census bin assignment.

units_to_structure_bin <- function(u) {
  fcase(
    u < 1.5,  "u_1_attached",   # effectively 1 unit
    u < 2.5,  "u_2_units",
    u < 4.5,  "u_3_4_units",
    u < 9.5,  "u_5_9_units",
    u < 19.5, "u_10_19_units",
    u < 49.5, "u_20_49_units",
    default = "u_50plus_units"
  )
}

# Detached and ROW stay in their Census structural categories regardless of unit count.
# Census classifies by structure type: a row home with 2 apartments is still "1 attached".
# Only refine types where unit count meaningfully distinguishes Census bins.
parcel_agg[, structure_bin_refined := fcase(
  building_type == "DETACHED", "u_1_detached",
  building_type == "ROW",      "u_1_attached",
  default = units_to_structure_bin(num_units_base)
)]

n_changed <- parcel_agg[structure_bin != structure_bin_refined, .N]
logf("  [structure_bin refinement] ", n_changed, " PIDs reassigned using num_units_base",
     log_file = log_file)

# Log the transitions
if (n_changed > 0) {
  transitions <- parcel_agg[structure_bin != structure_bin_refined,
                            .N, by = .(from = structure_bin, to = structure_bin_refined)]
  setorder(transitions, -N)
  for (i in seq_len(min(15, nrow(transitions)))) {
    logf("    ", transitions$from[i], " -> ", transitions$to[i], ": ", transitions$N[i],
         log_file = log_file)
  }
}

parcel_agg[, structure_bin := structure_bin_refined]
parcel_agg[, structure_bin_refined := NULL]

# Expand to parcel-building table to preserve within-parcel heterogeneity across buildings
parcel_building <- merge(
  parcel_agg[, .(
    PID, GEOID, year_built, year_built_decade,
    building_type, is_condo, structure_bin,
    num_units_base, num_bldgs_imp, num_stories_imp,
    total_sq_ft, total_livable_area, total_area
  )],
  philly_bldgs[, .(PID, max_hgt, square_ft)],
  by = "PID",
  all.x = TRUE
)

# If a parcel has no building rows, keep a synthetic building record
parcel_building[is.na(square_ft), square_ft := total_sq_ft]
parcel_building[is.na(max_hgt),   max_hgt   := NA_real_]

# Allocate parcel units to buildings using regression-calibrated *building weights*
# (estimated from model? we don’t have building-level labels, so use a parsimonious
#  intensity weight that is still consistent with “estimated coefficients” at parcel level,
#  but allows within-parcel variation via sqft/height.)
parcel_building[, b_w := exp(0.8 * safe_log(square_ft) + 0.2 * safe_log1p(max_hgt))]
parcel_building[!is.finite(b_w) | is.na(b_w) | b_w <= 0, b_w := 1]
parcel_building[, b_w_sum := sum(b_w), by = PID]

parcel_building[, units_per_bldg_seed := num_units_base * (b_w / b_w_sum)]
parcel_building[!is.finite(units_per_bldg_seed) | is.na(units_per_bldg_seed), units_per_bldg_seed := 0]

# Pre/post 2010 participation for 2010 constraints
parcel_building[, post2010 := !is.na(year_built) & as.numeric(year_built) > 2010]

# Attach BG targets
# convert GEOID to num
parcel_building[, GEOID := as.numeric(GEOID)]
parcel_bg <- merge(parcel_building, bg_targets, by = "GEOID", all.x = TRUE)

bins <- grep("^u_", names(bg_targets), value = TRUE)
parcel_bg[, units_raked := NA_real_]

# 6a) Rake within BG×structure_bin for ≤2010 stock
# Small-building bins: skip raking entirely (preserve seed units)
# ROW/TWIN/DETACHED homes are individual structures; raking gives them fractional units
small_bins <- c("u_1_detached", "u_1_attached", "u_2_units")
rakeable_bins <- setdiff(bins, small_bins)

# Small bins: keep seed directly
parcel_bg[structure_bin %in% small_bins & post2010 == FALSE,
          units_raked := units_per_bldg_seed]

# Larger bins: rake only when our inventory covers >= 70% of Census target
for (b in rakeable_bins) {
  parcel_bg[
    structure_bin == b & post2010 == FALSE,
    `:=`(
      seed_sum   = sum(units_per_bldg_seed, na.rm = TRUE),
      bin_target = unique(get(b))
    ),
    by = GEOID
  ]
  # Only rake if coverage is adequate

  parcel_bg[
    structure_bin == b & post2010 == FALSE &
      !is.na(bin_target) & bin_target > 0 & seed_sum / bin_target >= 0.70,
    units_raked := scale_to_target_simple(units_per_bldg_seed, unique(get(b))),
    by = GEOID
  ]
}
# Everything else (low-coverage bins, post-2010) keeps seed
parcel_bg[is.na(units_raked), units_raked := units_per_bldg_seed]

# Log raking stats per bin
for (b in small_bins) {
  n_total <- parcel_bg[structure_bin == b & post2010 == FALSE, .N]
  logf("  Raking [", b, "]: raked=0 skipped=", n_total, " (small bin, no raking)", log_file = log_file)
}
for (b in rakeable_bins) {
  n_raked   <- parcel_bg[structure_bin == b & post2010 == FALSE &
                           !is.na(bin_target) & bin_target > 0 &
                           seed_sum / bin_target >= 0.70, .N]
  n_skipped <- parcel_bg[structure_bin == b & post2010 == FALSE, .N] - n_raked
  logf("  Raking [", b, "]: raked=", n_raked, " skipped=", n_skipped, log_file = log_file)
}
# Clean up temp cols (only exist if rakeable_bins was non-empty)
if (length(rakeable_bins) > 0) parcel_bg[, c("seed_sum", "bin_target") := NULL]

# 6b) REMOVED: BG-level total HU raking
# Previously re-scaled ALL pre-2010 buildings within each BG to hit hu_bg_target.
# This undid the selective raking decisions above (e.g., giving ROW homes fractional units).
# With selective bin-level raking and coverage checks, BG-wide raking is no longer appropriate.

# Optional: very conservative caps (no floors) to prevent a few buildings soaking huge mass
parcel_bg[, units_raked := cap_units_building(structure_bin, units_raked)]

# --- Diagnostic: raked units vs Census targets per bin ---
logf("--- Unit raking diagnostics (pre-2010 stock, post-cap) ---", log_file = log_file)
total_raked_all <- 0
total_census_all <- 0
for (b in bins) {
  # Our pipeline's raked units for this bin
  pipeline_units <- parcel_bg[structure_bin == b & post2010 == FALSE, sum(units_raked, na.rm = TRUE)]
  # Census target: sum of unique BG-level targets (each BG contributes once)
  census_target <- parcel_bg[structure_bin == b & post2010 == FALSE,
                             sum(unique_val <- tapply(get(b), GEOID, function(x) unique(x)[1]),
                                 na.rm = TRUE)]
  # Simpler: aggregate at BG level first
  bg_level <- parcel_bg[post2010 == FALSE & !is.na(get(b)),
                        .(pipeline = sum(units_raked[structure_bin == b], na.rm = TRUE),
                          census   = unique(get(b))[1]),
                        by = GEOID]
  pipeline_total <- bg_level[, sum(pipeline, na.rm = TRUE)]
  census_total   <- bg_level[census > 0, sum(census, na.rm = TRUE)]
  ratio <- fifelse(census_total > 0, pipeline_total / census_total, NA_real_)
  n_bldgs <- parcel_bg[structure_bin == b & post2010 == FALSE, .N]
  # Correlation at BG level
  bg_corr <- if (nrow(bg_level[pipeline > 0 & census > 0]) >= 10) {
    round(cor(bg_level[pipeline > 0 & census > 0]$pipeline,
              bg_level[pipeline > 0 & census > 0]$census), 3)
  } else NA_real_
  logf("  [", b, "] buildings=", n_bldgs,
       " pipeline_units=", round(pipeline_total),
       " census_target=", round(census_total),
       " ratio=", round(ratio, 3),
       " BG_corr=", bg_corr,
       log_file = log_file)
  total_raked_all  <- total_raked_all + pipeline_total
  total_census_all <- total_census_all + census_total
}
logf("  [TOTAL] pipeline_units=", round(total_raked_all),
     " census_target=", round(total_census_all),
     " ratio=", round(total_raked_all / total_census_all, 3),
     log_file = log_file)
# Also compare to hu_bg_target (total HU from Census)
hu_census <- parcel_bg[post2010 == FALSE & !is.na(hu_bg_target),
                       sum(tapply(hu_bg_target, GEOID, function(x) unique(x)[1]), na.rm = TRUE)]
logf("  [HU total] pipeline=", round(total_raked_all),
     " census_hu_bg_target=", round(hu_census),
     " ratio=", round(total_raked_all / hu_census, 3),
     log_file = log_file)

# Collapse back to PID totals for the 2010 snapshot
parcel_keep_2010_units <- parcel_bg[, .(
  GEOID = first(GEOID),
  year_built = first(year_built),
  post2010 = first(post2010),
  total_units_2010 = sum(units_raked, na.rm = TRUE)
), by = PID]

# Integerize PID totals for panel use (preserve BG totals approximately—exactness happens at building level)
parcel_keep_2010_units[, total_units_2010 := pmax(1L, as.integer(round(total_units_2010)))]

# Fix 1a: Constrain total_units using max_households as soft ceiling.
# If InfoUSA has ever observed max_households at a PID, the building
# probably doesn't have 3× that many units. Apply 1.5× slack to account
# for InfoUSA undercoverage. Only applies when max_households >= 5.
parcel_keep_2010_units <- merge(
  parcel_keep_2010_units, hh_agg[, .(PID, max_households)],
  by = "PID", all.x = TRUE
)
n_capped_2010 <- parcel_keep_2010_units[
  !is.na(max_households) & max_households >= 5 & total_units_2010 > 1.5 * max_households, .N
]
parcel_keep_2010_units[
  !is.na(max_households) & max_households >= 5 & total_units_2010 > 1.5 * max_households,
  total_units_2010 := as.integer(ceiling(1.5 * max_households))
]
parcel_keep_2010_units[, max_households := NULL]
logf("  [Fix 1a] Capped total_units_2010 at 1.5*max_households for ", n_capped_2010, " PIDs",
     log_file = log_file)

# -------------------------------
# 7) RENTERS (2010): allocate using evidence + building_type/size; constrained to BG renter_occ
# -------------------------------
logf("Allocating 2010 renters (BG constrained; no InfoUSA floors)...", log_file = log_file)

# PID-level rental evidence
ever_pid_stats <- ever_rentals_panel[
  ,
  .(
    ever_rental_any      = as.integer(any(ever_rental_any_year, na.rm = TRUE)),
    ever_rental_altos    = as.integer(any(rental_from_altos,   na.rm = TRUE)),
    ever_rental_license  = as.integer(any(rental_from_license, na.rm = TRUE)),
    ever_rental_evict    = as.integer(any(rental_from_evict,   na.rm = TRUE)),
    ever_filings_any     = as.integer(any(num_filings > 0,     na.rm = TRUE))
  ),
  by = PID
]

parcel_bg <- merge(parcel_bg, ever_pid_stats, by = "PID", all.x = TRUE)
for (nm in c("ever_rental_any","ever_rental_altos","ever_rental_license","ever_rental_evict","ever_filings_any")) {
  parcel_bg[is.na(get(nm)), (nm) := 0L]
}

# renter propensity score (smooth; allows within-multi heterogeneity)
parcel_bg[, log_bldg_sqft := safe_log(square_ft)]
parcel_bg[, log_bldg_hgt  := safe_log1p(max_hgt)]
building_type = parcel_bg$building_type
base <- fcase(
  building_type %in% c("HIGHRISE_MULTI"), 1.8,
  building_type %in% c("MIDRISE_MULTI"),  1.4,
  building_type %in% c("LOWRISE_MULTI","MULTI_BLDG_COMPLEX"), 1.1,
  building_type %in% c("SMALL_MULTI_2_4"), 0.9,
  building_type %in% c("ROW","TWIN"), 0.4,
  building_type %in% c("DETACHED"), 0.2,
  default = 0.3
)

# label: choose something you believe indicates "rental"
parcel_bg[, y_rental := as.integer(ever_rental_license == 1L)]  # or ever_rental_any_year, etc.

parcel_bg[, z_log_bldg_sqft := scale(log_bldg_sqft)]
parcel_bg[, z_log_bldg_hgt  := scale(log_bldg_hgt)]

m <- glm(
  y_rental ~ building_type + is_condo + z_log_bldg_sqft + z_log_bldg_hgt + ever_filings_any + ever_rental_altos + ever_rental_evict,
  data = parcel_bg,
  family = binomial()
)

parcel_bg[, p_rent := predict(m, type = "response", newdata = parcel_bg)]
# replace p_rent = 1 if there is rental evidence (to ensure all evidence gets allocated renters)
parcel_bg[ever_rental_license == 1L, p_rent := 1]
# now for p <= 0.25 -> 0
parcel_bg[p_rent < 0.25, p_rent := 0]

# Prior occupancy among renter stock (not total stock). Use  0.91 for 2010.
parcel_bg[, occ_prior := 0.91] # https://www.huduser.gov/portal/publications/pdf/PhiladelphiaPA-CHMA-22.pdf

# raw renters mass (≤2010 only)
parcel_bg[, renters_raw := fifelse(post2010 == FALSE, units_raked * occ_prior * p_rent, 0)]
parcel_bg[, sum_raw := sum(renters_raw, na.rm = TRUE), by = GEOID]

# scale to BG renter_occ totals
parcel_bg[, renters_scaled := fifelse(sum_raw > 0, renters_raw * (renter_occ / sum_raw), 0)]

# Enforce renters <= units (no BG-level re-normalization)
# Previously re-normalized to hit Census BG renter totals, but this inflated buildings
# above their unit limits when Census renter counts exceeded our pipeline's raked units
# (686/1,325 BGs, 75K panel rows affected). Now we simply cap at units_raked and accept
# that BG renter totals won't match Census exactly. The gap is absorbed by the outside good.
parcel_bg[, renters_final := pmin(renters_scaled, units_raked)]

# --- Diagnostic: renter allocation vs Census per bin ---
logf("--- Renter allocation diagnostics (pre-2010 stock) ---", log_file = log_file)
total_renters_pipeline <- 0
for (b in bins) {
  renters_bin <- parcel_bg[structure_bin == b & post2010 == FALSE, sum(renters_final, na.rm = TRUE)]
  units_bin   <- parcel_bg[structure_bin == b & post2010 == FALSE, sum(units_raked, na.rm = TRUE)]
  n_rental    <- parcel_bg[structure_bin == b & post2010 == FALSE & renters_final > 0, .N]
  occ_rate    <- fifelse(units_bin > 0, renters_bin / units_bin, NA_real_)
  logf("  [", b, "] rental_bldgs=", n_rental,
       " renters=", round(renters_bin),
       " units=", round(units_bin),
       " occ_rate=", round(occ_rate, 3),
       log_file = log_file)
  total_renters_pipeline <- total_renters_pipeline + renters_bin
}
# Census total renters across all BGs
census_renters <- parcel_bg[post2010 == FALSE & !is.na(renter_occ),
                            sum(tapply(renter_occ, GEOID, function(x) unique(x)[1]), na.rm = TRUE)]
n_capped_renters <- parcel_bg[post2010 == FALSE & renters_scaled > units_raked, .N]
logf("  [RENTER TOTAL] pipeline=", round(total_renters_pipeline),
     " census_bg_renters=", round(census_renters),
     " ratio=", round(total_renters_pipeline / census_renters, 3),
     " buildings_capped=", n_capped_renters,
     log_file = log_file)

# BG-level correlation: pipeline renters vs Census renters
bg_renter_compare <- parcel_bg[post2010 == FALSE,
                               .(pipeline_renters = sum(renters_final, na.rm = TRUE),
                                 census_renters   = unique(renter_occ)[1]),
                               by = GEOID]
bg_renter_compare <- bg_renter_compare[!is.na(census_renters) & census_renters > 0]
bg_renter_corr <- round(cor(bg_renter_compare$pipeline_renters,
                            bg_renter_compare$census_renters), 3)
bg_renter_mean_dev <- round(bg_renter_compare[, mean(pipeline_renters / census_renters)], 3)
bg_renter_med_dev  <- round(bg_renter_compare[, median(pipeline_renters / census_renters)], 3)
logf("  [BG-level] corr(pipeline, census)=", bg_renter_corr,
     " mean_ratio=", bg_renter_mean_dev,
     " median_ratio=", bg_renter_med_dev,
     log_file = log_file)

# Collapse to PID 2010 renters
parcel_keep_2010_renters <- parcel_bg[, .(
  GEOID = first(GEOID),
  year_built = first(year_built),
  renters_2010 = sum(renters_final, na.rm = TRUE)
), by = PID]
parcel_keep_2010_renters[, renters_2010 := as.integer(round(pmax(0, renters_2010)))]
# log 2010 renters
logf("  Total allocated renters_2010: ", sum(parcel_keep_2010_renters$renters_2010, na.rm = TRUE), log_file = log_file)
# Merge 2010 snapshot
parcel_keep_2010 <- merge(parcel_keep_2010_units, parcel_keep_2010_renters,
                          by = c("PID","GEOID","year_built"), all.x = TRUE)

parcel_keep_2010[is.na(renters_2010), renters_2010 := 0L]
parcel_keep_2010[, occupancy_rate_2010 := fifelse(total_units_2010 > 0, renters_2010 / total_units_2010, NA_real_)]
parcel_keep_2010[, occupancy_rate_2010 := pmin(pmax(occupancy_rate_2010, 0), 1)]

# -------------------------------
# 8) BUILD PARCEL-YEAR PANEL (mostly same; but units now from full-universe imputation)
# -------------------------------
logf("Building parcel-year panel...", log_file = log_file)

parcel_static <- parcel_agg[, .(
  PID, GEOID, year_built, structure_bin, building_type,
  # keep these as diagnostics
  num_units_label,
  num_units_seed,
  num_units_base
)]

# yearly InfoUSA summary (panel)
yearly_infousa <- infousa_m[!is.na(PID),
                            .(
                              num_households    = first(num_households),
                              num_households_g1 = first(num_households_g1),
                              num_homeowners    = sum(home_owner, na.rm = TRUE),
                              max_households    = first(max_households)
                            ),
                            by = .(PID, year)
]
yearly_infousa <- merge(
  yearly_infousa,
  infousa_gapfill_pid_year,
  by = c("PID", "year"),
  all = TRUE
)

# rental panel info
ever_panel_year <- ever_rentals_panel[, .(
  PID, year,
  ever_rental_any_year,
  rental_from_altos,
  rental_from_license,
  rental_from_evict,
  num_filings
)]

all_pids  <- sort(unique(c(parcel_static$PID, yearly_infousa$PID, ever_panel_year$PID)))
years_seq <- sort(unique(c(yearly_infousa$year, ever_panel_year$year)))

panel <- CJ(PID = all_pids, year = years_seq)
panel <- merge(panel, parcel_static, by = "PID", all.x = TRUE)
panel[,GEOID := as.numeric(GEOID)]
panel <- merge(panel, parcel_keep_2010, by = c("PID","GEOID","year_built"), all.x = TRUE)
panel <- merge(panel, yearly_infousa, by = c("PID","year"), all.x = TRUE)
panel <- merge(panel, ever_panel_year, by = c("PID","year"), all.x = TRUE)

# Respect year_built: drop pre-construction years
panel <- panel[is.na(year_built) | year >= year_built]

# Units in panel:
# - For parcels built ≤2010: use BG-constrained total_units_2010 for all years <=2010;
#   for years > 2010, keep total_units_2010 as a stock measure unless you later add construction dynamics.
# - For parcels built >2010: use num_units_base (imputed from model) as stock measure.
panel[, total_units := fifelse(
  !is.na(total_units_2010) & (is.na(year_built) | as.numeric(year_built) <= 2010),
  total_units_2010,
  as.integer(round(pmax(1, num_units_base)))
)]

# Fix 1b: Soft ceiling — if InfoUSA has ever observed max_households >= 5,
# cap total_units to prevent extreme over-imputation. Allow 1.5× slack for
# InfoUSA undercoverage.
n_capped_panel <- panel[
  !is.na(max_households) & max_households >= 5 & total_units > 1.5 * max_households, .N
]
panel[
  !is.na(max_households) & max_households >= 5 & total_units > 1.5 * max_households,
  total_units := as.integer(ceiling(1.5 * max_households))
]
logf("  [Fix 1b] Capped total_units at 1.5*max_households for ", n_capped_panel, " panel rows",
     log_file = log_file)

# Why this step:
# BG raking + structure-bin caps can still pull some parcels to implausibly low
# units relative to observed households. Re-apply conservative household floors in
# the final panel so large-occupancy parcels cannot end up with 1-2 units.
panel[
  !is.na(max_households) & max_households >= 20,
  total_units := pmax(as.integer(total_units), as.integer(ceiling(0.75 * max_households)))
]
panel[
  !is.na(max_households) & max_households >= 50 & total_units < 10,
  total_units := pmax(as.integer(total_units), as.integer(ceiling(max_households)))
]

# Defensive clamp: keep unit counts strictly positive in final panel.
panel[is.na(total_units) | total_units <= 0, total_units := 1L]

# Combine observed InfoUSA household counts with deterministic raw-gap fill counts.
panel[, num_households_completed := fifelse(
  is.na(num_households) & is.na(num_households_raw_gapfill_1y),
  NA_integer_,
  as.integer(fcoalesce(num_households, 0L) + fcoalesce(num_households_raw_gapfill_1y, 0L))
)]

# Observed-vs-completed flags for diagnostics.
panel[, has_hh_observed := !is.na(num_households)]
panel[, has_hh_completed := !is.na(num_households_completed)]
panel[, pid_has_any_hh_observed := any(has_hh_observed), by = PID]
panel[, pid_has_any_hh_completed := any(has_hh_completed), by = PID]
n_raw_gap_rows_filled <- panel[
  is.na(num_households) & !is.na(num_households_raw_gapfill_1y),
  .N
]
n_raw_gap_units_filled <- panel[
  is.na(num_households) & !is.na(num_households_raw_gapfill_1y),
  sum(total_units, na.rm = TRUE)
]
logf(
  "  [raw InfoUSA gap-fill] Filled ",
  n_raw_gap_rows_filled,
  " PID-year rows before imputation (units=",
  round(n_raw_gap_units_filled),
  ")",
  log_file = log_file
)

# Renters in panel:
# ASSUMPTION: For rental properties, all InfoUSA households are renters.
# We do NOT subtract num_homeowners because the InfoUSA owner/renter status
# field is unreliable. Properties are already identified as rentals via
# licenses, Apartments.com, or eviction filings.
#
# renters_2010 was raked to match 2010 Census BG totals, so it only applies
# to (a) observations in year <= 2010, AND (b) buildings that existed by 2010.
# Post-2010 years or post-2010 construction use InfoUSA num_households,
# including deterministic raw-family gap-fill rows (1-year interior gaps).
# When num_households_completed is NA, renter_occ is left as NA (later imputed).
panel[, renter_occ := fifelse(
  !is.na(renters_2010) & year <= 2010 & (is.na(year_built) | as.numeric(year_built) <= 2010),
  renters_2010,
  pmin(as.integer(num_households_completed), as.integer(total_units))
)]
panel[renter_occ < 0L, renter_occ := 0L]

# Fix 2: Replace forced-zero with within-PID interpolation + BG fallback.
# Previously, missing renter_occ was treated as 0, making ~40% of buildings
# appear vacant when we simply had no InfoUSA data.

# Tag which rows have real household data
panel[, has_hh_data := !is.na(renter_occ)]
panel[, pid_has_any_hh := any(has_hh_data), by = PID]

n_missing_before <- panel[has_hh_data == FALSE, .N]

# Diagnostic: who is missing renter_occ? Break down by unit size, building type,
# and rental evidence to understand whether these are (1) non-rentals misclassified
# or (2) legitimate rentals with InfoUSA gaps.
# Compute unit bin for diagnostic (created later for scaling, need it here too)
panel[, diag_units_bin := cut(
  total_units,
  breaks = c(-Inf, 1, 2, 5, 20, 50, Inf),
  labels = c("1", "2", "3-5", "6-20", "21-50", "51+"),
  include.lowest = TRUE
)]

missing_diag <- panel[
  year >= 2011 & year <= 2019,
  .(
    n_total = .N,
    n_missing = sum(has_hh_data == FALSE),
    pct_missing = round(100 * mean(has_hh_data == FALSE), 1),
    # Among missing: how many have ANY InfoUSA data in other years?
    n_missing_pid_has_some = sum(has_hh_data == FALSE & pid_has_any_hh_completed == TRUE),
    n_missing_pid_has_none = sum(has_hh_data == FALSE & pid_has_any_hh_completed == FALSE)
  ),
  by = .(diag_units_bin)
]
setorder(missing_diag, diag_units_bin)
logf("  [Fix 2 diagnostic] Missing renter_occ by unit bin (2011-2019):", log_file = log_file)
for (r in seq_len(nrow(missing_diag))) {
  logf(sprintf(
    "    bin=%s: %d/%d missing (%.1f%%), pid_has_some=%d, pid_has_none=%d",
    missing_diag$diag_units_bin[r], missing_diag$n_missing[r], missing_diag$n_total[r],
    missing_diag$pct_missing[r], missing_diag$n_missing_pid_has_some[r],
    missing_diag$n_missing_pid_has_none[r]
  ), log_file = log_file)
}
panel[, diag_units_bin := NULL]

# By building_type
missing_btype <- panel[
  year >= 2011 & year <= 2019 & !is.na(building_type),
  .(
    n_total = .N,
    n_missing = sum(has_hh_data == FALSE),
    pct_missing = round(100 * mean(has_hh_data == FALSE), 1),
    n_missing_pid_has_none = sum(has_hh_data == FALSE & pid_has_any_hh_completed == FALSE)
  ),
  by = building_type
]
setorder(missing_btype, -n_missing)
logf("  [Fix 2 diagnostic] Missing renter_occ by building_type (2011-2019):", log_file = log_file)
for (r in seq_len(nrow(missing_btype))) {
  logf(sprintf(
    "    type=%s: %d/%d missing (%.1f%%), pid_never_in_infousa=%d",
    missing_btype$building_type[r], missing_btype$n_missing[r], missing_btype$n_total[r],
    missing_btype$pct_missing[r], missing_btype$n_missing_pid_has_none[r]
  ), log_file = log_file)
}

# Among PIDs with NO InfoUSA data ever: what rental evidence do they have?
# These are the ones Step B (BG fallback) would fill — are they real rentals?
never_hh_pids <- panel[
  year >= 2011 & year <= 2019 & pid_has_any_hh_completed == FALSE,
  .(
    total_units = first(total_units),
    building_type = first(building_type),
    structure_bin = first(structure_bin),
    has_license = any(rental_from_license %in% TRUE),
    has_altos = any(rental_from_altos %in% TRUE),
    has_evict = any(rental_from_evict %in% TRUE),
    total_filings = sum(fifelse(is.na(num_filings), 0L, num_filings))
  ),
  by = PID
]
never_hh_summary <- never_hh_pids[, .(
  n_pids = .N,
  n_units_1_2 = sum(total_units <= 2),
  n_units_3_5 = sum(total_units >= 3 & total_units <= 5),
  n_units_6_20 = sum(total_units >= 6 & total_units <= 20),
  n_units_21plus = sum(total_units >= 21),
  has_license = sum(has_license),
  has_altos = sum(has_altos),
  has_evict = sum(has_evict),
  has_any_evidence = sum(has_license | has_altos | has_evict),
  has_no_evidence = sum(!has_license & !has_altos & !has_evict & total_filings == 0)
)]
logf(sprintf(
  "  [Fix 2 diagnostic] PIDs with NO InfoUSA data ever (2011-2019): %d PIDs",
  never_hh_summary$n_pids
), log_file = log_file)
logf(sprintf(
  "    By size: 1-2 units=%d, 3-5=%d, 6-20=%d, 21+=%d",
  never_hh_summary$n_units_1_2, never_hh_summary$n_units_3_5,
  never_hh_summary$n_units_6_20, never_hh_summary$n_units_21plus
), log_file = log_file)
logf(sprintf(
  "    Rental evidence: license=%d, altos=%d, evict=%d, any=%d, NONE=%d",
  never_hh_summary$has_license, never_hh_summary$has_altos,
  never_hh_summary$has_evict, never_hh_summary$has_any_evidence,
  never_hh_summary$has_no_evidence
), log_file = log_file)

# Coverage categories for investigation:
# 1) partial coverage (lease-up window),
# 2) never in InfoUSA,
# 3) spotty year-to-year InfoUSA presence.
leaseup_window_years <- 2L
panel[, in_leaseup_window := !is.na(year_built) &
       as.integer(year_built) >= 2010L &
       year <= as.integer(year_built) + leaseup_window_years]
panel[, coverage_category := fcase(
  has_hh_completed == TRUE, "covered",
  has_hh_completed == FALSE & in_leaseup_window == TRUE, "partial_coverage_leaseup",
  has_hh_completed == FALSE & pid_has_any_hh_completed == FALSE, "never_in_infousa",
  has_hh_completed == FALSE & pid_has_any_hh_completed == TRUE, "spotty_in_infousa",
  default = "unclassified"
)]
panel[, rental_row := ever_rental_any_year %in% TRUE]

coverage_unit_year <- panel[
  year >= 2011 & year <= 2022 & rental_row == TRUE,
  .(
    n_pid_year = .N,
    units = sum(total_units, na.rm = TRUE),
    n_large_pid_year = sum(total_units >= 20, na.rm = TRUE),
    large_units = sum(fifelse(total_units >= 20, total_units, 0), na.rm = TRUE)
  ),
  by = .(year, coverage_category)
]
setorder(coverage_unit_year, year, coverage_category)

coverage_year_total <- panel[
  year >= 2011 & year <= 2022 & rental_row == TRUE,
  .(
    n_pid_year_total = .N,
    total_units_rental = sum(total_units, na.rm = TRUE)
  ),
  by = year
]
coverage_units_wide <- dcast(
  coverage_unit_year[, .(year, coverage_category, units)],
  year ~ coverage_category,
  value.var = "units",
  fun.aggregate = sum,
  fill = 0
)
for (nm in c("covered", "partial_coverage_leaseup", "never_in_infousa", "spotty_in_infousa")) {
  if (!nm %in% names(coverage_units_wide)) coverage_units_wide[, (nm) := 0]
}
setcolorder(coverage_units_wide, c("year", "covered", "partial_coverage_leaseup", "never_in_infousa", "spotty_in_infousa"))
setnames(
  coverage_units_wide,
  old = c("covered", "partial_coverage_leaseup", "never_in_infousa", "spotty_in_infousa"),
  new = c("units_covered", "units_partial_leaseup", "units_never_infousa", "units_spotty_infousa")
)

coverage_summary <- merge(coverage_year_total, coverage_units_wide, by = "year", all.x = TRUE)
for (nm in c("units_covered", "units_partial_leaseup", "units_never_infousa", "units_spotty_infousa")) {
  coverage_summary[is.na(get(nm)), (nm) := 0]
}
coverage_summary[, units_missing_total := units_partial_leaseup + units_never_infousa + units_spotty_infousa]
coverage_summary[, pct_units_missing := fifelse(total_units_rental > 0, 100 * units_missing_total / total_units_rental, NA_real_)]
coverage_summary[, gap_vs_330k := total_units_rental - 330000]
setorder(coverage_summary, year)

logf("  [coverage categories] Rental unit-years by year (2011-2022):", log_file = log_file)
for (r in seq_len(nrow(coverage_summary))) {
  logf(sprintf(
    "    year=%d total_units=%.0f covered=%.0f partial=%.0f never=%.0f spotty=%.0f missing_pct=%.2f%% gap_vs_330k=%+.0f",
    coverage_summary$year[r],
    coverage_summary$total_units_rental[r],
    coverage_summary$units_covered[r],
    coverage_summary$units_partial_leaseup[r],
    coverage_summary$units_never_infousa[r],
    coverage_summary$units_spotty_infousa[r],
    coverage_summary$pct_units_missing[r],
    coverage_summary$gap_vs_330k[r]
  ), log_file = log_file)
}

coverage_out <- p_out(cfg, "qa", "infousa_coverage_categories_unit_year.csv")
coverage_summary_out <- p_out(cfg, "qa", "infousa_coverage_categories_year_summary.csv")
fwrite(coverage_unit_year, coverage_out)
fwrite(coverage_summary, coverage_summary_out)
logf("  Wrote InfoUSA coverage category unit-years: ", nrow(coverage_unit_year), " rows to ", coverage_out, log_file = log_file)
logf("  Wrote InfoUSA coverage category year summary: ", nrow(coverage_summary), " rows to ", coverage_summary_out, log_file = log_file)

never_infousa_pid_review <- panel[
  year >= 2011 & year <= 2022 & rental_row == TRUE & pid_has_any_hh_completed == FALSE,
  .(
    year_built = first(year_built),
    total_units_max = max(total_units, na.rm = TRUE),
    building_type = first(building_type),
    structure_bin = first(structure_bin),
    years_license = sum(rental_from_license %in% TRUE, na.rm = TRUE),
    years_altos = sum(rental_from_altos %in% TRUE, na.rm = TRUE),
    years_evict = sum(rental_from_evict %in% TRUE, na.rm = TRUE),
    total_filings = sum(fifelse(is.na(num_filings), 0L, num_filings))
  ),
  by = PID
]
never_infousa_pid_review[, strong_counterevidence := (
  (years_license >= 2L) + (years_altos >= 2L) + (years_evict >= 2L) >= 2L
) | total_filings >= 10L]
setorder(never_infousa_pid_review, -total_units_max, -years_license, -years_altos, -years_evict, -total_filings)
never_review_out <- p_out(cfg, "qa", "infousa_never_in_infousa_pid_review.csv")
fwrite(never_infousa_pid_review, never_review_out)
logf(
  "  Wrote never-in-InfoUSA PID review: ",
  nrow(never_infousa_pid_review),
  " rows to ",
  never_review_out,
  " (strong counterevidence=",
  never_infousa_pid_review[strong_counterevidence == TRUE, .N],
  ")",
  log_file = log_file
)

setorder(panel, PID, year)
panel[, hh_obs_prev := data.table::shift(has_hh_observed, type = "lag"), by = PID]
panel[, hh_obs_next := data.table::shift(has_hh_observed, type = "lead"), by = PID]
panel[, spotty_adjacent_observed := (!has_hh_observed) &
       (hh_obs_prev %in% TRUE) &
       (hh_obs_next %in% TRUE)]

spotty_large_pid_year <- panel[
  year >= 2011 & year <= 2022 &
    coverage_category == "spotty_in_infousa" &
    total_units >= 20 &
    rental_row == TRUE,
  .(
    PID, year, total_units, year_built, building_type, structure_bin,
    num_households_observed = num_households,
    num_households_raw_gapfill_1y,
    num_households_completed,
    spotty_adjacent_observed,
    rental_from_license, rental_from_altos, rental_from_evict,
    num_filings
  )
]
setorder(spotty_large_pid_year, -total_units, PID, year)
spotty_large_out <- p_out(cfg, "qa", "infousa_spotty_large_pid_year.csv")
fwrite(spotty_large_pid_year, spotty_large_out)
logf("  Wrote spotty large-building review: ", nrow(spotty_large_pid_year), " rows to ", spotty_large_out,
     log_file = log_file)

# Step A: Within-PID carry-forward/backward for PIDs with SOME data.
# Interpolate occupancy_rate (unit-normalized) rather than raw counts,
# since units are stable but buildings differ in size.
panel[has_hh_data == TRUE & total_units > 0, occ_rate_observed := renter_occ / total_units]
setorder(panel, PID, year)
panel[pid_has_any_hh == TRUE,
      occ_rate_filled := nafill(nafill(occ_rate_observed, type = "locf"), type = "nocb"),
      by = PID]

# Reconstruct renter_occ from filled rate for previously-missing observations
panel[pid_has_any_hh == TRUE & has_hh_data == FALSE & !is.na(occ_rate_filled),
      renter_occ := as.integer(round(pmin(total_units, pmax(0, occ_rate_filled * total_units))))]

n_filled_locf <- panel[pid_has_any_hh == TRUE & has_hh_data == FALSE & !is.na(occ_rate_filled), .N]

# Step B: BG-level fallback for PIDs with NO InfoUSA data in any year.
# RESTRICTED: Only apply to PIDs that have actual rental evidence (license,
# altos, eviction filings). PIDs with no InfoUSA AND no rental evidence are
# likely not rentals at all — imputing occupancy for them inflates denominators.
panel[, pid_has_rental_evidence := any(
  rental_from_license %in% TRUE |
  rental_from_altos %in% TRUE |
  rental_from_evict %in% TRUE |
  (!is.na(num_filings) & num_filings > 0)
), by = PID]

bg_occ <- panel[
  has_hh_data == TRUE & total_units > 0,
  .(bg_occ_rate = weighted.mean(renter_occ / total_units, w = total_units, na.rm = TRUE)),
  by = .(GEOID, structure_bin, year)
]
panel <- merge(panel, bg_occ, by = c("GEOID", "structure_bin", "year"), all.x = TRUE)
panel[pid_has_any_hh == FALSE & pid_has_rental_evidence == TRUE & !is.na(bg_occ_rate),
      renter_occ := as.integer(round(pmin(total_units, pmax(0, bg_occ_rate * total_units))))]

n_eligible_bg <- panel[pid_has_any_hh == FALSE & has_hh_data == FALSE, .N]
n_filled_bg <- panel[pid_has_any_hh == FALSE & pid_has_rental_evidence == TRUE &
                       !is.na(bg_occ_rate) & has_hh_data == FALSE, .N]
n_skipped_no_evidence <- panel[pid_has_any_hh == FALSE & pid_has_rental_evidence == FALSE &
                                 has_hh_data == FALSE, .N]
logf(sprintf(
  "  [Fix 2 Step B] %d eligible (no InfoUSA ever), %d filled (has rental evidence), %d skipped (no evidence)",
  n_eligible_bg, n_filled_bg, n_skipped_no_evidence
), log_file = log_file)
n_still_missing <- panel[is.na(renter_occ), .N]

logf(sprintf(
  "  [Fix 2] Occupancy imputation: %d missing before, %d filled via LOCF/NOCB, %d via BG fallback, %d still missing",
  n_missing_before, n_filled_locf, n_filled_bg, n_still_missing
), log_file = log_file)

# Step 5: Defensive clamp — renter_occ cannot exceed total_units.
# Root cause: Section 7 BG renter re-normalization (line ~787). After capping
# renters at building units, the code re-normalizes to hit BG totals, which
# pushes uncapped buildings ABOVE their unit limits. 100% of clamped rows
# come from the renters_2010 path (years 2006-2010). See output/qa/ for details.
n_clamped <- panel[!is.na(renter_occ) & renter_occ > total_units, .N]
panel[!is.na(renter_occ) & renter_occ > total_units,
      renter_occ := as.integer(total_units)]
logf("  [Step 5] Clamped renter_occ to total_units for ", n_clamped, " rows",
     log_file = log_file)

# Floor at 0
panel[!is.na(renter_occ) & renter_occ < 0L, renter_occ := 0L]

# Clean up temp columns
panel[, c("has_hh_data", "pid_has_any_hh", "pid_has_rental_evidence",
           "occ_rate_observed", "occ_rate_filled", "bg_occ_rate",
           "num_households_raw_gapfill_1y", "num_households_completed",
           "has_hh_observed", "has_hh_completed",
           "pid_has_any_hh_observed", "pid_has_any_hh_completed",
           "in_leaseup_window", "coverage_category", "rental_row",
           "hh_obs_prev", "hh_obs_next", "spotty_adjacent_observed") := NULL]

# Compute occupancy_rate. After Fix 2, most renter_occ values are filled.
# Any remaining NAs (no BG match) fall back to 0 conservatively.
panel[, occupancy_rate := fifelse(
  total_units > 0,
  pmin(1, pmax(0, fifelse(is.na(renter_occ), 0, renter_occ)) / total_units),
  NA_real_
)]

# QA diagnostics for household-vs-units consistency in estimation-relevant stock
qa_units <- panel[
  year >= 2011 & year <= 2019 & total_units > 5 & total_units <= 500,
  .(
    n = .N,
    hh_missing = mean(is.na(num_households)),
    hh_gt_units = mean(!is.na(num_households) & num_households > total_units),
    hh_eq_units = mean(!is.na(num_households) & num_households == total_units),
    hh_lt_half_units = mean(!is.na(num_households) & num_households < 0.5 * total_units)
  )
]
if (nrow(qa_units) == 1L) {
  logf(
    sprintf(
      "[QA units-vs-households 2011-2019, 6-500 units] n=%d, hh_missing=%.3f, hh>units=%.3f, hh==units=%.3f, hh<0.5*units=%.3f",
      qa_units$n, qa_units$hh_missing, qa_units$hh_gt_units, qa_units$hh_eq_units, qa_units$hh_lt_half_units
    ),
    log_file = log_file
  )
}

# Additional QA requested: relationship between units and max_households
qa_units_maxhh <- panel[
  year >= 2011 & year <= 2019 & !is.na(total_units) & !is.na(max_households),
  .(
    n = .N,
    corr_units_maxhh = if (.N > 1) cor(total_units, max_households) else NA_real_,
    corr_log_units_log_maxhh = if (.N > 1) cor(log1p(total_units), log1p(max_households)) else NA_real_,
    n_units0_hh_gt10 = sum(total_units == 0 & max_households > 10, na.rm = TRUE),
    n_units_gt10_hh0 = sum(total_units > 10 & max_households == 0, na.rm = TRUE),
    n_units_le2_hh_gt10 = sum(total_units <= 2 & max_households > 10, na.rm = TRUE),
    n_units_gt10_hh_le2 = sum(total_units > 10 & max_households <= 2, na.rm = TRUE)
  )
]
if (nrow(qa_units_maxhh) == 1L) {
  logf(
    sprintf(
      "[QA units-vs-max_households 2011-2019] n=%d, corr=%.4f, corr_log=%.4f, n_units0_hh>10=%d, n_units>10_hh0=%d, n_units<=2_hh>10=%d, n_units>10_hh<=2=%d",
      qa_units_maxhh$n,
      qa_units_maxhh$corr_units_maxhh,
      qa_units_maxhh$corr_log_units_log_maxhh,
      qa_units_maxhh$n_units0_hh_gt10,
      qa_units_maxhh$n_units_gt10_hh0,
      qa_units_maxhh$n_units_le2_hh_gt10,
      qa_units_maxhh$n_units_gt10_hh_le2
    ),
    log_file = log_file
  )
}

# Worst offenders: largest unit-household disagreement by absolute gap
offenders_dt <- panel[
  year >= 2011 & year <= 2019 & !is.na(total_units) & !is.na(max_households),
  .(
    PID, year, total_units, max_households, num_households,
    building_type, structure_bin, year_built
  )
]
offenders_dt[, gap_units_minus_maxhh := total_units - max_households]
offenders_dt[, abs_gap := abs(gap_units_minus_maxhh)]
setorder(offenders_dt, -abs_gap)
worst_offenders <- offenders_dt[1:min(2000L, .N)]

worst_offenders_out <- p_out(cfg, "qa", "units_vs_max_households_worst_offenders.csv")
fwrite(worst_offenders, worst_offenders_out)
logf("Wrote worst offenders (units vs max_households): ", nrow(worst_offenders),
     " rows to ", worst_offenders_out, log_file = log_file)

# RTT sanity check for unit plausibility:
# Extremely high sale price per unit can indicate unit undercounting.
if (file.exists(p_product(cfg, "rtt_clean"))) {
  rtt_dt <- fread(p_product(cfg, "rtt_clean"))
  rtt_dt[, PID := normalize_pid(PID)]
  rtt_dt[, year := as.integer(year)]

  rtt_panel <- merge(
    rtt_dt[, .(PID, year, total_consideration)],
    panel[, .(PID, year, total_units, max_households, building_type)],
    by = c("PID", "year"),
    all = FALSE
  )

  rtt_panel[
    ,
    price_per_unit := fifelse(
      is.finite(total_consideration) & total_consideration > 0 &
        is.finite(total_units) & total_units > 0,
      total_consideration / total_units,
      NA_real_
    )
  ]

  n_ppu_2m <- rtt_panel[is.finite(price_per_unit) & price_per_unit > 2e6, .N]
  n_ppu_2m_units2 <- rtt_panel[is.finite(price_per_unit) & price_per_unit > 2e6 & total_units <= 2, .N]
  n_ppu_2m_units2_hh20 <- rtt_panel[
    is.finite(price_per_unit) & price_per_unit > 2e6 & total_units <= 2 &
      is.finite(max_households) & max_households >= 20,
    .N
  ]

  logf(
    "[QA RTT price_per_unit] matched_rows=", nrow(rtt_panel),
    ", n_ppu>2m=", n_ppu_2m,
    ", n_ppu>2m_units<=2=", n_ppu_2m_units2,
    ", n_ppu>2m_units<=2_hh>=20=", n_ppu_2m_units2_hh20,
    log_file = log_file
  )

  rtt_offenders <- rtt_panel[
    is.finite(price_per_unit) & price_per_unit > 2e6,
    .(PID, year, building_type, total_consideration, total_units, max_households, price_per_unit)
  ][order(-price_per_unit)][1:min(2000L, .N)]

  rtt_offenders_out <- p_out(cfg, "qa", "rtt_price_per_unit_offenders.csv")
  fwrite(rtt_offenders, rtt_offenders_out)
  logf("Wrote RTT price_per_unit offenders: ", nrow(rtt_offenders),
       " rows to ", rtt_offenders_out, log_file = log_file)
}

# QA: InfoUSA addresses with many households but unmatched or oversubscribed.
# These suggest address mismatches where e.g. residents report "133 Main St"
# but the parcel is registered as "123 Main St".
logf("Running InfoUSA address coverage diagnostics...", log_file = log_file)

# Part A: Unmatched addresses — high-household InfoUSA addresses with no parcel match
unmatched_addr <- infousa_m[
  is.na(PID),
  .(max_hh = uniqueN(family_id),
    years_observed = uniqueN(year),
    example_year = max(year)),
  by = n_sn_ss_c
]
unmatched_addr <- unmatched_addr[max_hh >= 5]
setorder(unmatched_addr, -max_hh)

# Enrich with address components from first observation
unmatched_detail <- merge(
  unmatched_addr,
  unique(philly_infousa_dt[, .(n_sn_ss_c, zip, pm.street, pm.house)], by = "n_sn_ss_c"),
  by = "n_sn_ss_c", all.x = TRUE
)
setorder(unmatched_detail, -max_hh)

unmatched_out <- p_out(cfg, "qa", "infousa_unmatched_high_hh_addresses.csv")
fwrite(unmatched_detail[1:min(5000L, .N)], unmatched_out)
logf(sprintf(
  "[QA InfoUSA unmatched] %d addresses with >=5 households have NO parcel match (total unmatched hh=%d). Top 5000 written to %s",
  nrow(unmatched_addr),
  sum(unmatched_addr$max_hh),
  unmatched_out
), log_file = log_file)

# Part B: Oversubscribed addresses — matched, but households >> imputed units.
# Aggregate panel to PID-level max for comparison.
pid_units <- panel[year >= 2011 & year <= 2019,
  .(total_units = max(total_units, na.rm = TRUE)),
  by = PID
]
oversub <- merge(hh_agg, pid_units, by = "PID", all.x = TRUE)
oversub[, ratio := max_households / total_units]
oversub <- oversub[!is.na(ratio) & max_households >= 10 & ratio > 2]
setorder(oversub, -ratio)

# Enrich with address from parcel data
oversub <- merge(
  oversub,
  parcel_agg[, .(PID, n_sn_ss_c, structure_bin, building_type)],
  by = "PID", all.x = TRUE
)

# Also find nearby unmatched addresses in the same zip to flag potential mismatches.
# For each oversubscribed PID, check if there's an unmatched address in the same zip
# with similar household counts — a smoking gun for address mismatch.
oversub_parcel_zip <- merge(
  oversub,
  philly_parcels[, .(PID, pm.zip)],
  by = "PID", all.x = TRUE
)

# Add nearby unmatched address info (same zip, high hh)
if (nrow(unmatched_detail) > 0 && nrow(oversub_parcel_zip) > 0) {
  # For each oversubscribed PID's zip, find unmatched addresses in that zip
  unmatched_by_zip <- unmatched_detail[, .(
    unmatched_addresses_in_zip = .N,
    unmatched_hh_in_zip = sum(max_hh),
    top_unmatched_addr = paste0(pm.house[1], " ", pm.street[1], " (hh=", max_hh[1], ")")
  ), by = .(zip)]

  unmatched_by_zip[, zip := as.character(zip)]
  oversub_parcel_zip[, pm.zip := as.character(pm.zip)]
  oversub_parcel_zip <- merge(
    oversub_parcel_zip,
    unmatched_by_zip,
    by.x = "pm.zip", by.y = "zip",
    all.x = TRUE
  )
}

oversub_out <- p_out(cfg, "qa", "infousa_oversubscribed_addresses.csv")
fwrite(oversub_parcel_zip[1:min(5000L, .N)], oversub_out)
logf(sprintf(
  "[QA InfoUSA oversubscribed] %d PIDs with max_hh>=10 and ratio>2 (hh/units). Top 5000 written to %s",
  nrow(oversub), oversub_out
), log_file = log_file)

# Summary stats
if (nrow(oversub) > 0) {
  logf(sprintf(
    "  Oversubscribed breakdown: median ratio=%.1f, mean ratio=%.1f, max ratio=%.1f, total excess hh=%d",
    median(oversub$ratio), mean(oversub$ratio), max(oversub$ratio),
    as.integer(sum(oversub$max_households - oversub$total_units))
  ), log_file = log_file)
}

# ---- Identify rental PIDs for scaling ----
# A PID-year qualifies as "rental" if it has any rental signal that year
panel[, rental_year := as.integer(
  (rental_from_altos   %in% TRUE) |
  (rental_from_license %in% TRUE) |
  (rental_from_evict   %in% TRUE) |
  (fifelse(is.na(num_filings), 0L, num_filings) > 0)
)]
panel[, n_rental_years := sum(rental_year, na.rm = TRUE), by = PID]
panel[, is_rental := n_rental_years > 3L]

rental_pid_summary <- panel[year == 2010, .(
  total_pids  = uniqueN(PID),
  rental_pids = uniqueN(PID[is_rental == TRUE]),
  pct_rental  = round(100 * uniqueN(PID[is_rental == TRUE]) / uniqueN(PID), 1)
)]
logf("Rental PID filter (>3 qualifying years): ",
     rental_pid_summary$rental_pids, " / ", rental_pid_summary$total_pids,
     " PIDs (", rental_pid_summary$pct_rental, "%)",
     log_file = log_file)

# Scale occupancy rate to unit-weighted mean = 0.9 in 2010, per num_units_bin
# Each bin gets its own scaling factor so all bins hit 0.9 independently
# Only rental PIDs contribute to the scale factor (includes their vacant years)

## Create num_units_bin for scaling (same breaks as make-analytic-sample.R)
panel[, num_units_bin := cut(
  total_units,
  breaks = c(-Inf, 1, 5, 20, 50, Inf),
  labels = c("1", "2-5", "6-20", "21-50", "51+"),
  include.lowest = TRUE
)]

## Compute per-bin scaling factor from 2010 anchor among rental PIDs
## Includes vacant years for rental PIDs so vacancy is reflected in the mean
scale_factors <- panel[
  year == 2010 & total_units > 0 & is_rental == TRUE,
  .(mean_occ_2010 = weighted.mean(occupancy_rate, w = total_units, na.rm = TRUE),
    n_rental_pids  = .N,
    total_units    = sum(total_units)),
  by = num_units_bin
]
scale_factors[, scale_factor := 0.9 / mean_occ_2010]

# Fix 3: Guard — scale factors near 1.0 indicate that fixes 1-2 worked.
# Factors > 1.5 indicate residual unit/occupancy bias.
if (any(scale_factors$scale_factor > 1.5, na.rm = TRUE)) {
  logf("WARNING: scale factor > 1.5 detected — residual unit/occupancy bias may remain",
       log_file = log_file)
}
if (all(scale_factors$scale_factor <= 1.3, na.rm = TRUE)) {
  logf("  [Fix 3] All scale factors <= 1.3 — fixes 1-2 appear effective", log_file = log_file)
}

logf("Per-bin 2010 occupancy scaling factors (rental PIDs only):", log_file = log_file)
for (r in 1:nrow(scale_factors)) {
  logf("  bin=", scale_factors$num_units_bin[r],
       " n_rental=", scale_factors$n_rental_pids[r],
       " total_units=", scale_factors$total_units[r],
       " mean_occ_2010=", round(scale_factors$mean_occ_2010[r], 4),
       " scale_factor=", round(scale_factors$scale_factor[r], 4),
       log_file = log_file)
}

## Merge scale factors and apply
panel <- merge(panel, scale_factors[, .(num_units_bin, scale_factor)],
               by = "num_units_bin", all.x = TRUE)

## For bins with no 2010 data (unlikely), fall back to global factor
global_factor <- 0.9 / panel[year == 2010 & total_units > 0 & is_rental == TRUE,
  weighted.mean(occupancy_rate, w = total_units, na.rm = TRUE)]
panel[is.na(scale_factor), scale_factor := global_factor]

## Apply: each building's occupancy_rate × its bin's scale factor
panel[total_units > 0, rental_occupancy_rate_scaled := occupancy_rate * scale_factor]

## Clamp to [0, 1]
panel[rental_occupancy_rate_scaled > 1, rental_occupancy_rate_scaled := 1]
panel[rental_occupancy_rate_scaled < 0, rental_occupancy_rate_scaled := 0]

## Clean up temp columns
panel[, c("scale_factor", "rental_year") := NULL]

# log sanity checks: per-bin occupancy rates by year (rental PIDs only)
panel[, total_units := as.numeric(total_units)]
panel[, renter_occ := as.numeric(renter_occ)]
panel_summary <- panel[total_units > 0 & is_rental == TRUE, .(
  n_parcels       = .N,
  total_units_sum = sum(total_units, na.rm = TRUE),
  renter_occ_sum  = sum(renter_occ, na.rm = TRUE),
  occ_raw_wm      = weighted.mean(occupancy_rate, w = total_units, na.rm = TRUE),
  occ_scaled_wm   = weighted.mean(rental_occupancy_rate_scaled, w = total_units, na.rm = TRUE)
), by = .(num_units_bin, year)]
setorder(panel_summary, year, num_units_bin)

# log panel summary (per-bin × year, rental PIDs)
logf("--- Per-bin x year occupancy summary (rental PIDs) ---", log_file = log_file)
for (r in 1:nrow(panel_summary)) {
  logf(
    "  Year ", panel_summary$year[r],
    " bin=", panel_summary$num_units_bin[r],
    ", n=", panel_summary$n_parcels[r],
    ", total_units=", panel_summary$total_units_sum[r],
    ", renter_occ=", panel_summary$renter_occ_sum[r],
    ", occ_raw_wm=", round(panel_summary$occ_raw_wm[r], 4),
    ", occ_scaled_wm=", round(panel_summary$occ_scaled_wm[r], 4),
    log_file = log_file
  )
}

# log per building_type × year occupancy summary (rental PIDs)
btype_summary <- panel[total_units > 0 & is_rental == TRUE & !is.na(building_type), .(
  n_parcels       = .N,
  total_units_sum = sum(total_units, na.rm = TRUE),
  renter_occ_sum  = sum(renter_occ, na.rm = TRUE),
  occ_raw_wm      = weighted.mean(occupancy_rate, w = total_units, na.rm = TRUE),
  occ_scaled_wm   = weighted.mean(rental_occupancy_rate_scaled, w = total_units, na.rm = TRUE)
), by = .(building_type, year)]
setorder(btype_summary, year, building_type)

logf("--- Per building_type x year occupancy summary (rental PIDs) ---", log_file = log_file)
for (r in 1:nrow(btype_summary)) {
  logf(
    "  Year ", btype_summary$year[r],
    " type=", btype_summary$building_type[r],
    ", n=", btype_summary$n_parcels[r],
    ", total_units=", btype_summary$total_units_sum[r],
    ", renter_occ=", btype_summary$renter_occ_sum[r],
    ", occ_raw_wm=", round(btype_summary$occ_raw_wm[r], 4),
    ", occ_scaled_wm=", round(btype_summary$occ_scaled_wm[r], 4),
    log_file = log_file
  )
}

# write building_type summary as CSV for easy inspection
btype_out <- p_out(cfg, "qa", "occupancy_by_building_type_year.csv")
fwrite(btype_summary, btype_out)
logf("Wrote building_type summary: ", btype_out, log_file = log_file)


# -------------------------------
# 9) ASSERTIONS + EXPORT
# -------------------------------
logf("Running final assertions...", log_file = log_file)

assert_unique(panel, c("PID", "year"), "panel final")
assert_has_cols(panel, c("PID","year","total_units","occupancy_rate"), "panel")

## Drop columns used only for scaling/logging — building_type will be
## re-derived from parcels_clean in make-analytic-sample.R and would
## otherwise cause .x/.y suffix conflicts on merge.
panel[, c("building_type", "is_rental", "n_rental_years") := NULL]

out_path_all  <- p_product(cfg, "parcel_occupancy_panel")
out_path_rent <- p_product(cfg, "parcel_occupancy_rentals_only")

fwrite(panel, out_path_all)
logf("Wrote parcel_occupancy_panel: ", nrow(panel), " rows to ", out_path_all, log_file = log_file)

# Rental-only subset (fix old bug: rental_from_altos is boolean; do NOT use > 1)
panel[, ever_rental_license := any(rental_from_license, na.rm = TRUE), by = PID]
panel[, ever_rental_altos   := any(rental_from_altos,   na.rm = TRUE), by = PID]
panel[, ever_evict          := any(num_filings > 0,     na.rm = TRUE), by = PID]

# repeat but years
panel[,num_years_license := sum(rental_from_license, na.rm=TRUE), by=PID]
panel[,num_years_altos   := sum(rental_from_altos,   na.rm=TRUE), by=PID]
panel[,num_filings       := sum(num_filings,       na.rm=TRUE), by=PID]

panel_rentals <- panel[
  (rental_from_altos %||% FALSE) |
    (rental_from_license %||% FALSE) |
    ((num_filings > 5)) |
    (num_years_license > 0) |
    (num_years_altos   > 3)
]



fwrite(panel_rentals, out_path_rent)
logf("Wrote parcel_occupancy_rentals_only: ", nrow(panel_rentals), " rows to ", out_path_rent, log_file = log_file)

logf("Total parcels: ", uniqueN(panel$PID), log_file = log_file)
logf("Parcels with rental evidence: ", uniqueN(panel_rentals$PID), log_file = log_file)
logf("=== Finished make-occupancy-vars.r (rewrite) ===", log_file = log_file)
