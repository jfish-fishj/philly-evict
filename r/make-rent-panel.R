## ============================================================
## make-rent-panel.R (formerly make-ever-rentals.R)
## ============================================================
## Purpose: Build the ever-rental panel combining Altos, licenses,
##          parcels, and evictions into a unified dataset.
##
## Inputs (from config):
##   - parcels_clean, parcel_building_summary, philly_bg_shp
##   - business_licenses (cleaned), altos_year_bedrooms (from make-altos-aggs.R)
##   - evictions_clean, evict_address_xwalk
##   - infousa_cleaned, infousa_address_xwalk
##   - tenure_bg_2010, uis_tr_2010
##
## Outputs (to config):
##   - ever_rentals_pid, ever_rentals_panel, ever_rental_parcel_units
##
## Primary keys:
##   - ever_rentals_pid: PID
##   - ever_rentals_panel: PID × year
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(sf)
  library(tidyverse)
  library(fixest)
})

# ---- Load config and set up logging ----
source("r/config.R")
source("R/helper-functions.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "make-rent-panel.log")

logf("=== Starting make-rent-panel.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

normalize_pid <- function(x) {
  stringr::str_pad(as.character(x), width = 9, side = "left", pad = "0")
}

# ---- Load input data ----
logf("Loading input data...", log_file = log_file)

philly_parcels     <- fread(p_product(cfg, "parcels_clean"))
logf("  parcels_clean: ", nrow(philly_parcels), " rows", log_file = log_file)

philly_building_df <- fread(p_product(cfg, "parcel_building_summary"))
logf("  parcel_building_summary: ", nrow(philly_building_df), " rows", log_file = log_file)

philly_bg          <- st_read(p_input(cfg, "philly_bg_shp"), quiet = TRUE)
logf("  philly_bg_shp: ", nrow(philly_bg), " block groups", log_file = log_file)

philly_lic         <- fread(p_input(cfg, "business_licenses"))
logf("  business_licenses: ", nrow(philly_lic), " rows", log_file = log_file)

philly_altos       <- fread(p_product(cfg, "altos_year_bedrooms"))
logf("  altos_year_bedrooms: ", nrow(philly_altos), " rows", log_file = log_file)

philly_evict       <- fread(p_product(cfg, "evictions_clean"))
logf("  evictions_clean: ", nrow(philly_evict), " rows", log_file = log_file)

evict_xwalk        <- fread(p_product(cfg, "evict_address_xwalk"))
logf("  evict_address_xwalk: ", nrow(evict_xwalk), " rows", log_file = log_file)

philly_infousa_dt  <- fread(p_input(cfg, "infousa_cleaned"))
logf("  infousa_cleaned: ", nrow(philly_infousa_dt), " rows", log_file = log_file)

info_usa_xwalk     <- fread(p_product(cfg, "infousa_address_xwalk"))
logf("  infousa_address_xwalk: ", nrow(info_usa_xwalk), " rows", log_file = log_file)

tenure_bg_2010     <- fread(p_input(cfg, "tenure_bg_2010"))
logf("  tenure_bg_2010: ", nrow(tenure_bg_2010), " rows", log_file = log_file)

uis_tr_2010        <- fread(p_input(cfg, "uis_tr_2010"))
logf("  uis_tr_2010: ", nrow(uis_tr_2010), " rows", log_file = log_file)


## ============================================================
## 2) EVER-RENTALS: PREP PIECES
## ============================================================

## 2.1 Parcel backbone (minimal version)
## Spatial join parcels → BG, standardize key fields.

prep_parcel_backbone <- function(philly_parcels, philly_bg) {
  philly_parcels <- copy(philly_parcels)
  setDT(philly_parcels)

  if (!"PID" %in% names(philly_parcels) && "parcel_number" %in% names(philly_parcels)) {
    philly_parcels[, PID := parcel_number]
  }

  philly_parcels[, PID := normalize_pid(PID)]

  # One row per PID (first obs)
  parcels_first <- philly_parcels[, .SD[1], by = PID]

  # Spatial join to BG (expects geocode_x/geocode_y in same CRS as philly_bg)
  stopifnot(all(c("geocode_x", "geocode_y") %in% names(parcels_first)))

  # Filter out rows with missing coordinates (can't geocode)
  n_before <- nrow(parcels_first)
  parcels_first <- parcels_first[!is.na(geocode_x) & !is.na(geocode_y)]
  n_dropped <- n_before - nrow(parcels_first)
  if (n_dropped > 0) {
    message("  Dropped ", n_dropped, " parcels with missing coordinates")
  }

  parcels_sf <- st_as_sf(
    parcels_first,
    coords = c("geocode_x", "geocode_y"),
    crs    = st_crs(philly_bg)
  )
  parcels_sf <- st_join(parcels_sf, philly_bg)

  parcels_dt <- as.data.table(parcels_sf)[, geometry := NULL]

  # Extract stories from building code description
  if ("building_code_description" %in% names(parcels_dt)) {
    parcels_dt[, stories_from_code := extract_stories_from_code(building_code_description)]
  }

  # Standardize building type using both old and new columns
  if (all(c("building_code_description", "building_code_description_new") %in% names(parcels_dt))) {
    parcels_dt[, building_type := standardize_building_type(
      bldg_code_desc = building_code_description,
      bldg_code_desc_new = building_code_description_new,
      num_bldgs = if ("num_bldgs" %in% names(parcels_dt)) num_bldgs else NA_integer_,
      num_stories = if ("stories_from_code" %in% names(parcels_dt)) stories_from_code else NA_real_
    )]

    # Also create legacy column for backward compatibility with models
    parcels_dt[, building_code_description_new_fixed := fcase(
      building_type == "ROW", "ROW",
      building_type == "TWIN", "TWIN",
      building_type == "LOWRISE_MULTI", "LOW RISE APARTMENTS",
      building_type == "MIDRISE_MULTI", "MID RISE APARTMENTS",
      building_type == "HIGHRISE_MULTI", "HIGH RISE APARTMENTS",
      building_type == "MULTI_BLDG_COMPLEX", "GARDEN APARTMENTS",
      building_type %in% c("SMALL_MULTI_2_4", "OTHER"), "APARTMENTS OTHER",
      building_type == "CONDO", "CONDO",
      building_type == "DETACHED", "OTHER",
      building_type == "COMMERCIAL_MIXED", "OTHER",
      default = "OTHER"
    )]
  } else if ("building_code_description_new" %in% names(parcels_dt)) {
    # Fallback if only new column exists (legacy behavior)
    parcels_dt[, building_code_description_new_fixed := fcase(
      grepl("ROW", building_code_description_new, ignore.case = TRUE), "ROW",
      grepl("TWIN", building_code_description_new, ignore.case = TRUE), "TWIN",
      grepl("APARTMENTS|APTS", building_code_description_new, ignore.case = TRUE) &
        grepl("LOW", building_code_description_new, ignore.case = TRUE), "LOW RISE APARTMENTS",
      grepl("APARTMENTS|APTS", building_code_description_new, ignore.case = TRUE) &
        grepl("MID", building_code_description_new, ignore.case = TRUE), "MID RISE APARTMENTS",
      grepl("APARTMENTS|APTS", building_code_description_new, ignore.case = TRUE) &
        grepl("HIGH", building_code_description_new, ignore.case = TRUE), "HIGH RISE APARTMENTS",
      grepl("APARTMENTS|APTS", building_code_description_new, ignore.case = TRUE) &
        grepl("GARDEN", building_code_description_new, ignore.case = TRUE), "GARDEN APARTMENTS",
      grepl("APARTMENTS|APTS", building_code_description_new, ignore.case = TRUE), "APARTMENTS OTHER",
      grepl("CONDO", building_code_description_new, ignore.case = TRUE), "CONDO",
      grepl("OLD", building_code_description_new, ignore.case = TRUE), "OLD STYLE",
      is.na(building_code_description_new), NA_character_,
      default = "OTHER"
    )]
  }

  parcels_dt[]
}

## 2.2 Altos + License panel
## Roughly mirrors the rent/license prep in the analytic script, but stripped down.

prep_rent_altos_simple <- function(philly_lic, philly_altos) {
  lic   <- copy(philly_lic)
  altos <- copy(philly_altos)
  setDT(lic); setDT(altos)

  ## --- Clean / filter licenses to residential rentals ---
  if (!"modal_rentalcategory" %in% names(lic) && "rentalcategory" %in% names(lic)) {
    lic[, rentalcategory := fifelse(rentalcategory == "", NA_character_, rentalcategory)]
    lic[, modal_rentalcategory := Mode(rentalcategory, na.rm = TRUE),
        by = .(opa_account_num, ownercontact1name)]
  }

  lic <- lic[licensetype == "Rental" &
               (modal_rentalcategory %in% c("Residential Dwellings", "Other"))]

  lic[, start_year := as.numeric(substr(initialissuedate, 1, 4))]
  lic[, end_year   := as.numeric(substr(expirationdate, 1, 4))]
  lic[, num_years  := end_year - start_year]
  lic[, id         := .I]

  # Normalize pm.zip: strip "_" prefix if present, pad to 5 digits
  lic[, pm.zip := coalesce(
    stringr::str_remove(as.character(pm.zip), "^_"),
    stringr::str_sub(zip, 1, 5)
  ) %>% stringr::str_pad(5, "left", "0")]

  ## Expand to one row per PID × year
  lic_long <- lic[num_years > 0]
  lic_long <- lic_long[, .SD[rep(1L, num_years)], by = id]
  lic_long[, year := start_year + seq_len(.N) - 1L, by = id]
  lic_long[, opa_account_num := normalize_pid(opa_account_num)]
  lic_long[, PID := opa_account_num]
  lic_long[, numberofunits := as.double(numberofunits)]

  ## PID-level median units from licenses
  lic_units <- lic_long[
    year <= 2024,
    .(
      num_units       = median(numberofunits, na.rm = TRUE),
      mode_num_units  = Mode(numberofunits, na.rm = TRUE),
      mean_num_units  = mean(numberofunits, na.rm = TRUE),
      max_units = max(numberofunits, na.rm = TRUE),
      min_units = min(numberofunits, na.rm = TRUE)
    ),
    by = .(PID)
  ]

  ## Minimal license panel: PID × year
  lic_long_min <- lic_long[
    ,
    .(
      PID,
      year,
      pm.zip,
      numberofunits
    )
  ]

  ## --- Altos panel cleanup ---
  altos[, PID := normalize_pid(PID)]
  altos <- altos[
    med_price > 500 & med_price < 7500 &
      year %in% 2006:2024
  ]

  ## Fix beds / baths if available
  if ("beds_imp_first" %in% names(altos)) {
    altos[, beds_imp_first := as.numeric(beds_imp_first)]
  }
  if ("baths_first" %in% names(altos)) {
    altos[, baths_first := as.numeric(baths_first)]
  }

  altos[, source        := "altos"]
  altos[, log_med_price := log(med_price)]

  rent <- altos[
    ,
    .(
      PID,
      year,
      month,
      med_price,
      beds_imp_first,
      baths_first,
      source
    )
  ]

  list(
    rent        = rent[],
    lic_units   = lic_units[],
    lic_long    = lic_long[],
    lic_long_min = lic_long_min[]
  )
}

## 2.3 Evictions → parcel-level aggregates (simplified)
## This assumes you have evict_xwalk (n_sn_ss_c → PID) and philly_evict
## with at least: n_sn_ss_c, plaintiff, defendant, year, ongoing_rent, etc.

prep_evictions_simple <- function(philly_evict, evict_xwalk) {
  ev <- copy(philly_evict)
  xw <- copy(evict_xwalk)
  setDT(ev); setDT(xw)

  # If you have clean_name() / business_regex in helper-functions.R, you can uncomment:
  # ev[, clean_defendant_name := clean_name(defendant)]
  # ev[, commercial_alt := str_detect(clean_defendant_name, business_regex)]
  # Here we just assume philly_evict already has a "commercial" flag

  ev[, dup := .N, by = .(n_sn_ss_c, plaintiff, defendant, d_filing)]
  ev[, dup := dup > 1]

  ev <- ev[
    commercial == "f" &
      dup == FALSE &
      year >= 2000 &
      total_rent <= 5e4 &
      !is.na(n_sn_ss_c)
  ]

  ## Join to PID, limit to unique parcel matches
  xw1 <- xw[num_parcels_matched == 1, .(PID, n_sn_ss_c)]
  xw1[, PID := normalize_pid(PID)]

  ev_m <- merge(ev, xw1, by = "n_sn_ss_c")

  ## Housing authority flag (can refine)
  ev_m[, housing_auth := str_detect(
    stringr::str_to_upper(plaintiff),
    "(PHILA.+A?UTH|HOUS.+AUTH|COMMON.+PENN)"
  )]

  ## Simple parcel-year aggregate
  # drop rnets < 500 and > 7500
  ev_m <- ev_m[ongoing_rent > 500 & ongoing_rent < 7500]
  ev_parcel_agg <- ev_m[
    ,
    .(
      num_filings              = sum(housing_auth == FALSE, na.rm = TRUE),
      num_filings_with_ha      = .N,
      med_eviction_rent        = suppressWarnings(median(ongoing_rent[housing_auth == FALSE], na.rm = TRUE)),
      pm.zip                   = first(pm.zip),
      month                    = first(month)
    ),
    by = .(year, PID)
  ]

  ## Complete PID-year grid so you get zeros where no filings
  grid <- CJ(
    PID  = unique(ev_parcel_agg$PID),
    year = unique(ev_parcel_agg$year)
  )

  ev_parcel_agg <- merge(
    grid,
    ev_parcel_agg,
    by    = c("PID", "year"),
    all.x = TRUE
  )

  ev_parcel_agg[
    ,
    `:=`(
      num_filings         = fifelse(is.na(num_filings), 0, num_filings),
      num_filings_with_ha = fifelse(is.na(num_filings_with_ha), 0, num_filings_with_ha)
    )
  ]

  ev_parcel_agg[]
}

## ============================================================
## 3) ASSEMBLE EVER-RENTAL PANEL
## ============================================================

build_ever_rentals_panel <- function(parcel_backbone,
                                     rent_list,
                                     evict_agg) {

  parcel_dt <- copy(parcel_backbone)
  rent      <- copy(rent_list$rent)
  lic_long  <- copy(rent_list$lic_long_min)
  lic_units <- copy(rent_list$lic_units)
  ev_pid_year <- copy(evict_agg)

  setDT(parcel_dt); setDT(rent); setDT(lic_long); setDT(lic_units); setDT(ev_pid_year)

  ## Ensure PIDs are padded
  parcel_dt[,     PID := normalize_pid(PID)]
  rent[,          PID := normalize_pid(PID)]
  lic_long[,      PID := normalize_pid(PID)]
  lic_units[,     PID := normalize_pid(PID)]
  ev_pid_year[,   PID := normalize_pid(PID)]

  ## --- PID-level ever-rental flags ---

  altos_pids  <- unique(rent$PID)
  lic_pids    <- unique(lic_long$PID)
  evict_pids  <- unique(ev_pid_year$PID)

  ever_rental_pid <- unique(c(altos_pids, lic_pids, evict_pids))

  pid_summary <- data.table(PID = ever_rental_pid)

  pid_summary[
    ,
    `:=`(
      ever_rental_altos   = PID %in% altos_pids,
      ever_rental_license = PID %in% lic_pids,
      ever_rental_evict   = PID %in% evict_pids
    )
  ]
  pid_summary[, ever_rental_any := (ever_rental_altos | ever_rental_license | ever_rental_evict)]

  ## Attach parcel-level covariates
  pid_summary <- merge(
    pid_summary,
    parcel_dt,
    by = "PID",
    all.x = TRUE
  )

  ## --- PID × year panel ---

  rent_year <- rent[
    ,
    .(
      med_rent_altos = median(med_price, na.rm = TRUE),
      n_altos_listings = .N
    ),
    by = .(PID, year)
  ]

  lic_year <- lic_long[
    ,
    .(
      lic_num_units = median(numberofunits, na.rm = TRUE),
      pm.zip        = first(pm.zip)
    ),
    by = .(PID, year)
  ]

  ev_year <- ev_pid_year[
    ,
    .(
      num_filings              = sum(num_filings, na.rm = TRUE),
      num_filings_with_ha      = sum(num_filings_with_ha, na.rm = TRUE),
      med_eviction_rent        = median(med_eviction_rent, na.rm = TRUE)
    ),
    by = .(PID, year)
  ]

  all_years <- sort(unique(c(rent_year$year, lic_year$year, ev_year$year)))
  # just years in 2006:2024
  all_years <- all_years[all_years >= 2006 & all_years <= 2024]
  panel_skeleton <- CJ(
    PID  = ever_rental_pid,
    year = all_years
  )

  ever_panel <- Reduce(
    f = function(d1, d2) merge(d1, d2, by = c("PID", "year"), all.x = TRUE, all.y = FALSE),
    x = list(panel_skeleton, rent_year, lic_year, ev_year)
  )

  ever_panel[
    ,
    `:=`(
      rental_from_altos   = !is.na(med_rent_altos),
      rental_from_license = !is.na(lic_num_units),
      rental_from_evict   = num_filings > 0
    )
  ]
  ever_panel[
    ,
    ever_rental_any_year := (rental_from_altos | rental_from_license | rental_from_evict)
  ]

  ever_panel <- merge(
    ever_panel,
    parcel_dt,
    by = "PID",
    all.x = TRUE
  )

  list(
    ever_rentals_pid   = pid_summary[],
    ever_rentals_panel = ever_panel[]
  )
}

## ============================================================
## 4) UNIT IMPUTATION (UNITS_MODEL-STYLE)
## ============================================================
## This is a simplified version of the make-occupancy-vars logic.
## It:
##  - Builds parcel-level features (buildings, households, licenses)
##  - Fits a feols regression to predict units
##  - Creates num_units_imp per parcel
##  - DOES NOT do BG-level raking / occupancy; you can layer that on
##    later if you want the full constrained version.

build_units_imputation <- function(philly_parcels,
                                   philly_building_df,
                                   philly_infousa_dt,
                                   info_usa_xwalk,
                                   philly_rentals) {

  parcels <- copy(philly_parcels)
  bldgs   <- copy(philly_building_df)
  iu      <- copy(philly_infousa_dt)
  xwalk   <- copy(info_usa_xwalk)
  rents   <- copy(philly_rentals)

  setDT(parcels); setDT(bldgs); setDT(iu); setDT(xwalk); setDT(rents)

  parcels[, PID := as.numeric(parcel_number)]
  bldgs[,   PID := as.numeric(PID)]
  rents[,   PID := as.numeric(PID)]
  xwalk[,   PID := as.numeric(PID)]

  ## --- Building aggregates (by PID) ---
  ## You may need to adjust column names here
  bldg_agg <- bldgs[
    ,
    .(
      num_bldgs_imp      = .N,
      total_area         = sum(square_ft, na.rm = TRUE),
      #total_livable_area = sum(livable_area, na.rm = TRUE),
      num_stories_imp    = median(round(max_hgt/10) , na.rm = TRUE)
    ),
    by = PID
  ]

  parcel_bldg <- merge(
    parcels,
    bldg_agg,
    by    = "PID",
    all.x = TRUE
  )

  ## --- License-based units per PID ---
  rents[, numberofunits := as.numeric(numberofunits)]
  lic_units <- rents[
    !is.na(numberofunits),
    .(
      num_units       = median(numberofunits, na.rm = TRUE),
      mode_num_units  = Mode(numberofunits, na.rm = TRUE),
      mean_num_units  = mean(numberofunits, na.rm = TRUE)
    ),
    by = PID
  ]

  parcel_bldg <- merge(
    parcel_bldg,
    lic_units,
    by    = "PID",
    all.x = TRUE
  )

  ## --- InfoUSA households per PID (restrict to unique matches) ---
  xwalk_1 <- xwalk[num_parcels_matched == 1 & !is.na(PID)]

  iu_m <- merge(
    iu,
    xwalk_1[, .(n_sn_ss_c, PID)],
    by    = "n_sn_ss_c",
    all.x = FALSE
  )

  iu_m[,num_households := .N, by = .(PID, year)]

  ## Assumes iu has a num_households var; adjust if needed
  if (!"num_households" %in% names(iu_m)) {
    stop("philly_infousa_dt / iu_m must have a num_households column for this template.")
  }

  hh_agg <- iu_m[
    ,
    .(
      max_households    = max(num_households, na.rm = TRUE),
      median_households = median(num_households, na.rm = TRUE),
      mean_households   = mean(num_households, na.rm = TRUE)
    ),
    by = PID
  ]

  parcel_agg_for_model <- merge(
    parcel_bldg,
    hh_agg,
    by    = "PID",
    all.x = TRUE
  )

  ## --- Clean num_units for modeling ---
  parcel_agg_for_model[
    ,
    num_units_fixed := as.integer(num_units)
  ]
  parcel_agg_for_model[
    num_units_fixed <= 0,
    num_units_fixed := NA_integer_
  ]

  parcel_agg_for_model[,total_area := coalesce(total_area.x, total_area.y)]
  parcel_agg_for_model[,num_stories_imp := coalesce(num_stories_imp, number_stories)]
  parcel_agg_for_model[,num_bldgs_imp := coalesce(num_bldgs_imp, 1)]
  ## Year built decade (if you have year_built)
  if ("year_built" %in% names(parcel_agg_for_model)) {
    parcel_agg_for_model[
      ,
      year_built_decade := floor(year_built / 10) * 10
    ]
  } else {
    parcel_agg_for_model[, year_built_decade := NA_integer_]
  }

  ## --- Fit units_model (similar to make-occupancy-vars) ---
  ## Requires quality_grade_fixed & building_code_description_new_fixed; if not
  ## present, we just treat them as factors directly from existing columns.
  if (!"quality_grade_fixed" %in% names(parcel_agg_for_model) &&
      "quality_grade" %in% names(parcel_agg_for_model)) {
    parcel_agg_for_model[, quality_grade_fixed := as.factor(quality_grade)]
  }

  # Extract stories and standardize building type if not already done
  if (!"stories_from_code" %in% names(parcel_agg_for_model) &&
      "building_code_description" %in% names(parcel_agg_for_model)) {
    parcel_agg_for_model[, stories_from_code := extract_stories_from_code(building_code_description)]
  }

  if (!"building_type" %in% names(parcel_agg_for_model) &&
      all(c("building_code_description", "building_code_description_new") %in% names(parcel_agg_for_model))) {
    parcel_agg_for_model[, building_type := standardize_building_type(
      bldg_code_desc = building_code_description,
      bldg_code_desc_new = building_code_description_new,
      num_bldgs = if ("num_bldgs_imp" %in% names(parcel_agg_for_model)) num_bldgs_imp else NA_integer_,
      num_stories = if ("num_stories_imp" %in% names(parcel_agg_for_model)) num_stories_imp else stories_from_code
    )]
  }

  # Create legacy column for model (mapping from new building_type)
  if ("building_type" %in% names(parcel_agg_for_model)) {
    parcel_agg_for_model[, building_code_description_new_fixed := fcase(
      building_type == "ROW", "ROW",
      building_type == "TWIN", "TWIN",
      building_type == "LOWRISE_MULTI", "LOW RISE APARTMENTS",
      building_type == "MIDRISE_MULTI", "MID RISE APARTMENTS",
      building_type == "HIGHRISE_MULTI", "HIGH RISE APARTMENTS",
      building_type == "MULTI_BLDG_COMPLEX", "GARDEN APARTMENTS",
      building_type %in% c("SMALL_MULTI_2_4", "OTHER"), "APARTMENTS OTHER",
      building_type == "CONDO", "CONDO",
      building_type == "DETACHED", "OTHER",
      building_type == "COMMERCIAL_MIXED", "OTHER",
      default = "OTHER"
    )]
  } else if ("building_code_description_new" %in% names(parcel_agg_for_model)) {
    # Fallback to legacy behavior if building_type not available
    parcel_agg_for_model[, building_code_description_new_fixed := fcase(
      grepl("ROW", building_code_description_new, ignore.case = TRUE), "ROW",
      grepl("TWIN", building_code_description_new, ignore.case = TRUE), "TWIN",
      grepl("APARTMENTS|APTS", building_code_description_new, ignore.case = TRUE) &
        grepl("LOW", building_code_description_new, ignore.case = TRUE), "LOW RISE APARTMENTS",
      grepl("APARTMENTS|APTS", building_code_description_new, ignore.case = TRUE) &
        grepl("MID", building_code_description_new, ignore.case = TRUE), "MID RISE APARTMENTS",
      grepl("APARTMENTS|APTS", building_code_description_new, ignore.case = TRUE) &
        grepl("HIGH", building_code_description_new, ignore.case = TRUE), "HIGH RISE APARTMENTS",
      grepl("APARTMENTS|APTS", building_code_description_new, ignore.case = TRUE) &
        grepl("GARDEN", building_code_description_new, ignore.case = TRUE), "GARDEN APARTMENTS",
      grepl("APARTMENTS|APTS", building_code_description_new, ignore.case = TRUE), "APARTMENTS OTHER",
      grepl("CONDO", building_code_description_new, ignore.case = TRUE), "CONDO",
      grepl("OLD", building_code_description_new, ignore.case = TRUE), "OLD STYLE",
      is.na(building_code_description_new), NA_character_,
      default = "OTHER"
    )]
  }
  # make logs
  parcel_agg_for_model[,log_total_area :=  log(total_area)]
  parcel_agg_for_model[,log_total_livable_area := log(total_livable_area)]

  # drop infinites
  parcel_agg_for_model[
    log_total_area == -Inf | is.na(log_total_area),
    log_total_area := NA_real_
  ]
  parcel_agg_for_model[
    log_total_livable_area == -Inf | is.na(log_total_livable_area),
    log_total_livable_area := NA_real_
  ]


  units_model <- feols(
    num_units_fixed ~
      log_total_area + I(log_total_area^2) +
      log_total_livable_area + I(log_total_livable_area^2) +
      median_households + I(median_households^2) +
      num_bldgs_imp + I(num_bldgs_imp^2) +
      num_stories_imp + I(num_stories_imp^2) +
      #quality_grade_fixed +
      building_code_description_new_fixed +
      year_built_decade,
    data          = parcel_agg_for_model,
    combine.quick = FALSE,
    cluster = ~PID
  )

  message("[units_model] summary:")
  print(summary(units_model))

  parcel_agg_for_model[
    ,
    num_units_pred_new := predict(units_model, newdata = parcel_agg_for_model)
  ]

  ## --- Build num_units_imp ---
  parcel_agg_for_model[
    ,
    num_units_imp := fifelse(
      is.na(num_units),
      round(num_units_pred_new),
      round(num_units)
    )
  ]
  parcel_agg_for_model[
    ,
    num_units_imp := fifelse(num_units_imp <= 0, 1L, num_units_imp)
  ]

  ## Harmonize extreme gaps between num_units and max_households
  parcel_agg_for_model[
    !is.na(num_units) & !is.na(max_households) &
      abs(num_units_pred_new - num_units) > 50 &
      abs(max_households - num_units) > 50,
    num_units_imp := round(num_units_pred_new / 2)
  ]

  parcel_agg_for_model[
    (num_units_imp - max_households) > 100,
    num_units_imp := NA_integer_
  ]
  parcel_agg_for_model[
    (num_units_imp / pmax(1, max_households)) > 3,
    num_units_imp := NA_integer_
  ]

  ## Final PID-level table; drop down to just rentals
  out_units <- parcel_agg_for_model[
   # PID %in% rents$PID ,
   , .(
      PID              = normalize_pid(PID),
      num_units_imp    = num_units_imp,
      num_units_raw    = num_units,
      num_units_pred   = num_units_pred_new,
      max_households   = max_households,
      median_households = median_households
    )
  ]

  out_units[]
}


## ============================================================
## 5) RUN PIPELINE
## ============================================================

## 5.1 Parcel backbone
logf("Building parcel backbone...", log_file = log_file)
parcel_backbone <- prep_parcel_backbone(philly_parcels, philly_bg)
assert_unique(parcel_backbone, "PID", "parcel_backbone")
logf("  parcel_backbone: ", nrow(parcel_backbone), " rows, PID unique - PASSED", log_file = log_file)

## 5.2 Altos + licenses
logf("Preparing Altos + licenses...", log_file = log_file)
rent_list <- prep_rent_altos_simple(philly_lic, philly_altos)
logf("  rent: ", nrow(rent_list$rent), " rows", log_file = log_file)
logf("  lic_units: ", nrow(rent_list$lic_units), " rows", log_file = log_file)
logf("  lic_long: ", nrow(rent_list$lic_long), " rows", log_file = log_file)

## 5.3 Evictions
logf("Preparing evictions...", log_file = log_file)
evict_agg <- prep_evictions_simple(philly_evict, evict_xwalk)
assert_unique(evict_agg, c("PID", "year"), "evict_agg")
logf("  evict_agg: ", nrow(evict_agg), " rows, (PID, year) unique - PASSED", log_file = log_file)

## 5.4 Ever-rentals panel
ever_rental_out <- build_ever_rentals_panel(
  parcel_backbone = parcel_backbone,
  rent_list       = rent_list,
  evict_agg       = evict_agg
)

ever_rentals_pid   <- ever_rental_out$ever_rentals_pid
ever_rentals_panel <- ever_rental_out$ever_rentals_panel

assert_unique(ever_rentals_pid, "PID", "ever_rentals_pid")
logf("  ever_rentals_pid: ", nrow(ever_rentals_pid), " rows, PID unique - PASSED", log_file = log_file)

assert_unique(ever_rentals_panel, c("PID", "year"), "ever_rentals_panel")
logf("  ever_rentals_panel: ", nrow(ever_rentals_panel), " rows, (PID, year) unique - PASSED", log_file = log_file)

## 5.5 Unit imputation (parcel-level)
## For renters, you probably want a rentals-only version of philly_rentals
## that corresponds to the license_long_min from rent_list
philly_rentals_for_units <- rent_list$lic_long

parcel_units <- build_units_imputation(
  philly_parcels     = philly_parcels,
  philly_building_df = philly_building_df,
  philly_infousa_dt  = philly_infousa_dt,
  info_usa_xwalk     = info_usa_xwalk,
  philly_rentals     = philly_rentals_for_units
)

assert_unique(parcel_units, "PID", "parcel_units")
logf("  parcel_units: ", nrow(parcel_units), " rows, PID unique - PASSED", log_file = log_file)

## 5.6 Merge units onto ever-rentals
logf("Merging units onto ever-rentals...", log_file = log_file)

ever_rentals_pid <- merge(
  ever_rentals_pid,
  parcel_units,
  by    = "PID",
  all.x = TRUE
)
assert_unique(ever_rentals_pid, "PID", "ever_rentals_pid after units merge")
logf("  ever_rentals_pid after units merge: ", nrow(ever_rentals_pid), " rows, PID unique - PASSED", log_file = log_file)

ever_rentals_panel <- merge(
  ever_rentals_panel,
  parcel_units,
  by    = "PID",
  all.x = TRUE
)
assert_unique(ever_rentals_panel, c("PID", "year"), "ever_rentals_panel after units merge")
logf("  ever_rentals_panel after units merge: ", nrow(ever_rentals_panel), " rows, (PID, year) unique - PASSED", log_file = log_file)

## 5.7 Export
logf("Writing outputs...", log_file = log_file)

out_pid <- p_product(cfg, "ever_rentals_pid")
fwrite(ever_rentals_pid, out_pid)
logf("  Wrote ever_rentals_pid: ", nrow(ever_rentals_pid), " rows to ", out_pid, log_file = log_file)

out_panel <- p_product(cfg, "ever_rentals_panel")
fwrite(ever_rentals_panel, out_panel)
logf("  Wrote ever_rentals_panel: ", nrow(ever_rentals_panel), " rows to ", out_panel, log_file = log_file)

out_units <- p_product(cfg, "ever_rental_parcel_units")
fwrite(parcel_units, out_units)
logf("  Wrote ever_rental_parcel_units: ", nrow(parcel_units), " rows to ", out_units, log_file = log_file)

logf("=== Finished make-rent-panel.R ===", log_file = log_file)


# NOTE: Exploratory analysis code moved to analysis/ folder
# See analysis/rent-validation.R for correlation checks between Altos and eviction rents
