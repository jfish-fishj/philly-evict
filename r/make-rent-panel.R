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
    bldg_result <- standardize_building_type(
      bldg_code_desc = parcels_dt$building_code_description,
      bldg_code_desc_new = parcels_dt$building_code_description_new,
      num_bldgs = if ("num_bldgs" %in% names(parcels_dt)) parcels_dt$num_bldgs else NA_integer_,
      num_stories = if ("stories_from_code" %in% names(parcels_dt)) parcels_dt$stories_from_code else NA_real_
    )
    parcels_dt[, building_type := bldg_result$building_type]
    parcels_dt[, is_condo := bldg_result$is_condo]

    # Also create legacy column for backward compatibility with models
    parcels_dt[, building_code_description_new_fixed := fcase(
      building_type == "ROW", "ROW",
      building_type == "TWIN", "TWIN",
      building_type == "LOWRISE_MULTI", "LOW RISE APARTMENTS",
      building_type == "MIDRISE_MULTI", "MID RISE APARTMENTS",
      building_type == "HIGHRISE_MULTI", "HIGH RISE APARTMENTS",
      building_type == "MULTI_BLDG_COMPLEX", "GARDEN APARTMENTS",
      building_type %in% c("SMALL_MULTI_2_4", "OTHER"), "APARTMENTS OTHER",
      building_type == "DETACHED", "OTHER",
      building_type == "COMMERCIAL", "OTHER",
      building_type == "GROUP_QUARTERS", "OTHER",
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

  # Explicit non-residential categories to exclude
  nonres_categories <- c("Hotel", "Rooming House / Boarding House")

  # Tag owner-parcels that have ANY non-residential category in their history
  lic[, has_nonres := any(rentalcategory %in% nonres_categories, na.rm = TRUE),
      by = .(opa_account_num)]

  # Log excluded license counts by category
  n_before <- nrow(lic[licensetype == "Rental"])
  n_hotel <- lic[licensetype == "Rental" & modal_rentalcategory == "Hotel", .N]
  n_rooming <- lic[licensetype == "Rental" &
                     modal_rentalcategory == "Rooming House / Boarding House", .N]
  n_other_nonres <- lic[licensetype == "Rental" &
                          modal_rentalcategory == "Other" & has_nonres == TRUE, .N]

  # Keep "Residential Dwellings" always;
  # Keep "Other" (blank) only if the owner-parcel has no non-residential evidence
  lic <- lic[licensetype == "Rental" &
               (modal_rentalcategory == "Residential Dwellings" |
                (modal_rentalcategory == "Other" & has_nonres == FALSE))]

  n_after <- nrow(lic)
  message(sprintf(
    "  License filter: %d -> %d (excluded: %d Hotel, %d Rooming House, %d Other-with-nonres-evidence)",
    n_before, n_after, n_hotel, n_rooming, n_other_nonres
  ))

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

build_ever_rentals_panel <- function(rent_list,
                                     evict_agg) {

  rent      <- copy(rent_list$rent)
  lic_long  <- copy(rent_list$lic_long_min)
  lic_units <- copy(rent_list$lic_units)
  ev_pid_year <- copy(evict_agg)

  setDT(rent); setDT(lic_long); setDT(lic_units); setDT(ev_pid_year)

  ## Ensure PIDs are padded
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

  ## Attach license unit summaries (mean, median, mode); NA when missing
  pid_summary <- merge(
    pid_summary,
    lic_units[, .(PID, num_units, mean_num_units, mode_num_units)],
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
      med_eviction_rent        = median(med_eviction_rent, na.rm = TRUE),
      pm.zip                   = first(pm.zip)
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

  ## Coalesce pm.zip: license > eviction, then fill within PID
  if ("pm.zip.x" %in% names(ever_panel) && "pm.zip.y" %in% names(ever_panel)) {
    ever_panel[, pm.zip := coalesce(pm.zip.x, pm.zip.y)]
    ever_panel[, c("pm.zip.x", "pm.zip.y") := NULL]
  }
  ## Normalize empty / garbage zips to NA, then fill within PID
  ever_panel[pm.zip == "" | is.na(pm.zip), pm.zip := NA_character_]
  ever_panel[, pm.zip := pm.zip[!is.na(pm.zip)][1], by = PID]

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

  list(
    ever_rentals_pid   = pid_summary[],
    ever_rentals_panel = ever_panel[]
  )
}


## ============================================================
## 4) ASSIGN P(RENTAL)
## ============================================================
#' Add rental evidence intensity scores to a PID x year panel (data.table)
#'
#' Expected columns (some optional):
#'   - pid (or parcel id), year
#'   - n_altos_listings (optional; defaults to 0)
#'   - num_filings (optional; defaults to 0)
#'   - num_filings_with_ha (optional; defaults to 0)
#'   - lic_num_units (optional; used as license presence indicator)
#'
#' Adds (per year):
#'   - intensity_altos_y
#'   - intensity_evict_y
#'   - intensity_license_y
#'   - intensity_total_y
#'   - intensity_total_y_recent (decayed by recency vs as_of_year)
#'
#' Notes:
#'   - Altos & eviction components are robust-normalized *within year* to reduce coverage drift.
#'   - License is treated as a presence indicator by default (can be changed).
#'   - This function does NOT infer units; it is an evidence strength score only.
add_rental_intensity <- function(
    dt,
    id_col = "pid",
    year_col = "year",
    altos_count_col = "n_altos_listings",
    evict_count_col = "num_filings",
    evict_ha_count_col = "num_filings_with_ha",
    license_units_col = "lic_num_units",
    weights = list(altos = 0.7, evict = 0.7, license = 1.0, evict_ha = 0.0),
    decay_base = 0.7,
    as_of_year = NULL,
    clamp_component_at_zero = TRUE
) {
  stopifnot(data.table::is.data.table(dt))

  # --- helpers ---
  robust_z <- function(x) {
    x <- as.numeric(x)
    med <- stats::median(x, na.rm = TRUE)
    madv <- stats::mad(x, constant = 1, na.rm = TRUE) # constant=1 for a pure MAD scale
    if (!is.finite(madv) || madv <= 0) {
      return(rep(0, length(x)))
    }
    z <- (x - med) / madv
    z[!is.finite(z)] <- 0
    z
  }

  clamp0 <- function(x) {
    if (!clamp_component_at_zero) return(x)
    x[!is.finite(x)] <- 0
    pmax(0, x)
  }

  # --- column existence / defaults ---
  if (!id_col %in% names(dt)) stop(sprintf("Missing id_col '%s' in dt.", id_col))
  if (!year_col %in% names(dt)) stop(sprintf("Missing year_col '%s' in dt.", year_col))

  if (!altos_count_col %in% names(dt)) dt[, (altos_count_col) := 0L]
  if (!evict_count_col %in% names(dt)) dt[, (evict_count_col) := 0L]
  if (!evict_ha_count_col %in% names(dt)) dt[, (evict_ha_count_col) := 0L]
  if (!license_units_col %in% names(dt)) dt[, (license_units_col) := NA_real_]

  if (is.null(as_of_year)) {
    as_of_year <- max(dt[[year_col]], na.rm = TRUE)
    if (!is.finite(as_of_year)) as_of_year <- NA_integer_
  }

  # Ensure numeric counts
  dt[, (altos_count_col) := as.numeric(get(altos_count_col))]
  dt[, (evict_count_col) := as.numeric(get(evict_count_col))]
  dt[, (evict_ha_count_col) := as.numeric(get(evict_ha_count_col))]

  # License presence indicator (you can swap this to a log1p(lic_num_units) if desired)
  dt[, intensity_license_y := as.numeric(!is.na(get(license_units_col)) & get(license_units_col) > 0)]

  # Per-year robust-normalized components to account for coverage drift
  dt[, altos_log := log1p(pmax(0, get(altos_count_col)))]
  dt[, evict_log := log1p(pmax(0, get(evict_count_col)))]
  dt[, evict_ha_log := log1p(pmax(0, get(evict_ha_count_col)))]

  dt[, altos_z := robust_z(altos_log), by = year_col]
  dt[, evict_z := robust_z(evict_log), by = year_col]
  dt[, evict_ha_z := robust_z(evict_ha_log), by = year_col]

  dt[, intensity_altos_y := clamp0(altos_z)]
  dt[, intensity_evict_y := clamp0(evict_z)]
  dt[, intensity_evict_ha_y := clamp0(evict_ha_z)]

  # Total intensity (per year)
  wA <- weights$altos
  wE <- weights$evict
  wL <- weights$license
  wEH <- weights$evict_ha

  dt[, intensity_total_y :=
       wA * intensity_altos_y +
       wE * intensity_evict_y +
       wEH * intensity_evict_ha_y +
       wL * intensity_license_y
  ]

  # Recency decay (optional; if as_of_year missing, set to NA)
  if (is.finite(as_of_year)) {
    dt[, intensity_total_y_recent := intensity_total_y * (decay_base ^ pmax(0, as_of_year - get(year_col)))]
  } else {
    dt[, intensity_total_y_recent := NA_real_]
  }

  # Clean up temporary cols (keep if you want diagnostics)
  dt[, c("altos_log", "evict_log", "evict_ha_log", "altos_z", "evict_z", "evict_ha_z") := NULL]

  # Return dt invisibly (modified by reference)
  invisible(dt)
}

#' Collapse PID x year intensities to PID-level summary table
summarize_intensity_to_pid <- function(
    dt_panel,
    id_col = "pid",
    year_col = "year"
) {
  stopifnot(data.table::is.data.table(dt_panel))
  if (!id_col %in% names(dt_panel)) stop(sprintf("Missing id_col '%s' in dt_panel.", id_col))
  if (!year_col %in% names(dt_panel)) stop(sprintf("Missing year_col '%s' in dt_panel.", year_col))

  required <- c("intensity_total_y", "intensity_total_y_recent",
                "intensity_altos_y", "intensity_evict_y", "intensity_license_y")
  missing_req <- setdiff(required, names(dt_panel))
  if (length(missing_req) > 0) {
    stop(sprintf(
      "dt_panel missing required columns: %s. Did you run add_rental_intensity() first?",
      paste(missing_req, collapse = ", ")
    ))
  }

  out <- dt_panel[, .(
    intensity_total_sum    = sum(intensity_total_y, na.rm = TRUE),
    intensity_total_mean   = mean(intensity_total_y, na.rm = TRUE),
    intensity_total_recent = sum(intensity_total_y_recent, na.rm = TRUE),

    intensity_altos_sum    = sum(intensity_altos_y, na.rm = TRUE),
    intensity_evict_sum    = sum(intensity_evict_y, na.rm = TRUE),
    intensity_license_sum  = sum(intensity_license_y, na.rm = TRUE),

    years_any_evidence     = sum(intensity_total_y > 0, na.rm = TRUE),
    years_altos            = sum(intensity_altos_y > 0, na.rm = TRUE),
    years_evict            = sum(intensity_evict_y > 0, na.rm = TRUE),
    years_license          = sum(intensity_license_y > 0, na.rm = TRUE),

    first_year             = suppressWarnings(min(get(year_col), na.rm = TRUE)),
    last_year              = suppressWarnings(max(get(year_col), na.rm = TRUE))
  ), by = id_col]

  out[]
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
  rent_list       = rent_list,
  evict_agg       = evict_agg
)

ever_rentals_pid   <- ever_rental_out$ever_rentals_pid
ever_rentals_panel <- ever_rental_out$ever_rentals_panel

assert_unique(ever_rentals_pid, "PID", "ever_rentals_pid")
logf("  ever_rentals_pid: ", nrow(ever_rentals_pid), " rows, PID unique - PASSED", log_file = log_file)

assert_unique(ever_rentals_panel, c("PID", "year"), "ever_rentals_panel")
logf("  ever_rentals_panel: ", nrow(ever_rentals_panel), " rows, (PID, year) unique - PASSED", log_file = log_file)

# ADD rental intensity scores
library(data.table)

# ever_rentals_panel is PID x year
add_rental_intensity(
  ever_rentals_panel,
  id_col = "PID"
  )

pid_intensity <- summarize_intensity_to_pid(ever_rentals_panel, id_col = "PID")

# Merge onto your parcel list or ever_rentals_pid summary
ever_rentals_pid <- merge(
  ever_rentals_pid,
  pid_intensity,
  by = "PID",
  all.x = TRUE
)




## 5.7 Export
logf("Writing outputs...", log_file = log_file)

out_pid <- p_product(cfg, "ever_rentals_pid")
fwrite(ever_rentals_pid, out_pid)
logf("  Wrote ever_rentals_pid: ", nrow(ever_rentals_pid), " rows to ", out_pid, log_file = log_file)

out_panel <- p_product(cfg, "ever_rentals_panel")
fwrite(ever_rentals_panel, out_panel)
logf("  Wrote ever_rentals_panel: ", nrow(ever_rentals_panel), " rows to ", out_panel, log_file = log_file)


logf("=== Finished make-rent-panel.R ===", log_file = log_file)


# NOTE: Exploratory analysis code moved to analysis/ folder
# See analysis/rent-validation.R for correlation checks between Altos and eviction rents
