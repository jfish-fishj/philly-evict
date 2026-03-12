## ============================================================
## make-rent-panel.R (formerly make-ever-rentals.R)
## ============================================================
## Purpose: Build the ever-rental panel combining Altos, licenses,
##          parcels, and evictions into a unified dataset.
##
## Inputs (from config):
##   - parcels_clean, parcel_building_summary, philly_bg_shp
##   - business_licenses (cleaned), altos_year_building + altos_pid_year_bedbin (from make-altos-aggs.R)
##   - nhpd_clean, nhpd_parcel_xwalk_property
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
source("r/helper-functions.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "make-rent-panel.log")

logf("=== Starting make-rent-panel.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

ALTOS_MIN_COVERAGE <- 0.60  # Diagnostic threshold for level coverage (not a hard gate for std levels)
ALTOS_MIN_OVERLAP  <- 0.60
ALTOS_BED_MIX_INPUT_KEY <- "ahs_table0_bed_mix"

logf(sprintf("Altos composition settings: MIN_COVERAGE=%.2f, MIN_OVERLAP=%.2f",
             ALTOS_MIN_COVERAGE, ALTOS_MIN_OVERLAP), log_file = log_file)

normalize_pid <- function(x) {
  y <- as.character(x)
  y[!nzchar(y) | y %in% c("NA", "NaN")] <- NA_character_
  out <- stringr::str_pad(y, width = 9, side = "left", pad = "0")
  out[is.na(y)] <- NA_character_
  out
}

ModeChar <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (!length(x)) return(NA_character_)
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
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

philly_altos_bldg  <- fread(p_product(cfg, "altos_year_building"))
logf("  altos_year_building: ", nrow(philly_altos_bldg), " rows", log_file = log_file)

philly_altos_bedbin <- fread(p_product(cfg, "altos_pid_year_bedbin"))
logf("  altos_pid_year_bedbin: ", nrow(philly_altos_bedbin), " rows", log_file = log_file)

philly_nhpd <- fread(p_product(cfg, "nhpd_clean"))
logf("  nhpd_clean: ", nrow(philly_nhpd), " rows", log_file = log_file)

nhpd_xwalk <- fread(p_product(cfg, "nhpd_parcel_xwalk_property"))
logf("  nhpd_parcel_xwalk_property: ", nrow(nhpd_xwalk), " rows", log_file = log_file)

philly_evict       <- fread(p_product(cfg, "evictions_clean"))
logf("  evictions_clean: ", nrow(philly_evict), " rows", log_file = log_file)

evict_xwalk_path_phase1 <- tryCatch(p_product(cfg, "xwalk_evictions_to_parcel"), error = function(e) NULL)
evict_xwalk_path_legacy <- p_product(cfg, "evict_address_xwalk")
evict_xwalk_path <- if (!is.null(evict_xwalk_path_phase1) && file.exists(evict_xwalk_path_phase1)) {
  evict_xwalk_path_phase1
} else {
  evict_xwalk_path_legacy
}
evict_xwalk <- fread(evict_xwalk_path)
logf("  evict_xwalk: ", nrow(evict_xwalk), " rows from ", evict_xwalk_path, log_file = log_file)

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
  if (!"owner_1" %in% names(philly_parcels)) {
    stop("parcels_clean is missing required column 'owner_1'")
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
  parcels_dt[, `:=`(
    is_pha_owner = is_pha_owner_name(owner_1),
    is_university_owned = is_university_owner_name(owner_1)
  )]

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

prep_rent_altos_simple <- function(philly_lic,
                                   philly_altos_bldg,
                                   philly_altos_bedbin,
                                   parcel_backbone,
                                   cfg,
                                   log_file = NULL) {
  lic <- copy(philly_lic)
  altos_bldg <- copy(philly_altos_bldg)
  altos_bedbin <- copy(philly_altos_bedbin)
  parcels <- copy(parcel_backbone)
  setDT(lic); setDT(altos_bldg); setDT(altos_bedbin); setDT(parcels)

  log_local <- function(...) logf(..., log_file = log_file)

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

  ## --- Altos panel cleanup (Phase 1: raw + bed-bin standardized metrics) ---
  altos_bldg[, PID := normalize_pid(PID)]
  for (cc in c("altos_any_ownership_unsafe", "altos_any_condo_group_link")) {
    if (!cc %in% names(altos_bldg)) altos_bldg[, (cc) := FALSE]
  }
  for (cc in c("altos_link_type_mode", "altos_xwalk_status_mode")) {
    if (!cc %in% names(altos_bldg)) altos_bldg[, (cc) := NA_character_]
  }
  altos_bldg <- altos_bldg[
    med_price > 500 & med_price < 7500 &
      year %in% 2006:2024 &
      !is.na(PID)
  ]
  assert_unique(altos_bldg, c("PID", "year"), "altos_year_building (filtered)")

  altos_bldg_year <- altos_bldg[
    ,
    .(
      PID,
      year,
      med_rent_altos_raw = as.numeric(med_price),
      n_altos_listing_rows = as.integer(num_listings),
      altos_any_ownership_unsafe = as.logical(fcoalesce(altos_any_ownership_unsafe, FALSE)),
      altos_any_condo_group_link = as.logical(fcoalesce(altos_any_condo_group_link, FALSE)),
      altos_link_type_mode = as.character(altos_link_type_mode),
      altos_xwalk_status_mode = as.character(altos_xwalk_status_mode)
    )
  ]

  altos_bedbin[, PID := normalize_pid(PID)]
  altos_bedbin[, bed_bin := as.character(bed_bin)]
  for (cc in c("altos_any_ownership_unsafe", "altos_any_condo_group_link")) {
    if (!cc %in% names(altos_bedbin)) altos_bedbin[, (cc) := FALSE]
  }
  for (cc in c("altos_link_type_mode", "altos_xwalk_status_mode")) {
    if (!cc %in% names(altos_bedbin)) altos_bedbin[, (cc) := NA_character_]
  }
  altos_bedbin <- altos_bedbin[
    med_rent_cell > 500 & med_rent_cell < 7500 &
      year %in% 2006:2024 &
      !is.na(PID) &
      bed_bin %in% c("studio", "1br", "2br", "3plus")
  ]
  assert_unique(altos_bedbin, c("PID", "year", "bed_bin"), "altos_pid_year_bedbin (filtered)")

  altos_prov_year_bed <- altos_bedbin[
    ,
    .(
      altos_any_ownership_unsafe = any(as.logical(altos_any_ownership_unsafe), na.rm = TRUE),
      altos_any_condo_group_link = any(as.logical(altos_any_condo_group_link), na.rm = TRUE),
      altos_link_type_mode = ModeChar(altos_link_type_mode),
      altos_xwalk_status_mode = ModeChar(altos_xwalk_status_mode)
    ),
    by = .(PID, year)
  ]
  altos_prov_year_bldg <- altos_bldg_year[
    ,
    .(
      altos_any_ownership_unsafe = any(as.logical(altos_any_ownership_unsafe), na.rm = TRUE),
      altos_any_condo_group_link = any(as.logical(altos_any_condo_group_link), na.rm = TRUE),
      altos_link_type_mode = ModeChar(altos_link_type_mode),
      altos_xwalk_status_mode = ModeChar(altos_xwalk_status_mode)
    ),
    by = .(PID, year)
  ]
  altos_prov_year <- rbindlist(list(altos_prov_year_bed, altos_prov_year_bldg), use.names = TRUE, fill = TRUE)
  altos_prov_year <- altos_prov_year[
    ,
    .(
      altos_any_ownership_unsafe = any(as.logical(altos_any_ownership_unsafe), na.rm = TRUE),
      altos_any_condo_group_link = any(as.logical(altos_any_condo_group_link), na.rm = TRUE),
      altos_link_type_mode = ModeChar(altos_link_type_mode),
      altos_xwalk_status_mode = ModeChar(altos_xwalk_status_mode)
    ),
    by = .(PID, year)
  ]
  assert_unique(altos_prov_year, c("PID", "year"), "altos_prov_year")

  mix_lookup <- unique(parcels[, .(PID, building_type)], by = "PID")
  assert_unique(mix_lookup, "PID", "mix_lookup (parcel_backbone)")
  mix_lookup[, mix_class := altos_bed_mix_class(building_type)]

  w_bed <- get_altos_bed_mix_weights(
    cfg = cfg,
    input_key = ALTOS_BED_MIX_INPUT_KEY,
    log_file = log_file
  )
  assert_unique(w_bed, c("mix_class", "bed_bin"), "Altos bed-mix weights")

  altos_cells <- merge(
    altos_bedbin,
    mix_lookup[, .(PID, mix_class, building_type)],
    by = "PID",
    all.x = TRUE
  )
  altos_cells[is.na(mix_class) | !nzchar(mix_class), mix_class := "large_apartment"]

  altos_cells <- merge(
    altos_cells,
    w_bed[, .(mix_class, bed_bin, weight, weight_source)],
    by = c("mix_class", "bed_bin"),
    all.x = TRUE
  )

  if (altos_cells[is.na(weight), .N] > 0) {
    stop("Missing bed-mix weights for some Altos PID-year-bed_bin rows after join")
  }
  altos_cells[, med_rent_cell := as.numeric(med_rent_cell)]
  altos_cells[, n_listing_rows_cell := as.integer(n_listing_rows_cell)]
  altos_cells[, log_med_rent_cell := fifelse(med_rent_cell > 0, log(med_rent_cell), NA_real_)]

  altos_std_year <- altos_cells[
    is.finite(med_rent_cell) & med_rent_cell > 0 & is.finite(weight),
    {
      w_obs <- sum(weight, na.rm = TRUE)
      w_norm <- if (is.finite(w_obs) && w_obs > 0) weight / w_obs else rep(NA_real_, .N)
      list(
        n_altos_cells = uniqueN(bed_bin),
        n_altos_listing_rows_with_bed_bin = sum(n_listing_rows_cell, na.rm = TRUE),
        altos_share_weight_observed = w_obs,
        # Phase 1: compute standardized level whenever at least one valid bed-bin cell
        # is observed. Coverage is tracked diagnostically via altos_share_weight_observed.
        med_rent_altos_std_fixed = if (is.finite(w_obs) && w_obs > 0) {
          sum(w_norm * med_rent_cell, na.rm = TRUE)
        } else {
          NA_real_
        },
        altos_std_weight_norm_sum = if (is.finite(w_obs) && w_obs > 0) sum(w_norm, na.rm = TRUE) else NA_real_
      )
    },
    by = .(PID, year)
  ]

  altos_overlap_cells <- altos_cells[
    is.finite(log_med_rent_cell) & is.finite(weight),
    .(PID, year, bed_bin, weight, log_med_rent_cell)
  ]
  setorder(altos_overlap_cells, PID, bed_bin, year)
  altos_overlap_cells[
    ,
    `:=`(
      year_prev = data.table::shift(year),
      log_med_rent_cell_prev = data.table::shift(log_med_rent_cell)
    ),
    by = .(PID, bed_bin)
  ]

  altos_overlap_year <- altos_overlap_cells[
    year_prev == year - 1L & is.finite(log_med_rent_cell_prev),
    {
      w_ol <- sum(weight, na.rm = TRUE)
      w_norm <- if (is.finite(w_ol) && w_ol > 0) weight / w_ol else rep(NA_real_, .N)
      dlog_vec <- log_med_rent_cell - log_med_rent_cell_prev
      list(
        altos_overlap_weight_prev = w_ol,
        altos_cell_overlap_prev = .N,
        dlog_rent_altos_overlap = if (is.finite(w_ol) && w_ol >= ALTOS_MIN_OVERLAP) {
          sum(w_norm * dlog_vec, na.rm = TRUE)
        } else {
          NA_real_
        },
        altos_overlap_weight_norm_sum = if (is.finite(w_ol) && w_ol > 0) sum(w_norm, na.rm = TRUE) else NA_real_
      )
    },
    by = .(PID, year)
  ]

  rent <- merge(altos_bldg_year, altos_std_year, by = c("PID", "year"), all = TRUE)
  rent <- merge(rent, altos_overlap_year, by = c("PID", "year"), all = TRUE)
  rent <- merge(
    rent,
    unique(altos_cells[, .(PID, mix_class, building_type)], by = "PID"),
    by = "PID",
    all.x = TRUE
  )
  rent <- merge(rent, altos_prov_year, by = c("PID", "year"), all.x = TRUE)

  # Altos provenance columns may appear in both altos_bldg_year and altos_prov_year.
  # Collapse merge suffix pairs back to a single set of columns.
  collapse_altos_bool <- function(dt, col) {
    cx <- paste0(col, ".x")
    cy <- paste0(col, ".y")
    if (!all(c(cx, cy) %in% names(dt))) return(invisible(NULL))
    vx <- as.logical(dt[[cx]])
    vy <- as.logical(dt[[cy]])
    v <- fcoalesce(vy, vx, FALSE)
    v[is.na(vx) & is.na(vy)] <- NA
    dt[, (col) := v]
    dt[, c(cx, cy) := NULL]
    invisible(NULL)
  }
  collapse_altos_chr <- function(dt, col) {
    cx <- paste0(col, ".x")
    cy <- paste0(col, ".y")
    if (!all(c(cx, cy) %in% names(dt))) return(invisible(NULL))
    vx <- as.character(dt[[cx]])
    vy <- as.character(dt[[cy]])
    v <- fcoalesce(vy, vx)
    dt[, (col) := v]
    dt[, c(cx, cy) := NULL]
    invisible(NULL)
  }
  collapse_altos_bool(rent, "altos_any_ownership_unsafe")
  collapse_altos_bool(rent, "altos_any_condo_group_link")
  collapse_altos_chr(rent, "altos_link_type_mode")
  collapse_altos_chr(rent, "altos_xwalk_status_mode")

  rent[is.na(n_altos_listing_rows), n_altos_listing_rows := 0L]
  rent[is.na(n_altos_listing_rows_with_bed_bin), n_altos_listing_rows_with_bed_bin := 0L]
  rent[is.na(n_altos_cells), n_altos_cells := 0L]
  rent[is.na(altos_share_weight_observed), altos_share_weight_observed := 0]
  # If a PID-year has valid bed-bin cells but no surviving building-level raw row
  # (e.g., building-year raw median filtered out), keep a conservative listing-row
  # count from the cell table so count diagnostics remain coherent.
  rent[n_altos_listing_rows == 0L & n_altos_listing_rows_with_bed_bin > 0L,
       n_altos_listing_rows := n_altos_listing_rows_with_bed_bin]
  rent[, n_altos_listing_rows := as.integer(n_altos_listing_rows)]
  rent[, n_altos_listing_rows_with_bed_bin := as.integer(n_altos_listing_rows_with_bed_bin)]
  rent[, n_altos_cells := as.integer(n_altos_cells)]

  rent[
    ,
    share_altos_rows_with_bed_bin := fifelse(
      n_altos_listing_rows > 0,
      pmin(1, pmax(0, n_altos_listing_rows_with_bed_bin / n_altos_listing_rows)),
      NA_real_
    )
  ]

  setorder(rent, PID, year)
  rent[
    ,
    year_gap_altos_prev := year - data.table::shift(year),
    by = PID
  ]
  rent[
    ,
    dlog_rent_altos_raw_yoy := fifelse(
      year_gap_altos_prev == 1 &
        is.finite(med_rent_altos_raw) & med_rent_altos_raw > 0 &
        is.finite(data.table::shift(med_rent_altos_raw)) & data.table::shift(med_rent_altos_raw) > 0,
      log(med_rent_altos_raw) - log(data.table::shift(med_rent_altos_raw)),
      NA_real_
    ),
    by = PID
  ]

  # Backward-compat aliases for downstream scripts (Phase 1 legacy rent contract)
  rent[, med_rent_altos := med_rent_altos_raw]
  rent[, n_altos_listings := n_altos_listing_rows]
  rent[, n_altos_cells_legacy := n_altos_cells]
  rent[, source := "altos"]
  rent[, altos_any_ownership_unsafe := as.logical(fcoalesce(altos_any_ownership_unsafe, FALSE))]
  rent[, altos_any_condo_group_link := as.logical(fcoalesce(altos_any_condo_group_link, FALSE))]

  # Phase 1 assertions / diagnostics
  assert_unique(rent, c("PID", "year"), "Altos PID-year rent panel (Phase 1)")
  stopifnot(rent[, all(is.na(altos_share_weight_observed) | (altos_share_weight_observed >= 0 & altos_share_weight_observed <= 1 + 1e-8))])
  stopifnot(rent[, all(is.na(altos_overlap_weight_prev) | (altos_overlap_weight_prev >= 0 & altos_overlap_weight_prev <= 1 + 1e-8))])
  stopifnot(rent[, all(n_altos_listing_rows >= n_altos_cells)])
  stopifnot(rent[!is.na(med_rent_altos_raw), all(med_rent_altos_raw > 0)])
  stopifnot(rent[!is.na(med_rent_altos_std_fixed), all(med_rent_altos_std_fixed > 0)])

  if (rent[!is.na(med_rent_altos_std_fixed), .N] > 0) {
    stopifnot(rent[!is.na(med_rent_altos_std_fixed), all(abs(altos_std_weight_norm_sum - 1) < 1e-8)])
  }
  if (rent[!is.na(dlog_rent_altos_overlap), .N] > 0) {
    stopifnot(rent[!is.na(dlog_rent_altos_overlap), all(abs(altos_overlap_weight_norm_sum - 1) < 1e-8)])
  }

  fallback_share <- altos_cells[mix_class == "large_apartment" & is.na(building_type), uniqueN(paste(PID, year))]
  total_altos_pid_year <- uniqueN(rent[!is.na(med_rent_altos_raw) | !is.na(med_rent_altos_std_fixed), .(PID, year)])
  if (total_altos_pid_year > 0) {
    log_local("  Altos Phase 1: fallback large-apartment weights used for ~",
              round(100 * fallback_share / total_altos_pid_year, 1), "% of Alto's PID-years (missing building_type)")
  }
  log_local("  Altos Phase 1 coverage: ",
            round(100 * mean(rent$altos_share_weight_observed < ALTOS_MIN_COVERAGE & rent$n_altos_listing_rows > 0, na.rm = TRUE), 1),
            "% of Alto's PID-years below MIN_COVERAGE (diagnostic only; std levels are still computed when >=1 bed-bin cell is observed)")
  log_local("  Altos Phase 1 overlap: ",
            round(100 * mean(rent$altos_overlap_weight_prev < ALTOS_MIN_OVERLAP & !is.na(rent$altos_overlap_weight_prev), na.rm = TRUE), 1),
            "% of Alto's PID-years with overlap below MIN_OVERLAP (among rows with prior-year overlap info)")

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

  use_phase1_xwalk <- all(c("source_address_id", "anchor_pid", "xwalk_status", "link_type", "ownership_unsafe") %in% names(xw))

  if (use_phase1_xwalk && "pm.uid" %in% names(ev)) {
    ev[, source_address_id := as.character(pm.uid)]
    assert_unique(xw, "source_address_id", "xwalk_evictions_to_parcel (source_address_id)")
    xw1 <- xw[
      ,
      .(
        source_address_id = as.character(source_address_id),
        PID = normalize_pid(fcoalesce(anchor_pid, parcel_number)),
        evict_link_type = as.character(link_type),
        evict_xwalk_status = as.character(xwalk_status),
        evict_ownership_unsafe = as.logical(fcoalesce(ownership_unsafe, FALSE)),
        evict_link_id = as.character(link_id),
        evict_anchor_pid = normalize_pid(anchor_pid)
      )
    ]
    ev_m <- merge(ev, xw1, by = "source_address_id", all.x = FALSE, all.y = FALSE)
  } else {
    ## Legacy path: join to unique PID only
    xw1 <- xw[num_parcels_matched == 1, .(PID, n_sn_ss_c)]
    xw1[, PID := normalize_pid(PID)]
    ev_m <- merge(ev, xw1, by = "n_sn_ss_c")
    ev_m[, `:=`(
      evict_link_type = "parcel",
      evict_xwalk_status = "legacy_unique_match",
      evict_ownership_unsafe = FALSE,
      evict_link_id = as.character(PID),
      evict_anchor_pid = as.character(PID)
    )]
  }

  ev_m[, PID := fifelse(is.na(PID) | PID %in% c("NA", ""), NA_character_, normalize_pid(PID))]

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
      month                    = first(month),
      evict_any_ownership_unsafe = any(as.logical(evict_ownership_unsafe), na.rm = TRUE),
      evict_any_condo_group_link = any(evict_link_type == "condo_group", na.rm = TRUE),
      evict_link_type_mode = ModeChar(evict_link_type),
      evict_xwalk_status_mode = ModeChar(evict_xwalk_status)
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
      num_filings_with_ha = fifelse(is.na(num_filings_with_ha), 0, num_filings_with_ha),
      evict_any_ownership_unsafe = fcoalesce(evict_any_ownership_unsafe, FALSE),
      evict_any_condo_group_link = fcoalesce(evict_any_condo_group_link, FALSE)
    )
  ]

  ev_parcel_agg[]
}

prep_nhpd_simple <- function(philly_nhpd, nhpd_xwalk) {
  nhpd <- copy(philly_nhpd)
  xw <- copy(nhpd_xwalk)
  setDT(nhpd); setDT(xw)

  required_nhpd <- c("nhpd_property_id", "PropertyStatus", "TotalUnits")
  missing_nhpd <- setdiff(required_nhpd, names(nhpd))
  if (length(missing_nhpd) > 0L) {
    stop("nhpd_clean is missing required columns: ", paste(missing_nhpd, collapse = ", "))
  }

  required_xw <- c("nhpd_property_id", "PID", "matched_unique")
  missing_xw <- setdiff(required_xw, names(xw))
  if (length(missing_xw) > 0L) {
    stop("nhpd_parcel_xwalk_property is missing required columns: ", paste(missing_xw, collapse = ", "))
  }

  nhpd[, nhpd_property_id := as.character(nhpd_property_id)]
  xw[, nhpd_property_id := as.character(nhpd_property_id)]
  xw[, PID := normalize_pid(PID)]

  xw_use <- xw[matched_unique == TRUE & !is.na(PID)]
  assert_unique(xw_use, "nhpd_property_id", "nhpd unique property xwalk")

  nhpd_m <- merge(
    nhpd,
    xw_use[, .(nhpd_property_id, PID)],
    by = "nhpd_property_id",
    all = FALSE
  )
  if (nrow(nhpd_m) == 0L) {
    stop("No NHPD properties uniquely matched to parcels")
  }

  nhpd_pid <- nhpd_m[
    ,
    .(
      nhpd_property_count = uniqueN(nhpd_property_id),
      nhpd_total_units = sum(as.numeric(TotalUnits), na.rm = TRUE),
      nhpd_any_active = any(PropertyStatus == "Active", na.rm = TRUE),
      nhpd_any_inconclusive = any(PropertyStatus == "Inconclusive", na.rm = TRUE)
    ),
    by = PID
  ]
  assert_unique(nhpd_pid, "PID", "nhpd_pid")

  list(
    nhpd_pid = nhpd_pid[],
    nhpd_matched = nhpd_m[]
  )
}

## ============================================================
## 3) ASSEMBLE EVER-RENTAL PANEL
## ============================================================

build_ever_rentals_panel <- function(rent_list,
                                     evict_agg,
                                     parcel_backbone,
                                     nhpd_pid) {

  rent      <- copy(rent_list$rent)
  lic_long  <- copy(rent_list$lic_long_min)
  lic_units <- copy(rent_list$lic_units)
  ev_pid_year <- copy(evict_agg)
  nhpd_pid_dt <- copy(nhpd_pid)
  parcel_flags <- unique(
    copy(parcel_backbone)[, .(PID, owner_1, is_pha_owner, is_university_owned)],
    by = "PID"
  )

  setDT(rent); setDT(lic_long); setDT(lic_units); setDT(ev_pid_year); setDT(parcel_flags); setDT(nhpd_pid_dt)

  ## Ensure PIDs are padded
  rent[,          PID := normalize_pid(PID)]
  lic_long[,      PID := normalize_pid(PID)]
  lic_units[,     PID := normalize_pid(PID)]
  ev_pid_year[,   PID := normalize_pid(PID)]
  nhpd_pid_dt[,   PID := normalize_pid(PID)]
  parcel_flags[,  PID := normalize_pid(PID)]
  assert_unique(parcel_flags, "PID", "parcel_flags")
  assert_unique(nhpd_pid_dt, "PID", "nhpd_pid_dt")

  ## --- PID-level ever-rental flags ---

  altos_pids  <- unique(rent$PID)
  lic_pids    <- unique(lic_long$PID)
  evict_pids  <- unique(ev_pid_year$PID)
  nhpd_pids   <- unique(nhpd_pid_dt$PID)
  pha_pids    <- unique(parcel_flags[is_pha_owner == TRUE, PID])

  candidate_pid <- unique(c(altos_pids, lic_pids, evict_pids, nhpd_pids, pha_pids))

  pid_summary <- data.table(PID = candidate_pid)

  pid_summary[
    ,
    `:=`(
      ever_rental_altos   = PID %in% altos_pids,
      ever_rental_license = PID %in% lic_pids,
      ever_rental_evict   = PID %in% evict_pids,
      ever_rental_nhpd    = PID %in% nhpd_pids
    )
  ]
  pid_summary <- merge(pid_summary, parcel_flags, by = "PID", all.x = TRUE)
  pid_summary <- merge(pid_summary, nhpd_pid_dt, by = "PID", all.x = TRUE)
  pid_summary[, `:=`(
    is_pha_owner = as.logical(fcoalesce(is_pha_owner, FALSE)),
    is_university_owned = as.logical(fcoalesce(is_university_owned, FALSE)),
    ever_rental_nhpd = as.logical(fcoalesce(ever_rental_nhpd, FALSE)),
    nhpd_property_count = as.integer(fcoalesce(nhpd_property_count, 0L)),
    nhpd_total_units = as.numeric(fcoalesce(nhpd_total_units, 0)),
    nhpd_any_active = as.logical(fcoalesce(nhpd_any_active, FALSE)),
    nhpd_any_inconclusive = as.logical(fcoalesce(nhpd_any_inconclusive, FALSE))
  )]
  pid_summary[, ever_rental_owner := is_pha_owner & !is_university_owned]
  pid_summary[, include_in_rental_universe :=
    (ever_rental_altos | ever_rental_license | ever_rental_evict | ever_rental_nhpd | ever_rental_owner) &
    !is_university_owned
  ]
  pid_summary[, ever_rental_any := include_in_rental_universe]

  ## Attach license unit summaries (mean, median, mode); NA when missing
  pid_summary <- merge(
    pid_summary,
    lic_units[, .(PID, num_units, mean_num_units, mode_num_units)],
    by = "PID",
    all.x = TRUE
  )

  altos_pid_flags <- rent[
    ,
    .(
      ever_rental_altos_ownership_unsafe = any(as.logical(fcoalesce(altos_any_ownership_unsafe, FALSE)), na.rm = TRUE),
      ever_rental_altos_condo_group_link = any(as.logical(fcoalesce(altos_any_condo_group_link, FALSE)), na.rm = TRUE)
    ),
    by = PID
  ]
  evict_pid_flags <- ev_pid_year[
    ,
    .(
      ever_rental_evict_ownership_unsafe = any(as.logical(fcoalesce(evict_any_ownership_unsafe, FALSE)), na.rm = TRUE),
      ever_rental_evict_condo_group_link = any(as.logical(fcoalesce(evict_any_condo_group_link, FALSE)), na.rm = TRUE)
    ),
    by = PID
  ]
  pid_summary <- merge(pid_summary, altos_pid_flags, by = "PID", all.x = TRUE)
  pid_summary <- merge(pid_summary, evict_pid_flags, by = "PID", all.x = TRUE)
  for (cc in c(
    "ever_rental_altos_ownership_unsafe", "ever_rental_altos_condo_group_link",
    "ever_rental_evict_ownership_unsafe", "ever_rental_evict_condo_group_link"
  )) {
    if (cc %in% names(pid_summary)) pid_summary[, (cc) := as.logical(fcoalesce(get(cc), FALSE))]
  }
  pid_summary[, ever_rental_ownership_unsafe_any := as.logical(fcoalesce(ever_rental_altos_ownership_unsafe, FALSE) |
                                                                 fcoalesce(ever_rental_evict_ownership_unsafe, FALSE))]

  ## --- PID × year panel ---
  assert_unique(rent, c("PID", "year"), "rent_list$rent (Altos Phase 1 PID-year panel)")
  rent_year <- copy(rent)

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
      pm.zip                   = first(pm.zip),
      evict_any_ownership_unsafe = any(as.logical(fcoalesce(evict_any_ownership_unsafe, FALSE)), na.rm = TRUE),
      evict_any_condo_group_link = any(as.logical(fcoalesce(evict_any_condo_group_link, FALSE)), na.rm = TRUE),
      evict_link_type_mode = ModeChar(evict_link_type_mode),
      evict_xwalk_status_mode = ModeChar(evict_xwalk_status_mode)
    ),
    by = .(PID, year)
  ]

  all_years <- sort(unique(c(rent_year$year, lic_year$year, ev_year$year)))
  # just years in 2006:2024
  all_years <- all_years[all_years >= 2006 & all_years <= 2024]
  panel_skeleton <- CJ(
    PID  = candidate_pid,
    year = all_years
  )

  ever_panel <- Reduce(
    f = function(d1, d2) merge(d1, d2, by = c("PID", "year"), all.x = TRUE, all.y = FALSE),
    x = list(panel_skeleton, rent_year, lic_year, ev_year)
  )
  ever_panel <- merge(
    ever_panel,
    pid_summary[, .(
      PID,
      owner_1,
      is_pha_owner,
      is_university_owned,
      ever_rental_nhpd,
      nhpd_property_count,
      nhpd_total_units,
      nhpd_any_active,
      nhpd_any_inconclusive,
      ever_rental_owner,
      include_in_rental_universe
    )],
    by = "PID",
    all.x = TRUE
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
      rental_from_altos_raw   = !is.na(med_rent_altos),
      rental_from_license_raw = !is.na(lic_num_units),
      rental_from_evict_raw   = num_filings > 0,
      rental_from_nhpd_raw    = ever_rental_nhpd %in% TRUE
    )
  ]
  ever_panel[, rental_from_owner := is_pha_owner & !is_university_owned]
  ever_panel[, `:=`(
    rental_from_altos   = rental_from_altos_raw & !is_university_owned,
    rental_from_license = rental_from_license_raw & !is_university_owned,
    rental_from_evict   = rental_from_evict_raw & !is_university_owned,
    rental_from_nhpd    = rental_from_nhpd_raw & !is_university_owned
  )]
  for (cc in c("altos_any_ownership_unsafe", "altos_any_condo_group_link", "evict_any_ownership_unsafe", "evict_any_condo_group_link")) {
    if (cc %in% names(ever_panel)) ever_panel[, (cc) := as.logical(fcoalesce(get(cc), FALSE))]
  }
  ever_panel[, rental_ownership_unsafe_any := as.logical(
    fcoalesce(altos_any_ownership_unsafe, FALSE) | fcoalesce(evict_any_ownership_unsafe, FALSE)
  )]
  ever_panel[
    ,
    ever_rental_any_year := (rental_from_altos | rental_from_license | rental_from_evict | rental_from_nhpd | rental_from_owner)
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
rent_list <- prep_rent_altos_simple(
  philly_lic = philly_lic,
  philly_altos_bldg = philly_altos_bldg,
  philly_altos_bedbin = philly_altos_bedbin,
  parcel_backbone = parcel_backbone,
  cfg = cfg,
  log_file = log_file
)
logf("  rent: ", nrow(rent_list$rent), " rows", log_file = log_file)
logf("  lic_units: ", nrow(rent_list$lic_units), " rows", log_file = log_file)
logf("  lic_long: ", nrow(rent_list$lic_long), " rows", log_file = log_file)

## 5.3 Evictions
logf("Preparing evictions...", log_file = log_file)
evict_agg <- prep_evictions_simple(philly_evict, evict_xwalk)
assert_unique(evict_agg, c("PID", "year"), "evict_agg")
logf("  evict_agg: ", nrow(evict_agg), " rows, (PID, year) unique - PASSED", log_file = log_file)

## 5.4 NHPD
logf("Preparing NHPD rental flags...", log_file = log_file)
nhpd_out <- prep_nhpd_simple(philly_nhpd, nhpd_xwalk)
nhpd_pid <- nhpd_out$nhpd_pid
assert_unique(nhpd_pid, "PID", "nhpd_pid")
logf("  nhpd_pid: ", nrow(nhpd_pid), " rows, PID unique - PASSED", log_file = log_file)

## 5.5 Ever-rentals panel
ever_rental_out <- build_ever_rentals_panel(
  rent_list       = rent_list,
  evict_agg       = evict_agg,
  parcel_backbone = parcel_backbone,
  nhpd_pid        = nhpd_pid
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

intensity_cols <- c(
  "intensity_total_sum", "intensity_total_mean", "intensity_total_recent",
  "intensity_altos_sum", "intensity_evict_sum", "intensity_license_sum",
  "years_any_evidence", "years_altos", "years_evict", "years_license"
)
for (cc in intensity_cols) {
  if (cc %in% names(ever_rentals_pid)) {
    ever_rentals_pid[include_in_rental_universe != TRUE & is.finite(get(cc)), (cc) := 0]
  }
}

owner_adjustment_summary <- ever_rentals_pid[, .(
  n_pid = .N,
  units_mode_sum = sum(fcoalesce(as.numeric(mode_num_units), 0), na.rm = TRUE)
), by = .(
  is_pha_owner = as.integer(is_pha_owner %in% TRUE),
  is_university_owned = as.integer(is_university_owned %in% TRUE),
  ever_rental_nhpd = as.integer(ever_rental_nhpd %in% TRUE),
  ever_rental_owner = as.integer(ever_rental_owner %in% TRUE),
  include_in_rental_universe = as.integer(include_in_rental_universe %in% TRUE)
)][order(-n_pid)]
owner_adjustment_out <- p_out(cfg, "qa", "rental_owner_adjustments_summary.csv")
owner_flagged_out <- p_out(cfg, "qa", "rental_owner_adjustments_flagged_pids.csv")
fwrite(owner_adjustment_summary, owner_adjustment_out)
fwrite(
  ever_rentals_pid[
    is_pha_owner == TRUE | is_university_owned == TRUE | ever_rental_nhpd == TRUE,
    .(
      PID, owner_1, is_pha_owner, is_university_owned,
      ever_rental_owner, ever_rental_nhpd, nhpd_property_count, nhpd_total_units,
      ever_rental_altos, ever_rental_license,
      ever_rental_evict, include_in_rental_universe,
      mode_num_units, num_units, mean_num_units
    )
  ],
  owner_flagged_out
)
logf("  Owner-based rental adjustment summary:\n",
     paste(capture.output(print(owner_adjustment_summary)), collapse = "\n"),
     log_file = log_file)
logf("  Wrote owner adjustment QA: ", owner_adjustment_out, log_file = log_file)
logf("  Wrote owner-flagged PID QA: ", owner_flagged_out, log_file = log_file)




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
