## ============================================================
## make-ever-rentals.R
## ============================================================
## Goal:
##  1) Combine Altos, rental licenses, parcels, and evictions
##     into a unified "ever-rental" dataset.
##  2) Impute number of dwelling units per parcel using parcels,
##     licenses, InfoUSA, and building features (units_model-style).
##
## Assumptions:
##  - You already have these data objects loaded OR you will
##    replace the placeholder fread/readRDS calls below:
##      * philly_parcels
##      * philly_building_df (or philly_bldgs)
##      * philly_bg (2010 block groups sf)
##      * philly_lic
##      * philly_altos
##      * philly_evict (with at least n_sn_ss_c, defendant, plaintiff, etc.)
##      * evict_xwalk (address → PID xwalk, with num_parcels_matched)
##      * philly_infousa_dt
##      * info_usa_xwalk (InfoUSA address → parcel xwalk, with num_parcels_matched)
##      * tenure_bg_2010, uis_tr_2010 (if you later want BG constraints)
##
##  - PIDs are ultimately 9-character, left-padded strings.
##
## You’ll likely want to edit:
##  - Data loading block
##  - Column names for building areas, number of stories, etc.
##  - Paths for fwrite() outputs.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(sf)
  library(tidyverse)
  library(fixest)
})

## -------------------------
## 0) WORKING DIRECTORY / HELPERS
## -------------------------
## (Optional) setwd to your project root
setwd("/Users/joefish/Documents/GitHub/philly-evictions")
source("r/helper-functions.R")  # if you rely on Mode(), clean_name(), etc.

normalize_pid <- function(x) {
  stringr::str_pad(as.character(x), width = 9, side = "left", pad = "0")
}

## -------------------------
## 1) DATA LOADING (PLACEHOLDERS)
## -------------------------
## Comment these out if you already have the objects in memory and
## replace with your actual paths as needed.

philly_parcels     <- fread("/Users/joefish/Desktop/data/philly-evict/parcel_address_cleaned.csv")
philly_building_df <- fread('~/Desktop/data/philly-evict/processed/parcel_building_summary_2024.csv')

philly_bg          <- st_read("~/Desktop/data/philly-evict/philly_bg.shp")

philly_lic         <- fread("/Users/joefish/Desktop/data/philly-evict/business_licenses_clean.csv")
philly_altos       <- fread("~/Desktop/data/philly-evict/altos_year_bedrooms_philly.csv")

philly_evict       <- fread("/Users/joefish/Desktop/data/philly-evict/evict_address_cleaned.csv")
evict_xwalk        <- fread("~/Desktop/data/philly-evict/philly_evict_address_agg_xwalk.csv")

philly_infousa_dt  <- fread("~/Desktop/data/philly-evict/infousa_address_cleaned.csv")
info_usa_xwalk     <- fread("~/Desktop/data/philly-evict/philly_infousa_dt_address_agg_xwalk.csv")

tenure_bg_2010     <- fread('/Users/joefish/Desktop/data/philly-evict/census/tenure_bg_2010.csv')
uis_tr_2010        <- fread("~/Desktop/data/philly-evict/census/uis_tr_2010.csv")


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
  parcels_sf <- st_as_sf(
    parcels_first,
    coords = c("geocode_x", "geocode_y"),
    crs    = st_crs(philly_bg)
  )
  parcels_sf <- st_join(parcels_sf, philly_bg)

  parcels_dt <- as.data.table(parcels_sf)[, geometry := NULL]

  # Basic cleaning / recodes for building descriptors if present
  if ("building_code_description_new" %in% names(parcels_dt)) {
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

  lic[, pm.zip := stringr::str_pad(stringr::str_sub(coalesce(pm.zip, zip), 1, 5),
                                   5, "left", "0")]

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
  if ("building_code_description_new" %in% names(parcel_agg_for_model)) {
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
parcel_backbone <- prep_parcel_backbone(philly_parcels, philly_bg)

## 5.2 Altos + licenses
rent_list <- prep_rent_altos_simple(philly_lic, philly_altos)

## 5.3 Evictions
evict_agg <- prep_evictions_simple(philly_evict, evict_xwalk)

## 5.4 Ever-rentals panel
ever_rental_out <- build_ever_rentals_panel(
  parcel_backbone = parcel_backbone,
  rent_list       = rent_list,
  evict_agg       = evict_agg
)

ever_rentals_pid   <- ever_rental_out$ever_rentals_pid
ever_rentals_panel <- ever_rental_out$ever_rentals_panel

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

## 5.6 Merge units onto ever-rentals
ever_rentals_pid <- merge(
  ever_rentals_pid,
  parcel_units,
  by    = "PID",
  all.x = TRUE
)

ever_rentals_panel <- merge(
  ever_rentals_panel,
  parcel_units,
  by    = "PID",
  all.x = TRUE
)

## 5.7 Export (edit paths as needed)
OUTDIR = "~/Desktop/data/philly-evict/processed"
fwrite(ever_rentals_pid,
      paste0( OUTDIR,"/ever_rentals_pid.csv")
       )
fwrite(ever_rentals_panel,
       #"output/ever_rentals_panel.csv"
       paste0(OUTDIR, "/ever_rentals_panel.csv")
       )
fwrite(parcel_units,
       #"output/ever_rental_parcel_units.csv"
       paste0(OUTDIR, "/ever_rental_parcel_units.csv")
       )

message("Done building ever-rentals and unit imputes.")
message("Ever-rental parcels (PID-level): ", nrow(ever_rentals_pid))
message("Ever-rental panel rows (PID-year): ", nrow(ever_rentals_panel))


#
ever_rentals_panel <- fread(paste0(OUTDIR, "/ever_rentals_panel.csv"))

# get correlation between evict / altos rents
cor_test <- cor.test(
  ever_rentals_panel$med_rent_altos,
  ever_rentals_panel$med_eviction_rent,
  use = "pairwise.complete.obs"
)
# ols
ols_model <- feols(
  med_eviction_rent ~ med_rent_altos,
  data = ever_rentals_panel
)
ols_model

# ggplot
ggplot(ever_rentals_panel, aes(x = med_rent_altos, y = med_eviction_rent)) +
  geom_point(alpha = 0.1) +
  geom_smooth( color = "blue") +
  # add abline
  geom_abline(1, intercept = 0, color = "red") +
  labs(
    title = "Correlation between Altos and Eviction Rents",
    x = "Median Rent (Altos)",
    y = "Median Eviction Rent"
  ) +
  theme_minimal()
