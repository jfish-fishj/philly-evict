## =========================
## Libraries & helpers
## =========================
library(data.table)
library(stringr)
library(sf)
library(tidyverse)
library(tidycensus)
library(zipcodeR)   # for zip_code_db
library(ggplot2)
library(spatstat.geom)
library(fixest)
setwd(
  '/Users/joefish/Documents/GitHub/philly-evictions'
)
source("r/helper-functions.R")
# helper: normalize 5-digit ZIP
normalize_zip <- function(z) str_pad(str_sub(z, 1, 5), width = 5, side = "left", pad = "0")

# helper: NA-out empty strings for selected character cols
na_blank <- function(DT, cols) {
  for (v in cols) if (v %in% names(DT)) DT[get(v) == "", (v) := NA_character_]
  invisible(DT)
}

## =========================
## 1) Parcels + Buildings
## =========================
prep_parcel_building <- function(philly_parcels, philly_bg, philly_building_df,occ_df, lic_units) {
  philly_parcels <- copy(philly_parcels)
  philly_building_df <- copy(philly_building_df)

  # Ensure PID
  if (!"PID" %in% names(philly_parcels) && "parcel_number" %in% names(philly_parcels))
    philly_parcels[, PID := parcel_number]

  # One row per PID (first observation)
  parcels_first <- philly_parcels[, .SD[1], by = PID]

  # Spatial join (parcels -> BG)
  stopifnot(all(c("geocode_x","geocode_y") %in% names(parcels_first)))
  parcels_sf <- st_as_sf(parcels_first, coords = c("geocode_x","geocode_y"), crs = st_crs(philly_bg))
  parcels_sf <- st_join(parcels_sf, philly_bg)
  parcels_dt <- as.data.table(parcels_sf)[, geometry := NULL]

  # Add a few derived fields
  if (all(c("owner_1","owner_2") %in% names(parcels_dt)))
    parcels_dt[, owner := paste(owner_1, owner_2)]
  if (all(c("owner","mailing_street") %in% names(parcels_dt)))
    parcels_dt[, owner_mailing := paste(owner, mailing_street)]
  if ("GEOID" %in% names(parcels_dt))
    parcels_dt[, CT_ID_10 := str_sub(GEOID, 1, 11)]

  # Clean text fields commonly blank
  na_cols <- c("quality_grade","building_code_description","building_code_description_new",
               "basements","number_of_bathrooms","separate_utilities","total_area",
               "building_code_new","zoning","state_code","zip_code")
  na_blank(parcels_dt, na_cols)

  # Quality grade fix
  allowed_grades <- c("A","A+","A-","B","B+","B-","C","C+","C-","D","D+","D-","F")
  if ("quality_grade" %in% names(parcels_dt)) {
    parcels_dt[, quality_grade_fixed := fifelse(quality_grade %chin% allowed_grades,
                                                quality_grade, NA_character_)]
  }

  # Building code description bucketing
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

  # Building DF -> harmonize keys
  if ("parcel_number" %in% names(philly_building_df)) setnames(philly_building_df, "parcel_number", "PID")

  # Merge (PID-year where available; parcels-only fields are static)
  # If building_df lacks a year column, we leave it as PID-only
  if ("year" %in% names(philly_building_df)) {
    parcel_bldg <- merge(philly_building_df, parcels_dt, by = "PID", all.x = TRUE)
  } else {
    parcel_bldg <- merge(philly_building_df, parcels_dt, by = "PID", all.x = TRUE)
    parcel_bldg[, year := NA_integer_]
  }

  # Normalize zip as numeric (for later joins to zip-level metrics)
  if ("zip_code" %in% names(parcel_bldg)) {
    parcel_bldg[, zip_code := as.integer(normalize_zip(as.character(zip_code)))]
  }

  # pad PID
  parcel_bldg[, PID := str_pad(PID, 9, "left", "0")]

  # merge lic units
  if (!is.null(lic_units)) {
    lic_units <- copy(lic_units)
    if ("numberofunits" %in% names(lic_units)) {
      setnames(lic_units, "numberofunits", "num_units")
    }
    lic_units <- lic_units[!is.na(PID)]
    parcel_bldg <- merge(parcel_bldg, lic_units, by = "PID", all.x = TRUE)
  }
  occ_df[, PID := str_pad(PID, 9, "left", "0")]
  # use max occupancy to get num units
  occ_df_max = occ_df[,list(num_households = max(num_households, na.rm = T)), by = .(PID)]
  # merge occ df w/ parcel_bldg
  parcel_bldg = merge(
    parcel_bldg,
    occ_df[,.(num_units_imp, PID, year)],
    by = c("PID","year"),
    all.x = T
  )
  print(glue::glue("After merging lic and occ, NAs in num_households:{sum(is.na(parcel_bldg$num_households))}, num_units: {sum(is.na(parcel_bldg$num_units))}"))

  # imput missing number of units
  # impute units for those w/ missing units
  # parcel_bldg[,num_stories_mean := mean(number_stories,na.rm=T), by = building_code_description_new_fixed]
  # parcel_bldg[,num_stories_imp := coalesce(number_stories, num_stories_mean)]
  # units_model <- feols(num_units ~total_area + total_area^2 + total_livable_area + total_livable_area^2
  #                      #  year_built + year_built^2 +
  #                      + num_stories_imp + num_stories_imp^2 + num_households + num_households^2
  #                      |quality_grade_fixed  + building_code_description_new_fixed +
  #                        general_construction + GEOID,
  #                      data = parcel_bldg[], combine.quick = FALSE)
  #
  #
  # parcel_bldg[,num_units_pred := round(predict(units_model, newdata = parcel_bldg))]
  # parcel_bldg[,num_units_imp := fifelse(is.na(num_units), num_units_pred,round(num_units))]
  # parcel_bldg[,num_units_imp := fifelse(num_units_imp <= 0 , 1, num_units_imp)]
  # # where there's a >50 unit gap between reported and predicted, use predicted
  # parcel_bldg[,num_units_imp := fifelse(!is.na(num_units) & (num_units_pred - num_units) > 50,
  #                                       num_units_pred, num_units_imp)]
  # # pad PID
  # parcel_bldg[, PID := str_pad(PID, 9, "left", "0")]

  return(parcel_bldg[])
}

## =========================
## 2) Rent / Altos + Licenses
## =========================
# =========================
# EDIT: Rent / Altos + Licenses (returns lic_long_min too)
# =========================
prep_rent_altos <- function(philly_lic, philly_altos) {
  lic <- copy(philly_lic)
  altos <- copy(philly_altos)

  # clean up rentalcategory
  # first make blanks NA
  lic[,rentalcategory := fifelse(rentalcategory == "", NA_character_, rentalcategory)]
  # now within opa_account_num and owner, get modal category (a bit weird cause it's not weighted by years but whatever)
  lic[, modal_rentalcategory := Mode(rentalcategory,na.rm = T), by = .(opa_account_num, ownercontact1name)]

  lic <- lic[licensetype == "Rental" & modal_rentalcategory %in% c("Residential Dwellings", "Other")]
  lic[, start_year := as.numeric(substr(initialissuedate, 1, 4))]
  lic[, end_year   := as.numeric(substr(expirationdate, 1, 4))]
  lic[, num_years  := end_year - start_year]
  lic[, id := .I]
  lic[, pm.zip := stringr::str_pad(stringr::str_sub(coalesce(pm.zip, zip), 1, 5), 5, "left", "0")]

  lic[, `:=`(geocode_y = lng, geocode_x = lat)]

  lic_long <- as.data.table(lic[num_years > 0])
  lic_long <- lic_long[, .SD[rep(1L, num_years)], by = id]
  lic_long[, year := start_year + seq_len(.N) - 1L, by = id]
  lic_long[, opa_account_num := stringr::str_pad(opa_account_num, 9, "left", "0")]
  lic_long[, PID := opa_account_num]
  lic_long <- lic_long[year %in% 2016:2024 & n_sn_ss_c != ""]

  # Units per PID (median up to 2022)
  lic_long[,numberofunits := as.double(numberofunits)]
  lic_units <- lic_long[year <= 2024,
                        list(num_units = median(numberofunits, na.rm = TRUE),
                             mode_num_units = Mode(numberofunits, na.rm = TRUE),
                             mean_num_units = mean(numberofunits,na.rm=T)),
                        by = .(PID)]

  # Altos rent panel restricted to licensed PIDs
  rent <- as.data.table(altos[PID %in% lic_long$PID])
  # if (!"ymd" %in% names(rent) && all(c("year","month","day") %in% names(rent))) {
  #   rent[, ymd := paste0(year, "-", month, "-", day)]
  # }
  # rent[, ymd_char := paste0(year, "-", month, "-", day)]
  rent <- rent[year %in% 2006:2024 & med_price > 500 & med_price < 7500]

  if ("beds_imp_first" %in% names(rent)) {
    rent[, beds_imp_first_fixed := fifelse(!is.na(beds_imp_first),
                                           as.character(round(beds_imp_first)), "UNKNOWN")]
  }
  if ("baths_first" %in% names(rent)) {
    rent[, baths_first_fixed := fifelse(!is.na(baths_first),
                                        as.character(round(baths_first)), "UNKNOWN")]
  }
  rent[, log_med_price := log(med_price)]

  # Minimal license panel for shares: PID-year units + pm.zip
  lic_long_min <- lic_long[, .(PID, year, numberofunits, pm.zip = as.integer(pm.zip))]

  # pad PID
  rent[, PID := str_pad(PID, 9, "left", "0")]
  lic_units[, PID := str_pad(PID, 9, "left", "0")]
  lic_long_min[, PID := str_pad(PID, 9, "left", "0")]

  # drop NA PID
  rent <- rent[!is.na(PID)]
  lic_units <- lic_units[!is.na(PID)]
  lic_long_min <- lic_long_min[!is.na(PID)] %>% distinct(PID, year,.keep_all = T)
  rent[,source := "altos"]
  list(
    rent = rent[, .(PID, bed_bath_pid_ID, beds_imp_first, beds_imp_first_fixed,#ymd_char,
                    source,year, month,
                    baths_first, baths_first_fixed, #ymd,
                     med_price, num_listings, log_med_price)],
    lic_units = lic_units,
    lic_long_min = lic_long_min
  )
}

##
## =========================
## 2) Impute futures
## =========================
build_models <- function(rent_df, bldg_df){
  drop_cols = intersect(colnames(rent_df), colnames(bldg_df))
  drop_cols = drop_cols[drop_cols != "PID"]
  analytic_df <- merge(rent_df, bldg_df %>% select(-!!drop_cols), by = c("PID"), all.x = TRUE)
  analytic_df = analytic_df[!is.na(num_units_imp) & !is.na(med_price) &
                            !is.na(beds_imp_first) & !is.na(baths_first) &
                            !is.na(year_built) & !is.na(number_stories) &
                            !is.na(building_code_description_new_fixed) &
                            !is.na(quality_grade_fixed) & !is.na(general_construction) &
                            !is.na(GEOID) & !is.na(market_value) &
                            !is.na(total_area)]
  analytic_df[,log_med_price := log(med_price)]
  bed_reg <- fixest::feols(
    beds_imp_first ~ log_med_price*year + log_med_price^2*year + log_med_price^3*year+
      #building_code_description_new_fixed +
      num_units_imp + num_units_imp^2 + num_units_imp^3+
      year_built + year_built^2 +
      number_stories + number_stories^2 +
      log(market_value) +
      building_code_description_new_fixed+
      log(total_area)|
      CT_ID_10+ quality_grade_fixed    ,
    data = analytic_df, combine.quick = FALSE
  )

  bath_reg <-  fixest::feols(
    baths_first ~ log_med_price + log_med_price^2 + log_med_price^3 +
      num_units_imp + num_units_imp^2 + num_units_imp^3 +
      year_built + year_built^2 +
      number_stories + number_stories^2 +
      log(market_value) +
      building_code_description_new_fixed+
      log(total_area)|
      CT_ID_10+quality_grade_fixed+year   ,
    data = analytic_df, combine.quick = FALSE
  )
  return(list(
    bed_reg = bed_reg,
    bath_reg = bath_reg
  ))
}

## =========================
## 3) Evictions
## =========================
prep_evictions <- function(philly_evict, evict_xwalk, pa_zip_acs, models, bldg_df) {
  ev <- copy(philly_evict)
  xw <- copy(evict_xwalk)
  # clean some names
  ev[,clean_defendant_name := clean_name(defendant)]
  # flag commercial
  ev[,commercial_alt := str_detect(clean_defendant_name, business_regex)]

  ev[,dup := .N, by = .(n_sn_ss_c, plaintiff,clean_defendant_name,d_filing)][,dup := dup > 1]
  ev = ev[commercial == "f" &
            commercial_alt == F &
            dup == F &
            year >= 2000 &
            total_rent <= 5e4 &
            !is.na(n_sn_ss_c)]

  # Join to PID (limit to unique parcel matches)

  xw1 <- xw[num_parcels_matched == 1, .(PID, n_sn_ss_c)]
  # pad PID
  xw1[, PID := str_pad(PID, 9, "left", "0")]
  ev_m <- merge(ev, xw1, by = "n_sn_ss_c")

  # Housing authority flag
  ev_m[, housing_auth := str_detect(str_to_upper(plaintiff), "(PHILA.+A?UTH|HOUS.+AUTH|COMMON.+PENN)")]
  ev_m[,log_med_price := log(ongoing_rent)]

  # impute chars
  ev_m = ev_m %>%
    merge(
      bldg_df[, .(PID, year_built, number_stories, building_code_description_new_fixed,
                  quality_grade_fixed, general_construction, GEOID, market_value,num_units_imp,CT_ID_10,
                  total_area, number_of_bathrooms, number_of_bedrooms, type_heater)]  ,
      by = "PID"
    )
  # keep ev_m w/ rents in [300,10000] and year >= 2003
  ev_m = ev_m[ongoing_rent >= 300 & ongoing_rent <= 10000 & year >= 2003]

  ev_m[,beds_imp_first := predict(models$bed_reg, newdata = ev_m)]
  # fix ev_m beds_imp_first
  ev_m[,beds_imp_first := case_when(
    beds_imp_first < 0 ~ 0,
    beds_imp_first <= 6 ~ round(beds_imp_first),
    beds_imp_first > 6 ~ 6,
    TRUE ~ NA_real_
  )]

  ev_m[,baths_first := predict(models$bath_reg, newdata = ev_m)]
  # fix ev_m baths_first
  ev_m[,baths_first := case_when(
    baths_first < 0 ~ 0,
    baths_first <= 4 ~ round(baths_first),
    baths_first > 4 ~ 4,
    TRUE ~ NA_real_
  )]
  # Parcel-year aggregates (non-housing authority only for rates/medians)
  # Keep medians/prices where housing_auth == FALSE
  ev_parcel_agg <- ev_m[commercial_alt == FALSE, .(
    num_filings            = sum(housing_auth == FALSE, na.rm = TRUE),
    num_filings_with_houth_auth = .N,
    med_price              = suppressWarnings(median(ongoing_rent, na.rm = TRUE)),
    pm.zip                 = first(pm.zip),
    month                  = first(month),
    ymd                    = as.character(first(d_filing))
  ), by = .(year, PID)]

  ev_parcel_bed_agg <- ev_m[commercial_alt == FALSE, .(
    num_filings            = sum(housing_auth == FALSE, na.rm = TRUE),
    num_filings_with_houth_auth = .N,
    med_price              = suppressWarnings(median(ongoing_rent[housing_auth == FALSE], na.rm = TRUE)),
    pm.zip                 = first(pm.zip),
    month                  = first(month),
   ymd                    = as.character(first(d_filing))
  ), by = .(year, PID,  beds_imp_first, baths_first)]


  # expand ev_parcel_agg
  grid <- expand_grid(
                      PID = ev_parcel_agg[!is.na(PID), unique(PID)],
                      year =ev_parcel_agg[!is.na(year), unique(year)]
  )
  ev_parcel_agg = merge(
    as.data.table(grid),
    ev_parcel_agg,
    all.x = T,
    by = c("PID", "year")
  )
  ev_parcel_agg[,num_filings := fifelse(is.na(num_filings), 0, num_filings)]
  ev_parcel_agg[,num_filings_with_houth_auth := fifelse(is.na(num_filings_with_houth_auth),
                                                        0
                                                        , num_filings_with_houth_auth)]
  ev_parcel_agg[,pm.zip_first := first(pm.zip[!is.na(pm.zip)]), by = PID]
  ev_parcel_agg[,pm.zip := coalesce(pm.zip,pm.zip_first)]
  ev_parcel_agg[,pm.zip_first := NULL]

  # repeat for beds agg
  grid_bed <- expand_grid(PID = ev_parcel_bed_agg[!is.na(PID), unique(PID)],
                          year = ev_parcel_bed_agg[!is.na(year), unique(year)],
                          beds_imp_first = unique(ev_parcel_bed_agg$beds_imp_first),
                          baths_first = unique(ev_parcel_bed_agg$baths_first)
  )

  ev_parcel_bed_agg = merge(
    as.data.table(grid_bed),
    ev_parcel_bed_agg,
    all.x = T,
    by = c("PID", "year", "beds_imp_first", "baths_first")
  )

  ev_parcel_bed_agg[,num_filings := fifelse(is.na(num_filings), 0, num_filings)]
  ev_parcel_bed_agg[,num_filings_with_houth_auth := fifelse(is.na(num_filings_with_houth_auth),
                                                        0
                                                        , num_filings_with_houth_auth)]
  ev_parcel_bed_agg[,pm.zip_first := first(pm.zip), by = PID]
  ev_parcel_bed_agg[,pm.zip := coalesce(pm.zip,pm.zip_first)]
  ev_parcel_bed_agg[,pm.zip_first := NULL]


  # ZIP-year aggregates + ACS + filter to Philadelphia major city
  ev_zip <- ev_parcel_agg[, .(
    num_filings_zip = sum(num_filings_with_houth_auth, na.rm = TRUE),
    med_price_zip   = suppressWarnings(median(med_price, na.rm = TRUE))
  ), by = .(year, pm.zip)]
  ev_zip[, source := "evict"]

  acs_zip <- as.data.table(pa_zip_acs)[, .(pm.zip = as.character(GEOID),
                                           renter_occupiedE)]
  zmap <- as.data.table(zipcodeR::zip_code_db)[, .(zipcode, major_city)]
  ev_zip_m <- merge(ev_zip, acs_zip, by = "pm.zip", all.x = TRUE)
  ev_zip_m <- merge(ev_zip_m, zmap, by.x = "pm.zip", by.y = "zipcode", all.x = TRUE)
  ev_zip_m <- ev_zip_m[major_city == "Philadelphia" & num_filings_zip > 10]

  ev_zip_m[, filing_rate_zip := num_filings_zip / renter_occupiedE]
  # normalize pm.zip to integer for later joins to parcel zip
  ev_zip_m[, pm.zip := as.integer(pm.zip)]

  # pad PID
  ev_parcel_agg[, PID := str_pad(PID, 9, "left", "0")]

  # add source column
  ev_parcel_agg[, source := "evict"]
  ev_parcel_bed_agg[, source := "evict"]
  ev_zip_m[, source := "evict"]

  ev_parcel_agg[,ymd_char := ymd]
  ev_parcel_agg[, ymd := as.Date(ymd_char)]
  ev_parcel_agg[, month := as.integer(substr(ymd_char, 6, 7))]
  ev_parcel_agg[, day := as.integer(substr(ymd_char, 9, 10))]
  ev_parcel_agg[,num_filings_total := sum(num_filings,na.rm=T), by = PID]
  ev_parcel_agg[,num_filings_total_preCOVID := sum(num_filings[year < 2020],na.rm=T), by = PID]
  ev_parcel_agg[order(year),num_filings_cumsum := cumsum(num_filings), by = PID]

  list(
    ev_pid_year = ev_parcel_agg[, .(PID, year,month, num_filings,
                             num_filings_with_houth_auth,
                             source,
                             num_filings_total,
                             num_filings_total_preCOVID,
                             num_filings_cumsum
                              )],
    ev_pid_bed_year = ev_parcel_bed_agg[, .(PID, year, beds_imp_first, baths_first,
                                   source,
                                   med_price, month )],
    ev_zip_year = ev_zip_m[, .(pm.zip, year, num_filings_zip, med_price_zip,
                               renter_occupiedE, filing_rate_zip)]
  )
}

# =========================
# NEW: Market shares / HHI / CDF plot
# =========================
prep_share_metrics <- function(rent_list, parcel_bldg, final_panel,
                               evict_threshold = 0.15,
                               cdf_path = "figs/market_share_cdf.png") {

  lic_long_min <- copy(rent_list$lic_long_min)
  lic_long_min = lic_long_min[numberofunits > 0 & !PID %in% final_panel$PID ]
  # extend rent data w/ lic long min w/
  lic_long_min_extend <- final_panel[#! PID %in% lic_long_min$PID
                                      ,list(
                                        numberofunits = median(num_units_imp,na.rm=T)
                                      ), by = .(PID, pm.zip) ]

  lic_long_min_extend = cross_join(lic_long_min_extend, final_panel[,.(year = unique(year))])

  lic_long_min <- rbindlist(list(
    lic_long_min,
    lic_long_min_extend
  ),use.names = TRUE, fill = TRUE)

  # drop ones w/ NA units
  lic_long_min <- lic_long_min[!is.na(numberofunits) & numberofunits > 0]

  # owner_mailing (PID-level) from parcel_bldg
  owners <- unique(copy(parcel_bldg)[, .(PID, owner_mailing, zip_code)], by = "PID")

  # filing_rate and (optional) total filings per PID-year from final panel
  rate_cols <- c("PID","year","filing_rate","num_filings_total")
  have_rate <- intersect(rate_cols, names(final_panel))
  rates <- unique(copy(final_panel)[, ..have_rate], by = c("PID","year"))

  # share_df base
  share_df <- merge(lic_long_min, owners, by = "PID", all.x = TRUE)
  share_df <- merge(share_df, rates, by = c("PID","year"), all.x = TRUE)

  # Use license pm.zip if present, else fallback to parcel zip_code
  share_df[, pm.zip := fifelse(!is.na(pm.zip), pm.zip, as.integer(zip_code))]

  # Ensure owner_mailing not NA (replicates your random fill, but stable)
  if (anyNA(share_df$owner_mailing)) {
    n_na <- sum(is.na(share_df$owner_mailing))
    share_df[is.na(owner_mailing), owner_mailing := paste0("UNK_", seq_len(n_na))]
  }

  # Units & bins
  setnames(share_df, "numberofunits", "num_units")
  # bins of 1, 2-5, 6-50, 51+
  share_df[, num_units_bins := cut(num_units,
                                       breaks = c(-Inf, 1, 5, 50, Inf),
                                       labels = c("1", "2-5", "6-50", "51+"),
                                       right = TRUE)]
  # share_df[, num_units_bins := fifelse(num_units == 1, "1",
  #                                      fifelse(num_units <= 5, "2-5",
  #                                              fifelse(num_units <= 50, "6-50",
  #                                                      fifelse(num_units > 50, "51+",
  #                                                              NA_character_))))]

  # Filing rate fill & thresholds
  share_df[, filing_rate := fifelse(is.na(filing_rate), 0, filing_rate)]
  share_df[, high_evict := filing_rate > evict_threshold]

  # Owner and zip totals
  share_df[, num_units_owner := sum(num_units), by = .(year, pm.zip, owner_mailing)]
  share_df[, num_units_zip   := sum(num_units), by = .(year, pm.zip)]
  share_df[, share_units_zip := num_units_owner / num_units_zip]
  share_df[, share_units_zip_naive := num_units / num_units_zip]

  # High-evicting aggregates
  share_df[, num_units_evict        := sum(num_units[high_evict], na.rm = TRUE), by = .(pm.zip, year)]
  share_df[, num_units_evict_owner  := sum(num_units[high_evict], na.rm = TRUE), by = .(year, pm.zip, owner_mailing)]
  share_df[, share_units_evict      := fifelse(high_evict, num_units_evict_owner / num_units_evict, 0)]
  share_df[, share_units_evict_naive      := fifelse(high_evict, num_units_evict / num_units_evict, 0)]

  # By unit bin
  share_df[, num_units_evict_unit_bins  := sum(num_units[high_evict], na.rm = TRUE), by = .(year, pm.zip, num_units_bins)]
  share_df[, num_units_evict_owner_bins := sum(num_units[high_evict], na.rm = TRUE), by = .(year, pm.zip, owner_mailing, num_units_bins)]
  share_df[, share_units_evict_bins     := fifelse(high_evict, num_units_evict_owner_bins / num_units_evict_unit_bins, 0)]
  share_df[, share_units_evict_bins_naive     := fifelse(high_evict, num_units / num_units_evict_unit_bins, 0)]

  share_df[, num_units_unit_bins  := sum(num_units), by = .(year, pm.zip, num_units_bins)]
  share_df[, num_units_owner_bins := sum(num_units), by = .(year, pm.zip, owner_mailing, num_units_bins)]
  share_df[, share_units_bins     := num_units_owner_bins / num_units_unit_bins]
  share_df[, share_units_bins_naive     := num_units / num_units_unit_bins]

  # HHIs (zip market; and zip×bin markets)
  hhi_df <- unique(share_df, by = c("year","pm.zip","owner_mailing"))
  hhi_df[, hhi      := sum((100 * share_units_zip)^2),   by = .(year, pm.zip)]
  hhi_df[, hhi_evict:= sum((100 * share_units_evict)^2), by = .(year, pm.zip)]

  hhi_df_units <- unique(share_df, by = c("year","pm.zip","owner_mailing","num_units_bins"))
  hhi_df_units[, hhi      := sum((100 * share_units_bins)^2),       by = .(year, pm.zip, num_units_bins)]
  hhi_df_units[, hhi_evict:= sum((100 * share_units_evict_bins)^2), by = .(year, pm.zip, num_units_bins)]

  # Merge HHIs back for convenience
  share_df <- merge(
    share_df[, !grep("^hhi", names(share_df), value = TRUE), with = FALSE],
    unique(hhi_df, by = c("year","pm.zip"))[, .(year, pm.zip, hhi, hhi_evict)],
    by = c("year","pm.zip")
  )
  share_df <- merge(
    share_df,
    unique(hhi_df_units, by = c("year","pm.zip","owner_mailing"))[, .(year, pm.zip, owner_mailing, hhi, hhi_evict)],
    by = c("year","pm.zip","owner_mailing"),
    suffixes = c("", "_unit_bins")
  )


  # PID-level aggregation for merge with analytic_df
  share_df_agg <- share_df[, .(
    share_units_zip       = mean(share_units_zip,       na.rm = TRUE),
    share_units_evict     = mean(share_units_evict,     na.rm = TRUE),
    share_units_bins      = mean(share_units_bins,      na.rm = TRUE),
    share_units_evict_bins= mean(share_units_evict_bins,na.rm = TRUE),
    # add naive columns
    share_units_zip_naive       = mean(share_units_zip_naive,       na.rm = TRUE),
    share_units_evict_naive     = mean(share_units_evict_naive,     na.rm = TRUE),
    share_units_bins_naive      = mean(share_units_bins_naive,      na.rm = TRUE),
    share_units_evict_bins_naive= mean(share_units_evict_bins_naive,na.rm = TRUE),
    num_units_zip         = mean(num_units_zip,         na.rm = TRUE),
    num_units_evict       = mean(num_units_evict,       na.rm = TRUE),
    num_units_bins         = first(num_units_bins),
    hhi                   = mean(hhi,                   na.rm = TRUE),
    hhi_evict             = mean(hhi_evict,             na.rm = TRUE),
    hhi_evict_unit_bins   = mean(hhi_evict_unit_bins,   na.rm = TRUE),
    hhi_unit_bins         = mean(hhi_unit_bins,         na.rm = TRUE)
  ), by = .(PID = PID)]



  list(
    share_df = share_df,
    share_df_agg = share_df_agg,
    cdf_path = cdf_path
  )
}

# occupancy rate adjust shares
adjust_share_metrics <- function(occ_df,parcel_data, apartment_vac = 0.05, rental_vac = 0.09){
  # we take the year X PID unit vars and adjust them for the occupancy rates implied by
  # infoUSA data
  # im going to assume that average occupancy rates for apartments is 95%
  # so im going to normalize the occupancy rates in the infoUSA to be accordant
  # with a 95% occupancy rate, and then do changes based on that normalized number
  # merge parcel data onto occ_df
  occ_df[,PID := as.numeric(PID)]
  parcel_data[,PID := as.numeric(PID)]
  # occ_df = merge(occ_df, parcel_data[,.(PID, year_built, num_units_imp,
  #                                       building_code_description_new_fixed,
  #                                       total_area,
  #                                       total_livable_area,
  #                                       pm.zip,
  #                                       quality_grade_fixed)],
  #                by = "PID", all.x = TRUE)
  # reg occupancy rates on chars
  occ_df[, num_units_bins := cut(
    num_units_imp,
    breaks = c(-Inf, 1, 5, 50, Inf),
    labels = c("1", "2-5", "6-50", "51+"),
    right = TRUE
  )]
  occ_df[,year_blt_decade := floor(year_built / 10) * 10]
  #occ_df[,quality_grade_fixed_coarse := str_remove_all(quality_grade_fixed, "\\+|-")]
  # homogeneous by year but whatever
  occ_df[,occupancy_rate_trimmed := pmin(pmax(occupancy_rate,0.25),1)]
  occ_df_grid <- (occ_df[,
                        weighted.mean(occupancy_rate_trimmed, w= num_units_imp, na.rm=TRUE),
                        by = .(num_units_bins)][
                          order(num_units_bins)])
  setnames(occ_df_grid, "V1", "mean_occ_rate")
  #
  # adj the occ_df_grid so that it has a mean rental vac rate = input
  non_apartment_vac = (rental_vac * (occ_df[,sum(num_units_imp,na.rm=T)]) -  (occ_df[num_units_imp >= 6,sum(num_units_imp,na.rm=T)] * apartment_vac)) /
    occ_df[num_units_imp < 6,sum(num_units_imp,na.rm=T) ]
  # merge back on
  occ_df = merge(occ_df, occ_df_grid, by = c("num_units_bins"), all.x = TRUE)
  occ_df[num_units_imp ==1, mean_occ_rate := (1 - non_apartment_vac)]
  # normalize to 95% for apartments, 91% for rentals overall and whatever else is implied by those two
  occ_df[,adj_occ_rate := occupancy_rate_trimmed ]
  # occ_df[, adj_occ_rate := fifelse(num_units_bins %in% c("1","2-5"), occupancy_rate_trimmed * (1 - non_apartment_vac) / mean_occ_rate,
  #                                 fifelse(num_units_bins %in% c("6-50","51+","51-100","101+"),
  #                                         occupancy_rate_trimmed * (1 - apartment_vac) / mean_occ_rate,
  #                                         NA_real_))]
  # now rescale adj_occ_rate to equal the population averages again
  # the issue is occupancy rates cant go > 1, so there's a certain amount of fudge going on here...
  # occ_df[, adj_occ_rate := case_when(
  #   num_units_bins %in% c("1","2-5") ~ adj_occ_rate / (1 - non_apartment_vac)  ,
  #   num_units_bins %in% c("6-50","51+", "51-100","100+") ~ adj_occ_rate / (1 - apartment_vac)  ,
  #   TRUE ~ NA_real_
  # )]
  occ_df[num_units_imp ==1, adj_occ_rate := (1 - non_apartment_vac)]
  # retrim the adj occ rate
  occ_df[, adj_occ_rate := pmin(pmax(adj_occ_rate,0.25),1)]
  occ_df[,adj_num_units_imp := round(num_units_imp * adj_occ_rate)]
  occ_df[adj_num_units_imp == 0, adj_num_units_imp := 1]
  # check num units, adj_num_units_imp
  occ_df[, list(
    num_units_imp = sum(num_units_imp, na.rm = TRUE),
    adj_num_units_imp = sum(adj_num_units_imp, na.rm = TRUE),
    mean_occ_rate = weighted.mean(mean_occ_rate,w = num_units_imp, na.rm = TRUE),
    adj_occ_rate = weighted.mean(adj_occ_rate, w = num_units_imp ,na.rm = TRUE)
  ), by = num_units_bins][,implied_vac_rate := 1 - adj_num_units_imp / num_units_imp][] %>%
    print()

  occ_df[, list(
    num_units_imp = sum(num_units_imp, na.rm = TRUE),
    adj_num_units_imp = sum(adj_num_units_imp, na.rm = TRUE),
    mean_occ_rate = weighted.mean(mean_occ_rate,w = num_units_imp, na.rm = TRUE),
    adj_occ_rate = weighted.mean(adj_occ_rate, w = num_units_imp ,na.rm = TRUE)
  ), by = num_units_imp >5 ][,implied_vac_rate := 1 - adj_num_units_imp / num_units_imp][]

  occ_df[, list(
    num_units_imp = sum(num_units_imp, na.rm = TRUE),
    adj_num_units_imp = sum(adj_num_units_imp, na.rm = TRUE),
    mean_occ_rate = weighted.mean(mean_occ_rate,w = num_units_imp, na.rm = TRUE),
    adj_occ_rate = weighted.mean(adj_occ_rate, w = adj_num_units_imp ,na.rm = TRUE)
  ) ][,implied_vac_rate := 1 - adj_num_units_imp / num_units_imp][]

  # make some share aggs by zip-year, zip-unit-year zip-high-filing-year, zip-unit-high-filing-year
  # dont do by owner
#  occ_df[,high_evict := filing_rate > 0.1]
  occ_df[,num_units_zip := sum(adj_num_units_imp, na.rm = TRUE), by = .(year, pm.zip_parcels)]
  occ_df[,share_units_zip := adj_num_units_imp / num_units_zip]
 # occ_df[,num_units_zip_evict := sum(adj_num_units_imp[high_evict], na.rm = TRUE), by = .(year, pm.zip)]
 # occ_df[,share_units_zip_evict := fifelse(high_evict, adj_num_units_imp / num_units_zip_evict, 0)]
  occ_df[,num_occupied_units_zip_unit := sum(adj_num_units_imp,na.rm=T), by = .(year, pm.zip_parcels, num_units_bins)]
  occ_df[,total_num_units_zip_unit := sum(num_units_imp,na.rm=T), by = .(year, pm.zip_parcels, num_units_bins)]
  occ_df[,share_units_zip_unit := adj_num_units_imp / num_occupied_units_zip_unit]
  occ_df[,mean_occ_rate_zip := weighted.mean(adj_occ_rate, w = adj_num_units_imp, na.rm = TRUE), by = .(year, pm.zip_parcels)]
  occ_df[,mean_occ_rate_zip_units := weighted.mean(adj_occ_rate, w = adj_num_units_imp, na.rm = TRUE), by = .(year, pm.zip_parcels, num_units_bins)]
  occ_df[,change_adj_occ_rate := adj_occ_rate - lag(adj_occ_rate), by = PID]
  occ_df[,change_share_units_zip := share_units_zip - lag(share_units_zip), by = PID]

  #occ_df[,num_units_zip_unit_evict := sum(adj_num_units_imp[high_evict], na.rm = TRUE), by = .(year, pm.zip, num_units_bins)]
 # occ_df[,share_units_zip_unit_evict := fifelse(high_evict, adj_num_units_imp / num_units_zip_unit_evict, 0)]
  share_df = occ_df %>%
    select(PID,
           year,
           share_units_zip,
           adj_num_units_imp,
           adj_occ_rate,
           occupancy_rate,
        #   share_units_zip_evict,
           share_units_zip_unit,
       #    share_units_zip_unit_evict,
           num_units_zip,
         #  num_units_zip_evict,
       num_occupied_units_zip_unit,
       change_adj_occ_rate,
       change_share_units_zip,
         mean_occ_rate_zip,
       mean_occ_rate_zip_units
        #   num_units_zip_unit_evict
    )
  occ_df%>%
  return()
}

## =========================
## 4) Final Merge Orchestrator
## =========================
assemble_panel <- function(parcel_bldg,
                           rent_list,
                           evict_list) {

  # unpack pieces
  altos <- copy(rent_list$rent)
  ev_pid_year <- copy(evict_list$ev_pid_year)
  ev_pid_bed_year <- copy(evict_list$ev_pid_bed_year)
  ev_zip_year <- copy(evict_list$ev_zip_year)

  # start by making rent data
  rent <- rbindlist(list(
    altos[,.(PID, beds_imp_first, month, source,
            baths_first, #ymd=ymd_char,
            year, med_price)],
    ev_pid_bed_year[!is.na(med_price),.(PID, med_price,month, #ymd,
                                        year,  source = "evict",
                   baths_first, beds_imp_first
                   )]
  ), use.names = T, fill = TRUE)

  #rent[,month := as.integer(str_replace(substr(ymd, 6, 7),"-",""))]
  #rent[,day := as.integer(str_replace(str_sub(ymd, -2,-1), "-",""))]
  # get list of parcels that exist in either rent or evict data
  valid_parcels = parcel_bldg[PID %in% rent[,unique(PID)] & !is.na(PID), unique(PID)]

  # Start with rent (PID-year) then enrich
  out <- merge(rent, parcel_bldg[PID %in% valid_parcels], by = c("PID","year"), all.x = TRUE,all.y = T)

  # PID-year eviction counts
  out <- merge(out, ev_pid_year[,.(year,PID,num_filings,
                                   num_filings_with_houth_auth,
                                   num_filings_cumsum,
                                   num_filings_total_preCOVID,
                                   num_filings_total )],
               by = c("PID","year"), all.x = TRUE)


  # ZIP-year eviction rates (parcel zip_code already normalized to integer in parcel_bldg)
  if ("zip_code" %in% names(out)) {
    out <- merge(out,
                 ev_zip_year,
                 by.x = c("zip_code","year"),
                 by.y = c("pm.zip","year"),
                 suffixes = c("", "_zip_evict"),
                 all.x = TRUE)
  }

  # Fill zeros and compute filing_rate (PID-level)
  if (!"num_filings" %in% names(out)) out[, num_filings := NA_integer_]
  if (!"num_filings_with_houth_auth" %in% names(out)) out[, num_filings_with_houth_auth := NA_integer_]

  # filter for >= 2006
  out = out[year >= 2006]

  out[is.na(num_filings), num_filings := 0L]
  out[is.na(num_filings_with_houth_auth), num_filings_with_houth_auth := 0L]
  out[is.na(num_filings_total), num_filings_total := 0L]
  out[is.na(num_filings_total_preCOVID), num_filings_total_preCOVID := 0L]
  out[is.na(num_filings_cumsum), num_filings_cumsum := 0L]
  # PID-level totals and rates
  out[, years_per_pid := fifelse(year_built <= 2003,2024 - 2003, 2024 - year_built )]
  out[,years_per_pid_rel06 := years_per_pid - (year - 2006) ]
  out[, filing_rate := fifelse(!is.na(num_units_imp) & num_units_imp > 0 & years_per_pid > 0,
                               num_filings_total / num_units_imp / years_per_pid, NA_real_)]
  out[, filing_rate_preCOVID := fifelse(!is.na(num_units_imp) & num_units_imp > 0 & years_per_pid > 0 & year_built<= 2019,
                                        num_filings_total_preCOVID / num_units_imp / 17, NA_real_)] # 2003-2019

  out[, filing_rate_cumulative := fifelse(!is.na(num_units_imp) & num_units_imp > 0 & years_per_pid_rel06 > 0,
                               num_filings_cumsum / num_units_imp / years_per_pid_rel06, NA_real_)]
  out[is.na(source),source := "unknown"]

  # drop unknowns
  out <- out[source != "unknown"]

  # flag housing auth parcels
  out[, housing_auth_parcel := str_detect(str_to_upper(owner_1), "PHILA.+A?UTH|HOUS.+AUTH")]
  # flag redevelopment parcels
  out[, redevelopment_parcel := str_detect(str_to_upper(owner_1), "(REDEVELOPMENT|REDEV|REDEVLOPMENT).+AUTH")]

}

# =========================
# NEW: Analytic dataset + models
# =========================
build_analytic_df <- function(final_panel, share_df_agg) {

  dt <- (final_panel)
  dt[,log_med_rent := log(med_price)]
  # Filter as in your analytic_df block (add a safe guard for log() domain)
  analytic_df <- dt[
    !is.na(year_built) &
      !is.na(total_area) &
      !is.na(num_units_imp) &
      !is.na(log_med_rent) &
      !is.na(num_filings) &
      !is.na(quality_grade_fixed) &
      !is.na(zoning) &
      !is.na(building_code_description_new_fixed) &
     # total_area < 1e6 &
      !is.na(GEOID) &
      med_price > 300 &
      med_price <= 10000 &
      num_units_imp > 0 &
      !is.na(number_stories) &
      total_area > 100
  ]
  print(glue::glue("reduced rows from {nrow(dt)} to {nrow(analytic_df)}"))

  # Derived features
  #analytic_df[, ymd_char := as.character(ymd)]
  #analytic_df[, month := lubridate::month(ymd_char)]
  analytic_df[, filing_rate_sq   := filing_rate^2]
  analytic_df[, filing_rate_cube := filing_rate^3]
  analytic_df[, num_filings_total_sq := num_filings_total^2]

  # Merge share_df_agg
  # drop_cols <- grep("share_units|^num_units_.*|^hhi", names(analytic_df), value = TRUE)
  # if (length(drop_cols)) analytic_df[, (drop_cols) := NULL]
  analytic_df[,PID := as.numeric(PID)]
  share_df_agg[,PID := as.numeric(PID)]
  analytic_df <- merge(analytic_df, adj_shares, by = c("PID","year"), all.x = TRUE, suffixes = c("", "_share"))
  #analytic_df[,multi_source := uniqueN(source) > 1L, by = .(PID)]

  # More derived fields
  # analytic_df[, share_units_evict_sq := share_units_evict^2]
  # analytic_df[, share_units_zip_sq   := share_units_zip^2]

  analytic_df[, num_rentals := uniqueN(PID), by = .(GEOID, year)]
  analytic_df[, filing_rate_g25 := fifelse(filing_rate > 0.25, 1L, 0L)]
  analytic_df[, filing_rate_g50 := fifelse(filing_rate > 0.50, 1L, 0L)]
  analytic_df[, filing_rate_zero := fifelse(filing_rate == 0, 1L, 0L)]

  analytic_df
}

plot_market_share_cdf <- function(share_df,
                                  cdf_path = "figs/market_share_cdf.png",
                                  width = 10, height = 6, dpi = 300) {
  stopifnot(all(c("num_units",
                  "share_units_zip",
                  "share_units_evict",
                  "share_units_bins",
                  "share_units_evict_bins") %in% names(share_df)))

  if (!requireNamespace("data.table", quietly = TRUE)) stop("Please install data.table")
  if (!requireNamespace("ggplot2", quietly = TRUE))    stop("Please install ggplot2")

  library(data.table)
  library(ggplot2)

  # filter share_df so that each zip code is in each series
  # (this is to ensure that the CDFs are comparable across series)
  # valid_zips <- share_df[,any(
  #   !is.na(share_units_zip) &
  #   (share_units_evict > 0 & is.finite(share_units_evict)) &
  #   !is.na(share_units_bins) &
  #   (share_units_evict_bins > 0 & is.finite(share_units_evict_bins))
  # ), by = .(year, pm.zip)][V1 == TRUE, unique(pm.zip)]
  valid_zips <- unique(share_df$pm.zip)

  DT <- as.data.table(share_df[pm.zip %in% valid_zips])



  # ---- helper: compute weighted ECDF as a data.table (x vs F) ----
  w_ecdf_dt <- function(x, w) {
    ok <- is.finite(x) & is.finite(w) & w > 0
    if (!any(ok)) return(data.table(x = numeric(0), F = numeric(0)))
    x <- x[ok]; w <- w[ok]

    ord <- order(x)
    x <- x[ord]; w <- w[ord]

    # aggregate duplicate x to keep the step plot clean
    tmp <- data.table(x = x, w = w)[, .(w = sum(w)), by = x]
    tmp[, F := cumsum(w) / sum(w)]
    tmp[]
  }

  # ---- build series (with their filters & legend labels) ----
  specs <- list(
    list(col = "share_units_zip",
         filter = quote(!is.na(share_units_zip) & !is.na(share_units_bins)),
         label = "Zip Code"),
    list(col = "share_units_evict",
         filter = quote(share_units_evict > 0 & is.finite(share_units_evict)),
         label = "Zip Code X High-Evicting Units"),
    list(col = "share_units_bins",
         filter = quote(!is.na(share_units_bins) & !is.na(share_units_zip)),
         label = "Zip Code X Property Size"),
    list(col = "share_units_evict_bins",
         filter = quote(share_units_evict_bins > 0 & is.finite(share_units_evict_bins)),
         label = "Zip Code X High-Evicting Units X Property Size")
  )

  # ---- compute weighted ECDFs for each series ----
  ecdfs <- lapply(specs, function(sp) {
    subDT <- DT[eval(sp$filter)]
    if (nrow(subDT) == 0L) return(NULL)
    out <- w_ecdf_dt(subDT[[sp$col]], subDT[["num_units"]])
    if (nrow(out) == 0L) return(NULL)
    out[, series := sp$label]
    out
  })
  ecdf_data <- rbindlist(Filter(Negate(is.null), ecdfs), use.names = TRUE, fill = TRUE)

  if (nrow(ecdf_data) == 0L) stop("No data available to plot ECDFs after filtering.")

  # ---- make sure output directory exists ----
  dir.create(dirname(cdf_path), recursive = TRUE, showWarnings = FALSE)

  # ---- plot (step CDFs) ----
  gg <- ggplot(ecdf_data, aes(x = x, y = F, color = series)) +
    geom_step() +
    labs(
      x = "Share of Units",
      y = "Cumulative Density",
      title = "Cumulative Distribution of Market Shares",
      subtitle = "Weighted by Number of Units",
      color = "Market Definition"
    ) +
    scale_x_continuous(breaks = seq(0, 1, 0.1)) +
    scale_y_continuous(breaks = seq(0, 1, 0.1), limits = c(0, 1)) +
    theme_minimal()

  ggsave(gg, file = cdf_path, width = width, height = height, dpi = dpi, bg = "white")

  invisible(list(gg = gg, ecdf_data = ecdf_data, path = cdf_path))
}

# make bldg panel
make_bldg_panel <- function(analytic_df){
  unit_vars <- c( "beds_imp_first", "baths_first","med_price", "log_med_rent")
  # hedonically adjust rents by number of beds
  analytic_df[,num_beds_round := round(beds_imp_first) %>% fifelse(. < 0,0,.)]
  analytic_df[,num_beds_round := fifelse(num_beds_round > 5, 5, num_beds_round)]
  bed_reg <- feols(log(med_price) ~ i(num_beds_round, ref = 1), data = analytic_df[source == "altos"])
  bed_reg_coef_df = as.data.table(coef(bed_reg), keep.rownames = TRUE) %>%
    mutate(num_beds = as.numeric(str_extract(V1, "[\\d\\.]+")) ) %>%
    filter(!is.na(num_beds))
  # deflate rents to 1-bed equivalent
  analytic_df <- merge(analytic_df %>% select(-matches("V2")),
                       bed_reg_coef_df[,.(num_beds, V2)],
                       by.x = "num_beds_round",
                       by.y = "num_beds",
                       all.x = TRUE,
                       suffixes = c("","_bed_adj"))
  analytic_df[,med_price_adj := fifelse(num_beds_round==1,med_price, med_price / exp(V2))]

  rent_panel <- analytic_df[,list(
    med_price = median(med_price_adj, na.rm = TRUE),
    med_price_unadj = median(med_price, na.rm = TRUE),
    num_diff_beds = uniqueN(beds_imp_first),
    med_beds = median(beds_imp_first, na.rm = TRUE),
    min_beds = min(beds_imp_first, na.rm = TRUE),
    max_beds = max(beds_imp_first, na.rm = TRUE)
  ), by = .(PID, year)]

  bldg_panel <- analytic_df %>% select(-c(unit_vars )) %>%
    distinct(PID, year, .keep_all = T)

  bldg_panel <- merge(bldg_panel,rent_panel, by = c("PID", "year"), all.x = TRUE)
  bldg_panel <- bldg_panel[!is.na(PID) & !is.na(year)]
  bldg_panel[,log_med_rent := log(med_price)]
  bldg_panel[,log_med_rent_unadj := log(med_price_unadj)]
  return(bldg_panel)
}



#' Make BLP-style instruments (data.table) — patched for by= assignment
#'
#' @param DT            data.table with product-level rows.
#' @param market        Column name (string) for market IDs.
#' @param product_id    Optional product ID column (string). Only used for naming if desired.
#' @param firm          Optional firm/owner column (string). If given, creates same-firm vs other-firm instruments.
#' @param nest          Optional nest column (string).  If given, creates same-nest vs other-nest instruments.
#' @param cont_vars     Character vector of continuous variable names to instrument (exogenous x's).
#' @param cat_vars      Character vector of categorical variable names.
#' @param add_market_means Logical; also add market means for cont_vars (Hausman-style).
#' @param moments       Integer vector of powers (>=2) for higher-moment sums on cont_vars, e.g., c(2,3).
#' @param add_counts    Logical; if TRUE, add rival counts per market / firm / nest.
#' @param interaction_pairs Optional list of 2-length character vectors of existing column names to multiply.
#' @param prefix        Prefix for instrument columns (default "z").
#'
#' @return Invisibly returns the character vector of new column names; DT is modified in place.
make_blp_instruments <- function(
    DT,
    market,
    product_id = NULL,
    firm = NULL,
    nest = NULL,
    cont_vars = character(),
    cat_vars = character(),
    add_market_means = TRUE,
    moments = integer(),
    add_counts = TRUE,
    interaction_pairs = NULL,
    prefix = "z"
) {
  stopifnot(data.table::is.data.table(DT))
  stopifnot(is.character(market), length(market) == 1)

  # Validate columns exist
  cols_needed <- c(market, product_id, firm, nest, cont_vars, cat_vars)
  cols_needed <- cols_needed[nzchar(cols_needed)]
  miss <- setdiff(cols_needed, names(DT))
  if (length(miss)) stop("Missing columns in DT: ", paste(miss, collapse = ", "))

  # Shorthands
  mkt <- market; frm <- firm; nst <- nest
  pfx <- function(...) paste(c(prefix, ...), collapse = "_")

  # data.table helpers w/ fallback (avoid NA minus NA propagation)
  has_fcoalesce <- "fcoalesce" %in% getNamespaceExports("data.table")
  fco <- if (has_fcoalesce) data.table::fcoalesce else function(x, y) { x[is.na(x)] <- y[is.na(x)]; x }

  new_cols <- character()

  # ------------------------
  # Continuous variables
  # ------------------------
  if (length(cont_vars)) {
    for (v in cont_vars) {
      # ∑ rivals' v within market: sum(x) - own x
      nm_all <- pfx("sum_others", v)
      DT[, (nm_all) := {
        x <- fco(get(v), 0)
        sum(x) - x
      }, by = .(get(mkt))]
      new_cols <- c(new_cols, nm_all)

      # Same-firm / Other-firm splits
      if (!is.null(frm)) {
        nm_sf <- pfx("sum_samefirm", v)
        nm_of <- pfx("sum_otherfirm", v)
        DT[, (nm_sf) := {
          x <- fco(get(v), 0)
          sum(x) - x
        }, by = .(get(mkt), get(frm))]
        DT[, (nm_of) := get(nm_all) - get(nm_sf)]
        new_cols <- c(new_cols, nm_sf, nm_of)
      }

      # Same-nest / Other-nest splits
      if (!is.null(nst)) {
        nm_sn <- pfx("sum_samenest", v)
        nm_on <- pfx("sum_othernest", v)
        DT[, (nm_sn) := {
          x <- fco(get(v), 0)
          sum(x) - x
        }, by = .(get(mkt), get(nst))]
        DT[, (nm_on) := get(nm_all) - get(nm_sn)]
        new_cols <- c(new_cols, nm_sn, nm_on)
      }

      # Market means (Hausman-style)
      if (isTRUE(add_market_means)) {
        nm_mm <- pfx("mean_market", v)
        DT[, (nm_mm) := mean(get(v), na.rm = TRUE), by = .(get(mkt))]
        new_cols <- c(new_cols, nm_mm)
      }

      # Higher moments
      mp <- unique(moments[moments >= 2])
      if (length(mp)) {
        for (p in mp) {
          nm_all_p <- pfx(sprintf("sum_others_%spow", p), v)
          DT[, (nm_all_p) := {
            x <- fco(get(v), 0)
            xp <- x^p
            sum(xp) - xp
          }, by = .(get(mkt))]
          new_cols <- c(new_cols, nm_all_p)

          if (!is.null(frm)) {
            nm_sf_p <- pfx(sprintf("sum_samefirm_%spow", p), v)
            nm_of_p <- pfx(sprintf("sum_otherfirm_%spow", p), v)
            DT[, (nm_sf_p) := {
              x <- fco(get(v), 0)
              xp <- x^p
              sum(xp) - xp
            }, by = .(get(mkt), get(frm))]
            DT[, (nm_of_p) := get(nm_all_p) - get(nm_sf_p)]
            new_cols <- c(new_cols, nm_sf_p, nm_of_p)
          }
          if (!is.null(nst)) {
            nm_sn_p <- pfx(sprintf("sum_samenest_%spow", p), v)
            nm_on_p <- pfx(sprintf("sum_othernest_%spow", p), v)
            DT[, (nm_sn_p) := {
              x <- fco(get(v), 0)
              xp <- x^p
              sum(xp) - xp
            }, by = .(get(mkt), get(nst))]
            DT[, (nm_on_p) := get(nm_all_p) - get(nm_sn_p)]
            new_cols <- c(new_cols, nm_sn_p, nm_on_p)
          }
        }
      }
    }
  }

  # ------------------------
  # Categorical variables (same-level rival counts)
  # ------------------------
  if (length(cat_vars)) {
    for (cv in cat_vars) {
      # market same-level rivals
      nm <- pfx("cnt_samelevel", cv)
      DT[, (nm) := .N - 1L, by = .(get(mkt), get(cv))]
      new_cols <- c(new_cols, nm)

      # split by firm
      if (!is.null(frm)) {
        nm_sf <- pfx("cnt_samelevel_samefirm", cv)
        nm_of <- pfx("cnt_samelevel_otherfirm", cv)
        DT[, (nm_sf) := .N - 1L, by = .(get(mkt), get(frm), get(cv))]
        DT[, (nm_of) := get(nm) - get(nm_sf)]
        new_cols <- c(new_cols, nm_sf, nm_of)
      }

      # split by nest
      if (!is.null(nst)) {
        nm_sn <- pfx("cnt_samelevel_samenest", cv)
        nm_on <- pfx("cnt_samelevel_othernest", cv)
        DT[, (nm_sn) := .N - 1L, by = .(get(mkt), get(nst), get(cv))]
        DT[, (nm_on) := get(nm) - get(nm_sn)]
        new_cols <- c(new_cols, nm_sn, nm_on)
      }
    }
  }

  # ------------------------
  # Raw rival counts (irrespective of category)
  # ------------------------
  if (isTRUE(add_counts)) {
    nm_cnt_all <- pfx("cnt_others")
    DT[, (nm_cnt_all) := .N - 1L, by = .(get(mkt))]
    new_cols <- c(new_cols, nm_cnt_all)

    if (!is.null(frm)) {
      nm_cnt_sf <- pfx("cnt_samefirm")
      nm_cnt_of <- pfx("cnt_otherfirm")
      DT[, (nm_cnt_sf) := .N - 1L, by = .(get(mkt), get(frm))]
      DT[, (nm_cnt_of) := get(nm_cnt_all) - get(nm_cnt_sf)]
      new_cols <- c(new_cols, nm_cnt_sf, nm_cnt_of)
    }
    if (!is.null(nst)) {
      nm_cnt_sn <- pfx("cnt_samenest")
      nm_cnt_on <- pfx("cnt_othernest")
      DT[, (nm_cnt_sn) := .N - 1L, by = .(get(mkt), get(nst))]
      DT[, (nm_cnt_on) := get(nm_cnt_all) - get(nm_cnt_sn)]
      new_cols <- c(new_cols, nm_cnt_sn, nm_cnt_on)
    }
  }

  # ------------------------
  # Optional interactions (after everything exists)
  # ------------------------
  if (length(interaction_pairs)) {
    for (pr in interaction_pairs) {
      if (length(pr) != 2L) next
      a <- pr[[1]]; b <- pr[[2]]
      if (!all(c(a, b) %in% names(DT))) next
      nm <- pfx("int", a, b)
      DT[, (nm) := get(a) * get(b)]
      new_cols <- c(new_cols, nm)
    }
  }

  invisible(unique(new_cols))
}




## run stuff ####
# -- Inputs you already load earlier in your script:
philly_lic = fread("/Users/joefish/Desktop/data/philly-evict/business_licenses_clean.csv")
philly_evict = fread("/Users/joefish/Desktop/data/philly-evict/evict_address_cleaned.csv")
philly_parcels = fread("/Users/joefish/Desktop/data/philly-evict/parcel_address_cleaned.csv")
philly_occ = fread("~/Desktop/data/philly-evict/processed/infousa_parcel_occupancy_vars.csv")
philly_altos = fread("~/Desktop/data/philly-evict/altos_year_bedrooms_philly.csv")
philly_bg = read_sf("~/Desktop/data/philly-evict/philly_bg.shp")
philly_building_df = fread("~/Desktop/data/philly-evict/open-data/building_data.csv")
evict_xwalk = fread("~/Desktop/data/philly-evict/philly_evict_address_agg_xwalk.csv")
ass = fread("~/Desktop/data/philly-evict/assessments.csv")
pa_zip <- get_acs(
  geography = "zcta",
  variables = c(
    "meddhinc"=  "B19013_001", # median household income
    "housing_units"=  "B25001_001",
    "renter_occupied"=  "B25003_003",
    "owner_occupied"=  "B25003_002",
    "gross_rent"="B25064_001",
    "contract_rent" = "B25058_001"

  ),
  year = 2015,
  survey = "acs5",
  state = "PA",
  output = "wide"
  #county = "Philadelphia"
)

pa_zip2022 <- get_acs(
  geography = "zcta",
  variables = c(
    "meddhinc"=  "B19013_001", # median household income
    "housing_units"=  "B25001_001",
    "renter_occupied"=  "B25003_003",
    "owner_occupied"=  "B25003_002",
    "gross_rent"="B25064_001",
    "contract_rent" = "B25058_001"

  ),
  year = 2022,
  survey = "acs5",
  #state = "PA",
  output = "wide"
  #county = "Philadelphia"
)

# dec_2000 = load_variables(2000, "sf1", cache = TRUE)
# dec_2000_sf3 = load_variables(2000, "sf3", cache = TRUE)
# dec_vars_2010 = load_variables(2010, "sf1", cache = TRUE)
#
# # from 2000; 2010 censuses get block + block group level counts of number of housing units
# # number of rental units and number of units by structure
# bg_2000 = get_decennial(
#   geography = "block group",
#   variables = c(
#     total_housing_units = "H001001",
#     total_renter_occupied = "H003002",
#     total_owner_occupied = "H003001",
#     total_1_unit_detached = "H004003",
#     total_1_unit_attached = "H004004",
#     total_2_units = "H004005",
#     total_3_4_units = "H004006",
#     total_5_9_units = "H004007",
#     total_10_19_units = "H004008",
#     total_20_plus_units = "H004009"
#   ),
#   year = 2000,
#   state = "PA",
#   county = "Philadelphia",
#   output = "wide"
# )


# 1) Rent/Altos + Licenses
rent_list <- prep_rent_altos(philly_lic, philly_altos)

# 2) Parcels + Buildings
parcel_bldg <- prep_parcel_building(philly_parcels, philly_bg, philly_building_df,philly_occ,rent_list$lic_units)

# 2.5) model for imputing beds and baths
models_out <- build_models( rent_list$rent,parcel_bldg[year == 2019])

# 3) Evictions (returns bed-bath-PID-year and ZIP-year products)
evict_list <- prep_evictions(philly_evict, evict_xwalk, pa_zip, models_out, parcel_bldg[year == 2019])

# check that imputed beds/baths aren't getting weird
(evict_list$ev_pid_bed_year)[!is.na(med_price),mean(beds_imp_first,na.rm=T), by = year][order(year)]
(evict_list$ev_pid_year)[,sum(num_filings), by = year][order(year)]
# 4) Final assembled panel
final_panel <- assemble_panel(parcel_bldg, rent_list, evict_list)

# 4.5 # get average eviction rates for all plausible rentals
rentals <- c(
  rent_list$lic_units$PID,
  final_panel$PID
) %>% unique()

# now go back and remove rentals w/ bad license categories
invalid_rentals <- philly_lic[opa_account_num %in% rentals & (is.na(rentalcategory) |
                                             !rentalcategory %in% c("Residential Dwellings","","Other")), opa_account_num]
valid_rentals <- rentals[!rentals %in% rentals]
rental_panel <- parcel_bldg[PID %in% rentals, .(PID,num_units_imp,num_units, year)]
rental_panel <- merge(rental_panel, evict_list$ev_pid_year[, .(PID, year,num_filings)],
                      by = c("PID", "year"), all.x = TRUE)
# replace missing num_filings with 0
rental_panel[is.na(num_filings), num_filings := 0L]
# calculate filing rate
rental_panel[, filing_rate := num_filings / num_units_imp]
rental_panel[, filing_rate_alt := num_filings / num_units]

# weighted avg filing rate by year
rental_panel_agg <- rental_panel[filing_rate <= 1,
  list(
    filing_rate = weighted.mean(filing_rate, num_units_imp, na.rm = TRUE),
    filing_rate_alt = weighted.mean(filing_rate_alt, num_units, na.rm = TRUE),
    num_units_imp = sum(num_units_imp, na.rm = TRUE),
    num_units = sum(num_units, na.rm = TRUE)
), by = year]

rental_panel_agg

share_out   <- prep_share_metrics(rent_list,
                                  parcel_bldg,
                                  final_panel,
                                  evict_threshold = 0.05, # roughly avg filing rate (not filing rate / rental units)
                                  cdf_path = "figs/market_share_cdf.png")

out <- plot_market_share_cdf(share_out$share_df, cdf_path = "figs/market_share_cdf.png")
out
# adjust occupancy rates
#final_panel <- adjust_share_metrics(share_out, philly_occ, apartment_vac = 0.05, other_vac = 0.10)
adj_shares <- adjust_share_metrics( philly_occ, parcel_bldg[year == 2019 ], apartment_vac = 0.05, rental_vac = 0.10)


analytic_df  <- build_analytic_df(final_panel, adj_shares)

# override num_units_bin
analytic_df[, num_units_bins := cut(
  num_units_imp,
  breaks = c(-Inf, 1, 5, 50, Inf),
  labels = c("1", "2-5", "6-50", "51+"),
  right = TRUE
)]

# now take analytic panel and convert to bldg level
bldg_panel <- make_bldg_panel(analytic_df)
bldg_panel[,max_abs_change_occ_rate := max(change_adj_occ_rate,na.rm =T), by = PID]
# qc chceks
bldg_panel[,num_filing_rates := uniqueN(filing_rate), by = .(PID)]
bldg_panel[,.N, by = num_filing_rates]
# number of beds baths over time
bldg_panel[,num_med_beds := uniqueN(med_beds), by = .(PID)]
bldg_panel[,.N, by = num_med_beds]
bldg_panel[,total_market_share_zip :=   sum(share_units_zip,na.rm=T), by = .(pm.zip, year)]
bldg_panel[,total_market_share_units_bins := sum(share_units_zip_unit,na.rm=T), by = .(pm.zip,num_units_bins, year)]

bldg_panel[,quantile(total_market_share_zip,na.rm=T)]
bldg_panel[,quantile(total_market_share_units_bins,na.rm=T)]

bldg_panel[,sum(total_market_share_units_bins >1,na.rm=T)]
# last couple things, make the rental listing by year data
# make blp instruments
# DT has: market_id, product_id, firm_id, group_id, price, size, quality (categorical)

bldg_panel[,log_total_area := log(total_area)]
bldg_panel[,market_id := .GRP, by = .(pm.zip,num_units_bins, year)]
bldg_panel[,owner_grp := .GRP, by = .(owner_mailing)]

# make pct change in assessed value vars
ass[,PID := as.numeric(parcel_number)]
ass[,taxable_value := as.numeric(taxable_land + taxable_building)]
ass[order(year),change_taxable_value := taxable_value - data.table::shift(taxable_value), by = PID]
# pct change
ass[,pct_change_taxable_value := change_taxable_value / data.table::shift(taxable_value), by = PID]

# where there are huge changes in taxable values, set all to NA for that parcel
ass[,min_change_taxable_value := min(change_taxable_value, na.rm = TRUE), by = PID]
ass[,max_change_taxable_value := max(change_taxable_value, na.rm = TRUE), by = PID]
ass[,range_change_taxable_value := max_change_taxable_value - min_change_taxable_value]

# fix NAs and infinites
ass[!is.finite(pct_change_taxable_value), pct_change_taxable_value := NA_real_]
ass[,min_pct_change_taxable_value := min(pct_change_taxable_value, na.rm = TRUE), by = PID]
ass[,max_pct_change_taxable_value := max(pct_change_taxable_value, na.rm = TRUE), by =PID]
ass[,range_pct_change_taxable_value := max_pct_change_taxable_value - min_pct_change_taxable_value]

ass[,quantile(range_change_taxable_value,na.rm=T, probs = seq(0,1,0.01))]

ass[,quantile(range_pct_change_taxable_value,na.rm=T, probs = seq(0,1,0.01))] %>% round(3)

ass[,log_taxable_value := log(taxable_value)]
# drop infinites
ass[,log_taxable_value := fifelse(is.finite(log_taxable_value), log_taxable_value, NA_real_)]
ass[order(PID,year),change_log_taxable_value := log_taxable_value - data.table::shift(log_taxable_value), by = PID]
# drop infinit
ass[,change_log_taxable_value := fifelse(is.finite(change_log_taxable_value), change_log_taxable_value, NA_real_)]
# drop ones that have huge swings in assessed value
# ass[
#   abs(min_pct_change_taxable_value) >= 3 |
#   abs(max_pct_change_taxable_value) >= 3
# ,  c("taxable_value", "change_taxable_value",
#      "pct_change_taxable_value") := NA_real_]

# make some lags
ass[order(year), taxable_value_lag1 := data.table::shift(taxable_value), by = PID]
ass[order(year), taxable_value_lag2 := data.table::shift(taxable_value, 2), by = PID]
# for chnages and pct changes
ass[order(year), change_taxable_value_lag1 := data.table::shift(change_taxable_value), by = PID]
ass[order(year), pct_change_taxable_value_lag1 := data.table::shift(pct_change_taxable_value), by = PID]
ass[,max_abs_change_log_taxable_value := max(abs(change_log_taxable_value),na.rm=T), by = PID]
ass[,.N, by = max_abs_change_log_taxable_value>= 2]
# set assessent changes > 1 to NA
ass = ass %>%
  mutate(across(contains('taxable'), ~fifelse(max_abs_change_log_taxable_value >= 2, NA_real_, .x ) ) )

# merge on assessment data
bldg_panel_m = merge(bldg_panel %>% mutate(PID = as.numeric(PID)) %>% select(-contains("value"), -contains("exempt"), -contains("taxable")),
                     ass %>%
                       select(-parcel_number, - contains("range"), matches("min|max")), by = c("PID", "year"), all.x = T)

# per unit
bldg_panel_m[,change_taxable_value_per_unit := change_taxable_value / num_units_imp]
bldg_panel_m[,log_taxable_value_per_unit := log(taxable_value / num_units_imp)]
bldg_panel_m[,high_evict := filing_rate >= 0.1]
bldg_panel_m = bldg_panel_m %>%
  mutate(across(contains("permit"), ~as.numeric(.x)))

bldg_panel_m = bldg_panel_m %>%
  mutate(across(contains("violation"), ~as.numeric(.x)))

# drop instruments
bldg_panel_m = bldg_panel_m %>%
  select(-matches("^z_"))

make_blp_instruments(
  bldg_panel_m,
  market      = "market_id",
  product_id  = "PID",
  firm        = "owner_grp",
  nest        = "high_evict",
  cont_vars   = c(
                  "imminently_dangerous_violation_count",
                  "hazardous_violation_count" ,
                  "total_violations",
                  "building_permit_count",
                  "general_permit_count",
                  "total_permits",
                  "mechanical_permit_count",
                  "zoning_permit_count"  ,
                  "plumbing_permit_count",
                  "log_taxable_value",
                  "change_log_taxable_value",
                  "change_taxable_value_per_unit"
                  ),
  cat_vars    = c("quality_grade_fixed"),
  add_market_means = F,
  moments     = 1,  # also add ∑ x_j^2 instruments
  add_counts  = TRUE,
  #interaction_pairs = list(c("z_sum_otherfirm_num_units_imp", "z_cnt_otherfirm")),
  prefix      = "z"
)

## =========================
## Spatial rivals (1–3 km band) helpers
## =========================
suppressPackageStartupMessages({
  require(data.table)
  require(geodist)     # fast geodesic distances
})



# Add 1–3 km rival-band sums/means for selected variables, within market
# Creates:
#   z_cnt_1to3km                 (rival count in band)
#   z_sum_1to3km_<var>           (sum of var among rivals in band)
#   z_mean_1to3km_<var>          (mean of var among rivals in band)
add_spatial_rival_band <- function(
    DT,
    market,
    cont_vars,
    lat = "parcel_lat",
    lon = "parcel_long",
    dmin_km = 1,
    dmax_km = 3,
    prefix = "z"
) {
  stopifnot(is.data.table(DT))
  stopifnot(is.character(market), length(market) == 1)

  # Prepare output column names
  band_tag <- "1to3km"
  cnt_col  <- paste(prefix, "cnt", band_tag, sep = "_")
  to_make  <- c(cnt_col, unlist(lapply(cont_vars, function(v)
    c(paste(prefix, "sum",  band_tag, v, sep = "_"),
      paste(prefix, "mean", band_tag, v, sep = "_"))
  )))

  # pre-create with NA to avoid reallocation
  for (nm in to_make) if (!(nm %in% names(DT))) DT[, (nm) := NA_real_]

  # Work market-by-market to keep matrices manageable
  DT[, .I, by = market][, {
    idx <- I
    sub  <- DT[idx]

    # coordinates must be numeric & finite to compute distances
    latv <- suppressWarnings(as.numeric(sub[[lat]]))
    lonv <- suppressWarnings(as.numeric(sub[[lon]]))
    ok   <- is.finite(latv) & is.finite(lonv)

    if (!any(ok)) {
      # nothing we can do; leave NA for this market
      NULL
    } else {
      # distance matrix in meters
      coords <- cbind(lonv[ok], latv[ok])
      colnames(coords) <- c("lon", "lat")
      dmat_m <- geodist::geodist(coords, measure = "geodesic")
      # convert to km and build 1–3 km band mask (exclude self)
      dmat_km <- dmat_m / 1000
      W <- (dmat_km > dmin_km) & (dmat_km <= dmax_km)
      diag(W) <- FALSE

      # row counts
      cnt <- rowSums(W)
      # write counts back to the right rows
      DT[idx[ok], (cnt_col) := as.numeric(cnt)]

      # For each var: sums & means over rivals in the band
      for (v in cont_vars) {
        x_all <- suppressWarnings(as.numeric(sub[[v]]))
        xv    <- x_all[ok]               # values aligned to W’s columns
        okx   <- is.finite(xv)           # which rivals have non-missing x

        # Exclude NA rivals from contributing:
        W2 <- W
        if (any(!okx)) W2[, !okx] <- FALSE

        cnt_nonmiss <- rowSums(W2)       # rivals in band with non-missing x
        xv0 <- xv
        xv0[!okx] <- 0                   # safe for multiplication

        sums  <- as.numeric(W2 %*% matrix(xv0, ncol = 1))
        means <- ifelse(cnt_nonmiss > 0, sums / cnt_nonmiss, NA_real_)

        nm_sum  <- paste(prefix, "sum",  band_tag, v, sep = "_")
        nm_mean <- paste(prefix, "mean", band_tag, v, sep = "_")

        DT[idx[ok], (nm_sum)  := sums]
        DT[idx[ok], (nm_mean) := means]
      }
      NULL
    }
  }, by = market]

  invisible(to_make)
}

# Ensure we have usable lat/long and build spatial rival-band (1–3 km) instruments
# If you already have lat/lon columns with different names, the helper will try to auto-map them.
# Otherwise it will still create parcel_lat/parcel_long (blank strings) to match your spec.

# Choose which variables you want spatial sums/means for (can reuse your cont_vars)
spatial_vars <- c(
  # "imminently_dangerous_violation_count",
  # "hazardous_violation_count",
  # "total_violations",
  # "building_permit_count",
  # "general_permit_count",
  # "total_permits",
  # "mechanical_permit_count",
  # "zoning_permit_count",
  # "plumbing_permit_count",
  "log_taxable_value",
  'year_blt_decade',
  "change_log_taxable_value",
  "log_taxable_value_per_unit"
)

# If your market is zip x size bin x year (your market_id), we already created it:
# bldg_panel_m[, market_id := .GRP, by = .(pm.zip,num_units_bins, year)]
# Run the spatial band adder:
# turn SHAPE into lat / long; format is  -75.1486628430591|39.9300970916323
bldg_panel_m[, c("parcel_long", "parcel_lat") := tstrsplit(SHAPE, "\\|")]
bldg_panel_m[, parcel_long := as.numeric(parcel_long)]
bldg_panel_m[, parcel_lat  := as.numeric(parcel_lat)]
bldg_panel_m[,log_market_value := log(market_value)]
bldg_panel_m[,num_unit_bins_year := .GRP, by = .(num_units_bins, year)]

temp = bldg_panel_m[num_units_imp > 5 & !is.na(parcel_long) & !is.na(parcel_long) & year >= 2016]
add_spatial_rival_band(
  bldg_panel_m, # subset to where it makes sense
  market   = "num_unit_bins_year",
  cont_vars= spatial_vars,
  lat      = "parcel_lat",   # will auto-create/auto-fill if missing
  lon      = "parcel_long",  # will auto-create/auto-fill if missing
  dmin_km  = 1,
  dmax_km  = 3,
  prefix   = "z"
)
feols(share_units_zip_unit ~1|
         market_id +census_tract|
        log_med_rent ~ poly(z_sum_1to3km_log_taxable_value,1) +
        poly(z_mean_1to3km_log_taxable_value,1)+
        poly(z_sum_1to3km_log_taxable_value_per_unit,1),
      data = bldg_panel_m[num_units_imp <= 500 &
                            num_units_imp > 5 &
                            year %in% 2012:2019&
                            !is.na(z_mean_1to3km_log_taxable_value) &
                            !is.na(z_sum_1to3km_log_taxable_value_per_unit)])

feols(share_units_zip_unit ~ log_med_rent + log_market_value+log_total_area +poly(num_units_imp,2)|
        census_tract^year + year_blt_decade+census_tract^year + building_code_description_new_fixed + quality_grade_fixed,
      data = bldg_panel_m[num_units_imp <= 500 & num_units_imp > 5 & year %in% 2012:2019])

feols(log_med_rent ~poly(z_sum_1to3km_log_taxable_value,2)
    |   census_tract^year + year_blt_decade+census_tract^year + building_code_description_new_fixed + quality_grade_fixed,
    data = bldg_panel_m[num_units_imp <= 500 & num_units_imp > 5& year %in% 2012:2019 & year_built >= 2010])


# (Optional) sanity checks
bldg_panel_m[, summary(z_cnt_1to3km)]
bldg_panel_m[, lapply(.SD, function(x) quantile(x, na.rm = TRUE)),
             .SDcols = patterns("^z_sum_1to3km_|^z_mean_1to3km_")]



bldg_panel_m[year == 2019,quantile(z_sum_otherfirm_change_log_taxable_value,na.rm=T)]
bldg_panel_m[year == 2019,quantile(change_log_taxable_value,na.rm=T)]
# write files
outdir <- "/Users/joefish/Desktop/data/philly-evict/processed"
# make outdir if it doesn't exist
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
fwrite(final_panel, file.path(outdir, "final_panel.csv"), row.names = FALSE)
fwrite(analytic_df, file.path(outdir, "analytic_df.csv"), row.names = FALSE)
fwrite(bldg_panel_m, file.path(outdir, "bldg_panel.csv"), row.names = FALSE)
fwrite(rent_list$lic_long_min, file.path(outdir, "license_long_min.csv"), row.names = FALSE)
fwrite(parcel_bldg, file.path(outdir, "parcel_building.csv"), row.names = FALSE)
fwrite(parcel_bldg[year == 2024], file.path(outdir, "parcel_building_2024.csv"), row.names = FALSE)
fwrite(share_out$share_df_agg, file.path(outdir, "market_share_details.csv"), row.names = FALSE)
fwrite(share_out$share_df, file.path(outdir, "market_share_all_zips.csv"), row.names = FALSE)
fwrite(evict_list$ev_pid_bed_year, file.path(outdir, "evict_pid_bed_year.csv"), row.names = FALSE)
fwrite(evict_list$ev_pid_year, file.path(outdir, "evict_pid_year.csv"), row.names = FALSE)
fwrite(rent_list$rent, file.path(outdir, "rent_altos.csv"), row.names = FALSE)
fwrite(rent_list$lic_units, file.path(outdir, "rent_licenses.csv"), row.names = FALSE)
fwrite(evict_list$ev_zip_year, file.path(outdir, "evict_zip_year.csv"), row.names = FALSE)
# write adj shares
fwrite(adj_shares, file.path(outdir, "adjusted_market_shares.csv"), row.names = FALSE)
# final_panel now contains: rent (Altos) + parcel/building attributes + PID-year eviction counts
# + ZIP-year eviction rates + num_units from licenses, with your cleaned fields preserved.
bldg_panel_m[,change_log_med_rent := log_med_rent - data.table::shift(log_med_rent), by = PID]
bldg_panel_m[,change_years := year - data.table::shift(year), by = PID]
bldg_panel_m[,change_log_med_rent_per_year := change_log_med_rent / change_years]
bldg_panel_m[,quantile(change_log_med_rent_per_year,na.rm=T)]
bldg_panel_m[,existed_in_2018 := any(year == 2018), by = PID]

# min max shares
bldg_panel_m[,min_share_units_zip := min(share_units_zip, na.rm = TRUE), by = PID]
bldg_panel_m[,max_share_units_zip := max(share_units_zip, na.rm = TRUE), by = PID]
bldg_panel_m[,range_share_units_zip := max_share_units_zip - min_share_units_zip]

# check how much the adjustment is doing
bldg_panel_m[year %in% 2016:2019,list(
  num_units = sum(num_units,na.rm=T) ,
  num_units_imp = sum(num_units_imp,na.rm=T) ,
  adj_num_units_imp = sum(adj_num_units_imp,na.rm=T)
), by = num_units_bins ][,implied_occ := adj_num_units_imp / num_units_imp][order(num_units_bins)][]

View(bldg_panel_m[PID %in% sample(PID[num_units_imp>=5], 10)] %>% relocate( num_units_imp,
                                                                            adj_num_units_imp,
                                                                            log_med_rent,
                                                                            source,
                                                                            share_units_zip_unit,
                                                                            PID, year,contains("occupancy_ra")))

bldg_panel_m[,num_sources := uniqueN(source), by = PID]

bldg_panel_m[ num_units_imp >= 10 &
                year %in% 2016:2019 ,quantile(change_log_taxable_value,na.rm=T, probs = seq(0,1,0.01))]
# try to instrument for prices given market + PID fixed effects
sample_df = bldg_panel_m[ num_units_imp > 5&
                           year %in% 2011:2019
                           #abs(change_log_med_rent_per_year) <= 0.5
                         & abs(change_log_taxable_value )<= 1.5]


sample_df[,num_years := .N, by = PID]
sample_df[,quantile(num_years)]

feols(log_med_rent ~
        # z_sum_otherfirm_building_permit_count+
        # z_sum_others_general_permit_count +
        #num_filings+
       z_sum_otherfirm_imminently_dangerous_violation_count +
        log_taxable_value +#+ change_log_taxable_value^2
        z_sum_otherfirm_change_log_taxable_value
       # z_sum_othernest_change_log_taxable_value
      |PID +market_id + month,
      cluster = ~PID,
      data = sample_df[])

fwlplot::fwl_plot(log_med_rent ~
        # z_sum_otherfirm_building_permit_count+
        # z_sum_others_general_permit_count +
          log_taxable_value
        #z_sum_otherfirm_imminently_dangerous_violation_count +
        #log_taxable_value +#+ change_log_taxable_value^2
        #z_sum_otherfirm_change_log_taxable_value
      # z_sum_othernest_change_log_taxable_value
      |PID + market_id + month,
      cluster = ~PID,
      data = sample_df)

# make annualized change in rent
sample_df[,annualized_change_log_med_rent := change_log_med_rent / change_years]
sample_df[,annualized_change_shares := change_share_units_zip / change_years]
# annualized occ rate change
sample_df[,annualized_change_adj_occ_rate := change_adj_occ_rate / change_years]
sample_df[,quantile(annualized_change_adj_occ_rate,na.rm=T, probs = seq(0,1,0.01))]
sample_df[,num_units_imp_cuts := cut(num_units_imp, breaks = c(-Inf,1,5,10,50,100,200, Inf),
                                 labels = c("1","2-5","6-10","11-50","51-100","101-200","200+"))]
share_resid_reg <- feols(share_units_zip_unit~ 1|market_id + PID,
      cluster=~PID,
      data = sample_df[abs(annualized_change_log_med_rent) <= 0.25 & year_built <= 2000
                       ])
rent_resid_reg <- feols(log_med_rent ~ 1|market_id + PID,
      cluster=~PID,
      data = sample_df[abs(annualized_change_log_med_rent) <= 0.25 & year_built <= 2000
                       ])
sample_df[,share_units_zip_unit_resid :=share_units_zip_unit- predict(share_resid_reg,newdata = sample_df)]
sample_df[,log_med_rent_resid :=log_med_rent - predict(rent_resid_reg,newdata = sample_df)]

ggplot(sample_df[abs(annualized_change_log_med_rent) <= 0.25 & year_built <= 2000 & abs(log_med_rent_resid)<=0.1],
       aes(x = log_med_rent_resid, y = share_units_zip_unit_resid)) +
  geom_point(alpha = 0.5) +
  geom_smooth(color = "blue") +
  theme_minimal()

ggplot(sample_df[abs(annualized_change_log_med_rent) <= 0.25 & year_built <= 2000 & annualized_change_adj_occ_rate != 0],
       aes(x = annualized_change_log_med_rent, y = annualized_change_adj_occ_rate)) +
  geom_point(alpha = 0.5) +
  geom_smooth(color = "blue") +
  theme_minimal()

# in reg form
fwlplot::fwl_plot(adj_occ_rate ~ log_med_rent +log(market_value)|census_tract^year ,

      cluster=~PID,
      data = sample_df[abs(annualized_change_log_med_rent) <= 0.25 & abs(annualized_change_adj_occ_rate)>=0.05
      ])


feols(share_units_zip_unit ~ adj_occ_rate+log_med_rent|market_id +PID,

      cluster=~PID,
      data = sample_df[abs(annualized_change_log_med_rent) <= 0.25 & year_built <= 2000
                       ])


feols(share_units_zip_unit_resid ~ log_med_rent_resid,

      cluster=~PID,
      data = sample_df[abs(annualized_change_log_med_rent) <= 0.25 & year_built <= 2000 & abs(log_med_rent_resid)<=0.2 & abs(share_units_zip_unit_resid)<=0.2
                       ])



fwlplot::fwl_plot(share_units_zip ~log_med_rent+log(market_value)|census_tract^year,
      data =sample_df[abs(annualized_change_log_med_rent) <= 0.25 & year_built <= 2000])

philly_occ[,mean(occupancy_rate,nabuilding_code_description_new_fixed.rm=T), by = cut(num_households, breaks = c(-Inf,1,5,50, Inf),
                                                        labels = c("1","2-5","6-50","51+"))][order(V1)]
philly_occ[,change_occupancy_rate %>% mean(na.rm=T), by = cut(num_households, breaks = c(-Inf,1,5,50, Inf),
                                                                    labels = c("1","2-5","6-50","51+"))][order(V1)]
bldg_df_m[order(year),change_occupancy_rate := adj_occ_rate - data.table::shift(adj_occ_rate), by = PID]
bldg_df_m[,median(change_occupancy_rate,na.rm=T),by = num_units_bins]
bldg_df_m[year %in% 2016:2019,mean(1-adj_occ_rate,na.rm = T),by = num_units_bins]
feols(log_med_rent ~ log_taxable_value|PID + pm.zip^year^num_units_bins + month,
      #weights = ~num_units_imp,
                  data =bldg_df_m[num_units_imp > 10 & year %in% 2016:2019])

sample_df %>%
  select(matches("z.+others.+(viol|permit|value)"), log_med_rent) %>%
  cor(use = "pairwise.complete.obs") %>%
  # grab just log_mes_rent row
  .["log_med_rent", ] %>%
  sort(decreasing = TRUE)


