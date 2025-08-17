## =========================
## Libraries & helpers
## =========================
library(data.table)
library(stringr)
library(sf)
library(tidycensus)
library(zipcodeR)   # for zip_code_db
library(ggplot2)
library(spatstat.geom)


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
prep_parcel_building <- function(philly_parcels, philly_bg, philly_building_df, lic_units) {
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

  # imput missing number of units
  # impute units for those w/ missing units
  units_model <- feols(num_units ~total_area + total_area^2 + total_livable_area + total_livable_area^2+
                         year_built + year_built^2 + number_stories + number_stories^2 |
                         quality_grade_fixed  + building_code_description_new_fixed +
                         general_construction + GEOID,
                       data = parcel_bldg, combine.quick = FALSE)

  parcel_bldg[,num_units_pred := round(predict(units_model, newdata = parcel_bldg))]
  parcel_bldg[,num_units_imp := fifelse(is.na(num_units), num_units_pred,round(num_units))]
  parcel_bldg[,num_units_imp := fifelse(num_units_imp < 0 , num_units_imp,num_units_imp)]
  # pad PID
  parcel_bldg[, PID := str_pad(PID, 9, "left", "0")]

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

  lic <- lic[licensetype == "Rental"]
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
  lic_units <- lic_long[year <= 2022,
                        .(num_units = median(numberofunits, na.rm = TRUE)),
                        by = .(PID)]

  # Altos rent panel restricted to licensed PIDs
  rent <- as.data.table(altos[PID %in% lic_long$PID])
  if (!"ymd" %in% names(rent) && all(c("year","month","day") %in% names(rent))) {
    rent[, ymd := paste0(year, "-", month, "-", day)]
  }
  rent[, ymd_char := paste0(year, "-", month, "-", day)]
  rent <- rent[year %in% 2006:2022 & med_price > 500 & med_price < 7500]

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
  lic_long_min <- lic_long_min[!is.na(PID)]
  rent[,source := "altos"]
  list(
    rent = rent[, .(PID, bed_bath_pid_ID, beds_imp_first, beds_imp_first_fixed,month, day,ymd_char,source,
                    baths_first, baths_first_fixed, ymd, year, med_price, num_listings, log_med_price)],
    lic_units = lic_units,
    lic_long_min = lic_long_min
  )
}

##
## =========================
## 2) Impute futures
## =========================
build_models <- function(rent_df, bldg_df){
  analytic_df <- merge(rent_df, bldg_df, by = c("PID"), all.x = TRUE)
  analytic_df = analytic_df[!is.na(num_units_imp) & !is.na(med_price) &
                            !is.na(beds_imp_first) & !is.na(baths_first) &
                            !is.na(year_built) & !is.na(number_stories) &
                            !is.na(building_code_description_new_fixed) &
                            !is.na(quality_grade_fixed) & !is.na(general_construction) &
                            !is.na(GEOID) & !is.na(market_value) &
                            !is.na(total_area) & !is.na(number_of_bathrooms) &
                            !is.na(number_of_bedrooms) &
                            !is.na(type_heater)]
  analytic_df[,log_med_price := log(med_price)]
  bed_reg <- fixest::feols(
    beds_imp_first ~ log_med_price + log_med_price^2 + log_med_price^3+
      #building_code_description_new_fixed +
      num_units_imp + num_units_imp^2 + num_units_imp^3+
      year_built + year_built^2 +
      number_stories + number_stories^2 +
      number_of_bathrooms + number_of_bathrooms^2 +
      number_of_bedrooms + number_of_bedrooms^2 +
      log(market_value) +
      #building_code_description_new_fixed+
      log(total_area)|
      CT_ID_10+ quality_grade_fixed   ,
    data = analytic_df, combine.quick = FALSE
  )

  bath_reg <-  fixest::feols(
    baths_first ~ log_med_price + log_med_price^2 + log_med_price^3 +
      num_units_imp + num_units_imp^2 + num_units_imp^3 +
      year_built + year_built^2 +
      number_stories + number_stories^2 +
      number_of_bathrooms + number_of_bathrooms^2 +
      number_of_bedrooms + number_of_bedrooms^2 +
      log(market_value) +
      #building_code_description_new_fixed+
      log(total_area)|
      CT_ID_10+quality_grade_fixed   ,
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

  # Business entity keyword screen
  business_words <- c(
    "LLC","L\\.L\\.C\\.","LLCS","LIMITED PARTNERSHIP","LTD","L\\.T\\.D\\.","LTDS",
    "INC","INC\\.","INCS","INCORPORATED","CORP","CORPORATION","CORPS","L\\.P\\.","LP","LPS",
    "LLP","LLPS","CO","CO\\.","COMPANY","COMPANIES","HOLDING","HOLDINGS","PARTNERSHIP",
    "PARTNERSHIPS","PARTNER","PARTNERS","ASSOC","ASSOCS","ASSOCIATES","ASSOCIATION",
    "ASSOCIATIONS","ENTERPRISE","ENTERPRISES","VENTURE","VENTURES","GROUP","GROUPS",
    "SOLUTIONS","STRATEGIES","BROS","BROTHERS","FIRM","FIRMS","TRUST","TRUSTS",
    "HOUS","HOUSING","APARTMENTS","APTS?","REAL","ESTATE","REALTY","MANAGEMENT","MGMT",
    "DEV","DEVELOPMENT","DEVELOPS","DEVELOPERS","THE"
  )
  pattern <- str_c("\\b(", str_c(business_words, collapse = "|"), ")\\b")

  ev[, commercial_alt := str_detect(str_to_upper(defendant), pattern)]

  # Join to PID (limit to unique parcel matches)

  xw1 <- xw[num_parcels_matched == 1, .(PID, n_sn_ss_c)]
  # pad PID
  xw1[, PID := str_pad(PID, 9, "left", "0")]
  ev_m <- merge(ev, xw1, by = "n_sn_ss_c")

  # Housing authority flag
  ev_m[, housing_auth := str_detect(str_to_upper(plaintiff), "PHILA.+A?UTH|HOUS.+AUTH")]
  ev_m[,log_med_price := log(ongoing_rent)]

  # impute chars
  ev_m = ev_m %>%
    merge(
      bldg_df[, .(PID, year_built, number_stories, building_code_description_new_fixed,
                  quality_grade_fixed, general_construction, GEOID, market_value,num_units_imp,CT_ID_10,
                  total_area, number_of_bathrooms, number_of_bedrooms, type_heater)]  ,
      by = "PID"
    )
  # keep ev_m w/ rents in [300,10000]
  ev_m = ev_m[ongoing_rent >= 300 & ongoing_rent <= 10000]

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
    med_price              = suppressWarnings(median(ongoing_rent[housing_auth == FALSE], na.rm = TRUE)),
    pm.zip                 = first(pm.zip),
    ymd                    = as.character(first(d_filing))
  ), by = .(year, PID)]

  ev_parcel_bed_agg <- ev_m[commercial_alt == FALSE, .(
    num_filings            = sum(housing_auth == FALSE, na.rm = TRUE),
    num_filings_with_houth_auth = .N,
    med_price              = suppressWarnings(median(ongoing_rent[housing_auth == FALSE], na.rm = TRUE)),
    pm.zip                 = first(pm.zip),
    ymd                    = as.character(first(d_filing))
  ), by = .(year, PID,  beds_imp_first, baths_first)]

  ev_parcel_agg[, source := "evict"]
  ev_parcel_bed_agg[, source := "evict"]

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
  ev_zip_m[, source := "evict"]

  ev_parcel_agg[,ymd_char := ymd]
  ev_parcel_agg[, ymd := as.Date(ymd_char)]
  ev_parcel_agg[, year := as.integer(substr(ymd_char, 1, 4))]
  ev_parcel_agg[, month := as.integer(substr(ymd_char, 6, 7))]
  ev_parcel_agg[, day := as.integer(substr(ymd_char, 9, 10))]
  ev_parcel_agg[,num_filings_total := sum(num_filings), by = PID]
  ev_parcel_agg[order(year),num_filings_cumsum := cumsum(num_filings), by = PID]

  list(
    ev_pid_year = ev_parcel_agg[, .(PID, year, num_filings,
                             num_filings_with_houth_auth,
                             source,
                             num_filings_total,
                             num_filings_cumsum,
                              ymd)],
    ev_pid_bed_year = ev_parcel_bed_agg[, .(PID, year, beds_imp_first, baths_first,
                                   source,
                                   med_price, ymd)],
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

  # extend lic long min w/ rent data
  lic_long_min_extend <- final_panel[! PID %in% lic_long_min$PID ,
                                      list(
                                        numberofunits = median(num_units_imp)
                                      ), by = .(PID, year, pm.zip) ]

  lic_long_min <- rbindlist(list(
    lic_long_min,
    lic_long_min_extend
  ),use.names = TRUE, fill = TRUE)

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
  # bins of 1, 2-5, 6-50, 51-100, 100 +
  share_df[, num_units_bins := fifelse(num_units == 1, "1",
                                       fifelse(num_units <= 5, "2-5",
                                               fifelse(num_units <= 50, "6-50",
                                                       fifelse(num_units <= 100, "51-100",
                                                               fifelse(num_units > 100, "101+", NA_character_)))))]
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

  # High-evicting aggregates
  share_df[, num_units_evict        := sum(num_units[high_evict], na.rm = TRUE), by = .(pm.zip, year)]
  share_df[, num_units_evict_owner  := sum(num_units[high_evict], na.rm = TRUE), by = .(year, pm.zip, owner_mailing)]
  share_df[, share_units_evict      := fifelse(high_evict, num_units_evict_owner / num_units_evict, 0)]

  # By unit bin
  share_df[, num_units_evict_unit_bins  := sum(num_units[high_evict], na.rm = TRUE), by = .(year, pm.zip, num_units_bins)]
  share_df[, num_units_evict_owner_bins := sum(num_units[high_evict], na.rm = TRUE), by = .(year, pm.zip, owner_mailing, num_units_bins)]
  share_df[, share_units_evict_bins     := fifelse(high_evict, num_units_evict_owner_bins / num_units_evict_unit_bins, 0)]

  share_df[, num_units_unit_bins  := sum(num_units), by = .(year, pm.zip, num_units_bins)]
  share_df[, num_units_owner_bins := sum(num_units), by = .(year, pm.zip, owner_mailing, num_units_bins)]
  share_df[, share_units_bins     := num_units_owner_bins / num_units_unit_bins]

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
    altos[,.(PID, beds_imp_first, month, day, source,
            baths_first, ymd=ymd_char, year, med_price)],
    ev_pid_bed_year[,.(PID, med_price, ymd, year,  source = "evict",
                   baths_first, beds_imp_first
                   )]
  ), use.names = T, fill = TRUE)

  rent[,month := as.integer(str_replace(substr(ymd, 6, 7),"-",""))]
  rent[,day := as.integer(str_replace(str_sub(ymd, -2,-1), "-",""))]
  # get list of parcels that exist in either rent or evict data
  valid_parcels = parcel_bldg[PID %in% rent[,unique(PID)] & !is.na(PID), unique(PID)]

  # Start with rent (PID-year) then enrich
  out <- merge(rent, parcel_bldg[PID %in% valid_parcels], by = c("PID","year"), all.x = TRUE,all.y = T)

  # PID-year eviction counts
  out <- merge(out, ev_pid_year[,.(year,PID,num_filings,
                                   num_filings_with_houth_auth,
                                   num_filings_cumsum,
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
  out[is.na(num_filings_cumsum), num_filings_cumsum := 0L]
  # PID-level totals and rates
  out[, years_per_pid := fifelse(year_built <= 2006,2024 - 2006, 2024 - year_built )]
  out[,years_per_pid_rel06 := years_per_pid - (year - 2006) ]
  out[, filing_rate := fifelse(!is.na(num_units_imp) & num_units_imp > 0 & years_per_pid > 0,
                               num_filings_total / num_units_imp / years_per_pid, NA_real_)]

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

  dt <- copy(final_panel)
  dt[,log_med_rent := log(med_price)]
  # Filter as in your analytic_df block (add a safe guard for log() domain)
  analytic_df <- dt[
    !is.na(year_built_estimate) &
      !is.na(total_area) &
      !is.na(num_units_imp) &
      !is.na(log_med_rent) &
      !is.na(num_filings) &
      !is.na(quality_grade_fixed) &
      !is.na(zoning) &
      !is.na(building_code_description_new_fixed) &
      total_area < 1e6 &
      !is.na(GEOID) &
      med_price > 300 &
      med_price <= 10000 &
      num_units_imp > 0 &
      !is.na(number_stories) &
      total_area > 100
  ]
  print(glue::glue("reduced rows from {nrow(dt)} to {nrow(analytic_df)}"))

  # Derived features
  analytic_df[, ymd_char := as.character(ymd)]
  analytic_df[, month := lubridate::month(ymd_char)]
  analytic_df[, filing_rate_sq   := filing_rate^2]
  analytic_df[, filing_rate_cube := filing_rate^3]
  analytic_df[, num_filings_total_sq := num_filings_total^2]

  # Merge share_df_agg
  # drop_cols <- grep("share_units|^num_units_.*|^hhi", names(analytic_df), value = TRUE)
  # if (length(drop_cols)) analytic_df[, (drop_cols) := NULL]
  analytic_df <- merge(analytic_df, share_df_agg, by = "PID", all.x = TRUE, suffixes = c("", "_share"))
  #analytic_df[,multi_source := uniqueN(source) > 1L, by = .(PID)]

  # More derived fields
  analytic_df[, share_units_evict_sq := share_units_evict^2]
  analytic_df[, share_units_zip_sq   := share_units_zip^2]

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
  valid_zips <- share_df[,any(
    !is.na(share_units_zip) &
    (share_units_evict > 0 & is.finite(share_units_evict)) &
    !is.na(share_units_bins) &
    (share_units_evict_bins > 0 & is.finite(share_units_evict_bins))
  ), by = .(year, pm.zip)][V1 == TRUE, unique(pm.zip)]

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
         filter = quote(!is.na(share_units_zip)),
         label = "Zip Code"),
    list(col = "share_units_evict",
         filter = quote(share_units_evict > 0 & is.finite(share_units_evict)),
         label = "Zip Code X High-Evicting Units"),
    list(col = "share_units_bins",
         filter = quote(!is.na(share_units_bins)),
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


## run stuff ####
# -- Inputs you already load earlier in your script:
philly_lic = fread("/Users/joefish/Desktop/data/philly-evict/business_licenses_clean.csv")
philly_evict = fread("/Users/joefish/Desktop/data/philly-evict/evict_address_cleaned.csv")
philly_parcels = fread("/Users/joefish/Desktop/data/philly-evict/parcel_address_cleaned.csv")
philly_altos = fread("~/Desktop/data/philly-evict/altos_year_bedrooms_philly.csv")
philly_bg = read_sf("~/Desktop/data/philly-evict/philly_bg.shp")
philly_building_df = fread("~/Desktop/data/philly-evict/open-data/building_data.csv")
evict_xwalk = fread("~/Desktop/data/philly-evict/philly_evict_address_agg_xwalk.csv")
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


# 1) Rent/Altos + Licenses
rent_list <- prep_rent_altos(philly_lic, philly_altos)

# 2) Parcels + Buildings
parcel_bldg <- prep_parcel_building(philly_parcels, philly_bg, philly_building_df,rent_list$lic_units)

# 2.5) model for imputing beds and baths
models_out <- build_models( rent_list$rent,parcel_bldg[year == 2023])

# 3) Evictions (returns bed-bath-PID-year and ZIP-year products)
evict_list <- prep_evictions(philly_evict, evict_xwalk, pa_zip, models_out, parcel_bldg[year == 2023])


# 4) Final assembled panel
final_panel <- assemble_panel(parcel_bldg, rent_list, evict_list)

share_out   <- prep_share_metrics(rent_list,
                                  parcel_bldg,
                                  final_panel,
                                  evict_threshold = 0.08,
                                  cdf_path = "figs/market_share_cdf.png")

out <- plot_market_share_cdf(share_out$share_df, cdf_path = "figs/market_share_cdf.png")

analytic_df  <- build_analytic_df(final_panel, share_out$share_df_agg)


# final_panel now contains: rent (Altos) + parcel/building attributes + PID-year eviction counts
# + ZIP-year eviction rates + num_units from licenses, with your cleaned fields preserved.
