## ============================================================
## make-altos-aggs.R
## ============================================================
## Purpose: Aggregate Altos rental listing data to bedroom-level
##          and building-level panels by year.
##
## Inputs:
##   - cfg$products$altos_clean (clean/altos_clean.csv)
##   - cfg$products$altos_parcel_xwalk (xwalks/altos_parcel_xwalk.csv)
##
## Outputs:
##   - cfg$products$altos_year_bedrooms (panels/altos_year_bedrooms.csv)
##   - cfg$products$altos_year_building (panels/altos_year_building.csv)
##
## Primary keys:
##   - altos_year_bedrooms: PID + bed_bath_pid_ID + year
##   - altos_year_building: PID + year
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
})

# ---- Load config and set up logging ----
source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "make-altos-aggs.log")

logf("=== Starting make-altos-aggs.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ============================================================
# HELPER FUNCTIONS
# ============================================================

Mode <- function(x, na.rm = FALSE) {
  if (na.rm) {
    x <- x[!is.na(x)]
  }
  ux <- unique(x)
  return(ux[which.max(tabulate(match(x, ux)))])
}

impute_units <- function(col) {
  col <- col %>% str_remove("\\.0+")
  case_when(
    str_detect(col, regex("studio", ignore_case = TRUE)) ~ 0,
    str_detect(col, regex("(zero|0)[\\s,]?(bd|bed|br[^a-z])", ignore_case = TRUE)) ~ 0,
    str_detect(col, regex("(one|1)[\\s,]?(bd|bed|br[^a-z])", ignore_case = TRUE)) ~ 1,
    str_detect(col, regex("(two|2)[\\s,]?(bd|bed|br[^a-z])", ignore_case = TRUE)) ~ 2,
    str_detect(col, regex("(three|3)[\\s,]?(bd|bed|br[^a-z])", ignore_case = TRUE)) ~ 3,
    str_detect(col, regex("(four|4)[\\s,]?(bd|bed|br[^a-z])", ignore_case = TRUE)) ~ 4,
    str_detect(col, regex("(five|5)[\\s,]?(bd|bed|br[^a-z])", ignore_case = TRUE)) ~ 5,
    TRUE ~ NA_real_
  ) %>% return()
}

impute_baths <- function(col) {
  col <- col %>% str_remove("\\.0+")
  case_when(
    str_detect(col, regex("(zero|0)[\\s,]?(bath|ba[^a-z])", ignore_case = TRUE)) ~ 0,
    str_detect(col, regex("(0.5)[\\s,]?(bath|ba[^a-z])", ignore_case = TRUE)) ~ 0.5,
    str_detect(col, regex("(one|1)[\\s,]?(bath|ba[^a-z])", ignore_case = TRUE)) ~ 1,
    str_detect(col, regex("(1.5)[\\s,]?(bath|ba[^a-z])", ignore_case = TRUE)) ~ 1,
    str_detect(col, regex("(two|2)[\\s,]?(bath|ba[^a-z])", ignore_case = TRUE)) ~ 2,
    str_detect(col, regex("(2.5)[\\s,]?(bath|ba[^a-z])", ignore_case = TRUE)) ~ 2.5,
    str_detect(col, regex("(three|3)[\\s,]?(bath|ba[^a-z])", ignore_case = TRUE)) ~ 3,
    str_detect(col, regex("(four|4)[\\s,]?(bath|ba[^a-z])", ignore_case = TRUE)) ~ 4,
    str_detect(col, regex("(five|5)[\\s,]?(bath|ba[^a-z])", ignore_case = TRUE)) ~ 5,
    TRUE ~ NA_real_
  ) %>% return()
}

# ============================================================
# LOAD INPUT DATA
# ============================================================

altos_path <- p_product(cfg, "altos_clean")
xwalk_path <- p_product(cfg, "altos_parcel_xwalk")

logf("Loading cleaned Altos data from: ", altos_path, log_file = log_file)
altos <- fread(altos_path)
logf("  Loaded ", nrow(altos), " rows", log_file = log_file)

logf("Loading address crosswalk from: ", xwalk_path, log_file = log_file)
xwalk <- fread(xwalk_path)
logf("  Loaded ", nrow(xwalk), " rows", log_file = log_file)

# Filter crosswalk to unique parcel matches
xwalk_unique <- xwalk[num_parcels_matched == 1 & !is.na(PID)]
logf("  Unique parcel matches: ", nrow(xwalk_unique), " rows", log_file = log_file)

# ============================================================
# CREATE ALTOS VARIABLES
# ============================================================

logf("Creating Altos variables...", log_file = log_file)

# Time and price variables
altos[, ym := year + (month - 1) / 12]
altos[, price := as.numeric(price)]
altos[, beds_txt := impute_units(property_name)]
altos[, beds_imp := coalesce(beds, beds_txt)]
altos[, price_per_bed := as.numeric(price) / beds_imp]
altos[, address_id := .GRP, by = .(pm.house, pm.street, pm.streetSuf, pm.preDir, pm.sufDir, pm.zip)]
altos[, bed_address_id := .GRP, by = .(address_id, beds_imp)]

# ============================================================
# MERGE WITH CROSSWALK
# ============================================================

logf("Merging Altos with parcel crosswalk...", log_file = log_file)

altos_m <- merge(altos,
                xwalk_unique,
                by = "n_sn_ss_c",
                all.x = TRUE)

# Log merge statistics
match_stats <- altos_m[, .N, by = is.na(PID)][, per := N / sum(N)][]
logf("  Matched to parcel: ", match_stats[is.na == FALSE, N], " (",
     round(100 * match_stats[is.na == FALSE, per], 1), "%)", log_file = log_file)
logf("  No parcel match: ", match_stats[is.na == TRUE, N], " (",
     round(100 * match_stats[is.na == TRUE, per], 1), "%)", log_file = log_file)

# Create ID variables
altos_m[, PID_replace := coalesce(PID, address_id)]
altos_m[, bed_bath_pid_ID := .GRP, by = .(PID_replace, beds_imp, baths)]
altos_m[, beds_round := round(beds_imp, 0)]
altos_m[, bed_year_percentile := ntile(price, 1000), by = year]

# ============================================================
# CLEAN LISTINGS (REMOVE STALE LISTINGS)
# ============================================================

logf("Cleaning stale listings...", log_file = log_file)

# Track repeated listings
altos_m[, num_listings_ym := .N, by = .(bed_bath_pid_ID, ym)]
altos_m[, num_prices_ym := uniqueN(price), by = .(bed_bath_pid_ID, ym)]
altos_m[, num_prices_ym_round100 := uniqueN(100 * round(price / 100, 0)), by = .(bed_bath_pid_ID, ym)]
altos_m[, num_years_listed := uniqueN(year), by = .(bed_bath_pid_ID)]
altos_m[, num_years_listed_PID := uniqueN(year), by = .(PID_replace)]
altos_m[, listing_id := .GRP, by = .(bed_bath_pid_ID, property_name)]

# Identify repeated listings (same price as previous period)
altos_m <- altos_m[order(bed_bath_pid_ID, ym), ]
altos_m[, rep_listing := replace_na(price == lag(price), 0), by = listing_id]
altos_m[, cum_rep_listing := ifelse(rep_listing == 1, seq_len(.N), 0), by = .(listing_id, rleid(rep_listing))]

n_before <- nrow(altos_m)
# Remove listings that have been up > 10 periods without a price change
altos_m <- altos_m[cum_rep_listing <= 10]
n_after <- nrow(altos_m)

logf("  Removed ", n_before - n_after, " stale listing rows (",
     round(100 * (n_before - n_after) / n_before, 1), "%)", log_file = log_file)

# ============================================================
# CREATE ADDITIONAL VARIABLES
# ============================================================

altos_m[, price := as.numeric(price)]
altos_m[, num_prices := length(unique(price)), by = .(ym, bed_bath_pid_ID)]
altos_m[, num_prices_year := length(unique(price)), by = .(year, bed_bath_pid_ID)]
altos_m[, num_addresses := length(unique(address_id)), by = .(ym, bed_bath_pid_ID)]
altos_m[, change_price_year := price != lag(price), by = .(bed_bath_pid_ID, year)]

# ============================================================
# AGGREGATE TO BEDROOM LEVEL
# ============================================================

logf("Aggregating to bedroom level...", log_file = log_file)

altos_year_bedrooms <- altos_m[beds_imp <= 6 & baths <= 4, list(
  med_price = median(price, na.rm = TRUE),
  mean_price = mean(price, na.rm = TRUE),
  sd_price = sd(price, na.rm = TRUE),
  mad_price = mad(price, na.rm = TRUE),
  min_price = min(price, na.rm = TRUE),
  max_price = max(price, na.rm = TRUE),
  num_listings = .N,
  num_prices = uniqueN(price),
  num_price_changes = sum(change_price_year, na.rm = TRUE),
  street_address_lower_first = first(street_address_lower),
  address_id_first = first(address_id),
  beds_imp_first = first(beds_imp),
  baths_first = first(baths),
  month = first(month)
), by = .(PID, bed_bath_pid_ID, year)]

logf("  Bedroom-level panel: ", nrow(altos_year_bedrooms), " rows", log_file = log_file)

# ============================================================
# AGGREGATE TO BUILDING LEVEL
# ============================================================

logf("Aggregating to building level...", log_file = log_file)

altos_year_building <- altos_m[beds_imp <= 6 & baths <= 4, list(
  med_price = median(price, na.rm = TRUE),
  mean_price = mean(price, na.rm = TRUE),
  sd_price = sd(price, na.rm = TRUE),
  mad_price = mad(price, na.rm = TRUE),
  min_price = min(price, na.rm = TRUE),
  max_price = max(price, na.rm = TRUE),
  num_listings = .N,
  num_prices = uniqueN(price),
  num_price_changes = sum(change_price_year, na.rm = TRUE),
  street_address_lower_first = first(street_address_lower),
  address_id_first = first(address_id),
  month = first(month)
), by = .(PID, year)]

logf("  Building-level panel: ", nrow(altos_year_building), " rows", log_file = log_file)

# ============================================================
# ADD PANEL VARIABLES
# ============================================================

logf("Adding panel variables...", log_file = log_file)

# Year gaps for building panel
altos_year_building[order(year), year_gap := c(NA, diff(year)), by = PID]
altos_year_building[, num_years_in_sample := uniqueN(year[!is.na(med_price)]), by = PID]
altos_year_building[, max_year_gap := max(year_gap, na.rm = TRUE), by = PID]

# Year gaps for bedroom panel
altos_year_bedrooms[order(year), year_gap := c(NA, diff(year)), by = bed_bath_pid_ID]
altos_year_bedrooms[, num_years_in_sample := uniqueN(year[!is.na(med_price)]), by = bed_bath_pid_ID]
altos_year_bedrooms[, max_year_gap := max(year_gap, na.rm = TRUE), by = bed_bath_pid_ID]

# ============================================================
# ASSERTIONS
# ============================================================

logf("Running assertions...", log_file = log_file)

# Check bedroom panel has required columns
required_cols_bed <- c("PID", "bed_bath_pid_ID", "year", "med_price", "num_listings")
missing_cols_bed <- setdiff(required_cols_bed, names(altos_year_bedrooms))
if (length(missing_cols_bed) > 0) {
  stop("Missing required columns in bedroom panel: ", paste(missing_cols_bed, collapse = ", "))
}
logf("  Bedroom panel columns: PASS", log_file = log_file)

# Check building panel has required columns
required_cols_bldg <- c("PID", "year", "med_price", "num_listings")
missing_cols_bldg <- setdiff(required_cols_bldg, names(altos_year_building))
if (length(missing_cols_bldg) > 0) {
  stop("Missing required columns in building panel: ", paste(missing_cols_bldg, collapse = ", "))
}
logf("  Building panel columns: PASS", log_file = log_file)

# Log summary stats
logf("  Bedroom panel: ", altos_year_bedrooms[, uniqueN(PID)], " unique PIDs, ",
     altos_year_bedrooms[, uniqueN(year)], " years", log_file = log_file)
logf("  Building panel: ", altos_year_building[, uniqueN(PID)], " unique PIDs, ",
     altos_year_building[, uniqueN(year)], " years", log_file = log_file)

# ============================================================
# ENHANCED DIAGNOSTICS
# ============================================================

logf("", log_file = log_file)
logf("=== Enhanced Diagnostics ===", log_file = log_file)

# 1. Temporal coverage
year_counts <- altos_year_building[, .N, by = year][order(year)]
logf("Listings by year (building-level):", log_file = log_file)
for (i in 1:nrow(year_counts)) {
  logf("  ", year_counts$year[i], ": ", year_counts$N[i], " building-years", log_file = log_file)
}

# 2. Years per building
years_per_bldg <- altos_year_building[!is.na(PID), .(n_years = .N), by = PID]
logf("Years per building:", log_file = log_file)
logf("  Mean: ", round(mean(years_per_bldg$n_years), 2), log_file = log_file)
logf("  Median: ", median(years_per_bldg$n_years), log_file = log_file)
logf("  Max: ", max(years_per_bldg$n_years), log_file = log_file)
ypb_dist <- years_per_bldg[, .N, by = n_years][order(n_years)]
logf("  Distribution: ", paste(ypb_dist$n_years, "yrs=", ypb_dist$N, collapse = ", "), log_file = log_file)

# 3. Year gap analysis
year_gaps <- altos_year_building[!is.na(year_gap) & year_gap > 0, year_gap]
if (length(year_gaps) > 0) {
  gap_dist <- table(year_gaps)
  logf("Year gaps between observations:", log_file = log_file)
  logf("  Gap distribution: ", paste(names(gap_dist), "yr=", gap_dist, collapse = ", "), log_file = log_file)
  logf("  Buildings with gaps >2 years: ", altos_year_building[max_year_gap > 2 & !is.infinite(max_year_gap), uniqueN(PID)], log_file = log_file)
}

# 4. Listings per building-year distribution
listing_quantiles <- quantile(altos_year_building$num_listings, probs = c(0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1), na.rm = TRUE)
logf("Listings per building-year:", log_file = log_file)
logf("  Min: ", listing_quantiles[1], ", Q1: ", listing_quantiles[2],
    ", Median: ", listing_quantiles[3], ", Q3: ", listing_quantiles[4], log_file = log_file)
logf("  P90: ", listing_quantiles[5], ", P95: ", listing_quantiles[6],
    ", P99: ", listing_quantiles[7], ", Max: ", listing_quantiles[8], log_file = log_file)

# 5. Price distribution
price_quantiles <- quantile(altos_year_building$med_price, probs = c(0, 0.1, 0.25, 0.5, 0.75, 0.9, 1), na.rm = TRUE)
logf("Median price distribution (building-year):", log_file = log_file)
logf("  Min: $", round(price_quantiles[1]), ", P10: $", round(price_quantiles[2]),
    ", Q1: $", round(price_quantiles[3]), ", Median: $", round(price_quantiles[4]), log_file = log_file)
logf("  Q3: $", round(price_quantiles[5]), ", P90: $", round(price_quantiles[6]),
    ", Max: $", round(price_quantiles[7]), log_file = log_file)

# 6. Match coverage (how much data has parcel linkage)
n_with_pid <- altos_year_building[!is.na(PID) & PID != "", .N]
n_without_pid <- altos_year_building[is.na(PID) | PID == "", .N]
logf("Parcel linkage:", log_file = log_file)
logf("  With PID: ", n_with_pid, " (", round(100 * n_with_pid / nrow(altos_year_building), 1), "%)", log_file = log_file)
logf("  Without PID: ", n_without_pid, " (", round(100 * n_without_pid / nrow(altos_year_building), 1), "%)", log_file = log_file)

# 7. High-volume buildings (potential large apartments)
high_vol <- altos_year_building[num_listings >= 50, .(PID, year, num_listings, med_price)][order(-num_listings)]
if (nrow(high_vol) > 0) {
  logf("High-volume buildings (>=50 listings/year): ", nrow(high_vol), " building-years", log_file = log_file)
  logf("  Top 5:", log_file = log_file)
  for (i in 1:min(5, nrow(high_vol))) {
    logf("    PID ", high_vol$PID[i], " (", high_vol$year[i], "): ",
        high_vol$num_listings[i], " listings, $", round(high_vol$med_price[i]), " median", log_file = log_file)
  }
}

logf("", log_file = log_file)

# ============================================================
# WRITE OUTPUTS
# ============================================================

out_path_bedrooms <- p_product(cfg, "altos_year_bedrooms")
out_path_building <- p_product(cfg, "altos_year_building")

logf("Writing bedroom panel to: ", out_path_bedrooms, log_file = log_file)
dir.create(dirname(out_path_bedrooms), showWarnings = FALSE, recursive = TRUE)
fwrite(altos_year_bedrooms, out_path_bedrooms)
logf("  Wrote ", nrow(altos_year_bedrooms), " rows", log_file = log_file)

logf("Writing building panel to: ", out_path_building, log_file = log_file)
dir.create(dirname(out_path_building), showWarnings = FALSE, recursive = TRUE)
fwrite(altos_year_building, out_path_building)
logf("  Wrote ", nrow(altos_year_building), " rows", log_file = log_file)

logf("=== Finished make-altos-aggs.R ===", log_file = log_file)
