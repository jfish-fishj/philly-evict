## ============================================================
## make-altos-aggs.R
## ============================================================
## Purpose: Aggregate Altos rental listing data to bedroom-level
##          and building-level panels by year.
##
## Inputs:
##   - cfg$products$altos_clean (clean/altos_clean.csv)
##   - cfg$products$xwalk_altos_to_parcel (xwalks/xwalk_altos_to_parcel.csv)
##
## Outputs:
##   - cfg$products$altos_year_bedrooms (panels/altos_year_bedrooms.csv)
##   - cfg$products$altos_year_building (panels/altos_year_building.csv)
##   - cfg$products$altos_pid_year_bedbin (panels/altos_pid_year_bedbin.csv)
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
source("r/helper-functions.R")

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

ModeChar <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (!length(x)) return(NA_character_)
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

normalize_pid_local <- function(x) {
  y <- as.character(x)
  y[!nzchar(y) | y %in% c("NA", "NaN")] <- NA_character_
  out <- stringr::str_pad(y, width = 9L, side = "left", pad = "0")
  out[is.na(y)] <- NA_character_
  out
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
xwalk_path <- p_product(cfg, "xwalk_altos_to_parcel")

logf("Loading cleaned Altos data from: ", altos_path, log_file = log_file)
altos <- fread(altos_path)
logf("  Loaded ", nrow(altos), " rows", log_file = log_file)

if (!file.exists(xwalk_path)) {
  stop("Missing required Altos winner crosswalk at ", xwalk_path,
       ". Re-run r/make-address-parcel-xwalk.R first.")
}

logf("Loading address crosswalk from: ", xwalk_path, log_file = log_file)
xwalk <- fread(xwalk_path)
logf("  Loaded ", nrow(xwalk), " rows", log_file = log_file)

assert_has_cols(
  xwalk,
  c("source_address_id", "anchor_pid", "parcel_number", "xwalk_status", "link_type", "ownership_unsafe"),
  "xwalk_altos_to_parcel"
)
assert_unique(xwalk, "source_address_id", "xwalk_altos_to_parcel (source_address_id)")
logf("  Crosswalk mode: Phase 1 winner xwalk", log_file = log_file)

xwalk_unique <- xwalk[
  ,
  .(
    source_address_id = as.character(source_address_id),
    PID = normalize_pid_local(fcoalesce(anchor_pid, parcel_number)),
    altos_parcel_number_raw = normalize_pid_local(parcel_number),
    altos_anchor_pid = normalize_pid_local(anchor_pid),
    altos_link_id = as.character(link_id),
    altos_link_type = as.character(link_type),
    altos_xwalk_status = as.character(xwalk_status),
    altos_ownership_unsafe = as.logical(fcoalesce(ownership_unsafe, FALSE))
  )
]
logf("  Winner xwalk rows (one per source address): ", nrow(xwalk_unique), log_file = log_file)

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
assert_has_cols(altos, "pm.uid", "altos_clean")
altos[, source_address_id := as.character(pm.uid)]
if (!"PID" %in% names(altos)) altos[, PID := NA_character_]
if (!"altos_parcel_number_raw" %in% names(altos)) altos[, altos_parcel_number_raw := NA_character_]
if (!"altos_anchor_pid" %in% names(altos)) altos[, altos_anchor_pid := NA_character_]
if (!"altos_link_id" %in% names(altos)) altos[, altos_link_id := NA_character_]
if (!"altos_link_type" %in% names(altos)) altos[, altos_link_type := NA_character_]
if (!"altos_xwalk_status" %in% names(altos)) altos[, altos_xwalk_status := NA_character_]
if (!"altos_ownership_unsafe" %in% names(altos)) altos[, altos_ownership_unsafe := FALSE]
xwalk_join <- copy(xwalk_unique)
setkey(xwalk_join, source_address_id)
altos[xwalk_join, on = "source_address_id", `:=`(
  PID = i.PID,
  altos_parcel_number_raw = i.altos_parcel_number_raw,
  altos_anchor_pid = i.altos_anchor_pid,
  altos_link_id = i.altos_link_id,
  altos_link_type = i.altos_link_type,
  altos_xwalk_status = i.altos_xwalk_status,
  altos_ownership_unsafe = i.altos_ownership_unsafe
)]
altos_m <- altos
altos_m[, PID := fifelse(is.na(PID) | !nzchar(as.character(PID)), NA_character_, normalize_pid_local(PID))]

# Log merge statistics
match_stats <- altos_m[, .N, by = is.na(PID)][, per := N / sum(N)][]
n_matched <- match_stats[is.na == FALSE, sum(N)]
p_matched <- match_stats[is.na == FALSE, sum(per)]
n_unmatched <- match_stats[is.na == TRUE, sum(N)]
p_unmatched <- match_stats[is.na == TRUE, sum(per)]
if (!is.finite(n_matched)) n_matched <- 0L
if (!is.finite(p_matched)) p_matched <- 0
if (!is.finite(n_unmatched)) n_unmatched <- 0L
if (!is.finite(p_unmatched)) p_unmatched <- 0
logf("  Matched to parcel: ", n_matched, " (", round(100 * p_matched, 1), "%)", log_file = log_file)
logf("  No parcel match: ", n_unmatched, " (", round(100 * p_unmatched, 1), "%)", log_file = log_file)

if ("altos_xwalk_status" %in% names(altos_m)) {
  xwalk_mix <- altos_m[!is.na(PID), .N, by = altos_xwalk_status][order(-N)]
  logf("  Altos xwalk status mix among linked rows:", log_file = log_file)
  for (i in seq_len(nrow(xwalk_mix))) {
    logf("    ", xwalk_mix$altos_xwalk_status[i], ": ", xwalk_mix$N[i], log_file = log_file)
  }
}

# Create ID variables
altos_m[, PID_replace := fcoalesce(as.character(PID), as.character(address_id))]
altos_m[, bed_bath_pid_ID := .GRP, by = .(PID_replace, beds_imp, baths)]
altos_m[, beds_round := round(beds_imp, 0)]
altos_m[, bed_year_percentile := ntile(price, 1000), by = year]
altos_m[, bed_bin := altos_bed_bin(beds_imp)]

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
# AGGREGATE TO PID x YEAR x BED_BIN (PHASE 1 STANDARDIZATION INPUT)
# ============================================================

logf("Aggregating to PID x year x bed_bin...", log_file = log_file)

altos_pid_year_bedbin <- altos_m[
  !is.na(PID) &
    !is.na(bed_bin) &
    beds_imp <= 6 &
    baths <= 4 &
    is.finite(price) &
    price > 0 &
    (is.na(per_room_listing) | per_room_listing == FALSE),
  .(
    med_rent_cell = median(price, na.rm = TRUE),
    mean_rent_cell = mean(price, na.rm = TRUE),
    p25_rent_cell = as.numeric(quantile(price, probs = 0.25, na.rm = TRUE, type = 7)),
    p75_rent_cell = as.numeric(quantile(price, probs = 0.75, na.rm = TRUE, type = 7)),
    n_listing_rows_cell = .N,
    n_price_points_cell = uniqueN(price),
    n_price_changes_cell = sum(change_price_year, na.rm = TRUE),
    altos_any_ownership_unsafe = any(as.logical(altos_ownership_unsafe), na.rm = TRUE),
    altos_any_condo_group_link = any(altos_link_type == "condo_group", na.rm = TRUE),
    altos_link_type_mode = ModeChar(altos_link_type),
    altos_xwalk_status_mode = ModeChar(altos_xwalk_status),
    altos_n_link_ids = uniqueN(altos_link_id[!is.na(altos_link_id) & nzchar(altos_link_id)])
  ),
  by = .(PID, year, bed_bin)
]

logf("  PID-year-bed_bin panel: ", nrow(altos_pid_year_bedbin), " rows", log_file = log_file)

# ============================================================
# AGGREGATE TO BEDROOM LEVEL
# ============================================================

logf("Aggregating to bedroom level...", log_file = log_file)

altos_year_bedrooms <- altos_m[beds_imp <= 6 & baths <= 4 &
    (is.na(per_room_listing) | per_room_listing == FALSE), list(
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
  month = first(month),
  altos_any_ownership_unsafe = any(as.logical(altos_ownership_unsafe), na.rm = TRUE),
  altos_any_condo_group_link = any(altos_link_type == "condo_group", na.rm = TRUE),
  altos_link_type_mode = ModeChar(altos_link_type),
  altos_xwalk_status_mode = ModeChar(altos_xwalk_status)
), by = .(PID, bed_bath_pid_ID, year)]

logf("  Bedroom-level panel: ", nrow(altos_year_bedrooms), " rows", log_file = log_file)

# ============================================================
# AGGREGATE TO BUILDING LEVEL
# ============================================================

logf("Aggregating to building level...", log_file = log_file)

altos_year_building <- altos_m[beds_imp <= 6 & baths <= 4 &
    (is.na(per_room_listing) | per_room_listing == FALSE), list(
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
  month = first(month),
  altos_any_ownership_unsafe = any(as.logical(altos_ownership_unsafe), na.rm = TRUE),
  altos_any_condo_group_link = any(altos_link_type == "condo_group", na.rm = TRUE),
  altos_link_type_mode = ModeChar(altos_link_type),
  altos_xwalk_status_mode = ModeChar(altos_xwalk_status),
  altos_n_link_ids = uniqueN(altos_link_id[!is.na(altos_link_id) & nzchar(altos_link_id)])
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

# Check key uniqueness for bedroom panel: (PID, bed_bath_pid_ID, year)
n_bed_rows <- nrow(altos_year_bedrooms)
n_bed_unique <- altos_year_bedrooms[, uniqueN(.SD), .SDcols = c("PID", "bed_bath_pid_ID", "year")]
if (n_bed_rows != n_bed_unique) {
  logf("WARNING: Bedroom panel not unique on (PID, bed_bath_pid_ID, year): ",
       n_bed_rows, " rows vs ", n_bed_unique, " unique keys", log_file = log_file)
} else {
  logf("  Bedroom panel unique on (PID, bed_bath_pid_ID, year): PASS", log_file = log_file)
}

# Check key uniqueness for building panel: (PID, year)
n_bldg_rows <- nrow(altos_year_building)
n_bldg_unique <- altos_year_building[, uniqueN(.SD), .SDcols = c("PID", "year")]
if (n_bldg_rows != n_bldg_unique) {
  logf("WARNING: Building panel not unique on (PID, year): ",
       n_bldg_rows, " rows vs ", n_bldg_unique, " unique keys", log_file = log_file)
} else {
  logf("  Building panel unique on (PID, year): PASS", log_file = log_file)
}

# Check key uniqueness for bed-bin panel: (PID, year, bed_bin)
n_bedbin_rows <- nrow(altos_pid_year_bedbin)
n_bedbin_unique <- altos_pid_year_bedbin[, uniqueN(.SD), .SDcols = c("PID", "year", "bed_bin")]
if (n_bedbin_rows != n_bedbin_unique) {
  stop("ASSERTION FAILED: altos_pid_year_bedbin is not unique on (PID, year, bed_bin)")
} else {
  logf("  Bed-bin panel unique on (PID, year, bed_bin): PASS", log_file = log_file)
}

if (altos_pid_year_bedbin[, any(!(bed_bin %in% c("studio", "1br", "2br", "3plus")))] ) {
  stop("ASSERTION FAILED: altos_pid_year_bedbin has invalid bed_bin values")
}
logf("  Bed-bin values valid: PASS", log_file = log_file)

# Log summary stats
logf("  Bedroom panel: ", altos_year_bedrooms[, uniqueN(PID)], " unique PIDs, ",
     altos_year_bedrooms[, uniqueN(year)], " years", log_file = log_file)
logf("  Building panel: ", altos_year_building[, uniqueN(PID)], " unique PIDs, ",
     altos_year_building[, uniqueN(year)], " years", log_file = log_file)
logf("  Bed-bin panel: ", altos_pid_year_bedbin[, uniqueN(PID)], " unique PIDs, ",
     altos_pid_year_bedbin[, uniqueN(year)], " years", log_file = log_file)

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
# BUILD altos_unit_year: enriched bedroom panel for event studies
# ============================================================

logf("Building altos_unit_year panel...", log_file = log_file)

# Modal ZIP per bed_bath_pid_ID (from matched rows only)
zip_lookup_unit <- altos_m[
  !is.na(bed_bath_pid_ID) & !is.na(pm.zip) & nzchar(as.character(pm.zip)),
  .(pm.zip = ModeChar(as.character(pm.zip))),
  by = bed_bath_pid_ID
]

altos_unit_year <- copy(altos_year_bedrooms)
altos_unit_year <- merge(altos_unit_year, zip_lookup_unit, by = "bed_bath_pid_ID", all.x = TRUE)

altos_unit_year[, log_med_price := log(med_price)]
altos_unit_year[, price_spread := max_price - min_price]
# Flag cells where within-year max/min price ratio exceeds 2x.
# This catches cases like a 5BR listed simultaneously at $2,000 and $6,000
# (two different products mis-labeled as the same typology), while tolerating
# normal floor/size variation (~1.1-1.3x). Use this flag rather than dropping
# outright so downstream scripts can choose their own tolerance.
altos_unit_year[, price_ratio := fifelse(min_price > 0, max_price / min_price, NA_real_)]
altos_unit_year[, high_price_dispersion := !is.na(price_ratio) & price_ratio > 2]
setorder(altos_unit_year, bed_bath_pid_ID, year)
altos_unit_year[, change_log_med_price := log_med_price - shift(log_med_price, 1L), by = bed_bath_pid_ID]

# Rename beds_imp_first/baths_first for convenience in downstream joins
altos_unit_year[, beds_imp := beds_imp_first]
altos_unit_year[, baths    := baths_first]

n_uy     <- nrow(altos_unit_year)
n_uy_key <- altos_unit_year[, uniqueN(.SD), .SDcols = c("bed_bath_pid_ID", "year")]
if (n_uy != n_uy_key) stop("ASSERTION FAILED: altos_unit_year not unique on (bed_bath_pid_ID, year)")
logf("  altos_unit_year: ", n_uy, " rows, ",
     altos_unit_year[, uniqueN(bed_bath_pid_ID)], " units, ",
     altos_unit_year[!is.na(PID), uniqueN(PID)], " PIDs",
     log_file = log_file)

# ============================================================
# WRITE OUTPUTS
# ============================================================

out_path_bedrooms <- p_product(cfg, "altos_year_bedrooms")
out_path_building <- p_product(cfg, "altos_year_building")
out_path_bedbin <- p_product(cfg, "altos_pid_year_bedbin")

logf("Writing bedroom panel to: ", out_path_bedrooms, log_file = log_file)
dir.create(dirname(out_path_bedrooms), showWarnings = FALSE, recursive = TRUE)
fwrite(altos_year_bedrooms, out_path_bedrooms)
logf("  Wrote ", nrow(altos_year_bedrooms), " rows", log_file = log_file)

logf("Writing building panel to: ", out_path_building, log_file = log_file)
dir.create(dirname(out_path_building), showWarnings = FALSE, recursive = TRUE)
fwrite(altos_year_building, out_path_building)
logf("  Wrote ", nrow(altos_year_building), " rows", log_file = log_file)

logf("Writing PID-year-bed_bin panel to: ", out_path_bedbin, log_file = log_file)
dir.create(dirname(out_path_bedbin), showWarnings = FALSE, recursive = TRUE)
fwrite(altos_pid_year_bedbin, out_path_bedbin)
logf("  Wrote ", nrow(altos_pid_year_bedbin), " rows", log_file = log_file)

out_path_unit_year <- p_product(cfg, "altos_unit_year")
logf("Writing altos_unit_year to: ", out_path_unit_year, log_file = log_file)
dir.create(dirname(out_path_unit_year), showWarnings = FALSE, recursive = TRUE)
fwrite(altos_unit_year, out_path_unit_year)
logf("  Wrote ", nrow(altos_unit_year), " rows", log_file = log_file)

logf("=== Finished make-altos-aggs.R ===", log_file = log_file)
