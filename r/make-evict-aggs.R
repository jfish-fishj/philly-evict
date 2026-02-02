## ============================================================
## make-evict-aggs.R
## ============================================================
## Purpose: Aggregate eviction filings to parcel-year and zip-year levels
##
## Inputs:
##   - cfg$products$evictions_clean (clean/evictions_clean.csv)
##   - cfg$products$xwalk_evictions_to_parcel (xwalks/xwalk_evictions_to_parcel.csv)
##
## Outputs:
##   - cfg$products$evict_pid_year (panels/evict_pid_year.csv)
##   - cfg$products$evict_zip_year (panels/evict_zip_year.csv)
##
## Primary keys:
##   - evict_pid_year: (parcel_number, year)
##   - evict_zip_year: (pm.zip, year)
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
})

# ---- Load config and set up logging ----
source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "make-evict-aggs.log")

logf("=== Starting make-evict-aggs.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Helper functions ----
Mode <- function(x, na.rm = FALSE) {
  if (na.rm) {
    x <- x[!is.na(x)]
  }
  if (length(x) == 0) return(NA)
  ux <- unique(x)
  return(ux[which.max(tabulate(match(x, ux)))])
}

# ---- Load input data ----
logf("Loading input data...", log_file = log_file)

# Load cleaned eviction data
evict_path <- p_product(cfg, "evictions_clean")
logf("  Loading evictions: ", evict_path, log_file = log_file)
evict <- fread(evict_path)
logf("  Loaded ", nrow(evict), " eviction records", log_file = log_file)

# Load address-to-parcel crosswalk
xwalk_path <- p_product(cfg, "xwalk_evictions_to_parcel")
logf("  Loading crosswalk: ", xwalk_path, log_file = log_file)

if (!file.exists(xwalk_path)) {
  logf("WARNING: Crosswalk file not found. Run make-address-parcel-xwalk.R first.", log_file = log_file)
  logf("Proceeding without parcel linkage - will aggregate by address only.", log_file = log_file)
  xwalk <- NULL
} else {
  xwalk <- fread(xwalk_path)
  logf("  Loaded ", nrow(xwalk), " crosswalk records", log_file = log_file)
}

# ---- Create filing date variables ----
logf("Creating date variables...", log_file = log_file)

# Parse filing date components
evict[, day := as.numeric(str_sub(d_filing, start = -2, end = -1))]
evict[, ym := year + (month - 1) / 12]
evict[, ymd := year + (month - 1) / 12 + (day - 1) / 365]
evict[, year_quarter := paste0(year, "Q", ceiling(month / 3))]

# Parse rent if available (often missing or zero)
evict[, claimed_rent := as.numeric(ongoing_rent)]
evict[claimed_rent <= 0 | claimed_rent > 10000, claimed_rent := NA_real_]

# Create address-based ID for records without parcel match
evict[, address_id := paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir, pm.zip, sep = "_")]

logf("  Date variables created", log_file = log_file)

# ---- Merge with crosswalk to get parcel IDs ----
logf("Merging with address-parcel crosswalk...", log_file = log_file)

if (!is.null(xwalk)) {
  # The crosswalk links source_address_id to parcel_number
  # Need to match on the address key used in the crosswalk

  # First check what key columns are in the crosswalk
  xwalk_cols <- names(xwalk)
  logf("  Crosswalk columns: ", paste(xwalk_cols, collapse = ", "), log_file = log_file)

  # Merge on n_sn_ss_c (the composite address key)
  if ("n_sn_ss_c" %in% xwalk_cols && "n_sn_ss_c" %in% names(evict)) {
    # Deduplicate crosswalk: take best match (highest score) per address
    xwalk_unique <- xwalk[order(-match_score)][!duplicated(n_sn_ss_c)]
    logf("  Deduplicated crosswalk: ", nrow(xwalk_unique), " unique addresses", log_file = log_file)

    evict_m <- merge(
      evict,
      xwalk_unique[, .(n_sn_ss_c, parcel_number, match_tier, match_score)],
      by = "n_sn_ss_c",
      all.x = TRUE
    )
  } else if ("source_address_id" %in% xwalk_cols) {
    # Alternative: match on source_address_id
    evict_m <- merge(
      evict,
      xwalk[, .(source_address_id, parcel_number, match_tier, match_score)],
      by.x = "pm.uid",
      by.y = "source_address_id",
      all.x = TRUE
    )
  } else {
    logf("WARNING: Cannot determine crosswalk join key. Using address_id fallback.", log_file = log_file)
    evict_m <- copy(evict)
    evict_m[, parcel_number := NA_character_]
  }

  # Report match rate
  n_matched <- evict_m[!is.na(parcel_number), .N]
  match_rate <- round(100 * n_matched / nrow(evict_m), 1)
  logf("  Matched to parcel: ", n_matched, " (", match_rate, "%)", log_file = log_file)
} else {
  evict_m <- copy(evict)
  evict_m[, parcel_number := NA_character_]
  logf("  No crosswalk - parcel_number set to NA", log_file = log_file)
}

# Create combined ID (parcel if available, else address)
evict_m[, pid_or_address := coalesce(as.character(parcel_number), address_id)]

# ---- Aggregate to parcel-year level ----
logf("Aggregating to parcel-year level...", log_file = log_file)

evict_pid_year <- evict_m[!is.na(parcel_number), .(
  n_filings = .N,
  n_cases = uniqueN(id),
  med_claimed_rent = median(claimed_rent, na.rm = TRUE),
  mean_claimed_rent = mean(claimed_rent, na.rm = TRUE),
  n_with_rent = sum(!is.na(claimed_rent)),
  first_filing_date = min(d_filing, na.rm = TRUE),
  last_filing_date = max(d_filing, na.rm = TRUE),
  n_unique_addresses = uniqueN(n_sn_ss_c),
  modal_zip = Mode(pm.zip, na.rm = TRUE)
), by = .(parcel_number, year)]

logf("  Created evict_pid_year: ", nrow(evict_pid_year), " rows", log_file = log_file)
logf("  Unique parcels: ", uniqueN(evict_pid_year$parcel_number), log_file = log_file)
logf("  Year range: ", min(evict_pid_year$year, na.rm = TRUE), " - ", max(evict_pid_year$year, na.rm = TRUE), log_file = log_file)

# ---- ENHANCED DIAGNOSTICS: Parcel-Year Panel ----
logf("  --- Enhanced Diagnostics ---", log_file = log_file)

# 1. Temporal coverage
year_counts <- evict_pid_year[, .N, by = year][order(year)]
logf("  Filings by year:", log_file = log_file)
for (i in 1:nrow(year_counts)) {
  logf("    ", year_counts$year[i], ": ", year_counts$N[i], " parcel-years", log_file = log_file)
}

# 2. Average years per parcel
years_per_parcel <- evict_pid_year[, .(n_years = .N), by = parcel_number]
logf("  Years per parcel:", log_file = log_file)
logf("    Mean: ", round(mean(years_per_parcel$n_years), 2), log_file = log_file)
logf("    Median: ", median(years_per_parcel$n_years), log_file = log_file)
logf("    Max: ", max(years_per_parcel$n_years), log_file = log_file)
ypp_dist <- years_per_parcel[, .N, by = n_years][order(n_years)]
logf("    Distribution: ", paste(ypp_dist$n_years, "yrs=", ypp_dist$N, collapse = ", "), log_file = log_file)

# 3. Filings per parcel-year distribution
filing_quantiles <- quantile(evict_pid_year$n_filings, probs = c(0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1))
logf("  Filings per parcel-year:", log_file = log_file)
logf("    Min: ", filing_quantiles[1], ", Q1: ", filing_quantiles[2],
    ", Median: ", filing_quantiles[3], ", Q3: ", filing_quantiles[4], log_file = log_file)
logf("    P90: ", filing_quantiles[5], ", P95: ", filing_quantiles[6],
    ", P99: ", filing_quantiles[7], ", Max: ", filing_quantiles[8], log_file = log_file)

# 4. High-frequency parcels (potential large landlords or data issues)
high_freq <- evict_pid_year[n_filings >= 10, .(parcel_number, year, n_filings)][order(-n_filings)]
if (nrow(high_freq) > 0) {
  logf("  High-frequency parcels (>=10 filings/year): ", nrow(high_freq), " parcel-years", log_file = log_file)
  logf("    Top 5:", log_file = log_file)
  for (i in 1:min(5, nrow(high_freq))) {
    logf("      ", high_freq$parcel_number[i], " (", high_freq$year[i], "): ",
        high_freq$n_filings[i], " filings", log_file = log_file)
  }
}

# 5. Match tier coverage (if match_tier available)
if ("match_tier" %in% names(evict_m)) {
  tier_coverage <- evict_m[!is.na(parcel_number), .N, by = match_tier][, pct := round(100 * N / sum(N), 1)]
  logf("  Match tier coverage:", log_file = log_file)
  for (i in 1:nrow(tier_coverage)) {
    logf("    ", tier_coverage$match_tier[i], ": ", tier_coverage$N[i],
        " (", tier_coverage$pct[i], "%)", log_file = log_file)
  }
}

# ---- Aggregate to zip-year level ----
logf("Aggregating to zip-year level...", log_file = log_file)

evict_zip_year <- evict_m[!is.na(pm.zip) & nzchar(pm.zip), .(
  n_filings = .N,
  n_cases = uniqueN(id),
  n_parcels = uniqueN(parcel_number[!is.na(parcel_number)]),
  n_addresses = uniqueN(n_sn_ss_c),
  med_claimed_rent = median(claimed_rent, na.rm = TRUE),
  mean_claimed_rent = mean(claimed_rent, na.rm = TRUE),
  n_with_rent = sum(!is.na(claimed_rent))
), by = .(pm.zip, year)]

logf("  Created evict_zip_year: ", nrow(evict_zip_year), " rows", log_file = log_file)
logf("  Unique zips: ", uniqueN(evict_zip_year$pm.zip), log_file = log_file)

# ---- Assertions ----
logf("Running assertions...", log_file = log_file)

# Check evict_pid_year uniqueness
n_pid_year <- nrow(evict_pid_year)
n_unique_pid_year <- uniqueN(evict_pid_year, by = c("parcel_number", "year"))
if (n_pid_year != n_unique_pid_year) {
  stop("ASSERTION FAILED: evict_pid_year is not unique on (parcel_number, year)")
}
logf("  evict_pid_year unique on (parcel_number, year): OK", log_file = log_file)

# Check evict_zip_year uniqueness
n_zip_year <- nrow(evict_zip_year)
n_unique_zip_year <- uniqueN(evict_zip_year, by = c("pm.zip", "year"))
if (n_zip_year != n_unique_zip_year) {
  stop("ASSERTION FAILED: evict_zip_year is not unique on (pm.zip, year)")
}
logf("  evict_zip_year unique on (pm.zip, year): OK", log_file = log_file)

# ---- Write outputs ----
logf("Writing outputs...", log_file = log_file)

# Write parcel-year aggregation
out_pid_year <- p_product(cfg, "evict_pid_year")
fwrite(evict_pid_year, out_pid_year)
logf("  Wrote evict_pid_year: ", nrow(evict_pid_year), " rows to ", out_pid_year, log_file = log_file)

# Write zip-year aggregation
out_zip_year <- p_product(cfg, "evict_zip_year")
fwrite(evict_zip_year, out_zip_year)
logf("  Wrote evict_zip_year: ", nrow(evict_zip_year), " rows to ", out_zip_year, log_file = log_file)

logf("=== Finished make-evict-aggs.R ===", log_file = log_file)
