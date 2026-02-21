## ============================================================
## clean-infousa-addresses.R
## ============================================================
## Purpose: Clean and standardize InfoUSA household addresses
##          to match the pm.* naming convention used by other
##          address files for merging with parcel data.
##
## Inputs:
##   - cfg$inputs$infousa_raw (infousa/philly-infousa.csv)
##
## Outputs:
##   - cfg$products$infousa_clean (clean/infousa_clean.csv)
##
## Primary key: pm.uid (unique address ID based on address + zip + city + state)
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
})

# ---- Load config and set up logging ----
source("r/config.R")
source("r/lib/address_utils.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "clean-infousa-addresses.log")

logf("=== Starting clean-infousa-addresses.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ============================================================
# LOAD INPUT DATA
# ============================================================

input_path <- p_input(cfg, "infousa_raw")
logf("Loading raw InfoUSA data from: ", input_path, log_file = log_file)

philly_infousa_dt <- fread(input_path)
logf("  Loaded ", nrow(philly_infousa_dt), " rows, ", ncol(philly_infousa_dt), " columns", log_file = log_file)

# ============================================================
# ADDRESS STANDARDIZATION
# ============================================================

logf("Standardizing address columns to pm.* format...", log_file = log_file)

# Zip code - pad to 5 digits
# Add "_" prefix to pm.zip to ensure character type on read (e.g., "_19104")
philly_infousa_dt[, pm.zip := paste0("_", str_pad(zip, 5, "left", "0"))]
philly_infousa_dt[pm.zip == "_NA" | pm.zip == "_   NA", pm.zip := NA_character_]

# City - lowercase and standardize abbreviations
philly_infousa_dt[, pm.city := str_to_lower(city)]
philly_infousa_dt[, pm.city := fifelse(pm.city == "phila", "philadelphia", pm.city)]

# State - lowercase
philly_infousa_dt[, pm.state := str_to_lower(state)]

# Street name - lowercase
philly_infousa_dt[, pm.street := str_to_lower(street_name)]

# House number - lowercase, remove trailing letters, clean whitespace
philly_infousa_dt[, pm.house := str_to_lower(house_num) %>% str_remove_all("[a-z]$") %>% str_squish()]

# Street suffix - lowercase
philly_infousa_dt[, pm.streetSuf := str_to_lower(street_suffix)]

# Street pre-direction (N, S, E, W before street name)
philly_infousa_dt[, pm.preDir := str_to_lower(street_pre_dir)]

# Street post-direction (N, S, E, W after street name)
philly_infousa_dt[, pm.sufDir := str_to_lower(street_post_dir)]

# Normalize direction tokens so merge guardrails compare like-to-like.
# This avoids false mismatches such as "north" vs "n".
normalize_dir_token <- function(x) {
  y <- str_to_lower(str_squish(as.character(x)))
  y[y %in% c("", "na", "n/a", "null")] <- NA_character_
  y <- case_when(
    y %in% c("n", "north") ~ "n",
    y %in% c("s", "south") ~ "s",
    y %in% c("e", "east")  ~ "e",
    y %in% c("w", "west")  ~ "w",
    TRUE ~ y
  )
  y
}
philly_infousa_dt[, pm.preDir := normalize_dir_token(pm.preDir)]
philly_infousa_dt[, pm.sufDir := normalize_dir_token(pm.sufDir)]

# Concatenated direction (prefer pre, fallback to post)
philly_infousa_dt[, pm.dir_concat := coalesce(pm.preDir, pm.sufDir)]

# ============================================================
# Step 5b: Oracle-validated canonicalization (infousa)
# ============================================================

# Use reusable canonicalization function from address_utils.R
philly_infousa_dt <- canonicalize_parsed_addresses(
  philly_infousa_dt,
  cfg,
  source = "infousa",
  export_qa = TRUE,
  log_file = log_file
)

# Composite address string for matching
philly_infousa_dt[, n_sn_ss_c := str_squish(str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir)))]

# Replace literal "na" strings with actual NA
philly_infousa_dt[, n_sn_ss_c := fifelse(n_sn_ss_c == "na", NA_character_, n_sn_ss_c)]

# Create unique address ID
philly_infousa_dt[, pm.uid := .GRP, by = .(n_sn_ss_c, pm.zip, pm.city, pm.state)]

logf("  Created ", philly_infousa_dt[, uniqueN(pm.uid)], " unique address IDs", log_file = log_file)

# ============================================================
# ASSERTIONS
# ============================================================

logf("Running assertions...", log_file = log_file)

# Check required columns exist
required_cols <- c("pm.uid", "pm.house", "pm.street", "pm.zip", "n_sn_ss_c")
missing_cols <- setdiff(required_cols, names(philly_infousa_dt))
if (length(missing_cols) > 0) {
  stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
}
logf("  Required columns: PASS", log_file = log_file)

# Check pm.uid was created properly (should have values)
n_uid <- philly_infousa_dt[, uniqueN(pm.uid)]
if (n_uid == 0) {
  stop("pm.uid creation failed - no unique IDs generated")
}
logf("  pm.uid creation: PASS (", n_uid, " unique addresses)", log_file = log_file)

# Log address parsing stats
n_missing_address <- philly_infousa_dt[is.na(n_sn_ss_c), .N]
logf("  Rows with missing composite address: ", n_missing_address,
     " (", round(100 * n_missing_address / nrow(philly_infousa_dt), 2), "%)", log_file = log_file)

# ============================================================
# ENHANCED DIAGNOSTICS: Address Parsing Quality
# ============================================================

logf("", log_file = log_file)
logf("=== Address Parsing Diagnostics ===", log_file = log_file)

# 1. Component parsing success rates
n_total <- nrow(philly_infousa_dt)
logf("Parsing success rates:", log_file = log_file)
logf("  pm.house: ", sum(!is.na(philly_infousa_dt$pm.house) & philly_infousa_dt$pm.house != ""), " (",
    round(100 * sum(!is.na(philly_infousa_dt$pm.house) & philly_infousa_dt$pm.house != "") / n_total, 1), "%)", log_file = log_file)
logf("  pm.street: ", sum(!is.na(philly_infousa_dt$pm.street) & philly_infousa_dt$pm.street != ""), " (",
    round(100 * sum(!is.na(philly_infousa_dt$pm.street) & philly_infousa_dt$pm.street != "") / n_total, 1), "%)", log_file = log_file)
logf("  pm.streetSuf: ", sum(!is.na(philly_infousa_dt$pm.streetSuf) & philly_infousa_dt$pm.streetSuf != ""), " (",
    round(100 * sum(!is.na(philly_infousa_dt$pm.streetSuf) & philly_infousa_dt$pm.streetSuf != "") / n_total, 1), "%)", log_file = log_file)
logf("  pm.zip (w/ _ prefix): ", sum(!is.na(philly_infousa_dt$pm.zip) & philly_infousa_dt$pm.zip != ""), " (",
    round(100 * sum(!is.na(philly_infousa_dt$pm.zip) & philly_infousa_dt$pm.zip != "") / n_total, 1), "%)", log_file = log_file)
logf("  n_sn_ss_c (composite): ", sum(!is.na(philly_infousa_dt$n_sn_ss_c) & philly_infousa_dt$n_sn_ss_c != ""), " (",
    round(100 * sum(!is.na(philly_infousa_dt$n_sn_ss_c) & philly_infousa_dt$n_sn_ss_c != "") / n_total, 1), "%)", log_file = log_file)

# 2. Unique addresses
n_unique_addrs <- uniqueN(philly_infousa_dt$n_sn_ss_c)
logf("Address deduplication:", log_file = log_file)
logf("  Total records: ", n_total, log_file = log_file)
logf("  Unique addresses: ", n_unique_addrs, log_file = log_file)
logf("  Avg records per address: ", round(n_total / n_unique_addrs, 1), log_file = log_file)

# 3. Top 20 streets (by record count)
top_streets <- philly_infousa_dt[!is.na(pm.street) & pm.street != "", .N, by = pm.street][order(-N)][1:20]
logf("Top 20 streets by record count:", log_file = log_file)
for (i in 1:nrow(top_streets)) {
  logf("  ", i, ". ", top_streets$pm.street[i], ": ", top_streets$N[i], " records", log_file = log_file)
}

# 4. Geographic coverage (zip codes)
n_zips <- uniqueN(philly_infousa_dt$pm.zip[!is.na(philly_infousa_dt$pm.zip) & philly_infousa_dt$pm.zip != ""])
top_zips <- philly_infousa_dt[!is.na(pm.zip) & pm.zip != "", .N, by = pm.zip][order(-N)][1:10]
logf("Geographic coverage:", log_file = log_file)
logf("  Unique zip codes: ", n_zips, log_file = log_file)
logf("  Top 10 zips: ", paste(str_remove(top_zips$pm.zip, "^_"), "(", top_zips$N, ")", collapse = ", "), log_file = log_file)

# 5. Temporal coverage (if year column exists)
if ("file_year" %in% names(philly_infousa_dt)) {
  year_dist <- philly_infousa_dt[, .N, by = file_year][order(file_year)]
  logf("Records by file year:", log_file = log_file)
  for (i in 1:nrow(year_dist)) {
    logf("  ", year_dist$file_year[i], ": ", year_dist$N[i], " records", log_file = log_file)
  }
}

# 6. Records with invalid/missing addresses
n_invalid <- sum(is.na(philly_infousa_dt$n_sn_ss_c) | philly_infousa_dt$n_sn_ss_c == "" | philly_infousa_dt$n_sn_ss_c == "na")
if (n_invalid > 0) {
  logf("Records with invalid/missing composite address: ", n_invalid, " (",
      round(100 * n_invalid / n_total, 2), "%)", log_file = log_file)
}

logf("", log_file = log_file)

# ============================================================
# WRITE OUTPUT
# ============================================================

out_path <- p_product(cfg, "infousa_clean")
logf("Writing output to: ", out_path, log_file = log_file)

# Ensure output directory exists
dir.create(dirname(out_path), showWarnings = FALSE, recursive = TRUE)

fwrite(philly_infousa_dt, out_path)
logf("  Wrote ", nrow(philly_infousa_dt), " rows, ", ncol(philly_infousa_dt), " columns", log_file = log_file)

logf("=== Finished clean-infousa-addresses.R ===", log_file = log_file)
