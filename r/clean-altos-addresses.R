## ============================================================
## clean-altos-addresses.R
## ============================================================
## Purpose: Clean and standardize Altos rental listing addresses
##          for merging with parcel data.
##
## Inputs:
##   - cfg$inputs$altos_metro_raw (listings/philadelphia_metro_all_years.csv)
##
## Outputs:
##   - cfg$products$altos_clean (clean/altos_clean.csv)
##
## Primary key: listing_id (unique listing row)
## Secondary key: pm.uid (unique address parsing ID)
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(postmastr)
  library(bit64)
  library(glue)
})

# ---- Load config and set up logging ----
source("r/config.R")
source("r/lib/address_utils.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "clean-altos-addresses.log")

logf("=== Starting clean-altos-addresses.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ============================================================
# HELPER FUNCTIONS (postmastr-specific, not in address_utils.R)
# ============================================================

# Standardize street name between WOA and postmastr
standardize_st_name <- function(street_name) {
  addys <- tibble(
    sn = street_name,
    pm.id = seq(1, length(street_name)),
    pm.uid = seq(1, length(street_name))
  )
  st_tibble <- addys %>%
    pm_identify(var = sn) %>%
    pm_prep(var = "sn", type = "street") %>%
    pm_street_std(var = "pm.address") %>%
    mutate(pm.street = pm.address) %>%
    pm_replace(source = addys) %>%
    mutate(
      pm.street = coalesce(pm.street, sn) %>% str_to_lower(),
      pm.street = fifelse(pm.street == "center", "centre", pm.street),
      pm.street = str_replace_all(pm.street, "saint", "st"),
      pm.street = str_replace_all(pm.street, "farm to market road", "farm to market")
    )
  return(st_tibble$pm.street)
}

# ============================================================
# LOAD INPUT DATA
# ============================================================

input_path <- p_input(cfg, "altos_metro_raw")
logf("Loading raw Altos data from: ", input_path, log_file = log_file)

altos <- fread(input_path)
logf("  Loaded ", nrow(altos), " rows, ", ncol(altos), " columns", log_file = log_file)

# ============================================================
# ADDRESS CLEANING
# ============================================================

logf("Pre-processing addresses...", log_file = log_file)
altos[, street_address_lower := str_to_lower(street_address) %>% misc_pre_processing()]

# Setup postmastr dictionaries
dirs <- pm_dictionary(type = "directional", filter = c("N", "S", "E", "W"), locale = "us")
states <- pm_dictionary(type = "state", filter = unique(altos$state), case = c("title", "upper", "lower"), locale = "us")
cities <- tibble(city.input = unique(altos$city) %>% str_to_lower())

# Identify parseable addresses
altos_sample <- altos %>%
  pm_identify(var = street_address_lower)

dropped_ids_og <- altos_sample %>% filter(!pm.type %in% c("full", "short")) %>% pull(pm.uid)
logf("  Dropped ", length(dropped_ids_og), " unparseable addresses (not full/short type)", log_file = log_file)

altos_sample <- altos_sample %>%
  filter(pm.type %in% c("full", "short"))

logf("  Proceeding with ", nrow(altos_sample), " parseable addresses", log_file = log_file)

# ---- Address parsing workflow ----
# prep -> parse unit -> parse postal code -> parse state -> parse city ->
# parse unit again -> parse house # -> parse street directionals ->
# parse street suffix -> parse street name -> post processing

logf("Running postmastr parsing pipeline...", log_file = log_file)

altos_adds_sample <- pm_prep(altos_sample, var = "street_address_lower", type = "street")
dropped_ids <- altos_sample %>% filter(!pm.uid %in% altos_adds_sample$pm.uid) %>% pull(pm.uid)

# Parse unit first (for addresses like "123 main st boston ma #1")
altos_adds_sample_units <- parse_unit(altos_adds_sample$pm.address)
altos_adds_sample$pm.address <- altos_adds_sample_units$clean_address %>% str_squish()
altos_adds_sample <- pm_postal_parse(altos_adds_sample, locale = "us")

# Unit parsing -- 3 rounds
altos_adds_sample_units_r1 <- parse_unit(altos_adds_sample$pm.address)
altos_adds_sample_units_r2 <- parse_unit(altos_adds_sample_units_r1$clean_address %>% str_squish())
altos_adds_sample_units_r3 <- parse_unit_extra(altos_adds_sample_units_r2$clean_address %>% str_squish())
altos_adds_sample$pm.address <- altos_adds_sample_units_r2$clean_address %>% str_squish()
altos_adds_sample$pm.unit <- altos_adds_sample_units_r3$unit

# Save unit tibble for reference
unit_tibble <- tibble(
  address = altos_adds_sample_units$og_address,
  u1 = altos_adds_sample_units$unit,
  u2 = altos_adds_sample_units_r1$unit,
  u3 = altos_adds_sample_units_r2$unit,
  u4 = altos_adds_sample_units_r3$unit,
  uid = altos_adds_sample$pm.uid
) %>%
  mutate(pm.unit = coalesce(u1, u2, u3, u4))

# Parse house number
altos_adds_sample <- pm_house_parse(altos_adds_sample)
altos_adds_sample_nums <- altos_adds_sample$pm.house %>% parse_letter()
altos_adds_sample_range <- altos_adds_sample_nums$clean_address %>% parse_range()
new_pm_house <- coalesce(altos_adds_sample_range$range1, altos_adds_sample_range$og_address)
altos_adds_sample$pm.house <- new_pm_house %>% as.numeric()
altos_adds_sample$pm.house2 <- altos_adds_sample_range$range2 %>% as.numeric()
altos_adds_sample$pm.house.letter <- altos_adds_sample_nums$letter
altos_adds_sample_copy1 <- copy(altos_adds_sample)

# Parse street directionals and suffix
altos_adds_sample <- pm_streetDir_parse(altos_adds_sample, dictionary = dirs)
altos_adds_sample <- pm_streetSuf_parse(altos_adds_sample)
altos_adds_sample_copy <- copy(altos_adds_sample)

# Handle incomplete addresses (e.g., "123 centre" -> "123 centre st")
altos_adds_sample <- altos_adds_sample %>%
  mutate(
    pm.address = coalesce(pm.address, pm.streetSuf) %>% str_to_lower(),
    pm.address = case_when(
      pm.address == "riv" ~ "river",
      pm.address == "ctr" ~ "centre",
      pm.address == "pk" ~ "park",
      pm.address == "grv" ~ "grove",
      pm.address == "gdn" ~ "garden",
      pm.address == "smt" ~ "summit",
      pm.address == "spg" ~ "spring",
      TRUE ~ pm.address
    )
  )

# Filter out rows with NA pm.address before street parsing
na_mask <- is.na(altos_adds_sample$pm.address) | altos_adds_sample$pm.address == "" |
           !is.character(altos_adds_sample$pm.address)
logf("  Filtering out ", sum(na_mask), " rows with NA/empty/non-char pm.address before street parsing", log_file = log_file)
altos_adds_sample <- altos_adds_sample %>%
  filter(!is.na(pm.address) & pm.address != "") %>%
  mutate(pm.address = as.character(pm.address) %>% str_squish())

# Remove any remaining problematic addresses (very short or weird characters)
altos_adds_sample <- altos_adds_sample %>%
  filter(nchar(pm.address) >= 2)

logf("  After cleaning: ", nrow(altos_adds_sample), " rows remain for street parsing", log_file = log_file)

# Parse street name (wrap in tryCatch for robustness)
altos_adds_sample <- tryCatch({
  pm_street_parse(altos_adds_sample, ordinal = TRUE, drop = FALSE)
}, error = function(e) {
  logf("  WARNING: pm_street_parse failed, attempting fallback...", log_file = log_file)
  # Fallback: just use pm.address as pm.street if parsing fails
  altos_adds_sample %>% mutate(pm.street = pm.address)
})
altos_adds_sample$pm.street <- misc_post_processing(str_to_lower(altos_adds_sample$pm.street))

# Fix street suffix detection
altos_adds_sample$pm.streetSuf <- fifelse(
  str_detect(altos_adds_sample$pm.street, "\\s(st$)"),
  "st",
  altos_adds_sample$pm.streetSuf
) %>% str_squish()

altos_adds_sample$pm.street <- fifelse(
  str_detect(altos_adds_sample$pm.street, "\\s(st$)"),
  str_replace_all(altos_adds_sample$pm.street, "\\sst$", ""),
  altos_adds_sample$pm.street
) %>% str_squish()

# Standardize street suffix
altos_adds_sample$pm.streetSuf <- altos_adds_sample$pm.streetSuf %>%
  str_replace_all("spark", " pk") %>%
  str_replace_all("street", " st") %>%
  str_replace_all("square", " sq") %>%
  str_replace_all("lane", " ln") %>%
  str_replace_all("alley", " aly") %>%
  str_replace_all("way", " way")

# Create composite address column
altos_adds_sample_c <- altos_adds_sample %>%
  mutate(across(c(pm.sufDir, pm.street, pm.streetSuf, pm.preDir), ~ str_squish(str_to_lower(replace_na(.x, ""))))) %>%
  mutate(
    n_sn_ss_c = str_squish(str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir))),
    pm.dir_concat = coalesce(pm.preDir, pm.sufDir)
  )

setDT(altos_adds_sample_c)
setDT(altos_sample)

altos_adds_sample_c[, num_st_sfx_id := .GRP, by = .(pm.house, pm.street, pm.streetSuf)]
altos_adds_sample_c$pm.street <- misc_post_processing(str_to_lower(altos_adds_sample_c$pm.street))

# ============================================================
# MERGE PARSED ADDRESSES BACK TO ORIGINAL DATA
# ============================================================

logf("Merging parsed addresses back to original data...", log_file = log_file)

altos_adds_sample_c[, addy_id := .I]
altos_clean <- altos_adds_sample_c %>%
  merge(altos_sample[],
        by = "pm.uid",
        all.y = TRUE)

logf("  After merge: ", nrow(altos_clean), " rows", log_file = log_file)

# Add zipcode info and create composite IDs
# Impute pm.zip from raw zip column, add "_" prefix to ensure character type on read
altos_clean[, pm.zip := case_when(
  str_length(pm.zip) >= 4 ~ paste0("_", str_pad(pm.zip, 5, "left", "0")),
  str_length(zip) >= 4 ~ paste0("_", str_pad(zip, 5, "left", "0")),
  TRUE ~ NA_character_
)]
altos_clean[, num_zips := uniqueN(pm.zip, na.rm = TRUE), by = num_st_sfx_id]
altos_clean[, num_zips_uid := uniqueN(pm.zip, na.rm = TRUE), by = pm.uid]
altos_clean[, pm.uid_zip := .GRP, by = .(pm.uid, pm.zip)]
altos_clean[, .N, by = num_zips_uid]
altos_clean[, num_listings := .N, by = .(beds, pm.uid)]
altos_clean[, num_years := uniqueN(year), by = .(beds, pm.uid)]
altos_clean[, listing_id := seq(.N)]
altos_clean[, pm.city := city]
altos_clean[, pm.state := state]

# ============================================================
# ASSERTIONS
# ============================================================

logf("Running assertions...", log_file = log_file)

# Check listing_id is unique
n_listing_id <- altos_clean[, uniqueN(listing_id)]
stopifnot("listing_id must be unique" = n_listing_id == nrow(altos_clean))
logf("  listing_id uniqueness: PASS (", n_listing_id, " unique)", log_file = log_file)

# Check required columns exist
required_cols <- c("listing_id", "pm.uid", "pm.house", "pm.street", "n_sn_ss_c", "year")
missing_cols <- setdiff(required_cols, names(altos_clean))
if (length(missing_cols) > 0) {
  stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
}
logf("  Required columns: PASS", log_file = log_file)

# ============================================================
# ENHANCED DIAGNOSTICS: Address Parsing Quality
# ============================================================

logf("", log_file = log_file)
logf("=== Address Parsing Diagnostics ===", log_file = log_file)

# 1. Component parsing success rates
n_total <- nrow(altos_clean)
logf("Parsing success rates:", log_file = log_file)
logf("  pm.house: ", sum(!is.na(altos_clean$pm.house)), " (",
    round(100 * sum(!is.na(altos_clean$pm.house)) / n_total, 1), "%)", log_file = log_file)
logf("  pm.street: ", sum(!is.na(altos_clean$pm.street) & altos_clean$pm.street != ""), " (",
    round(100 * sum(!is.na(altos_clean$pm.street) & altos_clean$pm.street != "") / n_total, 1), "%)", log_file = log_file)
logf("  pm.streetSuf: ", sum(!is.na(altos_clean$pm.streetSuf) & altos_clean$pm.streetSuf != ""), " (",
    round(100 * sum(!is.na(altos_clean$pm.streetSuf) & altos_clean$pm.streetSuf != "") / n_total, 1), "%)", log_file = log_file)
logf("  pm.zip (imputed w/ _ prefix): ", sum(!is.na(altos_clean$pm.zip) & altos_clean$pm.zip != ""), " (",
    round(100 * sum(!is.na(altos_clean$pm.zip) & altos_clean$pm.zip != "") / n_total, 1), "%)", log_file = log_file)
logf("  n_sn_ss_c (composite): ", sum(!is.na(altos_clean$n_sn_ss_c) & altos_clean$n_sn_ss_c != ""), " (",
    round(100 * sum(!is.na(altos_clean$n_sn_ss_c) & altos_clean$n_sn_ss_c != "") / n_total, 1), "%)", log_file = log_file)

# 2. Unique addresses
n_unique_addrs <- uniqueN(altos_clean$n_sn_ss_c)
logf("Address deduplication:", log_file = log_file)
logf("  Total listings: ", n_total, log_file = log_file)
logf("  Unique addresses: ", n_unique_addrs, log_file = log_file)
logf("  Avg listings per address: ", round(n_total / n_unique_addrs, 1), log_file = log_file)

# 3. Top 20 streets (by listing count)
top_streets <- altos_clean[!is.na(pm.street) & pm.street != "", .N, by = pm.street][order(-N)][1:20]
logf("Top 20 streets by listing count:", log_file = log_file)
for (i in 1:nrow(top_streets)) {
  logf("  ", i, ". ", top_streets$pm.street[i], ": ", top_streets$N[i], " listings", log_file = log_file)
}

# 4. Temporal coverage
year_dist <- altos_clean[, .N, by = year][order(year)]
logf("Listings by year:", log_file = log_file)
for (i in 1:nrow(year_dist)) {
  logf("  ", year_dist$year[i], ": ", year_dist$N[i], " listings", log_file = log_file)
}

# 5. Geographic coverage (zip codes)
n_zips <- uniqueN(altos_clean$pm.zip[!is.na(altos_clean$pm.zip) & altos_clean$pm.zip != ""])
top_zips <- altos_clean[!is.na(pm.zip) & pm.zip != "", .N, by = pm.zip][order(-N)][1:10]
logf("Geographic coverage:", log_file = log_file)
logf("  Unique zip codes: ", n_zips, log_file = log_file)
logf("  Top 10 zips: ", paste(str_remove(top_zips$pm.zip, "^_"), "(", top_zips$N, ")", collapse = ", "), log_file = log_file)

# 6. Listings without valid addresses
n_invalid <- sum(is.na(altos_clean$n_sn_ss_c) | altos_clean$n_sn_ss_c == "")
if (n_invalid > 0) {
  logf("Listings with invalid/missing composite address: ", n_invalid, " (",
      round(100 * n_invalid / n_total, 2), "%)", log_file = log_file)
}

logf("", log_file = log_file)

# ============================================================
# WRITE OUTPUT
# ============================================================

out_path <- p_product(cfg, "altos_clean")
logf("Writing output to: ", out_path, log_file = log_file)

# Ensure output directory exists
dir.create(dirname(out_path), showWarnings = FALSE, recursive = TRUE)

fwrite(altos_clean, out_path)
logf("  Wrote ", nrow(altos_clean), " rows, ", ncol(altos_clean), " columns", log_file = log_file)

logf("=== Finished clean-altos-addresses.R ===", log_file = log_file)
