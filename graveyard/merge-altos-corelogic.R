## ============================================================
## merge-altos-corelogic.R
## ============================================================
## Purpose: Create crosswalk between Altos listings and CoreLogic property records
##          using tiered address matching
##
## Inputs:
##   - cfg$products$altos_clean (clean/altos_clean.csv)
##   - cfg$products$corelogic_clean (clean/corelogic_clean.csv)
##
## Outputs:
##   - cfg$products$altos_corelogic_xwalk (xwalks/altos_corelogic_xwalk.csv)
##   - cfg$products$altos_corelogic_xwalk_listing (xwalks/altos_corelogic_xwalk_listing.csv)
##
## Primary keys:
##   - altos_corelogic_xwalk: n_sn_ss_c (unique addresses)
##   - altos_corelogic_xwalk_listing: listing_id (individual listings)
##
## Match tiers (in order):
##   1. num_st_sfx_dir_zip: house + street + suffix + direction + zip
##   2. num_st: house + street + zip (no suffix/direction)
##   3. num_st_sfx: house + street + suffix (no zip)
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
})

# ---- Load config and set up logging ----
source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "merge-altos-corelogic.log")

logf("=== Starting merge-altos-corelogic.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Load input data ----
logf("Loading input data...", log_file = log_file)

# Load cleaned Altos data
altos_path <- p_product(cfg, "altos_clean")
logf("  Loading Altos: ", altos_path, log_file = log_file)
altos <- fread(altos_path)
logf("  Loaded ", nrow(altos), " Altos listings", log_file = log_file)

# Load cleaned CoreLogic data
corelogic_path <- p_product(cfg, "corelogic_clean")
logf("  Loading CoreLogic: ", corelogic_path, log_file = log_file)
corelogic <- fread(corelogic_path)
logf("  Loaded ", nrow(corelogic), " CoreLogic properties", log_file = log_file)

# ---- Prepare address data ----
logf("Preparing address data...", log_file = log_file)

# Create zip_imp for matching (padded and prefixed)
altos[, pm.zip_imp := paste0("_", str_pad(zip, 5, "left", "0"))]
corelogic[, pm.zip_imp := str_sub(pm.zip_imp, 1, 6)]

# Standardize directions to lowercase
corelogic <- corelogic %>% mutate(across(contains("dir"), ~str_to_lower(.x)))

# Get unique addresses from each dataset
altos_addys <- unique(altos, by = "n_sn_ss_c")
corelogic_addys <- unique(corelogic, by = "n_sn_ss_c")

logf("  Unique Altos addresses: ", nrow(altos_addys), log_file = log_file)
logf("  Unique CoreLogic addresses: ", nrow(corelogic_addys), log_file = log_file)

# ---- Tier 1: Match on house + street + suffix + direction + zip ----
logf("Tier 1: Matching on num_st_sfx_dir_zip...", log_file = log_file)

num_st_sfx_dir_zip_merge <- altos_addys %>%
  merge(
    corelogic_addys[, .(pm.house, pm.street, pm.zip_imp, pm.streetSuf, pm.dir_concat, clip)],
    by = c("pm.house", "pm.street", "pm.streetSuf", "pm.dir_concat", "pm.zip_imp"),
    all.x = TRUE,
    allow.cartesian = TRUE
  ) %>%
  mutate(merge = "num_st_sfx_dir_zip")

setDT(num_st_sfx_dir_zip_merge)
num_st_sfx_dir_zip_merge[, num_clips := uniqueN(clip, na.rm = TRUE), by = n_sn_ss_c]
num_st_sfx_dir_zip_merge[, matched := num_clips == 1]
matched_num_st_sfx_dir_zip <- num_st_sfx_dir_zip_merge[matched == TRUE, n_sn_ss_c]

logf("  Tier 1 unique matches: ", length(matched_num_st_sfx_dir_zip), log_file = log_file)

# ---- Tier 2: Match on house + street + zip (relaxed) ----
logf("Tier 2: Matching on num_st (unmatched from Tier 1)...", log_file = log_file)

num_st_merge <- altos_addys %>%
  filter(!n_sn_ss_c %in% matched_num_st_sfx_dir_zip) %>%
  merge(
    corelogic_addys[, .(pm.house, pm.street, pm.zip_imp, pm.streetSuf, pm.sufDir, pm.preDir, clip)],
    by = c("pm.house", "pm.street", "pm.zip_imp"),
    all.x = TRUE,
    allow.cartesian = TRUE
  ) %>%
  mutate(merge = "num_st")

setDT(num_st_merge)
num_st_merge[, num_clips := uniqueN(clip, na.rm = TRUE), by = n_sn_ss_c]
num_st_merge[, matched := num_clips == 1]
matched_num_st <- num_st_merge[matched == TRUE, n_sn_ss_c]

logf("  Tier 2 unique matches: ", length(matched_num_st), log_file = log_file)

# ---- Tier 3: Match on house + street + suffix (no zip) ----
logf("Tier 3: Matching on num_st_sfx (unmatched from Tiers 1-2)...", log_file = log_file)

num_st_sfx_merge <- altos_addys %>%
  filter(
    !n_sn_ss_c %in% matched_num_st_sfx_dir_zip &
    !n_sn_ss_c %in% matched_num_st
  ) %>%
  merge(
    corelogic_addys[, .(pm.house, pm.street, pm.streetSuf, clip)],
    by = c("pm.house", "pm.street", "pm.streetSuf"),
    all.x = TRUE,
    allow.cartesian = TRUE
  ) %>%
  mutate(merge = "num_st_sfx")

setDT(num_st_sfx_merge)
num_st_sfx_merge[, num_clips := uniqueN(clip, na.rm = TRUE), by = n_sn_ss_c]
num_st_sfx_merge[, matched := num_clips == 1]
matched_num_st_sfx <- num_st_sfx_merge[matched == TRUE, unique(n_sn_ss_c)]

logf("  Tier 3 unique matches: ", length(matched_num_st_sfx), log_file = log_file)

# ---- Combine all matched IDs ----
matched_ids <- c(
  matched_num_st_sfx_dir_zip,
  matched_num_st,
  matched_num_st_sfx
)

logf("Total unique matches: ", length(matched_ids), log_file = log_file)

# ---- Build crosswalk ----
logf("Building crosswalk...", log_file = log_file)

# Unique matches
xwalk_unique <- bind_rows(list(
  num_st_sfx_dir_zip_merge[num_clips == 1, .(clip, n_sn_ss_c, merge)] %>% distinct(),
  num_st_merge[num_clips == 1, .(clip, n_sn_ss_c, merge)] %>% distinct(),
  num_st_sfx_merge[num_clips == 1, .(clip, n_sn_ss_c, merge)] %>% distinct()
)) %>%
  mutate(unique = TRUE) %>%
  as.data.table()

# Non-unique matches (multiple candidates)
xwalk_non_unique <- bind_rows(list(
  num_st_sfx_dir_zip_merge[num_clips > 1, .(clip, n_sn_ss_c, merge)] %>% distinct(),
  num_st_merge[num_clips > 1, .(clip, n_sn_ss_c, merge)] %>% distinct(),
  num_st_sfx_merge[num_clips > 1, .(clip, n_sn_ss_c, merge)] %>% distinct()
)) %>%
  mutate(unique = FALSE) %>%
  filter(!n_sn_ss_c %in% xwalk_unique$n_sn_ss_c) %>%
  filter(!is.na(clip)) %>%
  distinct(clip, n_sn_ss_c, .keep_all = TRUE) %>%
  as.data.table()

# Unmatched addresses
xwalk_unmatched <- tibble(
  n_sn_ss_c = altos_addys[
    !n_sn_ss_c %in% xwalk_non_unique$n_sn_ss_c &
    !n_sn_ss_c %in% xwalk_unique$n_sn_ss_c,
    n_sn_ss_c
  ],
  merge = "not merged",
  clip = NA_character_
) %>%
  distinct()

# Combine all
xwalk <- bind_rows(list(xwalk_unique, xwalk_non_unique, xwalk_unmatched))

# Add summary columns
xwalk[, num_merges := uniqueN(merge), by = .(n_sn_ss_c, unique)]
xwalk[, num_parcels_matched := uniqueN(clip, na.rm = TRUE), by = n_sn_ss_c]
xwalk[, num_addys_matched := uniqueN(n_sn_ss_c, na.rm = TRUE), by = clip]

logf("  Crosswalk rows: ", nrow(xwalk), log_file = log_file)
logf("  Unique addresses: ", uniqueN(xwalk$n_sn_ss_c), log_file = log_file)

# ---- Match summary ----
logf("Match summary by tier:", log_file = log_file)
merge_summary <- unique(xwalk, by = "n_sn_ss_c")[, .N, by = merge][, per := round(N / sum(N), 3)]
for (i in 1:nrow(merge_summary)) {
  logf("  ", merge_summary$merge[i], ": ", merge_summary$N[i], " (", merge_summary$per[i] * 100, "%)", log_file = log_file)
}

# ---- Create listing-level crosswalk ----
logf("Creating listing-level crosswalk...", log_file = log_file)

xwalk_listing <- merge(
  xwalk,
  altos[, .(n_sn_ss_c, listing_id, city)] %>% distinct(),
  by = "n_sn_ss_c",
  allow.cartesian = TRUE
)

logf("  Listing crosswalk rows: ", nrow(xwalk_listing), log_file = log_file)

# ---- Assertions ----
logf("Running assertions...", log_file = log_file)

# Check that each address appears only once in xwalk (at most)
n_addys <- uniqueN(xwalk$n_sn_ss_c)
n_rows_unique_addr <- nrow(unique(xwalk, by = "n_sn_ss_c"))
if (n_addys != n_rows_unique_addr) {
  logf("WARNING: Multiple rows per address in xwalk (non-unique matches exist)", log_file = log_file)
}

# Verify all Altos addresses are covered
n_altos_addys <- nrow(altos_addys)
if (n_addys < n_altos_addys) {
  logf("WARNING: Not all Altos addresses are in crosswalk", log_file = log_file)
}

logf("  Assertions complete", log_file = log_file)

# ---- Write outputs ----
logf("Writing outputs...", log_file = log_file)

out_xwalk <- p_product(cfg, "altos_corelogic_xwalk")
fwrite(xwalk, out_xwalk)
logf("  Wrote xwalk: ", nrow(xwalk), " rows to ", out_xwalk, log_file = log_file)

out_xwalk_listing <- p_product(cfg, "altos_corelogic_xwalk_listing")
fwrite(xwalk_listing, out_xwalk_listing)
logf("  Wrote xwalk_listing: ", nrow(xwalk_listing), " rows to ", out_xwalk_listing, log_file = log_file)

logf("=== Finished merge-altos-corelogic.R ===", log_file = log_file)
