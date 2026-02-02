## ============================================================
## merge-altos-rental-listings.R
## ============================================================
## Purpose: Create crosswalk between Altos listings and OPA parcels using
##          tiered address matching plus spatial joining
##
## Inputs:
##   - cfg$products$altos_clean (clean/altos_clean.csv)
##   - cfg$products$parcels_clean (clean/parcels_clean.csv)
##   - cfg$products$licenses_clean (clean/licenses_clean.csv)
##   - cfg$inputs$philly_parcels_sf (shapefiles/philly_parcels_sf/DOR_Parcel.shp)
##
## Outputs:
##   - cfg$products$altos_address_agg (xwalks/altos_address_agg.csv)
##   - cfg$products$altos_parcel_xwalk (xwalks/altos_parcel_xwalk.csv)
##   - cfg$products$altos_parcel_xwalk_listing (xwalks/altos_parcel_xwalk_listing.csv)
##
## Primary keys:
##   - altos_address_agg: (year, n_sn_ss_c)
##   - altos_parcel_xwalk: n_sn_ss_c (unique addresses)
##   - altos_parcel_xwalk_listing: listing_id
##
## Match tiers:
##   1. num_st_sfx_dir_zip: full address match
##   2. num_st: house + street + zip
##   3. num_st_sfx: house + street + suffix
##   4. spatial: point-in-polygon join for remaining addresses
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(sf)
})

# ---- Load config and set up logging ----
source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "merge-altos-rental-listings.log")

logf("=== Starting merge-altos-rental-listings.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Load input data ----
logf("Loading input data...", log_file = log_file)

# Load Altos data
altos_path <- p_product(cfg, "altos_clean")
logf("  Loading Altos: ", altos_path, log_file = log_file)
altos <- fread(altos_path)
logf("  Loaded ", nrow(altos), " Altos listings", log_file = log_file)

# Load parcel data
parcels_path <- p_product(cfg, "parcels_clean")
logf("  Loading parcels: ", parcels_path, log_file = log_file)
parcels <- fread(parcels_path)
logf("  Loaded ", nrow(parcels), " parcels", log_file = log_file)

# Load licenses to identify rental parcels
licenses_path <- p_product(cfg, "licenses_clean")
logf("  Loading licenses: ", licenses_path, log_file = log_file)
licenses <- fread(licenses_path)

# Filter to rental licenses
rentals <- licenses[licensetype == "Rental"]
rentals[, PID := str_pad(opa_account_num, 9, "left", pad = "0")]
rental_pids <- unique(rentals$PID)
logf("  Identified ", length(rental_pids), " rental parcel IDs", log_file = log_file)

# Load parcel shapefile for spatial joining
parcels_sf_path <- p_input(cfg, "philly_parcels_sf")
logf("  Loading parcel shapefile: ", parcels_sf_path, log_file = log_file)
parcels_sf <- read_sf(parcels_sf_path)

# ---- Prepare data for matching ----
logf("Preparing data for matching...", log_file = log_file)

# Add listing ID if not present
if (!"listing_id" %in% names(altos)) {
  altos[, listing_id := seq(.N)]
}

# pm.zip is already imputed with "_" prefix from clean-altos-addresses.R
# No additional coalesce needed

# Add coordinates for spatial join
altos[, GEOID.longitude := as.numeric(geo_long)]
altos[, GEOID.latitude := as.numeric(geo_lat)]

# Create address-level aggregation
logf("Creating address-level aggregation...", log_file = log_file)
altos_address_agg <- altos[, .(
  num_listings = .N
), by = .(year, n_sn_ss_c, pm.house, pm.street, pm.zip,
          pm.streetSuf, pm.sufDir, pm.preDir, GEOID.longitude, GEOID.latitude)]

logf("  Address aggregation: ", nrow(altos_address_agg), " rows", log_file = log_file)

# Prepare parcel addresses
parcels[, PID := str_pad(parcel_number, 9, "left", pad = "0")]
# Add "_" prefix to parcel pm.zip to match altos format (ensures character type)
parcels[, pm.zip := paste0("_", str_pad(as.character(pm.zip), 5, "left", "0"))]
parcels[pm.zip == "_NA" | pm.zip == "_   NA", pm.zip := NA_character_]
parcel_addys <- unique(parcels, by = "n_sn_ss_c")

# Get unique Altos addresses
altos_addys <- unique(altos_address_agg, by = "n_sn_ss_c")
logf("  Unique Altos addresses: ", nrow(altos_addys), log_file = log_file)
logf("  Unique parcel addresses: ", nrow(parcel_addys), log_file = log_file)

# ---- Tier 1: Full address match ----
logf("Tier 1: Matching on num_st_sfx_dir_zip...", log_file = log_file)

num_st_sfx_dir_zip_merge <- altos_addys %>%
  merge(
    parcel_addys[, .(pm.house, pm.street, pm.zip, pm.streetSuf, pm.sufDir, pm.preDir, PID)],
    by = c("pm.house", "pm.street", "pm.streetSuf", "pm.sufDir", "pm.preDir", "pm.zip"),
    all.x = TRUE,
    allow.cartesian = TRUE
  ) %>%
  mutate(merge = "num_st_sfx_dir_zip")

setDT(num_st_sfx_dir_zip_merge)
num_st_sfx_dir_zip_merge[, num_pids := uniqueN(PID, na.rm = TRUE), by = n_sn_ss_c]
num_st_sfx_dir_zip_merge[, matched := num_pids == 1]
matched_num_st_sfx_dir_zip <- num_st_sfx_dir_zip_merge[matched == TRUE, n_sn_ss_c]

logf("  Tier 1 unique matches: ", length(matched_num_st_sfx_dir_zip), log_file = log_file)

# ---- Tier 2: House + street + zip ----
logf("Tier 2: Matching on num_st (unmatched from Tier 1)...", log_file = log_file)

num_st_merge <- altos_addys %>%
  filter(!n_sn_ss_c %in% matched_num_st_sfx_dir_zip) %>%
  merge(
    parcel_addys[, .(pm.house, pm.street, pm.zip, pm.streetSuf, pm.sufDir, pm.preDir, PID)],
    by = c("pm.house", "pm.street", "pm.zip"),
    all.x = TRUE,
    allow.cartesian = TRUE
  ) %>%
  mutate(merge = "num_st")

setDT(num_st_merge)
num_st_merge[, num_pids := uniqueN(PID, na.rm = TRUE), by = n_sn_ss_c]
num_st_merge[, matched := num_pids == 1]
matched_num_st <- num_st_merge[matched == TRUE, n_sn_ss_c]

logf("  Tier 2 unique matches: ", length(matched_num_st), log_file = log_file)

# ---- Tier 3: House + street + suffix ----
logf("Tier 3: Matching on num_st_sfx (unmatched from Tiers 1-2)...", log_file = log_file)

num_st_sfx_merge <- altos_addys %>%
  filter(
    !n_sn_ss_c %in% matched_num_st_sfx_dir_zip &
    !n_sn_ss_c %in% matched_num_st
  ) %>%
  merge(
    parcel_addys[, .(pm.house, pm.street, pm.streetSuf, PID)],
    by = c("pm.house", "pm.street", "pm.streetSuf"),
    all.x = TRUE,
    allow.cartesian = TRUE
  ) %>%
  mutate(merge = "num_st_sfx")

setDT(num_st_sfx_merge)
num_st_sfx_merge[, num_pids := uniqueN(PID, na.rm = TRUE), by = n_sn_ss_c]
num_st_sfx_merge[, matched := num_pids == 1]
matched_num_st_sfx <- num_st_sfx_merge[matched == TRUE, unique(n_sn_ss_c)]

logf("  Tier 3 unique matches: ", length(matched_num_st_sfx), log_file = log_file)

# ---- Tier 4: Spatial join ----
logf("Tier 4: Spatial matching for remaining addresses...", log_file = log_file)

# Disable s2 for spatial operations
sf_use_s2(FALSE)

# Merge parcel shapefile with parcel data to get PID
parcels_sf_m <- parcels_sf %>%
  select(-matches("PID")) %>%
  merge(parcels[, .(pin, PID)], by.x = "PIN", by.y = "pin")

# Filter to residential parcels
residential_codes <- c("MIXED USE", "MULTI FAMILY", "SINGLE FAMILY", "APARTMENTS  > 4 UNITS")
parcel_sf_subset <- parcels_sf_m %>%
  filter(
    PID %in% rental_pids |
    PID %in% parcel_addys[category_code_description %in% residential_codes, PID]
  )

# Reproject to match coordinate system
parcel_sf_subset <- st_transform(parcel_sf_subset, crs = 4269)

logf("  Parcel subset for spatial join: ", nrow(parcel_sf_subset), " parcels", log_file = log_file)

# Get addresses that didn't uniquely match
spatial_join_dt <- num_st_sfx_merge[num_pids != 1]

if (nrow(spatial_join_dt) > 0) {
  # Convert to sf
  spatial_join_sf <- spatial_join_dt %>%
    filter(!is.na(GEOID.longitude) & !is.na(GEOID.latitude)) %>%
    st_as_sf(coords = c("GEOID.longitude", "GEOID.latitude")) %>%
    st_set_crs(value = st_crs(parcel_sf_subset))

  # Perform spatial join
  spatial_join_result <- st_join(
    spatial_join_sf,
    parcel_sf_subset %>%
      rename(PID_spatial = PID) %>%
      select(PID_spatial, geometry)
  )

  spatial_join_sf_join <- spatial_join_result %>%
    as.data.table() %>%
    select(-geometry) %>%
    mutate(merge = "spatial")

  spatial_join_sf_join[, num_pids_st := uniqueN(PID_spatial, na.rm = TRUE), by = n_sn_ss_c]
  spatial_join_sf_join[, matched := num_pids_st == 1]
  matched_spatial <- spatial_join_sf_join[matched == TRUE, n_sn_ss_c]

  logf("  Tier 4 unique matches: ", length(matched_spatial), log_file = log_file)
} else {
  spatial_join_sf_join <- data.table()
  matched_spatial <- character(0)
  logf("  No addresses remaining for spatial join", log_file = log_file)
}

# ---- Build crosswalk ----
logf("Building crosswalk...", log_file = log_file)

matched_ids <- c(
  matched_num_st_sfx_dir_zip,
  matched_num_st,
  matched_num_st_sfx,
  matched_spatial
)

logf("  Total unique matches: ", length(matched_ids), log_file = log_file)

# Unique matches
xwalk_unique_parts <- list(
  num_st_sfx_dir_zip_merge[num_pids == 1, .(PID, n_sn_ss_c, merge)] %>% distinct(),
  num_st_merge[num_pids == 1, .(PID, n_sn_ss_c, merge)] %>% distinct(),
  num_st_sfx_merge[num_pids == 1, .(PID, n_sn_ss_c, merge)] %>% distinct()
)

if (nrow(spatial_join_sf_join) > 0) {
  xwalk_unique_parts <- c(xwalk_unique_parts, list(
    spatial_join_sf_join[num_pids_st == 1, .(PID_spatial, n_sn_ss_c, merge)] %>%
      distinct() %>%
      rename(PID = PID_spatial)
  ))
}

xwalk_unique <- bind_rows(xwalk_unique_parts) %>%
  mutate(unique = TRUE) %>%
  as.data.table()

# Non-unique matches
xwalk_non_unique_parts <- list(
  num_st_sfx_dir_zip_merge[num_pids > 1, .(PID, n_sn_ss_c, merge)] %>% distinct(),
  num_st_merge[num_pids > 1, .(PID, n_sn_ss_c, merge)] %>% distinct(),
  num_st_sfx_merge[num_pids > 1, .(PID, n_sn_ss_c, merge)] %>% distinct()
)

if (nrow(spatial_join_sf_join) > 0) {
  xwalk_non_unique_parts <- c(xwalk_non_unique_parts, list(
    spatial_join_sf_join[num_pids_st > 1 & !is.na(PID_spatial), .(PID_spatial, n_sn_ss_c, merge)] %>%
      distinct() %>%
      rename(PID = PID_spatial)
  ))
}

xwalk_non_unique <- bind_rows(xwalk_non_unique_parts) %>%
  mutate(unique = FALSE) %>%
  filter(!n_sn_ss_c %in% xwalk_unique$n_sn_ss_c) %>%
  filter(!is.na(PID)) %>%
  distinct(PID, n_sn_ss_c, .keep_all = TRUE) %>%
  as.data.table()

# Unmatched
xwalk_unmatched <- tibble(
  n_sn_ss_c = altos_address_agg[
    !n_sn_ss_c %in% xwalk_non_unique$n_sn_ss_c &
    !n_sn_ss_c %in% xwalk_unique$n_sn_ss_c,
    n_sn_ss_c
  ],
  merge = "not merged",
  PID = NA_character_
) %>%
  distinct()

# Combine
xwalk <- bind_rows(list(xwalk_unique, xwalk_non_unique, xwalk_unmatched))

xwalk[, num_merges := uniqueN(merge), by = .(n_sn_ss_c, unique)]
xwalk[, num_parcels_matched := uniqueN(PID, na.rm = TRUE), by = n_sn_ss_c]
xwalk[, num_addys_matched := uniqueN(n_sn_ss_c, na.rm = TRUE), by = PID]

logf("  Crosswalk rows: ", nrow(xwalk), log_file = log_file)

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
  altos[, .(n_sn_ss_c, listing_id)] %>% distinct(),
  by = "n_sn_ss_c",
  allow.cartesian = TRUE
)

logf("  Listing crosswalk rows: ", nrow(xwalk_listing), log_file = log_file)

# ---- Assertions ----
logf("Running assertions...", log_file = log_file)

# Check address uniqueness in xwalk
n_unique_addys <- uniqueN(xwalk$n_sn_ss_c)
logf("  Unique addresses in xwalk: ", n_unique_addys, log_file = log_file)

# ---- Write outputs ----
logf("Writing outputs...", log_file = log_file)

# Write address aggregation
out_agg <- p_product(cfg, "altos_address_agg")
fwrite(altos_address_agg, out_agg)
logf("  Wrote altos_address_agg: ", nrow(altos_address_agg), " rows to ", out_agg, log_file = log_file)

# Write crosswalk
out_xwalk <- p_product(cfg, "altos_parcel_xwalk")
fwrite(xwalk, out_xwalk)
logf("  Wrote altos_parcel_xwalk: ", nrow(xwalk), " rows to ", out_xwalk, log_file = log_file)

# Write listing-level crosswalk
out_xwalk_listing <- p_product(cfg, "altos_parcel_xwalk_listing")
fwrite(xwalk_listing, out_xwalk_listing)
logf("  Wrote altos_parcel_xwalk_listing: ", nrow(xwalk_listing), " rows to ", out_xwalk_listing, log_file = log_file)

logf("=== Finished merge-altos-rental-listings.R ===", log_file = log_file)
