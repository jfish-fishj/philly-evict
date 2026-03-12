## ============================================================
## merge-nhpd-parcels.R
## ============================================================
## Purpose: Create crosswalk between NHPD properties and OPA
##          parcels using tiered address matching plus spatial join.
##
## Inputs:
##   - cfg$products$nhpd_clean
##   - cfg$products$parcels_clean
##   - cfg$products$licenses_clean
##   - cfg$inputs$philly_parcels_sf
##
## Outputs:
##   - cfg$products$nhpd_address_agg
##   - cfg$products$nhpd_parcel_xwalk
##   - cfg$products$nhpd_parcel_xwalk_property
##
## Primary keys:
##   - nhpd_address_agg: n_sn_ss_c
##   - nhpd_parcel_xwalk: n_sn_ss_c
##   - nhpd_parcel_xwalk_property: nhpd_property_id
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(sf)
})

source("r/config.R")
source("r/helper-functions.R")

ModeChar <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (!length(x)) return(NA_character_)
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

normalize_pid <- function(x) {
  stringr::str_pad(as.character(x), width = 9, side = "left", pad = "0")
}

normalize_match_char <- function(x) {
  y <- stringr::str_squish(stringr::str_to_lower(as.character(x)))
  y[is.na(y) | y %in% c("na", "n/a")] <- ""
  y
}

cfg <- read_config()
log_file <- p_out(cfg, "logs", "merge-nhpd-parcels.log")

logf("=== Starting merge-nhpd-parcels.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

logf("Loading input data...", log_file = log_file)

nhpd <- fread(p_product(cfg, "nhpd_clean"))
parcels <- fread(p_product(cfg, "parcels_clean"))
licenses_path <- p_product(cfg, "licenses_clean")
if (!file.exists(licenses_path)) {
  stop("licenses_clean product not found at ", licenses_path)
}
licenses <- fread(licenses_path)
parcels_sf <- read_sf(p_input(cfg, "philly_parcels_sf"))

logf("  nhpd_clean: ", nrow(nhpd), " rows", log_file = log_file)
logf("  parcels_clean: ", nrow(parcels), " rows", log_file = log_file)
logf("  licenses_clean: ", nrow(licenses), " rows", log_file = log_file)
logf("  philly_parcels_sf: ", nrow(parcels_sf), " rows", log_file = log_file)

required_nhpd_cols <- c("nhpd_property_id", "n_sn_ss_c", "pm.house", "pm.street", "pm.streetSuf",
                        "pm.preDir", "pm.sufDir", "pm.zip", "GEOID.longitude", "GEOID.latitude")
missing_nhpd_cols <- setdiff(required_nhpd_cols, names(nhpd))
if (length(missing_nhpd_cols) > 0L) {
  stop("nhpd_clean is missing required columns: ", paste(missing_nhpd_cols, collapse = ", "))
}

nhpd[, nhpd_property_id := as.character(nhpd_property_id)]
parcels[, PID := normalize_pid(parcel_number)]
parcels[, pm.zip := paste0("_", str_pad(as.character(pm.zip), 5, "left", "0"))]
parcels[pm.zip %in% c("_NA", "_   NA"), pm.zip := NA_character_]

for (cc in c("pm.street", "pm.streetSuf", "pm.preDir", "pm.sufDir", "pm.zip", "n_sn_ss_c")) {
  nhpd[, (cc) := normalize_match_char(get(cc))]
  parcels[, (cc) := normalize_match_char(get(cc))]
}
nhpd[, pm.house := as.numeric(pm.house)]
parcels[, pm.house := as.numeric(pm.house)]

license_rental_pids <- unique(normalize_pid(licenses[licensetype == "Rental", opa_account_num]))
license_rental_pids <- license_rental_pids[!is.na(license_rental_pids)]
logf("  Rental-license PIDs: ", length(license_rental_pids), log_file = log_file)

logf("Creating address-level aggregation...", log_file = log_file)
nhpd_address_agg <- nhpd[
  ,
  .(
    nhpd_property_count = uniqueN(nhpd_property_id),
    nhpd_total_units = sum(as.numeric(TotalUnits), na.rm = TRUE),
    property_status_mode = ModeChar(PropertyStatus),
    GEOID.longitude = first(GEOID.longitude),
    GEOID.latitude = first(GEOID.latitude),
    pm.house = first(pm.house),
    pm.street = first(pm.street),
    pm.zip = first(pm.zip),
    pm.streetSuf = first(pm.streetSuf),
    pm.sufDir = first(pm.sufDir),
    pm.preDir = first(pm.preDir)
  ),
  by = .(n_sn_ss_c)
]
assert_unique(nhpd_address_agg, "n_sn_ss_c", "nhpd_address_agg")

parcel_addys <- unique(
  parcels[, .(PID, n_sn_ss_c, pm.house, pm.street, pm.zip, pm.streetSuf, pm.sufDir, pm.preDir,
              category_code_description, owner_1)],
  by = c("PID", "n_sn_ss_c")
)
assert_unique(parcel_addys, c("PID", "n_sn_ss_c"), "parcel_addys")

nhpd_addys <- unique(nhpd_address_agg, by = "n_sn_ss_c")
logf("  Unique NHPD addresses: ", nrow(nhpd_addys), log_file = log_file)
logf("  Unique parcel addresses: ", uniqueN(parcel_addys$n_sn_ss_c), log_file = log_file)

logf("Tier 1: house + street + suffix + directional + zip", log_file = log_file)
num_st_sfx_dir_zip_merge <- nhpd_addys %>%
  merge(
    parcel_addys[, .(pm.house, pm.street, pm.streetSuf, pm.sufDir, pm.preDir, pm.zip, PID)],
    by = c("pm.house", "pm.street", "pm.streetSuf", "pm.sufDir", "pm.preDir", "pm.zip"),
    all.x = TRUE,
    allow.cartesian = TRUE
  ) %>%
  mutate(merge = "num_st_sfx_dir_zip")
setDT(num_st_sfx_dir_zip_merge)
num_st_sfx_dir_zip_merge[, num_pids := uniqueN(PID, na.rm = TRUE), by = n_sn_ss_c]
matched_num_st_sfx_dir_zip <- num_st_sfx_dir_zip_merge[num_pids == 1, unique(n_sn_ss_c)]
logf("  Tier 1 unique matches: ", length(matched_num_st_sfx_dir_zip), log_file = log_file)

logf("Tier 2: house + street + zip", log_file = log_file)
num_st_merge <- nhpd_addys %>%
  filter(!n_sn_ss_c %in% matched_num_st_sfx_dir_zip) %>%
  merge(
    parcel_addys[, .(pm.house, pm.street, pm.zip, PID)],
    by = c("pm.house", "pm.street", "pm.zip"),
    all.x = TRUE,
    allow.cartesian = TRUE
  ) %>%
  mutate(merge = "num_st_zip")
setDT(num_st_merge)
num_st_merge[, num_pids := uniqueN(PID, na.rm = TRUE), by = n_sn_ss_c]
matched_num_st <- num_st_merge[num_pids == 1, unique(n_sn_ss_c)]
logf("  Tier 2 unique matches: ", length(matched_num_st), log_file = log_file)

logf("Tier 3: house + street + suffix", log_file = log_file)
num_st_sfx_merge <- nhpd_addys %>%
  filter(!n_sn_ss_c %in% matched_num_st_sfx_dir_zip & !n_sn_ss_c %in% matched_num_st) %>%
  merge(
    parcel_addys[, .(pm.house, pm.street, pm.streetSuf, PID)],
    by = c("pm.house", "pm.street", "pm.streetSuf"),
    all.x = TRUE,
    allow.cartesian = TRUE
  ) %>%
  mutate(merge = "num_st_sfx")
setDT(num_st_sfx_merge)
num_st_sfx_merge[, num_pids := uniqueN(PID, na.rm = TRUE), by = n_sn_ss_c]
matched_num_st_sfx <- num_st_sfx_merge[num_pids == 1, unique(n_sn_ss_c)]
logf("  Tier 3 unique matches: ", length(matched_num_st_sfx), log_file = log_file)

logf("Tier 4: spatial join for remaining addresses", log_file = log_file)
sf_use_s2(FALSE)
parcels_sf_m <- parcels_sf %>%
  select(-matches("PID")) %>%
  merge(parcels[, .(pin, PID)], by.x = "PIN", by.y = "pin")
parcel_sf_subset <- st_transform(parcels_sf_m, crs = 4269)
parcel_sf_subset_proj <- st_transform(parcels_sf_m, crs = 2272)

spatial_join_dt <- num_st_sfx_merge[num_pids != 1]
if (nrow(spatial_join_dt) > 0L) {
  spatial_join_sf <- spatial_join_dt[
    !is.na(GEOID.longitude) & !is.na(GEOID.latitude)
  ] %>%
    st_as_sf(coords = c("GEOID.longitude", "GEOID.latitude"), crs = 4269)

  spatial_join_result <- st_join(
    spatial_join_sf,
    parcel_sf_subset %>%
      rename(PID_spatial = PID) %>%
      select(PID_spatial, geometry)
  )

  spatial_join_sf_join <- as.data.table(spatial_join_result)[, geometry := NULL]
  spatial_join_sf_join[, merge := "spatial"]
  spatial_join_sf_join[, num_pids_st := uniqueN(PID_spatial, na.rm = TRUE), by = n_sn_ss_c]
  matched_spatial <- spatial_join_sf_join[num_pids_st == 1, unique(n_sn_ss_c)]
  logf("  Tier 4 unique matches: ", length(matched_spatial), log_file = log_file)
} else {
  spatial_join_sf_join <- data.table()
  matched_spatial <- character(0)
  logf("  No addresses remaining for spatial join", log_file = log_file)
}

logf("Tier 5: nearest parcel fallback for still-unmatched addresses (<= 50 ft)", log_file = log_file)
remaining_for_nearest <- nhpd_addys[
  !n_sn_ss_c %in% c(matched_num_st_sfx_dir_zip, matched_num_st, matched_num_st_sfx, matched_spatial)
]
if (nrow(remaining_for_nearest) > 0L) {
  nearest_pts <- remaining_for_nearest[
    !is.na(GEOID.longitude) & !is.na(GEOID.latitude)
  ] %>%
    st_as_sf(coords = c("GEOID.longitude", "GEOID.latitude"), crs = 4269) %>%
    st_transform(2272)

  nearest_idx <- st_nearest_feature(nearest_pts, parcel_sf_subset_proj)
  nearest_dist_ft <- as.numeric(st_distance(nearest_pts, parcel_sf_subset_proj[nearest_idx, ], by_element = TRUE))

  nearest_join_dt <- data.table(
    n_sn_ss_c = remaining_for_nearest[!is.na(GEOID.longitude) & !is.na(GEOID.latitude), n_sn_ss_c],
    PID = normalize_pid(parcel_sf_subset_proj$PID[nearest_idx]),
    merge = "nearest_50ft",
    nearest_dist_ft = nearest_dist_ft
  )
  nearest_join_dt <- nearest_join_dt[is.finite(nearest_dist_ft) & nearest_dist_ft <= 50]
  nearest_join_dt <- unique(nearest_join_dt, by = "n_sn_ss_c")
  matched_nearest <- nearest_join_dt[, unique(n_sn_ss_c)]
  logf("  Tier 5 unique matches: ", length(matched_nearest), log_file = log_file)
} else {
  nearest_join_dt <- data.table()
  matched_nearest <- character(0)
  logf("  No addresses remaining for nearest fallback", log_file = log_file)
}

logf("Building crosswalk...", log_file = log_file)
xwalk_unique_parts <- list(
  num_st_sfx_dir_zip_merge[num_pids == 1, .(PID, n_sn_ss_c, merge)],
  num_st_merge[num_pids == 1, .(PID, n_sn_ss_c, merge)],
  num_st_sfx_merge[num_pids == 1, .(PID, n_sn_ss_c, merge)]
)
if (nrow(spatial_join_sf_join) > 0L) {
  xwalk_unique_parts <- c(xwalk_unique_parts, list(
    spatial_join_sf_join[num_pids_st == 1, .(PID = PID_spatial, n_sn_ss_c, merge)]
  ))
}
if (nrow(nearest_join_dt) > 0L) {
  xwalk_unique_parts <- c(xwalk_unique_parts, list(
    nearest_join_dt[, .(PID, n_sn_ss_c, merge)]
  ))
}
xwalk_unique <- unique(rbindlist(xwalk_unique_parts, use.names = TRUE, fill = TRUE))
xwalk_unique[, unique := TRUE]

xwalk_non_unique_parts <- list(
  num_st_sfx_dir_zip_merge[num_pids > 1, .(PID, n_sn_ss_c, merge)],
  num_st_merge[num_pids > 1, .(PID, n_sn_ss_c, merge)],
  num_st_sfx_merge[num_pids > 1, .(PID, n_sn_ss_c, merge)]
)
if (nrow(spatial_join_sf_join) > 0L) {
  xwalk_non_unique_parts <- c(xwalk_non_unique_parts, list(
    spatial_join_sf_join[num_pids_st > 1 & !is.na(PID_spatial), .(PID = PID_spatial, n_sn_ss_c, merge)]
  ))
}
xwalk_non_unique <- unique(rbindlist(xwalk_non_unique_parts, use.names = TRUE, fill = TRUE))
xwalk_non_unique <- xwalk_non_unique[
  !n_sn_ss_c %in% xwalk_unique$n_sn_ss_c & !is.na(PID)
]
xwalk_non_unique[, unique := FALSE]

xwalk_unmatched <- data.table(
  n_sn_ss_c = nhpd_address_agg[
    !n_sn_ss_c %in% xwalk_unique$n_sn_ss_c & !n_sn_ss_c %in% xwalk_non_unique$n_sn_ss_c,
    n_sn_ss_c
  ],
  merge = "not_merged",
  PID = NA_character_,
  unique = FALSE
)

xwalk <- rbindlist(list(xwalk_unique, xwalk_non_unique, xwalk_unmatched), use.names = TRUE, fill = TRUE)
xwalk[, num_parcels_matched := uniqueN(PID, na.rm = TRUE), by = n_sn_ss_c]
xwalk[, num_addys_matched := uniqueN(n_sn_ss_c, na.rm = TRUE), by = PID]
setorder(xwalk, n_sn_ss_c, -unique, merge)

merge_summary <- unique(xwalk[, .(n_sn_ss_c, merge)], by = "n_sn_ss_c")[, .N, by = merge][order(-N)]
for (i in seq_len(nrow(merge_summary))) {
  logf("  ", merge_summary$merge[i], ": ", merge_summary$N[i], log_file = log_file)
}

property_xwalk <- merge(
  xwalk,
  nhpd[, .(nhpd_property_id, n_sn_ss_c)],
  by = "n_sn_ss_c",
  all.y = TRUE,
  allow.cartesian = TRUE
)
property_xwalk[, nhpd_property_id := as.character(nhpd_property_id)]
property_xwalk[, matched_unique := unique == TRUE & num_parcels_matched == 1L & !is.na(PID)]

unique_match_n <- property_xwalk[matched_unique == TRUE, uniqueN(nhpd_property_id)]
match_rate <- unique_match_n / uniqueN(nhpd$nhpd_property_id)
logf("  Unique property match rate: ", round(100 * match_rate, 1), "%", log_file = log_file)

matched_unique_pid <- unique(property_xwalk[matched_unique == TRUE, PID])
licensed_overlap_n <- sum(matched_unique_pid %chin% license_rental_pids)
logf(
  "  Unique matched PIDs also in rental-license data: ",
  licensed_overlap_n,
  " / ",
  length(matched_unique_pid),
  " (",
  round(100 * licensed_overlap_n / max(1L, length(matched_unique_pid)), 1),
  "%)",
  log_file = log_file
)

assert_unique(nhpd_address_agg, "n_sn_ss_c", "nhpd_address_agg output")

out_agg <- p_product(cfg, "nhpd_address_agg")
out_xwalk <- p_product(cfg, "nhpd_parcel_xwalk")
out_property <- p_product(cfg, "nhpd_parcel_xwalk_property")

fwrite(nhpd_address_agg, out_agg)
fwrite(xwalk, out_xwalk)
fwrite(property_xwalk, out_property)

logf("Wrote nhpd_address_agg: ", nrow(nhpd_address_agg), " rows to ", out_agg, log_file = log_file)
logf("Wrote nhpd_parcel_xwalk: ", nrow(xwalk), " rows to ", out_xwalk, log_file = log_file)
logf("Wrote nhpd_parcel_xwalk_property: ", nrow(property_xwalk), " rows to ", out_property, log_file = log_file)

logf("=== Finished merge-nhpd-parcels.R ===", log_file = log_file)
