## ============================================================
## build-acs-files.r
## ============================================================
## Purpose: Fetch census/ACS data from the Census API and write
##          all census input files used by the pipeline.
##
## This is a one-time utility script -- run once to populate:
##   - data/inputs/census/
##   - data/inputs/shapefiles/philly_bgs/
##
## Requires: tidycensus::census_api_key("YOUR_KEY", install = TRUE)
##
## Outputs:
##   - hu_blocks_2000, hu_blocks_2010  (block-level total housing units)
##   - tenure_bg_2000, tenure_bg_2010  (BG tenure from decennial)
##   - uis_tr_2000, uis_tr_2010        (tract units-in-structure)
##   - bg_2000, bg_2010                (BG tenure + tract UIS merged)
##   - philly_bg_sf (as GeoPackage under shapefiles/philly_bgs/)
## ============================================================

suppressPackageStartupMessages({
  library(tidycensus)
  library(tigris)
  library(dplyr)
  library(stringr)
  library(sf)
  library(data.table)
})

# ---- Load config and set up logging ----
source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "build-acs-files.log")
logf("=== Starting build-acs-files.r ===", log_file = log_file)

options(tigris_use_cache = TRUE)

state_abbr  <- "PA"
county_name <- "Philadelphia"

# ---- Output locations (per your request) ---------------------------------
census_dir <- file.path("data", "inputs", "census")
shp_dir    <- file.path("data", "inputs", "shapefiles", "philly_bgs")

dir.create(census_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(shp_dir, recursive = TRUE, showWarnings = FALSE)

# --- helpers ---------------------------------------------------------------
is_all_na_payload <- function(df) {
  num_cols <- names(df)[vapply(df, is.numeric, logical(1))]
  if (!length(num_cols)) return(TRUE)
  all(vapply(df[num_cols], function(x) all(is.na(x)), logical(1)))
}

validate_nonmissing <- function(df, cols, name) {
  for (cl in cols) {
    if (!(cl %in% names(df))) stop(name, ": missing expected column ", cl)
    if (all(is.na(df[[cl]]))) stop(name, ": column ", cl, " is all NA")
  }
  TRUE
}

add_tract_id <- function(df_bg) {
  df_bg %>% mutate(tract_geoid = str_sub(GEOID, 1, 11))
}

# robust decennial getter (2010 tables sometimes drop leading zeros)
# also handles 2010 block queries which often require tract nesting
grab_decennial <- function(year, geography, table_try, sumfile = "sf1", ...) {
  extra <- list(...)

  # Special-case: 2010 SF1 blocks frequently require tract nesting
  if (year == 2010 && geography == "block" && is.null(extra$tract)) {
    extra$tract <- "*"
  }

  try1 <- try(
    do.call(get_decennial, c(list(
      year = year,
      geography = geography,
      table = table_try,
      sumfile = sumfile,
      state = state_abbr,
      county = county_name,
      output = "wide",
      cache_table = TRUE
    ), extra)),
    silent = TRUE
  )

  if (!inherits(try1, "try-error") && nrow(try1) && !is_all_na_payload(try1)) return(try1)

  # try zero-padded variant if needed (e.g., H1 vs H001, H4 vs H004)
  table_alt <- ifelse(
    str_detect(table_try, "^H\\d$"),
    str_replace(table_try, "^H", "H00"),
    ifelse(
      str_detect(table_try, "^H\\d\\d$"),
      str_replace(table_try, "^H", "H0"),
      table_try
    )
  )

  if (!identical(table_alt, table_try)) {
    try2 <- try(
      do.call(get_decennial, c(list(
        year = year,
        geography = geography,
        table = table_alt,
        sumfile = sumfile,
        state = state_abbr,
        county = county_name,
        output = "wide",
        cache_table = TRUE
      ), extra)),
      silent = TRUE
    )

    if (!inherits(try2, "try-error") && nrow(try2) && !is_all_na_payload(try2)) return(try2)
  }

  stop(sprintf(
    "No usable decennial data returned for table '%s' (%s, %s).",
    table_try, geography, year
  ))
}

# --- 1) TOTAL HOUSING UNITS @ BLOCK (decennial) ---------------------------
logf("Fetching housing units at block level...", log_file = log_file)

hu_blocks_2000 <- grab_decennial(2000, "block", "H001") %>%
  rename(hu_total = H001001) %>%
  mutate(year = 2000, .before = 1)

hu_blocks_2010 <- grab_decennial(2010, "block", "H1") %>%
  rename(hu_total = H001001) %>%
  mutate(year = 2010, .before = 1)

logf("  hu_blocks_2000: ", nrow(hu_blocks_2000), " rows", log_file = log_file)
logf("  hu_blocks_2010: ", nrow(hu_blocks_2010), " rows", log_file = log_file)

validate_nonmissing(hu_blocks_2000, c("GEOID", "hu_total"), "hu_blocks_2000")
validate_nonmissing(hu_blocks_2010, c("GEOID", "hu_total"), "hu_blocks_2010")

# --- 2) TENURE @ BLOCK-GROUP (decennial) ----------------------------------
logf("Fetching tenure at block-group level...", log_file = log_file)

# 2000: H004 (Tenure) ; 2010: H4 (Tenure)
tenure_bg_2000 <- grab_decennial(2000, "block group", "H004") %>%
  rename(occ_total = H004001, owner_occ = H004002, renter_occ = H004003) %>%
  mutate(year = 2000, .before = 1)

tenure_bg_2010 <- grab_decennial(2010, "block group", "H4") %>%
  rename(occ_total = H004001, renter_occ = H004004) %>%
  mutate(owner_occ = H004002 + H004003) |>
  # drop H00 cols
  select(-matches("^H00")) %>%
  mutate(year = 2010, .before = 1)

logf("  tenure_bg_2000: ", nrow(tenure_bg_2000), " rows", log_file = log_file)
logf("  tenure_bg_2010: ", nrow(tenure_bg_2010), " rows", log_file = log_file)

validate_nonmissing(tenure_bg_2000, c("GEOID", "occ_total", "owner_occ", "renter_occ"), "tenure_bg_2000")
validate_nonmissing(tenure_bg_2010, c("GEOID", "occ_total", "owner_occ", "renter_occ"), "tenure_bg_2010")

# --- 3) UNITS IN STRUCTURE @ TRACT ----------------------------------------
logf("Fetching units-in-structure at tract level...", log_file = log_file)

# --- 3) UNITS IN STRUCTURE @ TRACT ----------------------------------------
logf("Fetching units-in-structure at tract level...", log_file = log_file)

# 2000: SF3 (sample / long form) table H030 at tract
# Pull variables directly (avoids table/group quirks)
uis_vars_2000 <- sprintf("H030%03d", 1:11)

uis_tr_2000 <- get_decennial(
  year = 2000,
  sumfile = "sf3",
  geography = "tract",
  variables = uis_vars_2000,
  state = state_abbr,
  county = county_name,
  output = "wide",
  cache_table = TRUE
) %>%
  rename(
    hu_total        = H030001,
    u_1_detached    = H030002,
    u_1_attached    = H030003,
    u_2_units       = H030004,
    u_3_4_units     = H030005,
    u_5_9_units     = H030006,
    u_10_19_units   = H030007,
    u_20_49_units   = H030008,
    u_50plus_units  = H030009,
    u_mobile_home   = H030010,
    u_other         = H030011
  ) %>%
  mutate(year = 2000, .before = 1)

logf("  uis_tr_2000: ", nrow(uis_tr_2000), " rows", log_file = log_file)
stopifnot(nrow(uis_tr_2000) > 0)
stopifnot(!all(is.na(uis_tr_2000$hu_total)))


# 2010 "era": ACS 2006-2010 (acs5 year=2010) at tract
# IMPORTANT: output="wide" returns ...E and ...M columns; rename from ...E
uis_tr_2010 <- get_acs(
  year = 2010, survey = "acs5",
  geography = "tract",
  state = state_abbr, county = county_name,
  table = "B25024", output = "wide"
) %>%
  rename(
    hu_total        = B25024_001E,
    u_1_detached    = B25024_002E,
    u_1_attached    = B25024_003E,
    u_2_units       = B25024_004E,
    u_3_4_units     = B25024_005E,
    u_5_9_units     = B25024_006E,
    u_10_19_units   = B25024_007E,
    u_20_49_units   = B25024_008E,
    u_50plus_units  = B25024_009E,
    u_mobile_home   = B25024_010E,
    u_boat_rv_other = B25024_011E
  ) %>%
  select(-matches("M$")) %>%   # drop MOE columns only
  mutate(year = 2010, .before = 1)

logf("  uis_tr_2000: ", nrow(uis_tr_2000), " rows", log_file = log_file)
logf("  uis_tr_2010: ", nrow(uis_tr_2010), " rows", log_file = log_file)

# --- 4) Merge tenure + UIS -> combined BG files ---------------------------
logf("Merging tenure + units-in-structure at BG level...", log_file = log_file)

tenure_bg_2000 <- add_tract_id(tenure_bg_2000)
tenure_bg_2010 <- add_tract_id(tenure_bg_2010)

uis_tr_2000 <- uis_tr_2000 %>% rename(tract_geoid = GEOID)
uis_tr_2010 <- uis_tr_2010 %>% rename(tract_geoid = GEOID)

# Integrity checks: ensure join keys are unique
stopifnot(!anyDuplicated(tenure_bg_2000$GEOID))
stopifnot(!anyDuplicated(tenure_bg_2010$GEOID))
stopifnot(!anyDuplicated(uis_tr_2000$tract_geoid))
stopifnot(!anyDuplicated(uis_tr_2010$tract_geoid))

bg_2000 <- tenure_bg_2000 %>%
  left_join(
    uis_tr_2000 %>% select(tract_geoid, starts_with("u_"), hu_total),
    by = "tract_geoid"
  )

bg_2010 <- tenure_bg_2010 %>%
  left_join(
    uis_tr_2010 %>% select(tract_geoid, starts_with("u_"), hu_total),
    by = "tract_geoid"
  )

logf("  bg_2000: ", nrow(bg_2000), " rows", log_file = log_file)
logf("  bg_2010: ", nrow(bg_2010), " rows", log_file = log_file)

# Join coverage logs
logf("  bg_2000 tract match rate (hu_total non-missing): ",
     round(mean(!is.na(bg_2000$hu_total)), 4), log_file = log_file)
logf("  bg_2010 tract match rate (hu_total non-missing): ",
     round(mean(!is.na(bg_2010$hu_total)), 4), log_file = log_file)

validate_nonmissing(uis_tr_2000, c("tract_geoid", "hu_total"), "uis_tr_2000")
validate_nonmissing(uis_tr_2010, c("tract_geoid", "hu_total"), "uis_tr_2010")
validate_nonmissing(bg_2000, c("GEOID", "tract_geoid", "occ_total", "hu_total"), "bg_2000")
validate_nonmissing(bg_2010, c("GEOID", "tract_geoid", "occ_total", "hu_total"), "bg_2010")

# --- 5) BLOCK GROUP SHAPEFILE ---------------------------------------------
logf("Fetching block group geometry from tigris...", log_file = log_file)

philly_bg_sf <- block_groups(state = state_abbr, county = county_name, year = 2010, cb = TRUE)
logf("  philly_bg_sf: ", nrow(philly_bg_sf), " block groups", log_file = log_file)
stopifnot(inherits(philly_bg_sf, "sf"))
stopifnot(nrow(philly_bg_sf) > 0)

# --- 6) WRITE OUTPUTS ------------------------------------------------------
logf("Writing outputs...", log_file = log_file)

# Census tables -> data/inputs/census/
fwrite(hu_blocks_2000, file.path(census_dir, "hu_blocks_2000.csv"))
logf("  Wrote hu_blocks_2000: ", nrow(hu_blocks_2000), " rows",
     log_file = log_file)

fwrite(hu_blocks_2010, file.path(census_dir, "hu_blocks_2010.csv"))
logf("  Wrote hu_blocks_2010: ", nrow(hu_blocks_2010), " rows",
     log_file = log_file)

fwrite(tenure_bg_2000, file.path(census_dir, "tenure_bg_2000.csv"))
logf("  Wrote tenure_bg_2000: ", nrow(tenure_bg_2000), " rows",
     log_file = log_file)

fwrite(tenure_bg_2010, file.path(census_dir, "tenure_bg_2010.csv"))
logf("  Wrote tenure_bg_2010: ", nrow(tenure_bg_2010), " rows",
     log_file = log_file)

fwrite(uis_tr_2000, file.path(census_dir, "uis_tr_2000.csv"))
logf("  Wrote uis_tr_2000: ", nrow(uis_tr_2000), " rows",
     log_file = log_file)

fwrite(uis_tr_2010, file.path(census_dir, "uis_tr_2010.csv"))
logf("  Wrote uis_tr_2010: ", nrow(uis_tr_2010), " rows",
     log_file = log_file)

fwrite(bg_2000, file.path(census_dir, "bg_2000.csv"))
logf("  Wrote bg_2000: ", nrow(bg_2000), " rows",
     log_file = log_file)

fwrite(bg_2010, file.path(census_dir, "bg_2010.csv"))
logf("  Wrote bg_2010: ", nrow(bg_2010), " rows",
     log_file = log_file)

# Geometry -> its own subfolder under shapefiles/
# Write as a single-file GeoPackage (more reliable than .shp sidecars)
gpkg_path <- file.path(shp_dir, "philly_block_groups_2010_cb.gpkg")
if (file.exists(gpkg_path)) file.remove(gpkg_path)
st_write(philly_bg_sf, gpkg_path, quiet = TRUE)
logf("  Wrote philly_bgs GeoPackage: ", nrow(philly_bg_sf), " features to ",
     gpkg_path, log_file = log_file)

# now write as shp
shp_path <- file.path(shp_dir, "philly_block_groups_2010_cb.shp")
if (file.exists(shp_path)) {
  file.remove(list.files(shp_dir, pattern = "philly_block_groups_2010_cb.*", full.names = TRUE))
}
st_write(philly_bg_sf, shp_path, quiet = TRUE)
logf("  Wrote philly_bgs Shapefile: ", nrow(philly_bg_sf), " features to ",
     shp_path, log_file = log_file)


logf("=== Finished build-acs-files.r ===", log_file = log_file)
