## ============================================================
## clean-parcel-addresses.R
## ============================================================
## Purpose: Clean and standardize OPA parcel addresses
##
## Inputs:
##   - cfg$inputs$opa_gdb (parcels/opa_properties_public.gdb)
##
## Outputs:
##   - cfg$products$parcels_clean (clean/parcels_clean.csv)
##
## Primary key: parcel_number (OPA account number / PID)
##
## Workflow:
##   1. Load OPA geodatabase
##   2. Extract coordinates and convert to data.table
##   3. Parse address components using postmastr
##   4. Standardize street names, directions, suffixes
##   5. Create composite address key (n_sn_ss_c)
##   6. Export cleaned data
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(sf)
  library(postmastr)
})

# ---- Load config and set up logging ----
source("r/config.R")
source("r/lib/address_utils.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "clean-parcel-addresses.log")

logf("=== Starting clean-parcel-addresses.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Load input data ----
input_path <- p_input(cfg, "opa_gdb")
logf("Loading: ", input_path, log_file = log_file)

philly_sf <- read_sf(input_path)
logf("Loaded ", nrow(philly_sf), " parcels", log_file = log_file)

# Transform and extract coordinates
logf("Transforming coordinates to EPSG:4326", log_file = log_file)
philly_sf_dt <- philly_sf %>%
  st_transform("EPSG:4326") %>%
  mutate(
    geocode_x = st_coordinates(.)[, 1],
    geocode_y = st_coordinates(.)[, 2]
  ) %>%
  select(-SHAPE) %>%
  as.data.table()

logf("Converted to data.table: ", nrow(philly_sf_dt), " rows", log_file = log_file)

# ---- Set up postmastr dictionaries ----
dirs <- pm_dictionary(type = "directional", filter = c("N", "S", "E", "W"), locale = "us")
pa <- pm_dictionary(type = "state", filter = "PA", case = c("title", "upper", "lower"), locale = "us")
philly <- tibble(city.input = "philadelphia")

# ---- Step 1: Identify addresses ----
logf("Step 1: Identifying addresses", log_file = log_file)

philly_sf_dt_sample <- philly_sf_dt %>%
  pm_identify(var = location)

logf("Identified ", nrow(philly_sf_dt_sample), " addresses", log_file = log_file)

# ---- Step 2: Postmastr parsing ----
logf("Step 2: Postmastr address parsing", log_file = log_file)

philly_sf_dt_adds_sample <- pm_prep(philly_sf_dt_sample, var = "location", type = "street")
logf("After pm_prep: ", nrow(philly_sf_dt_adds_sample), " rows", log_file = log_file)

philly_sf_dt_adds_sample <- pm_postal_parse(philly_sf_dt_adds_sample, locale = "us")
philly_sf_dt_adds_sample <- pm_state_parse(philly_sf_dt_adds_sample, dictionary = pa)
philly_sf_dt_adds_sample <- pm_city_parse(philly_sf_dt_adds_sample, dictionary = philly)

# ---- Step 3: Unit parsing (multiple rounds) ----
logf("Step 3: Unit parsing", log_file = log_file)

philly_sf_dt_adds_sample_units <- parse_unit(philly_sf_dt_adds_sample$pm.address)
philly_sf_dt_adds_sample$pm.address <- philly_sf_dt_adds_sample_units$clean_address %>% str_squish()

philly_sf_dt_adds_sample_units_r1 <- parse_unit(philly_sf_dt_adds_sample$pm.address)
philly_sf_dt_adds_sample_units_r2 <- parse_unit(philly_sf_dt_adds_sample_units_r1$clean_address %>% str_squish())
philly_sf_dt_adds_sample_units_r3 <- parse_unit_extra(philly_sf_dt_adds_sample_units_r2$clean_address %>% str_squish())

philly_sf_dt_adds_sample$pm.address <- philly_sf_dt_adds_sample_units_r3$clean_address %>% str_squish()
philly_sf_dt_adds_sample$pm.unit <- philly_sf_dt_adds_sample_units_r3$unit

# ---- Step 4: House number and street parsing ----
logf("Step 4: House number and street parsing", log_file = log_file)

philly_sf_dt_adds_sample <- pm_house_parse(philly_sf_dt_adds_sample)
philly_sf_dt_adds_sample_nums <- philly_sf_dt_adds_sample$pm.house %>% parse_letter()
philly_sf_dt_adds_sample_range <- philly_sf_dt_adds_sample_nums$clean_address %>% parse_range()

new_pm_house <- coalesce(philly_sf_dt_adds_sample_range$range1, philly_sf_dt_adds_sample_range$og_address)
philly_sf_dt_adds_sample$pm.house <- new_pm_house %>% as.numeric()
philly_sf_dt_adds_sample$pm.house2 <- philly_sf_dt_adds_sample_range$range2 %>% as.numeric()
philly_sf_dt_adds_sample$pm.house.letter <- philly_sf_dt_adds_sample_nums$letter

philly_sf_dt_adds_sample <- pm_streetDir_parse(philly_sf_dt_adds_sample, dictionary = dirs)
philly_sf_dt_adds_sample <- pm_streetSuf_parse(philly_sf_dt_adds_sample)

philly_sf_dt_adds_sample <- pm_street_parse(philly_sf_dt_adds_sample, ordinal = TRUE, drop = FALSE)

# ---- Step 5: Street name standardization ----
logf("Step 5: Street name standardization", log_file = log_file)

philly_sf_dt_adds_sample <- philly_sf_dt_adds_sample %>% mutate(
  pm.street = case_when(
    str_detect(pm.street, "[a-zA-Z]") ~ pm.street,
    pm.street == 1 ~ "1st",
    pm.street == 2 ~ "2nd",
    pm.street == 3 ~ "3rd",
    (pm.street) >= 4 ~ paste0((pm.street), "th"),
    TRUE ~ pm.street
  )
)

# Fix known misspellings
philly_sf_dt_adds_sample <- philly_sf_dt_adds_sample %>% mutate(
  pm.street = case_when(
    pm.street == "berkeley" ~ "berkley",
    TRUE ~ pm.street
  )
)

# Expand abbreviations
philly_sf_dt_adds_sample <- philly_sf_dt_adds_sample %>% mutate(
  pm.street = pm.street %>%
    str_replace_all("[Mm]t ", "mount ") %>%
    str_replace_all("[Mm]c ", "mc") %>%
    str_replace_all("[Ss]t ", "saint ")
)

# ---- Step 6: Create composite address key ----
logf("Step 6: Creating composite address key", log_file = log_file)

philly_sf_dt_adds_sample_c <- philly_sf_dt_adds_sample %>%
  mutate(across(c(pm.sufDir, pm.street, pm.streetSuf, pm.preDir), ~str_squish(str_to_lower(replace_na(.x, ""))))) %>%
  mutate(
    n_sn_ss_c = str_squish(str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir))),
    pm.dir_concat = coalesce(pm.preDir, pm.sufDir)
  )

setDT(philly_sf_dt_adds_sample_c)
setDT(philly_sf_dt_sample)

# ---- Step 7: Merge and finalize ----
logf("Step 7: Merging parsed addresses back to original data", log_file = log_file)

philly_sf_dt_adds_sample_m <- merge(
  philly_sf_dt_sample,
  philly_sf_dt_adds_sample_c,
  by.x = "pm.uid",
  by.y = "pm.uid",
  all.x = TRUE
)

# Add "_" prefix to pm.zip to ensure character type on read (e.g., "_19104")
setDT(philly_sf_dt_adds_sample_m)
philly_sf_dt_adds_sample_m[, pm.zip := {
  raw_zip <- coalesce(zip_code, pm.zip)
  fifelse(
    !is.na(raw_zip) & nchar(as.character(raw_zip)) >= 4,
    paste0("_", str_pad(as.character(raw_zip), 5, "left", "0")),
    NA_character_
  )
}]

logf("After merge: ", nrow(philly_sf_dt_adds_sample_m), " rows", log_file = log_file)

# ---- Assertions ----
logf("Running assertions...", log_file = log_file)

n_total <- nrow(philly_sf_dt_adds_sample_m)
n_unique_parcel <- philly_sf_dt_adds_sample_m[, uniqueN(parcel_number)]
logf("Total rows: ", n_total, ", Unique parcel_number: ", n_unique_parcel, log_file = log_file)

# Check key columns exist
required_cols <- c("parcel_number", "n_sn_ss_c", "pm.house", "pm.street", "geocode_x", "geocode_y")
missing_cols <- setdiff(required_cols, names(philly_sf_dt_adds_sample_m))
if (length(missing_cols) > 0) {
  logf("WARNING: Missing columns: ", paste(missing_cols, collapse = ", "), log_file = log_file)
}

# Log address parsing success rate
n_with_address <- philly_sf_dt_adds_sample_m[!is.na(n_sn_ss_c) & nzchar(n_sn_ss_c), .N]
logf("Addresses parsed: ", n_with_address, " (", round(100 * n_with_address / n_total, 1), "%)", log_file = log_file)

# ---- Write output ----
output_path <- p_product(cfg, "parcels_clean")
logf("Writing output to: ", output_path, log_file = log_file)

fwrite(philly_sf_dt_adds_sample_m, output_path)

logf("Wrote ", nrow(philly_sf_dt_adds_sample_m), " rows to ", output_path, log_file = log_file)
logf("=== Finished clean-parcel-addresses.R ===", log_file = log_file)

# write address counts
addr_counts <- philly_sf_dt_adds_sample_m[, .N, by = .(pm.street, pm.streetSuf)][order(-N)]
addr_counts_path <- p_out(cfg, "logs", "parcel_address_counts")
fwrite(addr_counts, addr_counts_path)
logf("Wrote address counts to: ", addr_counts_path, log_file = log_file)
