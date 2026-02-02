## ============================================================
## clean-license-addresses.R
## ============================================================
## Purpose: Clean and standardize business license addresses
##
## Inputs:
##   - cfg$inputs$open_data_business_licenses (open-data/business_licenses.csv)
##
## Outputs:
##   - cfg$products$licenses_clean (clean/licenses_clean.csv)
##
## Primary key: pm.uid (postmastr-assigned unique ID)
##
## Workflow:
##   1. Load raw business license data
##   2. Parse address components using postmastr
##   3. Standardize street names, directions, suffixes
##   4. Create composite address key (n_sn_ss_c)
##   5. Export cleaned data
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(postmastr)
})

# ---- Load config and set up logging ----
source("r/config.R")
source("r/lib/address_utils.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "clean-license-addresses.log")

logf("=== Starting clean-license-addresses.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Load input data ----
input_path <- p_input(cfg, "open_data_business_licenses")
logf("Loading: ", input_path, log_file = log_file)

philly_lic <- fread(input_path)
logf("Loaded ", nrow(philly_lic), " rows", log_file = log_file)

# ---- Set up postmastr dictionaries ----
dirs <- pm_dictionary(type = "directional", filter = c("N", "S", "E", "W"), locale = "us")
pa <- pm_dictionary(type = "state", filter = "PA", case = c("title", "upper", "lower"), locale = "us")
philly <- tibble(city.input = "philadelphia")

# ---- Step 1: Initial address cleaning ----
logf("Step 1: Initial address cleaning", log_file = log_file)

philly_lic[, address := str_remove_all(address, ", philadelphia, pa")]

philly_lic_sample <- philly_lic %>%
  pm_identify(var = address)

logf("Identified ", nrow(philly_lic_sample), " addresses", log_file = log_file)

# ---- Step 2: Postmastr parsing ----
logf("Step 2: Postmastr address parsing", log_file = log_file)

philly_lic_adds_sample <- pm_prep(philly_lic_sample, var = "address", type = "street")
logf("After pm_prep: ", nrow(philly_lic_adds_sample), " rows", log_file = log_file)

philly_lic_adds_sample <- pm_postal_parse(philly_lic_adds_sample, locale = "us")
philly_lic_adds_sample <- pm_state_parse(philly_lic_adds_sample, dictionary = pa)
philly_lic_adds_sample <- pm_city_parse(philly_lic_adds_sample, dictionary = philly)

# ---- Step 3: Unit parsing (multiple rounds) ----
logf("Step 3: Unit parsing", log_file = log_file)

philly_lic_adds_sample_units <- parse_unit(philly_lic_adds_sample$pm.address)
philly_lic_adds_sample$pm.address <- philly_lic_adds_sample_units$clean_address %>% str_squish()

philly_lic_adds_sample_units_r1 <- parse_unit(philly_lic_adds_sample$pm.address)
philly_lic_adds_sample_units_r2 <- parse_unit(philly_lic_adds_sample_units_r1$clean_address %>% str_squish())
philly_lic_adds_sample_units_r3 <- parse_unit_extra(philly_lic_adds_sample_units_r2$clean_address %>% str_squish())

philly_lic_adds_sample$pm.address <- philly_lic_adds_sample_units_r3$clean_address %>% str_squish()
philly_lic_adds_sample$pm.unit <- philly_lic_adds_sample_units_r3$unit

# ---- Step 4: House number and street parsing ----
logf("Step 4: House number and street parsing", log_file = log_file)

philly_lic_adds_sample <- pm_house_parse(philly_lic_adds_sample)
philly_lic_adds_sample_nums <- philly_lic_adds_sample$pm.house %>% parse_letter()
philly_lic_adds_sample_range <- philly_lic_adds_sample_nums$clean_address %>% parse_range()

new_pm_house <- coalesce(philly_lic_adds_sample_range$range1, philly_lic_adds_sample_range$og_address)
philly_lic_adds_sample$pm.house <- new_pm_house %>% as.numeric()
philly_lic_adds_sample$pm.house2 <- philly_lic_adds_sample_range$range2 %>% as.numeric()
philly_lic_adds_sample$pm.house.letter <- philly_lic_adds_sample_nums$letter

philly_lic_adds_sample <- pm_streetDir_parse(philly_lic_adds_sample, dictionary = dirs)
philly_lic_adds_sample <- pm_streetSuf_parse(philly_lic_adds_sample)

# Peel off trailing letters before street parse
philly_lic_adds_sample <- philly_lic_adds_sample %>% mutate(
  pm.address = str_remove_all(pm.address, "(\\s[a-z]{1}|1st|2nd)$")
)

philly_lic_adds_sample <- pm_street_parse(philly_lic_adds_sample, ordinal = TRUE, drop = FALSE)

# ============================================================
# Step 4b: Oracle-validated canonicalization (licenses)
# ============================================================

setDT(philly_lic_adds_sample)

# Use reusable canonicalization function from address_utils.R
philly_lic_adds_sample <- canonicalize_parsed_addresses(
  philly_lic_adds_sample,
  cfg,
  source = "licenses",
  export_qa = TRUE,
  log_file = log_file
)

# ---- Step 5: Create composite address key ----
logf("Step 5: Creating composite address key", log_file = log_file)

philly_lic_adds_sample_c <- philly_lic_adds_sample %>%
  mutate(across(c(pm.sufDir, pm.street, pm.streetSuf, pm.preDir), ~str_squish(str_to_lower(replace_na(.x, ""))))) %>%
  mutate(
    n_sn_ss_c = str_squish(str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir))),
    pm.dir_concat = coalesce(pm.preDir, pm.sufDir)
  )

setDT(philly_lic_adds_sample_c)
setDT(philly_lic_sample)

# ---- Step 6: Merge and finalize ----
logf("Step 6: Merging parsed addresses back to original data", log_file = log_file)

philly_lic_sample_m <- merge(
  philly_lic_sample,
  philly_lic_adds_sample_c,
  by = "pm.uid",
  all.x = TRUE
)

logf("After merge: ", nrow(philly_lic_sample_m), " rows", log_file = log_file)

# Add "_" prefix to pm.zip to ensure character type on read (e.g., "_19104")
setDT(philly_lic_sample_m)
philly_lic_sample_m[, pm.zip := {
  raw_zip <- pm.zip
  fifelse(
    !is.na(raw_zip) & nchar(as.character(raw_zip)) >= 4,
    paste0("_", str_pad(as.character(raw_zip), 5, "left", "0")),
    NA_character_
  )
}]

# ---- Assertions ----
logf("Running assertions...", log_file = log_file)

n_total <- nrow(philly_lic_sample_m)
n_unique_uid <- philly_lic_sample_m[, uniqueN(pm.uid)]
logf("Total rows: ", n_total, ", Unique pm.uid: ", n_unique_uid, log_file = log_file)

# Check key columns exist
required_cols <- c("pm.uid", "n_sn_ss_c", "pm.house", "pm.street", "licensetype")
missing_cols <- setdiff(required_cols, names(philly_lic_sample_m))
if (length(missing_cols) > 0) {
  logf("WARNING: Missing columns: ", paste(missing_cols, collapse = ", "), log_file = log_file)
}

# Log address parsing success rate
n_with_address <- philly_lic_sample_m[!is.na(n_sn_ss_c) & nzchar(n_sn_ss_c), .N]
logf("Addresses parsed: ", n_with_address, " (", round(100 * n_with_address / n_total, 1), "%)", log_file = log_file)

# ---- Write output ----
output_path <- p_product(cfg, "licenses_clean")
logf("Writing output to: ", output_path, log_file = log_file)

fwrite(philly_lic_sample_m, output_path)

logf("Wrote ", nrow(philly_lic_sample_m), " rows to ", output_path, log_file = log_file)
logf("=== Finished clean-license-addresses.R ===", log_file = log_file)
