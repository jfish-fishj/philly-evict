## ============================================================
## clean-corelogic-addresses.R
## ============================================================
## Purpose: Clean and standardize CoreLogic property addresses
##          for Philadelphia county (FIPS 42101) to match the
##          pm.* naming convention for merging with other data.
##
## Inputs:
##   - cfg$inputs$corelogic_philly_raw (corelogic/filtered_42101.csv)
##
## Outputs:
##   - cfg$products$corelogic_clean (clean/corelogic_clean.csv)
##
## Primary key: pm.uid (unique address ID based on street_address_lower)
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(postmastr)
  library(bit64)
  library(janitor)
})

# ---- Load config and set up logging ----
source("r/config.R")
source("r/lib/address_utils.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "clean-corelogic-addresses.log")

logf("=== Starting clean-corelogic-addresses.R ===", log_file = log_file)
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

input_path <- p_input(cfg, "corelogic_philly_raw")
logf("Loading raw CoreLogic data from: ", input_path, log_file = log_file)

# First read column names to select relevant columns
corelogic_cols <- fread(input_path, nrows = 1) %>%
  select(
    CLIP, `FIPS CODE`, `APN (PARCEL NUMBER UNFORMATTED)`, `ORIGINAL APN`,
    RANGE, `CENSUS ID`, TOWNSHIP, `MUNICIPALITY NAME`, `MUNICIPALITY CODE`,
    `SITUS CORE BASED STATISTICAL AREA (CBSA)`:`SITUS STREET ADDRESS`,
    `TOTAL VALUE CALCULATED`:`TAX EXEMPT AMOUNT TOTAL`,
    `FRONT FOOTAGE`:last_col()
  ) %>%
  colnames()

corelogic <- fread(input_path, select = corelogic_cols)
corelogic <- corelogic %>% janitor::clean_names()

logf("  Loaded ", nrow(corelogic), " rows, ", ncol(corelogic), " columns", log_file = log_file)

# ============================================================
# ADDRESS STANDARDIZATION
# ============================================================

logf("Standardizing address columns to pm.* format...", log_file = log_file)

# CoreLogic comes pre-parsed, so we skip most of the usual parsing
# Just standardize to match the pm.* naming convention

# Clean and lowercase the full street address
corelogic[, street_address_lower := str_to_lower(situs_street_address) %>% misc_pre_processing()]

# Zip code - imputed with underscore prefix for matching
corelogic[, pm.zip_imp := paste0("_", str_pad(situs_zip_code, 5, "left", "0"))]

# City - lowercase and clean
corelogic[, pm.city_imp := str_to_lower(situs_city) %>% str_squish()]

# State - lowercase and clean
corelogic[, pm.state_imp := str_to_lower(situs_state) %>% str_squish()]

# Create unique address ID based on cleaned street address
corelogic[, pm.uid := .GRP, by = street_address_lower]

# Copy address for reference
corelogic[, pm.address := street_address_lower]

# House number from CoreLogic's pre-parsed field
corelogic[, pm.house := situs_house_number]

# Street name - lowercase and post-process
corelogic[, pm.street := situs_street_name %>% str_to_lower() %>% misc_post_processing()]

# Street suffix - standardize using postmastr
corelogic[, pm.streetSuf := standardize_st_name(situs_mode)]

# Street direction (CoreLogic only has pre-direction in situs_direction)
corelogic[, pm.preDir := situs_direction]
corelogic[, pm.sufDir := NA_character_]
corelogic[, pm.dir_concat := situs_direction]

# ============================================================
# Step 5b: Oracle-validated canonicalization (corelogic)
# ============================================================

# Use reusable canonicalization function from address_utils.R
corelogic <- canonicalize_parsed_addresses(
  corelogic,
  cfg,
  source = "corelogic",
  export_qa = TRUE,
  log_file = log_file
)

# Composite address string for matching
corelogic[, n_sn_ss_c := str_squish(str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf)))]

logf("  Created ", corelogic[, uniqueN(pm.uid)], " unique address IDs", log_file = log_file)

# ============================================================
# ASSERTIONS
# ============================================================

logf("Running assertions...", log_file = log_file)

# Check required columns exist
required_cols <- c("pm.uid", "pm.house", "pm.street", "n_sn_ss_c", "clip")
missing_cols <- setdiff(required_cols, names(corelogic))
if (length(missing_cols) > 0) {
  stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
}
logf("  Required columns: PASS", log_file = log_file)

# Check pm.uid was created properly
n_uid <- corelogic[, uniqueN(pm.uid)]
if (n_uid == 0) {
  stop("pm.uid creation failed - no unique IDs generated")
}
logf("  pm.uid creation: PASS (", n_uid, " unique addresses)", log_file = log_file)

# Log address parsing stats
n_missing_address <- corelogic[is.na(n_sn_ss_c) | n_sn_ss_c == "", .N]
logf("  Rows with missing composite address: ", n_missing_address,
     " (", round(100 * n_missing_address / nrow(corelogic), 2), "%)", log_file = log_file)

# ============================================================
# WRITE OUTPUT
# ============================================================

out_path <- p_product(cfg, "corelogic_clean")
logf("Writing output to: ", out_path, log_file = log_file)

# Ensure output directory exists
dir.create(dirname(out_path), showWarnings = FALSE, recursive = TRUE)

fwrite(corelogic, out_path)
logf("  Wrote ", nrow(corelogic), " rows, ", ncol(corelogic), " columns", log_file = log_file)

logf("=== Finished clean-corelogic-addresses.R ===", log_file = log_file)
