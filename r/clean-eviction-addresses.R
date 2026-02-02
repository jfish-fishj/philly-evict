## ============================================================
## clean-eviction-addresses.R
## ============================================================
## Purpose: Clean and standardize eviction filing addresses
##
## Inputs:
##   - cfg$inputs$evictions_summary_table (phila-lt-data/summary-table.txt)
##
## Outputs:
##   - cfg$products$evictions_clean (clean/evictions_clean.csv)
##
## Primary key: pm.uid (unique eviction filing ID)
##
## Workflow:
##   1. Load raw eviction summary data
##   2. Extract short address from defendant_address
##   3. Parse address components using postmastr
##   4. Standardize street names, directions, suffixes
##   5. Create composite address key (n_sn_ss_c)
##   6. Export cleaned data
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
log_file <- p_out(cfg, "logs", "clean-eviction-addresses.log")

logf("=== Starting clean-eviction-addresses.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Load input data ----
input_path <- p_input(cfg, "evictions_summary_table")
logf("Loading: ", input_path, log_file = log_file)

philly_evict <- fread(input_path)
logf("Loaded ", nrow(philly_evict), " rows", log_file = log_file)

# ---- Build suffix regex patterns ----
suffixes = c(
  postmastr::dic_us_suffix %>% pull(suf.input),
  postmastr::dic_us_suffix %>% pull(suf.type),
  postmastr::dic_us_suffix %>% pull(suf.output)
) %>%
  str_replace("(pt|pts)","") %>%
  unique() %>%
  str_to_lower() %>%
  sort() %>%
  append("st") %>%
  append("la") %>%
  append("blv") %>%
  append("bld") %>%
  str_c(collapse = "|")

suffix_regex = paste0("(^|,|\\|)?([0-9-]+[a-z]?\\s[a-z0-9\\.\\s-]+\\s(", suffixes, "))")
stuck_regex = paste0("([a-z])(", suffixes, ")(\\.|\\|)")

# ---- Step 1: Initial address cleaning ----
logf("Step 1: Initial address cleaning", log_file = log_file)

philly_evict[, def_address_lower := str_to_lower(defendant_address) %>%
               str_replace_all("\\. ,", ".,") %>%
               str_remove_all("\\.")]
philly_evict[, def_address_lower := (def_address_lower) %>%
               str_replace_all("\\s([nsew])([0-9]+)", "\\1 \\2")]
philly_evict[, def_address_lower := str_replace_all(def_address_lower, stuck_regex, "\\1 \\2\\3")]

# Extract short address
philly_evict[, short_address_r1 := str_match(def_address_lower, suffix_regex)[,3]]
philly_evict[, short_address_r2 := str_match(short_address_r1, "(.+)\\saka(.+)")[,2]]
philly_evict[, short_address := coalesce(short_address_r2, short_address_r1)]

# ---- Step 2: Postmastr parsing ----
logf("Step 2: Postmastr address parsing", log_file = log_file)

dirs <- pm_dictionary(type = "directional", filter = c("N", "S", "E", "W"), locale = "us")
pa <- pm_dictionary(type = "state", filter = "PA", case = c("title", "upper", "lower"), locale = "us")
cities = c("phila", "philadelphia") %>% str_to_lower()
philly <- tibble(city.input = unique(cities))

philly_evict_sample = philly_evict %>%
  pm_identify(var = short_address)

logf("Identified ", nrow(philly_evict_sample), " addresses", log_file = log_file)

# Prep for parsing (filters to street addresses only)
philly_evict_adds_sample <- pm_prep(philly_evict_sample, var = "short_address", type = "street")
logf("After pm_prep: ", nrow(philly_evict_adds_sample), " rows", log_file = log_file)

# Parse postal, state, city
philly_evict_adds_sample <- pm_postal_parse(philly_evict_adds_sample, locale = "us")
philly_evict_adds_sample <- pm_state_parse(philly_evict_adds_sample, dictionary = pa)
philly_evict_adds_sample <- pm_city_parse(philly_evict_adds_sample, dictionary = philly)

# ---- Step 3: Unit parsing (multiple rounds) ----
logf("Step 3: Unit parsing", log_file = log_file)

philly_evict_adds_sample_units = parse_unit(philly_evict_adds_sample$pm.address)
philly_evict_adds_sample$pm.address = philly_evict_adds_sample_units$clean_address %>% str_squish()

philly_evict_adds_sample_units_r1 = parse_unit(philly_evict_adds_sample$pm.address)
philly_evict_adds_sample_units_r2 = parse_unit(philly_evict_adds_sample_units_r1$clean_address %>% str_squish())
philly_evict_adds_sample_units_r3 = parse_unit_extra(philly_evict_adds_sample_units_r2$clean_address %>% str_squish())

philly_evict_adds_sample$pm.address = philly_evict_adds_sample_units_r3$clean_address %>% str_squish()
philly_evict_adds_sample$pm.unit = philly_evict_adds_sample_units_r3$unit

# Save unit tibble for reference
unit_tibble = tibble(
  address = philly_evict_adds_sample_units$og_address,
  clean_address = philly_evict_adds_sample_units_r3$clean_address,
  u1 = philly_evict_adds_sample_units$unit,
  u2 = philly_evict_adds_sample_units_r1$unit,
  u3 = philly_evict_adds_sample_units_r2$unit,
  u4 = philly_evict_adds_sample_units_r3$unit,
  uid = philly_evict_adds_sample$pm.uid
) %>%
  mutate(pm.unit = coalesce(u1, u2, u3, u4))

# ---- Step 4: House number and street parsing ----
logf("Step 4: House number and street parsing", log_file = log_file)

philly_evict_adds_sample = pm_house_parse(philly_evict_adds_sample)
philly_evict_adds_sample_nums = philly_evict_adds_sample$pm.house %>% parse_letter()
philly_evict_adds_sample_range = philly_evict_adds_sample_nums$clean_address %>% parse_range()

new_pm_house = coalesce(philly_evict_adds_sample_range$range1, philly_evict_adds_sample_range$og_address)
philly_evict_adds_sample$pm.house = new_pm_house %>% as.numeric()
philly_evict_adds_sample$pm.house2 = philly_evict_adds_sample_range$range2 %>% as.numeric()
philly_evict_adds_sample$pm.house.letter = philly_evict_adds_sample_nums$letter

philly_evict_adds_sample = pm_streetDir_parse(philly_evict_adds_sample, dictionary = dirs)
philly_evict_adds_sample = pm_streetSuf_parse(philly_evict_adds_sample)

# Ordinalize street names
philly_evict_adds_sample = philly_evict_adds_sample %>%
  mutate(pm.address = ord_words_to_nums(pm.address))

philly_evict_adds_sample = pm_street_parse(philly_evict_adds_sample, ordinal = TRUE, drop = FALSE)

# ---- Step 5: Street name standardization ----
logf("Step 5: Street name standardization", log_file = log_file)

philly_evict_adds_sample = philly_evict_adds_sample %>% mutate(
  pm.street = case_when(
    str_detect(pm.street, "[a-zA-Z]") ~ pm.street,
    pm.street == 1 ~ "1st",
    pm.street == 2 ~ "2nd",
    pm.street == 3 ~ "3rd",
    (pm.street) >= 4 ~ paste0((pm.street), "th"),
    TRUE ~ pm.street
  )
)

# Fix directional prefix issues
philly_evict_adds_sample = philly_evict_adds_sample %>% mutate(
  pm.street = str_replace_all(pm.street, "^([S])([dfgjbvxzs])", "\\2") %>%
    str_replace_all("^([E])([ebpgq])", "\\2") %>%
    str_replace_all("^([NW])([dfgjbvxzs])", "\\2")
)

# Fix known misspellings
philly_evict_adds_sample = philly_evict_adds_sample %>% mutate(
  pm.street = case_when(
    pm.street == "berkeley" ~ "berkley",
    pm.street == "Berkeley" ~ "Berkley",
    TRUE ~ pm.street
  )
)

# Expand abbreviations
philly_evict_adds_sample = philly_evict_adds_sample %>% mutate(
  pm.street = pm.street %>%
    str_replace_all("[Mm]t ", "mount ") %>%
    str_replace_all("[Mm]c ", "mc") %>%
    str_replace_all("[Ss]t ", "saint ")
)


# ============================================================
# Step 5b: Oracle-validated canonicalization (evictions)
# ============================================================

data.table::setDT(philly_evict_adds_sample)

# Use reusable canonicalization function from address_utils.R
philly_evict_adds_sample <- canonicalize_parsed_addresses(
  philly_evict_adds_sample,
  cfg,
  source = "evictions",
  export_qa = TRUE,
  log_file = log_file
)

# Fix "la" suffix
philly_evict_adds_sample = philly_evict_adds_sample %>%
  mutate(
    pm.streetSuf = fifelse(is.na(pm.streetSuf) & str_detect(pm.street, "\\s[lL]a$"), "ln", pm.streetSuf),
    pm.street = str_remove_all(pm.street, "\\s[lL]a$")
  )

# ---- Step 6: Create composite address key ----
logf("Step 6: Creating composite address key", log_file = log_file)

philly_evict_adds_sample_c = philly_evict_adds_sample %>%
  mutate(across(c(pm.sufDir, pm.street, pm.streetSuf, pm.preDir), ~str_squish(str_to_lower(replace_na(.x, ""))))) %>%
  mutate(
    n_sn_ss_c = str_squish(str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir))),
    pm.dir_concat = coalesce(pm.preDir, pm.sufDir)
  )

setDT(philly_evict_adds_sample_c)
setDT(philly_evict_sample)

# ---- Step 7: Merge and finalize ----
logf("Step 7: Merging parsed addresses back to original data", log_file = log_file)

philly_evict_adds_sample_m = merge(
  philly_evict_sample,
  philly_evict_adds_sample_c,
  by.x = "pm.uid",
  by.y = "pm.uid",
  all.x = TRUE
)

logf("After merge: ", nrow(philly_evict_adds_sample_m), " rows", log_file = log_file)

# Fill in defaults
# Add "_" prefix to pm.zip to ensure character type on read (e.g., "_19104")
philly_evict_adds_sample_m[, pm.zip := {
  raw_zip <- coalesce(pm.zip, zip)
  fifelse(
    !is.na(raw_zip) & nchar(as.character(raw_zip)) >= 4,
    paste0("_", str_pad(as.character(raw_zip), 5, "left", "0")),
    NA_character_
  )
}]
philly_evict_adds_sample_m[, pm.city := coalesce(pm.city, "philadelphia")]
philly_evict_adds_sample_m[, pm.state := coalesce(pm.state, "PA")]

# ---- Assertions ----
logf("Running assertions...", log_file = log_file)

# Check pm.uid uniqueness
n_total <- nrow(philly_evict_adds_sample_m)
n_unique_uid <- philly_evict_adds_sample_m[, uniqueN(pm.uid)]
logf("Total rows: ", n_total, ", Unique pm.uid: ", n_unique_uid, log_file = log_file)

if (n_total != n_unique_uid) {
  logf("WARNING: pm.uid is not unique! Duplicates exist.", log_file = log_file)
}

# Check key columns exist
required_cols <- c("pm.uid", "n_sn_ss_c", "year", "pm.house", "pm.street", "pm.zip")
missing_cols <- setdiff(required_cols, names(philly_evict_adds_sample_m))
if (length(missing_cols) > 0) {
  logf("WARNING: Missing columns: ", paste(missing_cols, collapse = ", "), log_file = log_file)
}

# Log address parsing success rate
n_with_address <- philly_evict_adds_sample_m[!is.na(n_sn_ss_c) & nzchar(n_sn_ss_c), .N]
logf("Addresses parsed: ", n_with_address, " (", round(100 * n_with_address / n_total, 1), "%)", log_file = log_file)

# ---- Write output ----
output_path <- p_product(cfg, "evictions_clean")
logf("Writing output to: ", output_path, log_file = log_file)

fwrite(philly_evict_adds_sample_m, output_path)

logf("Wrote ", nrow(philly_evict_adds_sample_m), " rows to ", output_path, log_file = log_file)
logf("=== Finished clean-eviction-addresses.R ===", log_file = log_file)
