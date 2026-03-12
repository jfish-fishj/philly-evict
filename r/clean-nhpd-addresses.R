## ============================================================
## clean-nhpd-addresses.R
## ============================================================
## Purpose: Clean and standardize NHPD property addresses for
##          parcel matching and rental-universe integration.
##
## Inputs:
##   - cfg$inputs$nhpd_properties_raw
##
## Outputs:
##   - cfg$products$nhpd_clean
##
## Primary key: nhpd_property_id
## Secondary key: pm.uid
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(postmastr)
  library(readxl)
})

source("r/config.R")
source("r/helper-functions.R")
source("r/lib/address_utils.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "clean-nhpd-addresses.log")

logf("=== Starting clean-nhpd-addresses.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

input_path <- p_input(cfg, "nhpd_properties_raw")
logf("Loading raw NHPD data from: ", input_path, log_file = log_file)

nhpd_raw <- as.data.table(read_excel(input_path))
logf("  Loaded ", nrow(nhpd_raw), " rows, ", ncol(nhpd_raw), " columns", log_file = log_file)

required_cols <- c(
  "NHPDPropertyID", "PropertyName", "PropertyAddress", "City", "State", "Zip",
  "Latitude", "Longitude", "PropertyStatus", "TotalUnits", "Owner"
)
missing_cols <- setdiff(required_cols, names(nhpd_raw))
if (length(missing_cols) > 0L) {
  stop("NHPD raw file is missing required columns: ", paste(missing_cols, collapse = ", "))
}

nhpd <- copy(nhpd_raw)
nhpd[, nhpd_property_id := as.character(NHPDPropertyID)]
nhpd[, `:=`(
  City = as.character(City),
  State = as.character(State),
  County = as.character(County),
  PropertyStatus = as.character(PropertyStatus),
  PropertyAddress = as.character(PropertyAddress),
  PropertyName = as.character(PropertyName),
  Owner = as.character(Owner),
  Zip = as.character(Zip)
)]

logf("Filtering to Philadelphia active/inconclusive properties...", log_file = log_file)
nhpd <- nhpd[
  str_to_upper(State) == "PA" &
    str_to_upper(City) == "PHILADELPHIA" &
    PropertyStatus %chin% c("Active", "Inconclusive")
]
logf("  Remaining rows: ", nrow(nhpd), log_file = log_file)
if (nrow(nhpd) == 0L) {
  stop("NHPD Philadelphia filter returned zero rows")
}
assert_unique(nhpd, "nhpd_property_id", "nhpd filtered properties")

status_counts <- nhpd[, .N, by = PropertyStatus][order(-N)]
for (i in seq_len(nrow(status_counts))) {
  logf("  ", status_counts$PropertyStatus[i], ": ", status_counts$N[i], log_file = log_file)
}

nhpd[, property_address_lower := str_to_lower(PropertyAddress) %>% misc_pre_processing()]

dirs <- pm_dictionary(type = "directional", filter = c("N", "S", "E", "W"), locale = "us")

nhpd_parse <- nhpd %>%
  .[, .(nhpd_property_id, property_address_lower)] %>%
  pm_identify(var = property_address_lower)
setDT(nhpd_parse)

dropped_unparseable <- nhpd_parse[!pm.type %in% c("full", "short"), nhpd_property_id]
logf("  Dropped ", length(dropped_unparseable), " unparseable addresses", log_file = log_file)

nhpd_parse <- nhpd_parse[pm.type %in% c("full", "short")]

logf("  Proceeding with ", nrow(nhpd_parse), " parseable addresses", log_file = log_file)

logf("Running postmastr parsing pipeline...", log_file = log_file)

nhpd_adds <- pm_prep(nhpd_parse, var = "property_address_lower", type = "street")
nhpd_adds_units <- parse_unit(nhpd_adds$pm.address)
nhpd_adds$pm.address <- nhpd_adds_units$clean_address %>% str_squish()
nhpd_adds <- pm_postal_parse(nhpd_adds, locale = "us")

nhpd_adds_units_r1 <- parse_unit(nhpd_adds$pm.address)
nhpd_adds_units_r2 <- parse_unit(nhpd_adds_units_r1$clean_address %>% str_squish())
nhpd_adds_units_r3 <- parse_unit_extra(nhpd_adds_units_r2$clean_address %>% str_squish())
nhpd_adds$pm.address <- nhpd_adds_units_r2$clean_address %>% str_squish()
nhpd_adds$pm.unit <- nhpd_adds_units_r3$unit

nhpd_adds <- pm_house_parse(nhpd_adds)
nhpd_adds_nums <- nhpd_adds$pm.house %>% parse_letter()
nhpd_adds_range <- nhpd_adds_nums$clean_address %>% parse_range()
nhpd_adds$pm.house <- coalesce(nhpd_adds_range$range1, nhpd_adds_range$og_address) %>% as.numeric()
nhpd_adds$pm.house2 <- nhpd_adds_range$range2 %>% as.numeric()
nhpd_adds$pm.house.letter <- nhpd_adds_nums$letter

nhpd_adds <- pm_streetDir_parse(nhpd_adds, dictionary = dirs)
nhpd_adds <- pm_streetSuf_parse(nhpd_adds)

nhpd_adds <- nhpd_adds %>%
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
  ) %>%
  filter(!is.na(pm.address) & pm.address != "") %>%
  mutate(pm.address = as.character(pm.address) %>% str_squish()) %>%
  filter(nchar(pm.address) >= 2)

logf("  After cleaning: ", nrow(nhpd_adds), " rows remain for street parsing", log_file = log_file)

nhpd_adds <- tryCatch({
  pm_street_parse(nhpd_adds, ordinal = TRUE, drop = FALSE)
}, error = function(e) {
  logf("  WARNING: pm_street_parse failed, using pm.address fallback", log_file = log_file)
  nhpd_adds %>% mutate(pm.street = pm.address)
})
nhpd_adds$pm.street <- misc_post_processing(str_to_lower(nhpd_adds$pm.street))

nhpd_adds$pm.streetSuf <- fifelse(
  str_detect(nhpd_adds$pm.street, "\\s(st$)"),
  "st",
  nhpd_adds$pm.streetSuf
) %>% str_squish()
nhpd_adds$pm.street <- fifelse(
  str_detect(nhpd_adds$pm.street, "\\s(st$)"),
  str_replace_all(nhpd_adds$pm.street, "\\sst$", ""),
  nhpd_adds$pm.street
) %>% str_squish()

nhpd_adds$pm.streetSuf <- nhpd_adds$pm.streetSuf %>%
  str_replace_all("spark", " pk") %>%
  str_replace_all("street", " st") %>%
  str_replace_all("square", " sq") %>%
  str_replace_all("lane", " ln") %>%
  str_replace_all("alley", " aly") %>%
  str_replace_all("way", " way")

setDT(nhpd_adds)
for (cc in c("pm.preDir", "pm.sufDir", "pm.street", "pm.streetSuf")) {
  if (!cc %in% names(nhpd_adds)) nhpd_adds[, (cc) := NA_character_]
}
nhpd_adds <- canonicalize_parsed_addresses(
  nhpd_adds,
  cfg,
  source = "nhpd",
  export_qa = TRUE,
  log_file = log_file
)

nhpd_adds <- nhpd_adds %>%
  mutate(across(c(pm.sufDir, pm.street, pm.streetSuf, pm.preDir), ~ str_squish(str_to_lower(replace_na(.x, ""))))) %>%
  mutate(
    n_sn_ss_c = str_squish(str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir))),
    pm.dir_concat = coalesce(pm.preDir, pm.sufDir)
  )
setDT(nhpd_adds)
nhpd_adds[, num_st_sfx_id := .GRP, by = .(pm.house, pm.street, pm.streetSuf)]
nhpd_adds$pm.street <- misc_post_processing(str_to_lower(nhpd_adds$pm.street))

logf("Merging parsed addresses back to original rows...", log_file = log_file)
nhpd_clean <- merge(
  nhpd_adds,
  nhpd_parse[],
  by = "pm.uid",
  all.y = TRUE
)

nhpd_clean <- merge(
  nhpd_clean,
  nhpd,
  by = "nhpd_property_id",
  all.x = TRUE,
  all.y = FALSE
)

nhpd_clean[, pm.zip := case_when(
  str_length(pm.zip) >= 4 ~ paste0("_", str_pad(pm.zip, 5, "left", "0")),
  str_length(Zip) >= 4 ~ paste0("_", str_pad(Zip, 5, "left", "0")),
  TRUE ~ NA_character_
)]
nhpd_clean[, pm.city := City]
nhpd_clean[, pm.state := State]
nhpd_clean[, GEOID.longitude := as.numeric(Longitude)]
nhpd_clean[, GEOID.latitude := as.numeric(Latitude)]
nhpd_clean[, source_address_id := nhpd_property_id]
nhpd_clean[, num_properties_at_address := .N, by = n_sn_ss_c]
nhpd_clean[, address_total_units := sum(as.numeric(TotalUnits), na.rm = TRUE), by = n_sn_ss_c]

logf("Running assertions...", log_file = log_file)
assert_unique(nhpd_clean, "nhpd_property_id", "nhpd_clean (property id)")

required_out_cols <- c("nhpd_property_id", "pm.uid", "pm.house", "pm.street", "n_sn_ss_c", "PropertyStatus")
missing_out_cols <- setdiff(required_out_cols, names(nhpd_clean))
if (length(missing_out_cols) > 0L) {
  stop("nhpd_clean is missing required columns after parsing: ", paste(missing_out_cols, collapse = ", "))
}

n_total <- nrow(nhpd_clean)
logf("Address parsing diagnostics:", log_file = log_file)
logf("  pm.house: ", sum(!is.na(nhpd_clean$pm.house)), " / ", n_total, log_file = log_file)
logf("  pm.street: ", sum(!is.na(nhpd_clean$pm.street) & nhpd_clean$pm.street != ""), " / ", n_total, log_file = log_file)
logf("  n_sn_ss_c: ", sum(!is.na(nhpd_clean$n_sn_ss_c) & nhpd_clean$n_sn_ss_c != ""), " / ", n_total, log_file = log_file)
logf("  pm.zip: ", sum(!is.na(nhpd_clean$pm.zip) & nhpd_clean$pm.zip != ""), " / ", n_total, log_file = log_file)
logf("  unique addresses: ", uniqueN(nhpd_clean$n_sn_ss_c), log_file = log_file)

out_path <- p_product(cfg, "nhpd_clean")
dir.create(dirname(out_path), showWarnings = FALSE, recursive = TRUE)
fwrite(nhpd_clean, out_path)
logf("Wrote nhpd_clean: ", nrow(nhpd_clean), " rows, ", ncol(nhpd_clean), " columns to ", out_path, log_file = log_file)

logf("=== Finished clean-nhpd-addresses.R ===", log_file = log_file)
