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
##   2-7. Parse defendant_address via postmastr → n_sn_ss_c_def
##   8. Parse clean_address via light regex pipeline → n_sn_ss_c_ca
##      (only for rows where defendant_address parsing failed)
##   9. Coalesce: n_sn_ss_c = coalesce(n_sn_ss_c_def, n_sn_ss_c_ca)
##      Track provenance in addr_source column
##  10. Export cleaned data
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

# Strip recurring non-address artifacts that leak into parsed street strings.
strip_address_artifacts <- function(x) {
  x <- str_to_lower(replace_na(as.character(x), ""))

  # Normalize stuck "...staka..." variants so the alias clause can be removed.
  x <- str_replace_all(
    x,
    "\\b(st|street|ave|avenue|av|rd|road|dr|drive|ln|lane|blvd|boulevard|ct|court|cir|circle|way|pike|sq|pl)(?=aka\\b)",
    "\\1 "
  )

  # Keep primary address when aliases are appended.
  x <- str_replace(x, "\\s+aka\\s+.*$", "")

  # If a unit/suite token appears before a second full address, keep trailing full address.
  x <- str_replace(
    x,
    "^.*\\b(suite|ste|apt|apartment|unit|rm|room|fl|floor)\\s*[0-9a-z-]+\\s+([0-9]+\\s+.+)$",
    "\\2"
  )

  # Drop common trailing noise tokens seen in filings.
  x <- str_replace(x, "\\s+(un|a\\s*pt|co\\s*[0-9a-z]+|garage)\\s*$", "")
  x <- str_replace(x, "\\s+(suite|ste|apt|apartment|unit|rm|room|fl|floor)\\s*[0-9a-z-]*\\s*$", "")
  x <- str_replace(x, "\\s*#\\s*[0-9a-z-]+\\s*$", "")

  x <- str_squish(x)
  x[x == ""] <- NA_character_
  x
}

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
philly_evict[, short_address := strip_address_artifacts(short_address)]

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

# ---- Step 6: Create composite address key (defendant_address) ----
logf("Step 6: Creating composite address key (defendant_address)", log_file = log_file)

philly_evict_adds_sample_c = philly_evict_adds_sample %>%
  mutate(across(c(pm.sufDir, pm.street, pm.streetSuf, pm.preDir), ~str_squish(str_to_lower(replace_na(.x, ""))))) %>%
  mutate(
    n_sn_ss_c_def = str_squish(str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir))),
    pm.dir_concat = coalesce(pm.preDir, pm.sufDir)
  )

setDT(philly_evict_adds_sample_c)
setDT(philly_evict_sample)

# ---- Step 7: Merge defendant_address results back ----
logf("Step 7: Merging parsed addresses back to original data", log_file = log_file)

philly_evict_adds_sample_m = merge(
  philly_evict_sample,
  philly_evict_adds_sample_c,
  by.x = "pm.uid",
  by.y = "pm.uid",
  all.x = TRUE
)

logf("After merge: ", nrow(philly_evict_adds_sample_m), " rows", log_file = log_file)

n_def_parsed <- philly_evict_adds_sample_m[!is.na(n_sn_ss_c_def) & nzchar(n_sn_ss_c_def), .N]
logf("defendant_address parsed: ", n_def_parsed, " / ", nrow(philly_evict_adds_sample_m),
     " (", round(100 * n_def_parsed / nrow(philly_evict_adds_sample_m), 1), "%)", log_file = log_file)

# ============================================================
# Step 8: Light-weight clean_address parsing (complementary)
# ============================================================
# clean_address is pre-cleaned (e.g., "4203 MANTUA AVE, PHILADELPHIA, PA")
# and cannot go through postmastr pm_prep (which drops 77% of rows).
# Instead, use regex-based extraction: comma-split → house/street/suffix/dir.
# ============================================================

logf("Step 8: Parsing clean_address (light regex pipeline)", log_file = log_file)

setDT(philly_evict_adds_sample_m)

# Add a unique row index for safe join-back (pm.uid is NOT unique per row)
philly_evict_adds_sample_m[, .row_idx := .I]

# Only attempt for rows where defendant_address parsing failed
ca_rows <- philly_evict_adds_sample_m[
  (is.na(n_sn_ss_c_def) | !nzchar(n_sn_ss_c_def)) & !is.na(clean_address) & nzchar(clean_address)
]
logf("  Rows needing clean_address fallback: ", nrow(ca_rows), log_file = log_file)

if (nrow(ca_rows) > 0) {

  # --- 8a: Extract street portion (everything before first comma) ---
  ca_rows[, ca_street_part := str_to_lower(str_trim(str_extract(clean_address, "^[^,]+")))]
  ca_rows[, ca_street_part := str_remove_all(ca_street_part, "\\.")]
  ca_rows[, ca_street_part := strip_address_artifacts(ca_street_part)]

  # --- 8b: Regex-extract house number and remainder ---
  # Pattern: leading digits (possibly with dash for ranges like "123-125"),
  #          optional letter suffix, then the street remainder
  ca_rows[, ca_house_raw := str_extract(ca_street_part, "^[0-9]+(\\s?-\\s?[0-9]+)?[a-z]?")]
  ca_rows[, ca_remainder := str_trim(str_remove(ca_street_part, "^[0-9]+(\\s?-\\s?[0-9]+)?[a-z]?\\s*"))]

  # Parse house number (take first number for ranges)
  ca_house_parsed <- parse_letter(ca_rows$ca_house_raw)
  ca_house_range  <- parse_range(ca_house_parsed$clean_address)
  ca_rows[, ca_house := as.numeric(coalesce(ca_house_range$range1, ca_house_range$og_address))]
  ca_rows[, ca_house2 := as.numeric(ca_house_range$range2)]
  ca_rows[, ca_house_letter := ca_house_parsed$letter]

  # --- 8c: Extract directional prefix (N/S/E/W at start of remainder) ---
  ca_rows[, ca_preDir := str_extract(ca_remainder, "^[nsew](?=\\s)")]
  ca_rows[, ca_remainder := str_trim(str_remove(ca_remainder, "^[nsew]\\s+"))]

  # --- 8d: Extract suffix (last token if it matches known suffixes) ---
  # Build suffix set from postmastr dictionary
  ca_suffix_set <- c(
    postmastr::dic_us_suffix %>% pull(suf.input),
    postmastr::dic_us_suffix %>% pull(suf.type),
    postmastr::dic_us_suffix %>% pull(suf.output)
  ) %>%
    str_to_lower() %>%
    unique() %>%
    c("st", "la", "blv", "bld")

  ca_rows[, ca_last_token := str_extract(ca_remainder, "[a-z]+$")]
  ca_rows[, ca_has_suffix := ca_last_token %chin% ca_suffix_set]
  ca_rows[ca_has_suffix == TRUE, ca_streetSuf := ca_last_token]
  ca_rows[ca_has_suffix == TRUE, ca_street := str_trim(str_remove(ca_remainder, "\\s*[a-z]+$"))]
  ca_rows[ca_has_suffix != TRUE | is.na(ca_has_suffix), ca_street := ca_remainder]
  ca_rows[ca_has_suffix != TRUE | is.na(ca_has_suffix), ca_streetSuf := NA_character_]

  # --- 8e: Extract suffix directional (e.g., trailing N/S/E/W after suffix) ---
  ca_rows[, ca_sufDir := NA_character_]

  # --- 8f: Unit parsing (clean_address may contain units) ---
  ca_units_r1 <- parse_unit(ca_rows$ca_street)
  ca_units_r2 <- parse_unit(ca_units_r1$clean_address %>% str_squish())
  ca_units_r3 <- parse_unit_extra(ca_units_r2$clean_address %>% str_squish())
  ca_rows[, ca_street := ca_units_r3$clean_address %>% str_squish()]
  ca_rows[, ca_unit := coalesce(ca_units_r1$unit, ca_units_r2$unit, ca_units_r3$unit)]

  # --- 8g: Ordinal word → number conversion ---
  ca_rows[, ca_street := ord_words_to_nums(ca_street)]

  # --- 8h: Street name standardization (same as defendant_address pipeline) ---
  ca_rows[, ca_street := ca_street %>%
    str_replace_all("berkeley", "berkley") %>%
    str_replace_all("[Mm]t ", "mount ") %>%
    str_replace_all("[Mm]c ", "mc") %>%
    str_replace_all("[Ss]t ", "saint ")]
  ca_rows[, ca_street := strip_address_artifacts(ca_street)]

  # --- 8i: Oracle canonicalization ---
  # Build a temporary dt with pm.* columns for canonicalize_parsed_addresses()
  ca_canon <- data.table(
    row_idx   = seq_len(nrow(ca_rows)),
    pm.house  = ca_rows$ca_house,
    pm.preDir = ca_rows$ca_preDir,
    pm.street = ca_rows$ca_street,
    pm.streetSuf = ca_rows$ca_streetSuf,
    pm.sufDir = ca_rows$ca_sufDir
  )

  # Replace NAs for canonicalization
  ca_canon[is.na(pm.preDir), pm.preDir := ""]
  ca_canon[is.na(pm.sufDir), pm.sufDir := ""]
  ca_canon[is.na(pm.streetSuf), pm.streetSuf := ""]

  ca_canon <- canonicalize_parsed_addresses(
    ca_canon,
    cfg,
    source = "evictions_clean_address",
    export_qa = TRUE,
    log_file = log_file
  )

  # Fix "la" suffix (same as defendant_address pipeline)
  ca_canon[, pm.streetSuf := fifelse(
    is.na(pm.streetSuf) & str_detect(pm.street, "\\s[lL]a$"), "ln", pm.streetSuf
  )]
  ca_canon[, pm.street := str_remove_all(pm.street, "\\s[lL]a$")]

  # --- 8j: Build n_sn_ss_c_ca key ---
  ca_canon[, n_sn_ss_c_ca := {
    p_preDir <- str_squish(str_to_lower(replace_na(pm.preDir, "")))
    p_street <- str_squish(str_to_lower(replace_na(pm.street, "")))
    p_suf    <- str_squish(str_to_lower(replace_na(pm.streetSuf, "")))
    p_sufDir <- str_squish(str_to_lower(replace_na(pm.sufDir, "")))
    str_squish(paste(pm.house, p_preDir, p_street, p_suf, p_sufDir))
  }]

  # Mark empty/invalid keys as NA
  ca_canon[n_sn_ss_c_ca == "" | is.na(pm.house) | pm.street == "", n_sn_ss_c_ca := NA_character_]

  # Write back to ca_rows
  ca_rows[, n_sn_ss_c_ca := ca_canon$n_sn_ss_c_ca]

  n_ca_parsed <- ca_rows[!is.na(n_sn_ss_c_ca) & nzchar(n_sn_ss_c_ca), .N]
  logf("  clean_address parsed: ", n_ca_parsed, " / ", nrow(ca_rows),
       " (", round(100 * n_ca_parsed / nrow(ca_rows), 1), "%)", log_file = log_file)

  # Merge clean_address keys back to main table (use row index, NOT pm.uid which is non-unique)
  philly_evict_adds_sample_m[ca_rows, n_sn_ss_c_ca := i.n_sn_ss_c_ca, on = ".row_idx"]

} else {
  philly_evict_adds_sample_m[, n_sn_ss_c_ca := NA_character_]
}

# ============================================================
# Step 9: Coalesce and add provenance
# ============================================================
logf("Step 9: Coalescing address keys and adding provenance", log_file = log_file)

# Clean up temporary row index
philly_evict_adds_sample_m[, .row_idx := NULL]

philly_evict_adds_sample_m[, n_sn_ss_c := coalesce(n_sn_ss_c_def, n_sn_ss_c_ca)]

philly_evict_adds_sample_m[, addr_source := fcase(
  !is.na(n_sn_ss_c_def) & nzchar(n_sn_ss_c_def), "defendant_address",
  !is.na(n_sn_ss_c_ca) & nzchar(n_sn_ss_c_ca),  "clean_address",
  default = NA_character_
)]

# Log provenance breakdown
addr_source_counts <- philly_evict_adds_sample_m[, .N, by = addr_source][order(-N)]
for (i in seq_len(nrow(addr_source_counts))) {
  logf("  addr_source = ", addr_source_counts$addr_source[i], ": ",
       addr_source_counts$N[i], log_file = log_file)
}

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

# Log address parsing success rate (coalesced)
n_with_address <- philly_evict_adds_sample_m[!is.na(n_sn_ss_c) & nzchar(n_sn_ss_c), .N]
logf("Addresses parsed (coalesced): ", n_with_address, " (", round(100 * n_with_address / n_total, 1), "%)",
     log_file = log_file)
logf("  defendant_address only: ", n_def_parsed, " (", round(100 * n_def_parsed / n_total, 1), "%)",
     log_file = log_file)
logf("  clean_address fallback added: ", n_with_address - n_def_parsed,
     " (+", round(100 * (n_with_address - n_def_parsed) / n_total, 1), " pp)", log_file = log_file)

# ---- Write output ----
output_path <- p_product(cfg, "evictions_clean")
logf("Writing output to: ", output_path, log_file = log_file)

fwrite(philly_evict_adds_sample_m, output_path)

logf("Wrote ", nrow(philly_evict_adds_sample_m), " rows to ", output_path, log_file = log_file)
logf("=== Finished clean-eviction-addresses.R ===", log_file = log_file)
