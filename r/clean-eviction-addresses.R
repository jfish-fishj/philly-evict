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
# Step 5b: Oracle-validated canonicalization against parcels
# ============================================================

logf("Step 5b: Oracle canonicalization (evictions)", log_file = log_file)

# ---- Load parcel oracle ----
parcel_counts_path <- p_input(cfg, "parcel_address_counts")
logf("Loading parcel oracle: ", parcel_counts_path, log_file = log_file)

parcel_counts <- data.table::fread(parcel_counts_path)
stopifnot(all(c("pm.street","pm.streetSuf","N") %in% names(parcel_counts)))

# normalize oracle
parcel_counts[, pm.street := stringr::str_squish(tolower(pm.street))]
parcel_counts[, pm.streetSuf := stringr::str_squish(tolower(pm.streetSuf))]
parcel_counts <- parcel_counts[pm.street != "" & pm.streetSuf != ""]
parcel_counts[, key := paste(pm.street, pm.streetSuf, sep="|")]
data.table::setkey(parcel_counts, key)

parcel_suffixes <- unique(parcel_counts$pm.streetSuf)

# ---- Prepare eviction parsed table ----
data.table::setDT(philly_evict_adds_sample)

# keep originals for QA
philly_evict_adds_sample[, `:=`(
  pm.preDir_orig    = pm.preDir,
  pm.street_orig    = pm.street,
  pm.streetSuf_orig = pm.streetSuf
)]

# normalize parsed pieces (lower/squish, NA->"")
philly_evict_adds_sample[, `:=`(
  pm.preDir    = stringr::str_squish(tolower(data.table::fifelse(is.na(pm.preDir), "", as.character(pm.preDir)))),
  pm.sufDir    = stringr::str_squish(tolower(data.table::fifelse(is.na(pm.sufDir), "", as.character(pm.sufDir)))),
  pm.street    = stringr::str_squish(tolower(data.table::fifelse(is.na(pm.street), "", as.character(pm.street)))),
  pm.streetSuf = stringr::str_squish(tolower(data.table::fifelse(is.na(pm.streetSuf), "", as.character(pm.streetSuf))))
)]

# canonical key + oracle membership
philly_evict_adds_sample[, key := paste(pm.street, pm.streetSuf, sep="|")]
# parcel_counts must be keyed by "key"
setkey(parcel_counts, key)

# add parcelN via join; i.parcelN is one value per row in philly_evict_adds_sample
philly_evict_adds_sample[parcel_counts, on = .(key), parcelN := i.N]
philly_evict_adds_sample[, in_parcels := !is.na(parcelN)]


before_rate <- philly_evict_adds_sample[, mean(in_parcels)]
logf("Oracle-valid BEFORE fixes: ", round(100*before_rate, 3), "%", log_file = log_file)

# Track fixes
philly_evict_adds_sample[, `:=`(fix_tag = NA_character_, fix_changed = FALSE, fix_parcelN = as.integer(NA))]

# ============================================================
# Rule B (high impact): fix est/rst by attaching extra letter back to street
# spruc + est -> spruce + st   ; bouvie + rst -> bouvier + st
# ============================================================
iB <- philly_evict_adds_sample[
  !in_parcels & pm.streetSuf %chin% c("est","rst") & pm.street != "",
  which = TRUE
]

if (length(iB) > 0) {
  extra <- sub("st$", "", philly_evict_adds_sample$pm.streetSuf[iB])  # "e" or "r"
  new_st <- paste0(philly_evict_adds_sample$pm.street[iB], extra)
  new_key <- paste(new_st, "st", sep="|")
  newN <- parcel_counts[.(new_key), x.N]

  ok <- !is.na(newN)
  if (any(ok)) {
    ii <- iB[ok]
    philly_evict_adds_sample[ii, `:=`(
      pm.street = new_st[ok],
      pm.streetSuf = "st",
      fix_tag = "attach_extra_from_suf:st",
      fix_changed = TRUE,
      fix_parcelN = as.integer(newN[ok])
    )]
  }
}

# refresh oracle membership
philly_evict_adds_sample[, key := paste(pm.street, pm.streetSuf, sep="|")]
# parcel_counts must be keyed by "key"
setkey(parcel_counts, key)

# add parcelN via join; i.parcelN is one value per row in philly_evict_adds_sample
philly_evict_adds_sample[parcel_counts, on = .(key), parcelN := i.N]
philly_evict_adds_sample[, in_parcels := !is.na(parcelN)]


# ============================================================
# Rule A (high impact): split glued direction when pm.preDir missing
# wlehigh + ave -> preDir=w, street=lehigh, suf=ave (oracle validated)
# ============================================================
iA <- philly_evict_adds_sample[
  is.na(fix_tag) & pm.preDir == "" & !in_parcels & stringr::str_detect(pm.street, "^[nsew][a-z]"),
  which = TRUE
]

if (length(iA) > 0) {
  dir <- substr(philly_evict_adds_sample$pm.street[iA], 1, 1)
  st2 <- substr(philly_evict_adds_sample$pm.street[iA], 2, nchar(philly_evict_adds_sample$pm.street[iA]))
  key2 <- paste(st2, philly_evict_adds_sample$pm.streetSuf[iA], sep="|")
  N2 <- parcel_counts[.(key2), x.N]

  ok <- !is.na(N2)
  if (any(ok)) {
    ii <- iA[ok]
    philly_evict_adds_sample[ii, `:=`(
      pm.preDir = dir[ok],
      pm.street = st2[ok],
      fix_tag = "split_dir_same_suffix",
      fix_changed = TRUE,
      fix_parcelN = as.integer(N2[ok])
    )]
  }
}

# final refresh
philly_evict_adds_sample[, key := paste(pm.street, pm.streetSuf, sep="|")]
philly_evict_adds_sample[, parcelN := parcel_counts[.(key), x.N]]
philly_evict_adds_sample[, in_parcels := !is.na(parcelN)]

after_rate <- philly_evict_adds_sample[, mean(in_parcels)]
logf("Oracle-valid AFTER fixes: ", round(100*after_rate, 3), "%", log_file = log_file)

# ---- Hard checks that should move if the block ran ----
logf("Counts of est/rst/un AFTER fixes:",
     " est=", philly_evict_adds_sample[pm.streetSuf == "est", .N],
     " rst=", philly_evict_adds_sample[pm.streetSuf == "rst", .N],
     " un=",  philly_evict_adds_sample[pm.streetSuf == "un",  .N],
     log_file = log_file)

logf("Fix tag counts (top):", log_file = log_file)
print(philly_evict_adds_sample[, .N, by = fix_tag][order(-N)][1:10])

# Optional: export QA artifacts
qa_dir <- p_out(cfg, "qa", "evictions_address_canonicalize")
dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)

data.table::fwrite(
  philly_evict_adds_sample[, .N, by = fix_tag][order(-N)],
  file.path(qa_dir, "fix_tag_counts.csv")
)
data.table::fwrite(
  data.table::data.table(metric=c("oracle_valid_rate_before","oracle_valid_rate_after"),
                         value=c(before_rate, after_rate)),
  file.path(qa_dir, "oracle_valid_rate_before_after.csv")
)

# ---- QA outputs ----
qa_dir <- p_out(cfg, "qa", "evictions_address_canonicalize")
dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)

fix_counts <- philly_evict_adds_sample[, .N, by = fix_tag][order(-N)]
fix_counts[is.na(fix_tag), fix_tag := "NO_FIX"]
fwrite(fix_counts, file.path(qa_dir, "fix_tag_counts.csv"))

# a compact before/after summary
oracle_summary <- data.table(
  metric = c("oracle_valid_rate_before", "oracle_valid_rate_after"),
  value  = c(before_rate, after_rate)
)
fwrite(oracle_summary, file.path(qa_dir, "oracle_valid_rate_before_after.csv"))

# sample of changed rows for inspection
changed_sample <- philly_evict_adds_sample[fix_changed == TRUE][
  , .(pm.uid, pm.house, pm.preDir_orig, pm.street_orig, pm.streetSuf_orig,
      pm.preDir, pm.street, pm.streetSuf, fix_tag, fix_parcelN)
]
set.seed(123)
if (nrow(changed_sample) > 0) {
  changed_sample <- changed_sample[sample.int(.N, min(.N, 500))]
}
fwrite(changed_sample, file.path(qa_dir, "changed_rows_sample.csv"))

# ============================================================
# End canonicalization; proceed to key creation using pm.* fields
# ============================================================


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
