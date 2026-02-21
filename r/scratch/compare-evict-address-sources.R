## compare-evict-address-sources.R
## Purpose: Compare parcel match rates when merge-evictions-parcels uses
##          defendant_address (current) vs clean_address (pre-cleaned in raw data)
##
## Approach:
##   1. Parse clean_address through postmastr (same pipeline as current)
##   2. Build n_sn_ss_c keys from both sources
##   3. Run tiered address matching against parcels for each
##   4. Report match rates side-by-side

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(postmastr)
})

source("r/config.R")
source("r/lib/address_utils.R")

cfg <- read_config()

cat("=== Loading data ===\n")

# Load raw eviction data (both address columns)
raw <- fread(p_input(cfg, "evictions_summary_table"),
             select = c("id", "year", "defendant_address", "clean_address",
                         "commercial", "plaintiff", "defendant", "d_filing",
                         "total_rent", "zip", "latitude", "longitude"))
cat("Raw rows:", nrow(raw), "\n")
cat("defendant_address non-empty:", sum(!is.na(raw$defendant_address) & raw$defendant_address != ""), "\n")
cat("clean_address non-empty:", sum(!is.na(raw$clean_address) & raw$clean_address != ""), "\n")

# Load current cleaned evictions (has n_sn_ss_c from defendant_address)
evict_current <- fread(p_product(cfg, "evictions_clean"))
cat("Current evictions_clean rows:", nrow(evict_current), "\n")

# Load parcels
parcels <- fread(p_product(cfg, "parcels_clean"))
parcels[, PID := str_pad(parcel_number, 9, "left", pad = "0")]
# Normalize pm.zip: strip "_" prefix, pad to 5 digits
parcels[, pm.zip_norm := str_remove(as.character(pm.zip), "^_") %>%
          str_pad(5, "left", pad = "0")]
parcel_addys <- unique(parcels, by = "n_sn_ss_c")

cat("\n=== Parsing clean_address through postmastr ===\n")

# --- Parse clean_address ---
# Build suffix regex (same as clean-eviction-addresses.R)
suffixes <- c(
  postmastr::dic_us_suffix %>% pull(suf.input),
  postmastr::dic_us_suffix %>% pull(suf.type),
  postmastr::dic_us_suffix %>% pull(suf.output)
) %>%
  str_replace("(pt|pts)", "") %>%
  unique() %>%
  str_to_lower() %>%
  sort() %>%
  append("st") %>%
  append("la") %>%
  append("blv") %>%
  append("bld") %>%
  str_c(collapse = "|")

# Prep clean_address for postmastr
ca <- raw[!is.na(clean_address) & clean_address != "",
          .(id, year, clean_address, zip, latitude, longitude)]
ca[, clean_address_lower := str_to_lower(clean_address) %>%
     str_remove_all("\\.") %>%
     str_squish()]

# postmastr needs a short_address that's just the street part
# clean_address format: "413 W WYOMING AVE, PHILADELPHIA, PA"
# Extract everything before the first comma
# Extract street part (before first comma) - this is what postmastr will parse
ca[, short_address := str_extract(clean_address_lower, "^[^,]+") %>% str_squish()]
# Drop rows where short_address is empty or NA
ca <- ca[!is.na(short_address) & short_address != ""]

# Set up postmastr dictionaries
dirs <- pm_dictionary(type = "directional", filter = c("N", "S", "E", "W"), locale = "us")
pa <- pm_dictionary(type = "state", filter = "PA", case = c("title", "upper", "lower"), locale = "us")
philly <- tibble(city.input = c("phila", "philadelphia"))

ca_sample <- ca %>% pm_identify(var = short_address)
cat("pm_identify results:\n")
print(table(ca_sample$pm.type))
ca_prep <- pm_prep(ca_sample, var = "short_address", type = "street")
cat("After pm_prep:", nrow(ca_prep), "rows (from", nrow(ca), ")\n")

# Parse components
ca_prep <- pm_postal_parse(ca_prep, locale = "us")
ca_prep <- pm_state_parse(ca_prep, dictionary = pa)
ca_prep <- pm_city_parse(ca_prep, dictionary = philly)

# Unit parsing (multiple rounds)
ca_units <- parse_unit(ca_prep$pm.address)
ca_prep$pm.address <- ca_units$clean_address %>% str_squish()
ca_units_r1 <- parse_unit(ca_prep$pm.address)
ca_units_r2 <- parse_unit(ca_units_r1$clean_address %>% str_squish())
ca_units_r3 <- parse_unit_extra(ca_units_r2$clean_address %>% str_squish())
ca_prep$pm.address <- ca_units_r3$clean_address %>% str_squish()

# House number parsing
ca_prep <- pm_house_parse(ca_prep)
ca_nums <- ca_prep$pm.house %>% parse_letter()
ca_range <- ca_nums$clean_address %>% parse_range()
ca_prep$pm.house <- coalesce(ca_range$range1, ca_range$og_address) %>% as.numeric()

# Direction and suffix
ca_prep <- pm_streetDir_parse(ca_prep, dictionary = dirs)
ca_prep <- pm_streetSuf_parse(ca_prep)

# Ordinalize
ca_prep <- ca_prep %>% mutate(pm.address = ord_words_to_nums(pm.address))
ca_prep <- pm_street_parse(ca_prep, ordinal = TRUE, drop = FALSE)

# Street name fixes (same as clean-eviction-addresses.R)
ca_prep <- ca_prep %>% mutate(
  pm.street = case_when(
    str_detect(pm.street, "[a-zA-Z]") ~ pm.street,
    pm.street == 1 ~ "1st",
    pm.street == 2 ~ "2nd",
    pm.street == 3 ~ "3rd",
    (pm.street) >= 4 ~ paste0((pm.street), "th"),
    TRUE ~ pm.street
  )
)
ca_prep <- ca_prep %>% mutate(
  pm.street = str_replace_all(pm.street, "^([S])([dfgjbvxzs])", "\\2") %>%
    str_replace_all("^([E])([ebpgq])", "\\2") %>%
    str_replace_all("^([NW])([dfgjbvxzs])", "\\2")
)
ca_prep <- ca_prep %>% mutate(
  pm.street = case_when(
    pm.street == "berkeley" ~ "berkley",
    pm.street == "Berkeley" ~ "Berkley",
    TRUE ~ pm.street
  )
)
ca_prep <- ca_prep %>% mutate(
  pm.street = pm.street %>%
    str_replace_all("[Mm]t ", "mount ") %>%
    str_replace_all("[Mm]c ", "mc") %>%
    str_replace_all("[Ss]t ", "saint ")
)

# Canonicalize
setDT(ca_prep)
ca_prep <- canonicalize_parsed_addresses(ca_prep, cfg, source = "evictions_clean_address", export_qa = FALSE)

# Fix "la" suffix
ca_prep <- ca_prep %>% mutate(
  pm.streetSuf = fifelse(is.na(pm.streetSuf) & str_detect(pm.street, "\\s[lL]a$"), "ln", pm.streetSuf),
  pm.street = str_remove_all(pm.street, "\\s[lL]a$")
)

# Create n_sn_ss_c
ca_final <- ca_prep %>%
  mutate(across(c(pm.sufDir, pm.street, pm.streetSuf, pm.preDir),
                ~str_squish(str_to_lower(replace_na(.x, ""))))) %>%
  mutate(
    n_sn_ss_c_ca = str_squish(str_to_lower(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir)))
  )

setDT(ca_final)

# Merge back to get zip
setDT(ca_sample)
setDT(ca_final)
ca_merged <- merge(ca_sample[, list(pm.uid, id, zip)],
                   ca_final[, list(pm.uid, n_sn_ss_c_ca, pm.house, pm.street,
                                pm.streetSuf, pm.sufDir, pm.preDir)],
                   by = "pm.uid", all.x = TRUE)

# Add zip
ca_merged[, pm.zip := fifelse(
  !is.na(zip) & nchar(as.character(zip)) >= 4,
  str_pad(as.character(zip), 5, "left", "0"),
  NA_character_
)]

cat("clean_address parsed:", sum(!is.na(ca_merged$n_sn_ss_c_ca)), "of", nrow(ca_merged), "\n")

# ============================================================
# Now run the tiered matching for BOTH sources
# ============================================================

run_match <- function(evict_addys, parcel_addys, label) {
  cat("\n=== Matching:", label, "===\n")
  cat("Input addresses:", nrow(evict_addys), "\n")

  # Tier 1: full address match
  t1 <- merge(
    evict_addys,
    parcel_addys[, .(pm.house, pm.street, pm.zip_norm, pm.streetSuf, pm.sufDir, pm.preDir, PID)],
    by = c("pm.house", "pm.street", "pm.streetSuf", "pm.sufDir", "pm.preDir", "pm.zip_norm"),
    all.x = TRUE, allow.cartesian = TRUE
  )
  setDT(t1)
  t1[, num_pids := uniqueN(PID, na.rm = TRUE), by = addr_key]
  t1[, matched := num_pids == 1]
  matched_t1 <- t1[matched == TRUE, unique(addr_key)]
  cat("Tier 1 (full): ", length(matched_t1), " unique matches\n")

  # Tier 2: house + street + zip
  t2 <- evict_addys[!addr_key %in% matched_t1] %>%
    merge(
      parcel_addys[, .(pm.house, pm.street, pm.zip_norm, PID)],
      by = c("pm.house", "pm.street", "pm.zip_norm"),
      all.x = TRUE, allow.cartesian = TRUE
    )
  setDT(t2)
  t2[, num_pids := uniqueN(PID, na.rm = TRUE), by = addr_key]
  t2[, matched := num_pids == 1]
  matched_t2 <- t2[matched == TRUE, unique(addr_key)]
  cat("Tier 2 (house+st+zip): ", length(matched_t2), " unique matches\n")

  # Tier 3: house + street + suffix
  t3 <- evict_addys[!addr_key %in% c(matched_t1, matched_t2)] %>%
    merge(
      parcel_addys[, .(pm.house, pm.street, pm.streetSuf, PID)],
      by = c("pm.house", "pm.street", "pm.streetSuf"),
      all.x = TRUE, allow.cartesian = TRUE
    )
  setDT(t3)
  t3[, num_pids := uniqueN(PID, na.rm = TRUE), by = addr_key]
  t3[, matched := num_pids == 1]
  matched_t3 <- t3[matched == TRUE, unique(addr_key)]
  cat("Tier 3 (house+st+sfx): ", length(matched_t3), " unique matches\n")

  total_unique <- nrow(evict_addys)
  total_matched <- length(matched_t1) + length(matched_t2) + length(matched_t3)
  unmatched <- total_unique - total_matched
  cat("\n--- Summary (", label, ") ---\n")
  cat("Total unique addresses: ", total_unique, "\n")
  cat("Total matched (tiers 1-3): ", total_matched, " (", round(100 * total_matched / total_unique, 1), "%)\n")
  cat("Unmatched: ", unmatched, " (", round(100 * unmatched / total_unique, 1), "%)\n")

  list(matched_t1 = matched_t1, matched_t2 = matched_t2, matched_t3 = matched_t3,
       total = total_unique, matched = total_matched)
}

# --- Prepare CURRENT (defendant_address) addresses ---
cat("\n=== Preparing current (defendant_address) addresses ===\n")
current <- evict_current[commercial == "f" & year >= 2000 & !is.na(n_sn_ss_c) & n_sn_ss_c != ""]
current_addys <- unique(current[, .(n_sn_ss_c, pm.house, pm.street, pm.streetSuf, pm.sufDir, pm.preDir, pm.zip)],
                        by = "n_sn_ss_c")
# Normalize zip
current_addys[, pm.zip_norm := str_remove(as.character(pm.zip), "^_") %>%
                str_pad(5, "left", pad = "0")]
current_addys[pm.zip_norm %in% c("   NA", "NA"), pm.zip_norm := NA_character_]
current_addys[, addr_key := n_sn_ss_c]

# --- Prepare NEW (clean_address) addresses ---
cat("=== Preparing new (clean_address) addresses ===\n")
new_addys <- ca_merged[!is.na(n_sn_ss_c_ca) & n_sn_ss_c_ca != ""]
new_addys <- unique(new_addys[, .(n_sn_ss_c_ca, pm.house, pm.street, pm.streetSuf, pm.sufDir, pm.preDir, pm.zip)],
                    by = "n_sn_ss_c_ca")
new_addys[, pm.zip_norm := pm.zip]
new_addys[pm.zip_norm %in% c("   NA", "NA"), pm.zip_norm := NA_character_]
new_addys[, addr_key := n_sn_ss_c_ca]

# Run matching
result_current <- run_match(current_addys, parcel_addys, "defendant_address (current)")
result_new     <- run_match(new_addys, parcel_addys, "clean_address (new)")

# --- Compare overlap ---
cat("\n=== Overlap Analysis ===\n")
both <- intersect(current_addys$addr_key, new_addys$addr_key)
only_current <- setdiff(current_addys$addr_key, new_addys$addr_key)
only_new <- setdiff(new_addys$addr_key, current_addys$addr_key)
cat("Addresses in both: ", length(both), "\n")
cat("Only in defendant_address: ", length(only_current), "\n")
cat("Only in clean_address: ", length(only_new), "\n")

# Where clean_address produces a DIFFERENT key, show examples
cat("\n=== Sample differences (same id, different n_sn_ss_c) ===\n")
compare <- merge(
  evict_current[, .(id, n_sn_ss_c_def = n_sn_ss_c)],
  ca_merged[, .(id, n_sn_ss_c_ca)],
  by = "id"
)
diffs <- compare[!is.na(n_sn_ss_c_def) & !is.na(n_sn_ss_c_ca) & n_sn_ss_c_def != n_sn_ss_c_ca]
cat("Records with same id but different address key:", nrow(diffs), "of", nrow(compare), "\n")
if (nrow(diffs) > 0) {
  print(head(diffs, 20))
}
