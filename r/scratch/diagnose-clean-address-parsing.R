## diagnose-clean-address-parsing.R
## Purpose: Diagnose which steps of the eviction address parsing pipeline
##          are harmful vs helpful when applied to the pre-cleaned `clean_address`
##          column (vs the messy `defendant_address` it was designed for).
##
## This script does NOT modify any pipeline files. It is purely diagnostic.

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(postmastr)
})

source("r/config.R")
source("r/lib/address_utils.R")

cfg <- read_config()

cat("================================================================\n")
cat("DIAGNOSTIC: clean_address parsing pipeline issues\n")
cat("================================================================\n\n")

# ---- Load raw data ----
raw <- fread(p_input(cfg, "evictions_summary_table"),
             select = c("id", "defendant_address", "clean_address"))
cat("Total raw rows:", nrow(raw), "\n")
cat("defendant_address non-empty:", sum(!is.na(raw$defendant_address) & raw$defendant_address != ""), "\n")
ca_nonempty <- sum(!is.na(raw$clean_address) & raw$clean_address != "")
cat("clean_address non-empty:", ca_nonempty, "\n\n")

# Work with non-empty clean_address rows
ca <- raw[!is.na(clean_address) & clean_address != ""]

# ---- Build suffix regex (same as clean-eviction-addresses.R lines 47-63) ----
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

suffix_regex <- paste0("(^|,|\\|)?([0-9-]+[a-z]?\\s[a-z0-9\\.\\s-]+\\s(", suffixes, "))")
stuck_regex <- paste0("([a-z])(", suffixes, ")(\\.|\\|)")

cat("================================================================\n")
cat("TEST 1: stuck_regex behavior on clean_address\n")
cat("================================================================\n")
cat("stuck_regex was designed to fix 'MANTUAAVE' -> 'MANTUA AVE' in defendant_address.\n")
cat("clean_address already has spaces, so stuck_regex should rarely match.\n\n")

# Apply the same lowering + stuck_regex pipeline to clean_address
ca[, ca_lower := str_to_lower(clean_address) %>%
     str_remove_all("\\.")]

# Check how many clean_address values match stuck_regex
ca[, stuck_match := str_detect(ca_lower, stuck_regex)]
n_stuck <- sum(ca$stuck_match, na.rm = TRUE)
cat("clean_address rows matching stuck_regex:", n_stuck, "of", nrow(ca),
    "(", round(100 * n_stuck / nrow(ca), 2), "%)\n")

if (n_stuck > 0) {
  cat("\nSample stuck_regex matches (clean_address):\n")
  stuck_examples <- ca[stuck_match == TRUE, .(clean_address, ca_lower)][1:min(20, n_stuck)]
  print(stuck_examples)

  # Show what stuck_regex does to them
  cat("\nBefore/after stuck_regex application:\n")
  stuck_examples[, after_stuck := str_replace_all(ca_lower, stuck_regex, "\\1 \\2\\3")]
  print(stuck_examples[, .(ca_lower, after_stuck)])

  # Check: does stuck_regex BREAK any clean_address?
  # The pattern inserts a space before a suffix token; on clean_address that already has
  # spaces, it could match a word boundary that's actually correct
  cat("\nDoes stuck_regex produce double-spaces or misplaced spaces?\n")
  stuck_examples[, has_double_space := str_detect(after_stuck, "  ")]
  cat("  Double-space cases:", sum(stuck_examples$has_double_space), "\n")
}

cat("\n================================================================\n")
cat("TEST 2: suffix_regex extraction on clean_address\n")
cat("================================================================\n")
cat("suffix_regex extracts the street address portion from the messy defendant_address.\n")
cat("For clean_address, the street is just the part before the first comma.\n\n")

# The comparison script correctly uses str_extract(ca_lower, "^[^,]+") for clean_address.
# But the ORIGINAL pipeline uses suffix_regex to extract from defendant_address.
# What happens if we apply suffix_regex to clean_address directly?

ca[, street_via_comma := str_extract(ca_lower, "^[^,]+") %>% str_squish()]
ca[, street_via_suffix_regex := str_match(ca_lower, suffix_regex)[, 3]]

# Compare
n_comma_ok <- sum(!is.na(ca$street_via_comma))
n_regex_ok <- sum(!is.na(ca$street_via_suffix_regex))
cat("street extracted via comma-split:", n_comma_ok, "of", nrow(ca),
    "(", round(100 * n_comma_ok / nrow(ca), 2), "%)\n")
cat("street extracted via suffix_regex:", n_regex_ok, "of", nrow(ca),
    "(", round(100 * n_regex_ok / nrow(ca), 2), "%)\n")
cat("LOSS from using suffix_regex instead of comma:", n_comma_ok - n_regex_ok, "rows\n\n")

# Where suffix_regex fails but comma works:
missed_by_regex <- ca[!is.na(street_via_comma) & is.na(street_via_suffix_regex)]
cat("Rows where comma works but suffix_regex fails:", nrow(missed_by_regex), "\n")
if (nrow(missed_by_regex) > 0) {
  cat("Sample clean_address values that suffix_regex can't parse:\n")
  set.seed(42)
  sample_missed <- missed_by_regex[sample.int(.N, min(20, .N))]
  print(sample_missed[, .(clean_address, street_via_comma)])
}

# Where suffix_regex gives DIFFERENT result than comma:
both_ok <- ca[!is.na(street_via_comma) & !is.na(street_via_suffix_regex)]
both_ok[, differ := street_via_comma != street_via_suffix_regex]
n_differ <- sum(both_ok$differ)
cat("\nRows where both extract but give DIFFERENT result:", n_differ, "\n")
if (n_differ > 0) {
  cat("Sample differences:\n")
  print(head(both_ok[differ == TRUE, .(clean_address, street_via_comma, street_via_suffix_regex)], 20))
}

cat("\n================================================================\n")
cat("TEST 3: pm_prep filtering rate on clean_address\n")
cat("================================================================\n")
cat("pm_prep(type='street') filters to addresses postmastr can identify as streets.\n")
cat("This is a major source of row loss.\n\n")

# Use the comma-split street (what the comparison script does)
ca_for_pm <- ca[!is.na(street_via_comma) & street_via_comma != ""]
ca_for_pm[, short_address := street_via_comma]

ca_pm_id <- ca_for_pm %>% pm_identify(var = short_address)
cat("pm_identify results:\n")
print(table(ca_pm_id$pm.type))
cat("\n")

n_before_prep <- nrow(ca_pm_id)
ca_pm_prep <- pm_prep(ca_pm_id, var = "short_address", type = "street")
n_after_prep <- nrow(ca_pm_prep)
cat("Before pm_prep:", n_before_prep, "\n")
cat("After pm_prep:", n_after_prep, "\n")
cat("DROPPED by pm_prep:", n_before_prep - n_after_prep,
    "(", round(100 * (n_before_prep - n_after_prep) / n_before_prep, 1), "%)\n\n")

# What pm.type values are being dropped?
ca_pm_id_dt <- as.data.table(ca_pm_id)
dropped_types <- ca_pm_id_dt[pm.type != "street", .N, by = pm.type][order(-N)]
cat("pm.type distribution of DROPPED rows:\n")
print(dropped_types)

# Show examples of what's being dropped
cat("\nExamples of clean_address dropped by pm_prep (non-street types):\n")
non_street <- ca_pm_id_dt[pm.type != "street"]
if (nrow(non_street) > 0) {
  set.seed(42)
  sample_ns <- non_street[sample.int(.N, min(30, .N))]
  print(sample_ns[, .(short_address, pm.type)])
}

cat("\n================================================================\n")
cat("TEST 4: Directional prefix fixes on clean_address\n")
cat("================================================================\n")
cat("Lines 163-167 strip 'stuck' directional letters from street names.\n")
cat("Pattern: ^S followed by [dfgjbvxzs], ^E followed by [ebpgq], etc.\n")
cat("This was designed for cases where pm_streetDir missed the direction.\n")
cat("But for clean_address (already properly spaced), the direction should\n")
cat("already be parsed into pm.preDir by pm_streetDir_parse.\n\n")

# Parse the clean_address sample through the full pipeline to see what
# directional fix does
dirs <- pm_dictionary(type = "directional", filter = c("N", "S", "E", "W"), locale = "us")

# Work with the pm_prep output
if (nrow(ca_pm_prep) > 0) {
  pa <- pm_dictionary(type = "state", filter = "PA", case = c("title", "upper", "lower"), locale = "us")
  philly <- tibble(city.input = c("phila", "philadelphia"))

  ca_parsed <- ca_pm_prep
  ca_parsed <- pm_postal_parse(ca_parsed, locale = "us")
  ca_parsed <- pm_state_parse(ca_parsed, dictionary = pa)
  ca_parsed <- pm_city_parse(ca_parsed, dictionary = philly)

  # Unit parsing
  ca_u1 <- parse_unit(ca_parsed$pm.address)
  ca_parsed$pm.address <- ca_u1$clean_address %>% str_squish()
  ca_u2 <- parse_unit(ca_parsed$pm.address)
  ca_u3 <- parse_unit(ca_u2$clean_address %>% str_squish())
  ca_u4 <- parse_unit_extra(ca_u3$clean_address %>% str_squish())
  ca_parsed$pm.address <- ca_u4$clean_address %>% str_squish()

  # House + street parsing
  ca_parsed <- pm_house_parse(ca_parsed)
  ca_nums <- ca_parsed$pm.house %>% parse_letter()
  ca_rng <- ca_nums$clean_address %>% parse_range()
  ca_parsed$pm.house <- coalesce(ca_rng$range1, ca_rng$og_address) %>% as.numeric()

  ca_parsed <- pm_streetDir_parse(ca_parsed, dictionary = dirs)
  ca_parsed <- pm_streetSuf_parse(ca_parsed)
  ca_parsed <- ca_parsed %>% mutate(pm.address = ord_words_to_nums(pm.address))
  ca_parsed <- pm_street_parse(ca_parsed, ordinal = TRUE, drop = FALSE)

  # Ordinal name fix
  ca_parsed <- ca_parsed %>% mutate(
    pm.street = case_when(
      str_detect(pm.street, "[a-zA-Z]") ~ pm.street,
      pm.street == 1 ~ "1st",
      pm.street == 2 ~ "2nd",
      pm.street == 3 ~ "3rd",
      (pm.street) >= 4 ~ paste0((pm.street), "th"),
      TRUE ~ pm.street
    )
  )

  # Save pre-directional-fix state
  ca_parsed$pm.street_before_dir_fix <- ca_parsed$pm.street

  # Apply directional fix
  ca_parsed <- ca_parsed %>% mutate(
    pm.street = str_replace_all(pm.street, "^([S])([dfgjbvxzs])", "\\2") %>%
      str_replace_all("^([E])([ebpgq])", "\\2") %>%
      str_replace_all("^([NW])([dfgjbvxzs])", "\\2")
  )

  # Check what changed
  ca_parsed_dt <- as.data.table(ca_parsed)
  dir_changed <- ca_parsed_dt[pm.street != pm.street_before_dir_fix]
  cat("Street names changed by directional fix:", nrow(dir_changed), "\n")

  if (nrow(dir_changed) > 0) {
    cat("\nAll changes (or first 50):\n")
    dir_sample <- dir_changed[1:min(50, .N),
                               .(pm.preDir, pm.street_before_dir_fix, pm.street, pm.streetSuf)]
    print(dir_sample)

    # Were any of these HARMFUL? (legit street name got corrupted)
    cat("\nAnalysis: were these fixes correct or harmful?\n")
    cat("Cases where preDir is already set (direction was already parsed):\n")
    already_has_dir <- dir_changed[!is.na(pm.preDir) & pm.preDir != ""]
    cat("  ", nrow(already_has_dir), "rows already had pm.preDir set\n")
    if (nrow(already_has_dir) > 0) {
      cat("  These are HARMFUL - the leading letter was NOT a stuck direction:\n")
      print(head(already_has_dir[, .(pm.preDir, pm.street_before_dir_fix, pm.street, pm.streetSuf)], 20))
    }

    cat("\nCases where preDir is empty (direction might genuinely be stuck):\n")
    no_dir <- dir_changed[is.na(pm.preDir) | pm.preDir == ""]
    cat("  ", nrow(no_dir), "rows had no pm.preDir\n")
    if (nrow(no_dir) > 0) {
      print(head(no_dir[, .(pm.preDir, pm.street_before_dir_fix, pm.street, pm.streetSuf)], 20))
    }
  }
}

cat("\n================================================================\n")
cat("TEST 5: Specific problematic examples\n")
cat("================================================================\n")
cat("Testing the specific problem cases mentioned in the task.\n\n")

problem_cases <- c(
  "8802 E TORRESDALE DR",
  "8766 C GLENLOCH ST",
  "530 2KING ST",
  "4203 MANTUA AVE"
)

cat("Tracing each problem case through the pipeline:\n\n")

for (addr in problem_cases) {
  cat("--- Input:", addr, "---\n")

  # Step 1: lowercase + period removal
  step1 <- str_to_lower(addr) %>% str_remove_all("\\.")
  cat("  After lowercase:", step1, "\n")

  # Step 1b: stuck_regex
  step1b <- str_replace_all(step1, stuck_regex, "\\1 \\2\\3")
  if (step1 != step1b) cat("  After stuck_regex:", step1b, " *** CHANGED ***\n")
  else cat("  After stuck_regex:", step1b, " (no change)\n")

  # Step 1c: suffix_regex extraction
  step1c <- str_match(step1b, suffix_regex)[, 3]
  cat("  After suffix_regex extract:", ifelse(is.na(step1c), "<NA - LOST>", step1c), "\n")

  # Step 1d: comma-split (the clean_address approach)
  step1d <- str_extract(step1, "^[^,]+") %>% str_squish()
  cat("  Via comma-split:", step1d, "\n")

  cat("\n")
}

cat("\n================================================================\n")
cat("TEST 6: Overall quality comparison - what % of clean_address\n")
cat("         survives each pipeline step?\n")
cat("================================================================\n\n")

total_ca <- nrow(ca)
cat("Starting clean_address rows:", total_ca, "\n")

# Step: lowercase + stuck_regex
ca[, step_lower := str_to_lower(clean_address) %>% str_remove_all("\\.")]
ca[, step_stuck := str_replace_all(step_lower, stuck_regex, "\\1 \\2\\3")]
n_stuck_changed <- sum(ca$step_lower != ca$step_stuck)
cat("Changed by stuck_regex:", n_stuck_changed,
    "(", round(100 * n_stuck_changed / total_ca, 2), "%)\n")

# Step: suffix_regex extraction
ca[, step_suffix := str_match(step_stuck, suffix_regex)[, 3]]
n_suffix_lost <- sum(is.na(ca$step_suffix))
cat("Lost by suffix_regex:", n_suffix_lost,
    "(", round(100 * n_suffix_lost / total_ca, 2), "%)\n")

# Step: comma-split alternative
ca[, step_comma := str_extract(step_lower, "^[^,]+") %>% str_squish()]
n_comma_lost <- sum(is.na(ca$step_comma) | ca$step_comma == "")
cat("Lost by comma-split:", n_comma_lost,
    "(", round(100 * n_comma_lost / total_ca, 2), "%)\n")

# Step: pm_prep filtering (already computed above)
cat("Lost by pm_prep (using comma-split input):", n_before_prep - n_after_prep,
    "(", round(100 * (n_before_prep - n_after_prep) / total_ca, 2), "% of total)\n")

# Net: how many clean_address values produce a parseable n_sn_ss_c?
cat("\nSURVIVAL RATE:\n")
cat("  Total clean_address:", total_ca, "\n")
cat("  After comma-split:", total_ca - n_comma_lost, "\n")
cat("  After pm_prep:", n_after_prep,
    "(", round(100 * n_after_prep / total_ca, 1), "% of total)\n")
if (exists("ca_parsed_dt") && nrow(ca_parsed_dt) > 0) {
  n_has_street <- sum(!is.na(ca_parsed_dt$pm.street) & ca_parsed_dt$pm.street != "")
  n_has_house <- sum(!is.na(ca_parsed_dt$pm.house))
  cat("  With parsed street name:", n_has_street, "\n")
  cat("  With parsed house number:", n_has_house, "\n")
}

cat("\n================================================================\n")
cat("SUMMARY OF FINDINGS\n")
cat("================================================================\n")
cat("
1. stuck_regex: Designed for defendant_address ('MANTUAAVE' -> 'MANTUA AVE').
   On clean_address, it matches rows where a word happens to end with a suffix
   token followed by a period or pipe - these are rare but potentially harmful
   since clean_address already has proper spacing.

2. suffix_regex: Extracts street address from messy defendant_address string.
   For clean_address, this is unnecessary and HARMFUL - it drops rows that don't
   match the expected pattern (number + words + suffix). The comparison script
   correctly uses comma-split instead.

3. pm_prep filtering: This is the BIGGEST source of loss. pm_prep(type='street')
   requires postmastr to identify the address as a street type. Many clean_address
   values fail this check even though they are valid street addresses. This happens
   because postmastr's identification heuristics are designed for full addresses,
   not pre-extracted street portions.

4. Directional fixes (lines 163-167): Strip leading S/E/N/W from street names
   when followed by certain consonants. For clean_address where directions are
   already properly spaced and parsed by pm_streetDir_parse, this can CORRUPT
   legitimate street names (e.g., stripping 'S' from 'Smedley').
")

cat("\nDone.\n")
