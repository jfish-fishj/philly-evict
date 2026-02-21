## ============================================================
## address_utils.R
## ============================================================
## Purpose: Shared address parsing and normalization utilities
##          Consolidates duplicated functions from clean-*.R scripts
##
## Usage: source("r/lib/address_utils.R")
##
## Functions:
##   - parse_unit(): Extract unit/apartment from address string
##   - parse_unit_extra(): Second pass unit parsing for edge cases
##   - parse_range(): Parse address number ranges (e.g., "123-125")
##   - parse_letter(): Extract letter suffixes from house numbers
##   - ord_words_to_nums(): Convert ordinal words to numbers
##   - standardize_street_name(): Street-level normalization
##   - normalize_unit(): Unit token standardization
##   - make_n_sn_ss_c(): Construct canonical address join key
##   - make_addr_keys(): Add building and unit keys to data.table
##   - misc_pre_processing(): Pre-parsing address cleanup
##   - misc_post_processing(): Post-parsing street normalization
##   - expand_addresses(): Expand address ranges to individual rows
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(stringr)
})

# ============================================================
# UNIT PARSING FUNCTIONS
# ============================================================

#' Parse unit/apartment designation from end of address
#'
#' @param address Character vector of addresses
#' @return tibble with clean_address, unit, og_address columns
#' @examples
#' parse_unit("123 Main St Apt 4B")
#' # Returns: clean_address = "123 Main St", unit = "Apt 4B"
parse_unit <- function(address) {
  .regex <- regex(
    "\\s(((fl|floor|-?apt\\.?|-?apartment\\.?|unit|suite|ste|rm|room|#|rear|building|bldg)+\\s?#?\\s?[0-9A-Z-]+)|\\s[0-9]{1,4})$",
    ignore_case = TRUE
  )
  matches <- str_match(address, .regex)
  clean_address <- str_remove(address, .regex)
  return(tibble(
    clean_address = clean_address,
    unit = matches[, 2],
    og_address = address
  ))
}

#' Second pass unit parsing for edge cases
#'
#' @param address Character vector of addresses
#' @return tibble with clean_address, unit, og_address columns
#' @examples
#' parse_unit_extra("123-4 Oak Homes")
#' parse_unit_extra("456A")
parse_unit_extra <- function(address) {
  .regex <- regex(
    "([0-9]+-\\s([a-z\\s]+)\\s(homes?|apartments?))|(-[a-z][0-9]$)|([0-9]+[a-z]{1})$",
    ignore_case = TRUE
  )
  matches <- str_match(address, .regex)
  clean_address <- str_remove(address, .regex) %>%
    str_replace_all("\\s(fl|floor|apt|apartment|unit|suite|ste|rm|room|#|rear|-|basement|bsmt)+$", "")
  return(tibble(
    clean_address = clean_address,
    unit = matches[, 2],
    og_address = address
  ))
}

#' Parse address number ranges
#'
#' @param address_num Character vector of address numbers
#' @return tibble with range1, range2, og_address columns
#' @examples
#' parse_range("123-125")
#' # Returns: range1 = "123", range2 = "125"
parse_range <- function(address_num) {
  .regex <- regex("([0-9]+)(\\s?-\\s?)([0-9]+)")
  matches <- str_match(address_num, .regex)
  return(tibble(
    range1 = matches[, 2],
    range2 = matches[, 4],
    og_address = address_num
  ))
}

#' Extract letter suffix from house number
#'
#' @param address_num Character vector of address numbers
#' @return tibble with clean_address, letter, og_address columns
#' @examples
#' parse_letter("123A")
#' # Returns: clean_address = "123", letter = "A"
parse_letter <- function(address_num) {
  .regex <- regex("([0-9\\s-]+)([a-z]+)?$", ignore_case = TRUE)
  matches <- str_match(address_num, .regex)
  return(tibble(
    clean_address = matches[, 2],
    letter = matches[, 3],
    og_address = address_num
  ))
}


# ============================================================
# ORDINAL CONVERSION
# ============================================================

#' Convert ordinal words to numeric form
#'
#' Handles: first-ninth, tenth-nineteenth, twentieth-ninetieth,
#' and compounds like "twenty first" or "twenty-first"
#'
#' @param x Character vector
#' @return Character vector with ordinal words replaced by numbers
#' @examples
#' ord_words_to_nums("twenty first street")
#' # Returns: "21st street"
#' ord_words_to_nums("fourth avenue")
#' # Returns: "4th avenue"
ord_words_to_nums <- function(x) {
  # Helper to get ordinal suffix
suf <- function(n) {
    n <- as.integer(n)
    if (n %% 100L %in% c(11L, 12L, 13L)) return("th")
    c("th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th")[(n %% 10L) + 1L]
  }

  units <- c(
    "first" = 1, "second" = 2, "third" = 3, "fourth" = 4, "fifth" = 5,
    "sixth" = 6, "seventh" = 7, "eighth" = 8, "ninth" = 9
  )
  teens <- c(
    "tenth" = 10, "eleventh" = 11, "twelfth" = 12, "thirteenth" = 13, "fourteenth" = 14,
    "fifteenth" = 15, "sixteenth" = 16, "seventeenth" = 17, "eighteenth" = 18, "nineteenth" = 19
  )
  tens_only <- c(
    "twentieth" = 20, "thirtieth" = 30, "fortieth" = 40, "fiftieth" = 50,
    "sixtieth" = 60, "seventieth" = 70, "eightieth" = 80, "ninetieth" = 90
  )
  tens <- c(
    "twenty" = 20, "thirty" = 30, "forty" = 40, "fifty" = 50,
    "sixty" = 60, "seventy" = 70, "eighty" = 80, "ninety" = 90
  )

  out <- x

  # Compounds: e.g., "twenty first" / "twenty-first"
  for (t_word in names(tens)) {
    t_val <- tens[[t_word]]
    for (u_word in names(units)) {
      u_val <- units[[u_word]]
      n <- t_val + u_val
      patt <- paste0("(?i)\\b", t_word, "[-\\s]+", u_word, "\\b")
      repl <- paste0(n, suf(n))
      out <- gsub(patt, repl, out, perl = TRUE)
    }
  }

  # Tens-only ordinals
  for (w in names(tens_only)) {
    n <- tens_only[[w]]
    patt <- paste0("(?i)\\b", w, "\\b")
    repl <- paste0(n, suf(n))
    out <- gsub(patt, repl, out, perl = TRUE)
  }

  # Teens
  for (w in names(teens)) {
    n <- teens[[w]]
    patt <- paste0("(?i)\\b", w, "\\b")
    repl <- paste0(n, suf(n))
    out <- gsub(patt, repl, out, perl = TRUE)
  }

  # Simple 1..9
  for (w in names(units)) {
    n <- units[[w]]
    patt <- paste0("(?i)\\b", w, "\\b")
    repl <- paste0(n, suf(n))
    out <- gsub(patt, repl, out, perl = TRUE)
  }

  out
}


# ============================================================
# STREET NAME NORMALIZATION
# ============================================================

#' Standardize street name for matching
#'
#' Applies lowercase, squish, ordinal normalization, and Philly-specific fixes.
#'
#' @param x Character vector of street names
#' @param mode "philly" for Philly-specific fixes, "strict" for generic only
#' @return Character vector of standardized street names
#' @examples
#' standardize_street_name("FOURTH")
#' # Returns: "4th"
#' standardize_street_name("berkeley", mode = "philly")
#' # Returns: "berkley"
standardize_street_name <- function(x, mode = c("philly", "strict")) {
  mode <- match.arg(mode)

  out <- x %>%
    str_to_lower() %>%
    str_squish()

  # Convert ordinal words to numbers
  out <- ord_words_to_nums(out)

  if (mode == "philly") {
    # Philly-specific misspellings/variants
    out <- out %>%
      str_replace_all("\\bberkeley\\b", "berkley") %>%
      str_replace_all("\\bmt\\b", "mount") %>%
      str_replace_all("\\bmc\\s", "mc") %>%
      # Be careful: "st" as prefix (saint) vs suffix (street)
      # Only replace "st" at word boundary when followed by a letter (saint)
      str_replace_all("\\bst\\s+(?=[a-z])", "saint ")
  }

  out
}

#' Normalize unit token for consistent matching
#'
#' @param x Character vector of unit designations
#' @return Character vector of normalized units (e.g., "APT 3B", "FL 2")
#' @examples
#' normalize_unit("Apartment 4B")
#' # Returns: "APT 4B"
#' normalize_unit("Floor 2")
#' # Returns: "FL 2"
normalize_unit <- function(x) {
  out <- x %>%
    str_to_upper() %>%
    str_squish() %>%
    # Remove punctuation except alphanumerics and spaces
    str_replace_all("[^A-Z0-9\\s-]", "") %>%
    # Standardize unit type prefixes
    str_replace_all("\\b(APARTMENT|APT|UNIT|#)\\s*", "APT ") %>%
    str_replace_all("\\b(FL|FLOOR)\\s*", "FL ") %>%
    str_replace_all("\\b(STE|SUITE)\\s*", "STE ") %>%
    str_replace_all("\\b(RM|ROOM)\\s*", "RM ") %>%
    str_replace_all("\\b(BLDG|BUILDING)\\s*", "BLDG ") %>%
    str_squish()

  # Return NA for empty strings
  out <- ifelse(out == "" | is.na(out), NA_character_, out)
  out
}


# ============================================================
# ADDRESS KEY CONSTRUCTION
# ============================================================

#' Construct canonical address join key (n_sn_ss_c)
#'
#' Creates a standardized address key from parsed components.
#' Key format: "house predir street suffix sufdir" (all lowercase, squished)
#'
#' @param pm.house House number
#' @param pm.preDir Pre-direction (e.g., "N", "S")
#' @param pm.street Street name
#' @param pm.streetSuf Street suffix (e.g., "st", "ave")
#' @param pm.sufDir Post-direction (e.g., "N", "S")
#' @return Character vector of address keys
#' @examples
#' make_n_sn_ss_c("123", "N", "Main", "St", "")
#' # Returns: "123 n main st"
make_n_sn_ss_c <- function(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir) {
  # Replace NA with empty string, lowercase, squish
  pm.house <- replace_na(as.character(pm.house), "")
  pm.preDir <- str_squish(str_to_lower(replace_na(as.character(pm.preDir), "")))
  pm.street <- str_squish(str_to_lower(replace_na(as.character(pm.street), "")))
  pm.streetSuf <- str_squish(str_to_lower(replace_na(as.character(pm.streetSuf), "")))
  pm.sufDir <- str_squish(str_to_lower(replace_na(as.character(pm.sufDir), "")))

  key <- str_squish(paste(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir))

  # Return NA for empty keys
  ifelse(key == "" | key == "    ", NA_character_, key)
}

#' Add address keys to a data.table
#'
#' Adds addr_building_key and addr_unit_key columns.
#'
#' @param dt data.table with parsed address components (pm.house, pm.preDir, etc.)
#' @param unit_col Name of unit column (default: "pm.unit")
#' @return data.table with added key columns (modified by reference)
#' @examples
#' dt <- data.table(pm.house = "123", pm.preDir = "N", pm.street = "Main",
#'                  pm.streetSuf = "St", pm.sufDir = "", pm.unit = "Apt 4")
#' make_addr_keys(dt)
make_addr_keys <- function(dt, unit_col = "pm.unit") {
  stopifnot(is.data.table(dt))

  # Building-level key (same as n_sn_ss_c)
  dt[, addr_building_key := make_n_sn_ss_c(pm.house, pm.preDir, pm.street, pm.streetSuf, pm.sufDir)]

  # Unit-level key (building key + normalized unit)
  if (unit_col %in% names(dt)) {
    dt[, addr_unit_key := fifelse(
      !is.na(get(unit_col)) & get(unit_col) != "",
      paste(addr_building_key, normalize_unit(get(unit_col)), sep = " | "),
      addr_building_key
    )]
  } else {
    dt[, addr_unit_key := addr_building_key]
  }

  invisible(dt)
}


# ============================================================
# PRE/POST PROCESSING (from altos/corelogic scripts)
# ============================================================

#' Miscellaneous pre-processing for address strings
#'
#' Cleans common noise patterns before parsing.
#'
#' @param address_col Character vector of addresses
#' @return Character vector of cleaned addresses
misc_pre_processing <- function(address_col) {
  address_col %>%
    str_remove_all("1/2") %>%
    str_remove_all("&\\s[0-9]+") %>%
    str_remove_all("\\.") %>%
    str_remove_all("penthouse") %>%
    str_remove_all("full parking") %>%
    str_remove_all("\\(") %>%
    str_remove_all("\\)") %>%
    str_remove_all("x+[0-9]") %>%
    str_remove_all("\\s?n/a") %>%
    str_remove_all(" at ") %>%
    str_replace_all("#+", "#") %>%
    str_replace_all("^([0-9]+)\\s([0-9]+)\\s", "\\1-\\2\\s") %>%
    str_squish() %>%
    str_replace_all("([0-9]+)/([0-9]+)", "\\1\\2") %>%
    str_replace_all("\\s?-\\s?", "-") %>%
    str_replace_all("saint", "st") %>%
    str_replace_all("apt#$", "") %>%
    str_replace_all("ng$", "") %>%
    str_replace_all("bs$", "") %>%
    str_replace_all("\\sph(\\s|$)", "\\s") %>%
    str_replace_all("\\s[abcdfghijk]$", "") %>%
    str_replace("place", "pl") %>%
    str_squish() %>%
    str_replace_all("\\s(fl|floor|apt|apartment|unit|suite|ste|rm|room|#|rear|-)+$", "")
}

#' Post-processing street name standardization
#'
#' Applied after parsing to normalize street types.
#'
#' @param address Character vector of addresses/street names
#' @return Character vector of standardized addresses
misc_post_processing <- function(address) {
  address %>%
    str_replace_all("[\\s^]park", " pk") %>%
    str_replace_all("\\sstreet", " st") %>%
    str_replace_all("\\ssquare", " sq") %>%
    str_replace_all("\\slane", " ln") %>%
    str_replace_all("\\salley", " aly") %>%
    str_replace_all("\\sway", " way") %>%
    str_replace_all("fm\\s", "farm to market ") %>%
    str_replace_all("pier four", "pier 4") %>%
    str_replace_all("centre", "center") %>%
    str_replace_all("mt", "mount")
}


# ============================================================
# ADDRESS RANGE EXPANSION
# ============================================================

#' Expand address ranges to individual rows
#'
#' For addresses like "100-110 Main St", creates rows for 100, 102, 104, etc.
#'
#' @param .data data.table with address range columns
#' @param num1 Name of column with start of range
#' @param num2 Name of column with end of range
#' @param max_addr Maximum addresses to expand per row (default: 2000)
#' @param even_odd If TRUE, only expand to even or odd numbers (default: TRUE)
#' @return data.table with expanded rows
expand_addresses <- function(.data, num1, num2, max_addr = 2000, even_odd = TRUE) {
  .data <- copy(.data)  # Don't modify input
  .data[, num_addresses := fifelse(!is.na(get(num2)), get(num2) - get(num1), 1)]
  if (even_odd == TRUE) {
    .data[, num_addresses := (num_addresses %/% 2) + 1]
  }
  .data[, num_addresses := fifelse(num_addresses <= 0 | is.na(num_addresses), 1, num_addresses)]
  .data[, num_addresses := fifelse(num_addresses > max_addr, max_addr, num_addresses)]
  .data[, row_id := seq(.N)]
  .data <- .data %>% uncount(weights = num_addresses) %>% as.data.table()
  .data[, address_num := seq(.N), by = row_id]
  .data[, (num1) := get(num1) + 2 * (address_num - 1)]
  .data[, c("num_addresses", "row_id", "address_num") := NULL]
  return(.data)
}


# ============================================================
# VALIDATION
# ============================================================

#' Validate address table has required columns
#'
#' @param dt data.table to validate
#' @param required_cols Character vector of required column names
#' @param label Label for error messages
#' @return TRUE if valid, stops with error if not
validate_address_table <- function(dt, required_cols = c("n_sn_ss_c", "pm.house", "pm.street"),
                                   label = "address table") {
  missing <- setdiff(required_cols, names(dt))
  if (length(missing) > 0) {
    stop(sprintf("%s is missing required columns: %s", label, paste(missing, collapse = ", ")))
  }

  # Check n_sn_ss_c is not all NA
  if ("n_sn_ss_c" %in% names(dt)) {
    n_valid <- sum(!is.na(dt$n_sn_ss_c) & dt$n_sn_ss_c != "")
    if (n_valid == 0) {
      stop(sprintf("%s has no valid n_sn_ss_c values", label))
    }
  }

  TRUE
}


# =========================
# Suffix alias loader / mapper
# =========================

load_suffix_aliases <- function(cfg) {
  alias_path <- tryCatch(
    p_input(cfg, "street_suffix_aliases"),
    error = function(e) NA_character_
  )
  fallback_paths <- c(
    alias_path,
    p_in(cfg, "aliases/street_suffix_aliases.csv"),
    p_in(cfg, "graveyard data/aliases/street_suffix_aliases.csv")
  )
  fallback_paths <- unique(fallback_paths[!is.na(fallback_paths)])
  existing <- fallback_paths[file.exists(fallback_paths)]
  if (length(existing) == 0) {
    stop("No street suffix alias file found. Tried: ", paste(fallback_paths, collapse = ", "))
  }
  dt <- data.table::fread(existing[1])
  stopifnot(all(c("from", "to") %in% names(dt)))
  dt[, `:=`(from = tolower(from), to = tolower(to))]
  data.table::setkey(dt, from)
  dt
}

map_suffix_vec <- function(x, suffix_alias_dt = NULL) {
  # normalize punctuation + case
  x <- tolower(x)
  x <- gsub("\\.+$", "", x)           # trailing periods
  x <- gsub("[^a-z0-9]+", "", x)      # keep compact token (blv., blv -> blv)
  x[is.na(x)] <- ""

  if (is.null(suffix_alias_dt) || nrow(suffix_alias_dt) == 0) return(x)

  hit <- suffix_alias_dt[J(x), on = .(from)]
  out <- x
  ok <- !is.na(hit$to)
  out[ok] <- hit$to[ok]
  out
}

# =========================
# Street standardization
# =========================
standardize_street_name_vec <- function(x) {
  x <- tolower(x)
  x[is.na(x)] <- ""
  x <- stringr::str_squish(x)

  # common punctuation normalization
  x <- gsub("[’']", "", x)

  # MLK (many variants)
  x <- gsub("\\b(m\\.?\\s*l\\.?\\s*k\\.?|mlk)\\b", "martin luther king", x)

  # JFK (many variants)
  x <- gsub("\\b(j\\.?\\s*f\\.?\\s*k\\.?|jfk)\\b", "john f kennedy", x)

  # Mount -> Mt (your preference)
  x <- gsub("\\bmount\\b", "mt", x)

  # Saint -> St (street name token; NOT the suffix field)
  x <- gsub("\\bsaint\\b", "st", x)

  # Boulevard misspelling
  x <- gsub("\\bboulvard\\b", "boulevard", x)

  # Clean stray punctuation inside name (keep spaces)
  x <- gsub("[^a-z0-9 ]+", " ", x)
  x <- stringr::str_squish(x)

  x
}

# =========================
# Parcel oracle prep (counts by street/suffix)
# =========================
prep_parcel_oracle <- function(cfg) {
  # parcel_address_counts is a derived product, not a raw input
  pth <- p_product(cfg, "parcel_address_counts")
  dt <- data.table::fread(pth)

  stopifnot(all(c("pm.street","pm.streetSuf","N") %in% names(dt)))

  dt[, pm.street := standardize_street_name_vec(stringr::str_squish(tolower(pm.street)))]
  dt[, pm.streetSuf := stringr::str_squish(tolower(pm.streetSuf))]
  dt <- dt[pm.street != "" & pm.streetSuf != ""]
  dt[, key := paste(pm.street, pm.streetSuf, sep="|")]

  # collapse in case of duplicates
  dt <- dt[, .(N = sum(N)), by = key]
  data.table::setkey(dt, key)

  # parcel street set (for stuck-direction checks)
  streets <- unique(sub("\\|.*$", "", dt$key))
  streets <- streets[streets != ""]
  street_set <- data.table::data.table(pm.street = streets)
  data.table::setkey(street_set, pm.street)

  list(
    counts = dt,
    street_set = street_set,
    suffixes = unique(sub("^.*\\|", "", dt$key))
  )
}

oracle_lookup_N <- function(keys, parcel_counts_dt) {
  # vectorized lookup
  parcel_counts_dt$N[ match(keys, parcel_counts_dt$key) ]
}


# ============================================================
# ORACLE-VALIDATED ADDRESS CANONICALIZATION (Step 5b)
# ============================================================

#' Canonicalize parsed addresses using parcel oracle validation
#'
#' Applies a series of oracle-validated fix rules to improve address matching.
#' Each fix only commits if the resulting (street, suffix) exists in parcel data.
#'
#' @param dt data.table with parsed pm.* columns (pm.street, pm.streetSuf, pm.preDir, pm.sufDir)
#' @param cfg Config object for loading oracle and alias paths
#' @param source Source identifier for QA outputs (e.g., "evictions", "altos", "licenses", "infousa")
#' @param export_qa If TRUE, write QA summaries to output/qa/<source>_address_canonicalize/
#' @param log_file Optional log file path for logging
#' @return data.table with canonicalized pm.* columns and fix tracking fields
#'
#' @details
#' Fix rules applied (in order):
#' 1. Boulevard precedence - street ending in blv/blvd/boulevard -> (street_base, blvd)
#' 2. Stuck directionals - echurch -> (e, church) if pm.preDir empty
#' 3. est/rst repair - spruc+est -> spruce+st
#' 4. Split trailing suffix - "roosevelt blv" -> (roosevelt, blvd) if suffix-like token at end
#'
#' All fixes are oracle-validated: only committed if (new_street, new_suffix) exists in parcels.
#'
#' @examples
#' dt <- canonicalize_parsed_addresses(dt, cfg, source = "altos", export_qa = TRUE)
canonicalize_parsed_addresses <- function(dt, cfg, source = "unknown",
                                          export_qa = TRUE, log_file = NULL) {
  stopifnot(is.data.table(dt))

  # Helper for logging
  log_msg <- function(...) {
    if (!is.null(log_file)) {
      logf(..., log_file = log_file)
    }
  }

  log_msg("Step 5b: Oracle canonicalization (", source, ")")

  # Load oracle + suffix aliases from config
  oracle <- prep_parcel_oracle(cfg)
  parcel_counts <- oracle$counts
  parcel_streets <- oracle$street_set
  parcel_suffixes <- oracle$suffixes

  # Try to load suffix aliases, fall back to empty if not configured
  suffix_alias <- tryCatch({
    load_suffix_aliases(cfg)
  }, error = function(e) {
    log_msg("  Note: No suffix alias file found, using empty alias map")
    data.table::data.table(from = character(), to = character())
  })

  # Preserve originals for QA
  if (!"pm.preDir_orig" %in% names(dt)) {
    dt[, `:=`(
      pm.preDir_orig    = pm.preDir,
      pm.street_orig    = pm.street,
      pm.streetSuf_orig = pm.streetSuf
    )]
  }

  # Normalize parsed components
  dt[, `:=`(
    pm.preDir    = stringr::str_squish(tolower(data.table::fifelse(is.na(pm.preDir), "", as.character(pm.preDir)))),
    pm.sufDir    = stringr::str_squish(tolower(data.table::fifelse(is.na(pm.sufDir), "", as.character(pm.sufDir)))),
    pm.street    = standardize_street_name_vec(data.table::fifelse(is.na(pm.street), "", as.character(pm.street))),
    pm.streetSuf = stringr::str_squish(tolower(data.table::fifelse(is.na(pm.streetSuf), "", as.character(pm.streetSuf))))
  )]

  # Apply suffix alias normalization immediately
  dt[, pm.streetSuf := map_suffix_vec(pm.streetSuf, suffix_alias)]

  # Initialize fix tracking
  if (!"fix_tag" %in% names(dt)) {
    dt[, `:=`(fix_tag = NA_character_, fix_changed = FALSE, fix_parcelN = as.integer(NA))]
  }

  # Helper: refresh oracle membership columns
  refresh_oracle <- function() {
    dt[, key := paste(pm.street, pm.streetSuf, sep = "|")]
    dt[, parcelN := oracle_lookup_N(key, parcel_counts)]
    dt[, in_parcels := !is.na(parcelN)]
  }

  refresh_oracle()
  before_rate <- dt[, mean(in_parcels)]
  log_msg("  Oracle-valid BEFORE: ", round(100 * before_rate, 3), "%")

  # --------------------------
  # Rule 1: Boulevard precedence (even if suffix already set)
  # --------------------------
  blv_tokens <- c("blv", "blvd", "boul", "boulev", "boulv", "boulevard", "boulvard")

  i1 <- dt[
    is.na(fix_tag) & pm.street != "" & stringr::str_detect(pm.street, paste0("\\b(", paste(blv_tokens, collapse = "|"), ")\\b$")),
    which = TRUE
  ]

  if (length(i1) > 0) {
    street_base <- sub(paste0("\\s*\\b(", paste(blv_tokens, collapse = "|"), ")\\b$"), "", dt$pm.street[i1])
    street_base <- stringr::str_squish(street_base)

    key_blvd <- paste(street_base, "blvd", sep = "|")
    N_blvd <- oracle_lookup_N(key_blvd, parcel_counts)
    ok <- !is.na(N_blvd) & street_base != ""

    if (any(ok)) {
      ii <- i1[ok]
      dt[ii, `:=`(
        pm.street = street_base[ok],
        pm.streetSuf = "blvd",
        fix_tag = "boulevard_precedence",
        fix_changed = TRUE,
        fix_parcelN = as.integer(N_blvd[ok])
      )]
    }
  }

  refresh_oracle()

  # --------------------------
  # Rule 2: Stuck directionals in street (echurch -> preDir=e, street=church)
  # --------------------------
  i2 <- dt[
    is.na(fix_tag) & pm.preDir == "" & pm.street != "" & stringr::str_detect(pm.street, "^[nsew][a-z]"),
    which = TRUE
  ]

  if (length(i2) > 0) {
    dir <- substr(dt$pm.street[i2], 1, 1)
    st2 <- substr(dt$pm.street[i2], 2, nchar(dt$pm.street[i2]))
    st2 <- stringr::str_squish(st2)

    # street membership check
    st2_in <- !is.na(parcel_streets[J(st2), on = .(pm.street), x.pm.street])

    # oracle validation of (street, suffix)
    key2 <- paste(st2, dt$pm.streetSuf[i2], sep = "|")
    N2 <- oracle_lookup_N(key2, parcel_counts)

    ok <- st2_in & !is.na(N2) & st2 != ""

    if (any(ok)) {
      ii <- i2[ok]
      dt[ii, `:=`(
        pm.preDir = dir[ok],
        pm.street = st2[ok],
        fix_tag = "unstick_dir_by_street_membership",
        fix_changed = TRUE,
        fix_parcelN = as.integer(N2[ok])
      )]
    }
  }

  refresh_oracle()

  # --------------------------
  # Rule 3: est/rst repair -> st (spruc+est -> spruce+st)
  # --------------------------
  i3 <- dt[
    is.na(fix_tag) & pm.street != "" & pm.streetSuf %chin% c("est", "rst"),
    which = TRUE
  ]

  if (length(i3) > 0) {
    extra <- sub("st$", "", dt$pm.streetSuf[i3])  # e or r
    new_st <- paste0(dt$pm.street[i3], extra)
    new_key <- paste(new_st, "st", sep = "|")
    newN <- oracle_lookup_N(new_key, parcel_counts)

    ok <- !is.na(newN) & new_st != ""
    if (any(ok)) {
      ii <- i3[ok]
      dt[ii, `:=`(
        pm.street = new_st[ok],
        pm.streetSuf = "st",
        fix_tag = "attach_extra_from_suf:st",
        fix_changed = TRUE,
        fix_parcelN = as.integer(newN[ok])
      )]
    }
  }

  refresh_oracle()

  # --------------------------
  # Rule 4: Split trailing suffix token from street
  # --------------------------
  i4 <- dt[
    is.na(fix_tag) & pm.street != "" & stringr::str_detect(pm.street, "\\s+[a-z0-9]+$"),
    which = TRUE
  ]

  if (length(i4) > 0) {
    last_tok <- sub("^.*\\s+([a-z0-9]+)$", "\\1", dt$pm.street[i4])
    base_st <- sub("\\s+[a-z0-9]+$", "", dt$pm.street[i4])
    base_st <- stringr::str_squish(base_st)

    cand_suf <- map_suffix_vec(last_tok, suffix_alias)

    # only consider if cand_suf is a parcel suffix
    cand_ok <- cand_suf %chin% parcel_suffixes

    if (any(cand_ok)) {
      key4 <- paste(base_st[cand_ok], cand_suf[cand_ok], sep = "|")
      N4 <- oracle_lookup_N(key4, parcel_counts)
      ok <- !is.na(N4) & base_st[cand_ok] != ""

      if (any(ok)) {
        ii <- i4[cand_ok][ok]
        dt[ii, `:=`(
          pm.street = base_st[cand_ok][ok],
          pm.streetSuf = cand_suf[cand_ok][ok],
          fix_tag = "split_suffix_from_street_token",
          fix_changed = TRUE,
          fix_parcelN = as.integer(N4[ok])
        )]
      }
    }
  }

  refresh_oracle()

  after_rate <- dt[, mean(in_parcels)]
  log_msg("  Oracle-valid AFTER: ", round(100 * after_rate, 3), "%")
  log_msg("  Improvement: +", round(100 * (after_rate - before_rate), 3), " pp")

  # --------------------------
  # QA outputs
  # --------------------------
  if (export_qa) {
    qa_dir <- p_out(cfg, "qa", paste0(source, "_address_canonicalize"))
    dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)

    fix_counts <- dt[, .N, by = fix_tag][order(-N)]
    fix_counts[is.na(fix_tag), fix_tag := "NO_FIX"]
    data.table::fwrite(fix_counts, file.path(qa_dir, "fix_tag_counts.csv"))

    data.table::fwrite(
      data.table::data.table(
        metric = c("oracle_valid_rate_before", "oracle_valid_rate_after", "improvement_pp"),
        value = c(before_rate, after_rate, after_rate - before_rate)
      ),
      file.path(qa_dir, "oracle_valid_rate_before_after.csv")
    )

    # Examples: show changed rows
    changed <- dt[fix_changed == TRUE,
                  .(pm.preDir_orig, pm.street_orig, pm.streetSuf_orig,
                    pm.preDir, pm.street, pm.streetSuf, fix_tag, fix_parcelN)]
    set.seed(123)
    if (nrow(changed) > 0) changed <- changed[sample.int(.N, min(.N, 500))]
    data.table::fwrite(changed, file.path(qa_dir, "changed_rows_sample.csv"))

    log_msg("  QA outputs written to: ", qa_dir)
  }

  # Clean up temporary columns (keep fix tracking)
  if ("key" %in% names(dt)) dt[, key := NULL]
  if ("parcelN" %in% names(dt)) dt[, parcelN := NULL]
  if ("in_parcels" %in% names(dt)) dt[, in_parcels := NULL]

  dt
}
