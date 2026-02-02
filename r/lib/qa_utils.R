## ============================================================
## qa_utils.R
## ============================================================
## Purpose: Merge diagnostics and QA utilities for address matching
##          Standardizes the "ugly code blocks" in merge scripts
##
## Usage: source("r/lib/qa_utils.R")
##
## Functions:
##   - merge_summary_stats(): Compute tier-level merge statistics
##   - write_merge_summary(): Write QA tables to CSV
##   - sample_unmatched(): Random sample of unmatched addresses
##   - diagnose_unmatched_addresses(): Flag WHY addresses fail to match
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
})

# ============================================================
# MERGE SUMMARY STATISTICS
# ============================================================

#' Compute merge summary statistics for a tier
#'
#' Returns standardized diagnostics tables for each merge tier.
#'
#' @param dt data.table with merge results
#' @param addr_key Column name for address key (default: "n_sn_ss_c")
#' @param num_pids_col Column name for PID count per address (default: "num_pids")
#' @param street_col Column name for street (default: "pm.street")
#' @param top_n Number of top items to include (default: 20)
#' @param restrict_to_unique_addr Compute stats on unique addresses only (default: TRUE)
#' @param label Label for this tier (stored in output tables)
#' @return List of data.tables: dist_num_pids, matched_vs_unmatched,
#'         top_unmatched_streets, top_unmatched_addresses
#'
#' @examples
#' stats <- merge_summary_stats(tier_dt, label = "tier_1_full_match")
merge_summary_stats <- function(dt,
                                addr_key = "n_sn_ss_c",
                                num_pids_col = "num_pids",
                                street_col = "pm.street",
                                top_n = 20,
                                restrict_to_unique_addr = TRUE,
                                label = NULL) {

  dt <- as.data.table(dt)

  # Work on unique addresses if requested
  if (restrict_to_unique_addr) {
    dt_unique <- unique(dt, by = addr_key)
  } else {
    dt_unique <- dt
  }

  # 1. Distribution of num_pids
  dist_num_pids <- dt_unique[, .N, by = c(num_pids_col)]
  setnames(dist_num_pids, num_pids_col, "num_pids")
  dist_num_pids[, pct := round(N / sum(N), 4)]
  dist_num_pids[order(num_pids)]
  if (!is.null(label)) dist_num_pids[, tier := label]

  # 2. Matched vs unmatched
  dt_unique[, .unmatched := get(num_pids_col) == 0]
  matched_vs_unmatched <- dt_unique[, .N, by = .unmatched]
  setnames(matched_vs_unmatched, ".unmatched", "unmatched")
  matched_vs_unmatched[, pct := round(N / sum(N), 4)]
  matched_vs_unmatched[, status := fifelse(unmatched, "unmatched", "matched")]
  if (!is.null(label)) matched_vs_unmatched[, tier := label]

  # 3. Top unmatched streets
  if (street_col %in% names(dt_unique)) {
    top_unmatched_streets <- dt_unique[get(num_pids_col) == 0, .N, by = c(street_col)]
    setnames(top_unmatched_streets, street_col, "street")
    top_unmatched_streets[, pct := round(N / sum(N), 4)]
    top_unmatched_streets <- top_unmatched_streets[order(-N)][1:min(top_n, .N)]
    top_unmatched_streets[, cum_pct := cumsum(pct)]
    if (!is.null(label)) top_unmatched_streets[, tier := label]
  } else {
    top_unmatched_streets <- data.table()
  }

  # 4. Top unmatched addresses
  top_unmatched_addresses <- dt_unique[get(num_pids_col) == 0, .N, by = c(addr_key)]
  setnames(top_unmatched_addresses, addr_key, "address_key")
  top_unmatched_addresses[, pct := round(N / sum(N), 4)]
  top_unmatched_addresses <- top_unmatched_addresses[order(-N)][1:min(top_n, .N)]
  top_unmatched_addresses[, cum_pct := cumsum(pct)]
  if (!is.null(label)) top_unmatched_addresses[, tier := label]

  list(
    dist_num_pids = dist_num_pids,
    matched_vs_unmatched = matched_vs_unmatched,
    top_unmatched_streets = top_unmatched_streets,
    top_unmatched_addresses = top_unmatched_addresses
  )
}


#' Write merge summary tables to CSV files
#'
#' @param stats List from merge_summary_stats()
#' @param out_dir Output directory path
#' @param prefix File name prefix (e.g., "tier_1")
#' @return Invisible NULL
#'
#' @examples
#' write_merge_summary(stats, "output/qa/merge-evict-rentals", "tier_1")
write_merge_summary <- function(stats, out_dir, prefix) {
  # Create directory if needed
  if (!dir.exists(out_dir)) {
    dir.create(out_dir, recursive = TRUE)
  }

  # Write each table
  fwrite(stats$dist_num_pids,
         file.path(out_dir, paste0(prefix, "_dist_num_pids.csv")))
  fwrite(stats$matched_vs_unmatched,
         file.path(out_dir, paste0(prefix, "_matched_vs_unmatched.csv")))

  if (nrow(stats$top_unmatched_streets) > 0) {
    fwrite(stats$top_unmatched_streets,
           file.path(out_dir, paste0(prefix, "_top_unmatched_streets.csv")))
  }

  fwrite(stats$top_unmatched_addresses,
         file.path(out_dir, paste0(prefix, "_top_unmatched_addresses.csv")))

  invisible(NULL)
}


# ============================================================
# UNMATCHED SAMPLING
# ============================================================

#' Sample unmatched addresses for manual inspection
#'
#' @param dt data.table with merge results
#' @param addr_key Column name for address key (default: "n_sn_ss_c")
#' @param num_pids_col Column name for PID count (default: "num_pids")
#' @param n Number of samples to return (default: 200)
#' @param strata Sampling strategy: "none" for simple random, "street" for
#'        stratified by street (samples streets first, then addresses within)
#' @param seed Random seed for reproducibility
#' @return data.table of sampled unmatched addresses
#'
#' @examples
#' sample <- sample_unmatched(tier_dt, n = 100, strata = "street", seed = 123)
sample_unmatched <- function(dt,
                             addr_key = "n_sn_ss_c",
                             num_pids_col = "num_pids",
                             street_col = "pm.street",
                             n = 200,
                             strata = c("none", "street"),
                             seed = NULL) {

  strata <- match.arg(strata)
  dt <- as.data.table(dt)

  # Get unique unmatched addresses
  unmatched <- unique(dt[get(num_pids_col) == 0], by = addr_key)

  if (nrow(unmatched) == 0) {
    message("No unmatched addresses to sample")
    return(data.table())
  }

  if (!is.null(seed)) set.seed(seed)

  if (strata == "none") {
    # Simple random sample
    sample_idx <- sample(seq_len(nrow(unmatched)), min(n, nrow(unmatched)))
    result <- unmatched[sample_idx]
  } else if (strata == "street") {
    # Stratified by street: sample streets, then addresses within
    if (!street_col %in% names(unmatched)) {
      warning("street_col not found, falling back to simple random sample")
      sample_idx <- sample(seq_len(nrow(unmatched)), min(n, nrow(unmatched)))
      result <- unmatched[sample_idx]
    } else {
      streets <- unique(unmatched[[street_col]])
      n_streets <- min(length(streets), ceiling(n / 5))  # Sample ~5 addresses per street
      sampled_streets <- sample(streets, n_streets)

      result <- unmatched[get(street_col) %in% sampled_streets]

      # If we have more than n, subsample
      if (nrow(result) > n) {
        result <- result[sample(seq_len(.N), n)]
      }
    }
  }

  result
}


# ============================================================
# UNMATCHED DIAGNOSTICS
# ============================================================

#' Diagnose why addresses fail to match
#'
#' Creates "shadow keys" to test hypotheses about match failures:
#' - Ordinal mismatch (FOURTH vs 4TH)
#' - Suffix/direction mismatch
#' - Zip mismatch
#' - Street not in parcels
#' - Missing house number
#'
#' @param src_addys Unique source addresses (data.table with parsed components)
#' @param parcel_addys Unique parcel addresses (data.table with parsed components)
#' @param addr_key Address key column name (default: "n_sn_ss_c")
#' @param street_col Street column name (default: "pm.street")
#' @param house_col House number column name (default: "pm.house")
#' @param suffix_col Suffix column name (default: "pm.streetSuf")
#' @param predir_col Pre-direction column name (default: "pm.preDir")
#' @param sufdir_col Suf-direction column name (default: "pm.sufDir")
#' @param zip_col Zip column name (default: "pm.zip")
#' @param top_n Number of top examples per reason (default: 50)
#' @param seed Random seed for sampling
#' @return List with: flagged (data.table with all flags), reason_counts,
#'         examples (top by reason), sample (random unmatched sample)
#'
#' @examples
#' diag <- diagnose_unmatched_addresses(evict_addys, parcel_addys)
#' fwrite(diag$reason_counts, "output/qa/unmatched_reason_counts.csv")
diagnose_unmatched_addresses <- function(src_addys,
                                         parcel_addys,
                                         addr_key = "n_sn_ss_c",
                                         street_col = "pm.street",
                                         house_col = "pm.house",
                                         suffix_col = "pm.streetSuf",
                                         predir_col = "pm.preDir",
                                         sufdir_col = "pm.sufDir",
                                         zip_col = "pm.zip",
                                         top_n = 50,
                                         seed = 123) {

  # Source the address utils for ord_words_to_nums if not already loaded
  if (!exists("ord_words_to_nums", mode = "function")) {
    source("r/lib/address_utils.R")
  }

  src_addys <- as.data.table(copy(src_addys))
  parcel_addys <- as.data.table(parcel_addys)

  # Get parcel address key set for fast lookup
  parcel_keys <- unique(parcel_addys[[addr_key]])
  parcel_streets <- unique(parcel_addys[[street_col]])

  # Work only with unmatched
  unmatched <- src_addys[!get(addr_key) %in% parcel_keys]

  if (nrow(unmatched) == 0) {
    message("No unmatched addresses to diagnose")
    return(list(
      flagged = data.table(),
      reason_counts = data.table(reason = character(), N = integer(), pct = numeric()),
      examples = data.table(),
      sample = data.table()
    ))
  }

  # ---- Create shadow keys and flags ----

  # Check which columns exist (do this once, outside data.table operations)
  has_street <- street_col %in% names(unmatched)
  has_house <- house_col %in% names(unmatched)
  has_predir <- predir_col %in% names(unmatched)
  has_sufdir <- sufdir_col %in% names(unmatched)
  has_suffix <- suffix_col %in% names(unmatched)

  # Ensure columns exist with empty defaults if missing
  if (!has_predir) unmatched[, (predir_col) := ""]
  if (!has_sufdir) unmatched[, (sufdir_col) := ""]
  if (!has_suffix) unmatched[, (suffix_col) := ""]

  # 1. Ordinal mismatch: convert word ordinals to numeric in street name
  if (has_street && has_house) {
    unmatched[, street_ord_numeric := ord_words_to_nums(get(street_col))]
    unmatched[, key_ord_numeric := str_squish(str_to_lower(paste(
      get(house_col),
      replace_na(get(predir_col), ""),
      street_ord_numeric,
      replace_na(get(suffix_col), ""),
      replace_na(get(sufdir_col), "")
    )))]
    unmatched[, flag_ordinal_fixable := key_ord_numeric %in% parcel_keys & key_ord_numeric != get(addr_key)]
  } else {
    unmatched[, flag_ordinal_fixable := FALSE]
  }

  # 2. Street not in parcels
  if (has_street) {
    unmatched[, flag_street_not_in_parcels := !get(street_col) %in% parcel_streets]
  } else {
    unmatched[, flag_street_not_in_parcels := FALSE]
  }

  # 3. Missing house number
  if (has_house) {
    unmatched[, flag_missing_house := is.na(get(house_col)) | get(house_col) == ""]
  } else {
    unmatched[, flag_missing_house := TRUE]
  }

  # 4. House number contains range
  if (has_house) {
    unmatched[, flag_house_range := str_detect(as.character(get(house_col)), "-")]
  } else {
    unmatched[, flag_house_range := FALSE]
  }

  # 5. Direction mismatch: try without pre-direction
  if (has_predir && has_street && has_house) {
    unmatched[, key_no_predir := str_squish(str_to_lower(paste(
      get(house_col),
      "",  # no predir
      get(street_col),
      replace_na(get(suffix_col), ""),
      replace_na(get(sufdir_col), "")
    )))]
    unmatched[, flag_predir_mismatch := key_no_predir %in% parcel_keys &
                key_no_predir != get(addr_key) &
                get(predir_col) != "" &
                !is.na(get(predir_col))]
  } else {
    unmatched[, flag_predir_mismatch := FALSE]
  }

  # 6. Suffix mismatch: try without suffix
  if (has_suffix && has_street && has_house) {
    unmatched[, key_no_suffix := str_squish(str_to_lower(paste(
      get(house_col),
      replace_na(get(predir_col), ""),
      get(street_col),
      "",  # no suffix
      replace_na(get(sufdir_col), "")
    )))]
    unmatched[, flag_suffix_mismatch := key_no_suffix %in% parcel_keys &
                key_no_suffix != get(addr_key)]
  } else {
    unmatched[, flag_suffix_mismatch := FALSE]
  }

  # ---- Assign primary reason (priority order) ----
  unmatched[, unmatched_reason := fcase(
    flag_street_not_in_parcels == TRUE, "street_not_in_parcels",
    flag_missing_house == TRUE, "missing_house",
    flag_ordinal_fixable == TRUE, "ordinal_word_numeric_mismatch",
    flag_predir_mismatch == TRUE, "predir_mismatch",
    flag_suffix_mismatch == TRUE, "suffix_mismatch",
    flag_house_range == TRUE, "house_range",
    default = "other"
  )]

  # ---- Summary tables ----

  # Reason counts
  reason_counts <- unmatched[, .N, by = unmatched_reason]
  reason_counts[, pct := round(N / sum(N), 4)]
  reason_counts <- reason_counts[order(-N)]

  # Top examples by reason
  set.seed(seed)
  examples_list <- lapply(unique(unmatched$unmatched_reason), function(r) {
    subset <- unmatched[unmatched_reason == r]
    if (nrow(subset) > top_n) {
      subset <- subset[sample(seq_len(.N), top_n)]
    }
    subset
  })
  examples <- rbindlist(examples_list, fill = TRUE)

  # Random sample across all unmatched
  set.seed(seed)
  n_sample <- min(200, nrow(unmatched))
  sample_unmatched <- unmatched[sample(seq_len(.N), n_sample)]

  list(
    flagged = unmatched,
    reason_counts = reason_counts,
    examples = examples,
    sample = sample_unmatched
  )
}


#' Write unmatched diagnostics to files
#'
#' @param diag List from diagnose_unmatched_addresses()
#' @param out_dir Output directory path
#' @param prefix File name prefix
#' @return Invisible NULL
write_unmatched_diagnostics <- function(diag, out_dir, prefix = "unmatched") {
  if (!dir.exists(out_dir)) {
    dir.create(out_dir, recursive = TRUE)
  }

  fwrite(diag$reason_counts,
         file.path(out_dir, paste0(prefix, "_reason_counts.csv")))
  fwrite(diag$examples,
         file.path(out_dir, paste0(prefix, "_fixable_examples.csv")))
  fwrite(diag$sample,
         file.path(out_dir, paste0(prefix, "_random_sample.csv")))

  invisible(NULL)
}


# ============================================================
# LOGGING HELPERS
# ============================================================

#' Log merge statistics to console and/or file
#'
#' @param stats List from merge_summary_stats()
#' @param tier_name Name of the tier for logging
#' @param log_file Optional log file path
#' @param logf_fn Logging function (default: message)
log_merge_stats <- function(stats, tier_name, log_file = NULL, logf_fn = NULL) {
  # Use provided logf or fall back to message
  if (is.null(logf_fn)) {
    logf_fn <- function(..., log_file = NULL) message(paste0(...))
  }

  logf_fn("--- ", tier_name, " ---", log_file = log_file)

  # Match rate
  matched <- stats$matched_vs_unmatched[status == "matched", N]
  unmatched <- stats$matched_vs_unmatched[status == "unmatched", N]
  total <- matched + unmatched
  match_rate <- round(matched / total * 100, 1)

  logf_fn("  Unique addresses: ", total, log_file = log_file)
  logf_fn("  Matched: ", matched, " (", match_rate, "%)", log_file = log_file)
  logf_fn("  Unmatched: ", unmatched, " (", round(100 - match_rate, 1), "%)", log_file = log_file)

  # Top unmatched streets
  if (nrow(stats$top_unmatched_streets) > 0) {
    logf_fn("  Top 5 unmatched streets:", log_file = log_file)
    for (i in seq_len(min(5, nrow(stats$top_unmatched_streets)))) {
      row <- stats$top_unmatched_streets[i]
      logf_fn("    ", row$street, ": ", row$N, " (", round(row$pct * 100, 1), "%)", log_file = log_file)
    }
  }

  invisible(NULL)
}
