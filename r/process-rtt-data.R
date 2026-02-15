## ============================================================
## process-rtt-data.R
## ============================================================
## Purpose: Clean RTT (Real Estate Transfer Tax) summary data,
##          match to parcels, filter to arms-length transfers.
##
## Inputs:
##   - cfg$inputs$rtt_summary (open-data/rtt_summary.csv)
##   - cfg$products$parcels_clean (clean/parcels_clean.csv)
##
## Outputs:
##   - cfg$products$rtt_clean (clean/rtt_clean.csv)
##
## Primary key: (PID, document_id)
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
})

# ---- Load config and set up logging ----
source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "process-rtt-data.log")

logf("=== Starting process-rtt-data.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

normalize_pid <- function(x) {
  stringr::str_pad(as.character(x), width = 9, side = "left", pad = "0")
}

# ---- Load inputs ----
rtt_path <- p_input(cfg, "rtt_summary")
logf("Loading RTT data: ", rtt_path, log_file = log_file)
rtt <- fread(rtt_path, colClasses = list(character = c("opa_account_num", "zip_code", "unit_num")))
logf("Loaded ", nrow(rtt), " rows, ", ncol(rtt), " cols", log_file = log_file)

parcels_path <- p_product(cfg, "parcels_clean")
logf("Loading parcels_clean: ", parcels_path, log_file = log_file)
parcels <- fread(parcels_path, select = "parcel_number")
parcels[, PID := normalize_pid(parcel_number)]
parcels[, parcel_number := NULL]
parcels <- unique(parcels)
logf("Loaded ", nrow(parcels), " unique parcels", log_file = log_file)

# ============================================================
# STEP 1: Basic cleaning
# ============================================================
logf("--- STEP 1: Basic cleaning ---", log_file = log_file)

# Parse dates
date_cols <- c("display_date", "document_date", "recording_date", "receipt_date")
for (dc in date_cols) {
  if (dc %in% names(rtt)) {
    rtt[, (dc) := as.Date(substr(get(dc), 1, 10))]
  }
}

# Extract year and year_quarter from display_date
rtt[, year := year(display_date)]
rtt[, year_quarter := paste0(year, "Q", quarter(display_date))]

logf("Date range: ", min(rtt$display_date, na.rm = TRUE), " to ",
     max(rtt$display_date, na.rm = TRUE), log_file = log_file)

# Convert money columns to numeric
money_cols <- c("cash_consideration", "other_consideration", "total_consideration",
                "assessed_value", "fair_market_value",
                "state_tax_amount", "local_tax_amount",
                "adjusted_cash_consideration", "adjusted_other_consideration",
                "adjusted_total_consideration", "adjusted_assessed_value",
                "adjusted_fair_market_value", "adjusted_state_tax_amount",
                "adjusted_local_tax_amount")
for (mc in intersect(money_cols, names(rtt))) {
  rtt[, (mc) := as.numeric(get(mc))]
}

# Convert opa_account_num to PID
n_before <- nrow(rtt)
# opa_account_num may have ".0" suffix from float encoding — strip it
rtt[, opa_account_num := str_remove(opa_account_num, "\\.0$")]
n_has_opa <- rtt[!is.na(opa_account_num) & nzchar(opa_account_num), .N]
logf("OPA account num coverage: ", n_has_opa, " / ", n_before,
     " (", round(100 * n_has_opa / n_before, 1), "%)", log_file = log_file)

# Drop rows without opa_account_num
rtt <- rtt[!is.na(opa_account_num) & nzchar(opa_account_num)]
logf("After dropping missing opa_account_num: ", nrow(rtt), " rows", log_file = log_file)

# Normalize to PID
rtt[, PID := normalize_pid(opa_account_num)]

# ============================================================
# STEP 2: Filter to relevant document types
# ============================================================
logf("--- STEP 2: Filter to deed/mortgage document types ---", log_file = log_file)

# Log document type distribution before filtering
doc_counts <- rtt[, .N, by = document_type][order(-N)]
logf("Document types (all):", log_file = log_file)
for (i in seq_len(nrow(doc_counts))) {
  logf("  ", doc_counts$document_type[i], ": ", doc_counts$N[i], log_file = log_file)
}

# Keep deed document types only
keep_types <- c("DEED", "SHERIFF'S DEED", "DEED SHERIFF", "DEED OF CONDEMNATION")
rtt[, document_type_upper := toupper(trimws(document_type))]
n_before <- nrow(rtt)
rtt <- rtt[document_type_upper %in% keep_types]
logf("Kept deed types: ", nrow(rtt), " / ", n_before,
     " (dropped ", n_before - nrow(rtt), ")", log_file = log_file)
rtt[, document_type_upper := NULL]

# ============================================================
# STEP 3: Match to parcels_clean
# ============================================================
logf("--- STEP 3: Match to parcels_clean ---", log_file = log_file)

n_before <- nrow(rtt)
n_unique_pid_before <- rtt[, uniqueN(PID)]

# Log match rate by document type BEFORE filtering
rtt[, matched := PID %in% parcels$PID]
match_by_type <- rtt[, .(total = .N, matched = sum(matched),
                          pct = round(100 * sum(matched) / .N, 1)),
                      by = document_type][order(-total)]
logf("Match rate by document type:", log_file = log_file)
for (i in seq_len(nrow(match_by_type))) {
  flag <- if (match_by_type$pct[i] < 95) " *** LOW" else ""
  logf("  ", match_by_type$document_type[i], ": ",
       match_by_type$matched[i], " / ", match_by_type$total[i],
       " (", match_by_type$pct[i], "%)", flag, log_file = log_file)
}
rtt[, matched := NULL]

rtt <- rtt[PID %in% parcels$PID]

logf("Matched to parcels: ", nrow(rtt), " / ", n_before, " rows",
     " (", rtt[, uniqueN(PID)], " / ", n_unique_pid_before, " PIDs)",
     log_file = log_file)

# ============================================================
# STEP 4: Arms-length transfer filter
# ============================================================
logf("--- STEP 4: Arms-length transfer filter ---", log_file = log_file)

n_before <- nrow(rtt)

# Flag each filter reason independently (rows can hit multiple)
rtt[, low_consideration := is.na(total_consideration) | total_consideration < 100]

rtt[, grantors_clean := toupper(trimws(grantors))]
rtt[, grantees_clean := toupper(trimws(grantees))]
rtt[, self_transfer := (!is.na(grantors_clean) & !is.na(grantees_clean) &
                         nzchar(grantors_clean) & nzchar(grantees_clean) &
                         grantors_clean == grantees_clean)]

rtt[, property_count := as.numeric(property_count)]
rtt[, bulk_transfer := !is.na(property_count) & property_count > 10]

rtt[, is_arms_length := !(low_consideration | self_transfer | bulk_transfer)]

# Log how many rows each filter would drop (applied sequentially for marginal counts)
n_low  <- rtt[low_consideration == TRUE, .N]
n_self <- rtt[low_consideration == FALSE & self_transfer == TRUE, .N]
n_bulk <- rtt[low_consideration == FALSE & self_transfer == FALSE & bulk_transfer == TRUE, .N]
n_overlap_any <- rtt[is_arms_length == FALSE, .N]

logf("  Arms-length filter breakdown (", n_before, " rows entering):", log_file = log_file)
logf("    Low consideration (<$100 or NA): ", n_low,
     " (", round(100 * n_low / n_before, 1), "%)", log_file = log_file)
logf("    Self-transfers (grantor == grantee, excl. above): ", n_self,
     " (", round(100 * n_self / n_before, 1), "%)", log_file = log_file)
logf("    Bulk transfers (property_count > 10, excl. above): ", n_bulk,
     " (", round(100 * n_bulk / n_before, 1), "%)", log_file = log_file)
logf("    Total dropped: ", n_overlap_any,
     " (", round(100 * n_overlap_any / n_before, 1), "%)",
     log_file = log_file)
logf("    Kept (arms-length): ", rtt[is_arms_length == TRUE, .N],
     " (", round(100 * rtt[is_arms_length == TRUE, .N] / n_before, 1), "%)",
     log_file = log_file)

# Also log breakdown by document type
filter_by_type <- rtt[, .(total = .N, dropped = sum(!is_arms_length),
                           kept = sum(is_arms_length),
                           pct_kept = round(100 * sum(is_arms_length) / .N, 1)),
                       by = document_type][order(-total)]
logf("  Arms-length filter by document type:", log_file = log_file)
for (i in seq_len(nrow(filter_by_type))) {
  logf("    ", filter_by_type$document_type[i], ": kept ",
       filter_by_type$kept[i], " / ", filter_by_type$total[i],
       " (", filter_by_type$pct_kept[i], "%), dropped ",
       filter_by_type$dropped[i], log_file = log_file)
}

# Keep only arms-length
rtt <- rtt[is_arms_length == TRUE]
logf("After arms-length filter: ", nrow(rtt), " rows (dropped ", n_before - nrow(rtt), ")",
     log_file = log_file)

# Drop temp filter columns
rtt[, c("low_consideration", "self_transfer", "bulk_transfer",
        "is_arms_length", "grantors_clean", "grantees_clean") := NULL]

# ============================================================
# STEP 5: Select columns & final checks
# ============================================================
logf("--- STEP 5: Final column selection and checks ---", log_file = log_file)

keep_cols <- c("PID", "display_date", "year", "year_quarter",
               "document_type", "document_id", "record_id",
               "total_consideration", "adjusted_total_consideration",
               "cash_consideration", "assessed_value", "fair_market_value",
               "grantors", "grantees",
               "street_address", "zip_code", "unit_num",
               "property_count", "reg_map_id", "matched_regmap")

# Keep only columns that exist
keep_cols <- intersect(keep_cols, names(rtt))
rtt <- rtt[, ..keep_cols]

# Uniqueness check on (PID, document_id)
n_dups <- rtt[, .N, by = .(PID, document_id)][N > 1L, .N]
if (n_dups > 0) {
  logf("WARNING: ", n_dups, " duplicate (PID, document_id) groups. Deduplicating (keeping first).",
       log_file = log_file)
  rtt <- unique(rtt, by = c("PID", "document_id"))
}

assert_unique(rtt, c("PID", "document_id"), "rtt_clean")
logf("Uniqueness check passed on (PID, document_id)", log_file = log_file)

# Log year distribution
year_dist <- rtt[, .N, by = year][order(year)]
logf("Year distribution (recent):", log_file = log_file)
recent <- year_dist[year >= 2000]
for (i in seq_len(nrow(recent))) {
  logf("  ", recent$year[i], ": ", recent$N[i], log_file = log_file)
}

logf("Final dataset: ", nrow(rtt), " rows, ", ncol(rtt), " cols, ",
     rtt[, uniqueN(PID)], " unique PIDs", log_file = log_file)

# ============================================================
# STEP 6: Export
# ============================================================
logf("--- STEP 6: Export ---", log_file = log_file)

output_path <- p_product(cfg, "rtt_clean")
fwrite(rtt, output_path)
logf("Wrote ", nrow(rtt), " rows to ", output_path, log_file = log_file)

logf("=== Finished process-rtt-data.R ===", log_file = log_file)
