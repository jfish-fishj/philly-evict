## ============================================================
## address-history-analysis.R
## ============================================================
## Purpose: Analyze tenant address histories and eviction persistence.
##          Tracks households across moves using InfoUSA panel data
##          to test whether tenants from high-eviction buildings
##          end up in other high-eviction buildings.
##
## Inputs:  infousa_cleaned, infousa_address_xwalk, bldg_panel_blp
## Outputs: tables/evict_persist.tex
## ============================================================

library(data.table)
library(fixest)
library(ggalluvial)
library(ggplot2)

# ============================================================
# EMPIRICAL BAYES FILING-RATE MEMO
# ============================================================
# Source of truth:
#   r/make-analytic-sample.R
#   Section "6b) FILINGS INTENSITY: RAW RATES + EMPIRICAL BAYES (POOLED ACROSS YEARS)"
#
# Model (pre-COVID pooled years):
#   y_it | lambda_i ~ Poisson(E_it * lambda_i)
#   lambda_i ~ Gamma(alpha, beta)
# where y_it = filings and E_it = occupied-unit exposure.
#
# For each building i, pooled sufficient stats over t <= 2019:
#   Y_i = sum_t y_it
#   E_i = sum_t E_it
# Posterior mean:
#   E[lambda_i | data] = (alpha + Y_i) / (beta + E_i)
#
# City and ZIP priors are estimated by method-of-moments; ZIP prior falls
# back to city prior when ZIP support is thin. In this script, EB is the
# default filing-intensity measure for main regressions, with raw long-run
# filing-rate results exported separately as appendix robustness.
# ============================================================

# ============================================================
# DATA CONSTRUCTION NOTES
# ============================================================
#
# RENT VARIABLES
# - med_rent: coalesce(Altos median listing rent, eviction ongoing_rent)
# - log_med_rent: log(med_rent), nominal
# - log_med_rent_real: hedonic-deflated log rent. Year FE extracted from
#     a weighted hedonic regression (log_med_rent ~ 1 | year +
#     building_type + census_tract, weights = total_units) are
#     subtracted from log_med_rent. Base year = earliest year in sample.
#     Removes aggregate rent inflation while preserving cross-sectional
#     and within-building variation.
# - bldg_rent_real / prev_bldg_rent_real: building-level average of
#     log_med_rent_real across all years (PID-level aggregate). Used in
#     persistence regressions since year-specific rent has high missingness.
#
# FILING RATE VARIABLES
# - filing_rate_raw: annual filings / occupied_units (volatile, year-varying)
# - filing_rate_longrun_pre2019: total_filings_pre2019 / (total_units *
#     num_years_pre2019). Raw long-run average, time-invariant building
#     characteristic. Noisier for buildings with short panels.
# - filing_rate_eb_pre_covid: Empirical Bayes shrinkage estimator
#     (Gamma-Poisson), pre-2019, city-wide prior. Time-invariant building
#     characteristic. Shrinks noisy short-panel rates toward the city mean.
#   Both long-run measures are building-level, not year-varying.
# ============================================================

# ---- Config & logging ----
source("r/config.R")
cfg <- read_config()
log_file <- p_out(cfg, "logs", "address-history-analysis.log")
tab_dir  <- p_out(cfg, "address-history-analysis", "tables")
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)
high_filing_threshold <- 0.15
logf("=== Starting address-history-analysis.R ===", log_file = log_file)
logf("High-filing cutoff (EB): >= ", high_filing_threshold, log_file = log_file)

# ---- Join safety helpers ----
assert_has_required_cols <- function(dt, cols, label) {
  miss <- setdiff(cols, names(dt))
  if (length(miss) > 0L) {
    stop("Missing required columns in ", label, ": ", paste(miss, collapse = ", "))
  }
}

assert_unique_keys <- function(dt, keys, label) {
  dup <- dt[, .N, by = keys][N > 1L]
  if (nrow(dup) > 0L) {
    stop(label, " is not unique on keys [", paste(keys, collapse = ", "), "]. Duplicate keys: ", nrow(dup))
  }
}

assert_rows_unchanged <- function(n_before, n_after, label) {
  if (!identical(n_before, n_after)) {
    stop(label, " changed row count unexpectedly: ", n_before, " -> ", n_after)
  }
}

normalize_pid <- function(x) {
  x <- as.character(x)
  x <- gsub("[^0-9]", "", x)
  x[nchar(x) == 0L] <- NA_character_
  stringr::str_pad(x, width = 9L, side = "left", pad = "0")
}

# ---- Load data via config ----
logf("Loading inputs...", log_file = log_file)

philly_infousa_dt <- fread(p_input(cfg, "infousa_cleaned"))
philly_infousa_dt[, obs_id := .I]
assert_has_required_cols(
  philly_infousa_dt,
  c("n_sn_ss_c", "familyid", "year", "owner_renter_status",
    "ge_census_state_2010", "ge_census_county", "ge_census_tract", "ge_census_bg"),
  "infousa_cleaned"
)
philly_infousa_dt[, year := suppressWarnings(as.integer(year))]
logf("  infousa_cleaned: ", format(nrow(philly_infousa_dt), big.mark = ","), " rows",
     log_file = log_file)

xwalk_path <- p_product(cfg, "xwalk_infousa_to_parcel")
info_usa_xwalk <- fread(
  xwalk_path,
  select = c("n_sn_ss_c", "parcel_number", "match_tier", "match_score",
             "xwalk_status", "link_type", "link_id", "anchor_pid", "ownership_unsafe")
)
assert_has_required_cols(
  info_usa_xwalk,
  c("n_sn_ss_c", "parcel_number", "match_tier", "xwalk_status", "link_type"),
  "xwalk_infousa_to_parcel"
)
info_usa_xwalk[, n_sn_ss_c := as.character(n_sn_ss_c)]
info_usa_xwalk[, PID := normalize_pid(parcel_number)]
info_usa_xwalk[, anchor_pid := normalize_pid(anchor_pid)]
info_usa_xwalk[, link_id := as.character(link_id)]
info_usa_xwalk <- info_usa_xwalk[!is.na(n_sn_ss_c) & nzchar(n_sn_ss_c)]
info_usa_xwalk[, match_tier_num := suppressWarnings(as.numeric(gsub("[^0-9.-]", "", as.character(match_tier))))]
info_usa_xwalk[, match_score_num := suppressWarnings(as.numeric(match_score))]
info_usa_xwalk[, match_tier_ord := fifelse(is.na(match_tier_num), 1e9, match_tier_num)]
info_usa_xwalk[, match_score_ord := fifelse(is.na(match_score_num), -Inf, match_score_num)]
setorder(info_usa_xwalk, n_sn_ss_c, match_tier_ord, -match_score_ord, PID)
info_usa_xwalk <- info_usa_xwalk[, .SD[1L], by = n_sn_ss_c]
assert_unique_keys(info_usa_xwalk, "n_sn_ss_c", "info_usa_xwalk winner table")
logf("  xwalk_infousa_to_parcel (winner rows): ", format(nrow(info_usa_xwalk), big.mark = ","),
     " rows from ", xwalk_path, log_file = log_file)
logf("  xwalk status distribution (winner rows):\n",
     paste(capture.output(print(info_usa_xwalk[, .N, by = xwalk_status][order(-N)])), collapse = "\n"),
     log_file = log_file)
logf("  link type distribution (winner rows):\n",
     paste(capture.output(print(info_usa_xwalk[, .N, by = link_type][order(-N)])), collapse = "\n"),
     log_file = log_file)
info_usa_xwalk[, c("parcel_number", "match_tier", "match_score", "match_tier_num", "match_score_num",
                   "match_tier_ord", "match_score_ord") := NULL]

## bldg_panel_blp has everything: filing rates, rents, occupancy, parcel chars,
## GEOID, unit bins, and rental flags. Replaces separate loads of bldg_panel,
## license_long_min, infousa_occupancy, and parcel_building_2024.
philly_rent_df <- fread(p_product(cfg, "bldg_panel_blp"))
assert_has_required_cols(
  philly_rent_df,
  c("PID", "year", "GEOID", "total_units", "num_units_bin",
    "filing_rate_raw", "num_filings", "log_med_rent", "med_rent",
    "filing_rate_eb_pre_covid", "filing_rate_longrun_pre2019",
    "building_type", "quality_grade", "year_blt_decade", "num_stories_bin",
    "total_area", "market_value", "total_severe_complaints",
    "total_violations", "total_severe_violations"),
  "bldg_panel_blp"
)
philly_rent_df[, PID := normalize_pid(PID)]
philly_rent_df[, year := suppressWarnings(as.integer(year))]
assert_unique_keys(
  philly_rent_df[!is.na(PID) & !is.na(year)],
  c("PID", "year"),
  "bldg_panel_blp PID-year"
)
logf("  bldg_panel_blp: ", format(nrow(philly_rent_df), big.mark = ","), " rows",
     log_file = log_file)

# ---- Hedonic price index: deflate nominal rents ----
logf("Estimating hedonic price index...", log_file = log_file)

hedonic_dt <- philly_rent_df[!is.na(log_med_rent)]
# Use census tract (not block group) to avoid over-absorption with other FE
hedonic_dt[, census_tract := substr(GEOID, 1, 11)]

m_hedonic <- feols(
  log_med_rent ~ 1 | year + building_type + census_tract,
  data = hedonic_dt,
  weights = ~total_units
)

# Extract year FE (relative to earliest year)
year_fe <- fixef(m_hedonic)$year
year_index <- data.table(year = as.integer(names(year_fe)), year_fe = as.numeric(year_fe))
base_year_fe <- year_index[year == min(year), year_fe]
year_index[, year_fe_adj := year_fe - base_year_fe]

logf("  Hedonic year FE range: ", round(min(year_index$year_fe_adj), 3),
     " to ", round(max(year_index$year_fe_adj), 3),
     " (base year = ", min(year_index$year), ")",
     log_file = log_file)

# Merge onto panel, create deflated rent
philly_rent_df <- merge(philly_rent_df, year_index[, .(year, year_fe_adj)],
                        by = "year", all.x = TRUE)
philly_rent_df[, log_med_rent_real := log_med_rent - year_fe_adj]

logf("  log_med_rent_real: N non-missing = ",
     philly_rent_df[!is.na(log_med_rent_real), .N],
     log_file = log_file)
rm(hedonic_dt)

# ---- Build eviction summary by PID ----
philly_evict_df <- philly_rent_df[, .(
  filing_rate = mean(filing_rate_raw, na.rm = TRUE),
  total_filings = sum(num_filings, na.rm = TRUE)
), by = PID]
logf("Eviction summary: ", format(nrow(philly_evict_df), big.mark = ","), " unique PIDs",
     log_file = log_file)

# ---- Merge InfoUSA with parcel crosswalk ----
n_pre <- nrow(philly_infousa_dt)
philly_infousa_dt_m <- merge(
  philly_infousa_dt,
  info_usa_xwalk[, .(n_sn_ss_c, PID, xwalk_status, link_type, link_id, anchor_pid, ownership_unsafe)],
  by = "n_sn_ss_c",
  all.x = TRUE
)
assert_rows_unchanged(n_pre, nrow(philly_infousa_dt_m), "Merge InfoUSA -> xwalk winner rows")
n_linked_pid <- philly_infousa_dt_m[!is.na(PID), .N]
logf("Merge InfoUSA -> xwalk winners: ", format(n_pre, big.mark = ","), " rows; PID-linked rows = ",
     format(n_linked_pid, big.mark = ","), " (", round(100 * n_linked_pid / n_pre, 1), "%)",
     log_file = log_file)
logf("PID-linked rows by xwalk status:\n",
     paste(capture.output(print(philly_infousa_dt_m[!is.na(PID), .N, by = xwalk_status][order(-N)])), collapse = "\n"),
     log_file = log_file)
logf("PID-linked rows by link type:\n",
     paste(capture.output(print(philly_infousa_dt_m[!is.na(PID), .N, by = link_type][order(-N)])), collapse = "\n"),
     log_file = log_file)

# Keep rows with resolved PID mappings for building-level move analysis.
philly_infousa_dt_m <- philly_infousa_dt_m[!is.na(PID)]
logf("Kept PID-linked InfoUSA rows for analysis: ",
     format(nrow(philly_infousa_dt_m), big.mark = ","),
     log_file = log_file)

# ---- Merge on building panel data (occupancy, parcel chars, rents, filings) ----
# Select only the columns we need from bldg_panel_blp to avoid column collisions
bldg_cols <- c("PID", "year", "total_units", "GEOID", "num_units_bin",
               "filing_rate_raw", "num_filings", "log_med_rent", "med_rent",
               "filing_rate_eb_pre_covid", "filing_rate_longrun_pre2019", "log_med_rent_real")
assert_has_required_cols(philly_rent_df, bldg_cols, "bldg_panel_blp columns for InfoUSA merge")
bldg_lookup <- philly_rent_df[, ..bldg_cols]
assert_unique_keys(
  bldg_lookup[!is.na(PID) & !is.na(year)],
  c("PID", "year"),
  "bldg_lookup PID-year"
)

n_pre <- nrow(philly_infousa_dt_m)
philly_infousa_dt_m <- merge(
  philly_infousa_dt_m,
  bldg_lookup,
  by = c("PID", "year"),
  all.x = TRUE
)
assert_rows_unchanged(n_pre, nrow(philly_infousa_dt_m), "Merge InfoUSA rows -> bldg_panel_blp")
logf("Merge -> bldg_panel_blp: ", format(n_pre, big.mark = ","), " -> ",
     format(nrow(philly_infousa_dt_m), big.mark = ","), " rows; PID-year matched = ",
     format(philly_infousa_dt_m[!is.na(total_units), .N], big.mark = ","),
     " (", round(100 * philly_infousa_dt_m[!is.na(total_units), .N] / pmax(1, nrow(philly_infousa_dt_m)), 1), "%)",
     log_file = log_file)

# Fill NA filings with 0
philly_infousa_dt_m[is.na(num_filings), num_filings := 0L]
philly_infousa_dt_m[is.na(filing_rate_raw), filing_rate_raw := 0]

# ---- Flag rentals ----
rental_pids <- unique(philly_rent_df[!is.na(PID), PID])
philly_infousa_dt_m[, rental := PID %chin% rental_pids]
n_rental <- philly_infousa_dt_m[rental == TRUE, .N]
logf("Rental flag (from bldg_panel_blp PID scope): ", format(n_rental, big.mark = ","), " / ",
     format(nrow(philly_infousa_dt_m), big.mark = ","), " rows (",
     round(100 * n_rental / nrow(philly_infousa_dt_m), 1), "%)",
     log_file = log_file)

# Owner-renter status vs rental flag
ors_tab <- philly_infousa_dt_m[, .(
  count = .N,
  sum_rental = sum(rental == TRUE)
), by = owner_renter_status]
ors_tab[, pct_rental := round(sum_rental / count, 2)]
setorder(ors_tab, owner_renter_status)
logf("Owner-renter status vs rental flag:\n",
     paste(capture.output(print(ors_tab)), collapse = "\n"),
     log_file = log_file)

# ---- Coalesce GEOID: parcel-linked (from bldg_panel_blp) > InfoUSA census fields ----
# GEOID comes from bldg_panel_blp merge (parcel data, reliable).
# For rows without bldg_panel match, construct BG GEOID from InfoUSA census columns.
# Pattern follows r/impute-race-infousa.R.
philly_infousa_dt_m[, bg_geoid_infousa := {
  st_raw <- suppressWarnings(as.integer(ge_census_state_2010))
  co_raw <- suppressWarnings(as.integer(ge_census_county))
  tr_raw <- suppressWarnings(as.integer(ge_census_tract))
  bg_raw <- suppressWarnings(as.integer(ge_census_bg))
  valid <- is.finite(st_raw) & st_raw > 0L &
           is.finite(co_raw) & co_raw > 0L &
           is.finite(tr_raw) &
           is.finite(bg_raw) & bg_raw >= 0L
  st <- formatC(st_raw, width = 2L, flag = "0")
  co <- formatC(co_raw, width = 3L, flag = "0")
  tr <- formatC(tr_raw, width = 6L, flag = "0")
  bg <- as.character(bg_raw)
  raw <- paste0(st, co, tr, bg)
  raw[!valid] <- NA_character_
  raw
}]

# Ensure GEOID is character for coalescing
philly_infousa_dt_m[, GEOID := as.character(GEOID)]
n_parcel_geo <- philly_infousa_dt_m[!is.na(GEOID) & GEOID != "NA", .N]
n_infousa_geo <- philly_infousa_dt_m[(is.na(GEOID) | GEOID == "NA") & !is.na(bg_geoid_infousa), .N]
philly_infousa_dt_m[GEOID == "NA", GEOID := NA_character_]
philly_infousa_dt_m[, GEOID := fcoalesce(GEOID, bg_geoid_infousa)]
n_total_geo <- philly_infousa_dt_m[!is.na(GEOID), .N]
logf("GEOID coalesce: parcel=", format(n_parcel_geo, big.mark = ","),
     ", InfoUSA fallback=", format(n_infousa_geo, big.mark = ","),
     ", total=", format(n_total_geo, big.mark = ","), " / ",
     format(nrow(philly_infousa_dt_m), big.mark = ","),
     log_file = log_file)
philly_infousa_dt_m[, bg_geoid_infousa := NULL]

# ---- Construct lagged address/eviction variables within household ----
setorder(philly_infousa_dt_m, familyid, year)
lag_cols <- c("n_sn_ss_c", "filing_rate_raw", "rental", "total_units",
              "GEOID", "pm.zip", "log_med_rent",
              "filing_rate_eb_pre_covid", "filing_rate_longrun_pre2019",
              "log_med_rent_real")
lag_names <- paste0("prev_", c("address", "evict", "rental", "total_units",
                               "geoid", "pm.zip", "log_med_rent",
                               "filing_rate_eb_pre_covid", "filing_rate_longrun_pre2019",
                               "log_med_rent_real"))
for (i in seq_along(lag_cols)) {
  philly_infousa_dt_m[, (lag_names[i]) := shift(get(lag_cols[i]), type = "lag"),
                      by = familyid]
}

# ---- Identify movers (rental-to-rental) ----
philly_infousa_dt_m[, move := n_sn_ss_c != prev_address]
movers <- philly_infousa_dt_m[
  move == TRUE &
    !is.na(prev_address) &
    rental == TRUE &
    !is.na(familyid) &
    prev_rental == TRUE
]
logf("Movers (rental-to-rental): ", format(nrow(movers), big.mark = ","), " rows",
     log_file = log_file)

# ============================================================
# TRACKING DESCRIPTIVES
# ============================================================

# ---- Step 1: Tracking statistics per household ----
logf("--- Tracking descriptives ---", log_file = log_file)

hh_tracking <- philly_infousa_dt_m[rental == TRUE, .(
  n_addresses = uniqueN(n_sn_ss_c),
  n_years     = uniqueN(year),
  min_year    = min(year),
  max_year    = max(year)
), by = familyid]
hh_tracking[, mover_status := fifelse(n_addresses >= 2, "mover", "stayer")]

logf("Tracking stats (rental HHs):", log_file = log_file)
logf("  N unique familyids: ", format(nrow(hh_tracking), big.mark = ","), log_file = log_file)
logf("  Distribution of addresses per HH:", log_file = log_file)
addr_dist <- hh_tracking[, .(N = .N), by = .(n_addr_grp = fifelse(n_addresses >= 4, "4+", as.character(n_addresses)))]
addr_dist[, pct := round(100 * N / sum(N), 1)]
setorder(addr_dist, n_addr_grp)
logf(paste(capture.output(print(addr_dist)), collapse = "\n"), log_file = log_file)
logf("  Median years observed: ", median(hh_tracking$n_years), log_file = log_file)
logf("  Mean years observed: ", round(mean(hh_tracking$n_years), 2), log_file = log_file)
logf("  % trackable 2+ years: ", round(100 * mean(hh_tracking$n_years >= 2), 1), "%",
     log_file = log_file)

# ---- Step 2: Load and merge race/gender data ----
logf("Loading race and gender imputation data...", log_file = log_file)

# Race imputation
race_dt <- fread(p_product(cfg, "infousa_race_imputed_person"))
logf("  infousa_race_imputed_person: ", format(nrow(race_dt), big.mark = ","), " rows",
     log_file = log_file)
race_hoh <- race_dt[person_num == 1 & !is.na(familyid), .(
  familyid, year, p_white, p_black, p_hispanic, p_asian, p_other,
  race_hat, race_impute_status
)]
rm(race_dt)
logf("  Head-of-household race records: ", format(nrow(race_hoh), big.mark = ","),
     log_file = log_file)

# Gender imputation (much more complete than raw gender_1 which is 91% missing)
gender_dt <- fread(p_product(cfg, "infousa_gender_imputed_person"))
logf("  infousa_gender_imputed_person: ", format(nrow(gender_dt), big.mark = ","), " rows",
     log_file = log_file)
gender_hoh <- gender_dt[person_num == 1 & !is.na(familyid), .(
  familyid, year, p_female, gender_hat
)]
rm(gender_dt)
logf("  Head-of-household gender records: ", format(nrow(gender_hoh), big.mark = ","),
     log_file = log_file)

# Merge race onto the merged infousa panel
philly_infousa_dt_m <- merge(
  philly_infousa_dt_m,
  race_hoh,
  by = c("familyid", "year"),
  all.x = TRUE
)
logf("  Race merge: ", round(100 * mean(!is.na(philly_infousa_dt_m$race_hat)), 1),
     "% of rows matched", log_file = log_file)

# Merge gender
philly_infousa_dt_m <- merge(
  philly_infousa_dt_m,
  gender_hoh,
  by = c("familyid", "year"),
  all.x = TRUE
)
logf("  Gender merge: ", round(100 * mean(!is.na(philly_infousa_dt_m$gender_hat)), 1),
     "% of rows matched", log_file = log_file)

# ---- Step 3: Descriptive comparison table — movers vs stayers vs all renters ----
logf("Building tracking comparison table...", log_file = log_file)

# Tag each row with mover_status
philly_infousa_dt_m <- merge(
  philly_infousa_dt_m,
  hh_tracking[, .(familyid, n_addresses, n_years, mover_status)],
  by = "familyid",
  all.x = TRUE
)

# Function to compute group summary
compute_group_stats <- function(dt, group_label) {
  hh_level <- dt[, .(n_addresses = n_addresses[1], n_years = n_years[1]), by = familyid]
  data.table(
    group           = group_label,
    n_hh_years      = nrow(dt),
    n_unique_hh     = uniqueN(dt$familyid),
    mean_addresses  = round(mean(hh_level$n_addresses, na.rm = TRUE), 2),
    mean_years_obs  = round(mean(hh_level$n_years, na.rm = TRUE), 2),
    pct_black       = round(100 * mean(dt$p_black, na.rm = TRUE), 1),
    pct_white       = round(100 * mean(dt$p_white, na.rm = TRUE), 1),
    pct_hispanic    = round(100 * mean(dt$p_hispanic, na.rm = TRUE), 1),
    pct_female_hoh  = round(100 * mean(dt$p_female, na.rm = TRUE), 1),
    mean_filing_rate = round(mean(dt$filing_rate_raw, na.rm = TRUE), 4),
    mean_total_units = round(mean(dt$total_units, na.rm = TRUE), 1),
    mean_log_rent   = round(mean(dt$log_med_rent, na.rm = TRUE), 3),
    n_unique_tracts = uniqueN(substr(dt$GEOID[!is.na(dt$GEOID)], 1, 11))
  )
}

rental_dt <- philly_infousa_dt_m[rental == TRUE]
comp_table <- rbindlist(list(
  compute_group_stats(rental_dt, "All Rental HHs"),
  compute_group_stats(rental_dt[mover_status == "stayer"], "Stayers (1 address)"),
  compute_group_stats(rental_dt[mover_status == "mover"], "Movers (2+ addresses)")
))

logf("Comparison table:\n",
     paste(capture.output(print(comp_table, nclass = Inf)), collapse = "\n"),
     log_file = log_file)

# ---- Step 4: Neighborhood race characterization for movers ----
logf("Neighborhood characterization for movers...", log_file = log_file)

# Load BG race priors
bg_priors_path <- file.path(
  cfg$paths$processed_dir, "xwalks", "bg_renter_race_priors_2013.csv"
)
bg_priors <- fread(bg_priors_path)
logf("  BG race priors: ", format(nrow(bg_priors), big.mark = ","), " BGs",
     log_file = log_file)

# movers already has geoid (current BG) and prev_geoid (previous BG)
# — but note: movers hasn't been clean_names'd yet at this point, use GEOID/prev_geoid
# Actually movers is defined above at line 128 but clean_names happens later (line 150).
# So we work on philly_infousa_dt_m directly for the mover subset before clean_names.

# Build mover-level neighborhood data from philly_infousa_dt_m
mover_nbhd <- philly_infousa_dt_m[
  !is.na(familyid) & rental == TRUE & mover_status == "mover" &
    !is.na(GEOID) & !is.na(prev_address)
]
# Keep only actual move rows
mover_nbhd <- mover_nbhd[n_sn_ss_c != prev_address & prev_rental == TRUE & !is.na(prev_geoid)]

# Coerce bg_geoid to character to match coalesced GEOID
bg_priors[, bg_geoid := as.character(bg_geoid)]

# Merge BG priors for destination (current GEOID)
mover_nbhd <- merge(
  mover_nbhd,
  bg_priors[, .(bg_geoid, dest_pct_black = p_black_prior)],
  by.x = "GEOID", by.y = "bg_geoid",
  all.x = TRUE
)
# Merge BG priors for origin (prev_geoid)
mover_nbhd <- merge(
  mover_nbhd,
  bg_priors[, .(bg_geoid, orig_pct_black = p_black_prior)],
  by.x = "prev_geoid", by.y = "bg_geoid",
  all.x = TRUE
)

nbhd_by_race <- mover_nbhd[
  !is.na(race_hat) & !is.na(orig_pct_black) & !is.na(dest_pct_black),
  .(
    n_moves          = .N,
    mean_orig_black  = round(mean(orig_pct_black), 3),
    mean_dest_black  = round(mean(dest_pct_black), 3),
    mean_change      = round(mean(dest_pct_black - orig_pct_black), 4)
  ),
  by = race_hat
]
setorder(nbhd_by_race, race_hat)
logf("Neighborhood pct_black at origin vs destination by mover race:\n",
     paste(capture.output(print(nbhd_by_race)), collapse = "\n"),
     log_file = log_file)

# ---- Step 4b: Building-level eviction data + LOO tract eviction rate ----
logf("--- Building eviction data and LOO tract eviction rate ---", log_file = log_file)

# Standardize quality grade to align with price-regs controls.
philly_rent_df[, quality_grade_standard := fifelse(
  !is.na(quality_grade) & grepl("[ABCDabcd]", quality_grade),
  gsub("[+-]", "", quality_grade),
  NA_character_
)]
philly_rent_df[is.na(quality_grade_standard), quality_grade_standard := "Unknown"]

mode_non_missing <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x) & x != "NA"]
  if (length(x) == 0L) return(NA_character_)
  names(sort(table(x), decreasing = TRUE))[1L]
}

safe_mean_positive <- function(x) {
  x <- as.numeric(x)
  x <- x[is.finite(x) & x > 0]
  if (length(x) == 0L) return(NA_real_)
  mean(x)
}

# Get PID-level EB eviction intensity and total filings (building-level, pre-COVID)
# n_years_pre2019 counts how many panel years each PID has before 2020,
# so we can annualize filings / unit-years for the LOO tract rate.
pid_evict <- philly_rent_df[, .(
  filing_rate_eb_pre_covid    = filing_rate_eb_pre_covid[1],
  filing_rate_longrun_pre2019 = filing_rate_longrun_pre2019[1],
  total_filings_pre2019       = total_filings_pre2019[1],
  severe_complaints_per_unit_longrun_pre2019 = {
    severe_pre <- sum(total_severe_complaints[year <= 2019], na.rm = TRUE)
    units_ref <- safe_mean_positive(total_units[year <= 2019])
    ifelse(is.na(units_ref) | units_ref <= 0, NA_real_, severe_pre / units_ref)
  },
  violations_per_unit_longrun_pre2019 = {
    viol_pre <- sum(total_violations[year <= 2019], na.rm = TRUE)
    units_ref <- safe_mean_positive(total_units[year <= 2019])
    ifelse(is.na(units_ref) | units_ref <= 0, NA_real_, viol_pre / units_ref)
  },
  severe_violations_per_unit_longrun_pre2019 = {
    severe_viol_pre <- sum(total_severe_violations[year <= 2019], na.rm = TRUE)
    units_ref <- safe_mean_positive(total_units[year <= 2019])
    ifelse(is.na(units_ref) | units_ref <= 0, NA_real_, severe_viol_pre / units_ref)
  },
  orig_total_area             = safe_mean_positive(total_area[year <= 2019]),
  orig_market_value           = safe_mean_positive(market_value[year <= 2019]),
  orig_year_blt_decade        = mode_non_missing(year_blt_decade[year <= 2019]),
  orig_num_units_bin          = mode_non_missing(num_units_bin[year <= 2019]),
  orig_building_type          = mode_non_missing(building_type[year <= 2019]),
  orig_quality_grade_standard = mode_non_missing(quality_grade_standard[year <= 2019]),
  orig_num_stories_bin        = mode_non_missing(num_stories_bin[year <= 2019]),
  total_units_mean            = mean(total_units, na.rm = TRUE),
  n_years_pre2019             = sum(year <= 2019, na.rm = TRUE),
  GEOID_bldg                  = GEOID[which.max(!is.na(GEOID))]
), by = PID]
pid_evict[, census_tract := substr(GEOID_bldg, 1, 11)]
# Unit-years: annualize the denominator so rate is filings per unit per year
pid_evict[, unit_years_pre2019 := total_units_mean * pmax(n_years_pre2019, 1L)]

# Helper: EB filing-rate bins (percent scale in decimals)
make_evict_bin <- function(rate) {
  fcase(
    !is.finite(rate) | rate < 0, NA_character_,
    rate < 0.01, "[0,1)",
    rate < 0.05, "[1,5)",
    rate < 0.10, "[5,10)",
    rate < 0.20, "[10,20)",
    rate >= 0.20, "[20,inf)",
    default = NA_character_
  )
}
evict_bin_levels <- c("[0,1)", "[1,5)", "[5,10)", "[10,20)", "[20,inf)")

# Build LOO tract filing rates using unit-year weighted building rates:
# - Raw long-run filing rate
# - EB filing rate
tract_totals <- pid_evict[
  !is.na(census_tract) & !is.na(unit_years_pre2019),
  .(
    tract_total_unit_years = sum(unit_years_pre2019, na.rm = TRUE),
    tract_total_raw_weighted = sum(filing_rate_longrun_pre2019 * unit_years_pre2019, na.rm = TRUE),
    tract_total_eb_weighted = sum(filing_rate_eb_pre_covid * unit_years_pre2019, na.rm = TRUE),
    tract_n_bldgs          = .N
  ),
  by = census_tract
]

pid_evict <- merge(pid_evict, tract_totals, by = "census_tract", all.x = TRUE)

# Leave-one-out: subtract this building's weighted contribution
pid_evict[, loo_tract_unit_years := tract_total_unit_years - fifelse(is.na(unit_years_pre2019), 0, unit_years_pre2019)]
pid_evict[, loo_tract_raw_weighted := tract_total_raw_weighted - fifelse(
  is.na(filing_rate_longrun_pre2019) | is.na(unit_years_pre2019),
  0,
  filing_rate_longrun_pre2019 * unit_years_pre2019
)]
pid_evict[, loo_tract_eb_weighted := tract_total_eb_weighted - fifelse(
  is.na(filing_rate_eb_pre_covid) | is.na(unit_years_pre2019),
  0,
  filing_rate_eb_pre_covid * unit_years_pre2019
)]
pid_evict[, loo_tract_evict_rate_raw := fifelse(
  loo_tract_unit_years > 0,
  loo_tract_raw_weighted / loo_tract_unit_years,
  NA_real_
)]
pid_evict[, loo_tract_evict_rate_eb := fifelse(
  loo_tract_unit_years > 0,
  loo_tract_eb_weighted / loo_tract_unit_years,
  NA_real_
)]

logf("  LOO tract filing rates: raw mean=", round(mean(pid_evict$loo_tract_evict_rate_raw, na.rm = TRUE), 4),
     ", raw median=", round(median(pid_evict$loo_tract_evict_rate_raw, na.rm = TRUE), 4),
     ", raw N=", pid_evict[!is.na(loo_tract_evict_rate_raw), .N],
     "; EB mean=", round(mean(pid_evict$loo_tract_evict_rate_eb, na.rm = TRUE), 4),
     ", EB median=", round(median(pid_evict$loo_tract_evict_rate_eb, na.rm = TRUE), 4),
     ", EB N=", pid_evict[!is.na(loo_tract_evict_rate_eb), .N],
     log_file = log_file)

# ---- Step 4c: Lag PIDs on full panel for origin/destination tracking ----
setorder(philly_infousa_dt_m, familyid, year)
philly_infousa_dt_m[, prev_PID := shift(PID, type = "lag"), by = familyid]

# ---- Step 4d: Inflow analysis — where do tenants at high-evict buildings come from? ----
logf("--- Inflow analysis: origin characteristics by destination eviction intensity ---",
     log_file = log_file)

# Merge destination eviction data onto mover_nbhd
mover_nbhd <- merge(
  mover_nbhd,
  pid_evict[, .(PID, dest_evict_eb = filing_rate_eb_pre_covid,
                dest_filings_pre2019 = total_filings_pre2019,
                dest_loo_tract_evict = loo_tract_evict_rate_eb)],
  by = "PID",
  all.x = TRUE
)

# Get prev_PID from full panel
mover_nbhd[, prev_PID := philly_infousa_dt_m[
  match(paste(mover_nbhd$familyid, mover_nbhd$year),
        paste(philly_infousa_dt_m$familyid, philly_infousa_dt_m$year)),
  prev_PID
]]

# Merge origin eviction data
mover_nbhd <- merge(
  mover_nbhd,
  pid_evict[, .(PID, orig_evict_eb = filing_rate_eb_pre_covid,
                orig_filings_pre2019 = total_filings_pre2019,
                orig_loo_tract_evict = loo_tract_evict_rate_eb)],
  by.x = "prev_PID", by.y = "PID",
  all.x = TRUE
)

# Bin eviction intensity for origin and destination
mover_nbhd[, dest_evict_bin := factor(
  make_evict_bin(dest_evict_eb),
  levels = evict_bin_levels
)]
mover_nbhd[, orig_evict_bin := factor(
  make_evict_bin(orig_evict_eb),
  levels = evict_bin_levels
)]

logf("  Movers with dest eviction data: ",
     mover_nbhd[!is.na(dest_evict_bin), .N], " / ", nrow(mover_nbhd),
     log_file = log_file)

# Summarize origin characteristics by destination eviction bin
inflow_summary <- mover_nbhd[
  !is.na(dest_evict_bin) & !is.na(orig_pct_black),
  .(
    n_moves              = .N,
    mean_orig_pct_black  = round(mean(orig_pct_black, na.rm = TRUE), 3),
    mean_dest_pct_black  = round(mean(dest_pct_black, na.rm = TRUE), 3),
    mean_orig_evict_eb   = round(mean(orig_evict_eb, na.rm = TRUE), 4),
    mean_orig_loo_tract  = round(mean(orig_loo_tract_evict, na.rm = TRUE), 4),
    mean_dest_loo_tract  = round(mean(dest_loo_tract_evict, na.rm = TRUE), 4),
    mover_pct_black      = round(mean(p_black, na.rm = TRUE), 3),
    mover_pct_female     = round(mean(p_female, na.rm = TRUE), 3),
    mean_orig_rent       = round(mean(prev_log_med_rent, na.rm = TRUE), 3),
    mean_dest_rent       = round(mean(log_med_rent, na.rm = TRUE), 3)
  ),
  by = dest_evict_bin
][order(dest_evict_bin)]

logf("Inflow summary by destination eviction intensity:\n",
     paste(capture.output(print(inflow_summary)), collapse = "\n"),
     log_file = log_file)

fwrite(inflow_summary, file.path(tab_dir, "inflow_by_dest_eviction_bin.csv"))

# ---- Step 4e: Outflow analysis — where do tenants FROM high-evict buildings go? ----
logf("--- Outflow analysis: destination characteristics by origin eviction intensity ---",
     log_file = log_file)

outflow_summary <- mover_nbhd[
  !is.na(orig_evict_bin) & !is.na(dest_pct_black),
  .(
    n_moves              = .N,
    mean_orig_pct_black  = round(mean(orig_pct_black, na.rm = TRUE), 3),
    mean_dest_pct_black  = round(mean(dest_pct_black, na.rm = TRUE), 3),
    mean_dest_evict_eb   = round(mean(dest_evict_eb, na.rm = TRUE), 4),
    mean_orig_loo_tract  = round(mean(orig_loo_tract_evict, na.rm = TRUE), 4),
    mean_dest_loo_tract  = round(mean(dest_loo_tract_evict, na.rm = TRUE), 4),
    delta_loo_tract      = round(mean(dest_loo_tract_evict - orig_loo_tract_evict, na.rm = TRUE), 4),
    mover_pct_black      = round(mean(p_black, na.rm = TRUE), 3),
    mover_pct_female     = round(mean(p_female, na.rm = TRUE), 3),
    mean_orig_rent       = round(mean(prev_log_med_rent, na.rm = TRUE), 3),
    mean_dest_rent       = round(mean(log_med_rent, na.rm = TRUE), 3)
  ),
  by = orig_evict_bin
][order(orig_evict_bin)]

logf("Outflow summary by origin eviction intensity:\n",
     paste(capture.output(print(outflow_summary)), collapse = "\n"),
     log_file = log_file)

fwrite(outflow_summary, file.path(tab_dir, "outflow_by_orig_eviction_bin.csv"))

# ---- Step 4f: 3-move trajectory analysis ----
# Track households across: origin → middle (high/low evict) → destination
# Question: do people who pass through high-evicting buildings end up in
# higher-eviction neighborhoods than where they started?
logf("--- 3-move trajectory analysis ---", log_file = log_file)

# Build a spell-level panel: each row is a (familyid, PID, spell_start, spell_end)
# Then identify sequences of 3 consecutive spells (origin → middle → dest)
setorder(philly_infousa_dt_m, familyid, year)

# Create spell ID: changes when PID changes within familyid
philly_infousa_dt_m[, pid_changed := PID != shift(PID, type = "lag"), by = familyid]
philly_infousa_dt_m[is.na(pid_changed), pid_changed := TRUE]  # first obs
philly_infousa_dt_m[, spell_id := cumsum(pid_changed), by = familyid]

# Collapse to spell level
spells <- philly_infousa_dt_m[
  rental == TRUE & !is.na(PID),
  .(
    spell_start = min(year),
    spell_end   = max(year),
    n_years     = .N,
    GEOID       = GEOID[which.max(!is.na(GEOID))]
  ),
  by = .(familyid, spell_id, PID)
]
setorder(spells, familyid, spell_start)

# Merge eviction data onto spells
spells <- merge(
  spells,
  pid_evict[, .(PID, evict_eb = filing_rate_eb_pre_covid,
                evict_raw = filing_rate_longrun_pre2019,
                severe_pu = severe_complaints_per_unit_longrun_pre2019,
                violations_pu = violations_per_unit_longrun_pre2019,
                severe_violations_pu = severe_violations_per_unit_longrun_pre2019,
                filings_pre2019 = total_filings_pre2019,
                loo_tract_evict_raw = loo_tract_evict_rate_raw,
                loo_tract_evict_eb = loo_tract_evict_rate_eb,
                total_area_pid = orig_total_area,
                market_value_pid = orig_market_value,
                year_blt_decade_pid = orig_year_blt_decade,
                num_units_bin_pid = orig_num_units_bin,
                building_type_pid = orig_building_type,
                quality_grade_standard_pid = orig_quality_grade_standard,
                num_stories_bin_pid = orig_num_stories_bin)],
  by = "PID",
  all.x = TRUE
)

# Number spells sequentially within household
spells[, spell_seq := seq_len(.N), by = familyid]
spells[, n_spells := .N, by = familyid]

# Bin the middle spell
spells[, evict_bin := factor(
  make_evict_bin(evict_eb),
  levels = evict_bin_levels
)]

logf("  Total spells: ", nrow(spells),
     "; HHs with 3+ spells: ", spells[n_spells >= 3, uniqueN(familyid)],
     log_file = log_file)

# Build 3-spell sequences: origin (spell s), middle (spell s+1), destination (spell s+2)
# Self-join: for each middle spell, get the previous and next
middle <- spells[
  spell_seq >= 2 & spell_seq <= n_spells - 1,
  .(
    familyid, spell_seq,
    mid_PID = PID, mid_GEOID = GEOID, mid_evict_eb = evict_eb, mid_evict_raw = evict_raw,
    mid_severe_pu = severe_pu, mid_violations_pu = violations_pu, mid_severe_violations_pu = severe_violations_pu,
    mid_filings_pre2019 = filings_pre2019, mid_loo_tract_raw = loo_tract_evict_raw, mid_loo_tract_eb = loo_tract_evict_eb,
    mid_evict_bin = evict_bin,
    mid_start = spell_start, mid_end = spell_end, mid_years = n_years
  )
]

origin <- spells[, .(
  familyid, spell_seq,
  orig_PID = PID, orig_GEOID = GEOID, orig_evict_eb = evict_eb, orig_evict_raw = evict_raw,
  orig_severe_pu = severe_pu, orig_violations_pu = violations_pu, orig_severe_violations_pu = severe_violations_pu,
  orig_filings = filings_pre2019, orig_loo_tract_raw = loo_tract_evict_raw, orig_loo_tract_eb = loo_tract_evict_eb,
  orig_total_area = total_area_pid, orig_market_value = market_value_pid,
  orig_year_blt_decade = year_blt_decade_pid, orig_num_units_bin = num_units_bin_pid,
  orig_building_type = building_type_pid, orig_quality_grade_standard = quality_grade_standard_pid,
  orig_num_stories_bin = num_stories_bin_pid,
  orig_start = spell_start, orig_end = spell_end
)]

dest <- spells[, .(
  familyid, spell_seq,
  dest_PID = PID, dest_GEOID = GEOID, dest_evict_eb = evict_eb, dest_evict_raw = evict_raw,
  dest_severe_pu = severe_pu, dest_violations_pu = violations_pu, dest_severe_violations_pu = severe_violations_pu,
  dest_filings = filings_pre2019, dest_loo_tract_raw = loo_tract_evict_raw, dest_loo_tract_eb = loo_tract_evict_eb,
  dest_start = spell_start, dest_end = spell_end
)]

# Join: middle spell s → origin spell s-1, dest spell s+1
trajectories <- merge(
  middle[, .(familyid, spell_seq, mid_PID, mid_GEOID, mid_evict_eb, mid_evict_raw,
             mid_severe_pu, mid_violations_pu, mid_severe_violations_pu,
             mid_filings_pre2019, mid_loo_tract_raw, mid_loo_tract_eb, mid_evict_bin,
             mid_start, mid_end, mid_years)],
  origin[, .(familyid, spell_seq_mid = spell_seq + 1L,
             orig_PID, orig_GEOID, orig_evict_eb, orig_evict_raw,
             orig_severe_pu, orig_violations_pu, orig_severe_violations_pu, orig_filings, orig_loo_tract_raw, orig_loo_tract_eb,
             orig_total_area, orig_market_value, orig_year_blt_decade,
             orig_num_units_bin, orig_building_type, orig_quality_grade_standard,
             orig_num_stories_bin,
             orig_start, orig_end)],
  by.x = c("familyid", "spell_seq"),
  by.y = c("familyid", "spell_seq_mid")
)

trajectories <- merge(
  trajectories,
  dest[, .(familyid, spell_seq_mid = spell_seq - 1L,
           dest_PID, dest_GEOID, dest_evict_eb, dest_evict_raw,
           dest_severe_pu, dest_violations_pu, dest_severe_violations_pu, dest_filings, dest_loo_tract_raw, dest_loo_tract_eb,
           dest_start, dest_end)],
  by.x = c("familyid", "spell_seq"),
  by.y = c("familyid", "spell_seq_mid")
)

logf("  3-spell trajectories: ", nrow(trajectories), " sequences from ",
     trajectories[, uniqueN(familyid)], " households", log_file = log_file)

# Define high-evict middle spell (>= 15% EB filing rate)
trajectories[, mid_high_evict := is.finite(mid_evict_eb) & mid_evict_eb >= high_filing_threshold]

# Key outcomes: building-level deltas (primary) and LOO tract delta (secondary)
trajectories[, delta_evict_eb  := dest_evict_eb  - orig_evict_eb]
trajectories[, delta_evict_raw := dest_evict_raw - orig_evict_raw]
trajectories[, delta_severe_pu := dest_severe_pu - orig_severe_pu]
trajectories[, delta_violations_pu := dest_violations_pu - orig_violations_pu]
trajectories[, delta_severe_violations_pu := dest_severe_violations_pu - orig_severe_violations_pu]
trajectories[, delta_loo_tract_raw := dest_loo_tract_raw  - orig_loo_tract_raw]
trajectories[, delta_loo_tract_eb := dest_loo_tract_eb  - orig_loo_tract_eb]

# Also bin origin eviction intensity for stratification
trajectories[, orig_evict_bin := factor(
  make_evict_bin(orig_evict_eb),
  levels = evict_bin_levels
)]

# Summary: trajectory outcomes by whether middle building is high-evict
traj_summary <- trajectories[
  !is.na(mid_high_evict) & !is.na(orig_evict_eb) & !is.na(dest_evict_eb),
  .(
    n_trajectories       = .N,
    n_households         = uniqueN(familyid),
    mean_mid_evict_eb    = round(mean(mid_evict_eb, na.rm = TRUE), 4),
    mean_orig_evict_eb   = round(mean(orig_evict_eb, na.rm = TRUE), 4),
    mean_dest_evict_eb   = round(mean(dest_evict_eb, na.rm = TRUE), 4),
    mean_delta_evict_eb  = round(mean(delta_evict_eb, na.rm = TRUE), 4),
    mean_orig_severe_pu  = round(mean(orig_severe_pu, na.rm = TRUE), 4),
    mean_dest_severe_pu  = round(mean(dest_severe_pu, na.rm = TRUE), 4),
    mean_delta_severe_pu = round(mean(delta_severe_pu, na.rm = TRUE), 4),
    mean_orig_violations_pu = round(mean(orig_violations_pu, na.rm = TRUE), 4),
    mean_dest_violations_pu = round(mean(dest_violations_pu, na.rm = TRUE), 4),
    mean_delta_violations_pu = round(mean(delta_violations_pu, na.rm = TRUE), 4),
    mean_orig_severe_violations_pu = round(mean(orig_severe_violations_pu, na.rm = TRUE), 4),
    mean_dest_severe_violations_pu = round(mean(dest_severe_violations_pu, na.rm = TRUE), 4),
    mean_delta_severe_violations_pu = round(mean(delta_severe_violations_pu, na.rm = TRUE), 4),
    mean_orig_loo_tract_eb  = round(mean(orig_loo_tract_eb, na.rm = TRUE), 4),
    mean_dest_loo_tract_eb  = round(mean(dest_loo_tract_eb, na.rm = TRUE), 4),
    mean_delta_loo_tract_eb = round(mean(delta_loo_tract_eb, na.rm = TRUE), 4),
    mean_orig_loo_tract_raw  = round(mean(orig_loo_tract_raw, na.rm = TRUE), 4),
    mean_dest_loo_tract_raw  = round(mean(dest_loo_tract_raw, na.rm = TRUE), 4),
    mean_delta_loo_tract_raw = round(mean(delta_loo_tract_raw, na.rm = TRUE), 4)
  ),
  by = mid_high_evict
]

logf("3-move trajectory summary (high vs non-high middle building):\n",
     paste(capture.output(print(traj_summary)), collapse = "\n"),
     log_file = log_file)

# Finer: by middle building eviction bin
traj_by_mid_bin <- trajectories[
  !is.na(mid_evict_bin) & !is.na(orig_evict_eb) & !is.na(dest_evict_eb),
  .(
    n_traj               = .N,
    mean_orig_evict_eb   = round(mean(orig_evict_eb, na.rm = TRUE), 4),
    mean_dest_evict_eb   = round(mean(dest_evict_eb, na.rm = TRUE), 4),
    mean_delta_evict_eb  = round(mean(delta_evict_eb, na.rm = TRUE), 4),
    mean_orig_evict_raw  = round(mean(orig_evict_raw, na.rm = TRUE), 4),
    mean_dest_evict_raw  = round(mean(dest_evict_raw, na.rm = TRUE), 4),
    mean_delta_evict_raw = round(mean(delta_evict_raw, na.rm = TRUE), 4),
    mean_orig_severe_pu  = round(mean(orig_severe_pu, na.rm = TRUE), 4),
    mean_dest_severe_pu  = round(mean(dest_severe_pu, na.rm = TRUE), 4),
    mean_delta_severe_pu = round(mean(delta_severe_pu, na.rm = TRUE), 4),
    mean_orig_violations_pu = round(mean(orig_violations_pu, na.rm = TRUE), 4),
    mean_dest_violations_pu = round(mean(dest_violations_pu, na.rm = TRUE), 4),
    mean_delta_violations_pu = round(mean(delta_violations_pu, na.rm = TRUE), 4),
    mean_orig_severe_violations_pu = round(mean(orig_severe_violations_pu, na.rm = TRUE), 4),
    mean_dest_severe_violations_pu = round(mean(dest_severe_violations_pu, na.rm = TRUE), 4),
    mean_delta_severe_violations_pu = round(mean(delta_severe_violations_pu, na.rm = TRUE), 4),
    mean_orig_loo_tract_eb  = round(mean(orig_loo_tract_eb, na.rm = TRUE), 4),
    mean_dest_loo_tract_eb  = round(mean(dest_loo_tract_eb, na.rm = TRUE), 4),
    mean_delta_loo_tract_eb = round(mean(delta_loo_tract_eb, na.rm = TRUE), 4),
    mean_orig_loo_tract_raw  = round(mean(orig_loo_tract_raw, na.rm = TRUE), 4),
    mean_dest_loo_tract_raw  = round(mean(dest_loo_tract_raw, na.rm = TRUE), 4),
    mean_delta_loo_tract_raw = round(mean(delta_loo_tract_raw, na.rm = TRUE), 4)
  ),
  by = mid_evict_bin
][order(mid_evict_bin)]

logf("3-move trajectory by middle building eviction bin:\n",
     paste(capture.output(print(traj_by_mid_bin)), collapse = "\n"),
     log_file = log_file)

fwrite(traj_by_mid_bin, file.path(tab_dir, "trajectory_by_mid_eviction_bin.csv"))

# ---- Trajectory regressions: building-level (primary) + LOO (secondary) ----
# Outcomes: EB filing rate, raw filing rate, severe complaints per unit, and LOO tract rate.
# We report delta and destination-level models separately.

traj_reg_dt <- trajectories[
  !is.na(mid_evict_bin) & !is.na(orig_GEOID) & nzchar(orig_GEOID)
]
traj_reg_dt[, orig_tract := substr(orig_GEOID, 1, 11)]
traj_reg_dt[, orig_filing_bin := factor(
  make_evict_bin(orig_evict_eb),
  levels = evict_bin_levels
)]
traj_reg_dt[, orig_log_total_area := fifelse(orig_total_area > 0, log(orig_total_area), NA_real_)]
traj_reg_dt[, orig_log_market_value := fifelse(orig_market_value > 0, log(orig_market_value), NA_real_)]

# Origin parcel-characteristic controls (aligned with price-regs-style variables).
origin_ctrl_fe_vars <- c(
  "orig_year_blt_decade",
  "orig_num_units_bin",
  "orig_building_type",
  "orig_quality_grade_standard",
  "orig_num_stories_bin"
)
for (cc in origin_ctrl_fe_vars) {
  traj_reg_dt[, (cc) := as.character(get(cc))]
  traj_reg_dt[is.na(get(cc)) | !nzchar(get(cc)), (cc) := "Unknown"]
}
traj_reg_dt[, mid_high_evict_dummy := as.integer(mid_high_evict)]

run_traj_specs <- function(dt, delta_var, level_var, orig_var, label, regressor = c("bins", "high_dummy")) {
  regressor <- match.arg(regressor)

  if (regressor == "bins") {
    dt_sub <- dt[!is.na(get(delta_var)) & !is.na(get(level_var)) & !is.na(mid_evict_bin)]
  } else {
    dt_sub <- dt[!is.na(get(delta_var)) & !is.na(get(level_var)) & !is.na(mid_high_evict_dummy)]
  }
  if (nrow(dt_sub) < 100) return(NULL)

  rhs_mid <- if (regressor == "bins") "i(mid_evict_bin, ref = '[0,1)')" else "mid_high_evict_dummy"
  rhs_label <- if (regressor == "bins") "mid_evict_bin" else "mid_high_evict_dummy"
  rhs_mid_orig <- paste0(rhs_mid, " + ", orig_var)
  rhs_mid_orig_ctrl <- paste(rhs_mid_orig, "orig_log_total_area + orig_log_market_value", sep = " + ")
  rhs_mid_ctrl_no_orig <- paste(rhs_mid, "orig_log_total_area + orig_log_market_value", sep = " + ")
  fe_ctrl <- paste(
    c("orig_tract", "orig_year_blt_decade", "orig_num_units_bin", "orig_building_type",
      "orig_quality_grade_standard", "orig_num_stories_bin"),
    collapse = " + "
  )
  fe_ctrl_int <- paste(
    c("orig_tract^orig_filing_bin", "orig_year_blt_decade", "orig_num_units_bin", "orig_building_type",
      "orig_quality_grade_standard", "orig_num_stories_bin"),
    collapse = " + "
  )

  # Delta outcomes
  m_delta_a <- feols(
    as.formula(paste0(delta_var, " ~ ", rhs_mid)),
    data = dt_sub,
    cluster = ~ orig_GEOID
  )
  m_delta_b <- feols(
    as.formula(paste0(delta_var, " ~ ", rhs_mid, " | orig_tract")),
    data = dt_sub,
    cluster = ~ orig_GEOID
  )

  dt_sub_orig <- dt_sub[!is.na(get(orig_var))]
  m_delta_c <- feols(
    as.formula(paste0(delta_var, " ~ ", rhs_mid_orig_ctrl, " | ", fe_ctrl)),
    data = dt_sub_orig,
    cluster = ~ orig_GEOID
  )
  dt_sub_orig_bin <- dt_sub_orig[!is.na(orig_filing_bin)]
  m_delta_d <- feols(
    as.formula(paste0(delta_var, " ~ ", rhs_mid_ctrl_no_orig, " | ", fe_ctrl_int)),
    data = dt_sub_orig_bin,
    cluster = ~ orig_GEOID
  )

  # Destination-level outcomes
  m_dest_a <- feols(
    as.formula(paste0(level_var, " ~ ", rhs_mid, " | orig_tract")),
    data = dt_sub,
    cluster = ~ orig_GEOID
  )
  m_dest_b <- feols(
    as.formula(paste0(level_var, " ~ ", rhs_mid_orig, " | orig_tract")),
    data = dt_sub_orig,
    cluster = ~ orig_GEOID
  )
  m_dest_c <- feols(
    as.formula(paste0(level_var, " ~ ", rhs_mid_orig_ctrl, " | ", fe_ctrl)),
    data = dt_sub_orig,
    cluster = ~ orig_GEOID
  )
  m_dest_d <- feols(
    as.formula(paste0(level_var, " ~ ", rhs_mid_ctrl_no_orig, " | ", fe_ctrl_int)),
    data = dt_sub_orig_bin,
    cluster = ~ orig_GEOID
  )

  logf("  Trajectory regressions (", label, "):", log_file = log_file)
  logf("    Delta (A): ", delta_var, " ~ ", rhs_label, log_file = log_file)
  logf(paste(capture.output(summary(m_delta_a)), collapse = "\n"), log_file = log_file)
  logf("    Delta (B): ", delta_var, " ~ ", rhs_label, " | orig_tract", log_file = log_file)
  logf(paste(capture.output(summary(m_delta_b)), collapse = "\n"), log_file = log_file)
  logf("    Delta (C): + ", orig_var, " + origin parcel controls", log_file = log_file)
  logf(paste(capture.output(summary(m_delta_c)), collapse = "\n"), log_file = log_file)
  logf("    Delta (D): + origin parcel controls + orig_tract^orig_filing_bin FE (no origin outcome control)", log_file = log_file)
  logf(paste(capture.output(summary(m_delta_d)), collapse = "\n"), log_file = log_file)
  logf("    Dest (A): ", level_var, " ~ ", rhs_label, " | orig_tract", log_file = log_file)
  logf(paste(capture.output(summary(m_dest_a)), collapse = "\n"), log_file = log_file)
  logf("    Dest (B): + ", orig_var, log_file = log_file)
  logf(paste(capture.output(summary(m_dest_b)), collapse = "\n"), log_file = log_file)
  logf("    Dest (C): + origin parcel controls", log_file = log_file)
  logf(paste(capture.output(summary(m_dest_c)), collapse = "\n"), log_file = log_file)
  logf("    Dest (D): + origin parcel controls + orig_tract^orig_filing_bin FE (no origin outcome control)", log_file = log_file)
  logf(paste(capture.output(summary(m_dest_d)), collapse = "\n"), log_file = log_file)

  list(
    delta = list(m_a = m_delta_a, m_b = m_delta_b, m_c = m_delta_c, m_d = m_delta_d),
    dest = list(m_a = m_dest_a, m_b = m_dest_b, m_c = m_dest_c, m_d = m_dest_d)
  )
}

logf("Running trajectory regressions...", log_file = log_file)

# Primary filing outcomes: EB (main) and raw (appendix), bins + high-filing dummy
traj_eb_bins <- run_traj_specs(
  traj_reg_dt,
  "delta_evict_eb",
  "dest_evict_eb",
  "orig_evict_eb",
  "EB filing rate (bins)",
  regressor = "bins"
)
traj_eb_high <- run_traj_specs(
  traj_reg_dt,
  "delta_evict_eb",
  "dest_evict_eb",
  "orig_evict_eb",
  "EB filing rate (high-filing dummy)",
  regressor = "high_dummy"
)
traj_raw_bins <- run_traj_specs(
  traj_reg_dt,
  "delta_evict_raw",
  "dest_evict_raw",
  "orig_evict_raw",
  "Raw filing rate (bins)",
  regressor = "bins"
)
traj_raw_high <- run_traj_specs(
  traj_reg_dt,
  "delta_evict_raw",
  "dest_evict_raw",
  "orig_evict_raw",
  "Raw filing rate (high-filing dummy)",
  regressor = "high_dummy"
)

# Additional outcome: severe complaints per unit (long-run pre-2019)
traj_severe <- run_traj_specs(
  traj_reg_dt,
  "delta_severe_pu",
  "dest_severe_pu",
  "orig_severe_pu",
  "Severe complaints per unit",
  regressor = "bins"
)

# Additional outcomes: violations per unit
traj_viol <- run_traj_specs(
  traj_reg_dt,
  "delta_violations_pu",
  "dest_violations_pu",
  "orig_violations_pu",
  "Total violations per unit",
  regressor = "bins"
)
traj_severe_viol <- run_traj_specs(
  traj_reg_dt,
  "delta_severe_violations_pu",
  "dest_severe_violations_pu",
  "orig_severe_violations_pu",
  "Severe violations per unit",
  regressor = "bins"
)

# Secondary neighborhood outcomes: LOO EB (main) and LOO raw (appendix)
traj_loo_eb_bins <- run_traj_specs(
  traj_reg_dt,
  "delta_loo_tract_eb",
  "dest_loo_tract_eb",
  "orig_loo_tract_eb",
  "LOO tract filing rate (EB bins)",
  regressor = "bins"
)
traj_loo_eb_high <- run_traj_specs(
  traj_reg_dt,
  "delta_loo_tract_eb",
  "dest_loo_tract_eb",
  "orig_loo_tract_eb",
  "LOO tract filing rate (EB high-filing dummy)",
  regressor = "high_dummy"
)
traj_loo_raw_bins <- run_traj_specs(
  traj_reg_dt,
  "delta_loo_tract_raw",
  "dest_loo_tract_raw",
  "orig_loo_tract_raw",
  "LOO tract filing rate (raw bins)",
  regressor = "bins"
)
traj_loo_raw_high <- run_traj_specs(
  traj_reg_dt,
  "delta_loo_tract_raw",
  "dest_loo_tract_raw",
  "orig_loo_tract_raw",
  "LOO tract filing rate (raw high-filing dummy)",
  regressor = "high_dummy"
)

# Table dictionary for all etables in this script
fixest::setFixest_etable(digits = 4, fitstat = c("n"))
etable_dict <- c(
  "(Intercept)" = "Constant",
  "mid_evict_bin" = "Middle building filing-rate bin",
  "num_units_bins" = "Units bin (destination)",
  "prev_num_units_bins" = "Units bin (origin)",
  "orig_GEOID" = "Origin block group",
  "census_tract" = "Destination tract FE",
  "prev_census_tract" = "Origin tract FE",
  "delta_evict_eb" = "Change in filing rate (destination - origin, EB)",
  "dest_evict_eb" = "Destination filing rate (EB)",
  "delta_evict_raw" = "Change in filing rate (destination - origin, raw long-run)",
  "dest_evict_raw" = "Destination filing rate (raw long-run)",
  "delta_severe_pu" = "Change in severe complaints per unit",
  "dest_severe_pu" = "Destination severe complaints per unit",
  "delta_violations_pu" = "Change in total violations per unit",
  "dest_violations_pu" = "Destination total violations per unit",
  "delta_severe_violations_pu" = "Change in severe violations per unit",
  "dest_severe_violations_pu" = "Destination severe violations per unit",
  "delta_loo_tract_eb" = "Change in LOO tract filing rate (EB)",
  "dest_loo_tract_eb" = "Destination LOO tract filing rate (EB)",
  "delta_loo_tract_raw" = "Change in LOO tract filing rate (raw)",
  "dest_loo_tract_raw" = "Destination LOO tract filing rate (raw)",
  "filing_rate_longrun_pre2019" = "Filing rate (destination, raw long-run)",
  "prev_filing_rate_longrun_pre2019" = "Filing rate (origin, raw long-run)",
  "filing_rate_eb_pre_covid" = "Filing rate (destination, EB)",
  "prev_filing_rate_eb_pre_covid" = "Filing rate (origin, EB)",
  "high_filing_orig" = "Origin high-filing indicator (>=15%)",
  "bldg_rent_real" = "Avg real rent (destination building)",
  "prev_bldg_rent_real" = "Avg real rent (origin building)",
  "bldg_rent_nom" = "Avg nominal rent (destination building)",
  "prev_bldg_rent_nom" = "Avg nominal rent (origin building)",
  "mid_high_evict_dummy" = "Middle building high-filing indicator (>=15%)",
  "orig_evict_eb" = "Origin filing rate (EB)",
  "orig_evict_raw" = "Origin filing rate (raw long-run)",
  "orig_severe_pu" = "Origin severe complaints per unit",
  "orig_violations_pu" = "Origin total violations per unit",
  "orig_severe_violations_pu" = "Origin severe violations per unit",
  "orig_loo_tract_eb" = "Origin LOO tract filing rate (EB)",
  "orig_loo_tract_raw" = "Origin LOO tract filing rate (raw)",
  "orig_log_total_area" = "Log(origin total area)",
  "orig_log_market_value" = "Log(origin market value)",
  "orig_tract" = "Origin tract FE",
  "orig_year_blt_decade" = "Origin year-built decade FE",
  "orig_num_units_bin" = "Origin unit-bin FE",
  "orig_building_type" = "Origin building-type FE",
  "orig_quality_grade_standard" = "Origin quality-grade FE",
  "orig_num_stories_bin" = "Origin stories-bin FE",
  "orig_tract^orig_filing_bin" = "Origin tract x origin filing-bin FE"
)
setFixest_dict(etable_dict)

write_etable <- function(models, path, title, headers) {
  tab <- do.call(
    etable,
    c(
      models,
      list(
        title = title,
        headers = headers,
        dict = etable_dict,
        tex = TRUE
      )
    )
  )
  writeLines(tab, path)
}

# Output tables
if (!is.null(traj_eb_bins)) {
  write_etable(
    list(traj_eb_bins$delta$m_a, traj_eb_bins$delta$m_b, traj_eb_bins$delta$m_c, traj_eb_bins$delta$m_d),
    file.path(tab_dir, "trajectory_regressions_delta.tex"),
    "3-move trajectory: delta filing outcomes (EB bins, main)",
    c("Delta A", "Delta B", "Delta C", "Delta D")
  )
  logf("Wrote delta filing trajectory table (EB bins)", log_file = log_file)

  write_etable(
    list(traj_eb_bins$dest$m_a, traj_eb_bins$dest$m_b, traj_eb_bins$dest$m_c, traj_eb_bins$dest$m_d),
    file.path(tab_dir, "trajectory_regressions_dest.tex"),
    "3-move trajectory: destination filing outcomes (EB bins, main)",
    c("Dest A", "Dest B", "Dest C", "Dest D")
  )
  logf("Wrote destination filing trajectory table (EB bins)", log_file = log_file)
}

if (!is.null(traj_eb_high)) {
  write_etable(
    list(traj_eb_high$dest$m_a, traj_eb_high$dest$m_b, traj_eb_high$dest$m_c, traj_eb_high$dest$m_d),
    file.path(tab_dir, "trajectory_regressions_dest_high_dummy.tex"),
    "3-move trajectory: destination filing outcomes (EB high-filing dummy, main)",
    c("Dest A", "Dest B", "Dest C", "Dest D")
  )
  logf("Wrote destination filing trajectory table (EB high-filing dummy)", log_file = log_file)
}

if (!is.null(traj_raw_bins)) {
  write_etable(
    list(traj_raw_bins$delta$m_a, traj_raw_bins$delta$m_b, traj_raw_bins$delta$m_c, traj_raw_bins$delta$m_d),
    file.path(tab_dir, "trajectory_regressions_delta_raw.tex"),
    title = "3-move trajectory: delta filing outcomes (raw long-run robustness)",
    headers = c("Delta A", "Delta B", "Delta C", "Delta D")
  )
  logf("Wrote raw delta filing trajectory table (appendix, bins)", log_file = log_file)

  write_etable(
    list(traj_raw_bins$dest$m_a, traj_raw_bins$dest$m_b, traj_raw_bins$dest$m_c, traj_raw_bins$dest$m_d),
    file.path(tab_dir, "trajectory_regressions_dest_raw.tex"),
    title = "3-move trajectory: destination filing outcomes (raw long-run robustness)",
    headers = c("Dest A", "Dest B", "Dest C", "Dest D")
  )
  logf("Wrote raw destination filing trajectory table (appendix, bins)", log_file = log_file)
}

if (!is.null(traj_raw_high)) {
  write_etable(
    list(traj_raw_high$dest$m_a, traj_raw_high$dest$m_b, traj_raw_high$dest$m_c, traj_raw_high$dest$m_d),
    file.path(tab_dir, "trajectory_regressions_dest_high_dummy_raw.tex"),
    "3-move trajectory: destination filing outcomes (raw high-filing dummy, appendix robustness)",
    c("Dest A", "Dest B", "Dest C", "Dest D")
  )
  logf("Wrote raw destination filing trajectory table (appendix, high-filing dummy)", log_file = log_file)
}

if (!is.null(traj_severe)) {
  write_etable(
    list(traj_severe$delta$m_a, traj_severe$delta$m_b, traj_severe$delta$m_c, traj_severe$delta$m_d),
    file.path(tab_dir, "trajectory_regressions_severe_delta.tex"),
    title = "3-move trajectory: delta severe complaints per unit (long-run pre-2019)",
    headers = c("Delta A", "Delta B", "Delta C", "Delta D")
  )
  logf("Wrote delta severe trajectory table", log_file = log_file)

  write_etable(
    list(traj_severe$dest$m_a, traj_severe$dest$m_b, traj_severe$dest$m_c, traj_severe$dest$m_d),
    file.path(tab_dir, "trajectory_regressions_severe_dest.tex"),
    title = "3-move trajectory: destination severe complaints per unit (long-run pre-2019)",
    headers = c("Dest A", "Dest B", "Dest C", "Dest D")
  )
  logf("Wrote destination severe trajectory table", log_file = log_file)
}

if (!is.null(traj_viol)) {
  write_etable(
    list(traj_viol$delta$m_a, traj_viol$delta$m_b, traj_viol$delta$m_c, traj_viol$delta$m_d),
    file.path(tab_dir, "trajectory_regressions_violations_delta.tex"),
    "3-move trajectory: delta total violations per unit (long-run pre-2019)",
    c("Delta A", "Delta B", "Delta C", "Delta D")
  )
  logf("Wrote delta total-violations trajectory table", log_file = log_file)

  write_etable(
    list(traj_viol$dest$m_a, traj_viol$dest$m_b, traj_viol$dest$m_c, traj_viol$dest$m_d),
    file.path(tab_dir, "trajectory_regressions_violations_dest.tex"),
    "3-move trajectory: destination total violations per unit (long-run pre-2019)",
    c("Dest A", "Dest B", "Dest C", "Dest D")
  )
  logf("Wrote destination total-violations trajectory table", log_file = log_file)
}

if (!is.null(traj_severe_viol)) {
  write_etable(
    list(traj_severe_viol$delta$m_a, traj_severe_viol$delta$m_b, traj_severe_viol$delta$m_c, traj_severe_viol$delta$m_d),
    file.path(tab_dir, "trajectory_regressions_severe_violations_delta.tex"),
    "3-move trajectory: delta severe violations per unit (long-run pre-2019)",
    c("Delta A", "Delta B", "Delta C", "Delta D")
  )
  logf("Wrote delta severe-violations trajectory table", log_file = log_file)

  write_etable(
    list(traj_severe_viol$dest$m_a, traj_severe_viol$dest$m_b, traj_severe_viol$dest$m_c, traj_severe_viol$dest$m_d),
    file.path(tab_dir, "trajectory_regressions_severe_violations_dest.tex"),
    "3-move trajectory: destination severe violations per unit (long-run pre-2019)",
    c("Dest A", "Dest B", "Dest C", "Dest D")
  )
  logf("Wrote destination severe-violations trajectory table", log_file = log_file)
}

if (!is.null(traj_loo_eb_bins)) {
  write_etable(
    list(
      traj_loo_eb_bins$delta$m_a, traj_loo_eb_bins$delta$m_b,
      traj_loo_eb_bins$dest$m_a, traj_loo_eb_bins$dest$m_b, traj_loo_eb_bins$dest$m_c, traj_loo_eb_bins$dest$m_d
    ),
    file.path(tab_dir, "trajectory_regressions_loo.tex"),
    "3-move trajectory: LOO tract filing-rate outcomes (EB bins, main)",
    c("Delta A", "Delta B", "Dest A", "Dest B", "Dest C", "Dest D")
  )
  logf("Wrote LOO trajectory table (EB bins)", log_file = log_file)
}

if (!is.null(traj_loo_eb_high)) {
  write_etable(
    list(
      traj_loo_eb_high$delta$m_a, traj_loo_eb_high$delta$m_b,
      traj_loo_eb_high$dest$m_a, traj_loo_eb_high$dest$m_b, traj_loo_eb_high$dest$m_c, traj_loo_eb_high$dest$m_d
    ),
    file.path(tab_dir, "trajectory_regressions_loo_high_dummy.tex"),
    "3-move trajectory: LOO tract filing-rate outcomes (EB high-filing dummy, main)",
    c("Delta A", "Delta B", "Dest A", "Dest B", "Dest C", "Dest D")
  )
  logf("Wrote LOO trajectory table (EB high-filing dummy)", log_file = log_file)
}

if (!is.null(traj_loo_raw_bins)) {
  write_etable(
    list(
      traj_loo_raw_bins$delta$m_a, traj_loo_raw_bins$delta$m_b,
      traj_loo_raw_bins$dest$m_a, traj_loo_raw_bins$dest$m_b, traj_loo_raw_bins$dest$m_c, traj_loo_raw_bins$dest$m_d
    ),
    file.path(tab_dir, "trajectory_regressions_loo_raw.tex"),
    "3-move trajectory: LOO tract filing-rate outcomes (raw bins, appendix robustness)",
    c("Delta A", "Delta B", "Dest A", "Dest B", "Dest C", "Dest D")
  )
  logf("Wrote LOO trajectory table (raw bins appendix)", log_file = log_file)
}

if (!is.null(traj_loo_raw_high)) {
  write_etable(
    list(
      traj_loo_raw_high$delta$m_a, traj_loo_raw_high$delta$m_b,
      traj_loo_raw_high$dest$m_a, traj_loo_raw_high$dest$m_b, traj_loo_raw_high$dest$m_c, traj_loo_raw_high$dest$m_d
    ),
    file.path(tab_dir, "trajectory_regressions_loo_high_dummy_raw.tex"),
    "3-move trajectory: LOO tract filing-rate outcomes (raw high-filing dummy, appendix robustness)",
    c("Delta A", "Delta B", "Dest A", "Dest B", "Dest C", "Dest D")
  )
  logf("Wrote LOO trajectory table (raw high-filing dummy appendix)", log_file = log_file)
}

if (!is.null(traj_eb_high)) {
  write_etable(
    list(traj_eb_high$delta$m_a, traj_eb_high$delta$m_b, traj_eb_high$delta$m_c, traj_eb_high$delta$m_d),
    file.path(tab_dir, "trajectory_regressions_delta_high_dummy.tex"),
    "3-move trajectory: delta filing outcomes (EB high-filing dummy, main)",
    c("Delta A", "Delta B", "Delta C", "Delta D")
  )
  logf("Wrote delta filing trajectory table (EB high-filing dummy)", log_file = log_file)
}

if (!is.null(traj_raw_high)) {
  write_etable(
    list(traj_raw_high$delta$m_a, traj_raw_high$delta$m_b, traj_raw_high$delta$m_c, traj_raw_high$delta$m_d),
    file.path(tab_dir, "trajectory_regressions_delta_high_dummy_raw.tex"),
    "3-move trajectory: delta filing outcomes (raw high-filing dummy, appendix robustness)",
    c("Delta A", "Delta B", "Delta C", "Delta D")
  )
  logf("Wrote delta filing trajectory table (raw high-filing dummy appendix)", log_file = log_file)
}

if (!is.null(traj_loo_eb_bins)) {
  # keeps backward compatibility with previous log naming
  logf("Wrote LOO trajectory table", log_file = log_file)
}

if (!is.null(traj_loo_eb_bins)) {
  # legacy alias file for continuity
  write_etable(
    list(
      traj_loo_eb_bins$delta$m_a, traj_loo_eb_bins$delta$m_b,
      traj_loo_eb_bins$dest$m_a, traj_loo_eb_bins$dest$m_b, traj_loo_eb_bins$dest$m_c, traj_loo_eb_bins$dest$m_d
    ),
    file.path(tab_dir, "trajectory_regressions_loo_eb.tex"),
    title = "3-move trajectory: LOO tract eviction rate outcomes",
    headers = c("Delta A", "Delta B", "Dest A", "Dest B", "Dest C", "Dest D")
  )
}

logf("--- End inflow/outflow/trajectory analysis ---", log_file = log_file)

# ---- Step 5: Write tracking descriptives outputs ----

# CSV (long format)
comp_long <- melt(comp_table, id.vars = "group", variable.name = "metric", value.name = "value")
qa_csv_path <- p_out(cfg, "qa", "address_history_tracking_descriptives.csv")
dir.create(dirname(qa_csv_path), showWarnings = FALSE, recursive = TRUE)
fwrite(comp_long, qa_csv_path)
logf("Wrote tracking descriptives CSV -> ", qa_csv_path, log_file = log_file)

# LaTeX table
tex_path <- file.path(tab_dir, "tracking_descriptives.tex")
dir.create(dirname(tex_path), showWarnings = FALSE, recursive = TRUE)

# Format numbers with commas for display
fmt_num <- function(x) {
  ifelse(abs(x) >= 1000, format(round(x), big.mark = ","), as.character(x))
}

# Build LaTeX manually for full control
tex_lines <- c(
  "\\begin{table}[htbp]",
  "\\centering",
  "\\caption{Tracking Descriptives: Movers vs.\\ Stayers}",
  "\\label{tab:tracking_descriptives}",
  "\\begin{tabular}{lrrr}",
  "\\toprule",
  paste0(" & ", paste(comp_table$group, collapse = " & "), " \\\\"),
  "\\midrule"
)

row_labels <- c(
  n_hh_years = "N HH-Years",
  n_unique_hh = "N Unique HHs",
  mean_addresses = "Mean Addresses per HH",
  mean_years_obs = "Mean Years Observed",
  pct_black = "\\% Black (mean posterior)",
  pct_white = "\\% White (mean posterior)",
  pct_hispanic = "\\% Hispanic (mean posterior)",
  pct_female_hoh = "\\% Female HoH",
  mean_filing_rate = "Mean Filing Rate",
  mean_total_units = "Mean Building Size (units)",
  mean_log_rent = "Mean Log Rent",
  n_unique_tracts = "N Unique Tracts"
)

for (metric in names(row_labels)) {
  vals <- comp_table[[metric]]
  if (metric %in% c("n_hh_years", "n_unique_hh", "n_unique_tracts")) {
    vals_fmt <- format(vals, big.mark = ",")
  } else {
    vals_fmt <- as.character(vals)
  }
  tex_lines <- c(tex_lines,
    paste0(row_labels[metric], " & ", paste(vals_fmt, collapse = " & "), " \\\\")
  )
}

tex_lines <- c(tex_lines,
  "\\bottomrule",
  "\\end{tabular}",
  "\\end{table}"
)
writeLines(tex_lines, tex_path)
logf("Wrote tracking descriptives LaTeX -> ", tex_path, log_file = log_file)

logf("--- End tracking descriptives ---", log_file = log_file)

# ---- Unit size bins (current and previous) ----
bin_units <- function(x) {
  fifelse(x == 1, "1",
  fifelse(x <= 5, "2-5",
  fifelse(x <= 50, "6-50",
  fifelse(x <= 100, "51-100",
  fifelse(x > 100, "101+", NA_character_)))))
}
movers[, num_units_bins := bin_units(total_units)]
movers[, prev_num_units_bins := bin_units(prev_total_units)]

# Add prev_PID (created on philly_infousa_dt_m after movers was extracted)
movers <- merge(
  movers,
  philly_infousa_dt_m[, .(obs_id, prev_PID)],
  by = "obs_id", all.x = TRUE
)

# Clean column names for fixest (GEOID -> geoid, PID -> pid, etc.)
movers <- janitor::clean_names(movers)

# ---- Building-level average rent (PID-level, all years) ----
# Rent is sparse year-to-year, so aggregate to building level for persistence regressions.
# Compute from full bldg_panel_blp, then merge by destination PID and origin PID separately.
pid_rent <- philly_rent_df[!is.na(log_med_rent_real), .(
  bldg_rent_real = mean(log_med_rent_real, na.rm = TRUE),
  bldg_rent_nom  = mean(log_med_rent, na.rm = TRUE)
), by = PID]
pid_rent[, PID := as.character(PID)]
logf("Building-level avg rent: ", format(nrow(pid_rent), big.mark = ","),
     " PIDs with rent data", log_file = log_file)

# Merge destination rent
movers[, pid_chr := as.character(pid)]
movers[pid_rent, bldg_rent_real := i.bldg_rent_real, on = c("pid_chr" = "PID")]
movers[pid_rent, bldg_rent_nom  := i.bldg_rent_nom,  on = c("pid_chr" = "PID")]
# Merge origin rent (prev_pid -> PID)
movers[, prev_pid_chr := as.character(prev_pid)]
movers[pid_rent, prev_bldg_rent_real := i.bldg_rent_real, on = c("prev_pid_chr" = "PID")]
movers[pid_rent, prev_bldg_rent_nom  := i.bldg_rent_nom,  on = c("prev_pid_chr" = "PID")]
movers[, c("pid_chr", "prev_pid_chr") := NULL]

n_dest_rent <- movers[!is.na(bldg_rent_real), .N]
n_both_rent <- movers[!is.na(bldg_rent_real) & !is.na(prev_bldg_rent_real), .N]
logf("  Movers with dest bldg rent: ", format(n_dest_rent, big.mark = ","),
     "; both origin+dest: ", format(n_both_rent, big.mark = ","),
     " / ", format(nrow(movers), big.mark = ","),
     log_file = log_file)

# ---- Eviction persistence regressions ----
# Main specifications use EB shrinkage rate; raw long-run rate is appendix robustness.
# These are time-invariant building characteristics (not annual filing_rate_raw).
logf("Running fixest regressions (EB main + raw appendix robustness)...", log_file = log_file)

log_model <- function(label, mod, log_file) {
  s <- summary(mod)
  ct <- s$coeftable
  logf("  ", label, " (N=", mod$nobs, "):", log_file = log_file)
  for (v in rownames(ct)) {
    logf("    ", v, ": coef=", round(ct[v, "Estimate"], 4),
         " se=", round(ct[v, "Std. Error"], 4),
         " p=", format.pval(ct[v, "Pr(>|t|)"], digits = 3),
         log_file = log_file)
  }
}

# Create census tract variables for FE (coarser than BG to avoid over-absorption
# with time-invariant building-level filing rates)
movers[, census_tract := substr(geoid, 1, 11)]
movers[, prev_census_tract := substr(prev_geoid, 1, 11)]

# Helper: run a persistence spec with a given filing rate variable pair
# Uses census tract FE (not BG) since filing rates are building-level constants
# and BG FE would absorb most variation in BGs with few movers.
run_persist_specs <- function(dt, fr_dest, fr_orig, rent_dest, rent_orig, label_suffix) {
  # Filter to non-missing
  dt_base <- dt[!is.na(get(fr_dest)) & !is.na(get(fr_orig))]

  # m1: bivariate
  f1 <- as.formula(paste0(fr_dest, " ~ ", fr_orig))
  m1 <- feols(f1, cluster = ~pid, data = dt_base)

  # m2: + unit bins + census tract FE
  f2 <- as.formula(paste0(fr_dest, " ~ ", fr_orig,
    " | num_units_bins + prev_num_units_bins + census_tract + prev_census_tract"))
  m2 <- feols(f2, cluster = ~pid, data = dt_base)

  # m3: + hedonic-deflated rent
  f3 <- as.formula(paste0(fr_dest, " ~ ", fr_orig, " + ", rent_dest, " + ", rent_orig,
    " | num_units_bins + prev_num_units_bins + census_tract + prev_census_tract"))
  dt_rent <- dt_base[!is.na(get(rent_dest)) & !is.na(get(rent_orig))]
  m3 <- feols(f3, cluster = ~pid, data = dt_rent)

  # m4: high-filer indicator (>= 15%)
  dt_base[, high_filing_dest := get(fr_dest) >= high_filing_threshold]
  dt_base[, high_filing_orig := get(fr_orig) >= high_filing_threshold]
  m4 <- feols(
    high_filing_dest ~ high_filing_orig |
      num_units_bins + prev_num_units_bins + census_tract + prev_census_tract,
    cluster = ~pid, data = dt_base
  )

  # m5: high-filer + rent
  dt_hf_rent <- dt_base[!is.na(get(rent_dest)) & !is.na(get(rent_orig)) &
                           total_units > 1 & prev_total_units > 1]
  f5 <- as.formula(paste0("high_filing_dest ~ high_filing_orig + ",
                           rent_dest, " + ", rent_orig,
    " | num_units_bins + prev_num_units_bins + census_tract + prev_census_tract"))
  m5 <- feols(f5, cluster = ~pid, data = dt_hf_rent)

  # m_rent: rent ~ prev filing rate + prev rent + current filing rate
  f_rent <- as.formula(paste0(rent_dest, " ~ ", fr_orig, " + ", rent_orig, " + ", fr_dest,
    " | num_units_bins + prev_num_units_bins + census_tract + prev_census_tract"))
  dt_rent2 <- dt_base[!is.na(get(rent_dest)) & !is.na(get(rent_orig))]
  m_rent <- feols(f_rent, cluster = ~pid, data = dt_rent2)

  list(m1 = m1, m2 = m2, m3 = m3, m4 = m4, m5 = m5, m_rent = m_rent)
}

# --- Run with long-run raw rate (filing_rate_longrun_pre2019) ---
res_raw <- run_persist_specs(
  movers, "filing_rate_longrun_pre2019", "prev_filing_rate_longrun_pre2019",
  "bldg_rent_real", "prev_bldg_rent_real", "raw"
)

logf("Long-run raw rate results:", log_file = log_file)
for (nm in names(res_raw)) log_model(paste0(nm, " (raw)"), res_raw[[nm]], log_file)

# --- Run with EB shrinkage rate (filing_rate_eb_pre_covid) ---
res_eb <- run_persist_specs(
  movers, "filing_rate_eb_pre_covid", "prev_filing_rate_eb_pre_covid",
  "bldg_rent_real", "prev_bldg_rent_real", "eb"
)

logf("EB shrinkage rate results:", log_file = log_file)
for (nm in names(res_eb)) log_model(paste0(nm, " (EB)"), res_eb[[nm]], log_file)

# ---- Output regression tables ----
fixest::setFixest_etable(digits = 4, fitstat = c("n"))
setFixest_dict(etable_dict)

# Table 1: Continuous persistence (EB main)
tex_out <- file.path(tab_dir, "evict_persist.tex")
etable_lines <- etable(
  list(res_eb$m1, res_eb$m2, res_eb$m3),
  title = "Eviction Persistence: EB Shrinkage Filing Rates (Main)",
  headers = c("m1", "m2", "m3"),
  label = "tab:evict_persist",
  drop = "Constant",
  tex = TRUE
)
writeLines(etable_lines, tex_out)
logf("Wrote persistence table -> ", tex_out, log_file = log_file)

# Table 2: High-filer and rent regressions (EB main)
tex_out2 <- file.path(tab_dir, "evict_persist_hf_rent.tex")
etable_lines2 <- etable(
  list(res_eb$m4, res_eb$m5, res_eb$m_rent),
  title = "High-Filer Persistence and Rent Regressions (EB Main)",
  headers = c("m4", "m5", "m_rent"),
  label = "tab:evict_persist_hf_rent",
  drop = "Constant",
  tex = TRUE
)
writeLines(etable_lines2, tex_out2)
logf("Wrote high-filer/rent table -> ", tex_out2, log_file = log_file)

# Appendix robustness tables: raw long-run filing rate
tex_out_raw <- file.path(tab_dir, "evict_persist_raw.tex")
etable_lines_raw <- etable(
  list(res_raw$m1, res_raw$m2, res_raw$m3),
  title = "Eviction Persistence: Raw Long-Run Filing Rates (Appendix Robustness)",
  headers = c("m1", "m2", "m3"),
  label = "tab:evict_persist_raw",
  drop = "Constant",
  tex = TRUE
)
writeLines(etable_lines_raw, tex_out_raw)
logf("Wrote raw persistence appendix table -> ", tex_out_raw, log_file = log_file)

tex_out2_raw <- file.path(tab_dir, "evict_persist_hf_rent_raw.tex")
etable_lines2_raw <- etable(
  list(res_raw$m4, res_raw$m5, res_raw$m_rent),
  title = "High-Filer Persistence and Rent Regressions (Raw Long-Run Appendix Robustness)",
  headers = c("m4", "m5", "m_rent"),
  label = "tab:evict_persist_hf_rent_raw",
  drop = "Constant",
  tex = TRUE
)
writeLines(etable_lines2_raw, tex_out2_raw)
logf("Wrote raw high-filer/rent appendix table -> ", tex_out2_raw, log_file = log_file)

# ---- Correlations ----
cor_eb <- movers[
  prev_total_units >= 1 & total_units >= 1 &
    !is.na(filing_rate_eb_pre_covid) & !is.na(prev_filing_rate_eb_pre_covid),
  cor(prev_filing_rate_eb_pre_covid, filing_rate_eb_pre_covid, use = "complete.obs")
]
cor_raw_lr <- movers[
  prev_total_units >= 1 & total_units >= 1 &
    !is.na(filing_rate_longrun_pre2019) & !is.na(prev_filing_rate_longrun_pre2019),
  cor(prev_filing_rate_longrun_pre2019, filing_rate_longrun_pre2019, use = "complete.obs")
]
cor_rent_real <- movers[
  prev_total_units >= 1 & total_units >= 1 &
    !is.na(bldg_rent_real) & !is.na(prev_bldg_rent_real),
  cor(bldg_rent_real, prev_bldg_rent_real, use = "complete.obs")
]
logf("Correlations (multi-unit):", log_file = log_file)
logf("  prev EB <-> dest EB: ", round(cor_eb, 4), log_file = log_file)
logf("  prev raw LR <-> dest raw LR: ", round(cor_raw_lr, 4), log_file = log_file)
logf("  bldg_rent_real <-> prev_bldg_rent_real: ", round(cor_rent_real, 4), log_file = log_file)

# ---- Eviction bin transitions (using EB pre-COVID rates) ----
movers[, `:=`(
  prev_evict_bin = pmin(round(prev_filing_rate_eb_pre_covid / 0.05) * 0.05, 0.25),
  evict_bin = pmin(round(filing_rate_eb_pre_covid / 0.05) * 0.05, 0.25)
)]

flows <- movers[
  !is.na(prev_evict_bin) & !is.na(evict_bin) &
    total_units > 1 & prev_total_units > 1,
  .N,
  by = .(prev_evict_bin, evict_bin)
]
setorder(flows, prev_evict_bin, evict_bin)
flows[, total_from_bin := sum(N)]
flows[, percent := round(N / total_from_bin, 3)]
flows[, total_to_bin := sum(N), by = evict_bin]
flows[, percent_to := round(N / total_to_bin, 3)]
flows[, `:=`(
  prev_evict_bin_factor = as.factor(prev_evict_bin),
  evict_bin_factor = as.factor(evict_bin)
)]

# ---- High-filer transition matrix (EB rates) ----
movers[, high_filing_eb := filing_rate_eb_pre_covid >= high_filing_threshold]
movers[, high_filing_eb_prev := prev_filing_rate_eb_pre_covid >= high_filing_threshold]

transition_evict <- movers[
  prev_total_units > 1 & total_units > 1 &
    !is.na(high_filing_eb) & !is.na(high_filing_eb_prev),
  .(
    count = .N,
    from_high_to_low = sum(high_filing_eb == FALSE & high_filing_eb_prev == TRUE),
    from_low_to_high = sum(high_filing_eb == TRUE & high_filing_eb_prev == FALSE),
    stay_high = sum(high_filing_eb == TRUE & high_filing_eb_prev == TRUE),
    stay_low = sum(high_filing_eb == FALSE & high_filing_eb_prev == FALSE)
  )
]
transition_evict_pct <- copy(transition_evict)
count_val <- transition_evict_pct$count
transition_evict_pct[, names(transition_evict_pct) := lapply(.SD, function(x) x / count_val)]

logf("Eviction transition matrix (multi-unit movers, EB rates):\n",
     paste(capture.output(print(transition_evict_pct)), collapse = "\n"),
     log_file = log_file)

# ---- Rent decile transitions (hedonic-deflated) ----
# Building-level rent is time-invariant, so rank across all movers (not by year)
movers[, imp_rent_decile := frank(bldg_rent_real, ties.method = "dense")]
movers[, imp_rent_decile := ceiling(10 * imp_rent_decile / max(imp_rent_decile, na.rm = TRUE))]
movers[, imp_prev_rent_decile := frank(prev_bldg_rent_real, ties.method = "dense")]
movers[, imp_prev_rent_decile := ceiling(10 * imp_prev_rent_decile / max(imp_prev_rent_decile, na.rm = TRUE))]

movers[, low_rent := imp_rent_decile <= 2]
movers[, low_prev_rent := imp_prev_rent_decile <= 2]

transition_rent <- movers[
  prev_total_units > 1 & total_units > 1,
  .(
    count = sum(!is.na(low_rent) & !is.na(low_prev_rent)),
    from_high_to_low = sum(low_rent == FALSE & low_prev_rent == TRUE, na.rm = TRUE),
    from_low_to_high = sum(low_rent == TRUE & low_prev_rent == FALSE, na.rm = TRUE),
    stay_high = sum(low_rent == TRUE & low_prev_rent == TRUE, na.rm = TRUE),
    stay_low = sum(low_rent == FALSE & low_prev_rent == FALSE, na.rm = TRUE)
  )
]

logf("Rent transition matrix (multi-unit movers):\n",
     paste(capture.output(print(transition_rent)), collapse = "\n"),
     log_file = log_file)

# ---- Random-assignment counterfactual (EB rates) ----
set.seed(cfg$run$seed %||% 123L)

probs <- movers[
  !is.na(high_filing_eb) & prev_total_units > 1 & total_units > 1,
  .N,
  by = high_filing_eb
][, prob := N / sum(N)]$prob

sim_df <- data.table(
  high_filing_eb = sample(c(FALSE, TRUE), size = 10000, replace = TRUE, prob = probs),
  high_filing_eb_prev = sample(c(FALSE, TRUE), size = 10000, replace = TRUE, prob = probs)
)
sim_transitions <- sim_df[, .(
  count = .N,
  from_high_to_low = sum(high_filing_eb == FALSE & high_filing_eb_prev == TRUE),
  from_low_to_high = sum(high_filing_eb == TRUE & high_filing_eb_prev == FALSE),
  stay_high = sum(high_filing_eb == TRUE & high_filing_eb_prev == TRUE),
  stay_low = sum(high_filing_eb == FALSE & high_filing_eb_prev == FALSE)
)]
sim_count <- sim_transitions$count
sim_transitions_pct <- copy(sim_transitions)
sim_transitions_pct[, names(sim_transitions_pct) := lapply(.SD, function(x) x / sim_count)]

logf("Random-assignment counterfactual:\n",
     paste(capture.output(print(sim_transitions_pct)), collapse = "\n"),
     log_file = log_file)

# ---- Bin scatter data (EB rates) ----
bin_scatter <- movers[
  !is.na(filing_rate_eb_pre_covid) & !is.na(prev_evict_bin) &
    prev_total_units > 1 & total_units > 1 & prev_evict_bin <= 1,
  .(mean_evict = mean(filing_rate_eb_pre_covid, na.rm = TRUE), .N),
  by = prev_evict_bin
]

# ---- Plots ----
logf("Generating plots...", log_file = log_file)

# Alluvial flow plot
p_alluvial <- ggplot(
  flows[evict_bin == 0.0],
  aes(axis1 = prev_evict_bin_factor, axis2 = evict_bin_factor, y = percent_to)
) +
  scale_x_discrete(limits = c("Input", "Output"), expand = c(.1, .05)) +
  geom_alluvium(aes(fill = prev_evict_bin_factor), width = 1 / 12, alpha = 0.9) +
  geom_stratum(width = 1 / 12, fill = "grey90", color = "grey40") +
  geom_text(stat = "stratum", aes(label = after_stat(stratum)), size = 3.5) +
  labs(title = "Flows from Inputs to Outputs",
       y = "Weight", x = NULL, fill = "Input") +
  scale_fill_viridis_d(option = "C") +
  theme_minimal(base_size = 12) +
  theme(panel.grid = element_blank())

# Bin scatter
p_binscatter <- ggplot(
  bin_scatter[mean_evict <= 1 & prev_evict_bin <= 1],
  aes(x = prev_evict_bin, y = mean_evict)
) +
  geom_point() +
  geom_smooth() +
  geom_smooth(method = "lm", color = "black") +
  geom_abline(slope = 1, intercept = 0, color = "red")

# Raw scatter (EB rates)
p_scatter <- ggplot(
  movers[
    !is.na(filing_rate_eb_pre_covid) & !is.na(prev_filing_rate_eb_pre_covid) &
      filing_rate_eb_pre_covid <= 0.5 & prev_filing_rate_eb_pre_covid <= 0.5 &
      prev_total_units > 1 & total_units > 1
  ],
  aes(x = prev_filing_rate_eb_pre_covid, y = filing_rate_eb_pre_covid)
) +
  geom_point(alpha = 0.1) +
  geom_smooth() +
  geom_abline(slope = 1, intercept = 0, color = "red")

logf("=== Finished address-history-analysis.R ===", log_file = log_file)
