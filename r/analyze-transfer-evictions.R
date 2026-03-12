## ============================================================
## analyze-transfer-evictions.R  [DEPRECATED]
## ============================================================
## NOTE: This script is superseded by analyze-transfer-evictions-unified.R
##       which supports both annual and quarterly frequency.
##       Kept for reference only.
## ============================================================
## Purpose: Regression event study of eviction filing rates
##          around ownership transfers (t-3 to t+5).
##
## Inputs (from config):
##   - rtt_clean
##   - bldg_panel_blp
##   - owner_linkage_xwalk    (from build-owner-linkage.R; PID key for person entities)
##   - owner_portfolio        (from build-ownership-panel.R; keyed on conglomerate_id)
##   - xwalk_pid_conglomerate (from build-ownership-panel.R; PID x year → conglomerate_id)
##
## Outputs (to output/transfer_evictions/):
##   - Coefficients CSVs, LaTeX tables, coefficient plots
##   - Descriptives and QA files
##   - output/logs/analyze-transfer-evictions.log
##
## Prerequisite: Run r/build-owner-linkage.R first.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(ggplot2)
  library(fixest)
})

source("r/config.R")
source("r/helper-functions.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "analyze-transfer-evictions.log")

logf("=== Starting analyze-transfer-evictions.R ===", log_file = log_file)

# Output directories
out_dir <- p_out(cfg, "transfer_evictions")
fig_dir <- p_out(cfg, "figs")
fs::dir_create(out_dir, recurse = TRUE)
fs::dir_create(fig_dir, recurse = TRUE)

normalize_pid <- function(x) {
  stringr::str_pad(as.character(x), width = 9, side = "left", pad = "0")
}

# Helper to extract fixest coefs
extract_feols_coefs <- function(fit, spec_label) {
  ct <- as.data.table(coeftable(fit), keep.rownames = "term")
  setnames(ct, c("term", "estimate", "std_error", "t_value", "p_value"))
  ct[, estimate  := round(estimate, 6)]
  ct[, std_error := round(std_error, 6)]
  ct[, t_value   := round(t_value, 2)]
  ct[, p_value   := round(p_value, 4)]
  ct[, spec := spec_label]
  ct[, n := fit$nobs]
  ct[, r2 := round(fixest::r2(fit, "ar2"), 4)]
  ct
}

# Helper to clean spec labels for human-readable facet strips / legends
clean_spec_labels <- function(specs) {
  label_map <- c(
    "full_pid_year"                = "Full (year FE)",
    "full_pid_bgxyear"             = "Full (BG\u00d7year)",
    "large_pid_year"               = "5+ (year FE)",
    "large_pid_bgxyear"            = "5+ (BG\u00d7year)",
    "known_rental_pid_year"        = "Known rental",
    "known_rental_large_pid_year"  = "Known rental 5+",
    "2plus_pid_year"               = "2+ units",
    "corporate"                    = "Corporate",
    "individual"                   = "Individual",
    "corporate_large"              = "Corporate (5+)",
    "individual_large"             = "Individual (5+)",
    "size_1 unit"                  = "1 unit",
    "size_2-4 units"               = "2-4 units",
    "size_5-9 units"               = "5-9 units",
    "size_10-19 units"             = "10-19 units",
    "size_20+ units"               = "20+ units",
    "no_sheriff"                   = "No sheriff deeds",
    "single_transfer"              = "Single-transfer PIDs",
    "count_outcome"                = "Count outcome",
    "violations"                   = "Violations",
    "violations_rate"              = "Violations rate",
    "full_highfiler_portfolio"     = "High-filer",
    "full_lowfiler_portfolio"      = "Low-filer",
    "full_nonfiler_has_portfolio"  = "Non-filer (portfolio)",
    "full_singlepurchase"          = "Single-purchase"
  )
  out <- character(length(specs))
  for (i in seq_along(specs)) {
    s <- specs[i]
    if (s %in% names(label_map)) {
      out[i] <- label_map[s]
    } else if (grepl("^portfolio_", s)) {
      out[i] <- gsub("^portfolio_", "Portfolio: ", s)
    } else {
      out[i] <- s
    }
  }
  out
}

# ============================================================
# SECTION 0: Data Loading
# ============================================================
logf("--- SECTION 0: Loading data ---", log_file = log_file)

rtt <- fread(p_product(cfg, "rtt_clean"))
rtt[, PID := normalize_pid(PID)]
rtt[, year := as.integer(year)]
logf("  rtt_clean: ", nrow(rtt), " rows, ", rtt[, uniqueN(PID)], " unique PIDs",
     log_file = log_file)

bldg <- fread(p_product(cfg, "bldg_panel_blp"))
bldg[, PID := normalize_pid(PID)]
bldg[, year := as.integer(year)]
logf("  bldg_panel_blp: ", nrow(bldg), " rows, ", bldg[, uniqueN(PID)], " unique PIDs",
     log_file = log_file)
if ("rental_ownership_unsafe_any" %in% names(bldg)) {
  bldg[, rental_ownership_unsafe_any := as.logical(rental_ownership_unsafe_any)]
  n_bldg_before <- nrow(bldg)
  n_pid_before <- bldg[, uniqueN(PID)]
  units_before <- if ("total_units" %in% names(bldg)) bldg[, sum(total_units, na.rm = TRUE)] else NA_real_
  unsafe_idx <- !is.na(bldg$rental_ownership_unsafe_any) & bldg$rental_ownership_unsafe_any
  n_flag_rows <- bldg[unsafe_idx, .N]
  n_flag_pids <- bldg[unsafe_idx, uniqueN(PID)]
  units_flag <- if ("total_units" %in% names(bldg)) {
    bldg[unsafe_idx, sum(total_units, na.rm = TRUE)]
  } else NA_real_
  bldg <- bldg[!unsafe_idx]
  logf("  Excluded ownership-unsafe rental rows from bldg_panel_blp: ",
       n_flag_rows, " rows (", round(100 * n_flag_rows / n_bldg_before, 2), "%), ",
       n_flag_pids, " PIDs",
       if (!is.na(units_flag) && !is.na(units_before) && units_before > 0) {
         paste0(", ", units_flag, " units (", round(100 * units_flag / units_before, 2), "%)")
       } else "",
       log_file = log_file)
  logf("  bldg_panel_blp after ownership-unsafe exclusion: ", nrow(bldg), " rows, ",
       bldg[, uniqueN(PID)], " unique PIDs (dropped ",
       n_bldg_before - nrow(bldg), " rows, ", n_pid_before - bldg[, uniqueN(PID)], " PIDs)",
       log_file = log_file)
} else {
  logf("  WARNING: rental_ownership_unsafe_any not found in bldg_panel_blp; no ownership-unsafe exclusion applied",
       log_file = log_file)
}

# Owner linkage
xwalk         <- fread(p_product(cfg, "owner_linkage_xwalk"))
portfolio_cong <- fread(p_product(cfg, "owner_portfolio"))
xwalk_cong    <- fread(p_product(cfg, "xwalk_pid_conglomerate"))
xwalk_cong[, PID := normalize_pid(PID)]
logf("  owner_linkage_xwalk: ", nrow(xwalk), " rows", log_file = log_file)
logf("  owner_portfolio (conglomerate-level): ", nrow(portfolio_cong), " rows",
     log_file = log_file)

# ============================================================
# SECTION 1: Build Transfer Event Panel
# ============================================================
logf("--- SECTION 1: Building transfer event panel ---", log_file = log_file)

# Prep transfer-level data
rtt[, grantee_upper := toupper(trimws(as.character(grantees)))]
rtt[, is_sheriff_deed := grepl("SHERIFF", toupper(document_type))]

# Collapse to (PID, year): keep transfer with highest total_consideration
setorder(rtt, PID, year, -total_consideration)
transfers <- rtt[, .SD[1], by = .(PID, year)]
n_collapsed <- nrow(rtt) - nrow(transfers)
logf("  Collapsed RTT to PID-year level: ", nrow(transfers), " transfers (",
     n_collapsed, " duplicates dropped)", log_file = log_file)

# Merge owner linkage — two-step join: corps by grantee_upper, persons by (grantee_upper, PID).
xwalk_corp_ow <- xwalk[is.na(PID), .(grantee_upper, owner_group_id, is_corp)]
xwalk_pers_ow <- xwalk[!is.na(PID), .(grantee_upper, PID, owner_group_id, is_corp)]
corp_grantees_ow <- xwalk_corp_ow[, unique(grantee_upper)]

transfers_corp_ow <- merge(
  transfers[grantee_upper %in% corp_grantees_ow],
  xwalk_corp_ow, by = "grantee_upper", all.x = TRUE)
transfers_pers_ow <- merge(
  transfers[!grantee_upper %in% corp_grantees_ow],
  xwalk_pers_ow, by = c("grantee_upper", "PID"), all.x = TRUE)
transfers <- rbind(transfers_corp_ow, transfers_pers_ow, fill = TRUE)

# Portfolio: join via (PID, year) → conglomerate_id → portfolio stats.
transfers <- merge(transfers,
  xwalk_cong[, .(PID, year, conglomerate_id)],
  by = c("PID", "year"), all.x = TRUE)
transfers <- merge(transfers,
  portfolio_cong[, .(conglomerate_id, n_properties_total, portfolio_bin)],
  by = "conglomerate_id", all.x = TRUE)

logf("  Owner linkage match rate: ",
     round(100 * transfers[!is.na(owner_group_id), .N] / nrow(transfers), 1), "%",
     log_file = log_file)

# Buyer type labels
transfers[, buyer_type := fifelse(is_corp == TRUE, "Corporate", "Individual")]

# Expand to event-time panel: t-3 to t+5
event_grid <- transfers[, .(event_time = -3L:5L), by = .(PID, transfer_year = year)]
event_grid[, year := transfer_year + event_time]
logf("  Event grid: ", nrow(event_grid), " rows (", event_grid[, uniqueN(PID)],
     " PIDs x 9 event periods)", log_file = log_file)

# Merge building outcomes
bldg_cols <- c("PID", "year", "num_filings", "total_units", "total_violations",
               "total_complaints", "med_rent", "GEOID", "building_type", "num_units_bin",
               "rental_from_license", "rental_from_altos", "rental_from_evict",
               "ever_rental_any_year", "ever_rental_any_year_ever")
bldg_cols <- intersect(bldg_cols, names(bldg))
bldg_sub <- bldg[, ..bldg_cols]

event_panel <- merge(event_grid, bldg_sub, by = c("PID", "year"), all.x = TRUE)
logf("  Event panel after outcome merge: ", nrow(event_panel), " rows",
     log_file = log_file)
logf("  Outcome coverage: ",
     round(100 * event_panel[!is.na(num_filings), .N] / nrow(event_panel), 1),
     "% have filing data", log_file = log_file)

# Compute fresh filing rate
event_panel[, filing_rate := fifelse(
  !is.na(num_filings) & !is.na(total_units) & total_units > 0,
  num_filings / total_units,
  NA_real_
)]

# Merge transfer-level characteristics
transfer_chars <- transfers[, .(PID, transfer_year = year,
                                buyer_type, is_corp, is_sheriff_deed,
                                owner_group_id, n_properties_total, portfolio_bin,
                                total_consideration)]
event_panel <- merge(event_panel, transfer_chars,
                     by = c("PID", "transfer_year"), all.x = TRUE)

# --- Acquirer filing behavior classification (PRE-acquisition, temporal) ---
# For each transfer of PID_i by owner_j in year Y:
#   1. Get all OTHER PIDs that owner_j acquired
#   2. From bldg_panel, compute mean(num_filings / total_units) for those PIDs
#      in years strictly BEFORE Y
#   3. Classify into High/Low/Non-filer/Single-purchase via median split
# This avoids look-ahead bias (no post-transfer data used).

bldg_annual <- bldg[!is.na(total_units) & total_units > 0,
                     .(PID, year, annual_filing_rate = num_filings / total_units)]

owner_pids_temporal <- transfers[!is.na(owner_group_id),
                                 .(PID, owner_group_id, transfer_year = year)]
owner_unique_pids <- unique(owner_pids_temporal[, .(owner_group_id, PID)])

# Cross-join: for each transfer, get all OTHER PIDs by the same owner
cross <- merge(
  owner_pids_temporal[, .(owner_group_id, PID_transfer = PID, transfer_year)],
  owner_unique_pids[, .(owner_group_id, PID_other = PID)],
  by = "owner_group_id", allow.cartesian = TRUE
)
cross <- cross[PID_transfer != PID_other]

if (nrow(cross) > 0) {
  # Merge with bldg_annual: filing rates for other PIDs in years < transfer_year
  cross <- merge(cross, bldg_annual,
                 by.x = "PID_other", by.y = "PID",
                 allow.cartesian = TRUE)
  cross <- cross[year < transfer_year & !is.na(annual_filing_rate)]

  # Aggregate: mean filing rate and n_other per transfer
  temporal_agg <- cross[, .(
    acq_rate = mean(annual_filing_rate, na.rm = TRUE),
    n_other = uniqueN(PID_other)
  ), by = .(owner_group_id, PID_transfer, transfer_year)]
  setnames(temporal_agg, "PID_transfer", "PID")
} else {
  temporal_agg <- data.table(owner_group_id = character(), PID = character(),
                              transfer_year = integer(),
                              acq_rate = numeric(), n_other = integer())
}

# Merge back: transfers without other PIDs get NA (-> Single-purchase)
acq_computed <- merge(owner_pids_temporal, temporal_agg,
                      by = c("owner_group_id", "PID", "transfer_year"),
                      all.x = TRUE)
acq_computed[is.na(n_other), n_other := 0L]

# Classify via median split of positive pre-acq rates
acq_med <- acq_computed[!is.na(acq_rate) & acq_rate > 0,
                         median(acq_rate, na.rm = TRUE)]
logf("  Acquirer pre-acq filing rate median (among filers with portfolio): ",
     round(acq_med, 5), log_file = log_file)

acq_computed[, acq_filer_bin := fcase(
  is.na(acq_rate) | n_other <= 0L, "Single-purchase",
  acq_rate == 0, "Non-filer (has portfolio)",
  acq_rate <= acq_med, "Low-filer portfolio",
  acq_rate > acq_med, "High-filer portfolio",
  default = NA_character_
)]

acq_lookup <- acq_computed[, .(PID, owner_group_id, transfer_year,
                                acq_filer_bin, acq_rate, n_other)]

logf("  Acquirer bin distribution:", log_file = log_file)
for (ab in c("High-filer portfolio", "Low-filer portfolio",
             "Non-filer (has portfolio)", "Single-purchase")) {
  logf("    ", ab, ": ", acq_lookup[acq_filer_bin == ab, .N], " transfers",
       log_file = log_file)
}

# Merge onto event_panel
if (nrow(acq_lookup) > 0) {
  event_panel <- merge(event_panel,
                       acq_lookup[, .(PID, transfer_year,
                                      acq_filer_bin, acq_rate, n_other)],
                       by = c("PID", "transfer_year"), all.x = TRUE)
} else {
  event_panel[, acq_filer_bin := NA_character_]
  event_panel[, acq_rate := NA_real_]
  event_panel[, n_other := NA_integer_]
}
logf("  Acquirer classification coverage: ",
     event_panel[!is.na(acq_filer_bin) & event_time == 0L, .N], " / ",
     event_panel[event_time == 0L, .N], " transfers at t=0",
     log_file = log_file)

# Flag overlapping event windows (multiple transfers for same PID)
pid_transfer_counts <- transfers[, .(n_transfers_pid = .N), by = PID]
event_panel <- merge(event_panel, pid_transfer_counts, by = "PID", all.x = TRUE)
event_panel[, has_overlap := n_transfers_pid > 1L]
logf("  PIDs with multiple transfers (overlapping windows): ",
     event_panel[has_overlap == TRUE, uniqueN(PID)], " / ",
     event_panel[, uniqueN(PID)], log_file = log_file)

# Assertions
assert_unique(event_panel, c("PID", "transfer_year", "event_time"),
              name = "event_panel")

# Define subsamples
event_panel[, large_building := total_units >= 5]

# Known-rental flag: ever had rental license, Altos listing, or eviction filing
# For selection concern: corporate transfers may bring owner-occupied onto rental market
if ("ever_rental_any_year_ever" %in% names(event_panel)) {
  event_panel[, known_rental := ever_rental_any_year_ever == TRUE]
} else if ("ever_rental_any_year" %in% names(event_panel)) {
  event_panel[, known_rental := ever_rental_any_year == TRUE]
} else {
  event_panel[, known_rental := FALSE]  # fallback
}

# Rental market entry: indicator for entering rental license or Altos in post-transfer period
# Only meaningful for year >= 2011 (rental license coverage starts ~2013, Altos ~2006)
if ("rental_from_license" %in% names(event_panel)) {
  event_panel[, entered_license := as.integer(rental_from_license == TRUE)]
}
if ("rental_from_altos" %in% names(event_panel)) {
  event_panel[, entered_altos := as.integer(rental_from_altos == TRUE)]
}
# Combined: any rental evidence this year
event_panel[, has_rental_evidence := as.integer(
  (rental_from_license == TRUE) | (rental_from_altos == TRUE) | (rental_from_evict == TRUE)
)]

logf("  Large buildings (5+ units): ",
     event_panel[large_building == TRUE & event_time == 0, .N], " transfers",
     log_file = log_file)
logf("  Known rentals: ",
     event_panel[known_rental == TRUE & event_time == 0, .N], " / ",
     event_panel[event_time == 0, .N], " transfers (",
     round(100 * event_panel[known_rental == TRUE & event_time == 0, .N] /
             event_panel[event_time == 0, .N], 1), "%)",
     log_file = log_file)

# ============================================================
# SECTION 2: Descriptive Event Study Plots (raw means)
# ============================================================
logf("--- SECTION 2: Descriptive event study plots ---", log_file = log_file)

plot_raw_event_study <- function(dt, group_var = NULL, title, filename,
                                ylab = "Filing Rate (per unit)") {
  if (is.null(group_var)) {
    means <- dt[!is.na(filing_rate), .(
      mean_rate = mean(filing_rate, na.rm = TRUE),
      se = sd(filing_rate, na.rm = TRUE) / sqrt(.N),
      n = .N
    ), by = event_time]
    p <- ggplot(means, aes(x = event_time, y = mean_rate)) +
      geom_line(linewidth = 1.2, color = "steelblue") +
      geom_point(size = 3, color = "steelblue") +
      geom_ribbon(aes(ymin = mean_rate - 1.96 * se, ymax = mean_rate + 1.96 * se),
                  alpha = 0.15, fill = "steelblue")
  } else {
    means <- dt[!is.na(filing_rate) & !is.na(get(group_var)), .(
      mean_rate = mean(filing_rate, na.rm = TRUE),
      se = sd(filing_rate, na.rm = TRUE) / sqrt(.N),
      n = .N
    ), by = c("event_time", group_var)]
    setnames(means, group_var, "group")
    p <- ggplot(means, aes(x = event_time, y = mean_rate, color = group, fill = group)) +
      geom_line(linewidth = 1.2) +
      geom_point(size = 3) +
      geom_ribbon(aes(ymin = mean_rate - 1.96 * se, ymax = mean_rate + 1.96 * se),
                  alpha = 0.15, color = NA)
  }
  p <- p +
    geom_vline(xintercept = 0, linetype = "dashed", color = "gray40") +
    scale_x_continuous(breaks = -3:5) +
    labs(title = title,
         subtitle = "Event time: 0 = year of sale",
         x = "Years Relative to Transfer", y = ylab,
         color = "", fill = "") +
    theme_philly_evict()
  ggsave(file.path(fig_dir, filename), p, width = 12, height = 8, dpi = 150)
  logf("  Saved ", filename, log_file = log_file)
}

# By building size (create label before subsetting)
event_panel[, size_label := fifelse(large_building == TRUE, "5+ Units", "< 5 Units")]

# Pre-COVID restriction for raw means
raw_dt <- event_panel[year <= 2019L]

# Full sample
plot_raw_event_study(raw_dt, NULL,
                     "Eviction Filing Rate Around Ownership Transfer",
                     "rtt_eviction_event_study_raw.png")

# By buyer type
plot_raw_event_study(raw_dt, "buyer_type",
                     "Eviction Filing Rate Around Transfer: by Buyer Type",
                     "rtt_eviction_event_study_raw_by_buyer.png")

# By portfolio bin
plot_raw_event_study(raw_dt[!is.na(portfolio_bin)], "portfolio_bin",
                     "Eviction Filing Rate Around Transfer: by Portfolio Size",
                     "rtt_eviction_event_study_raw_by_portfolio.png")

# By building size
plot_raw_event_study(raw_dt[!is.na(size_label)], "size_label",
                     "Eviction Filing Rate Around Transfer: by Building Size",
                     "rtt_eviction_event_study_raw_by_size.png")

# ============================================================
# SECTION 3: Baseline Regression Event Study
# ============================================================
logf("--- SECTION 3: Baseline regression event study ---", log_file = log_file)

# Prep regression data (pre-COVID: outcome years <= 2019)
reg_dt <- event_panel[!is.na(filing_rate) & year <= 2019L]
reg_dt[, event_time_fac := as.factor(event_time)]
has_geoid <- "GEOID" %in% names(reg_dt) && reg_dt[!is.na(GEOID), .N] > 0

baseline_coefs <- list()

# Spec 1: Full sample, building + year FE
logf("  Spec 1: Full sample, PID + year FE...", log_file = log_file)
fit1 <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
              data = reg_dt, cluster = ~PID)
c1 <- extract_feols_coefs(fit1, "full_pid_year")
logf("    N=", fit1$nobs, ", adj-R2=", round(fixest::r2(fit1, "ar2"), 4),
     log_file = log_file)
baseline_coefs[[1]] <- c1

# Spec 2: Full sample, building + GEOID x year FE
if (has_geoid) {
  logf("  Spec 2: Full sample, PID + GEOID x year FE...", log_file = log_file)
  fit2 <- feols(filing_rate ~ i(event_time, ref = -1) | PID + GEOID^year,
                data = reg_dt[!is.na(GEOID)], cluster = ~PID)
  c2 <- extract_feols_coefs(fit2, "full_pid_bgxyear")
  logf("    N=", fit2$nobs, ", adj-R2=", round(fixest::r2(fit2, "ar2"), 4),
       log_file = log_file)
  baseline_coefs[[2]] <- c2
}

# Spec 3: Large buildings, building + year FE
logf("  Spec 3: Large buildings, PID + year FE...", log_file = log_file)
fit3 <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
              data = reg_dt[large_building == TRUE], cluster = ~PID)
c3 <- extract_feols_coefs(fit3, "large_pid_year")
logf("    N=", fit3$nobs, ", adj-R2=", round(fixest::r2(fit3, "ar2"), 4),
     log_file = log_file)
baseline_coefs[[3]] <- c3

# Spec 4: Large buildings, building + GEOID x year FE
if (has_geoid) {
  logf("  Spec 4: Large buildings, PID + GEOID x year FE...", log_file = log_file)
  fit4 <- feols(filing_rate ~ i(event_time, ref = -1) | PID + GEOID^year,
                data = reg_dt[large_building == TRUE & !is.na(GEOID)], cluster = ~PID)
  c4 <- extract_feols_coefs(fit4, "large_pid_bgxyear")
  logf("    N=", fit4$nobs, ", adj-R2=", round(fixest::r2(fit4, "ar2"), 4),
       log_file = log_file)
  baseline_coefs[[4]] <- c4
}

# Spec 5: Known rentals only (ever had license/Altos/eviction), PID + year FE
logf("  Spec 5: Known rentals only, PID + year FE...", log_file = log_file)
fit5 <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
              data = reg_dt[known_rental == TRUE], cluster = ~PID)
c5 <- extract_feols_coefs(fit5, "known_rental_pid_year")
logf("    N=", fit5$nobs, ", adj-R2=", round(fixest::r2(fit5, "ar2"), 4),
     log_file = log_file)
baseline_coefs[[5]] <- c5

# Spec 6: Known rentals + large buildings (5+ units) — most conservative sample
logf("  Spec 6: Known rentals + 5+ units, PID + year FE...", log_file = log_file)
fit6 <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
              data = reg_dt[known_rental == TRUE & large_building == TRUE],
              cluster = ~PID)
c6 <- extract_feols_coefs(fit6, "known_rental_large_pid_year")
logf("    N=", fit6$nobs, ", adj-R2=", round(fixest::r2(fit6, "ar2"), 4),
     log_file = log_file)
baseline_coefs[[6]] <- c6

# Spec 7: 2+ unit buildings, PID + year FE
logf("  Spec 7: 2+ unit buildings, PID + year FE...", log_file = log_file)
fit7 <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
              data = reg_dt[!is.na(total_units) & total_units >= 2], cluster = ~PID)
c7 <- extract_feols_coefs(fit7, "2plus_pid_year")
logf("    N=", fit7$nobs, ", adj-R2=", round(fixest::r2(fit7, "ar2"), 4),
     log_file = log_file)
baseline_coefs[[7]] <- c7

# Write baseline coefficients
baseline_all <- rbindlist(baseline_coefs)
fwrite(baseline_all, file.path(out_dir, "transfer_evictions_baseline_coefs.csv"))
logf("  Wrote transfer_evictions_baseline_coefs.csv", log_file = log_file)

# Baseline coefficient plot
plot_event_coefs <- function(coef_dt, specs, title, filename, facet = FALSE) {
  plot_dt <- coef_dt[spec %in% specs & grepl("event_time::", term)]
  # Clean spec labels, then add N
  n_lookup <- unique(coef_dt[spec %in% specs, .(spec, n)])
  n_lookup <- n_lookup[, .(n = n[1]), by = spec]
  n_lookup[, clean_label := clean_spec_labels(spec)]
  plot_dt <- merge(plot_dt, n_lookup, by = "spec", all.x = TRUE, suffixes = c("", "_lkp"))
  spec_labels <- plot_dt[, .(spec_label = paste0(clean_label, " (N=", formatC(n_lkp, format = "d", big.mark = ","), ")")),
                         by = spec][, unique(spec_label)]
  plot_dt[, spec := paste0(clean_label, " (N=", formatC(n_lkp, format = "d", big.mark = ","), ")")]
  plot_dt[, et := as.integer(str_extract(term, "-?\\d+"))]
  # Add reference point at -1
  ref_rows <- data.table(
    et = -1L, estimate = 0, std_error = 0,
    spec = spec_labels
  )
  plot_dt <- rbindlist(list(
    plot_dt[, .(et, estimate, std_error, spec)],
    ref_rows
  ), fill = TRUE)
  setorder(plot_dt, spec, et)

  if (facet) {
    p <- ggplot(plot_dt, aes(x = et, y = estimate)) +
      geom_hline(yintercept = 0, color = "gray60") +
      geom_vline(xintercept = -0.5, linetype = "dashed", color = "gray40") +
      geom_point(size = 2, color = "steelblue") +
      geom_errorbar(aes(ymin = estimate - 1.96 * std_error,
                        ymax = estimate + 1.96 * std_error),
                    width = 0.2, color = "steelblue") +
      facet_wrap(~spec, scales = "fixed") +
      scale_x_continuous(breaks = -3:5) +
      labs(title = title, subtitle = "Reference period: t = -1",
           x = "Years Relative to Transfer",
           y = "Filing Rate (per unit)") +
      theme_philly_evict()
  } else {
    p <- ggplot(plot_dt, aes(x = et, y = estimate, color = spec)) +
      geom_hline(yintercept = 0, color = "gray60") +
      geom_vline(xintercept = -0.5, linetype = "dashed", color = "gray40") +
      geom_point(position = position_dodge(width = 0.3), size = 2.5) +
      geom_errorbar(aes(ymin = estimate - 1.96 * std_error,
                        ymax = estimate + 1.96 * std_error),
                    position = position_dodge(width = 0.3), width = 0.2) +
      scale_x_continuous(breaks = -3:5) +
      labs(title = title, subtitle = "Reference period: t = -1",
           x = "Years Relative to Transfer",
           y = "Filing Rate (per unit)", color = "Specification") +
      theme_philly_evict()
  }
  ggsave(file.path(fig_dir, filename), p, width = 12, height = 8, dpi = 150)
  logf("  Saved ", filename, log_file = log_file)
}

baseline_specs <- unique(baseline_all$spec)
plot_event_coefs(baseline_all, baseline_specs,
                 "Event Study: Eviction Filing Rate Around Transfer",
                 "rtt_eviction_event_study_baseline.png")

# LaTeX table
if (has_geoid) {
  baseline_fits <- list(fit1, fit2, fit3, fit4, fit5, fit6, fit7)
  names(baseline_fits) <- c("Full: Year FE", "Full: BG×Year",
                             "Large: Year FE", "Large: BG×Year",
                             "Known Rental", "Known Rental 5+", "2+ Units")
} else {
  baseline_fits <- list(fit1, fit3, fit5, fit6, fit7)
  names(baseline_fits) <- c("Full: Year FE", "Large: Year FE",
                             "Known Rental", "Known Rental 5+", "2+ Units")
}
etable(baseline_fits,
       file = file.path(out_dir, "transfer_evictions_baseline_etable.tex"),
       style.tex = style.tex("aer"))
logf("  Wrote transfer_evictions_baseline_etable.tex", log_file = log_file)

# ============================================================
# SECTION 4: Heterogeneity by Buyer Type
# ============================================================
logf("--- SECTION 4: Buyer type heterogeneity ---", log_file = log_file)

buyer_coefs <- list()

# A) Interaction model: full sample
logf("  Interaction model: full sample...", log_file = log_file)
fit_buyer_int <- feols(
  filing_rate ~ i(event_time, i.is_corp, ref = -1) | PID + year,
  data = reg_dt[!is.na(is_corp)], cluster = ~PID
)
c_buyer_int <- extract_feols_coefs(fit_buyer_int, "interaction_full")
logf("    N=", fit_buyer_int$nobs, log_file = log_file)
buyer_coefs[[1]] <- c_buyer_int

# B) Split-sample: Corporate buyers
logf("  Split: Corporate buyers...", log_file = log_file)
fit_corp <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                  data = reg_dt[is_corp == TRUE], cluster = ~PID)
c_corp <- extract_feols_coefs(fit_corp, "corporate")
logf("    N=", fit_corp$nobs, log_file = log_file)
buyer_coefs[[2]] <- c_corp

# C) Split-sample: Individual buyers
logf("  Split: Individual buyers...", log_file = log_file)
fit_indiv <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                   data = reg_dt[is_corp == FALSE], cluster = ~PID)
c_indiv <- extract_feols_coefs(fit_indiv, "individual")
logf("    N=", fit_indiv$nobs, log_file = log_file)
buyer_coefs[[3]] <- c_indiv

# Large buildings: split
logf("  Split (large bldgs): Corporate...", log_file = log_file)
fit_corp_lg <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                     data = reg_dt[is_corp == TRUE & large_building == TRUE],
                     cluster = ~PID)
c_corp_lg <- extract_feols_coefs(fit_corp_lg, "corporate_large")
logf("    N=", fit_corp_lg$nobs, log_file = log_file)
buyer_coefs[[4]] <- c_corp_lg

logf("  Split (large bldgs): Individual...", log_file = log_file)
fit_indiv_lg <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                      data = reg_dt[is_corp == FALSE & large_building == TRUE],
                      cluster = ~PID)
c_indiv_lg <- extract_feols_coefs(fit_indiv_lg, "individual_large")
logf("    N=", fit_indiv_lg$nobs, log_file = log_file)
buyer_coefs[[5]] <- c_indiv_lg

buyer_all <- rbindlist(buyer_coefs)
fwrite(buyer_all, file.path(out_dir, "transfer_evictions_buyer_type_coefs.csv"))
logf("  Wrote transfer_evictions_buyer_type_coefs.csv", log_file = log_file)

# Buyer type coefficient plot
plot_event_coefs(buyer_all, c("corporate", "individual"),
                 "Event Study by Buyer Type: Corporate vs Individual",
                 "rtt_eviction_event_study_by_buyer.png")

# LaTeX
etable(list("Corporate" = fit_corp, "Individual" = fit_indiv,
            "Corp (5+)" = fit_corp_lg, "Indiv (5+)" = fit_indiv_lg),
       file = file.path(out_dir, "transfer_evictions_buyer_type_etable.tex"),
       style.tex = style.tex("aer"))
logf("  Wrote transfer_evictions_buyer_type_etable.tex", log_file = log_file)

# ============================================================
# SECTION 4b: Heterogeneity by Building Size
# ============================================================
logf("--- SECTION 4b: Building size heterogeneity ---", log_file = log_file)

# Define size bins based on total_units
reg_dt[, size_bin := fcase(
  total_units == 1L, "1 unit",
  total_units %in% 2:4, "2-4 units",
  total_units %in% 5:9, "5-9 units",
  total_units %in% 10:19, "10-19 units",
  total_units >= 20, "20+ units",
  default = NA_character_
)]

size_coefs <- list()
size_fits <- list()
size_bins_ordered <- c("1 unit", "2-4 units", "5-9 units", "10-19 units", "20+ units")

for (sb in size_bins_ordered) {
  sub_dt <- reg_dt[size_bin == sb]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  Size bin '", sb, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

  if (n_sub < 100 || n_pids < 10) {
    logf("    WARNING: too few observations, skipping regression", log_file = log_file)
    next
  }

  fit_sb <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                  data = sub_dt, cluster = ~PID)
  c_sb <- extract_feols_coefs(fit_sb, paste0("size_", sb))
  size_coefs[[length(size_coefs) + 1]] <- c_sb
  size_fits[[sb]] <- fit_sb
  logf("    N=", fit_sb$nobs, ", adj-R2=", round(fixest::r2(fit_sb, "ar2"), 4),
       log_file = log_file)
}

if (length(size_coefs) > 0) {
  size_all <- rbindlist(size_coefs)
  fwrite(size_all, file.path(out_dir, "transfer_evictions_size_coefs.csv"))
  logf("  Wrote transfer_evictions_size_coefs.csv", log_file = log_file)

  plot_event_coefs(size_all, unique(size_all$spec),
                   "Event Study by Building Size",
                   "rtt_eviction_event_study_by_size_bins.png")

  if (length(size_fits) > 0) {
    etable(size_fits,
           file = file.path(out_dir, "transfer_evictions_size_etable.tex"),
           style.tex = style.tex("aer"))
    logf("  Wrote transfer_evictions_size_etable.tex", log_file = log_file)
  }
}

# Also: corporate buyer effect by building size (is the corp effect only in small bldgs?)
corp_size_coefs <- list()
corp_size_fits <- list()

for (sb in size_bins_ordered) {
  sub_dt_corp <- reg_dt[size_bin == sb & is_corp == TRUE]
  sub_dt_indiv <- reg_dt[size_bin == sb & is_corp == FALSE]

  for (buyer_label in c("corp", "indiv")) {
    sub <- if (buyer_label == "corp") sub_dt_corp else sub_dt_indiv
    n_sub <- nrow(sub)
    n_pids <- sub[, uniqueN(PID)]
    spec_name <- paste0(buyer_label, "_", sb)

    if (n_sub < 100 || n_pids < 10) {
      logf("  Skipping ", spec_name, " (N=", n_sub, ", PIDs=", n_pids, ")",
           log_file = log_file)
      next
    }

    fit_cs <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                    data = sub, cluster = ~PID)
    c_cs <- extract_feols_coefs(fit_cs, spec_name)
    corp_size_coefs[[length(corp_size_coefs) + 1]] <- c_cs
    corp_size_fits[[spec_name]] <- fit_cs
    logf("  ", spec_name, ": N=", fit_cs$nobs, log_file = log_file)
  }
}

if (length(corp_size_coefs) > 0) {
  corp_size_all <- rbindlist(corp_size_coefs)
  fwrite(corp_size_all, file.path(out_dir, "transfer_evictions_corp_x_size_coefs.csv"))
  logf("  Wrote transfer_evictions_corp_x_size_coefs.csv", log_file = log_file)
}

# ============================================================
# SECTION 4c: Heterogeneity by Acquirer Filing Rate
# ============================================================
logf("--- SECTION 4c: Acquirer filing rate heterogeneity ---", log_file = log_file)

# acq_filer_bin is already on reg_dt via event_panel merge (pre-acquisition temporal)
acq_bins_ordered <- c("High-filer portfolio", "Low-filer portfolio",
                      "Non-filer (has portfolio)", "Single-purchase")

# Log acquirer bin distribution at t=0
logf("  Acquirer bin distribution (at t=0):", log_file = log_file)
for (ab in acq_bins_ordered) {
  logf("    ", ab, ": ",
       reg_dt[acq_filer_bin == ab & event_time == 0L, .N], " transfers",
       log_file = log_file)
}

acq_coefs <- list()
acq_fits <- list()

for (ab in acq_bins_ordered) {
  sub_dt <- reg_dt[acq_filer_bin == ab]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  Acquirer bin '", ab, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

  if (n_sub < 100 || n_pids < 10) next

  fit_ab <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                  data = sub_dt, cluster = ~PID)
  spec_name <- paste0("full_", gsub("[^a-z_]", "", gsub(" ", "_", tolower(ab))))
  acq_coefs[[length(acq_coefs) + 1]] <- extract_feols_coefs(fit_ab, spec_name)
  acq_fits[[ab]] <- fit_ab
  logf("    N=", fit_ab$nobs, ", adj-R2=", round(fixest::r2(fit_ab, "ar2"), 4),
       log_file = log_file)
}

if (length(acq_coefs) > 0) {
  acq_all <- rbindlist(acq_coefs)
  fwrite(acq_all, file.path(out_dir, "transfer_evictions_acq_filer_coefs.csv"))
  logf("  Wrote transfer_evictions_acq_filer_coefs.csv", log_file = log_file)

  plot_event_coefs(acq_all, unique(acq_all$spec),
                   "Event Study by Acquirer Filing Rate (Pre-Acquisition)",
                   "rtt_eviction_event_study_by_acq_filer.png",
                   facet = TRUE)

  if (length(acq_fits) > 0) {
    etable(acq_fits,
           file = file.path(out_dir, "transfer_evictions_acq_filer_etable.tex"),
           style.tex = style.tex("aer"))
    logf("  Wrote transfer_evictions_acq_filer_etable.tex", log_file = log_file)
  }
}

# ============================================================
# SECTION 4d: Rental Market Entry (Selection Concern)
# ============================================================
logf("--- SECTION 4d: Rental market entry around transfer ---", log_file = log_file)

# Check if corporate transfers bring properties onto the rental market
# Restrict to year >= 2011 for license coverage
entry_dt <- event_panel[!is.na(has_rental_evidence) & year >= 2011]
logf("  Entry analysis sample (year >= 2011): ", nrow(entry_dt), " rows",
     log_file = log_file)

entry_coefs <- list()

# Rental license entry around transfer
if ("entered_license" %in% names(entry_dt) && entry_dt[!is.na(entered_license), .N] > 100) {
  logf("  Rental license entry: full sample...", log_file = log_file)
  fit_lic <- feols(entered_license ~ i(event_time, ref = -1) | PID + year,
                   data = entry_dt[!is.na(entered_license)], cluster = ~PID)
  entry_coefs[[1]] <- extract_feols_coefs(fit_lic, "license_entry_full")
  logf("    N=", fit_lic$nobs, log_file = log_file)

  # By buyer type
  fit_lic_corp <- feols(entered_license ~ i(event_time, ref = -1) | PID + year,
                        data = entry_dt[is_corp == TRUE & !is.na(entered_license)],
                        cluster = ~PID)
  entry_coefs[[2]] <- extract_feols_coefs(fit_lic_corp, "license_entry_corp")

  fit_lic_indiv <- feols(entered_license ~ i(event_time, ref = -1) | PID + year,
                         data = entry_dt[is_corp == FALSE & !is.na(entered_license)],
                         cluster = ~PID)
  entry_coefs[[3]] <- extract_feols_coefs(fit_lic_indiv, "license_entry_indiv")
}

# Altos listing entry around transfer
if ("entered_altos" %in% names(entry_dt) && entry_dt[!is.na(entered_altos), .N] > 100) {
  logf("  Altos listing entry: full sample...", log_file = log_file)
  fit_alt <- feols(entered_altos ~ i(event_time, ref = -1) | PID + year,
                   data = entry_dt[!is.na(entered_altos)], cluster = ~PID)
  entry_coefs[[length(entry_coefs) + 1]] <- extract_feols_coefs(fit_alt, "altos_entry_full")
  logf("    N=", fit_alt$nobs, log_file = log_file)
}

# Any rental evidence entry
logf("  Any rental evidence entry...", log_file = log_file)
fit_any <- feols(has_rental_evidence ~ i(event_time, ref = -1) | PID + year,
                 data = entry_dt, cluster = ~PID)
entry_coefs[[length(entry_coefs) + 1]] <- extract_feols_coefs(fit_any, "any_rental_entry_full")
logf("    N=", fit_any$nobs, log_file = log_file)

# By buyer type for any rental evidence
fit_any_corp <- feols(has_rental_evidence ~ i(event_time, ref = -1) | PID + year,
                      data = entry_dt[is_corp == TRUE], cluster = ~PID)
entry_coefs[[length(entry_coefs) + 1]] <- extract_feols_coefs(fit_any_corp, "any_rental_entry_corp")

fit_any_indiv <- feols(has_rental_evidence ~ i(event_time, ref = -1) | PID + year,
                       data = entry_dt[is_corp == FALSE], cluster = ~PID)
entry_coefs[[length(entry_coefs) + 1]] <- extract_feols_coefs(fit_any_indiv, "any_rental_entry_indiv")

if (length(entry_coefs) > 0) {
  entry_all <- rbindlist(entry_coefs)
  fwrite(entry_all, file.path(out_dir, "transfer_evictions_rental_entry_coefs.csv"))
  logf("  Wrote transfer_evictions_rental_entry_coefs.csv", log_file = log_file)

  # Plot: license entry by buyer type
  lic_specs <- grep("license_entry", unique(entry_all$spec), value = TRUE)
  if (length(lic_specs) > 0) {
    plot_event_coefs(entry_all, lic_specs,
                     "Rental License Entry Around Transfer (year >= 2011)",
                     "rtt_rental_license_entry.png")
  }

  # Plot: any rental entry by buyer type
  any_specs <- grep("any_rental_entry", unique(entry_all$spec), value = TRUE)
  if (length(any_specs) > 0) {
    plot_event_coefs(entry_all, any_specs,
                     "Any Rental Evidence Entry Around Transfer (year >= 2011)",
                     "rtt_any_rental_entry.png")
  }
}

# ============================================================
# SECTION 5: Heterogeneity by Portfolio Size
# ============================================================
logf("--- SECTION 5: Portfolio size heterogeneity ---", log_file = log_file)

portfolio_coefs <- list()
portfolio_fits <- list()

portfolio_bins <- c("Single-purchase", "2-4", "5-9", "10+")

for (pb in portfolio_bins) {
  sub_dt <- reg_dt[portfolio_bin == pb]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  Portfolio bin '", pb, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

  if (n_sub < 100 || n_pids < 10) {
    logf("    WARNING: too few observations, skipping regression", log_file = log_file)
    next
  }

  fit_pb <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                  data = sub_dt, cluster = ~PID)
  c_pb <- extract_feols_coefs(fit_pb, paste0("portfolio_", pb))
  portfolio_coefs[[length(portfolio_coefs) + 1]] <- c_pb
  portfolio_fits[[pb]] <- fit_pb
}

if (length(portfolio_coefs) > 0) {
  portfolio_all <- rbindlist(portfolio_coefs)
  fwrite(portfolio_all, file.path(out_dir, "transfer_evictions_portfolio_coefs.csv"))
  logf("  Wrote transfer_evictions_portfolio_coefs.csv", log_file = log_file)

  plot_event_coefs(portfolio_all, unique(portfolio_all$spec),
                   "Event Study by Portfolio Size",
                   "rtt_eviction_event_study_by_portfolio.png")

  if (length(portfolio_fits) > 0) {
    etable(portfolio_fits,
           file = file.path(out_dir, "transfer_evictions_portfolio_etable.tex"),
           style.tex = style.tex("aer"))
    logf("  Wrote transfer_evictions_portfolio_etable.tex", log_file = log_file)
  }
}

# ============================================================
# SECTION 6: Robustness
# ============================================================
logf("--- SECTION 6: Robustness checks ---", log_file = log_file)

robustness_coefs <- list()

# 6a: No sheriff's deeds
logf("  Robustness: No sheriff's deeds...", log_file = log_file)
fit_no_sheriff <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                        data = reg_dt[is_sheriff_deed == FALSE], cluster = ~PID)
robustness_coefs[[1]] <- extract_feols_coefs(fit_no_sheriff, "no_sheriff")
logf("    N=", fit_no_sheriff$nobs, log_file = log_file)

# 6b: First transfer only (per PID), proper event study panel
# Uses ALL years from the building panel for each PID, not just the event window.
# This breaks the collinearity between event_time dummies and PID + year FE.
# PIDs with only one transfer get event_time dummies in [-3,5]; all other years
# serve as the "far from event" comparison periods.
logf("  Robustness: Single-transfer PIDs (full-panel event study)...", log_file = log_file)
single_transfer_pids <- pid_transfer_counts[n_transfers_pid == 1L, PID]
logf("    Single-transfer PIDs: ", length(single_transfer_pids), log_file = log_file)

# Build full-panel: all years from bldg for these PIDs
bldg_single <- bldg_sub[PID %chin% single_transfer_pids]
bldg_single[, filing_rate := fifelse(
  !is.na(num_filings) & !is.na(total_units) & total_units > 0,
  num_filings / total_units, NA_real_
)]

# Merge transfer year
single_transfers <- transfers[PID %chin% single_transfer_pids,
                               .(PID, transfer_year = year)]
single_transfers <- single_transfers[, .SD[1], by = PID]  # safety
bldg_single <- merge(bldg_single, single_transfers, by = "PID", all.x = TRUE)
bldg_single[, event_time := year - transfer_year]
# Trim event_time to [-3, 5]; other periods are comparison
bldg_single[, event_time_trim := fifelse(
  event_time >= -3L & event_time <= 5L, event_time, NA_integer_
)]

# Create explicit dummy variables so periods outside the event window
# remain in the regression (contributing to PID and year FE estimation)
# but don't get their own event_time coefficients.
for (k in c(-3L, -2L, 0L, 1L, 2L, 3L, 4L, 5L)) {
  col_name <- paste0("et_", ifelse(k < 0, paste0("m", abs(k)), k))
  bldg_single[, (col_name) := as.integer(event_time == k)]
  # Set to 0 for observations outside the event window
  bldg_single[is.na(event_time) | event_time < -3L | event_time > 5L,
              (col_name) := 0L]
}

bldg_single_reg <- bldg_single[!is.na(filing_rate) & year <= 2019L]
logf("    Full-panel rows: ", nrow(bldg_single_reg),
     " (", bldg_single_reg[, uniqueN(PID)], " PIDs)",
     log_file = log_file)
logf("    In-window rows: ", bldg_single_reg[event_time >= -3L & event_time <= 5L, .N],
     ", out-of-window rows: ",
     bldg_single_reg[is.na(event_time) | event_time < -3L | event_time > 5L, .N],
     log_file = log_file)

if (nrow(bldg_single_reg) > 100) {
  fit_single <- feols(
    filing_rate ~ et_m3 + et_m2 + et_0 + et_1 + et_2 + et_3 + et_4 + et_5 |
      PID + year,
    data = bldg_single_reg, cluster = ~PID
  )
  c_single <- extract_feols_coefs(fit_single, "single_transfer")
  # Rename terms for consistency with other specs
  c_single[, term := gsub("^et_m", "event_time::-", term)]
  c_single[, term := gsub("^et_", "event_time::", term)]
  robustness_coefs[[2]] <- c_single
  logf("    N=", fit_single$nobs, ", adj-R2=",
       round(fixest::r2(fit_single, "ar2"), 4), log_file = log_file)
}

# 6c: Count outcome (num_filings)
logf("  Robustness: Count outcome (num_filings)...", log_file = log_file)
fit_count <- feols(num_filings ~ i(event_time, ref = -1) | PID + year,
                   data = reg_dt[!is.na(num_filings)], cluster = ~PID)
robustness_coefs[[length(robustness_coefs) + 1]] <- extract_feols_coefs(fit_count, "count_outcome")
logf("    N=", fit_count$nobs, log_file = log_file)

# 6d: Violations outcome
if ("total_violations" %in% names(reg_dt)) {
  logf("  Robustness: Violations outcome...", log_file = log_file)
  fit_viol <- feols(total_violations ~ i(event_time, ref = -1) | PID + year,
                    data = reg_dt[!is.na(total_violations)], cluster = ~PID)
  robustness_coefs[[length(robustness_coefs) + 1]] <- extract_feols_coefs(fit_viol, "violations")
  logf("    N=", fit_viol$nobs, log_file = log_file)
}

# 6e: Pre-COVID only — REMOVED (now the main sample is pre-COVID by default)

# Combine robustness
robust_all <- rbindlist(robustness_coefs)
fwrite(robust_all, file.path(out_dir, "transfer_evictions_robustness_coefs.csv"))
logf("  Wrote transfer_evictions_robustness_coefs.csv", log_file = log_file)

# Robustness comparison plot (faceted)
robust_plot_dt <- robust_all[grepl("event_time::", term)]
# Add N to spec labels
n_lookup_r <- unique(robust_all[, .(spec, n)])[, .(n = n[1]), by = spec]
robust_plot_dt <- merge(robust_plot_dt, n_lookup_r, by = "spec", all.x = TRUE, suffixes = c("", "_lkp"))
spec_labels_r <- robust_plot_dt[, .(spec_label = paste0(spec, " (N=", formatC(n_lkp, format = "d", big.mark = ","), ")")),
                                by = spec][, unique(spec_label)]
robust_plot_dt[, spec := paste0(spec, " (N=", formatC(n_lkp, format = "d", big.mark = ","), ")")]
robust_plot_dt[, et := as.integer(str_extract(term, "-?\\d+"))]
# Add reference points
ref_rows <- data.table(
  et = -1L, estimate = 0, std_error = 0,
  spec = spec_labels_r
)
robust_plot_dt <- rbindlist(list(
  robust_plot_dt[, .(et, estimate, std_error, spec)],
  ref_rows
), fill = TRUE)
setorder(robust_plot_dt, spec, et)

p_robust <- ggplot(robust_plot_dt, aes(x = et, y = estimate)) +
  geom_hline(yintercept = 0, color = "gray60") +
  geom_vline(xintercept = -0.5, linetype = "dashed", color = "gray40") +
  geom_point(size = 2, color = "steelblue") +
  geom_errorbar(aes(ymin = estimate - 1.96 * std_error,
                    ymax = estimate + 1.96 * std_error),
                width = 0.2, color = "steelblue") +
  facet_wrap(~spec, scales = "fixed") +
  scale_x_continuous(breaks = -3:5) +
  labs(title = "Robustness: Event Study Specifications",
       x = "Years Relative to Transfer", y = "Coefficient") +
  theme_philly_evict()
ggsave(file.path(fig_dir, "rtt_eviction_event_study_robustness.png"),
       p_robust, width = 14, height = 10, dpi = 150)
logf("  Saved rtt_eviction_event_study_robustness.png", log_file = log_file)

# ============================================================
# SECTION 6f: Violations Diagnostic
# ============================================================
logf("--- SECTION 6f: Violations diagnostic ---", log_file = log_file)

# Violations by buyer type to understand the pattern
if ("total_violations" %in% names(reg_dt)) {
  viol_diag_coefs <- list()

  # By buyer type
  fit_viol_corp <- feols(total_violations ~ i(event_time, ref = -1) | PID + year,
                         data = reg_dt[is_corp == TRUE & !is.na(total_violations)],
                         cluster = ~PID)
  viol_diag_coefs[[1]] <- extract_feols_coefs(fit_viol_corp, "violations_corp")

  fit_viol_indiv <- feols(total_violations ~ i(event_time, ref = -1) | PID + year,
                          data = reg_dt[is_corp == FALSE & !is.na(total_violations)],
                          cluster = ~PID)
  viol_diag_coefs[[2]] <- extract_feols_coefs(fit_viol_indiv, "violations_indiv")

  # Violations rate (per unit) instead of count
  reg_dt[, violations_rate := fifelse(
    !is.na(total_violations) & total_units > 0,
    total_violations / total_units, NA_real_
  )]
  fit_viol_rate <- feols(violations_rate ~ i(event_time, ref = -1) | PID + year,
                         data = reg_dt[!is.na(violations_rate)], cluster = ~PID)
  viol_diag_coefs[[3]] <- extract_feols_coefs(fit_viol_rate, "violations_rate")

  viol_diag_all <- rbindlist(viol_diag_coefs)
  fwrite(viol_diag_all, file.path(out_dir, "transfer_evictions_violations_diag_coefs.csv"))
  logf("  Wrote transfer_evictions_violations_diag_coefs.csv", log_file = log_file)

  plot_event_coefs(viol_diag_all, c("violations_corp", "violations_indiv"),
                   "Violations Around Transfer by Buyer Type",
                   "rtt_violations_by_buyer.png")
}

# ============================================================
# SECTION 6g: Pre-trend Diagnostic
# ============================================================
logf("--- SECTION 6g: Pre-trend diagnostic ---", log_file = log_file)

# Raw means by event_time to visualize the pre-trend
pretrend_means <- event_panel[!is.na(filing_rate) & year <= 2019L, .(
  mean_filing_rate = mean(filing_rate, na.rm = TRUE),
  mean_violations = mean(total_violations, na.rm = TRUE),
  mean_filings = mean(num_filings, na.rm = TRUE),
  n = .N
), by = event_time][order(event_time)]
fwrite(pretrend_means, file.path(out_dir, "transfer_evictions_raw_means_by_et.csv"))
logf("  Raw means by event_time:", log_file = log_file)
for (i in seq_len(nrow(pretrend_means))) {
  logf("    t=", pretrend_means$event_time[i],
       ": filing_rate=", round(pretrend_means$mean_filing_rate[i], 4),
       ", violations=", round(pretrend_means$mean_violations[i], 2),
       ", N=", pretrend_means$n[i], log_file = log_file)
}

# Pre-trend by buyer type
pretrend_buyer <- event_panel[!is.na(filing_rate) & !is.na(buyer_type) & year <= 2019L, .(
  mean_filing_rate = mean(filing_rate, na.rm = TRUE),
  mean_violations = mean(total_violations, na.rm = TRUE),
  n = .N
), by = .(event_time, buyer_type)][order(event_time, buyer_type)]
fwrite(pretrend_buyer, file.path(out_dir, "transfer_evictions_raw_means_by_et_buyer.csv"))
logf("  Wrote transfer_evictions_raw_means_by_et_buyer.csv", log_file = log_file)

# ============================================================
# SECTION 7: Descriptives and QA
# ============================================================
logf("--- SECTION 7: Descriptives and QA ---", log_file = log_file)

# Transfer sample descriptives
desc_by_buyer <- event_panel[event_time == 0 & !is.na(buyer_type), .(
  n_transfers = .N,
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  median_units = as.double(median(total_units, na.rm = TRUE)),
  mean_filing_rate = round(mean(filing_rate, na.rm = TRUE), 4),
  mean_violations = round(mean(total_violations, na.rm = TRUE), 2),
  median_price = as.double(median(total_consideration, na.rm = TRUE)),
  pct_sheriff = round(100 * mean(is_sheriff_deed, na.rm = TRUE), 1)
), by = buyer_type]

desc_by_portfolio <- event_panel[event_time == 0 & !is.na(portfolio_bin), .(
  n_transfers = .N,
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  median_units = as.double(median(total_units, na.rm = TRUE)),
  mean_filing_rate = round(mean(filing_rate, na.rm = TRUE), 4),
  mean_violations = round(mean(total_violations, na.rm = TRUE), 2),
  median_price = as.double(median(total_consideration, na.rm = TRUE)),
  pct_corp = round(100 * mean(is_corp, na.rm = TRUE), 1)
), by = portfolio_bin]

fwrite(rbindlist(list(
  cbind(group_var = "buyer_type", desc_by_buyer),
  cbind(group_var = "portfolio_bin", setnames(desc_by_portfolio, "portfolio_bin", "buyer_type"))
), fill = TRUE), file.path(out_dir, "transfer_evictions_descriptives.csv"))
logf("  Wrote transfer_evictions_descriptives.csv", log_file = log_file)

# Pre-transfer balance table: mean outcomes at t=-2,-1 by buyer group
pre_dt <- event_panel[event_time %in% c(-2L, -1L)]
balance_buyer <- pre_dt[!is.na(buyer_type), .(
  mean_filing_rate = round(mean(filing_rate, na.rm = TRUE), 4),
  mean_violations = round(mean(total_violations, na.rm = TRUE), 2),
  mean_complaints = round(mean(total_complaints, na.rm = TRUE), 2),
  mean_rent = round(mean(med_rent, na.rm = TRUE), 0),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  n = .N
), by = buyer_type]

balance_portfolio <- pre_dt[!is.na(portfolio_bin), .(
  mean_filing_rate = round(mean(filing_rate, na.rm = TRUE), 4),
  mean_violations = round(mean(total_violations, na.rm = TRUE), 2),
  mean_complaints = round(mean(total_complaints, na.rm = TRUE), 2),
  mean_rent = round(mean(med_rent, na.rm = TRUE), 0),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  n = .N
), by = .(buyer_type = portfolio_bin)]

fwrite(rbindlist(list(
  cbind(group_var = "buyer_type", balance_buyer),
  cbind(group_var = "portfolio_bin", balance_portfolio)
), fill = TRUE), file.path(out_dir, "transfer_evictions_balance.csv"))
logf("  Wrote transfer_evictions_balance.csv", log_file = log_file)

# Acquirer descriptives (pre-acquisition temporal classification)
acq_descriptives <- event_panel[event_time == 0L & !is.na(acq_filer_bin), .(
  n_transfers = .N,
  n_owners = uniqueN(owner_group_id),
  n_pids = uniqueN(PID),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  total_units_sum = sum(total_units, na.rm = TRUE),
  pct_corporate = round(100 * mean(is_corp == TRUE, na.rm = TRUE), 1),
  mean_portfolio = round(mean(n_properties_total, na.rm = TRUE), 1),
  median_price = round(median(total_consideration, na.rm = TRUE), 0),
  mean_acq_rate = round(mean(acq_rate, na.rm = TRUE), 5),
  mean_n_other = round(mean(n_other, na.rm = TRUE), 1)
), by = acq_filer_bin]
acq_descriptives[, pct_of_transfers := round(100 * n_transfers / sum(n_transfers), 1)]
acq_descriptives[, pct_of_units := round(100 * total_units_sum / sum(total_units_sum), 1)]
fwrite(acq_descriptives,
       file.path(out_dir, "transfer_evictions_acq_descriptives.csv"))
logf("  Wrote transfer_evictions_acq_descriptives.csv", log_file = log_file)

# Corporate / non-corporate within each acquirer filing bin
corp_by_filer_bin <- event_panel[event_time == 0L & !is.na(acq_filer_bin), .(
  n_transfers = .N,
  n_owners = uniqueN(owner_group_id),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  total_units_sum = sum(total_units, na.rm = TRUE),
  mean_portfolio = round(mean(n_properties_total, na.rm = TRUE), 1),
  mean_acq_rate = round(mean(acq_rate, na.rm = TRUE), 5),
  mean_n_other = round(mean(n_other, na.rm = TRUE), 1)
), by = .(acq_filer_bin, is_corp)]
corp_by_filer_bin[, pct_of_bin := round(100 * n_transfers / sum(n_transfers), 1),
                  by = acq_filer_bin]
setorder(corp_by_filer_bin, acq_filer_bin, -is_corp)
fwrite(corp_by_filer_bin,
       file.path(out_dir, "transfer_evictions_corp_by_filer_bin.csv"))
logf("  Wrote transfer_evictions_corp_by_filer_bin.csv", log_file = log_file)

logf("  Corporate / non-corporate within each filing bin:", log_file = log_file)
for (i in seq_len(nrow(corp_by_filer_bin))) {
  logf("    ", corp_by_filer_bin$acq_filer_bin[i], " x ",
       fifelse(corp_by_filer_bin$is_corp[i] == TRUE, "Corp", "Individual"), ": ",
       corp_by_filer_bin$n_transfers[i], " transfers (",
       corp_by_filer_bin$pct_of_bin[i], "% of bin), ",
       corp_by_filer_bin$n_owners[i], " owners, ",
       "mean portfolio=", corp_by_filer_bin$mean_portfolio[i], ", ",
       "mean acq rate=", corp_by_filer_bin$mean_acq_rate[i],
       log_file = log_file)
}

# Coverage QA
coverage <- event_panel[, .(
  n_total = .N,
  n_with_filings = sum(!is.na(num_filings)),
  pct_filings = round(100 * mean(!is.na(num_filings)), 1),
  n_with_violations = sum(!is.na(total_violations)),
  pct_violations = round(100 * mean(!is.na(total_violations)), 1),
  n_with_rent = sum(!is.na(med_rent)),
  pct_rent = round(100 * mean(!is.na(med_rent)), 1)
), by = event_time][order(event_time)]

qa_file <- file.path(out_dir, "transfer_evictions_qa.txt")
qa_lines <- c(
  "=== Transfer-Eviction Event Study QA ===",
  paste0("Run date: ", Sys.time()),
  "",
  paste0("Total transfers (PID-year): ", nrow(transfers)),
  paste0("Event panel rows: ", nrow(event_panel)),
  paste0("PIDs with overlapping windows: ", event_panel[has_overlap == TRUE, uniqueN(PID)]),
  paste0("Large buildings (5+ units) transfers: ",
         event_panel[large_building == TRUE & event_time == 0, .N]),
  paste0("Known rental transfers: ",
         event_panel[known_rental == TRUE & event_time == 0, .N]),
  paste0("Single-transfer PIDs: ", length(single_transfer_pids)),
  "",
  "--- Acquirer classification (pre-acquisition temporal) ---",
  paste0("Method: mean filing rate on acquirer's OTHER PIDs in years < transfer year"),
  paste0("Median pre-acq rate (among filers): ", round(acq_med, 5)),
  paste0(capture.output(print(acq_descriptives)), collapse = "\n"),
  "",
  "--- Acquirer x Corporate cross-tab ---",
  paste0(capture.output(print(corp_by_filer_bin)), collapse = "\n"),
  "",
  "--- Raw means by event_time ---",
  paste0(capture.output(print(pretrend_means)), collapse = "\n"),
  "",
  "--- Coverage by event_time ---",
  paste0(capture.output(print(coverage)), collapse = "\n"),
  "",
  "--- Descriptives by buyer type ---",
  paste0(capture.output(print(desc_by_buyer)), collapse = "\n"),
  "",
  "--- Descriptives by portfolio bin ---",
  paste0(capture.output(print(desc_by_portfolio)), collapse = "\n"),
  "",
  "--- Pre-transfer balance ---",
  paste0(capture.output(print(balance_buyer)), collapse = "\n"),
  paste0(capture.output(print(balance_portfolio)), collapse = "\n")
)
writeLines(qa_lines, qa_file)
logf("  Wrote transfer_evictions_qa.txt", log_file = log_file)

logf("=== analyze-transfer-evictions.R complete ===", log_file = log_file)
