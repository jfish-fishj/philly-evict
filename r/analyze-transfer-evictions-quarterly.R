## ============================================================
## analyze-transfer-evictions-quarterly.R
## ============================================================
## Purpose: Quarterly event study of eviction filing rates
##          around ownership transfers, using exact transfer
##          dates and day-level filing dates.
##
## Motivation: The annual event study shows a suspicious t=-2
##   to t=-1 jump. Monthly analysis reveals filings *decline*
##   3-6 months before transfer and bottom at the transfer date,
##   then recover. Annual binning creates artifacts because
##   t=-1 mixes elevated pre-decline with declining periods.
##
## Inputs (from config):
##   - rtt_clean            (display_date for transfer timing)
##   - evictions_clean      (d_filing for day-level filing dates)
##   - evict_address_xwalk  (n_sn_ss_c → PID linkage)
##   - bldg_panel_blp       (total_units, GEOID, ever_rental, filing_rate_eb_pre_covid)
##   - owner_linkage_xwalk  (buyer identity)
##   - owner_portfolio      (portfolio info)
##
## Outputs (to output/transfer_evictions/):
##   - Quarterly coefficients CSVs and coefficient plots
##   - Raw quarterly means
##   - Descriptives about transfer patterns
##   - QA file
##   - output/logs/analyze-transfer-evictions-quarterly.log
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
log_file <- p_out(cfg, "logs", "analyze-transfer-evictions-quarterly.log")

logf("=== Starting analyze-transfer-evictions-quarterly.R ===", log_file = log_file)

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

# Helper to clean spec labels for human-readable facet strips
clean_spec_labels <- function(specs) {
  label_map <- c(
    "full_unweighted"       = "Full (unwtd)",
    "full_unit_weighted"    = "Full (wtd)",
    "full_geoid_x_yq"      = "Full (GEOID\u00d7yq)",
    "5plus_unweighted"      = "5+ units (unwtd)",
    "5plus_unit_weighted"   = "5+ units (wtd)",
    "known_rental_5plus"    = "Known rental 5+",
    "2plus_unweighted"      = "2+ units (unwtd)",
    "known_rental_all"      = "Known rental (all)",
    "corporate_5plus"       = "Corporate 5+",
    "individual_5plus"      = "Individual 5+",
    "corporate_2plus"       = "Corporate 2+",
    "individual_2plus"      = "Individual 2+",
    "size_1 unit"           = "1 unit",
    "size_2-4 units"        = "2-4 units",
    "size_5-9 units"        = "5-9 units",
    "size_10+ units"        = "10+ units",
    "full_highfiler_portfolio" = "High-filer",
    "full_lowfiler_portfolio"  = "Low-filer",
    "full_singlepurchase"      = "Single-purchase",
    "full_nonfiler_has_portfolio" = "Non-filer (portfolio)",
    "5plus_highfiler_portfolio" = "High-filer (5+)",
    "5plus_lowfiler_portfolio"  = "Low-filer (5+)",
    "5plus_singlepurchase"      = "Single-purchase (5+)",
    "5plus_nonfiler_has_portfolio" = "Non-filer (5+)",
    "2plus_highfiler_portfolio" = "High-filer (2+)",
    "2plus_lowfiler_portfolio"  = "Low-filer (2+)",
    "2plus_singlepurchase"      = "Single-purchase (2+)",
    "2plus_nonfiler_has_portfolio" = "Non-filer (2+)"
  )
  out <- character(length(specs))
  for (i in seq_along(specs)) {
    s <- specs[i]
    if (s %in% names(label_map)) {
      out[i] <- label_map[s]
    } else if (grepl("^2plus_portfolio_", s)) {
      out[i] <- paste0(gsub("^2plus_portfolio_", "", s), " (2+)")
    } else if (grepl("^5plus_portfolio_", s)) {
      out[i] <- paste0(gsub("^5plus_portfolio_", "", s), " (5+)")
    } else {
      out[i] <- s
    }
  }
  out
}

# Helper to plot quarterly event study coefs
plot_event_coefs_q <- function(coef_dt, specs, title, filename, ref_q = -4L,
                               facet = FALSE) {
  plot_dt <- coef_dt[spec %in% specs & grepl("q_relative::", term)]
  # Clean spec labels, then add N
  n_lookup <- unique(coef_dt[spec %in% specs, .(spec, n)])
  n_lookup <- n_lookup[, .(n = n[1]), by = spec]
  n_lookup[, clean_label := clean_spec_labels(spec)]
  plot_dt <- merge(plot_dt, n_lookup, by = "spec", all.x = TRUE, suffixes = c("", "_lkp"))
  spec_labels <- plot_dt[, .(spec_label = paste0(clean_label, " (N=", formatC(n_lkp, format = "d", big.mark = ","), ")")),
                         by = spec][, unique(spec_label)]
  plot_dt[, spec := paste0(clean_label, " (N=", formatC(n_lkp, format = "d", big.mark = ","), ")")]
  plot_dt[, qr := as.integer(str_extract(term, "-?\\d+"))]
  # Add reference point
  ref_rows <- data.table(
    qr = ref_q, estimate = 0, std_error = 0,
    spec = spec_labels
  )
  plot_dt <- rbindlist(list(
    plot_dt[, .(qr, estimate, std_error, spec)],
    ref_rows
  ), fill = TRUE)
  setorder(plot_dt, spec, qr)

  if (facet) {
    p <- ggplot(plot_dt, aes(x = qr, y = estimate)) +
      geom_hline(yintercept = 0, color = "gray60") +
      geom_vline(xintercept = -0.5, linetype = "dashed", color = "gray40") +
      geom_point(size = 2, color = "steelblue") +
      geom_errorbar(aes(ymin = estimate - 1.96 * std_error,
                        ymax = estimate + 1.96 * std_error),
                    width = 0.3, color = "steelblue") +
      facet_wrap(~spec, scales = "fixed") +
      scale_x_continuous(breaks = seq(-12, 20, by = 4))
  } else {
    p <- ggplot(plot_dt, aes(x = qr, y = estimate, color = spec)) +
      geom_hline(yintercept = 0, color = "gray60") +
      geom_vline(xintercept = -0.5, linetype = "dashed", color = "gray40") +
      geom_point(position = position_dodge(width = 0.4), size = 2.5) +
      geom_errorbar(aes(ymin = estimate - 1.96 * std_error,
                        ymax = estimate + 1.96 * std_error),
                    position = position_dodge(width = 0.4), width = 0.3) +
      scale_x_continuous(breaks = seq(-12, 20, by = 4))
  }
  p <- p +
    labs(title = title, subtitle = paste0("Reference period: q = ", ref_q),
         x = "Quarters Relative to Transfer",
         y = "Filing Rate (per unit-quarter)", color = "Specification") +
    theme_philly_evict()
  ggsave(file.path(fig_dir, filename), p, width = 14, height = 8, dpi = 150)
  logf("  Saved ", filename, log_file = log_file)
}

# ============================================================
# SECTION 0: Data Loading
# ============================================================
logf("--- SECTION 0: Loading data ---", log_file = log_file)

# RTT (transfers) — need display_date for exact timing
rtt <- fread(p_product(cfg, "rtt_clean"))
rtt[, PID := normalize_pid(PID)]
rtt[, display_date := as.IDate(display_date)]
rtt[, year := as.integer(year)]
logf("  rtt_clean: ", nrow(rtt), " rows, ", rtt[, uniqueN(PID)], " unique PIDs",
     log_file = log_file)

# Evictions — need d_filing for day-level timing
ev <- fread(p_product(cfg, "evictions_clean"),
            select = c("d_filing", "n_sn_ss_c", "commercial"))
setDT(ev)
logf("  evictions_clean: ", nrow(ev), " rows", log_file = log_file)

# Address crosswalk
xw <- fread(p_product(cfg, "evict_address_xwalk"),
            select = c("PID", "n_sn_ss_c", "num_parcels_matched"))
setDT(xw)
xw <- xw[num_parcels_matched == 1L]
xw[, PID := normalize_pid(PID)]
xw <- unique(xw[, .(PID, n_sn_ss_c)], by = c("PID", "n_sn_ss_c"))
logf("  evict_address_xwalk (1-match): ", nrow(xw), " rows", log_file = log_file)

# Building panel — grab filing rate for descriptives
bldg <- fread(p_product(cfg, "bldg_panel_blp"))
bldg[, PID := normalize_pid(PID)]
bldg[, year := as.integer(year)]
logf("  bldg_panel_blp: ", nrow(bldg), " rows, ", bldg[, uniqueN(PID)], " unique PIDs",
     log_file = log_file)

# Owner linkage
xwalk_owner <- fread(p_product(cfg, "owner_linkage_xwalk"))
portfolio <- fread(p_product(cfg, "owner_portfolio"))
logf("  owner_linkage_xwalk: ", nrow(xwalk_owner), " rows", log_file = log_file)
logf("  owner_portfolio: ", nrow(portfolio), " rows", log_file = log_file)

# ============================================================
# SECTION 1: Build Quarterly Eviction Counts (PID × quarter)
# ============================================================
logf("--- SECTION 1: Quarterly eviction counts ---", log_file = log_file)

# Link evictions to PIDs
setkey(xw, n_sn_ss_c)
setkey(ev, n_sn_ss_c)
ev <- ev[xw, nomatch = 0L]
ev <- ev[is.na(commercial) | tolower(commercial) != "t"]

# Parse filing date
ev[, filing_date := as.IDate(substr(as.character(d_filing), 1L, 10L),
                              format = "%Y-%m-%d")]
ev <- ev[!is.na(filing_date)]
ev[, filing_year := as.integer(year(filing_date))]
ev[, filing_month := as.integer(month(filing_date))]
ev[, filing_qtr := as.integer((filing_month - 1L) %/% 3L + 1L)]
ev[, year_quarter := paste0(filing_year, "-Q", filing_qtr)]

# Pre-COVID restriction: only filings through 2019
ev <- ev[filing_year <= 2019L]

logf("  Evictions linked to PID (pre-COVID, <=2019): ", nrow(ev), " filings", log_file = log_file)
logf("  Date range: ", min(ev$filing_date), " to ", max(ev$filing_date),
     log_file = log_file)

# Aggregate to PID × quarter
ev_q <- ev[, .(num_filings_q = .N), by = .(PID, year_quarter)]
assert_unique(ev_q, c("PID", "year_quarter"), "ev_q")
logf("  PID × quarter eviction cells: ", nrow(ev_q), log_file = log_file)

# ============================================================
# SECTION 2: Build Transfer Event Panel (quarterly)
# ============================================================
logf("--- SECTION 2: Building quarterly event panel ---", log_file = log_file)

# Prep transfer-level data
rtt[, grantee_upper := toupper(trimws(as.character(grantees)))]
rtt[, is_sheriff_deed := grepl("SHERIFF", toupper(document_type))]

# Compute transfer quarter from display_date
rtt[, transfer_qtr := as.integer(quarter(display_date))]
rtt[, transfer_year_quarter := paste0(year, "-Q", transfer_qtr)]

# Collapse to PID-quarter: keep transfer with highest total_consideration
setorder(rtt, PID, display_date, -total_consideration)
transfers <- rtt[, .SD[1], by = .(PID, transfer_year_quarter)]
n_collapsed <- nrow(rtt) - nrow(transfers)
logf("  Collapsed RTT to PID-quarter level: ", nrow(transfers), " transfers (",
     n_collapsed, " duplicates dropped)", log_file = log_file)

# Merge owner linkage
transfers <- merge(transfers, xwalk_owner[, .(grantee_upper, owner_group_id, is_corp)],
                   by = "grantee_upper", all.x = TRUE)
transfers <- merge(transfers,
                   portfolio[, .(owner_group_id, n_properties_total, portfolio_bin)],
                   by = "owner_group_id", all.x = TRUE)
logf("  Owner linkage match rate: ",
     round(100 * transfers[!is.na(owner_group_id), .N] / nrow(transfers), 1), "%",
     log_file = log_file)

# Buyer type labels
transfers[, buyer_type := fifelse(is_corp == TRUE, "Corporate", "Individual")]

# --- Acquirer filing behavior classification (PRE-acquisition, temporal) ---
# For each transfer of PID_i by owner_j (transfer year = Y), we:
#   1. Get all OTHER PIDs that owner_j acquired
#   2. From bldg_panel, compute mean(num_filings / total_units) for those PIDs
#      in years strictly BEFORE Y
#   3. Classify into High/Low/Non-filer/Single-purchase via median split
# This uses actual pre-acquisition filing rates rather than EB-smoothed rates.

has_eb_col <- "filing_rate_eb_pre_covid" %in% names(bldg)

# Build annual filing rates from bldg_panel for all PIDs
bldg_annual <- bldg[!is.na(total_units) & total_units > 0,
                     .(PID, year, annual_filing_rate = num_filings / total_units)]

# Transfers with owner info
owner_pids_temporal <- transfers[!is.na(owner_group_id),
                                 .(PID, owner_group_id,
                                   transfer_year = year,
                                   transfer_year_quarter)]
owner_unique_pids <- unique(owner_pids_temporal[, .(owner_group_id, PID)])

# Cross-join: for each transfer, get all OTHER PIDs by the same owner
cross <- merge(
  owner_pids_temporal[, .(owner_group_id, PID_transfer = PID,
                          transfer_year, transfer_year_quarter)],
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
  ), by = .(owner_group_id, PID_transfer, transfer_year_quarter)]
  setnames(temporal_agg, "PID_transfer", "PID")
} else {
  temporal_agg <- data.table(owner_group_id = character(), PID = character(),
                              transfer_year_quarter = character(),
                              acq_rate = numeric(), n_other = integer())
}

# Merge back: transfers without other PIDs get NA (→ Single-purchase)
acq_computed <- merge(owner_pids_temporal, temporal_agg,
                      by = c("owner_group_id", "PID", "transfer_year_quarter"),
                      all.x = TRUE)
acq_computed[is.na(n_other), n_other := 0L]

# Classify
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

acq_lookup <- acq_computed[, .(PID, owner_group_id, transfer_year_quarter,
                                acq_filer_bin, acq_rate, n_other)]

logf("  Acquirer bin distribution:", log_file = log_file)
for (ab in c("High-filer portfolio", "Low-filer portfolio",
             "Non-filer (has portfolio)", "Single-purchase")) {
  logf("    ", ab, ": ", acq_lookup[acq_filer_bin == ab, .N], " transfers",
       log_file = log_file)
}

# Merge pre-transfer filing rate from bldg panel (for descriptives)
bldg_meta <- bldg[, .(PID, year,
                       filing_rate_eb_pre_covid = if (has_eb_col)
                         get("filing_rate_eb_pre_covid") else NA_real_,
                       total_units_bldg = total_units)]
transfers <- merge(transfers,
                   bldg_meta[, .(PID, year, filing_rate_eb_pre_covid, total_units_bldg)],
                   by = c("PID", "year"), all.x = TRUE)

# Expand to event-time panel: q ∈ {-12, ..., +20} (3 yrs before, 5 yrs after)
event_grid <- transfers[, .(q_relative = -12L:20L),
                        by = .(PID, transfer_year_quarter, display_date)]

# Map q_relative to calendar quarter using actual transfer date
event_grid[, cal_date := display_date + as.integer(q_relative) * 91L]
event_grid[, cal_year := as.integer(year(cal_date))]
event_grid[, cal_qtr := as.integer(quarter(cal_date))]
event_grid[, year_quarter := paste0(cal_year, "-Q", cal_qtr)]
event_grid[, year := cal_year]

logf("  Event grid: ", nrow(event_grid), " rows (",
     event_grid[, uniqueN(PID)], " PIDs × 33 event quarters)",
     log_file = log_file)

# Merge quarterly filing counts (zeros where no filings)
event_panel <- merge(event_grid, ev_q, by = c("PID", "year_quarter"), all.x = TRUE)
event_panel[is.na(num_filings_q), num_filings_q := 0L]

# Merge annual total_units for rate computation (use calendar year)
bldg_cols <- c("PID", "year", "total_units", "GEOID", "num_units_bin",
               "ever_rental_any_year_ever")
bldg_cols <- intersect(bldg_cols, names(bldg))
bldg_sub <- unique(bldg[, ..bldg_cols], by = c("PID", "year"))

event_panel <- merge(event_panel, bldg_sub, by = c("PID", "year"), all.x = TRUE)
logf("  Event panel after merges: ", nrow(event_panel), " rows", log_file = log_file)
logf("  total_units coverage: ",
     round(100 * event_panel[!is.na(total_units), .N] / nrow(event_panel), 1), "%",
     log_file = log_file)

# Compute quarterly filing rate
event_panel[, filing_rate_q := fifelse(
  !is.na(total_units) & total_units > 0,
  num_filings_q / total_units,
  NA_real_
)]

# Merge transfer-level characteristics
transfer_chars <- transfers[, .(PID, transfer_year_quarter,
                                buyer_type, is_corp, is_sheriff_deed,
                                owner_group_id, n_properties_total, portfolio_bin,
                                total_consideration,
                                filing_rate_eb_pre_covid, total_units_bldg)]
event_panel <- merge(event_panel, transfer_chars,
                     by = c("PID", "transfer_year_quarter"), all.x = TRUE)

# Merge acquirer filing behavior classification
if (nrow(acq_lookup) > 0) {
  event_panel <- merge(event_panel,
                       acq_lookup[, .(PID, transfer_year_quarter,
                                      acq_filer_bin, acq_rate, n_other)],
                       by = c("PID", "transfer_year_quarter"), all.x = TRUE)
} else {
  event_panel[, acq_filer_bin := NA_character_]
  event_panel[, acq_rate := NA_real_]
  event_panel[, n_other := NA_integer_]
}

# Flag overlapping event windows
pid_transfer_counts <- transfers[, .(n_transfers_pid = .N), by = PID]
event_panel <- merge(event_panel, pid_transfer_counts, by = "PID", all.x = TRUE)
event_panel[, has_overlap := n_transfers_pid > 1L]
logf("  PIDs with multiple transfers: ",
     event_panel[has_overlap == TRUE, uniqueN(PID)], " / ",
     event_panel[, uniqueN(PID)], log_file = log_file)

# Assertions
assert_unique(event_panel, c("PID", "transfer_year_quarter", "q_relative"),
              name = "event_panel_quarterly")

# Define subsamples
event_panel[, size_bin := fcase(
  total_units == 1L, "1 unit",
  total_units %in% 2:4, "2-4 units",
  total_units %in% 5:9, "5-9 units",
  total_units >= 10, "10+ units",
  default = NA_character_
)]
event_panel[, size_binary := fifelse(total_units == 1L, "1 unit", ">1 unit")]
event_panel[, large_building := !is.na(total_units) & total_units >= 5L]

# Known-rental flag
if ("ever_rental_any_year_ever" %in% names(event_panel)) {
  event_panel[, known_rental := ever_rental_any_year_ever == TRUE]
} else {
  event_panel[, known_rental := FALSE]
}

logf("  Size bin distribution (at q=0):", log_file = log_file)
for (sb in c("1 unit", "2-4 units", "5-9 units", "10+ units")) {
  logf("    ", sb, ": ",
       event_panel[size_bin == sb & q_relative == 0L, .N], " transfers",
       log_file = log_file)
}
logf("  Known rentals: ",
     event_panel[known_rental == TRUE & q_relative == 0L, .N], " / ",
     event_panel[q_relative == 0L, .N], log_file = log_file)

logf("  Acquirer filing bin distribution (at q=0):", log_file = log_file)
for (ab in c("High-filer portfolio", "Low-filer portfolio",
             "Non-filer (has portfolio)", "Single-purchase")) {
  logf("    ", ab, ": ",
       event_panel[acq_filer_bin == ab & q_relative == 0L, .N], " transfers",
       log_file = log_file)
}

# Year-quarter factor for FE
event_panel[, yq := as.factor(year_quarter)]

# Regression sample (pre-COVID: outcome quarters <= 2019)
reg_dt <- event_panel[!is.na(filing_rate_q) & cal_year <= 2019L]
has_geoid <- "GEOID" %in% names(reg_dt) && reg_dt[!is.na(GEOID), .N] > 0

# ============================================================
# SECTION 3: Raw Quarterly Profile (descriptive)
# ============================================================
logf("--- SECTION 3: Raw quarterly profile ---", log_file = log_file)

# Raw means by q-relative (pre-COVID: outcome quarters <= 2019)
raw_means <- event_panel[!is.na(filing_rate_q) & cal_year <= 2019L, .(
  mean_rate = mean(filing_rate_q, na.rm = TRUE),
  se = sd(filing_rate_q, na.rm = TRUE) / sqrt(.N),
  mean_filings = mean(num_filings_q, na.rm = TRUE),
  n = .N
), by = q_relative][order(q_relative)]

fwrite(raw_means, file.path(out_dir, "transfer_evictions_quarterly_raw_means.csv"))
logf("  Wrote transfer_evictions_quarterly_raw_means.csv", log_file = log_file)

# Raw means by q-relative: 1 unit vs >1 unit (pre-COVID)
raw_means_binary <- event_panel[!is.na(filing_rate_q) & !is.na(size_binary) & cal_year <= 2019L, .(
  mean_rate = mean(filing_rate_q, na.rm = TRUE),
  se = sd(filing_rate_q, na.rm = TRUE) / sqrt(.N),
  n = .N
), by = .(q_relative, size_binary)][order(q_relative, size_binary)]

fwrite(raw_means_binary, file.path(out_dir, "transfer_evictions_quarterly_raw_means_binary.csv"))

# Raw means by q-relative: 4-way size bin (pre-COVID)
raw_means_size <- event_panel[!is.na(filing_rate_q) & !is.na(size_bin) & cal_year <= 2019L, .(
  mean_rate = mean(filing_rate_q, na.rm = TRUE),
  se = sd(filing_rate_q, na.rm = TRUE) / sqrt(.N),
  n = .N
), by = .(q_relative, size_bin)][order(q_relative, size_bin)]

# Plot: raw profile (full sample)
p_raw <- ggplot(raw_means, aes(x = q_relative, y = mean_rate)) +
  geom_line(linewidth = 1.2, color = "steelblue") +
  geom_point(size = 2.5, color = "steelblue") +
  geom_ribbon(aes(ymin = mean_rate - 1.96 * se, ymax = mean_rate + 1.96 * se),
              alpha = 0.15, fill = "steelblue") +
  geom_vline(xintercept = 0, linetype = "dashed", color = "gray40") +
  scale_x_continuous(breaks = seq(-12, 20, by = 4)) +
  labs(title = "Quarterly Filing Rate Around Ownership Transfer",
       subtitle = "Raw means with 95% CI. q=0 = quarter of transfer.",
       x = "Quarters Relative to Transfer", y = "Filing Rate (per unit-quarter)") +
  theme_philly_evict()
ggsave(file.path(fig_dir, "rtt_eviction_quarterly_raw_profile.png"),
       p_raw, width = 12, height = 8, dpi = 150)
logf("  Saved rtt_eviction_quarterly_raw_profile.png", log_file = log_file)

# Plot: raw profile by 1 unit vs >1 unit
p_raw_binary <- ggplot(raw_means_binary,
                       aes(x = q_relative, y = mean_rate,
                           color = size_binary, fill = size_binary)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2.5) +
  geom_ribbon(aes(ymin = mean_rate - 1.96 * se, ymax = mean_rate + 1.96 * se),
              alpha = 0.10, color = NA) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "gray40") +
  scale_x_continuous(breaks = seq(-12, 20, by = 4)) +
  labs(title = "Quarterly Filing Rate: 1-Unit vs Multi-Unit Buildings",
       subtitle = "Raw means with 95% CI. q=0 = quarter of transfer.",
       x = "Quarters Relative to Transfer", y = "Filing Rate (per unit-quarter)",
       color = "", fill = "") +
  theme_philly_evict()
ggsave(file.path(fig_dir, "rtt_eviction_quarterly_raw_1v_multi.png"),
       p_raw_binary, width = 12, height = 8, dpi = 150)
logf("  Saved rtt_eviction_quarterly_raw_1v_multi.png", log_file = log_file)

# Plot: raw profile by 4-way size bin
p_raw_size <- ggplot(raw_means_size,
                     aes(x = q_relative, y = mean_rate,
                         color = size_bin, fill = size_bin)) +
  geom_line(linewidth = 1.0) +
  geom_point(size = 2) +
  geom_ribbon(aes(ymin = mean_rate - 1.96 * se, ymax = mean_rate + 1.96 * se),
              alpha = 0.10, color = NA) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "gray40") +
  scale_x_continuous(breaks = seq(-12, 20, by = 4)) +
  labs(title = "Quarterly Filing Rate Around Transfer by Building Size",
       subtitle = "Raw means with 95% CI. q=0 = quarter of transfer.",
       x = "Quarters Relative to Transfer", y = "Filing Rate (per unit-quarter)",
       color = "", fill = "") +
  theme_philly_evict()
ggsave(file.path(fig_dir, "rtt_eviction_quarterly_by_size_4way.png"),
       p_raw_size, width = 14, height = 8, dpi = 150)
logf("  Saved rtt_eviction_quarterly_by_size_4way.png", log_file = log_file)

# ============================================================
# SECTION 4: Baseline Regression Event Study
# ============================================================
logf("--- SECTION 4: Baseline regression event study ---", log_file = log_file)

# All regressions cluster SE at PID (the unit of treatment).

baseline_coefs <- list()

# Spec 1: Full sample, PID + yq FE (unweighted)
logf("  Spec 1: Full sample, PID + yq FE (unweighted)...", log_file = log_file)
fit1 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
              data = reg_dt, cluster = ~PID)
baseline_coefs[[1]] <- extract_feols_coefs(fit1, "full_unweighted")
logf("    N=", fit1$nobs, ", adj-R2=", round(fixest::r2(fit1, "ar2"), 4),
     log_file = log_file)

# Spec 2: Full sample, PID + yq FE (unit-weighted)
logf("  Spec 2: Full sample, PID + yq FE (unit-weighted)...", log_file = log_file)
fit2 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
              data = reg_dt, cluster = ~PID, weights = ~total_units)
baseline_coefs[[2]] <- extract_feols_coefs(fit2, "full_unit_weighted")
logf("    N=", fit2$nobs, ", adj-R2=", round(fixest::r2(fit2, "ar2"), 4),
     log_file = log_file)

# Spec 3: Full sample, PID + GEOID × yq FE (unweighted)
if (has_geoid) {
  logf("  Spec 3: Full sample, PID + GEOID × yq FE...", log_file = log_file)
  fit3 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + GEOID^yq,
                data = reg_dt[!is.na(GEOID)], cluster = ~PID)
  baseline_coefs[[3]] <- extract_feols_coefs(fit3, "full_geoid_x_yq")
  logf("    N=", fit3$nobs, ", adj-R2=", round(fixest::r2(fit3, "ar2"), 4),
       log_file = log_file)
}

# Spec 4: 5+ units, PID + yq FE (unweighted)
logf("  Spec 4: 5+ units, PID + yq FE...", log_file = log_file)
fit4 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
              data = reg_dt[large_building == TRUE], cluster = ~PID)
baseline_coefs[[length(baseline_coefs) + 1]] <- extract_feols_coefs(fit4, "5plus_unweighted")
logf("    N=", fit4$nobs, ", adj-R2=", round(fixest::r2(fit4, "ar2"), 4),
     log_file = log_file)

# Spec 5: 5+ units, PID + yq FE (unit-weighted)
logf("  Spec 5: 5+ units, PID + yq FE (unit-weighted)...", log_file = log_file)
fit5 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
              data = reg_dt[large_building == TRUE], cluster = ~PID,
              weights = ~total_units)
baseline_coefs[[length(baseline_coefs) + 1]] <- extract_feols_coefs(fit5, "5plus_unit_weighted")
logf("    N=", fit5$nobs, ", adj-R2=", round(fixest::r2(fit5, "ar2"), 4),
     log_file = log_file)

# Spec 6: Known rentals 5+, PID + yq FE
logf("  Spec 6: Known rentals 5+, PID + yq FE...", log_file = log_file)
fit6 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
              data = reg_dt[known_rental == TRUE & total_units >= 5],
              cluster = ~PID)
baseline_coefs[[length(baseline_coefs) + 1]] <- extract_feols_coefs(fit6, "known_rental_5plus")
logf("    N=", fit6$nobs, ", adj-R2=", round(fixest::r2(fit6, "ar2"), 4),
     log_file = log_file)

# Spec 7: 2+ units, PID + yq FE — drops single-family
logf("  Spec 7: 2+ units, PID + yq FE...", log_file = log_file)
fit7 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
              data = reg_dt[!is.na(total_units) & total_units >= 2],
              cluster = ~PID)
baseline_coefs[[length(baseline_coefs) + 1]] <- extract_feols_coefs(fit7, "2plus_unweighted")
logf("    N=", fit7$nobs, ", adj-R2=", round(fixest::r2(fit7, "ar2"), 4),
     log_file = log_file)

# Spec 8: Known rentals (any size), PID + yq FE
logf("  Spec 8: Known rentals (all sizes), PID + yq FE...", log_file = log_file)
fit8 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
              data = reg_dt[known_rental == TRUE],
              cluster = ~PID)
baseline_coefs[[length(baseline_coefs) + 1]] <- extract_feols_coefs(fit8, "known_rental_all")
logf("    N=", fit8$nobs, ", adj-R2=", round(fixest::r2(fit8, "ar2"), 4),
     log_file = log_file)

# Write baseline coefficients
baseline_all <- rbindlist(baseline_coefs)
fwrite(baseline_all, file.path(out_dir, "transfer_evictions_quarterly_baseline_coefs.csv"))
logf("  Wrote transfer_evictions_quarterly_baseline_coefs.csv", log_file = log_file)

plot_event_coefs_q(baseline_all, unique(baseline_all$spec),
                   "Quarterly Event Study: Filing Rate Around Transfer",
                   "rtt_eviction_quarterly_baseline.png",
                   facet = TRUE)

# ============================================================
# SECTION 5: Building Size Heterogeneity (FACETED)
# ============================================================
logf("--- SECTION 5: Building size heterogeneity ---", log_file = log_file)

size_coefs <- list()
size_bins_ordered <- c("1 unit", "2-4 units", "5-9 units", "10+ units")

for (sb in size_bins_ordered) {
  sub_dt <- reg_dt[size_bin == sb]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  Size bin '", sb, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

  if (n_sub < 100 || n_pids < 10) {
    logf("    WARNING: too few observations, skipping regression", log_file = log_file)
    next
  }

  fit_sb <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                  data = sub_dt, cluster = ~PID)
  size_coefs[[length(size_coefs) + 1]] <- extract_feols_coefs(fit_sb, paste0("size_", sb))
  logf("    N=", fit_sb$nobs, ", adj-R2=", round(fixest::r2(fit_sb, "ar2"), 4),
       log_file = log_file)
}

if (length(size_coefs) > 0) {
  size_all <- rbindlist(size_coefs)
  fwrite(size_all, file.path(out_dir, "transfer_evictions_quarterly_size_coefs.csv"))
  logf("  Wrote transfer_evictions_quarterly_size_coefs.csv", log_file = log_file)

  # Faceted plot — each size bin gets its own panel with free y-axis
  plot_event_coefs_q(size_all, unique(size_all$spec),
                     "Quarterly Event Study by Building Size",
                     "rtt_eviction_quarterly_by_size.png",
                     facet = TRUE)
}

# ============================================================
# SECTION 6: Buyer Type Heterogeneity (5+ units)
# (Section 6 was formerly Sun & Abraham, removed)
# ============================================================
logf("--- SECTION 6: Buyer type heterogeneity (5+ units) ---", log_file = log_file)

buyer_coefs <- list()
reg_5plus <- reg_dt[large_building == TRUE]

logf("  Split: Corporate buyers (5+ units)...", log_file = log_file)
fit_corp <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                  data = reg_5plus[is_corp == TRUE], cluster = ~PID)
buyer_coefs[[1]] <- extract_feols_coefs(fit_corp, "corporate_5plus")
logf("    N=", fit_corp$nobs, log_file = log_file)

logf("  Split: Individual buyers (5+ units)...", log_file = log_file)
fit_indiv <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                   data = reg_5plus[is_corp == FALSE], cluster = ~PID)
buyer_coefs[[2]] <- extract_feols_coefs(fit_indiv, "individual_5plus")
logf("    N=", fit_indiv$nobs, log_file = log_file)

# Corporate vs Individual for 2+ units
reg_2plus <- reg_dt[!is.na(total_units) & total_units >= 2]

logf("  Split: Corporate buyers (2+ units)...", log_file = log_file)
fit_corp2 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                   data = reg_2plus[is_corp == TRUE], cluster = ~PID)
buyer_coefs[[3]] <- extract_feols_coefs(fit_corp2, "corporate_2plus")
logf("    N=", fit_corp2$nobs, log_file = log_file)

logf("  Split: Individual buyers (2+ units)...", log_file = log_file)
fit_indiv2 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                    data = reg_2plus[is_corp == FALSE], cluster = ~PID)
buyer_coefs[[4]] <- extract_feols_coefs(fit_indiv2, "individual_2plus")
logf("    N=", fit_indiv2$nobs, log_file = log_file)

buyer_all <- rbindlist(buyer_coefs)
fwrite(buyer_all, file.path(out_dir, "transfer_evictions_quarterly_buyer_coefs.csv"))
logf("  Wrote transfer_evictions_quarterly_buyer_coefs.csv", log_file = log_file)

plot_event_coefs_q(buyer_all, unique(buyer_all$spec),
                   "Quarterly Event Study by Buyer Type",
                   "rtt_eviction_quarterly_by_buyer.png",
                   facet = TRUE)

# ============================================================
# SECTION 7: Acquirer Filing Rate Heterogeneity (2+ units default)
# ============================================================
logf("--- SECTION 7: Acquirer filing rate heterogeneity ---", log_file = log_file)

# Classification uses PRE-ACQUISITION temporal filing rate on acquirer's OTHER
# properties (computed in Section 2). acq_filer_bin is already on event_panel.

acq_bins_ordered <- c("High-filer portfolio", "Low-filer portfolio",
                      "Non-filer (has portfolio)", "Single-purchase")

# --- 2+ units (default) ---
logf("  Acquirer heterogeneity: 2+ units", log_file = log_file)
reg_2plus_acq <- reg_dt[!is.na(total_units) & total_units >= 2]

acq_stats_2plus <- event_panel[q_relative == 0L & !is.na(total_units) &
                                 total_units >= 2 & !is.na(acq_filer_bin), .(
  n_transfers = .N,
  n_pids = uniqueN(PID),
  n_owners = uniqueN(owner_group_id),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  total_units_sum = sum(total_units, na.rm = TRUE),
  mean_acq_rate = round(mean(acq_rate, na.rm = TRUE), 5),
  mean_n_other = round(mean(n_other, na.rm = TRUE), 1)
), by = acq_filer_bin]
acq_stats_2plus[, pct_of_transfers := round(100 * n_transfers / sum(n_transfers), 1)]
acq_stats_2plus[, pct_of_units := round(100 * total_units_sum / sum(total_units_sum), 1)]
acq_stats_2plus[, sample := "2plus"]

logf("  2+ units acquirer bin stats (at q=0):", log_file = log_file)
for (i in seq_len(nrow(acq_stats_2plus))) {
  logf("    ", acq_stats_2plus$acq_filer_bin[i], ": ",
       acq_stats_2plus$n_transfers[i], " transfers (",
       acq_stats_2plus$pct_of_transfers[i], "%), ",
       acq_stats_2plus$n_owners[i], " owners, ",
       acq_stats_2plus$total_units_sum[i], " units (",
       acq_stats_2plus$pct_of_units[i], "%)",
       log_file = log_file)
}

acq_coefs_2plus <- list()
for (ab in acq_bins_ordered) {
  sub_dt <- reg_2plus_acq[acq_filer_bin == ab]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  2+: '", ab, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

  if (n_sub < 100 || n_pids < 10) next

  fit_ab <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                  data = sub_dt, cluster = ~PID)
  acq_coefs_2plus[[length(acq_coefs_2plus) + 1]] <- extract_feols_coefs(
    fit_ab, paste0("2plus_", gsub("[^a-z_]", "", gsub(" ", "_", tolower(ab))))
  )
  logf("    N=", fit_ab$nobs, log_file = log_file)
}

# --- 5+ units ---
logf("  Acquirer heterogeneity: 5+ units", log_file = log_file)

acq_stats_5plus <- event_panel[q_relative == 0L & large_building == TRUE &
                                 !is.na(acq_filer_bin), .(
  n_transfers = .N,
  n_pids = uniqueN(PID),
  n_owners = uniqueN(owner_group_id),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  total_units_sum = sum(total_units, na.rm = TRUE),
  mean_acq_rate = round(mean(acq_rate, na.rm = TRUE), 5),
  mean_n_other = round(mean(n_other, na.rm = TRUE), 1)
), by = acq_filer_bin]
acq_stats_5plus[, pct_of_transfers := round(100 * n_transfers / sum(n_transfers), 1)]
acq_stats_5plus[, pct_of_units := round(100 * total_units_sum / sum(total_units_sum), 1)]
acq_stats_5plus[, sample := "5plus"]

acq_coefs_5plus <- list()
for (ab in acq_bins_ordered) {
  sub_dt <- reg_dt[large_building == TRUE & acq_filer_bin == ab]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  5+: '", ab, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

  if (n_sub < 100 || n_pids < 10) next

  fit_ab <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                  data = sub_dt, cluster = ~PID)
  acq_coefs_5plus[[length(acq_coefs_5plus) + 1]] <- extract_feols_coefs(
    fit_ab, paste0("5plus_", gsub("[^a-z_]", "", gsub(" ", "_", tolower(ab))))
  )
  logf("    N=", fit_ab$nobs, log_file = log_file)
}

# Write acquirer results
acq_stats_all <- rbindlist(list(acq_stats_2plus, acq_stats_5plus), fill = TRUE)
fwrite(acq_stats_all, file.path(out_dir, "transfer_evictions_quarterly_acq_bin_stats.csv"))

if (length(acq_coefs_2plus) > 0) {
  acq_2plus_all <- rbindlist(acq_coefs_2plus)
  fwrite(acq_2plus_all, file.path(out_dir, "transfer_evictions_quarterly_acq_filer_coefs.csv"))
  plot_event_coefs_q(acq_2plus_all, unique(acq_2plus_all$spec),
                     "Quarterly Event Study by Acquirer Filing Rate (2+ Units)",
                     "rtt_eviction_quarterly_by_acq_filer.png",
                     facet = TRUE)
}

if (length(acq_coefs_5plus) > 0) {
  acq_5plus_all <- rbindlist(acq_coefs_5plus)
  fwrite(acq_5plus_all, file.path(out_dir, "transfer_evictions_quarterly_acq_filer_5plus_coefs.csv"))
  plot_event_coefs_q(acq_5plus_all, unique(acq_5plus_all$spec),
                     "Quarterly Event Study by Acquirer Filing Rate (5+ Units)",
                     "rtt_eviction_quarterly_by_acq_filer_5plus.png",
                     facet = TRUE)
}

# ============================================================
# SECTION 8: Portfolio Size Heterogeneity
# ============================================================
logf("--- SECTION 8: Portfolio size heterogeneity ---", log_file = log_file)

# Portfolio bins: count of non-sheriff acquisitions by the buyer entity
# "Single-purchase", "2-4", "5-9", "10+"
portfolio_bins <- c("Single-purchase", "2-4", "5-9", "10+")

# --- 2+ units (default) ---
logf("  Portfolio heterogeneity: 2+ units", log_file = log_file)
reg_2plus_port <- reg_dt[!is.na(total_units) & total_units >= 2]

port_stats_2plus <- event_panel[q_relative == 0L & !is.na(total_units) &
                                  total_units >= 2 & !is.na(portfolio_bin), .(
  n_transfers = .N,
  n_owners = uniqueN(owner_group_id),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  total_units_sum = sum(total_units, na.rm = TRUE),
  pct_corp = round(100 * mean(is_corp == TRUE, na.rm = TRUE), 1)
), by = portfolio_bin]
port_stats_2plus[, sample := "2plus"]

port_coefs_2plus <- list()
for (pb in portfolio_bins) {
  sub_dt <- reg_2plus_port[portfolio_bin == pb]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  2+: '", pb, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

  if (n_sub < 100 || n_pids < 10) next

  fit_pb <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                  data = sub_dt, cluster = ~PID)
  port_coefs_2plus[[length(port_coefs_2plus) + 1]] <- extract_feols_coefs(
    fit_pb, paste0("2plus_portfolio_", pb)
  )
  logf("    N=", fit_pb$nobs, log_file = log_file)
}

# --- 5+ units ---
logf("  Portfolio heterogeneity: 5+ units", log_file = log_file)

port_stats_5plus <- event_panel[q_relative == 0L & large_building == TRUE &
                                  !is.na(portfolio_bin), .(
  n_transfers = .N,
  n_owners = uniqueN(owner_group_id),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  total_units_sum = sum(total_units, na.rm = TRUE),
  pct_corp = round(100 * mean(is_corp == TRUE, na.rm = TRUE), 1)
), by = portfolio_bin]
port_stats_5plus[, sample := "5plus"]

port_coefs_5plus <- list()
for (pb in portfolio_bins) {
  sub_dt <- reg_dt[large_building == TRUE & portfolio_bin == pb]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  5+: '", pb, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

  if (n_sub < 100 || n_pids < 10) next

  fit_pb <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                  data = sub_dt, cluster = ~PID)
  port_coefs_5plus[[length(port_coefs_5plus) + 1]] <- extract_feols_coefs(
    fit_pb, paste0("5plus_portfolio_", pb)
  )
  logf("    N=", fit_pb$nobs, log_file = log_file)
}

# Write portfolio results
port_stats_all <- rbindlist(list(port_stats_2plus, port_stats_5plus), fill = TRUE)
fwrite(port_stats_all, file.path(out_dir, "transfer_evictions_quarterly_portfolio_bin_stats.csv"))

if (length(port_coefs_2plus) > 0) {
  port_2plus_all <- rbindlist(port_coefs_2plus)
  fwrite(port_2plus_all, file.path(out_dir, "transfer_evictions_quarterly_portfolio_coefs.csv"))
  plot_event_coefs_q(port_2plus_all, unique(port_2plus_all$spec),
                     "Quarterly Event Study by Portfolio Size (2+ Units)",
                     "rtt_eviction_quarterly_by_portfolio.png",
                     facet = TRUE)
}

if (length(port_coefs_5plus) > 0) {
  port_5plus_all <- rbindlist(port_coefs_5plus)
  fwrite(port_5plus_all, file.path(out_dir, "transfer_evictions_quarterly_portfolio_5plus_coefs.csv"))
  plot_event_coefs_q(port_5plus_all, unique(port_5plus_all$spec),
                     "Quarterly Event Study by Portfolio Size (5+ Units)",
                     "rtt_eviction_quarterly_by_portfolio_5plus.png",
                     facet = TRUE)
}

# ============================================================
# SECTION 9: Descriptives — Transfer Patterns
# ============================================================
logf("--- SECTION 9: Transfer pattern descriptives ---", log_file = log_file)

# Build a transfer-level dataset (one row per transfer at q=0)
transfer_desc <- event_panel[q_relative == 0L]

# --- 10a: Do high-evicting properties transfer more often? ---
# Use filing_rate_eb_pre_covid from bldg_panel (pre-COVID EB-smoothed annual rate)
# to classify properties BEFORE the transfer occurs.
med_filing_eb <- transfer_desc[!is.na(filing_rate_eb_pre_covid) &
                                 filing_rate_eb_pre_covid > 0,
                               median(filing_rate_eb_pre_covid, na.rm = TRUE)]
logf("  Median filing_rate_eb_pre_covid (among filers): ",
     round(med_filing_eb, 4), log_file = log_file)

transfer_desc[, property_evict_class := fcase(
  is.na(filing_rate_eb_pre_covid), "No data",
  filing_rate_eb_pre_covid == 0, "Zero-filing",
  filing_rate_eb_pre_covid <= med_filing_eb, "Below-median filing",
  filing_rate_eb_pre_covid > med_filing_eb, "Above-median filing",
  default = NA_character_
)]

# Compare to universe of properties from bldg_panel (2010-2019, pre-COVID)
bldg_universe <- bldg[year >= 2010 & year <= 2019, .(
  filing_rate_eb_pre_covid = if (has_eb_col)
    mean(get("filing_rate_eb_pre_covid"), na.rm = TRUE) else NA_real_,
  total_units = mean(total_units, na.rm = TRUE)
), by = PID]

bldg_universe[, property_evict_class := fcase(
  is.na(filing_rate_eb_pre_covid), "No data",
  filing_rate_eb_pre_covid == 0, "Zero-filing",
  filing_rate_eb_pre_covid <= med_filing_eb, "Below-median filing",
  filing_rate_eb_pre_covid > med_filing_eb, "Above-median filing",
  default = NA_character_
)]

transfer_pids <- unique(transfer_desc$PID)
bldg_universe[, ever_transferred := PID %chin% transfer_pids]

transfer_rate_by_class <- bldg_universe[, .(
  n_properties = .N,
  n_transferred = sum(ever_transferred),
  pct_transferred = round(100 * mean(ever_transferred), 1),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  total_units = round(sum(total_units, na.rm = TRUE), 0)
), by = property_evict_class]
setorder(transfer_rate_by_class, -pct_transferred)

logf("  Transfer rate by property eviction class:", log_file = log_file)
for (i in seq_len(nrow(transfer_rate_by_class))) {
  logf("    ", transfer_rate_by_class$property_evict_class[i], ": ",
       transfer_rate_by_class$pct_transferred[i], "% transferred (",
       transfer_rate_by_class$n_transferred[i], "/",
       transfer_rate_by_class$n_properties[i], " properties, ",
       transfer_rate_by_class$total_units[i], " total units)",
       log_file = log_file)
}
fwrite(transfer_rate_by_class,
       file.path(out_dir, "transfer_evictions_quarterly_transfer_rate_by_evict_class.csv"))

# --- 10b: Who buys high-evicting properties? ---
buyer_by_prop_class <- transfer_desc[!is.na(property_evict_class) &
                                       property_evict_class != "No data", .(
  n_transfers = .N,
  n_pids = uniqueN(PID),
  pct_corporate = round(100 * mean(is_corp == TRUE, na.rm = TRUE), 1),
  mean_portfolio = round(mean(n_properties_total, na.rm = TRUE), 1),
  median_price = round(median(total_consideration, na.rm = TRUE), 0),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  total_units = sum(total_units, na.rm = TRUE),
  pct_sheriff = round(100 * mean(is_sheriff_deed == TRUE, na.rm = TRUE), 1)
), by = property_evict_class]
setorder(buyer_by_prop_class, property_evict_class)

logf("  Who buys each property class:", log_file = log_file)
for (i in seq_len(nrow(buyer_by_prop_class))) {
  logf("    ", buyer_by_prop_class$property_evict_class[i], ": ",
       buyer_by_prop_class$pct_corporate[i], "% corp, ",
       "mean portfolio=", buyer_by_prop_class$mean_portfolio[i], ", ",
       buyer_by_prop_class$n_transfers[i], " transfers (",
       buyer_by_prop_class$total_units[i], " units), ",
       buyer_by_prop_class$pct_sheriff[i], "% sheriff",
       log_file = log_file)
}
fwrite(buyer_by_prop_class,
       file.path(out_dir, "transfer_evictions_quarterly_buyer_by_prop_evict_class.csv"))

# --- 10c: Acquirer descriptives (using pre-acquisition temporal filing rate) ---
acq_descriptives <- transfer_desc[!is.na(acq_filer_bin), .(
  n_transfers = .N,
  n_owners = uniqueN(owner_group_id),
  n_pids = uniqueN(PID),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  total_units = sum(total_units, na.rm = TRUE),
  pct_corporate = round(100 * mean(is_corp == TRUE, na.rm = TRUE), 1),
  mean_portfolio = round(mean(n_properties_total, na.rm = TRUE), 1),
  median_price = round(median(total_consideration, na.rm = TRUE), 0),
  mean_acq_rate = round(mean(acq_rate, na.rm = TRUE), 5),
  mean_n_other = round(mean(n_other, na.rm = TRUE), 1)
), by = acq_filer_bin]
acq_descriptives[, pct_of_transfers := round(100 * n_transfers / sum(n_transfers), 1)]
acq_descriptives[, pct_of_units := round(100 * total_units / sum(total_units), 1)]

logf("  Acquirer descriptives:", log_file = log_file)
for (i in seq_len(nrow(acq_descriptives))) {
  logf("    ", acq_descriptives$acq_filer_bin[i], ": ",
       acq_descriptives$n_transfers[i], " transfers (",
       acq_descriptives$pct_of_transfers[i], "%), ",
       acq_descriptives$n_owners[i], " owners, ",
       acq_descriptives$total_units[i], " units (",
       acq_descriptives$pct_of_units[i], "%), ",
       "mean acq rate=", acq_descriptives$mean_acq_rate[i], ", ",
       "mean n_other=", acq_descriptives$mean_n_other[i],
       log_file = log_file)
}
fwrite(acq_descriptives,
       file.path(out_dir, "transfer_evictions_quarterly_acq_descriptives.csv"))

# --- 10c2: Corporate / non-corporate within each filing bin ---
corp_by_filer_bin <- transfer_desc[!is.na(acq_filer_bin), .(
  n_transfers = .N,
  n_owners = uniqueN(owner_group_id),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  total_units = sum(total_units, na.rm = TRUE),
  mean_portfolio = round(mean(n_properties_total, na.rm = TRUE), 1),
  mean_acq_rate = round(mean(acq_rate, na.rm = TRUE), 5),
  mean_n_other = round(mean(n_other, na.rm = TRUE), 1)
), by = .(acq_filer_bin, is_corp)]
corp_by_filer_bin[, pct_of_bin := round(100 * n_transfers / sum(n_transfers), 1),
                  by = acq_filer_bin]
setorder(corp_by_filer_bin, acq_filer_bin, -is_corp)

logf("  Corporate / non-corporate within each filing bin:", log_file = log_file)
for (i in seq_len(nrow(corp_by_filer_bin))) {
  logf("    ", corp_by_filer_bin$acq_filer_bin[i], " × ",
       fifelse(corp_by_filer_bin$is_corp[i] == TRUE, "Corp", "Individual"), ": ",
       corp_by_filer_bin$n_transfers[i], " transfers (",
       corp_by_filer_bin$pct_of_bin[i], "% of bin), ",
       corp_by_filer_bin$n_owners[i], " owners, ",
       "mean portfolio=", corp_by_filer_bin$mean_portfolio[i], ", ",
       "mean acq rate=", corp_by_filer_bin$mean_acq_rate[i],
       log_file = log_file)
}
fwrite(corp_by_filer_bin,
       file.path(out_dir, "transfer_evictions_quarterly_corp_by_filer_bin.csv"))

# --- 10d: Cross-tab: property eviction class × acquirer type ---
cross_tab <- transfer_desc[!is.na(property_evict_class) &
                             property_evict_class != "No data" &
                             !is.na(acq_filer_bin), .(
  n_transfers = .N,
  total_units = sum(total_units, na.rm = TRUE)
), by = .(property_evict_class, acq_filer_bin)]
cross_tab[, pct_of_class := round(100 * n_transfers / sum(n_transfers), 1),
          by = property_evict_class]
setorder(cross_tab, property_evict_class, acq_filer_bin)

logf("  Cross-tab: property class × acquirer type:", log_file = log_file)
for (i in seq_len(nrow(cross_tab))) {
  logf("    ", cross_tab$property_evict_class[i], " → ",
       cross_tab$acq_filer_bin[i], ": ",
       cross_tab$n_transfers[i], " transfers (",
       cross_tab$pct_of_class[i], "% of class, ",
       cross_tab$total_units[i], " units)",
       log_file = log_file)
}
fwrite(cross_tab,
       file.path(out_dir, "transfer_evictions_quarterly_prop_x_acq_crosstab.csv"))

# --- 10e: Single-purchase corporates: are they actually missing from other data? ---
# Take a random sample of "Single-purchase" transfers by corporate entities
# and check whether their grantee names appear elsewhere in the RTT or
# whether their PIDs appear in bldg_panel.
logf("  --- Single-purchase corporate investigation ---", log_file = log_file)

set.seed(42)
# Use transfers (not transfer_desc) since we need grantee_upper
sp_corps <- transfers[!is.na(owner_group_id) & is_corp == TRUE]
sp_corps <- merge(sp_corps, acq_lookup[, .(PID, transfer_year_quarter, acq_filer_bin)],
                  by = c("PID", "transfer_year_quarter"), all.x = TRUE)
sp_corps <- sp_corps[acq_filer_bin == "Single-purchase"]
n_sp_corp <- nrow(sp_corps)
logf("  Total single-purchase corporate transfers: ", n_sp_corp, log_file = log_file)

sample_n <- min(1000L, n_sp_corp)
sp_sample <- sp_corps[sample(.N, sample_n)]

# Check 1: Does their grantee_upper appear on OTHER RTT records (other PIDs)?
sp_grantees <- unique(sp_sample$grantee_upper)
rtt_other_deeds <- rtt[grantee_upper %chin% sp_grantees]
grantee_multi <- rtt_other_deeds[, .(n_deeds = .N, n_pids = uniqueN(PID)),
                                  by = grantee_upper]
grantee_multi[, has_multiple_pids := n_pids > 1L]

sp_sample <- merge(sp_sample, grantee_multi[, .(grantee_upper, n_deeds_rtt = n_deeds,
                                                  n_pids_rtt = n_pids)],
                   by = "grantee_upper", all.x = TRUE)

# Check 2: Does their PID appear in bldg_panel (i.e., do we have building data)?
bldg_pids <- unique(bldg$PID)
sp_sample[, pid_in_bldg_panel := PID %chin% bldg_pids]

# Check 3: Does their owner_group_id appear on multiple PIDs in the transfers table?
if ("owner_group_id" %in% names(sp_sample)) {
  og_counts <- transfers[!is.na(owner_group_id),
                          .(n_pids_owner = uniqueN(PID)), by = owner_group_id]
  sp_sample <- merge(sp_sample, og_counts, by = "owner_group_id", all.x = TRUE)
} else {
  sp_sample[, n_pids_owner := NA_integer_]
}

# Summarize
sp_summary <- data.table(
  metric = c(
    "Sample size",
    "PID in bldg_panel",
    "PID NOT in bldg_panel",
    "Grantee has >1 PID in RTT",
    "Grantee has 1 PID in RTT",
    "Owner group has >1 PID in transfers",
    "Mean deeds in RTT for grantee",
    "Median deeds in RTT for grantee"
  ),
  value = c(
    sample_n,
    sp_sample[pid_in_bldg_panel == TRUE, .N],
    sp_sample[pid_in_bldg_panel == FALSE, .N],
    sp_sample[n_pids_rtt > 1, .N],
    sp_sample[n_pids_rtt == 1, .N],
    sp_sample[!is.na(n_pids_owner) & n_pids_owner > 1, .N],
    round(mean(sp_sample$n_deeds_rtt, na.rm = TRUE), 1),
    median(sp_sample$n_deeds_rtt, na.rm = TRUE)
  )
)

logf("  Single-purchase corporate sample (N=", sample_n, "):", log_file = log_file)
for (i in seq_len(nrow(sp_summary))) {
  logf("    ", sp_summary$metric[i], ": ", sp_summary$value[i], log_file = log_file)
}
fwrite(sp_summary,
       file.path(out_dir, "transfer_evictions_quarterly_sp_corp_investigation.csv"))

# Also save sample details for manual inspection
sp_detail <- sp_sample[, .(PID, grantee_upper, owner_group_id,
                             total_consideration,
                             pid_in_bldg_panel, n_deeds_rtt, n_pids_rtt,
                             n_pids_owner)]
fwrite(sp_detail,
       file.path(out_dir, "transfer_evictions_quarterly_sp_corp_sample_detail.csv"))
logf("  Wrote sp_corp_investigation.csv and sp_corp_sample_detail.csv",
     log_file = log_file)

# ============================================================
# SECTION 10: QA
# ============================================================
logf("--- SECTION 10: QA ---", log_file = log_file)

qa_file <- file.path(out_dir, "transfer_evictions_quarterly_qa.txt")

coverage_q <- event_panel[, .(
  n_total = .N,
  n_with_rate = sum(!is.na(filing_rate_q)),
  pct_rate = round(100 * mean(!is.na(filing_rate_q)), 1),
  mean_filings = round(mean(num_filings_q, na.rm = TRUE), 3),
  mean_rate = round(mean(filing_rate_q, na.rm = TRUE), 5)
), by = q_relative][order(q_relative)]

size_summary <- event_panel[q_relative == 0L & !is.na(size_bin), .(
  n_transfers = .N,
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  mean_rate = round(mean(filing_rate_q, na.rm = TRUE), 5),
  pct_known_rental = round(100 * mean(known_rental, na.rm = TRUE), 1)
), by = size_bin]

qa_lines <- c(
  "=== Quarterly Transfer-Eviction Event Study QA ===",
  paste0("Run date: ", Sys.time()),
  "",
  "--- Sample ---",
  paste0("Eviction filings linked to PID: ", nrow(ev)),
  paste0("PID × quarter eviction cells: ", nrow(ev_q)),
  paste0("Transfers (PID-quarter): ", nrow(transfers)),
  paste0("Event panel rows: ", nrow(event_panel)),
  paste0("PIDs with overlapping windows: ",
         event_panel[has_overlap == TRUE, uniqueN(PID)]),
  paste0("Clustering: PID-level (unit of treatment)"),
  "",
  "--- Size bin distribution at q=0 ---",
  paste0(capture.output(print(size_summary)), collapse = "\n"),
  "",
  "--- Acquirer filing rate classification ---",
  "  Method: PRE-ACQUISITION temporal filing rate on acquirer's OTHER properties",
  "  For each transfer of PID_i by owner_j (transfer year Y):",
  "    rate = mean(num_filings / total_units) for other PIDs in years < Y",
  paste0("  Median (among acquirers with rate > 0): ",
         if (exists("acq_med")) round(acq_med, 5) else "N/A"),
  "  High-filer portfolio: rate > median, has other properties",
  "  Low-filer portfolio: 0 < rate <= median, has other properties",
  "  Non-filer (has portfolio): rate == 0 but n_other > 0",
  "  Single-purchase: n_other == 0 (no other matched properties)",
  "",
  "--- Acquirer bin stats (2+ units, at q=0) ---",
  paste0(capture.output(print(acq_stats_2plus)), collapse = "\n"),
  "",
  "--- Acquirer bin stats (5+ units, at q=0) ---",
  paste0(capture.output(print(acq_stats_5plus)), collapse = "\n"),
  "",
  "--- Corporate / non-corporate within each filing bin ---",
  paste0(capture.output(print(corp_by_filer_bin)), collapse = "\n"),
  "",
  "--- Single-purchase corporate investigation ---",
  paste0(capture.output(print(sp_summary)), collapse = "\n"),
  "",
  "--- Portfolio bin stats (2+ units, at q=0) ---",
  paste0(capture.output(print(port_stats_2plus)), collapse = "\n"),
  "",
  "--- Portfolio bin stats (5+ units, at q=0) ---",
  paste0(capture.output(print(port_stats_5plus)), collapse = "\n"),
  "",
  "--- Property eviction class definition ---",
  "  Based on filing_rate_eb_pre_covid from bldg_panel_blp",
  "  EB-smoothed annual filing rate (per unit), pre-COVID, time-invariant",
  paste0("  Median (among properties with any filings): ",
         round(med_filing_eb, 4)),
  "",
  "--- Transfer rate by property eviction class ---",
  paste0(capture.output(print(transfer_rate_by_class)), collapse = "\n"),
  "",
  "--- Who buys each property class? ---",
  paste0(capture.output(print(buyer_by_prop_class)), collapse = "\n"),
  "",
  "--- Acquirer descriptives ---",
  paste0(capture.output(print(acq_descriptives)), collapse = "\n"),
  "",
  "--- Cross-tab: property class × acquirer type ---",
  paste0(capture.output(print(cross_tab)), collapse = "\n"),
  "",
  "--- Coverage by q_relative ---",
  paste0(capture.output(print(coverage_q)), collapse = "\n"),
  "",
  "--- Raw means by q_relative ---",
  paste0(capture.output(print(raw_means)), collapse = "\n")
)

writeLines(qa_lines, qa_file)
logf("  Wrote transfer_evictions_quarterly_qa.txt", log_file = log_file)

logf("=== analyze-transfer-evictions-quarterly.R complete ===", log_file = log_file)
