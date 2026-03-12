## ============================================================
## analyze-transfer-evictions-unified.R
## ============================================================
## Purpose: Unified event study of eviction filing rates around
##          ownership transfers. Supports both annual and
##          quarterly frequency via config or CLI flag.
##
## Supersedes:
##   - r/analyze-transfer-evictions.R       (annual)
##   - r/analyze-transfer-evictions-quarterly.R (quarterly)
##
## Invocation:
##   Rscript r/analyze-transfer-evictions-unified.R
##   Rscript r/analyze-transfer-evictions-unified.R --frequency quarterly
##
## Inputs (from config):
##   - rtt_clean
##   - bldg_panel_blp
##   - owner_linkage_xwalk    (from build-owner-linkage.R; PID key for person entities)
##   - owner_portfolio        (from build-ownership-panel.R; keyed on conglomerate_id)
##   - xwalk_pid_conglomerate (from build-ownership-panel.R; PID x year → conglomerate_id)
##   [quarterly only]:
##   - evictions_clean
##   - evict_address_xwalk
##
## Outputs (to output/transfer_evictions/):
##   - Prefixed by frequency: annual_* or quarterly_*
##   - Coefficients CSVs, LaTeX tables, coefficient plots
##   - Pooled-window estimates, composition tables
##   - Descriptives and QA files
##   - output/logs/analyze-transfer-evictions-{freq}.log
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

# ============================================================
# SETUP: Parse frequency (config or CLI)
# ============================================================

# CLI override: --frequency annual|quarterly
args <- commandArgs(trailingOnly = TRUE)
freq_arg <- NULL
if ("--frequency" %in% args) {
  idx <- which(args == "--frequency")
  if (idx < length(args)) freq_arg <- args[idx + 1L]
}

# Config fallback
freq <- if (!is.null(freq_arg)) {
  freq_arg
} else if (!is.null(cfg$run$transfer_eviction_frequency)) {
  cfg$run$transfer_eviction_frequency
} else {
  "annual"
}
stopifnot(freq %in% c("annual", "quarterly"))

# ============================================================
# Frequency-dependent constants
# ============================================================

FREQ_PARAMS <- list(
  annual = list(
    label        = "annual",
    et_var       = "event_time",
    et_range     = -3L:5L,
    ref_period   = -1L,
    fe_time      = "year",
    outcome_var  = "filing_rate",
    count_var    = "num_filings",
    x_breaks     = -3:5,
    pre_window        = -3L:-2L,
    transition_window = c(-1L, 0L),
    early_post_window = 1L:2L,
    late_post_window  = 3L:5L,
    size_bins    = c("2-4 units", "5-19 units", "20+ units"),
    needs_evictions_clean = FALSE,
    file_prefix  = "annual",
    x_label      = "Years Relative to Transfer",
    y_label      = "Filing Rate (per unit)",
    vline_x      = -0.5,
    event_subtitle = "Event time: 0 = year of sale"
  ),
  quarterly = list(
    label        = "quarterly",
    et_var       = "q_relative",
    et_range     = -12L:20L,
    ref_period   = -4L,
    fe_time      = "yq",
    outcome_var  = "filing_rate_q",
    count_var    = "num_filings_q",
    x_breaks     = seq(-12L, 20L, 4L),
    pre_window        = -12L:-5L,
    transition_window = -4L:0L,
    early_post_window = 1L:4L,
    late_post_window  = 5L:12L,
    size_bins    = c("2-4 units", "5-19 units", "20+ units"),
    needs_evictions_clean = TRUE,
    file_prefix  = "quarterly",
    x_label      = "Quarters Relative to Transfer",
    y_label      = "Filing Rate (per unit-quarter)",
    vline_x      = -0.5,
    event_subtitle = "q = 0 = quarter of transfer"
  )
)

fp <- FREQ_PARAMS[[freq]]

log_file <- p_out(cfg, "logs", paste0("analyze-transfer-evictions-", fp$label, ".log"))
logf("=== Starting analyze-transfer-evictions-unified.R (", fp$label, ") ===",
     log_file = log_file)

# Output directories
out_dir <- p_out(cfg, "transfer_evictions")
fig_dir <- p_out(cfg, "figs")
fs::dir_create(out_dir, recurse = TRUE)
fs::dir_create(fig_dir, recurse = TRUE)

# Convenience: prefixed output path
out_path <- function(...) file.path(out_dir, paste0(fp$file_prefix, "_", ...))
fig_path <- function(...) file.path(fig_dir, paste0("rtt_", fp$file_prefix, "_", ...))

# ============================================================
# Shared helpers
# ============================================================

normalize_pid <- function(x) {
  stringr::str_pad(as.character(x), width = 9, side = "left", pad = "0")
}

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

clean_spec_labels <- function(specs) {
  label_map <- c(
    "full"                         = "Full sample",
    "2plus"                        = "2+ units",
    "5plus"                        = "5+ units",
    "2plus_rental"                 = "2+ & known rental",
    "5plus_rental"                 = "5+ & known rental",
    "2plus_rental_pre"             = "2+ & pre-transfer rental",
    "5plus_rental_pre"             = "5+ & pre-transfer rental",
    "full_pid_year"                = "Full sample",
    "full_unweighted"              = "Full (unwtd)",
    "full_unit_weighted"           = "Full (wtd)",
    "large_pid_year"               = "5+ units",
    "known_rental_pid_year"        = "Known rental",
    "known_rental_large_pid_year"  = "Known rental 5+",
    "known_rental_5plus"           = "Known rental 5+",
    "known_rental_all"             = "Known rental (all)",
    "2plus_pid_year"               = "2+ units",
    "2plus_unweighted"             = "2+ units (unwtd)",
    "corporate"                    = "Corporate",
    "individual"                   = "Individual",
    "corporate_large"              = "Corporate (5+)",
    "individual_large"             = "Individual (5+)",
    "corporate_5plus"              = "Corporate 5+",
    "individual_5plus"             = "Individual 5+",
    "corporate_2plus"              = "Corporate 2+",
    "individual_2plus"             = "Individual 2+",
    "no_sheriff"                   = "No sheriff deeds",
    "single_transfer"              = "Single-transfer PIDs",
    "no_close_overlap"             = "No nearby re-transfer",
    "count_outcome"                = "Count outcome",
    "any_filing"                   = "Any filing",
    "violations"                   = "Violations",
    "violations_rate"              = "Violations rate",
    "full_highfiler_portfolio"     = "High-filer",
    "full_lowfiler_portfolio"      = "Low-filer",
    "full_nonfiler_has_portfolio"  = "Non-filer (portfolio)",
    "full_singlepurchase"          = "Single-purchase"
  )
  # Size bins
  for (sb in c("2-4 units", "5-19 units", "20+ units", "10+ units")) {
    label_map[paste0("size_", sb)] <- sb
  }
  out <- character(length(specs))
  for (i in seq_along(specs)) {
    s <- specs[i]
    if (s %in% names(label_map)) {
      out[i] <- label_map[s]
    } else if (grepl("^portfolio_", s)) {
      out[i] <- gsub("^portfolio_", "Portfolio: ", s)
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

# Unified event study coefficient plot
plot_event_coefs <- function(coef_dt, specs, title, filename, facet = FALSE) {
  et_pattern <- paste0(fp$et_var, "::")
  plot_dt <- coef_dt[spec %in% specs & grepl(et_pattern, term, fixed = TRUE)]
  if (nrow(plot_dt) == 0) {
    logf("  WARNING: no matching terms for plot ", filename, log_file = log_file)
    return(invisible(NULL))
  }
  # Clean spec labels with N
  n_lookup <- unique(coef_dt[spec %in% specs, .(spec, n)])
  n_lookup <- n_lookup[, .(n = n[1]), by = spec]
  n_lookup[, clean_label := clean_spec_labels(spec)]
  plot_dt <- merge(plot_dt, n_lookup, by = "spec", all.x = TRUE, suffixes = c("", "_lkp"))
  plot_dt[, spec := paste0(clean_label, " (N=", formatC(n_lkp, format = "d", big.mark = ","), ")")]
  spec_labels <- unique(plot_dt$spec)
  plot_dt[, et := as.integer(str_extract(term, "-?\\d+"))]
  # Add reference point
  ref_rows <- data.table(
    et = fp$ref_period, estimate = 0, std_error = 0,
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
      geom_vline(xintercept = fp$vline_x, linetype = "dashed", color = "gray40") +
      geom_point(size = 2, color = "steelblue") +
      geom_errorbar(aes(ymin = estimate - 1.96 * std_error,
                        ymax = estimate + 1.96 * std_error),
                    width = 0.2, color = "steelblue") +
      facet_wrap(~spec, scales = "fixed") +
      scale_x_continuous(breaks = fp$x_breaks)
  } else {
    p <- ggplot(plot_dt, aes(x = et, y = estimate, color = spec)) +
      geom_hline(yintercept = 0, color = "gray60") +
      geom_vline(xintercept = fp$vline_x, linetype = "dashed", color = "gray40") +
      geom_point(position = position_dodge(width = 0.3), size = 2.5) +
      geom_errorbar(aes(ymin = estimate - 1.96 * std_error,
                        ymax = estimate + 1.96 * std_error),
                    position = position_dodge(width = 0.3), width = 0.2) +
      scale_x_continuous(breaks = fp$x_breaks)
  }
  p <- p +
    labs(title = title, subtitle = paste0("Reference period: ", fp$et_var, " = ", fp$ref_period),
         x = fp$x_label, y = fp$y_label, color = "Specification") +
    theme_philly_evict()
  ggsave(filename, p, width = 14, height = 8, dpi = 150)
  logf("  Saved ", basename(filename), log_file = log_file)
}

# Raw event study plot
plot_raw_event_study <- function(dt, group_var = NULL, title, filename,
                                 ylab = fp$y_label) {
  et_var_sym <- fp$et_var
  if (is.null(group_var)) {
    means <- dt[!is.na(get(fp$outcome_var)), .(
      mean_rate = mean(get(fp$outcome_var), na.rm = TRUE),
      se = sd(get(fp$outcome_var), na.rm = TRUE) / sqrt(.N),
      n = .N
    ), by = et_var_sym]
    p <- ggplot(means, aes(x = get(et_var_sym), y = mean_rate)) +
      geom_line(linewidth = 1.2, color = "steelblue") +
      geom_point(size = 3, color = "steelblue") +
      geom_ribbon(aes(ymin = mean_rate - 1.96 * se, ymax = mean_rate + 1.96 * se),
                  alpha = 0.15, fill = "steelblue")
  } else {
    means <- dt[!is.na(get(fp$outcome_var)) & !is.na(get(group_var)), .(
      mean_rate = mean(get(fp$outcome_var), na.rm = TRUE),
      se = sd(get(fp$outcome_var), na.rm = TRUE) / sqrt(.N),
      n = .N
    ), by = c(et_var_sym, group_var)]
    setnames(means, group_var, "group")
    p <- ggplot(means, aes(x = get(et_var_sym), y = mean_rate, color = group, fill = group)) +
      geom_line(linewidth = 1.2) +
      geom_point(size = 3) +
      geom_ribbon(aes(ymin = mean_rate - 1.96 * se, ymax = mean_rate + 1.96 * se),
                  alpha = 0.15, color = NA)
  }
  p <- p +
    geom_vline(xintercept = 0, linetype = "dashed", color = "gray40") +
    scale_x_continuous(breaks = fp$x_breaks) +
    labs(title = title, subtitle = fp$event_subtitle,
         x = fp$x_label, y = ylab, color = "", fill = "") +
    theme_philly_evict()
  ggsave(filename, p, width = 12, height = 8, dpi = 150)
  logf("  Saved ", basename(filename), log_file = log_file)
}

# ============================================================
# SECTION 0: Data Loading
# ============================================================
logf("--- SECTION 0: Loading data ---", log_file = log_file)

rtt <- fread(p_product(cfg, "rtt_clean"))
rtt[, PID := normalize_pid(PID)]
rtt[, year := as.integer(year)]
if (freq == "quarterly") rtt[, display_date := as.IDate(display_date)]
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
xwalk_owner   <- fread(p_product(cfg, "owner_linkage_xwalk"))
portfolio_cong <- fread(p_product(cfg, "owner_portfolio"))
xwalk_cong    <- fread(p_product(cfg, "xwalk_pid_conglomerate"))
xwalk_cong[, PID := normalize_pid(PID)]
logf("  owner_linkage_xwalk: ", nrow(xwalk_owner), " rows (new: includes PID for persons)",
     log_file = log_file)
logf("  owner_portfolio (conglomerate-level): ", nrow(portfolio_cong), " rows",
     log_file = log_file)
logf("  xwalk_pid_conglomerate: ", nrow(xwalk_cong), " rows", log_file = log_file)

# normalize all xwalk pids
xwalk_owner[, PID := normalize_pid(PID)]

# Quarterly-only: load day-level evictions and address crosswalk
ev_q <- NULL
if (fp$needs_evictions_clean) {
  logf("  Loading evictions_clean and evict_address_xwalk for quarterly...", log_file = log_file)
  ev <- fread(p_product(cfg, "evictions_clean"),
              select = c("d_filing", "n_sn_ss_c", "commercial"))
  setDT(ev)

  xw <- fread(p_product(cfg, "evict_address_xwalk"),
              select = c("PID", "n_sn_ss_c", "num_parcels_matched"))
  setDT(xw)
  xw <- xw[num_parcels_matched == 1L]
  xw[, PID := normalize_pid(PID)]
  xw <- unique(xw[, .(PID, n_sn_ss_c)], by = c("PID", "n_sn_ss_c"))

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
  ev[, filing_qtr := as.integer((month(filing_date) - 1L) %/% 3L + 1L)]
  ev[, year_quarter := paste0(filing_year, "-Q", filing_qtr)]

  # Pre-COVID restriction
  ev <- ev[filing_year <= 2019L]
  logf("  Evictions linked to PID (pre-COVID): ", nrow(ev), " filings", log_file = log_file)

  # Aggregate to PID x quarter

  ev_q <- ev[, .(num_filings_q = .N), by = .(PID, year_quarter)]
  assert_unique(ev_q, c("PID", "year_quarter"), "ev_q")
  logf("  PID x quarter eviction cells: ", nrow(ev_q), log_file = log_file)
}

# ============================================================
# SECTION 1: Build Transfer Event Panel
# ============================================================
logf("--- SECTION 1: Building transfer event panel ---", log_file = log_file)

# Prep transfer-level data
rtt[, grantee_upper := toupper(trimws(as.character(grantees)))]
rtt[, is_sheriff_deed := grepl("SHERIFF", toupper(document_type))]

if (freq == "annual") {
  # Collapse to PID x year
  setorder(rtt, PID, year, -total_consideration)
  transfers <- rtt[, .SD[1], by = .(PID, year)]
} else {
  # Collapse to PID x quarter
  rtt[, transfer_qtr := as.integer(quarter(display_date))]
  rtt[, transfer_year_quarter := paste0(year, "-Q", transfer_qtr)]
  setorder(rtt, PID, display_date, -total_consideration)
  transfers <- rtt[, .SD[1], by = .(PID, transfer_year_quarter)]
}
n_collapsed <- nrow(rtt) - nrow(transfers)
logf("  Collapsed RTT to PID-", fp$label, " level: ", nrow(transfers),
     " transfers (", n_collapsed, " duplicates dropped)", log_file = log_file)

# Merge owner linkage — two-step join: corps by grantee_upper, persons by (grantee_upper, PID).
# This prevents a person name appearing across many PIDs from exploding the merge.
xwalk_corp_ow <- xwalk_owner[is.na(PID), .(grantee_upper, owner_group_id, is_corp)]
xwalk_pers_ow <- xwalk_owner[!is.na(PID), .(grantee_upper, PID, owner_group_id, is_corp)]
corp_grantees_ow <- xwalk_corp_ow[, unique(grantee_upper)]

transfers_corp_ow <- merge(
  transfers[grantee_upper %in% corp_grantees_ow],
  xwalk_corp_ow, by = "grantee_upper", all.x = TRUE)
transfers_pers_ow <- merge(
  transfers[!grantee_upper %in% corp_grantees_ow],
  xwalk_pers_ow, by = c("grantee_upper", "PID"), all.x = TRUE)
transfers <- rbind(transfers_corp_ow, transfers_pers_ow, fill = TRUE)

# Portfolio: join transfers to conglomerate_id via (PID, year), then to portfolio stats.
transfers <- merge(transfers,
  xwalk_cong[, .(PID, year, conglomerate_id)],
  by = c("PID", "year"), all.x = TRUE)
transfers <- merge(transfers,
  portfolio_cong[, .(conglomerate_id, n_properties_total, portfolio_bin)],
  by = "conglomerate_id", all.x = TRUE)

logf("  Owner linkage match rate: ",
     round(100 * transfers[!is.na(owner_group_id), .N] / nrow(transfers), 1), "%",
     log_file = log_file)

transfers[, buyer_type := fifelse(is_corp == TRUE, "Corporate", "Individual")]

# --- Acquirer filing behavior classification (PRE-acquisition, temporal) ---
bldg_annual <- bldg[!is.na(total_units) & total_units > 0,
                     .(PID, year, annual_filing_rate = num_filings / total_units)]

if (freq == "annual") {
  owner_pids_temporal <- transfers[!is.na(owner_group_id),
                                   .(PID, owner_group_id, transfer_year = year)]
  acq_merge_key <- c("owner_group_id", "PID", "transfer_year")
} else {
  owner_pids_temporal <- transfers[!is.na(owner_group_id),
                                   .(PID, owner_group_id,
                                     transfer_year = year,
                                     transfer_year_quarter)]
  acq_merge_key <- c("owner_group_id", "PID", "transfer_year_quarter")
}
owner_unique_pids <- unique(owner_pids_temporal[, .(owner_group_id, PID)])

# Cross-join: for each transfer, get all OTHER PIDs by the same owner
cross <- merge(
  owner_pids_temporal[, c("owner_group_id", "PID", "transfer_year",
                           if (freq == "quarterly") "transfer_year_quarter"),
                      with = FALSE],
  owner_unique_pids[, .(owner_group_id, PID_other = PID)],
  by = "owner_group_id", allow.cartesian = TRUE
)
setnames(cross, "PID", "PID_transfer")
cross <- cross[PID_transfer != PID_other]

if (nrow(cross) > 0) {
  cross <- merge(cross, bldg_annual,
                 by.x = "PID_other", by.y = "PID",
                 allow.cartesian = TRUE)
  cross <- cross[year < transfer_year & !is.na(annual_filing_rate)]

  agg_by_cols <- c("owner_group_id", "PID_transfer")
  if (freq == "annual") {
    agg_by_cols <- c(agg_by_cols, "transfer_year")
  } else {
    agg_by_cols <- c(agg_by_cols, "transfer_year_quarter")
  }
  temporal_agg <- cross[, .(
    acq_rate = mean(annual_filing_rate, na.rm = TRUE),
    n_other = uniqueN(PID_other)
  ), by = agg_by_cols]
  setnames(temporal_agg, "PID_transfer", "PID")
} else {
  temporal_agg <- data.table(owner_group_id = character(), PID = character(),
                              acq_rate = numeric(), n_other = integer())
  if (freq == "annual") temporal_agg[, transfer_year := integer()]
  else temporal_agg[, transfer_year_quarter := character()]
}

acq_computed <- merge(owner_pids_temporal, temporal_agg,
                      by = acq_merge_key, all.x = TRUE)
acq_computed[is.na(n_other), n_other := 0L]

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

acq_lookup_cols <- c("PID", "owner_group_id", "acq_filer_bin", "acq_rate", "n_other")
if (freq == "annual") {
  acq_lookup_cols <- c(acq_lookup_cols, "transfer_year")
} else {
  acq_lookup_cols <- c(acq_lookup_cols, "transfer_year_quarter")
}
acq_lookup <- acq_computed[, ..acq_lookup_cols]

logf("  Acquirer bin distribution:", log_file = log_file)
for (ab in c("High-filer portfolio", "Low-filer portfolio",
             "Non-filer (has portfolio)", "Single-purchase")) {
  logf("    ", ab, ": ", acq_lookup[acq_filer_bin == ab, .N], " transfers",
       log_file = log_file)
}

# --- Build event grid ---
if (freq == "annual") {
  event_grid <- transfers[, .(event_time = fp$et_range), by = .(PID, transfer_year = year)]
  event_grid[, year := transfer_year + event_time]
} else {
  event_grid <- transfers[, .(q_relative = fp$et_range),
                          by = .(PID, transfer_year_quarter, display_date)]
  # MEMO 2: Proper calendar quarter arithmetic (no *91 days)
  event_grid[, transfer_year := as.integer(year(display_date))]
  event_grid[, transfer_qtr_num := as.integer(quarter(display_date))]
  event_grid[, cal_qtr_total := (transfer_year * 4L + transfer_qtr_num - 1L) + q_relative]
  event_grid[, cal_year := cal_qtr_total %/% 4L]
  event_grid[, cal_qtr := (cal_qtr_total %% 4L) + 1L]
  event_grid[, year_quarter := paste0(cal_year, "-Q", cal_qtr)]
  event_grid[, year := cal_year]
  # Drop intermediate columns
  event_grid[, c("transfer_year", "transfer_qtr_num", "cal_qtr_total",
                  "cal_year", "cal_qtr", "display_date") := NULL]
}
logf("  Event grid: ", nrow(event_grid), " rows (", event_grid[, uniqueN(PID)],
     " PIDs x ", length(fp$et_range), " event periods)", log_file = log_file)

# --- Merge building outcomes ---
bldg_cols <- c("PID", "year", "num_filings", "total_units", "total_violations",
               "total_complaints", "med_rent", "GEOID", "building_type", "num_units_bin",
               "rental_from_license", "rental_from_altos", "rental_from_evict",
               "ever_rental_any_year", "ever_rental_any_year_ever")
bldg_cols <- intersect(bldg_cols, names(bldg))
bldg_sub <- unique(bldg[, ..bldg_cols], by = c("PID", "year"))

if (freq == "annual") {
  event_panel <- merge(event_grid, bldg_sub, by = c("PID", "year"), all.x = TRUE)
} else {
  # Quarterly: merge filing counts first, then annual building data
  event_panel <- merge(event_grid, ev_q, by = c("PID", "year_quarter"), all.x = TRUE)
  event_panel[is.na(num_filings_q), num_filings_q := 0L]
  event_panel <- merge(event_panel, bldg_sub, by = c("PID", "year"), all.x = TRUE)
}
logf("  Event panel after outcome merge: ", nrow(event_panel), " rows", log_file = log_file)

# Compute filing rate
if (freq == "annual") {
  event_panel[, filing_rate := fifelse(
    !is.na(num_filings) & !is.na(total_units) & total_units > 0,
    num_filings / total_units, NA_real_
  )]
} else {
  event_panel[, filing_rate_q := fifelse(
    !is.na(total_units) & total_units > 0,
    num_filings_q / total_units, NA_real_
  )]
}

# Merge transfer characteristics
if (freq == "annual") {
  transfer_chars <- transfers[, .(PID, transfer_year = year,
                                  buyer_type, is_corp, is_sheriff_deed,
                                  owner_group_id, n_properties_total, portfolio_bin,
                                  total_consideration)]
  event_panel <- merge(event_panel, transfer_chars,
                       by = c("PID", "transfer_year"), all.x = TRUE)
} else {
  transfer_chars <- transfers[, .(PID, transfer_year_quarter,
                                  buyer_type, is_corp, is_sheriff_deed,
                                  owner_group_id, n_properties_total, portfolio_bin,
                                  total_consideration)]
  event_panel <- merge(event_panel, transfer_chars,
                       by = c("PID", "transfer_year_quarter"), all.x = TRUE)
}

# Merge acquirer classification
if (freq == "annual") {
  acq_merge_cols <- c("PID", "transfer_year", "acq_filer_bin", "acq_rate", "n_other")
} else {
  acq_merge_cols <- c("PID", "transfer_year_quarter", "acq_filer_bin", "acq_rate", "n_other")
}
acq_merge_by <- acq_merge_cols[1:2]
if (nrow(acq_lookup) > 0) {
  event_panel <- merge(event_panel,
                       acq_lookup[, ..acq_merge_cols],
                       by = acq_merge_by, all.x = TRUE)
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
if (freq == "annual") {
  assert_unique(event_panel, c("PID", "transfer_year", "event_time"),
                name = "event_panel")
} else {
  assert_unique(event_panel, c("PID", "transfer_year_quarter", "q_relative"),
                name = "event_panel_quarterly")
}

# Define subsamples
event_panel[, size_bin := fcase(
  total_units == 1L, "1 unit",
  total_units %in% 2:4, "2-4 units",
  total_units %in% 5:19, "5-19 units",
  total_units >= 20, "20+ units",
  default = NA_character_
)]
event_panel[, large_building := !is.na(total_units) & total_units >= 5L]

# Known-rental flags: split into PRE-transfer and POST-transfer rental evidence.
# - known_rental_pre: property had rental evidence in ANY year BEFORE the transfer
#   (event_time < 0). Identifies properties already operating as rentals, where
#   post-transfer filing changes reflect landlord behavior, not conversion from
#   owner-occupied to rental.
# - known_rental_post_only: property first appears as rental ONLY AFTER transfer.
#   These are potential OO-to-rental conversions where filing rate increases may
#   be mechanical (new rental = new eviction risk population).
#
# Rental evidence: rental license records, Altos/Apartments.com listings,
# or eviction filings (rental_from_license, rental_from_altos, rental_from_evict).
rental_flag_cols <- intersect(c("rental_from_license", "rental_from_altos", "rental_from_evict"),
                               names(event_panel))
if (length(rental_flag_cols) > 0) {
  # Per-row rental evidence indicator
  event_panel[, .rental_yr := Reduce(`|`, lapply(.SD, function(x) x == TRUE)),
              .SDcols = rental_flag_cols]

  # Event-level grouping key (one transfer = one event)
  if (freq == "annual") {
    .event_key <- c("PID", "transfer_year")
  } else {
    .event_key <- c("PID", "transfer_year_quarter")
  }

  # Pre-transfer: any rental evidence at event_time < 0
  pre_rental <- event_panel[get(fp$et_var) < 0L & .rental_yr == TRUE,
                             .(known_rental_pre = TRUE), by = .event_key]
  pre_rental <- unique(pre_rental, by = .event_key)
  event_panel <- merge(event_panel, pre_rental, by = .event_key, all.x = TRUE)
  event_panel[is.na(known_rental_pre), known_rental_pre := FALSE]

  # Post-transfer: any rental evidence at event_time >= 0
  post_rental <- event_panel[get(fp$et_var) >= 0L & .rental_yr == TRUE,
                              .(.has_post = TRUE), by = .event_key]
  post_rental <- unique(post_rental, by = .event_key)
  event_panel <- merge(event_panel, post_rental, by = .event_key, all.x = TRUE)
  event_panel[is.na(.has_post), .has_post := FALSE]

  # Post-only: first appears as rental after transfer (not pre)
  event_panel[, known_rental_post_only := .has_post == TRUE & known_rental_pre == FALSE]

  # Legacy: known_rental = pre OR post (backward compatible with "ever")
  event_panel[, known_rental := known_rental_pre | .has_post]

  logf("  Known rental pre-transfer at et=0: ",
       event_panel[known_rental_pre == TRUE & get(fp$et_var) == 0L, .N],
       " transfer-events", log_file = log_file)
  logf("  Known rental post-only (new rental) at et=0: ",
       event_panel[known_rental_post_only == TRUE & get(fp$et_var) == 0L, .N],
       " transfer-events", log_file = log_file)
  logf("  Known rental (either) at et=0: ",
       event_panel[known_rental == TRUE & get(fp$et_var) == 0L, .N],
       " / ", event_panel[get(fp$et_var) == 0L, .N], log_file = log_file)

  # Clean up temp cols
  event_panel[, c(".rental_yr", ".has_post") := NULL]
} else {
  event_panel[, known_rental := FALSE]
  event_panel[, known_rental_pre := FALSE]
  event_panel[, known_rental_post_only := FALSE]
  logf("  WARNING: No rental flag columns found; known_rental* set to FALSE",
       log_file = log_file)
}

# Annual-only: rental entry indicators
if (freq == "annual") {
  if ("rental_from_license" %in% names(event_panel)) {
    event_panel[, entered_license := as.integer(rental_from_license == TRUE)]
  }
  if ("rental_from_altos" %in% names(event_panel)) {
    event_panel[, entered_altos := as.integer(rental_from_altos == TRUE)]
  }
  event_panel[, has_rental_evidence := as.integer(
    (rental_from_license == TRUE) | (rental_from_altos == TRUE) | (rental_from_evict == TRUE)
  )]
}

# Year-quarter factor for FE (quarterly only)
if (freq == "quarterly") {
  event_panel[, yq := as.factor(year_quarter)]
}

logf("  Large buildings (5+ units) at et=0: ",
     event_panel[large_building == TRUE & get(fp$et_var) == 0L, .N], " transfers",
     log_file = log_file)
logf("  Known rentals at et=0: ",
     event_panel[known_rental == TRUE & get(fp$et_var) == 0L, .N], " / ",
     event_panel[get(fp$et_var) == 0L, .N], log_file = log_file)

# ============================================================
# SECTION 2: Descriptive Event Study Plots (raw means)
# ============================================================
logf("--- SECTION 2: Descriptive event study plots ---", log_file = log_file)

event_panel[, size_label := fifelse(large_building == TRUE, "5+ Units", "< 5 Units")]

# Pre-COVID restriction for raw means
raw_dt <- event_panel[year <= 2019L]

plot_raw_event_study(raw_dt, NULL,
                     paste0("Eviction Filing Rate Around Ownership Transfer (", fp$label, ")"),
                     fig_path("raw.png"))

plot_raw_event_study(raw_dt[!is.na(buyer_type)], "buyer_type",
                     paste0("Filing Rate Around Transfer: by Buyer Type (", fp$label, ")"),
                     fig_path("raw_by_buyer.png"))

plot_raw_event_study(raw_dt[!is.na(portfolio_bin)], "portfolio_bin",
                     paste0("Filing Rate Around Transfer: by Portfolio Size (", fp$label, ")"),
                     fig_path("raw_by_portfolio.png"))

plot_raw_event_study(raw_dt[!is.na(size_label)], "size_label",
                     paste0("Filing Rate Around Transfer: by Building Size (", fp$label, ")"),
                     fig_path("raw_by_size.png"))

# Raw means table
raw_means <- raw_dt[!is.na(get(fp$outcome_var)), .(
  mean_rate = mean(get(fp$outcome_var), na.rm = TRUE),
  se = sd(get(fp$outcome_var), na.rm = TRUE) / sqrt(.N),
  n = .N
), by = c(fp$et_var)][order(get(fp$et_var))]
fwrite(raw_means, out_path("raw_means.csv"))
logf("  Wrote ", fp$file_prefix, "_raw_means.csv", log_file = log_file)

raw_means_buyer <- raw_dt[!is.na(get(fp$outcome_var)) & !is.na(buyer_type), .(
  mean_rate = mean(get(fp$outcome_var), na.rm = TRUE),
  se = sd(get(fp$outcome_var), na.rm = TRUE) / sqrt(.N),
  n = .N
), by = c(fp$et_var, "buyer_type")][order(get(fp$et_var), buyer_type)]
fwrite(raw_means_buyer, out_path("raw_means_by_buyer.csv"))

# ============================================================
# SECTION 3: Baseline Regression Event Study
# ============================================================
logf("--- SECTION 3: Baseline regression event study ---", log_file = log_file)

reg_dt <- event_panel[!is.na(get(fp$outcome_var)) & year <= 2019L]
reg_dt_2plus <- reg_dt[!is.na(total_units) & total_units >= 2L]
logf("  Regression sample: ", nrow(reg_dt), " rows (full), ",
     nrow(reg_dt_2plus), " rows (2+ units)", log_file = log_file)
has_geoid <- "GEOID" %in% names(reg_dt) && reg_dt[!is.na(GEOID), .N] > 0

# Build formulas
make_formula <- function(outcome, et_var, ref, fe_parts) {
  as.formula(paste0(outcome, " ~ i(", et_var, ", ref = ", ref, ") | ",
                    paste(fe_parts, collapse = " + ")))
}

baseline_coefs <- list()
baseline_fits <- list()

# --- Baseline specs: Full, 2+, 5+, 2+ & Known Rental, 5+ & Known Rental ---
# All use PID + year/yq FE (no BG×year trends)
baseline_spec_list <- list(
  list(label = "full",               filter_expr = quote(TRUE),
       desc = "All buildings"),
  list(label = "2plus",              filter_expr = quote(!is.na(total_units) & total_units >= 2L),
       desc = "2+ unit buildings"),
  list(label = "5plus",              filter_expr = quote(large_building == TRUE),
       desc = "5+ unit buildings"),
  list(label = "2plus_rental_pre",   filter_expr = quote(!is.na(total_units) & total_units >= 2L & known_rental_pre == TRUE),
       desc = "2+ units, pre-transfer rental"),
  list(label = "5plus_rental_pre",   filter_expr = quote(large_building == TRUE & known_rental_pre == TRUE),
       desc = "5+ units, pre-transfer rental")
)

for (bs in baseline_spec_list) {
  sub_dt <- reg_dt[eval(bs$filter_expr)]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  Spec '", bs$label, "' (", bs$desc, "): N=", n_sub, ", PIDs=", n_pids,
       log_file = log_file)
  if (n_sub < 100 || n_pids < 10) next

  if (freq == "annual") {
    fit <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                 data = sub_dt, cluster = ~PID)
  } else {
    fit <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                 data = sub_dt, cluster = ~PID)
  }
  baseline_coefs[[length(baseline_coefs) + 1]] <- extract_feols_coefs(fit, bs$label)
  baseline_fits[[bs$desc]] <- fit
}

for (i in seq_along(baseline_fits)) {
  logf("    ", names(baseline_fits)[i], ": N=", baseline_fits[[i]]$nobs,
       ", adj-R2=", round(fixest::r2(baseline_fits[[i]], "ar2"), 4), log_file = log_file)
}

baseline_all <- rbindlist(baseline_coefs)
fwrite(baseline_all, out_path("baseline_coefs.csv"))
logf("  Wrote ", fp$file_prefix, "_baseline_coefs.csv", log_file = log_file)

plot_event_coefs(baseline_all, unique(baseline_all$spec),
                 paste0("Event Study: Filing Rate Around Transfer (", fp$label, ")"),
                 fig_path("baseline.png"), facet = TRUE)

etable(baseline_fits,
       file = out_path("baseline_etable.tex"),
       style.tex = style.tex("aer"),
       headers = list("Sample" = names(baseline_fits)))
logf("  Wrote baseline_etable.tex", log_file = log_file)

# ============================================================
# SECTION 4a: Heterogeneity by Buyer Type
# ============================================================
logf("--- SECTION 4a: Buyer type heterogeneity ---", log_file = log_file)

buyer_coefs <- list()
buyer_fits <- list()

if (freq == "annual") {
  # Interaction model (2+ units)
  fit_buyer_int <- feols(
    filing_rate ~ i(event_time, i.is_corp, ref = -1) | PID + year,
    data = reg_dt_2plus[!is.na(is_corp)], cluster = ~PID
  )
  buyer_coefs[[1]] <- extract_feols_coefs(fit_buyer_int, "interaction_full")

  # Split: corporate (2+ units)
  fit_corp <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                    data = reg_dt_2plus[is_corp == TRUE], cluster = ~PID)
  buyer_coefs[[2]] <- extract_feols_coefs(fit_corp, "corporate")
  buyer_fits[["Corporate"]] <- fit_corp

  fit_indiv <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                     data = reg_dt_2plus[is_corp == FALSE], cluster = ~PID)
  buyer_coefs[[3]] <- extract_feols_coefs(fit_indiv, "individual")
  buyer_fits[["Individual"]] <- fit_indiv

  # 5+ buildings split
  fit_corp_lg <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                       data = reg_dt_2plus[is_corp == TRUE & large_building == TRUE], cluster = ~PID)
  buyer_coefs[[4]] <- extract_feols_coefs(fit_corp_lg, "corporate_large")
  buyer_fits[["Corp (5+)"]] <- fit_corp_lg

  fit_indiv_lg <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                        data = reg_dt_2plus[is_corp == FALSE & large_building == TRUE], cluster = ~PID)
  buyer_coefs[[5]] <- extract_feols_coefs(fit_indiv_lg, "individual_large")
  buyer_fits[["Indiv (5+)"]] <- fit_indiv_lg

} else {
  # Quarterly: 2+ units base, also 5+ splits
  fit_corp <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                    data = reg_dt_2plus[is_corp == TRUE], cluster = ~PID)
  buyer_coefs[[1]] <- extract_feols_coefs(fit_corp, "corporate")
  buyer_fits[["Corporate"]] <- fit_corp

  fit_indiv <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                     data = reg_dt_2plus[is_corp == FALSE], cluster = ~PID)
  buyer_coefs[[2]] <- extract_feols_coefs(fit_indiv, "individual")
  buyer_fits[["Individual"]] <- fit_indiv

  reg_5plus <- reg_dt_2plus[large_building == TRUE]
  fit_corp5 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                     data = reg_5plus[is_corp == TRUE], cluster = ~PID)
  buyer_coefs[[3]] <- extract_feols_coefs(fit_corp5, "corporate_5plus")
  buyer_fits[["Corp (5+)"]] <- fit_corp5

  fit_indiv5 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                      data = reg_5plus[is_corp == FALSE], cluster = ~PID)
  buyer_coefs[[4]] <- extract_feols_coefs(fit_indiv5, "individual_5plus")
  buyer_fits[["Indiv (5+)"]] <- fit_indiv5
}

buyer_all <- rbindlist(buyer_coefs)
fwrite(buyer_all, out_path("buyer_type_coefs.csv"))
logf("  Wrote buyer_type_coefs.csv", log_file = log_file)

plot_event_coefs(buyer_all,
                 setdiff(unique(buyer_all$spec), "interaction_full"),
                 paste0("Event Study by Buyer Type (", fp$label, ")"),
                 fig_path("by_buyer.png"), facet = TRUE)

etable(buyer_fits,
       file = out_path("buyer_type_etable.tex"),
       style.tex = style.tex("aer"),
       headers = list("Buyer type" = names(buyer_fits)))

# ============================================================
# SECTION 4b: Building Size Heterogeneity
# ============================================================
logf("--- SECTION 4b: Building size heterogeneity ---", log_file = log_file)

size_coefs <- list()
size_fits <- list()

size_bins_2plus <- setdiff(fp$size_bins, "1 unit")
for (sb in size_bins_2plus) {
  sub_dt <- reg_dt_2plus[size_bin == sb]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  Size bin '", sb, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

  if (n_sub < 100 || n_pids < 10) next

  if (freq == "annual") {
    fit_sb <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                    data = sub_dt, cluster = ~PID)
  } else {
    fit_sb <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                    data = sub_dt, cluster = ~PID)
  }
  size_coefs[[length(size_coefs) + 1]] <- extract_feols_coefs(fit_sb, paste0("size_", sb))
  size_fits[[sb]] <- fit_sb
}

if (length(size_coefs) > 0) {
  size_all <- rbindlist(size_coefs)
  fwrite(size_all, out_path("size_coefs.csv"))
  logf("  Wrote size_coefs.csv", log_file = log_file)

  plot_event_coefs(size_all, unique(size_all$spec),
                   paste0("Event Study by Building Size (", fp$label, ")"),
                   fig_path("by_size.png"), facet = TRUE)

  if (length(size_fits) > 0) {
    etable(size_fits,
           file = out_path("size_etable.tex"),
           style.tex = style.tex("aer"),
           headers = list("Building size" = names(size_fits)))
  }
}

# Corporate x size (annual only)
if (freq == "annual") {
  corp_size_coefs <- list()
  for (sb in size_bins_2plus) {
    for (buyer_label in c("corp", "indiv")) {
      sub <- reg_dt_2plus[size_bin == sb & is_corp == (buyer_label == "corp")]
      n_sub <- nrow(sub)
      n_pids <- sub[, uniqueN(PID)]
      spec_name <- paste0(buyer_label, "_", sb)
      if (n_sub < 100 || n_pids < 10) next

      fit_cs <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                      data = sub, cluster = ~PID)
      corp_size_coefs[[length(corp_size_coefs) + 1]] <- extract_feols_coefs(fit_cs, spec_name)
    }
  }
  if (length(corp_size_coefs) > 0) {
    fwrite(rbindlist(corp_size_coefs), out_path("corp_x_size_coefs.csv"))
    logf("  Wrote corp_x_size_coefs.csv", log_file = log_file)
  }
}

# ============================================================
# SECTION 4c: Acquirer Filing Rate Heterogeneity
# ============================================================
logf("--- SECTION 4c: Acquirer filing rate heterogeneity ---", log_file = log_file)

acq_bins_ordered <- c("High-filer portfolio", "Low-filer portfolio",
                      "Non-filer (has portfolio)", "Single-purchase")

acq_coefs <- list()
acq_fits <- list()

for (ab in acq_bins_ordered) {
  sub_dt <- reg_dt_2plus[acq_filer_bin == ab]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  Acquirer bin '", ab, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

  if (n_sub < 100 || n_pids < 10) next

  spec_name <- paste0("full_", gsub("[^a-z_]", "", gsub(" ", "_", tolower(ab))))
  if (freq == "annual") {
    fit_ab <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                    data = sub_dt, cluster = ~PID)
  } else {
    fit_ab <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                    data = sub_dt, cluster = ~PID)
  }
  acq_coefs[[length(acq_coefs) + 1]] <- extract_feols_coefs(fit_ab, spec_name)
  acq_fits[[ab]] <- fit_ab
}

if (length(acq_coefs) > 0) {
  acq_all <- rbindlist(acq_coefs)
  fwrite(acq_all, out_path("acq_filer_coefs.csv"))
  logf("  Wrote acq_filer_coefs.csv", log_file = log_file)

  plot_event_coefs(acq_all, unique(acq_all$spec),
                   paste0("Event Study by Acquirer Filing Rate (", fp$label, ")"),
                   fig_path("by_acq_filer.png"), facet = TRUE)

  if (length(acq_fits) > 0) {
    etable(acq_fits,
           file = out_path("acq_filer_etable.tex"),
           style.tex = style.tex("aer"),
           headers = list("Acquirer type" = names(acq_fits)))
  }
}

# ============================================================
# SECTION 4d: Rental Market Entry (annual only)
# ============================================================
if (freq == "annual") {
  logf("--- SECTION 4d: Rental market entry around transfer ---", log_file = log_file)

  entry_dt <- event_panel[!is.na(has_rental_evidence) & year >= 2011 &
                           !is.na(total_units) & total_units >= 2L & year <= 2019L]
  entry_coefs <- list()

  if ("entered_license" %in% names(entry_dt) && entry_dt[!is.na(entered_license), .N] > 100) {
    fit_lic <- feols(entered_license ~ i(event_time, ref = -1) | PID + year,
                     data = entry_dt[!is.na(entered_license)], cluster = ~PID)
    entry_coefs[[1]] <- extract_feols_coefs(fit_lic, "license_entry_full")

    fit_lic_corp <- feols(entered_license ~ i(event_time, ref = -1) | PID + year,
                          data = entry_dt[is_corp == TRUE & !is.na(entered_license)], cluster = ~PID)
    entry_coefs[[2]] <- extract_feols_coefs(fit_lic_corp, "license_entry_corp")

    fit_lic_indiv <- feols(entered_license ~ i(event_time, ref = -1) | PID + year,
                           data = entry_dt[is_corp == FALSE & !is.na(entered_license)], cluster = ~PID)
    entry_coefs[[3]] <- extract_feols_coefs(fit_lic_indiv, "license_entry_indiv")
  }

  if ("entered_altos" %in% names(entry_dt) && entry_dt[!is.na(entered_altos), .N] > 100) {
    fit_alt <- feols(entered_altos ~ i(event_time, ref = -1) | PID + year,
                     data = entry_dt[!is.na(entered_altos)], cluster = ~PID)
    entry_coefs[[length(entry_coefs) + 1]] <- extract_feols_coefs(fit_alt, "altos_entry_full")
  }

  fit_any <- feols(has_rental_evidence ~ i(event_time, ref = -1) | PID + year,
                   data = entry_dt, cluster = ~PID)
  entry_coefs[[length(entry_coefs) + 1]] <- extract_feols_coefs(fit_any, "any_rental_entry_full")

  fit_any_corp <- feols(has_rental_evidence ~ i(event_time, ref = -1) | PID + year,
                        data = entry_dt[is_corp == TRUE], cluster = ~PID)
  entry_coefs[[length(entry_coefs) + 1]] <- extract_feols_coefs(fit_any_corp, "any_rental_entry_corp")

  fit_any_indiv <- feols(has_rental_evidence ~ i(event_time, ref = -1) | PID + year,
                         data = entry_dt[is_corp == FALSE], cluster = ~PID)
  entry_coefs[[length(entry_coefs) + 1]] <- extract_feols_coefs(fit_any_indiv, "any_rental_entry_indiv")

  if (length(entry_coefs) > 0) {
    entry_all <- rbindlist(entry_coefs)
    fwrite(entry_all, out_path("rental_entry_coefs.csv"))

    lic_specs <- grep("license_entry", unique(entry_all$spec), value = TRUE)
    if (length(lic_specs) > 0) {
      plot_event_coefs(entry_all, lic_specs,
                       "Rental License Entry Around Transfer (year >= 2011)",
                       fig_path("rental_license_entry.png"))
    }
    any_specs <- grep("any_rental_entry", unique(entry_all$spec), value = TRUE)
    if (length(any_specs) > 0) {
      plot_event_coefs(entry_all, any_specs,
                       "Any Rental Evidence Entry Around Transfer (year >= 2011)",
                       fig_path("any_rental_entry.png"))
    }
  }
}

# ============================================================
# SECTION 4e: Portfolio Size Heterogeneity
# ============================================================
logf("--- SECTION 4e: Portfolio size heterogeneity ---", log_file = log_file)

portfolio_bins <- c("Single-purchase", "2-4", "5-9", "10+")
portfolio_coefs <- list()
portfolio_fits <- list()

for (pb in portfolio_bins) {
  sub_dt <- reg_dt_2plus[portfolio_bin == pb]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  Portfolio bin '", pb, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)
  if (n_sub < 100 || n_pids < 10) next

  if (freq == "annual") {
    fit_pb <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                    data = sub_dt, cluster = ~PID)
  } else {
    fit_pb <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                    data = sub_dt, cluster = ~PID)
  }
  portfolio_coefs[[length(portfolio_coefs) + 1]] <- extract_feols_coefs(fit_pb, paste0("portfolio_", pb))
  portfolio_fits[[pb]] <- fit_pb
}

if (length(portfolio_coefs) > 0) {
  portfolio_all <- rbindlist(portfolio_coefs)
  fwrite(portfolio_all, out_path("portfolio_coefs.csv"))
  logf("  Wrote portfolio_coefs.csv", log_file = log_file)

  plot_event_coefs(portfolio_all, unique(portfolio_all$spec),
                   paste0("Event Study by Portfolio Size (", fp$label, ")"),
                   fig_path("by_portfolio.png"), facet = TRUE)

  if (length(portfolio_fits) > 0) {
    etable(portfolio_fits,
           file = out_path("portfolio_etable.tex"),
           style.tex = style.tex("aer"),
           headers = list("Portfolio size" = names(portfolio_fits)))
  }
}

# ============================================================
# SECTION 4f: Corporate x Filer-bin Cross-splits (MEMO ITEM 4)
# ============================================================
logf("--- SECTION 4f: Corporate x filer-bin cross-splits ---", log_file = log_file)

corp_filer_coefs <- list()
for (ab in acq_bins_ordered) {
  for (corp_flag in c(TRUE, FALSE)) {
    sub_dt <- reg_dt_2plus[acq_filer_bin == ab & is_corp == corp_flag]
    n_sub <- nrow(sub_dt)
    n_pids <- sub_dt[, uniqueN(PID)]
    corp_label <- if (corp_flag) "corp" else "indiv"
    spec_name <- paste0(corp_label, "_", gsub("[^a-z_]", "", gsub(" ", "_", tolower(ab))))
    logf("  ", spec_name, ": N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

    if (n_sub < 100 || n_pids < 10) next

    if (freq == "annual") {
      fit_cf <- feols(filing_rate ~ i(event_time, ref = -1) | PID + year,
                      data = sub_dt, cluster = ~PID)
    } else {
      fit_cf <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                      data = sub_dt, cluster = ~PID)
    }
    corp_filer_coefs[[length(corp_filer_coefs) + 1]] <- extract_feols_coefs(fit_cf, spec_name)
  }
}

if (length(corp_filer_coefs) > 0) {
  corp_filer_all <- rbindlist(corp_filer_coefs)
  fwrite(corp_filer_all, out_path("corp_x_filer_coefs.csv"))
  logf("  Wrote corp_x_filer_coefs.csv", log_file = log_file)

  plot_event_coefs(corp_filer_all, unique(corp_filer_all$spec),
                   paste0("Event Study: Corporate x Acquirer Filer Bin (", fp$label, ")"),
                   fig_path("corp_x_filer.png"), facet = TRUE)
}

# ============================================================
# SECTION 5: Robustness
# ============================================================
# All robustness uses the full building panel with explicit event-time
# dummies, so that years outside the [-3,+5] window (and never-transferred
# buildings) serve as within-PID controls. Sample: 2+ units, pre-COVID.
# ============================================================
logf("--- SECTION 5: Robustness checks (full-panel design, 2+ units) ---",
     log_file = log_file)

robustness_coefs <- list()
robustness_fits <- list()
count_var_name <- fp$count_var
single_transfer_pids <- pid_transfer_counts[n_transfers_pid == 1L, PID]
logf("  Single-transfer PIDs: ", length(single_transfer_pids), log_file = log_file)

# --- Helper: build full-panel regression dataset with event-time dummies ---
# Uses the full building panel (bldg_sub), merging in transfer timing so that
# years outside the event window have all dummies = 0.
build_robustness_panel <- function(transfer_dt, bldg_dt, sample_filter = NULL) {
  # transfer_dt: data.table with (PID, transfer_year) — one row per transfer to include
  # bldg_dt: building panel (PID, year, filing_rate, total_units, etc.)
  # sample_filter: optional expression for additional filtering

  # Get PIDs involved
  tpids <- unique(transfer_dt$PID)

  # Merge building data with transfer timing
  panel <- merge(bldg_dt, transfer_dt, by = "PID", all.x = FALSE, allow.cartesian = TRUE)
  panel[, event_time := year - transfer_year]

  # Create explicit dummies for event-time periods (ref period omitted)
  for (k in setdiff(fp$et_range, fp$ref_period)) {
    col_name <- paste0("et_", ifelse(k < 0, paste0("m", abs(k)), k))
    panel[, (col_name) := as.integer(event_time == k)]
    # Outside the event window: dummy = 0 (natural control period)
    panel[is.na(event_time) | event_time < min(fp$et_range) | event_time > max(fp$et_range),
          (col_name) := 0L]
  }

  # Filter: non-missing outcome, pre-COVID, 2+ units
  panel <- panel[!is.na(filing_rate) & year <= 2019L &
                   !is.na(total_units) & total_units >= 2L]

  if (!is.null(sample_filter)) {
    panel <- panel[eval(sample_filter)]
  }
  panel
}

# Build building panel with filing_rate for robustness use
bldg_robust <- copy(bldg_sub)
bldg_robust[, filing_rate := fifelse(
  !is.na(num_filings) & !is.na(total_units) & total_units > 0,
  num_filings / total_units, NA_real_
)]

# Event-time dummy names and formula
et_dummies <- paste0("et_", ifelse(setdiff(fp$et_range, fp$ref_period) < 0,
                                    paste0("m", abs(setdiff(fp$et_range, fp$ref_period))),
                                    setdiff(fp$et_range, fp$ref_period)))
et_fml_str <- paste0("filing_rate ~ ", paste(et_dummies, collapse = " + "), " | PID + year")

# Helper: run robustness regression and extract coefs with standard term naming
run_robustness_fit <- function(panel, fml_str, spec_label) {
  if (nrow(panel) < 100 || panel[, uniqueN(PID)] < 10) {
    logf("    ", spec_label, ": skipping (N=", nrow(panel), ")", log_file = log_file)
    return(NULL)
  }
  fit <- feols(as.formula(fml_str), data = panel, cluster = ~PID)
  ct <- extract_feols_coefs(fit, spec_label)
  # Standardize term names to match i() output format: event_time::-3, event_time::0, etc.
  ct[, term := gsub("^et_m", paste0(fp$et_var, "::-"), term)]
  ct[, term := gsub("^et_", paste0(fp$et_var, "::"), term)]
  logf("    ", spec_label, ": N=", fit$nobs, ", PIDs=", panel[, uniqueN(PID)],
       log_file = log_file)
  list(coefs = ct, fit = fit)
}

# 5a: No sheriff deeds
logf("  Robustness: No sheriff's deeds...", log_file = log_file)
no_sheriff_transfers <- unique(
  transfers[is_sheriff_deed == FALSE, .(PID, transfer_year = year)],
  by = c("PID", "transfer_year")
)
ns_panel <- build_robustness_panel(no_sheriff_transfers, bldg_robust)
ns_result <- run_robustness_fit(ns_panel, et_fml_str, "no_sheriff")
if (!is.null(ns_result)) {
  robustness_coefs[[length(robustness_coefs) + 1]] <- ns_result$coefs
  robustness_fits[["No sheriff deeds"]] <- ns_result$fit
}

# 5b: Single-transfer PIDs only
logf("  Robustness: Single-transfer PIDs...", log_file = log_file)
single_t <- unique(
  transfers[PID %chin% single_transfer_pids, .(PID, transfer_year = year)],
  by = c("PID", "transfer_year")
)
st_panel <- build_robustness_panel(single_t, bldg_robust)
st_result <- run_robustness_fit(st_panel, et_fml_str, "single_transfer")
if (!is.null(st_result)) {
  robustness_coefs[[length(robustness_coefs) + 1]] <- st_result$coefs
  robustness_fits[["Single-transfer PIDs"]] <- st_result$fit
}

# 5c: No subsequent transfer within 2 years
logf("  Robustness: No nearby re-transfer...", log_file = log_file)
all_t <- transfers[, .(PID, transfer_year = year)]
setorder(all_t, PID, transfer_year)
all_t[, next_transfer := shift(transfer_year, -1L, type = "lead"), by = PID]
all_t[, gap := next_transfer - transfer_year]
clean_t <- all_t[is.na(gap) | gap > 2L, .(PID, transfer_year)]
cl_panel <- build_robustness_panel(clean_t, bldg_robust)
cl_result <- run_robustness_fit(cl_panel, et_fml_str, "no_close_overlap")
if (!is.null(cl_result)) {
  robustness_coefs[[length(robustness_coefs) + 1]] <- cl_result$coefs
  robustness_fits[["No nearby re-transfer"]] <- cl_result$fit
}

# 5d: Count outcome
logf("  Robustness: Count outcome...", log_file = log_file)
all_transfers_for_robust <- transfers[, .(PID, transfer_year = year)]
all_transfers_for_robust <- unique(all_transfers_for_robust, by = c("PID", "transfer_year"))
count_panel <- build_robustness_panel(all_transfers_for_robust, bldg_robust)
if (nrow(count_panel) > 100 && "num_filings" %in% names(count_panel)) {
  count_fml <- paste0("num_filings ~ ", paste(et_dummies, collapse = " + "), " | PID + year")
  ct_result <- run_robustness_fit(count_panel[!is.na(num_filings)], count_fml, "count_outcome")
  if (!is.null(ct_result)) {
    robustness_coefs[[length(robustness_coefs) + 1]] <- ct_result$coefs
    robustness_fits[["Count outcome"]] <- ct_result$fit
  }
}

# 5e: Any-filing indicator
logf("  Robustness: Any-filing indicator...", log_file = log_file)
count_panel[, any_filing := as.integer(num_filings > 0)]
any_fml <- paste0("any_filing ~ ", paste(et_dummies, collapse = " + "), " | PID + year")
af_result <- run_robustness_fit(count_panel, any_fml, "any_filing")
if (!is.null(af_result)) {
  robustness_coefs[[length(robustness_coefs) + 1]] <- af_result$coefs
  robustness_fits[["Any filing"]] <- af_result$fit
}

# 5f: Violations outcome (year >= 2007)
if ("total_violations" %in% names(bldg_robust)) {
  logf("  Robustness: Violations (year >= 2007)...", log_file = log_file)
  viol_panel <- build_robustness_panel(all_transfers_for_robust, bldg_robust,
                                        sample_filter = quote(year >= 2007L))
  viol_fml <- paste0("total_violations ~ ", paste(et_dummies, collapse = " + "), " | PID + year")
  vl_result <- run_robustness_fit(viol_panel[!is.na(total_violations)], viol_fml, "violations")
  if (!is.null(vl_result)) {
    robustness_coefs[[length(robustness_coefs) + 1]] <- vl_result$coefs
    robustness_fits[["Violations"]] <- vl_result$fit
  }
}

# Combine robustness
robust_all <- rbindlist(robustness_coefs)
fwrite(robust_all, out_path("robustness_coefs.csv"))
logf("  Wrote robustness_coefs.csv", log_file = log_file)

plot_event_coefs(robust_all, unique(robust_all$spec),
                 paste0("Robustness Checks (", fp$label, ", 2+ units, full-panel)"),
                 fig_path("robustness.png"), facet = TRUE)

if (length(robustness_fits) > 0) {
  etable(robustness_fits,
         file = out_path("robustness_etable.tex"),
         style.tex = style.tex("aer"),
         headers = list("Robustness" = names(robustness_fits)))
}

# 5g: Violations diagnostic (by buyer type, year >= 2007, 2+ units)
if ("total_violations" %in% names(bldg_robust)) {
  logf("  Violations diagnostic by buyer type (year >= 2007, 2+ units)...", log_file = log_file)
  viol_diag_coefs <- list()

  # Build panels by buyer type
  corp_transfers <- transfers[is_corp == TRUE, .(PID, transfer_year = year)]
  corp_transfers <- unique(corp_transfers, by = c("PID", "transfer_year"))
  indiv_transfers <- transfers[is_corp == FALSE, .(PID, transfer_year = year)]
  indiv_transfers <- unique(indiv_transfers, by = c("PID", "transfer_year"))

  viol_diag_dt_corp <- build_robustness_panel(corp_transfers, bldg_robust,
                                               sample_filter = quote(year >= 2007L))
  viol_diag_dt_indiv <- build_robustness_panel(indiv_transfers, bldg_robust,
                                                sample_filter = quote(year >= 2007L))

  viol_fml <- paste0("total_violations ~ ", paste(et_dummies, collapse = " + "), " | PID + year")

  vc_result <- run_robustness_fit(viol_diag_dt_corp[!is.na(total_violations)],
                                   viol_fml, "violations_corp")
  vi_result <- run_robustness_fit(viol_diag_dt_indiv[!is.na(total_violations)],
                                   viol_fml, "violations_indiv")

  if (!is.null(vc_result)) viol_diag_coefs[[1]] <- vc_result$coefs
  if (!is.null(vi_result)) viol_diag_coefs[[2]] <- vi_result$coefs

  # Violations rate
  all_viol_panel <- build_robustness_panel(all_transfers_for_robust, bldg_robust,
                                            sample_filter = quote(year >= 2007L))
  all_viol_panel[, violations_rate := fifelse(
    total_units > 0 & !is.na(total_violations), total_violations / total_units, NA_real_)]
  vr_fml <- paste0("violations_rate ~ ", paste(et_dummies, collapse = " + "), " | PID + year")
  vr_result <- run_robustness_fit(all_viol_panel[!is.na(violations_rate)],
                                   vr_fml, "violations_rate")
  if (!is.null(vr_result)) viol_diag_coefs[[3]] <- vr_result$coefs

  if (length(viol_diag_coefs) > 0) {
    fwrite(rbindlist(viol_diag_coefs), out_path("violations_diag_coefs.csv"))
    plot_specs <- intersect(c("violations_corp", "violations_indiv"),
                            rbindlist(viol_diag_coefs)$spec)
    if (length(plot_specs) > 0) {
      plot_event_coefs(rbindlist(viol_diag_coefs), plot_specs,
                       paste0("Violations Around Transfer by Buyer Type (", fp$label, ")"),
                       fig_path("violations_by_buyer.png"))
    }
  }
}

# ============================================================
# SECTION 6: Pooled Window Estimates (MEMO ITEM 1 + MEMO ITEM 8)
# ============================================================
logf("--- SECTION 6: Pooled window estimates ---", log_file = log_file)

# Create window variable (on 2+ units)
reg_dt_2plus[, window := fcase(
  get(fp$et_var) %in% fp$pre_window, "1_pre",
  get(fp$et_var) %in% fp$transition_window, "2_transition",
  get(fp$et_var) %in% fp$early_post_window, "3_early_post",
  get(fp$et_var) %in% fp$late_post_window, "4_late_post",
  default = NA_character_
)]
reg_dt_pooled <- reg_dt_2plus[!is.na(window)]

# Subgroups for pooled analysis
pooled_subgroups <- list(
  full = reg_dt_pooled,
  corporate = reg_dt_pooled[is_corp == TRUE],
  individual = reg_dt_pooled[is_corp == FALSE]
)
# Add acquirer bins
for (ab in acq_bins_ordered) {
  key <- gsub("[^a-z_]", "", gsub(" ", "_", tolower(ab)))
  pooled_subgroups[[key]] <- reg_dt_pooled[acq_filer_bin == ab]
}

pooled_results <- list()
for (sg_name in names(pooled_subgroups)) {
  sg_dt <- pooled_subgroups[[sg_name]]
  n_sg <- nrow(sg_dt)
  n_pids_sg <- sg_dt[, uniqueN(PID)]
  if (n_sg < 100 || n_pids_sg < 10) {
    logf("  Pooled '", sg_name, "': skipping (N=", n_sg, ")", log_file = log_file)
    next
  }

  if (freq == "annual") {
    fit_pooled <- feols(filing_rate ~ window | PID + year,
                        data = sg_dt, cluster = ~PID)
  } else {
    fit_pooled <- feols(filing_rate_q ~ window | PID + yq,
                        data = sg_dt, cluster = ~PID)
  }
  ct <- as.data.table(coeftable(fit_pooled), keep.rownames = "term")
  setnames(ct, c("term", "estimate", "std_error", "t_value", "p_value"))
  ct[, subgroup := sg_name]
  ct[, n := fit_pooled$nobs]
  pooled_results[[length(pooled_results) + 1]] <- ct

  logf("  Pooled '", sg_name, "': N=", fit_pooled$nobs, log_file = log_file)
}

if (length(pooled_results) > 0) {
  pooled_all <- rbindlist(pooled_results)
  fwrite(pooled_all, out_path("pooled_window_coefs.csv"))
  logf("  Wrote pooled_window_coefs.csv", log_file = log_file)

  # MEMO ITEM 8: Post-minus-pre summary
  # For each subgroup, compute mean(early_post + late_post) vs pre
  post_pre_summary <- list()
  for (sg_name in unique(pooled_all$subgroup)) {
    sg <- pooled_all[subgroup == sg_name]
    # Reference category (1_pre) is absorbed, so coefficients are relative to pre
    early <- sg[term == "window3_early_post"]
    late <- sg[term == "window4_late_post"]
    if (nrow(early) > 0 && nrow(late) > 0) {
      # Average of early and late post (relative to pre)
      avg_post <- (early$estimate + late$estimate) / 2
      # Approximate SE (conservative: assume independence)
      avg_se <- sqrt((early$std_error^2 + late$std_error^2) / 4)
      post_pre_summary[[length(post_pre_summary) + 1]] <- data.table(
        subgroup = sg_name,
        post_minus_pre = round(avg_post, 6),
        se = round(avg_se, 6),
        ci_lower = round(avg_post - 1.96 * avg_se, 6),
        ci_upper = round(avg_post + 1.96 * avg_se, 6),
        n = sg$n[1]
      )
    }
  }

  if (length(post_pre_summary) > 0) {
    pps_dt <- rbindlist(post_pre_summary)
    fwrite(pps_dt, out_path("post_minus_pre_summary.csv"))
    logf("  Wrote post_minus_pre_summary.csv", log_file = log_file)

    # Post-minus-pre plot (MEMO ITEM 8)
    pps_dt[, subgroup_label := clean_spec_labels(
      paste0("full_", gsub("[^a-z_]", "", gsub(" ", "_", subgroup)))
    )]
    # Use original name if label didn't change
    pps_dt[subgroup_label == paste0("full_", gsub("[^a-z_]", "", gsub(" ", "_", subgroup))),
           subgroup_label := subgroup]

    pps_dt[, subgroup_label := factor(subgroup_label, levels = rev(subgroup_label))]

    p_pps <- ggplot(pps_dt, aes(x = post_minus_pre, y = subgroup_label)) +
      geom_vline(xintercept = 0, color = "gray60") +
      geom_point(size = 3, color = "steelblue") +
      geom_errorbarh(aes(xmin = ci_lower, xmax = ci_upper),
                     height = 0.2, color = "steelblue") +
      labs(title = paste0("Post-minus-Pre Filing Rate Change (", fp$label, ")"),
           subtitle = "Average of early+late post windows vs pre window",
           x = "Change in Filing Rate", y = "") +
      theme_philly_evict()
    ggsave(fig_path("post_minus_pre.png"), p_pps, width = 10, height = 6, dpi = 150)
    logf("  Saved post_minus_pre.png", log_file = log_file)
  }

  # Stacked pooled-window bar plot
  pooled_plot_dt <- pooled_all[grepl("window", term)]
  pooled_plot_dt[, window_label := fcase(
    grepl("2_transition", term), "Transition",
    grepl("3_early_post", term), "Early Post",
    grepl("4_late_post", term), "Late Post",
    default = term
  )]
  pooled_plot_dt[, window_label := factor(window_label,
                                           levels = c("Transition", "Early Post", "Late Post"))]
  # Limit to key subgroups for readability
  key_subgroups <- intersect(c("full", "corporate", "individual",
                                "highfiler_portfolio", "singlepurchase"),
                              unique(pooled_plot_dt$subgroup))
  if (length(key_subgroups) > 0) {
    p_pooled <- ggplot(pooled_plot_dt[subgroup %in% key_subgroups],
                       aes(x = window_label, y = estimate, fill = subgroup)) +
      geom_col(position = position_dodge(width = 0.7), width = 0.6) +
      geom_errorbar(aes(ymin = estimate - 1.96 * std_error,
                        ymax = estimate + 1.96 * std_error),
                    position = position_dodge(width = 0.7), width = 0.2) +
      geom_hline(yintercept = 0, color = "gray60") +
      labs(title = paste0("Pooled Window Estimates (", fp$label, ")"),
           subtitle = "Relative to pre-transfer window",
           x = "Window", y = "Coefficient", fill = "Subgroup") +
      theme_philly_evict()
    ggsave(fig_path("pooled_windows.png"), p_pooled, width = 12, height = 7, dpi = 150)
    logf("  Saved pooled_windows.png", log_file = log_file)
  }
}

# ============================================================
# SECTION 7: Diagnostics (MEMO ITEMS 6, 7)
# ============================================================
logf("--- SECTION 7: Diagnostics ---", log_file = log_file)

# 7a: Composition by event time (MEMO ITEM 6, pre-COVID)
logf("  Composition by event time (pre-COVID)...", log_file = log_file)
composition <- event_panel[year <= 2019L, .(
  n_obs = .N,
  n_pids = uniqueN(PID),
  pct_units_observed = round(100 * mean(!is.na(total_units)), 1),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  pct_rental_pre = round(100 * mean(known_rental_pre, na.rm = TRUE), 1),
  pct_rental_post_only = round(100 * mean(known_rental_post_only, na.rm = TRUE), 1),
  pct_corporate = round(100 * mean(is_corp == TRUE, na.rm = TRUE), 1),
  pct_large = round(100 * mean(large_building == TRUE, na.rm = TRUE), 1)
), by = c(fp$et_var)][order(get(fp$et_var))]
fwrite(composition, out_path("composition_by_et.csv"))
logf("  Wrote composition_by_et.csv", log_file = log_file)

# 7b: Pre-transfer balance by acquirer type (MEMO ITEM 7)
logf("  Pre-transfer balance by acquirer type...", log_file = log_file)
pre_periods <- fp$pre_window
balance_acq <- event_panel[get(fp$et_var) %in% pre_periods & year <= 2019L & !is.na(acq_filer_bin), .(
  mean_filing_rate = round(mean(get(fp$outcome_var), na.rm = TRUE), 5),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  n = .N
), by = acq_filer_bin]

# Add violations if available
if ("total_violations" %in% names(event_panel)) {
  balance_viol <- event_panel[get(fp$et_var) %in% pre_periods & year <= 2019L &
                               year >= 2007L & !is.na(acq_filer_bin), .(
    mean_violations = round(mean(total_violations, na.rm = TRUE), 2)
  ), by = acq_filer_bin]
  balance_acq <- merge(balance_acq, balance_viol, by = "acq_filer_bin", all.x = TRUE)
}
fwrite(balance_acq, out_path("balance_by_acq.csv"))
logf("  Wrote balance_by_acq.csv", log_file = log_file)

# Pre-transfer balance by buyer type
balance_buyer <- event_panel[get(fp$et_var) %in% pre_periods & year <= 2019L & !is.na(buyer_type), .(
  mean_filing_rate = round(mean(get(fp$outcome_var), na.rm = TRUE), 5),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  n = .N
), by = buyer_type]
if ("total_violations" %in% names(event_panel)) {
  bv <- event_panel[get(fp$et_var) %in% pre_periods & year <= 2019L &
                     year >= 2007L & !is.na(buyer_type), .(
    mean_violations = round(mean(total_violations, na.rm = TRUE), 2)
  ), by = buyer_type]
  balance_buyer <- merge(balance_buyer, bv, by = "buyer_type", all.x = TRUE)
}
fwrite(balance_buyer, out_path("balance_by_buyer.csv"))

# 7c: Pre-trend diagnostic (raw means)
pretrend_means <- event_panel[!is.na(get(fp$outcome_var)) & year <= 2019L, .(
  mean_filing_rate = mean(get(fp$outcome_var), na.rm = TRUE),
  mean_filings = mean(get(fp$count_var), na.rm = TRUE),
  n = .N
), by = c(fp$et_var)][order(get(fp$et_var))]
if ("total_violations" %in% names(event_panel)) {
  pt_viol <- event_panel[!is.na(get(fp$outcome_var)) & year <= 2019L, .(
    mean_violations = mean(total_violations, na.rm = TRUE)
  ), by = c(fp$et_var)]
  pretrend_means <- merge(pretrend_means, pt_viol, by = c(fp$et_var))
}
fwrite(pretrend_means, out_path("pretrend_means.csv"))

# ============================================================
# SECTION 8: Descriptives and QA
# ============================================================
logf("--- SECTION 8: Descriptives and QA ---", log_file = log_file)

et0_flag <- event_panel[[fp$et_var]] == 0L & event_panel[["year"]] <= 2019L

# Descriptives by buyer type
desc_by_buyer <- event_panel[et0_flag & !is.na(buyer_type), .(
  n_transfers = .N,
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  median_units = as.double(median(total_units, na.rm = TRUE)),
  mean_filing_rate = round(mean(get(fp$outcome_var), na.rm = TRUE), 4),
  mean_violations = if ("total_violations" %in% names(.SD))
    round(mean(total_violations, na.rm = TRUE), 2) else NA_real_,
  median_price = as.double(median(total_consideration, na.rm = TRUE)),
  pct_sheriff = round(100 * mean(is_sheriff_deed, na.rm = TRUE), 1)
), by = buyer_type]

# Acquirer descriptives
acq_descriptives <- event_panel[et0_flag & !is.na(acq_filer_bin), .(
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
fwrite(acq_descriptives, out_path("acq_descriptives.csv"))
logf("  Wrote acq_descriptives.csv", log_file = log_file)

# Corp x filer bin
corp_by_filer_bin <- event_panel[et0_flag & !is.na(acq_filer_bin), .(
  n_transfers = .N,
  n_owners = uniqueN(owner_group_id),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  total_units_sum = sum(total_units, na.rm = TRUE),
  mean_portfolio = round(mean(n_properties_total, na.rm = TRUE), 1),
  mean_acq_rate = round(mean(acq_rate, na.rm = TRUE), 5)
), by = .(acq_filer_bin, is_corp)]
corp_by_filer_bin[, pct_of_bin := round(100 * n_transfers / sum(n_transfers), 1),
                  by = acq_filer_bin]
setorder(corp_by_filer_bin, acq_filer_bin, -is_corp)
fwrite(corp_by_filer_bin, out_path("corp_by_filer_bin.csv"))

# Descriptives file
fwrite(desc_by_buyer, out_path("descriptives.csv"))

# Coverage by event time
coverage <- event_panel[year <= 2019L, .(
  n_total = .N,
  n_with_outcome = sum(!is.na(get(fp$outcome_var))),
  pct_outcome = round(100 * mean(!is.na(get(fp$outcome_var))), 1)
), by = c(fp$et_var)][order(get(fp$et_var))]

# QA text file
qa_file <- out_path("qa.txt")
qa_lines <- c(
  paste0("=== Transfer-Eviction Event Study QA (", fp$label, ") ==="),
  paste0("Run date: ", Sys.time()),
  paste0("Frequency: ", fp$label),
  paste0("Reference period: ", fp$et_var, " = ", fp$ref_period),
  "",
  paste0("Total transfers: ", nrow(transfers)),
  paste0("Event panel rows: ", nrow(event_panel)),
  paste0("Regression sample rows: ", nrow(reg_dt)),
  paste0("PIDs with overlapping windows: ", event_panel[has_overlap == TRUE, uniqueN(PID)]),
  paste0("Single-transfer PIDs: ", length(single_transfer_pids)),
  "",
  "--- Acquirer classification (pre-acquisition temporal) ---",
  paste0("Median pre-acq rate (among filers): ", round(acq_med, 5)),
  paste0(capture.output(print(acq_descriptives)), collapse = "\n"),
  "",
  "--- Acquirer x Corporate cross-tab ---",
  paste0(capture.output(print(corp_by_filer_bin)), collapse = "\n"),
  "",
  "--- Composition by event time ---",
  paste0(capture.output(print(composition)), collapse = "\n"),
  "",
  "--- Pre-transfer balance by acquirer type ---",
  paste0(capture.output(print(balance_acq)), collapse = "\n"),
  "",
  "--- Pre-transfer balance by buyer type ---",
  paste0(capture.output(print(balance_buyer)), collapse = "\n"),
  "",
  "--- Raw means by event time ---",
  paste0(capture.output(print(pretrend_means)), collapse = "\n"),
  "",
  "--- Coverage by event time ---",
  paste0(capture.output(print(coverage)), collapse = "\n"),
  "",
  "--- Descriptives by buyer type ---",
  paste0(capture.output(print(desc_by_buyer)), collapse = "\n")
)
writeLines(qa_lines, qa_file)
logf("  Wrote ", basename(qa_file), log_file = log_file)

logf("=== analyze-transfer-evictions-unified.R (", fp$label, ") complete ===",
     log_file = log_file)
