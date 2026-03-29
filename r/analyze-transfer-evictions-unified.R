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
    et_range     = -3L:3L,
    ref_period   = -2L,
    fe_time      = "year",
    outcome_var  = "filing_rate",
    count_var    = "num_filings",
    x_breaks     = -3:3,
    pre_window        = -3L:-2L,
    transition_window = c(-1L, 0L),
    early_post_window = 1L:2L,
    late_post_window  = 3L:3L,
    size_bins    = c("2-4 units", "5-19 units", "20+ units"),
    needs_evictions_clean = FALSE,
    file_prefix  = "annual",
    x_label      = "Years Relative to Transfer",
    y_label      = "Filing Rate (per unit)",
    vline_x      = 0,
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
    vline_x      = 0,
    event_subtitle = "q = 0 = quarter of transfer"
  )
)

fp <- FREQ_PARAMS[[freq]]
# Canonical reference period for i(event_time, ref = ...) — used throughout Sections 3/4.
# Must be a plain scalar so fixest's formula parser can evaluate it inside i().
ET_REF <- fp$ref_period

ANALYSIS_YEAR_MIN <- 2006L
ANALYSIS_YEAR_MAX <- 2019L
LOO_MIN_UNIT_YEARS <- suppressWarnings(as.integer(cfg$run$loo_min_unit_years %||% 100L))
if (!is.finite(LOO_MIN_UNIT_YEARS) || LOO_MIN_UNIT_YEARS <= 0L) {
  stop("Invalid run$loo_min_unit_years: expected a positive integer.")
}
# Fixed high-filer rate threshold (filings per unit-year). Portfolios above this are "high-filers".
# Using a fixed threshold rather than the median makes the classification invariant to sample
# composition and has a direct economic interpretation (~7.5 filings per 100 units per year).
ACQ_HIGH_FILER_THRESHOLD <- as.numeric(
  cfg$run$loo_filing_threshold %||%
    cfg$run$acq_high_filer_threshold %||%
    0.05
)
MAX_ALLOWABLE_FILING_RATE <- as.numeric(
  cfg$run$transfer_evictions_max_filing_rate %||% 0.75
)
if (!is.finite(ACQ_HIGH_FILER_THRESHOLD) || ACQ_HIGH_FILER_THRESHOLD <= 0 || ACQ_HIGH_FILER_THRESHOLD >= 1) {
  stop("Invalid LOO filing threshold: expected a number strictly between 0 and 1.")
}
if (!is.finite(MAX_ALLOWABLE_FILING_RATE) || MAX_ALLOWABLE_FILING_RATE <= 0) {
  stop("Invalid transfer-evictions filing-rate cap: expected a positive number.")
}
ACQ_BIN_LABELS <- c(
  "High-filer portfolio",
  "Low-filer portfolio",
  "Small portfolio",
  "Single-purchase"
)
# Preserve legacy suffixes to avoid churn in downstream output column names.
ACQ_BIN_SUFFIX <- c(
  setNames("highfiler_portfolio_10plus_otherunits", ACQ_BIN_LABELS[1]),
  setNames("lowfiler_portfolio_10plus_otherunits", ACQ_BIN_LABELS[2]),
  setNames("smallportfolio_lt10_otherunits", ACQ_BIN_LABELS[3]),
  setNames("singlepurchase", ACQ_BIN_LABELS[4])
)
acq_bin_suffix <- function(label) unname(ACQ_BIN_SUFFIX[[label]])

log_file <- p_out(cfg, "logs", paste0("analyze-transfer-evictions-", fp$label, ".log"))
logf("=== Starting analyze-transfer-evictions-unified.R (", fp$label, ") ===",
     log_file = log_file)

# Output directories
out_dir <- p_out(cfg, "transfer_evictions")
fig_dir <- p_out(cfg, "transfer_evictions", "figs")
fs::dir_create(out_dir, recurse = TRUE)
fs::dir_create(fig_dir, recurse = TRUE)

# Convenience: prefixed output path
out_path <- function(...) file.path(out_dir, paste0(fp$file_prefix, "_", ...))
fig_path <- function(...) file.path(fig_dir, paste0("rtt_", fp$file_prefix, "_", ...))

LEGACY_EVENT_COLORS <- c(
  "#F8766D",
  "#00BA38",
  "#619CFF",
  "#C77CFF",
  "#E69F00",
  "#00BFC4"
)

# ============================================================
# Shared helpers
# ============================================================

normalize_pid <- function(x) {
  stringr::str_pad(as.character(x), width = 9, side = "left", pad = "0")
}

add_tract_year_fe <- function(dt, geoid_col = "GEOID", year_col = "year") {
  out <- copy(dt)
  if (!geoid_col %in% names(out) || !year_col %in% names(out)) {
    return(out)
  }
  out[, tract_geoid := fifelse(!is.na(get(geoid_col)) & nchar(get(geoid_col)) >= 11L,
                               substr(get(geoid_col), 1L, 11L), NA_character_)]
  out[, tract_year_fe := fifelse(!is.na(tract_geoid) & !is.na(get(year_col)),
                                 paste0(tract_geoid, "_", get(year_col)),
                                 NA_character_)]
  out
}

extract_feols_coefs <- function(fit, spec_label) {
  fit_stat <- tryCatch(fixest::r2(fit, "ar2"), error = function(e) NA_real_)
  if (!is.finite(fit_stat[1])) {
    fit_stat <- tryCatch(fixest::r2(fit, "pr2"), error = function(e) NA_real_)
  }
  ct <- as.data.table(coeftable(fit), keep.rownames = "term")
  setnames(ct, c("term", "estimate", "std_error", "t_value", "p_value"))
  ct[, estimate  := round(estimate, 6)]
  ct[, std_error := round(std_error, 6)]
  ct[, t_value   := round(t_value, 2)]
  ct[, p_value   := round(p_value, 4)]
  ct[, spec := spec_label]
  ct[, n := fit$nobs]
  ct[, r2 := round(fit_stat[1], 4)]
  ct
}

clean_spec_labels <- function(specs) {
  label_map <- c(
    "full"                         = "Full sample",
    "full_wtd"                     = "Full (unit-wtd)",
    "1unit"                        = "1-unit buildings",
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
    "allow_repeat"                 = "Allow repeat-transfer PIDs",
    "no_close_overlap"             = "No nearby re-transfer",
    "count_outcome"                = "Count outcome",
    "any_filing"                   = "Any filing",
    "port_units_0_solo"            = "0 other units (solo)",
    "port_units_1_9"               = "1-9 other units",
    "port_units_10_49"             = "10-49 other units",
    "port_units_50plus"            = "50+ other units",
    "violations"                   = "Violations",
    "violations_rate"              = "Violations rate",
    "pct_black_tract_year"         = "Black",
    "pct_asian_tract_year"         = "Asian",
    "pct_white_tract_year"         = "White",
    "pct_latino_tract_year"        = "Latino",
    "pct_other_tract_year"         = "Other",
    "occupancy_rate_tract_year"    = "Occupancy rate",
    "log_income_tract_year"        = "Log income",
    "rent_tract_year"              = "Tract x year FE",
    "rent_ctrl3"                   = "Controls with 3+ rent years",
    "rent_ctrl3_tract_year"        = "Ctrl 3+ + tract x year FE",
    "full_highfiler_portfolio_10plus_otherunits" = "High-filer",
    "full_lowfiler_portfolio_10plus_otherunits"  = "Low-filer",
    "full_smallportfolio_lt10_otherunits"        = "Small portfolio",
    "highfiler_portfolio_10plus_otherunits"      = "High-filer",
    "lowfiler_portfolio_10plus_otherunits"       = "Low-filer",
    "smallportfolio_lt10_otherunits"             = "Small portfolio",
    "full_singlepurchase"          = "Single-purchase",
    "stacked_full_wtd"             = "Stacked full (wtd)",
    "stacked_2plus_wtd"            = "Stacked 2+ (wtd)"
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
plot_event_coefs <- function(coef_dt, specs, title, filename, facet = FALSE,
                             y_label = fp$y_label, ncol = NULL,
                             custom_labels = NULL, colors = NULL,
                             show_n_in_labels = TRUE) {
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
  if ("treated_pids" %in% names(coef_dt)) {
    treated_lookup <- unique(coef_dt[spec %in% specs, .(spec, treated_pids)])
    treated_lookup <- treated_lookup[, .(treated_pids = treated_pids[1]), by = spec]
    n_lookup <- merge(n_lookup, treated_lookup, by = "spec", all.x = TRUE)
  }
  if (!is.null(custom_labels)) {
    n_lookup[spec %in% names(custom_labels), clean_label := unname(custom_labels[spec])]
  }
  plot_dt <- merge(plot_dt, n_lookup, by = "spec", all.x = TRUE, suffixes = c("", "_lkp"))
  plot_dt[, spec_raw := spec]
  if (show_n_in_labels && facet && "treated_pids" %in% names(plot_dt)) {
    plot_dt[, spec_label := fifelse(
      !is.na(treated_pids),
      paste0(
        clean_label, "\nN=", formatC(n_lkp, format = "d", big.mark = ","),
        "; treated PIDs=", formatC(treated_pids, format = "d", big.mark = ",")
      ),
      paste0(clean_label, "\nN=", formatC(n_lkp, format = "d", big.mark = ","))
    )]
  } else if (show_n_in_labels) {
    plot_dt[, spec_label := paste0(clean_label, " (N=", formatC(n_lkp, format = "d", big.mark = ","), ")")]
  } else {
    plot_dt[, spec_label := clean_label]
  }
  spec_levels <- unique(plot_dt[, .(spec_raw, spec_label)])[match(specs, spec_raw), spec_label]
  plot_dt[, et := as.integer(str_extract(term, "-?\\d+"))]
  # Add reference point
  ref_rows <- data.table(
    et = fp$ref_period, estimate = 0, std_error = 0,
    spec_raw = specs,
    spec_label = spec_levels
  )
  plot_dt <- rbindlist(list(
    plot_dt[, .(et, estimate, std_error, spec_raw, spec_label)],
    ref_rows
  ), fill = TRUE)
  plot_dt[, spec_label := factor(spec_label, levels = spec_levels)]
  setorder(plot_dt, spec_label, et)
  single_spec <- length(spec_levels) == 1L
  facet_ncol <- if (is.null(ncol)) {
    if (length(spec_levels) <= 4L) 2L else 3L
  } else {
    ncol
  }

  if (facet) {
    p <- ggplot(plot_dt, aes(x = et, y = estimate)) +
      geom_hline(yintercept = 0, color = "gray60") +
      geom_vline(xintercept = fp$vline_x, linetype = "dashed", color = "gray40") +
      geom_point(size = 2, color = "steelblue") +
      geom_errorbar(aes(ymin = estimate - 1.96 * std_error,
                        ymax = estimate + 1.96 * std_error),
                    width = 0.2, color = "steelblue") +
      facet_wrap(~spec_label, scales = "fixed", ncol = facet_ncol,
                 labeller = label_wrap_gen(width = 25)) +
      scale_x_continuous(breaks = fp$x_breaks)
  } else if (single_spec) {
    p <- ggplot(plot_dt, aes(x = et, y = estimate)) +
      geom_hline(yintercept = 0, color = "gray60") +
      geom_vline(xintercept = fp$vline_x, linetype = "dashed", color = "gray40") +
      geom_point(size = 2.3, color = "steelblue") +
      geom_errorbar(aes(ymin = estimate - 1.96 * std_error,
                        ymax = estimate + 1.96 * std_error),
                    width = 0.2, color = "steelblue") +
      scale_x_continuous(breaks = fp$x_breaks)
  } else {
    p <- ggplot(plot_dt, aes(x = et, y = estimate, color = spec_label)) +
      geom_hline(yintercept = 0, color = "gray60") +
      geom_vline(xintercept = fp$vline_x, linetype = "dashed", color = "gray40") +
      geom_point(position = position_dodge(width = 0.3), size = 2.5) +
      geom_errorbar(aes(ymin = estimate - 1.96 * std_error,
                        ymax = estimate + 1.96 * std_error),
                    position = position_dodge(width = 0.3), width = 0.2) +
      scale_x_continuous(breaks = fp$x_breaks)
    if (!is.null(colors)) {
      label_map_colors <- unique(plot_dt[, .(spec_raw, spec_label)])
      color_values <- unname(colors[label_map_colors$spec_raw])
      names(color_values) <- as.character(label_map_colors$spec_label)
      p <- p + scale_color_manual(values = color_values, breaks = spec_levels)
    }
  }
  p <- p +
    labs(title = title, subtitle = paste0("Reference period: ", fp$et_var, " = ", fp$ref_period),
         x = fp$x_label, y = y_label, color = "Specification") +
    theme_philly_evict() +
    theme(
      strip.text = element_text(size = 10, lineheight = 0.95),
      plot.title = element_text(size = 15, face = "bold"),
      plot.subtitle = element_text(size = 10, face = "plain"),
      axis.title.x = element_text(size = 11, margin = margin(t = 8)),
      axis.title.y = element_text(size = 11),
      axis.text = element_text(size = 9),
      legend.title = element_text(size = 10, color = "black"),
      legend.text = element_text(size = 9),
      legend.position = if (facet || single_spec) "none" else "bottom",
      panel.grid.minor = element_blank()
    )
  if (!facet && !single_spec) {
    p <- p + guides(color = guide_legend(nrow = 2, byrow = TRUE))
  }
  plot_width <- if (facet) 13.5 else 10.5
  plot_height <- if (facet) if (length(spec_levels) <= 4L) 7.5 else 9 else 6.5
  ggsave(filename, p, width = plot_width, height = plot_height, dpi = 150)
  logf("  Saved ", basename(filename), log_file = log_file)
}

# Raw event study plot
# weight_var: if non-NULL, compute weighted means by that column (e.g. "total_units")
plot_raw_event_study <- function(dt, group_var = NULL, title, filename,
                                 ylab = fp$y_label, weight_var = NULL) {
  et_var_sym <- fp$et_var
  if (is.null(group_var)) {
    if (!is.null(weight_var)) {
      means <- dt[!is.na(get(fp$outcome_var)) & !is.na(get(weight_var)) & get(weight_var) > 0, .(
        mean_rate = weighted.mean(get(fp$outcome_var), w = get(weight_var), na.rm = TRUE),
        se = sd(get(fp$outcome_var), na.rm = TRUE) / sqrt(.N),
        n = .N
      ), by = et_var_sym]
    } else {
      means <- dt[!is.na(get(fp$outcome_var)), .(
        mean_rate = mean(get(fp$outcome_var), na.rm = TRUE),
        se = sd(get(fp$outcome_var), na.rm = TRUE) / sqrt(.N),
        n = .N
      ), by = et_var_sym]
    }
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
    theme_philly_evict() +
    theme(
      strip.text = element_text(size = 10, lineheight = 0.95),
      plot.title = element_text(size = 15, face = "bold"),
      plot.subtitle = element_text(size = 10, face = "plain"),
      axis.title.x = element_text(size = 11, margin = margin(t = 8)),
      axis.title.y = element_text(size = 11),
      axis.text = element_text(size = 9),
      legend.title = element_text(size = 10, color = "black"),
      legend.text = element_text(size = 9),
      legend.position = if (is.null(group_var)) "none" else "bottom",
      panel.grid.minor = element_blank()
    )
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
rtt_pre1800_n <- rtt[!is.na(year) & year < 1800L, .N]
rtt_oow_n <- rtt[is.na(year) | year < ANALYSIS_YEAR_MIN | year > ANALYSIS_YEAR_MAX, .N]
if (rtt_oow_n > 0L) {
  logf("  RTT rows outside analysis window [", ANALYSIS_YEAR_MIN, ", ", ANALYSIS_YEAR_MAX, "]: ",
       rtt_oow_n, " (including ", rtt_pre1800_n, " rows before 1800)",
       log_file = log_file)
}
rtt <- rtt[!is.na(year) & year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX]
logf("  rtt_clean in analysis window: ", nrow(rtt), " rows, ",
     rtt[, uniqueN(PID)], " unique PIDs", log_file = log_file)

bldg <- fread(p_product(cfg, "bldg_panel_blp"))
bldg[, PID := normalize_pid(PID)]
bldg[, year := as.integer(year)]
logf("  bldg_panel_blp: ", nrow(bldg), " rows, ", bldg[, uniqueN(PID)], " unique PIDs",
     log_file = log_file)
bldg_pre1800_n <- bldg[!is.na(year) & year < 1800L, .N]
bldg_oow_n <- bldg[is.na(year) | year < ANALYSIS_YEAR_MIN | year > ANALYSIS_YEAR_MAX, .N]
if (bldg_oow_n > 0L) {
  logf("  bldg_panel_blp rows outside analysis window [", ANALYSIS_YEAR_MIN, ", ",
       ANALYSIS_YEAR_MAX, "]: ", bldg_oow_n, " (including ", bldg_pre1800_n,
       " rows before 1800)", log_file = log_file)
}
bldg <- bldg[!is.na(year) & year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX]
logf("  bldg_panel_blp in analysis window: ", nrow(bldg), " rows, ",
     bldg[, uniqueN(PID)], " unique PIDs", log_file = log_file)
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

# Entity-conglomerate xwalk: used to flag Phase 4 (business filing) merges
# and to derive modal owner_category per conglomerate (for NP/PHA/religious exclusion)
xwalk_ent_cong <- fread(p_product(cfg, "xwalk_entity_conglomerate"),
                        select = c("conglomerate_id", "link_method", "owner_category"))
phase4_cong_ids <- unique(xwalk_ent_cong[link_method == "business_filing", conglomerate_id])
logf("  Conglomerates with Phase 4 (business filing) merges: ", length(phase4_cong_ids),
     log_file = log_file)

# Modal owner_category per conglomerate
cong_cat_modal <- xwalk_ent_cong[!is.na(owner_category),
                                  .N, by = .(conglomerate_id, owner_category)
                                  ][order(-N)][, .SD[1L], by = conglomerate_id
                                  ][, .(conglomerate_id, buyer_owner_category = owner_category)]
logf("  Conglomerates with modal owner_category: ", nrow(cong_cat_modal),
     log_file = log_file)
rm(xwalk_ent_cong)

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

  # Analysis-window restriction
  ev_oow_n <- ev[filing_year < ANALYSIS_YEAR_MIN | filing_year > ANALYSIS_YEAR_MAX, .N]
  if (ev_oow_n > 0L) {
    logf("  Eviction rows outside analysis window [", ANALYSIS_YEAR_MIN, ", ",
         ANALYSIS_YEAR_MAX, "]: ", ev_oow_n, log_file = log_file)
  }
  ev <- ev[filing_year >= ANALYSIS_YEAR_MIN & filing_year <= ANALYSIS_YEAR_MAX]
  logf("  Evictions linked to PID in analysis window: ", nrow(ev), " filings", log_file = log_file)

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
pid_transfer_counts <- transfers[, .(n_transfers_pid = .N), by = PID]
single_transfer_pids <- pid_transfer_counts[n_transfers_pid == 1L, PID]
multi_transfer_pids <- pid_transfer_counts[n_transfers_pid > 1L, PID]
logf("  Single-transfer PIDs in analysis window: ", length(single_transfer_pids),
     "; repeat-transfer PIDs: ", length(multi_transfer_pids), log_file = log_file)

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

# Portfolio: join transfers to conglomerate_id + total_units_conglomerate_yr via (PID, year),
# then to portfolio stats.
transfers <- merge(transfers,
  xwalk_cong[, .(PID, year, conglomerate_id, total_units_conglomerate_yr)],
  by = c("PID", "year"), all.x = TRUE)
portfolio_cong <- merge(portfolio_cong, cong_cat_modal, by = "conglomerate_id", all.x = TRUE)
transfers <- merge(transfers,
  portfolio_cong[, .(conglomerate_id, n_properties_total, portfolio_bin, buyer_owner_category)],
  by = "conglomerate_id", all.x = TRUE)

# Flag transfers whose conglomerate was extended by Phase 4 (business filing linkage)
transfers[, phase4_conglomerate := !is.na(conglomerate_id) & conglomerate_id %in% phase4_cong_ids]

logf("  Owner linkage match rate (entity): ",
     round(100 * transfers[!is.na(owner_group_id), .N] / nrow(transfers), 1),
     "%; conglomerate coverage: ",
     round(100 * transfers[!is.na(conglomerate_id), .N] / nrow(transfers), 1), "%",
     log_file = log_file)
logf("  Transfers in Phase 4-expanded conglomerates: ",
     transfers[phase4_conglomerate == TRUE, .N], " (",
     round(100 * transfers[phase4_conglomerate == TRUE, .N] / nrow(transfers), 1), "%)",
     log_file = log_file)

transfers[, buyer_type := fifelse(is_corp == TRUE, "Corporate", "Individual")]

# --- Acquirer filing behavior classification (full-period LOO) ---
# Uses compute_loo_filing_type() from helper-functions.R with the full building
# panel. Landlord type is treated as a structural attribute, consistent with
# retaliatory-evictions.r. One row per (PID, conglomerate_id) — same
# classification regardless of transfer year.
bldg_loo <- merge(
  bldg[!is.na(total_units) & total_units > 0,
       .(PID, year,
         num_filings = fifelse(is.na(num_filings), 0L, as.integer(num_filings)),
         total_units)],
  xwalk_cong[, .(PID, year, conglomerate_id)],
  by = c("PID", "year")
)

n_analysis_years <- bldg_loo[, max(year) - min(year) + 1L]
logf("  bldg_loo panel span: ", bldg_loo[, min(year)], "-", bldg_loo[, max(year)],
     " (", n_analysis_years, " years) | loo_min_unit_years = ",
     LOO_MIN_UNIT_YEARS, log_file = log_file)

loo_type <- compute_loo_filing_type(
  pid_yr_dt            = bldg_loo,
  loo_min_unit_years   = LOO_MIN_UNIT_YEARS,
  loo_filing_threshold = ACQ_HIGH_FILER_THRESHOLD,
  max_filing_rate      = MAX_ALLOWABLE_FILING_RATE
)

# Map helper levels to ACQ_BIN_LABELS
loo_type[, acq_filer_bin := fcase(
  portfolio_evict_group_cong == "High-evicting portfolio", ACQ_BIN_LABELS[1],
  portfolio_evict_group_cong == "Low-evicting portfolio",  ACQ_BIN_LABELS[2],
  portfolio_evict_group_cong == "Small portfolio",         ACQ_BIN_LABELS[3],
  default                                                = "Single-purchase"
)]
# Retain LOO rate and unit-years for descriptives.
# `other_units_transfer_year` is a legacy column name kept for downstream compatibility.
setnames(loo_type, c("loo_rate_cong", "loo_units_cong"),
                   c("acq_rate",       "other_units_transfer_year"))

acq_rate_pool <- loo_type[acq_filer_bin %in% ACQ_BIN_LABELS[1:2] & !is.na(acq_rate) & acq_rate > 0,
                           acq_rate]
acq_threshold_quantile <- if (length(acq_rate_pool) > 0L) mean(acq_rate_pool <= ACQ_HIGH_FILER_THRESHOLD, na.rm = TRUE) else NA_real_

acq_cutoffs <- data.table(
  analysis_year_min                 = ANALYSIS_YEAR_MIN,
  analysis_year_max                 = ANALYSIS_YEAR_MAX,
  meaningful_loo_unit_years_cutoff  = LOO_MIN_UNIT_YEARS,
  high_filer_rate_cutoff            = ACQ_HIGH_FILER_THRESHOLD,
  filing_rate_cap                   = MAX_ALLOWABLE_FILING_RATE,
  high_filer_rate_quantile          = acq_threshold_quantile,
  n_positive_meaningful_portfolio   = length(acq_rate_pool)
)
fwrite(acq_cutoffs, out_path("acq_cutoffs.csv"))

logf("  Acquirer classification: full-period LOO (", ANALYSIS_YEAR_MIN, "-", ANALYSIS_YEAR_MAX, ")",
     log_file = log_file)
logf("  Small-portfolio screen: <", LOO_MIN_UNIT_YEARS,
     " leave-one-out unit-years",
     log_file = log_file)
logf("  High-filer threshold: ", ACQ_HIGH_FILER_THRESHOLD, " filings/unit-year",
     log_file = log_file)
logf("  Filing-rate cap for transfer event studies and LOO bins: ",
     MAX_ALLOWABLE_FILING_RATE, " filings/unit-year",
     log_file = log_file)

acq_lookup <- loo_type[, .(PID, conglomerate_id, acq_filer_bin, acq_rate,
                            other_units_transfer_year)]

logf("  Acquirer bin distribution:", log_file = log_file)
for (ab in ACQ_BIN_LABELS) {
  logf("    ", ab, ": ", acq_lookup[acq_filer_bin == ab, .N], " PIDs",
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
               "total_complaints", "total_permits", "log_med_rent", "log_med_rent_safe",
               "infousa_pct_asian", "infousa_pct_black", "infousa_pct_hispanic",
               "infousa_pct_female", "infousa_pct_male", "infousa_pct_other",
               "infousa_pct_white",
               "infousa_find_mean_k", "occupancy_rate",
               "med_rent", "GEOID", "building_type", "num_units_bin",
               "rental_from_license", "rental_from_altos", "rental_from_evict",
               "ever_rental_any_year", "ever_rental_any_year_ever")
bldg_cols <- intersect(bldg_cols, names(bldg))
bldg_sub <- unique(bldg[, ..bldg_cols], by = c("PID", "year"))
if (!"log_med_rent_safe" %in% names(bldg_sub))
  stop("log_med_rent_safe missing from bldg_panel_blp — re-run make-analytic-sample.R")

if (freq == "annual") {
  event_panel <- merge(event_grid, bldg_sub, by = c("PID", "year"), all.x = TRUE)
} else {
  # Quarterly: merge filing counts first, then annual building data
  event_panel <- merge(event_grid, ev_q, by = c("PID", "year_quarter"), all.x = TRUE)
  event_panel[is.na(num_filings_q), num_filings_q := 0L]
  event_panel <- merge(event_panel, bldg_sub, by = c("PID", "year"), all.x = TRUE)
}
logf("  Event panel after outcome merge: ", nrow(event_panel), " rows", log_file = log_file)

# --- Join neighborhood gentrification status (from analyze-nhood-portfolio-transitions.R) ---
nhood_port_dir    <- p_out(cfg, "nhood_portfolio")
gent_status_file  <- file.path(nhood_port_dir, "tract_gent_status.csv")
if (file.exists(gent_status_file)) {
  gent_lu <- fread(gent_status_file)
  gent_lu[, tract_fips := as.character(tract_fips)]
  # GEOID in event_panel is BG-level (12 chars); first 11 = tract FIPS
  event_panel[, tract_fips_evt := fifelse(
    !is.na(GEOID) & nchar(as.character(GEOID)) >= 11L,
    substr(as.character(GEOID), 1L, 11L), NA_character_
  )]
  event_panel <- merge(
    event_panel,
    gent_lu[, .(tract_fips, nhood_gent = as.character(gent_status))],
    by.x = "tract_fips_evt", by.y = "tract_fips", all.x = TRUE
  )
  event_panel[, nhood_gent := factor(
    nhood_gent, levels = c("Gentrified", "Did not gentrify", "Not gentrifiable")
  )]
  logf("  nhood_gent joined: ",
       event_panel[!is.na(nhood_gent), uniqueN(PID)], " PIDs with status | ",
       event_panel[, .N, keyby = nhood_gent], log_file = log_file)
} else {
  logf("  WARNING: tract_gent_status.csv not found in nhood_portfolio/ — run ",
       "analyze-nhood-portfolio-transitions.R first to enable nhood event studies",
       log_file = log_file)
  event_panel[, nhood_gent      := NA_character_]
  event_panel[, tract_fips_evt  := NA_character_]
}

# Compute filing rate
if (freq == "annual") {
  event_panel[, filing_rate := fifelse(
    !is.na(num_filings) & !is.na(total_units) & total_units > 0,
    pmin(num_filings / total_units, MAX_ALLOWABLE_FILING_RATE), NA_real_
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
                                  owner_group_id, conglomerate_id, phase4_conglomerate,
                                  n_properties_total, portfolio_bin,
                                  buyer_owner_category,
                                  total_units_conglomerate_yr,
                                  total_consideration)]
  event_panel <- merge(event_panel, transfer_chars,
                       by = c("PID", "transfer_year"), all.x = TRUE)
} else {
  transfer_chars <- transfers[, .(PID, transfer_year_quarter,
                                  buyer_type, is_corp, is_sheriff_deed,
                                  owner_group_id, conglomerate_id, phase4_conglomerate,
                                  n_properties_total, portfolio_bin,
                                  buyer_owner_category,
                                  total_units_conglomerate_yr,
                                  total_consideration)]
  event_panel <- merge(event_panel, transfer_chars,
                       by = c("PID", "transfer_year_quarter"), all.x = TRUE)
}

# Merge acquirer classification — full-period LOO is transfer-year independent;
# merge by (PID, conglomerate_id), which event_panel carries from transfer_chars.
if (nrow(acq_lookup) > 0) {
  event_panel <- merge(event_panel, acq_lookup,
                       by = c("PID", "conglomerate_id"), all.x = TRUE)
} else {
  event_panel[, acq_filer_bin            := NA_character_]
  event_panel[, acq_rate                 := NA_real_]
  event_panel[, other_units_transfer_year := NA_real_]
}

# Portfolio units net of this building (units in OTHER portfolio buildings at acquisition)
event_panel[, portfolio_units_net := fcase(
  is.na(total_units_conglomerate_yr), NA_integer_,
  total_units_conglomerate_yr > coalesce(total_units, 0L),
    as.integer(total_units_conglomerate_yr - coalesce(total_units, 0L)),
  default = 0L
)]
event_panel[, portfolio_units_bin := fcase(
  is.na(portfolio_units_net),          NA_character_,
  portfolio_units_net == 0L,           "0 units (solo)",
  portfolio_units_net < 10L,           "1-9 other units",
  portfolio_units_net < 50L,           "10-49 other units",
  portfolio_units_net >= 50L,          "50+ other units",
  default = NA_character_
)]

# Derived outcome rates for Section 5 event studies
event_panel[, permit_rate    := fifelse(!is.na(total_permits)    & !is.na(total_units) & total_units > 0,
                                         total_permits    / total_units, NA_real_)]
event_panel[, complaint_rate := fifelse(!is.na(total_complaints) & !is.na(total_units) & total_units > 0,
                                         total_complaints / total_units, NA_real_)]
# log household income: infousa_find_mean_k is predicted income in $1000s
event_panel[, log_infousa_income := fifelse(!is.na(infousa_find_mean_k) & infousa_find_mean_k > 0,
                                             log(infousa_find_mean_k), NA_real_)]
event_panel <- add_tract_year_fe(event_panel)

# Race-share accounting diagnostic
race_share_cols <- c("infousa_pct_asian", "infousa_pct_black", "infousa_pct_hispanic",
                     "infousa_pct_white", "infousa_pct_other")
if (all(race_share_cols %in% names(bldg_sub))) {
  race_diag_dt <- bldg_sub[complete.cases(bldg_sub[, ..race_share_cols])]
  if (nrow(race_diag_dt) > 0) {
    race_diag_dt[, race_share_sum := rowSums(.SD), .SDcols = race_share_cols]
    race_diag <- race_diag_dt[, .(
      n_complete = .N,
      mean_abs_dev = mean(abs(race_share_sum - 1)),
      p99_abs_dev = quantile(abs(race_share_sum - 1), 0.99),
      max_abs_dev = max(abs(race_share_sum - 1)),
      n_gt_0_001 = sum(abs(race_share_sum - 1) > 0.001),
      n_gt_0_01 = sum(abs(race_share_sum - 1) > 0.01)
    )]
    fwrite(race_diag, out_path("race_share_sum_diag.csv"))
    logf("  Wrote race_share_sum_diag.csv (n_complete=", race_diag$n_complete[1],
         ", max abs dev=", signif(race_diag$max_abs_dev[1], 3), ")",
         log_file = log_file)
  }
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
# Rental evidence: rental license records, Altos listings,
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
event_panel[, size_1_vs_2plus := fifelse(total_units == 1, "1 Unit",
                                         fifelse(!is.na(total_units) & total_units >= 2, "2+ Units", NA_character_))]

# Pre-COVID restriction for raw means
raw_dt <- event_panel[year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX]

plot_raw_event_study(raw_dt, NULL,
                     paste0("Eviction Filing Rate Around Ownership Transfer (", fp$label, ")"),
                     fig_path("raw.png"))
plot_raw_event_study(raw_dt[!is.na(total_units)], NULL,
                     paste0("Eviction Filing Rate Around Ownership Transfer — Unit-Weighted (", fp$label, ")"),
                     fig_path("raw_wtd.png"), weight_var = "total_units")

plot_raw_event_study(raw_dt[!is.na(buyer_type)], "buyer_type",
                     paste0("Filing Rate Around Transfer: by Buyer Type (", fp$label, ")"),
                     fig_path("raw_by_buyer.png"))

plot_raw_event_study(raw_dt[!is.na(portfolio_bin)], "portfolio_bin",
                     paste0("Filing Rate Around Transfer: by Portfolio Size (", fp$label, ")"),
                     fig_path("raw_by_portfolio.png"))

plot_raw_event_study(raw_dt[!is.na(size_label)], "size_label",
                     paste0("Filing Rate Around Transfer: by Building Size (", fp$label, ")"),
                     fig_path("raw_by_size.png"))

plot_raw_event_study(raw_dt[!is.na(size_1_vs_2plus)], "size_1_vs_2plus",
                     paste0("Filing Rate Around Transfer: 1 Unit vs 2+ Units (", fp$label, ")"),
                     fig_path("raw_by_1unit_2plus.png"))

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

reg_dt <- event_panel[!is.na(get(fp$outcome_var)) &
                        year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX]
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

if (freq != "annual") {
  # --- Baseline specs: Full, Full (unit-wtd), 1-unit, 2+, 5+, 2+ & Known Rental, 5+ & Known Rental ---
  # All use PID + year/yq FE (no BG×year trends)
  baseline_spec_list <- list(
    list(label = "full",               filter_expr = quote(TRUE),
         weights = NULL,               desc = "All buildings"),
    list(label = "full_wtd",           filter_expr = quote(!is.na(total_units)),
         weights = ~total_units,       desc = "All buildings (unit-weighted)"),
    list(label = "1unit",              filter_expr = quote(!is.na(total_units) & total_units == 1L),
         weights = NULL,               desc = "1-unit buildings"),
    list(label = "2plus",              filter_expr = quote(!is.na(total_units) & total_units >= 2L),
         weights = ~total_units,       desc = "2+ unit buildings (unit-weighted)"),
    list(label = "5plus",              filter_expr = quote(large_building == TRUE),
         weights = ~total_units,       desc = "5+ unit buildings (unit-weighted)"),
    list(label = "2plus_rental_pre",   filter_expr = quote(!is.na(total_units) & total_units >= 2L & known_rental_pre == TRUE),
         weights = ~total_units,       desc = "2+ units, pre-transfer rental (unit-weighted)"),
    list(label = "5plus_rental_pre",   filter_expr = quote(large_building == TRUE & known_rental_pre == TRUE),
         weights = ~total_units,       desc = "5+ units, pre-transfer rental (unit-weighted)")
  )

  for (bs in baseline_spec_list) {
    sub_dt <- reg_dt[eval(bs$filter_expr)]
    n_sub <- nrow(sub_dt)
    n_pids <- sub_dt[, uniqueN(PID)]
    logf("  Spec '", bs$label, "' (", bs$desc, "): N=", n_sub, ", PIDs=", n_pids,
         log_file = log_file)
    if (n_sub < 100 || n_pids < 10) next

    fit_fml <- if (freq == "annual") {
      filing_rate ~ i(event_time, ref = ET_REF) | PID + year
    } else {
      filing_rate_q ~ i(q_relative, ref = -4) | PID + yq
    }
    fit_args <- list(fml = fit_fml, data = sub_dt, cluster = ~PID)
    if (!is.null(bs$weights)) fit_args$weights <- bs$weights
    fit <- do.call(feols, fit_args)
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
} else {
  logf("  Annual run: baseline outputs handled by streamlined annual block below", log_file = log_file)
}

# Main heterogeneity sample: full panel, requires non-NA total_units for unit-weighting.
# All heterogeneity sections (4a–4f, 4g, 5, 6) use this sample with weights = ~total_units.
reg_dt_wtd <- reg_dt[!is.na(total_units)]
logf("  Heterogeneity sample (reg_dt_wtd, unit-weighted): ", nrow(reg_dt_wtd), " rows, ",
     reg_dt_wtd[, uniqueN(PID)], " PIDs", log_file = log_file)

# ============================================================
# Canonical annual event-study panel: first transfer + never-sold controls
# ============================================================
# Annual results are re-centered on a full building-year panel with:
# - treated units = buildings with exactly one observed sale in the RTT data window
# - controls = buildings never sold in the RTT data window
# - sample window fixed at ANALYSIS_YEAR_MIN:ANALYSIS_YEAR_MAX
run_fullpanel_fit <- function(panel, fml_str, spec_label, weights = ~total_units) {
  if (nrow(panel) < 100 || panel[, uniqueN(PID)] < 10) {
    logf("    ", spec_label, ": skipping (N=", nrow(panel), ")", log_file = log_file)
    return(NULL)
  }
  treated_pids_n <- panel[!is.na(transfer_year), uniqueN(PID)]
  treated_rows_n <- panel[!is.na(transfer_year), .N]
  treated_event_window_rows_n <- panel[
    !is.na(transfer_year) &
      !is.na(event_time) &
      event_time >= min(fp$et_range) &
      event_time <= max(fp$et_range),
    .N
  ]
  control_pids_n <- panel[is.na(transfer_year), uniqueN(PID)]
  fit_args <- list(fml = as.formula(fml_str), data = panel, cluster = ~PID)
  if (!is.null(weights)) fit_args$weights <- weights
  fit <- do.call(feols, fit_args)
  ct <- extract_feols_coefs(fit, spec_label)
  ct[, treated_pids := treated_pids_n]
  ct[, treated_rows := treated_rows_n]
  ct[, treated_event_rows := treated_event_window_rows_n]
  ct[, control_pids := control_pids_n]
  ct[, term := gsub("^et_m", paste0(fp$et_var, "::-"), term)]
  ct[, term := gsub("^et_", paste0(fp$et_var, "::"), term)]
  logf("    ", spec_label, ": N=", fit$nobs, ", PIDs=", panel[, uniqueN(PID)],
       ", treated PIDs=", treated_pids_n, ", treated rows=", treated_rows_n,
       log_file = log_file)
  list(coefs = ct, fit = fit)
}

run_fullpanel_fit_poisson <- function(panel, fml_str, spec_label,
                                      offset = ~log(total_units),
                                      weights = ~total_units) {
  if (nrow(panel) < 100 || panel[, uniqueN(PID)] < 10) {
    logf("    ", spec_label, ": skipping (N=", nrow(panel), ")", log_file = log_file)
    return(NULL)
  }
  treated_pids_n <- panel[!is.na(transfer_year), uniqueN(PID)]
  treated_rows_n <- panel[!is.na(transfer_year), .N]
  treated_event_window_rows_n <- panel[
    !is.na(transfer_year) &
      !is.na(event_time) &
      event_time >= min(fp$et_range) &
      event_time <= max(fp$et_range),
    .N
  ]
  control_pids_n <- panel[is.na(transfer_year), uniqueN(PID)]
  fit_args <- list(fml = as.formula(fml_str), data = panel, cluster = ~PID, offset = offset)
  if (!is.null(weights)) fit_args$weights <- weights
  fit <- do.call(fepois, fit_args)
  ct <- extract_feols_coefs(fit, spec_label)
  ct[, treated_pids := treated_pids_n]
  ct[, treated_rows := treated_rows_n]
  ct[, treated_event_rows := treated_event_window_rows_n]
  ct[, control_pids := control_pids_n]
  ct[, term := gsub("^et_m", paste0(fp$et_var, "::-"), term)]
  ct[, term := gsub("^et_", paste0(fp$et_var, "::"), term)]
  logf("    ", spec_label, ": N=", fit$nobs, ", PIDs=", panel[, uniqueN(PID)],
       ", treated PIDs=", treated_pids_n, ", treated rows=", treated_rows_n,
       " [Poisson offset]", log_file = log_file)
  list(coefs = ct, fit = fit)
}

build_weighted_stacked_panel <- function(panel, outcome_col,
                                         event_window = -3L:3L,
                                         ref_period = ET_REF,
                                         label = "stacked_panel") {
  req_cols <- c("PID", "year", "transfer_year", "total_units", outcome_col)
  assert_has_cols(panel, req_cols, paste0(label, ": panel"))
  assert_unique(panel, c("PID", "year"), paste0(label, ": panel PID-year"))

  if (!ref_period %in% event_window) {
    stop(label, ": ref_period must be inside event_window")
  }
  if (length(event_window) < 2L) {
    stop(label, ": event_window must contain at least two event times")
  }

  base <- copy(panel)[
    year >= ANALYSIS_YEAR_MIN &
      year <= ANALYSIS_YEAR_MAX &
      !is.na(total_units) &
      total_units > 0 &
      !is.na(get(outcome_col))
  ]
  assert_unique(base, c("PID", "year"), paste0(label, ": filtered base PID-year"))

  treated_lookup <- unique(base[!is.na(transfer_year), .(PID, transfer_year)])
  assert_unique(treated_lookup, "PID", paste0(label, ": treated lookup PID"))
  control_lookup <- unique(base[is.na(transfer_year), .(PID)])
  assert_unique(control_lookup, "PID", paste0(label, ": control lookup PID"))

  trimmed_cohorts <- sort(unique(
    treated_lookup[
      transfer_year + min(event_window) >= ANALYSIS_YEAR_MIN &
        transfer_year + max(event_window) <= ANALYSIS_YEAR_MAX,
      transfer_year
    ]
  ))
  if (length(trimmed_cohorts) == 0L) {
    stop(label, ": no treated cohorts survive the trimmed event window")
  }

  stack_parts <- vector("list", length(trimmed_cohorts))
  cohort_meta <- vector("list", length(trimmed_cohorts))

  for (i in seq_along(trimmed_cohorts)) {
    cohort_year <- trimmed_cohorts[i]
    cohort_treated <- treated_lookup[transfer_year == cohort_year, PID]
    if (length(cohort_treated) == 0L) {
      stop(label, ": empty treated cohort for year ", cohort_year)
    }

    years_use <- cohort_year + event_window
    sub <- base[
      year %in% years_use &
        (PID %chin% cohort_treated | is.na(transfer_year))
    ]
    sub[, cohort_year := cohort_year]
    sub[, treated := as.integer(PID %chin% cohort_treated)]
    sub[, rel_time := year - cohort_year]
    sub <- sub[rel_time %in% event_window]
    assert_unique(
      sub,
      c("PID", "year", "cohort_year"),
      paste0(label, ": stacked cohort PID-year ", cohort_year)
    )
    sub[, pid_cohort := paste0(PID, "_", cohort_year)]
    sub[, yr_cohort := paste0(year, "_", cohort_year)]

    stack_parts[[i]] <- sub
    cohort_meta[[i]] <- data.table(
      cohort_year = cohort_year,
      treated_pids = uniqueN(sub[treated == 1L, PID]),
      control_pids = uniqueN(sub[treated == 0L, PID]),
      treated_rows = sub[treated == 1L, .N],
      control_rows = sub[treated == 0L, .N],
      treated_unit_weight = sub[treated == 1L, sum(total_units, na.rm = TRUE)],
      control_unit_weight = sub[treated == 0L, sum(total_units, na.rm = TRUE)]
    )
  }

  stacked <- rbindlist(stack_parts, use.names = TRUE, fill = TRUE)
  cohort_weights <- rbindlist(cohort_meta, use.names = TRUE, fill = TRUE)
  assert_unique(cohort_weights, "cohort_year", paste0(label, ": cohort weights"))

  total_treated_pids <- cohort_weights[, sum(treated_pids)]
  total_control_pids <- cohort_weights[, sum(control_pids)]
  if (total_treated_pids <= 0L || total_control_pids <= 0L) {
    stop(label, ": invalid stacked totals for corrective weights")
  }

  cohort_weights[, q_weight_control := (treated_pids / total_treated_pids) /
                                       (control_pids / total_control_pids)]
  cohort_weights[, q_weight_rule := "treated=1; controls=(cohort treated PID share)/(cohort control PID share)"]

  stacked <- merge(
    stacked,
    cohort_weights[, .(cohort_year, q_weight_control)],
    by = "cohort_year",
    all.x = TRUE
  )
  assert_unique(stacked, c("PID", "year", "cohort_year"), paste0(label, ": merged stacked PID-year"))
  if (stacked[is.na(q_weight_control), .N] > 0L) {
    stop(label, ": missing q_weight_control after merge")
  }

  stacked[, q_weight := fifelse(treated == 1L, 1, q_weight_control)]
  stacked[, stack_weight := q_weight * total_units]
  if (stacked[!is.finite(stack_weight) | stack_weight <= 0, .N] > 0L) {
    stop(label, ": non-positive stacked weights detected")
  }

  for (k in setdiff(event_window, ref_period)) {
    nm <- paste0("st_et_", ifelse(k < 0L, paste0("m", abs(k)), k))
    stacked[, (nm) := as.integer(treated == 1L & rel_time == k)]
  }

  logf(
    "    ", label, ": built weighted stacked panel with ",
    nrow(stacked), " rows across ", length(trimmed_cohorts),
    " trimmed cohorts; treated PIDs=",
    uniqueN(stacked[treated == 1L, PID]),
    ", controls=", uniqueN(stacked[treated == 0L, PID]),
    log_file = log_file
  )

  list(
    panel = stacked,
    cohort_weights = cohort_weights,
    treated_pids = uniqueN(stacked[treated == 1L, PID]),
    control_pids = uniqueN(stacked[treated == 0L, PID]),
    n_cohorts = length(trimmed_cohorts)
  )
}

run_weighted_stacked_fit <- function(panel, outcome_col, spec_label,
                                     event_window = -3L:3L,
                                     ref_period = ET_REF) {
  built <- build_weighted_stacked_panel(
    panel = panel,
    outcome_col = outcome_col,
    event_window = event_window,
    ref_period = ref_period,
    label = spec_label
  )
  dummy_terms <- paste0(
    "st_et_",
    ifelse(setdiff(event_window, ref_period) < 0L,
           paste0("m", abs(setdiff(event_window, ref_period))),
           setdiff(event_window, ref_period))
  )
  fml_str <- paste0(outcome_col, " ~ ", paste(dummy_terms, collapse = " + "),
                    " | pid_cohort + yr_cohort")
  fit <- feols(
    as.formula(fml_str),
    data = built$panel,
    weights = ~stack_weight,
    cluster = ~PID
  )
  ct <- extract_feols_coefs(fit, spec_label)
  ct[, treated_pids := built$treated_pids]
  ct[, control_pids := built$control_pids]
  ct[, trimmed_cohorts := built$n_cohorts]
  ct[, term := gsub("^st_et_m", paste0(fp$et_var, "::-"), term)]
  ct[, term := gsub("^st_et_", paste0(fp$et_var, "::"), term)]

  list(coefs = ct, fit = fit, stacked = built$panel, cohort_weights = built$cohort_weights)
}

et_dummies_main <- paste0("et_", ifelse(setdiff(fp$et_range, fp$ref_period) < 0,
                                        paste0("m", abs(setdiff(fp$et_range, fp$ref_period))),
                                        setdiff(fp$et_range, fp$ref_period)))

build_never_sold_panel <- function(transfer_dt, bldg_dt, exclude_pids = character(),
                                   sample_filter = NULL, label = "panel") {
  req_transfer <- c("PID", "transfer_year")
  if (!all(req_transfer %in% names(transfer_dt))) {
    stop("build_never_sold_panel requires transfer_dt columns: ", paste(req_transfer, collapse = ", "))
  }
  assert_unique(transfer_dt, "PID", paste0(label, " transfer_dt PID"))
  assert_unique(bldg_dt, c("PID", "year"), paste0(label, " bldg_dt"))

  panel <- merge(bldg_dt, transfer_dt, by = "PID", all.x = TRUE)
  assert_unique(panel, c("PID", "year"), paste0(label, " merged panel"))
  if (length(exclude_pids) > 0L) {
    n_before <- nrow(panel)
    panel <- panel[!PID %chin% exclude_pids]
    logf("    ", label, ": dropped ", n_before - nrow(panel),
         " rows from future-treated PIDs outside the estimation window", log_file = log_file)
  }
  panel[, event_time := year - transfer_year]
  for (k in setdiff(fp$et_range, fp$ref_period)) {
    col_name <- paste0("et_", ifelse(k < 0, paste0("m", abs(k)), k))
    panel[, (col_name) := as.integer(event_time == k)]
    panel[is.na(event_time) | event_time < min(fp$et_range) | event_time > max(fp$et_range),
          (col_name) := 0L]
  }
  panel <- panel[year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX &
                   !is.na(total_units) & total_units > 0]
  if (!is.null(sample_filter)) {
    panel <- panel[eval(sample_filter)]
  }
  logf("    Built ", label, ": ", nrow(panel), " rows, ",
       panel[!is.na(transfer_year), uniqueN(PID)], " treated PIDs, ",
       panel[is.na(transfer_year), uniqueN(PID)], " never-sold control PIDs",
       log_file = log_file)
  panel
}

full_panel_main <- NULL
first_transfer_dt <- NULL
future_treated_pids <- character()
if (freq == "annual") {
  transfer_first_any <- copy(transfers)
  setorder(transfer_first_any, PID, year)
  transfer_first_any <- transfer_first_any[, .SD[1], by = PID]
  assert_unique(transfer_first_any, "PID", "transfer_first_any")

  first_transfer_attrs <- unique(
    event_panel[event_time == 0L,
                .(PID,
                  transfer_year,
                  buyer_type,
                  is_corp,
                  is_sheriff_deed,
                  acq_filer_bin,
                  acq_rate,
                  other_units_transfer_year,
                  portfolio_bin,
                  n_properties_total,
                  total_units_conglomerate_yr,
                  phase4_conglomerate,
                  total_consideration,
                  conglomerate_id,
                  buyer_owner_category)],
    by = c("PID", "transfer_year")
  )
  setorder(first_transfer_attrs, PID, transfer_year)
  first_transfer_attrs <- first_transfer_attrs[, .SD[1], by = PID]
  assert_unique(first_transfer_attrs, "PID", "first_transfer_attrs")
  first_transfer_loss_attrs <- unique(
    event_panel[event_time == 0L,
                .(PID, transfer_year, total_units, filing_rate)],
    by = c("PID", "transfer_year")
  )
  assert_unique(first_transfer_loss_attrs, c("PID", "transfer_year"), "first_transfer_loss_attrs")

  first_transfer_all_dt <- merge(
    transfer_first_any[year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX,
                       .(PID, transfer_year = year)],
    first_transfer_attrs,
    by = c("PID", "transfer_year"),
    all.x = TRUE
  )
  assert_unique(first_transfer_all_dt, "PID", "first_transfer_all_dt")
  first_transfer_dt <- first_transfer_all_dt[PID %chin% single_transfer_pids]
  assert_unique(first_transfer_dt, "PID", "first_transfer_dt")

  bldg_full <- copy(bldg_sub)
  bldg_full[, filing_rate := fifelse(
    !is.na(num_filings) & !is.na(total_units) & total_units > 0,
    pmin(num_filings / total_units, MAX_ALLOWABLE_FILING_RATE), NA_real_
  )]
  bldg_full[, permit_rate := fifelse(
    !is.na(total_permits) & !is.na(total_units) & total_units > 0,
    total_permits / total_units, NA_real_
  )]
  bldg_full[, complaint_rate := fifelse(
    !is.na(total_complaints) & !is.na(total_units) & total_units > 0,
    total_complaints / total_units, NA_real_
  )]
  bldg_full[, violations_rate := fifelse(
    !is.na(total_violations) & !is.na(total_units) & total_units > 0,
    total_violations / total_units, NA_real_
  )]
  bldg_full[, any_filing := as.integer(!is.na(num_filings) & num_filings > 0)]
  bldg_full[, entered_license := fifelse(!is.na(rental_from_license),
                                         as.integer(rental_from_license == TRUE),
                                         NA_integer_)]
  bldg_full[, entered_altos := fifelse(!is.na(rental_from_altos),
                                       as.integer(rental_from_altos == TRUE),
                                       NA_integer_)]
  bldg_full[, has_rental_evidence := fifelse(
    !is.na(rental_from_license) | !is.na(rental_from_altos) | !is.na(rental_from_evict),
    as.integer(
      coalesce(rental_from_license, FALSE) |
      coalesce(rental_from_altos, FALSE) |
      coalesce(rental_from_evict, FALSE)
    ),
    NA_integer_
  )]
  bldg_full[, log_infousa_income := fifelse(
    !is.na(infousa_find_mean_k) & infousa_find_mean_k > 0,
    log(infousa_find_mean_k), NA_real_
  )]
  if (!"ever_rental_any_year_ever" %in% names(bldg_full)) {
    stop("bldg_sub is missing required column ever_rental_any_year_ever for annual full-panel rental-stock restrictions")
  }
  bldg_full[, observed_rental_stock := ever_rental_any_year_ever == TRUE]
  bldg_full[, large_building := !is.na(total_units) & total_units >= 5L]
  bldg_full[, size_bin := fcase(
    is.na(total_units), NA_character_,
    total_units == 1L, "1 unit",
    total_units >= 2L & total_units <= 4L, "2-4 units",
    total_units >= 5L & total_units <= 19L, "5-19 units",
    total_units >= 20L, "20+ units",
    default = NA_character_
  )]
  bldg_full <- add_tract_year_fe(bldg_full)

  single_transfer_loss_dt <- merge(
    first_transfer_all_dt,
    first_transfer_loss_attrs,
    by = c("PID", "transfer_year"),
    all.x = TRUE
  )
  single_transfer_loss_dt <- merge(
    single_transfer_loss_dt,
    pid_transfer_counts,
    by = "PID",
    all.x = TRUE
  )
  single_transfer_loss_dt[, sample_group := fifelse(
    n_transfers_pid == 1L,
    "Kept: single-transfer treated PIDs",
    "Dropped: repeat-transfer treated PIDs"
  )]
  panel_rows_by_pid <- bldg_full[, .(panel_rows = .N), by = PID]
  single_transfer_loss_dt <- merge(single_transfer_loss_dt, panel_rows_by_pid, by = "PID", all.x = TRUE)
  single_transfer_loss <- single_transfer_loss_dt[, .(
    n_pids = .N,
    panel_rows = sum(panel_rows, na.rm = TRUE),
    mean_units = round(mean(total_units, na.rm = TRUE), 1),
    pct_corporate = round(100 * mean(is_corp == TRUE, na.rm = TRUE), 1),
    pct_sheriff = round(100 * mean(is_sheriff_deed == TRUE, na.rm = TRUE), 1),
    median_price = round(median(total_consideration, na.rm = TRUE), 0),
    mean_filing_rate = round(mean(filing_rate, na.rm = TRUE), 4)
  ), by = sample_group]
  single_transfer_loss[, pct_of_treated := round(100 * n_pids / sum(n_pids), 1)]
  setorder(single_transfer_loss, sample_group)
  fwrite(single_transfer_loss, out_path("single_transfer_sample_loss.csv"))
  logf("  Wrote single_transfer_sample_loss.csv", log_file = log_file)

  full_panel_main <- build_never_sold_panel(
    first_transfer_dt,
    bldg_full,
    exclude_pids = future_treated_pids,
    label = "main full panel"
  )
  logf("  Canonical annual full panel (single-transfer treated sample): ",
       full_panel_main[!is.na(transfer_year), uniqueN(PID)], " treated PIDs, ",
       full_panel_main[is.na(transfer_year), uniqueN(PID)], " never-sold controls, ",
       single_transfer_loss_dt[n_transfers_pid > 1L, uniqueN(PID)], " repeat-transfer treated PIDs dropped",
       log_file = log_file)
  full_panel_main[, portfolio_units_net := fcase(
    is.na(total_units_conglomerate_yr), NA_integer_,
    total_units_conglomerate_yr > coalesce(total_units, 0L),
      as.integer(total_units_conglomerate_yr - coalesce(total_units, 0L)),
    default = 0L
  )]
  full_panel_main[, portfolio_units_bin := fcase(
    is.na(portfolio_units_net), NA_character_,
    portfolio_units_net == 0L, "0 units (solo)",
    portfolio_units_net < 10L, "1-9 other units",
    portfolio_units_net < 50L, "10-49 other units",
    portfolio_units_net >= 50L, "50+ other units",
    default = NA_character_
  )]

  # Join nhood gentrification status onto full_panel_main via PID → GEOID → tract FIPS.
  # bldg_full has GEOID for all PIDs (treated + never-sold controls); gent_lu has tract-level status.
  if (exists("gent_lu") && "GEOID" %in% names(bldg_full)) {
    pid_gent_lu <- unique(bldg_full[, .(PID, GEOID)])[
      !is.na(GEOID) & nchar(as.character(GEOID)) >= 11L,
      .(PID, tract_fips_bldg = substr(as.character(GEOID), 1L, 11L))
    ]
    pid_gent_lu <- merge(pid_gent_lu,
      gent_lu[, .(tract_fips, nhood_gent_bldg = gent_status)],
      by.x = "tract_fips_bldg", by.y = "tract_fips", all.x = TRUE
    )
    pid_gent_lu <- unique(pid_gent_lu[, .(PID, nhood_gent_bldg)])
    full_panel_main <- merge(full_panel_main, pid_gent_lu, by = "PID", all.x = TRUE)
    full_panel_main[, nhood_gent := factor(
      as.character(nhood_gent_bldg),
      levels = c("Gentrified", "Did not gentrify", "Not gentrifiable")
    )]
    full_panel_main[, nhood_gent_bldg := NULL]
    logf("  nhood_gent joined to full_panel_main: ",
         full_panel_main[!is.na(nhood_gent), uniqueN(PID)], " PIDs with status",
         log_file = log_file)
  } else {
    full_panel_main[, nhood_gent := NA_character_]
  }

  # Filer/portfolio specs exclude NP/PHA/Religious/Government acquirers.
  # Controls (is.na(transfer_year)) and unmatched buyers (is.na(buyer_owner_category)) are kept.
  FOR_PROFIT_LANDLORD_CATS <- c("For-profit corp", "Person", "Trust")
  full_panel_filer <- full_panel_main[
    is.na(transfer_year) |
    is.na(buyer_owner_category) |
    buyer_owner_category %in% FOR_PROFIT_LANDLORD_CATS
  ]
  n_excl_treated <- full_panel_main[!is.na(transfer_year), uniqueN(PID)] -
                    full_panel_filer[!is.na(transfer_year), uniqueN(PID)]
  logf("  full_panel_filer: excluded ", n_excl_treated,
       " non-for-profit treated PIDs (NP/PHA/Religious/Government)",
       log_file = log_file)
}

if (freq != "annual") {

# ============================================================
# SECTION 4a: Heterogeneity by Buyer Type
# ============================================================
logf("--- SECTION 4a: Buyer type heterogeneity ---", log_file = log_file)

buyer_coefs <- list()
buyer_fits <- list()

if (freq == "annual") {
  # Interaction model (full sample, unit-weighted)
  fit_buyer_int <- feols(
    filing_rate ~ i(event_time, i.is_corp, ref = ET_REF) | PID + year,
    data = reg_dt_wtd[!is.na(is_corp)], cluster = ~PID, weights = ~total_units
  )
  buyer_coefs[[1]] <- extract_feols_coefs(fit_buyer_int, "interaction_full")

  # Split: corporate (full, unit-weighted)
  fit_corp <- feols(filing_rate ~ i(event_time, ref = ET_REF) | PID + year,
                    data = reg_dt_wtd[is_corp == TRUE], cluster = ~PID, weights = ~total_units)
  buyer_coefs[[2]] <- extract_feols_coefs(fit_corp, "corporate")
  buyer_fits[["Corporate"]] <- fit_corp

  fit_indiv <- feols(filing_rate ~ i(event_time, ref = ET_REF) | PID + year,
                     data = reg_dt_wtd[is_corp == FALSE], cluster = ~PID, weights = ~total_units)
  buyer_coefs[[3]] <- extract_feols_coefs(fit_indiv, "individual")
  buyer_fits[["Individual"]] <- fit_indiv

  # 5+ buildings split (unit-weighted)
  fit_corp_lg <- feols(filing_rate ~ i(event_time, ref = ET_REF) | PID + year,
                       data = reg_dt_wtd[is_corp == TRUE & large_building == TRUE],
                       cluster = ~PID, weights = ~total_units)
  buyer_coefs[[4]] <- extract_feols_coefs(fit_corp_lg, "corporate_large")
  buyer_fits[["Corp (5+)"]] <- fit_corp_lg

  fit_indiv_lg <- feols(filing_rate ~ i(event_time, ref = ET_REF) | PID + year,
                        data = reg_dt_wtd[is_corp == FALSE & large_building == TRUE],
                        cluster = ~PID, weights = ~total_units)
  buyer_coefs[[5]] <- extract_feols_coefs(fit_indiv_lg, "individual_large")
  buyer_fits[["Indiv (5+)"]] <- fit_indiv_lg

} else {
  # Quarterly: full sample, unit-weighted
  fit_corp <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                    data = reg_dt_wtd[is_corp == TRUE], cluster = ~PID, weights = ~total_units)
  buyer_coefs[[1]] <- extract_feols_coefs(fit_corp, "corporate")
  buyer_fits[["Corporate"]] <- fit_corp

  fit_indiv <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                     data = reg_dt_wtd[is_corp == FALSE], cluster = ~PID, weights = ~total_units)
  buyer_coefs[[2]] <- extract_feols_coefs(fit_indiv, "individual")
  buyer_fits[["Individual"]] <- fit_indiv

  reg_5plus <- reg_dt_wtd[large_building == TRUE]
  fit_corp5 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                     data = reg_5plus[is_corp == TRUE], cluster = ~PID, weights = ~total_units)
  buyer_coefs[[3]] <- extract_feols_coefs(fit_corp5, "corporate_5plus")
  buyer_fits[["Corp (5+)"]] <- fit_corp5

  fit_indiv5 <- feols(filing_rate_q ~ i(q_relative, ref = -4) | PID + yq,
                      data = reg_5plus[is_corp == FALSE], cluster = ~PID, weights = ~total_units)
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

all_size_bins <- c("1 unit", fp$size_bins)
for (sb in all_size_bins) {
  sub_dt <- reg_dt_wtd[size_bin == sb]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  Size bin '", sb, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

  if (n_sub < 100 || n_pids < 10) next

  fit_fml <- if (freq == "annual") {
    filing_rate ~ i(event_time, ref = ET_REF) | PID + year
  } else {
    filing_rate_q ~ i(q_relative, ref = -4) | PID + yq
  }
  fit_sb <- do.call(feols, list(fml = fit_fml, data = sub_dt,
                                cluster = ~PID, weights = ~total_units))
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
  for (sb in all_size_bins) {
    for (buyer_label in c("corp", "indiv")) {
      sub <- reg_dt_wtd[size_bin == sb & is_corp == (buyer_label == "corp")]
      n_sub <- nrow(sub)
      n_pids <- sub[, uniqueN(PID)]
      spec_name <- paste0(buyer_label, "_", sb)
      if (n_sub < 100 || n_pids < 10) next

      fit_cs <- feols(filing_rate ~ i(event_time, ref = ET_REF) | PID + year,
                      data = sub, cluster = ~PID, weights = ~total_units)
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

acq_bins_ordered <- ACQ_BIN_LABELS

acq_coefs <- list()
acq_fits <- list()

for (ab in acq_bins_ordered) {
  sub_dt <- reg_dt_wtd[acq_filer_bin == ab]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  Acquirer bin '", ab, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

  if (n_sub < 100 || n_pids < 10) next

  spec_name <- paste0("full_", acq_bin_suffix(ab))
  fit_fml <- if (freq == "annual") {
    filing_rate ~ i(event_time, ref = ET_REF) | PID + year
  } else {
    filing_rate_q ~ i(q_relative, ref = -4) | PID + yq
  }
  fit_ab <- do.call(feols, list(fml = fit_fml, data = sub_dt,
                                cluster = ~PID, weights = ~total_units))
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
# SECTION 4e: Portfolio Size Heterogeneity
# ============================================================
logf("--- SECTION 4e: Portfolio size heterogeneity ---", log_file = log_file)

portfolio_bins <- c("Single-purchase", "2-4", "5-9", "10+")
portfolio_coefs <- list()
portfolio_fits <- list()

for (pb in portfolio_bins) {
  sub_dt <- reg_dt_wtd[portfolio_bin == pb]
  n_sub <- nrow(sub_dt)
  n_pids <- sub_dt[, uniqueN(PID)]
  logf("  Portfolio bin '", pb, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)
  if (n_sub < 100 || n_pids < 10) next

  fit_fml <- if (freq == "annual") {
    filing_rate ~ i(event_time, ref = ET_REF) | PID + year
  } else {
    filing_rate_q ~ i(q_relative, ref = -4) | PID + yq
  }
  fit_pb <- do.call(feols, list(fml = fit_fml, data = sub_dt,
                                cluster = ~PID, weights = ~total_units))
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
    sub_dt <- reg_dt_wtd[acq_filer_bin == ab & is_corp == corp_flag]
    n_sub <- nrow(sub_dt)
    n_pids <- sub_dt[, uniqueN(PID)]
    corp_label <- if (corp_flag) "corp" else "indiv"
    spec_name <- paste0(corp_label, "_", acq_bin_suffix(ab))
    logf("  ", spec_name, ": N=", n_sub, ", PIDs=", n_pids, log_file = log_file)

    if (n_sub < 100 || n_pids < 10) next

    fit_fml <- if (freq == "annual") {
      filing_rate ~ i(event_time, ref = ET_REF) | PID + year
    } else {
      filing_rate_q ~ i(q_relative, ref = -4) | PID + yq
    }
    fit_cf <- do.call(feols, list(fml = fit_fml, data = sub_dt,
                                  cluster = ~PID, weights = ~total_units))
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
# SECTION 4g: Portfolio Units Net Heterogeneity
# ============================================================
# Split by units in OTHER portfolio buildings at acquisition (net of this building).
# "0 units (solo)" = first-time buyers; "50+ other units" = established portfolio operators.
# ============================================================
logf("--- SECTION 4g: Portfolio units net heterogeneity ---", log_file = log_file)

port_units_bins <- c("0 units (solo)", "1-9 other units", "10-49 other units", "50+ other units")
port_units_spec_labels <- c("port_units_0_solo", "port_units_1_9",
                             "port_units_10_49", "port_units_50plus")

if (freq == "annual" && "portfolio_units_bin" %in% names(reg_dt_wtd)) {
  pu_coefs <- list()
  pu_fits  <- list()
  for (i in seq_along(port_units_bins)) {
    pub <- port_units_bins[i]
    sub_dt <- reg_dt_wtd[portfolio_units_bin == pub]
    n_sub <- nrow(sub_dt)
    n_pids <- sub_dt[, uniqueN(PID)]
    logf("  Portfolio units bin '", pub, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)
    if (n_sub < 100 || n_pids < 10) next

    fit_pu <- do.call(feols, list(
      fml = filing_rate ~ i(event_time, ref = ET_REF) | PID + year,
      data = sub_dt, cluster = ~PID, weights = ~total_units
    ))
    pu_coefs[[length(pu_coefs) + 1]] <- extract_feols_coefs(fit_pu, port_units_spec_labels[i])
    pu_fits[[pub]] <- fit_pu
  }

  if (length(pu_coefs) > 0) {
    pu_all <- rbindlist(pu_coefs)
    fwrite(pu_all, out_path("portfolio_units_coefs.csv"))
    logf("  Wrote portfolio_units_coefs.csv", log_file = log_file)

    plot_event_coefs(pu_all, unique(pu_all$spec),
                     paste0("Event Study by Portfolio Units (net of acquiring building, ", fp$label, ")"),
                     fig_path("by_portfolio_units.png"), facet = TRUE)

    if (length(pu_fits) > 0) {
      etable(pu_fits,
             file = out_path("portfolio_units_etable.tex"),
             style.tex = style.tex("aer"),
             headers = list("Portfolio units (net)" = names(pu_fits)))
    }
  }
}

# ============================================================
# SECTION 4h: Neighborhood Gentrification Status Heterogeneity
# ============================================================
# Split acquired buildings by the gentrification trajectory of their tract.
# Three groups: Gentrified / Did not gentrify / Not gentrifiable.
# "Not gentrifiable" (above-median 2010 income) serves as the comparison baseline.
# Requires: analyze-nhood-portfolio-transitions.R run first.
# ============================================================
logf("--- SECTION 4h: Neighborhood gentrification heterogeneity ---", log_file = log_file)

GENT_PALETTE <- c(
  "Gentrified"       = "#D1495B",
  "Did not gentrify" = "#E9C46A",
  "Not gentrifiable" = "#2A9D8F"
)
GENT_LEVELS <- c("Gentrified", "Did not gentrify", "Not gentrifiable")

if (freq == "annual" && !all(is.na(event_panel$nhood_gent))) {
  gent_coefs <- list()
  gent_fits  <- list()

  for (gs in GENT_LEVELS) {
    sub_dt <- reg_dt_wtd[as.character(nhood_gent) == gs]
    n_sub  <- nrow(sub_dt)
    n_pids <- sub_dt[, uniqueN(PID)]
    logf("  nhood_gent '", gs, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)
    if (n_sub < 100 || n_pids < 10) next

    fit_gs <- do.call(feols, list(
      fml     = filing_rate ~ i(event_time, ref = ET_REF) | PID + year,
      data    = sub_dt,
      cluster = ~PID,
      weights = ~total_units
    ))
    spec_nm <- paste0("nhood_", gsub("[^a-zA-Z]", "_", tolower(gs)))
    gent_coefs[[length(gent_coefs) + 1]] <- extract_feols_coefs(fit_gs, spec_nm)
    gent_fits[[gs]] <- fit_gs
  }

  if (length(gent_coefs) > 0) {
    gent_all <- rbindlist(gent_coefs)
    fwrite(gent_all, out_path("nhood_gent_coefs.csv"))
    logf("  Wrote nhood_gent_coefs.csv", log_file = log_file)

    # Overlay plot: all three groups on one panel for direct comparison
    plot_event_coefs(gent_all, unique(gent_all$spec),
                     paste0("Event Study by Neighborhood Gentrification Status (",
                            fp$label, ")"),
                     fig_path("by_nhood_gent.png"),
                     facet  = FALSE,
                     colors = setNames(GENT_PALETTE, paste0(
                       "nhood_", gsub("[^a-zA-Z]", "_", tolower(GENT_LEVELS))
                     )))

    if (length(gent_fits) > 0) {
      etable(gent_fits,
             file = out_path("nhood_gent_etable.tex"),
             style.tex = style.tex("aer"),
             headers = list("Nhood trajectory" = names(gent_fits)))
    }
  }

  # --- Interacted model: single regression, "Not gentrifiable" as base ---
  # i(event_time, nhood_gent, ref=c(-1,"Not gentrifiable")) gives one coefficient
  # per (event_time × gent_level) pair. Coefficients on Gentrified/Did-not-gentrify
  # are deviations from the Not-gentrifiable baseline path.
  fit_gent_interact <- feols(
    filing_rate ~ i(event_time, nhood_gent,
                    ref = ET_REF, ref2 = "Not gentrifiable") | PID + year,
    data    = reg_dt_wtd[!is.na(nhood_gent)],
    cluster = ~PID,
    weights = ~total_units
  )
  saveRDS(fit_gent_interact, out_path("nhood_gent_interact_fit.rds"))
  logf("  Interacted model: ",
       length(coef(fit_gent_interact)), " coefficients",
       log_file = log_file)

  png(fig_path("by_nhood_gent_interact.png"), width = 1400, height = 700, res = 150)
  iplot(fit_gent_interact,
        main   = paste0("Filing rate: event study × nhood gentrification status (",
                        fp$label, ")"),
        xlab   = "Years relative to acquisition",
        ylab   = "Filing rate (relative to ref year)",
        ci_level = 0.95)
  dev.off()
  logf("  Wrote by_nhood_gent_interact.png", log_file = log_file)

  # Also cross-tab with acq_filer_bin: high-filer x gentrified is the key cell
  if ("acq_filer_bin" %in% names(reg_dt_wtd)) {
    gent_x_filer_coefs <- list()
    hi_bin <- ACQ_BIN_LABELS[1]  # high-filer
    for (gs in GENT_LEVELS) {
      sub_dt <- reg_dt_wtd[as.character(nhood_gent) == gs & acq_filer_bin == hi_bin]
      n_sub  <- nrow(sub_dt)
      n_pids <- sub_dt[, uniqueN(PID)]
      spec_nm <- paste0("hifiler_", gsub("[^a-zA-Z]", "_", tolower(gs)))
      logf("  High-filer x '", gs, "': N=", n_sub, ", PIDs=", n_pids, log_file = log_file)
      if (n_sub < 100 || n_pids < 10) next
      fit_gf <- do.call(feols, list(
        fml = filing_rate ~ i(event_time, ref = ET_REF) | PID + year,
        data = sub_dt, cluster = ~PID, weights = ~total_units
      ))
      gent_x_filer_coefs[[length(gent_x_filer_coefs) + 1]] <-
        extract_feols_coefs(fit_gf, spec_nm)
    }
    if (length(gent_x_filer_coefs) > 0) {
      gxf_all <- rbindlist(gent_x_filer_coefs)
      fwrite(gxf_all, out_path("nhood_gent_x_hifiler_coefs.csv"))
      plot_event_coefs(gxf_all, unique(gxf_all$spec),
                       paste0("High-filer x Neighborhood Gentrification (", fp$label, ")"),
                       fig_path("by_nhood_gent_hifiler.png"), facet = FALSE)
      logf("  Wrote nhood_gent_x_hifiler_coefs.csv", log_file = log_file)
    }
  }
} else {
  logf("  Skipping Section 4h: nhood_gent unavailable or non-annual frequency",
       log_file = log_file)
}

# --- Event-time dummy names (used by Section 5a rent panel and Section 7 robustness) ---
et_dummies <- paste0("et_", ifelse(setdiff(fp$et_range, fp$ref_period) < 0,
                                    paste0("m", abs(setdiff(fp$et_range, fp$ref_period))),
                                    setdiff(fp$et_range, fp$ref_period)))

# --- Helper: run robustness regression and extract coefs with standard term naming ---
# Used by Section 5a (full-panel rent DiD) and Section 7 (filing rate robustness).
run_robustness_fit <- function(panel, fml_str, spec_label, weights = ~total_units) {
  if (nrow(panel) < 100 || panel[, uniqueN(PID)] < 10) {
    logf("    ", spec_label, ": skipping (N=", nrow(panel), ")", log_file = log_file)
    return(NULL)
  }
  fit_args <- list(fml = as.formula(fml_str), data = panel, cluster = ~PID)
  if (!is.null(weights)) fit_args$weights <- weights
  fit <- do.call(feols, fit_args)
  ct <- extract_feols_coefs(fit, spec_label)
  # Standardize term names to match i() output format: event_time::-3, event_time::0, etc.
  ct[, term := gsub("^et_m", paste0(fp$et_var, "::-"), term)]
  ct[, term := gsub("^et_", paste0(fp$et_var, "::"), term)]
  logf("    ", spec_label, ": N=", fit$nobs, ", PIDs=", panel[, uniqueN(PID)],
       log_file = log_file)
  list(coefs = ct, fit = fit)
}

# ============================================================
# SECTION 5: Outcome Variable Event Studies
# ============================================================
# For each outcome: (1) full sample, unit-weighted;
#                   (2) split by acquirer filer bin (acq_filer_bin);
#                   (3) split by acquirer portfolio bin (portfolio_bin).
# All unit-weighted via weights = ~total_units.
# ============================================================
logf("--- SECTION 5: Outcome variable event studies ---", log_file = log_file)

# --- Helper: run full + acq_filer_bin + portfolio_bin event studies for one outcome ---
run_outcome_event_study <- function(outcome_col, reg_data, out_stem, outcome_label) {
  if (!outcome_col %in% names(reg_data)) {
    logf("  SKIP ", out_stem, ": column '", outcome_col, "' not found in reg_data",
         log_file = log_file)
    return(invisible(NULL))
  }
  fml_fit <- as.formula(
    if (freq == "annual") {
      paste0(outcome_col, " ~ i(event_time, ref = ", ET_REF, ") | PID + year")
    } else {
      paste0(outcome_col, " ~ i(q_relative, ref = -4) | PID + yq")
    }
  )

  coef_list <- list()
  fit_list  <- list()

  run_one <- function(sub, spec_label) {
    sub <- sub[!is.na(get(outcome_col))]
    if (nrow(sub) < 100 || sub[, uniqueN(PID)] < 10) {
      logf("    ", spec_label, ": skip (N=", nrow(sub), ")", log_file = log_file)
      return(invisible(NULL))
    }
    fit <- do.call(feols, list(fml = fml_fit, data = sub, cluster = ~PID, weights = ~total_units))
    logf("    ", spec_label, ": N=", fit$nobs, ", PIDs=", sub[, uniqueN(PID)],
         log_file = log_file)
    coef_list[[length(coef_list) + 1L]] <<- extract_feols_coefs(fit, spec_label)
    fit_list[[spec_label]]              <<- fit
  }

  # Spec 1: full sample
  run_one(reg_data, paste0(out_stem, "_full"))

  # Spec 2: by acq_filer_bin
  if ("acq_filer_bin" %in% names(reg_data)) {
    for (fb in ACQ_BIN_LABELS) {
      sub <- reg_data[acq_filer_bin == fb]
      if (nrow(sub) > 0)
        run_one(sub, paste0(out_stem, "_acq_", acq_bin_suffix(fb)))
    }
  }

  # Spec 3: by portfolio_bin
  if ("portfolio_bin" %in% names(reg_data)) {
    for (pb in c("Single-purchase", "2-4", "5-9", "10+")) {
      sub <- reg_data[portfolio_bin == pb]
      if (nrow(sub) > 0)
        run_one(sub, paste0(out_stem, "_port_", gsub("[^a-z0-9_]", "_", tolower(pb))))
    }
  }

  if (length(coef_list) == 0) return(invisible(NULL))
  all_coefs <- rbindlist(coef_list)
  fwrite(all_coefs, out_path(paste0(out_stem, "_coefs.csv")))

  full_spec <- paste0(out_stem, "_full")
  if (full_spec %in% all_coefs$spec)
    plot_event_coefs(all_coefs, full_spec,
                     paste0("Event Study: ", outcome_label, " (", fp$label, ")"),
                     fig_path(paste0(out_stem, "_full.png")),
                     y_label = outcome_label)

  filer_specs <- grep(paste0("^", out_stem, "_acq_"), unique(all_coefs$spec), value = TRUE)
  if (length(filer_specs) > 1)
    plot_event_coefs(all_coefs, filer_specs,
                     paste0("Event Study: ", outcome_label, " by Acquirer Type (", fp$label, ")"),
                     fig_path(paste0(out_stem, "_by_acq_filer.png")), facet = TRUE,
                     y_label = outcome_label)

  port_specs <- grep(paste0("^", out_stem, "_port_"), unique(all_coefs$spec), value = TRUE)
  if (length(port_specs) > 1)
    plot_event_coefs(all_coefs, port_specs,
                     paste0("Event Study: ", outcome_label, " by Portfolio Size (", fp$label, ")"),
                     fig_path(paste0(out_stem, "_by_port.png")), facet = TRUE,
                     y_label = outcome_label)

  logf("  Wrote ", out_stem, "_coefs.csv (", nrow(all_coefs), " rows)", log_file = log_file)
  invisible(all_coefs)
}

run_outcome_tract_fe <- function(outcome_col, reg_data, out_stem, outcome_label) {
  if (freq != "annual") return(invisible(NULL))
  req_cols <- c(outcome_col, "tract_year_fe", "total_units", "PID", "year")
  if (!all(req_cols %in% names(reg_data))) {
    logf("  SKIP tract FE ", out_stem, ": missing required columns", log_file = log_file)
    return(invisible(NULL))
  }
  sub <- reg_data[!is.na(get(outcome_col)) & !is.na(tract_year_fe) &
                    !is.na(total_units) & total_units > 0]
  if (nrow(sub) < 100 || sub[, uniqueN(PID)] < 10) {
    logf("  SKIP tract FE ", out_stem, ": insufficient sample (N=", nrow(sub), ")",
         log_file = log_file)
    return(invisible(NULL))
  }
  fit <- feols(
    as.formula(paste0(outcome_col, " ~ i(event_time, ref = ", fp$ref_period, ") | PID + tract_year_fe")),
    data = sub, cluster = ~PID, weights = ~total_units
  )
  coefs <- extract_feols_coefs(fit, paste0(out_stem, "_tract_year"))
  logf("    ", out_stem, "_tract_year: N=", fit$nobs, ", PIDs=", sub[, uniqueN(PID)],
       log_file = log_file)
  fwrite(coefs, out_path(paste0(out_stem, "_tract_year_coefs.csv")))
  plot_event_coefs(coefs, unique(coefs$spec),
                   paste0("Event Study: ", outcome_label, " with Tract x Year FE (", fp$label, ")"),
                   fig_path(paste0(out_stem, "_tract_year.png")),
                   y_label = outcome_label)
  invisible(coefs)
}

run_outcome_tract_fe_by_buyer <- function(outcome_col, reg_data, out_stem, outcome_label) {
  if (freq != "annual") return(invisible(NULL))
  req_cols <- c(outcome_col, "tract_year_fe", "total_units", "PID", "year", "is_corp")
  if (!all(req_cols %in% names(reg_data))) {
    logf("  SKIP tract FE buyer split ", out_stem, ": missing required columns", log_file = log_file)
    return(invisible(NULL))
  }
  coef_list <- list()
  run_one <- function(sub, spec_label) {
    sub <- sub[!is.na(get(outcome_col)) & !is.na(tract_year_fe) &
                 !is.na(total_units) & total_units > 0]
    if (nrow(sub) < 100 || sub[, uniqueN(PID)] < 10) {
      logf("    ", spec_label, ": skip (N=", nrow(sub), ")", log_file = log_file)
      return(invisible(NULL))
    }
    fit <- feols(
      as.formula(paste0(outcome_col, " ~ i(event_time, ref = ", fp$ref_period, ") | PID + tract_year_fe")),
      data = sub, cluster = ~PID, weights = ~total_units
    )
    coef_list[[length(coef_list) + 1L]] <<- extract_feols_coefs(fit, spec_label)
    logf("    ", spec_label, ": N=", fit$nobs, ", PIDs=", sub[, uniqueN(PID)], log_file = log_file)
  }

  run_one(reg_data[is_corp == TRUE], paste0(out_stem, "_tract_year_corporate"))
  run_one(reg_data[is_corp == FALSE], paste0(out_stem, "_tract_year_individual"))
  if (length(coef_list) == 0L) return(invisible(NULL))

  coefs <- rbindlist(coef_list, use.names = TRUE, fill = TRUE)
  fwrite(coefs, out_path(paste0(out_stem, "_tract_year_buyer_coefs.csv")))
  buyer_specs <- intersect(c(paste0(out_stem, "_tract_year_corporate"),
                             paste0(out_stem, "_tract_year_individual")),
                           unique(coefs$spec))
  if (length(buyer_specs) > 1L) {
    plot_event_coefs(
      coefs,
      buyer_specs,
      paste0("Event Study: ", outcome_label, " with Tract x Year FE by Buyer Type (", fp$label, ")"),
      fig_path(paste0(out_stem, "_tract_year_by_buyer.png")),
      facet = TRUE,
      y_label = outcome_label,
      ncol = 2L,
      custom_labels = c(
        setNames("Corporate", paste0(out_stem, "_tract_year_corporate")),
        setNames("Individual", paste0(out_stem, "_tract_year_individual"))
      )
    )
  }
  invisible(coefs)
}

# --- 5a: Rental price (log_med_rent) — full-panel DiD design ---
# Full-panel design: includes never-transferred buildings as controls so that
# PID fixed effects have variation to absorb.  Rent data (Altos) is sparse
# (~25-45% coverage) so the event-panel-only approach would leave many transferred
# buildings with too few within-PID observations.
#
# Design:
#   - Transferred PIDs (single-transfer only, to avoid duplicate year rows):
#       require >=1 non-NA rent observation pre-acquisition (event_time < 0)
#       AND >=1 post-acquisition (event_time >= 0).
#   - Never-transferred PIDs: require >=2 non-NA rent observations total.
#   - Explicit et_ dummies (0 for control buildings and outside-window years).
logf("  5a: log_med_rent (full-panel DiD) ...", log_file = log_file)
if (freq == "annual" && "log_med_rent" %in% names(bldg_sub)) {
  # Single-transfer PIDs
  .st_pids_rent  <- pid_transfer_counts[n_transfers_pid == 1L, PID]
  .st_trans_rent <- unique(
    transfers[PID %chin% .st_pids_rent, .(PID, transfer_year = year)],
    by = c("PID", "transfer_year")
  )
  .all_tx_pids <- unique(transfers$PID)  # for identifying control vs. treated

  # Base panel: buildings with rent, pre-COVID, non-NA units
  .bldg_rent <- bldg_sub[!is.na(log_med_rent) &
                           year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX &
                           !is.na(total_units) & total_units > 0]

  # Merge transfer timing; all.x = TRUE keeps never-transferred buildings
  .rent_panel <- merge(.bldg_rent, .st_trans_rent, by = "PID", all.x = TRUE)
  .rent_panel[, .et_r := year - transfer_year]  # NA for never-transferred

  # Flag rows: treated (single-transfer), control (never transferred),
  # or dropped (multi-transfer, not in single-transfer set)
  .rent_panel[, .is_tx   := !is.na(transfer_year)]
  .rent_panel[, .is_ctrl := !PID %chin% .all_tx_pids]
  .rent_panel <- .rent_panel[.is_tx | .is_ctrl]  # drop multi-transfer PIDs

  # Create event-time dummies; 0 for controls and years outside the window
  for (.k in setdiff(fp$et_range, fp$ref_period)) {
    .cn <- paste0("et_", ifelse(.k < 0, paste0("m", abs(.k)), .k))
    .rent_panel[, (.cn) := as.integer(.is_tx & !is.na(.et_r) & .et_r == .k)]
  }

  # Transferred PIDs: require >=1 non-NA rent pre AND >=1 post
  .tx_cov <- .rent_panel[.is_tx == TRUE,
                          .(has_pre  = any(.et_r < 0L, na.rm = TRUE),
                            has_post = any(.et_r >= 0L, na.rm = TRUE)),
                          by = PID]
  .valid_tx_rent <- .tx_cov[has_pre == TRUE & has_post == TRUE, PID]

  # Control PIDs: require >=2 rent observations
  .ctrl_cov <- .rent_panel[.is_ctrl == TRUE, .(n_obs = .N), by = PID]
  .valid_ctrl_rent <- .ctrl_cov[n_obs >= 2L, PID]

  # Final rent dataset
  rent_dt_full <- .rent_panel[
    (.is_ctrl & PID %chin% .valid_ctrl_rent) |
    (.is_tx   & PID %chin% .valid_tx_rent)
  ]
  rent_dt_full <- add_tract_year_fe(rent_dt_full)
  rent_dt_full[, c(".et_r", ".is_tx", ".is_ctrl") := NULL]

  logf("    Rent sample: ", rent_dt_full[, uniqueN(PID)], " PIDs (",
       rent_dt_full[PID %chin% .valid_tx_rent,  uniqueN(PID)], " transferred, ",
       rent_dt_full[PID %chin% .valid_ctrl_rent, uniqueN(PID)], " control), ",
       nrow(rent_dt_full), " rows", log_file = log_file)

  # Formula (reuses et_dummies and run_robustness_fit from Section 7, which is defined later)
  # We inline the formula here and use the same run_robustness_fit pattern.
  .rent_fml <- paste0("log_med_rent ~ ", paste(et_dummies, collapse = " + "), " | PID + year")

  # PID-level acq_filer_bin / portfolio_bin lookup (at acquisition, et=0)
  .tx_attrs <- unique(event_panel[event_time == 0L & PID %chin% .valid_tx_rent,
                                   .(PID, acq_filer_bin, portfolio_bin)],
                       by = "PID")

  rent_coef_list <- list()

  # Full sample
  .res <- run_robustness_fit(rent_dt_full, .rent_fml, "rent_full")
  if (!is.null(.res)) rent_coef_list[[length(rent_coef_list) + 1]] <- .res$coefs

  # By acq_filer_bin (control buildings always included)
  if ("acq_filer_bin" %in% names(.tx_attrs)) {
    for (.fb in ACQ_BIN_LABELS) {
      .fb_pids <- .tx_attrs[acq_filer_bin == .fb, PID]
      .sub <- rent_dt_full[(PID %chin% .valid_ctrl_rent) | (PID %chin% .fb_pids)]
      .spec <- paste0("rent_acq_", acq_bin_suffix(.fb))
      .res <- run_robustness_fit(.sub, .rent_fml, .spec)
      if (!is.null(.res)) rent_coef_list[[length(rent_coef_list) + 1]] <- .res$coefs
    }
  }

  # By portfolio_bin (control buildings always included)
  if ("portfolio_bin" %in% names(.tx_attrs)) {
    for (.pb in c("Single-purchase", "2-4", "5-9", "10+")) {
      .pb_pids <- .tx_attrs[portfolio_bin == .pb, PID]
      .sub <- rent_dt_full[(PID %chin% .valid_ctrl_rent) | (PID %chin% .pb_pids)]
      .spec <- paste0("rent_port_", gsub("[^a-z0-9_]", "_", tolower(.pb)))
      .res <- run_robustness_fit(.sub, .rent_fml, .spec)
      if (!is.null(.res)) rent_coef_list[[length(rent_coef_list) + 1]] <- .res$coefs
    }
  }

  if (length(rent_coef_list) > 0) {
    all_rent_coefs <- rbindlist(rent_coef_list)
    fwrite(all_rent_coefs, out_path("rent_coefs.csv"))

    plot_event_coefs(all_rent_coefs, "rent_full",
                     paste0("Event Study: Log Median Rent (", fp$label, ")"),
                     fig_path("rent_full.png"),
                     y_label = "Log median rent")

    .filer_rent_specs <- grep("^rent_acq_", unique(all_rent_coefs$spec), value = TRUE)
    if (length(.filer_rent_specs) > 1)
      plot_event_coefs(all_rent_coefs, .filer_rent_specs,
                       paste0("Event Study: Log Median Rent by Acquirer Type (", fp$label, ")"),
                       fig_path("rent_by_acq_filer.png"), facet = TRUE,
                       y_label = "Log median rent")

    .port_rent_specs <- grep("^rent_port_", unique(all_rent_coefs$spec), value = TRUE)
    if (length(.port_rent_specs) > 1)
      plot_event_coefs(all_rent_coefs, .port_rent_specs,
                       paste0("Event Study: Log Median Rent by Portfolio Size (", fp$label, ")"),
                       fig_path("rent_by_port.png"), facet = TRUE,
                       y_label = "Log median rent")

    logf("  Wrote rent_coefs.csv (", nrow(all_rent_coefs), " rows)", log_file = log_file)
  }

  rent_robust_coefs <- list()
  .rent_fml_tract <- paste0("log_med_rent ~ ", paste(et_dummies, collapse = " + "),
                            " | PID + tract_year_fe")
  if (rent_dt_full[!is.na(tract_year_fe), .N] > 100) {
    .res <- run_robustness_fit(rent_dt_full[!is.na(tract_year_fe)],
                               .rent_fml_tract, "rent_tract_year")
    if (!is.null(.res)) rent_robust_coefs[[length(rent_robust_coefs) + 1L]] <- .res$coefs
  }

  .ctrl_cov3 <- .rent_panel[.is_ctrl == TRUE, .(n_obs = .N), by = PID]
  .valid_ctrl_rent3 <- .ctrl_cov3[n_obs >= 3L, PID]
  rent_dt_ctrl3 <- .rent_panel[
    (.is_ctrl & PID %chin% .valid_ctrl_rent3) |
    (.is_tx   & PID %chin% .valid_tx_rent)
  ]
  rent_dt_ctrl3 <- add_tract_year_fe(rent_dt_ctrl3)
  rent_dt_ctrl3[, c(".et_r", ".is_tx", ".is_ctrl") := NULL]
  logf("    Rent ctrl3 sample: ", rent_dt_ctrl3[, uniqueN(PID)], " PIDs (",
       rent_dt_ctrl3[PID %chin% .valid_tx_rent, uniqueN(PID)], " transferred, ",
       rent_dt_ctrl3[PID %chin% .valid_ctrl_rent3, uniqueN(PID)], " control), ",
       nrow(rent_dt_ctrl3), " rows", log_file = log_file)

  .res <- run_robustness_fit(rent_dt_ctrl3, .rent_fml, "rent_ctrl3")
  if (!is.null(.res)) rent_robust_coefs[[length(rent_robust_coefs) + 1L]] <- .res$coefs
  if (rent_dt_ctrl3[!is.na(tract_year_fe), .N] > 100) {
    .res <- run_robustness_fit(rent_dt_ctrl3[!is.na(tract_year_fe)],
                               .rent_fml_tract, "rent_ctrl3_tract_year")
    if (!is.null(.res)) rent_robust_coefs[[length(rent_robust_coefs) + 1L]] <- .res$coefs
  }
  if (length(rent_robust_coefs) > 0) {
    rent_robust_all <- rbindlist(rent_robust_coefs)
    fwrite(rent_robust_all, out_path("rent_robustness_coefs.csv"))
    plot_event_coefs(rent_robust_all, unique(rent_robust_all$spec),
                     paste0("Rent Robustness Checks (", fp$label, ")"),
                     fig_path("rent_robustness.png"),
                     facet = TRUE, y_label = "Log median rent")
    logf("  Wrote rent_robustness_coefs.csv (", nrow(rent_robust_all), " rows)",
         log_file = log_file)
  }
  # --- 5a-safe: log_med_rent_safe (single-source; no source-switching artifact) ---
  logf("  5a-safe: log_med_rent_safe (full-panel DiD, single-source) ...", log_file = log_file)
    .rent_fml_safe       <- paste0("log_med_rent_safe ~ ", paste(et_dummies, collapse = " + "), " | PID + year")
    .rent_fml_safe_tract <- paste0("log_med_rent_safe ~ ", paste(et_dummies, collapse = " + "), " | PID + tract_year_fe")
    rent_safe_coef_list <- list()
    .res <- run_robustness_fit(rent_dt_full[!is.na(log_med_rent_safe)], .rent_fml_safe, "rent_safe_full")
    if (!is.null(.res)) rent_safe_coef_list[[length(rent_safe_coef_list) + 1L]] <- .res$coefs
    if ("acq_filer_bin" %in% names(.tx_attrs)) {
      for (.fb in ACQ_BIN_LABELS) {
        .fb_pids <- .tx_attrs[acq_filer_bin == .fb, PID]
        .sub <- rent_dt_full[!is.na(log_med_rent_safe) &
                               ((PID %chin% .valid_ctrl_rent) | (PID %chin% .fb_pids))]
        .spec <- paste0("rent_safe_acq_", acq_bin_suffix(.fb))
        .res <- run_robustness_fit(.sub, .rent_fml_safe, .spec)
        if (!is.null(.res)) rent_safe_coef_list[[length(rent_safe_coef_list) + 1L]] <- .res$coefs
      }
    }
    if (length(rent_safe_coef_list) > 0L) {
      all_rent_safe_coefs <- rbindlist(rent_safe_coef_list)
      fwrite(all_rent_safe_coefs, out_path("rent_safe_coefs.csv"))
      plot_event_coefs(all_rent_safe_coefs, "rent_safe_full",
                       paste0("Event Study: Log Rent (Safe, single-source) (", fp$label, ")"),
                       fig_path("rent_safe_full.png"), y_label = "Log median rent (safe)")
      .filer_safe_specs <- grep("^rent_safe_acq_", unique(all_rent_safe_coefs$spec), value = TRUE)
      if (length(.filer_safe_specs) > 1L)
        plot_event_coefs(all_rent_safe_coefs, .filer_safe_specs,
                         paste0("Event Study: Log Rent (Safe) by Acquirer Type (", fp$label, ")"),
                         fig_path("rent_safe_by_acq_filer.png"), facet = TRUE,
                         y_label = "Log median rent (safe)")
      logf("  Wrote rent_safe_coefs.csv (", nrow(all_rent_safe_coefs), " rows)", log_file = log_file)
    }
    rent_safe_robust_coefs <- list()
    .res <- run_robustness_fit(rent_dt_ctrl3[!is.na(log_med_rent_safe)],
                               .rent_fml_safe, "rent_safe_ctrl3")
    if (!is.null(.res)) rent_safe_robust_coefs[[length(rent_safe_robust_coefs) + 1L]] <- .res$coefs
    if (rent_dt_ctrl3[!is.na(log_med_rent_safe) & !is.na(tract_year_fe), .N] > 100) {
      .res <- run_robustness_fit(rent_dt_ctrl3[!is.na(log_med_rent_safe) & !is.na(tract_year_fe)],
                                 .rent_fml_safe_tract, "rent_safe_ctrl3_tract_year")
      if (!is.null(.res)) rent_safe_robust_coefs[[length(rent_safe_robust_coefs) + 1L]] <- .res$coefs
    }
    if (length(rent_safe_robust_coefs) > 0L) {
      rent_safe_robust_all <- rbindlist(rent_safe_robust_coefs)
      fwrite(rent_safe_robust_all, out_path("rent_safe_robustness_coefs.csv"))
      plot_event_coefs(rent_safe_robust_all, unique(rent_safe_robust_all$spec),
                       paste0("Rent (Safe) Robustness Checks (", fp$label, ")"),
                       fig_path("rent_safe_robustness.png"),
                       facet = TRUE, y_label = "Log median rent (safe)")
    }

  # Clean up temporaries
  rm(.st_pids_rent, .st_trans_rent, .all_tx_pids, .bldg_rent, .rent_panel,
     .tx_cov, .valid_tx_rent, .ctrl_cov, .valid_ctrl_rent, .tx_attrs, .rent_fml,
     .ctrl_cov3, .valid_ctrl_rent3, .rent_fml_tract)
}

# --- 5b: Racial composition ---
race_all_coefs <- list()

logf("  5b: infousa_pct_black ...", log_file = log_file)
.race_black <- run_outcome_event_study("infousa_pct_black", reg_dt_wtd, "pct_black", "Share Black Residents")
if (!is.null(.race_black)) race_all_coefs[[length(race_all_coefs) + 1L]] <- .race_black

logf("  5b2: infousa_pct_asian ...", log_file = log_file)
.race_asian <- run_outcome_event_study("infousa_pct_asian", reg_dt_wtd, "pct_asian", "Share Asian Residents")
if (!is.null(.race_asian)) race_all_coefs[[length(race_all_coefs) + 1L]] <- .race_asian

# --- 5c: Gender composition ---
gender_all_coefs <- list()
logf("  5c: infousa_pct_female ...", log_file = log_file)
.gender_female <- run_outcome_event_study("infousa_pct_female", reg_dt_wtd, "pct_female", "Share Female Residents")
if (!is.null(.gender_female)) gender_all_coefs[[length(gender_all_coefs) + 1L]] <- .gender_female

# --- 5d: Permit rate ---
logf("  5d: permit_rate ...", log_file = log_file)
run_outcome_event_study("permit_rate", reg_dt_wtd, "permit_rate", "Permit Rate (per unit)")

# --- 5e: Complaint rate ---
logf("  5e: complaint_rate ...", log_file = log_file)
run_outcome_event_study("complaint_rate", reg_dt_wtd, "complaint_rate", "Complaint Rate (per unit)")

# --- 5f: Gender composition (male) — diagnostic for pct_female dip ---
logf("  5f: infousa_pct_male ...", log_file = log_file)
.gender_male <- run_outcome_event_study("infousa_pct_male", reg_dt_wtd, "pct_male", "Share Male Residents (diagnostic)")
if (!is.null(.gender_male)) gender_all_coefs[[length(gender_all_coefs) + 1L]] <- .gender_male

# --- 5g: Racial composition (white) — diagnostic for pct_black dip ---
logf("  5g: infousa_pct_white ...", log_file = log_file)
.race_white <- run_outcome_event_study("infousa_pct_white", reg_dt_wtd, "pct_white", "Share White Residents (diagnostic)")
if (!is.null(.race_white)) race_all_coefs[[length(race_all_coefs) + 1L]] <- .race_white

# --- 5h: Racial composition (Latino/Hispanic) — diagnostic for race-share accounting ---
logf("  5h: infousa_pct_hispanic ...", log_file = log_file)
.race_latino <- run_outcome_event_study("infousa_pct_hispanic", reg_dt_wtd, "pct_latino", "Share Latino Residents")
if (!is.null(.race_latino)) race_all_coefs[[length(race_all_coefs) + 1L]] <- .race_latino

logf("  5h2: infousa_pct_other ...", log_file = log_file)
.race_other <- run_outcome_event_study("infousa_pct_other", reg_dt_wtd, "pct_other", "Share Other Residents")
if (!is.null(.race_other)) race_all_coefs[[length(race_all_coefs) + 1L]] <- .race_other

if (length(race_all_coefs) > 0) {
  race_plot_all <- rbindlist(race_all_coefs, use.names = TRUE, fill = TRUE)
  race_full_specs <- intersect(c("pct_asian_full", "pct_black_full", "pct_latino_full",
                                 "pct_white_full", "pct_other_full"),
                               unique(race_plot_all$spec))
  if (length(race_full_specs) > 1L) {
    plot_event_coefs(
      race_plot_all,
      race_full_specs,
      paste0("Race Shares Around Transfer (", fp$label, ")"),
      fig_path("race_shares_full.png"),
      facet = TRUE,
      y_label = "Share of residents",
      ncol = 3L,
      custom_labels = c(
        pct_asian_full = "Asian",
        pct_black_full = "Black",
        pct_latino_full = "Latino",
        pct_white_full = "White",
        pct_other_full = "Other"
      )
    )
  }
  race_acq_specs <- intersect(c(
                                paste0("pct_asian_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1])),
                                paste0("pct_black_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1])),
                                paste0("pct_latino_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1])),
                                paste0("pct_white_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1])),
                                paste0("pct_other_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1]))),
                              unique(race_plot_all$spec))
  if (length(race_acq_specs) > 1L) {
    plot_event_coefs(
      race_plot_all,
      race_acq_specs,
      paste0("Race Shares Around Transfer by Acquirer Type (", fp$label, ")"),
      fig_path("race_shares_by_acq_filer.png"),
      facet = TRUE,
      y_label = "Share of residents",
      ncol = 3L,
      custom_labels = c(
        setNames("Asian", paste0("pct_asian_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1]))),
        setNames("Black", paste0("pct_black_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1]))),
        setNames("Latino", paste0("pct_latino_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1]))),
        setNames("White", paste0("pct_white_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1]))),
        setNames("Other", paste0("pct_other_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1])))
      )
    )
  }
  race_port_specs <- intersect(c("pct_asian_port_10_", "pct_black_port_10_",
                                 "pct_latino_port_10_", "pct_white_port_10_",
                                 "pct_other_port_10_"),
                               unique(race_plot_all$spec))
  if (length(race_port_specs) > 1L) {
    plot_event_coefs(
      race_plot_all,
      race_port_specs,
      paste0("Race Shares Around Transfer by Portfolio Size (", fp$label, ")"),
      fig_path("race_shares_by_port.png"),
      facet = TRUE,
      y_label = "Share of residents",
      ncol = 3L,
      custom_labels = c(
        pct_asian_port_10_ = "Asian",
        pct_black_port_10_ = "Black",
        pct_latino_port_10_ = "Latino",
        pct_white_port_10_ = "White",
        pct_other_port_10_ = "Other"
      )
    )
  }
}

if (length(gender_all_coefs) > 0) {
  gender_plot_all <- rbindlist(gender_all_coefs, use.names = TRUE, fill = TRUE)
  gender_full_specs <- intersect(c("pct_female_full", "pct_male_full"), unique(gender_plot_all$spec))
  if (length(gender_full_specs) > 1L) {
    plot_event_coefs(
      gender_plot_all,
      gender_full_specs,
      paste0("Gender Shares Around Transfer (", fp$label, ")"),
      fig_path("gender_shares_full.png"),
      facet = TRUE,
      y_label = "Share of residents",
      ncol = 2L,
      custom_labels = c(pct_female_full = "Female", pct_male_full = "Male")
    )
  }
  gender_acq_specs <- intersect(c(
                                  paste0("pct_female_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1])),
                                  paste0("pct_male_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1]))),
                                unique(gender_plot_all$spec))
  if (length(gender_acq_specs) > 1L) {
    plot_event_coefs(
      gender_plot_all,
      gender_acq_specs,
      paste0("Gender Shares Around Transfer by Acquirer Type (", fp$label, ")"),
      fig_path("gender_shares_by_acq_filer.png"),
      facet = TRUE,
      y_label = "Share of residents",
      ncol = 2L,
      custom_labels = c(
        setNames("Female", paste0("pct_female_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1]))),
        setNames("Male", paste0("pct_male_acq_", acq_bin_suffix(ACQ_BIN_LABELS[1])))
      )
    )
  }
  gender_port_specs <- intersect(c("pct_female_port_10_", "pct_male_port_10_"),
                                 unique(gender_plot_all$spec))
  if (length(gender_port_specs) > 1L) {
    plot_event_coefs(
      gender_plot_all,
      gender_port_specs,
      paste0("Gender Shares Around Transfer by Portfolio Size (", fp$label, ")"),
      fig_path("gender_shares_by_port.png"),
      facet = TRUE,
      y_label = "Share of residents",
      ncol = 2L,
      custom_labels = c(pct_female_port_10_ = "Female", pct_male_port_10_ = "Male")
    )
  }
}

# --- 5i: Occupancy rate ---
logf("  5i: occupancy_rate ...", log_file = log_file)
run_outcome_event_study("occupancy_rate", reg_dt_wtd, "occupancy_rate", "Occupancy Rate")

# --- 5j: Log household income (infousa_find_mean_k = predicted income in $1000s) ---
logf("  5j: log_infousa_income ...", log_file = log_file)
run_outcome_event_study("log_infousa_income", reg_dt_wtd, "log_income", "Log Household Income ($1000s)")

# --- 5k: Neighborhood-trend robustness for selected outcomes ---
if (freq == "annual") {
  logf("  5k: tract x year FE robustness ...", log_file = log_file)
  tract_fe_coefs <- list()
  for (rob in list(
    list(col = "occupancy_rate", stem = "occupancy_rate", label = "Occupancy Rate"),
    list(col = "log_infousa_income", stem = "log_income", label = "Log Household Income ($1000s)"),
    list(col = "infousa_pct_asian", stem = "pct_asian", label = "Share Asian Residents"),
    list(col = "infousa_pct_black", stem = "pct_black", label = "Share Black Residents"),
    list(col = "infousa_pct_white", stem = "pct_white", label = "Share White Residents"),
    list(col = "infousa_pct_hispanic", stem = "pct_latino", label = "Share Latino Residents"),
    list(col = "infousa_pct_other", stem = "pct_other", label = "Share Other Residents")
  )) {
    .co <- run_outcome_tract_fe(rob$col, reg_dt_wtd, rob$stem, rob$label)
    if (!is.null(.co)) tract_fe_coefs[[length(tract_fe_coefs) + 1L]] <- .co
  }
  run_outcome_tract_fe_by_buyer("log_infousa_income", reg_dt_wtd, "log_income", "Log Household Income ($1000s)")
  if (length(tract_fe_coefs) > 0) {
    tract_fe_all <- rbindlist(tract_fe_coefs)
    fwrite(tract_fe_all, out_path("tract_year_robustness_coefs.csv"))
    race_tract_specs <- intersect(c("pct_asian_tract_year", "pct_black_tract_year",
                                    "pct_latino_tract_year", "pct_white_tract_year",
                                    "pct_other_tract_year"),
                                  unique(tract_fe_all$spec))
    if (length(race_tract_specs) > 1) {
      plot_event_coefs(tract_fe_all, race_tract_specs,
                       paste0("Race Shares Around Transfer with Tract x Year FE (", fp$label, ")"),
                       fig_path("race_shares_tract_year.png"),
                       facet = TRUE, y_label = "Share of residents", ncol = 3L,
                       custom_labels = c(
                         pct_asian_tract_year = "Asian",
                         pct_black_tract_year = "Black",
                         pct_latino_tract_year = "Latino",
                         pct_white_tract_year = "White",
                         pct_other_tract_year = "Other"
                       ))
    }
    logf("  Wrote tract_year_robustness_coefs.csv (", nrow(tract_fe_all), " rows)",
         log_file = log_file)
  }
}

# ============================================================
# SECTION 6: Selection Concern — Rental Market Presence (annual only)
# ============================================================
# Moved here from Section 4d: after outcome event studies, before robustness.
# ============================================================
if (freq == "annual") {
  logf("--- SECTION 6: Rental market presence around transfer ---", log_file = log_file)

  # Unit-weighted: P(building obtains license/listing) weighted by number of units.
  # Include all properties (including single-unit rowhouses, the primary
  # owner-to-renter conversion target in Philadelphia). Sample: 2011-2019.
  entry_dt <- event_panel[!is.na(has_rental_evidence) &
                           year >= max(2011L, ANALYSIS_YEAR_MIN) & year <= ANALYSIS_YEAR_MAX &
                           !is.na(total_units)]
  entry_coefs <- list()

  ent_fml_lic <- entered_license ~ i(event_time, ref = fp$ref_period) | PID + year
  ent_fml_any <- has_rental_evidence ~ i(event_time, ref = fp$ref_period) | PID + year

  if ("entered_license" %in% names(entry_dt) && entry_dt[!is.na(entered_license), .N] > 100) {
    entry_coefs[[1]] <- extract_feols_coefs(
      feols(ent_fml_lic, data = entry_dt[!is.na(entered_license)],
            cluster = ~PID, weights = ~total_units), "license_entry_full")
    entry_coefs[[2]] <- extract_feols_coefs(
      feols(ent_fml_lic, data = entry_dt[is_corp == TRUE & !is.na(entered_license)],
            cluster = ~PID, weights = ~total_units), "license_entry_corp")
    entry_coefs[[3]] <- extract_feols_coefs(
      feols(ent_fml_lic, data = entry_dt[is_corp == FALSE & !is.na(entered_license)],
            cluster = ~PID, weights = ~total_units), "license_entry_indiv")
  }

  if ("entered_altos" %in% names(entry_dt) && entry_dt[!is.na(entered_altos), .N] > 100) {
    entry_coefs[[length(entry_coefs) + 1]] <- extract_feols_coefs(
      feols(entered_altos ~ i(event_time, ref = fp$ref_period) | PID + year,
            data = entry_dt[!is.na(entered_altos)], cluster = ~PID, weights = ~total_units),
      "altos_entry_full")
  }

  entry_coefs[[length(entry_coefs) + 1]] <- extract_feols_coefs(
    feols(ent_fml_any, data = entry_dt, cluster = ~PID, weights = ~total_units),
    "any_rental_entry_full")
  entry_coefs[[length(entry_coefs) + 1]] <- extract_feols_coefs(
    feols(ent_fml_any, data = entry_dt[is_corp == TRUE], cluster = ~PID, weights = ~total_units),
    "any_rental_entry_corp")
  entry_coefs[[length(entry_coefs) + 1]] <- extract_feols_coefs(
    feols(ent_fml_any, data = entry_dt[is_corp == FALSE], cluster = ~PID, weights = ~total_units),
    "any_rental_entry_indiv")

  if (length(entry_coefs) > 0) {
    entry_all <- rbindlist(entry_coefs)
    fwrite(entry_all, out_path("rental_entry_coefs.csv"))

    lic_specs <- grep("license_entry", unique(entry_all$spec), value = TRUE)
    if (length(lic_specs) > 0)
      plot_event_coefs(entry_all, lic_specs,
                       paste0("Rental License Record Around Transfer (year >= 2011; ref=", fp$ref_period, ")"),
                       fig_path("rental_license_entry.png"),
                       y_label = "Probability of rental license record")

    any_specs <- grep("any_rental_entry", unique(entry_all$spec), value = TRUE)
    if (length(any_specs) > 0)
      plot_event_coefs(entry_all, any_specs,
                       paste0("Any Rental Evidence Around Transfer (year >= 2011; ref=", fp$ref_period, ")"),
                       fig_path("any_rental_entry.png"),
                       y_label = "Probability of any rental evidence")
  }
}

# ============================================================
# SECTION 7: Robustness
# ============================================================
# All robustness uses the full building panel with explicit event-time
# dummies, so that years outside the [-3,+5] window and never-transferred
# buildings can serve as control observations. Full sample, unit-weighted.
# ============================================================
logf("--- SECTION 7: Robustness checks (full-panel design, all units, unit-weighted) ---",
     log_file = log_file)

robustness_coefs <- list()
robustness_fits <- list()
count_var_name <- fp$count_var

# --- Helper: build full-panel regression dataset with event-time dummies ---
# Uses the full building panel (bldg_sub), merging in transfer timing so that
# years outside the event window have all dummies = 0.
build_robustness_panel <- function(transfer_dt, bldg_dt, sample_filter = NULL) {
  # transfer_dt: data.table with (PID, transfer_year) — one row per transfer to include
  # bldg_dt: building panel (PID, year, filing_rate, total_units, etc.)
  # sample_filter: optional expression for additional filtering

  assert_unique(transfer_dt, c("PID", "transfer_year"), "robustness transfer_dt")
  assert_unique(bldg_dt, c("PID", "year"), "robustness bldg_dt")

  # Merge building data with transfer timing; all.x = TRUE keeps never-transferred controls.
  panel <- merge(bldg_dt, transfer_dt, by = "PID", all.x = TRUE, allow.cartesian = TRUE)
  assert_unique(panel, c("PID", "year", "transfer_year"), "robustness merged panel")
  panel[, event_time := year - transfer_year]

  # Create explicit dummies for event-time periods (ref period omitted)
  for (k in setdiff(fp$et_range, fp$ref_period)) {
    col_name <- paste0("et_", ifelse(k < 0, paste0("m", abs(k)), k))
    panel[, (col_name) := as.integer(event_time == k)]
    # Outside the event window: dummy = 0 (natural control period)
    panel[is.na(event_time) | event_time < min(fp$et_range) | event_time > max(fp$et_range),
          (col_name) := 0L]
  }

  # Filter: analysis window, non-missing positive unit counts (needed for unit weights)
  panel <- panel[year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX &
                   !is.na(total_units) & total_units > 0]

  if (!is.null(sample_filter)) {
    panel <- panel[eval(sample_filter)]
  }
  logf("    Built robustness panel: ", nrow(panel), " rows, ",
       panel[!is.na(transfer_year), uniqueN(PID)], " treated PIDs, ",
       panel[is.na(transfer_year), uniqueN(PID)], " never-transferred control PIDs",
       log_file = log_file)
  panel
}

# Build building panel with filing_rate for robustness use
bldg_robust <- copy(bldg_sub)
bldg_robust[, filing_rate := fifelse(
  !is.na(num_filings) & !is.na(total_units) & total_units > 0,
  pmin(num_filings / total_units, MAX_ALLOWABLE_FILING_RATE), NA_real_
)]

# et_dummies and run_robustness_fit defined before Section 5 (shared with Section 5a rent).
et_fml_str <- paste0("filing_rate ~ ", paste(et_dummies, collapse = " + "), " | PID + year")

# 5a: No sheriff deeds
logf("  Robustness: No sheriff's deeds...", log_file = log_file)
no_sheriff_transfers <- unique(
  transfers[is_sheriff_deed == FALSE, .(PID, transfer_year = year)],
  by = c("PID", "transfer_year")
)
ns_panel <- build_robustness_panel(no_sheriff_transfers, bldg_robust)
ns_result <- run_robustness_fit(ns_panel[!is.na(filing_rate)], et_fml_str, "no_sheriff")
if (!is.null(ns_result)) {
  robustness_coefs[[length(robustness_coefs) + 1]] <- ns_result$coefs
  robustness_fits[["No sheriff deeds"]] <- ns_result$fit
}

# 5b: Allow repeat-transfer PIDs back into the treated sample
logf("  Robustness: Allow repeat-transfer PIDs...", log_file = log_file)
all_first_sales <- unique(
  transfers[order(PID, year), .SD[1], by = PID][, .(PID, transfer_year = year)],
  by = c("PID", "transfer_year")
)
st_panel <- build_robustness_panel(all_first_sales, bldg_robust)
st_result <- run_robustness_fit(st_panel[!is.na(filing_rate)], et_fml_str, "allow_repeat")
if (!is.null(st_result)) {
  robustness_coefs[[length(robustness_coefs) + 1]] <- st_result$coefs
  robustness_fits[["Allow repeat-transfer PIDs"]] <- st_result$fit
}

# 5c: No subsequent transfer within 2 years
logf("  Robustness: No nearby re-transfer...", log_file = log_file)
all_t <- transfers[, .(PID, transfer_year = year)]
setorder(all_t, PID, transfer_year)
all_t[, next_transfer := shift(transfer_year, -1L, type = "lead"), by = PID]
all_t[, gap := next_transfer - transfer_year]
clean_t <- all_t[is.na(gap) | gap > 2L, .(PID, transfer_year)]
cl_panel <- build_robustness_panel(clean_t, bldg_robust)
cl_result <- run_robustness_fit(cl_panel[!is.na(filing_rate)], et_fml_str, "no_close_overlap")
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

# 5f: Violations outcome — excluded from the main robustness table/plot;
# handled separately in Section 5g (violations by buyer type).

# Combine robustness
robust_all <- rbindlist(robustness_coefs)
fwrite(robust_all, out_path("robustness_coefs.csv"))
logf("  Wrote robustness_coefs.csv", log_file = log_file)

robust_plot_specs <- intersect(c("no_sheriff", "allow_repeat", "no_close_overlap"),
                               unique(robust_all$spec))
if (length(robust_plot_specs) > 0) {
  plot_event_coefs(robust_all, robust_plot_specs,
                   paste0("Robustness Checks (", fp$label, ", all units, unit-weighted)"),
                   fig_path("robustness.png"), facet = TRUE,
                   y_label = "Filing rate (per unit)")
}

if (length(robustness_fits) > 0) {
  etable(robustness_fits,
         file = out_path("robustness_etable.tex"),
         style.tex = style.tex("aer"),
         headers = list("Robustness" = names(robustness_fits)))
}

# 5g: Violations diagnostic (by buyer type, year >= 2007, full panel)
if ("total_violations" %in% names(bldg_robust)) {
  logf("  Violations diagnostic by buyer type (year >= 2007, full panel)...", log_file = log_file)
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
                       fig_path("violations_by_buyer.png"),
                       y_label = "Total violations")
    }
  }
}

# ============================================================
# SECTION 8: Pooled Window Estimates (MEMO ITEM 1 + MEMO ITEM 8)
# ============================================================
logf("--- SECTION 8: Pooled window estimates ---", log_file = log_file)

# Create window variable (full sample, unit-weighted)
reg_dt_wtd[, window := fcase(
  get(fp$et_var) %in% fp$pre_window, "1_pre",
  get(fp$et_var) %in% fp$transition_window, "2_transition",
  get(fp$et_var) %in% fp$early_post_window, "3_early_post",
  get(fp$et_var) %in% fp$late_post_window, "4_late_post",
  default = NA_character_
)]
reg_dt_pooled <- reg_dt_wtd[!is.na(window)]

# Subgroups for pooled analysis
pooled_subgroups <- list(
  full = reg_dt_pooled,
  corporate = reg_dt_pooled[is_corp == TRUE],
  individual = reg_dt_pooled[is_corp == FALSE]
)
# Add acquirer bins
for (ab in acq_bins_ordered) {
  key <- acq_bin_suffix(ab)
  pooled_subgroups[[key]] <- reg_dt_pooled[acq_filer_bin == ab]
}
# Add portfolio property-count bins
for (pb in portfolio_bins) {
  key <- paste0("portfolio_", gsub("[^a-z0-9_]", "", gsub("[+ ]", "_", tolower(pb))))
  pooled_subgroups[[key]] <- reg_dt_pooled[portfolio_bin == pb]
}

pooled_results <- list()
pooled_fits    <- list()   # stored for vcov-correct SE in post-minus-pre
for (sg_name in names(pooled_subgroups)) {
  sg_dt <- pooled_subgroups[[sg_name]]
  n_sg <- nrow(sg_dt)
  n_pids_sg <- sg_dt[, uniqueN(PID)]
  if (n_sg < 100 || n_pids_sg < 10) {
    logf("  Pooled '", sg_name, "': skipping (N=", n_sg, ")", log_file = log_file)
    next
  }

  pool_fml <- if (freq == "annual") {
    filing_rate ~ window | PID + year
  } else {
    filing_rate_q ~ window | PID + yq
  }
  fit_pooled <- do.call(feols, list(fml = pool_fml, data = sg_dt,
                                    cluster = ~PID, weights = ~total_units))
  ct <- as.data.table(coeftable(fit_pooled), keep.rownames = "term")
  setnames(ct, c("term", "estimate", "std_error", "t_value", "p_value"))
  ct[, subgroup := sg_name]
  ct[, n := fit_pooled$nobs]
  pooled_results[[length(pooled_results) + 1]] <- ct
  pooled_fits[[sg_name]] <- fit_pooled

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
      # SE from vcov — early/late are correlated (same regression), so independence
      # assumption understates SE. Use Var((X+Y)/2) = (Var(X) + 2Cov(X,Y) + Var(Y)) / 4.
      fit_sg <- pooled_fits[[sg_name]]
      e_nm <- "window3_early_post"
      l_nm <- "window4_late_post"
      if (!is.null(fit_sg) && e_nm %in% rownames(vcov(fit_sg)) && l_nm %in% rownames(vcov(fit_sg))) {
        vc <- vcov(fit_sg)
        var_avg <- (vc[e_nm, e_nm] + 2 * vc[e_nm, l_nm] + vc[l_nm, l_nm]) / 4
        avg_se <- sqrt(pmax(var_avg, 0))
      } else {
        avg_se <- sqrt((early$std_error^2 + late$std_error^2) / 4)
      }
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
    pps_dt[, subgroup_label := clean_spec_labels(subgroup)]
    pps_dt[subgroup_label == subgroup, subgroup_label := subgroup]

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
                                "highfiler_portfolio_10plus_otherunits", "singlepurchase",
                                "portfolio_single_purchase", "portfolio_2_4",
                                "portfolio_5_9", "portfolio_10_"),
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
# SECTION 9: Diagnostics (MEMO ITEMS 6, 7)
# ============================================================
logf("--- SECTION 9: Diagnostics ---", log_file = log_file)

# 7a: Composition by event time (MEMO ITEM 6, pre-COVID)
logf("  Composition by event time (pre-COVID)...", log_file = log_file)
composition <- event_panel[year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX, .(
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
balance_acq <- event_panel[get(fp$et_var) %in% pre_periods &
                             year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX &
                             !is.na(acq_filer_bin), .(
  mean_filing_rate = round(mean(get(fp$outcome_var), na.rm = TRUE), 5),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  n = .N
), by = acq_filer_bin]

# Add violations if available
if ("total_violations" %in% names(event_panel)) {
  balance_viol <- event_panel[get(fp$et_var) %in% pre_periods &
                               year >= max(2007L, ANALYSIS_YEAR_MIN) &
                               year <= ANALYSIS_YEAR_MAX & !is.na(acq_filer_bin), .(
    mean_violations = round(mean(total_violations, na.rm = TRUE), 2)
  ), by = acq_filer_bin]
  balance_acq <- merge(balance_acq, balance_viol, by = "acq_filer_bin", all.x = TRUE)
}
fwrite(balance_acq, out_path("balance_by_acq.csv"))
logf("  Wrote balance_by_acq.csv", log_file = log_file)

# Pre-transfer balance by buyer type
balance_buyer <- event_panel[get(fp$et_var) %in% pre_periods &
                               year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX &
                               !is.na(buyer_type), .(
  mean_filing_rate = round(mean(get(fp$outcome_var), na.rm = TRUE), 5),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  n = .N
), by = buyer_type]
if ("total_violations" %in% names(event_panel)) {
  bv <- event_panel[get(fp$et_var) %in% pre_periods &
                     year >= max(2007L, ANALYSIS_YEAR_MIN) &
                     year <= ANALYSIS_YEAR_MAX & !is.na(buyer_type), .(
    mean_violations = round(mean(total_violations, na.rm = TRUE), 2)
  ), by = buyer_type]
  balance_buyer <- merge(balance_buyer, bv, by = "buyer_type", all.x = TRUE)
}
fwrite(balance_buyer, out_path("balance_by_buyer.csv"))

# 7c: Pre-trend diagnostic (raw means)
pretrend_means <- event_panel[!is.na(get(fp$outcome_var)) &
                                year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX, .(
  mean_filing_rate = mean(get(fp$outcome_var), na.rm = TRUE),
  mean_filings = mean(get(fp$count_var), na.rm = TRUE),
  n = .N
), by = c(fp$et_var)][order(get(fp$et_var))]
if ("total_violations" %in% names(event_panel)) {
  pt_viol <- event_panel[!is.na(get(fp$outcome_var)) &
                           year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX, .(
    mean_violations = mean(total_violations, na.rm = TRUE)
  ), by = c(fp$et_var)]
  pretrend_means <- merge(pretrend_means, pt_viol, by = c(fp$et_var))
}
fwrite(pretrend_means, out_path("pretrend_means.csv"))

# ============================================================
# SECTION 10: Descriptives and QA
# ============================================================
logf("--- SECTION 10: Descriptives and QA ---", log_file = log_file)

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
  n_conglomerates = uniqueN(conglomerate_id),
  n_pids = uniqueN(PID),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  total_units_sum = sum(total_units, na.rm = TRUE),
  pct_corporate = round(100 * mean(is_corp == TRUE, na.rm = TRUE), 1),
  mean_portfolio = round(mean(n_properties_total, na.rm = TRUE), 1),
  median_price = round(median(total_consideration, na.rm = TRUE), 0),
  mean_acq_rate = round(mean(acq_rate, na.rm = TRUE), 5),
  mean_other_units_transfer = round(mean(other_units_transfer_year, na.rm = TRUE), 1)
), by = acq_filer_bin]
acq_descriptives[, pct_of_transfers := round(100 * n_transfers / sum(n_transfers), 1)]
acq_descriptives[, pct_of_units := round(100 * total_units_sum / sum(total_units_sum), 1)]
fwrite(acq_descriptives, out_path("acq_descriptives.csv"))
logf("  Wrote acq_descriptives.csv", log_file = log_file)

# Corp x filer bin
corp_by_filer_bin <- event_panel[et0_flag & !is.na(acq_filer_bin), .(
  n_transfers = .N,
  n_conglomerates = uniqueN(conglomerate_id),
  mean_units = round(mean(total_units, na.rm = TRUE), 1),
  total_units_sum = sum(total_units, na.rm = TRUE),
  mean_portfolio = round(mean(n_properties_total, na.rm = TRUE), 1),
  mean_acq_rate = round(mean(acq_rate, na.rm = TRUE), 5),
  mean_other_units_transfer = round(mean(other_units_transfer_year, na.rm = TRUE), 1)
), by = .(acq_filer_bin, is_corp)]
corp_by_filer_bin[, pct_of_bin := round(100 * n_transfers / sum(n_transfers), 1),
                  by = acq_filer_bin]
setorder(corp_by_filer_bin, acq_filer_bin, -is_corp)
fwrite(corp_by_filer_bin, out_path("corp_by_filer_bin.csv"))

# Descriptives file
fwrite(desc_by_buyer, out_path("descriptives.csv"))

# Coverage by event time
coverage <- event_panel[year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX, .(
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
    paste0("Analysis years: ", ANALYSIS_YEAR_MIN, "-", ANALYSIS_YEAR_MAX),
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

# ============================================================
# SECTION 10b: Phase 4 (Business Filing) Consolidation Diagnostics
# ============================================================
logf("--- SECTION 10b: Phase 4 consolidation diagnostics ---", log_file = log_file)

# Portfolio bin distribution for Phase 4-expanded vs. non-Phase-4 conglomerates.
# Transfers in Phase 4 conglomerates are those where the entity-resolution step
# used business filing records to merge shell company networks into a single
# conglomerate. These are acquirers who would have appeared as "Single-purchase"
# under entity-level (pre-Phase-4) matching but now correctly inherit a portfolio.
phase4_portfolio_tbl <- event_panel[et0_flag & !is.na(portfolio_bin), .(
  n_transfers = .N,
  pct_of_type  = NA_real_,     # filled below
  mean_portfolio = round(mean(n_properties_total, na.rm = TRUE), 1),
  pct_corporate  = round(100 * mean(is_corp == TRUE, na.rm = TRUE), 1)
), by = .(phase4_conglomerate, portfolio_bin)]
phase4_portfolio_tbl[, pct_of_type := round(100 * n_transfers / sum(n_transfers), 1),
                     by = phase4_conglomerate]
setorder(phase4_portfolio_tbl, phase4_conglomerate, portfolio_bin)
fwrite(phase4_portfolio_tbl, out_path("phase4_consolidation.csv"))
logf("  Wrote ", fp$file_prefix, "_phase4_consolidation.csv", log_file = log_file)

# Summary stats
n_p4_total    <- event_panel[et0_flag & phase4_conglomerate == TRUE, .N]
n_p4_portfolio <- event_panel[et0_flag & phase4_conglomerate == TRUE &
                                !is.na(portfolio_bin) & portfolio_bin != "Single-purchase", .N]
n_all_et0 <- sum(et0_flag)
logf("  Transfers in Phase 4-expanded conglomerates (at et=0, year<=2019): ",
     n_p4_total, " of ", n_all_et0,
     " (", round(100 * n_p4_total / n_all_et0, 1), "%)", log_file = log_file)
logf("  Of those, with portfolio_bin != Single-purchase: ",
     n_p4_portfolio, " (",
     round(100 * n_p4_portfolio / max(n_p4_total, 1L), 1), "%)",
     log_file = log_file)

# Conglomerate-level size distribution for Phase 4 conglomerates in the transfer sample
if (n_p4_total > 0) {
  p4_cong_sizes <- unique(event_panel[phase4_conglomerate == TRUE & et0_flag,
                                       .(conglomerate_id, n_properties_total)])
  logf("  Distinct Phase 4 conglomerates in transfer sample: ", nrow(p4_cong_sizes),
       "; median size: ", median(p4_cong_sizes$n_properties_total, na.rm = TRUE),
       log_file = log_file)
}

}

# ============================================================
# ANNUAL OVERRIDE: canonical full-panel never-sold-control design
# ============================================================
if (freq == "annual" && !is.null(full_panel_main)) {
  logf("--- ANNUAL OVERRIDE: full-panel never-sold-control event studies ---",
       log_file = log_file)

  acq_bins_ordered <- ACQ_BIN_LABELS

  subset_panel_main <- function(panel, sample_filter = NULL, treated_filter = NULL,
                                outcome_col = NULL, year_min = NULL) {
    out <- copy(panel)
    if (!is.null(year_min)) out <- out[year >= year_min]
    if (!is.null(sample_filter)) out <- out[eval(sample_filter)]
    if (!is.null(treated_filter)) out <- out[is.na(transfer_year) | eval(treated_filter)]
    if (!is.null(outcome_col)) out <- out[!is.na(get(outcome_col))]
    out
  }

  run_named_fullpanel <- function(panel, spec_label, weights = ~total_units,
                                  lhs = "filing_rate", fe = "PID + year",
                                  dummies = et_dummies_main) {
    fml_str <- paste0(lhs, " ~ ", paste(dummies, collapse = " + "), " | ", fe)
    run_fullpanel_fit(panel, fml_str, spec_label, weights = weights)
  }
  run_named_fullpanel_poisson <- function(panel, spec_label,
                                          lhs = "num_filings", fe = "PID + year") {
    fml_str <- paste0(lhs, " ~ ", paste(et_dummies_main, collapse = " + "), " | ", fe)
    run_fullpanel_fit_poisson(panel, fml_str, spec_label, offset = ~log(total_units))
  }
  core_baseline_specs <- list(
    list(label = "core_all_units", sample_filter = NULL, pretty = "All buildings"),
    list(label = "core_1unit", sample_filter = quote(total_units == 1L), pretty = "1-unit"),
    list(label = "core_2plus", sample_filter = quote(total_units >= 2L), pretty = "2+ units"),
    list(label = "core_2plus_rental_evidence",
         sample_filter = quote(total_units >= 2L & observed_rental_stock == TRUE),
         pretty = "2+ units, rental evidence"),
    list(label = "core_5plus", sample_filter = quote(total_units >= 5L), pretty = "5+ units"),
    list(label = "core_5plus_rental_evidence",
         sample_filter = quote(total_units >= 5L & observed_rental_stock == TRUE),
         pretty = "5+ units, rental evidence"),
    list(label = "core_10plus", sample_filter = quote(total_units >= 10L), pretty = "10+ units"),
    list(label = "core_10plus_rental_evidence",
         sample_filter = quote(total_units >= 10L & observed_rental_stock == TRUE),
         pretty = "10+ units, rental evidence")
  )
  FILING_CORE_MAIN_PLOT_SPECS <- c(
    "core_1unit",
    "core_2plus",
    "core_5plus",
    "core_5plus_rental_evidence"
  )
  FILING_CORE_COLORS <- c(
    core_1unit = LEGACY_EVENT_COLORS[1],
    core_2plus = LEGACY_EVENT_COLORS[2],
    core_5plus = LEGACY_EVENT_COLORS[3],
    core_5plus_rental_evidence = LEGACY_EVENT_COLORS[4]
  )
  FILING_ACQ_MAIN_PLOT_SPECS <- c(
    acq_bin_suffix("High-filer portfolio"),
    acq_bin_suffix("Low-filer portfolio"),
    acq_bin_suffix("Small portfolio"),
    acq_bin_suffix("Single-purchase")
  )
  FILING_ACQ_COLORS <- c(
    setNames(LEGACY_EVENT_COLORS[1], acq_bin_suffix("High-filer portfolio")),
    setNames(LEGACY_EVENT_COLORS[2], acq_bin_suffix("Low-filer portfolio")),
    setNames(LEGACY_EVENT_COLORS[3], acq_bin_suffix("Small portfolio")),
    setNames(LEGACY_EVENT_COLORS[4], acq_bin_suffix("Single-purchase"))
  )
  run_core_sample_grid <- function(panel, outcome_col, out_stem, plot_title, y_label,
                                   specs = core_baseline_specs, weights = ~total_units,
                                   estimator = c("fullpanel", "stacked"),
                                   plot_specs = NULL, plot_colors = NULL,
                                   plot_facet = TRUE, plot_show_n = TRUE) {
    estimator <- match.arg(estimator)
    coef_list <- list()
    fit_list <- list()
    label_map <- character()
    for (sp in specs) {
      .panel <- subset_panel_main(panel, sample_filter = sp$sample_filter, outcome_col = outcome_col)
      .res <- if (estimator == "stacked") {
        run_weighted_stacked_fit(
          panel = .panel,
          outcome_col = outcome_col,
          spec_label = sp$label,
          event_window = fp$et_range,
          ref_period = ET_REF
        )
      } else {
        run_named_fullpanel(.panel, sp$label, weights = weights, lhs = outcome_col)
      }
      if (!is.null(.res)) {
        coef_list[[length(coef_list) + 1L]] <- .res$coefs
        fit_list[[sp$pretty]] <- .res$fit
        label_map[[sp$label]] <- sp$pretty
      }
    }
    if (length(coef_list) == 0L) return(invisible(NULL))
    all_coefs <- rbindlist(coef_list, use.names = TRUE, fill = TRUE)
    fwrite(all_coefs, out_path(paste0(out_stem, "_coefs.csv")))
    plot_event_coefs(
      all_coefs,
      plot_specs %||% unique(all_coefs$spec),
      paste0(plot_title, " (", fp$label, ")"),
      fig_path(paste0(out_stem, ".png")),
      facet = plot_facet,
      y_label = y_label,
      custom_labels = label_map,
      colors = plot_colors,
      show_n_in_labels = plot_show_n
    )
    etable(
      fit_list,
      file = out_path(paste0(out_stem, "_etable.tex")),
      style.tex = style.tex("aer"),
      headers = list("Sample" = names(fit_list))
    )
    all_coefs
  }
  run_core_acq_split <- function(panel, outcome_col, out_stem, plot_title, y_label,
                                 sample_filter = quote(total_units >= 2L),
                                 weights = ~total_units,
                                 estimator = c("fullpanel", "stacked"),
                                 plot_specs = NULL, plot_colors = NULL,
                                 plot_facet = TRUE, plot_show_n = TRUE) {
    estimator <- match.arg(estimator)
    coef_list <- list()
    fit_list <- list()
    label_map <- character()
    for (ab in acq_bins_ordered) {
      .panel <- subset_panel_main(
        panel,
        sample_filter = sample_filter,
        treated_filter = substitute(acq_filer_bin == AB, list(AB = ab)),
        outcome_col = outcome_col
      )
      .spec <- acq_bin_suffix(ab)
      .res <- if (estimator == "stacked") {
        run_weighted_stacked_fit(
          panel = .panel,
          outcome_col = outcome_col,
          spec_label = .spec,
          event_window = fp$et_range,
          ref_period = ET_REF
        )
      } else {
        run_named_fullpanel(.panel, .spec, weights = weights, lhs = outcome_col)
      }
      if (!is.null(.res)) {
        coef_list[[length(coef_list) + 1L]] <- .res$coefs
        fit_list[[ab]] <- .res$fit
        label_map[[.spec]] <- ab
      }
    }
    if (length(coef_list) == 0L) return(invisible(NULL))
    all_coefs <- rbindlist(coef_list, use.names = TRUE, fill = TRUE)
    fwrite(all_coefs, out_path(paste0(out_stem, "_coefs.csv")))
    plot_event_coefs(
      all_coefs,
      plot_specs %||% unique(all_coefs$spec),
      paste0(plot_title, " (", fp$label, ")"),
      fig_path(paste0(out_stem, ".png")),
      facet = plot_facet,
      y_label = y_label,
      custom_labels = label_map,
      colors = plot_colors,
      show_n_in_labels = plot_show_n
    )
    etable(
      fit_list,
      file = out_path(paste0(out_stem, "_etable.tex")),
      style.tex = style.tex("aer"),
      headers = list("Acquirer type" = names(fit_list))
    )
    all_coefs
  }
  build_supported_outcome_panel <- function(panel, outcome_col, sample_filter = NULL,
                                            min_control_obs = 2L) {
    req_cols <- c(outcome_col, "PID", "year", "event_time", "transfer_year")
    assert_has_cols(panel, req_cols, paste0("build_supported_outcome_panel: ", outcome_col))
    src <- subset_panel_main(panel, sample_filter = sample_filter, outcome_col = outcome_col)
    tx_cov <- src[!is.na(transfer_year),
                  .(has_pre = any(event_time < 0L, na.rm = TRUE),
                    has_post = any(event_time >= 0L, na.rm = TRUE)),
                  by = PID]
    valid_tx <- tx_cov[has_pre == TRUE & has_post == TRUE, PID]
    ctrl_cov <- src[is.na(transfer_year), .(n_obs = .N), by = PID]
    valid_ctrl <- ctrl_cov[n_obs >= min_control_obs, PID]
    out <- src[
      (!is.na(transfer_year) & PID %chin% valid_tx) |
      (is.na(transfer_year) & PID %chin% valid_ctrl)
    ]
    logf(
      "    supported panel ", outcome_col, ": ",
      out[, uniqueN(PID)], " PIDs (treated=", out[!is.na(transfer_year), uniqueN(PID)],
      ", controls=", out[is.na(transfer_year), uniqueN(PID)], "), N=", nrow(out),
      log_file = log_file
    )
    out
  }

  logf("--- ANNUAL CORE OUTPUTS: streamlined regression surface ---", log_file = log_file)

  filing_core_all <- run_core_sample_grid(
    panel = full_panel_main,
    outcome_col = "filing_rate",
    out_stem = "filing_core",
    plot_title = "Weighted Stacked Event Study: Filing Rate Around Transfer",
    y_label = "Filing Rate (per unit-year)",
    estimator = "stacked",
    plot_specs = FILING_CORE_MAIN_PLOT_SPECS,
    plot_colors = FILING_CORE_COLORS,
    plot_facet = FALSE,
    plot_show_n = FALSE
  )
  filing_acq_2plus <- run_core_acq_split(
    panel = full_panel_filer,
    outcome_col = "filing_rate",
    out_stem = "filing_acq_2plus",
    plot_title = "Weighted Stacked Event Study: Filing Rate by Acquirer Type",
    y_label = "Filing Rate (per unit-year)",
    sample_filter = quote(total_units >= 2L),
    estimator = "stacked",
    plot_specs = FILING_ACQ_MAIN_PLOT_SPECS,
    plot_colors = FILING_ACQ_COLORS,
    plot_facet = FALSE,
    plot_show_n = FALSE
  )

  for (.demo in list(
    list(col = "occupancy_rate", stem = "occupancy_2plus", label = "Occupancy Rate"),
    list(col = "infousa_pct_black", stem = "pct_black_2plus", label = "Share Black Residents"),
    list(col = "infousa_pct_asian", stem = "pct_asian_2plus", label = "Share Asian Residents"),
    list(col = "infousa_pct_hispanic", stem = "pct_latino_2plus", label = "Share Latino Residents"),
    list(col = "infousa_pct_white", stem = "pct_white_2plus", label = "Share White Residents"),
    list(col = "infousa_pct_other", stem = "pct_other_2plus", label = "Share Other Residents"),
    list(col = "infousa_pct_female", stem = "pct_female_2plus", label = "Share Female Residents"),
    list(col = "infousa_pct_male", stem = "pct_male_2plus", label = "Share Male Residents"),
    list(col = "permit_rate", stem = "permit_rate_2plus", label = "Permit Rate (per unit-year)"),
    list(col = "complaint_rate", stem = "complaint_rate_2plus", label = "Complaint Rate (per unit-year)")
  )) {
    run_core_acq_split(
      panel = full_panel_filer,
      outcome_col = .demo$col,
      out_stem = .demo$stem,
      plot_title = paste0("Event Study: ", .demo$label, " by Acquirer Filing Type"),
      y_label = .demo$label,
      sample_filter = quote(total_units >= 2L)
    )
  }

  if ("log_med_rent" %in% names(full_panel_main)) {
    rent_supported <- build_supported_outcome_panel(full_panel_main, "log_med_rent")
    rent_supported_filer <- build_supported_outcome_panel(full_panel_filer, "log_med_rent")
    run_core_acq_split(
      panel = rent_supported_filer,
      outcome_col = "log_med_rent",
      out_stem = "rent_core",
      plot_title = "Event Study: Log Median Rent by Acquirer Filing Type",
      y_label = "Log median rent",
      sample_filter = NULL
    )
  }
  if ("log_med_rent_safe" %in% names(full_panel_main)) {
    rent_safe_supported_filer <- build_supported_outcome_panel(full_panel_filer, "log_med_rent_safe")
    run_core_acq_split(
      panel = rent_safe_supported_filer,
      outcome_col = "log_med_rent_safe",
      out_stem = "rent_safe_core",
      plot_title = "Event Study: Log Median Rent (Safe, single-source) by Acquirer Filing Type",
      y_label = "Log median rent (safe)",
      sample_filter = NULL
    )
  }

  entry_panel_core <- full_panel_main[
    year >= max(2011L, ANALYSIS_YEAR_MIN) &
      year <= ANALYSIS_YEAR_MAX &
      !is.na(total_units) & total_units > 0
  ]
  if ("has_rental_evidence" %in% names(entry_panel_core)) {
    entry_panel_core_filer <- full_panel_filer[
      year >= max(2011L, ANALYSIS_YEAR_MIN) &
        year <= ANALYSIS_YEAR_MAX &
        !is.na(total_units) & total_units > 0
    ]
    run_core_acq_split(
      panel = entry_panel_core_filer,
      outcome_col = "has_rental_evidence",
      out_stem = "rental_evidence_1unit",
      plot_title = "Event Study: Probability of Rental Evidence (1-unit) by Acquirer Filing Type",
      y_label = "Probability of rental evidence",
      sample_filter = quote(total_units == 1L)
    )
    run_core_acq_split(
      panel = entry_panel_core_filer,
      outcome_col = "has_rental_evidence",
      out_stem = "rental_evidence_2plus",
      plot_title = "Event Study: Probability of Rental Evidence (2+ units) by Acquirer Filing Type",
      y_label = "Probability of rental evidence",
      sample_filter = quote(total_units >= 2L)
    )
  }


  # --- Diagnostics and descriptives based on first-transfer treated sample ---
  treated_event_panel_main <- merge(
    event_panel[year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX],
    first_transfer_dt[, .(PID, transfer_year)],
    by = c("PID", "transfer_year")
  )
  assert_unique(treated_event_panel_main, c("PID", "transfer_year", "event_time"),
                "treated_event_panel_main")

  composition <- treated_event_panel_main[, .(
    n_obs = .N,
    n_pids = uniqueN(PID),
    pct_units_observed = round(100 * mean(!is.na(filing_rate)), 1),
    mean_units = round(mean(total_units, na.rm = TRUE), 1),
    pct_rental_pre = round(100 * mean(known_rental_pre, na.rm = TRUE), 1),
    pct_rental_post_only = round(100 * mean(known_rental_post_only, na.rm = TRUE), 1),
    pct_corporate = round(100 * mean(is_corp == TRUE, na.rm = TRUE), 1),
    pct_large = round(100 * mean(large_building == TRUE, na.rm = TRUE), 1)
  ), by = event_time][order(event_time)]
  fwrite(composition, out_path("composition_by_et.csv"))

  balance_acq <- treated_event_panel_main[event_time %in% fp$pre_window & !is.na(acq_filer_bin), .(
    mean_filing_rate = round(mean(filing_rate, na.rm = TRUE), 5),
    mean_units = round(mean(total_units, na.rm = TRUE), 1),
    n = .N
  ), by = acq_filer_bin]
  if ("total_violations" %in% names(treated_event_panel_main)) {
    balance_viol <- treated_event_panel_main[event_time %in% fp$pre_window & year >= 2007L & !is.na(acq_filer_bin), .(
      mean_violations = round(mean(total_violations, na.rm = TRUE), 2)
    ), by = acq_filer_bin]
    balance_acq <- merge(balance_acq, balance_viol, by = "acq_filer_bin", all.x = TRUE)
  }
  fwrite(balance_acq, out_path("balance_by_acq.csv"))

  balance_buyer <- treated_event_panel_main[event_time %in% fp$pre_window & !is.na(buyer_type), .(
    mean_filing_rate = round(mean(filing_rate, na.rm = TRUE), 5),
    mean_units = round(mean(total_units, na.rm = TRUE), 1),
    n = .N
  ), by = buyer_type]
  if ("total_violations" %in% names(treated_event_panel_main)) {
    bv <- treated_event_panel_main[event_time %in% fp$pre_window & year >= 2007L & !is.na(buyer_type), .(
      mean_violations = round(mean(total_violations, na.rm = TRUE), 2)
    ), by = buyer_type]
    balance_buyer <- merge(balance_buyer, bv, by = "buyer_type", all.x = TRUE)
  }
  fwrite(balance_buyer, out_path("balance_by_buyer.csv"))

  pretrend_means <- treated_event_panel_main[!is.na(filing_rate), .(
    mean_filing_rate = mean(filing_rate, na.rm = TRUE),
    mean_filings = mean(num_filings, na.rm = TRUE),
    n = .N
  ), by = event_time][order(event_time)]
  if ("total_violations" %in% names(treated_event_panel_main)) {
    pt_viol <- treated_event_panel_main[!is.na(filing_rate), .(
      mean_violations = mean(total_violations, na.rm = TRUE)
    ), by = event_time]
    pretrend_means <- merge(pretrend_means, pt_viol, by = "event_time", all.x = TRUE)
  }
  fwrite(pretrend_means, out_path("pretrend_means.csv"))

  et0_main <- treated_event_panel_main[event_time == 0L]
  desc_by_buyer <- et0_main[!is.na(buyer_type), .(
    n_transfers = .N,
    mean_units = round(mean(total_units, na.rm = TRUE), 1),
    median_units = as.double(median(total_units, na.rm = TRUE)),
    mean_filing_rate = round(mean(filing_rate, na.rm = TRUE), 4),
    mean_violations = if ("total_violations" %in% names(.SD))
      round(mean(total_violations, na.rm = TRUE), 2) else NA_real_,
    median_price = as.double(median(total_consideration, na.rm = TRUE)),
    pct_sheriff = round(100 * mean(is_sheriff_deed, na.rm = TRUE), 1)
  ), by = buyer_type]
  fwrite(desc_by_buyer, out_path("descriptives.csv"))

  acq_descriptives <- et0_main[!is.na(acq_filer_bin), .(
    n_transfers = .N,
    n_conglomerates = uniqueN(conglomerate_id),
    n_pids = uniqueN(PID),
    mean_units = round(mean(total_units, na.rm = TRUE), 1),
    total_units_sum = sum(total_units, na.rm = TRUE),
    pct_corporate = round(100 * mean(is_corp == TRUE, na.rm = TRUE), 1),
    mean_portfolio = round(mean(n_properties_total, na.rm = TRUE), 1),
    median_price = round(median(total_consideration, na.rm = TRUE), 0),
    mean_acq_rate = round(mean(acq_rate, na.rm = TRUE), 5),
    mean_other_units_transfer = round(mean(other_units_transfer_year, na.rm = TRUE), 1)
  ), by = acq_filer_bin]
  acq_descriptives[, pct_of_transfers := round(100 * n_transfers / sum(n_transfers), 1)]
  acq_descriptives[, pct_of_units := round(100 * total_units_sum / sum(total_units_sum), 1)]
  fwrite(acq_descriptives, out_path("acq_descriptives.csv"))

  corp_by_filer_bin <- et0_main[!is.na(acq_filer_bin), .(
    n_transfers = .N,
    n_conglomerates = uniqueN(conglomerate_id),
    mean_units = round(mean(total_units, na.rm = TRUE), 1),
    total_units_sum = sum(total_units, na.rm = TRUE),
    mean_portfolio = round(mean(n_properties_total, na.rm = TRUE), 1),
    mean_acq_rate = round(mean(acq_rate, na.rm = TRUE), 5),
    mean_other_units_transfer = round(mean(other_units_transfer_year, na.rm = TRUE), 1)
  ), by = .(acq_filer_bin, is_corp)]
  corp_by_filer_bin[, pct_of_bin := round(100 * n_transfers / sum(n_transfers), 1), by = acq_filer_bin]
  setorder(corp_by_filer_bin, acq_filer_bin, -is_corp)
  fwrite(corp_by_filer_bin, out_path("corp_by_filer_bin.csv"))

  phase4_portfolio_tbl <- et0_main[!is.na(portfolio_bin), .(
    n_transfers = .N,
    pct_of_type = NA_real_,
    mean_portfolio = round(mean(n_properties_total, na.rm = TRUE), 1),
    pct_corporate = round(100 * mean(is_corp == TRUE, na.rm = TRUE), 1)
  ), by = .(phase4_conglomerate, portfolio_bin)]
  phase4_portfolio_tbl[, pct_of_type := round(100 * n_transfers / sum(n_transfers), 1),
                       by = phase4_conglomerate]
  setorder(phase4_portfolio_tbl, phase4_conglomerate, portfolio_bin)
  fwrite(phase4_portfolio_tbl, out_path("phase4_consolidation.csv"))
}

logf("=== analyze-transfer-evictions-unified.R (", fp$label, ") complete ===",
     log_file = log_file)
