## ============================================================
## retaliatory-evictions.r
## ============================================================
## Purpose: Estimate retaliatory timing patterns between complaints
## and eviction filings using distributed-lag PID x period models,
## plus eviction-bandwidth diagnostics.
##
## Inputs:
##   - processed/panels/building_data_rental_month.parquet (default)
##   - processed/analytic/analytic_sample.csv (PID universe filter)
##   - processed/products evictions_clean + evict_address_xwalk
##   - processed/analytic/bldg_panel_blp.csv (tenant composition)
##
## Outputs:
##   - output/retaliatory/retaliatory_{monthly|quarterly}_distlag_*.csv/png
##   - output/retaliatory/retaliatory_{monthly|quarterly}_bandwidth_*.csv
##   - output/retaliatory/retaliatory_{monthly|quarterly}_qa.txt
##   - output/logs/retaliatory-evictions.log
##
## EB Filing Intensity Memo (source: r/make-analytic-sample.R, section 6b):
##   We use the pre-COVID building-level Empirical Bayes filing rate
##   `filing_rate_eb_pre_covid` as the default filing-intensity measure.
##   Construction is Gamma-Poisson shrinkage on pooled pre-2019 filings:
##     y_it | lambda_i ~ Poisson(exposure_it * lambda_i)
##     lambda_i ~ Gamma(alpha, beta)
##   with sufficient stats Y_i = sum_t y_it and E_i = sum_t exposure_it, giving
##     E[lambda_i | data] = (alpha + Y_i) / (beta + E_i).
##   City and ZIP priors are estimated by method-of-moments, with ZIP falling
##   back to city prior when ZIP support is thin. Raw long-run filing rate is
##   retained as a robustness measure only.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(fixest)
  library(ggplot2)
})

source("r/config.R")
source("r/helper-functions.R")

parse_cli_args <- function(args) {
  out <- list()
  if (length(args) == 0L) return(out)
  for (arg in args) {
    if (!startsWith(arg, "--")) next
    arg <- sub("^--", "", arg)
    parts <- strsplit(arg, "=", fixed = TRUE)[[1L]]
    key <- gsub("-", "_", parts[1L])
    val <- if (length(parts) >= 2L) paste(parts[-1L], collapse = "=") else "TRUE"
    out[[key]] <- val
  }
  out
}

to_int <- function(x, default) {
  if (is.null(x) || length(x) == 0L) return(as.integer(default))
  x1 <- x[[1L]]
  if (is.na(x1) || !nzchar(as.character(x1))) return(as.integer(default))
  v <- suppressWarnings(as.integer(x1))
  if (is.na(v)) as.integer(default) else v
}

to_bool <- function(x, default = FALSE) {
  if (is.null(x) || is.na(x)) return(default)
  tolower(as.character(x)) %chin% c("1", "true", "t", "yes", "y")
}

parse_tf_flag <- function(x) {
  if (is.logical(x)) return(x)
  x_chr <- tolower(trimws(as.character(x)))
  out <- rep(NA, length(x_chr))
  out[x_chr %chin% c("1", "true", "t", "yes", "y")] <- TRUE
  out[x_chr %chin% c("0", "false", "f", "no", "n")] <- FALSE
  as.logical(out)
}

normalize_time_unit <- function(x) {
  x1 <- tolower(trimws(as.character(x %||% "")))
  if (x1 %chin% c("month", "months", "m")) return("month")
  if (x1 %chin% c("quarter", "quarters", "q", "year_quarter", "year-quarter", "yq")) return("quarter")
  stop("Unsupported time_unit='", x1, "'. Use 'month' or 'quarter'.")
}

stop_missing_cols <- function(available_cols, required_cols, context) {
  missing_cols <- setdiff(required_cols, available_cols)
  if (length(missing_cols)) {
    stop("Missing required columns in ", context, ": ", paste(missing_cols, collapse = ", "))
  }
  invisible(TRUE)
}

build_pid_filter_cmd <- function(pid_file, csv_file) {
  sprintf(
    "awk -F, 'NR==FNR{keep[$1]=1; next} FNR==1 || ($1 in keep)' %s %s",
    shQuote(pid_file),
    shQuote(csv_file)
  )
}

read_panel_slice <- function(panel_path, select_cols, pid_filter = NULL) {
  ext <- tolower(tools::file_ext(panel_path))

  if (ext == "parquet") {
    if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("Package 'arrow' is required to read parquet: ", panel_path)
    }
    ds <- arrow::open_dataset(panel_path, format = "parquet")
    avail <- names(ds)
    stop_missing_cols(avail, select_cols, "building panel parquet")
    dt <- as.data.table(arrow::read_parquet(panel_path, col_select = select_cols))
  } else {
    hdr <- names(fread(panel_path, nrows = 0L))
    stop_missing_cols(hdr, select_cols, "building panel flat file")

    if (!is.null(pid_filter) && length(pid_filter)) {
      pid_file <- tempfile(fileext = ".txt")
      writeLines(pid_filter, pid_file)
      on.exit(unlink(pid_file), add = TRUE)
      cmd <- build_pid_filter_cmd(pid_file, panel_path)
      dt <- fread(cmd = cmd, select = select_cols)
    } else {
      dt <- fread(panel_path, select = select_cols)
    }
  }

  assert_has_cols(dt, c("parcel_number", "period", "total_complaints", "total_severe_complaints"), "building panel slice")
  setDT(dt)
  setnames(dt, "parcel_number", "PID")
  dt[, PID := stringr::str_pad(as.character(PID), width = 9L, side = "left", pad = "0")]
  if (!is.null(pid_filter) && length(pid_filter)) {
    dt <- dt[PID %chin% pid_filter]
  }
  dt
}

add_tenant_composition <- function(dt, bldg_panel_path, start_year, end_year, pid_filter = NULL) {
  tenant_cols <- c(
    "infousa_pct_black",
    "infousa_pct_female",
    "infousa_pct_black_female",
    "infousa_share_persons_demog_ok"
  )

  hdr <- names(fread(bldg_panel_path, nrows = 0L))
  required_cols <- c("PID", "year", tenant_cols)
  stop_missing_cols(hdr, required_cols, "bldg_panel_blp")

  bldg <- fread(bldg_panel_path, select = required_cols)
  setDT(bldg)
  bldg[, PID := stringr::str_pad(as.character(PID), width = 9L, side = "left", pad = "0")]
  bldg[, year := suppressWarnings(as.integer(year))]
  if (!is.null(pid_filter) && length(pid_filter)) bldg <- bldg[PID %chin% pid_filter]
  bldg <- bldg[year >= start_year & year <= end_year]

  for (cc in tenant_cols) {
    bldg[, (cc) := suppressWarnings(as.numeric(get(cc)))]
  }

  bldg <- unique(bldg, by = c("PID", "year"))
  assert_unique(bldg, c("PID", "year"), "bldg tenant composition (PID-year)")

  setkey(dt, PID, year)
  setkey(bldg, PID, year)
  out <- bldg[dt]
  for (cc in tenant_cols) {
    out[, (cc) := suppressWarnings(as.numeric(get(cc)))]
  }
  out
}

make_leads_lags <- function(dt, stem, h) {
  filed_col <- paste0("filed_", stem)
  for (k in seq_len(h)) {
    lag_col <- paste0("lag_", stem, "_", k)
    lead_col <- paste0("lead_", stem, "_", k)
    dt[, (lag_col) := data.table::shift(get(filed_col), n = k, type = "lag"), by = PID]
    dt[, (lead_col) := data.table::shift(get(filed_col), n = k, type = "lead"), by = PID]
    dt[is.na(get(lag_col)), (lag_col) := 0L]
    dt[is.na(get(lead_col)), (lead_col) := 0L]
  }
  invisible(dt)
}

build_rhs <- function(stem, h) {
  terms <- c(
    paste0("lag_", stem, "_", h:1L),
    paste0("filed_", stem),
    paste0("lead_", stem, "_", 1L:h)
  )
  paste(terms, collapse = " + ")
}

fit_dist_lag <- function(dt, stem, h, y = "filed_eviction") {
  rhs <- build_rhs(stem, h)
  fml <- as.formula(paste0(y, " ~ ", rhs, " | period_fe + PID"))
  feols(fml, data = dt, cluster = ~PID, lean = TRUE)
}

extract_dist_lag_coefs <- function(model, stem, sample_name) {
  ct <- as.data.table(summary(model)$coeftable, keep.rownames = "term")
  setnames(
    ct,
    c("Estimate", "Std. Error", "t value", "Pr(>|t|)"),
    c("estimate", "std_error", "t_value", "p_value")
  )
  pat <- paste0("^(lag_", stem, "_|filed_", stem, "$|lead_", stem, "_)")
  ct <- ct[grepl(pat, term)]
  if (!nrow(ct)) return(data.table())

  ct[, timing := fifelse(
    grepl("^lag_", term), -as.integer(str_extract(term, "\\d+$")),
    fifelse(grepl("^lead_", term), as.integer(str_extract(term, "\\d+$")), 0L)
  )]
  ct[, `:=`(
    sample = sample_name,
    stem = stem,
    model = "distributed_lag",
    conf_low = estimate - 1.96 * std_error,
    conf_high = estimate + 1.96 * std_error
  )]
  setorder(ct, sample, stem, timing)
  ct
}

extract_lpm_coefs <- function(model, spec, outcome) {
  ct <- as.data.table(summary(model)$coeftable, keep.rownames = "term")
  setnames(
    ct,
    c("Estimate", "Std. Error", "t value", "Pr(>|t|)"),
    c("estimate", "std_error", "t_value", "p_value")
  )
  ct[, `:=`(
    spec = spec,
    outcome = outcome,
    n_obs = as.integer(nobs(model)),
    conf_low = estimate - 1.96 * std_error,
    conf_high = estimate + 1.96 * std_error
  )]
  setcolorder(ct, c(
    "spec", "outcome", "term", "estimate", "std_error", "t_value", "p_value",
    "conf_low", "conf_high", "n_obs"
  ))
  ct
}

extract_pois_coefs <- function(model, spec, outcome) {
  ct <- as.data.table(summary(model)$coeftable, keep.rownames = "term")
  # fepois uses "z value" / "Pr(>|z|)" instead of "t value" / "Pr(>|t|)"
  setnames(
    ct,
    c("Estimate", "Std. Error", "z value", "Pr(>|z|)"),
    c("estimate", "std_error", "t_value", "p_value")
  )
  ct[, `:=`(
    spec = spec,
    outcome = outcome,
    n_obs = as.integer(nobs(model)),
    conf_low = estimate - 1.96 * std_error,
    conf_high = estimate + 1.96 * std_error
  )]
  setcolorder(ct, c(
    "spec", "outcome", "term", "estimate", "std_error", "t_value", "p_value",
    "conf_low", "conf_high", "n_obs"
  ))
  ct
}

pretty_stem <- function(stem) {
  fifelse(
    stem == "complaint", "Full",
    fifelse(
      stem == "severe", "Severe",
      fifelse(stem == "non_severe", "Non-Severe", stem)
    )
  )
}

make_filing_rate_bin <- function(rate) {
  x <- suppressWarnings(as.numeric(rate))
  out <- fifelse(
    !is.finite(x) | x < 0, NA_character_,
    fifelse(
      x < 0.01, "[0,1)",
      fifelse(
        x < 0.05, "[1,5)",
        fifelse(
          x < 0.10, "[5,10)",
          fifelse(x < 0.20, "[10,20)", "[20,inf)")
        )
      )
    )
  )
  factor(out, levels = c("[0,1)", "[1,5)", "[5,10)", "[10,20)", "[20,inf)"))
}

compute_bandwidth_tables <- function(dt, max_bw, time_unit) {
  out_summary <- vector("list", max_bw)
  out_status <- vector("list", max_bw)

  ev_rows <- dt[filed_eviction == 1L, .(PID, period, filed_complaint)]
  if (!nrow(ev_rows)) stop("No eviction rows available after period-level merge.")

  for (b in seq_len(max_bw)) {
    lag_cols <- paste0("lag_complaint_", seq_len(b))
    lead_cols <- paste0("lead_complaint_", seq_len(b))
    win_cols <- c(lag_cols, lead_cols)
    stop_missing_cols(names(dt), win_cols, "bandwidth complaint lag/lead columns")

    tmp <- dt[filed_eviction == 1L, c("PID", "period", "filed_complaint", win_cols), with = FALSE]
    tmp[, nearby_any := as.integer(rowSums(.SD, na.rm = TRUE) > 0L), .SDcols = win_cols]
    tmp[, within_bw := as.integer(filed_complaint == 1L | nearby_any == 1L)]
    tmp[, status := fifelse(
      filed_complaint == 1L, "same_period",
      fifelse(nearby_any == 1L, "within_bw_not_same_period", "non_retaliatory")
    )]

    s <- tmp[, .(
      bandwidth_periods = b,
      eviction_rows = .N,
      same_period_n = sum(filed_complaint == 1L),
      within_bw_n = sum(within_bw == 1L),
      plausible_only_n = sum(status == "within_bw_not_same_period"),
      non_retaliatory_n = sum(status == "non_retaliatory")
    )]
    s[, `:=`(
      time_unit = time_unit,
      same_period_share = same_period_n / eviction_rows,
      within_bw_share = within_bw_n / eviction_rows,
      plausible_only_share = plausible_only_n / eviction_rows,
      non_retaliatory_share = non_retaliatory_n / eviction_rows
    )]

    st <- tmp[, .N, by = status][order(status)]
    st[, `:=`(
      bandwidth_periods = b,
      time_unit = time_unit,
      share = N / sum(N)
    )]

    out_summary[[b]] <- s
    out_status[[b]] <- st
  }

  list(
    summary = rbindlist(out_summary, use.names = TRUE, fill = TRUE),
    status = rbindlist(out_status, use.names = TRUE, fill = TRUE)
  )
}

write_dt_maybe <- function(dt_obj, path, label, compact_outputs = TRUE, force = FALSE, log_file = NULL) {
  n <- if (is.null(dt_obj)) 0L else nrow(dt_obj)
  if (force || !compact_outputs || n > 0L) {
    fwrite(dt_obj, path)
    logf("Wrote ", label, ": ", path, " (rows=", n, ")", log_file = log_file)
  } else {
    logf("Skipped empty ", label, " (compact_outputs=TRUE)", log_file = log_file)
  }
}

write_lines_maybe <- function(lines_obj, path, label, compact_outputs = TRUE, force = FALSE, log_file = NULL) {
  n <- length(lines_obj)
  if (force || !compact_outputs || n > 0L) {
    writeLines(lines_obj, con = path)
    logf("Wrote ", label, ": ", path, " (lines=", n, ")", log_file = log_file)
  } else {
    logf("Skipped empty ", label, " (compact_outputs=TRUE)", log_file = log_file)
  }
}

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

time_unit_in <- opts$time_unit %||% opts$retaliatory_time_unit
if (is.null(time_unit_in) || !nzchar(as.character(time_unit_in))) {
  cfg_time <- cfg$run$retaliatory_time_unit %||% ""
  if (!nzchar(as.character(cfg_time))) {
    bdl <- tolower(as.character(cfg$run$building_data_agg_level %||% ""))
    cfg_time <- if (bdl %chin% c("month", "quarter")) bdl else "quarter"
  }
  time_unit_in <- cfg_time
}

time_unit <- normalize_time_unit(time_unit_in)
periods_per_year <- if (time_unit == "quarter") 4L else 12L
period_label <- if (time_unit == "quarter") "quarter" else "month"
period_label_title <- if (time_unit == "quarter") "Quarterly" else "Monthly"
period_label_plural <- if (time_unit == "quarter") "quarters" else "months"
out_prefix <- paste0("retaliatory_", if (time_unit == "quarter") "quarterly" else "monthly")

panel_path_default <- if (time_unit == "quarter") {
  p_product(cfg, "building_data_rental_quarter")
} else {
  p_product(cfg, "building_data_rental_month")
}
panel_path <- opts$building_data_panel %||%
  if (time_unit == "quarter") {
    opts$building_data_quarter %||% panel_path_default
  } else {
    opts$building_data_month %||% panel_path_default
  }

analytic_path <- opts$analytic_sample %||% p_product(cfg, "analytic_sample")
bldg_panel_path <- opts$bldg_panel_blp %||% p_product(cfg, "bldg_panel_blp")
evictions_path <- opts$evictions_clean %||% p_product(cfg, "evictions_clean")
evict_xwalk_path <- opts$evict_address_xwalk %||% p_product(cfg, "evict_address_xwalk")
race_case_path <- opts$race_imputed_case %||% p_out(cfg, "qa", "race_imputed_case_sample.csv")
out_dir <- opts$output_dir %||% p_out(cfg, "retaliatory-evictions")
tab_dir <- file.path(out_dir, "tables")
fig_dir <- file.path(out_dir, "figs")
log_file <- p_out(cfg, "logs", "retaliatory-evictions.log")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir,  recursive = TRUE, showWarnings = FALSE)

start_year <- to_int(opts$start_year, 2011L)
end_year <- to_int(opts$end_year, 2019L)
pre_covid_end <- to_int(opts$pre_covid_end, 2019L)
h_default <- if (time_unit == "quarter") 4L else 12L
h <- max(1L, to_int(opts$horizon, h_default))
max_bw_default <- if (time_unit == "quarter") 2L else 6L
max_bw <- max(1L, to_int(opts$max_bandwidth, max_bw_default))
max_lead_lag <- max(h, max_bw)

sample_mode <- to_bool(opts$sample_mode, cfg$run$sample_mode %||% FALSE)
sample_n <- to_int(opts$sample_n, cfg$run$sample_n %||% 200000L)
seed <- to_int(opts$seed, cfg$run$seed %||% 123L)
use_pid_filter <- to_bool(opts$use_pid_filter, cfg$run$use_pid_filter %||% TRUE)
skip_distlag <- to_bool(opts$skip_distlag, cfg$run$retaliatory_skip_distlag %||% FALSE)
include_full <- to_bool(opts$include_full, cfg$run$retaliatory_include_full %||% FALSE)
compact_outputs <- to_bool(opts$compact_outputs, TRUE)
one_month_back_rent_tol <- suppressWarnings(as.numeric(opts$one_month_back_rent_tol %||% "0.10"))
if (!is.finite(one_month_back_rent_tol) || one_month_back_rent_tol <= 0) one_month_back_rent_tol <- 0.10
high_filing_threshold <- suppressWarnings(as.numeric(
  opts$high_filing_threshold %||% cfg$run$retaliatory_high_filing_threshold %||% 0.15
))
if (!is.finite(high_filing_threshold) || high_filing_threshold <= 0 || high_filing_threshold >= 1) {
  high_filing_threshold <- 0.15
}
black_split_method <- tolower(as.character(
  opts$distlag_black_split_method %||%
    opts$black_split_method %||%
    cfg$run$retaliatory_black_split_method %||%
    "mean"
))
black_split_quantile <- suppressWarnings(as.numeric(
  opts$distlag_black_split_quantile %||%
    opts$black_split_quantile %||%
    cfg$run$retaliatory_black_split_quantile %||%
    0.75
))
if (!is.finite(black_split_quantile)) black_split_quantile <- 0.75
black_split_quantile <- min(max(black_split_quantile, 0), 1)
loo_filing_threshold <- suppressWarnings(as.numeric(
  opts$loo_filing_threshold %||%
    cfg$run$retaliatory_loo_filing_threshold %||% 0.05
))
if (!is.finite(loo_filing_threshold) || loo_filing_threshold <= 0 || loo_filing_threshold >= 1) {
  loo_filing_threshold <- 0.05
}
loo_min_units <- suppressWarnings(as.numeric(
  opts$loo_min_portfolio_units %||%
    cfg$run$retaliatory_loo_min_portfolio_units %||% 10
))
if (!is.finite(loo_min_units) || loo_min_units < 0) loo_min_units <- 10

logf("=== Starting retaliatory-evictions.r ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf(period_label_title, " panel input: ", panel_path, log_file = log_file)
logf("Analytic PID universe input: ", analytic_path, log_file = log_file)
logf("BLP panel input (tenant composition): ", bldg_panel_path, log_file = log_file)
logf("Case race input (from impute-race.R): ", race_case_path, log_file = log_file)
logf("Output dir: ", out_dir, log_file = log_file)
logf("Tables dir: ", tab_dir, log_file = log_file)
logf("Figs dir: ", fig_dir, log_file = log_file)
logf(
  "Time unit=", time_unit,
  ", years=", start_year, "-", end_year,
  ", pre_covid_end=", pre_covid_end,
  ", horizon=", h,
  ", max_bandwidth=", max_bw,
  ", skip_distlag=", skip_distlag,
  ", include_full=", include_full,
  ", one_month_back_rent_tol=", one_month_back_rent_tol,
  ", high_filing_threshold=", high_filing_threshold,
  ", black_split_method=", black_split_method,
  ", black_split_quantile=", black_split_quantile,
  ", compact_outputs=", compact_outputs,
  ", sample_mode=", sample_mode,
  ", sample_n=", sample_n,
  ", use_pid_filter=", use_pid_filter,
  log_file = log_file
)

if (!file.exists(panel_path)) stop(period_label_title, " building panel not found: ", panel_path)
if (!file.exists(analytic_path)) stop("Analytic sample not found: ", analytic_path)
if (!file.exists(bldg_panel_path)) stop("BLP panel not found: ", bldg_panel_path)
if (!file.exists(evictions_path)) stop("Evictions clean not found: ", evictions_path)
if (!file.exists(evict_xwalk_path)) stop("Eviction xwalk not found: ", evict_xwalk_path)
if (!file.exists(race_case_path)) stop("Case race output not found: ", race_case_path)

pid_filter <- NULL
if (use_pid_filter) {
  pid_dt <- fread(analytic_path, select = "PID")
  pid_dt[, PID := stringr::str_pad(as.character(PID), width = 9L, side = "left", pad = "0")]
  pid_dt <- unique(pid_dt[!is.na(PID) & nzchar(PID)], by = "PID")
  if (!nrow(pid_dt)) stop("No PID rows found in analytic sample.")

  if (sample_mode && sample_n > 0L && sample_n < nrow(pid_dt)) {
    set.seed(seed)
    pid_dt <- pid_dt[sample(.N, sample_n)]
    logf("Sample mode active: retained ", nrow(pid_dt), " PIDs", log_file = log_file)
  }
  pid_filter <- pid_dt$PID
}

complaint_candidates <- c(
  "heat_complaint_count", "fire_complaint_count", "property_maintenance_complaint_count",
  "building_complaint_count", "zoning_complaint_count", "trash_weeds_complaint_count",
  "license_business_complaint_count", "program_initiative_complaint_count",
  "vacant_property_complaint_count", "work_without_permit_violation_complaint_count"
)
need_cols <- c("parcel_number", "period", "total_complaints", "total_severe_complaints", complaint_candidates)

dt <- read_panel_slice(panel_path, need_cols, pid_filter = pid_filter)

dt[, period := as.character(period)]
dt[, year := suppressWarnings(as.integer(substr(period, 1L, 4L)))]
if (time_unit == "month") {
  dt[, period_num := suppressWarnings(as.integer(substr(period, 6L, 7L)))]
} else {
  dt[, period := gsub("_", "-", toupper(period))]
  dt[, period_num := suppressWarnings(as.integer(str_extract(period, "(?<=Q)[1-4]$")))]
}

dt <- dt[!is.na(PID) & !is.na(year) & !is.na(period_num)]
dt <- dt[year >= start_year & year <= end_year]
dt <- add_tenant_composition(
  dt,
  bldg_panel_path = bldg_panel_path,
  start_year = start_year,
  end_year = end_year,
  pid_filter = pid_filter
)
assert_unique(dt, c("PID", "period"), paste0("retaliatory ", period_label, " panel (PID-period)"))

logf(period_label_title, " panel rows loaded: ", nrow(dt), ", PIDs: ", uniqueN(dt$PID), log_file = log_file)
logf(
  "Tenant composition non-missing share: black=",
  round(mean(!is.na(dt$infousa_pct_black), na.rm = TRUE), 4),
  ", female=", round(mean(!is.na(dt$infousa_pct_female), na.rm = TRUE), 4),
  ", black_female=", round(mean(!is.na(dt$infousa_pct_black_female), na.rm = TRUE), 4),
  ", demog_ok=", round(mean(!is.na(dt$infousa_share_persons_demog_ok), na.rm = TRUE), 4),
  log_file = log_file
)

# Period-level eviction filings from evictions_clean + xwalk.
ev <- fread(
  evictions_path,
  select = c("id", "d_filing", "n_sn_ss_c", "commercial", "non_residential", "a", "total_rent", "ongoing_rent")
)
xw <- fread(evict_xwalk_path, select = c("PID", "n_sn_ss_c", "num_parcels_matched"))
setDT(ev)
setDT(xw)
assert_has_cols(
  ev,
  c("id", "d_filing", "n_sn_ss_c", "commercial", "non_residential", "a", "total_rent", "ongoing_rent"),
  "evictions_clean (subset)"
)
assert_has_cols(xw, c("PID", "n_sn_ss_c", "num_parcels_matched"), "evict_address_xwalk (subset)")

xw <- xw[num_parcels_matched == 1L]
xw[, PID := stringr::str_pad(as.character(PID), width = 9L, side = "left", pad = "0")]
xw <- unique(xw[, .(PID, n_sn_ss_c)], by = c("PID", "n_sn_ss_c"))
setkey(xw, n_sn_ss_c)
setkey(ev, n_sn_ss_c)

ev_filter_counts <- data.table(
  step = character(),
  n_rows = integer(),
  retained_share_from_raw = numeric()
)
ev_filter_counts <- rbind(
  ev_filter_counts,
  data.table(step = "raw_evictions_clean_rows", n_rows = nrow(ev), retained_share_from_raw = 1)
)

ev <- ev[xw, nomatch = 0L]
ev_filter_counts <- rbind(
  ev_filter_counts,
  data.table(
    step = "unique_parcel_xwalk_match",
    n_rows = nrow(ev),
    retained_share_from_raw = nrow(ev) / ev_filter_counts[step == "raw_evictions_clean_rows", n_rows][1L]
  )
)

ev[, is_commercial := parse_tf_flag(commercial)]
ev[, is_non_residential := parse_tf_flag(non_residential)]
ev[, is_nonpayment_rent := parse_tf_flag(a)]

ev <- ev[is.na(is_commercial) | is_commercial == FALSE]
ev_filter_counts <- rbind(
  ev_filter_counts,
  data.table(
    step = "drop_commercial_cases",
    n_rows = nrow(ev),
    retained_share_from_raw = nrow(ev) / ev_filter_counts[step == "raw_evictions_clean_rows", n_rows][1L]
  )
)

ev <- ev[is.na(is_non_residential) | is_non_residential == FALSE]
ev_filter_counts <- rbind(
  ev_filter_counts,
  data.table(
    step = "drop_non_residential_cases",
    n_rows = nrow(ev),
    retained_share_from_raw = nrow(ev) / ev_filter_counts[step == "raw_evictions_clean_rows", n_rows][1L]
  )
)

ev <- ev[is_nonpayment_rent == TRUE]
ev_filter_counts <- rbind(
  ev_filter_counts,
  data.table(
    step = "keep_nonpayment_of_rent_cases_a_flag",
    n_rows = nrow(ev),
    retained_share_from_raw = nrow(ev) / ev_filter_counts[step == "raw_evictions_clean_rows", n_rows][1L]
  )
)

logf("Eviction-case filters before date-window:", log_file = log_file)
for (ii in seq_len(nrow(ev_filter_counts))) {
  logf(
    "  ", ev_filter_counts$step[ii], ": n=", ev_filter_counts$n_rows[ii],
    " (share of raw=", round(ev_filter_counts$retained_share_from_raw[ii], 4), ")",
    log_file = log_file
  )
}

ev[, filing_date_chr := substr(as.character(d_filing), 1L, 10L)]
ev[, filing_date := as.IDate(filing_date_chr, format = "%Y-%m-%d")]
ev <- ev[!is.na(filing_date)]
ev[, year := as.integer(substr(as.character(filing_date), 1L, 4L))]
ev <- ev[year >= start_year & year <= end_year]
ev_filter_counts <- rbind(
  ev_filter_counts,
  data.table(
    step = paste0("keep_year_between_", start_year, "_and_", end_year),
    n_rows = nrow(ev),
    retained_share_from_raw = nrow(ev) / ev_filter_counts[step == "raw_evictions_clean_rows", n_rows][1L]
  )
)
if (time_unit == "month") {
  ev[, period := substr(as.character(filing_date), 1L, 7L)]
} else {
  ev[, mm := suppressWarnings(as.integer(substr(as.character(filing_date), 6L, 7L)))]
  ev[, qtr := as.integer((mm - 1L) %/% 3L + 1L)]
  ev <- ev[!is.na(qtr)]
  ev[, period := paste0(year, "-Q", qtr)]
}

ev_agg <- ev[, .(num_filings = .N), by = .(PID, period)]
assert_unique(ev_agg, c("PID", "period"), paste0(period_label, " eviction aggregates"))

setkey(dt, PID, period)
setkey(ev_agg, PID, period)
dt[ev_agg, num_filings := i.num_filings]
dt[is.na(num_filings), num_filings := 0L]

num_zero_cols <- c("num_filings", "total_complaints", "total_severe_complaints", complaint_candidates)
stop_missing_cols(names(dt), num_zero_cols, "retaliatory panel post-merge")
for (cc in num_zero_cols) {
  dt[, (cc) := suppressWarnings(as.numeric(get(cc)))]
  dt[is.na(get(cc)), (cc) := 0]
}

if (dt[, any(total_severe_complaints > total_complaints, na.rm = TRUE)]) {
  stop("Invalid panel: total_severe_complaints exceeds total_complaints for at least one PID-period.")
}

severe_candidates <- c("heat_complaint_count", "fire_complaint_count", "property_maintenance_complaint_count")
non_severe_candidates <- c(
  "building_complaint_count", "zoning_complaint_count", "trash_weeds_complaint_count",
  "license_business_complaint_count", "program_initiative_complaint_count",
  "vacant_property_complaint_count", "work_without_permit_violation_complaint_count"
)
stop_missing_cols(names(dt), severe_candidates, paste0(period_label, " building panel"))
stop_missing_cols(names(dt), non_severe_candidates, paste0(period_label, " building panel"))

dt[, filed_eviction := as.integer(num_filings > 0)]
dt[, filed_complaint := as.integer(total_complaints > 0)]
dt[, filed_severe := as.integer(total_severe_complaints > 0)]
dt[, filed_non_severe := as.integer(rowSums(.SD > 0, na.rm = TRUE) > 0L), .SDcols = non_severe_candidates]

dt[, time_index := year * periods_per_year + period_num]
setorder(dt, PID, time_index)

make_leads_lags(dt, stem = "complaint", h = max_lead_lag)
make_leads_lags(dt, stem = "severe", h = max_lead_lag)
make_leads_lags(dt, stem = "non_severe", h = max_lead_lag)
dt[, period_fe := as.factor(period)]

n_pre <- dt[year <= pre_covid_end, .N]
if (n_pre == 0L) stop("Pre-COVID sample is empty after filters.")

logf("Panel rows (full, ", period_label, "): ", nrow(dt), ", PIDs: ", uniqueN(dt$PID), log_file = log_file)
logf("Panel rows (pre-covid, ", period_label, "): ", n_pre, ", PIDs: ", dt[year <= pre_covid_end, uniqueN(PID)], log_file = log_file)
logf("Eviction filing rate (full): ", round(mean(dt$filed_eviction, na.rm = TRUE), 6), log_file = log_file)
logf("Any complaint rate (full): ", round(mean(dt$filed_complaint, na.rm = TRUE), 6), log_file = log_file)

black_ref <- unique(dt[year <= pre_covid_end & is.finite(infousa_pct_black), .(PID, year, infousa_pct_black)])
black_split_cutoff_pre <- NA_real_
black_split_method_label <- black_split_method

if (nrow(black_ref)) {
  black_split_cutoff_pre <- switch(
    black_split_method,
    mean = black_ref[, mean(infousa_pct_black, na.rm = TRUE)],
    median = black_ref[, median(infousa_pct_black, na.rm = TRUE)],
    quantile = as.numeric(black_ref[, quantile(infousa_pct_black, probs = black_split_quantile, na.rm = TRUE)]),
    p75 = as.numeric(black_ref[, quantile(infousa_pct_black, probs = 0.75, na.rm = TRUE)]),
    p90 = as.numeric(black_ref[, quantile(infousa_pct_black, probs = 0.90, na.rm = TRUE)]),
    p95 = as.numeric(black_ref[, quantile(infousa_pct_black, probs = 0.95, na.rm = TRUE)]),
    {
      if (grepl("^p\\d{1,2}(\\.\\d+)?$", black_split_method)) {
        p_num <- suppressWarnings(as.numeric(sub("^p", "", black_split_method)))
        p_prob <- if (is.finite(p_num)) min(max(p_num / 100, 0), 1) else NA_real_
        if (is.finite(p_prob)) {
          black_split_method_label <- paste0("p", format(round(100 * p_prob, 2), trim = TRUE, nsmall = 0))
          as.numeric(black_ref[, quantile(infousa_pct_black, probs = p_prob, na.rm = TRUE)])
        } else {
          black_ref[, mean(infousa_pct_black, na.rm = TRUE)]
        }
      } else {
        black_ref[, mean(infousa_pct_black, na.rm = TRUE)]
      }
    }
  )

  if (black_split_method == "quantile") {
    black_split_method_label <- paste0("p", format(round(100 * black_split_quantile, 2), trim = TRUE, nsmall = 0))
  } else if (!(black_split_method %chin% c("mean", "median", "p75", "p90", "p95")) &&
             !grepl("^p\\d{1,2}(\\.\\d+)?$", black_split_method)) {
    black_split_method_label <- "mean"
  }
}

if (is.finite(black_split_cutoff_pre)) {
  dt[, high_black_split := fifelse(
    is.finite(infousa_pct_black),
    as.integer(infousa_pct_black >= black_split_cutoff_pre),
    NA_integer_
  )]

  black_q <- black_ref[, as.list(quantile(infousa_pct_black, probs = c(0.25, 0.5, 0.75), na.rm = TRUE))]
  logf(
    "Pre-COVID infousa_pct_black split (PID-year unique): method=", black_split_method_label,
    ", cutoff=", round(black_split_cutoff_pre, 4),
    ", mean=", round(black_ref[, mean(infousa_pct_black, na.rm = TRUE)], 4),
    ", median=", round(black_ref[, median(infousa_pct_black, na.rm = TRUE)], 4),
    ", p25=", round(black_q[[1L]], 4),
    ", p75=", round(black_q[[3L]], 4),
    "; high_black_split non-missing share=", round(mean(!is.na(dt$high_black_split)), 4),
    "; high group share among non-missing=", round(mean(dt$high_black_split == 1L, na.rm = TRUE), 4),
    log_file = log_file
  )
} else {
  dt[, high_black_split := NA_integer_]
  logf("WARNING: Could not compute pre-COVID infousa_pct_black split cutoff; dist-lag black-split heterogeneity disabled.",
       log_file = log_file)
}

# ── Portfolio eviction group (LOO) ───────────────────────────────────────────
# Leave-one-out (LOO) EB pre-COVID filing rate from the other buildings in the
# same conglomerate.  Three groups:
#   "Solo"                   — no other buildings in conglomerate (LOO is NA)
#   "Low-evicting portfolio" — LOO rate < median
#   "High-evicting portfolio"— LOO rate >= median
{
  xwalk_cong_loo <- fread(p_product(cfg, "xwalk_pid_conglomerate"),
                          select = c("PID", "year", "conglomerate_id"))
  xwalk_cong_loo[, PID  := stringr::str_pad(as.character(PID), width = 9L, side = "left", pad = "0")]
  xwalk_cong_loo[, year := as.integer(year)]

  # Modal conglomerate per PID across pre-COVID years
  pid_cong_modal_loo <- xwalk_cong_loo[year <= pre_covid_end,
    .(conglomerate_id = names(sort(table(conglomerate_id), decreasing = TRUE))[1L]),
    by = PID]

  # PID-level EB filing rate — lightweight read (main tenant read is inside add_tenant_composition)
  pid_eb_loo <- fread(bldg_panel_path, select = c("PID", "filing_rate_eb_pre_covid", "total_units"))
  pid_eb_loo[, PID                   := stringr::str_pad(as.character(PID), width = 9L, side = "left", pad = "0")]
  pid_eb_loo[, filing_rate_eb_pre_covid := suppressWarnings(as.numeric(filing_rate_eb_pre_covid))]
  pid_eb_loo[, total_units           := pmax(suppressWarnings(as.integer(total_units)), 1L, na.rm = TRUE)]
  pid_eb_loo <- unique(pid_eb_loo[!is.na(filing_rate_eb_pre_covid)], by = "PID")

  pid_cong_eb <- merge(pid_cong_modal_loo, pid_eb_loo, by = "PID")

  # Conglomerate-level weighted totals
  cong_totals_loo <- pid_cong_eb[, .(
    cong_wtd_rate = weighted.mean(filing_rate_eb_pre_covid, total_units, na.rm = TRUE),
    cong_units    = sum(total_units, na.rm = TRUE)
  ), by = conglomerate_id]
  pid_cong_eb <- merge(pid_cong_eb, cong_totals_loo, by = "conglomerate_id")

  # LOO: subtract the focal building's contribution
  pid_cong_eb[, loo_units := cong_units - total_units]
  pid_cong_eb[, loo_rate  := fifelse(
    loo_units > 0L,
    (cong_wtd_rate * cong_units - filing_rate_eb_pre_covid * total_units) / loo_units,
    NA_real_
  )]

  # Four-group classification — mirrors analyze-transfer-evictions-unified.R / nhood-portfolio:
  #   Solo             : no other buildings in conglomerate (loo_units == 0)
  #   Small portfolio  : 0 < loo_units < loo_min_units  (< 10 other units)
  #   Low-evicting     : loo_units >= loo_min_units AND loo_rate < loo_filing_threshold
  #   High-evicting    : loo_units >= loo_min_units AND loo_rate >= loo_filing_threshold
  # Threshold (0.05) and min-units (10) come from config for cross-script consistency.
  pid_cong_eb[, portfolio_evict_group := fcase(
    is.na(loo_rate) | loo_units == 0L,                                      "Solo",
    loo_units > 0L & loo_units < loo_min_units,                             "Small portfolio",
    loo_units >= loo_min_units & loo_rate >= loo_filing_threshold,          "High-evicting portfolio",
    loo_units >= loo_min_units & loo_rate < loo_filing_threshold,           "Low-evicting portfolio",
    default = "Low-evicting portfolio"
  )]
  pid_cong_eb[, portfolio_evict_group := factor(portfolio_evict_group,
    levels = c("Solo", "Small portfolio", "Low-evicting portfolio", "High-evicting portfolio"))]
  logf(
    "  LOO threshold=", loo_filing_threshold, ", min_units=", loo_min_units,
    "; LOO rate range among non-solo/non-small: [",
    round(pid_cong_eb[loo_units >= loo_min_units, min(loo_rate, na.rm = TRUE)], 4), ", ",
    round(pid_cong_eb[loo_units >= loo_min_units, max(loo_rate, na.rm = TRUE)], 4), "]",
    log_file = log_file
  )

  dt <- merge(dt, pid_cong_eb[, .(PID, portfolio_evict_group)], by = "PID", all.x = TRUE)
  # PIDs not in the conglomerate xwalk are single-building owners → Solo
  dt[is.na(portfolio_evict_group), portfolio_evict_group :=
    factor("Solo", levels = levels(pid_cong_eb$portfolio_evict_group))]
  logf(
    "  portfolio_evict_group distribution: ",
    paste(capture.output(dt[, .N, by = portfolio_evict_group]), collapse = " | "),
    log_file = log_file
  )
  rm(xwalk_cong_loo, pid_cong_modal_loo, pid_eb_loo, cong_totals_loo)
}

# ===========================================================================
# Quarter-window complaint-contingent filing hazard (Module A)
# ===========================================================================
# Constructs a quarterly proxy for complaint-contingent eviction filing
# escalation at the PID level.  "Hazard" here means per-quarter LPM
# probability of filing — this is NOT a continuous-time survival model.
#
# This is NOT a legal proof of retaliation.  It is a complaint-contingent
# legal escalation marker: abnormally elevated filing probability in the same
# quarter as a severe complaint, relative to a building's own baseline.
#
# Sections:
#   A1. Panel-level window status variables (severe + any-complaint parallel)
#   A2. LPM FE model: average filing elevation in complaint windows (pre-COVID)
#   A3. PID-level excess complaint-linked filing scores + EB shrinkage
#   A4. Exports + QA diagnostics
# ===========================================================================

if (time_unit == "quarter") {

  # -- Module A constants (adjust here) --------------------------------------
  BW_Q              <- 1L   # quarter bandwidth; only bandwidth=1 supported now
  MIN_SAME_Q_SEVERE <- 2L   # min same-q severe periods for score_ok_same_q flag
  MIN_BASE_Q        <- 8L   # min baseline quarters for score_ok_same_q flag
  MIN_NEAR_Q_SEVERE <- 2L   # min adjacent severe periods for score_ok_near_q flag
  SHRINK_N0_SAME       <- 5L   # shrinkage reliability constant (James-Stein style)
  MIN_BIN_N_FOR_SHRINK <- 20L  # min PIDs per bin for bin mean; else fall back to global
  # --------------------------------------------------------------------------

  logf("=== Module A: Quarter-window complaint-contingent filing hazard ===",
       log_file = log_file)

  # ── A1: Panel-level window status variables ────────────────────────────────
  # Mutually exclusive quarterly state: same-q severe complaint / adjacent / none.
  # Symmetric bandwidth BW_Q (default 1): uses lag_1 and lead_1.

  sev_adj_cols <- c(
    paste0("lag_severe_",  seq_len(BW_Q)),
    paste0("lead_severe_", seq_len(BW_Q))
  )
  stop_missing_cols(
    names(dt), c("filed_severe", sev_adj_cols),
    "Module A: severe lead/lag columns"
  )
  dt[, severe_adj_any := as.integer(
    rowSums(.SD > 0L, na.rm = TRUE) > 0L
  ), .SDcols = sev_adj_cols]
  dt[, severe_window_status := fcase(
    filed_severe == 1L,   "same_q_severe",
    severe_adj_any == 1L, "near_q_severe_not_same",
    default =              "no_severe_nearby"
  )]

  any_adj_cols <- c(
    paste0("lag_complaint_",  seq_len(BW_Q)),
    paste0("lead_complaint_", seq_len(BW_Q))
  )
  stop_missing_cols(
    names(dt), c("filed_complaint", any_adj_cols),
    "Module A: complaint lead/lag columns"
  )
  dt[, complaint_adj_any := as.integer(
    rowSums(.SD > 0L, na.rm = TRUE) > 0L
  ), .SDcols = any_adj_cols]
  dt[, any_window_status := fcase(
    filed_complaint == 1L,   "same_q_any",
    complaint_adj_any == 1L, "near_q_any_not_same",
    default =                 "no_any_nearby"
  )]

  # Pre-COVID subset: reused by models (A2) and score table (A3).
  dt_pre_q <- dt[year <= pre_covid_end]

  logf(
    "Module A window status (severe, pre-COVID): ",
    "same_q=",   round(mean(dt_pre_q$severe_window_status == "same_q_severe",          na.rm = TRUE), 4),
    ", near_q=", round(mean(dt_pre_q$severe_window_status == "near_q_severe_not_same", na.rm = TRUE), 4),
    ", base=",   round(mean(dt_pre_q$severe_window_status == "no_severe_nearby",        na.rm = TRUE), 4),
    log_file = log_file
  )

  # ── A2: LPM FE model — average filing elevation in complaint windows ────────
  # Outcome:    filed_eviction
  # Regressors: i(severe_window_status, ref = "no_severe_nearby")
  # FE:         PID + period_fe
  # Cluster:    ~PID
  # Sample:     pre-COVID
  # Analogous to the distributed-lag model but collapses lead/lag into a single
  # categorical window indicator (same-q, adjacent-q, no nearby complaint).

  hazard_win_lpm_severe_coefs  <- data.table()
  hazard_win_pois_severe_coefs <- data.table()
  hazard_win_lpm_any_coefs     <- data.table()

  sev_levels_pre <- unique(dt_pre_q$severe_window_status)
  if (length(sev_levels_pre) >= 2L && "no_severe_nearby" %chin% sev_levels_pre) {

    m_hwl_severe <- feols(
      filed_eviction ~ i(severe_window_status, ref = "no_severe_nearby") | PID + period_fe,
      data    = dt_pre_q,
      cluster = ~PID,
      lean    = TRUE
    )
    hazard_win_lpm_severe_coefs <- extract_lpm_coefs(
      m_hwl_severe,
      spec    = "filed_eviction_on_severe_window_lpm",
      outcome = "filed_eviction"
    )
    logf(
      "Module A LPM (severe window, pre-COVID): N=", nobs(m_hwl_severe),
      log_file = log_file
    )
    rm(m_hwl_severe); gc()

    # Optional Poisson robustness
    hazard_win_pois_severe_coefs <- tryCatch({
      m_hwp <- fepois(
        filed_eviction ~ i(severe_window_status, ref = "no_severe_nearby") | PID + period_fe,
        data    = dt_pre_q,
        cluster = ~PID
      )
      ec <- extract_pois_coefs(
        m_hwp,
        spec    = "filed_eviction_on_severe_window_pois",
        outcome = "filed_eviction"
      )
      logf("Module A Poisson (severe window, pre-COVID): N=", nobs(m_hwp), log_file = log_file)
      rm(m_hwp); gc()
      ec
    }, error = function(e) {
      logf("WARNING: Module A fepois (severe window) failed: ", conditionMessage(e),
           log_file = log_file)
      data.table()
    })

    # Parallel: any-complaint window
    any_levels_pre <- unique(dt_pre_q$any_window_status)
    if (length(any_levels_pre) >= 2L && "no_any_nearby" %chin% any_levels_pre) {
      hazard_win_lpm_any_coefs <- tryCatch({
        m_hwl_any <- feols(
          filed_eviction ~ i(any_window_status, ref = "no_any_nearby") | PID + period_fe,
          data    = dt_pre_q,
          cluster = ~PID,
          lean    = TRUE
        )
        ec <- extract_lpm_coefs(
          m_hwl_any,
          spec    = "filed_eviction_on_any_window_lpm",
          outcome = "filed_eviction"
        )
        rm(m_hwl_any); gc()
        ec
      }, error = function(e) {
        logf("WARNING: Module A LPM (any window) failed: ", conditionMessage(e),
             log_file = log_file)
        data.table()
      })
    }

  } else {
    logf(
      "WARNING: Module A LPM skipped — severe_window_status has < 2 levels or",
      " missing 'no_severe_nearby' reference in pre-COVID sample.",
      log_file = log_file
    )
  }

  # ── A3: PID-level excess complaint-linked filing scores ────────────────────
  # For each PID, compute filing rates within each window-status bucket and
  # derive excess scores measuring complaint-contingent filing elevation.

  pid_severe_counts <- dt_pre_q[, .(
    n_total_q           = .N,
    n_same_q_severe     = sum(severe_window_status == "same_q_severe"),
    n_near_q_severe     = sum(severe_window_status == "near_q_severe_not_same"),
    n_base_q            = sum(severe_window_status == "no_severe_nearby"),
    filings_same_severe = sum(filed_eviction[severe_window_status == "same_q_severe"],
                              na.rm = TRUE),
    filings_near_severe = sum(filed_eviction[severe_window_status == "near_q_severe_not_same"],
                              na.rm = TRUE),
    filings_base        = sum(filed_eviction[severe_window_status == "no_severe_nearby"],
                              na.rm = TRUE)
  ), by = PID]

  # Filing rates: NA (not NaN/Inf) when denominator is zero
  pid_severe_counts[, filing_rate_same_q_severe := fifelse(
    n_same_q_severe > 0L, filings_same_severe / n_same_q_severe, NA_real_
  )]
  pid_severe_counts[, filing_rate_near_q_severe := fifelse(
    n_near_q_severe > 0L, filings_near_severe / n_near_q_severe, NA_real_
  )]
  pid_severe_counts[, filing_rate_base := fifelse(
    n_base_q > 0L, filings_base / n_base_q, NA_real_
  )]

  # Raw excess scores
  pid_severe_counts[, excess_same_q_vs_base := filing_rate_same_q_severe - filing_rate_base]
  pid_severe_counts[, excess_near_q_vs_base := filing_rate_near_q_severe - filing_rate_base]
  pid_severe_counts[, excess_same_q_vs_near := filing_rate_same_q_severe - filing_rate_near_q_severe]

  # Score usability flags
  pid_severe_counts[, score_ok_same_q := n_same_q_severe >= MIN_SAME_Q_SEVERE & n_base_q >= MIN_BASE_Q]
  pid_severe_counts[, score_ok_near_q := n_same_q_severe >= MIN_SAME_Q_SEVERE & n_near_q_severe >= MIN_NEAR_Q_SEVERE]

  # EB shrinkage computed below after num_units_bin is joined (bin-specific targets).

  # Parallel: any-complaint window extra columns (merged in as optional output)
  pid_any_counts <- dt_pre_q[, .(
    n_same_q_any     = sum(any_window_status == "same_q_any"),
    n_near_q_any     = sum(any_window_status == "near_q_any_not_same"),
    n_base_q_any     = sum(any_window_status == "no_any_nearby"),
    filings_same_any = sum(filed_eviction[any_window_status == "same_q_any"],    na.rm = TRUE),
    filings_base_any = sum(filed_eviction[any_window_status == "no_any_nearby"], na.rm = TRUE)
  ), by = PID]
  pid_any_counts[, filing_rate_same_q_any := fifelse(
    n_same_q_any > 0L, filings_same_any / n_same_q_any, NA_real_
  )]
  pid_any_counts[, excess_same_q_any_vs_base := filing_rate_same_q_any - fifelse(
    n_base_q_any > 0L, filings_base_any / n_base_q_any, NA_real_
  )]

  # Add raw (unconditional) filing rate per PID for correlation diagnostic
  pid_raw_rate <- dt_pre_q[, .(filing_rate_raw = mean(filed_eviction, na.rm = TRUE)), by = PID]

  pid_hazard_score <- merge(
    pid_severe_counts,
    pid_any_counts[, .(PID, n_same_q_any, filing_rate_same_q_any, excess_same_q_any_vs_base)],
    by = "PID", all.x = TRUE
  )
  pid_hazard_score <- merge(pid_hazard_score, pid_raw_rate, by = "PID", all.x = TRUE)
  setorder(pid_hazard_score, PID)

  rm(dt_pre_q, pid_severe_counts, pid_any_counts, pid_raw_rate); gc()

  # ── Bin-specific EB shrinkage ──────────────────────────────────────────────
  # Pull num_units_bin from the BLP panel (already on disk as bldg_panel_path).
  # Shrink excess_same_q_vs_base toward the mean of buildings in the same unit-
  # size bin rather than the global mean.  Bins with fewer than
  # MIN_BIN_N_FOR_SHRINK finite-score PIDs fall back to the global mean.
  bldg_bin <- fread(bldg_panel_path,
                    select = c("PID", "num_units_bin", "owner_1",
                               "building_code_description_new", "year_built", "total_units"))
  bldg_bin[, PID := stringr::str_pad(as.character(PID), 9L, "left", "0")]
  bldg_bin <- unique(bldg_bin[!is.na(num_units_bin)], by = "PID")
  pid_hazard_score <- merge(pid_hazard_score, bldg_bin, by = "PID", all.x = TRUE)
  rm(bldg_bin); gc()

  # Join one address string per PID from the eviction xwalk (already in memory as xw)
  if (exists("xw") && "n_sn_ss_c" %in% names(xw)) {
    xw_pid_addr <- unique(xw[, .(PID, n_sn_ss_c)], by = "PID")
    pid_hazard_score <- merge(pid_hazard_score, xw_pid_addr, by = "PID", all.x = TRUE)
    rm(xw_pid_addr)
  }

  # Global mean: fallback for missing / small bins
  mu_excess_global <- pid_hazard_score[
    is.finite(excess_same_q_vs_base),
    mean(excess_same_q_vs_base, na.rm = TRUE)
  ]

  # Bin-specific means (computed among all finite-score PIDs, not just score_ok)
  bin_shrink_means <- pid_hazard_score[
    is.finite(excess_same_q_vs_base) & !is.na(num_units_bin),
    .(mu_bin = mean(excess_same_q_vs_base, na.rm = TRUE), n_bin = .N),
    by = num_units_bin
  ]
  bin_shrink_means[n_bin < MIN_BIN_N_FOR_SHRINK, mu_bin := mu_excess_global]

  pid_hazard_score <- merge(
    pid_hazard_score,
    bin_shrink_means[, .(num_units_bin, mu_bin, n_bin)],
    by = "num_units_bin", all.x = TRUE
  )
  pid_hazard_score[is.na(mu_bin), mu_bin := mu_excess_global]

  # Reliability weight and EB score shrunk toward bin mean
  pid_hazard_score[, w_same_shrink := n_same_q_severe / (n_same_q_severe + SHRINK_N0_SAME)]
  pid_hazard_score[, excess_same_q_vs_base_eb := fifelse(
    is.finite(excess_same_q_vs_base),
    w_same_shrink * excess_same_q_vs_base + (1 - w_same_shrink) * mu_bin,
    NA_real_
  )]

  logf(
    "Module A bin shrinkage: global_mu=", round(mu_excess_global, 4),
    "; bin means: ",
    paste(bin_shrink_means[order(num_units_bin),
      paste0(num_units_bin, "=", round(mu_bin, 4))], collapse = ", "),
    log_file = log_file
  )

  # ── A4: Exports ────────────────────────────────────────────────────────────
  path_hazard_lpm_severe  <- file.path(
    tab_dir, paste0(out_prefix, "_hazard_window_lpm_severe_coefficients.csv"))
  path_hazard_pois_severe <- file.path(
    tab_dir, paste0(out_prefix, "_hazard_window_pois_severe_coefficients.csv"))
  path_hazard_lpm_any     <- file.path(
    tab_dir, paste0(out_prefix, "_hazard_window_lpm_any_coefficients.csv"))
  path_pid_hazard_score   <- file.path(
    tab_dir, paste0(out_prefix, "_pid_excess_hazard_score.csv"))

  write_dt_maybe(
    hazard_win_lpm_severe_coefs, path_hazard_lpm_severe,
    "hazard window LPM (severe) coefficients",
    compact_outputs = compact_outputs, force = TRUE, log_file = log_file
  )
  write_dt_maybe(
    hazard_win_pois_severe_coefs, path_hazard_pois_severe,
    "hazard window Poisson (severe) coefficients",
    compact_outputs = compact_outputs, log_file = log_file
  )
  write_dt_maybe(
    hazard_win_lpm_any_coefs, path_hazard_lpm_any,
    "hazard window LPM (any complaint) coefficients",
    compact_outputs = compact_outputs, log_file = log_file
  )
  write_dt_maybe(
    pid_hazard_score, path_pid_hazard_score,
    "PID excess hazard score table",
    compact_outputs = compact_outputs, force = TRUE, log_file = log_file
  )

  # ── A4: QA diagnostics ────────────────────────────────────────────────────
  n_pids_mod_a    <- nrow(pid_hazard_score)
  n_ok_same_q     <- pid_hazard_score[score_ok_same_q == TRUE, .N]
  n_ok_near_q     <- pid_hazard_score[score_ok_near_q == TRUE, .N]
  share_ok_same_q <- n_ok_same_q / max(n_pids_mod_a, 1L)
  share_ok_near_q <- n_ok_near_q / max(n_pids_mod_a, 1L)

  n_finite_excess <- pid_hazard_score[is.finite(excess_same_q_vs_base), .N]
  mean_excess_raw <- if (n_finite_excess > 0L) {
    pid_hazard_score[is.finite(excess_same_q_vs_base), mean(excess_same_q_vs_base, na.rm = TRUE)]
  } else NA_real_
  mean_excess_eb  <- if (n_finite_excess > 0L) {
    pid_hazard_score[is.finite(excess_same_q_vs_base_eb), mean(excess_same_q_vs_base_eb, na.rm = TRUE)]
  } else NA_real_

  cor_excess_vs_raw <- if (
    pid_hazard_score[is.finite(excess_same_q_vs_base) & is.finite(filing_rate_raw), .N] > 2L
  ) {
    pid_hazard_score[
      is.finite(excess_same_q_vs_base) & is.finite(filing_rate_raw),
      cor(excess_same_q_vs_base, filing_rate_raw, method = "pearson")
    ]
  } else NA_real_
  cor_eb_vs_raw <- if (
    pid_hazard_score[is.finite(excess_same_q_vs_base_eb) & is.finite(filing_rate_raw), .N] > 2L
  ) {
    pid_hazard_score[
      is.finite(excess_same_q_vs_base_eb) & is.finite(filing_rate_raw),
      cor(excess_same_q_vs_base_eb, filing_rate_raw, method = "pearson")
    ]
  } else NA_real_

  logf(
    "Module A QA: n_PIDs=", n_pids_mod_a,
    ", score_ok_same_q=", n_ok_same_q, " (", round(100 * share_ok_same_q, 1), "%)",
    ", score_ok_near_q=", n_ok_near_q, " (", round(100 * share_ok_near_q, 1), "%)",
    ", mean_excess_raw=", round(mean_excess_raw, 4),
    ", cor(excess_raw, filing_rate_raw)=", round(cor_excess_vs_raw, 4),
    ", cor(excess_eb,  filing_rate_raw)=", round(cor_eb_vs_raw, 4),
    log_file = log_file
  )

  message(
    "\n--- Module A QA: Quarter-window complaint-contingent filing hazard ---\n",
    "  LPM model rows:          ",
    if (nrow(hazard_win_lpm_severe_coefs) > 0L) hazard_win_lpm_severe_coefs$n_obs[1L] else "SKIPPED",
    "\n",
    "  PID score table rows:    ", n_pids_mod_a, "\n",
    "  score_ok_same_q:         ", n_ok_same_q, " / ", n_pids_mod_a,
    " (", round(100 * share_ok_same_q, 1), "%)\n",
    "  score_ok_near_q:         ", n_ok_near_q, " / ", n_pids_mod_a,
    " (", round(100 * share_ok_near_q, 1), "%)\n",
    "  mean excess_same_q_raw:  ", round(mean_excess_raw, 4), "\n",
    "  mean excess_same_q_eb:   ", round(mean_excess_eb,  4), "\n",
    "  cor(excess_raw, filing_rate_raw): ", round(cor_excess_vs_raw, 4), "\n",
    "  cor(excess_eb,  filing_rate_raw): ", round(cor_eb_vs_raw,   4), "\n"
  )

  module_a_qa_lines <- c(
    "",
    paste0("Module A (Quarter-window filing hazard, bandwidth=", BW_Q, ")"),
    paste0("  PID score table rows: ", n_pids_mod_a),
    paste0("  score_ok_same_q (n_same>=", MIN_SAME_Q_SEVERE,
           ", n_base>=", MIN_BASE_Q, "): ", n_ok_same_q),
    paste0("  score_ok_near_q (n_same>=", MIN_SAME_Q_SEVERE,
           ", n_near>=", MIN_NEAR_Q_SEVERE, "): ", n_ok_near_q),
    paste0("  mean excess_same_q_vs_base (finite PIDs): ", round(mean_excess_raw, 4)),
    paste0("  mean excess_same_q_vs_base_eb (finite PIDs): ", round(mean_excess_eb, 4)),
    paste0("  cor(excess_raw, filing_rate_raw): ", round(cor_excess_vs_raw, 4)),
    paste0("  cor(excess_eb,  filing_rate_raw): ", round(cor_eb_vs_raw, 4)),
    paste0("  Wrote LPM coefs:  ", path_hazard_lpm_severe),
    paste0("  Wrote PID scores: ", path_pid_hazard_score)
  )

  logf("=== Module A complete ===", log_file = log_file)

} else {
  logf(
    "Module A (quarter-window filing hazard) skipped: time_unit=", time_unit,
    log_file = log_file
  )
  hazard_win_lpm_severe_coefs  <- data.table()
  hazard_win_pois_severe_coefs <- data.table()
  hazard_win_lpm_any_coefs     <- data.table()
  pid_hazard_score             <- data.table()
  module_a_qa_lines            <- character()
}

# Case-level back-rent ratio analysis:
# months_back_rent = total_rent / ongoing_rent
# one_month_back_rent = 1{ months_back_rent in [1 - tol, 1 + tol] }.
case_back_rent_summary <- data.table(
  sample = character(),
  group = character(),
  n_cases = integer(),
  n_valid_ratio = integer(),
  share_valid_ratio = numeric(),
  share_one_month = numeric(),
  mean_months_back = numeric(),
  median_months_back = numeric()
)
case_back_rent_by_year <- data.table(
  sample = character(),
  year = integer(),
  group = character(),
  n_cases = integer(),
  n_valid_ratio = integer(),
  share_one_month = numeric(),
  mean_months_back = numeric()
)

ev_case <- ev[, .(id, PID, year, period, filing_date, total_rent, ongoing_rent)]
stop_missing_cols(
  names(ev_case),
  c("id", "PID", "year", "period", "filing_date", "total_rent", "ongoing_rent"),
  "evictions case rent analysis input"
)
ev_case[, total_rent := suppressWarnings(as.numeric(total_rent))]
ev_case[, ongoing_rent := suppressWarnings(as.numeric(ongoing_rent))]
ev_case <- ev_case[!is.na(id) & nzchar(as.character(id))]
ev_case <- ev_case[, .(
  PID = PID[1L],
  year = year[1L],
  period = period[1L],
  filing_date = {
    x <- filing_date[!is.na(filing_date)]
    if (length(x)) min(x) else as.IDate(NA)
  },
  total_rent = {
    x <- total_rent[is.finite(total_rent)]
    if (length(x)) median(x) else NA_real_
  },
  ongoing_rent = {
    x <- ongoing_rent[is.finite(ongoing_rent)]
    if (length(x)) median(x) else NA_real_
  }
), by = id]
assert_unique(ev_case, "id", "eviction cases (id)")
ev_case <- ev_case[!is.na(PID) & !is.na(period) & !is.na(year)]
assert_unique(ev_case, "id", "eviction cases (id) post-clean")

ev_case_n_before <- nrow(ev_case)
panel_pid_universe <- unique(dt[, .(PID)])
setkey(panel_pid_universe, PID)
setkey(ev_case, PID)
ev_case <- panel_pid_universe[ev_case, nomatch = 0L]
logf(
  "Case-level sample restriction to panel PID universe: ",
  ev_case_n_before, " -> ", nrow(ev_case),
  " (retained share=", round(nrow(ev_case) / ev_case_n_before, 4), ")",
  log_file = log_file
)
assert_unique(ev_case, "id", "eviction cases (id) after panel PID restriction")

pid_year_black <- unique(dt[, .(PID, year, infousa_pct_black, high_black_split)], by = c("PID", "year"))
setkey(pid_year_black, PID, year)
setkey(ev_case, PID, year)
case_dt <- pid_year_black[ev_case]
if (nrow(case_dt) != nrow(ev_case)) {
  stop("Case-level PID-year demographic join changed row count unexpectedly.")
}
assert_unique(case_dt, "id", "case_dt after PID-year demographic join")

severe_win_cols <- c(
  "filed_severe",
  paste0("lag_severe_", seq_len(max_bw)),
  paste0("lead_severe_", seq_len(max_bw))
)
stop_missing_cols(names(dt), c("PID", "period", severe_win_cols), "retaliatory panel for case-level severe linkage")
dt_case_link <- dt[, c("PID", "period", severe_win_cols), with = FALSE]
assert_unique(dt_case_link, c("PID", "period"), "retaliatory panel case-link index (PID-period)")
setkey(dt_case_link, PID, period)
setkey(case_dt, PID, period)
case_dt <- dt_case_link[case_dt]
if (nrow(case_dt) != nrow(ev_case)) {
  stop("Case-level PID-period severe linkage changed row count unexpectedly.")
}
assert_unique(case_dt, "id", "case_dt after PID-period severe linkage")

case_dt[, has_panel_match := !is.na(filed_severe)]
case_dt[, severe_nearby := fifelse(
  has_panel_match,
  as.integer(rowSums(.SD > 0, na.rm = TRUE) > 0L),
  NA_integer_
), .SDcols = severe_win_cols]
case_dt[, retaliatory_severe := severe_nearby]
case_dt[, retaliatory_status := fifelse(
  !has_panel_match, "missing_panel_period",
  fifelse(
    filed_severe == 1L, "same_period_severe",
    fifelse(retaliatory_severe == 1L, "within_bw_severe_not_same_period", "non_retaliatory")
  )
)]

logf(
  "Case-level severe-link join: input cases=", nrow(ev_case),
  ", matched PID-period=", case_dt[, sum(has_panel_match, na.rm = TRUE)],
  ", unmatched PID-period=", case_dt[, sum(!has_panel_match, na.rm = TRUE)],
  ", unmatched share=", round(case_dt[, mean(!has_panel_match, na.rm = TRUE)], 4),
  ", retaliatory(severe proximity) among matched=", round(case_dt[has_panel_match == TRUE, mean(retaliatory_severe == 1L, na.rm = TRUE)], 4),
  log_file = log_file
)

case_dt[, valid_rent_ratio := is.finite(total_rent) & total_rent >= 0 & is.finite(ongoing_rent) & ongoing_rent > 0]
case_dt[, months_back_rent := fifelse(valid_rent_ratio, total_rent / ongoing_rent, NA_real_)]
case_dt[, one_month_back_rent := as.integer(
  valid_rent_ratio &
    is.finite(months_back_rent) &
    months_back_rent >= (1 - one_month_back_rent_tol) &
    months_back_rent <= (1 + one_month_back_rent_tol)
)]
case_dt[, black_group := fifelse(
  is.na(high_black_split), "missing_black",
  fifelse(high_black_split == 1L, "high_black", "low_black")
)]

race_case <- fread(
  race_case_path,
  select = c("id", "case_impute_status", "case_p_white", "case_p_black", "case_race_group_l80")
)
setDT(race_case)
assert_has_cols(
  race_case,
  c("id", "case_impute_status", "case_p_white", "case_p_black", "case_race_group_l80"),
  "race_imputed_case_sample (subset)"
)
race_case <- race_case[!is.na(id) & nzchar(as.character(id))]
race_case[, case_p_white := suppressWarnings(as.numeric(case_p_white))]
race_case[, case_p_black := suppressWarnings(as.numeric(case_p_black))]
race_case <- unique(race_case, by = "id")
assert_unique(race_case, "id", "race_imputed_case (id)")

setkey(case_dt, id)
setkey(race_case, id)
case_dt[race_case, `:=`(
  case_impute_status = i.case_impute_status,
  case_p_white = i.case_p_white,
  case_p_black = i.case_p_black,
  case_race_group_l80 = i.case_race_group_l80
)]

case_dt[, has_case_race := !is.na(case_impute_status)]
logf(
  "Case race join coverage: matched=", case_dt[, sum(has_case_race, na.rm = TRUE)],
  ", unmatched=", case_dt[, sum(!has_case_race, na.rm = TRUE)],
  ", unmatched_share=", round(case_dt[, mean(!has_case_race, na.rm = TRUE)], 4),
  ", imputed_ok_share_among_matched=", round(case_dt[has_case_race == TRUE, mean(case_impute_status == "ok", na.rm = TRUE)], 4),
  log_file = log_file
)

# drop cases with outlier rent ratios
case_dt = case_dt[months_back_rent < 24 & ongoing_rent >= 300 & ongoing_rent <= 8000 ]

# Join portfolio_evict_group to case-level data (by PID)
case_dt <- merge(case_dt, pid_cong_eb[, .(PID, portfolio_evict_group)], by = "PID", all.x = TRUE)
case_dt[is.na(portfolio_evict_group), portfolio_evict_group :=
  factor("Solo", levels = levels(pid_cong_eb$portfolio_evict_group))]

summ_case <- function(d, sample_name) {
  dd <- if (sample_name == "pre") d[year <= pre_covid_end] else d
  if (!nrow(dd)) return(data.table())

  all_row <- dd[, .(
    sample = sample_name,
    group = "all",
    n_cases = .N,
    n_valid_ratio = sum(valid_rent_ratio, na.rm = TRUE),
    share_valid_ratio = mean(valid_rent_ratio, na.rm = TRUE),
    share_one_month = mean(one_month_back_rent, na.rm = TRUE),
    mean_months_back = mean(months_back_rent, na.rm = TRUE),
    median_months_back = suppressWarnings(median(months_back_rent, na.rm = TRUE))
  )]
  grp_rows <- dd[, .(
    sample = sample_name,
    n_cases = .N,
    n_valid_ratio = sum(valid_rent_ratio, na.rm = TRUE),
    share_valid_ratio = mean(valid_rent_ratio, na.rm = TRUE),
    share_one_month = mean(one_month_back_rent, na.rm = TRUE),
    mean_months_back = mean(months_back_rent, na.rm = TRUE),
    median_months_back = suppressWarnings(median(months_back_rent, na.rm = TRUE))
  ), by = .(group = black_group)]

  rbind(all_row, grp_rows, use.names = TRUE, fill = TRUE)
}

summ_case_year <- function(d, sample_name) {
  dd <- if (sample_name == "pre") d[year <= pre_covid_end] else d
  if (!nrow(dd)) return(data.table())
  dd[, .(
    sample = sample_name,
    n_cases = .N,
    n_valid_ratio = sum(valid_rent_ratio, na.rm = TRUE),
    share_one_month = mean(one_month_back_rent, na.rm = TRUE),
    mean_months_back = mean(months_back_rent, na.rm = TRUE)
  ), by = .(year, group = black_group)]
}

case_samples <- if (include_full) c("full", "pre") else c("pre")
case_back_rent_summary <- rbindlist(
  lapply(case_samples, function(s) summ_case(case_dt, s)),
  use.names = TRUE,
  fill = TRUE
)
case_back_rent_by_year <- rbindlist(
  lapply(case_samples, function(s) summ_case_year(case_dt, s)),
  use.names = TRUE,
  fill = TRUE
)
setorderv(case_back_rent_summary, c("sample", "group"))
setorderv(case_back_rent_by_year, c("sample", "year", "group"))

logf(
  "Case back-rent ratio analysis: tol=+/-", one_month_back_rent_tol,
  ", n_cases=", nrow(case_dt),
  ", valid_ratio_share=", round(mean(case_dt$valid_rent_ratio, na.rm = TRUE), 4),
  ", one_month_share_among_valid=", round(mean(case_dt$one_month_back_rent, na.rm = TRUE), 4),
  log_file = log_file
)

case_retaliatory_status_summary <- case_dt[
  year <= pre_covid_end,
  .(n_cases = .N),
  by = .(retaliatory_status)
][order(retaliatory_status)]
case_retaliatory_status_summary[, share := n_cases / sum(n_cases)]

case_lpm_coefs <- data.table(
  spec = character(),
  outcome = character(),
  term = character(),
  estimate = numeric(),
  std_error = numeric(),
  t_value = numeric(),
  p_value = numeric(),
  conf_low = numeric(),
  conf_high = numeric(),
  n_obs = integer()
)
case_lpm_sample_summary <- data.table(
  spec = character(),
  n_obs = integer(),
  mean_outcome = numeric(),
  same_period_share = numeric(),
  bw_not_same_period_share = numeric(),
  high_black_share = numeric(),
  defendant_race_black_share = numeric(),
  defendant_race_white_share = numeric(),
  defendant_race_unknown_share = numeric(),
  defendant_race_ok_share = numeric()
)
etable_case_lpm_txt <- character()
etable_case_lpm_tex <- character()

case_reg_base <- case_dt[
  year <= pre_covid_end &
    valid_rent_ratio == TRUE &
    !is.na(one_month_back_rent) &
    !is.na(retaliatory_severe) &
    !is.na(retaliatory_status)
]
if (!nrow(case_reg_base)) {
  stop("No usable pre-COVID case rows for one-month-rent retaliatory regressions.")
}
if (uniqueN(case_reg_base$retaliatory_status) < 2L) {
  stop("Case retaliatory_status has no variation in pre-COVID regression sample.")
}
if (!("non_retaliatory" %chin% unique(case_reg_base$retaliatory_status))) {
  stop("Reference category non_retaliatory is missing from case_reg_base.")
}

m_case_1 <- feols(
  one_month_back_rent ~ i(retaliatory_status, ref = "non_retaliatory") | PID + year,
  data = case_reg_base,
  cluster = ~PID,
  lean = TRUE
)
case_lpm_sample_summary <- rbind(
  case_lpm_sample_summary,
  data.table(
    spec = "one_month_on_retaliatory_severe",
    n_obs = as.integer(nobs(m_case_1)),
    mean_outcome = mean(case_reg_base$one_month_back_rent, na.rm = TRUE),
    same_period_share = mean(case_reg_base$retaliatory_status == "same_period_severe", na.rm = TRUE),
    bw_not_same_period_share = mean(case_reg_base$retaliatory_status == "within_bw_severe_not_same_period", na.rm = TRUE),
    high_black_share = mean(case_reg_base$high_black_split == 1L, na.rm = TRUE),
    defendant_race_black_share = NA_real_,
    defendant_race_white_share = NA_real_,
    defendant_race_unknown_share = NA_real_,
    defendant_race_ok_share = mean(case_reg_base$case_impute_status == "ok", na.rm = TRUE)
  ),
  use.names = TRUE
)
case_lpm_coefs <- rbind(
  case_lpm_coefs,
  extract_lpm_coefs(m_case_1, spec = "one_month_on_retaliatory_severe", outcome = "one_month_back_rent"),
  use.names = TRUE
)

case_reg_int <- case_reg_base[!is.na(high_black_split)]
if (nrow(case_reg_int) < 50L) {
  stop("Insufficient non-missing high_black_split rows for interaction regression: n=", nrow(case_reg_int))
}
if (uniqueN(case_reg_int$high_black_split) < 2L) {
  stop("high_black_split has no variation in interaction regression sample.")
}

m_case_2 <- feols(
  one_month_back_rent ~ i(retaliatory_status, ref = "non_retaliatory") * high_black_split | PID + year,
  data = case_reg_int,
  cluster = ~PID,
  lean = TRUE
)
case_lpm_sample_summary <- rbind(
  case_lpm_sample_summary,
  data.table(
    spec = "one_month_on_retaliatory_severe_x_high_black",
    n_obs = as.integer(nobs(m_case_2)),
    mean_outcome = mean(case_reg_int$one_month_back_rent, na.rm = TRUE),
    same_period_share = mean(case_reg_int$retaliatory_status == "same_period_severe", na.rm = TRUE),
    bw_not_same_period_share = mean(case_reg_int$retaliatory_status == "within_bw_severe_not_same_period", na.rm = TRUE),
    high_black_share = mean(case_reg_int$high_black_split == 1L, na.rm = TRUE),
    defendant_race_black_share = NA_real_,
    defendant_race_white_share = NA_real_,
    defendant_race_unknown_share = NA_real_,
    defendant_race_ok_share = mean(case_reg_int$case_impute_status == "ok", na.rm = TRUE)
  ),
  use.names = TRUE
)
case_lpm_coefs <- rbind(
  case_lpm_coefs,
  extract_lpm_coefs(m_case_2, spec = "one_month_on_retaliatory_severe_x_high_black", outcome = "one_month_back_rent"),
  use.names = TRUE
)

spec_keys <- c(
  "one_month_on_retaliatory_severe",
  "one_month_on_retaliatory_severe_x_high_black"
)
same_period_pct_vals <- sapply(spec_keys, function(k) {
  v <- case_lpm_sample_summary[spec == k, same_period_share][1L]
  if (!is.finite(v)) return(NA_real_)
  round(100 * v, 1)
})
same_period_pct_cells <- ifelse(is.finite(same_period_pct_vals), format(same_period_pct_vals, nsmall = 1, trim = TRUE), "--")
bw_pct_vals <- sapply(spec_keys, function(k) {
  v <- case_lpm_sample_summary[spec == k, bw_not_same_period_share][1L]
  if (!is.finite(v)) return(NA_real_)
  round(100 * v, 1)
})
bw_pct_cells <- ifelse(is.finite(bw_pct_vals), format(bw_pct_vals, nsmall = 1, trim = TRUE), "--")

old_fixest_dict <- tryCatch(getFixest_dict(), error = function(e) NULL)
etable_dict <- c(
  "retaliatory_status::same_period_severe" = "Same-period severe",
  "retaliatory_status::within_bw_severe_not_same_period" = "One-quarter pre/post severe",
  "high_black_split" = "Above-mean building pct black",
  "high_black_split:retaliatory_status::same_period_severe" = "Same-period severe x above-mean building pct black",
  "high_black_split:retaliatory_status::within_bw_severe_not_same_period" = "One-quarter pre/post severe x above-mean building pct black",
  "retaliatory_status::same_period_severe:high_black_split" = "Same-period severe x above-mean building pct black",
  "retaliatory_status::within_bw_severe_not_same_period:high_black_split" = "One-quarter pre/post severe x above-mean building pct black"
)
setFixest_dict(etable_dict)
on.exit({
  if (is.null(old_fixest_dict)) {
    try(setFixest_dict(reset = TRUE), silent = TRUE)
  } else {
    try(setFixest_dict(old_fixest_dict, reset = TRUE), silent = TRUE)
  }
}, add = TRUE)

etable_case_lpm_txt <- capture.output(etable(
  list(
    "One-Month Back Rent ~ Retaliatory (Severe Proximity)" = m_case_1,
    "One-Month Back Rent ~ Retaliatory x High Black (Building)" = m_case_2
  ),
  se.below = TRUE,
  digits = 3,
  extralines = list(
    "% cases filed in quarter of complaint" = same_period_pct_cells,
    "% cases in one-quarter pre/post window" = bw_pct_cells
  )
))
etable_case_lpm_tex <- capture.output(etable(
  list(
    "One-Month Back Rent ~ Retaliatory (Severe Proximity)" = m_case_1,
    "One-Month Back Rent ~ Retaliatory x High Black (Building)" = m_case_2
  ),
  se.below = TRUE,
  digits = 3,
  tex = TRUE,
  extralines = list(
    "% cases filed in quarter of complaint" = same_period_pct_cells,
    "% cases in one-quarter pre/post window" = bw_pct_cells
  )
))

logf(
  "Case one-month regression sample: n_base=", nrow(case_reg_base),
  ", n_interaction_building=", nrow(case_reg_int),
  ", mean(one_month_back_rent)=", round(mean(case_reg_base$one_month_back_rent, na.rm = TRUE), 4),
  ", same_period_share=", round(mean(case_reg_base$retaliatory_status == "same_period_severe", na.rm = TRUE), 4),
  ", bw_not_same_period_share=", round(mean(case_reg_base$retaliatory_status == "within_bw_severe_not_same_period", na.rm = TRUE), 4),
  log_file = log_file
)

# ---------------------------------------------------------------------------
# Poisson (PPML) regressions: outcome = months_back_rent (continuous ratio),
# same two specifications as the LPM above.
# ---------------------------------------------------------------------------
case_pois_coefs <- data.table(
  spec = character(),
  outcome = character(),
  term = character(),
  estimate = numeric(),
  std_error = numeric(),
  t_value = numeric(),
  p_value = numeric(),
  conf_low = numeric(),
  conf_high = numeric(),
  n_obs = integer()
)
etable_case_pois_txt <- character()
etable_case_pois_tex <- character()

m_pois_1 <- fepois(
  months_back_rent ~ i(retaliatory_status, ref = "non_retaliatory") | PID + year,
  data = case_reg_base,
  cluster = ~PID
)
case_pois_coefs <- rbind(
  case_pois_coefs,
  extract_pois_coefs(m_pois_1, spec = "months_back_rent_on_retaliatory_severe", outcome = "months_back_rent"),
  use.names = TRUE
)

m_pois_2 <- fepois(
  months_back_rent ~ i(retaliatory_status, ref = "non_retaliatory") * high_black_split | PID + year,
  data = case_reg_int,
  cluster = ~PID
)
case_pois_coefs <- rbind(
  case_pois_coefs,
  extract_pois_coefs(m_pois_2, spec = "months_back_rent_on_retaliatory_severe_x_high_black", outcome = "months_back_rent"),
  use.names = TRUE
)

spec_keys_pois <- c(
  "months_back_rent_on_retaliatory_severe",
  "months_back_rent_on_retaliatory_severe_x_high_black"
)
same_period_pct_cells_pois <- same_period_pct_cells
bw_pct_cells_pois <- bw_pct_cells

etable_case_pois_txt <- capture.output(etable(
  list(
    "Months Back Rent ~ Retaliatory (Severe Proximity)" = m_pois_1,
    "Months Back Rent ~ Retaliatory x High Black (Building)" = m_pois_2
  ),
  se.below = TRUE,
  digits = 3,
  extralines = list(
    "% cases filed in quarter of complaint" = same_period_pct_cells_pois,
    "% cases in one-quarter pre/post window" = bw_pct_cells_pois
  )
))
etable_case_pois_tex <- capture.output(etable(
  list(
    "Months Back Rent ~ Retaliatory (Severe Proximity)" = m_pois_1,
    "Months Back Rent ~ Retaliatory x High Black (Building)" = m_pois_2
  ),
  se.below = TRUE,
  digits = 3,
  tex = TRUE,
  extralines = list(
    "% cases filed in quarter of complaint" = same_period_pct_cells_pois,
    "% cases in one-quarter pre/post window" = bw_pct_cells_pois
  )
))

logf(
  "Case months-back-rent Poisson (PPML) regressions: n_base=", nrow(case_reg_base),
  ", mean(months_back_rent)=", round(mean(case_reg_base$months_back_rent, na.rm = TRUE), 4),
  log_file = log_file
)

# ── Case back-rent by portfolio eviction group ─────────────────────────────
# Separate LPM (one_month_back_rent) + PPML (months_back_rent) per group.
# Mirrors the high_black_split approach above.
case_lpm_port_coefs <- rbindlist(lapply(levels(case_reg_base$portfolio_evict_group), function(grp) {
  d <- case_reg_base[portfolio_evict_group == grp]
  if (nrow(d) < 50L || uniqueN(d$retaliatory_status) < 2L) {
    logf("  Skipping case back-rent port split (small/no-variation): ", grp, " | N=", nrow(d),
         log_file = log_file)
    return(NULL)
  }
  m_lpm  <- tryCatch(
    feols(one_month_back_rent ~ i(retaliatory_status, ref = "non_retaliatory") | PID + year,
          data = d, cluster = ~PID, lean = TRUE),
    error = function(e) {
      logf("  LPM port split error (", grp, "): ", conditionMessage(e), log_file = log_file); NULL
    }
  )
  m_pois <- tryCatch(
    fepois(months_back_rent ~ i(retaliatory_status, ref = "non_retaliatory") | PID + year,
           data = d, cluster = ~PID),
    error = function(e) {
      logf("  PPML port split error (", grp, "): ", conditionMessage(e), log_file = log_file); NULL
    }
  )
  lpm_rows  <- if (!is.null(m_lpm))
    extract_lpm_coefs(m_lpm,  spec = paste0("one_month_on_retaliatory_severe_port_", grp),
                      outcome = "one_month_back_rent")
  else NULL
  pois_rows <- if (!is.null(m_pois))
    extract_pois_coefs(m_pois, spec = paste0("months_back_rent_on_retaliatory_severe_port_", grp),
                       outcome = "months_back_rent")
  else NULL
  rows <- rbindlist(list(lpm_rows, pois_rows), use.names = TRUE, fill = TRUE)
  if (nrow(rows)) rows[, portfolio_evict_group := grp]
  logf("  Case back-rent port split: ", grp, " | N=", nrow(d), log_file = log_file)
  rows
}), use.names = TRUE, fill = TRUE)

# ---------------------------------------------------------------------------
# OLS log-rent regression: log(total_rent) ~ retaliatory_status + log(ongoing_rent)
# Conditioning on log(ongoing_rent) removes rent-level variation across tenants.
# The coefficient on retaliatory_status gives the % change in total back rent
# claimed for retaliatory cases vs non-retaliatory cases, holding ongoing rent fixed.
# Interpretation: "landlords file for X% less back rent in retaliatory cases."
# ---------------------------------------------------------------------------
case_log_rent_coefs <- data.table(
  spec = character(),
  outcome = character(),
  term = character(),
  estimate = numeric(),
  std_error = numeric(),
  t_value = numeric(),
  p_value = numeric(),
  conf_low = numeric(),
  conf_high = numeric(),
  n_obs = integer()
)
etable_case_log_rent_txt <- character()
etable_case_log_rent_tex <- character()

# Require total_rent > 0 for log(total_rent); valid_rent_ratio already ensures ongoing_rent > 0
case_reg_log     <- case_reg_base[total_rent > 0]
case_reg_log_int <- case_reg_log[!is.na(high_black_split)]
logf(
  "Log-rent OLS sample: n_base=", nrow(case_reg_log),
  " (dropped ", nrow(case_reg_base) - nrow(case_reg_log), " zero-total-rent cases from base)",
  ", n_int=", nrow(case_reg_log_int),
  log_file = log_file
)

m_log_1 <- feols(
  log(total_rent) ~ i(retaliatory_status, ref = "non_retaliatory") + log(ongoing_rent) | PID + year,
  data = case_reg_log,
  cluster = ~PID,
  lean = TRUE
)
case_log_rent_coefs <- rbind(
  case_log_rent_coefs,
  extract_lpm_coefs(m_log_1, spec = "log_total_rent_on_retaliatory_severe", outcome = "log_total_rent"),
  use.names = TRUE
)

m_log_2 <- feols(
  log(total_rent) ~ i(retaliatory_status, ref = "non_retaliatory") * high_black_split +
    log(ongoing_rent) | PID + year,
  data = case_reg_log_int,
  cluster = ~PID,
  lean = TRUE
)
case_log_rent_coefs <- rbind(
  case_log_rent_coefs,
  extract_lpm_coefs(m_log_2, spec = "log_total_rent_on_retaliatory_severe_x_high_black", outcome = "log_total_rent"),
  use.names = TRUE
)

same_period_pct_cells_log <- same_period_pct_cells
bw_pct_cells_log <- bw_pct_cells

etable_case_log_rent_txt <- capture.output(etable(
  list(
    "log(Total Rent) ~ Retaliatory + log(Ongoing Rent)" = m_log_1,
    "log(Total Rent) ~ Retaliatory x High Black + log(Ongoing Rent)" = m_log_2
  ),
  se.below = TRUE,
  digits = 3,
  extralines = list(
    "% cases filed in quarter of complaint" = same_period_pct_cells_log,
    "% cases in one-quarter pre/post window" = bw_pct_cells_log
  )
))
etable_case_log_rent_tex <- capture.output(etable(
  list(
    "log(Total Rent) ~ Retaliatory + log(Ongoing Rent)" = m_log_1,
    "log(Total Rent) ~ Retaliatory x High Black + log(Ongoing Rent)" = m_log_2
  ),
  se.below = TRUE,
  digits = 3,
  tex = TRUE,
  extralines = list(
    "% cases filed in quarter of complaint" = same_period_pct_cells_log,
    "% cases in one-quarter pre/post window" = bw_pct_cells_log
  )
))

logf(
  "Case log-total-rent OLS regressions: n_base=", nrow(case_reg_log),
  ", mean(log(total_rent))=", round(mean(log(case_reg_log$total_rent), na.rm = TRUE), 4),
  log_file = log_file
)

base_samples <- if (include_full) c("full", "pre") else c("pre")
base_stems <- c("complaint", "non_severe", "severe")
model_specs <- as.data.table(expand.grid(
  sample = base_samples,
  stem = base_stems,
  stringsAsFactors = FALSE
))
setcolorder(model_specs, c("sample", "stem"))

coef_dist <- data.table(
  sample = character(),
  stem = character(),
  model = character(),
  timing = integer(),
  estimate = numeric(),
  std_error = numeric(),
  t_value = numeric(),
  p_value = numeric(),
  conf_low = numeric(),
  conf_high = numeric(),
  stem_label = character(),
  sample_label = character()
)
etable_dist_lines <- list()
evict_port_samples <- character(0L)  # populated inside if (!skip_distlag) when portfolio group is available

if (!skip_distlag) {
  model_specs_dist <- copy(model_specs)
  if (is.finite(black_split_cutoff_pre) && dt[!is.na(high_black_split), .N] > 0L) {
    split_samples <- if (include_full) {
      c("full_hi_black", "full_lo_black", "pre_hi_black", "pre_lo_black")
    } else {
      c("pre_hi_black", "pre_lo_black")
    }
    model_specs_dist <- rbind(
      model_specs_dist,
      data.table(
        sample = rep(split_samples, each = length(base_stems)),
        stem = rep(base_stems, times = length(split_samples))
      ),
      use.names = TRUE
    )
  }

  if (!is.null(dt$portfolio_evict_group) && dt[!is.na(portfolio_evict_group), .N] > 0L) {
    evict_port_samples <- if (include_full) {
      c("full_hi_evict_port", "full_lo_evict_port", "full_small_port", "full_solo_port",
        "pre_hi_evict_port",  "pre_lo_evict_port",  "pre_small_port",  "pre_solo_port")
    } else {
      c("pre_hi_evict_port", "pre_lo_evict_port", "pre_small_port", "pre_solo_port")
    }
    model_specs_dist <- rbind(
      model_specs_dist,
      data.table(
        sample = rep(evict_port_samples, each = length(base_stems)),
        stem   = rep(base_stems, times = length(evict_port_samples))
      ),
      use.names = TRUE
    )
  } else {
    evict_port_samples <- character(0L)
    logf("WARNING: portfolio_evict_group missing or empty; port-split distlag disabled.", log_file = log_file)
  }

  subset_distlag_sample <- function(d, s_name) {
    if (s_name == "full") return(d)
    if (s_name == "pre") return(d[year <= pre_covid_end])
    if (s_name == "full_hi_black") return(d[high_black_split == 1L])
    if (s_name == "full_lo_black") return(d[high_black_split == 0L])
    if (s_name == "pre_hi_black") return(d[year <= pre_covid_end & high_black_split == 1L])
    if (s_name == "pre_lo_black") return(d[year <= pre_covid_end & high_black_split == 0L])
    if (s_name == "full_hi_evict_port")   return(d[portfolio_evict_group == "High-evicting portfolio"])
    if (s_name == "full_lo_evict_port")   return(d[portfolio_evict_group == "Low-evicting portfolio"])
    if (s_name == "full_solo_port")       return(d[portfolio_evict_group == "Solo"])
    if (s_name == "full_small_port")      return(d[portfolio_evict_group == "Small portfolio"])
    if (s_name == "pre_hi_evict_port")    return(d[year <= pre_covid_end & portfolio_evict_group == "High-evicting portfolio"])
    if (s_name == "pre_lo_evict_port")    return(d[year <= pre_covid_end & portfolio_evict_group == "Low-evicting portfolio"])
    if (s_name == "pre_solo_port")        return(d[year <= pre_covid_end & portfolio_evict_group == "Solo"])
    if (s_name == "pre_small_port")       return(d[year <= pre_covid_end & portfolio_evict_group == "Small portfolio"])
    d[0]
  }

  coef_list <- list()
  for (i in seq_len(nrow(model_specs_dist))) {
    s <- model_specs_dist$sample[i]
    stem <- model_specs_dist$stem[i]
    key <- paste0(s, "_", stem)

    dat <- subset_distlag_sample(dt, s)
    if (nrow(dat) < 100L) {
      logf("Skipping dist-lag model (small sample): ", key, " | N=", nrow(dat), log_file = log_file)
      rm(dat)
      gc()
      next
    }

    m <- fit_dist_lag(dat, stem = stem, h = h)
    coef_list[[key]] <- extract_dist_lag_coefs(m, stem = stem, sample_name = s)
    etable_dist_lines[[key]] <- capture.output(etable(m, se.below = TRUE, digits = 3))
    logf("Fitted dist-lag model: ", key, " | N=", nobs(m), log_file = log_file)
    rm(m, dat)
    gc()
  }

  coef_dist <- rbindlist(coef_list, use.names = TRUE, fill = TRUE)
  coef_dist[, stem_label := pretty_stem(stem)]

  cutoff_lbl <- if (is.finite(black_split_cutoff_pre)) sprintf("%.3f", black_split_cutoff_pre) else "NA"
  split_lbl <- paste0(black_split_method_label, " cutoff")
  sample_label_lookup <- c(
    full = "Full Sample",
    pre = paste0("Pre-COVID (<=", pre_covid_end, ")"),
    full_hi_black = paste0("Full: pct_black >= ", split_lbl, " (", cutoff_lbl, ")"),
    full_lo_black = paste0("Full: pct_black < ", split_lbl, " (", cutoff_lbl, ")"),
    pre_hi_black = paste0("Pre-COVID: pct_black >= ", split_lbl, " (", cutoff_lbl, ")"),
    pre_lo_black = paste0("Pre-COVID: pct_black < ", split_lbl, " (", cutoff_lbl, ")"),
    full_hi_evict_port  = paste0("Full: High-evicting portfolio (LOO rate >= ", loo_filing_threshold, ")"),
    full_lo_evict_port  = paste0("Full: Low-evicting portfolio (LOO rate < ", loo_filing_threshold, ")"),
    full_small_port     = paste0("Full: Small portfolio (< ", loo_min_units, " other units)"),
    full_solo_port      = "Full: Solo (no other portfolio buildings)",
    pre_hi_evict_port   = paste0("Pre-COVID: High-evicting portfolio (LOO rate >= ", loo_filing_threshold, ")"),
    pre_lo_evict_port   = paste0("Pre-COVID: Low-evicting portfolio (LOO rate < ", loo_filing_threshold, ")"),
    pre_small_port      = paste0("Pre-COVID: Small portfolio (< ", loo_min_units, " other units)"),
    pre_solo_port       = "Pre-COVID: Solo (no other portfolio buildings)"
  )
  coef_dist[, sample_label := unname(sample_label_lookup[sample])]
  coef_dist[is.na(sample_label), sample_label := sample]
  coef_dist[, stem_label := factor(stem_label, levels = c("Full", "Non-Severe", "Severe"))]
} else {
  logf("Skipping distributed-lag block (--skip_distlag=TRUE).", log_file = log_file)
}

bandwidth_tabs <- compute_bandwidth_tables(dt, max_bw = max_bw, time_unit = time_unit)
band_summary <- bandwidth_tabs$summary
band_status <- bandwidth_tabs$status
etable_dist_txt <- if (skip_distlag) character() else unlist(etable_dist_lines)

path_dist_coef <- file.path(tab_dir, paste0(out_prefix, "_distlag_coefficients.csv"))
path_dist_coef_black_split <- file.path(tab_dir, paste0(out_prefix, "_distlag_coefficients_black_split.csv"))
path_band_summary <- file.path(tab_dir, paste0(out_prefix, "_bandwidth_summary.csv"))
path_band_status <- file.path(tab_dir, paste0(out_prefix, "_bandwidth_status_counts.csv"))
path_case_back_rent_summary <- file.path(tab_dir, paste0(out_prefix, "_case_back_rent_summary.csv"))
path_case_back_rent_by_year <- file.path(tab_dir, paste0(out_prefix, "_case_back_rent_by_year.csv"))
path_case_retaliatory_status <- file.path(tab_dir, paste0(out_prefix, "_case_retaliatory_status_summary.csv"))
path_case_lpm_coefs <- file.path(tab_dir, paste0(out_prefix, "_case_one_month_retaliatory_coefficients.csv"))
path_case_lpm_sample <- file.path(tab_dir, paste0(out_prefix, "_case_one_month_retaliatory_sample_summary.csv"))
path_case_lpm_table_txt <- file.path(tab_dir, paste0(out_prefix, "_case_one_month_retaliatory_model_table.txt"))
path_case_lpm_table_tex <- file.path(tab_dir, paste0(out_prefix, "_case_one_month_retaliatory_model_table.tex"))
path_case_pois_coefs <- file.path(tab_dir, paste0(out_prefix, "_case_one_month_retaliatory_pois_coefficients.csv"))
path_case_pois_table_txt <- file.path(tab_dir, paste0(out_prefix, "_case_one_month_retaliatory_pois_model_table.txt"))
path_case_pois_table_tex <- file.path(tab_dir, paste0(out_prefix, "_case_one_month_retaliatory_pois_model_table.tex"))
path_case_log_rent_coefs <- file.path(tab_dir, paste0(out_prefix, "_case_log_rent_retaliatory_coefficients.csv"))
path_case_log_rent_table_txt <- file.path(tab_dir, paste0(out_prefix, "_case_log_rent_retaliatory_model_table.txt"))
path_case_log_rent_table_tex <- file.path(tab_dir, paste0(out_prefix, "_case_log_rent_retaliatory_model_table.tex"))
path_ev_filter_counts <- file.path(tab_dir, paste0(out_prefix, "_eviction_case_filter_counts.csv"))
path_dist_table <- file.path(tab_dir, paste0(out_prefix, "_distlag_model_table.txt"))
path_dist_plot <- file.path(fig_dir, paste0(out_prefix, "_distlag_coefficients.png"))
path_dist_plot_black_split <- file.path(fig_dir, paste0(out_prefix, "_distlag_coefficients_black_split.png"))
path_qa <- file.path(out_dir, paste0(out_prefix, "_qa.txt"))
path_maint_table_txt <- file.path(tab_dir, paste0(out_prefix, "_maintenance_model_table.txt"))
path_maint_table_tex <- file.path(tab_dir, paste0(out_prefix, "_maintenance_model_table.tex"))
path_maint_raw_table_txt <- file.path(tab_dir, paste0(out_prefix, "_maintenance_model_table_raw.txt"))
path_maint_raw_table_tex <- file.path(tab_dir, paste0(out_prefix, "_maintenance_model_table_raw.tex"))
path_maint_port_table_txt <- file.path(tab_dir, paste0(out_prefix, "_maintenance_model_table_port.txt"))
path_maint_port_table_tex <- file.path(tab_dir, paste0(out_prefix, "_maintenance_model_table_port.tex"))
path_filing_bin_summary <- file.path(tab_dir, paste0(out_prefix, "_maintenance_filing_bin_summary.csv"))
path_spell_maint_table_txt  <- file.path(tab_dir, paste0(out_prefix, "_spell_maintenance_model_table.txt"))
path_spell_maint_table_tex  <- file.path(tab_dir, paste0(out_prefix, "_spell_maintenance_model_table.tex"))
path_spell_dt_summary       <- file.path(tab_dir, paste0(out_prefix, "_spell_maintenance_sample_summary.csv"))
path_high_evict_stability   <- file.path(tab_dir, paste0(out_prefix, "_high_evict_status_stability.csv"))
path_landlord_resp_coef <- file.path(tab_dir, paste0(out_prefix, "_landlord_response_coefficients.csv"))
path_landlord_resp_plot <- file.path(fig_dir, paste0(out_prefix, "_landlord_response_plot.png"))
path_dist_coef_evict_port <- file.path(tab_dir, paste0(out_prefix, "_distlag_coefficients_evict_port_split.csv"))
path_dist_plot_evict_port <- file.path(fig_dir, paste0(out_prefix, "_distlag_coefficients_evict_port_split.png"))
path_case_lpm_port_coefs <- file.path(tab_dir, paste0(out_prefix, "_case_one_month_retaliatory_evict_port_split_coefficients.csv"))
path_landlord_resp_port_coef <- file.path(tab_dir, paste0(out_prefix, "_landlord_response_evict_port_split_coefficients.csv"))
path_landlord_resp_port_plot <- file.path(fig_dir, paste0(out_prefix, "_landlord_response_evict_port_split_plot.png"))

black_split_samples <- if (include_full) {
  c("full_hi_black", "full_lo_black", "pre_hi_black", "pre_lo_black")
} else {
  c("pre_hi_black", "pre_lo_black")
}
main_samples <- if (include_full) c("full", "pre") else c("pre")
coef_dist_black_split <- coef_dist[sample %chin% black_split_samples]
coef_dist_main <- coef_dist[sample %chin% main_samples]

coef_dist_evict_port_split <- if (length(evict_port_samples)) {
  coef_dist[sample %chin% evict_port_samples]
} else {
  coef_dist[0L]
}
write_dt_maybe(coef_dist, path_dist_coef, "dist-lag coefficients", compact_outputs = compact_outputs, log_file = log_file)
write_dt_maybe(coef_dist_black_split, path_dist_coef_black_split, "dist-lag black-split coefficients", compact_outputs = compact_outputs, log_file = log_file)
write_dt_maybe(coef_dist_evict_port_split, path_dist_coef_evict_port, "dist-lag evict-port-split coefficients", compact_outputs = compact_outputs, log_file = log_file)
write_dt_maybe(band_summary, path_band_summary, "bandwidth summary", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_dt_maybe(band_status, path_band_status, "bandwidth status", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_dt_maybe(case_back_rent_summary, path_case_back_rent_summary, "case back-rent summary", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_dt_maybe(case_back_rent_by_year, path_case_back_rent_by_year, "case back-rent by-year summary", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_dt_maybe(case_retaliatory_status_summary, path_case_retaliatory_status, "case retaliatory-status summary", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_dt_maybe(case_lpm_coefs, path_case_lpm_coefs, "case one-month retaliatory coefficients", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_dt_maybe(case_lpm_sample_summary, path_case_lpm_sample, "case one-month retaliatory sample summary", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_case_lpm_txt, path_case_lpm_table_txt, "case one-month retaliatory model table (txt)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_case_lpm_tex, path_case_lpm_table_tex, "case one-month retaliatory model table (tex)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_dt_maybe(case_pois_coefs, path_case_pois_coefs, "case months-back-rent Poisson coefficients", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_dt_maybe(case_lpm_port_coefs, path_case_lpm_port_coefs, "case back-rent evict-port-split coefficients", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_case_pois_txt, path_case_pois_table_txt, "case months-back-rent Poisson model table (txt)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_case_pois_tex, path_case_pois_table_tex, "case months-back-rent Poisson model table (tex)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_dt_maybe(case_log_rent_coefs, path_case_log_rent_coefs, "case log-rent retaliatory coefficients", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_case_log_rent_txt, path_case_log_rent_table_txt, "case log-rent retaliatory model table (txt)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_case_log_rent_tex, path_case_log_rent_table_tex, "case log-rent retaliatory model table (tex)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_dt_maybe(ev_filter_counts, path_ev_filter_counts, "eviction-case filter counts", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_dist_txt, path_dist_table, "dist-lag model table", compact_outputs = compact_outputs, log_file = log_file)

if (!skip_distlag && nrow(coef_dist_main)) {
  one_sample_only <- uniqueN(coef_dist_main$sample_label) == 1L
  p_dist <- ggplot(coef_dist_main, aes(x = timing, y = estimate)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray45") +
    geom_point(size = 1.3) +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.15) +
    scale_x_continuous(breaks = seq(-h, h, by = 1)) +
    labs(
      title = paste0(period_label_title, " Distributed-Lag Estimates: Complaints and Eviction Filings"),
      subtitle = paste0("LPM with PID and ", period_label, " fixed effects; clustered by PID"),
      x = paste0(tools::toTitleCase(period_label_plural), " relative to complaint indicator"),
      y = "Coefficient"
    ) +
    theme_minimal()
  if (one_sample_only) {
    p_dist <- p_dist + facet_wrap(~ stem_label, ncol = 1)
  } else {
    p_dist <- p_dist + facet_grid(stem_label ~ sample_label)
  }

  ggsave(filename = path_dist_plot, plot = p_dist, width = 11, height = 8, dpi = 300, bg = "white")
}

if (!skip_distlag && nrow(coef_dist_black_split)) {
  p_dist_black <- ggplot(coef_dist_black_split, aes(x = timing, y = estimate)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray45") +
    geom_point(size = 1.3) +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.15) +
    facet_grid(stem_label ~ sample_label) +
    scale_x_continuous(breaks = seq(-h, h, by = 1)) +
    labs(
      title = paste0(period_label_title, " Dist-Lag Heterogeneity by pct_black Split"),
      subtitle = paste0("Split at pre-COVID ", black_split_method_label, " cutoff of infousa_pct_black = ", sprintf("%.3f", black_split_cutoff_pre)),
      x = paste0(tools::toTitleCase(period_label_plural), " relative to complaint indicator"),
      y = "Coefficient"
    ) +
    theme_minimal()

  ggsave(filename = path_dist_plot_black_split, plot = p_dist_black, width = 12, height = 8, dpi = 300, bg = "white")
}

distlag_plot_note <- if (skip_distlag) {
  "Distributed lag skipped; no dist-lag plot written."
} else if (!nrow(coef_dist_main)) {
  "Distributed lag ran but produced no coefficient rows; no dist-lag plot written."
} else {
  paste0("Wrote dist-lag plot: ", path_dist_plot)
}

distlag_black_split_note <- if (skip_distlag) {
  "Distributed lag black-split skipped because distributed lag was skipped."
} else if (!nrow(coef_dist_black_split)) {
  "Distributed lag black-split produced no coefficient rows."
} else {
  paste0("Wrote dist-lag black-split coefficients CSV: ", path_dist_coef_black_split, "; plot: ", path_dist_plot_black_split)
}

if (!skip_distlag && nrow(coef_dist_evict_port_split)) {
  p_dist_port <- ggplot(coef_dist_evict_port_split, aes(x = timing, y = estimate)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray45") +
    geom_point(size = 1.3) +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.15) +
    facet_grid(stem_label ~ sample_label) +
    scale_x_continuous(breaks = seq(-h, h, by = 1)) +
    labs(
      title = paste0(period_label_title, " Dist-Lag Heterogeneity by Portfolio Eviction Group"),
      subtitle = paste0("LOO EB pre-COVID filing rate; High threshold = ", loo_filing_threshold, "; min portfolio units = ", loo_min_units),
      x = paste0(tools::toTitleCase(period_label_plural), " relative to complaint indicator"),
      y = "Coefficient"
    ) +
    theme_minimal()
  ggsave(filename = path_dist_plot_evict_port, plot = p_dist_port, width = 14, height = 8, dpi = 300, bg = "white")
  logf("Wrote dist-lag evict-port-split plot: ", path_dist_plot_evict_port, log_file = log_file)
}


# equilibrium maintenance
# regressions are counts of permits as a function of building characteristics.
# Main specification uses EB pre-COVID filing intensity; raw long-run rate is
# estimated as appendix robustness.

blp <- fread(bldg_panel_path)
stop_missing_cols(
  names(blp),
  c("PID", "year", "GEOID", "building_type", "quality_grade", "year_blt_decade",
    "num_units_bin", "total_units", "total_permits", "infousa_pct_black", "log_med_rent",
    "filing_rate_eb_pre_covid", "filing_rate_longrun_pre2019",
    "filing_rate_eb", "num_filings"),
  "maintenance baseline (bldg_panel_blp)"
)
blp[, PID := stringr::str_pad(as.character(PID), width = 9L, side = "left", pad = "0")]
blp[, year := suppressWarnings(as.integer(year))]
blp[, filing_rate_eb_pre_covid := suppressWarnings(as.numeric(filing_rate_eb_pre_covid))]
blp[, filing_rate_longrun_pre2019 := suppressWarnings(as.numeric(filing_rate_longrun_pre2019))]

# Join portfolio_evict_group (built above in LOO block) to blp
blp <- merge(blp, pid_cong_eb[, .(PID, portfolio_evict_group)], by = "PID", all.x = TRUE)
# PIDs not in the conglomerate xwalk (no linkage) are single-building owners → Solo
blp[is.na(portfolio_evict_group), portfolio_evict_group :=
  factor("Solo", levels = levels(pid_cong_eb$portfolio_evict_group))]

blp[, filing_bin_eb := make_filing_rate_bin(filing_rate_eb_pre_covid)]
blp[, filing_bin_raw := make_filing_rate_bin(filing_rate_longrun_pre2019)]
blp[, high_filing_eb := as.integer(is.finite(filing_rate_eb_pre_covid) &
                                     filing_rate_eb_pre_covid >= high_filing_threshold)]
blp[, high_filing_raw := as.integer(is.finite(filing_rate_longrun_pre2019) &
                                      filing_rate_longrun_pre2019 >= high_filing_threshold)]

blp[,mean_pct_black_bt := weighted.mean(infousa_pct_black,total_units, na.rm = TRUE), by = GEOID]
blp[,above_mean_pct_black_bt := infousa_pct_black > mean_pct_black_bt]
blp[is.na(above_mean_pct_black_bt), above_mean_pct_black_bt := FALSE]

filing_bin_summary <- rbindlist(list(
  blp[year %in% 2011:2019 & !is.na(filing_bin_eb), .(
    measure = "eb_pre_covid",
    filing_bin = as.character(filing_bin_eb),
    n_rows = .N,
    n_pid = uniqueN(PID)
  ), by = filing_bin_eb][, filing_bin_eb := NULL],
  blp[year %in% 2011:2019 & !is.na(filing_bin_raw), .(
    measure = "raw_longrun_pre2019",
    filing_bin = as.character(filing_bin_raw),
    n_rows = .N,
    n_pid = uniqueN(PID)
  ), by = filing_bin_raw][, filing_bin_raw := NULL]
), use.names = TRUE, fill = TRUE)
setorder(filing_bin_summary, measure, filing_bin)
write_dt_maybe(
  filing_bin_summary, path_filing_bin_summary, "maintenance filing-bin summary",
  compact_outputs = compact_outputs, force = TRUE, log_file = log_file
)
logf(
  "Maintenance filing bins (2011-2019) written to: ", path_filing_bin_summary,
  "; high_filing threshold=", high_filing_threshold,
  log_file = log_file
)

# inflation adjust rent
inf_adj_reg <- feols(
  log_med_rent ~ 1 | GEOID ^ year, data = blp[year %in% 2011:2019], weights = ~total_units
)

# create inf adj rent variable
blp[, log_med_rent_inf_adj := log_med_rent - predict(inf_adj_reg, newdata = blp)]
blp[,mean_log_med_rent_inf_adj_PID := mean(log_med_rent_inf_adj,na.rm = T), by = PID]
blp[,has_rent_data := !is.na(mean_log_med_rent_inf_adj_PID)]

baseline_maintenence <- fepois(
  total_permits ~ above_mean_pct_black_bt + high_filing_eb  | building_type + year + quality_grade + year_blt_decade + num_units_bin + GEOID + has_rent_data,
  offset = ~log(total_units),
  data = blp[year %in% 2011:2019]
)

baseline_maintenence_with_rent <- fepois(
  total_permits ~ above_mean_pct_black_bt + high_filing_eb + mean_log_med_rent_inf_adj_PID| building_type + year + quality_grade + year_blt_decade + num_units_bin + GEOID,
  offset = ~log(total_units),
  data = blp[year %in% 2011:2019]
)

# EB bins (replace "No filings" split with fully EB-based bins)
baseline_maintenence_bins <- fepois(
  total_permits ~ above_mean_pct_black_bt + i(filing_bin_eb, ref = "[0,1)") |
    building_type + year + quality_grade + year_blt_decade + num_units_bin + GEOID + has_rent_data,
  offset = ~log(total_units),
  data = blp[year %in% 2011:2019 & !is.na(filing_bin_eb)]
)

baseline_maintenence_bins_with_rent <- fepois(
  total_permits ~ above_mean_pct_black_bt + i(filing_bin_eb, ref = "[0,1)") + mean_log_med_rent_inf_adj_PID |
    building_type + year + quality_grade + year_blt_decade + num_units_bin + GEOID,
  offset = ~log(total_units),
  data = blp[year %in% 2011:2019 & !is.na(filing_bin_eb)]
)

# Raw robustness (appendix only)
baseline_maintenence_raw <- fepois(
  total_permits ~ above_mean_pct_black_bt + high_filing_raw  | building_type + year + quality_grade + year_blt_decade + num_units_bin + GEOID + has_rent_data,
  offset = ~log(total_units),
  data = blp[year %in% 2011:2019]
)

baseline_maintenence_raw_with_rent <- fepois(
  total_permits ~ above_mean_pct_black_bt + high_filing_raw + mean_log_med_rent_inf_adj_PID| building_type + year + quality_grade + year_blt_decade + num_units_bin + GEOID,
  offset = ~log(total_units),
  data = blp[year %in% 2011:2019]
)

baseline_maintenence_raw_bins <- fepois(
  total_permits ~ above_mean_pct_black_bt + i(filing_bin_raw, ref = "[0,1)") |
    building_type + year + quality_grade + year_blt_decade + num_units_bin + GEOID + has_rent_data,
  offset = ~log(total_units),
  data = blp[year %in% 2011:2019 & !is.na(filing_bin_raw)]
)

baseline_maintenence_raw_bins_with_rent <- fepois(
  total_permits ~ above_mean_pct_black_bt + i(filing_bin_raw, ref = "[0,1)") + mean_log_med_rent_inf_adj_PID |
    building_type + year + quality_grade + year_blt_decade + num_units_bin + GEOID,
  offset = ~log(total_units),
  data = blp[year %in% 2011:2019 & !is.na(filing_bin_raw)]
)

# ── Portfolio-group maintenance regressions ─────────────────────────────────
# Replace high_filing_eb with portfolio_evict_group (LOO-based), or include both.
# Reference level = "Solo" (no other buildings in the conglomerate).
baseline_maintenence_port <- fepois(
  total_permits ~ above_mean_pct_black_bt + portfolio_evict_group |
    building_type + year + quality_grade + year_blt_decade + num_units_bin + GEOID + has_rent_data,
  offset = ~log(total_units),
  data = blp[year %in% 2011:2019]
)

baseline_maintenence_eb_and_port <- fepois(
  total_permits ~ above_mean_pct_black_bt + high_filing_eb + portfolio_evict_group |
    building_type + year + quality_grade + year_blt_decade + num_units_bin + GEOID + has_rent_data,
  offset = ~log(total_units),
  data = blp[year %in% 2011:2019]
)

# ── Parcel × ownership-spell regressions ─────────────────────────────────────
# One row per contiguous run of the same conglomerate_id at each PID (2011–2019).
# This makes the cross-sectional unit of observation honest (~100–250 K spells),
# makes filing intensity spell-specific, and enables a PID-FE robustness column
# that identifies from ownership transitions within parcels.
# NOTE: filing_rate_eb is a static PID-level EB rate (does not vary by year).
#       mean(filing_rate_eb) over spell years = the PID's pooled EB rate.
#       The raw-rate robustness (filing_rate_raw_spell = filings / unit-years)
#       is the genuinely spell-varying measure.

logf("=== Spell-level maintenance block START ===", log_file = log_file)

{
  # -- Load year-varying conglomerate xwalk --------------------------------
  cong_yr_blp <- fread(p_product(cfg, "xwalk_pid_conglomerate"),
                       select = c("PID", "year", "conglomerate_id"))
  cong_yr_blp[, PID  := stringr::str_pad(as.character(PID), width = 9L, side = "left", pad = "0")]
  cong_yr_blp[, year := as.integer(year)]

  # Merge to blp (2011–2019 only)
  blp_cong <- merge(
    blp[year %in% 2011:2019],
    cong_yr_blp[year %in% 2011:2019],
    by = c("PID", "year"), all.x = TRUE
  )
  # Convert to character BEFORE solo assignment — xwalk stores conglomerate_id as
  # integer, and paste0() produces a character string that would be coerced to NA.
  blp_cong[, conglomerate_id := as.character(conglomerate_id)]
  # Parcels with no xwalk entry → solo (unique conglomerate per PID)
  blp_cong[is.na(conglomerate_id), conglomerate_id := paste0("solo_", PID)]

  # Detect spell boundaries: contiguous years under the same conglomerate at each PID
  setorder(blp_cong, PID, year)
  blp_cong[, spell_id := paste0(PID, "_", rleid(conglomerate_id)), by = PID]

  logf("  blp_cong rows (2011-2019): ", nrow(blp_cong),
       " | unique spell_id: ", uniqueN(blp_cong$spell_id), log_file = log_file)

  # -- Collapse to spell level ---------------------------------------------
  spell_dt <- blp_cong[, {
    spell_years       <- .N
    total_units_val   <- mean(total_units, na.rm = TRUE)
    unit_years_val    <- total_units_val * spell_years
    .(
      spell_years               = spell_years,
      total_permits_sum         = sum(total_permits, na.rm = TRUE),
      n_permit_years            = sum(!is.na(total_permits)),  # years with actual permit data
      total_filings_sum         = sum(num_filings, na.rm = TRUE),
      total_units               = total_units_val,
      unit_years                = unit_years_val,
      mean_filing_rate_eb       = mean(filing_rate_eb, na.rm = TRUE),
      mean_pct_black            = mean(infousa_pct_black, na.rm = TRUE),
      mean_log_rent_inf_adj     = mean(log_med_rent_inf_adj, na.rm = TRUE),
      conglomerate_id           = first(conglomerate_id),
      start_year                = min(year),
      end_year                  = max(year),
      building_type             = first(building_type),
      quality_grade             = first(quality_grade),
      year_blt_decade           = first(year_blt_decade),
      num_units_bin             = first(num_units_bin),
      GEOID                     = first(GEOID)
    )
  }, by = .(PID, spell_id)]
  assert_unique(spell_dt, c("PID", "spell_id"), "spell_dt after collapse")

  logf("  spell_dt total_permits_sum: min=", min(spell_dt$total_permits_sum),
       " max=", max(spell_dt$total_permits_sum),
       " n_spells_with_permit_data=", spell_dt[n_permit_years > 0, .N],
       log_file = log_file)

  logf("  spell_dt rows: ", nrow(spell_dt),
       " | unique PIDs: ", uniqueN(spell_dt$PID), log_file = log_file)

  # -- Spell-level covariates ----------------------------------------------
  # Primary: EB rate (constant per PID — spell mean = PID EB rate)
  spell_dt[, high_filing_spell  := as.integer(is.finite(mean_filing_rate_eb) &
                                                mean_filing_rate_eb >= high_filing_threshold)]
  spell_dt[, filing_bin_spell   := make_filing_rate_bin(mean_filing_rate_eb)]


  spell_dt[, total_permits_sum := as.numeric(total_permits_sum)]
  spell_dt[, GEOID := as.character(GEOID)]

  # Robustness: raw spell filing rate = total filings / unit-years
  spell_dt[, filing_rate_raw_spell := fifelse(unit_years > 0,
                                               total_filings_sum / unit_years, NA_real_)]
  spell_dt[, high_filing_raw_spell := as.integer(is.finite(filing_rate_raw_spell) &
                                                    filing_rate_raw_spell >= high_filing_threshold)]

  setorder(spell_dt,'PID', 'start_year')
  # Above-mean % Black (vs GEOID weighted mean)
  spell_dt[, mean_pct_black_GEOID := weighted.mean(mean_pct_black, total_units, na.rm = TRUE), by = GEOID]
  spell_dt[, above_mean_pct_black_spell := mean_pct_black > mean_pct_black_GEOID]
  spell_dt[is.na(above_mean_pct_black_spell), above_mean_pct_black_spell := FALSE]

  # Rent data flag
  spell_dt[, has_rent_data_spell := !is.na(mean_log_rent_inf_adj)]

  # -- Full-period conglomerate LOO classification -------------------------
  # Landlord type is a structural attribute — classify from the full 2011-2019
  # panel rather than the noise-prone spell-window snapshot. Uses shared helper
  # from helper-functions.R. One LOO type per (PID, conglomerate_id); joined to
  # spell_dt by those two keys (spell_dt.conglomerate_id = first(conglomerate_id)
  # is constant within a spell by construction).
  loo_type_dt <- compute_loo_filing_type(
    pid_yr_dt            = blp_cong[, .(PID, year, conglomerate_id, num_filings, total_units)],
    loo_min_unit_years   = loo_min_units * 9L,
    loo_filing_threshold = loo_filing_threshold
  )

  spell_dt <- merge(spell_dt, loo_type_dt, by = c("PID", "conglomerate_id"), all.x = TRUE)
  # PIDs with no conglomerate membership (solo_*) already have loo_units_cong == 0 → Solo
  spell_dt[is.na(portfolio_evict_group_cong),
    portfolio_evict_group_cong := factor("Solo",
      levels = levels(loo_type_dt$portfolio_evict_group_cong))]

  logf("  portfolio_evict_group_cong distribution (full-period LOO):",
       paste(capture.output(spell_dt[, .N, by = portfolio_evict_group_cong]), collapse = " | "),
       log_file = log_file)

  # Spell-level ownership transition vars (require portfolio_evict_group_cong to be merged first)
  setorder(spell_dt, 'PID', 'start_year')
  spell_dt[, prev_owner        := shift(portfolio_evict_group_cong), by = PID]
  spell_dt[, transfer_type     := paste(portfolio_evict_group_cong, prev_owner)]
  spell_dt[, high_file_acquirer := portfolio_evict_group_cong == "High-evicting portfolio" & !is.na(prev_owner)]
  spell_dt[, high_filer        := portfolio_evict_group_cong == "High-evicting portfolio"]
  spell_dt[, acquirer          := !is.na(prev_owner)]

  # -- PIDs with multiple spells (identifying sample for M4 PID FE) --------
  n_multi_spell_pids <- spell_dt[, .N, by = PID][N > 1, .N]
  logf("  PIDs with >1 spell (M4 identifying sample): ", n_multi_spell_pids, log_file = log_file)

  # -- Regressions ---------------------------------------------------------
  # Restrict to spells with at least one year of observed permit data.
  # Spells where all years are NA produce total_permits_sum = 0 (via na.rm=TRUE),
  # which is indistinguishable from "0 permits observed" — analogous to how the
  # annual regression drops NA rows rather than treating them as 0-permit years.
  spell_dt_reg <- spell_dt[n_permit_years > 0]
  logf("  spell_dt_reg (permit-data spells): ", nrow(spell_dt_reg),
       " of ", nrow(spell_dt), " total spells | unique PIDs: ", uniqueN(spell_dt_reg$PID),
       log_file = log_file)

  # Spell FE note: the annual model uses 6 FEs (building_type + year + quality_grade +
  # year_blt_decade + num_units_bin + GEOID). At spell level the panel is ~20× sparser
  # (267K spells vs 1.7M annual rows), so the same 6-way FE yields ~72M possible cells
  # for 267K observations — virtually every spell is a PPML singleton (PPML also drops
  # all-zero-outcome cells), leaving 0 rows. Fix: keep the two theoretically essential
  # absorbers (GEOID for neighbourhood and start_year for entry cohort) and move
  # quality_grade, year_blt_decade, num_units_bin to the RHS as covariates.
  # Cross-product after fix: building_type × start_year × GEOID ≈ 14K cells, 267K rows
  # → ~19 spells/cell. Scientifically equivalent (linear FEs absorbed vs estimated).

  # M1: EB filing dummy + racial composition; building_type + start_year + GEOID FE.
  # year_blt_decade and num_units_bin move to RHS to avoid PPML singleton explosion
  # (5-way FE × 267K sparse spells → ~72M cells ≈ all singletons → empty sample).
  spell_maint_m1 <- fepois(
    total_permits_sum ~ above_mean_pct_black_spell + high_filing_spell +
      has_rent_data_spell + year_blt_decade + num_units_bin |
      building_type + start_year + GEOID,
    offset = ~log(unit_years),
    data   = spell_dt_reg
  )

  # M2: add mean inflation-adjusted rent (restricts to rent-observed spells)
  spell_maint_m2 <- fepois(
    total_permits_sum ~ above_mean_pct_black_spell + high_filing_spell +
      mean_log_rent_inf_adj + year_blt_decade + num_units_bin |
      building_type + start_year + GEOID,
    offset = ~log(unit_years),
    data   = spell_dt_reg[!is.na(mean_log_rent_inf_adj)]
  )

  # M3: add portfolio eviction group (full-period LOO via conglomerate)
  spell_maint_m3 <- fepois(
    total_permits_sum ~ above_mean_pct_black_spell + high_filing_spell +
      has_rent_data_spell + portfolio_evict_group_cong +
      year_blt_decade + num_units_bin |
      building_type + start_year + GEOID,
    offset = ~log(unit_years),
    weights = ~unit_years,
    data   = spell_dt_reg[total_units > 2]
  )

  # M4: PID FE robustness — identifies from within-parcel ownership transitions.
  # Note: high_filing_spell is static per PID (EB rate does not vary by year),
  # so fixest will drop it as collinear with PID FE. Identifying variables are
  # portfolio_evict_group_cong (varies if conglomerate composition changes) and
  # above_mean_pct_black_spell (varies if pct_black or GEOID mean changes by spell).
  spell_maint_m4 <- fepois(
    total_permits_sum ~
      i(portfolio_evict_group_cong, ref = "Small portfolio") |
      PID + start_year,
    offset = ~log(unit_years),
    weights = ~unit_years,
    data   = spell_dt_reg[has_rent_data_spell==1],
    cluster = ~PID
  )

  logf("  spell_maint_m1 nobs=", nobs(spell_maint_m1),
       " | m2=", nobs(spell_maint_m2),
       " | m3=", nobs(spell_maint_m3),
       " | m4=", nobs(spell_maint_m4), log_file = log_file)

  # -- Sample summary ------------------------------------------------------
  spell_dt_summary <- spell_dt_reg[, .(
    n_spells_reg          = .N,
    n_pid_reg             = uniqueN(PID),
    n_spells_total        = nrow(spell_dt),
    n_pid_total           = uniqueN(spell_dt$PID),
    n_pid_multi_spell     = spell_dt_reg[, .N, by = PID][N > 1, .N],
    mean_spell_years      = round(mean(spell_years), 2),
    median_spell_years    = median(spell_years),
    mean_unit_years       = round(mean(unit_years, na.rm = TRUE), 1),
    pct_high_filing       = round(mean(high_filing_spell, na.rm = TRUE), 4),
    pct_has_rent          = round(mean(has_rent_data_spell), 4)
  )]

  # -- etables -------------------------------------------------------------
  etable_spell_maint_txt <- capture.output(etable(
    list(
      "M1: EB dummy + race" = spell_maint_m1,
      "M2: + Rent"          = spell_maint_m2,
      "M3: + Portfolio LOO" = spell_maint_m3,
      "M4: PID FE"          = spell_maint_m4
    ),
    se.below = TRUE,
    digits   = 3
  ))

  etable_spell_maint_tex <- capture.output(etable(
    list(
      "M1: EB dummy + race" = spell_maint_m1,
      "M2: + Rent"          = spell_maint_m2,
      "M3: + Portfolio LOO" = spell_maint_m3,
      "M4: PID FE"          = spell_maint_m4
    ),
    se.below = TRUE,
    digits   = 3,
    tex      = TRUE
  ))

  # -- Step 5b: High-evicting status stability diagnostic ------------------
  # Check how often a PID's annual high-evicting status flips (EB rate is static,
  # so this uses raw annual filing rate to detect noisy year-to-year changes).
  blp_stability <- blp[year %in% 2011:2019 & !is.na(filing_rate_longrun_pre2019), .(
    PID, year,
    high_filing_yr_raw = as.integer(is.finite(filing_rate_longrun_pre2019) &
                                      filing_rate_longrun_pre2019 >= high_filing_threshold)
  )]
  setorder(blp_stability, PID, year)
  blp_stability[, status_change_raw := high_filing_yr_raw != shift(high_filing_yr_raw, 1L), by = PID]
  blp_stability[is.na(status_change_raw), status_change_raw := FALSE]

  stability_summary <- blp_stability[, .(
    n_years      = .N,
    n_flips_raw  = sum(status_change_raw),
    always_high  = all(high_filing_yr_raw == 1L),
    always_low   = all(high_filing_yr_raw == 0L),
    mixed_status = any(status_change_raw)
  ), by = PID]

  high_evict_stability_overall <- stability_summary[, .N, by = .(always_high, always_low, mixed_status)]
  logf("  Filing-status stability (raw rate): always_high=",
       stability_summary[always_high == TRUE, .N],
       " | always_low=", stability_summary[always_low == TRUE, .N],
       " | mixed=", stability_summary[mixed_status == TRUE, .N], log_file = log_file)

  rm(cong_yr_blp, loo_type_dt, blp_stability, stability_summary)
}

logf("=== Spell-level maintenance block END ===", log_file = log_file)

# etables and export
etable_baseline_maintenence_txt <- capture.output(etable(
  list(
    "EB >=15% dummy" = baseline_maintenence,
    "EB >=15% dummy + Rent" = baseline_maintenence_with_rent,
    "EB filing bins" = baseline_maintenence_bins,
    "EB filing bins + Rent" = baseline_maintenence_bins_with_rent
  ),
  se.below = TRUE,
  digits = 3
))

etable_baseline_maintenence_tex <- capture.output(etable(
  list(
    "EB >=15% dummy" = baseline_maintenence,
    "EB >=15% dummy + Rent" = baseline_maintenence_with_rent,
    "EB filing bins" = baseline_maintenence_bins,
    "EB filing bins + Rent" = baseline_maintenence_bins_with_rent
  ),
  se.below = TRUE,
  digits = 3,
  tex = TRUE
))

etable_baseline_maintenence_raw_txt <- capture.output(etable(
  list(
    "Raw >=15% dummy" = baseline_maintenence_raw,
    "Raw >=15% dummy + Rent" = baseline_maintenence_raw_with_rent,
    "Raw filing bins" = baseline_maintenence_raw_bins,
    "Raw filing bins + Rent" = baseline_maintenence_raw_bins_with_rent
  ),
  se.below = TRUE,
  digits = 3
))

etable_baseline_maintenence_raw_tex <- capture.output(etable(
  list(
    "Raw >=15% dummy" = baseline_maintenence_raw,
    "Raw >=15% dummy + Rent" = baseline_maintenence_raw_with_rent,
    "Raw filing bins" = baseline_maintenence_raw_bins,
    "Raw filing bins + Rent" = baseline_maintenence_raw_bins_with_rent
  ),
  se.below = TRUE,
  digits = 3,
  tex = TRUE
))

etable_baseline_maintenence_port_txt <- capture.output(etable(
  list(
    "LOO portfolio group" = baseline_maintenence_port,
    "EB >=15% + LOO portfolio" = baseline_maintenence_eb_and_port
  ),
  se.below = TRUE,
  digits = 3
))

etable_baseline_maintenence_port_tex <- capture.output(etable(
  list(
    "LOO portfolio group" = baseline_maintenence_port,
    "EB >=15% + LOO portfolio" = baseline_maintenence_eb_and_port
  ),
  se.below = TRUE,
  digits = 3,
  tex = TRUE
))

# show landlords respond to maintence; within parcel regress maintenence counts
# on current + lagged complaint counts

# make leads / lags of severe complaints in blp data
setorder(blp, PID, year)
for(t in 1:3){
  blp[, paste0("total_severe_complaints_lag_", t) := shift(total_severe_complaints, n = t, type = "lag"), by = PID]
  blp[, paste0("total_severe_complaints_lead_", t) := shift(total_severe_complaints, n = t, type = "lead"), by = PID]


}

landlord_response_regs <- fepois(
  total_permits ~ total_severe_complaints + total_severe_complaints_lag_1 + total_severe_complaints_lag_2 + total_severe_complaints_lag_3 + total_severe_complaints_lead_1 + total_severe_complaints_lead_2 + total_severe_complaints_lead_3
  | PID + year,
  offset = ~log(total_units),
  data = blp[year %in% 2011:2019]
)

# Extract landlord response coefficients and build coefplot
lr_ct <- as.data.frame(coeftable(landlord_response_regs))
lr_coef <- data.table(
  term      = rownames(lr_ct),
  estimate  = lr_ct[["Estimate"]],
  std_error = lr_ct[["Std. Error"]]
)
lr_coef[, conf_low  := estimate - 1.96 * std_error]
lr_coef[, conf_high := estimate + 1.96 * std_error]
lr_coef[, timing := {
  n <- suppressWarnings(as.integer(sub(".*_(\\d+)$", "\\1", term)))
  fcase(
    term == "total_severe_complaints", 0L,
    grepl("_lag_", term),  n,
    grepl("_lead_", term), -n,
    default = NA_integer_
  )
}]
setorder(lr_coef, timing)

p_landlord_resp <- ggplot(lr_coef, aes(x = timing, y = estimate)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray45") +
  geom_vline(xintercept = 0, linetype = "dotted", color = "gray60") +
  geom_point(size = 1.5) +
  geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.2) +
  scale_x_continuous(breaks = -3:3,
    labels = c("-3\n(future)", "-2\n(future)", "-1\n(future)", "0", "+1\n(past)", "+2\n(past)", "+3\n(past)")) +
  labs(
    title = "Landlord Maintenance Response to Severe Complaints",
    subtitle = "PPML with PID + year FE; offset = log(total_units); 95% CI",
    x = "Severe-complaint timing relative to permit period",
    y = "Coefficient"
  ) +
  theme_minimal()

# export and save outputs
write_lines_maybe(etable_baseline_maintenence_txt, path_maint_table_txt, "maintenance model table (txt)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_baseline_maintenence_tex, path_maint_table_tex, "maintenance model table (tex)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_baseline_maintenence_raw_txt, path_maint_raw_table_txt, "maintenance raw-robustness model table (txt)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_baseline_maintenence_raw_tex, path_maint_raw_table_tex, "maintenance raw-robustness model table (tex)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_baseline_maintenence_port_txt, path_maint_port_table_txt, "maintenance portfolio model table (txt)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_baseline_maintenence_port_tex, path_maint_port_table_tex, "maintenance portfolio model table (tex)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_spell_maint_txt, path_spell_maint_table_txt, "spell-level maintenance model table (txt)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_lines_maybe(etable_spell_maint_tex, path_spell_maint_table_tex, "spell-level maintenance model table (tex)", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_dt_maybe(spell_dt_summary, path_spell_dt_summary, "spell-level maintenance sample summary", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_dt_maybe(high_evict_stability_overall, path_high_evict_stability, "high-evict status stability summary", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
write_dt_maybe(lr_coef, path_landlord_resp_coef, "landlord response coefficients", compact_outputs = compact_outputs, force = TRUE, log_file = log_file)
ggsave(filename = path_landlord_resp_plot, plot = p_landlord_resp, width = 9, height = 6, dpi = 300, bg = "white")
logf("Wrote landlord response plot: ", path_landlord_resp_plot, log_file = log_file)

# ── Landlord maintenance response by portfolio eviction group ──────────────
# Same PPML spec as landlord_response_regs, run separately per portfolio group.
lr_port_coefs <- rbindlist(lapply(levels(blp$portfolio_evict_group), function(grp) {
  d <- blp[year %in% 2011:2019 & portfolio_evict_group == grp]
  if (nrow(d) < 100L) {
    logf("  Skipping landlord response port split (small): ", grp, " | N=", nrow(d), log_file = log_file)
    return(NULL)
  }
  m <- tryCatch(
    fepois(
      total_permits ~
        total_severe_complaints +
        total_severe_complaints_lag_1 + total_severe_complaints_lag_2 + total_severe_complaints_lag_3 +
        total_severe_complaints_lead_1 + total_severe_complaints_lead_2 + total_severe_complaints_lead_3
      | PID + year,
      offset = ~log(total_units),
      data = d
    ),
    error = function(e) {
      logf("  Permit PPML port split error (", grp, "): ", conditionMessage(e), log_file = log_file); NULL
    }
  )
  if (is.null(m)) return(NULL)
  ct <- as.data.frame(coeftable(m))
  rows <- data.table(
    portfolio_evict_group = grp,
    term      = rownames(ct),
    estimate  = ct[["Estimate"]],
    std_error = ct[["Std. Error"]]
  )
  rows[, conf_low  := estimate - 1.96 * std_error]
  rows[, conf_high := estimate + 1.96 * std_error]
  rows[, timing := {
    n <- suppressWarnings(as.integer(sub(".*_(\\d+)$", "\\1", term)))
    fcase(
      term == "total_severe_complaints", 0L,
      grepl("_lag_",  term),  n,
      grepl("_lead_", term), -n,
      default = NA_integer_
    )
  }]
  logf("  Landlord response port split: ", grp, " | N=", nobs(m), log_file = log_file)
  rows
}), use.names = TRUE, fill = TRUE)

write_dt_maybe(lr_port_coefs, path_landlord_resp_port_coef,
               "landlord response port-split coefficients",
               compact_outputs = compact_outputs, force = TRUE, log_file = log_file)

if (nrow(lr_port_coefs)) {
  setorder(lr_port_coefs, portfolio_evict_group, timing)
  p_lr_port <- ggplot(lr_port_coefs, aes(x = timing, y = estimate)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray45") +
    geom_vline(xintercept = 0, linetype = "dotted", color = "gray60") +
    geom_point(size = 1.5) +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.2) +
    facet_wrap(~portfolio_evict_group) +
    scale_x_continuous(breaks = -3:3,
      labels = c("-3\n(future)", "-2\n(future)", "-1\n(future)", "0",
                 "+1\n(past)", "+2\n(past)", "+3\n(past)")) +
    labs(
      title = "Landlord Maintenance Response by Portfolio Eviction Group",
      subtitle = "PPML with PID + year FE; offset = log(total_units); 95% CI",
      x = "Severe-complaint timing relative to permit period",
      y = "Coefficient"
    ) +
    theme_minimal()
  ggsave(filename = path_landlord_resp_port_plot, plot = p_lr_port,
         width = 14, height = 6, dpi = 300, bg = "white")
  logf("Wrote landlord response port-split plot: ", path_landlord_resp_port_plot, log_file = log_file)
}


qa_lines <- c(
  paste0("Retaliatory Evictions QA (", period_label_title, ", Rental-Only)"),
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  paste0(period_label_title, " panel input: ", panel_path),
  paste0("Analytic PID input: ", analytic_path),
  paste0("Rows analyzed (full): ", nrow(dt)),
  paste0("Rows analyzed (pre-covid): ", n_pre),
  paste0("Unique PIDs (full): ", uniqueN(dt$PID)),
  paste0("Eviction filing rate (full): ", round(mean(dt$filed_eviction, na.rm = TRUE), 6)),
  paste0("Any complaint rate (full): ", round(mean(dt$filed_complaint, na.rm = TRUE), 6)),
  paste0("Case back-rent one-month tolerance: +/-", one_month_back_rent_tol),
  paste0("Case rows for back-rent analysis: ", nrow(case_dt)),
  paste0("Case share with valid total_rent/ongoing_rent ratio: ", round(mean(case_dt$valid_rent_ratio, na.rm = TRUE), 6)),
  paste0("Case one-month back-rent share (among valid ratios): ", round(mean(case_dt$one_month_back_rent, na.rm = TRUE), 6)),
  paste0("Case PID-period panel match share: ", round(mean(case_dt$has_panel_match, na.rm = TRUE), 6)),
  paste0("Case retaliatory(severe-proximity) share among matched: ", round(case_dt[has_panel_match == TRUE, mean(retaliatory_severe == 1L, na.rm = TRUE)], 6)),
  paste0("Case race join share: ", round(mean(case_dt$has_case_race, na.rm = TRUE), 6)),
  paste0("Case race impute-ok share among joined: ", round(case_dt[has_case_race == TRUE, mean(case_impute_status == "ok", na.rm = TRUE)], 6)),
  paste0("Filing intensity main measure: filing_rate_eb_pre_covid (EB pooled pre-2019)"),
  paste0("High filing threshold: ", high_filing_threshold),
  "Filing bins: [0,1), [1,5), [5,10), [10,20), [20,inf)",
  paste0("Pre-COVID infousa_pct_black split method: ", black_split_method_label),
  paste0("Pre-COVID infousa_pct_black cutoff used: ", if (is.finite(black_split_cutoff_pre)) round(black_split_cutoff_pre, 6) else "NA"),
  paste0("Time unit: ", time_unit),
  paste0("skip_distlag: ", skip_distlag),
  paste0("include_full: ", include_full),
  paste0("Bandwidths evaluated: 1 to ", max_bw, " ", period_label_plural),
  "",
  "Bandwidth summary (head):",
  capture.output(print(head(band_summary, 10L))),
  "",
  "Case back-rent summary (head):",
  capture.output(print(head(case_back_rent_summary, 12L))),
  "",
  "Case retaliatory status summary (pre-covid):",
  capture.output(print(case_retaliatory_status_summary)),
  "",
  "Case one-month retaliatory LPM sample summary:",
  capture.output(print(case_lpm_sample_summary)),
  "",
  "Case one-month retaliatory LPM coefficients (head):",
  capture.output(print(head(case_lpm_coefs, 20L))),
  "",
  "Case months-back-rent Poisson (PPML) coefficients (head):",
  capture.output(print(head(case_pois_coefs, 20L))),
  "",
  "Eviction case filter counts:",
  capture.output(print(ev_filter_counts)),
  "",
  paste0("Wrote dist-lag coefficients CSV: ", path_dist_coef),
  paste0("Wrote dist-lag black-split coefficients CSV: ", path_dist_coef_black_split),
  paste0("Wrote bandwidth summary CSV: ", path_band_summary),
  paste0("Wrote bandwidth status CSV: ", path_band_status),
  paste0("Wrote case back-rent summary CSV: ", path_case_back_rent_summary),
  paste0("Wrote case back-rent by-year CSV: ", path_case_back_rent_by_year),
  paste0("Wrote case retaliatory-status summary CSV: ", path_case_retaliatory_status),
  paste0("Wrote case one-month retaliatory coefficients CSV: ", path_case_lpm_coefs),
  paste0("Wrote case one-month retaliatory sample summary CSV: ", path_case_lpm_sample),
  paste0("Wrote case one-month retaliatory model table (txt): ", path_case_lpm_table_txt),
  paste0("Wrote case one-month retaliatory model table (tex): ", path_case_lpm_table_tex),
  paste0("Wrote case months-back-rent Poisson coefficients CSV: ", path_case_pois_coefs),
  paste0("Wrote case months-back-rent Poisson model table (txt): ", path_case_pois_table_txt),
  paste0("Wrote case months-back-rent Poisson model table (tex): ", path_case_pois_table_tex),
  paste0("Wrote eviction-case filter counts CSV: ", path_ev_filter_counts),
  paste0("Case race input path: ", race_case_path),
  paste0("Wrote dist-lag model table: ", path_dist_table),
  distlag_plot_note,
  distlag_black_split_note,
  paste0("Wrote maintenance model table (txt): ", path_maint_table_txt),
  paste0("Wrote maintenance model table (tex): ", path_maint_table_tex),
  paste0("Wrote maintenance raw-robustness table (txt): ", path_maint_raw_table_txt),
  paste0("Wrote maintenance raw-robustness table (tex): ", path_maint_raw_table_tex),
  paste0("Wrote maintenance portfolio table (txt): ", path_maint_port_table_txt),
  paste0("Wrote maintenance portfolio table (tex): ", path_maint_port_table_tex),
  paste0("Wrote maintenance filing-bin summary CSV: ", path_filing_bin_summary),
  paste0("Wrote landlord response coefficients CSV: ", path_landlord_resp_coef),
  paste0("Wrote landlord response plot: ", path_landlord_resp_plot),
  paste0("Portfolio LOO filing threshold (config): ", loo_filing_threshold, "; min portfolio units: ", loo_min_units),
  paste0("portfolio_evict_group distribution: ",
         paste(capture.output(dt[, .N, by = portfolio_evict_group]), collapse = " | ")),
  paste0("Wrote dist-lag evict-port-split coefficients CSV: ", path_dist_coef_evict_port),
  paste0("Wrote case back-rent evict-port-split coefficients CSV: ", path_case_lpm_port_coefs),
  paste0("Wrote landlord response evict-port-split coefficients CSV: ", path_landlord_resp_port_coef),
  paste0("Wrote landlord response evict-port-split plot: ", path_landlord_resp_port_plot),
  "",
  "Spell-level maintenance regressions:",
  paste0("  spell_dt rows: ", nrow(spell_dt), " | unique PIDs: ", uniqueN(spell_dt$PID)),
  paste0("  PIDs with >1 spell (M4 identifying sample): ", spell_dt[, .N, by = PID][N > 1, .N]),
  paste0("  portfolio_evict_group_cong distribution (full-period LOO): ",
         paste(capture.output(spell_dt[, .N, by = portfolio_evict_group_cong]), collapse = " | ")),
  paste0("  spell_maint_m1 nobs=", nobs(spell_maint_m1),
         " | m2=", nobs(spell_maint_m2),
         " | m3=", nobs(spell_maint_m3),
         " | m4=", nobs(spell_maint_m4)),
  paste0("  Wrote spell-level maintenance model table (txt): ", path_spell_maint_table_txt),
  paste0("  Wrote spell-level maintenance model table (tex): ", path_spell_maint_table_tex),
  paste0("  Wrote spell-level maintenance sample summary CSV: ", path_spell_dt_summary),
  paste0("  Wrote high-evict status stability CSV: ", path_high_evict_stability)
)

qa_lines <- c(qa_lines, module_a_qa_lines)
writeLines(qa_lines, con = path_qa)

logf("Wrote outputs to: ", out_dir, log_file = log_file)
logf("=== Finished retaliatory-evictions.r ===", log_file = log_file)
