## ============================================================
## retaliatory-evictions.r
## ============================================================
## Purpose: Distributed-lag and local-projections DiD relationship
## between complaint activity and eviction filings (PID x period panel),
## using rental-only building panels. Time unit toggles between month
## and quarter via config/CLI, plus retaliatory bandwidth summaries.
##
## Inputs:
##   - processed/panels/building_data_rental_month.parquet (default)
##   - processed/analytic/analytic_sample.csv (PID universe filter)
##   - processed/products evictions_clean + evict_address_xwalk
##
## Outputs:
##   - output/retaliatory/*
##   - output/logs/retaliatory-evictions.log
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(fixest)
  library(ggplot2)
})

source("r/config.R")

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

normalize_time_unit <- function(x) {
  x1 <- tolower(trimws(as.character(x %||% "")))
  if (x1 %chin% c("month", "months", "m")) return("month")
  if (x1 %chin% c("quarter", "quarters", "q", "year_quarter", "year-quarter", "yq")) return("quarter")
  stop("Unsupported time_unit='", x1, "'. Use 'month' or 'quarter'.")
}

build_pid_filter_cmd <- function(pid_file, csv_file) {
  sprintf(
    "awk -F, 'NR==FNR{keep[$1]=1; next} FNR==1 || ($1 in keep)' %s %s",
    shQuote(pid_file),
    shQuote(csv_file)
  )
}

make_leads_lags <- function(dt, stem, h) {
  filed_col <- paste0("filed_", stem)
  for (k in seq_len(h)) {
    lag_col <- paste0("lag_", stem, "_", k)
    lead_col <- paste0("lead_", stem, "_", k)
    dt[, (lag_col) := shift(get(filed_col), n = k, type = "lag"), by = PID]
    dt[, (lead_col) := shift(get(filed_col), n = k, type = "lead"), by = PID]
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

choose_lpdid_window <- function(dt, stem, preferred_periods = 24L, fallback_periods = 12L, min_retention = 0.60) {
  treat_col <- paste0("filed_", stem)
  if (!treat_col %in% names(dt)) {
    stop("Missing treatment column for lpdid window choice: ", treat_col)
  }

  first_treat <- dt[get(treat_col) == 1L, .(first_t = min(time_index)), by = PID]
  min_t <- suppressWarnings(min(dt$time_index, na.rm = TRUE))
  max_t <- suppressWarnings(max(dt$time_index, na.rm = TRUE))

  if (!nrow(first_treat) || !is.finite(min_t) || !is.finite(max_t)) {
    return(data.table(
      chosen_window_periods = fallback_periods,
      preferred_window_periods = preferred_periods,
      fallback_window_periods = fallback_periods,
      treated_units = nrow(first_treat),
      preferred_eligible_units = 0L,
      preferred_retention = NA_real_,
      min_retention = min_retention,
      used_fallback = TRUE
    ))
  }

  pref_eligible <- first_treat[
    first_t >= (min_t + preferred_periods) & first_t <= (max_t - preferred_periods),
    .N
  ]
  pref_retention <- pref_eligible / nrow(first_treat)
  used_fallback <- is.na(pref_retention) || pref_retention < min_retention
  chosen <- if (used_fallback) fallback_periods else preferred_periods

  data.table(
    chosen_window_periods = chosen,
    preferred_window_periods = preferred_periods,
    fallback_window_periods = fallback_periods,
    treated_units = nrow(first_treat),
    preferred_eligible_units = pref_eligible,
    preferred_retention = pref_retention,
    min_retention = min_retention,
    used_fallback = used_fallback
  )
}

fit_lpdid <- function(dt, stem, window_periods, nonabsorbing_lag = NULL, y = "filed_eviction") {
  if (!requireNamespace("lpdid", quietly = TRUE)) {
    stop("Package 'lpdid' is required. Install from https://github.com/danielegirardi/lpdid")
  }
  treat_col <- paste0("filed_", stem)
  if (!treat_col %in% names(dt)) stop("Missing treatment column for lpdid: ", treat_col)

  if (is.null(nonabsorbing_lag) || is.na(nonabsorbing_lag)) {
    nonabsorbing_lag <- as.integer(window_periods)
  }

  dat_lp <- dt[, .(
    PID,
    time_index,
    y = as.numeric(get(y)),
    treat = as.integer(get(treat_col))
  )]

  lpdid::lpdid(
    df = dat_lp,
    y = "y",
    treat_status = "treat",
    unit_index = "PID",
    time_index = "time_index",
    nonabsorbing_lag = as.integer(nonabsorbing_lag),
    window = c(-as.integer(window_periods), as.integer(window_periods)),
    cluster = "PID"
  )
}

extract_lpdid_coefs <- function(model, stem, sample_name) {
  ct <- as.data.table(model$coeftable)
  if (!nrow(ct)) return(data.table())
  if (!all(c("Estimate", "Std. Error", "Pr(>|t|)") %in% names(ct))) {
    stop("Unexpected lpdid coeftable columns.")
  }

  h_seq <- seq.int(from = as.integer(model$window[1L]), to = as.integer(model$window[2L]))
  if (length(h_seq) == nrow(ct)) {
    ct[, timing := h_seq]
  } else {
    ct[, timing := seq_len(.N) - ceiling(.N / 2L)]
  }

  setnames(
    ct,
    c("Estimate", "Std. Error", "Pr(>|t|)"),
    c("estimate", "std_error", "p_value")
  )
  if ("t value" %in% names(ct)) setnames(ct, "t value", "t_value")
  if (!"t_value" %in% names(ct)) ct[, t_value := NA_real_]

  ct[, `:=`(
    sample = sample_name,
    stem = stem,
    model = "lpdid",
    conf_low = estimate - 1.96 * std_error,
    conf_high = estimate + 1.96 * std_error
  )]

  ct[, .(
    sample, stem, model, timing, estimate, std_error, t_value, p_value, conf_low, conf_high
  )]
}

pretty_stem <- function(stem) {
  fifelse(
    stem == "complaint", "Any Complaint",
    fifelse(
      stem == "severe", "Severe Complaint",
      fifelse(stem == "non_severe", "Non-Severe Complaint", stem)
    )
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
    keep <- intersect(select_cols, avail)
    dt <- as.data.table(arrow::read_parquet(panel_path, col_select = keep))
  } else {
    hdr <- names(fread(panel_path, nrows = 0L))
    keep <- intersect(select_cols, hdr)
    if (!is.null(pid_filter) && length(pid_filter)) {
      pid_file <- tempfile(fileext = ".txt")
      writeLines(pid_filter, pid_file)
      on.exit(unlink(pid_file), add = TRUE)
      cmd <- build_pid_filter_cmd(pid_file, panel_path)
      dt <- fread(cmd = cmd, select = keep)
    } else {
      dt <- fread(panel_path, select = keep)
    }
  }

  assert_has_cols(dt, c("parcel_number", "period", "total_complaints"), "building panel slice")
  setDT(dt)
  setnames(dt, "parcel_number", "PID")
  dt[, PID := str_pad(as.character(PID), width = 9L, side = "left", pad = "0")]

  if (!is.null(pid_filter) && length(pid_filter)) {
    dt <- dt[PID %chin% pid_filter]
  }
  dt
}

compute_bandwidth_tables <- function(dt, max_bw, time_unit) {
  out_summary <- vector("list", max_bw)
  out_status <- vector("list", max_bw)

  ev_rows <- dt[filed_eviction == 1L, .(PID, period, filed_complaint)]
  if (!nrow(ev_rows)) {
    stop("No eviction rows available after period-level merge.")
  }

  for (b in seq_len(max_bw)) {
    lag_cols <- paste0("lag_complaint_", seq_len(b))
    lead_cols <- paste0("lead_complaint_", seq_len(b))
    win_cols <- c(lag_cols, lead_cols)
    win_cols <- win_cols[win_cols %in% names(dt)]

    tmp <- dt[filed_eviction == 1L, c("PID", "period", "filed_complaint", win_cols), with = FALSE]
    if (length(win_cols)) {
      tmp[, nearby_any := as.integer(rowSums(.SD, na.rm = TRUE) > 0L), .SDcols = win_cols]
    } else {
      tmp[, nearby_any := 0L]
    }
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
    # Backward-compatible aliases for existing month-based consumers.
    s[, `:=`(
      same_month_n = if (time_unit == "month") same_period_n else NA_integer_,
      same_month_share = if (time_unit == "month") same_period_share else NA_real_
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

add_tenant_composition <- function(dt, bldg_panel_path, start_year, end_year, pid_filter = NULL) {
  tenant_cols <- c(
    "infousa_pct_black",
    "infousa_pct_female",
    "infousa_pct_black_female",
    "infousa_share_persons_demog_ok"
  )
  bldg_obs_cols <- c(
    "total_area", "total_units", "market_value",
    "building_type", "num_units_bin", "year_blt_decade",
    "num_stories_bin", "quality_grade"
  )
  hdr <- names(fread(bldg_panel_path, nrows = 0L))
  keep <- intersect(c("PID", "year", "GEOID", tenant_cols, bldg_obs_cols), hdr)
  bldg <- fread(bldg_panel_path, select = keep)
  setDT(bldg)
  bldg[, PID := str_pad(as.character(PID), width = 9L, side = "left", pad = "0")]
  bldg[, year := suppressWarnings(as.integer(year))]
  if (!is.null(pid_filter) && length(pid_filter)) bldg <- bldg[PID %chin% pid_filter]
  bldg <- bldg[year >= start_year & year <= end_year]
  for (cc in tenant_cols) {
    if (!(cc %in% names(bldg))) bldg[, (cc) := NA_real_]
    bldg[, (cc) := suppressWarnings(as.numeric(get(cc)))]
  }
  extra_cols <- intersect(c("GEOID", bldg_obs_cols), names(bldg))
  bldg <- unique(bldg[, c("PID", "year", extra_cols, tenant_cols), with = FALSE], by = c("PID", "year"))
  assert_unique(bldg, c("PID", "year"), "bldg tenant composition (PID-year)")

  setkey(dt, PID, year)
  setkey(bldg, PID, year)
  dt <- bldg[dt]
  for (cc in tenant_cols) {
    dt[, (cc) := suppressWarnings(as.numeric(get(cc)))]
  }
  dt[, tenant_comp_missing := as.integer(
    is.na(infousa_pct_black) |
      is.na(infousa_pct_female) |
      is.na(infousa_pct_black_female) |
      is.na(infousa_share_persons_demog_ok)
  )]

  center_or_zero <- function(x) {
    mu <- mean(x, na.rm = TRUE)
    if (!is.finite(mu)) mu <- 0
    out <- fifelse(is.na(x), mu, x) - mu
    as.numeric(out)
  }
  dt[, tc_black_c := center_or_zero(infousa_pct_black)]
  dt[, tc_female_c := center_or_zero(infousa_pct_female)]
  dt[, tc_black_female_c := center_or_zero(infousa_pct_black_female)]
  dt[, tc_cov_c := center_or_zero(infousa_share_persons_demog_ok)]
  dt
}

fit_same_period_retaliatory <- function(dt, stem, y = "filed_eviction") {
  treat_col <- paste0("filed_", stem)
  fml <- as.formula(paste0(y, " ~ ", treat_col, " | period_fe + PID"))
  feols(fml, data = dt, cluster = ~PID, lean = TRUE)
}

fit_same_period_retaliatory_tenant <- function(dt, stem, y = "filed_eviction") {
  treat_col <- paste0("filed_", stem)
  fml <- as.formula(
    paste0(
      y, " ~ ", treat_col,
      " + ", treat_col, ":tc_black_c",
      " + ", treat_col, ":tc_female_c",
      " + ", treat_col, ":tc_black_female_c",
      " + ", treat_col, ":tc_cov_c",
      " + tenant_comp_missing | period_fe + PID"
    )
  )
  feols(fml, data = dt, cluster = ~PID, lean = TRUE)
}

fit_same_period_tract <- function(dt, stem, y = "filed_eviction") {
  treat_col <- paste0("filed_", stem)
  fml <- as.formula(paste0(y, " ~ ", treat_col, " | period_fe + GEOID"))
  feols(fml, data = dt, cluster = ~GEOID, lean = TRUE)
}

fit_same_period_tract_tenant <- function(dt, stem, y = "filed_eviction") {
  treat_col <- paste0("filed_", stem)
  fml <- as.formula(
    paste0(
      y, " ~ ", treat_col,
      " + ", treat_col, ":tc_black_c",
      " + ", treat_col, ":tc_female_c",
      " + ", treat_col, ":tc_black_female_c",
      " + ", treat_col, ":tc_cov_c",
      " + tenant_comp_missing | period_fe + GEOID"
    )
  )
  feols(fml, data = dt, cluster = ~GEOID, lean = TRUE)
}

extract_tenant_comp_coefs <- function(model, stem, sample_name, spec_name) {
  ct <- as.data.table(summary(model)$coeftable, keep.rownames = "term")
  if (!nrow(ct)) return(data.table())
  setnames(
    ct,
    c("Estimate", "Std. Error", "t value", "Pr(>|t|)"),
    c("estimate", "std_error", "t_value", "p_value")
  )
  treat_col <- paste0("^filed_", stem, "$")
  int_pat <- paste0(
    "(^filed_", stem, ":tc_|^tc_.*:filed_", stem, "$)"
  )
  ct <- ct[grepl(treat_col, term) | grepl(int_pat, term)]
  if (!nrow(ct)) return(data.table())
  ct[, `:=`(
    sample = sample_name,
    stem = stem,
    spec = spec_name,
    model = "same_period",
    conf_low = estimate - 1.96 * std_error,
    conf_high = estimate + 1.96 * std_error
  )]
  ct[]
}

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

time_unit_in <- opts$time_unit %||% opts$retaliatory_time_unit
if (is.null(time_unit_in) || !nzchar(as.character(time_unit_in))) {
  cfg_time <- cfg$run$retaliatory_time_unit %||% ""
  if (!nzchar(as.character(cfg_time))) {
    bdl <- tolower(as.character(cfg$run$building_data_agg_level %||% ""))
    cfg_time <- if (bdl %chin% c("month", "quarter")) bdl else "month"
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
out_dir <- opts$output_dir %||% p_out(cfg, "retaliatory")
log_file <- p_out(cfg, "logs", "retaliatory-evictions.log")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

start_year <- to_int(opts$start_year, 2007L)
end_year <- to_int(opts$end_year, 2024L)
pre_covid_end <- to_int(opts$pre_covid_end, 2019L)
h_default <- if (time_unit == "quarter") 4L else 12L
h <- to_int(opts$horizon, h_default)
if (h < 1L) h <- 1L
max_bw_default <- if (time_unit == "quarter") 2L else 6L
max_bw <- to_int(opts$max_bandwidth, max_bw_default)
if (max_bw < 1L) max_bw <- 1L
max_lead_lag <- max(h, max_bw)
lp_window_default <- 2L * periods_per_year
lp_fallback_default <- 1L * periods_per_year
lp_window_periods <- to_int(opts$lp_window_periods %||% opts$lp_window_months, lp_window_default)
lp_fallback_window_periods <- to_int(opts$lp_fallback_window_periods %||% opts$lp_fallback_window_months, lp_fallback_default)
if (lp_window_periods < 1L) lp_window_periods <- lp_window_default
if (lp_fallback_window_periods < 1L) lp_fallback_window_periods <- lp_fallback_default
if (lp_fallback_window_periods > lp_window_periods) lp_fallback_window_periods <- lp_window_periods
lp_nonabsorbing_lag <- suppressWarnings(as.integer(opts$lp_nonabsorbing_lag %||% NA))
if (length(lp_nonabsorbing_lag) == 0L || is.na(lp_nonabsorbing_lag[1L])) {
  lp_nonabsorbing_lag <- NA_integer_
} else {
  lp_nonabsorbing_lag <- as.integer(lp_nonabsorbing_lag[1L])
}
lp_min_treated_retention <- suppressWarnings(as.numeric(opts$lp_min_treated_retention %||% "0.60"))
if (!is.finite(lp_min_treated_retention)) lp_min_treated_retention <- 0.60
lp_min_treated_retention <- min(max(lp_min_treated_retention, 0), 1)

sample_mode <- to_bool(opts$sample_mode, cfg$run$sample_mode %||% FALSE)
sample_n <- to_int(opts$sample_n, cfg$run$sample_n %||% 200000L)
seed <- to_int(opts$seed, cfg$run$seed %||% 123L)
use_pid_filter <- to_bool(opts$use_pid_filter, TRUE)
skip_lpdid <- to_bool(opts$skip_lpdid, cfg$run$retaliatory_skip_lpdid %||% TRUE)

logf("=== Starting retaliatory-evictions.r ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf(period_label_title, " panel input: ", panel_path, log_file = log_file)
logf("Analytic PID universe input: ", analytic_path, log_file = log_file)
logf("BLP panel input (tenant composition): ", bldg_panel_path, log_file = log_file)
logf("Output dir: ", out_dir, log_file = log_file)
logf(
  "Time unit=", time_unit,
  ", periods_per_year=", periods_per_year,
  ", settings: years=", start_year, "-", end_year,
  ", pre_covid_end=", pre_covid_end,
  ", horizon=", h,
  ", max_bandwidth=", max_bw,
  ", lp_window_periods=", lp_window_periods,
  ", lp_fallback_window_periods=", lp_fallback_window_periods,
  ", lp_min_treated_retention=", lp_min_treated_retention,
  ", lp_nonabsorbing_lag=", if (is.na(lp_nonabsorbing_lag)) "auto" else lp_nonabsorbing_lag,
  ", skip_lpdid=", skip_lpdid,
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
if (!skip_lpdid && !requireNamespace("lpdid", quietly = TRUE)) {
  stop("Package 'lpdid' is required. Install from https://github.com/danielegirardi/lpdid")
}

pid_filter <- NULL
if (use_pid_filter) {
  pid_dt <- fread(analytic_path, select = "PID")
  pid_dt[, PID := str_pad(as.character(PID), width = 9L, side = "left", pad = "0")]
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
  "heat_complaint_count", "fire_complaint_count", "drainage_complaint_count",
  "property_maintenance_complaint_count", "building_complaint_count",
  "emergency_service_complaint_count", "zoning_complaint_count",
  "trash_weeds_complaint_count", "license_business_complaint_count",
  "program_initiative_complaint_count", "vacant_property_complaint_count"
)

need_cols <- c("parcel_number", "period", "total_complaints", "total_severe_complaints",
               "total_permits", "total_violations", complaint_candidates)
dt <- read_panel_slice(panel_path, need_cols, pid_filter = pid_filter)
assert_has_cols(dt, c("PID", "period", "total_complaints"), paste0(period_label, " building panel slice"))

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

# Derive census tract from block-group GEOID for tract FE models
if ("GEOID" %in% names(dt)) {
  dt[, census_tract := substr(as.character(GEOID), 1L, 11L)]
}

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
ev <- fread(evictions_path, select = c("d_filing", "n_sn_ss_c", "commercial"))
xw <- fread(evict_xwalk_path, select = c("PID", "n_sn_ss_c", "num_parcels_matched"))
setDT(ev)
setDT(xw)
assert_has_cols(ev, c("d_filing", "n_sn_ss_c"), "evictions_clean (subset)")
assert_has_cols(xw, c("PID", "n_sn_ss_c", "num_parcels_matched"), "evict_address_xwalk (subset)")

xw <- xw[num_parcels_matched == 1L]
xw[, PID := str_pad(as.character(PID), width = 9L, side = "left", pad = "0")]
xw <- unique(xw[, .(PID, n_sn_ss_c)], by = c("PID", "n_sn_ss_c"))
setkey(xw, n_sn_ss_c)
setkey(ev, n_sn_ss_c)
ev <- ev[xw, nomatch = 0L]
ev <- ev[is.na(commercial) | tolower(commercial) != "t"]

ev[, filing_date_chr := substr(as.character(d_filing), 1L, 10L)]
ev[, filing_date := as.IDate(filing_date_chr, format = "%Y-%m-%d")]
ev <- ev[!is.na(filing_date)]
ev[, year := as.integer(substr(as.character(filing_date), 1L, 4L))]
ev <- ev[year >= start_year & year <= end_year]
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

num_cols <- setdiff(names(dt), c("PID", "period"))
for (cc in num_cols) {
  suppressWarnings(dt[, (cc) := as.numeric(get(cc))])
  dt[is.na(get(cc)), (cc) := 0]
}

if ("total_severe_complaints" %in% names(dt) &&
    dt[, any(total_severe_complaints > total_complaints, na.rm = TRUE)]) {
  stop("Invalid panel: total_severe_complaints exceeds total_complaints for at least one PID-period.")
}

dt[, filed_eviction := as.integer(num_filings > 0)]
dt[, filed_complaint := as.integer(total_complaints > 0)]

severe_candidates <- c(
  "heat_complaint_count", "fire_complaint_count",
  "drainage_complaint_count", "property_maintenance_complaint_count"
)
non_severe_candidates <- c(
  "building_complaint_count", "emergency_service_complaint_count",
  "zoning_complaint_count", "trash_weeds_complaint_count",
  "license_business_complaint_count", "program_initiative_complaint_count",
  "vacant_property_complaint_count"
)

severe_cols <- intersect(severe_candidates, names(dt))
non_severe_cols <- intersect(non_severe_candidates, names(dt))
if (!length(non_severe_cols)) stop("No non-severe complaint columns available in ", period_label, " panel.")

if ("total_severe_complaints" %in% names(dt)) {
  dt[, filed_severe := as.integer(as.numeric(total_severe_complaints) > 0)]
  severe_source <- "total_severe_complaints"
} else {
  if (!length(severe_cols)) stop("No severe complaint columns available in ", period_label, " panel.")
  dt[, filed_severe := as.integer(rowSums(.SD > 0, na.rm = TRUE) > 0L), .SDcols = severe_cols]
  severe_source <- paste(severe_cols, collapse = ",")
}
dt[, filed_non_severe := as.integer(rowSums(.SD > 0, na.rm = TRUE) > 0L), .SDcols = non_severe_cols]

dt[, time_index := year * periods_per_year + period_num]
setorder(dt, PID, time_index)

make_leads_lags(dt, stem = "complaint", h = max_lead_lag)
make_leads_lags(dt, stem = "severe", h = max_lead_lag)
make_leads_lags(dt, stem = "non_severe", h = max_lead_lag)

dt[, period_fe := as.factor(period)]

# No separate panel_pre copy — filter inline via dt[year <= pre_covid_end] to save memory
n_pre <- dt[year <= pre_covid_end, .N]
if (n_pre == 0L) stop("Pre-COVID sample is empty after filters.")

logf("Panel rows (full, ", period_label, "): ", nrow(dt), ", PIDs: ", uniqueN(dt$PID), log_file = log_file)
logf("Panel rows (pre-covid, ", period_label, "): ", n_pre, ", PIDs: ", dt[year <= pre_covid_end, uniqueN(PID)], log_file = log_file)
logf("Eviction filing rate (full): ", round(mean(dt$filed_eviction, na.rm = TRUE), 6), log_file = log_file)
logf("Any complaint rate (full): ", round(mean(dt$filed_complaint, na.rm = TRUE), 6), log_file = log_file)
logf("Severe complaint source: ", severe_source, log_file = log_file)

model_specs <- data.table(
  sample = c("full", "full", "full", "pre", "pre", "pre"),
  stem = c("complaint", "severe", "non_severe", "complaint", "severe", "non_severe")
)

# Distributed lag specs.
# NOTE: model objects are discarded after coef extraction to avoid OOM on large panels.
# etable is called per-model and concatenated, instead of storing all models in memory.
etable_dist_lines <- list()
coef_dist_list <- list()
for (i in seq_len(nrow(model_specs))) {
  s <- model_specs$sample[i]
  stem <- model_specs$stem[i]
  key <- paste0(s, "_", stem)
  dat <- if (s == "pre") dt[year <= pre_covid_end] else dt
  m <- fit_dist_lag(dat, stem = stem, h = h)
  coef_dist_list[[key]] <- extract_dist_lag_coefs(m, stem = stem, sample_name = s)
  etable_dist_lines[[key]] <- capture.output(etable(m, se.below = TRUE, digits = 3))
  logf("Fitted dist-lag model: ", key, " | N=", nobs(m), log_file = log_file)
  rm(m, dat); gc()
}
coef_dist <- rbindlist(coef_dist_list, use.names = TRUE, fill = TRUE)
coef_dist[, stem_label := pretty_stem(stem)]
coef_dist[, sample_label := fifelse(sample == "pre", paste0("Pre-COVID (<=", pre_covid_end, ")"), "Full Sample")]

# Local projections DiD specs.
models_lpdid <- list()
coef_lpdid <- data.table(
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
lpdid_windows <- data.table(
  sample = character(),
  stem = character(),
  nonabsorbing_lag = integer(),
  chosen_window_periods = integer(),
  preferred_window_periods = integer(),
  fallback_window_periods = integer(),
  treated_units = integer(),
  preferred_eligible_units = integer(),
  preferred_retention = numeric(),
  min_retention = numeric(),
  used_fallback = logical()
)

if (!skip_lpdid) {
  coef_lpdid_list <- list()
  lpdid_window_list <- list()
  for (i in seq_len(nrow(model_specs))) {
    s <- model_specs$sample[i]
    stem <- model_specs$stem[i]
    key <- paste0(s, "_", stem)
    dat <- if (s == "pre") dt[year <= pre_covid_end] else dt

    w_info <- choose_lpdid_window(
      dat,
      stem = stem,
      preferred_periods = lp_window_periods,
      fallback_periods = lp_fallback_window_periods,
      min_retention = lp_min_treated_retention
    )
    chosen_w <- as.integer(w_info$chosen_window_periods[1L])
    chosen_nonabsorbing <- if (is.na(lp_nonabsorbing_lag)) chosen_w else as.integer(lp_nonabsorbing_lag)

    m_lp <- fit_lpdid(
      dat,
      stem = stem,
      window_periods = chosen_w,
      nonabsorbing_lag = chosen_nonabsorbing,
      y = "filed_eviction"
    )
    models_lpdid[[key]] <- m_lp
    coef_lpdid_list[[key]] <- extract_lpdid_coefs(m_lp, stem = stem, sample_name = s)
    lpdid_window_list[[key]] <- cbind(
      data.table(sample = s, stem = stem, nonabsorbing_lag = chosen_nonabsorbing),
      w_info
    )
    logf(
      "Fitted lpdid model: ", key,
      " | N=", m_lp$nobs,
      ", window=+/-", chosen_w,
      ", fallback=", isTRUE(w_info$used_fallback[1L]),
      ", preferred_retention=", round(w_info$preferred_retention[1L], 3),
      log_file = log_file
    )
  }
  coef_lpdid <- rbindlist(coef_lpdid_list, use.names = TRUE, fill = TRUE)
  coef_lpdid[, stem_label := pretty_stem(stem)]
  coef_lpdid[, sample_label := fifelse(sample == "pre", paste0("Pre-COVID (<=", pre_covid_end, ")"), "Full Sample")]
  lpdid_windows <- rbindlist(lpdid_window_list, use.names = TRUE, fill = TRUE)
} else {
  logf("Skipping LP-DiD block (--skip_lpdid=TRUE).", log_file = log_file)
}

# Same-period retaliation models: baseline + tenant-composition augmented.
etable_same_period_lines <- list()
etable_same_period_tex_lines <- list()
coef_same_period_list <- list()
for (i in seq_len(nrow(model_specs))) {
  s <- model_specs$sample[i]
  stem <- model_specs$stem[i]
  key <- paste0(s, "_", stem)
  dat <- if (s == "pre") dt[year <= pre_covid_end] else dt

  m_base <- fit_same_period_retaliatory(dat, stem = stem, y = "filed_eviction")
  m_tenant <- fit_same_period_retaliatory_tenant(dat, stem = stem, y = "filed_eviction")

  coef_same_period_list[[paste0(key, "_base")]] <- extract_tenant_comp_coefs(
    m_base, stem = stem, sample_name = s, spec_name = "baseline"
  )
  coef_same_period_list[[paste0(key, "_tenant")]] <- extract_tenant_comp_coefs(
    m_tenant, stem = stem, sample_name = s, spec_name = "tenant_augmented"
  )
  etable_same_period_lines[[key]] <- capture.output(
    etable(m_base, m_tenant, se.below = TRUE, digits = 3)
  )
  etable_same_period_tex_lines[[key]] <- capture.output(
    etable(m_base, m_tenant, se.below = TRUE, digits = 3, tex = TRUE)
  )
  logf("Fitted same-period models: ", key, " (baseline + tenant)", log_file = log_file)
  rm(m_base, m_tenant, dat); gc()
}
coef_same_period <- rbindlist(coef_same_period_list, use.names = TRUE, fill = TRUE)
coef_same_period[, stem_label := pretty_stem(stem)]
coef_same_period[, sample_label := fifelse(sample == "pre", paste0("Pre-COVID (<=", pre_covid_end, ")"), "Full Sample")]
coef_same_period_main <- coef_same_period[
  grepl("^filed_", term) & !grepl(":", term),
  .(sample, stem, spec, term, estimate, std_error, p_value)
]
coef_same_period_wide <- dcast(
  coef_same_period_main,
  sample + stem + term ~ spec,
  value.var = "estimate"
)
if ("baseline" %in% names(coef_same_period_wide) && "tenant_augmented" %in% names(coef_same_period_wide)) {
  coef_same_period_wide[, estimate_delta := tenant_augmented - baseline]
}

# Same-period retaliation models with TRACT FE (GEOID) instead of PID FE.
# Identifies off cross-building variation within neighborhoods.
has_geoid <- "GEOID" %in% names(dt) && dt[!is.na(GEOID) & nzchar(as.character(GEOID)), .N] > 0
etable_tract_lines <- list()
etable_tract_tex_lines <- list()
coef_tract_list <- list()
if (has_geoid) {
  for (i in seq_len(nrow(model_specs))) {
    s <- model_specs$sample[i]
    stem <- model_specs$stem[i]
    key <- paste0(s, "_", stem)
    dat <- if (s == "pre") dt[year <= pre_covid_end] else dt

    m_base_tract <- fit_same_period_tract(dat, stem = stem, y = "filed_eviction")
    m_tenant_tract <- fit_same_period_tract_tenant(dat, stem = stem, y = "filed_eviction")

    coef_tract_list[[paste0(key, "_base")]] <- extract_tenant_comp_coefs(
      m_base_tract, stem = stem, sample_name = s, spec_name = "tract_baseline"
    )
    coef_tract_list[[paste0(key, "_tenant")]] <- extract_tenant_comp_coefs(
      m_tenant_tract, stem = stem, sample_name = s, spec_name = "tract_tenant_augmented"
    )
    etable_tract_lines[[key]] <- capture.output(
      etable(m_base_tract, m_tenant_tract, se.below = TRUE, digits = 3)
    )
    etable_tract_tex_lines[[key]] <- capture.output(
      etable(m_base_tract, m_tenant_tract, se.below = TRUE, digits = 3, tex = TRUE)
    )
    logf("Fitted tract FE same-period models: ", key, " (baseline + tenant)", log_file = log_file)
    rm(m_base_tract, m_tenant_tract, dat); gc()
  }
} else {
  logf("Skipping tract FE models: GEOID not available in panel.", log_file = log_file)
}
coef_tract <- rbindlist(coef_tract_list, use.names = TRUE, fill = TRUE)
if (nrow(coef_tract)) {
  coef_tract[, stem_label := pretty_stem(stem)]
  coef_tract[, sample_label := fifelse(sample == "pre", paste0("Pre-COVID (<=", pre_covid_end, ")"), "Full Sample")]
}
etable_tract_txt <- unlist(etable_tract_lines)
etable_tract_tex <- unlist(etable_tract_tex_lines)

# ======================================================================
# Retaliatory Targeting: among eviction filings, are retaliatory ones
# disproportionately in buildings with more Black / female tenants?
# Sample: building-periods WITH an eviction filing
# Outcome: retaliatory_flag (complaint within bandwidth)
# FE: period + tract (within-neighborhood comparison)
# ======================================================================
has_tract <- "census_tract" %in% names(dt)
targeting_coef_list <- list()
targeting_etable_lines <- list()
targeting_etable_tex_lines <- list()
bldg_year_targeting_coef_list <- list()
bldg_year_targeting_etable_lines <- list()
bldg_year_targeting_etable_tex_lines <- list()

if (has_geoid && has_tract) {
  # Build retaliatory flags: same-period and within-bandwidth
  dt[, retaliatory_same := as.integer(filed_eviction == 1L & filed_complaint == 1L)]
  bw_lag_cols <- paste0("lag_complaint_", seq_len(max_bw))
  bw_lead_cols <- paste0("lead_complaint_", seq_len(max_bw))
  bw_all_cols <- intersect(c(bw_lag_cols, bw_lead_cols), names(dt))
  if (length(bw_all_cols)) {
    dt[, retaliatory_bw := as.integer(
      filed_eviction == 1L & (filed_complaint == 1L | rowSums(.SD > 0, na.rm = TRUE) > 0L)
    ), .SDcols = bw_all_cols]
  } else {
    dt[, retaliatory_bw := retaliatory_same]
  }

  ev_dt <- dt[filed_eviction == 1L]
  logf(
    "Retaliatory targeting sample: ", nrow(ev_dt), " eviction-periods; ",
    "retaliatory_same=", round(mean(ev_dt$retaliatory_same, na.rm = TRUE), 4),
    ", retaliatory_bw=", round(mean(ev_dt$retaliatory_bw, na.rm = TRUE), 4),
    log_file = log_file
  )

  for (outcome_name in c("same", "bw")) {
    y_col <- paste0("retaliatory_", outcome_name)
    for (s in c("full", "pre")) {
      dat <- if (s == "pre") ev_dt[year <= pre_covid_end] else ev_dt
      if (nrow(dat) < 100L) next
      key <- paste0(s, "_", outcome_name)

      # Tract FE
      fml_tract <- as.formula(paste0(
        y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
        " + tenant_comp_missing | period_fe + census_tract"
      ))
      m_tract <- feols(fml_tract, data = dat, cluster = ~census_tract, lean = TRUE)

      # BG FE
      fml_bg <- as.formula(paste0(
        y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
        " + tenant_comp_missing | period_fe + GEOID"
      ))
      m_bg <- feols(fml_bg, data = dat, cluster = ~GEOID, lean = TRUE)

      ct_tract <- as.data.table(summary(m_tract)$coeftable, keep.rownames = "term")
      ct_bg <- as.data.table(summary(m_bg)$coeftable, keep.rownames = "term")
      setnames(ct_tract, c("Estimate", "Std. Error", "t value", "Pr(>|t|)"),
               c("estimate", "std_error", "t_value", "p_value"))
      setnames(ct_bg, c("Estimate", "Std. Error", "t value", "Pr(>|t|)"),
               c("estimate", "std_error", "t_value", "p_value"))
      ct_tract[, `:=`(sample = s, outcome = outcome_name, fe = "tract")]
      ct_bg[, `:=`(sample = s, outcome = outcome_name, fe = "bg")]
      targeting_coef_list[[paste0(key, "_tract")]] <- ct_tract
      targeting_coef_list[[paste0(key, "_bg")]] <- ct_bg

      targeting_etable_lines[[key]] <- capture.output(
        etable(m_tract, m_bg, se.below = TRUE, digits = 4,
               headers = c("Tract FE", "BG FE"))
      )
      targeting_etable_tex_lines[[key]] <- capture.output(
        etable(m_tract, m_bg, se.below = TRUE, digits = 4, tex = TRUE,
               headers = c("Tract FE", "BG FE"))
      )
      logf("Fitted targeting model: ", key, " | N_tract=", nobs(m_tract),
           ", N_bg=", nobs(m_bg), log_file = log_file)
      rm(m_tract, m_bg, dat); gc()
    }
  }
  # ------------------------------------------------------------------
  # Building-year level targeting: collapse to PID x year, then regress
  # share_retaliatory ~ demographics + building observables | tract/BG
  # ------------------------------------------------------------------
  # Prepare building observables
  for (cc in c("total_area", "total_units", "market_value")) {
    if (cc %in% names(ev_dt)) ev_dt[, (cc) := suppressWarnings(as.numeric(get(cc)))]
  }
  # Standardize quality_grade if present
  has_quality_grade <- "quality_grade" %in% names(ev_dt)
  if (has_quality_grade) {
    ev_dt[, quality_grade := as.character(quality_grade)]
    ev_dt[, quality_grade_std := fifelse(
      stringr::str_detect(quality_grade, "[ABCDabcd]"), quality_grade, NA_character_
    )]
    ev_dt[, quality_grade_std := stringr::str_remove_all(quality_grade_std, "[+-]")]
    ev_dt[is.na(quality_grade_std), quality_grade_std := "Unknown"]
  } else {
    ev_dt[, quality_grade_std := NA_character_]
  }

  pid_year_ev <- ev_dt[, .(
    n_evictions = .N,
    n_retaliatory_same = sum(retaliatory_same, na.rm = TRUE),
    n_retaliatory_bw = sum(retaliatory_bw, na.rm = TRUE),
    tc_black_c = tc_black_c[1L],
    tc_female_c = tc_female_c[1L],
    tc_black_female_c = tc_black_female_c[1L],
    tc_cov_c = tc_cov_c[1L],
    tenant_comp_missing = tenant_comp_missing[1L],
    GEOID = GEOID[1L],
    census_tract = census_tract[1L],
    total_area = total_area[1L],
    total_units = total_units[1L],
    market_value = market_value[1L],
    building_type = building_type[1L],
    num_units_bin = num_units_bin[1L],
    year_blt_decade = year_blt_decade[1L],
    num_stories_bin = num_stories_bin[1L],
    quality_grade_std = quality_grade_std[1L]
  ), by = .(PID, year)]

  pid_year_ev[, share_retaliatory_same := n_retaliatory_same / n_evictions]
  pid_year_ev[, share_retaliatory_bw := n_retaliatory_bw / n_evictions]
  pid_year_ev[, log_total_area := suppressWarnings(log(total_area))]
  pid_year_ev[, log_market_value := suppressWarnings(log(market_value))]
  pid_year_ev[!is.finite(log_total_area), log_total_area := NA_real_]
  pid_year_ev[!is.finite(log_market_value), log_market_value := NA_real_]

  logf(
    "Building-year targeting sample: ", nrow(pid_year_ev), " PID-years; ",
    "mean share_retaliatory_same=", round(mean(pid_year_ev$share_retaliatory_same, na.rm = TRUE), 4),
    ", mean share_retaliatory_bw=", round(mean(pid_year_ev$share_retaliatory_bw, na.rm = TRUE), 4),
    log_file = log_file
  )

  bldg_year_targeting_coef_list <- list()
  bldg_year_targeting_etable_lines <- list()
  bldg_year_targeting_etable_tex_lines <- list()

  obs_rhs <- paste0(
    " + log_total_area + log_market_value"
  )
  obs_fe <- " + num_units_bin + building_type + year_blt_decade + num_stories_bin"

  for (outcome_name in c("same", "bw")) {
    y_col <- paste0("share_retaliatory_", outcome_name)
    for (s in c("full", "pre")) {
      dat <- if (s == "pre") pid_year_ev[year <= pre_covid_end] else pid_year_ev
      if (nrow(dat) < 100L) next
      key <- paste0(s, "_", outcome_name)

      # Tract FE + building observables
      fml_tract <- as.formula(paste0(
        y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
        " + tenant_comp_missing", obs_rhs,
        " | year + census_tract", obs_fe
      ))
      m_tract <- feols(fml_tract, data = dat, cluster = ~census_tract, lean = TRUE)

      # BG FE + building observables
      fml_bg <- as.formula(paste0(
        y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
        " + tenant_comp_missing", obs_rhs,
        " | year + GEOID", obs_fe
      ))
      m_bg <- feols(fml_bg, data = dat, cluster = ~GEOID, lean = TRUE)

      ct_tract <- as.data.table(summary(m_tract)$coeftable, keep.rownames = "term")
      ct_bg <- as.data.table(summary(m_bg)$coeftable, keep.rownames = "term")
      setnames(ct_tract, c("Estimate", "Std. Error", "t value", "Pr(>|t|)"),
               c("estimate", "std_error", "t_value", "p_value"))
      setnames(ct_bg, c("Estimate", "Std. Error", "t value", "Pr(>|t|)"),
               c("estimate", "std_error", "t_value", "p_value"))
      ct_tract[, `:=`(sample = s, outcome = outcome_name, fe = "tract", level = "bldg_year")]
      ct_bg[, `:=`(sample = s, outcome = outcome_name, fe = "bg", level = "bldg_year")]
      bldg_year_targeting_coef_list[[paste0(key, "_tract")]] <- ct_tract
      bldg_year_targeting_coef_list[[paste0(key, "_bg")]] <- ct_bg

      bldg_year_targeting_etable_lines[[key]] <- capture.output(
        etable(m_tract, m_bg, se.below = TRUE, digits = 4,
               headers = c("Tract FE", "BG FE"))
      )
      bldg_year_targeting_etable_tex_lines[[key]] <- capture.output(
        etable(m_tract, m_bg, se.below = TRUE, digits = 4, tex = TRUE,
               headers = c("Tract FE", "BG FE"))
      )
      logf("Fitted bldg-year targeting model: ", key, " | N_tract=", nobs(m_tract),
           ", N_bg=", nobs(m_bg), log_file = log_file)
      rm(m_tract, m_bg, dat); gc()
    }
  }

  rm(ev_dt, pid_year_ev); gc()
} else {
  logf("Skipping targeting models: GEOID/census_tract not available.", log_file = log_file)
}
coef_targeting <- rbindlist(targeting_coef_list, use.names = TRUE, fill = TRUE)
coef_bldg_year_targeting <- rbindlist(bldg_year_targeting_coef_list, use.names = TRUE, fill = TRUE)
etable_targeting_txt <- unlist(targeting_etable_lines)
etable_targeting_tex <- unlist(targeting_etable_tex_lines)
etable_bldg_year_targeting_txt <- unlist(bldg_year_targeting_etable_lines)
etable_bldg_year_targeting_tex <- unlist(bldg_year_targeting_etable_tex_lines)

# ======================================================================
# Complaint Suppression: controlling for maintenance (permits) and
# building observables, do buildings with more Black / female tenants
# file fewer complaints?
# Sample: all building-periods
# Outcome: filed_complaint (also filed_severe, filed_non_severe)
# FE: period + tract; period + BG
# ======================================================================
suppression_coef_list <- list()
suppression_etable_lines <- list()
suppression_etable_tex_lines <- list()

if (has_geoid && has_tract) {
  # Ensure permit/violation and building observable columns are numeric
  for (cc in c("total_permits", "total_violations")) {
    if (cc %in% names(dt)) {
      dt[, (cc) := suppressWarnings(as.numeric(get(cc)))]
      dt[is.na(get(cc)), (cc) := 0]
    } else {
      dt[, (cc) := 0]
    }
  }
  for (cc in c("total_area", "total_units", "market_value")) {
    if (cc %in% names(dt)) dt[, (cc) := suppressWarnings(as.numeric(get(cc)))]
  }
  if (!"log_total_area" %in% names(dt) && "total_area" %in% names(dt)) {
    dt[, log_total_area := suppressWarnings(log(total_area))]
    dt[!is.finite(log_total_area), log_total_area := NA_real_]
  }
  if (!"log_market_value" %in% names(dt) && "market_value" %in% names(dt)) {
    dt[, log_market_value := suppressWarnings(log(market_value))]
    dt[!is.finite(log_market_value), log_market_value := NA_real_]
  }

  # Building observable controls + FE (matching price-regs-eb.R pattern)
  sup_obs_rhs <- " + total_permits + total_violations + log_total_area + log_market_value"
  sup_obs_fe <- " + num_units_bin + building_type + year_blt_decade + num_stories_bin"

  suppression_outcomes <- c("complaint", "severe", "non_severe")
  for (stem in suppression_outcomes) {
    y_col <- paste0("filed_", stem)
    for (s in c("full", "pre")) {
      dat <- if (s == "pre") dt[year <= pre_covid_end] else dt
      key <- paste0(s, "_", stem)

      # Tract FE + building observables
      fml_tract <- as.formula(paste0(
        y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
        " + tenant_comp_missing", sup_obs_rhs,
        " | period_fe + census_tract", sup_obs_fe
      ))
      m_tract <- feols(fml_tract, data = dat, cluster = ~census_tract, lean = TRUE)

      # BG FE + building observables
      fml_bg <- as.formula(paste0(
        y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
        " + tenant_comp_missing", sup_obs_rhs,
        " | period_fe + GEOID", sup_obs_fe
      ))
      m_bg <- feols(fml_bg, data = dat, cluster = ~GEOID, lean = TRUE)

      ct_tract <- as.data.table(summary(m_tract)$coeftable, keep.rownames = "term")
      ct_bg <- as.data.table(summary(m_bg)$coeftable, keep.rownames = "term")
      setnames(ct_tract, c("Estimate", "Std. Error", "t value", "Pr(>|t|)"),
               c("estimate", "std_error", "t_value", "p_value"))
      setnames(ct_bg, c("Estimate", "Std. Error", "t value", "Pr(>|t|)"),
               c("estimate", "std_error", "t_value", "p_value"))
      ct_tract[, `:=`(sample = s, stem = stem, fe = "tract")]
      ct_bg[, `:=`(sample = s, stem = stem, fe = "bg")]
      suppression_coef_list[[paste0(key, "_tract")]] <- ct_tract
      suppression_coef_list[[paste0(key, "_bg")]] <- ct_bg

      suppression_etable_lines[[key]] <- capture.output(
        etable(m_tract, m_bg, se.below = TRUE, digits = 4,
               headers = c("Tract FE", "BG FE"))
      )
      suppression_etable_tex_lines[[key]] <- capture.output(
        etable(m_tract, m_bg, se.below = TRUE, digits = 4, tex = TRUE,
               headers = c("Tract FE", "BG FE"))
      )
      logf("Fitted suppression model: ", key, " | N_tract=", nobs(m_tract),
           ", N_bg=", nobs(m_bg), log_file = log_file)
      rm(m_tract, m_bg, dat); gc()
    }
  }
} else {
  logf("Skipping suppression models: GEOID/census_tract not available.", log_file = log_file)
}
coef_suppression <- rbindlist(suppression_coef_list, use.names = TRUE, fill = TRUE)
etable_suppression_txt <- unlist(suppression_etable_lines)
etable_suppression_tex <- unlist(suppression_etable_tex_lines)

bandwidth_tabs <- compute_bandwidth_tables(dt, max_bw = max_bw, time_unit = time_unit)
band_summary <- bandwidth_tabs$summary
band_status <- bandwidth_tabs$status

# Concatenate per-model etable outputs (models were not stored to save memory)
etable_dist_txt <- unlist(etable_dist_lines)
lpdid_txt <- if (skip_lpdid) {
  c(
    "Local Projections DiD Summary",
    "",
    "Skipped (--skip_lpdid=TRUE)."
  )
} else {
  out <- c(
    "Local Projections DiD Summary",
    "",
    "Window Selection:",
    capture.output(print(lpdid_windows)),
    ""
  )
  for (nm in names(models_lpdid)) {
    out <- c(
      out,
      paste0("Model: ", nm),
      capture.output(print(models_lpdid[[nm]]$coeftable)),
      ""
    )
  }
  out
}
etable_same_period_txt <- unlist(etable_same_period_lines)
etable_same_period_tex <- unlist(etable_same_period_tex_lines)

path_dist_coef <- file.path(out_dir, paste0(out_prefix, "_distlag_coefficients.csv"))
path_lpdid_coef <- file.path(out_dir, paste0(out_prefix, "_lpdid_coefficients.csv"))
path_same_period_coef <- file.path(out_dir, paste0(out_prefix, "_same_period_tenant_coefficients.csv"))
path_same_period_compare <- file.path(out_dir, paste0(out_prefix, "_same_period_tenant_main_compare.csv"))
path_lpdid_windows <- file.path(out_dir, paste0(out_prefix, "_lpdid_windows.csv"))
path_band_summary <- file.path(out_dir, paste0(out_prefix, "_bandwidth_summary.csv"))
path_band_status <- file.path(out_dir, paste0(out_prefix, "_bandwidth_status_counts.csv"))
path_dist_table <- file.path(out_dir, paste0(out_prefix, "_distlag_model_table.txt"))
path_lpdid_table <- file.path(out_dir, paste0(out_prefix, "_lpdid_model_table.txt"))
path_same_period_table <- file.path(out_dir, paste0(out_prefix, "_same_period_tenant_model_table.txt"))
path_same_period_table_tex <- file.path(out_dir, paste0(out_prefix, "_same_period_tenant_model_table.tex"))
path_dist_plot <- file.path(out_dir, paste0(out_prefix, "_distlag_coefficients.png"))
path_lpdid_plot <- file.path(out_dir, paste0(out_prefix, "_lpdid_coefficients.png"))
path_tract_coef <- file.path(out_dir, paste0(out_prefix, "_tract_fe_coefficients.csv"))
path_tract_table <- file.path(out_dir, paste0(out_prefix, "_tract_fe_model_table.txt"))
path_tract_table_tex <- file.path(out_dir, paste0(out_prefix, "_tract_fe_model_table.tex"))
path_targeting_coef <- file.path(out_dir, paste0(out_prefix, "_targeting_coefficients.csv"))
path_targeting_table <- file.path(out_dir, paste0(out_prefix, "_targeting_model_table.txt"))
path_targeting_table_tex <- file.path(out_dir, paste0(out_prefix, "_targeting_model_table.tex"))
path_bldg_year_targeting_coef <- file.path(out_dir, paste0(out_prefix, "_bldg_year_targeting_coefficients.csv"))
path_bldg_year_targeting_table <- file.path(out_dir, paste0(out_prefix, "_bldg_year_targeting_model_table.txt"))
path_bldg_year_targeting_table_tex <- file.path(out_dir, paste0(out_prefix, "_bldg_year_targeting_model_table.tex"))
path_suppression_coef <- file.path(out_dir, paste0(out_prefix, "_suppression_coefficients.csv"))
path_suppression_table <- file.path(out_dir, paste0(out_prefix, "_suppression_model_table.txt"))
path_suppression_table_tex <- file.path(out_dir, paste0(out_prefix, "_suppression_model_table.tex"))
path_qa <- file.path(out_dir, paste0(out_prefix, "_qa.txt"))

fwrite(coef_dist, path_dist_coef)
fwrite(coef_lpdid, path_lpdid_coef)
fwrite(coef_same_period, path_same_period_coef)
fwrite(coef_same_period_wide, path_same_period_compare)
fwrite(lpdid_windows, path_lpdid_windows)
fwrite(band_summary, path_band_summary)
fwrite(band_status, path_band_status)
if (nrow(coef_tract)) fwrite(coef_tract, path_tract_coef)
writeLines(etable_dist_txt, con = path_dist_table)
writeLines(lpdid_txt, con = path_lpdid_table)
writeLines(etable_same_period_txt, con = path_same_period_table)
writeLines(etable_same_period_tex, con = path_same_period_table_tex)
if (length(etable_tract_txt)) writeLines(etable_tract_txt, con = path_tract_table)
if (length(etable_tract_tex)) writeLines(etable_tract_tex, con = path_tract_table_tex)
if (nrow(coef_targeting)) fwrite(coef_targeting, path_targeting_coef)
if (length(etable_targeting_txt)) writeLines(etable_targeting_txt, con = path_targeting_table)
if (length(etable_targeting_tex)) writeLines(etable_targeting_tex, con = path_targeting_table_tex)
if (nrow(coef_bldg_year_targeting)) fwrite(coef_bldg_year_targeting, path_bldg_year_targeting_coef)
if (length(etable_bldg_year_targeting_txt)) writeLines(etable_bldg_year_targeting_txt, con = path_bldg_year_targeting_table)
if (length(etable_bldg_year_targeting_tex)) writeLines(etable_bldg_year_targeting_tex, con = path_bldg_year_targeting_table_tex)
if (nrow(coef_suppression)) fwrite(coef_suppression, path_suppression_coef)
if (length(etable_suppression_txt)) writeLines(etable_suppression_txt, con = path_suppression_table)
if (length(etable_suppression_tex)) writeLines(etable_suppression_tex, con = path_suppression_table_tex)

theme_use <- theme_minimal()

p_dist <- ggplot(coef_dist, aes(x = timing, y = estimate)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray45") +
  geom_point(size = 1.3) +
  geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.15) +
  facet_grid(stem_label ~ sample_label) +
  scale_x_continuous(breaks = seq(-h, h, by = 1)) +
  labs(
    title = paste0(period_label_title, " Distributed-Lag Estimates: Complaints and Eviction Filings"),
    subtitle = paste0("LPM with PID and ", period_label, " fixed effects; clustered by PID"),
    x = paste0(tools::toTitleCase(period_label_plural), " relative to complaint indicator"),
    y = "Coefficient"
  ) +
  theme_use

ggsave(
  filename = path_dist_plot,
  plot = p_dist,
  width = 11,
  height = 8,
  dpi = 300,
  bg = "white"
)

if (!skip_lpdid && nrow(coef_lpdid)) {
  p_lpdid <- ggplot(coef_lpdid, aes(x = timing, y = estimate)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray45") +
    geom_point(size = 1.3) +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.15) +
    facet_grid(stem_label ~ sample_label) +
    labs(
      title = paste0(period_label_title, " Local Projections DiD: Complaints and Eviction Filings"),
      subtitle = "Clustered by PID; window selected per sample/stem with fallback rule",
      x = paste0(tools::toTitleCase(period_label_plural), " relative to treatment"),
      y = "Coefficient"
    ) +
    theme_use

  ggsave(
    filename = path_lpdid_plot,
    plot = p_lpdid,
    width = 11,
    height = 8,
    dpi = 300,
    bg = "white"
  )
}

lpdid_plot_note <- if (skip_lpdid) {
  paste0("LP-DiD skipped; no LP-DiD plot written.")
} else if (!nrow(coef_lpdid)) {
  paste0("LP-DiD ran but produced no coefficient rows; no LP-DiD plot written.")
} else {
  paste0("Wrote lpdid plot: ", path_lpdid_plot)
}

lpdid_window_lines <- if (skip_lpdid) {
  "Skipped (--skip_lpdid=TRUE)"
} else {
  capture.output(print(lpdid_windows))
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
  paste0("Time unit: ", time_unit),
  paste0("skip_lpdid: ", skip_lpdid),
  paste0("Bandwidths evaluated: 1 to ", max_bw, " ", period_label_plural),
  paste0(
    "LP window rule: prefer +/-", lp_window_periods,
    " ", period_label_plural, ", fallback +/-", lp_fallback_window_periods,
    " ", period_label_plural, " when treated retention < ", lp_min_treated_retention
  ),
  "",
  "Bandwidth summary (head):",
  capture.output(print(head(band_summary, 10L))),
  "",
  "LP window summary:",
  lpdid_window_lines,
  "",
  paste0("Wrote dist-lag coefficients CSV: ", path_dist_coef),
  paste0("Wrote lpdid coefficients CSV: ", path_lpdid_coef),
  paste0("Wrote same-period tenant coefficients CSV: ", path_same_period_coef),
  paste0("Wrote same-period baseline vs tenant compare CSV: ", path_same_period_compare),
  paste0("Wrote lpdid windows CSV: ", path_lpdid_windows),
  paste0("Wrote bandwidth summary CSV: ", path_band_summary),
  paste0("Wrote bandwidth status CSV: ", path_band_status),
  paste0("Wrote dist-lag model table: ", path_dist_table),
  paste0("Wrote lpdid model table: ", path_lpdid_table),
  paste0("Wrote same-period tenant model table: ", path_same_period_table),
  paste0("Wrote same-period tenant model table (tex): ", path_same_period_table_tex),
  if (has_geoid) paste0("Wrote tract FE coefficients CSV: ", path_tract_coef) else "Tract FE models skipped (no GEOID)",
  if (has_geoid) paste0("Wrote tract FE model table: ", path_tract_table) else NULL,
  if (has_geoid) paste0("Wrote tract FE model table (tex): ", path_tract_table_tex) else NULL,
  if (nrow(coef_targeting)) paste0("Wrote targeting coefficients CSV: ", path_targeting_coef) else "Targeting models skipped",
  if (length(etable_targeting_tex)) paste0("Wrote targeting model table (tex): ", path_targeting_table_tex) else NULL,
  if (nrow(coef_suppression)) paste0("Wrote suppression coefficients CSV: ", path_suppression_coef) else "Suppression models skipped",
  if (length(etable_suppression_tex)) paste0("Wrote suppression model table (tex): ", path_suppression_table_tex) else NULL,
  paste0("Wrote dist-lag plot: ", path_dist_plot),
  lpdid_plot_note
)
writeLines(qa_lines, con = path_qa)

logf("Wrote outputs to: ", out_dir, log_file = log_file)
logf("=== Finished retaliatory-evictions.r ===", log_file = log_file)
