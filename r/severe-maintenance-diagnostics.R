## ============================================================
## severe-maintenance-diagnostics.R
## ============================================================
## Purpose: Diagnose the maintenance-complaint relationship using
## severe complaints only, before returning to demographic models.
##
## Inputs:
##   - products/building_data_rental_quarter (PID x quarter counts)
##   - products/bldg_panel_blp (PID x year controls; units, quality; rental-universe BLP panel)
##
## Outputs:
##   - output/tables/severe_maint_diag_*.tex / *.csv
##   - output/figs/severe_maint_diag_*.png
##   - output/logs/severe-maintenance-diagnostics.log
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(fixest)
  library(ggplot2)
  library(stringr)
  library(splines)
})

source("r/config.R")

parse_cli_args <- function(args) {
  out <- list()
  if (!length(args)) return(out)
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
  if (is.null(x) || !length(x)) return(as.integer(default))
  v <- suppressWarnings(as.integer(x[[1L]]))
  if (is.na(v)) as.integer(default) else v
}

to_num <- function(x, default) {
  if (is.null(x) || !length(x)) return(as.numeric(default))
  v <- suppressWarnings(as.numeric(x[[1L]]))
  if (is.na(v)) as.numeric(default) else v
}

to_bool <- function(x, default = TRUE) {
  if (is.null(x) || !length(x)) return(isTRUE(default))
  v <- tolower(trimws(as.character(x[[1L]])))
  if (v %chin% c("1", "true", "t", "yes", "y")) return(TRUE)
  if (v %chin% c("0", "false", "f", "no", "n")) return(FALSE)
  isTRUE(default)
}

center_impute <- function(x) {
  mu <- mean(x, na.rm = TRUE)
  if (!is.finite(mu)) mu <- 0
  list(val = fifelse(is.na(x), mu, x), mu = mu)
}

acquire_lockfile <- function(lock_path, log_file = NULL) {
  dir.create(dirname(lock_path), recursive = TRUE, showWarnings = FALSE)
  if (file.exists(lock_path)) {
    msg <- paste0("Lockfile exists: ", lock_path, ". Another run may still be active.")
    logf(msg, log_file = log_file)
    stop(msg)
  }
  ok <- file.create(lock_path)
  if (!isTRUE(ok)) stop("Failed to create lockfile: ", lock_path)
  info <- c(
    paste0("pid=", Sys.getpid()),
    paste0("started=", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    paste0("wd=", normalizePath(getwd(), winslash = "/", mustWork = FALSE))
  )
  writeLines(info, lock_path)
  logf("Acquired lockfile: ", lock_path, log_file = log_file)
  invisible(lock_path)
}

pad_pid <- function(x) str_pad(as.character(x), width = 9L, side = "left", pad = "0")

quarter_time_index <- function(year, qtr) as.integer(year) * 4L + as.integer(qtr) - 1L

parse_quarter_period <- function(period_chr) {
  p <- gsub("_", "-", toupper(as.character(period_chr)))
  year <- suppressWarnings(as.integer(substr(p, 1L, 4L)))
  qtr <- suppressWarnings(as.integer(str_extract(p, "(?<=Q)[1-4]$")))
  data.table(period = p, year = year, quarter = qtr)
}

time_index_to_period <- function(idx) {
  year <- idx %/% 4L
  qtr <- idx %% 4L + 1L
  data.table(time_index = idx, year = as.integer(year), quarter = as.integer(qtr), period = paste0(year, "-Q", qtr))
}

read_quarter_panel <- function(panel_path, start_year, end_year, pid_filter = NULL, log_file = NULL) {
  if (!requireNamespace("arrow", quietly = TRUE)) stop("Package 'arrow' is required to read parquet quarterly panel.")
  need <- c(
    "parcel_number", "period",
    "total_severe_complaints",
    "building_permit_count", "electrical_permit_count", "plumbing_permit_count",
    "mechanical_permit_count", "fire_suppression_permit_count"
  )
  dt <- as.data.table(arrow::read_parquet(panel_path, col_select = need))
  setnames(dt, "parcel_number", "PID")
  dt[, PID := pad_pid(PID)]
  pp <- parse_quarter_period(dt$period)
  dt[, c("period", "year", "quarter") := .(pp$period, pp$year, pp$quarter)]
  dt <- dt[!is.na(PID) & !is.na(year) & !is.na(quarter)]
  dt <- dt[year >= start_year & year <= end_year]
  if (!is.null(pid_filter) && length(pid_filter)) dt <- dt[PID %chin% pid_filter]
  for (cc in setdiff(names(dt), c("PID", "period", "year", "quarter"))) {
    dt[, (cc) := suppressWarnings(as.numeric(get(cc)))]
    dt[!is.finite(get(cc)), (cc) := 0]
  }
  assert_unique(dt, c("PID", "period"), "quarter panel (PID-period)")
  logf("Quarter panel rows read: ", nrow(dt), "; PIDs=", uniqueN(dt$PID), log_file = log_file)
  dt
}

build_balanced_quarter_panel <- function(dt, log_file = NULL) {
  assert_unique(dt, c("PID", "period"), "quarter panel pre-balance")
  dt[, time_index := quarter_time_index(year, quarter)]
  setorder(dt, PID, time_index)
  pid_rng <- dt[, .(t_min = min(time_index), t_max = max(time_index)), by = PID]
  grid <- pid_rng[, .(time_index = seq.int(t_min, t_max)), by = PID]
  ti <- time_index_to_period(grid$time_index)
  grid[, c("year", "quarter", "period") := .(ti$year, ti$quarter, ti$period)]
  assert_unique(grid, c("PID", "period"), "balanced quarter grid")

  n_before <- nrow(dt)
  setkey(dt, PID, period)
  setkey(grid, PID, period)
  out <- dt[grid]
  fill_cols <- setdiff(names(out), c("PID", "period", "year", "quarter", "time_index"))
  for (cc in fill_cols) if (is.numeric(out[[cc]])) out[is.na(get(cc)), (cc) := 0]
  assert_unique(out, c("PID", "period"), "quarter panel post-balance")
  logf("Balanced quarter panel rows: ", nrow(out), " (inserted ", nrow(out) - n_before, " rows)", log_file = log_file)
  out
}

read_annual_controls <- function(analytic_path, start_year, end_year, pid_filter = NULL, include_rent = TRUE, log_file = NULL) {
  hdr <- names(fread(analytic_path, nrows = 0L))
  need <- c(
    "PID", "year", "total_units", "quality_grade",
    "building_type", "year_blt_decade", "total_area", "market_value"
  )
  if (isTRUE(include_rent)) need <- c(need, "log_med_rent")
  keep <- intersect(need, hdr)
  dt <- fread(analytic_path, select = keep)
  dt[, PID := pad_pid(PID)]
  dt[, year := suppressWarnings(as.integer(year))]
  dt <- dt[!is.na(PID) & !is.na(year) & year >= start_year & year <= end_year]
  if (!is.null(pid_filter) && length(pid_filter)) dt <- dt[PID %chin% pid_filter]
  dt <- unique(dt, by = c("PID", "year"))
  assert_unique(dt, c("PID", "year"), "annual controls (PID-year)")
  logf("Annual controls rows read: ", nrow(dt), "; PIDs=", uniqueN(dt$PID), log_file = log_file)
  dt
}

attach_annual_controls <- function(q_dt, ctrl_dt, log_file = NULL) {
  assert_unique(q_dt, c("PID", "period"), "quarter panel pre-controls merge")
  assert_unique(ctrl_dt, c("PID", "year"), "annual controls pre-merge")
  setkey(q_dt, PID, year)
  setkey(ctrl_dt, PID, year)
  out <- ctrl_dt[q_dt]
  assert_unique(out, c("PID", "period"), "quarter panel post-controls merge")
  out[, controls_matched := as.integer(!is.na(total_units) | !is.na(quality_grade))]
  logf("Annual-controls merge match share=", round(mean(out$controls_matched, na.rm = TRUE), 4), log_file = log_file)
  out
}

ewma_vec <- function(x, lambda = 0.9) {
  x <- as.numeric(x)
  n <- length(x)
  out <- numeric(n)
  if (!n) return(out)
  out[1L] <- ifelse(is.na(x[1L]), 0, x[1L])
  if (n >= 2L) {
    for (i in 2:n) out[i] <- ifelse(is.na(x[i]), 0, x[i]) + lambda * out[i - 1L]
  }
  out
}

add_ns_basis_cols <- function(dt, x_col, prefix, df = 4L) {
  x <- as.numeric(dt[[x_col]])
  ok <- is.finite(x)
  cols <- paste0(prefix, "_", seq_len(df))
  for (cc in cols) dt[, (cc) := 0]
  if (!any(ok)) return(cols)
  x_ok <- x[ok]
  b <- tryCatch(splines::ns(x_ok, df = df), error = function(e) NULL)
  if (is.null(b)) {
    mu <- mean(x_ok, na.rm = TRUE)
    sdv <- sd(x_ok, na.rm = TRUE)
    if (!is.finite(sdv) || sdv <= 0) sdv <- 1
    dt[ok, (cols[1L]) := (x_ok - mu) / sdv]
    return(cols)
  }
  tmp <- as.data.table(b)
  k <- min(ncol(tmp), length(cols))
  setnames(tmp, names(tmp)[seq_len(k)], cols[seq_len(k)])
  if (k < length(cols)) for (j in seq.int(k + 1L, length(cols))) tmp[, (cols[j]) := 0]
  tmp <- tmp[, cols, with = FALSE]
  dt[ok, (cols) := tmp]
  cols
}

spline_term_block <- function(cols) paste(cols, collapse = " + ")

extract_coef_table <- function(models, outcome_label) {
  out <- list()
  for (nm in names(models)) {
    m <- models[[nm]]
    if (is.null(m) || !inherits(m, "fixest")) next
    ct <- as.data.table(summary(m)$coeftable, keep.rownames = "term")
    if (!nrow(ct)) next
    setnames(ct, c("Estimate", "Std. Error"), c("estimate", "std_error"), skip_absent = TRUE)
    if ("Pr(>|t|)" %in% names(ct)) setnames(ct, "Pr(>|t|)", "p_value")
    if ("Pr(>|z|)" %in% names(ct)) setnames(ct, "Pr(>|z|)", "p_value")
    if (!"p_value" %in% names(ct)) ct[, p_value := NA_real_]
    ct[, `:=`(outcome_family = outcome_label, model = nm)]
    out[[nm]] <- ct[, .(outcome_family, model, term, estimate, std_error, p_value)]
  }
  rbindlist(out, use.names = TRUE, fill = TRUE)
}

make_fine_unit_bins <- function(total_units_obs) {
  x <- suppressWarnings(as.numeric(total_units_obs))
  out <- rep("Unknown", length(x))
  ok <- is.finite(x) & x > 0
  if (!any(ok)) return(as.character(out))
  cuts <- c(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 30, 40, 50, 75, 100, 150, 200, Inf)
  labs <- c(
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
    "11-12", "13-15", "16-20", "21-30", "31-40", "41-50",
    "51-75", "76-100", "101-150", "151-200", "201+"
  )
  out[ok] <- as.character(cut(x[ok], breaks = cuts, labels = labs, include.lowest = TRUE, right = TRUE, ordered_result = FALSE))
  out[is.na(out) | !nzchar(out)] <- "Unknown"
  out
}

collapse_quality_grade_abcde <- function(x) {
  x <- toupper(trimws(as.character(x)))
  x <- substr(x, 1L, 1L)
  x[!(x %chin% c("A", "B", "C", "D", "E"))] <- "Other"
  x[is.na(x) | !nzchar(x)] <- "Other"
  x
}

plot_partial_corr_bins <- function(dt, x_cols, x_labels, out_path) {
  build_one <- function(x_col, x_lab) {
    keep <- dt[
      is.finite(total_severe_complaints) &
        is.finite(get(x_col)) &
        is.finite(total_units_ann) & total_units_ann > 0 &
        !is.na(quality_grade) & nzchar(quality_grade)
    ]
    if (!nrow(keep)) return(data.table())
    unit_cols <- grep("^units_ns_", names(keep), value = TRUE)
    if (!length(unit_cols)) return(data.table())
    ctrl_rhs <- spline_term_block(unit_cols)
    wt <- ~total_units_ann
    y_mod <- feols(as.formula(paste0("total_severe_complaints ~ ", ctrl_rhs, " | period_fe + quality_grade_abcde")), data = keep, weights = wt)
    x_mod <- feols(as.formula(paste0(x_col, " ~ ", ctrl_rhs, " | period_fe + quality_grade_abcde")), data = keep, weights = wt)
    keep[, y_resid := resid(y_mod)]
    keep[, x_resid := resid(x_mod)]
    keep <- keep[is.finite(x_resid) & is.finite(y_resid)]
    if (!nrow(keep)) return(data.table())
    brks <- unique(as.numeric(quantile(keep$x_resid, probs = seq(0, 1, length.out = 26L), na.rm = TRUE, names = FALSE)))
    if (length(brks) < 3L) return(data.table())
    keep[, bin := cut(x_resid, breaks = brks, include.lowest = TRUE)]
    keep[!is.na(bin), .(
      x_bin = weighted.mean(x_resid, w = pmax(total_units_ann, 1), na.rm = TRUE),
      y_bin = weighted.mean(y_resid, w = pmax(total_units_ann, 1), na.rm = TRUE),
      weight_sum = sum(pmax(total_units_ann, 1), na.rm = TRUE)
    ), by = bin][, signal := x_lab]
  }
  pts <- rbindlist(Map(build_one, x_cols, x_labels), use.names = TRUE, fill = TRUE)
  if (!nrow(pts)) return(invisible(NULL))
  p <- ggplot(pts, aes(x = x_bin, y = y_bin)) +
    geom_hline(yintercept = 0, linetype = 2, color = "grey70") +
    geom_vline(xintercept = 0, linetype = 2, color = "grey85") +
    geom_point(aes(size = weight_sum), alpha = 0.8, color = "#2C7FB8") +
    geom_smooth(method = "loess", se = FALSE, color = "#D95F0E", linewidth = 0.8, span = 0.9) +
    facet_wrap(~ signal, scales = "free_x") +
    labs(
      x = "Residualized maintenance signal",
      y = "Residualized severe complaints",
      size = "Units in bin",
      title = "Severe complaints vs maintenance (partial-correlation diagnostics)",
      subtitle = "Residualized on nonlinear unit controls with period and quality-grade fixed effects"
    ) +
    theme_minimal(base_size = 11)
  ggsave(out_path, p, width = 11, height = 5.7, dpi = 300, bg = "white")
}

make_panel <- function(cfg, start_year, end_year, sample_n_pid = 0L, lambda = 0.9, include_rent = TRUE, log_file = NULL) {
  controls_path <- p_product(cfg, "bldg_panel_blp")
  quarter_path <- p_product(cfg, "building_data_rental_quarter")

  pid_dt <- fread(controls_path, select = "PID")
  pid_dt[, PID := pad_pid(PID)]
  pid_dt <- unique(pid_dt[!is.na(PID) & nzchar(PID)], by = "PID")
  assert_unique(pid_dt, "PID", "bldg_panel_blp PID universe")
  if (sample_n_pid > 0L && sample_n_pid < nrow(pid_dt)) {
    set.seed(123)
    pid_dt <- pid_dt[sample(.N, sample_n_pid)]
    logf("Sample mode: selected ", nrow(pid_dt), " PIDs from bldg_panel_blp universe.", log_file = log_file)
  }

  q_dt <- read_quarter_panel(quarter_path, start_year, end_year, pid_filter = pid_dt$PID, log_file = log_file)
  q_dt <- build_balanced_quarter_panel(q_dt, log_file = log_file)
  c_dt <- read_annual_controls(controls_path, start_year, end_year, pid_filter = pid_dt$PID, include_rent = include_rent, log_file = log_file)
  dt <- attach_annual_controls(q_dt, c_dt, log_file = log_file)

  dt[, total_units_obs := suppressWarnings(as.numeric(total_units))]
  dt[, total_units_ann := total_units_obs]
  dt[!is.finite(total_units_ann) | total_units_ann <= 0, total_units_ann := 1]
  dt[, log1p_total_units_ann := log1p(total_units_ann)]
  dt[, quality_grade := as.character(quality_grade)]
  dt[is.na(quality_grade) | !nzchar(quality_grade), quality_grade := "Unknown"]
  dt[, quality_grade_abcde := collapse_quality_grade_abcde(quality_grade)]
  for (cc in c("building_type", "year_blt_decade")) {
    if (!(cc %in% names(dt))) dt[, (cc) := "Unknown"]
    dt[, (cc) := as.character(get(cc))]
    dt[is.na(get(cc)) | !nzchar(get(cc)), (cc) := "Unknown"]
  }
  if (!("log_med_rent" %in% names(dt))) dt[, log_med_rent := NA_real_]
  dt[, log_total_area_raw := fifelse(is.finite(as.numeric(total_area)) & as.numeric(total_area) > 0, log(as.numeric(total_area)), NA_real_)]
  dt[, log_market_value_raw := fifelse(is.finite(as.numeric(market_value)) & as.numeric(market_value) > 0, log(as.numeric(market_value)), NA_real_)]
  dt[, log_med_rent_raw := suppressWarnings(as.numeric(log_med_rent))]
  dt[, total_area_missing := as.integer(is.na(log_total_area_raw))]
  dt[, market_value_missing := as.integer(is.na(log_market_value_raw))]
  imp_area <- center_impute(dt$log_total_area_raw)
  imp_mkt <- center_impute(dt$log_market_value_raw)
  dt[, log_total_area_imp := as.numeric(imp_area$val)]
  dt[, log_market_value_imp := as.numeric(imp_mkt$val)]
  dt[, period_fe := as.factor(period)]
  dt[, unit_bin_fine := make_fine_unit_bins(total_units_obs)]
  dt[is.na(unit_bin_fine) | !nzchar(unit_bin_fine), unit_bin_fine := "Unknown"]

  permit_repair_cols <- c("electrical_permit_count", "plumbing_permit_count", "mechanical_permit_count", "fire_suppression_permit_count")
  permit_broad_cols <- c(permit_repair_cols, "building_permit_count")
  for (cc in unique(c(permit_repair_cols, permit_broad_cols, "total_severe_complaints"))) {
    if (!(cc %in% names(dt))) dt[, (cc) := 0]
    dt[, (cc) := suppressWarnings(as.numeric(get(cc)))]
    dt[!is.finite(get(cc)), (cc) := 0]
  }

  dt[, maint_flow_repair := rowSums(.SD, na.rm = TRUE), .SDcols = permit_repair_cols]
  dt[, maint_flow_broad := rowSums(.SD, na.rm = TRUE), .SDcols = permit_broad_cols]
  dt[, `:=`(
    maint_rate_curr = maint_flow_repair / pmax(total_units_ann, 1),
    maint_rate_curr_broad = maint_flow_broad / pmax(total_units_ann, 1)
  )]

  setorder(dt, PID, year, quarter)
  dt[, maint_flow_repair_l1 := shift(maint_flow_repair, 1L, type = "lag", fill = 0), by = PID]
  dt[, maint_flow_repair_l2 := shift(maint_flow_repair, 2L, type = "lag", fill = 0), by = PID]
  dt[, maint_flow_repair_l3 := shift(maint_flow_repair, 3L, type = "lag", fill = 0), by = PID]
  dt[, maint_flow_repair_l4 := shift(maint_flow_repair, 4L, type = "lag", fill = 0), by = PID]
  dt[, maint_flow_repair_f1 := shift(maint_flow_repair, 1L, type = "lead", fill = 0), by = PID]
  dt[, maint_flow_repair_f2 := shift(maint_flow_repair, 2L, type = "lead", fill = 0), by = PID]
  dt[, maint_rate_l1 := maint_flow_repair_l1 / pmax(total_units_ann, 1)]
  dt[, maint_rate_l1_4_mean := (maint_flow_repair_l1 + maint_flow_repair_l2 + maint_flow_repair_l3 + maint_flow_repair_l4) / 4 / pmax(total_units_ann, 1)]
  dt[, maint_rate_f1 := maint_flow_repair_f1 / pmax(total_units_ann, 1)]
  dt[, maint_rate_f2 := maint_flow_repair_f2 / pmax(total_units_ann, 1)]
  dt[, maint_stock_lag := ewma_vec(maint_flow_repair_l1, lambda = lambda), by = PID]
  dt[, maint_stock_lag_rate := maint_stock_lag / pmax(total_units_ann, 1)]

  unit_cols <- add_ns_basis_cols(dt, "log1p_total_units_ann", prefix = "units_ns", df = 4L)
  attr(dt, "unit_spline_cols") <- unit_cols
  attr(dt, "lambda") <- lambda

  logf(
    "Panel ready: rows=", nrow(dt), ", PIDs=", uniqueN(dt$PID),
    ", severe complaints mean=", round(mean(dt$total_severe_complaints, na.rm = TRUE), 4),
    ", repair maint_rate q50/q90/q99=", paste(round(quantile(dt$maint_rate_curr, c(.5, .9, .99), na.rm = TRUE), 4), collapse = "/"),
    ", controls matched=", round(mean(dt$controls_matched == 1L, na.rm = TRUE), 4),
    ", log_total_area non-missing=", round(mean(is.finite(dt$log_total_area_raw), na.rm = TRUE), 4),
    ", log_market_value non-missing=", round(mean(is.finite(dt$log_market_value_raw), na.rm = TRUE), 4),
    ", log_med_rent non-missing=", round(mean(is.finite(dt$log_med_rent_raw), na.rm = TRUE), 4),
    log_file = log_file
  )
  setkey(dt, PID, period)
  dt
}

fit_models <- function(dt, log_file = NULL, only = NULL) {
  want <- function(name) is.null(only) || (name %chin% only)
  fit_one <- function(name, expr) {
    t0 <- proc.time()[3]
    logf("[fit] START ", name, log_file = log_file)
    res <- tryCatch(
      expr,
      error = function(e) {
        logf("[fit] ERROR ", name, ": ", conditionMessage(e), log_file = log_file)
        NULL
      }
    )
    dt_sec <- round(proc.time()[3] - t0, 2)
    if (inherits(res, "fixest")) {
      logf("[fit] DONE ", name, " in ", dt_sec, "s", log_file = log_file)
    } else if (is.null(res)) {
      logf("[fit] NULL ", name, " after ", dt_sec, "s", log_file = log_file)
    }
    res
  }
  dtm <- dt[
    is.finite(total_severe_complaints) &
      is.finite(total_units_ann) & total_units_ann > 0 &
      !is.na(quality_grade_abcde) & nzchar(quality_grade_abcde)
  ]
  unit_cols <- grep("^units_ns_", names(dtm), value = TRUE)
  if (!length(unit_cols)) stop("Missing unit spline controls.")
  unit_ctrl <- spline_term_block(unit_cols)
  wt <- ~total_units_ann

  fe_period <- "period_fe"
  fe_q <- "period_fe + quality_grade_abcde"
  fe_q_fine <- "period_fe + quality_grade_abcde + unit_bin_fine"
  fe_q5_fine <- "period_fe + quality_grade_abcde + unit_bin_fine"
  fe_q_rich <- "period_fe + quality_grade_abcde + unit_bin_fine + year_blt_decade"
  dtm_match <- dtm[controls_matched == 1L & unit_bin_fine != "Unknown"]
  dtm_match_rent <- dtm_match[is.finite(log_med_rent_raw)]
  fe_pid <- "PID + period_fe"
  fe_q5_fine_rich <- "period_fe + quality_grade_abcde + unit_bin_fine + building_type + year_blt_decade"
  rich_cont <- "log_total_area_imp + total_area_missing"

  m_curr_noq <- if (want("m_curr_noq")) fit_one("m_curr_noq", fepois(
    as.formula(paste0("total_severe_complaints ~ maint_rate_curr + ", unit_ctrl, " | ", fe_period)),
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_curr_q <- if (want("m_curr_q")) fit_one("m_curr_q", fepois(
    as.formula(paste0("total_severe_complaints ~ maint_rate_curr + ", unit_ctrl, " | ", fe_q)),
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_l1_q <- if (want("m_l1_q")) fit_one("m_l1_q", fepois(
    as.formula(paste0("total_severe_complaints ~ maint_rate_l1 + ", unit_ctrl, " | ", fe_q)),
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_l1_4_q <- if (want("m_l1_4_q")) fit_one("m_l1_4_q", fepois(
    as.formula(paste0("total_severe_complaints ~ maint_rate_l1_4_mean + ", unit_ctrl, " | ", fe_q)),
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_stock_lag_q <- if (want("m_stock_lag_q")) fit_one("m_stock_lag_q", fepois(
    as.formula(paste0("total_severe_complaints ~ maint_stock_lag_rate + ", unit_ctrl, " | ", fe_q)),
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_curr_stock_lag_q <- if (want("m_curr_stock_lag_q")) fit_one("m_curr_stock_lag_q", fepois(
    as.formula(paste0("total_severe_complaints ~ maint_rate_curr + maint_stock_lag_rate + ", unit_ctrl, " | ", fe_q)),
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_stock_lag_q_fine <- if (want("m_stock_lag_q_fine")) fit_one("m_stock_lag_q_fine", fepois(
    as.formula(paste0("total_severe_complaints ~ maint_stock_lag_rate + ", unit_ctrl, " | ", fe_q_fine)),
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_stock_lag_q5_fine <- if (want("m_stock_lag_q5_fine") && nrow(dtm_match)) fit_one("m_stock_lag_q5_fine", fepois(
    total_severe_complaints ~ maint_stock_lag_rate | period_fe + quality_grade_abcde + unit_bin_fine,
    data = dtm_match, weights = wt, cluster = ~PID
  )) else NULL
  m_stock_lag_q_rich_adjcurr <- if (want("m_stock_lag_q_rich_adjcurr")) fit_one("m_stock_lag_q_rich_adjcurr", fepois(
    total_severe_complaints ~ maint_stock_lag_rate + log_total_area_raw |
      period_fe + year_blt_decade + quality_grade_abcde + unit_bin_fine,
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_stock_lag_q5_fine_rich_adjcurr <- if (want("m_stock_lag_q5_fine_rich_adjcurr") && !isTRUE(getOption("sev_maint_fast_mode", FALSE)) && nrow(dtm_match)) fit_one("m_stock_lag_q5_fine_rich_adjcurr", fepois(
    as.formula(paste0(
      "total_severe_complaints ~ maint_stock_lag_rate + maint_rate_curr + ",
      rich_cont, " | ", fe_q5_fine_rich
    )),
    data = dtm_match, weights = wt, cluster = ~PID
  )) else NULL
  m_stock_lag_q5_int_fine <- if (want("m_stock_lag_q5_int_fine") && !isTRUE(getOption("sev_maint_fast_mode", FALSE)) && nrow(dtm_match)) fit_one("m_stock_lag_q5_int_fine", fepois(
    total_severe_complaints ~ maint_stock_lag_rate + i(quality_grade_abcde, maint_stock_lag_rate, ref = "C") |
      period_fe + quality_grade_abcde + unit_bin_fine,
    data = dtm_match, weights = wt, cluster = ~PID
  )) else NULL
  m_stock_lag_q5_int_fine_adjcurr <- if (want("m_stock_lag_q5_int_fine_adjcurr") && !isTRUE(getOption("sev_maint_fast_mode", FALSE)) && nrow(dtm_match)) fit_one("m_stock_lag_q5_int_fine_adjcurr", fepois(
    total_severe_complaints ~ maint_stock_lag_rate + maint_rate_curr +
      i(quality_grade_abcde, maint_stock_lag_rate, ref = "C") |
      period_fe + quality_grade_abcde + unit_bin_fine,
    data = dtm_match, weights = wt, cluster = ~PID
  )) else NULL
  m_stock_lag_q5_int_fine_adjl1 <- if (want("m_stock_lag_q5_int_fine_adjl1") && !isTRUE(getOption("sev_maint_fast_mode", FALSE)) && nrow(dtm_match)) fit_one("m_stock_lag_q5_int_fine_adjl1", fepois(
    total_severe_complaints ~ maint_stock_lag_rate + maint_rate_l1 +
      i(quality_grade_abcde, maint_stock_lag_rate, ref = "C") |
      period_fe + quality_grade_abcde + unit_bin_fine,
    data = dtm_match, weights = wt, cluster = ~PID
  )) else NULL
  m_stock_lag_q5_int_fine_rich <- if (want("m_stock_lag_q5_int_fine_rich") && !isTRUE(getOption("sev_maint_fast_mode", FALSE)) && nrow(dtm_match)) fit_one("m_stock_lag_q5_int_fine_rich", fepois(
    as.formula(paste0(
      "total_severe_complaints ~ maint_stock_lag_rate + i(quality_grade_abcde, maint_stock_lag_rate, ref = 'C') + ",
      rich_cont, " | ", fe_q5_fine_rich
    )),
    data = dtm_match, weights = wt, cluster = ~PID
  )) else NULL
  m_stock_lag_q5_int_fine_rich_adjcurr <- if (want("m_stock_lag_q5_int_fine_rich_adjcurr") && !isTRUE(getOption("sev_maint_fast_mode", FALSE)) && nrow(dtm_match)) fit_one("m_stock_lag_q5_int_fine_rich_adjcurr", fepois(
    as.formula(paste0(
      "total_severe_complaints ~ maint_stock_lag_rate + maint_rate_curr + i(quality_grade_abcde, maint_stock_lag_rate, ref = 'C') + ",
      rich_cont, " | ", fe_q5_fine_rich
    )),
    data = dtm_match, weights = wt, cluster = ~PID
  )) else NULL
  m_stock_lag_q5_int_fine_rich_rent <- if (want("m_stock_lag_q5_int_fine_rich_rent") && !isTRUE(getOption("sev_maint_fast_mode", FALSE)) && nrow(dtm_match_rent)) fit_one("m_stock_lag_q5_int_fine_rich_rent", fepois(
    as.formula(paste0(
      "total_severe_complaints ~ maint_stock_lag_rate + i(quality_grade_abcde, maint_stock_lag_rate, ref = 'C') + ",
      rich_cont, " + log_med_rent_raw | ", fe_q5_fine_rich
    )),
    data = dtm_match_rent, weights = wt, cluster = ~PID
  )) else NULL
  m_f1_q <- if (want("m_f1_q")) fit_one("m_f1_q", fepois(
    as.formula(paste0("total_severe_complaints ~ maint_rate_f1 + ", unit_ctrl, " | ", fe_q)),
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_f2_q <- if (want("m_f2_q")) fit_one("m_f2_q", fepois(
    as.formula(paste0("total_severe_complaints ~ maint_rate_f2 + ", unit_ctrl, " | ", fe_q)),
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_l1_pid <- if (want("m_l1_pid") && !isTRUE(getOption("sev_maint_fast_mode", FALSE))) fit_one("m_l1_pid", fepois(
    total_severe_complaints ~ maint_rate_l1 | PID + period_fe,
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_stock_lag_pid <- if (want("m_stock_lag_pid") && !isTRUE(getOption("sev_maint_fast_mode", FALSE))) fit_one("m_stock_lag_pid", fepois(
    total_severe_complaints ~ maint_stock_lag_rate | PID + period_fe,
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_f1_pid <- if (want("m_f1_pid") && !isTRUE(getOption("sev_maint_fast_mode", FALSE))) fit_one("m_f1_pid", fepois(
    total_severe_complaints ~ maint_rate_f1 | PID + period_fe,
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_f2_pid <- if (want("m_f2_pid") && !isTRUE(getOption("sev_maint_fast_mode", FALSE))) fit_one("m_f2_pid", fepois(
    total_severe_complaints ~ maint_rate_f2 | PID + period_fe,
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL
  m_l1_q_broad <- if (want("m_curr_broad_q")) fit_one("m_curr_broad_q", fepois(
    as.formula(paste0("total_severe_complaints ~ maint_rate_curr_broad + ", unit_ctrl, " | ", fe_q)),
    data = dtm, weights = wt, cluster = ~PID
  )) else NULL

  sample_diag <- if (is.null(only) || ("sample_diag" %chin% only)) data.table(
    n_obs = nrow(dtm),
    n_pid = uniqueN(dtm$PID),
    n_obs_controls_matched = nrow(dtm_match),
    n_pid_controls_matched = uniqueN(dtm_match$PID),
    n_obs_controls_matched_rent = nrow(dtm_match_rent),
    n_pid_controls_matched_rent = uniqueN(dtm_match_rent$PID),
    share_nonzero_severe = mean(dtm$total_severe_complaints > 0, na.rm = TRUE),
    share_zero_curr_maint = mean(dtm$maint_rate_curr == 0, na.rm = TRUE),
    share_zero_l1_maint = mean(dtm$maint_rate_l1 == 0, na.rm = TRUE),
    share_nonmiss_log_total_area_match = mean(is.finite(dtm_match$log_total_area_raw)),
    share_nonmiss_log_market_value_match = mean(is.finite(dtm_match$log_market_value_raw)),
    share_nonmiss_log_med_rent_match = mean(is.finite(dtm_match$log_med_rent_raw))
  ) else NULL

  list(
    m_curr_noq = m_curr_noq,
    m_curr_q = m_curr_q,
    m_l1_q = m_l1_q,
    m_l1_4_q = m_l1_4_q,
    m_stock_lag_q = m_stock_lag_q,
    m_curr_stock_lag_q = m_curr_stock_lag_q,
    m_stock_lag_q_fine = m_stock_lag_q_fine,
    m_stock_lag_q5_fine = m_stock_lag_q5_fine,
    m_stock_lag_q_rich_adjcurr = m_stock_lag_q_rich_adjcurr,
    m_stock_lag_q5_fine_rich_adjcurr = m_stock_lag_q5_fine_rich_adjcurr,
    m_stock_lag_q5_int_fine = m_stock_lag_q5_int_fine,
    m_stock_lag_q5_int_fine_adjcurr = m_stock_lag_q5_int_fine_adjcurr,
    m_stock_lag_q5_int_fine_adjl1 = m_stock_lag_q5_int_fine_adjl1,
    m_stock_lag_q5_int_fine_rich = m_stock_lag_q5_int_fine_rich,
    m_stock_lag_q5_int_fine_rich_adjcurr = m_stock_lag_q5_int_fine_rich_adjcurr,
    m_stock_lag_q5_int_fine_rich_rent = m_stock_lag_q5_int_fine_rich_rent,
    m_f1_q = m_f1_q,
    m_f2_q = m_f2_q,
    m_l1_pid = m_l1_pid,
    m_stock_lag_pid = m_stock_lag_pid,
    m_f1_pid = m_f1_pid,
    m_f2_pid = m_f2_pid,
    m_curr_broad_q = m_l1_q_broad,
    sample_diag = sample_diag
  )
}

etable_write <- function(path, ..., title) {
  mods <- list(...)
  mods <- Filter(function(m) inherits(m, "fixest"), mods)
  if (!length(mods)) {
    warning("No estimable models for table: ", basename(path))
    return(invisible(NULL))
  }
  ok <- FALSE
  try({
    do.call(fixest::etable, c(mods, list(tex = TRUE, title = title, file = path, replace = TRUE)))
    ok <- file.exists(path)
  }, silent = TRUE)
  if (!ok) {
    tex_txt <- do.call(fixest::etable, c(mods, list(tex = TRUE, title = title)))
    writeLines(tex_txt, path)
    rm(tex_txt)
  }
  invisible(gc())
  invisible(path)
}

write_fast_placeholder_table <- function(path, message, title = "Fast-mode placeholder") {
  lines <- c(
    "\\begingroup",
    "\\centering",
    "\\begin{tabular}{p{0.9\\textwidth}}",
    "\\toprule",
    paste0("\\textbf{", title, "}\\\\"),
    "\\midrule",
    paste0(message, "\\\\"),
    "\\bottomrule",
    "\\end{tabular}",
    "\\par\\endgroup"
  )
  writeLines(lines, path)
  invisible(path)
}

build_support_tables <- function(dt) {
  dtm <- dt[
    is.finite(total_severe_complaints) &
      is.finite(total_units_ann) & total_units_ann > 0 &
      !is.na(quality_grade_abcde) & nzchar(quality_grade_abcde)
  ]
  by_q <- dtm[, .(
    n_obs = .N,
    n_pid = uniqueN(PID),
    controls_match_share = mean(controls_matched == 1L, na.rm = TRUE),
    severe_pos_share = mean(total_severe_complaints > 0, na.rm = TRUE),
    stock_pos_share = mean(maint_stock_lag_rate > 0, na.rm = TRUE),
    stock_p90 = quantile(maint_stock_lag_rate, 0.9, na.rm = TRUE),
    stock_p99 = quantile(maint_stock_lag_rate, 0.99, na.rm = TRUE),
    units_weight_sum = sum(total_units_ann, na.rm = TRUE)
  ), by = quality_grade_abcde][order(quality_grade_abcde)]

  by_q_bin <- dtm[controls_matched == 1L, .(
    n_obs = .N,
    n_pid = uniqueN(PID),
    severe_pos_share = mean(total_severe_complaints > 0, na.rm = TRUE),
    stock_pos_share = mean(maint_stock_lag_rate > 0, na.rm = TRUE),
    stock_p90 = quantile(maint_stock_lag_rate, 0.9, na.rm = TRUE),
    units_weight_sum = sum(total_units_ann, na.rm = TRUE)
  ), by = .(quality_grade_abcde, unit_bin_fine)][order(quality_grade_abcde, unit_bin_fine)]

  thin <- by_q_bin[, .(
    n_cells = as.integer(.N),
    cells_lt50 = as.integer(sum(n_obs < 50)),
    cells_lt200 = as.integer(sum(n_obs < 200)),
    cells_zero_stock = as.integer(sum(stock_pos_share == 0)),
    median_n = as.numeric(median(n_obs)),
    min_n = as.numeric(min(n_obs)),
    max_n = as.numeric(max(n_obs))
  ), by = quality_grade_abcde][order(quality_grade_abcde)]

  list(by_quality = by_q, by_quality_unitbin = by_q_bin, thin_cells = thin)
}

# ---------------- Main ----------------
opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
start_year <- to_int(opts$start_year, 2014L)
end_year <- to_int(opts$end_year, 2019L)
sample_n_pid <- to_int(opts$sample_n_pid, 0L)
lambda <- to_num(opts$lambda, 0.9)
fast_mode <- to_bool(opts$fast_mode, TRUE)

cfg <- read_config()
log_file <- p_out(cfg, "logs", "severe-maintenance-diagnostics.log")
tab_dir <- p_out(cfg, "tables")
fig_dir <- p_out(cfg, "figs")
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
lock_path <- p_out(cfg, "logs", "severe-maintenance-diagnostics.lock")

logf("=== Starting severe-maintenance-diagnostics.R ===", log_file = log_file)
logf("Years: ", start_year, "-", end_year, "; sample_n_pid=", sample_n_pid, "; lambda=", lambda, "; fast_mode=", fast_mode, log_file = log_file)
if (fast_mode) logf("Fast mode enabled: skipping the rich fine-bin stock-control model, stock-quality interaction variants, rich interaction/rent variants, PID FE specs, and the partial-correlation figure.", log_file = log_file)
acquire_lockfile(lock_path, log_file = log_file)
on.exit({
  if (file.exists(lock_path)) unlink(lock_path)
  logf("Released lockfile: ", lock_path, log_file = log_file)
}, add = TRUE)

dt <- make_panel(
  cfg, start_year, end_year,
  sample_n_pid = sample_n_pid,
  lambda = lambda,
  include_rent = !fast_mode,
  log_file = log_file
)
fixest::setFixest_etable(digits = 4, fitstat = c("n", "r2"))
options(sev_maint_fast_mode = fast_mode)
on.exit(options(sev_maint_fast_mode = NULL), add = TRUE)
support_tbls <- build_support_tables(dt)
fwrite(support_tbls$by_quality, file.path(tab_dir, "severe_maint_diag_support_by_quality.csv"))
fwrite(support_tbls$by_quality_unitbin, file.path(tab_dir, "severe_maint_diag_support_by_quality_unitbin.csv"))
fwrite(support_tbls$thin_cells, file.path(tab_dir, "severe_maint_diag_support_thin_cells.csv"))
logf("Built and wrote support diagnostics tables.", log_file = log_file)
if (nrow(support_tbls$thin_cells)) {
  cat("\n=== Support diagnostics by coarse quality grade (controls-matched cells for unit-bin table) ===\n")
  print(support_tbls$thin_cells)
}
logf("Note: category-specific severe complaint counts are not present in building_data_rental_quarter; diagnostics remain severe-total only.", log_file = log_file)

mods <- NULL
if (fast_mode) {
  logf("Fast mode batch 1: fitting main stock models.", log_file = log_file)
  mods_main <- fit_models(
    dt, log_file = log_file,
    only = c("sample_diag", "m_stock_lag_q", "m_stock_lag_q_fine", "m_stock_lag_q_rich_adjcurr")
  )
  if (!is.null(mods_main$sample_diag)) fwrite(mods_main$sample_diag, file.path(tab_dir, "severe_maint_diag_sample.csv"))
  logf("Writing main etable...", log_file = log_file)
  fixest::etable(
    mods_main$m_stock_lag_q,
    mods_main$m_stock_lag_q_fine,
    mods_main$m_stock_lag_q_rich_adjcurr,
    tex = TRUE,
    title = "Rolling maintenance stock diagnostics (fast mode; baseline, fine-bin, and parcel-controls stock specs)",
    file = file.path(tab_dir, "severe_maint_diag_models.tex"),
    replace = TRUE
  )
  logf("Wrote main etable.", log_file = log_file)
  rm(mods_main)
  invisible(gc())
  logf("Fast mode: dropped main-batch models and ran gc().", log_file = log_file)

  logf("Fast mode batch 2: fitting auxiliary flow models.", log_file = log_file)
  mods_aux <- fit_models(
    dt, log_file = log_file,
    only = c("m_curr_noq", "m_curr_q", "m_l1_q", "m_l1_4_q", "m_curr_stock_lag_q", "m_curr_broad_q")
  )
  logf("Writing auxiliary flow etable...", log_file = log_file)
  fixest::etable(
    mods_aux$m_curr_noq,
    mods_aux$m_curr_q,
    mods_aux$m_l1_q,
    mods_aux$m_l1_4_q,
    mods_aux$m_curr_stock_lag_q,
    mods_aux$m_curr_broad_q,
    tex = TRUE,
    title = "Auxiliary diagnostics: contemporaneous and lagged flow measures, plus broad permit proxy",
    file = file.path(tab_dir, "severe_maint_diag_stock_quality_models.tex"),
    replace = TRUE
  )
  logf("Wrote auxiliary flow etable.", log_file = log_file)
  rm(mods_aux)
  invisible(gc())
  logf("Fast mode: dropped auxiliary-batch models and ran gc().", log_file = log_file)

  logf("Writing falsification/PIDFE etable...", log_file = log_file)
  write_fast_placeholder_table(
    file.path(tab_dir, "severe_maint_diag_falsification_pidfe_models.tex"),
    message = "Fast mode omits the falsification and PID fixed-effect table. Use full mode to render these diagnostics.",
    title = "Falsification and within-building diagnostics (fast mode)"
  )
  logf("Wrote falsification/PIDFE etable.", log_file = log_file)
  logf("Fast mode: skipped coefficient-table extraction and terminal coefficient dumps.", log_file = log_file)
} else {
  mods <- fit_models(dt, log_file = log_file)
  logf("Fitted severe-maintenance diagnostic models.", log_file = log_file)
  if (!is.null(mods$sample_diag)) fwrite(mods$sample_diag, file.path(tab_dir, "severe_maint_diag_sample.csv"))

  logf("Writing main etable...", log_file = log_file)
  fixest::etable(
    mods$m_stock_lag_q,
    mods$m_stock_lag_q_fine,
    mods$m_stock_lag_q5_fine,
    mods$m_stock_lag_q5_fine_rich_adjcurr,
    mods$m_stock_lag_q5_int_fine,
    mods$m_stock_lag_q5_int_fine_adjcurr,
    mods$m_stock_lag_q5_int_fine_adjl1,
    mods$m_stock_lag_q5_int_fine_rich,
    mods$m_stock_lag_q5_int_fine_rich_adjcurr,
    mods$m_stock_lag_q5_int_fine_rich_rent,
    tex = TRUE,
    title = "Rolling maintenance stock diagnostics (weighted by units; fine unit-bin FE, coarse quality-grade interactions, and rich controls)",
    file = file.path(tab_dir, "severe_maint_diag_models.tex"),
    replace = TRUE
  )
  logf("Wrote main etable.", log_file = log_file)
  logf("Writing auxiliary flow etable...", log_file = log_file)
  fixest::etable(
    mods$m_curr_noq,
    mods$m_curr_q,
    mods$m_l1_q,
    mods$m_l1_4_q,
    mods$m_curr_stock_lag_q,
    mods$m_curr_broad_q,
    tex = TRUE,
    title = "Auxiliary diagnostics: contemporaneous and lagged flow measures, plus broad permit proxy",
    file = file.path(tab_dir, "severe_maint_diag_stock_quality_models.tex"),
    replace = TRUE
  )
  logf("Wrote auxiliary flow etable.", log_file = log_file)
  logf("Writing falsification/PIDFE etable...", log_file = log_file)
  fixest::etable(
    mods$m_f1_q,
    mods$m_f2_q,
    mods$m_l1_pid,
    mods$m_stock_lag_pid,
    mods$m_f1_pid,
    mods$m_f2_pid,
    tex = TRUE,
    title = "Severe complaint-maintenance falsification and within-building diagnostics (leads and PID FE)",
    file = file.path(tab_dir, "severe_maint_diag_falsification_pidfe_models.tex"),
    replace = TRUE
  )
  logf("Wrote falsification/PIDFE etable.", log_file = log_file)

  logf("Extracting coefficient table...", log_file = log_file)
  coef_dt <- extract_coef_table(mods, "severe_diag")
  fwrite(coef_dt, file.path(tab_dir, "severe_maint_diag_coefficients.csv"))
  logf("Wrote coefficient table CSV.", log_file = log_file)

  key_terms <- c(
    "maint_rate_curr", "maint_rate_l1", "maint_rate_l1_4_mean",
    "maint_stock_lag_rate", "maint_rate_curr_broad", "maint_rate_f1", "maint_rate_f2"
  )
  coef_key <- coef_dt[term %chin% key_terms][
    , .(model, term, estimate = round(estimate, 4), std_error = round(std_error, 4), p_value = signif(p_value, 3))]
  if (nrow(coef_key)) {
    cat("\n=== Severe complaints vs maintenance diagnostics (weighted by units) ===\n")
    print(coef_key)
  }

  coef_q5 <- coef_dt[
    grepl("^m_stock_lag_q5_", model) &
      (term == "maint_stock_lag_rate" | grepl("quality_grade_abcde::", term, fixed = TRUE))
  ][, .(model, term, estimate = round(estimate, 4), std_error = round(std_error, 4), p_value = signif(p_value, 3))]
  if (nrow(coef_q5)) {
    cat("\n=== Rolling stock x quality-grade (A-E/Other) interaction diagnostics ===\n")
    print(coef_q5)
  }

  coef_rich <- coef_dt[
    grepl("^m_stock_lag_q5_int_fine_rich", model) &
      (term == "maint_stock_lag_rate" | term == "maint_rate_curr" | term == "log_med_rent_raw")
  ][, .(model, term, estimate = round(estimate, 4), std_error = round(std_error, 4), p_value = signif(p_value, 3))]
  if (nrow(coef_rich)) {
    cat("\n=== Rich-controls stock diagnostics (selected coefficients) ===\n")
    print(coef_rich)
  }
}

if (!fast_mode) {
  plot_partial_corr_bins(
    dt,
    x_cols = c("maint_rate_curr", "maint_rate_l1", "maint_stock_lag_rate"),
    x_labels = c("Current repair permits / unit", "Lagged repair permits / unit (t-1)", "Lagged cumulative repair stock / unit"),
    out_path = file.path(fig_dir, "severe_maint_diag_partial_corr.png")
  )
} else {
  fig_partial <- file.path(fig_dir, "severe_maint_diag_partial_corr.png")
  if (file.exists(fig_partial)) file.remove(fig_partial)
  logf("Fast mode: skipped partial-correlation figure generation.", log_file = log_file)
}

rm(dt, support_tbls, mods)
invisible(gc())
logf("Dropped panel/support/model objects and ran gc() before exit.", log_file = log_file)

logf("Wrote tables to ", tab_dir, " and figures to ", fig_dir, log_file = log_file)
logf("=== Finished severe-maintenance-diagnostics.R ===", log_file = log_file)
if (file.exists(lock_path)) {
  unlink(lock_path)
  logf("Released lockfile: ", lock_path, log_file = log_file)
}
