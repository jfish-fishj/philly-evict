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
    "total_area", "total_units", "market_value", "year_built",
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
  dt[, tenant_black_missing := as.integer(is.na(infousa_pct_black))]

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

extract_bt_interaction_coefs <- function(model, sample_name, outcome_name, fe_name, block_name) {
  ct <- tidy_coeftable(model)
  if (!nrow(ct)) return(data.table())
  keep_pat <- paste(
    c("^tc_black_c$", "^tc_female_c$", "^tc_black_female_c$", "^tc_cov_c$", "^tenant_comp_missing$",
      "tc_black_c", "tc_female_c"),
    collapse = "|"
  )
  ct <- ct[grepl(keep_pat, term)]
  if (!nrow(ct)) return(data.table())
  ct[, `:=`(
    sample = sample_name,
    outcome = outcome_name,
    fe = fe_name,
    block = block_name,
    conf_low = estimate - 1.96 * std_error,
    conf_high = estimate + 1.96 * std_error
  )]
  ct[]
}

tidy_coeftable <- function(model) {
  ct <- as.data.table(summary(model)$coeftable, keep.rownames = "term")
  if (!nrow(ct)) return(ct)

  est_col <- intersect(c("Estimate", "estimate"), names(ct))[1L]
  se_col <- intersect(c("Std. Error", "Std.Error", "std_error"), names(ct))[1L]
  stat_col <- intersect(c("t value", "z value", "stat", "statistic", "t_value", "z_value"), names(ct))[1L]
  p_col <- intersect(c("Pr(>|t|)", "Pr(>|z|)", "p_value", "p"), names(ct))[1L]

  if (!is.na(est_col) && est_col != "estimate") setnames(ct, est_col, "estimate")
  if (!is.na(se_col) && se_col != "std_error") setnames(ct, se_col, "std_error")
  if (!is.na(stat_col) && stat_col != "t_value") setnames(ct, stat_col, "t_value")
  if (!is.na(p_col) && p_col != "p_value") setnames(ct, p_col, "p_value")

  # Keep stable columns for downstream code.
  if (!"estimate" %in% names(ct)) ct[, estimate := NA_real_]
  if (!"std_error" %in% names(ct)) ct[, std_error := NA_real_]
  if (!"t_value" %in% names(ct)) ct[, t_value := NA_real_]
  if (!"p_value" %in% names(ct)) ct[, p_value := NA_real_]
  ct[]
}

safe_model_eval <- function(expr, model_label, log_file = NULL) {
  tryCatch(
    eval.parent(substitute(expr)),
    error = function(e) {
      msg <- conditionMessage(e)
      if (grepl("dependent variable .* constant", msg, ignore.case = TRUE)) {
        logf("Skipping model (constant DV after cleaning): ", model_label, " | ", msg, log_file = log_file)
        return(NULL)
      }
      stop(e)
    }
  )
}

find_bt_interaction_term <- function(coef_names, group_var, group_level, slope_var) {
  target <- paste0(group_var, "::", group_level, ":", slope_var)
  if (target %chin% coef_names) return(target)
  alt <- paste0(slope_var, ":", group_var, "::", group_level)
  if (alt %chin% coef_names) return(alt)
  hits <- coef_names[
    grepl(paste0(group_var, "::", group_level), coef_names, fixed = TRUE) &
      grepl(paste0(":", slope_var, "$"), coef_names)
  ]
  if (!length(hits)) return(NA_character_)
  hits[1L]
}

build_bt_slope_table <- function(model, sample_name, outcome_name, fe_name, block_name,
                                 ref_building_type, building_types_seen,
                                 group_var = "building_type",
                                 group_col = "building_type",
                                 ref_group_col = "ref_building_type") {
  b <- coef(model)
  if (!length(b)) return(data.table())
  cn <- names(b)
  v <- tryCatch(as.matrix(vcov(model)), error = function(e) NULL)
  if (is.null(v) || !length(v)) return(data.table())

  bt_levels <- sort(unique(c(as.character(building_types_seen), ref_building_type)))
  bt_levels <- bt_levels[!is.na(bt_levels) & nzchar(bt_levels)]
  if (!length(bt_levels)) return(data.table())

  slope_vars <- c("tc_black_c", "tc_female_c")
  slope_labels <- c(tc_black_c = "pct_black", tc_female_c = "pct_women")

  out <- vector("list", length(bt_levels) * length(slope_vars))
  ii <- 0L
  for (sv in slope_vars) {
    base_term <- if (sv %chin% cn) sv else NA_character_
    for (bt in bt_levels) {
      ii <- ii + 1L
      int_term <- if (identical(bt, ref_building_type)) NA_character_ else find_bt_interaction_term(cn, group_var, bt, sv)
      est <- se <- pval <- stat <- NA_real_
      estimable <- !is.na(base_term)

      if (estimable) {
        if (is.na(int_term)) {
          # Reference-category slope or omitted interaction.
          if (identical(bt, ref_building_type)) {
            est <- unname(b[base_term])
            vv <- v[base_term, base_term]
            se <- if (is.finite(vv) && vv >= 0) sqrt(vv) else NA_real_
          } else {
            estimable <- FALSE
          }
        } else if (int_term %chin% cn) {
          est <- unname(b[base_term] + b[int_term])
          vv <- v[base_term, base_term] + v[int_term, int_term] + 2 * v[base_term, int_term]
          se <- if (is.finite(vv) && vv >= 0) sqrt(vv) else NA_real_
        } else {
          estimable <- FALSE
        }
      }

      if (is.finite(est) && is.finite(se) && se > 0) {
        stat <- est / se
        pval <- 2 * pnorm(abs(stat), lower.tail = FALSE)
      }

      row_dt <- data.table(
        block = block_name,
        sample = sample_name,
        outcome = outcome_name,
        fe = fe_name,
        group_var = group_var,
        group_level = bt,
        ref_group_level = ref_building_type,
        slope_var = sv,
        slope_label = unname(slope_labels[[sv]]),
        base_term = base_term,
        interaction_term = int_term,
        estimable = as.logical(estimable),
        estimate = est,
        std_error = se,
        stat_value = stat,
        p_value = pval,
        conf_low = if (is.finite(est) && is.finite(se)) est - 1.96 * se else NA_real_,
        conf_high = if (is.finite(est) && is.finite(se)) est + 1.96 * se else NA_real_
      )
      row_dt[, (group_col) := bt]
      row_dt[, (ref_group_col) := ref_building_type]
      out[[ii]] <- row_dt
    }
  }
  rbindlist(out, use.names = TRUE, fill = TRUE)
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
skip_distlag <- to_bool(opts$skip_distlag, cfg$run$retaliatory_skip_distlag %||% TRUE)
only_complaint_suppression <- to_bool(opts$only_complaint_suppression, TRUE)
if (skip_distlag) max_lead_lag <- max_bw

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
  ", skip_distlag=", skip_distlag,
  ", skip_lpdid=", skip_lpdid,
  ", only_complaint_suppression=", only_complaint_suppression,
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
               "total_permits", "total_violations",
               "hazardous_violation_count", "unsafe_violation_count",
               "electrical_permit_count", "plumbing_permit_count",
               "mechanical_permit_count", "fire_suppression_permit_count",
               complaint_candidates)
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

non_numeric_cols <- c(
  "PID", "period", "GEOID", "census_tract",
  "building_type", "num_units_bin", "year_blt_decade", "num_stories_bin", "quality_grade"
)
num_cols <- setdiff(names(dt), non_numeric_cols)
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

# Availability flags used in multiple model blocks (including complaint suppression).
has_geoid <- "GEOID" %in% names(dt) && dt[!is.na(GEOID) & nzchar(as.character(GEOID)), .N] > 0
has_tract <- "census_tract" %in% names(dt)

# No separate panel_pre copy — filter inline via dt[year <= pre_covid_end] to save memory
n_pre <- dt[year <= pre_covid_end, .N]
if (n_pre == 0L) stop("Pre-COVID sample is empty after filters.")

logf("Panel rows (full, ", period_label, "): ", nrow(dt), ", PIDs: ", uniqueN(dt$PID), log_file = log_file)
logf("Panel rows (pre-covid, ", period_label, "): ", n_pre, ", PIDs: ", dt[year <= pre_covid_end, uniqueN(PID)], log_file = log_file)
logf("Eviction filing rate (full): ", round(mean(dt$filed_eviction, na.rm = TRUE), 6), log_file = log_file)
logf("Any complaint rate (full): ", round(mean(dt$filed_complaint, na.rm = TRUE), 6), log_file = log_file)
logf("Severe complaint source: ", severe_source, log_file = log_file)

if (!only_complaint_suppression) {
model_specs <- data.table(
  sample = c("full", "full", "full", "pre", "pre", "pre"),
  stem = c("complaint", "severe", "non_severe", "complaint", "severe", "non_severe")
)

# Distributed lag specs.
# NOTE: model objects are discarded after coef extraction to avoid OOM on large panels.
# etable is called per-model and concatenated, instead of storing all models in memory.
etable_dist_lines <- list()
coef_dist_list <- list()
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
if (!skip_distlag) {
  for (i in seq_len(nrow(model_specs))) {
    s <- model_specs$sample[i]
    stem <- model_specs$stem[i]
    key <- paste0(s, "_", stem)
    dat <- if (s == "pre") dt[year <= pre_covid_end] else dt
    dat <- dat[!is.na(GEOID) & nzchar(as.character(GEOID))]
    if (nrow(dat) < 100L) {
      rm(dat); gc()
      next
    }
    m <- fit_dist_lag(dat, stem = stem, h = h)
    coef_dist_list[[key]] <- extract_dist_lag_coefs(m, stem = stem, sample_name = s)
    etable_dist_lines[[key]] <- capture.output(etable(m, se.below = TRUE, digits = 3))
    logf("Fitted dist-lag model: ", key, " | N=", nobs(m), log_file = log_file)
    rm(m, dat); gc()
  }
  coef_dist <- rbindlist(coef_dist_list, use.names = TRUE, fill = TRUE)
  coef_dist[, stem_label := pretty_stem(stem)]
  coef_dist[, sample_label := fifelse(sample == "pre", paste0("Pre-COVID (<=", pre_covid_end, ")"), "Full Sample")]
} else {
  logf("Skipping distributed-lag block (--skip_distlag=TRUE).", log_file = log_file)
}

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

    m_base_tract <- safe_model_eval(
      fit_same_period_tract(dat, stem = stem, y = "filed_eviction"),
      model_label = paste0("tract_fe_same_period_base_", key),
      log_file = log_file
    )
    m_tenant_tract <- safe_model_eval(
      fit_same_period_tract_tenant(dat, stem = stem, y = "filed_eviction"),
      model_label = paste0("tract_fe_same_period_tenant_", key),
      log_file = log_file
    )
    if (is.null(m_base_tract) || is.null(m_tenant_tract)) {
      rm(dat); gc()
      next
    }

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
etable_tract_txt <- if (length(etable_tract_lines)) unlist(etable_tract_lines) else character()
etable_tract_tex <- if (length(etable_tract_tex_lines)) unlist(etable_tract_tex_lines) else character()

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
targeting_btint_coef_list <- list()
targeting_btint_etable_lines <- list()
targeting_btint_etable_tex_lines <- list()
targeting_btint_slopes_list <- list()
bldg_year_targeting_coef_list <- list()
bldg_year_targeting_etable_lines <- list()
bldg_year_targeting_etable_tex_lines <- list()
bldg_year_targeting_btint_coef_list <- list()
bldg_year_targeting_btint_etable_lines <- list()
bldg_year_targeting_btint_etable_tex_lines <- list()
bldg_year_targeting_btint_slopes_list <- list()

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
      dat_tract <- dat[!is.na(census_tract) & nzchar(as.character(census_tract))]
      dat_bg <- dat[!is.na(GEOID) & nzchar(as.character(GEOID))]
      if (nrow(dat_tract) < 100L || nrow(dat_bg) < 100L) {
        rm(dat, dat_tract, dat_bg); gc()
        next
      }
      key <- paste0(s, "_", outcome_name)

      # Tract FE
      fml_tract <- as.formula(paste0(
        y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
        " + tenant_comp_missing | period_fe + census_tract"
      ))
      m_tract <- safe_model_eval(
        feols(fml_tract, data = dat_tract, cluster = ~census_tract, lean = TRUE),
        model_label = paste0("targeting_", key, "_tract"),
        log_file = log_file
      )

      # BG FE
      fml_bg <- as.formula(paste0(
        y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
        " + tenant_comp_missing | period_fe + GEOID"
      ))
      m_bg <- safe_model_eval(
        feols(fml_bg, data = dat_bg, cluster = ~GEOID, lean = TRUE),
        model_label = paste0("targeting_", key, "_bg"),
        log_file = log_file
      )
      if (is.null(m_tract) || is.null(m_bg)) {
        rm(dat, dat_tract, dat_bg); gc()
        next
      }

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
      rm(m_tract, m_bg, dat, dat_tract, dat_bg); gc()
    }
  }

  # Unit-bin slope heterogeneity for targeting (% Black / % women).
  bt_counts_ev <- ev_dt[!is.na(num_units_bin) & nzchar(as.character(num_units_bin)), .N, by = num_units_bin][order(-N, num_units_bin)]
  bt_ref_targeting <- if ("1" %in% bt_counts_ev$num_units_bin) {
    "1"
  } else if (nrow(bt_counts_ev)) {
    as.character(bt_counts_ev$num_units_bin[1L])
  } else {
    NA_character_
  }
  if (is.na(bt_ref_targeting)) {
    logf("Skipping targeting num_units_bin interactions: no non-missing num_units_bin.", log_file = log_file)
  } else {
    logf("Targeting num_units_bin interaction reference category: ", bt_ref_targeting, log_file = log_file)
    for (outcome_name in c("same", "bw")) {
      y_col <- paste0("retaliatory_", outcome_name)
      for (s in c("full", "pre")) {
        dat <- if (s == "pre") ev_dt[year <= pre_covid_end] else ev_dt
        dat_tract <- dat[!is.na(census_tract) & nzchar(as.character(census_tract))]
        dat_bg <- dat[!is.na(GEOID) & nzchar(as.character(GEOID))]
        if (nrow(dat_tract) < 100L || nrow(dat_bg) < 100L) {
          rm(dat, dat_tract, dat_bg); gc()
          next
        }
        if (dat[!is.na(num_units_bin) & nzchar(as.character(num_units_bin)), uniqueN(num_units_bin)] < 2L) {
          rm(dat, dat_tract, dat_bg); gc()
          next
        }
        key <- paste0(s, "_", outcome_name)

        fml_tract_btint <- as.formula(paste0(
          y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c + tenant_comp_missing",
          " + i(num_units_bin, tc_black_c, ref = '", bt_ref_targeting, "')",
          " + i(num_units_bin, tc_female_c, ref = '", bt_ref_targeting, "')",
          " | period_fe + census_tract + num_units_bin"
        ))
        m_tract_btint <- safe_model_eval(
          feols(fml_tract_btint, data = dat_tract, cluster = ~census_tract, lean = TRUE),
          model_label = paste0("targeting_btint_", key, "_tract"),
          log_file = log_file
        )

        fml_bg_btint <- as.formula(paste0(
          y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c + tenant_comp_missing",
          " + i(num_units_bin, tc_black_c, ref = '", bt_ref_targeting, "')",
          " + i(num_units_bin, tc_female_c, ref = '", bt_ref_targeting, "')",
          " | period_fe + GEOID + num_units_bin"
        ))
        m_bg_btint <- safe_model_eval(
          feols(fml_bg_btint, data = dat_bg, cluster = ~GEOID, lean = TRUE),
          model_label = paste0("targeting_btint_", key, "_bg"),
          log_file = log_file
        )
        if (is.null(m_tract_btint) || is.null(m_bg_btint)) {
          rm(dat, dat_tract, dat_bg); gc()
          next
        }

        targeting_btint_coef_list[[paste0(key, "_tract")]] <- extract_bt_interaction_coefs(
          m_tract_btint, sample_name = s, outcome_name = outcome_name, fe_name = "tract", block_name = "targeting"
        )
        targeting_btint_coef_list[[paste0(key, "_bg")]] <- extract_bt_interaction_coefs(
          m_bg_btint, sample_name = s, outcome_name = outcome_name, fe_name = "bg", block_name = "targeting"
        )
        bt_levels_dat <- dat[!is.na(num_units_bin) & nzchar(as.character(num_units_bin)), unique(as.character(num_units_bin))]
        targeting_btint_slopes_list[[paste0(key, "_tract")]] <- build_bt_slope_table(
          m_tract_btint,
          sample_name = s,
          outcome_name = outcome_name,
          fe_name = "tract",
          block_name = "targeting",
          ref_building_type = bt_ref_targeting,
          building_types_seen = bt_levels_dat,
          group_var = "num_units_bin",
          group_col = "num_units_bin",
          ref_group_col = "ref_num_units_bin"
        )
        targeting_btint_slopes_list[[paste0(key, "_bg")]] <- build_bt_slope_table(
          m_bg_btint,
          sample_name = s,
          outcome_name = outcome_name,
          fe_name = "bg",
          block_name = "targeting",
          ref_building_type = bt_ref_targeting,
          building_types_seen = bt_levels_dat,
          group_var = "num_units_bin",
          group_col = "num_units_bin",
          ref_group_col = "ref_num_units_bin"
        )
        targeting_btint_etable_lines[[key]] <- capture.output(
          etable(m_tract_btint, m_bg_btint, se.below = TRUE, digits = 4, headers = c("Tract FE", "BG FE"))
        )
        targeting_btint_etable_tex_lines[[key]] <- capture.output(
          etable(m_tract_btint, m_bg_btint, se.below = TRUE, digits = 4, tex = TRUE, headers = c("Tract FE", "BG FE"))
        )
        logf("Fitted targeting num_units_bin interaction model: ", key, " | N_tract=", nobs(m_tract_btint),
             ", N_bg=", nobs(m_bg_btint), log_file = log_file)
        rm(m_tract_btint, m_bg_btint, dat, dat_tract, dat_bg); gc()
      }
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
      dat_tract <- dat[!is.na(census_tract) & nzchar(as.character(census_tract))]
      dat_bg <- dat[!is.na(GEOID) & nzchar(as.character(GEOID))]
      if (nrow(dat_tract) < 100L || nrow(dat_bg) < 100L) {
        rm(dat, dat_tract, dat_bg); gc()
        next
      }
      key <- paste0(s, "_", outcome_name)

      # Tract FE + building observables
      fml_tract <- as.formula(paste0(
        y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
        " + tenant_comp_missing", obs_rhs,
        " | year + census_tract", obs_fe
      ))
      m_tract <- safe_model_eval(
        feols(fml_tract, data = dat_tract, cluster = ~census_tract, lean = TRUE),
        model_label = paste0("bldg_year_targeting_", key, "_tract"),
        log_file = log_file
      )

      # BG FE + building observables
      fml_bg <- as.formula(paste0(
        y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
        " + tenant_comp_missing", obs_rhs,
        " | year + GEOID", obs_fe
      ))
      m_bg <- safe_model_eval(
        feols(fml_bg, data = dat_bg, cluster = ~GEOID, lean = TRUE),
        model_label = paste0("bldg_year_targeting_", key, "_bg"),
        log_file = log_file
      )
      if (is.null(m_tract) || is.null(m_bg)) {
        rm(m_tract, m_bg, dat, dat_tract, dat_bg); gc()
        next
      }

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
      rm(m_tract, m_bg, dat, dat_tract, dat_bg); gc()
    }
  }

  # Unit-bin slope heterogeneity for building-year targeting (% Black / % women).
  bt_counts_byt <- pid_year_ev[!is.na(num_units_bin) & nzchar(as.character(num_units_bin)), .N, by = num_units_bin][order(-N, num_units_bin)]
  bt_ref_bldg_year_targeting <- if ("1" %in% bt_counts_byt$num_units_bin) {
    "1"
  } else if (nrow(bt_counts_byt)) {
    as.character(bt_counts_byt$num_units_bin[1L])
  } else {
    NA_character_
  }
  if (is.na(bt_ref_bldg_year_targeting)) {
    logf("Skipping building-year targeting num_units_bin interactions: no non-missing num_units_bin.", log_file = log_file)
  } else {
    logf("Building-year targeting num_units_bin interaction reference category: ", bt_ref_bldg_year_targeting, log_file = log_file)
    for (outcome_name in c("same", "bw")) {
      y_col <- paste0("share_retaliatory_", outcome_name)
      for (s in c("full", "pre")) {
        dat <- if (s == "pre") pid_year_ev[year <= pre_covid_end] else pid_year_ev
        dat_tract <- dat[!is.na(census_tract) & nzchar(as.character(census_tract))]
        dat_bg <- dat[!is.na(GEOID) & nzchar(as.character(GEOID))]
        if (nrow(dat_tract) < 100L || nrow(dat_bg) < 100L) {
          rm(dat, dat_tract, dat_bg); gc()
          next
        }
        if (dat[!is.na(num_units_bin) & nzchar(as.character(num_units_bin)), uniqueN(num_units_bin)] < 2L) {
          rm(dat, dat_tract, dat_bg); gc()
          next
        }
        key <- paste0(s, "_", outcome_name)

        fml_tract_btint <- as.formula(paste0(
          y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
          " + tenant_comp_missing",
          " + i(num_units_bin, tc_black_c, ref = '", bt_ref_bldg_year_targeting, "')",
          " + i(num_units_bin, tc_female_c, ref = '", bt_ref_bldg_year_targeting, "')",
          obs_rhs,
          " | year + census_tract", obs_fe
        ))
        m_tract_btint <- safe_model_eval(
          feols(fml_tract_btint, data = dat_tract, cluster = ~census_tract, lean = TRUE),
          model_label = paste0("bldg_year_targeting_btint_", key, "_tract"),
          log_file = log_file
        )

        fml_bg_btint <- as.formula(paste0(
          y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
          " + tenant_comp_missing",
          " + i(num_units_bin, tc_black_c, ref = '", bt_ref_bldg_year_targeting, "')",
          " + i(num_units_bin, tc_female_c, ref = '", bt_ref_bldg_year_targeting, "')",
          obs_rhs,
          " | year + GEOID", obs_fe
        ))
        m_bg_btint <- safe_model_eval(
          feols(fml_bg_btint, data = dat_bg, cluster = ~GEOID, lean = TRUE),
          model_label = paste0("bldg_year_targeting_btint_", key, "_bg"),
          log_file = log_file
        )
        if (is.null(m_tract_btint) || is.null(m_bg_btint)) {
          rm(dat, dat_tract, dat_bg); gc()
          next
        }

        bldg_year_targeting_btint_coef_list[[paste0(key, "_tract")]] <- extract_bt_interaction_coefs(
          m_tract_btint, sample_name = s, outcome_name = outcome_name, fe_name = "tract", block_name = "bldg_year_targeting"
        )
        bldg_year_targeting_btint_coef_list[[paste0(key, "_bg")]] <- extract_bt_interaction_coefs(
          m_bg_btint, sample_name = s, outcome_name = outcome_name, fe_name = "bg", block_name = "bldg_year_targeting"
        )
        bt_levels_dat <- dat[!is.na(num_units_bin) & nzchar(as.character(num_units_bin)), unique(as.character(num_units_bin))]
        bldg_year_targeting_btint_slopes_list[[paste0(key, "_tract")]] <- build_bt_slope_table(
          m_tract_btint,
          sample_name = s,
          outcome_name = outcome_name,
          fe_name = "tract",
          block_name = "bldg_year_targeting",
          ref_building_type = bt_ref_bldg_year_targeting,
          building_types_seen = bt_levels_dat,
          group_var = "num_units_bin",
          group_col = "num_units_bin",
          ref_group_col = "ref_num_units_bin"
        )
        bldg_year_targeting_btint_slopes_list[[paste0(key, "_bg")]] <- build_bt_slope_table(
          m_bg_btint,
          sample_name = s,
          outcome_name = outcome_name,
          fe_name = "bg",
          block_name = "bldg_year_targeting",
          ref_building_type = bt_ref_bldg_year_targeting,
          building_types_seen = bt_levels_dat,
          group_var = "num_units_bin",
          group_col = "num_units_bin",
          ref_group_col = "ref_num_units_bin"
        )
        bldg_year_targeting_btint_etable_lines[[key]] <- capture.output(
          etable(m_tract_btint, m_bg_btint, se.below = TRUE, digits = 4, headers = c("Tract FE", "BG FE"))
        )
        bldg_year_targeting_btint_etable_tex_lines[[key]] <- capture.output(
          etable(m_tract_btint, m_bg_btint, se.below = TRUE, digits = 4, tex = TRUE, headers = c("Tract FE", "BG FE"))
        )
        logf("Fitted bldg-year targeting num_units_bin interaction model: ", key, " | N_tract=", nobs(m_tract_btint),
             ", N_bg=", nobs(m_bg_btint), log_file = log_file)
        rm(m_tract_btint, m_bg_btint, dat, dat_tract, dat_bg); gc()
      }
    }
  }

} else {
  logf("Skipping targeting models: GEOID/census_tract not available.", log_file = log_file)
}
coef_targeting <- rbindlist(targeting_coef_list, use.names = TRUE, fill = TRUE)
coef_bldg_year_targeting <- rbindlist(bldg_year_targeting_coef_list, use.names = TRUE, fill = TRUE)
etable_targeting_txt <- unlist(targeting_etable_lines)
etable_targeting_tex <- unlist(targeting_etable_tex_lines)
etable_bldg_year_targeting_txt <- unlist(bldg_year_targeting_etable_lines)
etable_bldg_year_targeting_tex <- unlist(bldg_year_targeting_etable_tex_lines)

} else {
  logf("Skipping non-suppression model blocks (--only_complaint_suppression=TRUE).", log_file = log_file)
  targeting_coef_list <- list()
  targeting_etable_lines <- list()
  targeting_etable_tex_lines <- list()
  targeting_btint_coef_list <- list()
  targeting_btint_etable_lines <- list()
  targeting_btint_etable_tex_lines <- list()
  targeting_btint_slopes_list <- list()
  bldg_year_targeting_coef_list <- list()
  bldg_year_targeting_etable_lines <- list()
  bldg_year_targeting_etable_tex_lines <- list()
  bldg_year_targeting_btint_coef_list <- list()
  bldg_year_targeting_btint_etable_lines <- list()
  bldg_year_targeting_btint_etable_tex_lines <- list()
  bldg_year_targeting_btint_slopes_list <- list()
  coef_dist <- data.table()
  coef_lpdid <- data.table()
  lpdid_windows <- data.table()
  models_lpdid <- list()
  coef_same_period <- data.table()
  coef_same_period_wide <- data.table()
  coef_tract <- data.table()
  coef_targeting <- data.table()
  coef_bldg_year_targeting <- data.table()
  coef_targeting_btint <- data.table()
  coef_bldg_year_targeting_btint <- data.table()
  slopes_targeting_btint <- data.table()
  slopes_bldg_year_targeting_btint <- data.table()
  etable_dist_txt <- character()
  lpdid_txt <- character()
  etable_same_period_txt <- character()
  etable_same_period_tex <- character()
  etable_same_period_lines <- list()
  etable_same_period_tex_lines <- list()
  etable_tract_txt <- character()
  etable_tract_tex <- character()
  etable_tract_lines <- list()
  etable_tract_tex_lines <- list()
  etable_targeting_txt <- character()
  etable_targeting_tex <- character()
  etable_bldg_year_targeting_txt <- character()
  etable_bldg_year_targeting_tex <- character()
  etable_targeting_btint_txt <- character()
  etable_targeting_btint_tex <- character()
  etable_bldg_year_targeting_btint_txt <- character()
  etable_bldg_year_targeting_btint_tex <- character()
  slopes_targeting_btint_txt <- character()
  slopes_bldg_year_targeting_btint_txt <- character()
  distlag_plot_note <- "Distributed-lag plot skipped (only_complaint_suppression=TRUE)"
  lpdid_plot_note <- "LP-DiD plot skipped (only_complaint_suppression=TRUE)"
}

# ======================================================================
# Complaint Suppression (panel LPM + aggregated PPML): controlling for
# maintenance (permits) and building observables, do buildings with more
# Black / female tenants file fewer complaints?
# Sample: all building-periods
# Panel outcomes: complaint indicators (any / severe / non-severe)
# FE: period + tract; period + BG
# ======================================================================
suppression_coef_list <- list()
suppression_etable_lines <- list()
suppression_etable_tex_lines <- list()
suppression_btint_coef_list <- list()
suppression_btint_etable_lines <- list()
suppression_btint_etable_tex_lines <- list()
suppression_btint_slopes_list <- list()
suppression_longrun_coef_list <- list()
suppression_longrun_etable_lines <- list()
suppression_longrun_etable_tex_lines <- list()
suppression_severe_3spec_coef <- data.table(
  model = character(),
  term = character(),
  estimate = numeric(),
  std_error = numeric(),
  t_value = numeric(),
  p_value = numeric(),
  n_obs = integer(),
  n_pid = integer(),
  sample_note = character()
)
suppression_severe_3spec_etable_txt <- character()
suppression_severe_3spec_etable_tex <- character()
suppression_severe_3spec_sample <- data.table(
  model = character(),
  n_obs = integer(),
  n_pid = integer(),
  lambda = numeric(),
  maintenance_flow_source = character()
)
suppression_severe_3spec_missingness <- data.table(
  stage = character(),
  year = integer(),
  n_obs = integer(),
  n_pid = integer(),
  share_tenant_black_missing = numeric(),
  share_tenant_comp_missing = numeric(),
  share_log_total_area_missing = numeric(),
  share_missing_tract = numeric(),
  share_missing_maint_stock = numeric()
)

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
  for (cc in c("total_area", "total_units", "market_value", "year_built")) {
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

  # Cumulative maintenance / condition histories (through current period, by PID).
  setorder(dt, PID, time_index)
  dt[, cumulative_permit_count := cumsum(fifelse(is.na(total_permits), 0, total_permits)), by = PID]
  dt[, cumulative_violations_count := cumsum(fifelse(is.na(total_violations), 0, total_violations)), by = PID]

  # Building observable controls + FE (matching price-regs-eb.R pattern)
  sup_obs_rhs <- paste0(
    " + total_permits + total_violations",
    " + cumulative_permit_count + cumulative_violations_count",
    " + log_total_area + log_market_value"
  )
  sup_obs_fe <- " + num_units_bin + building_type + year_blt_decade + num_stories_bin"

  # Count outcomes for aggregated suppression specs.
  dt[, complaints_any_count := pmax(0, suppressWarnings(as.numeric(total_complaints)))]
  if ("total_severe_complaints" %in% names(dt)) {
    dt[, complaints_severe_count := pmax(0, suppressWarnings(as.numeric(total_severe_complaints)))]
  } else {
    dt[, complaints_severe_count := rowSums(.SD, na.rm = TRUE), .SDcols = severe_cols]
    dt[!is.finite(complaints_severe_count), complaints_severe_count := 0]
  }
  dt[, complaints_non_severe_count := rowSums(.SD, na.rm = TRUE), .SDcols = non_severe_cols]
  dt[!is.finite(complaints_non_severe_count), complaints_non_severe_count := 0]
  dt[, complaints_non_severe_count := pmax(0, complaints_non_severe_count)]

  # -------------------------------------------------------------------
  # Severe complaint PPML (3-spec ladder, raw pct_black):
  #   (1) pct_black + year FE
  #   (2) + building chars + neighborhood (tract) FE + year FE
  #   (3) + lagged maintenance stock per unit (preferred maintenance control)
  # -------------------------------------------------------------------
  collapse_quality_grade_abcde <- function(x) {
    x <- toupper(trimws(as.character(x)))
    x <- substr(x, 1L, 1L)
    x[!(x %chin% c("A", "B", "C", "D", "E"))] <- "Other"
    x[is.na(x) | !nzchar(x)] <- "Other"
    x
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
    out[ok] <- as.character(cut(x[ok], breaks = cuts, labels = labs, include.lowest = TRUE, right = TRUE))
    out[is.na(out) | !nzchar(out)] <- "Unknown"
    out
  }
  ewma_vec <- function(x, lambda = 0.9) {
    x <- as.numeric(x)
    n <- length(x)
    out <- numeric(n)
    if (!n) return(out)
    out[1L] <- ifelse(is.na(x[1L]), 0, x[1L])
    if (n >= 2L) for (i in 2L:n) out[i] <- ifelse(is.na(x[i]), 0, x[i]) + lambda * out[i - 1L]
    out
  }
  sev3_lambda <- 0.9
  if (!("total_severe_complaints" %in% names(dt))) {
    dt[, total_severe_complaints := complaints_severe_count]
  }
  dt[, total_units_wt := suppressWarnings(as.numeric(total_units))]
  dt[!is.finite(total_units_wt) | total_units_wt <= 0, total_units_wt := 1]
  dt[, quality_grade_abcde := collapse_quality_grade_abcde(quality_grade)]
  dt[, unit_bin_fine := make_fine_unit_bins(total_units)]
  dt[, log_total_area := suppressWarnings(as.numeric(log_total_area))]
  dt[!is.finite(log_total_area), log_total_area := NA_real_]
  repair_permit_cols <- intersect(
    c("electrical_permit_count", "plumbing_permit_count", "mechanical_permit_count", "fire_suppression_permit_count"),
    names(dt)
  )
  if (length(repair_permit_cols)) {
    dt[, maint_flow_repairs := rowSums(.SD, na.rm = TRUE), .SDcols = repair_permit_cols]
    sev3_flow_source <- paste(repair_permit_cols, collapse = ",")
  } else {
    dt[, maint_flow_repairs := fifelse(is.na(total_permits), 0, total_permits)]
    sev3_flow_source <- "fallback_total_permits"
  }
  setorder(dt, PID, time_index)
  dt[, maint_flow_repairs_l1 := shift(maint_flow_repairs, 1L, type = "lag", fill = 0), by = PID]
  dt[, maint_stock_lag_sev3 := ewma_vec(maint_flow_repairs_l1, lambda = sev3_lambda), by = PID]
  dt[, maint_stock_lag_rate_sev3 := maint_stock_lag_sev3 / pmax(total_units_wt, 1)]

  dat_sev3_r1 <- dt[
    tenant_comp_missing == 0L &
      is.finite(total_severe_complaints) &
      is.finite(infousa_pct_black)
  ]
  severe_viol_cols <- intersect(c("hazardous_violation_count", "unsafe_violation_count"), names(dt))
  if (length(severe_viol_cols)) {
    dt[, severe_viol_flow_sev3 := rowSums(.SD, na.rm = TRUE), .SDcols = severe_viol_cols]
    sev3_viol_source <- paste(severe_viol_cols, collapse = ",")
  } else {
    dt[, severe_viol_flow_sev3 := fifelse(is.na(total_violations), 0, total_violations)]
    sev3_viol_source <- "fallback_total_violations"
  }
  dt[, severe_viol_flow_l1_sev3 := shift(severe_viol_flow_sev3, 1L, type = "lag", fill = 0), by = PID]
  dt[, severe_viol_stock_lag_sev3 := ewma_vec(severe_viol_flow_l1_sev3, lambda = sev3_lambda), by = PID]
  dt[, severe_viol_stock_lag_rate_sev3 := severe_viol_stock_lag_sev3 / pmax(total_units_wt, 1)]

  dat_sev3_r23 <- dt[
    tenant_comp_missing == 0L &
    is.finite(total_severe_complaints) &
      is.finite(infousa_pct_black) &
      is.finite(log_total_area) &
      is.finite(maint_stock_lag_rate_sev3) &
      is.finite(severe_viol_stock_lag_rate_sev3) &
      !is.na(census_tract) & nzchar(as.character(census_tract))
  ]
  suppression_severe_3spec_missingness <- rbindlist(list(
    dt[, .(
      stage = "panel_all",
      year = NA_integer_,
      n_obs = .N,
      n_pid = uniqueN(PID),
      share_tenant_black_missing = mean(tenant_black_missing == 1L, na.rm = TRUE),
      share_tenant_comp_missing = mean(tenant_comp_missing == 1L, na.rm = TRUE),
      share_log_total_area_missing = mean(!is.finite(log_total_area), na.rm = TRUE),
      share_missing_tract = mean(is.na(census_tract) | !nzchar(as.character(census_tract))),
      share_missing_maint_stock = mean(!is.finite(maint_stock_lag_rate_sev3), na.rm = TRUE)
    )],
    dt[, .(
      stage = "panel_all_by_year",
      n_obs = .N,
      n_pid = uniqueN(PID),
      share_tenant_black_missing = mean(tenant_black_missing == 1L, na.rm = TRUE),
      share_tenant_comp_missing = mean(tenant_comp_missing == 1L, na.rm = TRUE),
      share_log_total_area_missing = mean(!is.finite(log_total_area), na.rm = TRUE),
      share_missing_tract = mean(is.na(census_tract) | !nzchar(as.character(census_tract))),
      share_missing_maint_stock = mean(!is.finite(maint_stock_lag_rate_sev3), na.rm = TRUE)
    ), by = year],
    dat_sev3_r1[, .(
      stage = "reg1_sample",
      year = NA_integer_,
      n_obs = .N,
      n_pid = uniqueN(PID),
      share_tenant_black_missing = mean(tenant_black_missing == 1L, na.rm = TRUE),
      share_tenant_comp_missing = mean(tenant_comp_missing == 1L, na.rm = TRUE),
      share_log_total_area_missing = mean(!is.finite(log_total_area), na.rm = TRUE),
      share_missing_tract = mean(is.na(census_tract) | !nzchar(as.character(census_tract))),
      share_missing_maint_stock = mean(!is.finite(maint_stock_lag_rate_sev3), na.rm = TRUE)
    )],
    dat_sev3_r23[, .(
      stage = "reg2_3_sample",
      year = NA_integer_,
      n_obs = .N,
      n_pid = uniqueN(PID),
      share_tenant_black_missing = mean(tenant_black_missing == 1L, na.rm = TRUE),
      share_tenant_comp_missing = mean(tenant_comp_missing == 1L, na.rm = TRUE),
      share_log_total_area_missing = mean(!is.finite(log_total_area), na.rm = TRUE),
      share_missing_tract = mean(is.na(census_tract) | !nzchar(as.character(census_tract))),
      share_missing_maint_stock = mean(!is.finite(maint_stock_lag_rate_sev3), na.rm = TRUE)
    )]
  ), use.names = TRUE, fill = TRUE)

  m_sev3_r1 <- safe_model_eval(
    fepois(
      total_severe_complaints ~ infousa_pct_black | year,
      data = dat_sev3_r1,
      weights = ~total_units_wt,
      cluster = ~PID,
      lean = TRUE
    ),
    model_label = "suppression_severe_ppml_3spec_reg1",
    log_file = log_file
  )
  m_sev3_r2 <- safe_model_eval(
    fepois(
      total_severe_complaints ~ infousa_pct_black + log_total_area |
        year + census_tract + building_type + unit_bin_fine + year_blt_decade + num_stories_bin + quality_grade_abcde,
      data = dat_sev3_r23,
      weights = ~total_units_wt,
      cluster = ~PID,
      lean = TRUE
    ),
    model_label = "suppression_severe_ppml_3spec_reg2",
    log_file = log_file
  )
  m_sev3_r3 <- safe_model_eval(
    fepois(
      total_severe_complaints ~ infousa_pct_black + log_total_area + maint_stock_lag_rate_sev3 |
        year + census_tract + building_type + unit_bin_fine + year_blt_decade + num_stories_bin + quality_grade_abcde,
      data = dat_sev3_r23,
      weights = ~total_units_wt,
      cluster = ~PID,
      lean = TRUE
    ),
    model_label = "suppression_severe_ppml_3spec_reg3",
    log_file = log_file
  )
  m_sev3_r4 <- safe_model_eval(
    fepois(
      total_severe_complaints ~ infousa_pct_black + log_total_area + maint_stock_lag_rate_sev3 + severe_viol_stock_lag_rate_sev3 |
        year + census_tract + building_type + unit_bin_fine + year_blt_decade + num_stories_bin + quality_grade_abcde,
      data = dat_sev3_r23,
      weights = ~total_units_wt,
      cluster = ~PID,
      lean = TRUE
    ),
    model_label = "suppression_severe_ppml_3spec_reg4_sevviol",
    log_file = log_file
  )
  if (!is.null(m_sev3_r1) && !is.null(m_sev3_r2) && !is.null(m_sev3_r3) && !is.null(m_sev3_r4)) {
    add_sev3_meta <- function(ct, model_name, n_pid, sample_note, n_obs_val) {
      if (!nrow(ct)) return(ct)
      ct[, `:=`(model = model_name, n_obs = as.integer(n_obs_val), n_pid = as.integer(n_pid), sample_note = sample_note)]
      ct[]
    }
    ct1 <- tidy_coeftable(m_sev3_r1)
    ct2 <- tidy_coeftable(m_sev3_r2)
    ct3 <- tidy_coeftable(m_sev3_r3)
    suppression_severe_3spec_coef <- rbindlist(list(
      add_sev3_meta(ct1, "reg1_pct_black_year", uniqueN(dat_sev3_r1$PID), "tenant_comp_missing==0", nobs(m_sev3_r1)),
      add_sev3_meta(ct2, "reg2_plus_building_nhood_year", uniqueN(dat_sev3_r23$PID), "tenant_comp_missing==0 + area + tract", nobs(m_sev3_r2)),
      add_sev3_meta(ct3, "reg3_plus_maintenance_stock", uniqueN(dat_sev3_r23$PID), "reg2 sample + maintenance stock", nobs(m_sev3_r3)),
      add_sev3_meta(tidy_coeftable(m_sev3_r4), "reg4_plus_severe_violation_stock", uniqueN(dat_sev3_r23$PID), "reg3 sample + lagged severe violation stock", nobs(m_sev3_r4))
    ), use.names = TRUE, fill = TRUE)
    suppression_severe_3spec_etable_txt <- capture.output(
      etable(
        m_sev3_r1, m_sev3_r2, m_sev3_r3, m_sev3_r4,
        se.below = TRUE, digits = 4,
        headers = c("Reg 1", "Reg 2", "Reg 3", "Reg 4")
      )
    )
    suppression_severe_3spec_etable_tex <- capture.output(
      etable(
        m_sev3_r1, m_sev3_r2, m_sev3_r3, m_sev3_r4,
        se.below = TRUE, digits = 4, tex = TRUE,
        headers = c("Reg 1", "Reg 2", "Reg 3", "Reg 4")
      )
    )
    suppression_severe_3spec_sample <- data.table(
      model = c("reg1_pct_black_year", "reg2_plus_building_nhood_year", "reg3_plus_maintenance_stock", "reg4_plus_severe_violation_stock"),
      n_obs = c(nobs(m_sev3_r1), nobs(m_sev3_r2), nobs(m_sev3_r3), nobs(m_sev3_r4)),
      n_pid = c(uniqueN(dat_sev3_r1$PID), uniqueN(dat_sev3_r23$PID), uniqueN(dat_sev3_r23$PID), uniqueN(dat_sev3_r23$PID)),
      lambda = sev3_lambda,
      maintenance_flow_source = sev3_flow_source,
      severe_violation_flow_source = sev3_viol_source
    )
    sev3_key <- suppression_severe_3spec_coef[
      term %chin% c("infousa_pct_black", "log_total_area", "maint_stock_lag_rate_sev3", "severe_viol_stock_lag_rate_sev3"),
      .(model, term, estimate = round(estimate, 4), std_error = round(std_error, 4), p_value = signif(p_value, 4))
    ]
    if (nrow(sev3_key)) {
      logf("Severe complaint 3-spec PPML key coefficients:\n", paste(capture.output(print(sev3_key)), collapse = "\n"), log_file = log_file)
    }
    if (nrow(suppression_severe_3spec_missingness)) {
      miss_key <- copy(suppression_severe_3spec_missingness[stage %chin% c("panel_all", "reg1_sample", "reg2_3_sample")])
      for (cc in names(miss_key)) if (is.numeric(miss_key[[cc]])) miss_key[, (cc) := round(get(cc), 4)]
      logf("Severe complaint 3-spec pct_black missingness summary:\n", paste(capture.output(print(miss_key)), collapse = "\n"), log_file = log_file)
    }
    logf("Fitted severe complaint PPML ladder (4 specs incl. severe violations stock) | N1=", nobs(m_sev3_r1), ", N23=", nobs(m_sev3_r2),
         ", maintenance_flow_source=", sev3_flow_source, ", severe_violation_flow_source=", sev3_viol_source,
         ", lambda=", sev3_lambda, log_file = log_file)
  }
  rm(m_sev3_r1, m_sev3_r2, m_sev3_r3, m_sev3_r4, dat_sev3_r1, dat_sev3_r23); gc()

  if (!only_complaint_suppression) {
    suppression_outcomes <- c("complaint", "severe", "non_severe")
    for (stem in suppression_outcomes) {
      y_col <- paste0("filed_", stem)
      for (s in c("full", "pre")) {
        dat <- if (s == "pre") dt[year <= pre_covid_end] else dt
        dat_tract <- dat[!is.na(census_tract) & nzchar(as.character(census_tract))]
        dat_bg <- dat[!is.na(GEOID) & nzchar(as.character(GEOID))]
        if (nrow(dat_tract) < 100L || nrow(dat_bg) < 100L) {
          rm(dat, dat_tract, dat_bg); gc()
          next
        }
        key <- paste0(s, "_", stem)

        # Tract FE + building observables
        fml_tract <- as.formula(paste0(
          y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
          " + tenant_comp_missing", sup_obs_rhs,
          " | period_fe + census_tract", sup_obs_fe
        ))
        m_tract <- safe_model_eval(
          feols(fml_tract, data = dat_tract, cluster = ~census_tract, lean = TRUE),
          model_label = paste0("suppression_", key, "_tract"),
          log_file = log_file
        )

        # BG FE + building observables
        fml_bg <- as.formula(paste0(
          y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
          " + tenant_comp_missing", sup_obs_rhs,
          " | period_fe + GEOID", sup_obs_fe
        ))
        m_bg <- safe_model_eval(
          feols(fml_bg, data = dat_bg, cluster = ~GEOID, lean = TRUE),
          model_label = paste0("suppression_", key, "_bg"),
          log_file = log_file
        )
        if (is.null(m_tract) || is.null(m_bg)) {
          rm(dat, dat_tract, dat_bg); gc()
          next
        }

        ct_tract <- tidy_coeftable(m_tract)
        ct_bg <- tidy_coeftable(m_bg)
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
        logf("Fitted suppression LPM model: ", key, " | N_tract=", nobs(m_tract),
             ", N_bg=", nobs(m_bg), log_file = log_file)
        rm(m_tract, m_bg, dat, dat_tract, dat_bg); gc()
      }
    }

    # Unit-bin slope heterogeneity for complaint suppression (% Black / % women).
    bt_counts_sup <- dt[!is.na(num_units_bin) & nzchar(as.character(num_units_bin)), .N, by = num_units_bin][order(-N, num_units_bin)]
    bt_ref_suppression <- if ("1" %in% bt_counts_sup$num_units_bin) {
      "1"
    } else if (nrow(bt_counts_sup)) {
      as.character(bt_counts_sup$num_units_bin[1L])
    } else {
      NA_character_
    }
    if (is.na(bt_ref_suppression)) {
      logf("Skipping suppression num_units_bin interactions: no non-missing num_units_bin.", log_file = log_file)
    } else {
      logf("Suppression num_units_bin interaction reference category: ", bt_ref_suppression, log_file = log_file)
      for (stem in suppression_outcomes) {
        y_col <- paste0("filed_", stem)
        for (s in c("full", "pre")) {
          dat <- if (s == "pre") dt[year <= pre_covid_end] else dt
          dat_tract <- dat[!is.na(census_tract) & nzchar(as.character(census_tract))]
          dat_bg <- dat[!is.na(GEOID) & nzchar(as.character(GEOID))]
          if (nrow(dat_tract) < 100L || nrow(dat_bg) < 100L) {
            rm(dat, dat_tract, dat_bg); gc()
            next
          }
          if (dat[!is.na(num_units_bin) & nzchar(as.character(num_units_bin)), uniqueN(num_units_bin)] < 2L) {
            rm(dat, dat_tract, dat_bg); gc()
            next
          }
          key <- paste0(s, "_", stem)

          fml_tract_btint <- as.formula(paste0(
            y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
            " + tenant_comp_missing",
            " + i(num_units_bin, tc_black_c, ref = '", bt_ref_suppression, "')",
            " + i(num_units_bin, tc_female_c, ref = '", bt_ref_suppression, "')",
            sup_obs_rhs,
            " | period_fe + census_tract", sup_obs_fe
          ))
          m_tract_btint <- safe_model_eval(
            feols(fml_tract_btint, data = dat_tract, cluster = ~census_tract, lean = TRUE),
            model_label = paste0("suppression_btint_", key, "_tract"),
            log_file = log_file
          )

          fml_bg_btint <- as.formula(paste0(
            y_col, " ~ tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c",
            " + tenant_comp_missing",
            " + i(num_units_bin, tc_black_c, ref = '", bt_ref_suppression, "')",
            " + i(num_units_bin, tc_female_c, ref = '", bt_ref_suppression, "')",
            sup_obs_rhs,
            " | period_fe + GEOID", sup_obs_fe
          ))
          m_bg_btint <- safe_model_eval(
            feols(fml_bg_btint, data = dat_bg, cluster = ~GEOID, lean = TRUE),
            model_label = paste0("suppression_btint_", key, "_bg"),
            log_file = log_file
          )
          if (is.null(m_tract_btint) || is.null(m_bg_btint)) {
            rm(dat, dat_tract, dat_bg); gc()
            next
          }

          suppression_btint_coef_list[[paste0(key, "_tract")]] <- extract_bt_interaction_coefs(
            m_tract_btint, sample_name = s, outcome_name = stem, fe_name = "tract", block_name = "suppression"
          )
          suppression_btint_coef_list[[paste0(key, "_bg")]] <- extract_bt_interaction_coefs(
            m_bg_btint, sample_name = s, outcome_name = stem, fe_name = "bg", block_name = "suppression"
          )
          bt_levels_dat <- dat[!is.na(num_units_bin) & nzchar(as.character(num_units_bin)), unique(as.character(num_units_bin))]
          suppression_btint_slopes_list[[paste0(key, "_tract")]] <- build_bt_slope_table(
            m_tract_btint,
            sample_name = s,
            outcome_name = stem,
            fe_name = "tract",
            block_name = "suppression",
            ref_building_type = bt_ref_suppression,
            building_types_seen = bt_levels_dat,
            group_var = "num_units_bin",
            group_col = "num_units_bin",
            ref_group_col = "ref_num_units_bin"
          )
          suppression_btint_slopes_list[[paste0(key, "_bg")]] <- build_bt_slope_table(
            m_bg_btint,
            sample_name = s,
            outcome_name = stem,
            fe_name = "bg",
            block_name = "suppression",
            ref_building_type = bt_ref_suppression,
            building_types_seen = bt_levels_dat,
            group_var = "num_units_bin",
            group_col = "num_units_bin",
            ref_group_col = "ref_num_units_bin"
          )
          suppression_btint_etable_lines[[key]] <- capture.output(
            etable(m_tract_btint, m_bg_btint, se.below = TRUE, digits = 4, headers = c("Tract FE", "BG FE"))
          )
          suppression_btint_etable_tex_lines[[key]] <- capture.output(
            etable(m_tract_btint, m_bg_btint, se.below = TRUE, digits = 4, tex = TRUE, headers = c("Tract FE", "BG FE"))
          )
          logf("Fitted suppression LPM num_units_bin interaction model: ", key, " | N_tract=", nobs(m_tract_btint),
               ", N_bg=", nobs(m_bg_btint), log_file = log_file)
          rm(m_tract_btint, m_bg_btint, dat, dat_tract, dat_bg); gc()
        }
      }
    }

    # Long-run parcel-level aggregates (pre-COVID only), with cumulative permit/violation counts.
    dt_pre_longrun <- dt[year <= pre_covid_end]
    pid_longrun <- dt_pre_longrun[, .(
    n_periods_pre = .N,
    total_complaints_any = sum(complaints_any_count, na.rm = TRUE),
    total_complaints_severe = sum(complaints_severe_count, na.rm = TRUE),
    total_complaints_non_severe = sum(complaints_non_severe_count, na.rm = TRUE),
    cumulative_permit_count = sum(total_permits, na.rm = TRUE),
    cumulative_violations_count = sum(total_violations, na.rm = TRUE),
    tc_black_c = mean(tc_black_c, na.rm = TRUE),
    tc_female_c = mean(tc_female_c, na.rm = TRUE),
    tc_black_female_c = mean(tc_black_female_c, na.rm = TRUE),
    tc_cov_c = mean(tc_cov_c, na.rm = TRUE),
    tenant_comp_missing = mean(tenant_comp_missing, na.rm = TRUE),
    GEOID = GEOID[1L],
    census_tract = census_tract[1L],
    total_area = mean(total_area, na.rm = TRUE),
    total_units = mean(total_units, na.rm = TRUE),
    market_value = mean(market_value, na.rm = TRUE),
    year_built = mean(year_built, na.rm = TRUE),
    building_type = building_type[1L],
    num_units_bin = num_units_bin[1L],
    year_blt_decade = year_blt_decade[1L],
    num_stories_bin = num_stories_bin[1L],
    exposure_periods_pre = sum(as.integer(
      !is.finite(year_built) | is.na(year_built) | year_built <= 0 | year >= year_built
    ), na.rm = TRUE)
  ), by = PID]
    pid_longrun[, log_total_area := suppressWarnings(log(total_area))]
    pid_longrun[, log_market_value := suppressWarnings(log(market_value))]
    pid_longrun[!is.finite(log_total_area), log_total_area := NA_real_]
    pid_longrun[!is.finite(log_market_value), log_market_value := NA_real_]
    pid_longrun[!is.finite(cumulative_violations_count), cumulative_violations_count := NA_real_]
    pid_longrun[!is.finite(cumulative_permit_count), cumulative_permit_count := NA_real_]
    pid_longrun[!is.finite(tc_black_c), tc_black_c := NA_real_]
    pid_longrun[!is.finite(tc_female_c), tc_female_c := NA_real_]
    pid_longrun[!is.finite(tc_black_female_c), tc_black_female_c := NA_real_]
    pid_longrun[!is.finite(tc_cov_c), tc_cov_c := NA_real_]
    pid_longrun[!is.finite(tenant_comp_missing), tenant_comp_missing := NA_real_]
    pid_longrun[!is.finite(exposure_periods_pre), exposure_periods_pre := NA_real_]
    pid_longrun <- pid_longrun[is.finite(exposure_periods_pre) & exposure_periods_pre > 0]
    pid_longrun[, log_exposure_periods_pre := log(exposure_periods_pre)]

    sup_longrun_rhs <- paste(
      "tc_black_c + tc_female_c + tc_black_female_c + tc_cov_c + tenant_comp_missing",
      "+ cumulative_permit_count + cumulative_violations_count + log_total_area + log_market_value",
      "+ offset(log_exposure_periods_pre)"
    )
    sup_longrun_fe <- " + num_units_bin + building_type + year_blt_decade + num_stories_bin"
    sup_longrun_outcomes <- c(any = "total_complaints_any", severe = "total_complaints_severe", non_severe = "total_complaints_non_severe")

    for (nm in names(sup_longrun_outcomes)) {
      y_col <- unname(sup_longrun_outcomes[[nm]])
      dat_tract <- pid_longrun[!is.na(census_tract) & nzchar(as.character(census_tract))]
      dat_bg <- pid_longrun[!is.na(GEOID) & nzchar(as.character(GEOID))]
      if (nrow(dat_tract) < 100L || nrow(dat_bg) < 100L) next

      fml_tract_long <- as.formula(paste0(
        y_col, " ~ ", sup_longrun_rhs,
        " | census_tract", sup_longrun_fe
      ))
      m_tract_long <- safe_model_eval(
        fepois(fml_tract_long, data = dat_tract, cluster = ~census_tract, lean = TRUE),
        model_label = paste0("suppression_longrun_pre_", nm, "_tract"),
        log_file = log_file
      )

      fml_bg_long <- as.formula(paste0(
        y_col, " ~ ", sup_longrun_rhs,
        " | GEOID", sup_longrun_fe
      ))
      m_bg_long <- safe_model_eval(
        fepois(fml_bg_long, data = dat_bg, cluster = ~GEOID, lean = TRUE),
        model_label = paste0("suppression_longrun_pre_", nm, "_bg"),
        log_file = log_file
      )
      if (is.null(m_tract_long) || is.null(m_bg_long)) {
        rm(m_tract_long, m_bg_long); gc()
        next
      }

      ct_tract_long <- tidy_coeftable(m_tract_long)
      ct_bg_long <- tidy_coeftable(m_bg_long)
      ct_tract_long[, `:=`(sample = "pre", stem = nm, fe = "tract", level = "parcel_longrun_pre")]
      ct_bg_long[, `:=`(sample = "pre", stem = nm, fe = "bg", level = "parcel_longrun_pre")]
      suppression_longrun_coef_list[[paste0(nm, "_tract")]] <- ct_tract_long
      suppression_longrun_coef_list[[paste0(nm, "_bg")]] <- ct_bg_long

      suppression_longrun_etable_lines[[nm]] <- capture.output(
        etable(m_tract_long, m_bg_long, se.below = TRUE, digits = 4, headers = c("Tract FE", "BG FE"))
      )
      suppression_longrun_etable_tex_lines[[nm]] <- capture.output(
        etable(m_tract_long, m_bg_long, se.below = TRUE, digits = 4, tex = TRUE, headers = c("Tract FE", "BG FE"))
      )
      logf("Fitted suppression long-run parcel-aggregate PPML model (pre-COVID): ", nm,
           " | N_tract=", nobs(m_tract_long), ", N_bg=", nobs(m_bg_long), log_file = log_file)
      rm(m_tract_long, m_bg_long); gc()
    }
  } else {
    logf("Skipping legacy suppression LPM / long-run blocks (--only_complaint_suppression=TRUE); keeping severe PPML 3-spec ladder only.", log_file = log_file)
  }
} else {
  logf("Skipping suppression models: GEOID/census_tract not available.", log_file = log_file)
}
if (exists("ev_dt")) rm(ev_dt)
if (exists("pid_year_ev")) rm(pid_year_ev)
gc()
coef_suppression <- rbindlist(suppression_coef_list, use.names = TRUE, fill = TRUE)
etable_suppression_txt <- unlist(suppression_etable_lines)
etable_suppression_tex <- unlist(suppression_etable_tex_lines)
coef_suppression_longrun <- if (length(suppression_longrun_coef_list)) {
  rbindlist(suppression_longrun_coef_list, use.names = TRUE, fill = TRUE)
} else {
  data.table(
    term = character(), estimate = numeric(), std_error = numeric(), t_value = numeric(), p_value = numeric(),
    sample = character(), stem = character(), fe = character(), level = character()
  )
}
etable_suppression_longrun_txt <- if (length(suppression_longrun_etable_lines)) unlist(suppression_longrun_etable_lines) else character()
etable_suppression_longrun_tex <- if (length(suppression_longrun_etable_tex_lines)) unlist(suppression_longrun_etable_tex_lines) else character()
coef_targeting_btint <- if (length(targeting_btint_coef_list)) {
  rbindlist(targeting_btint_coef_list, use.names = TRUE, fill = TRUE)
} else {
  data.table(
    term = character(), estimate = numeric(), std_error = numeric(), t_value = numeric(), p_value = numeric(),
    sample = character(), outcome = character(), fe = character(), block = character(),
    conf_low = numeric(), conf_high = numeric()
  )
}
coef_bldg_year_targeting_btint <- if (length(bldg_year_targeting_btint_coef_list)) {
  rbindlist(bldg_year_targeting_btint_coef_list, use.names = TRUE, fill = TRUE)
} else {
  data.table(
    term = character(), estimate = numeric(), std_error = numeric(), t_value = numeric(), p_value = numeric(),
    sample = character(), outcome = character(), fe = character(), block = character(),
    conf_low = numeric(), conf_high = numeric()
  )
}
coef_suppression_btint <- if (length(suppression_btint_coef_list)) {
  rbindlist(suppression_btint_coef_list, use.names = TRUE, fill = TRUE)
} else {
  data.table(
    term = character(), estimate = numeric(), std_error = numeric(), t_value = numeric(), p_value = numeric(),
    sample = character(), outcome = character(), fe = character(), block = character(),
    conf_low = numeric(), conf_high = numeric()
  )
}
etable_targeting_btint_txt <- if (length(targeting_btint_etable_lines)) unlist(targeting_btint_etable_lines) else character()
etable_targeting_btint_tex <- if (length(targeting_btint_etable_tex_lines)) unlist(targeting_btint_etable_tex_lines) else character()
etable_bldg_year_targeting_btint_txt <- if (length(bldg_year_targeting_btint_etable_lines)) unlist(bldg_year_targeting_btint_etable_lines) else character()
etable_bldg_year_targeting_btint_tex <- if (length(bldg_year_targeting_btint_etable_tex_lines)) unlist(bldg_year_targeting_btint_etable_tex_lines) else character()
etable_suppression_btint_txt <- if (length(suppression_btint_etable_lines)) unlist(suppression_btint_etable_lines) else character()
etable_suppression_btint_tex <- if (length(suppression_btint_etable_tex_lines)) unlist(suppression_btint_etable_tex_lines) else character()
slopes_targeting_btint <- if (length(targeting_btint_slopes_list)) {
  rbindlist(targeting_btint_slopes_list, use.names = TRUE, fill = TRUE)
} else {
  data.table(
    block = character(), sample = character(), outcome = character(), fe = character(),
    group_var = character(), group_level = character(), ref_group_level = character(),
    slope_var = character(), slope_label = character(), num_units_bin = character(), ref_num_units_bin = character(),
    base_term = character(), interaction_term = character(), estimable = logical(),
    estimate = numeric(), std_error = numeric(), stat_value = numeric(), p_value = numeric(),
    conf_low = numeric(), conf_high = numeric()
  )
}
slopes_bldg_year_targeting_btint <- if (length(bldg_year_targeting_btint_slopes_list)) {
  rbindlist(bldg_year_targeting_btint_slopes_list, use.names = TRUE, fill = TRUE)
} else {
  data.table(
    block = character(), sample = character(), outcome = character(), fe = character(),
    group_var = character(), group_level = character(), ref_group_level = character(),
    slope_var = character(), slope_label = character(), num_units_bin = character(), ref_num_units_bin = character(),
    base_term = character(), interaction_term = character(), estimable = logical(),
    estimate = numeric(), std_error = numeric(), stat_value = numeric(), p_value = numeric(),
    conf_low = numeric(), conf_high = numeric()
  )
}
slopes_suppression_btint <- if (length(suppression_btint_slopes_list)) {
  rbindlist(suppression_btint_slopes_list, use.names = TRUE, fill = TRUE)
} else {
  data.table(
    block = character(), sample = character(), outcome = character(), fe = character(),
    group_var = character(), group_level = character(), ref_group_level = character(),
    slope_var = character(), slope_label = character(), num_units_bin = character(), ref_num_units_bin = character(),
    base_term = character(), interaction_term = character(), estimable = logical(),
    estimate = numeric(), std_error = numeric(), stat_value = numeric(), p_value = numeric(),
    conf_low = numeric(), conf_high = numeric()
  )
}
setorderv(slopes_targeting_btint, c("sample", "outcome", "fe", "slope_var", "group_level"))
setorderv(slopes_bldg_year_targeting_btint, c("sample", "outcome", "fe", "slope_var", "group_level"))
setorderv(slopes_suppression_btint, c("sample", "outcome", "fe", "slope_var", "group_level"))
slopes_targeting_btint_txt <- if (nrow(slopes_targeting_btint)) {
  capture.output(print(slopes_targeting_btint[
    , .(sample, outcome, fe, group_var, slope_label, group_level, ref_group_level, estimate, std_error, p_value, estimable)
  ]))
} else character()
slopes_bldg_year_targeting_btint_txt <- if (nrow(slopes_bldg_year_targeting_btint)) {
  capture.output(print(slopes_bldg_year_targeting_btint[
    , .(sample, outcome, fe, group_var, slope_label, group_level, ref_group_level, estimate, std_error, p_value, estimable)
  ]))
} else character()
slopes_suppression_btint_txt <- if (nrow(slopes_suppression_btint)) {
  capture.output(print(slopes_suppression_btint[
    , .(sample, outcome, fe, group_var, slope_label, group_level, ref_group_level, estimate, std_error, p_value, estimable)
  ]))
} else character()

bandwidth_tabs <- compute_bandwidth_tables(dt, max_bw = max_bw, time_unit = time_unit)
band_summary <- bandwidth_tabs$summary
band_status <- bandwidth_tabs$status

# Concatenate per-model etable outputs (models were not stored to save memory)
etable_dist_txt <- if (skip_distlag) {
  c(
    "Distributed-Lag Summary",
    "",
    "Skipped (--skip_distlag=TRUE)."
  )
} else {
  unlist(etable_dist_lines)
}
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
etable_same_period_txt <- if (length(etable_same_period_lines)) unlist(etable_same_period_lines) else character()
etable_same_period_tex <- if (length(etable_same_period_tex_lines)) unlist(etable_same_period_tex_lines) else character()

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
path_suppression_longrun_coef <- file.path(out_dir, paste0(out_prefix, "_suppression_longrun_pre_covid_coefficients.csv"))
path_suppression_longrun_table <- file.path(out_dir, paste0(out_prefix, "_suppression_longrun_pre_covid_model_table.txt"))
path_suppression_longrun_table_tex <- file.path(out_dir, paste0(out_prefix, "_suppression_longrun_pre_covid_model_table.tex"))
path_suppression_severe_3spec_coef <- file.path(out_dir, paste0(out_prefix, "_suppression_severe_ppml_3spec_coefficients.csv"))
path_suppression_severe_3spec_table <- file.path(out_dir, paste0(out_prefix, "_suppression_severe_ppml_3spec_model_table.txt"))
path_suppression_severe_3spec_table_tex <- file.path(out_dir, paste0(out_prefix, "_suppression_severe_ppml_3spec_model_table.tex"))
path_suppression_severe_3spec_sample <- file.path(out_dir, paste0(out_prefix, "_suppression_severe_ppml_3spec_sample.csv"))
path_suppression_severe_3spec_missingness <- file.path(out_dir, paste0(out_prefix, "_suppression_severe_ppml_3spec_pct_black_missingness.csv"))
path_targeting_btint_coef <- file.path(out_dir, paste0(out_prefix, "_targeting_num_units_bin_interactions_coefficients.csv"))
path_targeting_btint_table <- file.path(out_dir, paste0(out_prefix, "_targeting_num_units_bin_interactions_model_table.txt"))
path_targeting_btint_table_tex <- file.path(out_dir, paste0(out_prefix, "_targeting_num_units_bin_interactions_model_table.tex"))
path_targeting_btint_slopes <- file.path(out_dir, paste0(out_prefix, "_targeting_num_units_bin_interactions_slopes.csv"))
path_targeting_btint_slopes_txt <- file.path(out_dir, paste0(out_prefix, "_targeting_num_units_bin_interactions_slopes.txt"))
path_bldg_year_targeting_btint_coef <- file.path(out_dir, paste0(out_prefix, "_bldg_year_targeting_num_units_bin_interactions_coefficients.csv"))
path_bldg_year_targeting_btint_table <- file.path(out_dir, paste0(out_prefix, "_bldg_year_targeting_num_units_bin_interactions_model_table.txt"))
path_bldg_year_targeting_btint_table_tex <- file.path(out_dir, paste0(out_prefix, "_bldg_year_targeting_num_units_bin_interactions_model_table.tex"))
path_bldg_year_targeting_btint_slopes <- file.path(out_dir, paste0(out_prefix, "_bldg_year_targeting_num_units_bin_interactions_slopes.csv"))
path_bldg_year_targeting_btint_slopes_txt <- file.path(out_dir, paste0(out_prefix, "_bldg_year_targeting_num_units_bin_interactions_slopes.txt"))
path_suppression_btint_coef <- file.path(out_dir, paste0(out_prefix, "_suppression_num_units_bin_interactions_coefficients.csv"))
path_suppression_btint_table <- file.path(out_dir, paste0(out_prefix, "_suppression_num_units_bin_interactions_model_table.txt"))
path_suppression_btint_table_tex <- file.path(out_dir, paste0(out_prefix, "_suppression_num_units_bin_interactions_model_table.tex"))
path_suppression_btint_slopes <- file.path(out_dir, paste0(out_prefix, "_suppression_num_units_bin_interactions_slopes.csv"))
path_suppression_btint_slopes_txt <- file.path(out_dir, paste0(out_prefix, "_suppression_num_units_bin_interactions_slopes.txt"))
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
if (nrow(coef_suppression_longrun)) fwrite(coef_suppression_longrun, path_suppression_longrun_coef)
if (length(etable_suppression_longrun_txt)) writeLines(etable_suppression_longrun_txt, con = path_suppression_longrun_table)
if (length(etable_suppression_longrun_tex)) writeLines(etable_suppression_longrun_tex, con = path_suppression_longrun_table_tex)
if (nrow(suppression_severe_3spec_coef)) fwrite(suppression_severe_3spec_coef, path_suppression_severe_3spec_coef)
if (length(suppression_severe_3spec_etable_txt)) writeLines(suppression_severe_3spec_etable_txt, con = path_suppression_severe_3spec_table)
if (length(suppression_severe_3spec_etable_tex)) writeLines(suppression_severe_3spec_etable_tex, con = path_suppression_severe_3spec_table_tex)
if (nrow(suppression_severe_3spec_sample)) fwrite(suppression_severe_3spec_sample, path_suppression_severe_3spec_sample)
if (nrow(suppression_severe_3spec_missingness)) fwrite(suppression_severe_3spec_missingness, path_suppression_severe_3spec_missingness)
if (nrow(coef_targeting_btint)) fwrite(coef_targeting_btint, path_targeting_btint_coef)
if (length(etable_targeting_btint_txt)) writeLines(etable_targeting_btint_txt, con = path_targeting_btint_table)
if (length(etable_targeting_btint_tex)) writeLines(etable_targeting_btint_tex, con = path_targeting_btint_table_tex)
if (nrow(slopes_targeting_btint)) fwrite(slopes_targeting_btint, path_targeting_btint_slopes)
if (length(slopes_targeting_btint_txt)) writeLines(slopes_targeting_btint_txt, con = path_targeting_btint_slopes_txt)
if (nrow(coef_bldg_year_targeting_btint)) fwrite(coef_bldg_year_targeting_btint, path_bldg_year_targeting_btint_coef)
if (length(etable_bldg_year_targeting_btint_txt)) writeLines(etable_bldg_year_targeting_btint_txt, con = path_bldg_year_targeting_btint_table)
if (length(etable_bldg_year_targeting_btint_tex)) writeLines(etable_bldg_year_targeting_btint_tex, con = path_bldg_year_targeting_btint_table_tex)
if (nrow(slopes_bldg_year_targeting_btint)) fwrite(slopes_bldg_year_targeting_btint, path_bldg_year_targeting_btint_slopes)
if (length(slopes_bldg_year_targeting_btint_txt)) writeLines(slopes_bldg_year_targeting_btint_txt, con = path_bldg_year_targeting_btint_slopes_txt)
if (nrow(coef_suppression_btint)) fwrite(coef_suppression_btint, path_suppression_btint_coef)
if (length(etable_suppression_btint_txt)) writeLines(etable_suppression_btint_txt, con = path_suppression_btint_table)
if (length(etable_suppression_btint_tex)) writeLines(etable_suppression_btint_tex, con = path_suppression_btint_table_tex)
if (nrow(slopes_suppression_btint)) fwrite(slopes_suppression_btint, path_suppression_btint_slopes)
if (length(slopes_suppression_btint_txt)) writeLines(slopes_suppression_btint_txt, con = path_suppression_btint_slopes_txt)

theme_use <- theme_minimal()

if (!skip_distlag && nrow(coef_dist)) {
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
}

distlag_plot_note <- if (skip_distlag) {
  "Distributed lag skipped; no dist-lag plot written."
} else if (!nrow(coef_dist)) {
  "Distributed lag ran but produced no coefficient rows; no dist-lag plot written."
} else {
  paste0("Wrote dist-lag plot: ", path_dist_plot)
}

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
  paste0("skip_distlag: ", skip_distlag),
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
  if (nrow(coef_targeting_btint)) paste0("Wrote targeting num_units_bin interaction coefficients CSV: ", path_targeting_btint_coef) else "Targeting num_units_bin interaction models skipped",
  if (length(etable_targeting_btint_tex)) paste0("Wrote targeting num_units_bin interaction model table (tex): ", path_targeting_btint_table_tex) else NULL,
  if (nrow(slopes_targeting_btint)) paste0("Wrote targeting num_units_bin interaction slopes CSV: ", path_targeting_btint_slopes) else "Targeting num_units_bin interaction slope tables skipped",
  if (nrow(coef_bldg_year_targeting)) paste0("Wrote building-year targeting coefficients CSV: ", path_bldg_year_targeting_coef) else "Building-year targeting models skipped",
  if (length(etable_bldg_year_targeting_tex)) paste0("Wrote building-year targeting model table (tex): ", path_bldg_year_targeting_table_tex) else NULL,
  if (nrow(coef_bldg_year_targeting_btint)) paste0("Wrote building-year targeting num_units_bin interaction coefficients CSV: ", path_bldg_year_targeting_btint_coef) else "Building-year targeting num_units_bin interaction models skipped",
  if (length(etable_bldg_year_targeting_btint_tex)) paste0("Wrote building-year targeting num_units_bin interaction model table (tex): ", path_bldg_year_targeting_btint_table_tex) else NULL,
  if (nrow(slopes_bldg_year_targeting_btint)) paste0("Wrote building-year targeting num_units_bin interaction slopes CSV: ", path_bldg_year_targeting_btint_slopes) else "Building-year targeting num_units_bin interaction slope tables skipped",
  if (nrow(coef_suppression)) paste0("Wrote suppression coefficients CSV: ", path_suppression_coef) else "Suppression models skipped",
  if (length(etable_suppression_tex)) paste0("Wrote suppression model table (tex): ", path_suppression_table_tex) else NULL,
  if (nrow(coef_suppression_longrun)) paste0("Wrote suppression long-run pre-COVID coefficients CSV: ", path_suppression_longrun_coef) else "Suppression long-run pre-COVID models skipped",
  if (length(etable_suppression_longrun_tex)) paste0("Wrote suppression long-run pre-COVID model table (tex): ", path_suppression_longrun_table_tex) else NULL,
  if (nrow(suppression_severe_3spec_coef)) paste0("Wrote severe complaint PPML 3-spec coefficients CSV: ", path_suppression_severe_3spec_coef) else "Severe complaint PPML 3-spec ladder skipped",
  if (length(suppression_severe_3spec_etable_tex)) paste0("Wrote severe complaint PPML 3-spec model table (tex): ", path_suppression_severe_3spec_table_tex) else NULL,
  if (nrow(suppression_severe_3spec_missingness)) paste0("Wrote severe complaint PPML 3-spec pct_black missingness CSV: ", path_suppression_severe_3spec_missingness) else NULL,
  if (nrow(coef_suppression_btint)) paste0("Wrote suppression num_units_bin interaction coefficients CSV: ", path_suppression_btint_coef) else "Suppression num_units_bin interaction models skipped",
  if (length(etable_suppression_btint_tex)) paste0("Wrote suppression num_units_bin interaction model table (tex): ", path_suppression_btint_table_tex) else NULL,
  if (nrow(slopes_suppression_btint)) paste0("Wrote suppression num_units_bin interaction slopes CSV: ", path_suppression_btint_slopes) else "Suppression num_units_bin interaction slope tables skipped",
  distlag_plot_note,
  lpdid_plot_note
)
writeLines(qa_lines, con = path_qa)

logf("Wrote outputs to: ", out_dir, log_file = log_file)
logf("=== Finished retaliatory-evictions.r ===", log_file = log_file)
