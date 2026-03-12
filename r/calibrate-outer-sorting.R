## ============================================================
## calibrate-outer-sorting.R
## ============================================================
## Purpose: Calibrate a subset of outer-sorting parameters by
## minimizing weighted distance between simulated and target moments.
## ============================================================

default_outer_calibration_moments <- function() {
  c(
    "mean_f_rate",
    "mean_m_rate",
    "mean_c_rate",
    "mean_m_rate_pos",
    "m_rate_pos_p50",
    "m_rate_pos_p90",
    "nhood_mean_s_sd",
    "nhood_mean_s_p90_p10",
    "nhood_mean_f_rate_sd",
    "nhood_mean_f_rate_p90_p10",
    "sorting_coef_high_filing_nhood",
    "sorting_coef_high_bin_nhood",
    "beta_ppml_s",
    "beta_ppml_s_x_high_filing",
    "beta_maint_on_c_rate",
    "filing_resid_sd_nhood_fe"
  )
}

default_outer_calibration_weights <- function() {
  c(
    mean_f_rate = 10,
    mean_m_rate = 10,
    mean_c_rate = 10,
    mean_m_rate_pos = 5,
    m_rate_pos_p50 = 5,
    m_rate_pos_p90 = 5,
    nhood_mean_s_sd = 8,
    nhood_mean_s_p90_p10 = 8,
    nhood_mean_f_rate_sd = 5,
    nhood_mean_f_rate_p90_p10 = 5,
    sorting_coef_high_filing_nhood = 5,
    sorting_coef_high_bin_nhood = 5,
    beta_ppml_s = 2,
    beta_ppml_s_x_high_filing = 2,
    beta_maint_on_c_rate = 3,
    filing_resid_sd_nhood_fe = 5
  )
}

default_outer_calibration_bounds <- function() {
  list(
    Dalph = c(0.05, 50),
    sigma_eta_sort = c(0, 5),
    p_bad_shape1 = c(0.2, 20),
    p_bad_shape2 = c(0.2, 20),
    aF = c(-8, 2),
    bF_theta = c(-2, 2),
    bF_delta = c(0, 10),
    aC = c(-6, 2),
    bC_theta = c(-2, 2),
    bC_s_bad = c(-2, 2),
    bC_old = c(-2, 2),
    bC_new = c(-2, 2),
    bC_M = c(-2, 0),
    m0 = c(1e-4, 5),
    bM_theta = c(-2, 2),
    bM_old = c(-2, 2),
    bM_new = c(-2, 2),
    bM_C = c(0, 2)
  )
}

build_outer_calibration_settings <- function(cfg) {
  run_cfg <- cfg$run %||% list()
  outer_cfg <- run_cfg$outer_sorting %||% list()
  cal_cfg <- outer_cfg$calibration %||% list()

  params <- as.character(cal_cfg$params %||% c(
    "Dalph", "sigma_eta_sort", "p_bad_shape1", "p_bad_shape2",
    "aF", "bF_theta", "bF_delta",
    "aC", "bC_theta", "bC_s_bad", "bC_old", "bC_new", "bC_M",
    "m0", "bM_theta", "bM_old", "bM_new", "bM_C"
  ))
  moment_cols <- as.character(cal_cfg$moment_cols %||% default_outer_calibration_moments())

  weights <- default_outer_calibration_weights()
  w_cfg <- cal_cfg$weights %||% list()
  if (length(w_cfg) > 0L) {
    for (nm in names(w_cfg)) weights[nm] <- as.numeric(w_cfg[[nm]])
  }
  weights <- weights[moment_cols]
  if (any(!is.finite(weights) | weights <= 0)) {
    stop("Calibration weights must be finite positive numbers for all selected moment_cols.")
  }

  bounds <- default_outer_calibration_bounds()
  b_cfg <- cal_cfg$bounds %||% list()
  if (length(b_cfg) > 0L) {
    for (nm in names(b_cfg)) bounds[[nm]] <- as.numeric(b_cfg[[nm]])
  }
  for (p in params) {
    if (is.null(bounds[[p]]) || length(bounds[[p]]) != 2L || !all(is.finite(bounds[[p]]))) {
      stop("Missing or invalid bounds for calibration parameter: ", p)
    }
  }

  list(
    params = params,
    moment_cols = moment_cols,
    weights = weights,
    bounds = bounds,
    maxit = as.integer(cal_cfg$maxit %||% 300L),
    reltol = as.numeric(cal_cfg$reltol %||% 1e-4),
    grid_Dalph = as.numeric(cal_cfg$grid_Dalph %||% c(2, 4, 6, 8, 10)),
    target_product_key = as.character(cal_cfg$target_product_key %||% "outer_sorting_sim_moments"),
    high_filing_threshold = as.numeric(cal_cfg$high_filing_threshold %||% outer_cfg$high_filing_threshold %||% NA_real_)
  )
}

load_outer_target_moments <- function(cfg, settings, target_dt_override = NULL) {
  target_path <- NULL
  if (!is.null(target_dt_override)) {
    target_dt <- data.table::as.data.table(target_dt_override)
  } else {
    target_path <- p_product(cfg, settings$target_product_key)
    if (!file.exists(target_path)) {
      stop("Target moments file not found: ", target_path,
           ". Run r/run-outer-sorting-sim.R first or set run.outer_sorting.calibration.target_product_key.")
    }
    target_dt <- data.table::fread(target_path)
  }
  if (nrow(target_dt) < 1L) {
    if (is.null(target_path)) stop("Target moments override is empty.")
    stop("Target moments file is empty: ", target_path)
  }

  miss <- setdiff(settings$moment_cols, names(target_dt))
  if (length(miss) > 0L) {
    stop("Target moments missing required columns: ", paste(miss, collapse = ", "))
  }

  moment_cols <- settings$moment_cols
  target_vec <- as.numeric(target_dt[1, ..moment_cols])

  threshold_from_target <- NA_real_
  if ("high_filing_threshold" %in% names(target_dt)) {
    threshold_from_target <- as.numeric(target_dt[1, high_filing_threshold])
  } else if ("filing_p85" %in% names(target_dt)) {
    threshold_from_target <- as.numeric(target_dt[1, filing_p85])
  }

  list(
    target_vec = target_vec,
    threshold_from_target = threshold_from_target
  )
}

calibrate_outer_sorting <- function(cfg, log_file = NULL, target_dt_override = NULL) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")

  settings <- build_outer_calibration_settings(cfg)
  par_base <- build_outer_sorting_params(cfg)

  for (p in settings$params) {
    if (is.null(par_base[[p]])) stop("Calibration parameter not found in outer params: ", p)
  }

  target_loaded <- load_outer_target_moments(cfg, settings, target_dt_override = target_dt_override)
  target_vec <- target_loaded$target_vec
  names(target_vec) <- settings$moment_cols
  weight_vec <- settings$weights

  high_filing_threshold <- settings$high_filing_threshold
  if (!is.finite(high_filing_threshold) || high_filing_threshold < 0) {
    high_filing_threshold <- target_loaded$threshold_from_target
  }
  if (!is.finite(high_filing_threshold) || high_filing_threshold < 0) {
    stop("No valid high_filing_threshold provided for calibration. Set run.outer_sorting.calibration.high_filing_threshold or include high_filing_threshold in target moments.")
  }

  if (is.null(target_dt_override) &&
      identical(settings$target_product_key, "outer_sorting_sim_moments") &&
      exists("logf", mode = "function") && !is.null(log_file)) {
    logf("Calibration target is simulation-generated moments (self-calibration check), not empirical data.",
         log_file = log_file)
  }

  eval_id <- 0L
  trace_rows <- vector("list", 0L)

  objective <- function(x) {
    eval_id <<- eval_id + 1L

    par_try <- par_base
    names(x) <- settings$params

    penalty <- 0
    for (p in settings$params) {
      lb <- settings$bounds[[p]][1]
      ub <- settings$bounds[[p]][2]
      v <- as.numeric(x[[p]])
      if (!is.finite(v)) {
        penalty <- penalty + 1e9
        v <- par_base[[p]]
      }
      if (v < lb) {
        penalty <- penalty + 1e6 + 1e6 * (lb - v)^2
        v <- lb
      }
      if (v > ub) {
        penalty <- penalty + 1e6 + 1e6 * (v - ub)^2
        v <- ub
      }
      par_try[[p]] <- v
    }

    sim_res <- simulate_outer_sorting(par_try, log_file = NULL, warn_if_not_converged = FALSE)
    if (!isTRUE(sim_res$diagnostics$converged[1])) {
      penalty <- penalty + 1e5
    }
    mom_dt <- compute_outer_sorting_moments(
      sim_res$panel,
      high_filing_threshold = high_filing_threshold,
      log_file = NULL
    )
    moment_cols <- settings$moment_cols
    sim_vec <- as.numeric(mom_dt[1, ..moment_cols])
    names(sim_vec) <- moment_cols
    bad_sim <- !is.finite(sim_vec)
    if (any(bad_sim)) {
      penalty <- penalty + 1e6 * sum(bad_sim)
      sim_vec[bad_sim] <- target_vec[bad_sim]
    }

    dif <- target_vec - sim_vec
    obj <- sum(weight_vec * (dif^2), na.rm = TRUE) + penalty

    trace_row <- data.table::data.table(
      eval_id = eval_id,
      objective = obj
    )
    for (p in settings$params) trace_row[[p]] <- par_try[[p]]
    for (m in settings$moment_cols) {
      trace_row[[paste0("target_", m)]] <- target_vec[[m]]
      trace_row[[paste0("sim_", m)]] <- sim_vec[[m]]
    }
    trace_rows[[length(trace_rows) + 1L]] <<- trace_row

    obj
  }

  x0 <- unlist(par_base[settings$params], use.names = TRUE)

  # Coarse Dalph grid for a better local-start (when Dalph is calibrated).
  if ("Dalph" %in% settings$params) {
    start_grid <- unique(settings$grid_Dalph)
    start_grid <- start_grid[is.finite(start_grid)]
    if (length(start_grid) > 0L) {
      best_grid_obj <- Inf
      best_grid_x <- x0
      for (d in start_grid) {
        x_try <- x0
        x_try["Dalph"] <- d
        o <- objective(x_try)
        if (is.finite(o) && o < best_grid_obj) {
          best_grid_obj <- o
          best_grid_x <- x_try
        }
      }
      x0 <- best_grid_x
    }
  }

  obj_start <- objective(x0)
  if (is.finite(obj_start) && obj_start < 1e-12) {
    opt <- list(
      par = x0,
      convergence = 0L,
      message = "Skipped local optimization: starting objective below tolerance."
    )
  } else {
    opt <- stats::optim(
      par = x0,
      fn = objective,
      method = "Nelder-Mead",
      control = list(
        maxit = settings$maxit,
        reltol = settings$reltol
      )
    )
  }

  # Build best-parameter object with in-bounds projection.
  x_best <- opt$par
  names(x_best) <- settings$params
  par_best <- par_base
  for (p in settings$params) {
    lb <- settings$bounds[[p]][1]
    ub <- settings$bounds[[p]][2]
    par_best[[p]] <- min(max(as.numeric(x_best[[p]]), lb), ub)
  }

  best_sim <- simulate_outer_sorting(par_best, log_file = NULL, warn_if_not_converged = FALSE)
  if (!isTRUE(best_sim$diagnostics$converged[1])) {
    warning("Best-parameter simulation did not converge under current max_iter/tol settings.")
  }
  best_mom <- compute_outer_sorting_moments(
    best_sim$panel,
    high_filing_threshold = high_filing_threshold,
    log_file = NULL
  )

  moment_cols <- settings$moment_cols
  moment_compare <- data.table::data.table(
    moment = moment_cols,
    target = as.numeric(target_vec[moment_cols]),
    simulated = as.numeric(best_mom[1, ..moment_cols]),
    weight = as.numeric(weight_vec[moment_cols])
  )
  moment_compare[, diff := target - simulated]
  moment_compare[, weighted_sq_error := weight * diff^2]

  best_params_dt <- data.table::data.table(
    param = names(par_best),
    value = vapply(par_best, function(v) {
      if (length(v) == 1L && is.numeric(v)) return(as.numeric(v))
      NA_real_
    }, numeric(1)),
    value_text = vapply(par_best, function(v) paste(v, collapse = ","), character(1))
  )
  best_params_dt[, calibrated := param %in% settings$params]

  trace_dt <- data.table::rbindlist(trace_rows, fill = TRUE)
  data.table::setorderv(trace_dt, c("objective", "eval_id"))

  opt_summary <- data.table::data.table(
    convergence_code = opt$convergence,
    convergence_message = opt$message %||% "",
    objective_best = sum(moment_compare$weighted_sq_error, na.rm = TRUE),
    n_evaluations = nrow(trace_dt)
  )

  if (!is.null(log_file) && exists("logf", mode = "function")) {
    logf("calibrate_outer_sorting: convergence_code=", opt$convergence,
         ", objective_best=", sprintf("%.6f", opt_summary$objective_best),
         ", n_evaluations=", nrow(trace_dt),
         log_file = log_file)
  }

  list(
    settings = settings,
    best_params = best_params_dt,
    best_panel = best_sim$panel,
    best_diagnostics = best_sim$diagnostics,
    best_moments = best_mom,
    target_moments = data.table::data.table(moment = names(target_vec), target = as.numeric(target_vec)),
    moment_comparison = moment_compare,
    optimization_trace = trace_dt,
    optimization_summary = opt_summary
  )
}
