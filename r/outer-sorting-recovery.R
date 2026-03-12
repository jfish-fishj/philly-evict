## ============================================================
## outer-sorting-recovery.R
## ============================================================
## Purpose: Monte Carlo recovery helpers for outer sorting calibration.
## ============================================================

build_outer_sorting_recovery_settings <- function(cfg) {
  rec_cfg <- cfg$run$outer_sorting$recovery %||% list()
  params <- as.character(rec_cfg$params %||% c("Dalph", "bF_delta", "bF_theta", "bC_s_bad", "bM_C"))
  bounds <- rec_cfg$truth_bounds %||% list(
    Dalph = c(3, 10),
    bF_delta = c(0.3, 2.5),
    bF_theta = c(0.2, 1.2),
    bC_s_bad = c(-0.8, 0.2),
    bM_C = c(0.02, 0.4)
  )
  list(
    n_rep = as.integer(rec_cfg$n_rep %||% 20L),
    seed = as.integer(rec_cfg$seed %||% 20260307L),
    params = params,
    truth_bounds = bounds
  )
}

sample_outer_sorting_truths <- function(par_base, settings) {
  set.seed(settings$seed)
  truths <- vector("list", settings$n_rep)
  for (i in seq_len(settings$n_rep)) {
    par_i <- par_base
    for (p in settings$params) {
      bnd <- as.numeric(settings$truth_bounds[[p]])
      if (length(bnd) != 2L || !all(is.finite(bnd))) stop("Invalid truth_bounds for recovery parameter: ", p)
      par_i[[p]] <- stats::runif(1L, min = bnd[1], max = bnd[2])
    }
    truths[[i]] <- par_i
  }
  truths
}

run_outer_sorting_recovery <- function(cfg, log_file = NULL) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")

  settings <- build_outer_sorting_recovery_settings(cfg)
  par_base <- build_outer_sorting_params(cfg)
  truths <- sample_outer_sorting_truths(par_base, settings)

  rep_rows <- vector("list", length(truths))
  summary_rows <- vector("list", 0L)

  for (i in seq_along(truths)) {
    truth_par <- truths[[i]]
    truth_sim <- simulate_outer_sorting(truth_par, log_file = NULL, warn_if_not_converged = FALSE)
    truth_mom <- compute_outer_sorting_moments(
      truth_sim$panel,
      high_filing_threshold = truth_par$high_filing_threshold,
      log_file = NULL
    )
    fit <- calibrate_outer_sorting(cfg, log_file = NULL, target_dt_override = truth_mom)

    rep_dt <- data.table::data.table(
      replication = i,
      truth_converged = truth_sim$diagnostics$converged[1],
      est_convergence_code = fit$optimization_summary$convergence_code[1],
      est_objective = fit$optimization_summary$objective_best[1]
    )
    for (p in settings$params) {
      truth_val <- as.numeric(truth_par[[p]])
      est_val <- as.numeric(fit$best_params[param == p, value][1])
      rep_dt[[paste0("truth_", p)]] <- truth_val
      rep_dt[[paste0("est_", p)]] <- est_val
      rep_dt[[paste0("bias_", p)]] <- est_val - truth_val
    }
    rep_rows[[i]] <- rep_dt
  }

  rep_out <- data.table::rbindlist(rep_rows, fill = TRUE)
  for (p in settings$params) {
    summary_rows[[length(summary_rows) + 1L]] <- data.table::data.table(
      parameter = p,
      mean_truth = mean(rep_out[[paste0("truth_", p)]], na.rm = TRUE),
      mean_est = mean(rep_out[[paste0("est_", p)]], na.rm = TRUE),
      mean_bias = mean(rep_out[[paste0("bias_", p)]], na.rm = TRUE),
      rmse = sqrt(mean(rep_out[[paste0("bias_", p)]]^2, na.rm = TRUE)),
      corr_truth_est = stats::cor(rep_out[[paste0("truth_", p)]], rep_out[[paste0("est_", p)]], use = "complete.obs")
    )
  }
  summary_out <- data.table::rbindlist(summary_rows, fill = TRUE)

  if (!is.null(log_file) && exists("logf", mode = "function")) {
    logf("run_outer_sorting_recovery: n_rep=", settings$n_rep,
         ", mean_objective=", round(mean(rep_out$est_objective, na.rm = TRUE), 6),
         log_file = log_file)
  }

  list(replications = rep_out, summary = summary_out)
}
