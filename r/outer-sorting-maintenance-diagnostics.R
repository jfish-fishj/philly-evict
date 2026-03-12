## ============================================================
## outer-sorting-maintenance-diagnostics.R
## ============================================================
## Purpose: Isolate the maintenance block by evaluating maintenance
## moments on a fixed panel while varying m0 and bM_C.
## ============================================================

safe_fepois_coef <- function(dt, dep_var, rhs_var, fe_var, offset_var) {
  fit <- tryCatch(
    fixest::fepois(
      stats::as.formula(paste0(dep_var, " ~ ", rhs_var, " | ", fe_var)),
      offset = stats::as.formula(paste0("~log(", offset_var, ")")),
      data = dt
    ),
    error = function(e) NULL
  )
  if (is.null(fit)) return(NA_real_)
  co <- stats::coef(fit)
  if (!(rhs_var %in% names(co))) return(NA_real_)
  unname(co[[rhs_var]])
}

build_outer_sorting_maintenance_diagnostics <- function(cfg, log_file = NULL) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")
  if (!requireNamespace("fixest", quietly = TRUE)) stop("Missing package: fixest")

  out_dir <- p_out(cfg, "outer_sorting")
  best_panel_path <- file.path(out_dir, "outer_sorting_calibration_best_panel.csv")
  best_param_path <- file.path(out_dir, "outer_sorting_calibration_best_params.csv")
  best_moment_path <- file.path(out_dir, "outer_sorting_calibration_best_moments.csv")

  if (!file.exists(best_panel_path)) stop("Missing calibration best panel: ", best_panel_path)
  if (!file.exists(best_param_path)) stop("Missing calibration best params: ", best_param_path)
  if (!file.exists(best_moment_path)) stop("Missing calibration best moments: ", best_moment_path)

  target_key <- as.character(cfg$run$outer_sorting$calibration$target_product_key)
  target_path <- p_product(cfg, target_key)
  if (!file.exists(target_path)) stop("Missing target moments: ", target_path)

  panel <- data.table::fread(best_panel_path)
  params <- data.table::fread(best_param_path)
  best_mom <- data.table::fread(best_moment_path)
  target_mom <- data.table::fread(target_path)

  assert_has_cols(panel, c("b", "nhood", "units", "theta_bad", "c_rate", "cbar", "m_rate", "mbar"), "maintenance diagnostic panel")
  assert_unique(panel, "b", "maintenance diagnostic panel")
  assert_has_cols(params, c("param", "value"), "maintenance diagnostic params")

  get_param <- function(name) {
    val <- params[param == name, value]
    if (!length(val)) stop("Missing parameter in calibration best params: ", name)
    as.numeric(val[1])
  }

  get_first_col <- function(dt, candidates) {
    for (nm in candidates) {
      if (nm %in% names(dt) && length(dt[[nm]]) > 0L) {
        val <- dt[[nm]][1]
        if (length(val)) return(as.numeric(val))
      }
    }
    NA_real_
  }

  m0_best <- get_param("m0")
  bM_theta_best <- get_param("bM_theta")
  bM_C_best <- get_param("bM_C")

  current_coef_counts <- safe_fepois_coef(
    dt = panel,
    dep_var = "M",
    rhs_var = "c_rate",
    fe_var = "nhood",
    offset_var = "units"
  )

  base_grid_m0 <- stats::quantile(
    c(m0_best, 0.02, 0.04, 0.08, 0.12, 0.20, 0.30),
    probs = seq(0, 1, length.out = 7),
    names = FALSE
  )
  grid_m0 <- sort(unique(as.numeric(base_grid_m0)))
  grid_bM_C <- seq(-0.5, 3.0, by = 0.25)

  scenarios <- data.table::data.table(
    scenario = c("fixed_c_rate", "fixed_cbar", "fixed_c_rate_no_reverse_feedback"),
    c_source = c("c_rate", "cbar", "c_rate"),
    bC_M_note = c("current_equilibrium_panel", "current_expected_panel", "interpreted_without_reverse_feedback")
  )

  grid_rows <- vector("list", length = length(grid_m0) * length(grid_bM_C) * nrow(scenarios))
  idx <- 0L

  for (sc_i in seq_len(nrow(scenarios))) {
    c_source <- scenarios$c_source[sc_i]
    c_vec <- as.numeric(panel[[c_source]])
    for (m0_try in grid_m0) {
      for (bM_C_try in grid_bM_C) {
        idx <- idx + 1L
        mbar_cf <- m0_try * exp(bM_theta_best * panel$theta_bad + bM_C_try * log1p(c_vec))
        dt_cf <- data.table::data.table(
          nhood = panel$nhood,
          units = panel$units,
          c_rate = panel$c_rate,
          cbar = panel$cbar,
          M_cf = panel$units * mbar_cf,
          mbar_cf = mbar_cf
        )

        coef_cf <- safe_fepois_coef(
          dt = dt_cf,
          dep_var = "M_cf",
          rhs_var = "c_rate",
          fe_var = "nhood",
          offset_var = "units"
        )

        grid_rows[[idx]] <- data.table::data.table(
          scenario = scenarios$scenario[sc_i],
          c_source = c_source,
          bC_M_note = scenarios$bC_M_note[sc_i],
          m0 = m0_try,
          bM_C = bM_C_try,
          mean_m_rate_cf = stats::weighted.mean(mbar_cf, w = panel$units),
          zero_share_m_cf = mean(mbar_cf <= 1e-8),
          beta_maint_on_c_rate_cf = coef_cf
        )
      }
    }
  }

  grid_dt <- data.table::rbindlist(grid_rows)

  summary_dt <- data.table::data.table(
    source = c("empirical_target", "baseline_sim", "calibrated_best"),
    mean_m_rate = c(
      as.numeric(target_mom$mean_m_rate[1]),
      NA_real_,
      as.numeric(best_mom$mean_m_rate[1])
    ),
    zero_share_m = c(
      as.numeric(target_mom$zero_share_m[1]),
      NA_real_,
      as.numeric(best_mom$zero_share_m[1])
    ),
    beta_maint_on_c_rate = c(
      as.numeric(target_mom$beta_maint_on_c_rate[1]),
      NA_real_,
      as.numeric(best_mom$beta_maint_on_c_rate[1])
    )
  )

  sim_mom_path <- p_product(cfg, "outer_sorting_sim_moments")
  if (file.exists(sim_mom_path)) {
    sim_mom <- data.table::fread(sim_mom_path)
    summary_dt[source == "baseline_sim", `:=`(
      mean_m_rate = get_first_col(sim_mom, c("mean_m_rate")),
      zero_share_m = get_first_col(sim_mom, c("zero_share_m")),
      beta_maint_on_c_rate = get_first_col(sim_mom, c("beta_maint_on_c_rate", "beta_maint_on_log1p_c_rate", "beta_maint_on_log1p_cbar"))
    )]
  }

  param_dt <- data.table::data.table(
    metric = c(
      "m0_best",
      "bM_theta_best",
      "bM_C_best",
      "current_panel_coef_counts",
      "weighted_mean_mbar_best_panel",
      "weighted_mean_cbar_best_panel",
      "corr_log1p_c_rate_m_rate_best_panel",
      "corr_log1p_c_rate_mbar_best_panel"
    ),
    value = c(
      m0_best,
      bM_theta_best,
      bM_C_best,
      current_coef_counts,
      stats::weighted.mean(panel$mbar, panel$units),
      stats::weighted.mean(panel$cbar, panel$units),
      stats::cor(log1p(panel$c_rate), panel$m_rate),
      stats::cor(log1p(panel$c_rate), panel$mbar)
    )
  )

  if (!is.null(log_file) && exists("logf", mode = "function")) {
    logf("build_outer_sorting_maintenance_diagnostics: grid rows=", nrow(grid_dt),
         ", best bM_C=", signif(bM_C_best, 4),
         ", best maintenance coefficient=", signif(current_coef_counts, 4),
         log_file = log_file)
  }

  list(summary = summary_dt, parameters = param_dt, grid = grid_dt)
}
