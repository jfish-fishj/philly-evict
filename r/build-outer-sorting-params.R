## ============================================================
## build-outer-sorting-params.R
## ============================================================
## Purpose: Build baseline parameter list for the outer sorting simulator
## from config defaults and optional config overrides.
##
## Baseline normalization:
##   x_b = \bar f_b
##   s_b^*(xi) = Lambda(xi + Delta_alpha * x_b + eta_{n(b)})
## ============================================================

build_outer_sorting_empirical_buildings <- function(cfg, outer_cfg) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")

  empirical_cfg <- outer_cfg$empirical %||% list()
  units_key <- as.character(outer_cfg$units_empirical_product_key %||% empirical_cfg$bldg_panel_product_key %||% "bldg_panel_blp")
  year_from <- as.integer(outer_cfg$units_empirical_year_from %||% empirical_cfg$year_from %||% 2011L)
  year_to <- as.integer(outer_cfg$units_empirical_year_to %||% empirical_cfg$year_to %||% 2019L)
  year_built_col <- as.character(outer_cfg$units_empirical_year_built_col %||% empirical_cfg$year_built_col %||% "year_built")

  path <- p_product(cfg, units_key)
  sel_cols <- c("PID", "year", "total_units", year_built_col)
  dt <- data.table::fread(path, select = sel_cols)
  data.table::setnames(dt, old = year_built_col, new = "year_built")

  assert_has_cols(dt, c("PID", "year", "total_units", "year_built"), "outer sorting empirical units input")

  dt <- data.table::as.data.table(dt)
  dt[, year_built := suppressWarnings(as.integer(year_built))]
  dt <- dt[
    year >= year_from &
      year <= year_to &
      (is.na(year_built) | year_built <= year_to) &
      is.finite(total_units) &
      total_units > 0
  ]
  if (nrow(dt) == 0L) {
    stop("No valid PID-year rows remain for empirical unit draws in the requested window.")
  }

  yr_check <- dt[, .(n_year_built = data.table::uniqueN(year_built[!is.na(year_built)])), by = PID]
  if (yr_check[n_year_built > 1L, .N] > 0L) {
    stop("Some PIDs have multiple year_built values in the empirical unit source window.")
  }

  pid_dt <- dt[, .(
    observed_years = data.table::uniqueN(year),
    units_unit_years = sum(total_units, na.rm = TRUE),
    year_built = year_built[which.max(!is.na(year_built))]
  ), by = PID]
  pid_dt <- pid_dt[observed_years > 0L]
  pid_dt[, avg_annual_units := units_unit_years / observed_years]
  pid_dt <- pid_dt[is.finite(avg_annual_units) & avg_annual_units > 0]
  if (nrow(pid_dt) == 0L) {
    stop("Empirical unit draw pool is empty after filtering.")
  }

  list(
    draws = pid_dt[, .(avg_annual_units, year_built)],
    source_product_key = units_key,
    year_from = year_from,
    year_to = year_to
  )
}

build_outer_sorting_params <- function(cfg) {
  run_cfg <- cfg$run %||% list()
  outer_cfg <- run_cfg$outer_sorting %||% list()

  units_draw_mode <- as.character(outer_cfg$units_draw_mode %||% "empirical_resample")

  par <- list(
    # Dimensions / seed
    B = as.integer(outer_cfg$B %||% 10000L),
    Nhood = as.integer(outer_cfg$Nhood %||% 300L),
    seed = as.integer(outer_cfg$seed %||% (run_cfg$seed %||% 123L)),
    sim_year_from = as.integer(outer_cfg$sim_year_from %||% outer_cfg$units_empirical_year_from %||% (outer_cfg$empirical %||% list())$year_from %||% 2011L),
    sim_year_to = as.integer(outer_cfg$sim_year_to %||% outer_cfg$units_empirical_year_to %||% (outer_cfg$empirical %||% list())$year_to %||% 2019L),

    # Anchored tenant-side parameters (held fixed in first pass)
    delta_L = as.numeric(outer_cfg$delta_L %||% 0.01),
    delta_H = as.numeric(outer_cfg$delta_H %||% 0.50),

    # Fixed citywide group-1 share
    mu = as.numeric(outer_cfg$mu %||% 0.30),

    # Fixed-point numerics
    lam = as.numeric(outer_cfg$lam %||% 0.40),
    tol = as.numeric(outer_cfg$tol %||% 1e-6),
    max_iter = as.integer(outer_cfg$max_iter %||% 300L),
    mass_tol_abs = as.numeric(outer_cfg$mass_tol_abs %||% 1e-6),

    # Built environment
    p_bad_shape1 = as.numeric(outer_cfg$p_bad_shape1 %||% 2.0),
    p_bad_shape2 = as.numeric(outer_cfg$p_bad_shape2 %||% 6.0),
    units_draw_mode = units_draw_mode,
    units_bin_values = as.numeric(outer_cfg$units_bin_values %||% c(1, 2, 3, 4, 7, 15, 35, 75)),
    units_bin_probs = as.numeric(outer_cfg$units_bin_probs %||% c(0.28, 0.16, 0.10, 0.08, 0.14, 0.12, 0.08, 0.04)),

    # Sorting strength
    Dalph = as.numeric(outer_cfg$Dalph %||% 6.0),
    sigma_eta_sort = as.numeric(outer_cfg$sigma_eta_sort %||% 0.60),
    high_filing_threshold = as.numeric(outer_cfg$high_filing_threshold %||% 0.15),
    old_bldg_cutoff_year = as.integer(outer_cfg$old_bldg_cutoff_year %||% 1940L),
    new_bldg_cutoff_year = as.integer(outer_cfg$new_bldg_cutoff_year %||% 2000L),

    # Outcome block parameters
    m0 = as.numeric(outer_cfg$m0 %||% exp(-1.70)),
    bM_theta = as.numeric(outer_cfg$bM_theta %||% -0.20),
    bM_old = as.numeric(outer_cfg$bM_old %||% 0.15),
    bM_new = as.numeric(outer_cfg$bM_new %||% 0.25),
    bM_C = as.numeric(outer_cfg$bM_C %||% 0.12),

    aC = as.numeric(outer_cfg$aC %||% -2.10),
    bC_theta = as.numeric(outer_cfg$bC_theta %||% 0.35),
    bC_s_bad = as.numeric(outer_cfg$bC_s_bad %||% -0.10),
    bC_old = as.numeric(outer_cfg$bC_old %||% 0.10),
    bC_new = as.numeric(outer_cfg$bC_new %||% -0.05),
    bC_M = as.numeric(outer_cfg$bC_M %||% -0.10),

    aF = as.numeric(outer_cfg$aF %||% -3.00),
    bF_theta = as.numeric(outer_cfg$bF_theta %||% 0.70),
    bF_delta = as.numeric(outer_cfg$bF_delta %||% 1.10),

    # Optional intercept bracket for root-finding
    xi_bracket = as.numeric(outer_cfg$xi_bracket %||% c(-40, 40))
  )

  # Basic validation
  if (par$B <= 0L) stop("outer_sorting B must be positive.")
  if (par$Nhood <= 0L) stop("outer_sorting Nhood must be positive.")
  if (!is.finite(par$mu) || par$mu <= 0 || par$mu >= 1) stop("outer_sorting mu must be in (0,1).")
  if (!is.finite(par$delta_L) || !is.finite(par$delta_H) || par$delta_L < 0 || par$delta_H < 0 || par$delta_H <= par$delta_L) {
    stop("outer_sorting requires 0 <= delta_L < delta_H.")
  }
  if (!is.finite(par$lam) || par$lam <= 0 || par$lam > 1) stop("outer_sorting lam must be in (0,1].")
  if (!is.finite(par$tol) || par$tol <= 0) stop("outer_sorting tol must be positive.")
  if (par$max_iter <= 0L) stop("outer_sorting max_iter must be positive.")
  if (!is.finite(par$mass_tol_abs) || par$mass_tol_abs < 0) stop("outer_sorting mass_tol_abs must be nonnegative.")
  if (!is.finite(par$sim_year_from) || !is.finite(par$sim_year_to) || par$sim_year_from > par$sim_year_to) {
    stop("outer_sorting requires finite simulation years with sim_year_from <= sim_year_to.")
  }
  if (!is.finite(par$Dalph) || par$Dalph <= 0) stop("outer_sorting Dalph must be positive.")
  if (!is.finite(par$sigma_eta_sort) || par$sigma_eta_sort < 0) {
    stop("outer_sorting sigma_eta_sort must be finite and nonnegative.")
  }
  if (!is.finite(par$m0) || par$m0 <= 0) stop("outer_sorting m0 must be strictly positive.")
  if (!is.finite(par$high_filing_threshold) || par$high_filing_threshold < 0) {
    stop("outer_sorting high_filing_threshold must be finite and nonnegative.")
  }
  if (!is.finite(par$old_bldg_cutoff_year) || !is.finite(par$new_bldg_cutoff_year) ||
      par$old_bldg_cutoff_year >= par$new_bldg_cutoff_year) {
    stop("outer_sorting requires finite age cutoffs with old_bldg_cutoff_year < new_bldg_cutoff_year.")
  }

  if (!(par$units_draw_mode %in% c("empirical_resample", "discrete_bins"))) {
    stop("outer_sorting units_draw_mode must be one of: empirical_resample, discrete_bins.")
  }

  if (identical(par$units_draw_mode, "empirical_resample")) {
    built_units <- build_outer_sorting_empirical_buildings(cfg, outer_cfg)
    par$building_empirical_draws <- built_units$draws
    par$units_empirical_draws <- built_units$draws$avg_annual_units
    par$units_empirical_source_product_key <- built_units$source_product_key
    par$units_empirical_year_from <- built_units$year_from
    par$units_empirical_year_to <- built_units$year_to
    par$units_bin_values <- NULL
    par$units_bin_probs <- NULL
  } else {
    if (length(par$units_bin_values) != length(par$units_bin_probs)) {
      stop("outer_sorting units_bin_values and units_bin_probs must have equal length.")
    }
    if (any(par$units_bin_values <= 0)) stop("outer_sorting units_bin_values must be positive.")
    if (any(par$units_bin_probs < 0)) stop("outer_sorting units_bin_probs must be nonnegative.")

    prob_sum <- sum(par$units_bin_probs)
    if (!is.finite(prob_sum) || prob_sum <= 0) stop("outer_sorting units_bin_probs must sum to a positive value.")
    if (abs(prob_sum - 1) > 0.01) {
      warning("outer_sorting units_bin_probs sums to ", signif(prob_sum, 6), "; renormalizing to 1.")
    }
    par$units_bin_probs <- par$units_bin_probs / prob_sum
    par$units_empirical_draws <- NULL
    par$building_empirical_draws <- NULL
  }

  if (length(par$xi_bracket) != 2L || !all(is.finite(par$xi_bracket))) {
    stop("outer_sorting xi_bracket must be a finite numeric vector of length 2.")
  }

  par$sim_n_years <- as.integer(par$sim_year_to - par$sim_year_from + 1L)

  par
}
