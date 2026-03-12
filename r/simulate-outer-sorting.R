## ============================================================
## simulate-outer-sorting.R
## ============================================================
## Purpose: Simulate baseline stationary outer sorting model.
##
## Core equations:
##   \bar m_b = m0 * exp(bM_theta * 1{theta_b=B} + bM_old * old_bldg + bM_new * new_bldg + bM_C * \bar c_b)
##   log(\bar c_b) = aC + bC_theta * 1{theta_b=B} + bC_s_bad * s_b * 1{theta_b=B} + bC_old * old_bldg + bC_new * new_bldg + bC_M * log(1 + \bar m_b)
##   \bar\delta_b = s_b * delta_H + (1 - s_b) * delta_L
##   log(\bar f_b) = aF + bF_theta * 1{theta_b=B} + bF_delta * \bar\delta_b
##   x_b = \bar f_b
##   s_b^*(xi) = Lambda(xi + Dalph * x_b + eta_{n(b)})
## with xi chosen each iteration so sum_b U_b s_b^*(xi) = mu * sum_b U_b.
## The realized panel is an annualized multi-year stationary average:
## for each building, draw total counts over its active years in the
## configured simulation window and divide back by exposure years so
## the simulated output is comparable to the empirical collapsed PID
## targets built from 2011-2019 averages.
## ============================================================

find_xi <- function(x, units, mu, bracket = c(-40, 40)) {
  stopifnot(length(x) == length(units))
  stopifnot(length(bracket) == 2L)

  target_mass <- mu * sum(units)
  f_mass <- function(xi) sum(units * plogis(xi + x)) - target_mass

  lower <- bracket[1]
  upper <- bracket[2]
  f_lower <- f_mass(lower)
  f_upper <- f_mass(upper)

  # Expand brackets deterministically if needed.
  n_expand <- 0L
  while (f_lower * f_upper > 0 && n_expand < 20L) {
    lower <- lower - 5
    upper <- upper + 5
    f_lower <- f_mass(lower)
    f_upper <- f_mass(upper)
    n_expand <- n_expand + 1L
  }
  if (f_lower * f_upper > 0) {
    stop("Unable to bracket xi root for citywide mass constraint.")
  }

  uniroot(f_mass, lower = lower, upper = upper, tol = .Machine$double.eps^0.5)$root
}

simulate_outer_sorting <- function(par, log_file = NULL, warn_if_not_converged = TRUE) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")
  data.table::setDTthreads(threads = 0L)

  set.seed(par$seed)

  B <- par$B
  Nhood <- par$Nhood
  sim_year_from <- as.integer(par$sim_year_from)
  sim_year_to <- as.integer(par$sim_year_to)

  dt <- data.table::data.table(
    b = seq_len(B),
    nhood = sample.int(Nhood, B, replace = TRUE)
  )

  p_n <- stats::rbeta(Nhood, shape1 = par$p_bad_shape1, shape2 = par$p_bad_shape2)
  dt[, theta_bad := stats::rbinom(.N, size = 1L, prob = p_n[nhood])]
  dt[, theta := ifelse(theta_bad == 1L, "B", "G")]
  eta_sort_n <- stats::rnorm(Nhood, mean = 0, sd = par$sigma_eta_sort)
  eta_sort_n <- eta_sort_n - mean(eta_sort_n)
  dt[, eta_sort := eta_sort_n[nhood]]
  if (identical(par$units_draw_mode, "empirical_resample")) {
    draw_idx <- sample.int(n = nrow(par$building_empirical_draws), size = B, replace = TRUE)
    draws <- par$building_empirical_draws[draw_idx]
    dt[, `:=`(
      units = as.numeric(draws$avg_annual_units),
      year_built = suppressWarnings(as.integer(draws$year_built))
    )]
  } else if (identical(par$units_draw_mode, "discrete_bins")) {
    dt[, units := as.numeric(sample(
      x = par$units_bin_values,
      size = .N,
      replace = TRUE,
      prob = par$units_bin_probs
    ))]
  } else {
    stop("Unsupported units_draw_mode: ", par$units_draw_mode)
  }
  if (!("year_built" %in% names(dt))) dt[, year_built := NA_integer_]
  dt[, `:=`(
    old_bldg = as.integer(!is.na(year_built) & year_built <= par$old_bldg_cutoff_year),
    new_bldg = as.integer(!is.na(year_built) & year_built >= par$new_bldg_cutoff_year)
  )]
  dt[, first_year_active := fifelse(!is.na(year_built), pmax(sim_year_from, year_built), sim_year_from)]
  dt[, exposure_years := pmax(0L, sim_year_to - first_year_active + 1L)]
  if (any(dt$exposure_years <= 0L)) {
    stop("Some simulated buildings have nonpositive exposure_years. Check year_built and simulation window settings.")
  }
  dt[, unit_years := units * exposure_years]

  # Initial values for fixed-point iteration.
  dt[, s := par$mu]
  dt[, cbar := exp(par$aC)]

  xi_prev <- NA_real_
  converged <- FALSE
  iters <- 0L
  max_diff <- NA_real_
  mass_resid <- NA_real_

  for (k in seq_len(par$max_iter)) {
    dt[, mbar := par$m0 * exp(par$bM_theta * theta_bad + par$bM_old * old_bldg + par$bM_new * new_bldg + par$bM_C * cbar)]
    dt[, cbar_next := exp(par$aC + par$bC_theta * theta_bad + par$bC_s_bad * s * theta_bad + par$bC_old * old_bldg + par$bC_new * new_bldg + par$bC_M * log1p(mbar))]
    dt[, delta_bar := s * par$delta_H + (1 - s) * par$delta_L]
    dt[, fbar_next := exp(par$aF + par$bF_theta * theta_bad + par$bF_delta * delta_bar)]

    x <- par$Dalph * dt[["fbar_next"]] + dt[["eta_sort"]]
    xi_k <- find_xi(
      x = x,
      units = dt[["units"]],
      mu = par$mu,
      bracket = par$xi_bracket
    )

    s_star <- plogis(xi_k + x)
    s_next <- (1 - par$lam) * dt[["s"]] + par$lam * s_star

    max_diff <- max(abs(s_next - dt[["s"]]))
    mass_resid <- sum(dt[["units"]] * s_next) - par$mu * sum(dt[["units"]])

    dt[, s := s_next]
    dt[, cbar := cbar_next]
    xi_prev <- xi_k
    iters <- k

    if (max_diff < par$tol && abs(mass_resid) <= par$mass_tol_abs) {
      converged <- TRUE
      break
    }
  }

  if (!converged && isTRUE(warn_if_not_converged)) {
    warning("simulate_outer_sorting did not converge after ", iters,
            " iterations (max_diff=", signif(max_diff, 6),
            ", mass_resid=", signif(mass_resid, 6), "). Returning last iterate.")
  }

  # Final coherent expected rates at converged composition.
  dt[, mbar := par$m0 * exp(par$bM_theta * theta_bad + par$bM_old * old_bldg + par$bM_new * new_bldg + par$bM_C * cbar)]
  dt[, cbar := exp(par$aC + par$bC_theta * theta_bad + par$bC_s_bad * s * theta_bad + par$bC_old * old_bldg + par$bC_new * new_bldg + par$bC_M * log1p(mbar))]
  dt[, delta_bar := s * par$delta_H + (1 - s) * par$delta_L]
  dt[, fbar := exp(par$aF + par$bF_theta * theta_bad + par$bF_delta * delta_bar)]

  # Enforce exact citywide mass in the final reported s.
  x_final <- par$Dalph * dt[["fbar"]] + dt[["eta_sort"]]
  xi_final <- find_xi(
    x = x_final,
    units = dt[["units"]],
    mu = par$mu,
    bracket = par$xi_bracket
  )
  dt[, s := plogis(xi_final + x_final)]

  # Recompute expected rates at the final projected composition.
  dt[, mbar := par$m0 * exp(par$bM_theta * theta_bad + par$bM_old * old_bldg + par$bM_new * new_bldg + par$bM_C * cbar)]
  dt[, cbar := exp(par$aC + par$bC_theta * theta_bad + par$bC_s_bad * s * theta_bad + par$bC_old * old_bldg + par$bC_new * new_bldg + par$bC_M * log1p(mbar))]
  dt[, delta_bar := s * par$delta_H + (1 - s) * par$delta_L]
  dt[, fbar := exp(par$aF + par$bF_theta * theta_bad + par$bF_delta * delta_bar)]

  final_mass_resid <- sum(dt[["units"]] * dt[["s"]]) - par$mu * sum(dt[["units"]])

  # Draw total counts over active years, then annualize to average annual counts.
  dt[, M_total := stats::rpois(.N, lambda = pmax(unit_years * mbar, 1e-12))]
  dt[, C_total := stats::rpois(.N, lambda = pmax(unit_years * cbar, 1e-12))]
  dt[, F_total := stats::rpois(.N, lambda = pmax(unit_years * fbar, 1e-12))]

  dt[, `:=`(
    M = M_total / exposure_years,
    C = C_total / exposure_years,
    F = F_total / exposure_years,
    m_rate = M_total / unit_years,
    c_rate = C_total / unit_years,
    f_rate = F_total / unit_years,
    x = fbar
  )]

  keep <- c(
    "b", "nhood", "theta", "theta_bad", "units", "year_built", "old_bldg", "new_bldg",
    "first_year_active", "exposure_years", "unit_years",
    "eta_sort", "s", "mbar", "cbar", "fbar", "x",
    "M_total", "C_total", "F_total", "M", "C", "F", "m_rate", "c_rate", "f_rate"
  )
  dt <- dt[, ..keep]

  # Output contract checks.
  if (any(!is.finite(dt$s)) || any(dt$s <= 0 | dt$s >= 1)) stop("s contains invalid values outside (0,1).")
  if (any(!is.finite(dt$mbar) | dt$mbar <= 0)) stop("mbar must be positive and finite.")
  if (any(!is.finite(dt$cbar) | dt$cbar <= 0)) stop("cbar must be positive and finite.")
  if (any(!is.finite(dt$fbar) | dt$fbar <= 0)) stop("fbar must be positive and finite.")
  if (any(dt$units <= 0)) stop("units must be strictly positive.")
  if (any(dt$M_total < 0 | dt$C_total < 0 | dt$F_total < 0)) stop("Poisson total count outcomes must be nonnegative.")
  if (any(dt$M < 0 | dt$C < 0 | dt$F < 0)) stop("Annualized count outcomes must be nonnegative.")
  if (anyDuplicated(dt$b) > 0) stop("Primary key b is not unique in simulated panel.")

  if (!is.null(log_file) && exists("logf", mode = "function")) {
    logf("simulate_outer_sorting: converged=", converged,
         ", iters=", iters,
         ", max_diff=", sprintf("%.3e", max_diff),
         ", mass_resid_before_final_proj=", sprintf("%.3e", mass_resid),
         ", mass_resid_final=", sprintf("%.3e", final_mass_resid),
         ", sim_years=", sim_year_from, "-", sim_year_to,
         log_file = log_file)
  }

  diag_dt <- data.table::data.table(
    converged = converged,
    iters = iters,
    max_diff = max_diff,
    mass_resid_before_final_proj = mass_resid,
    mass_resid_final = final_mass_resid,
    xi_last_iter = xi_prev,
    xi_final = xi_final,
    mu_target = par$mu,
    s_weighted_mean = sum(dt$units * dt$s) / sum(dt$units),
    sim_year_from = sim_year_from,
    sim_year_to = sim_year_to,
    sim_n_years = par$sim_n_years
  )

  list(
    panel = dt,
    diagnostics = diag_dt
  )
}
