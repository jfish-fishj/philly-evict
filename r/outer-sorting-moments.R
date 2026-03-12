## ============================================================
## outer-sorting-moments.R
## ============================================================
## Purpose: Compute baseline calibration moments from simulated
## outer-sorting panel output.
## ============================================================

compute_outer_sorting_moments <- function(dt, high_filing_threshold, log_file = NULL) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")
  if (!requireNamespace("fixest", quietly = TRUE)) stop("Missing package: fixest")
  if (missing(high_filing_threshold)) {
    stop("compute_outer_sorting_moments requires high_filing_threshold (fixed absolute threshold).")
  }
  if (!is.finite(high_filing_threshold) || high_filing_threshold < 0) {
    stop("high_filing_threshold must be finite and nonnegative.")
  }

  dt <- data.table::as.data.table(dt)
  req <- c("b", "nhood", "units", "s", "M", "C", "F", "f_rate", "c_rate", "m_rate")
  miss <- setdiff(req, names(dt))
  if (length(miss) > 0L) {
    stop("compute_outer_sorting_moments missing required columns: ", paste(miss, collapse = ", "))
  }
  if (any(dt$units <= 0)) stop("units must be strictly positive in moments input.")

  get_coef_or_na <- function(model, coef_name) {
    if (is.null(model)) return(NA_real_)
    co <- stats::coef(model)
    if (coef_name %in% names(co)) return(unname(co[[coef_name]]))
    NA_real_
  }
  q_or_na <- function(x, p) {
    x <- as.numeric(x)
    x <- x[is.finite(x) & !is.na(x)]
    if (!length(x)) return(NA_real_)
    as.numeric(stats::quantile(x, probs = p, na.rm = TRUE, names = FALSE))
  }
  safe_fit <- function(expr) {
    tryCatch(expr, error = function(e) NULL)
  }

  fixed_filing_cut <- as.numeric(high_filing_threshold)
  s_wmean <- as.numeric(stats::weighted.mean(dt$s, w = dt$units))

  dt[, high_filing := as.integer(f_rate >= fixed_filing_cut)]
  dt[, filing_bin3 := data.table::fcase(
    f_rate < 0.5 * fixed_filing_cut, "low",
    f_rate < fixed_filing_cut, "mid",
    default = "high"
  )]
  dt[, filing_bin3 := factor(filing_bin3, levels = c("low", "mid", "high"))]

  # Sorting moments: within-neighborhood composition gradients by filing exposure.
  sorting_high_model <- safe_fit(fixest::feols(
    s ~ high_filing | nhood,
    weights = ~units,
    data = dt
  ))
  sorting_high_coef <- get_coef_or_na(sorting_high_model, "high_filing")

  sorting_bins_model <- safe_fit(fixest::feols(
    s ~ i(filing_bin3, ref = "low") | nhood,
    weights = ~units,
    data = dt
  ))
  sorting_bin_high_coef <- get_coef_or_na(sorting_bins_model, "filing_bin3::high")

  # Complaint gradient moments: complaint counts on composition and composition x high filing.
  ppml <- safe_fit(fixest::fepois(
    C ~ s + s:high_filing + high_filing | nhood,
    offset = ~log(units),
    data = dt
  ))
  beta_s <- get_coef_or_na(ppml, "s")
  beta_high <- get_coef_or_na(ppml, "high_filing")
  beta_inter <- get_coef_or_na(ppml, "s:high_filing")

  # Filing dispersion residualized by neighborhood FE.
  dt[, filing_rate_resid_nhood := f_rate - mean(f_rate), by = nhood]
  fbar_resid_sd <- dt[, stats::sd(filing_rate_resid_nhood)]

  nhood_dt <- dt[, .(
    nhood_units = sum(units),
    nhood_mean_s = stats::weighted.mean(s, w = units),
    nhood_mean_f_rate = stats::weighted.mean(f_rate, w = units)
  ), by = nhood]
  nhood_mean_s_sd <- nhood_dt[, stats::sd(nhood_mean_s)]
  nhood_mean_s_p90_p10 <- as.numeric(stats::quantile(nhood_dt$nhood_mean_s, 0.90) - stats::quantile(nhood_dt$nhood_mean_s, 0.10))
  nhood_mean_f_rate_sd <- nhood_dt[, stats::sd(nhood_mean_f_rate)]
  nhood_mean_f_rate_p90_p10 <- as.numeric(stats::quantile(nhood_dt$nhood_mean_f_rate, 0.90) - stats::quantile(nhood_dt$nhood_mean_f_rate, 0.10))

  # Maintenance discipline moment.
  maint_model <- safe_fit(fixest::fepois(
    M ~ c_rate | nhood,
    offset = ~log(units),
    data = dt
  ))
  beta_maint_disc <- get_coef_or_na(maint_model, "c_rate")

  out <- data.table::data.table(
    n_buildings = nrow(dt),
    s_weighted_mean = s_wmean,
    mean_f_rate = dt[, stats::weighted.mean(f_rate, w = units)],
    mean_m_rate = dt[, stats::weighted.mean(m_rate, w = units)],
    mean_c_rate = dt[, stats::weighted.mean(c_rate, w = units)],
    mean_m_rate_pos = dt[M > 0L, mean(m_rate, na.rm = TRUE)],
    m_rate_pos_p50 = q_or_na(dt[M > 0L, m_rate], 0.50),
    m_rate_pos_p90 = q_or_na(dt[M > 0L, m_rate], 0.90),
    zero_share_f = dt[, mean(F == 0L)],
    zero_share_m = dt[, mean(M == 0L)],
    zero_share_c = dt[, mean(C == 0L)],
    share_high_filing = dt[, mean(high_filing)],
    high_filing_threshold = fixed_filing_cut,
    filing_p85 = fixed_filing_cut,
    sorting_coef_high_filing_nhood = sorting_high_coef,
    sorting_coef_high_bin_nhood = sorting_bin_high_coef,
    sorting_gap_high_minus_low = sorting_high_coef,
    beta_ppml_s = beta_s,
    beta_ppml_high_filing = beta_high,
    beta_ppml_s_x_high_filing = beta_inter,
    beta_ppml_above_mean_s = beta_s,
    beta_ppml_interaction = beta_inter,
    filing_resid_sd_nhood_fe = fbar_resid_sd,
    nhood_mean_s_sd = nhood_mean_s_sd,
    nhood_mean_s_p90_p10 = nhood_mean_s_p90_p10,
    nhood_mean_f_rate_sd = nhood_mean_f_rate_sd,
    nhood_mean_f_rate_p90_p10 = nhood_mean_f_rate_p90_p10,
    beta_maint_on_c_rate = beta_maint_disc,
    beta_maint_on_log1p_c_rate = beta_maint_disc,
    beta_maint_on_log1p_cbar = beta_maint_disc
  )

  if (!is.null(log_file) && exists("logf", mode = "function")) {
    logf("compute_outer_sorting_moments: computed ", ncol(out),
         " moments (high_filing_threshold=", fixed_filing_cut, ").",
         log_file = log_file)
  }

  out
}
