## ============================================================
## comp-statics-annual-race.R
## ============================================================
## Purpose: Annual-frequency complaint regressions using race
## composition as a proxy for building composition. Addresses
## the artificial within-year repetition from quarterly/monthly
## panels merged with annual InfoUSA demographics. Pre-COVID
## sample (<=2019). Sections: variance decomposition, PPML
## complaint ladder (5 specs), FE robustness ladder (5 specs),
## diagnostics, QA.
##
## Inputs:
##   - products/bldg_panel_blp (PID x year: demographics, building
##     observables, rent, filing rates)
##   - products/building_data (parcel_number x period, annual:
##     complaints, permits, violations)
##
## Outputs:
##   - output/tables/compstats_annual_*.tex / *.csv / *.txt
##   - output/figs/compstats_annual_*.png
##   - output/logs/comp-statics-annual-race.log
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(fixest)
  library(ggplot2)
  library(stringr)
})

source("r/config.R")

# ---- CLI parsing helpers (same pattern as comp-statics-quarterly) ----
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

to_bool <- function(x, default = FALSE) {
  if (is.null(x) || !length(x)) return(default)
  tolower(as.character(x[[1L]])) %chin% c("1", "true", "t", "yes", "y")
}

pad_pid <- function(x) str_pad(as.character(x), width = 9L, side = "left", pad = "0")

# ---- Reusable helpers ----
ewma_vec <- function(x, lambda = 0.9) {
  x <- as.numeric(x)
  n <- length(x)
  out <- numeric(n)
  if (!n) return(out)
  out[1L] <- ifelse(is.na(x[1L]), 0, x[1L])
  if (n >= 2L) for (i in 2L:n) out[i] <- ifelse(is.na(x[i]), 0, x[i]) + lambda * out[i - 1L]
  out
}

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
      if (grepl("dependent variable .* constant", msg, ignore.case = TRUE) ||
          grepl("collinearity", msg, ignore.case = TRUE)) {
        logf("Skipping model (", msg, "): ", model_label, log_file = log_file)
        return(NULL)
      }
      stop(e)
    }
  )
}

extract_coef_table <- function(models, outcome_label) {
  out <- list()
  for (nm in names(models)) {
    m <- models[[nm]]
    if (is.null(m) || !inherits(m, "fixest")) next
    ct <- tidy_coeftable(m)
    if (!nrow(ct)) next
    ct[, `:=`(model = nm, outcome_family = outcome_label)]
    out[[nm]] <- ct[, .(outcome_family, model, term, estimate, std_error, t_value, p_value)]
  }
  rbindlist(out, use.names = TRUE, fill = TRUE)
}

center_impute <- function(x) {
  mu <- mean(x, na.rm = TRUE)
  if (!is.finite(mu)) mu <- 0
  list(val = fifelse(is.na(x), mu, x), mu = mu)
}


# ======================================================================
# SECTION 0: SETUP & DATA
# ======================================================================
opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
start_year <- to_int(opts$start_year, 2007L)
end_year <- to_int(opts$end_year, 2019L)
sample_n_pid <- to_int(opts$sample_n_pid, 0L)
use_pid_filter <- to_bool(opts$use_pid_filter, FALSE)
ewma_lambda <- 0.9

cfg <- read_config()
# Override use_pid_filter from config if not set via CLI
if (is.null(opts$use_pid_filter)) {
  use_pid_filter <- cfg$run$use_pid_filter %||% FALSE
}

log_file <- p_out(cfg, "logs", "comp-statics-annual-race.log")
tab_dir <- p_out(cfg, "comp-statics-annual-race", "tables")
fig_dir <- p_out(cfg, "comp-statics-annual-race", "figs")
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)

logf("=== Starting comp-statics-annual-race.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf("Years: ", start_year, "-", end_year,
     "; sample_n_pid=", sample_n_pid,
     "; use_pid_filter=", use_pid_filter,
     "; ewma_lambda=", ewma_lambda,
     log_file = log_file)

blp_path <- p_product(cfg, "bldg_panel_blp")
dt <- fread(blp_path)
setkey(dt, PID, year)
setorder(dt, PID, year)
assert_unique(dt, c("PID", "year"), "merged panel (PID-year)")
logf("Merged panel: rows=", nrow(dt), ", PIDs=", uniqueN(dt$PID),
     ", complaint non-missing=", round(mean(!is.na(dt$total_severe_complaints)), 4),
     log_file = log_file)

# ---- Optional PID filter (from analytic_sample universe) ----
if (use_pid_filter) {
  analytic_path <- p_product(cfg, "analytic_sample")
  pid_univ <- unique(pad_pid(fread(analytic_path, select = "PID")$PID))
  dt <- dt[PID %chin% pid_univ]
  logf("PID filter applied: rows=", nrow(dt), ", PIDs=", uniqueN(dt$PID), log_file = log_file)
}

# ---- Optional subsample for dev ----
if (sample_n_pid > 0L && sample_n_pid < uniqueN(dt$PID)) {
  set.seed(123)
  keep_pids <- sample(unique(dt$PID), sample_n_pid)
  dt <- dt[PID %chin% keep_pids]
  logf("Sample mode: selected ", uniqueN(dt$PID), " PIDs.", log_file = log_file)
}

# filter based on start year / end year
dt <- dt[year >= start_year & year <= end_year]

# ---- Derive variables ----
# Numeric coercions
dt[, total_units_wt := suppressWarnings(as.numeric(total_units))]
dt[!is.finite(total_units_wt) | total_units_wt <= 0, total_units_wt := 1]
dt[, log_total_units_wt := log(total_units_wt)]
dt[, log1p_total_units := log1p(total_units_wt)]

dt[, total_area_num := suppressWarnings(as.numeric(total_area))]
dt[, log_total_area := fifelse(is.finite(total_area_num) & total_area_num > 0, log(total_area_num), NA_real_)]
dt[, total_area_missing := as.integer(is.na(log_total_area))]
imp_area <- center_impute(dt$log_total_area)
dt[, log_total_area_imp := as.numeric(imp_area$val)]

dt[, market_value_num := suppressWarnings(as.numeric(market_value))]
dt[, log_market_value := fifelse(is.finite(market_value_num) & market_value_num > 0, log(market_value_num), NA_real_)]
dt[, market_value_missing := as.integer(is.na(log_market_value))]
imp_mkt <- center_impute(dt$log_market_value)
dt[, log_market_value_imp := as.numeric(imp_mkt$val)]

# Rent
dt[, log_med_rent_raw := suppressWarnings(as.numeric(log_med_rent))]
dt[, log_med_rent_pid_mean := {
  x <- log_med_rent_raw
  if (all(!is.finite(x))) NA_real_ else mean(x[is.finite(x)])
}, by = PID]

# Filing rate (annual = current year)
dt[, num_filings_num := suppressWarnings(as.numeric(num_filings))]
dt[!is.finite(num_filings_num), num_filings_num := 0]
dt[, filing_rate_annual := num_filings_num / pmax(total_units_wt, 1)]

# Census tract from GEOID
dt[, census_tract := substr(as.character(GEOID), 1L, 11L)]
dt[is.na(census_tract) | !nzchar(census_tract), census_tract := NA_character_]

# FE-friendly factor columns
for (cc in c("building_type", "num_units_bin", "year_blt_decade", "num_stories_bin", "quality_grade")) {
  if (!(cc %in% names(dt))) dt[, (cc) := NA_character_]
  dt[, (cc) := as.character(get(cc))]
  dt[is.na(get(cc)) | !nzchar(get(cc)), (cc) := "Unknown"]
}
dt[, quality_grade_abcde := collapse_quality_grade_abcde(quality_grade)]
dt[, unit_bin_fine := make_fine_unit_bins(total_units)]


# Maintenance flow (repair permits, excluding zoning)
repair_permit_cols <- c("electrical_permit_count", "plumbing_permit_count", "mechanical_permit_count", "fire_suppression_permit_count")

dt[, maint_flow_repairs := rowSums(.SD, na.rm = TRUE), .SDcols = repair_permit_cols]


# EWMA stocks (annual, lagged 1 year)
setorder(dt, PID, year)
dt[, maint_flow_l1 := shift(maint_flow_repairs, 1L, type = "lag", fill = 0), by = PID]
dt[, maint_stock_lag := ewma_vec(maint_flow_l1, lambda = ewma_lambda), by = PID]
dt[, maint_stock_lag_rate := maint_stock_lag / pmax(total_units_wt, 1)]

# Severe violations flow
severe_viol_cols <- c("hazardous_violation_count", "unsafe_violation_count", "imminently_dangerous_violation_count")
dt[, severe_viol_flow := rowSums(.SD, na.rm = TRUE), .SDcols = severe_viol_cols]

# severe investigations flow
severe_investigation_cols <- c("total_investigations")
dt[, severe_investigation_flow := rowSums(.SD, na.rm = TRUE), .SDcols = severe_investigation_cols]

# total violations flow
dt[, total_violations_flow := total_violations]

# Lag maintenance flow terms (L1-L3)
dt[, maint_flow_rate := maint_flow_repairs / pmax(total_units_wt, 1)]
dt[, maint_flow_rate_l1 := shift(maint_flow_rate, 1L, type = "lag", fill = 0), by = PID]
dt[, maint_flow_rate_l2 := shift(maint_flow_rate, 2L, type = "lag", fill = 0), by = PID]
dt[, maint_flow_rate_l3 := shift(maint_flow_rate, 3L, type = "lag", fill = 0), by = PID]

# Lag severe violation flow terms (L1-L3)
dt[, severe_viol_rate := severe_viol_flow / pmax(total_units_wt, 1)]
dt[, severe_viol_rate_l1 := shift(severe_viol_rate, 1L, type = "lag", fill = 0), by = PID]
dt[, severe_viol_rate_l2 := shift(severe_viol_rate, 2L, type = "lag", fill = 0), by = PID]
dt[, severe_viol_rate_l3 := shift(severe_viol_rate, 3L, type = "lag", fill = 0), by = PID]

# Lag severe investigations flow
dt[, severe_investigation_rate := severe_investigation_flow / pmax(total_units_wt, 1)]
dt[, severe_investigation_rate_l1 := shift(severe_investigation_rate, 1L, type = "lag", fill = 0), by = PID]
dt[, severe_investigation_rate_l2 := shift(severe_investigation_rate, 2L, type = "lag", fill = 0), by = PID]
dt[, severe_investigation_rate_l3 := shift(severe_investigation_rate, 3L, type = "lag", fill = 0), by = PID]

# lag total violations flow
dt[, total_violations_rate := total_violations_flow / pmax(total_units_wt, 1)]
dt[, total_violations_rate_l1 := shift(total_violations_rate, 1L, type = "lag", fill = 0), by = PID]
dt[, total_violations_rate_l2 := shift(total_violations_rate, 2L, type = "lag", fill = 0), by = PID]
dt[, total_violations_rate_l3 := shift(total_violations_rate, 3L, type = "lag", fill = 0), by = PID]


# Hedonic rent residual: regress log_med_rent ~ factor(year), PID-level mean of residuals
dt[, rent_resid_pid := NA_real_]
rent_ok <- dt[is.finite(log_med_rent_raw)]
if (nrow(rent_ok) > 100L) {
  m_rent <- tryCatch(feols(log_med_rent_raw ~ 1 | year, data = rent_ok), error = function(e) NULL)
  if (!is.null(m_rent)) {
    rent_ok[, rent_resid := resid(m_rent)]
    rent_resid_means <- rent_ok[, .(rent_resid_pid = mean(rent_resid, na.rm = TRUE)), by = PID]
    dt[rent_resid_means, rent_resid_pid := i.rent_resid_pid, on = "PID"]
    logf("Hedonic rent residual computed for ", uniqueN(rent_resid_means$PID), " PIDs.", log_file = log_file)
  }
}

# Demographics
dt[, pct_black := suppressWarnings(as.numeric(infousa_pct_black))]
dt[, pct_female := suppressWarnings(as.numeric(infousa_pct_female))]
dt[, pct_black_female := suppressWarnings(as.numeric(infousa_pct_black_female))]
dt[, demog_cov := suppressWarnings(as.numeric(infousa_share_persons_demog_ok))]

# Lagged pct_black (for simultaneity check)
dt[, pct_black_l1 := shift(pct_black, 1L, type = "lag"), by = PID]

# Apartment indicator
apt_types <- c("SMALL_MULTI_2_4", "LOWRISE_MULTI", "MIDRISE_MULTI", "HIGHRISE_MULTI")
dt[, is_apartment := as.integer(building_type %chin% apt_types)]

logf("Panel construction complete: rows=", nrow(dt), ", PIDs=", uniqueN(dt$PID),
     ", pct_black non-missing=", round(mean(!is.na(dt$pct_black)), 4),
     ", log_med_rent non-missing=", round(mean(is.finite(dt$log_med_rent_raw)), 4),
     ", filing_rate_annual non-missing=", round(mean(is.finite(dt$filing_rate_annual)), 4),
     log_file = log_file)

fixest::setFixest_etable(digits = 4, fitstat = c("n", "r2"))

# ======================================================================
# SECTION 1: VARIANCE DECOMPOSITION
# ======================================================================
logf("=== Section 1: Variance Decomposition ===", log_file = log_file)

run_variance_decomposition <- function(dt, x_col, label) {
  d <- dt[is.finite(get(x_col)), .(PID, year, GEOID, x = get(x_col))]
  if (nrow(d) < 100L) return(NULL)

  overall_sd <- sd(d$x, na.rm = TRUE)

  # Between-PID vs within-PID
  pid_means <- d[, .(mu = mean(x, na.rm = TRUE)), by = PID]
  between_pid_sd <- sd(pid_means$mu, na.rm = TRUE)
  d[pid_means, x_dm_pid := x - i.mu, on = "PID"]
  within_pid_sd <- sd(d$x_dm_pid, na.rm = TRUE)

  # Between-GEOID*year vs within
  gy_means <- d[!is.na(GEOID), .(mu = mean(x, na.rm = TRUE)), by = .(GEOID, year)]
  between_gy_sd <- sd(gy_means$mu, na.rm = TRUE)
  d[gy_means, x_dm_gy := x - i.mu, on = .(GEOID, year)]
  within_gy_sd <- sd(d$x_dm_gy, na.rm = TRUE)

  # R^2 from successive FE demeanings
  r2_pid <- tryCatch(fixest::r2(feols(x ~ 1 | PID, data = d), type = "ar2"), error = function(e) NA_real_)
  r2_year <- tryCatch(fixest::r2(feols(x ~ 1 | year, data = d), type = "ar2"), error = function(e) NA_real_)
  r2_gy <- tryCatch(fixest::r2(feols(x ~ 1 | GEOID^year, data = d[!is.na(GEOID)]), type = "ar2"), error = function(e) NA_real_)
  r2_pid_gy <- tryCatch(fixest::r2(feols(x ~ 1 | PID + GEOID^year, data = d[!is.na(GEOID)]), type = "ar2"), error = function(e) NA_real_)

  data.table(
    variable = label,
    n_obs = nrow(d),
    n_pid = uniqueN(d$PID),
    overall_sd = overall_sd,
    between_pid_sd = between_pid_sd,
    within_pid_sd = within_pid_sd,
    between_geoid_year_sd = between_gy_sd,
    within_geoid_year_sd = within_gy_sd,
    r2_pid = r2_pid,
    r2_year = r2_year,
    r2_geoid_year = r2_gy,
    r2_pid_geoid_year = r2_pid_gy
  )
}

vardecomp <- rbindlist(list(
  run_variance_decomposition(dt, "pct_black", "infousa_pct_black"),
  run_variance_decomposition(dt, "pct_black_female", "infousa_pct_black_female"),
  run_variance_decomposition(dt, "pct_female", "infousa_pct_female")
), use.names = TRUE, fill = TRUE)

fwrite(vardecomp, file.path(tab_dir, "compstats_annual_variance_decomposition.csv"))
if (nrow(vardecomp)) {
  logf("Variance decomposition:\n", paste(capture.output(print(vardecomp)), collapse = "\n"), log_file = log_file)
}


# ======================================================================
# SECTION 2: COMPLAINT REGRESSION LADDER (9 specs)
# ======================================================================
logf("=== Section 2: Complaint Regression Ladder ===", log_file = log_file)

bldg_fe <- "year + census_tract + building_type + unit_bin_fine + year_blt_decade + num_stories_bin + quality_grade_abcde"
ctrl_no_maint <- "pct_black_l1 + log_total_area"
ctrl_full <- "pct_black_l1 + log_total_area + maint_flow_rate_l1 + maint_flow_rate_l2 + maint_flow_rate_l3 + severe_viol_rate_l1 + severe_viol_rate_l2 + severe_viol_rate_l3"
ctrl_within_lags <- "pct_black_l1 + maint_flow_rate_l1 + maint_flow_rate_l2 + maint_flow_rate_l3 + severe_viol_rate_l1 + severe_viol_rate_l2 + severe_viol_rate_l3"

dat_full <- dt[
  !is.na(pct_black_l1) &
    is.finite(total_severe_complaints) &
    is.finite(log_total_area) &
    is.finite(maint_flow_rate_l1) &
    is.finite(severe_viol_rate_l1) &
    !is.na(census_tract) & nzchar(census_tract)
]

# define high filing as above 0.15 %
dat_full[, high_filing_eb := as.integer(filing_rate_eb_pre_covid > 0.15)]
dat_full[, high_filing := as.integer(filing_rate_longrun_pre2019 > 0.15)]
dat_full[,mean_pct_black_bt := weighted.mean(infousa_pct_black,total_units, na.rm = TRUE), by = building_type]
dat_full[,mean_pct_black_ct := weighted.mean(infousa_pct_black,total_units, na.rm = TRUE), by = census_tract]
dat_full[,above_mean_pct_black_bt := infousa_pct_black > mean_pct_black_bt]
dat_full[,above_mean_pct_black_ct := infousa_pct_black > mean_pct_black_ct]
dat_full[,mean_pct_black_female_bt := weighted.mean(infousa_pct_black_female,total_units, na.rm = TRUE), by = building_type]
dat_full[,above_mean_pct_black_female_bt := infousa_pct_black_female > mean_pct_black_female_bt]

dat_full[,median_pct_black_bt := spatstat.univar::weighted.quantile(infousa_pct_black, total_units, na.rm = T, probs = 0.75), by = building_type]
dat_full[,above_median_pct_black_bt := infousa_pct_black > median_pct_black_bt]


dat_full[,severe_viol_rate_l1_l3 :=severe_viol_rate_l1 + severe_viol_rate_l2 + severe_viol_rate_l3 ]
dat_full[,severe_investigation_rate_l1_l3 := severe_investigation_rate_l1 + severe_investigation_rate_l2 + severe_investigation_rate_l3]
dat_full[,compositive_inv_viol_rate := severe_viol_rate_l1_l3 + severe_investigation_rate_l1_l3 ]

logf("Regression sample: rows=", nrow(dat_full), ", PIDs=", uniqueN(dat_full$PID), log_file = log_file)

# --- Cross-sectional specs (1-3) ---

# Spec 1: raw — pct_black_l1 | year + unit_bin_fine
m_1 <- safe_model_eval(
  fepois(
    total_severe_complaints ~
      above_mean_pct_black_bt : high_filing
    + above_mean_pct_black_bt : compositive_inv_viol_rate
    + compositive_inv_viol_rate + high_filing + above_mean_pct_black_bt
    #+ above_mean_pct_black_bt * severe_investigation_rate_l1_l3
    #+ above_mean_pct_black_bt * severe_viol_rate_l2
    #+ above_mean_pct_black_bt * severe_viol_rate_l3
    | year + unit_bin_fine + building_type,
    data = dat_full[],
    offset = ~ log_total_units_wt,
    cluster = ~ PID,
    lean = TRUE
  ),
  model_label = "spec1_raw",
  log_file = log_file
)

# Spec 2: building + nhood, no maintenance/severity
m_2 <- safe_model_eval(
  fepois(
    total_severe_complaints ~
      above_mean_pct_black_bt : high_filing
    + above_mean_pct_black_bt : compositive_inv_viol_rate
    + compositive_inv_viol_rate + high_filing + above_mean_pct_black_bt
    | year + unit_bin_fine + year_blt_decade + census_tract + quality_grade_abcde + building_type,
    data = dat_full, offset = ~log_total_units_wt, cluster = ~PID, lean = TRUE
  ),
  model_label = "spec1_ctrl", log_file = log_file
)

# same thing but add in rent

m_3 <- safe_model_eval(
  fepois(
    total_severe_complaints ~
      above_mean_pct_black_bt : high_filing
    + above_mean_pct_black_bt : compositive_inv_viol_rate
    + above_mean_pct_black_bt + high_filing + compositive_inv_viol_rate
    + log_med_rent_pid_mean
    | year + unit_bin_fine + year_blt_decade +census_tract  + quality_grade_abcde + building_type,
    data = dat_full, offset = ~log_total_units_wt, cluster = ~PID, lean = TRUE
  ),
  model_label = "spec1_ctrl_rent", log_file = log_file
)

# etable and export
setFixest_dict(
  "spec1_raw" = "Filing rate (raw)",
  "spec1_filing_bins" = "Filing rate (bins)",
  "total_severe_complaints" = "Total severe complaints",
  "pct_black" = "Pct. Black (InfoUSA)",
  "above_mean_pct_black_bt" = "Above mean pct. Black (by building type)",
  "above_mean_pct_black_btTRUE" = "Above mean pct. Black (by building type)",
  "high_filing" = "High filing rate (lagged)",
  "compositive_inv_viol_rate" = "Composite inv/viol rate (lagged)",
  "log_med_rent_pid_mean" = "Parcel-level mean log median rent",
  "year" = "Year FE",
  "unit_bin_fine" = "Unit bins (fine)",
  "year_blt_decade" = "Year built decade FE",
  "census_tract" = "Census tract FE",
  "quality_grade_abcde" = "Quality grade (A/B/C/D/E/Other)",
  "building_type" = "Building type",
  "log_total_units_wt" = "Log total units (offset)"
)

out_table_raw <- capture.output(etable(
  m_1, m_2, m_3,
  se.below = TRUE, digits = 4, tex = FALSE,
  title = "PPML complaint regressions with filing rates and composition"
))

out_table_tex <- capture.output(etable(
  m_1, m_2, m_3,
  se.below = TRUE, digits = 4, tex = TRUE,
  title = "PPML complaint regressions with filing rates and composition"
))

writeLines(out_table_raw, file.path(tab_dir, "compstats_annual_ppml_ladder.txt"))
writeLines(out_table_tex, file.path(tab_dir, "compstats_annual_ppml_ladder.tex"))

# next section w/ filing bins; bin into 0, 0.01, 0.05, 0.1, 0.15, 0.25, 0.25+
dat_full[,filing_rate_longrun_pre2019_cuts := cut(filing_rate_longrun_pre2019, breaks = c(-Inf, 0, 0.01, 0.05, 0.1, 0.15, 0.25, Inf), labels = c("0", "0-1%", "1-5%", "5-10%", "10-15%", "15-25%", "25%+"))]
dat_full[, pct_black_q75_bt := spatstat.univar::weighted.quantile(infousa_pct_black, total_units, na.rm = T, probs = c(0.75)), by = building_type]
m1_cut <- safe_model_eval(
  fepois(
    total_severe_complaints ~ pct_black_q75_bt * filing_rate_longrun_pre2019_cuts
    + pct_black_q75_bt * compositive_inv_viol_rate
    | year + unit_bin_fine + year_blt_decade  + quality_grade_abcde + building_type,
    data = dat_full, offset = ~log_total_units_wt, cluster = ~PID, lean = TRUE
  ),
  model_label = "spec1_filing_bins", log_file = log_file
)

# ======================================================================
# SECTION 3: DIAGNOSTICS & COVERAGE
# ======================================================================
logf("=== Section 3: Diagnostics & Coverage ===", log_file = log_file)

# Coverage table: by year, share of PIDs with non-missing demographics
coverage <- dt[, .(
  n_obs = .N,
  n_pid = uniqueN(PID),
  share_pct_black_nonmiss = mean(!is.na(pct_black)),
  share_pct_female_nonmiss = mean(!is.na(pct_female)),
  share_demog_cov_nonmiss = mean(!is.na(demog_cov)),
  share_log_med_rent_nonmiss = mean(is.finite(log_med_rent_raw)),
  share_log_total_area_nonmiss = mean(is.finite(log_total_area)),
  share_census_tract_nonmiss = mean(!is.na(census_tract) & nzchar(census_tract)),
  mean_total_severe_complaints = mean(total_severe_complaints, na.rm = TRUE),
  mean_total_units = mean(total_units_wt, na.rm = TRUE)
), by = year]
setorder(coverage, year)
fwrite(coverage, file.path(tab_dir, "compstats_annual_coverage_diagnostics.csv"))
logf("Coverage diagnostics by year:\n", paste(capture.output(print(coverage)), collapse = "\n"), log_file = log_file)

# Sample balance: missing vs non-missing demographics
dt[, has_demog := as.integer(!is.na(pct_black))]
balance <- dt[, .(
  n_obs = .N,
  n_pid = uniqueN(PID),
  mean_total_units = mean(total_units_wt, na.rm = TRUE),
  mean_total_complaints = mean(total_complaints, na.rm = TRUE),
  mean_total_severe_complaints = mean(total_severe_complaints, na.rm = TRUE),
  mean_filing_rate_annual = mean(filing_rate_annual, na.rm = TRUE),
  mean_log_total_area = mean(log_total_area, na.rm = TRUE),
  mean_log_market_value = mean(log_market_value, na.rm = TRUE),
  share_apartment = mean(is_apartment, na.rm = TRUE)
), by = has_demog]
fwrite(balance, file.path(tab_dir, "compstats_annual_sample_balance.csv"))
logf("Sample balance (has_demog):\n", paste(capture.output(print(balance)), collapse = "\n"), log_file = log_file)


# ======================================================================
# SECTION 4: OUTPUT WRITING & QA
# ======================================================================
logf("=== Section 4: QA & Final Output ===", log_file = log_file)

qa_lines <- c(
  paste0("comp-statics-annual-race.R QA report"),
  paste0("Run date: ", Sys.time()),
  paste0("Years: ", start_year, "-", end_year, " (pre-COVID)"),
  paste0("use_pid_filter: ", use_pid_filter),
  "",
  paste0("Total panel rows: ", nrow(dt)),
  paste0("Total PIDs: ", uniqueN(dt$PID)),
  paste0("pct_black non-missing share: ", round(mean(!is.na(dt$pct_black)), 4)),
  paste0("pct_black_l1 non-missing share: ", round(mean(!is.na(dt$pct_black_l1)), 4)),
  paste0("log_med_rent non-missing share: ", round(mean(is.finite(dt$log_med_rent_raw)), 4)),
  paste0("filing_rate_annual non-missing share: ", round(mean(is.finite(dt$filing_rate_annual)), 4)),
  ""
)

# Regression ladder summary
if (length(all_models)) {
  qa_lines <- c(qa_lines, "=== Regression Ladder (9 specs) ===")
  for (nm in names(all_models)) {
    m <- all_models[[nm]]
    if (!is.null(m)) {
      ct <- tidy_coeftable(m)
      pct_row <- ct[term == "pct_black_l1"]
      if (nrow(pct_row)) {
        qa_lines <- c(qa_lines, sprintf("  %s: pct_black_l1 = %.4f (SE = %.4f, p = %.4g, N = %d)",
                                         nm, pct_row$estimate, pct_row$std_error, pct_row$p_value, nobs(m)))
      }
    }
  }
  qa_lines <- c(qa_lines, "")
}

# Variance decomposition summary
if (nrow(vardecomp)) {
  qa_lines <- c(qa_lines, "=== Variance Decomposition ===")
  for (i in seq_len(nrow(vardecomp))) {
    r <- vardecomp[i]
    qa_lines <- c(qa_lines, sprintf(
      "  %s: overall_sd=%.4f, between_pid=%.4f, within_pid=%.4f, R2_pid=%.4f, R2_geoid_year=%.4f, R2_pid_geoid_year=%.4f",
      r$variable, r$overall_sd, r$between_pid_sd, r$within_pid_sd,
      r$r2_pid, r$r2_geoid_year, r$r2_pid_geoid_year
    ))
  }
  qa_lines <- c(qa_lines, "")
}

writeLines(qa_lines, file.path(tab_dir, "compstats_annual_qa.txt"))

logf("Wrote tables to ", tab_dir, log_file = log_file)
logf("=== Finished comp-statics-annual-race.R ===", log_file = log_file)
