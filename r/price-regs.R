## ============================================================
## price-regs.R
## ============================================================
## Purpose: Hedonic rent regressions using raw long-run filing rate
##          (filing_rate_longrun_pre2019) as main covariate.
##
## Pipeline assumption:
##   1) make-rent-panel.R
##   2) make-occupancy-vars.R
##   3) make-analytic-sample.R  -> exports analytic_sample (PID x year)
##
## Inputs:  products/analytic_sample (from config)
## Outputs: output/tables/*.tex, output/figs/*.png
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(fixest)
  library(ggplot2)
  library(stringr)
})

# ---- Load config + helpers (project-local) ----
source("r/config.R")
source("r/helper-functions.R")
cfg <- read_config()
log_file <- p_out(cfg, "logs", "price-regs.log")
tab_dir <- p_out(cfg, "price-regs", "tables")
fig_dir <- p_out(cfg, "price-regs", "figs")
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
logf("=== Starting price-regs.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Load analytic sample ----
bldg_panel <- fread(p_product(cfg, "analytic_sample"))

# ---- Defensive checks ----
req_cols <- c("PID", "year", "log_med_rent", "total_units", "GEOID",
              "filing_rate_longrun_pre2019")
missing <- setdiff(req_cols, names(bldg_panel))
if (length(missing) > 0) {
  stop("Missing required columns in analytic_sample: ", paste(missing, collapse = ", "))
}

# Sample restriction consistent with "pre-period" hedonic interpretation
reg_dt <- bldg_panel[
  year %in% 2014:2019 &
    !is.na(filing_rate_longrun_pre2019) &
    filing_rate_longrun_pre2019 <= 0.75
]

# ---- Tenant composition controls ----
tenant_comp_raw <- c(
  "infousa_pct_black",
  "infousa_pct_female",
  "infousa_pct_black_female",
  "infousa_share_persons_demog_ok"
)
for (cc in tenant_comp_raw) {
  if (!(cc %in% names(reg_dt))) reg_dt[, (cc) := NA_real_]
  reg_dt[, (cc) := suppressWarnings(as.numeric(get(cc)))]
}
fill_with_mean <- function(x) {
  mu <- mean(x, na.rm = TRUE)
  if (!is.finite(mu)) mu <- 0
  fifelse(is.na(x), mu, x)
}
reg_dt[, tenant_comp_missing := as.integer(
  is.na(infousa_pct_black) |
    is.na(infousa_pct_female) |
    is.na(infousa_pct_black_female) |
    is.na(infousa_share_persons_demog_ok)
)]
reg_dt[, infousa_pct_black_imp := fill_with_mean(infousa_pct_black)]
reg_dt[, infousa_pct_female_imp := fill_with_mean(infousa_pct_female)]
reg_dt[, infousa_pct_black_female_imp := fill_with_mean(infousa_pct_black_female)]
reg_dt[, infousa_share_persons_demog_ok_imp := fill_with_mean(infousa_share_persons_demog_ok)]

# ------------------------------------------------------------------
# Derived variables
# ------------------------------------------------------------------
reg_dt[, census_tract := str_sub(GEOID, 1, 11)]

# Standardize quality grade: keep A-D, remove +/-, NA -> Unknown
reg_dt[, quality_grade_standard := fifelse(
  stringr::str_detect(quality_grade, "[ABCDabcd]"), quality_grade, NA_character_
)]
reg_dt[, quality_grade_standard := stringr::str_remove_all(quality_grade_standard, "[+-]")]
reg_dt[is.na(quality_grade_standard), quality_grade_standard := "Unknown"]

# Violation/complaint flags (ever, within PID)
reg_dt[, any_unsafe_dangerous_violation := any(
  imminently_dangerous_violation_count > 0 |
    unsafe_violation_count > 0 |
    hazardous_violation_count > 0
), by = PID]
reg_dt[, any_heat_fire_drainage_plumbing_complaint := any(
  heat_complaint_count > 0 |
    fire_complaint_count > 0 |
    structural_deficiency_complaint_count > 0 |
    drainage_complaint_count > 0
), by = PID]

# High-eviction indicator
reg_dt[, high_evict_raw := filing_rate_longrun_pre2019 > 0.2]

# Binned filing rate: 0, (0-5%], (5-10%], (10-20%], 20%+
reg_dt[, filing_rate_bin := factor(
  fcase(
    filing_rate_longrun_pre2019 == 0,    "0",
    filing_rate_longrun_pre2019 <= 0.05, "(0-5%]",
    filing_rate_longrun_pre2019 <= 0.10, "(5-10%]",
    filing_rate_longrun_pre2019 <= 0.20, "(10-20%]",
    default = "20%+"
  ),
  levels = c("0", "(0-5%]", "(5-10%]", "(10-20%]", "20%+")
)]
reg_dt[, filing_rate_bin := relevel(filing_rate_bin, ref = "(5-10%]")]

# ------------------------------------------------------------------
# Hedonic specifications
# ------------------------------------------------------------------

# (1) Binned: relative rent levels by raw filing rate category
m_bin <- feols(
  log_med_rent ~ filing_rate_bin +
    any_unsafe_dangerous_violation +
    any_heat_fire_drainage_plumbing_complaint +
    log(total_area) + log(market_value) |
    year + GEOID + year_blt_decade + source + num_units_bin +
    building_type + quality_grade_standard + num_stories_bin,
  data    = reg_dt,
  weights = ~ total_units,
  cluster = ~ PID,
  combine.quick = TRUE
)

# (2) Continuous: raw filing rate, same FE
m_cont <- feols(
  log_med_rent ~ filing_rate_longrun_pre2019 +
    log(total_area) + log(market_value) |
    year + GEOID + year_blt_decade + source + num_units_bin +
    building_type + quality_grade_standard + num_stories_bin,
  data    = reg_dt,
  weights = ~ total_units,
  cluster = ~ PID,
  combine.quick = TRUE
)

# (3) Binned + tenant composition controls
m_bin_tenant <- feols(
  log_med_rent ~ filing_rate_bin +
    any_unsafe_dangerous_violation +
    any_heat_fire_drainage_plumbing_complaint +
    log(total_area) + log(market_value) +
    infousa_pct_black_imp + infousa_pct_female_imp +
    infousa_pct_black_female_imp + infousa_share_persons_demog_ok_imp +
    tenant_comp_missing |
    year + GEOID + year_blt_decade + source + num_units_bin +
    building_type + quality_grade_standard + num_stories_bin,
  data    = reg_dt,
  weights = ~ total_units,
  cluster = ~ PID,
  combine.quick = TRUE
)

# (4) Continuous + tenant composition controls
m_cont_tenant <- feols(
  log_med_rent ~ filing_rate_longrun_pre2019 +
    log(total_area) + log(market_value) +
    infousa_pct_black_imp + infousa_pct_female_imp +
    infousa_pct_black_female_imp + infousa_share_persons_demog_ok_imp +
    tenant_comp_missing |
    year + GEOID + year_blt_decade + source + num_units_bin +
    building_type + quality_grade_standard + num_stories_bin,
  data    = reg_dt,
  weights = ~ total_units,
  cluster = ~ PID,
  combine.quick = TRUE
)

# (5) Complaint x high-eviction interaction
m_hb <- feols(
  log_med_rent ~ any_heat_fire_drainage_plumbing_complaint * high_evict_raw +
    log(total_area) + log(market_value) |
    year + GEOID + num_units_bin,
  data    = reg_dt,
  weights = ~ total_units,
  cluster = ~ PID,
  combine.quick = TRUE
)

# ------------------------------------------------------------------
# Tables
# ------------------------------------------------------------------
setFixest_dict(c(
  "filing_rate_bin0" = "Filing rate = 0",
  "filing_rate_bin(0-5%]" = "Filing rate (0, 5%]",
  "filing_rate_bin(10-20%]" = "Filing rate (10, 20%]",
  "filing_rate_bin20%+" = "Filing rate 20%+",
  "filing_rate_longrun_pre2019" = "Filing rate (pre-2019)"
))

fixest::setFixest_etable(digits = 4, fitstat = c("n", "r2"))

tab <- etable(
  m_bin, m_cont,
  title = "Hedonic rent regressions: raw long-run filing rate (pre-2019)",
  keep  = "%filing_rate_bin|%filing_rate_longrun_pre2019",
  order = "%filing_rate_bin|%filing_rate_longrun_pre2019",
  tex   = TRUE
)
writeLines(tab, file.path(tab_dir, "hedonic_filing_rate_longrun.tex"))

tab_tenant <- etable(
  m_bin, m_cont, m_bin_tenant, m_cont_tenant,
  title = "Hedonic rent regressions with tenant composition controls (raw filing rate)",
  tex = TRUE
)
writeLines(tab_tenant, file.path(tab_dir, "hedonic_filing_rate_longrun_with_tenant_composition.tex"))

# ---- Coefficient comparison table ----
extract_model_coefs <- function(model, model_name, term_pattern) {
  ct <- as.data.table(summary(model)$coeftable, keep.rownames = "term")
  setnames(ct, c("Estimate", "Std. Error", "Pr(>|t|)"), c("estimate", "std_error", "p_value"))
  ct <- ct[grepl(term_pattern, term)]
  if (!nrow(ct)) return(data.table())
  ct[, model := model_name]
  ct[, .(model, term, estimate, std_error, p_value)]
}

coef_compare <- rbindlist(
  list(
    extract_model_coefs(m_bin, "baseline_bin", "^filing_rate_bin"),
    extract_model_coefs(m_bin_tenant, "tenant_bin", "^filing_rate_bin"),
    extract_model_coefs(m_cont, "baseline_cont", "^filing_rate_longrun_pre2019$"),
    extract_model_coefs(m_cont_tenant, "tenant_cont", "^filing_rate_longrun_pre2019$")
  ),
  use.names = TRUE,
  fill = TRUE
)
if (!nrow(coef_compare)) {
  coef_compare <- data.table(
    model = character(), term = character(),
    estimate = numeric(), std_error = numeric(), p_value = numeric()
  )
}
fwrite(
  coef_compare,
  file.path(tab_dir, "hedonic_filing_rate_longrun_tenant_composition_coef_compare.csv")
)

# ---- Coefficient plot for binned model ----
ct <- coeftable(m_bin, keep = "filing_rate_bin")
coef_dt <- as.data.table(ct, keep.rownames = "term")
if (nrow(coef_dt) > 0) {
  # Clean up term names and add the reference category at 0
  coef_dt[, term := gsub("^filing_rate_bin", "", term)]
  ref_row <- data.table(term = "(5-10%]", Estimate = 0, `Std. Error` = 0,
                         `t value` = NA_real_, `Pr(>|t|)` = NA_real_)
  coef_dt <- rbind(coef_dt, ref_row, fill = TRUE)
  bin_order <- c("0", "(0-5%]", "(5-10%]", "(10-20%]", "20%+")
  coef_dt[, term := factor(term, levels = bin_order)]

  p <- ggplot(coef_dt, aes(x = term, y = Estimate)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
    geom_point(size = 3) +
    geom_errorbar(aes(ymin = Estimate - 1.96 * `Std. Error`,
                      ymax = Estimate + 1.96 * `Std. Error`),
                  width = 0.15) +
    labs(
      x = "Filing rate bin (pre-2019)",
      y = "Effect on log rent (relative to (5-10%])",
      title = "Hedonic penalty by filing rate bin (raw long-run rate)",
      subtitle = "Tract and year FE; controls for units bin, decade built, building type, and source"
    ) +
    theme_minimal()

  ggsave(file.path(fig_dir, "coefplot_hedonic_filing_rate_longrun.png"),
         p, width = 8, height = 5, bg = "white")
}

# Console summaries
print(summary(m_bin))
print(summary(m_cont))
print(summary(m_bin_tenant))
print(summary(m_cont_tenant))

logf(
  "Built additive tenant-composition models. Non-missing share (raw): black=",
  round(mean(!is.na(reg_dt$infousa_pct_black)), 4),
  ", female=", round(mean(!is.na(reg_dt$infousa_pct_female)), 4),
  ", black_female=", round(mean(!is.na(reg_dt$infousa_pct_black_female)), 4),
  ", demog_ok=", round(mean(!is.na(reg_dt$infousa_share_persons_demog_ok)), 4),
  log_file = log_file
)

# ------------------------------------------------------------------
# Diagnostics: residual variation and spline filing curve
# ------------------------------------------------------------------

## Residualize filing_rate_longrun_pre2019 on same controls/FE
dt_resid <- copy(reg_dt)

m_resid <- feols(
  filing_rate_longrun_pre2019 ~ 1 + log(market_value) + log(total_area) |
    year + GEOID + year_blt_decade + source + num_units_bin +
    building_type + quality_grade_standard + num_stories_bin,
  data    = dt_resid,
  weights = ~ total_units
)

dt_resid[, filing_rate_longrun_pre2019_resid :=
           filing_rate_longrun_pre2019 - predict(m_resid, newdata = dt_resid)]

resid_sd <- sd(dt_resid$filing_rate_longrun_pre2019_resid, na.rm = TRUE)
resid_p95 <- quantile(abs(dt_resid$filing_rate_longrun_pre2019_resid), 0.95, na.rm = TRUE)

logf(
  "Residualized raw filing rate: SD=", round(resid_sd, 4),
  "; P95 abs(resid)=", round(resid_p95, 4),
  "; R2=", round(r2(m_resid, "pr2"), 4),
  log_file = log_file
)

# 1) Histogram (weighted by units)
p_hist <- ggplot(
  dt_resid[is.finite(filing_rate_longrun_pre2019_resid)],
  aes(x = filing_rate_longrun_pre2019_resid, weight = total_units)
) +
  geom_histogram(bins = 80) +
  scale_x_continuous(breaks = scales::pretty_breaks(10)) +
  labs(
    title = "Residual variation in raw filing rate (pre-2019)",
    subtitle = "Residualized on year + tract + size bin + decade built + source + log(market_value);\n weighted by imputed units",
    x = "Residualized filing rate",
    y = "Weighted count (units)"
  ) +
  theme_philly_evict()

ggsave(
  filename = file.path(fig_dir, "resid_filing_rate_longrun_pre2019_hist.png"),
  plot = p_hist, width = 9, height = 5, dpi = 300
)

# 2) Density by source
p_dens <- ggplot(
  dt_resid[is.finite(filing_rate_longrun_pre2019_resid)],
  aes(x = filing_rate_longrun_pre2019_resid, color = source, weight = total_units)
) +
  geom_density() +
  labs(
    title = "Residualized raw filing rate by source",
    subtitle = "Same residualization; weighted by units",
    x = "Residualized filing rate",
    y = "Weighted density"
  )

ggsave(
  filename = file.path(fig_dir, "resid_filing_rate_longrun_pre2019_density_by_source.png"),
  plot = p_dens, width = 10, height = 5, dpi = 300
)

# 3) Residual SD by size bin
sd_by_size <- dt_resid[
  is.finite(filing_rate_longrun_pre2019_resid),
  .(sd_resid = sd(filing_rate_longrun_pre2019_resid), n = .N),
  by = num_units_bin
][order(num_units_bin)]

fwrite(sd_by_size, file.path(tab_dir, "resid_filing_rate_longrun_sd_by_units_bin.csv"))

# ---- Residualized rent vs residualized filing rate ----
m_price_resid <- feols(
  log_med_rent ~ 1 + log(market_value) + log(total_area) |
    year + GEOID + year_blt_decade + source + num_units_bin +
    building_type + quality_grade_standard + num_stories_bin,
  data    = dt_resid,
  weights = ~ total_units
)

dt_resid[, log_med_rent_resid := log_med_rent - predict(m_price_resid, newdata = dt_resid)]

# Scatter
p_scatter <- ggplot(
  dt_resid[is.finite(filing_rate_longrun_pre2019_resid) &
             is.finite(log_med_rent_resid) & total_units >= 10],
  aes(x = filing_rate_longrun_pre2019_resid, y = log_med_rent_resid, weight = total_units)
) +
  geom_point() +
  geom_smooth(method = "loess", span = 0.5, se = FALSE, aes(color = "Loess: Span = 0.5")) +
  geom_smooth(method = "loess", span = 0.75, se = FALSE, aes(color = "Loess: Span = 0.75")) +
  geom_smooth(method = "loess", span = 1, se = FALSE, aes(color = "Loess: Span = 1")) +
  geom_smooth(method = "gam", se = FALSE, aes(color = "GAM")) +
  geom_smooth(method = "lm", aes(color = "LM")) +
  scale_y_continuous(breaks = scales::pretty_breaks(20)) +
  labs(
    title = "Residualized log rent vs residualized raw filing rate",
    subtitle = "Both residualized on same controls/FE; weighted by imputed units",
    x = "Residualized filing rate (pre-2019)",
    y = "Residualized log median rent"
  ) +
  theme_philly_evict()

ggsave(file.path(fig_dir, "scatter_resid_rent_vs_filing_rate_longrun.png"),
       p_scatter, width = 10, height = 6, dpi = 300, bg = "white")

# Bin scatter
dt_resid[, filing_rate_longrun_pre2019_resid_bin05 := cut(
  filing_rate_longrun_pre2019_resid,
  breaks = seq(-0.5, 0.5, by = 0.05),
  include.lowest = TRUE
)]

bin_scatter <- dt_resid[
  is.finite(filing_rate_longrun_pre2019_resid) & is.finite(log_med_rent_resid),
  .(mean_filing_resid = weighted.mean(filing_rate_longrun_pre2019_resid, w = total_units, na.rm = TRUE),
    mean_rent_resid = weighted.mean(log_med_rent_resid, w = total_units, na.rm = TRUE),
    total_units = sum(total_units)),
  by = filing_rate_longrun_pre2019_resid_bin05
]

p_bin_scatter <- ggplot(
  bin_scatter[total_units > 500],
  aes(x = mean_filing_resid, y = mean_rent_resid, size = total_units, weight = total_units)
) +
  geom_point(alpha = 0.7) +
  geom_smooth(method = "loess", span = 1, se = FALSE) +
  geom_smooth(method = "gam", aes(color = "GAM"), se = FALSE) +
  labs(
    title = "Binned: Residualized log rent vs residualized raw filing rate",
    subtitle = "Both residualized on same controls/FE; weighted by imputed units",
    x = "Residualized filing rate (pre-2019)",
    y = "Residualized log median rent"
  ) +
  theme_philly_evict()

ggsave(file.path(fig_dir, "binscatter_resid_rent_vs_filing_rate_longrun.png"),
       p_bin_scatter, width = 10, height = 6, dpi = 300, bg = "white")

# ------------------------------------------------------------------
# Spline filing curves: linear, quadratic, cubic
# ------------------------------------------------------------------
library(splines)

setDT(reg_dt)

# Place knots manually: concentrate at low filing rates where most variation lives
spline_knots <- c(0.01, 0.025, 0.05, 0.08, 0.12, 0.20)

# Hurdle: never-filers vs ever-filers
reg_dt[, never_filed_pre2019 := as.integer(replace(total_filings_pre2019, is.na(total_filings_pre2019), 0) == 0L)]
reg_dt[, filing_rate_longrun_pre2019_filed :=
         fifelse(never_filed_pre2019 == 1L, NA_real_, filing_rate_longrun_pre2019)]

x_filed <- reg_dt$filing_rate_longrun_pre2019_filed
bknots <- quantile(x_filed, c(0.005, 0.995), na.rm = TRUE)

# ---- Helper: estimate one spline spec and return grid + model ----
# Accepts custom FE string, extra RHS controls, and data subset
estimate_spline_spec <- function(dt_in, degree, label, fe_str,
                                  extra_rhs = NULL, hurdle = TRUE) {
  dt <- copy(dt_in)
  dt <- dt[, .SD, .SDcols = !grepl("^spline_", names(dt))]

  if (hurdle) {
    # Hurdle model: never-filer dummy + spline on ever-filers only
    x_var <- dt$filing_rate_longrun_pre2019_filed
    bk <- quantile(x_var, c(0.005, 0.995), na.rm = TRUE)
  } else {
    # No hurdle: spline spans full range including zero
    x_var <- dt$filing_rate_longrun_pre2019
    bk <- c(0, quantile(x_var[x_var > 0], 0.995, na.rm = TRUE))
  }

  if (degree == 3L) {
    B <- ns(x_var, knots = spline_knots, Boundary.knots = bk)
  } else {
    B <- bs(x_var, knots = spline_knots, Boundary.knots = bk, degree = degree)
  }
  n_sp <- ncol(B)
  sp_nm <- paste0("spline_", 1:n_sp)
  dt[, (sp_nm) := as.data.table(B)]

  if (hurdle) {
    dt[never_filed_pre2019 == 1L, (sp_nm) := 0]
    rhs_parts <- c("never_filed_pre2019", sp_nm,
                    "log(market_value)", "log(total_area)", "log(total_units)")
  } else {
    rhs_parts <- c(sp_nm,
                    "log(market_value)", "log(total_area)", "log(total_units)")
  }
  if (!is.null(extra_rhs)) rhs_parts <- c(rhs_parts, extra_rhs)
  rhs <- paste(rhs_parts, collapse = " + ")
  fml <- as.formula(paste0("log_med_rent ~ ", rhs, " | ", fe_str))
  m <- feols(fml, data = dt, weights = ~ total_units, cluster = ~ PID)

  # Extract coefficients
  b_all <- coef(m)
  b_sp <- b_all[sp_nm]

  if (hurdle) {
    never_name <- grep("^never_filed_pre2019", names(b_all), value = TRUE)
    stopifnot(length(never_name) == 1)
    beta_never <- as.numeric(b_all[never_name])
  } else {
    beta_never <- NA_real_
  }

  # Vcov for delta-method CI
  V_all <- vcov(m, cluster = "PID")
  V_sp <- V_all[sp_nm, sp_nm, drop = FALSE]

  # Grid prediction: start at 0 for non-hurdle, or 0.5th pctile for hurdle
  if (hurdle) {
    x_vec <- dt$filing_rate_longrun_pre2019_filed
    grid_lo <- quantile(x_vec, 0.01, na.rm = TRUE)
  } else {
    grid_lo <- 0
  }
  grid_hi <- quantile(dt$filing_rate_longrun_pre2019[dt$filing_rate_longrun_pre2019 > 0],
                       0.9, na.rm = TRUE)
  grid <- data.table(filing_rate = seq(grid_lo, grid_hi, length.out = 200))

  if (degree == 3L) {
    Bgrid <- ns(grid$filing_rate, knots = spline_knots, Boundary.knots = bk)
  } else {
    Bgrid <- bs(grid$filing_rate, knots = spline_knots,
                Boundary.knots = bk, degree = degree)
  }
  colnames(Bgrid) <- sp_nm

  g_hat <- as.vector(Bgrid %*% b_sp)

  if (hurdle) {
    # Relative to never-filer intercept
    grid[, y := g_hat - beta_never]
    var_never <- as.numeric(V_all[never_name, never_name])
    cov_never_sp <- V_all[never_name, sp_nm, drop = FALSE]
    var_g <- diag(Bgrid %*% V_sp %*% t(Bgrid))
    cov_ng <- as.vector(cov_never_sp %*% t(Bgrid))
    se_y <- sqrt(pmax(0, var_g + var_never - 2 * cov_ng))
  } else {
    # Relative to g(0): the spline value at filing_rate = 0
    if (degree == 3L) {
      B0 <- ns(0, knots = spline_knots, Boundary.knots = bk)
    } else {
      B0 <- bs(0, knots = spline_knots, Boundary.knots = bk, degree = degree)
    }
    g0 <- as.vector(B0 %*% b_sp)
    grid[, y := g_hat - g0]
    # Delta-method: var(g(x) - g(0)) = var(g(x)) + var(g(0)) - 2*cov(g(x),g(0))
    Bdiff <- sweep(Bgrid, 2, as.vector(B0))  # Bgrid - B0 row-wise
    var_diff <- diag(Bdiff %*% V_sp %*% t(Bdiff))
    se_y <- sqrt(pmax(0, var_diff))
  }
  grid[, lo := y - 1.96 * se_y]
  grid[, hi := y + 1.96 * se_y]
  grid[, spec := label]

  list(grid = grid, model = m, beta_never = beta_never,
       b_sp = b_sp, sp_nm = sp_nm, dt = dt, bk = bk, hurdle = hurdle)
}

# ---- Prepare extra controls ----
# num_years_pre2019: buildings observed longer have more chances to file
if (!"num_years_pre2019" %in% names(reg_dt)) {
  reg_dt[, num_years_pre2019 := sum(year <= 2019), by = PID]
}

# FE strings for different specs
fe_base <- paste(c("year", "census_tract", "year_blt_decade", "num_units_bin",
                    "building_type", "quality_grade_standard", "num_stories_bin"),
                 collapse = " + ")
fe_with_source <- paste(c(fe_base, "source"), collapse = " + ")

# ---- A) Degree comparison (no hurdle, baseline FE) ----
specs_degree <- list(
  list(degree = 1L, label = "Linear (degree 1)"),
  list(degree = 2L, label = "Quadratic (degree 2)"),
  list(degree = 3L, label = "Cubic (ns, degree 3)")
)

spline_results <- lapply(specs_degree, function(s) {
  estimate_spline_spec(reg_dt, degree = s$degree, label = s$label,
                       fe_str = fe_base, hurdle = FALSE)
})
names(spline_results) <- c("linear", "quadratic", "cubic")

# Combine grids for overlay plot
all_grids <- rbindlist(lapply(spline_results, `[[`, "grid"))

# ---- Non-hurdle cubic: spline through zero ----
nohurdle_cubic <- estimate_spline_spec(
  reg_dt, degree = 3L, label = "Cubic (no hurdle)",
  fe_str = fe_base, hurdle = FALSE
)

# ---- Hurdle cubic: separate intercept for never-filers ----
hurdle_cubic <- estimate_spline_spec(
  reg_dt, degree = 3L, label = "Cubic (hurdle)",
  fe_str = fe_base, hurdle = TRUE
)

# ---- Partial-residual bins (from NON-HURDLE cubic model) ----
m_nh <- nohurdle_cubic$model
dt_nh <- nohurdle_cubic$dt
sp_nm_nh <- nohurdle_cubic$sp_nm
b_sp_nh <- nohurdle_cubic$b_sp
bk_nh <- nohurdle_cubic$bk

sel_nh <- !is.na(weights(m_nh))
dt_used <- dt_nh[sel_nh]
dt_used[, g_i := as.vector(as.matrix(.SD) %*% b_sp_nh), .SDcols = sp_nm_nh]
dt_used[, eps := resid(m_nh)]
dt_used[, f_partial := g_i + eps]

# Compute g(0) for reference
B0_nh <- ns(0, knots = spline_knots, Boundary.knots = bk_nh)
g0_nh <- as.vector(B0_nh %*% b_sp_nh)
dt_used[, f_partial_rel := f_partial - g0_nh]

# Bin all observations (including never-filers at 0)
bin_brks <- c(-0.001, 0.001, 0.005, 0.01, 0.02, 0.03, 0.05, 0.075,
              0.10, 0.15, 0.20, 0.30, Inf)
dt_used[, bin := cut(filing_rate_longrun_pre2019, breaks = bin_brks,
                     include.lowest = TRUE)]
n_bins <- length(bin_brks) - 1

bin_means <- dt_used[!is.na(bin), .(
  x_bin = weighted.mean(filing_rate_longrun_pre2019, w = total_units, na.rm = TRUE),
  y_bin = weighted.mean(f_partial_rel, w = total_units, na.rm = TRUE),
  n_bin = .N,
  w_bin = sum(total_units, na.rm = TRUE)
), by = bin][order(x_bin)]

# Trim to grid range
grid_range_nh <- range(nohurdle_cubic$grid$filing_rate)
bin_means <- bin_means[x_bin >= grid_range_nh[1] & x_bin <= grid_range_nh[2]]

all_pts <- data.table(
  x = bin_means$x_bin, y = bin_means$y_bin,
  type = fifelse(bin_means$x_bin < 0.001, "Never-filers", "Ever-filers"),
  w = bin_means$w_bin
)

# ---- Individual spline plot (no-hurdle cubic, anchored at zero) ----
grid_cubic <- nohurdle_cubic$grid

p_spline <- ggplot() +
  geom_ribbon(
    data = grid_cubic,
    aes(x = filing_rate, ymin = lo, ymax = hi),
    alpha = 0.2
  ) +
  geom_line(
    data = grid_cubic,
    aes(x = filing_rate, y = y),
    linewidth = 1
  ) +
  geom_point(
    data = all_pts,
    aes(x = x, y = y, size = w), alpha = 0.85
  ) +
  scale_x_continuous(breaks = scales::pretty_breaks(20), limits = c(0, 0.25)) +
  scale_y_continuous(breaks = scales::pretty_breaks(10)) +
  labs(
    x = "Raw filing rate (pre-2019)",
    y = "Partial residual (filing component) vs rate = 0",
    title = "Spline-implied filing curve (cubic)",
    subtitle = paste0(
      "Cubic natural spline; boundary knot at 0; ",
      "points = binned partial residuals; weights = total_units"
    )
  ) +
  theme_philly_evict()

ggsave(file.path(fig_dir, "spline_filing_curve_raw.png"),
       p_spline, width = 10, height = 6, dpi = 300, bg = "white")

# ---- Hurdle vs non-hurdle comparison plot ----
compare_hurdle_grids <- rbind(hurdle_cubic$grid, nohurdle_cubic$grid)

p_hurdle_compare <- ggplot() +
  geom_ribbon(
    data = compare_hurdle_grids,
    aes(x = filing_rate, ymin = lo, ymax = hi, fill = spec),
    alpha = 0.12
  ) +
  geom_line(
    data = compare_hurdle_grids,
    aes(x = filing_rate, y = y, color = spec),
    linewidth = 1
  ) +
  geom_point(
    data = all_pts,
    aes(x = x, y = y, size = w),
    alpha = 0.6, color = "grey30"
  ) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
  scale_x_continuous(breaks = scales::pretty_breaks(20), limits = c(0, 0.25)) +
  scale_y_continuous(breaks = scales::pretty_breaks(10)) +
  scale_color_manual(values = c("Cubic (hurdle)" = "#E41A1C",
                                "Cubic (no hurdle)" = "#4DAF4A")) +
  scale_fill_manual(values = c("Cubic (hurdle)" = "#E41A1C",
                               "Cubic (no hurdle)" = "#4DAF4A")) +
  labs(
    x = "Raw filing rate (pre-2019)",
    y = "Fitted filing component vs rate = 0",
    title = "Hurdle vs no-hurdle spline: boundary artifact diagnostic",
    subtitle = "Hurdle: never-filer dummy + spline on ever-filers; No hurdle: spline through zero",
    color = "Specification",
    fill = "Specification",
    size = "Total units"
  ) +
  theme_philly_evict() +
  theme(legend.position = "right")

ggsave(file.path(fig_dir, "spline_hurdle_vs_nohurdle.png"),
       p_hurdle_compare, width = 12, height = 7, dpi = 300, bg = "white")

# ---- Overlay comparison: linear vs quadratic vs cubic ----
p_compare <- ggplot() +
  # CI ribbons for each spec
  geom_ribbon(
    data = all_grids,
    aes(x = filing_rate, ymin = lo, ymax = hi, fill = spec),
    alpha = 0.12
  ) +
  # Fitted curves
  geom_line(
    data = all_grids,
    aes(x = filing_rate, y = y, color = spec),
    linewidth = 1
  ) +
  # Binned partial residuals (from cubic)
  geom_point(
    data = all_pts,
    aes(x = x, y = y, size = w),
    alpha = 0.6, color = "grey30"
  ) +
  scale_x_continuous(breaks = scales::pretty_breaks(20), limits = c(0, 0.25)) +
  scale_y_continuous(breaks = scales::pretty_breaks(10)) +
  scale_color_manual(values = c("Linear (degree 1)" = "#E41A1C",
                                "Quadratic (degree 2)" = "#377EB8",
                                "Cubic (ns, degree 3)" = "#4DAF4A")) +
  scale_fill_manual(values = c("Linear (degree 1)" = "#E41A1C",
                               "Quadratic (degree 2)" = "#377EB8",
                               "Cubic (ns, degree 3)" = "#4DAF4A")) +
  labs(
    x = "Raw filing rate (pre-2019), ever-filers",
    y = "Fitted filing component vs never-filers",
    title = "filing curve robustness: linear vs quadratic vs cubic splines",
    subtitle = paste0(
      "Knots at ", paste(spline_knots * 100, collapse = ", "), "%; ",
      "shaded = 95% CI; points = binned partial residuals (cubic model)"
    ),
    color = "Spline degree",
    fill = "Spline degree",
    size = "Total units"
  ) +
  theme_philly_evict() +
  theme(legend.position = "right")

ggsave(file.path(fig_dir, "spline_filing_curve_comparison.png"),
       p_compare, width = 12, height = 7, dpi = 300, bg = "white")

logf(
  "Spline comparison: linear R2=", round(r2(spline_results$linear$model, "wr2"), 4),
  ", quadratic R2=", round(r2(spline_results$quadratic$model, "wr2"), 4),
  ", cubic R2=", round(r2(spline_results$cubic$model, "wr2"), 4),
  log_file = log_file
)

# ------------------------------------------------------------------
# B) Robustness: cubic spline (no hurdle) under different controls
# ------------------------------------------------------------------
# All specs use hurdle=FALSE (spline through zero) to avoid boundary
# artifacts from the hurdle parameterization.

logf("Running spline robustness variants (no hurdle)...", log_file = log_file)

hump_specs <- list(
  list(
    label = "Baseline",
    dt = reg_dt,
    fe = fe_base,
    extra_rhs = NULL
  ),
  list(
    label = "+ Source FE",
    dt = reg_dt,
    fe = fe_with_source,
    extra_rhs = NULL
  ),
  list(
    label = "+ Source FE + panel tenure",
    dt = reg_dt,
    fe = fe_with_source,
    extra_rhs = "num_years_pre2019"
  ),
  list(
    label = "Multi-family only",
    dt = reg_dt[total_units > 1],
    fe = fe_base,
    extra_rhs = NULL
  ),
  list(
    label = "+ Unit bin x Source FE",
    dt = reg_dt,
    fe = gsub("num_units_bin \\+ ", "num_units_bin^source + ",
              gsub("\\+ source", "", fe_with_source)),
    extra_rhs = NULL
  )
)

hump_results <- lapply(hump_specs, function(s) {
  estimate_spline_spec(s$dt, degree = 3L, label = s$label,
                       fe_str = s$fe, extra_rhs = s$extra_rhs,
                       hurdle = FALSE)
})

hump_grids <- rbindlist(lapply(hump_results, `[[`, "grid"))
hump_grids[, spec := factor(spec, levels = sapply(hump_specs, `[[`, "label"))]

# Log g(0) benchmark for each variant
for (i in seq_along(hump_specs)) {
  logf(
    "Robustness variant '", hump_specs[[i]]$label,
    "': n=", nobs(hump_results[[i]]$model),
    log_file = log_file
  )
}

# ---- Overlay plot: robustness ----
hump_colors <- c(
  "Baseline" = "#999999",
  "+ Source FE" = "#E41A1C",
  "+ Source FE + panel tenure" = "#377EB8",
  "Multi-family only" = "#FF7F00",
  "+ Unit bin x Source FE" = "#984EA3"
)

p_hump <- ggplot() +
  geom_ribbon(
    data = hump_grids,
    aes(x = filing_rate, ymin = lo, ymax = hi, fill = spec),
    alpha = 0.08
  ) +
  geom_line(
    data = hump_grids,
    aes(x = filing_rate, y = y, color = spec),
    linewidth = 0.9
  ) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
  scale_x_continuous(breaks = scales::pretty_breaks(20), limits = c(0, 0.15)) +
  scale_y_continuous(breaks = scales::pretty_breaks(10)) +
  scale_color_manual(values = hump_colors) +
  scale_fill_manual(values = hump_colors) +
  labs(
    x = "Raw filing rate (pre-2019)",
    y = "Fitted filing component vs rate = 0",
    title = "Spline robustness: cubic (no hurdle) under alternative controls",
    subtitle = "Cubic natural spline with boundary knot at 0; all weighted by total_units",
    color = "Specification",
    fill = "Specification"
  ) +
  theme_philly_evict() +
  theme(legend.position = "right")

ggsave(file.path(fig_dir, "spline_hump_diagnostic.png"),
       p_hump, width = 13, height = 7, dpi = 300, bg = "white")

logf("Spline robustness plot saved.", log_file = log_file)

# ------------------------------------------------------------------
# Diagnostic: never-filer vs low-filer composition
# ------------------------------------------------------------------
# The spline curve shows a hump near 0: buildings with very low positive
# filing rates have *higher* rents than never-filers. This diagnostic
# checks whether never-filers differ systematically on observables.

reg_dt[, filer_group := fcase(
  never_filed_pre2019 == 1L, "Never filed",
  filing_rate_longrun_pre2019 <= 0.01, "Filed: (0, 1%]",
  filing_rate_longrun_pre2019 <= 0.03, "Filed: (1, 3%]",
  filing_rate_longrun_pre2019 <= 0.05, "Filed: (3, 5%]",
  filing_rate_longrun_pre2019 <= 0.10, "Filed: (5, 10%]",
  default = "Filed: 10%+"
)]
reg_dt[, filer_group := factor(filer_group, levels = c(
  "Never filed", "Filed: (0, 1%]", "Filed: (1, 3%]",
  "Filed: (3, 5%]", "Filed: (5, 10%]", "Filed: 10%+"
))]

# Weighted means of key observables by filer group
diag_vars <- c("log_med_rent", "total_units", "market_value", "total_area")
# Add source shares and building type shares
reg_dt[, is_altos := as.integer(source == "altos")]
reg_dt[, is_sfh := as.integer(total_units <= 1)]
reg_dt[, log_market_value := fifelse(market_value > 0, log(market_value), NA_real_)]
reg_dt[, log_total_area := fifelse(total_area > 0, log(total_area), NA_real_)]

comp_table <- reg_dt[, .(
  n_pid_years       = .N,
  n_pids            = uniqueN(PID),
  mean_log_rent     = weighted.mean(log_med_rent, w = total_units, na.rm = TRUE),
  mean_total_units  = weighted.mean(total_units, w = total_units, na.rm = TRUE),
  median_total_units = as.double(median(total_units, na.rm = TRUE)),
  mean_log_mkt_val  = weighted.mean(log_market_value, w = total_units, na.rm = TRUE),
  mean_log_area     = weighted.mean(log_total_area, w = total_units, na.rm = TRUE),
  pct_altos         = weighted.mean(is_altos, w = total_units, na.rm = TRUE),
  pct_sfh           = weighted.mean(is_sfh, w = total_units, na.rm = TRUE),
  pct_quality_A     = weighted.mean(quality_grade_standard == "A", w = total_units, na.rm = TRUE),
  pct_quality_B     = weighted.mean(quality_grade_standard == "B", w = total_units, na.rm = TRUE),
  pct_quality_C     = weighted.mean(quality_grade_standard == "C", w = total_units, na.rm = TRUE),
  pct_quality_unknown = weighted.mean(quality_grade_standard == "Unknown", w = total_units, na.rm = TRUE),
  pct_pre1900       = weighted.mean(year_blt_decade == "pre-1900", w = total_units, na.rm = TRUE),
  pct_black_imp     = weighted.mean(infousa_pct_black_imp, w = total_units, na.rm = TRUE),
  pct_tenant_missing = weighted.mean(tenant_comp_missing, w = total_units, na.rm = TRUE),
  total_unit_years  = sum(total_units, na.rm = TRUE)
), by = filer_group][order(filer_group)]

# Round for readability
num_cols <- setdiff(names(comp_table), "filer_group")
comp_table[, (num_cols) := lapply(.SD, function(x) round(x, 4)), .SDcols = num_cols]

fwrite(comp_table, file.path(tab_dir, "never_filer_vs_low_filer_composition.csv"))

logf("Never-filer vs low-filer composition table written.", log_file = log_file)

# Also print to console for quick inspection
cat("\n===== Never-filer vs low-filer composition =====\n")
print(comp_table)
cat("\n")

logf("=== Finished price-regs.R ===", log_file = log_file)
