## ============================================================
## price-regs.R
## ============================================================
## Purpose: Hedonic rent regressions using eviction filings intensity
##          (Empirical Bayes shrinkage as main covariate).
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
log_file <- p_out(cfg, "logs", "price-regs-eb.log")
tab_dir <- p_out(cfg, "tables")
fig_dir <- p_out(cfg, "figs")
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
logf("=== Starting price-regs-eb.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Load analytic sample ----
bldg_panel <- fread(p_product(cfg, "analytic_sample"))

# ---- Defensive checks ----
req_cols <- c("PID", "year", "log_med_rent", "total_units", "GEOID",
              "filing_rate_eb", "filing_rate_eb_asinh")
missing <- setdiff(req_cols, names(bldg_panel))
if (length(missing) > 0) {
  stop("Missing required columns in analytic_sample: ", paste(missing, collapse = ", "))
}


# Sample restriction consistent with “pre-period” hedonic interpretation
# (adjust years as needed; keep pre-2020 for baseline results)
reg_dt <- bldg_panel[
  year %in% 2014:2019 &
  #  is_rental_year == TRUE &
    !is.na(filing_rate_eb_pre_covid) &
    filing_rate_eb_pre_covid <= 0.75
]

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
# Hedonic specifications
# ------------------------------------------------------------------
reg_dt[,census_tract := str_sub(GEOID,1,11)]
# fix quality grade; remove non abcd, remove +/-
reg_dt[, quality_grade_standard := fifelse(stringr::str_detect(quality_grade, "[ABCDabcd]"), quality_grade, NA_character_)]
reg_dt[, quality_grade_standard := stringr::str_remove_all(quality_grade_standard, "[+-]")]
# replace missing w/ unknown
reg_dt[is.na(quality_grade_standard), quality_grade_standard := "Unknown"]

# get violations
reg_dt[,any_unsafe_dangerous_violation := any(imminently_dangerous_violation_count > 0 | unsafe_violation_count > 0 | hazardous_violation_count >0), by = PID]
reg_dt[,any_heat_fire_drainage_plumbing_complaint := any(heat_complaint_count > 0 | fire_complaint_count > 0 |structural_deficiency_complaint_count>0|
                                                           drainage_complaint_count > 0 ), by = PID]
reg_dt[,high_evict_eb := filing_rate_eb_pre_covid > 0.2]
# make filing_rate_longrun_pre2019 cuts at 0,0.05,0.1,0.2, 0.2+
reg_dt[,filing_rate_longrun_pre2019_cuts := cut(filing_rate_eb_pre_covid, breaks = c(-Inf, 0, 0.05, 0.1, 0.2, Inf),
                                              labels = c("0", "(0-5%]", "(5-10%]", "(10-20%]", "20%+"))]

# (1) Binned curvature: relative rent levels by EB filing intensity category
m_bin <- feols(
  log_med_rent ~ i(filing_rate_longrun_pre2019_cuts, ref = "(5-10%]")+any_unsafe_dangerous_violation+any_heat_fire_drainage_plumbing_complaint + log(total_area) +log(market_value)|
    year + GEOID   + year_blt_decade + source + num_units_bin + building_type + quality_grade_standard +num_stories_bin,
  data    = reg_dt[],
  weights = ~ total_units,
  cluster = ~ PID,
  combine.quick = TRUE
)

# (2) Continuous curvature: asinh(EB intensity), same FE
m_cont <- feols(
  log_med_rent ~ filing_rate_eb_pre_covid + log(total_area) +log(market_value)|
    year + GEOID   + year_blt_decade + source + num_units_bin + building_type + quality_grade_standard  +num_stories_bin,
  data    = reg_dt,
  weights = ~ total_units,
  cluster = ~ PID,
  combine.quick = TRUE
)

m_bin_tenant <- feols(
  log_med_rent ~ i(filing_rate_longrun_pre2019_cuts, ref = "(5-10%]") +
    any_unsafe_dangerous_violation +
    any_heat_fire_drainage_plumbing_complaint +
    log(total_area) + log(market_value) +
    infousa_pct_black_imp + infousa_pct_female_imp +
    infousa_pct_black_female_imp + infousa_share_persons_demog_ok_imp +
    tenant_comp_missing |
    year + GEOID + year_blt_decade + source + num_units_bin + building_type + quality_grade_standard + num_stories_bin,
  data = reg_dt[],
  weights = ~ total_units,
  cluster = ~ PID,
  combine.quick = TRUE
)

m_cont_tenant <- feols(
  log_med_rent ~ filing_rate_eb_pre_covid + log(total_area) + log(market_value) +
    infousa_pct_black_imp + infousa_pct_female_imp +
    infousa_pct_black_female_imp + infousa_share_persons_demog_ok_imp +
    tenant_comp_missing |
    year + GEOID + year_blt_decade + source + num_units_bin + building_type + quality_grade_standard + num_stories_bin,
  data = reg_dt,
  weights = ~ total_units,
  cluster = ~ PID,
  combine.quick = TRUE
)

m_hb <- feols(
  log_med_rent ~any_heat_fire_drainage_plumbing_complaint*high_evict_eb + log(total_area) +log(market_value)|
    year +GEOID + num_units_bin,
  data    = reg_dt[],
  weights = ~ total_units,
  cluster = ~ PID,
  combine.quick = TRUE
)

# ------------------------------------------------------------------
# Reputation effects (commented out for now)
# ------------------------------------------------------------------
# ## If you later want reputation effects (NOT RUN):
# ##  - Panel/event approach is often more credible than RD on the rating level.
# if ("google_star_rating" %in% names(bldg_panel)) {
#   # Example: threshold indicator at 4.0 stars (customize)
#   bldg_panel[, rating_ge_4 := fifelse(!is.na(google_star_rating), google_star_rating >= 4.0, NA)]
#
#   # Example: add to the pre-period hedonic as a control
#   m_rep <- feols(
#     log_med_rent ~ i(filing_rate_eb_cuts, ref="(7.5-20%]") + rating_ge_4 |
#       year + GEOID + num_units_bin + year_blt_decade + source,
#     data = reg_dt, weights = ~total_units, cluster = ~PID
#   )
# }

# ------------------------------------------------------------------
# Tables + quick figure
# ------------------------------------------------------------------

setFixest_dict(c(
  "filing_rate_eb_cuts" = "EB filing intensity (pre-2019)",
  "filing_rate_eb_cuts::0" = "0",
  "filing_rate_eb_cuts::(0-7.5%]" = "(0, 7.5%]",
  "filing_rate_eb_cuts::20%+" = "20%+",
  "filing_rate_eb_pre_covid_asinh" = "asinh(EB filing intensity, pre-2019)"
))

fixest::setFixest_etable(digits = 4, fitstat = c("n", "r2"))

tab <- etable(
  m_bin, m_cont,
  title = "Hedonic rent regressions: eviction filings intensity (Empirical Bayes)",
  keep  = "filing_rate_eb_",
  order = "filing_rate_eb_",
  tex   = TRUE
)

writeLines(tab, p_out(cfg, "tables", "hedonic_eviction_intensity_eb.tex"))

tab_tenant <- etable(
  m_bin, m_cont, m_bin_tenant, m_cont_tenant,
  title = "Hedonic rent regressions with tenant composition controls (additive)",
  tex = TRUE
)
writeLines(tab_tenant, p_out(cfg, "tables", "hedonic_eviction_intensity_eb_with_tenant_composition.tex"))

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
    extract_model_coefs(m_bin, "baseline_bin", "^filing_rate_longrun_pre2019_cuts::"),
    extract_model_coefs(m_bin_tenant, "tenant_bin", "^filing_rate_longrun_pre2019_cuts::"),
    extract_model_coefs(m_cont, "baseline_cont", "^filing_rate_eb_pre_covid$"),
    extract_model_coefs(m_cont_tenant, "tenant_cont", "^filing_rate_eb_pre_covid$")
  ),
  use.names = TRUE,
  fill = TRUE
)
if (!nrow(coef_compare)) {
  coef_compare <- data.table(
    model = character(),
    term = character(),
    estimate = numeric(),
    std_error = numeric(),
    p_value = numeric()
  )
}
fwrite(
  coef_compare,
  p_out(cfg, "tables", "hedonic_eviction_intensity_eb_tenant_composition_coef_compare.csv")
)

# Coefficient plot for binned model
# (Ref category is omitted by construction.)
ct <- coeftable(m_bin, keep = "filing_rate_eb_cuts")
coef_dt <- as.data.table(ct, keep.rownames = "term")
if (nrow(coef_dt) > 0) {
  coef_dt[, term := gsub("^filing_rate_eb_cuts::", "", term)]
  p <- ggplot(coef_dt, aes(x = term, y = Estimate)) +
    geom_point() +
    geom_errorbar(aes(ymin = Estimate - 1.96 * `Std. Error`,
                      ymax = Estimate + 1.96 * `Std. Error`),
                  width = 0.15) +
    labs(
      x = "EB filing intensity bin (pre-2019)",
      y = "Effect on log rent (relative to (7.5-20%])",
      title = "Hedonic penalty by eviction intensity bin",
      subtitle = "Tract and year FE; controls for units bin, decade built, and source"
    ) +
    theme_minimal()

  ggsave(p_out(cfg, "figs", "coefplot_hedonic_eviction_intensity_eb.png"),
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
# Diagnostics: residual variation and spline stigma curve
# ------------------------------------------------------------------



## ------------------------------------------------------------
## DIAGNOSTIC: residual variation in eviction intensity
## Residualize filing_rate_eb_pre_covid on the same controls/FE
## used in the hedonic regressions, then plot residual distribution.
## ------------------------------------------------------------

library(fixest)
library(ggplot2)

# Use the same regression sample you use for the hedonic (important!)
# e.g., dt_pre is your 2014-2019 rental-years analytic sample
# Replace dt_pre with your object name.
dt_resid <- copy(reg_dt)

# Make sure outcome exists
stopifnot("filing_rate_eb_pre_covid" %in% names(dt_resid))

# Residualize: FE/controls should match the rent regression RHS/FE
# If your main hedonic is:
#   log_med_rent ~ ... | year + census_tract + num_units_bin + year_blt_decade + source
# then residualize filing_rate_eb_pre_covid the same way (no x's needed beyond FE).
m_resid <- feols(
  filing_rate_eb_pre_covid ~1+log(market_value) + log(total_area) |
    year + GEOID   + year_blt_decade + source + num_units_bin + building_type + quality_grade_standard + num_stories_bin  ,
  data    = dt_resid,
  weights = ~ total_units  # keep consistent with your hedonic weighting
)

dt_resid[, filing_rate_eb_pre_covid_resid :=filing_rate_eb_pre_covid -  predict(m_resid, newdata =dt_resid )]

# Summaries: how much is left?
resid_sd <- sd(dt_resid$filing_rate_eb_pre_covid_resid, na.rm = TRUE)
resid_p95 <- quantile(abs(dt_resid$filing_rate_eb_pre_covid_resid), 0.95, na.rm = TRUE)

logf(
  "Residualized EB filing rate: SD=", round(resid_sd, 4),
  "; P95 abs(resid)=", round(resid_p95, 4),
  "; R2=", round(r2(m_resid, "pr2"), 4),
  log_file = log_file
)

dt_resid[,spatstat.univar::weighted.quantile(filing_rate_eb_pre_covid_resid,na.rm = T, w= total_units, probs = seq(.1,.9,.1))]

# 1) Histogram (weighted by units)
p_hist <- ggplot(
  dt_resid[is.finite(filing_rate_eb_pre_covid_resid)],
  aes(x = filing_rate_eb_pre_covid_resid, weight = total_units)
) +
  geom_histogram(bins = 80) +
  scale_x_continuous(breaks = scales::pretty_breaks(10)) +
  labs(
    title = "Residual variation in EB eviction intensity (pre-COVID)",
    subtitle = "Residualized on year + tract + size bin + decade built + source + log(market_value);\n weighted by imputed units",
    x = "Residualized EB filing rate",
    y = "Weighted count (units)"
  ) +
  theme_philly_evict()

ggsave(
  filename = file.path(fig_dir, "resid_eviction_intensity_eb_pre_covid_hist.png"),
  plot = p_hist, width = 9, height = 5, dpi = 300
)

# plot unresidualized
p_hist_unresid <- ggplot(
  dt_resid[is.finite(filing_rate_eb_pre_covid)],
  aes(x = filing_rate_eb_pre_covid, weight = total_units)
) +
  geom_histogram(bins = 80) +
  scale_x_continuous(breaks = scales::pretty_breaks(10)) +
  labs(
    title = "Unresidualized EB eviction intensity (pre-COVID)",
    subtitle = "Weighted by imputed units",
    x = "EB filing rate (pre-COVID)",
    y = "Weighted count (units)"
  ) +
  theme_philly_evict()

# 2) Density by source (optional but often revealing)
p_dens <- ggplot(
  dt_resid[is.finite(filing_rate_eb_pre_covid_resid)],
  aes(x = filing_rate_eb_pre_covid_resid, color = source,weight = total_units)
) +
  geom_density() +
  #facet_wrap(~ source, scales = "free_y") +
  labs(
    title = "Residualized EB eviction intensity by source",
    subtitle = "Same residualization; weighted by units",
    x = "Residualized EB filing rate",
    y = "Weighted density"
  )

ggsave(
  filename = file.path(fig_dir, "resid_eviction_intensity_eb_pre_covid_density_by_source.png"),
  plot = p_dens, width = 10, height = 5, dpi = 300
)

# 3) (Optional) Residual SD by size bin (quick “where does variation live?”)
sd_by_size <- dt_resid[
  is.finite(filing_rate_eb_pre_covid_resid),
  .(sd_resid = sd(filing_rate_eb_pre_covid_resid), n = .N),
  by = num_units_bin
][order(num_units_bin)]

fwrite(sd_by_size, file.path(tab_dir, "resid_eviction_intensity_sd_by_units_bin.csv"))


# now residualize on prices; plot resid prices vs resid filing rates
m_price_resid <- feols(
  log_med_rent ~1+log(market_value) + log(total_area) |
    year + GEOID   + year_blt_decade + source + num_units_bin + building_type + quality_grade_standard + num_stories_bin  ,
  data    = dt_resid,
  weights = ~ total_units  # keep consistent with your hedonic weighting
)

dt_resid[, log_med_rent_resid :=log_med_rent -  predict(m_price_resid, newdata =dt_resid )]
# scatterplot of resid price vs resid filing rate
p_scatter <- ggplot(
  dt_resid[is.finite(filing_rate_eb_pre_covid_resid) & is.finite(log_med_rent_resid) & total_units >= 10 ],
  aes(x = filing_rate_eb_pre_covid_resid, y= log_med_rent_resid, weight = total_units)
) +
  #geom_point(data = bin_scatter, aes(x = mean_filing_resid, y = mean_rent_resid)) +
  geom_point() +
  geom_smooth(method = "loess", span = 0.5, se = F, aes(color = "Loess: Span = 0.5")) +
  geom_smooth(method = "loess", span = 0.75, se = F, aes(color = "Loess: Span = 0.75")) +
  geom_smooth(method = "loess", span = 1, se = F, aes(color = "Loess: Span = 1")) +
  geom_smooth(method = "gam", span = 0.5, se = F, aes(color = "Gam")) +
  geom_smooth(method = "lm", aes(color = "lm")) +
  scale_y_continuous(breaks = scales::pretty_breaks(20)) +
  labs(
    title = "Residualized log rent vs residualized EB eviction intensity",
    subtitle = "Both residualized on same controls/FE; weighted by imputed units",
    x = "Residualized EB filing rate",
    y = "Residualized log median rent"
  ) +
  theme_philly_evict()

ggsave(file.path(fig_dir, "scatter_resid_rent_vs_eviction.png"),
       p_scatter, width = 10, height = 6, dpi = 300, bg = "white")

# do a bin scatter
dt_resid[, filing_rate_eb_pre_covid_resid_bin := cut(filing_rate_eb_pre_covid_resid,
                                                        breaks = quantile(filing_rate_eb_pre_covid_resid, probs = seq(0,1,0.1), na.rm = T),
                                                        include.lowest = T)]

# bin into 0.05 buckets
dt_resid[, filing_rate_eb_pre_covid_resid_bin05 := cut(filing_rate_eb_pre_covid_resid,
                                                        breaks = seq(-0.5,0.5, by=0.05),
                                                        include.lowest = T)]
bin_scatter <- dt_resid[
  is.finite(filing_rate_eb_pre_covid_resid) & is.finite(log_med_rent_resid)  ,
  .(mean_filing_resid = weighted.mean(filing_rate_eb_pre_covid_resid, w= total_units, na.rm = T),
    mean_rent_resid = weighted.mean(log_med_rent_resid, w= total_units, na.rm = T),
    total_units = sum(total_units)
    ),
  by = filing_rate_eb_pre_covid_resid_bin05
]
p_bin_scatter <- ggplot(
  bin_scatter[total_units > 500],
  aes(x = mean_filing_resid, y= mean_rent_resid, size= total_units, weight = total_units)
) +
  geom_point(alpha = 0.7) +
  geom_smooth(method = "loess", span = 1, se = F) +
  geom_smooth(method = "gam", span = 1, aes(color = "gam"), se = F) +
  labs(
    title = "Binned: Residualized log rent vs residualized EB eviction intensity",
    subtitle = "Both residualized on same controls/FE; weighted by imputed units",
    x = "Residualized EB filing rate",
    y = "Residualized log median rent"
  ) +
  theme_philly_evict()
ggsave(file.path(fig_dir, "binscatter_resid_rent_vs_eviction.png"),
       p_bin_scatter, width = 10, height = 6, dpi = 300, bg = "white")


library(data.table)
library(fixest)
library(splines)
library(ggplot2)
library(tidyr)
library(dplyr)

setDT(reg_dt)

# ----------------------------
# 0) User knobs
# ----------------------------
n_splines <- 6          # df for ns()
q_lo <- 0.01            # plot lower trim
q_hi <- 0.90            # plot upper trim (use 0.95 if you want more tail)
n_bins <- 10            # number of quantile bins for overlay

# ----------------------------
# 1) Hurdle variables
# ----------------------------
reg_dt[, never_filed_pre_covid := as.integer(replace_na(total_filings_pre2019, 0) == 0L)]

# Filing intensity among ever-filers (EB), NA for never-filers
reg_dt[, filing_rate_eb_pre_covid_filed :=
         fifelse(never_filed_pre_covid == 1L, NA_real_, filing_rate_eb_zip_pre_covid)]

# ----------------------------
# 2) Build spline basis (ns) and set to 0 for never-filers
# ----------------------------
# Drop old spline columns if they exist
reg_dt <- reg_dt |> select(-contains("spline_"))
setDT(reg_dt)

# ns() will produce NA rows where x is NA (never-filers) — that's fine initially
B <- ns(reg_dt$filing_rate_eb_pre_covid_filed, df = n_splines)
sp_names <- paste0("spline_", 1:ncol(B))

reg_dt[, (sp_names) := as.data.table(B)]

# Replace spline rows with 0 for never-filers so they remain in regression
reg_dt[never_filed_pre_covid == 1L, (sp_names) := 0]

# ----------------------------
# 3) Estimate spline model
# ----------------------------
rhs_spline <- paste(c("never_filed_pre_covid", sp_names,
                      "log(market_value)", "log(total_area)", "log(total_units)"),
                    collapse = " + ")

fe_part <- paste(c("year", "census_tract", "year_blt_decade", "num_units_bin",
                   "building_type", "quality_grade_standard", "num_stories_bin"),
                 collapse = " + ")

fml <- as.formula(paste0("log_med_rent ~ ", rhs_spline, " | ", fe_part))

m_spline <- feols(
  fml,
  data = reg_dt,
  weights = ~ total_units,
  cluster = ~ PID
)

library(data.table)
library(ggplot2)

# --- pull exact estimation sample ---
sel <- !is.na(weights(m_spline))
dt_used <- reg_dt[sel]

# --- coefficient objects ---
b_all <- coef(m_spline)

# fixest may name the never coefficient slightly differently if it treated it as factor
never_name <- grep("^never_filed_pre_covid", names(b_all), value = TRUE)
stopifnot(length(never_name) == 1)
beta_never <- as.numeric(b_all[never_name])

sp_names <- paste0("spline_", 1:n_splines)  # adjust if n_splines differs
b_sp <- b_all[sp_names]

# --- compute fitted filing component for each obs ---
# g_i = spline_i' * b_sp
dt_used[, g_i := as.vector(as.matrix(.SD) %*% b_sp), .SDcols = sp_names]

# filing component includes the discontinuity
dt_used[, fhat_i := beta_never * never_filed_pre_covid + g_i]

# partial residual for filing component: fhat + regression residual
dt_used[, eps := resid(m_spline)]
dt_used[, f_partial := fhat_i + eps]

# relative to never baseline (never-filers should average near 0)
dt_used[, f_partial_rel := f_partial - beta_never]

# --- bin ever-filers and take weighted means ---
dt_pos <- dt_used[never_filed_pre_covid == 0L & !is.na(filing_rate_eb_pre_covid_filed)]

n_bins <- 10
brks <- unique(as.numeric(quantile(
  dt_pos$filing_rate_eb_pre_covid_filed,
  probs = seq(0, 1, length.out = n_bins + 1),
  na.rm = TRUE
)))

if (length(brks) < 5) {
  n_bins <- 5
  brks <- unique(as.numeric(quantile(
    dt_pos$filing_rate_eb_pre_covid_filed,
    probs = seq(0, 1, length.out = n_bins + 1),
    na.rm = TRUE
  )))
}

dt_pos[, bin := cut(filing_rate_eb_pre_covid_filed, breaks = brks, include.lowest = TRUE)]

bin_means <- dt_pos[, .(
  x_bin = weighted.mean(filing_rate_eb_pre_covid_filed, w = total_units, na.rm = TRUE),
  y_bin = weighted.mean(f_partial_rel, w = total_units, na.rm = TRUE),
  n_bin = .N,
  w_bin = sum(total_units, na.rm = TRUE)
), by = bin][order(x_bin)]

b_all <- coef(m_spline)
V_all <- vcov(m_spline, cluster = "PID")

# Find the *actual* coefficient name for never_filed_pre_covid (handles TRUE suffix)
never_name <- grep("^never_filed_pre_covid", names(b_all), value = TRUE)
stopifnot(length(never_name) == 1)

b_sp <- b_all[sp_names]
V_sp <- V_all[sp_names, sp_names, drop = FALSE]

beta_never <- as.numeric(b_all[never_name])
var_never  <- as.numeric(V_all[never_name, never_name])

# Cov( beta_never, spline coefs )
cov_never_sp <- V_all[never_name, sp_names, drop = FALSE]  # 1 x K

# make the grid as data.table w/ filing_rate_eb_pre_covid_filed
# Grid over positive support (ever-filers)
x <- reg_dt$filing_rate_eb_pre_covid_filed
lo <- quantile(x, 0.01, na.rm = TRUE)
hi <- quantile(x, 0.9, na.rm = TRUE)

grid <- data.table(filing_rate_eb_pre_covid_filed = seq(lo, hi, length.out = 200))
# Build spline basis on grid using SAME ns() spec
Bgrid <- ns(grid$filing_rate_eb_pre_covid_filed, df = n_splines)
colnames(Bgrid) <- sp_names

# g(x) = B(x) * b_sp
g_hat <- as.vector(Bgrid %*% b_sp)

# Delta relative to never: g(x) - beta_never
grid[, y := g_hat - beta_never]

# Delta-method SE for y = B b_sp - beta_never
# Var(y) = Var(B b_sp) + Var(beta_never) - 2 Cov(beta_never, B b_sp)
var_g <- diag(Bgrid %*% V_sp %*% t(Bgrid))
cov_ng <- as.vector((cov_never_sp %*% t(Bgrid)))  # 1 x Ngrid -> vector length Ngrid

se_y <- sqrt(pmax(0, var_g + var_never - 2 * cov_ng))

grid[, lo := y - 1.96 * se_y]
grid[, hi := y + 1.96 * se_y]



# keep bins in plot range
bin_means <- bin_means[
  x_bin >= min(grid$filing_rate_eb_pre_covid_filed) &
    x_bin <= max(grid$filing_rate_eb_pre_covid_filed)
]

# Never-filer anchor point (weighted mean partial residual among never-filers)
never_pt <- dt_used[never_filed_pre_covid == 1L, .(
  x = 0,
  y = weighted.mean(f_partial_rel, w = total_units, na.rm = TRUE),
  n = .N,
  w = sum(total_units, na.rm = TRUE)
)]

# append bin means and never_pt
all_rows = rbind(
  data.table(x = never_pt$x, y = never_pt$y, type = "Never-filers", w= never_pt$w),
  data.table(x = bin_means$x_bin, y = bin_means$y_bin, type = "Binned ever-filers", w= bin_means$w_bin)
)


# --- plot overlay ---
p_spline <- ggplot() +
  geom_ribbon(
    data = grid,
    aes(x = filing_rate_eb_pre_covid_filed, ymin = lo, ymax = hi),
    alpha = 0.2
  ) +
  geom_line(
    data = grid,
    aes(x = filing_rate_eb_pre_covid_filed, y = y),
    linewidth = 1
  ) +
  geom_point(
    data = all_rows,
    aes(x = x, y = y, size = w), alpha = 0.85
  ) +
  # geom_point(
  #   data = never_pt,
  #   aes(x = x, y = y),
  #   size = 3
  # ) +
  geom_smooth(data = all_rows, aes(x = x, y = y, weight = w), se = F)+
  scale_x_continuous(breaks = scales::pretty_breaks(20), limits = c(0,0.25)) +
  scale_y_continuous(breaks = scales::pretty_breaks(10)) +

  labs(
    x = "EB filing rate (pre-2019), ever-filers",
    y = "Partial residual (filing component) vs never-filers",
    title = "Spline-implied stigma curve with partial-residual binned means overlay",
    weights = "Point size = total unit-years in bin",
    subtitle = paste0(
      "Points: binned means of \n(fitted filing component + residual) − β_never; ",
      n_bins, " bins; weights = total_units"
    )
  ) +
  theme_philly_evict()

ggsave(file.path(fig_dir, "spline_stigma_curve.png"),
       p_spline, width = 10, height = 6, dpi = 300, bg = "white")

logf("=== Finished price-regs-eb.R ===", log_file = log_file)




