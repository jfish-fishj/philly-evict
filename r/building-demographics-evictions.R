## ============================================================
## building-demographics-evictions.R
## ============================================================
## Purpose: Characterize tenant demographics of high-evicting buildings
##          relative to other buildings, conditioning on tract/observables.
##
## Unit of analysis: Building (PID), collapsed across all available years.
##
## Inputs:  products/bldg_panel_blp (from make-analytic-sample.R)
## Outputs: output/tables/building_demog_*.tex/.csv
##          output/figs/coefplot_demog_*.png
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(fixest)
  library(ggplot2)
  library(stringr)
  library(splines)
})

# ---- Load config + helpers ----
source("r/config.R")
source("r/helper-functions.R")
cfg <- read_config()
log_file <- p_out(cfg, "logs", "building-demographics-evictions.log")
tab_dir  <- p_out(cfg, "tables")
fig_dir  <- p_out(cfg, "figs")
dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)
logf("=== Starting building-demographics-evictions.R ===", log_file = log_file)

# ---- Load data ----
bldg_panel <- fread(p_product(cfg, "bldg_panel_blp"))
logf("Loaded bldg_panel_blp: ", nrow(bldg_panel), " rows, ", ncol(bldg_panel), " cols", log_file = log_file)

# ---- Defensive checks ----
demog_outcomes <- c(
  "infousa_pct_black",
  "infousa_pct_female",
  "infousa_pct_black_female"
)
req_cols <- c("PID", "year", "total_units", "GEOID",
              "filing_rate_eb_pre_covid", "filing_rate_eb_city_pre_covid",
              "infousa_share_persons_demog_ok",
              demog_outcomes)
missing <- setdiff(req_cols, names(bldg_panel))
if (length(missing) > 0) {
  stop("Missing required columns: ", paste(missing, collapse = ", "))
}

# ---- Collapse to building level ----
# Weight demographics by total_units in each year; average across all years.
# Filter to buildings with adequate demographic coverage.

demog_coverage_min <- 0.5

# Ensure numeric
for (cc in c(demog_outcomes, "infousa_share_persons_demog_ok")) {
  bldg_panel[, (cc) := suppressWarnings(as.numeric(get(cc)))]
}

# Keep rows with demographic data above coverage threshold
bldg_panel[, has_demog := !is.na(infousa_share_persons_demog_ok) &
             infousa_share_persons_demog_ok >= demog_coverage_min]

logf("Rows with demog coverage >= ", demog_coverage_min, ": ",
     bldg_panel[has_demog == TRUE, .N], " / ", nrow(bldg_panel),
     log_file = log_file)

# Building-level collapse: weighted mean of demographics across years
# Use total_units as weight within each building-year
bldg_level <- bldg_panel[
  has_demog == TRUE & !is.na(total_units) & total_units > 0,
  {
    w <- total_units
    n_years_demog <- .N

    out <- list(
      # Demographics: weighted means across years
      infousa_pct_black        = weighted.mean(infousa_pct_black, w = w, na.rm = TRUE),
      infousa_pct_female       = weighted.mean(infousa_pct_female, w = w, na.rm = TRUE),
      infousa_pct_black_female = weighted.mean(infousa_pct_black_female, w = w, na.rm = TRUE),
      demog_coverage_mean      = weighted.mean(infousa_share_persons_demog_ok, w = w, na.rm = TRUE),

      # Take mean total_units across years as building size
      total_units = mean(total_units, na.rm = TRUE),
      n_years_demog = n_years_demog
    )
    out
  },
  by = PID
]

# Merge back time-invariant building characteristics (take first non-NA)
bldg_chars <- bldg_panel[, {
  list(
    GEOID                        = GEOID[which.max(!is.na(GEOID))],
    census_tract                 = str_sub(GEOID[which.max(!is.na(GEOID))], 1, 11),
    num_units_bin                = num_units_bin[which.max(!is.na(num_units_bin))],
    building_type                = building_type[which.max(!is.na(building_type))],
    year_blt_decade              = year_blt_decade[which.max(!is.na(year_blt_decade))],
    quality_grade                = quality_grade[which.max(!is.na(quality_grade))],
    number_stories               = mean(number_stories, na.rm = TRUE),
    num_stories_bin              = num_stories_bin[which.max(!is.na(num_stories_bin))],
    total_area                   = mean(total_area, na.rm = TRUE),
    market_value                 = mean(market_value, na.rm = TRUE),
    filing_rate_eb_pre_covid     = filing_rate_eb_pre_covid[1],
    filing_rate_eb_city_pre_covid = filing_rate_eb_city_pre_covid[1],
    filing_rate_eb_zip_pre_covid = if ("filing_rate_eb_zip_pre_covid" %in% names(.SD))
      filing_rate_eb_zip_pre_covid[1] else NA_real_,
    total_filings_pre2019        = if ("total_filings_pre2019" %in% names(.SD))
      total_filings_pre2019[1] else NA_integer_,
    pm.zip                       = pm.zip[which.max(!is.na(pm.zip))]
  )
}, by = PID]

bldg <- merge(bldg_level, bldg_chars, by = "PID", all.x = TRUE)

logf("Building-level dataset: ", nrow(bldg), " buildings", log_file = log_file)
logf("  with non-missing filing_rate_eb_pre_covid: ",
     bldg[!is.na(filing_rate_eb_pre_covid), .N], log_file = log_file)

# ---- Define eviction intensity bins ----
# Use total_filings_pre2019 to identify true never-filers (EB rate is always > 0)
bldg[, never_filed_pre2019 := is.na(total_filings_pre2019) | total_filings_pre2019 == 0L]
bldg[, filing_rate_bins := fcase(
  never_filed_pre2019 == TRUE, "No filings",
  filing_rate_eb_pre_covid <= 0.05, "(0-5%]",
  filing_rate_eb_pre_covid <= 0.10, "(5-10%]",
  filing_rate_eb_pre_covid <= 0.20, "(10-20%]",
  filing_rate_eb_pre_covid > 0.20, "20%+",
  default = NA_character_
)]
bldg[, filing_rate_bins := factor(filing_rate_bins,
  levels = c("No filings", "(0-5%]", "(5-10%]", "(10-20%]", "20%+")
)]

# Quality grade cleanup (same as price-regs)
bldg[, quality_grade_standard := fifelse(
  str_detect(quality_grade, "[ABCDabcd]"), quality_grade, NA_character_
)]
bldg[, quality_grade_standard := str_remove_all(quality_grade_standard, "[+-]")]
bldg[is.na(quality_grade_standard), quality_grade_standard := "Unknown"]

# Sample restriction: must have eviction measure and demog
reg_dt <- bldg[
  !is.na(filing_rate_eb_pre_covid) &
    filing_rate_eb_pre_covid <= 0.75 &
    !is.na(infousa_pct_black) &
    !is.na(GEOID) & nzchar(GEOID) &
    !is.na(total_area) & total_area > 0 &
    !is.na(market_value) & market_value > 0
]

logf("Regression sample: ", nrow(reg_dt), " buildings", log_file = log_file)
logf("  Filing rate bins:", log_file = log_file)
logf(paste(capture.output(reg_dt[, .(.N, mean_units = round(mean(total_units), 1)),
                                  by = filing_rate_bins][order(filing_rate_bins)]),
           collapse = "\n"), log_file = log_file)

# ==================================================================
# DESCRIPTIVE TABLE: mean demographics by eviction bin
# ==================================================================

desc_table <- reg_dt[, {
  w <- total_units
  list(
    n_buildings         = .N,
    total_unit_years    = round(sum(total_units * n_years_demog)),
    mean_units          = round(weighted.mean(total_units, w = w, na.rm = TRUE), 1),
    pct_black           = round(weighted.mean(infousa_pct_black, w = w, na.rm = TRUE), 3),
    pct_female          = round(weighted.mean(infousa_pct_female, w = w, na.rm = TRUE), 3),
    pct_black_female    = round(weighted.mean(infousa_pct_black_female, w = w, na.rm = TRUE), 3),
    mean_demog_coverage = round(weighted.mean(demog_coverage_mean, w = w, na.rm = TRUE), 3)
  )
}, by = filing_rate_bins][order(filing_rate_bins)]

logf("Descriptive table:", log_file = log_file)
logf(paste(capture.output(print(desc_table)), collapse = "\n"), log_file = log_file)

fwrite(desc_table, file.path(tab_dir, "building_demog_means_by_eviction_bin.csv"))

# ==================================================================
# REGRESSION MODELS
# ==================================================================
# For each outcome: 4 specs
#   (A) Unconditional (no FE)
#   (B) Tract FE + building controls
#   (C) Block group FE + building controls
#   (D) Hurdle spline: never_filed indicator + ns() for ever-filers, BG FE

ref_bin <- "(5-10%]"
n_splines <- 6

# ---- Build spline basis (shared across outcomes) ----
reg_dt[, never_filed := as.integer(
  is.na(total_filings_pre2019) | total_filings_pre2019 == 0L
)]
reg_dt[, filing_rate_filed := fifelse(
  never_filed == 1L, NA_real_, filing_rate_eb_pre_covid
)]

B <- ns(reg_dt$filing_rate_filed, df = n_splines)
sp_names <- paste0("spline_", 1:ncol(B))
reg_dt[, (sp_names) := as.data.table(B)]
reg_dt[never_filed == 1L, (sp_names) := 0]

run_demog_models <- function(outcome, dt) {
  # (A) Unconditional binned
  m_a <- feols(
    as.formula(paste0(outcome, " ~ i(filing_rate_bins, ref = '", ref_bin, "')")),
    data = dt,
    weights = ~ total_units,
    cluster = ~ census_tract
  )

  # (B) Tract FE + controls
  m_b <- feols(
    as.formula(paste0(
      outcome, " ~ i(filing_rate_bins, ref = '", ref_bin, "') + ",
      "log(total_area) + log(market_value) | ",
      "census_tract + num_units_bin + building_type + year_blt_decade + quality_grade_standard + num_stories_bin"
    )),
    data = dt,
    weights = ~ total_units,
    cluster = ~ census_tract
  )

  # (C) Block group FE + controls
  m_c <- feols(
    as.formula(paste0(
      outcome, " ~ i(filing_rate_bins, ref = '", ref_bin, "') + ",
      "log(total_area) + log(market_value) | ",
      "GEOID + num_units_bin + building_type + year_blt_decade + quality_grade_standard + num_stories_bin"
    )),
    data = dt,
    weights = ~ total_units,
    cluster = ~ GEOID
  )

  # (D) Hurdle spline: never_filed + ns(filing_rate) for ever-filers, BG FE
  rhs_spline <- paste(c("never_filed", sp_names,
                         "log(total_area)", "log(market_value)"),
                       collapse = " + ")
  fe_part <- "GEOID + num_units_bin + building_type + year_blt_decade + quality_grade_standard + num_stories_bin"
  fml <- as.formula(paste0(outcome, " ~ ", rhs_spline, " | ", fe_part))

  m_d <- feols(
    fml,
    data = dt,
    weights = ~ total_units,
    cluster = ~ GEOID
  )

  list(unconditional = m_a, tract_fe = m_b, bg_fe = m_c, spline = m_d)
}

# Run for each outcome
models_black        <- run_demog_models("infousa_pct_black", reg_dt)
models_female       <- run_demog_models("infousa_pct_female", reg_dt)
models_black_female <- run_demog_models("infousa_pct_black_female", reg_dt)

# ==================================================================
# TABLES
# ==================================================================
setFixest_dict(c(
  "filing_rate_bins" = "Eviction intensity (EB, pre-COVID)",
  "filing_rate_eb_pre_covid" = "EB filing rate (pre-COVID)",
  "infousa_pct_black" = "Share Black",
  "infousa_pct_female" = "Share Female",
  "infousa_pct_black_female" = "Share Black Female"
))
fixest::setFixest_etable(digits = 4, fitstat = c("n", "r2"))

# Main table: all 3 outcomes, specs B (tract) and C (BG)
tab_main <- etable(
  models_black$tract_fe, models_black$bg_fe,
  models_female$tract_fe, models_female$bg_fe,
  models_black_female$tract_fe, models_black_female$bg_fe,
  title = "Tenant demographics by eviction intensity (building-level)",
  headers = rep(c("Tract FE", "BG FE"), 3),
  keep = "%filing_rate_",
  order = "%filing_rate_",
  tex = TRUE
)
writeLines(tab_main, file.path(tab_dir, "building_demog_by_eviction_intensity.tex"))

# Full table: all specs for pct_black (most interesting outcome)
tab_black_full <- etable(
  models_black$unconditional, models_black$tract_fe,
  models_black$bg_fe, models_black$spline,
  title = "Share Black tenants by eviction intensity",
  headers = c("Unconditional", "Tract FE", "BG FE", "Spline"),
  keep = "%filing_rate_|%never_filed|%spline_",
  order = "%filing_rate_|%never_filed",
  tex = TRUE
)
writeLines(tab_black_full, file.path(tab_dir, "building_demog_black_full_specs.tex"))

# Console output
logf("--- Share Black models ---", log_file = log_file)
logf(paste(capture.output(etable(
  models_black$unconditional, models_black$tract_fe,
  models_black$bg_fe, models_black$spline,
  keep = "%filing_rate_|%never_filed|%spline_", tex = FALSE
)), collapse = "\n"), log_file = log_file)

logf("--- Share Female models ---", log_file = log_file)
logf(paste(capture.output(etable(
  models_female$unconditional, models_female$tract_fe,
  models_female$bg_fe, models_female$spline,
  keep = "%filing_rate_|%never_filed|%spline_", tex = FALSE
)), collapse = "\n"), log_file = log_file)

logf("--- Share Black Female models ---", log_file = log_file)
logf(paste(capture.output(etable(
  models_black_female$unconditional, models_black_female$tract_fe,
  models_black_female$bg_fe, models_black_female$spline,
  keep = "%filing_rate_|%never_filed|%spline_", tex = FALSE
)), collapse = "\n"), log_file = log_file)

# ==================================================================
# COEFFICIENT PLOTS
# ==================================================================

extract_bin_coefs <- function(model, outcome_label, spec_label) {
  ct <- as.data.table(summary(model)$coeftable, keep.rownames = "term")
  setnames(ct, c("Estimate", "Std. Error"), c("estimate", "std_error"))
  ct <- ct[grepl("filing_rate_bins::", term)]
  if (!nrow(ct)) return(data.table())
  ct[, term := gsub("^filing_rate_bins::", "", term)]
  ct[, outcome := outcome_label]
  ct[, spec := spec_label]
  ct[, .(outcome, spec, term, estimate, std_error)]
}

coef_all <- rbindlist(list(
  extract_bin_coefs(models_black$unconditional, "Share Black", "Unconditional"),
  extract_bin_coefs(models_black$tract_fe,      "Share Black", "Tract FE"),
  extract_bin_coefs(models_black$bg_fe,         "Share Black", "BG FE"),
  extract_bin_coefs(models_female$bg_fe,        "Share Female", "BG FE"),
  extract_bin_coefs(models_black_female$bg_fe,  "Share Black Female", "BG FE")
))

if (nrow(coef_all) > 0) {
  # Plot 1: Share Black across specs
  p_black <- ggplot(
    coef_all[outcome == "Share Black"],
    aes(x = term, y = estimate, color = spec)
  ) +
    geom_point(position = position_dodge(width = 0.3)) +
    geom_errorbar(
      aes(ymin = estimate - 1.96 * std_error,
          ymax = estimate + 1.96 * std_error),
      width = 0.15,
      position = position_dodge(width = 0.3)
    ) +
    geom_hline(yintercept = 0, linetype = "dashed", alpha = 0.5) +
    labs(
      x = "EB eviction intensity bin (pre-COVID)",
      y = "Effect on share Black (relative to 5-10%)",
      title = "Share Black tenants by eviction intensity",
      color = "Specification"
    ) +
    theme_minimal()

  ggsave(file.path(fig_dir, "coefplot_demog_black_by_eviction.png"),
         p_black, width = 9, height = 5, bg = "white")

  # Plot 2: All outcomes, BG FE spec
  p_all <- ggplot(
    coef_all[spec == "BG FE"],
    aes(x = term, y = estimate, color = outcome)
  ) +
    geom_point(position = position_dodge(width = 0.3)) +
    geom_errorbar(
      aes(ymin = estimate - 1.96 * std_error,
          ymax = estimate + 1.96 * std_error),
      width = 0.15,
      position = position_dodge(width = 0.3)
    ) +
    geom_hline(yintercept = 0, linetype = "dashed", alpha = 0.5) +
    labs(
      x = "EB eviction intensity bin (pre-COVID)",
      y = "Effect on demographic share (relative to 5-10%)",
      title = "Tenant demographics by eviction intensity (Block group FE)",
      color = "Outcome"
    ) +
    theme_minimal()

  ggsave(file.path(fig_dir, "coefplot_demog_all_by_eviction.png"),
         p_all, width = 9, height = 5, bg = "white")
}

# ==================================================================
# SUMMARY STATS
# ==================================================================
logf("--- Sample summary ---", log_file = log_file)
logf("  Buildings: ", nrow(reg_dt), log_file = log_file)
logf("  Total units: ", round(sum(reg_dt$total_units)), log_file = log_file)
logf("  Mean pct_black: ", round(weighted.mean(reg_dt$infousa_pct_black, reg_dt$total_units, na.rm = TRUE), 3),
     log_file = log_file)
logf("  Mean pct_female: ", round(weighted.mean(reg_dt$infousa_pct_female, reg_dt$total_units, na.rm = TRUE), 3),
     log_file = log_file)
logf("  Mean demog coverage: ", round(weighted.mean(reg_dt$demog_coverage_mean, reg_dt$total_units, na.rm = TRUE), 3),
     log_file = log_file)
logf("  Unique tracts: ", uniqueN(reg_dt$census_tract), log_file = log_file)
logf("  Unique block groups: ", uniqueN(reg_dt$GEOID), log_file = log_file)

logf("=== Finished building-demographics-evictions.R ===", log_file = log_file)
