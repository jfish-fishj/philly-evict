## ============================================================
## analyze-filing-decomposition.R
## ============================================================
## Purpose: Two-part analysis of building-level filing rate variation
##          and case-level back-rent patterns, 2011-2019.
##
##   Part A: Cross-sectional filing rate decomposition
##     - Sequential R² variance decomposition
##     - OLS coefficient tables (building, neighborhood, tenant, landlord)
##     - PPML count robustness
##
##   Part B: Case-level back-rent analysis
##     - Months-of-arrears regressions by plaintiff/owner type
##     - log(total_rent) elasticity
##     - Settlement discount
##     - Racial disparity (if race data available)
##
## Inputs (from config):
##   - bldg_panel_blp              (PID x year building panel)
##   - xwalk_pid_entity            (PID x year -> entity, portfolio)
##   - xwalk_pid_conglomerate      (PID x year -> conglomerate portfolio)
##   - name_lookup                 (raw name -> entity_id for plaintiff matching)
##   - evictions_clean             (case-level with rent/months fields)
##   - eviction_case_commercial    (case -> is_commercial flag from impute-race-preflight.R)
##   - evict_address_xwalk_case    (case -> PID linkage)
##   - infousa_building_race_panel (PID x year racial composition)
##
## Outputs: output/filing_decomposition/
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(fixest)
})

source("r/config.R")
source("r/helper-functions.R")

cfg      <- read_config()
log_file <- p_out(cfg, "logs", "analyze-filing-decomposition.log")

# Wipe old log to avoid appending to stale runs
if (file.exists(log_file)) file.remove(log_file)

logf("=== Starting analyze-filing-decomposition.R ===", log_file = log_file)

SAMPLE_YEARS <- 2011:2019

out_dir <- p_out(cfg, "filing_decomposition")
fs::dir_create(out_dir, recurse = TRUE)

p_fd <- function(...) file.path(out_dir, ...)

# ============================================================
# SECTION 0: Load data
# ============================================================
logf("--- SECTION 0: Loading data ---", log_file = log_file)

bldg <- fread(p_product(cfg, "bldg_panel_blp"))
bldg <- bldg[year %in% SAMPLE_YEARS]
bldg[, PID := str_pad(as.character(PID), 9, "left", "0")]
assert_has_cols(bldg, c("PID", "year", "filing_rate_eb_pre_covid", "num_filings",
  "total_units", "num_units_bin", "building_type",
  "quality_grade", "year_blt_decade", "GEOID", "pm.zip",
  "ever_rental_any_year"), "bldg")

own_py <- fread(p_product(cfg, "xwalk_pid_entity"))
own_py[, PID := str_pad(as.character(PID), 9, "left", "0")]
own_py <- own_py[year %in% SAMPLE_YEARS]
assert_has_cols(own_py, c("PID", "year", "entity_id", "is_corp_owner",
  "portfolio_size_entity_yr", "portfolio_bin_entity_yr",
  "total_units_entity_yr", "owner_category"), "xwalk_pid_entity")

own_cong <- fread(p_product(cfg, "xwalk_pid_conglomerate"))
own_cong[, PID := str_pad(as.character(PID), 9, "left", "0")]
own_cong <- own_cong[year %in% SAMPLE_YEARS]
assert_has_cols(own_cong, c("PID", "year", "conglomerate_id",
  "portfolio_size_conglomerate_yr", "portfolio_bin_conglomerate_yr",
  "total_units_conglomerate_yr"), "xwalk_pid_conglomerate")

nm_lookup <- fread(p_product(cfg, "name_lookup"))
assert_has_cols(nm_lookup, c("name_raw", "entity_id", "conglomerate_id", "is_corp"),
  "name_lookup")

# -----------------------------------------------------------------------
# Plaintiff/owner type regexes (used in Part B for plaintiff classification).
# Defined here so they are available when entity_owner_type is computed.
# -----------------------------------------------------------------------
pha_regex <- regex(
  paste0(
    # Full spelling — most common form
    "PHILADELPHIA\\s+HOUSING",
    # Abbreviated + OCR-mangled forms (e.g. "PHILA HOUSING AUTH")
    "|PHILA[.\\s]?HOUS[A-Z\\s.]{0,20}AUTH",
    # PAPMC acronym (PHA's management corp)
    "|\\bPAPMC\\b",
    # LLP affiliates that name PHA in their plaintiff string
    "|AFFILIATE\\s+OF\\s+THE\\s+PHILA",
    # Catch-all for any "HOUS...AUTH" not covered above
    "|\\bHOUS\\w*\\s+AUTH"
  ),
  ignore_case = TRUE
)

np_regex <- regex(
  paste0(
    "\\bAFFORDABLE\\s+HOUS",
    "|\\bWORKFORCE\\s+HOM",
    "|\\bCOMMUNITY\\s+DEV",
    "|\\bNEIGHBORHOOD\\s+REST",     # Neighborhood Restorations
    "|\\bNEIGHBORHOOD\\s+PRES",     # Neighborhood Preservation
    "|\\bCOMMUNITY\\s+LAND\\s+TRUST",
    "|\\bHABITAT\\s+FOR\\s+HUMAN",
    "|\\bOCTAVIA\\s+HILL",
    "|\\bSARAH\\s+ALLEN\\s+COMMUNITY",
    "|\\bMLK\\s+AFFORDABLE",
    "|\\bNEW\\s+LIFE\\s+AFFORD",
    "|\\bRENAISSANCE\\s+COMMUNITY\\s+DEV",
    "|\\bHELP\\s+PA\\s+AFFORD"
  ),
  ignore_case = TRUE
)

# Entity-level owner type from name_lookup$owner_category (built by build-ownership-panel.R).
# owner_category uses priority: FI > Gov-Federal > Gov-Local > PHA > Nonprofit > Religious
#                                > Trust > For-profit corp > Person.
# "Person" is the reference group, mirroring plaintiff_type = "Individual".
if (!"owner_category" %in% names(nm_lookup))
  stop("name_lookup missing owner_category — re-run build-ownership-panel.R")
nm_lookup[, name_raw_upper := toupper(name_raw)]
entity_owner_type <- nm_lookup[, .(owner_type = owner_category[1L]), by = entity_id]

ev_raw <- fread(p_product(cfg, "evictions_clean"))
# judgment_for_plaintiff is stored as character "t"/"f" in this dataset
ev_raw[, judgment_for_plaintiff_lgl := toupper(as.character(judgment_for_plaintiff)) %in% c("T", "TRUE", "1")]
ev_raw <- ev_raw[year %in% SAMPLE_YEARS]
# Filter commercial cases using defendant #1 classification from impute-race-preflight.R.
# The raw `commercial` flag in evictions_clean is unreliable; defendant #1's business-entity
# status from the party-names pipeline is more accurate and correctly excludes counterclaim
# parties (e.g. a corporate landlord counter-sued by a tenant would not make a case commercial).
case_commercial <- fread(p_product(cfg, "eviction_case_commercial"))
assert_has_cols(case_commercial, c("id", "is_commercial"), "eviction_case_commercial")
n_commercial <- case_commercial[is_commercial == TRUE, .N]
ev_raw <- ev_raw[!id %in% case_commercial[is_commercial == TRUE, id]]
logf("  Dropped ", n_commercial, " commercial cases (defendant #1 is not a person)",
     log_file = log_file)

xwalk_case <- fread(p_product(cfg, "evict_address_xwalk_case"))
xwalk_case[, PID := str_pad(as.character(PID), 9, "left", "0")]

race_bldg <- fread(p_product(cfg, "infousa_building_race_panel"))
race_bldg[, PID := str_pad(as.character(PID), 9, "left", "0")]
race_bldg <- race_bldg[year %in% SAMPLE_YEARS]

logf("  bldg: ", nrow(bldg), " rows, ", bldg[, uniqueN(PID)], " PIDs",
     log_file = log_file)
logf("  ev_raw: ", nrow(ev_raw), " rows", log_file = log_file)
logf("  xwalk_case: ", nrow(xwalk_case), " rows", log_file = log_file)
logf("  race_bldg: ", nrow(race_bldg), " rows", log_file = log_file)

# Optional: imputed defendant race
has_race_cases <- !is.null(cfg$products$race_imputed_case_sample) &&
  file.exists(p_product(cfg, "race_imputed_case_sample"))
if (has_race_cases) {
  race_cases <- fread(p_product(cfg, "race_imputed_case_sample"))
  logf("  race_cases: ", nrow(race_cases), " rows (loaded)", log_file = log_file)
} else {
  logf("  race_imputed_case_sample not available; Part B5 will be skipped",
       log_file = log_file)
}

# ============================================================
# PART A: Cross-Sectional Filing Rate Decomposition
# ============================================================
logf("=== PART A: Cross-sectional filing rate decomposition ===",
     log_file = log_file)

# ============================================================
# A1: Build PID-level cross-section
# ============================================================
logf("--- A1: Building PID cross-section ---", log_file = log_file)

# Mean pct_black across 2011-2019 per PID
race_pid <- race_bldg[!is.na(pct_black),
  .(pct_black_mean = mean(pct_black, na.rm = TRUE)),
  by = PID
]

# From bldg: one row per PID
pid_cs <- bldg[, {
  # EB rate: take any non-NA value (same for all years, or nearly so)
  eb_vals <- na.omit(filing_rate_eb_pre_covid)
  list(
    filing_rate_eb_pre_covid = if (length(eb_vals) > 0) eb_vals[1L] else NA_real_,
    num_filings_pre_covid    = sum(num_filings, na.rm = TRUE),
    unit_years_pre_covid     = sum(total_units, na.rm = TRUE),
    total_units_mean         = mean(total_units, na.rm = TRUE),
    num_years_observed       = .N,
    num_years_rental_license = sum(rental_from_license == T, na.rm = T),
    ever_rental_any_year     = any(ever_rental_any_year == TRUE, na.rm = TRUE),
    building_type            = Mode(na.omit(building_type)),
    year_blt_decade          = Mode(na.omit(as.character(year_blt_decade))),
    quality_grade            = Mode(na.omit(quality_grade)),
    num_units_bin            = Mode(na.omit(num_units_bin)),
    tract                    = substr(GEOID[1L], 1L, 11L),
    zip                      = pm.zip[1L]
  )
}, by = PID]

# Sample restrictions
# Note: no minimum unit count — any building with an eviction filing was a rental.
# pid_cs <- pid_cs[
#   ever_rental_any_year == TRUE &
#   num_years_observed >= 3
# ]

logf("  PID cross-section after restrictions: ", nrow(pid_cs), " PIDs",
     log_file = log_file)

# Ownership cross-section: modal owner_type per PID across all sample years.
# Join entity_owner_type (built in SECTION 0 from name_lookup) to the PID×year panel.
# Fall back to is_corp_owner for entities not in name_lookup (e.g. OPA-only ownership).
own_py_ot <- merge(own_py, entity_owner_type, by = "entity_id", all.x = TRUE)
# Fallback 1: use owner_category from xwalk_pid_entity for entities not in name_lookup
own_py_ot[is.na(owner_type) & !is.na(owner_category), owner_type := owner_category]
# Fallback 2: is_corp_owner flag for any remainder
own_py_ot[is.na(owner_type) & is_corp_owner == FALSE, owner_type := "Person"]
own_py_ot[is.na(owner_type) & is_corp_owner == TRUE,  owner_type := "For-profit corp"]

own_xsec <- own_py_ot[!is.na(owner_type), .(
  owner_type               = Mode(na.omit(owner_type)),
  portfolio_bin_entity_yr  = Mode(na.omit(as.character(portfolio_bin_entity_yr))),
  portfolio_size_entity_yr = mean(portfolio_size_entity_yr, na.rm = TRUE),
  mean_units_owned_entity  = mean(total_units_entity_yr, na.rm = TRUE)
), by = PID]

pid_cs <- merge(pid_cs,
  own_xsec[, .(PID, owner_type, portfolio_bin_entity_yr, portfolio_size_entity_yr,
               mean_units_owned_entity)],
  by = "PID", all.x = TRUE)

# Conglomerate portfolio cross-section (from improved Phase 2/3 entity reconciliation).
# Modal bin across sample years per PID; mean size for descriptive reporting.
cong_xsec <- own_cong[!is.na(portfolio_bin_conglomerate_yr), .(
  portfolio_bin_conglomerate_yr  = Mode(na.omit(as.character(portfolio_bin_conglomerate_yr))),
  portfolio_size_conglomerate_yr = mean(portfolio_size_conglomerate_yr, na.rm = TRUE),
  total_units_conglomerate_yr =    mean(total_units_conglomerate_yr,na.rm = T),
  mean_units_owned_cong          = mean(total_units_conglomerate_yr, na.rm = TRUE)
), by = PID]

pid_cs <- merge(pid_cs,
  cong_xsec[, .(PID, portfolio_bin_conglomerate_yr, portfolio_size_conglomerate_yr,
                total_units_conglomerate_yr,
                mean_units_owned_cong)],
  by = "PID", all.x = TRUE)

pid_cs <- merge(pid_cs, race_pid, by = "PID", all.x = TRUE)

# Rental share: fraction of observed sample years with a rental license.
# PHA-owned buildings are exempt from the City's rental license requirement —
# their num_years_rental_license is 0 even though they are full-time rentals.
# Correct by imputing rental_share = 1 for PHA owners identified via owner_type.
# For all others: rental_share = num_years_rental_license / num_years_observed,
# capped at 1 (should already be ≤ 1 but protects against data quirks).
pid_cs[, is_pha_owner_cs := (!is.na(owner_type) & owner_type == "PHA")]
pid_cs[, rental_share := fcase(
  is_pha_owner_cs == TRUE,  1.0,
  num_years_observed > 0,   pmin(num_years_rental_license / num_years_observed, 1.0),
  default = NA_real_
)]
logf("  rental_share: mean=", round(mean(pid_cs$rental_share, na.rm = TRUE), 3),
     " median=", round(median(pid_cs$rental_share, na.rm = TRUE), 3),
     " pct full-time (==1): ",
     round(100 * mean(pid_cs$rental_share == 1, na.rm = TRUE), 1), "%",
     log_file = log_file)

# log(unit_years) offset for PPML; also log-units covariate for OLS
pid_cs[unit_years_pre_covid > 0, log_unit_years := log(unit_years_pre_covid)]
pid_cs[total_units_mean > 0, log_units := log(total_units_mean)]

# Factor levels for fixed effects / categoricals
pid_cs[, owner_type := factor(owner_type,
  levels = c("Person", "For-profit corp", "Nonprofit", "PHA",
             "Religious", "Trust", "Government-Local", "Government-Federal",
             "Financial-Intermediary"))]
pid_cs[, portfolio_bin_entity_yr := factor(portfolio_bin_entity_yr,
  levels = c("1", "2-4", "5-19", "20+"))]
pid_cs[, portfolio_bin_conglomerate_yr := factor(portfolio_bin_conglomerate_yr,
  levels = c("1", "2-4", "5-19", "20+"))]
pid_cs[, building_type   := factor(building_type)]
pid_cs[, year_blt_decade := factor(year_blt_decade)]
pid_cs[, quality_grade     := factor(quality_grade)]
pid_cs[, num_units_bin     := factor(num_units_bin)]

logf("  PIDs with pct_black_mean: ", pid_cs[!is.na(pct_black_mean), .N],
     log_file = log_file)
logf("  PIDs with owner_type:     ", pid_cs[!is.na(owner_type), .N],
     log_file = log_file)
logf("  PIDs with portfolio_bin_entity_yr:       ",
     pid_cs[!is.na(portfolio_bin_entity_yr), .N], log_file = log_file)
logf("  PIDs with portfolio_bin_conglomerate_yr: ",
     pid_cs[!is.na(portfolio_bin_conglomerate_yr), .N], log_file = log_file)

# ============================================================
# A2: Descriptive means by landlord type
# ============================================================
logf("--- A2: Descriptive means by landlord type ---", log_file = log_file)

desc_groups <- list(
  is_corp_owner       = c(FALSE, TRUE),
  portfolio_bin       = c("1", "2-4", "5-19", "20+"),
  num_units_bin_grp   = NULL,
  building_code_grp   = NULL
)

desc_owner <- pid_cs[!is.na(owner_type), .(
  n_pids                  = .N,
  mean_filing_rate        = mean(filing_rate_eb_pre_covid, na.rm = TRUE),
  median_filing_rate      = median(filing_rate_eb_pre_covid, na.rm = TRUE),
  sd_filing_rate          = sd(filing_rate_eb_pre_covid, na.rm = TRUE),
  mean_bldg_units         = mean(total_units_mean, na.rm = TRUE),
  mean_units_owned_entity = mean(mean_units_owned_entity, na.rm = TRUE)
), by = .(owner_type)]

# do desc_owner but weight by total units
desc_owner_weighted = pid_cs[!is.na(owner_type), .(
  n_pids            = .N,
  mean_filing_rate  = weighted.mean(filing_rate_eb_pre_covid, total_units_mean, na.rm = TRUE),
  median_filing_rate = weighted.median(filing_rate_eb_pre_covid, total_units_mean, na.rm = TRUE),
  mean_units        = mean(total_units_mean, na.rm = TRUE)
), by = .(owner_type)]

desc_port <- pid_cs[!is.na(portfolio_bin_entity_yr), .(
  n_pids                  = .N,
  mean_filing_rate        = mean(filing_rate_eb_pre_covid, na.rm = TRUE),
  median_filing_rate      = median(filing_rate_eb_pre_covid, na.rm = TRUE),
  mean_bldg_units         = mean(total_units_mean, na.rm = TRUE),
  mean_units_owned_entity = mean(mean_units_owned_entity, na.rm = TRUE)
), by = .(portfolio_bin_entity_yr)]

desc_port_cong <- pid_cs[!is.na(portfolio_bin_conglomerate_yr), .(
  n_pids               = .N,
  mean_filing_rate     = mean(filing_rate_eb_pre_covid, na.rm = TRUE),
  median_filing_rate   = median(filing_rate_eb_pre_covid, na.rm = TRUE),
  mean_bldg_units      = mean(total_units_mean, na.rm = TRUE),
  mean_units_owned_cong = mean(mean_units_owned_cong, na.rm = TRUE)
), by = .(portfolio_bin_conglomerate_yr)]

desc_size <- pid_cs[!is.na(num_units_bin), .(
  n_pids           = .N,
  mean_filing_rate = mean(filing_rate_eb_pre_covid, na.rm = TRUE),
  median_filing_rate = median(filing_rate_eb_pre_covid, na.rm = TRUE)
), by = .(num_units_bin)]

desc_type <- pid_cs[!is.na(building_type), .(
  n_pids           = .N,
  mean_filing_rate = mean(filing_rate_eb_pre_covid, na.rm = TRUE),
  median_filing_rate = median(filing_rate_eb_pre_covid, na.rm = TRUE)
), by = .(building_type)][n_pids >= 10][order(-n_pids)]

# Combined descriptives
desc_all <- rbind(
  cbind(group_var = "owner_type",
        group_val = as.character(desc_owner$owner_type),
        desc_owner[, .(n_pids, mean_filing_rate, median_filing_rate,
                       mean_units_owned = mean_units_owned_entity)]),
  cbind(group_var = "portfolio_bin",
        group_val = as.character(desc_port$portfolio_bin_entity_yr),
        desc_port[, .(n_pids, mean_filing_rate, median_filing_rate,
                      mean_units_owned = mean_units_owned_entity)]),
  cbind(group_var = "portfolio_bin_conglomerate",
        group_val = as.character(desc_port_cong$portfolio_bin_conglomerate_yr),
        desc_port_cong[, .(n_pids, mean_filing_rate, median_filing_rate,
                           mean_units_owned = mean_units_owned_cong)]),
  cbind(group_var = "num_units_bin",
        group_val = as.character(desc_size$num_units_bin),
        desc_size[, .(n_pids, mean_filing_rate, median_filing_rate,
                      mean_units_owned = NA_real_)]),
  cbind(group_var = "building_type",
        group_val = as.character(desc_type$building_type),
        desc_type[, .(n_pids, mean_filing_rate, median_filing_rate,
                      mean_units_owned = NA_real_)]),
  fill = TRUE
)

fwrite(desc_all, p_fd("filing_decomp_descriptives_by_landlord.csv"))
logf("  Wrote: filing_decomp_descriptives_by_landlord.csv", log_file = log_file)

# ============================================================
# A3: Sequential R² variance decomposition
# ============================================================
logf("--- A3: Sequential R² decomposition ---", log_file = log_file)

# Building characteristics:
#   bldg_cov: shown as coefficients (informative continuous/ordinal variable)
#   bldg_fe:  shown in fixed-effects footer (categorical dummies, not informative row-by-row)
# rental_share is included from M1 onwards as a denominator correction:
# the EB filing rate denominator is all observed unit-years, but intermittent
# rentals (e.g. single-family homes rented only some years) have a deflated rate.
# rental_share = num_years_rental_license / num_years_observed (capped 0–1),
# with PHA buildings imputed to 1 (exempt from licensing, always fully rented).
bldg_cov <- "num_units_bin + rental_share"
bldg_fe  <- "building_type + year_blt_decade + quality_grade"

# Only keep PIDs with non-NA LHS, zip, tract, and rental_share
pid_cs_r2 <- pid_cs[!is.na(filing_rate_eb_pre_covid) &
                      !is.na(zip) & !is.na(tract) & !is.na(total_units_mean) &
                      !is.na(rental_share)]

# Weight by total_units_mean
wts <- pid_cs_r2$total_units_mean

# M0: intercept only
m0 <- feols(filing_rate_eb_pre_covid ~ 1,
  data = pid_cs_r2, weights = ~total_units_mean, notes = FALSE)

# M1: zip FE + building chars (bldg_fe absorbed into | side)
m1 <- feols(as.formula(paste0(
  "filing_rate_eb_pre_covid ~ ", bldg_cov, " | zip + ", bldg_fe)),
  data = pid_cs_r2, weights = ~total_units_mean, notes = FALSE)

# M2: tract FE + building chars
m2 <- feols(as.formula(paste0(
  "filing_rate_eb_pre_covid ~ ", bldg_cov, " | tract + ", bldg_fe)),
  data = pid_cs_r2, weights = ~total_units_mean, notes = FALSE)

# M3: tract FE + building chars + pct_black_mean
pid_cs_m3 <- pid_cs_r2[!is.na(pct_black_mean)]
m3 <- feols(as.formula(paste0(
  "filing_rate_eb_pre_covid ~ ", bldg_cov, " + pct_black_mean | tract + ", bldg_fe)),
  data = pid_cs_m3, weights = ~total_units_mean, notes = FALSE)

# M4: + owner_type + portfolio_bin (entity-level, on M3 sample)
pid_cs_m4 <- pid_cs_m3[!is.na(owner_type) & !is.na(portfolio_bin_entity_yr)]
m4 <- feols(as.formula(paste0(
  "filing_rate_eb_pre_covid ~ ", bldg_cov,
  " + pct_black_mean + owner_type + portfolio_bin_entity_yr | tract + ", bldg_fe)),
  data = pid_cs_m4, weights = ~total_units_mean, notes = FALSE)

# M5: + owner_type + conglomerate portfolio (branches from M3; uses improved Phase 2/3 linkage)
# Conglomerate portfolio measures landlord scale across all linked LLCs/entities.
pid_cs_m5 <- pid_cs_m3[!is.na(owner_type) & !is.na(portfolio_bin_conglomerate_yr)]
m5 <- feols(as.formula(paste0(
  "filing_rate_eb_pre_covid ~ ", bldg_cov,
  " + pct_black_mean + owner_type + portfolio_bin_conglomerate_yr | tract + ", bldg_fe)),
  data = pid_cs_m5, weights = ~total_units_mean, notes = FALSE)

# Compute R² values
# fixest::r2() uses "r2" and "ar2" (adjusted); "adj_r2" is not a valid key
get_r2 <- function(m) {
  list(
    r2     = unname(r2(m, type = "r2")),
    adj_r2 = unname(r2(m, type = "ar2")),
    n      = nobs(m)
  )
}

r2_m0 <- get_r2(m0)
r2_m1 <- get_r2(m1)
r2_m2 <- get_r2(m2)
r2_m3 <- get_r2(m3)
r2_m4 <- get_r2(m4)
r2_m5 <- get_r2(m5)

# Raw between-neighborhood variance share
tract_means <- pid_cs_r2[!is.na(filing_rate_eb_pre_covid),
  .(tract_mean = weighted.mean(filing_rate_eb_pre_covid, total_units_mean, na.rm = TRUE)),
  by = tract
]
pid_cs_r2_tm <- merge(pid_cs_r2, tract_means, by = "tract")
total_var    <- weighted.mean((pid_cs_r2_tm$filing_rate_eb_pre_covid -
                                weighted.mean(pid_cs_r2_tm$filing_rate_eb_pre_covid,
                                              pid_cs_r2_tm$total_units_mean))^2,
                              pid_cs_r2_tm$total_units_mean)
between_var  <- weighted.mean((pid_cs_r2_tm$tract_mean -
                                weighted.mean(pid_cs_r2_tm$tract_mean,
                                              pid_cs_r2_tm$total_units_mean))^2,
                              pid_cs_r2_tm$total_units_mean)
between_share <- between_var / total_var

r2_decomp <- data.table(
  model       = c("M0", "M1", "M2", "M3", "M4", "M5"),
  description = c(
    "Intercept only",
    "Zip FE + building chars + rental share",
    "Tract FE + building chars + rental share",
    "Tract FE + building + pct_black + rental share",
    "Tract FE + building + pct_black + rental share + landlord (entity portfolio)",
    "Tract FE + building + pct_black + rental share + landlord (conglomerate portfolio)"
  ),
  r2      = c(r2_m0$r2, r2_m1$r2, r2_m2$r2, r2_m3$r2, r2_m4$r2, r2_m5$r2),
  adj_r2  = c(r2_m0$adj_r2, r2_m1$adj_r2, r2_m2$adj_r2, r2_m3$adj_r2, r2_m4$adj_r2, r2_m5$adj_r2),
  n       = c(r2_m0$n, r2_m1$n, r2_m2$n, r2_m3$n, r2_m4$n, r2_m5$n),
  delta_r2 = NA_real_
)
# Sequential delta for M0-M4; M5 delta is relative to M3 (M4 and M5 both branch from M3)
r2_decomp[, delta_r2 := c(NA, diff(r2))]
r2_decomp[model == "M5", delta_r2 := r2_m5$r2 - r2_m3$r2]
r2_decomp[, between_nhood_share_raw := between_share]

fwrite(r2_decomp, p_fd("filing_decomp_r2_decomposition.csv"))
logf("  R² decomposition:", log_file = log_file)
for (i in seq_len(nrow(r2_decomp))) {
  logf("    ", r2_decomp$model[i], ": R²=",
       round(r2_decomp$r2[i], 4), "  ΔR²=",
       round(coalesce(r2_decomp$delta_r2[i], 0), 4),
       log_file = log_file)
}
logf("  Raw between-neighborhood share: ", round(between_share, 4),
     log_file = log_file)

# LaTeX table
r2_tex <- r2_decomp[, .(model, description, r2 = round(r2, 4),
                          adj_r2 = round(adj_r2, 4),
                          delta_r2 = round(coalesce(delta_r2, 0), 4), n)]
tex_lines <- c(
  "\\begin{table}[htbp]",
  "\\centering",
  "\\caption{Sequential R\\textsuperscript{2} Decomposition of Building-Level Filing Rates}",
  "\\label{tab:r2_decomp}",
  "\\begin{tabular}{p{2.4cm}p{7.5cm}cccr}",
  "\\hline",
  "Model & Description & $R^2$ & Adj.~$R^2$ & $\\Delta R^2$ & N \\\\",
  "\\hline",
  apply(r2_tex, 1, function(row) {
    paste(row["model"], "&", row["description"], "&",
          row["r2"], "&", row["adj_r2"], "&",
          row["delta_r2"], "&",
          format(as.integer(row["n"]), big.mark = ","), "\\\\")
  }),
  "\\hline",
  paste0("\\multicolumn{6}{l}{\\footnotesize Between-neighborhood raw variance share: ",
         round(between_share, 3), "} \\\\"),
  "\\multicolumn{6}{l}{\\footnotesize Weighted by total units. Sample: ever-rental PIDs with $\\geq$3 observed years, 2011--2019.} \\\\",
  "\\end{tabular}",
  "\\end{table}"
)
writeLines(tex_lines, p_fd("filing_decomp_r2_decomposition.tex"))
logf("  Wrote: filing_decomp_r2_decomposition.csv + .tex", log_file = log_file)

# ============================================================
# A4: Cross-sectional coefficient table (M1-M4)
# ============================================================
logf("--- A4: Cross-sectional coefficient table ---", log_file = log_file)

# Rerun on common sample for comparability; restrict to M4 sample
pid_cs_cmt <- pid_cs_m4  # M4 sample (most restricted)

m1c <- feols(as.formula(paste0(
  "filing_rate_eb_pre_covid ~ ", bldg_cov, " | zip + ", bldg_fe)),
  data = pid_cs_cmt, weights = ~total_units_mean,
  cluster = ~tract, notes = FALSE)

m2c <- feols(as.formula(paste0(
  "filing_rate_eb_pre_covid ~ ", bldg_cov, " | tract + ", bldg_fe)),
  data = pid_cs_cmt, weights = ~total_units_mean,
  cluster = ~tract, notes = FALSE)

m3c <- feols(as.formula(paste0(
  "filing_rate_eb_pre_covid ~ ", bldg_cov, " + pct_black_mean | tract + ", bldg_fe)),
  data = pid_cs_cmt, weights = ~total_units_mean,
  cluster = ~tract, notes = FALSE)

m4c <- feols(as.formula(paste0(
  "filing_rate_eb_pre_covid ~ ", bldg_cov,
  " + pct_black_mean + owner_type + portfolio_bin_entity_yr | tract + ", bldg_fe)),
  data = pid_cs_cmt, weights = ~total_units_mean,
  cluster = ~tract, notes = FALSE)

# M5c: conglomerate portfolio on M5 sample (may differ slightly from M4 sample)
m5c <- feols(as.formula(paste0(
  "filing_rate_eb_pre_covid ~ ", bldg_cov,
  " + pct_black_mean + owner_type + portfolio_bin_conglomerate_yr | tract + ", bldg_fe)),
  data = pid_cs_m5, weights = ~total_units_mean,
  cluster = ~tract, notes = FALSE)

coef_models <- list(M1 = m1c, M2 = m2c, M3 = m3c, M4 = m4c, M5 = m5c)

coef_dt <- rbindlist(lapply(names(coef_models), function(nm) {
  m  <- coef_models[[nm]]
  ct <- coeftable(m)
  data.table(
    model     = nm,
    term      = rownames(ct),
    estimate  = ct[, "Estimate"],
    std_error = ct[, "Std. Error"],
    t_stat    = ct[, "t value"],
    p_value   = ct[, "Pr(>|t|)"]
  )
}))

fwrite(coef_dt, p_fd("filing_decomp_xsec_coefs.csv"))
logf("  Wrote: filing_decomp_xsec_coefs.csv", log_file = log_file)

# LaTeX etable via fixest
etable(coef_models,
  file          = p_fd("filing_decomp_xsec_etable.tex"),
  title         = "Cross-Sectional Filing Rate Regressions",
  label         = "tab:xsec_filing",
  fitstat       = c("n", "r2", "ar2"),
  notes         = "Weighted by total units. Cluster SE at tract. Sample: 2011--2019 rental PIDs.",
  style.tex     = style.tex("base"),
  replace       = TRUE
)
logf("  Wrote: filing_decomp_xsec_etable.tex", log_file = log_file)

# ============================================================
# A5: PPML count robustness
# ============================================================
logf("--- A5: PPML count robustness ---", log_file = log_file)

pid_cs_pp <- pid_cs_m4[num_filings_pre_covid >= 0 & log_unit_years > 0]

m_ppml <- fepois(
  as.formula(paste0(
    "num_filings_pre_covid ~ ", bldg_cov,
    " + pct_black_mean + owner_type + portfolio_bin_entity_yr",
    " + offset(log_unit_years) | tract + ", bldg_fe)),
  data    = pid_cs_pp,
  cluster = ~tract,
  notes   = FALSE
)

# M5 PPML: conglomerate portfolio (branches from M3 PPML)
pid_cs_pp_m5 <- pid_cs_m5[num_filings_pre_covid >= 0 & log_unit_years > 0]
m_ppml_m5 <- fepois(
  as.formula(paste0(
    "num_filings_pre_covid ~ ", bldg_cov,
    " + pct_black_mean + owner_type + portfolio_bin_conglomerate_yr",
    " + offset(log_unit_years) | tract + ", bldg_fe)),
  data    = pid_cs_pp_m5,
  cluster = ~tract,
  notes   = FALSE
)

extract_ppml_coefs <- function(m, model_label) {
  ct    <- coeftable(m)
  z_col <- if ("z value"   %in% colnames(ct)) "z value"   else "t value"
  p_col <- if ("Pr(>|z|)" %in% colnames(ct)) "Pr(>|z|)" else "Pr(>|t|)"
  data.table(
    model     = model_label,
    term      = rownames(ct),
    estimate  = ct[, "Estimate"],
    std_error = ct[, "Std. Error"],
    z_stat    = ct[, z_col],
    p_value   = ct[, p_col]
  )
}

ppml_dt <- rbind(
  extract_ppml_coefs(m_ppml,    "M4_entity_portfolio"),
  extract_ppml_coefs(m_ppml_m5, "M5_conglomerate_portfolio")
)
fwrite(ppml_dt, p_fd("filing_decomp_ppml_coefs.csv"))
logf("  PPML M4 N: ", nobs(m_ppml), "  M5 N: ", nobs(m_ppml_m5), log_file = log_file)
logf("  Wrote: filing_decomp_ppml_coefs.csv", log_file = log_file)

# ============================================================
# PART B: Case-Level Back-Rent Analysis
# ============================================================
logf("=== PART B: Case-level back-rent analysis ===", log_file = log_file)

# ============================================================
# B0: Build case-level dataset
# ============================================================
logf("--- B0: Building case-level dataset ---", log_file = log_file)

# ev_raw is already filtered to non-commercial + SAMPLE_YEARS in S0 via commercial_lgl
# Additional filters: positive rent fields and months >= 1
ev <- ev_raw[
  !is.na(ongoing_rent) & ongoing_rent > 0 &
  !is.na(total_rent)   & total_rent   > 0 &
  !is.na(months)       & months       >= 1
]
logf("  Cases after rent/month filters: ", nrow(ev), log_file = log_file)

# Join case -> PID (single-PID matches only)
xwalk_1pid <- xwalk_case[num_parcels_matched == 1, .(id, PID)]
n_ev_universe <- nrow(ev_raw)
ev <- merge(ev, xwalk_1pid, by = "id", all.x = FALSE)
logf("  Cases after PID linkage: ", nrow(ev),
     " (", round(100 * nrow(ev) / n_ev_universe, 1),
     "% of non-commercial universe)", log_file = log_file)

# Winsorize: drop extreme rent values (data quality / outlier exclusion)
ev <- ev[ongoing_rent >= 30L & ongoing_rent <= 6000L &
         total_rent  >= 30L & total_rent  <= 15000L]
logf("  Cases after rent winsorization (ongoing 30-6000, total 30-15000): ",
     nrow(ev), log_file = log_file)

# Join bldg characteristics at PID x year
# Note: evictions_clean may also have pm.zip; suffix.y = bldg zip (parcel data, more reliable)
bldg_case_cols <- c("PID", "year", "filing_rate_eb_pre_covid", "num_units_bin",
  "building_type", "GEOID", "pm.zip", "total_units")
ev <- merge(ev,
  bldg[, ..bldg_case_cols],
  by = c("PID", "year"), all.x = TRUE
)
# Resolve pm.zip collision (if ev_raw also had pm.zip)
if ("pm.zip.y" %in% names(ev)) {
  ev[, pm.zip := coalesce(pm.zip.y, pm.zip.x)]
  ev[, c("pm.zip.x", "pm.zip.y") := NULL]
}

# Join entity ownership (owner_category used as fallback for entities not in name_lookup)
ev <- merge(ev,
  own_py[, .(PID, year, entity_id, is_corp_owner, owner_category,
              portfolio_size_entity_yr, has_rtt_owner)],
  by = c("PID", "year"), all.x = TRUE
)

# Join conglomerate (including conglomerate portfolio bin for B5 regression)
ev <- merge(ev,
  own_cong[, .(PID, year, conglomerate_id,total_units_conglomerate_yr,
               portfolio_size_conglomerate_yr, portfolio_bin_conglomerate_yr)],
  by = c("PID", "year"), all.x = TRUE
)

# Join building-level racial composition (annual)
ev <- merge(ev,
  race_bldg[, .(PID, year, pct_black)],
  by = c("PID", "year"), all.x = TRUE
)

# --- Plaintiff corporate classification and owner matching ---
ev[, plaintiff_name_upper := toupper(trimws(strip_occupant_phrases(
  coalesce(as.character(plaintiff), ""))))]

# Match plaintiff to entity/conglomerate via name_lookup (exact join)
ev <- merge(ev,
  nm_lookup[, .(
    name_raw,
    plaintiff_entity_id     = entity_id,
    plaintiff_conglomerate_id = conglomerate_id,
    is_corp_plaintiff       = is_corp
  )],
  by.x = "plaintiff_name_upper", by.y = "name_raw",
  all.x = TRUE
)

# Fallback: classify by regex if not in name_lookup
ev[is.na(is_corp_plaintiff),
   is_corp_plaintiff := str_detect(plaintiff_name_upper, business_regex)]

# plaintiff_is_owner: same entity
ev[, plaintiff_is_owner := (
  !is.na(plaintiff_entity_id) &
  !is.na(entity_id) &
  plaintiff_entity_id == entity_id
)]

# plaintiff_same_conglomerate: broader — same economic actor
ev[, plaintiff_same_conglomerate := (
  !is.na(plaintiff_conglomerate_id) &
  !is.na(conglomerate_id) &
  plaintiff_conglomerate_id == conglomerate_id
)]

# pha_regex and np_regex defined in SECTION 0 (before Part A, for plaintiff classification)
ev[, is_pha_plaintiff      := str_detect(plaintiff_name_upper, pha_regex)]
ev[, is_nonprofit_plaintiff := str_detect(plaintiff_name_upper, np_regex)]

# For-profit corporate: institutional (is_corp) but not PHA and not a known nonprofit
ev[, is_forprofit_corp_plaintiff :=
     is_corp_plaintiff & !is_pha_plaintiff & !is_nonprofit_plaintiff]

# Ordered categorical plaintiff type (Individual = reference)
ev[, plaintiff_type := factor(fcase(
  is_pha_plaintiff,         "PHA",
  is_nonprofit_plaintiff,   "Nonprofit",
  is_corp_plaintiff,        "For-profit corp",
  default = "Individual"
), levels = c("Individual", "For-profit corp", "Nonprofit", "PHA"))]

# -----------------------------------------------------------------------
# PHA owner flag — use owner_category from name_lookup (set in SECTION 0)
# -----------------------------------------------------------------------
pha_entity_ids <- nm_lookup[!is.na(owner_category) & owner_category == "PHA",
                             unique(entity_id)]
ev[, is_pha_owner := entity_id %in% pha_entity_ids]
logf("  PHA entity IDs identified: ", length(pha_entity_ids), log_file = log_file)

# Owner type categorical (Person = reference group), analogous to plaintiff_type.
# Joins entity_owner_type computed in SECTION 0 from name_lookup.
ev <- merge(ev, entity_owner_type, by = "entity_id", all.x = TRUE)
# Fallback 1: use owner_category from xwalk_pid_entity for entities not in name_lookup
ev[is.na(owner_type) & !is.na(owner_category), owner_type := owner_category]
# Fallback 2: is_corp_owner flag for any remainder
ev[is.na(owner_type) & !is.na(is_corp_owner) & is_corp_owner == FALSE, owner_type := "Person"]
ev[is.na(owner_type) & !is.na(is_corp_owner) & is_corp_owner == TRUE,  owner_type := "For-profit corp"]
ev[is.na(owner_type), owner_type := "Person"]  # assume person if no ownership info
ev[, owner_type := factor(owner_type,
  levels = c("Person", "For-profit corp", "Nonprofit", "PHA",
             "Religious", "Trust", "Government-Local", "Government-Federal",
             "Financial-Intermediary"))]
ev[, portfolio_bin_conglomerate_yr := factor(portfolio_bin_conglomerate_yr,
  levels = c("1", "2-4", "5-19", "20+"))]

# Derived variables
ev[, log_months          := log(months)]
ev[, log_total_rent      := log(total_rent)]
ev[, log_ongoing_rent    := log(ongoing_rent)]
ev[, months_implied      := total_rent / ongoing_rent]
ev[,total_units_conglomerate_yr_net_parcel :=total_units_conglomerate_yr - total_units ]
ev[,total_units_conglomerate_yr_net_parcel_bin := case_when(
  total_units_conglomerate_yr_net_parcel == 0 ~ "single-property",
  total_units_conglomerate_yr_net_parcel >0 & total_units_conglomerate_yr_net_parcel <= 10 ~ "small landlord",
  total_units_conglomerate_yr_net_parcel >=10 & total_units_conglomerate_yr_net_parcel <50 ~ "medium landlord",
  total_units_conglomerate_yr_net_parcel > 50 ~ "large landlord",
  TRUE ~ NA_character_
)]
ev[, total_units_conglomerate_yr_net_parcel_bin := factor(
  total_units_conglomerate_yr_net_parcel_bin,
  levels = c("single-property", "small landlord", "medium landlord", "large landlord"))]
# ev

# High-filer building flag (top quartile of EB rate across cases)
q75_rate <- quantile(ev$filing_rate_eb_pre_covid, 0.75, na.rm = TRUE) # note this doesnt make sense since were looking at high filing within cases filed, so it's a weird weighted avg
ev[, high_filer_bldg := filing_rate_eb_pre_covid >= 0.15]

# Cluster variable
ev[, tract := substr(as.character(GEOID), 1L, 11L)]

# Optional: join defendant race
if (has_race_cases) {
  assert_has_cols(race_cases, c("id"), "race_cases")
  race_col <- grep("race_group|race_black|black", names(race_cases),
                   value = TRUE, ignore.case = TRUE)[1L]
  if (!is.na(race_col)) {
    ev <- merge(ev, race_cases[, c("id", race_col), with = FALSE],
                by = "id", all.x = TRUE)
    setnames(ev, race_col, "case_race_group")
    ev[, case_race_black := case_race_group == "Black" |
         str_detect(tolower(coalesce(as.character(case_race_group), "")), "black")]
    logf("  Defendant race merged: ",
         ev[!is.na(case_race_black), .N], " cases", log_file = log_file)
  }
}

logf("  Final case dataset: ", nrow(ev), " cases", log_file = log_file)
logf("  PID match rate:                  ",
     round(100 * mean(!is.na(ev$entity_id)), 1), "%", log_file = log_file)
logf("  Plaintiff-in-lookup rate:        ",
     round(100 * mean(!is.na(ev$plaintiff_entity_id)), 1), "%", log_file = log_file)
logf("  Plaintiff-is-owner rate:         ",
     round(100 * mean(ev$plaintiff_is_owner, na.rm = TRUE), 1), "%", log_file = log_file)
logf("  is_corp_plaintiff rate:          ",
     round(100 * mean(ev$is_corp_plaintiff, na.rm = TRUE), 1), "%", log_file = log_file)
logf("  is_pha_plaintiff rate:           ",
     round(100 * mean(ev$is_pha_plaintiff, na.rm = TRUE), 1), "%", log_file = log_file)
logf("  is_nonprofit_plaintiff rate:     ",
     round(100 * mean(ev$is_nonprofit_plaintiff, na.rm = TRUE), 1), "%", log_file = log_file)
logf("  is_forprofit_corp_plaintiff rate:",
     round(100 * mean(ev$is_forprofit_corp_plaintiff, na.rm = TRUE), 1), "%", log_file = log_file)
logf("  is_corp_owner rate:              ",
     round(100 * mean(ev$is_corp_owner, na.rm = TRUE), 1), "%", log_file = log_file)
logf("  Plaintiff type distribution:\n",
     capture.output(print(ev[, .N, by = plaintiff_type][order(-N)])),
     log_file = log_file)
logf("  Owner type distribution:\n",
     capture.output(print(ev[, .N, by = owner_type][order(-N)])),
     log_file = log_file)

# ============================================================
# B1: Descriptive table by landlord type
# ============================================================
logf("--- B1: Descriptive table ---", log_file = log_file)

make_desc <- function(group_var, group_vals = NULL) {
  grp <- ev[!is.na(get(group_var)), .(
    n_cases                      = .N,
    mean_months                  = mean(months, na.rm = TRUE),
    median_months                = median(months, na.rm = TRUE),
    mean_ongoing_rent            = mean(ongoing_rent, na.rm = TRUE),
    mean_total_rent              = mean(total_rent, na.rm = TRUE),
    mean_months_implied          = mean(months_implied, na.rm = TRUE),
    mean_total_units_conglomerate_yr = mean(total_units_conglomerate_yr, na.rm = TRUE),
    pct_plaintiff_win            = mean(judgment_for_plaintiff_lgl == TRUE, na.rm = TRUE),
    mean_award_ratio             = mean(
      fifelse(!is.na(award_total_amount_due) & !is.na(amt_sought) &
                amt_sought > 0 & judgment_for_plaintiff == TRUE,
              award_total_amount_due / amt_sought, NA_real_),
      na.rm = TRUE)
  ), by = c(group_var)]
  cbind(group_var = group_var, as.data.table(grp))
}

desc_b <- rbind(
  make_desc("is_corp_plaintiff"),
  make_desc("is_forprofit_corp_plaintiff"),
  make_desc("is_pha_plaintiff"),
  make_desc("is_nonprofit_plaintiff"),
  make_desc("is_corp_owner"),
  make_desc("is_pha_owner"),
  make_desc("plaintiff_is_owner"),
  make_desc("high_filer_bldg"),
  make_desc("total_units_conglomerate_yr_net_parcel_bin"),
  fill = TRUE
)

fwrite(desc_b, p_fd("backrent_descriptives_by_landlord.csv"))
logf("  Wrote: backrent_descriptives_by_landlord.csv", log_file = log_file)

# ============================================================
# B2: Months-of-arrears regressions
# ============================================================
logf("--- B2: Months-of-arrears regressions ---", log_file = log_file)

ev_reg <- ev[!is.na(log_months) & !is.na(log_ongoing_rent) &
               !is.na(tract) & !is.na(year)]

# Spec 1: plaintiff_type + rent | year + tract (Individual = reference group)
bm1 <- feols(log_months ~ plaintiff_type + log_ongoing_rent |
               year + tract,
  data    = ev_reg,
  cluster = ~PID,
  notes   = FALSE
)

# Spec 2: + owner_type + filing intensity + landlord scale (Person = reference group)
bm2 <- feols(log_months ~ plaintiff_type +
               owner_type + high_filer_bldg +
               total_units_conglomerate_yr_net_parcel_bin +
               log_ongoing_rent |
               year + tract,
  data    = ev_reg[!is.na(owner_type) & !is.na(total_units_conglomerate_yr_net_parcel_bin)],
  cluster = ~PID,
  notes   = FALSE
)

# Spec 3: + plaintiff_is_owner + building_type FE + pct_black + landlord scale
bm3 <- feols(log_months ~ plaintiff_type +
               owner_type + plaintiff_is_owner +
               high_filer_bldg +
               total_units_conglomerate_yr_net_parcel_bin +
               log_ongoing_rent + pct_black + num_units_bin |
               year + tract + building_type,
  data    = ev_reg[!is.na(owner_type) & !is.na(pct_black) &
                     !is.na(building_type) & !is.na(total_units_conglomerate_yr_net_parcel_bin)],
  cluster = ~PID,
  notes   = FALSE
)

# Spec 4: Interaction plaintiff_type * high_filer_bldg + landlord scale
bm4 <- feols(log_months ~ plaintiff_type * high_filer_bldg +
               owner_type + plaintiff_is_owner +
               total_units_conglomerate_yr_net_parcel_bin +
               log_ongoing_rent + pct_black + num_units_bin |
               year + tract + building_type,
  data    = ev_reg[!is.na(owner_type) & !is.na(pct_black) &
                     !is.na(building_type) & !is.na(total_units_conglomerate_yr_net_parcel_bin)],
  cluster = ~PID,
  notes   = FALSE
)

# Spec 5: Add conglomerate portfolio size (landlord scale across all linked LLCs).
# Uses portfolio_bin_conglomerate_yr from improved Phase 2/3 entity reconciliation.
bm5 <- feols(log_months ~ plaintiff_type + high_filer_bldg +
               log(portfolio_size_conglomerate_yr) +
               high_filer_bldg +
               log_ongoing_rent + pct_black + num_units_bin |
               year + tract + building_type,
  data    = ev_reg[!is.na(owner_type) & !is.na(pct_black) &
                     !is.na(building_type) & !is.na(portfolio_bin_conglomerate_yr)],
  cluster = ~PID,
  notes   = FALSE
)

months_models <- list(B1 = bm1, B2 = bm2, B3 = bm3, B4 = bm4, B5 = bm5)

months_coef_dt <- rbindlist(lapply(names(months_models), function(nm) {
  m  <- months_models[[nm]]
  ct <- coeftable(m)
  data.table(model = nm, term = rownames(ct),
    estimate = ct[, "Estimate"], std_error = ct[, "Std. Error"],
    t_stat = ct[, "t value"], p_value = ct[, "Pr(>|t|)"])
}))

fwrite(months_coef_dt, p_fd("backrent_months_coefs.csv"))
logf("  Wrote: backrent_months_coefs.csv", log_file = log_file)

etable(months_models,
  file      = p_fd("backrent_months_etable.tex"),
  title     = "Back-Rent Months of Arrears Regressions",
  label     = "tab:backrent_months",
  fitstat   = c("n", "r2", "ar2"),
  notes     = "Cluster SE at PID. Sample: non-commercial cases 2011--2019 with months >= 1.",
  style.tex = style.tex("base"),
  replace   = TRUE
)
logf("  Wrote: backrent_months_etable.tex", log_file = log_file)

# ============================================================
# B3: Back-rent elasticity regression
# ============================================================
logf("--- B3: Back-rent elasticity ---", log_file = log_file)

ev_elast <- ev_reg[!is.na(owner_type) & !is.na(pct_black) &
                     !is.na(building_type)]

brent1 <- feols(log_total_rent ~ log_ongoing_rent +
                  plaintiff_type +
                  owner_type +
                  #plaintiff_is_owner +
                  high_filer_bldg +
                  log(portfolio_size_conglomerate_yr) +
                  pct_black  |
                  year+ num_units_bin + tract + building_type,
  data    = ev_elast,
  cluster = ~PID,
  notes   = FALSE
)

rent_ct <- coeftable(brent1)
rent_dt <- data.table(
  term      = rownames(rent_ct),
  estimate  = rent_ct[, "Estimate"],
  std_error = rent_ct[, "Std. Error"],
  t_stat    = rent_ct[, "t value"],
  p_value   = rent_ct[, "Pr(>|t|)"]
)
fwrite(rent_dt, p_fd("backrent_rent_coefs.csv"))
etable(brent1,
  file      = p_fd("backrent_rent_etable.tex"),
  title     = "Back-Rent Elasticity Regression",
  label     = "tab:backrent_rent",
  fitstat   = c("n", "r2", "ar2"),
  notes     = "Cluster SE at PID. Sample: non-commercial cases 2011--2019, ongoing\\_rent 300--6000, total\\_rent 300--15000.",
  style.tex = style.tex("base"),
  replace   = TRUE
)
logf("  log_ongoing_rent coefficient (elasticity): ",
     round(coef(brent1)["log_ongoing_rent"], 4), log_file = log_file)
logf("  Wrote: backrent_rent_coefs.csv + backrent_rent_etable.tex",
     log_file = log_file)

# ============================================================
# B4: Racial disparity specs (optional)
# ============================================================
if (has_race_cases && "case_race_black" %in% names(ev)) {
  logf("--- B4: Racial disparity specs ---", log_file = log_file)

  ev_race <- ev_reg[!is.na(case_race_black) & !is.na(owner_type) &
                      !is.na(pct_black) & !is.na(building_type)]

  brace <- feols(log_months ~ case_race_black + plaintiff_type +
                   case_race_black:plaintiff_type +
                   owner_type + plaintiff_is_owner +
                   log_ongoing_rent + pct_black |
                   year + tract + building_type,
    data    = ev_race,
    cluster = ~PID,
    notes   = FALSE
  )

  race_ct <- coeftable(brace)
  race_dt <- data.table(
    term      = rownames(race_ct),
    estimate  = race_ct[, "Estimate"],
    std_error = race_ct[, "Std. Error"],
    t_stat    = race_ct[, "t value"],
    p_value   = race_ct[, "Pr(>|t|)"]
  )
  fwrite(race_dt, p_fd("backrent_race_coefs.csv"))
  logf("  Racial disparity regression N: ", nobs(brace), log_file = log_file)
  logf("  Wrote: backrent_race_coefs.csv", log_file = log_file)
} else {
  logf("--- B4: Skipped (race_imputed_case_sample not available) ---",
       log_file = log_file)
}

logf("=== analyze-filing-decomposition.R complete ===", log_file = log_file)
logf("  All outputs in: ", out_dir, log_file = log_file)
