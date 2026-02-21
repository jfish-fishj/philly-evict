## ============================================================
## make-analytic-sample.R
## ============================================================
## Purpose: Build the final analytic sample from ever-rentals panel
##
## Inputs (from config):
##   - ever_rentals_panel (from make-rent-panel.R)
##   - parcel_occupancy_panel (from make-occupancy-vars.R; total_units, renter_occ)
##   - building_data (from make-building-data.R; violations, permits, complaints)
##   - infousa_building_demographics_panel (from make-building-demographics.R)
##   - parcels_clean (owner_1, mailing_street, pm.zip for owner/zip fallback)
##   - assessments (raw)
##
## Outputs (to config):
##   - bldg_panel_blp: full panel with shares, HHI, BLP instruments
##   - analytic_sample: filtered estimation sample
##
## Primary keys:
##   - bldg_panel_blp: PID × year
##   - analytic_sample: PID × year (subset)
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(tidyverse)
})

# ---- Load config and set up logging ----
source("r/config.R")
source("r/helper-functions.R")
cfg <- read_config()
log_file <- p_out(cfg, "logs", "make-analytic-sample.log")

logf("=== Starting make-analytic-sample.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

normalize_pid <- function(x) {
  stringr::str_pad(as.character(x), width = 9, side = "left", pad = "0")
}
fco <- function(x, val = 0) fifelse(is.na(x), val, x)

# ---- Load input data ----
logf("Loading input data...", log_file = log_file)

path_ever_panel   <- p_product(cfg, "ever_rentals_panel")
path_occ_panel    <- p_product(cfg, "parcel_occupancy_panel")
path_events_panel <- p_product(cfg, "building_data")
path_bldg_demog   <- p_product(cfg, "infousa_building_demographics_panel")
path_assess_csv   <- p_input(cfg, "assessments")
path_parcels_clean <- p_product(cfg, "parcels_clean")

ever_panel    <- fread(path_ever_panel)
logf("  ever_rentals_panel: ", nrow(ever_panel), " rows", log_file = log_file)

occ_panel     <- fread(path_occ_panel)
logf("  parcel_occupancy_panel: ", nrow(occ_panel), " rows", log_file = log_file)

events_panel  <- fread(path_events_panel)
logf("  building_data (events): ", nrow(events_panel), " rows", log_file = log_file)

bldg_demog <- fread(path_bldg_demog)
logf("  infousa_building_demographics_panel: ", nrow(bldg_demog), " rows", log_file = log_file)

ass <- fread(path_assess_csv)
logf("  assessments: ", nrow(ass), " rows", log_file = log_file)

parcels_meta <- fread(path_parcels_clean,
                      select = c("parcel_number", "owner_1", "mailing_street", "pm.zip",
                                 "number_stories", "topography" , "total_area" , "total_livable_area" , "type_heater",
                                 "unfinished"  ,"building_code_description"   ,  "building_code_description_new", "quality_grade"
                                 ))
parcels_meta[, PID := normalize_pid(parcel_number)]
parcels_meta[, parcel_number := NULL]
setnames(parcels_meta, "pm.zip", "pm.zip_parcels")
parcels_meta <- unique(parcels_meta, by = "PID")

# clean parcels meta
if (!"building_type" %in% names(parcels_meta) &&
    all(c("building_code_description", "building_code_description_new") %in% names(parcels_meta))) {

  bldg_result <- standardize_building_type(
    bldg_code_desc     = parcels_meta$building_code_description,
    bldg_code_desc_new = parcels_meta$building_code_description_new,
    num_bldgs   = if ("num_bldgs" %in% names(parcels_meta)) parcels_meta$num_bldgs else NA_integer_,
    num_stories = parcels_meta$number_stories
  )
  parcels_meta[, building_type := bldg_result$building_type]
  parcels_meta[, is_condo := bldg_result$is_condo]
}

# Impute stories by building_type (not fixed legacy column)
impute_group_col <- if ("building_type" %in% names(parcels_meta)) "building_type" else "building_code_description_new_fixed"

parcels_meta[, num_stories_mean := mean(number_stories, na.rm = TRUE), by = c(impute_group_col)]
parcels_meta[, num_stories_imp  := fifelse(!is.na(number_stories), number_stories, num_stories_mean)]

# parcels_meta[, num_bldgs_mean := mean(num_bldgs, na.rm = TRUE), by = c(impute_group_col)]
# parcels_meta[, num_bldgs_imp  := fifelse(!is.na(num_bldgs), num_bldgs, num_bldgs_mean)]
# parcels_meta[is.na(num_bldgs_imp), num_bldgs_imp := 1]

logf("  parcels_clean (meta): ", nrow(parcels_meta), " rows", log_file = log_file)

setDT(ever_panel)
setDT(occ_panel)
setDT(events_panel)
setDT(bldg_demog)
setDT(ass)

## Normalize PIDs
ever_panel[,   PID := normalize_pid(PID)]
occ_panel[,    PID := normalize_pid(PID)]
events_panel[, PID := normalize_pid(parcel_number)]
events_panel[, parcel_number := NULL]
if ("period" %in% names(events_panel)) events_panel[, period := NULL]
bldg_demog[, PID := normalize_pid(PID)]
bldg_demog[, year := as.integer(year)]
assert_unique(bldg_demog, c("PID", "year"), "infousa_building_demographics_panel")

## ------------------------------------------------------------
## 2) BUILD BASE PANEL (PID × YEAR) FROM OCC + EVER + EVENTS
## ------------------------------------------------------------
bldg_panel <- copy(occ_panel)

## Merge parcels_clean metadata (owner + zip fallback)
bldg_panel <- merge(bldg_panel, parcels_meta, by = "PID", all.x = TRUE)

## ever_rentals_panel: rents, rental flags, filings, parcel chars
logf("Merging ever_rentals_panel...", log_file = log_file)
logf("  bldg_panel rows before merge: ", nrow(bldg_panel), log_file = log_file)

# drop num_filings from bldg_panel
bldg_panel[,num_years_in_panel := .N, by = PID]
bldg_panel[,num_years_pre2019 := sum(year <= 2019), by = PID]
bldg_panel[,total_filings := sum(num_filings, na.rm = TRUE), by = PID]
bldg_panel[,total_filings_pre2019 := sum(num_filings[year <= 2019], na.rm = TRUE), by = PID]

bldg_panel[,filing_rate_longrun := total_filings / total_units / num_years_in_panel ]
bldg_panel[,filing_rate_longrun_pre2019 := total_filings_pre2019 / total_units / num_years_pre2019 ]

bldg_panel <- merge(
  bldg_panel,
  ever_panel,
  by    = c("PID", "year"),
  all.x = T,
  suffixes = c("", "_ever")
)

logf("  bldg_panel rows after merge: ", nrow(bldg_panel), log_file = log_file)
assert_unique(bldg_panel, c("PID", "year"), "bldg_panel after ever_panel merge")
logf("  Assertion passed: (PID, year) unique after ever_panel merge", log_file = log_file)

## Coalesce pm.zip: prefer ever_panel's, fall back to parcels_clean
## Must convert empty strings to NA first — otherwise coalesce treats "" as non-missing
clean_zip <- function(z) {
  z <- as.character(z)
  z[is.na(z) | z %in% c("", "NA", "00000", "000NA", "   NA")] <- NA_character_
  z <- str_remove(z, "^_")
  z <- str_pad(z, 5, "left", "0")
  z[z %in% c("00000", "000NA")] <- NA_character_
  z
}
bldg_panel[, pm.zip := coalesce(clean_zip(pm.zip), clean_zip(pm.zip_parcels))]
bldg_panel[, pm.zip_parcels := NULL]

## Single merged events file: violations + permits + complaints (PID×year)
logf("Merging events_panel...", log_file = log_file)
logf("  bldg_panel rows before merge: ", nrow(bldg_panel), log_file = log_file)

bldg_panel <- merge(
  bldg_panel,
  events_panel,
  by    = c("PID", "year"),
  # Keep all occupancy panel rows. Dropping rows here changes market denominators
  # and silently changes the implied outside good in demand shares.
  all.x = TRUE
)

logf("  bldg_panel rows after merge: ", nrow(bldg_panel), log_file = log_file)
assert_unique(bldg_panel, c("PID", "year"), "bldg_panel after events_panel merge")
logf("  Assertion passed: (PID, year) unique after events_panel merge", log_file = log_file)

## ------------------------------------------------------------
## 2b) MERGE INFOUSA BUILDING DEMOGRAPHICS (PID × YEAR)
## ------------------------------------------------------------
logf("Merging infousa_building_demographics_panel...", log_file = log_file)
logf("  bldg_panel rows before merge: ", nrow(bldg_panel), log_file = log_file)

bldg_panel <- merge(
  bldg_panel,
  bldg_demog,
  by = c("PID", "year"),
  all.x = TRUE
)

logf("  bldg_panel rows after merge: ", nrow(bldg_panel), log_file = log_file)
assert_unique(bldg_panel, c("PID", "year"), "bldg_panel after infousa demographics merge")
logf("  Assertion passed: (PID, year) unique after infousa demographics merge", log_file = log_file)

## ------------------------------------------------------------
## 3) ASSESSMENTS: BUILD VARIABLES ON THE FLY (from ass <- fread(...))
## ------------------------------------------------------------

## Normalize PID & year in assessments
ass[, PID  := normalize_pid(parcel_number)]
ass[, year := as.integer(year)]

# drop dupes by year X PID
ass = unique(ass, by = c("PID", "year"))

## Core taxable values
ass[, taxable_value := as.numeric(taxable_land + taxable_building)]

## Yearly changes
ass[order(year),
    change_taxable_value := taxable_value - data.table::shift(taxable_value),
    by = PID]

ass[order(year),
    pct_change_taxable_value := change_taxable_value / data.table::shift(taxable_value),
    by = PID]

## Range of changes per parcel
ass[, min_change_taxable_value := min(change_taxable_value, na.rm = TRUE), by = PID]
ass[, max_change_taxable_value := max(change_taxable_value, na.rm = TRUE), by = PID]
ass[, range_change_taxable_value := max_change_taxable_value - min_change_taxable_value]

## Clean pct_change & compute pct-change range
ass[!is.finite(pct_change_taxable_value), pct_change_taxable_value := NA_real_]
ass[, min_pct_change_taxable_value := min(pct_change_taxable_value, na.rm = TRUE), by = PID]
ass[, max_pct_change_taxable_value := max(pct_change_taxable_value, na.rm = TRUE), by = PID]
ass[, range_pct_change_taxable_value := max_pct_change_taxable_value - min_pct_change_taxable_value]

## Log taxable values and changes
ass[, log_taxable_value := log(taxable_value)]
ass[, log_taxable_value := fifelse(is.finite(log_taxable_value), log_taxable_value, NA_real_)]

ass[order(PID, year),
    change_log_taxable_value := log_taxable_value - data.table::shift(log_taxable_value),
    by = PID]
ass[, change_log_taxable_value := fifelse(is.finite(change_log_taxable_value), change_log_taxable_value, NA_real_)]

## Lags
ass[order(year),
    taxable_value_lag1 := data.table::shift(taxable_value),
    by = PID]
ass[order(year),
    taxable_value_lag2 := data.table::shift(taxable_value, 2),
    by = PID]
ass[order(year),
    change_taxable_value_lag1 := data.table::shift(change_taxable_value),
    by = PID]
ass[order(year),
    pct_change_taxable_value_lag1 := data.table::shift(pct_change_taxable_value),
    by = PID]

## Max abs log-change used to flag "crazy" parcels
ass[, max_abs_change_log_taxable_value := max(abs(change_log_taxable_value), na.rm = TRUE), by = PID]

## Set all *taxable* variables to NA for parcels with huge swings
tax_cols <- grep("taxable", names(ass), value = TRUE)
ass[max_abs_change_log_taxable_value >= 2,
    (tax_cols) := lapply(.SD, function(x) NA_real_),
    .SDcols = tax_cols]

## Drop range and min/max helper columns before merging
drop_cols <- c(
  "parcel_number",
  grep("range", names(ass), value = TRUE),
  grep("min_", names(ass),  value = TRUE),
  grep("max_", names(ass),  value = TRUE)
)
drop_cols <- intersect(drop_cols, names(ass))
if (length(drop_cols) > 0L) ass[, (drop_cols) := NULL]

## Merge assessments into bldg_panel (PID×year)
logf("Merging assessments...", log_file = log_file)
logf("  bldg_panel rows before merge: ", nrow(bldg_panel), log_file = log_file)
# ass has a couple dupes; drop them


bldg_panel <- merge(
  bldg_panel,
  ass,
  by    = c("PID", "year"),
  all.x = TRUE
)

logf("  bldg_panel rows after merge: ", nrow(bldg_panel), log_file = log_file)
assert_unique(bldg_panel, c("PID", "year"), "bldg_panel after assessments merge")
logf("  Assertion passed: (PID, year) unique after assessments merge", log_file = log_file)

## Per-unit assessment vars (using canonical units from occupancy)
if (!("total_units" %in% names(bldg_panel))) {
  stop("Expected 'total_units' in occupancy panel.")
}

bldg_panel[
  ,
  change_taxable_value_per_unit := fifelse(
    !is.na(change_taxable_value) & total_units > 0,
    change_taxable_value / total_units,
    NA_real_
  )
]
bldg_panel[
  ,
  log_taxable_value_per_unit := fifelse(
    !is.na(taxable_value) & taxable_value > 0 & total_units > 0,
    log(taxable_value / total_units),
    NA_real_
  )
]

## ------------------------------------------------------------
## 4) BASIC CLEANUP AND CORE VARIABLES
## ------------------------------------------------------------

## Units & occupancy (from occupancy script)
## occupied_units = renter-occupied units, capped at [0, total_units].
## Uses renter_occ directly (time-varying from InfoUSA num_households post-2010).
bldg_panel[, occupied_units := pmin(total_units, pmax(0, fifelse(is.na(renter_occ), 0, renter_occ)))]

## occupied_units_scaled: uses rental_occupancy_rate_scaled (benchmarked to
## 2010 Census mean=0.91) to produce a calibrated occupied-unit count.
## Falls back to raw occupied_units when the scaled rate is missing.
bldg_panel[, occupied_units_scaled := fifelse(
  !is.na(rental_occupancy_rate_scaled),
  rental_occupancy_rate_scaled * total_units,
  occupied_units
)]

## Rent variable (Altos)
bldg_panel[,med_rent := coalesce(
  med_rent_altos,
  med_eviction_rent,
)]
bldg_panel[
  ,
  log_med_rent := fifelse(
    !is.na(med_rent) & med_rent > 0,
    log(med_rent),
    NA_real_
  )
]



## ------------------------------------------------------------
## 5) MARKET & OWNER IDS
## ------------------------------------------------------------

## Market = zip-year (pm.zip already coalesced from ever_panel + parcels_clean)
market_zip_col <- if ("pm.zip" %in% names(bldg_panel)) {
  "pm.zip"
} else {
  NA_character_
}
if (is.na(market_zip_col)) {
  stop("No zip column found (pm.zip). Please adjust the market definition.")
}

bldg_panel[
  ,
  market_zip := str_pad(as.character(get(market_zip_col)), width = 5, side = "left", pad = "0")
]

## Unit-size bins (created here so we can support both:
##   - zip-year markets across all bins, and
##   - zip-year-bin markets for bin-specific demand specs)
bldg_panel[, num_units_bin := cut(
  total_units,
  breaks = c(-Inf, 1, 5, 20, 50, Inf),
  labels = c("1", "2-5", "6-20", "21-50", "51+"),
  include.lowest = TRUE
)]

## Two market IDs:
## 1) zip-year (all bins together) for share_zip_all_bins
## 2) zip-year-bin for share_zip_bin and bin-level estimation
bldg_panel[
  ,
  market_id_zip_year := paste(market_zip, year, sep = "_")
]

## Keep market_id as zip × year × unit-size bin for backward compatibility
bldg_panel[
  ,
  market_id := paste(market_zip, year, num_units_bin, sep = "_")
]

## Owner ID (from parcels_clean: owner_1)
if ("owner_1" %in% names(bldg_panel)) {
  bldg_panel[
    ,
    owner_mailing_clean := fifelse(
      !is.na(owner_1) & nzchar(owner_1),
      owner_1,
      paste0("UNK_", PID)
    )
  ]
} else if ("owner_mailing" %in% names(bldg_panel)) {
  bldg_panel[
    ,
    owner_mailing_clean := fifelse(
      !is.na(owner_mailing) & nzchar(owner_mailing),
      owner_mailing,
      paste0("UNK_", PID)
    )
  ]
} else {
  bldg_panel[
    ,
    owner_mailing_clean := paste0("UNK_", PID)
  ]
}

## ------------------------------------------------------------
## 6) PRODUCT SCOPE & BASIC FILTERS
## ------------------------------------------------------------

## Rental status from ever_rentals_panel columns
## Restrict to parcels that ever look like rentals
rental_pids <- unique(ever_panel$PID)
bldg_panel  <- bldg_panel[PID %in% rental_pids]

## Restrict to multi-unit rentals (tweak threshold as needed)
#bldg_panel  <- bldg_panel[total_units ]

## Drop pre-construction years if you have year_built
if ("year_built" %in% names(bldg_panel)) {
  bldg_panel <- bldg_panel[is.na(year_built) | year >= year_built]
}

## ------------------------------------------------------------
## 6b) FILINGS INTENSITY: RAW RATES + EMPIRICAL BAYES (POOLED ACROSS YEARS)
## ------------------------------------------------------------
## Target: a *building-level* long-run intensity (pre-COVID) pooled across years:
##   y_it | lambda_i ~ Poisson(exposure_it * lambda_i)
##   lambda_i ~ Gamma(alpha, beta)
## Sufficient stats over pre-period: Y_i = sum_t y_it, E_i = sum_t exposure_it
## Posterior mean: E[lambda_i | data] = (alpha + Y_i) / (beta + E_i)
##
## Two priors:
##   - city prior (one alpha,beta for all buildings)
##   - zip prior  (alpha_z,beta_z estimated within ZIP; fallback to city if thin)

if ("num_filings" %in% names(bldg_panel)) {

  fco <- function(x, val = 0) fifelse(is.na(x), val, x)

  # Ensure numeric + nonnegative
  bldg_panel[, num_filings := pmax(0, as.numeric(fco(num_filings, 0)))]

  # Denominators
  # NOTE: make sure occupied_units is truly a COUNT (units), not a rate.
  bldg_panel[, exposure_total := pmax(1, as.numeric(fco(total_units, 0)))]
  bldg_panel[, exposure_occ   := as.numeric(fco(occupied_units, NA_real_))]
  bldg_panel[, exposure_occ   := fifelse(is.finite(exposure_occ) & exposure_occ > 0, exposure_occ, NA_real_)]

  # RAW rates (yearly)
  bldg_panel[, filing_rate_total_raw := num_filings / exposure_total]
  bldg_panel[, filing_rate_occ_raw   := fifelse(!is.na(exposure_occ), num_filings / exposure_occ, NA_real_)]

  # Back-compat: main raw rate = occ-based
  bldg_panel[, filing_rate_raw := filing_rate_occ_raw]

  # ------------- Helper: method-of-moments Gamma prior on (Y,E) -------------
  est_gamma_prior_mom <- function(Y, E) {
    Y <- as.numeric(Y); E <- as.numeric(E)
    ok <- is.finite(Y) & is.finite(E) & E > 0
    if (!any(ok)) return(list(alpha = NA_real_, beta = NA_real_, mu = NA_real_))
    Y <- Y[ok]; E <- E[ok]

    S1 <- sum(Y, na.rm = TRUE)
    E1 <- sum(E, na.rm = TRUE)
    mu <- if (E1 > 0) S1 / E1 else 0

    # Mixture identity: E[Y(Y-1)] = E^2 * (mu^2 + mu/beta)
    yy1 <- sum(Y * pmax(Y - 1, 0), na.rm = TRUE)
    E2  <- sum(E^2, na.rm = TRUE)
    T   <- if (E2 > 0) yy1 / E2 else NA_real_

    eps <- 1e-12
    beta_hat <- if (!is.na(T) && (T > mu^2 + eps) && mu > 0) mu / (T - mu^2) else 1e8
    alpha_hat <- mu * beta_hat
    list(alpha = alpha_hat, beta = beta_hat, mu = mu)
  }

  # ------------- Build building-level sufficient stats over pre-COVID -------------
  # ZIP column for grouping: you already have market_zip created earlier.
  # Fallback to pm.zip if needed.
  zip_col <- if ("market_zip" %in% names(bldg_panel)) "market_zip" else if ("pm.zip" %in% names(bldg_panel)) "pm.zip" else NA_character_
  if (is.na(zip_col)) stop("No ZIP column found for EB ZIP prior (expected market_zip or pm.zip).")

  pre_dt <- bldg_panel[
    year <= 2019 &
      !is.na(exposure_occ) & exposure_occ > 0
  ]

  # One row per PID with pre-period sums
  eb_pid <- pre_dt[, .(
    eb_zip = {
      z <- get(zip_col)
      z <- z[which.max(!is.na(z))]  # grab first non-missing if any
      if (length(z) == 0) NA_character_ else as.character(z[1])
    },
    Y = sum(num_filings, na.rm = TRUE),
    E = sum(exposure_occ, na.rm = TRUE),
    n_years = .N
  ), by = PID]

  # ------------- CITY PRIOR -------------
  city_prior <- est_gamma_prior_mom(eb_pid$Y, eb_pid$E)
  alpha_city <- city_prior$alpha
  beta_city  <- city_prior$beta
  mu_city    <- city_prior$mu

  # ------------- ZIP PRIORS (with fallback to city) -------------
  # Safety thresholds: adjust as you like
  min_bldgs_per_zip <- 30
  min_total_E_zip   <- 500

  zip_stats <- eb_pid[!is.na(eb_zip) & E > 0,
                      .(
                        n_bldg = .N,
                        sumE   = sum(E, na.rm = TRUE),
                        sumY   = sum(Y, na.rm = TRUE)
                      ),
                      by = eb_zip
  ]

  # Estimate priors within each ZIP where there’s enough support
  zip_priors <- zip_stats[, {
    z <- eb_zip[1]
    dtz <- eb_pid[eb_zip == z & is.finite(Y) & is.finite(E) & E > 0]

    if (n_bldg[1] >= min_bldgs_per_zip && sumE[1] >= min_total_E_zip && nrow(dtz) > 0) {
      pr <- est_gamma_prior_mom(dtz$Y, dtz$E)
      list(alpha_zip = pr$alpha, beta_zip = pr$beta, mu_zip = pr$mu)
    } else {
      list(alpha_zip = NA_real_, beta_zip = NA_real_, mu_zip = NA_real_)
    }
  }, by = eb_zip]


  eb_pid <- merge(eb_pid, zip_priors, by = "eb_zip", all.x = TRUE)

  eb_pid[, alpha_use_zip := fifelse(is.finite(alpha_zip) & is.finite(beta_zip), alpha_zip, alpha_city)]
  eb_pid[,  beta_use_zip := fifelse(is.finite(alpha_zip) & is.finite(beta_zip),  beta_zip,  beta_city)]


  # ------------- Compute pooled EB building intensities -------------
  eb_pid[, filing_rate_eb_city_pre_covid := (alpha_city + Y) / (beta_city + E)]
  eb_pid[,  filing_rate_eb_zip_pre_covid := (alpha_use_zip + Y) / (beta_use_zip + E)]

  # Merge back into bldg_panel as building-level covariates
  bldg_panel <- merge(
    bldg_panel,
    eb_pid[, .(PID, filing_rate_eb_city_pre_covid, filing_rate_eb_zip_pre_covid)],
    by = "PID",
    all.x = TRUE
  )

  # Back-compat: keep filing_rate_eb as something sensible (choose city or zip)
  # For most uses, I’d keep BOTH and set filing_rate_eb to your preferred default.
  bldg_panel[, filing_rate_eb := filing_rate_eb_city_pre_covid]  # default
  bldg_panel[, filing_rate_eb_asinh := asinh(filing_rate_eb)]
  bldg_panel[, filing_rate_raw_asinh := asinh(filing_rate_raw)]

  # High-evict flags based on pooled EB (and raw)
  bldg_panel[, high_evict_raw := fifelse(!is.na(filing_rate_raw), filing_rate_raw >= 0.10, NA)]
  bldg_panel[, high_evict_eb_city := fifelse(!is.na(filing_rate_eb_city_pre_covid), filing_rate_eb_city_pre_covid >= 0.10, NA)]
  bldg_panel[,  high_evict_eb_zip := fifelse(!is.na(filing_rate_eb_zip_pre_covid),  filing_rate_eb_zip_pre_covid  >= 0.10, NA)]

  logf(
    "EB pooled across years (<=2019). City prior mu=", round(mu_city, 6),
    "; alpha=", round(alpha_city, 6),
    "; beta=", round(beta_city, 6),
    "; ZIP priors: min_bldg=", min_bldgs_per_zip, " min_sumE=", min_total_E_zip,
    log_file = log_file
  )

  # Optional: if you want to drop helper columns later:
  # bldg_panel[, c("exposure_total","exposure_occ") := NULL]
}


# ---- Minimal cleaning / derived covariates ----
bldg_panel[, year := as.integer(year)]
bldg_panel[, total_units := as.numeric(total_units)]

# Source label (keep your convention; infer if absent)
if (!("source" %in% names(bldg_panel))) {
  bldg_panel[, source := fifelse(rental_from_altos == 1, "altos",
                                 fifelse(rental_from_evict == 1, "evict", NA_character_))]
}

# Decade built
if ("year_built" %in% names(bldg_panel)) {
  bldg_panel[, year_blt_decade := floor(as.numeric(year_built) / 10) * 10]
  bldg_panel[, year_blt_decade := fifelse(!is.na(year_built) & year_built < 1900, "pre-1900",
                                          fifelse(is.na(year_blt_decade), "missing", as.character(year_blt_decade)))]
} else {
  bldg_panel[, year_blt_decade := "missing"]
}

# Unit bins (use existing if present; otherwise create a standard version)
if (!("num_units_bin" %in% names(bldg_panel))) {
  bldg_panel[, num_units_bin := cut(
    total_units,
    breaks = c(-Inf, 1, 2, 4, 9, 19, 49, Inf),
    labels = c("1", "2", "3-4", "5-9", "10-19", "20-49", "50+"),
    include.lowest = TRUE
  )]
}

# Optional: stories bin if available
if ("number_stories" %in% names(bldg_panel) && !("num_stories_bin" %in% names(bldg_panel))) {
  bldg_panel[, num_stories_bin := fcase(
    number_stories == 1, "1",
    number_stories <= 3, "2-3",
    number_stories <= 5, "4-5",
    number_stories <= 10, "6-10",
    number_stories > 10, "11+",
    default = "missing"
  )]
}

# ------------------------------------------------------------------
# Main eviction intensity for hedonics:
# Use PRE-COVID (<=2019) building-level EB intensity to avoid endogeneity
# to contemporaneous rent shocks and policy changes.
# ------------------------------------------------------------------
bldg_panel[, filing_rate_eb_pre_covid := {
  x <- filing_rate_eb[year <= 2019]
  if (length(x) == 0) NA_real_ else mean(x, na.rm = TRUE)
}, by = PID]

# Create interpretable bins (match your earlier breakpoints)
bldg_panel[, filing_rate_eb_cuts := cut(
  round(filing_rate_eb_pre_covid,2),
  breaks = c(-Inf, 0.01, 0.05, 0.10, 0.2, Inf),
  labels = c( "(0-1%]", "(1-5%]", "(5-10%]", "(10-20%]", "20%+"),
  include.lowest = TRUE
)]

# A continuous transform for curvature models
bldg_panel[, filing_rate_eb_pre_covid_asinh := asinh(filing_rate_eb_pre_covid)]


## ------------------------------------------------------------
## 7) MARKET SHARES & HHI MEASURES
## ------------------------------------------------------------

## -----------------------------------------------------------
## 7a) Shares in zip-year markets across all bins
##
## Why this exists:
## - This is the "all bins pooled" market share used by classic zip-year demand
##   definitions. Shares are comparable across unit-size bins within the same
##   zip-year because they use one common denominator.
## -----------------------------------------------------------

bldg_panel[
  ,
  `:=`(
    total_units_zip_all_bins         = sum(total_units, na.rm = TRUE),
    total_renters_zip_all_bins       = sum(renter_occ, na.rm = TRUE),
    total_occupied_units_zip_all_bins = sum(occupied_units_scaled, na.rm = TRUE)
  ),
  by = .(market_id_zip_year)
]

## share_zip_all_bins: occupied units over total units within zip-year.
## This leaves room for an outside good (vacancy and non-selected products).
bldg_panel[
  ,
  share_zip_all_bins := fifelse(
    total_units_zip_all_bins > 0,
    occupied_units_scaled / total_units_zip_all_bins,
    NA_real_
  )
]

## market_share_units uses occupied-units denominator and sums to 1 within
## zip-year. Keep for concentration summaries that require market shares that
## sum to one.
bldg_panel[
  ,
  market_share_units := fifelse(
    total_occupied_units_zip_all_bins > 0,
    occupied_units_scaled / total_occupied_units_zip_all_bins,
    NA_real_
  )
]

## Owner-market summary
owner_market <- bldg_panel[
  ,
  .(
    owner_units       = sum(occupied_units_scaled, na.rm = TRUE),
    owner_parcels     = .N,
    any_evict_owner   = any(fco(rental_from_evict, FALSE) | fco(num_filings, 0) > 0, na.rm = TRUE),
    any_altos_owner   = any(fco(rental_from_altos, FALSE), na.rm = TRUE),
    any_license_owner = any(fco(rental_from_license, FALSE), na.rm = TRUE)
  ),
  by = .(market_id, owner_mailing_clean)
]

market_hhi <- owner_market[
  ,
  {
    total_units <- sum(owner_units, na.rm = TRUE)
    share       <- if (total_units > 0) owner_units / total_units else rep(NA_real_, .N)

    total_parcels <- sum(owner_parcels, na.rm = TRUE)
    share_parcel  <- if (total_parcels > 0) owner_parcels / total_parcels else rep(NA_real_, .N)

    hhi_all   <- sum(share^2, na.rm = TRUE)
    hhi_par   <- sum(share_parcel^2, na.rm = TRUE)

    share_evict <- if (total_units > 0) ifelse(any_evict_owner, owner_units, 0) / total_units else rep(NA_real_, .N)
    hhi_evict   <- sum(share_evict^2, na.rm = TRUE)

    share_sorted <- sort(share, decreasing = TRUE)
    top1 <- share_sorted[1]
    top3 <- sum(head(share_sorted, 3))

    .(
      hhi_all_units   = hhi_all,
      hhi_parcels     = hhi_par,
      hhi_evict_units = hhi_evict,
      share_top1      = top1,
      share_top3      = top3,
      n_owners        = .N
    )
  },
  by = market_id
]

bldg_panel <- merge(
  bldg_panel,
  market_hhi,
  by    = "market_id",
  all.x = TRUE
)

## -----------------------------------------------------------
## 7b) Shares in zip-year-bin markets
##
## Why this exists:
## - Some specifications treat each unit-size bin as its own market. This share
##   uses bin-specific denominators to match that definition.
## -----------------------------------------------------------

bldg_panel[
  ,
  `:=`(
    total_units_zip_bin          = sum(total_units, na.rm = TRUE),
    total_renters_zip_bin        = sum(renter_occ, na.rm = TRUE),
    total_occupied_units_zip_bin = sum(occupied_units_scaled, na.rm = TRUE)
  ),
  by = .(market_id)
]

## Back-compat aliases used by older scripts.
bldg_panel[
  ,
  `:=`(
    total_units_market = total_units_zip_bin,
    total_renters_market = total_renters_zip_bin,
    total_occupied_units_market = total_occupied_units_zip_bin
  )
]

## share_zip_bin: occupied units over total units within zip-year-bin.
bldg_panel[
  ,
  share_zip_bin := fifelse(
    total_units_zip_bin > 0,
    occupied_units_scaled / total_units_zip_bin,
    NA_real_
  )
]

## share_units_zip_unit: legacy name, occupied-denominator version (sums to 1)
bldg_panel[
  ,
  share_units_zip_unit := fifelse(
    total_occupied_units_zip_bin > 0,
    occupied_units_scaled / total_occupied_units_zip_bin,
    NA_real_
  )
]

## -----------------------------------------------------------
## 7c) Share sanity checks
## -----------------------------------------------------------

## share_zip_all_bins should sum to <= 1 within each zip-year
share_zip_all_bins_sums <- bldg_panel[, .(share_sum = sum(share_zip_all_bins, na.rm = TRUE)), by = market_id_zip_year]
n_bad_zip_all <- share_zip_all_bins_sums[share_sum > 1 + 1e-8, .N]
if (n_bad_zip_all > 0) {
  logf("WARNING: ", n_bad_zip_all, " zip-year markets have share_zip_all_bins summing to > 1", log_file = log_file)
}
stopifnot("share_zip_all_bins sums > 1 in some zip-year markets - check occupancy vs total_units" =
            n_bad_zip_all == 0)
logf("  share_zip_all_bins: all ", nrow(share_zip_all_bins_sums), " zip-year markets sum to <= 1 - PASSED",
     log_file = log_file)

## share_zip_bin should sum to <= 1 within each (zip-year, bin)
share_bin_sums <- bldg_panel[, .(share_sum = sum(share_zip_bin, na.rm = TRUE)), by = market_id]
n_bad_bin <- share_bin_sums[share_sum > 1 + 1e-8, .N]
if (n_bad_bin > 0) {
  logf("WARNING: ", n_bad_bin, " (market, bin) cells have share_zip_bin > 1", log_file = log_file)
}
stopifnot("share_zip_bin sums > 1 in some (market, bin) cells" =
            n_bad_bin == 0)
logf("  share_zip_bin: all ", nrow(share_bin_sums), " (market, bin) cells sum to <= 1 - PASSED",
     log_file = log_file)

## All shares should be in [0, 1]
stopifnot("share_zip_all_bins out of [0,1]" =
            bldg_panel[!is.na(share_zip_all_bins), all(share_zip_all_bins >= 0 & share_zip_all_bins <= 1)])
stopifnot("share_zip_bin out of [0,1]" =
            bldg_panel[!is.na(share_zip_bin), all(share_zip_bin >= 0 & share_zip_bin <= 1)])
logf("  All shares in [0, 1] - PASSED", log_file = log_file)

## Diagnostic: share distribution summary
share_zip_all_vals <- bldg_panel[!is.na(share_zip_all_bins), share_zip_all_bins]
share_zip_bin_vals <- bldg_panel[!is.na(share_zip_bin), share_zip_bin]
logf(sprintf("  share_zip_all_bins dist: mean=%.6f, median=%.6f, p90=%.6f, max=%.6f",
             mean(share_zip_all_vals), median(share_zip_all_vals),
             quantile(share_zip_all_vals, 0.90), max(share_zip_all_vals)), log_file = log_file)
logf(sprintf("  share_zip_bin dist: mean=%.6f, median=%.6f, p90=%.6f, max=%.6f",
             mean(share_zip_bin_vals), median(share_zip_bin_vals),
             quantile(share_zip_bin_vals, 0.90), max(share_zip_bin_vals)), log_file = log_file)
logf(sprintf("  zip-year share sums (all bins): mean=%.4f, median=%.4f",
             mean(share_zip_all_bins_sums$share_sum), median(share_zip_all_bins_sums$share_sum)), log_file = log_file)
logf(sprintf("  zip-year-bin share sums: mean=%.4f, median=%.4f",
             mean(share_bin_sums$share_sum), median(share_bin_sums$share_sum)), log_file = log_file)

## ------------------------------------------------------------
## 8) SIMPLE BLP-STYLE INSTRUMENTS
## ------------------------------------------------------------

make_blp_instruments <- function(
    DT,
    market,
    product_id,
    firm,
    cont_vars = character(),
    prefix = "z"
) {
  stopifnot(data.table::is.data.table(DT))
  mkt <- market
  frm <- firm

  ## Counts
  DT[, `:=`(
    z_cnt_market = .N,
    z_cnt_firm   = .N
  ), by = c(mkt, frm)]

  DT[
    ,
    `:=`(
      z_cnt_others    = z_cnt_market - 1L,
      z_cnt_samefirm  = z_cnt_firm - 1L,
      z_cnt_otherfirm = z_cnt_market - z_cnt_firm
    )
  ]

  for (v in cont_vars) {
    if (!v %in% names(DT)) {
      warning("make_blp_instruments: skipping missing variable: ", v)
      next
    }

    nm_all <- paste(prefix, "sum_others",    v, sep = "_")
    nm_sf  <- paste(prefix, "sum_samefirm",  v, sep = "_")
    nm_of  <- paste(prefix, "sum_otherfirm", v, sep = "_")

    DT[, tmp_v := fco(get(v), 0)]

    DT[
      ,
      sum_market := sum(tmp_v),
      by = c(mkt)
    ]
    DT[
      ,
      sum_firm := sum(tmp_v),
      by = c(mkt, frm)
    ]
    DT[,(nm_all) := sum_market - tmp_v]
    DT[,(nm_sf)  := sum_firm   - tmp_v]
    DT[,(nm_of)  := sum_market - sum_firm]

  }

  DT[, c("tmp_v", "sum_market", "sum_firm") := NULL]
  invisible(DT)
}

blp_cont_vars <- c(
  "total_units",
  "log_med_rent",
  "hazardous_violation_count",
  "total_violations",
  "building_permit_count",
  "general_permit_count",
  "mechanical_permit_count",
  "zoning_permit_count",
  "plumbing_permit_count",
  "taxable_value",
  "change_taxable_value",
  "log_taxable_value",
  "change_taxable_value_per_unit",
  "log_taxable_value_per_unit"
)

bldg_panel <- make_blp_instruments(
  DT         = bldg_panel,
  market     = "market_id",
  product_id = "PID",
  firm       = "owner_mailing_clean",
  cont_vars  = blp_cont_vars,
  prefix     = "z"
)

## ------------------------------------------------------------
## 9) FILTER DOWN TO ANALYTIC SAMPLE
## ------------------------------------------------------------

analytic <- bldg_panel[
  !is.na(log_med_rent) &
    !is.na(market_share_units) &
    !is.na(total_units)
]

market_stats <- analytic[
  ,
  .(
    n_products = .N,
    n_owners   = uniqueN(owner_mailing_clean)
  ),
  by = market_id
]

good_markets <- market_stats[
  n_products >= 5 & n_owners >= 2,
  market_id
]

analytic <- analytic[market_id %in% good_markets]
analytic <- analytic[year >= 2006]  ## tweak time window if needed

## ------------------------------------------------------------
## 10) FINAL ASSERTIONS
## ------------------------------------------------------------
logf("Running final assertions...", log_file = log_file)

# Verify key columns exist
required_cols <- c("PID", "year", "market_id", "market_id_zip_year", "owner_mailing_clean", "log_med_rent",
                   "total_units", "market_share_units", "share_zip_all_bins", "share_zip_bin")
assert_has_cols(bldg_panel, required_cols, "bldg_panel")
logf("  Required columns present in bldg_panel", log_file = log_file)

assert_has_cols(analytic, required_cols, "analytic")
logf("  Required columns present in analytic", log_file = log_file)

# Verify uniqueness of final outputs
assert_unique(bldg_panel, c("PID", "year"), "bldg_panel final")
logf("  bldg_panel: (PID, year) unique - PASSED", log_file = log_file)

assert_unique(analytic, c("PID", "year"), "analytic final")
logf("  analytic: (PID, year) unique - PASSED", log_file = log_file)

## ------------------------------------------------------------
## 11) EXPORT
## ------------------------------------------------------------
logf("Writing outputs...", log_file = log_file)

out_bldg_panel <- p_product(cfg, "bldg_panel_blp")
out_analytic   <- p_product(cfg, "analytic_sample")

fwrite(bldg_panel, out_bldg_panel)
logf("  Wrote bldg_panel_blp: ", nrow(bldg_panel), " rows to ", out_bldg_panel, log_file = log_file)

fwrite(analytic, out_analytic)
logf("  Wrote analytic_sample: ", nrow(analytic), " rows to ", out_analytic, log_file = log_file)

logf("  Unique markets: ", analytic[, uniqueN(market_id)], log_file = log_file)
logf("  Unique owners: ", analytic[, uniqueN(owner_mailing_clean)], log_file = log_file)
logf("=== Finished make-analytic-sample.R ===", log_file = log_file)
