## ============================================================
## make-analytic-sample.R
## ============================================================
## Purpose: Build the final analytic sample from ever-rentals panel
##
## Inputs (from config):
##   - ever_rentals_panel (from make-rent-panel.R)
##   - parcel_occupancy_panel (from make-occupancy-vars.R; total_units, renter_occ)
##   - building_data (from make-building-data.R; violations, permits, complaints)
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
path_assess_csv   <- p_input(cfg, "assessments")
path_parcels_clean <- p_product(cfg, "parcels_clean")

ever_panel    <- fread(path_ever_panel)
logf("  ever_rentals_panel: ", nrow(ever_panel), " rows", log_file = log_file)

occ_panel     <- fread(path_occ_panel)
logf("  parcel_occupancy_panel: ", nrow(occ_panel), " rows", log_file = log_file)

events_panel  <- fread(path_events_panel)
logf("  building_data (events): ", nrow(events_panel), " rows", log_file = log_file)

ass <- fread(path_assess_csv)
logf("  assessments: ", nrow(ass), " rows", log_file = log_file)

parcels_meta <- fread(path_parcels_clean,
                      select = c("parcel_number", "owner_1", "mailing_street", "pm.zip"))
parcels_meta[, PID := normalize_pid(parcel_number)]
parcels_meta[, parcel_number := NULL]
setnames(parcels_meta, "pm.zip", "pm.zip_parcels")
parcels_meta <- unique(parcels_meta, by = "PID")
logf("  parcels_clean (meta): ", nrow(parcels_meta), " rows", log_file = log_file)

setDT(ever_panel)
setDT(occ_panel)
setDT(events_panel)
setDT(ass)

## Normalize PIDs
ever_panel[,   PID := normalize_pid(PID)]
occ_panel[,    PID := normalize_pid(PID)]
events_panel[, PID := normalize_pid(parcel_number)]
events_panel[, parcel_number := NULL]
if ("period" %in% names(events_panel)) events_panel[, period := NULL]

## ------------------------------------------------------------
## 2) BUILD BASE PANEL (PID × YEAR) FROM OCC + EVER + EVENTS
## ------------------------------------------------------------
bldg_panel <- copy(occ_panel)

## Merge parcels_clean metadata (owner + zip fallback)
bldg_panel <- merge(bldg_panel, parcels_meta, by = "PID", all.x = TRUE)

## ever_rentals_panel: rents, rental flags, filings, parcel chars
logf("Merging ever_rentals_panel...", log_file = log_file)
logf("  bldg_panel rows before merge: ", nrow(bldg_panel), log_file = log_file)

bldg_panel <- merge(
  bldg_panel,
  ever_panel,
  by    = c("PID", "year"),
  all.x = TRUE,
  suffixes = c("", "_ever")
)

logf("  bldg_panel rows after merge: ", nrow(bldg_panel), log_file = log_file)
assert_unique(bldg_panel, c("PID", "year"), "bldg_panel after ever_panel merge")
logf("  Assertion passed: (PID, year) unique after ever_panel merge", log_file = log_file)

## Coalesce pm.zip: prefer ever_panel's pm.zip, fall back to parcels_clean
bldg_panel[, pm.zip := coalesce(
  str_remove(as.character(pm.zip), "^_") %>% str_pad(5, "left", "0"),
  str_remove(as.character(pm.zip_parcels), "^_") %>% str_pad(5, "left", "0")
)]
bldg_panel[pm.zip %in% c("   NA", "NA", "00000"), pm.zip := NA_character_]
bldg_panel[, pm.zip_parcels := NULL]

## Single merged events file: violations + permits + complaints (PID×year)
logf("Merging events_panel...", log_file = log_file)
logf("  bldg_panel rows before merge: ", nrow(bldg_panel), log_file = log_file)

bldg_panel <- merge(
  bldg_panel,
  events_panel,
  by    = c("PID", "year"),
  all.x = TRUE
)

logf("  bldg_panel rows after merge: ", nrow(bldg_panel), log_file = log_file)
assert_unique(bldg_panel, c("PID", "year"), "bldg_panel after events_panel merge")
logf("  Assertion passed: (PID, year) unique after events_panel merge", log_file = log_file)

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
bldg_panel[
  ,
  occupied_units := fifelse(
    !is.na(occupancy_rate),
    total_units * occupancy_rate,
    total_units
  )
]

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
bldg_panel[
  ,
  market_id := paste(market_zip, year, sep = "_")
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
bldg_panel[
  ,
  is_rental_year := fifelse(
    fco(ever_rental_any_year, FALSE) |
      fco(rental_from_altos, FALSE)  |
      fco(rental_from_license, FALSE) |
      fco(rental_from_evict, FALSE),
    TRUE, FALSE
  )
]

## Restrict to parcels that ever look like rentals
rental_pids <- bldg_panel[is_rental_year == TRUE, unique(PID)]
bldg_panel  <- bldg_panel[PID %in% rental_pids]

## Restrict to multi-unit rentals (tweak threshold as needed)
#bldg_panel  <- bldg_panel[total_units ]

## Drop pre-construction years if you have year_built
if ("year_built" %in% names(bldg_panel)) {
  bldg_panel <- bldg_panel[is.na(year_built) | year >= year_built]
}

## Simple filing rate + high_evict (optional)
if ("num_filings" %in% names(bldg_panel)) {
  bldg_panel[
    ,
    filing_rate := fco(num_filings, 0) / pmax(total_units, 1)
  ]
  bldg_panel[
    ,
    high_evict := filing_rate >= 0.1
  ]
}

## ------------------------------------------------------------
## 7) MARKET SHARES & HHI MEASURES
## ------------------------------------------------------------

## Market totals
bldg_panel[
  ,
  total_occupied_units_market := sum(occupied_units, na.rm = TRUE),
  by = .(market_id)
]
bldg_panel[
  ,
  market_share_units := fifelse(
    total_occupied_units_market > 0,
    occupied_units / total_occupied_units_market,
    NA_real_
  )
]

## Owner-market summary
owner_market <- bldg_panel[
  ,
  .(
    owner_units       = sum(occupied_units, na.rm = TRUE),
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

## Unit-bin specific share (optional)
bldg_panel[
  ,
  num_units_bin := cut(
    total_units,
    breaks = c(-Inf, 1, 5, 20, 50, Inf),
    labels = c("1", "2-5", "6-20", "21-50", "51+"),
    include.lowest = TRUE
  )
]

bldg_panel[
  ,
  total_occupied_units_bin := sum(occupied_units, na.rm = TRUE),
  by = .(market_id, num_units_bin)
]
bldg_panel[
  ,
  share_units_zip_unit := fifelse(
    total_occupied_units_bin > 0,
    occupied_units / total_occupied_units_bin,
    NA_real_
  )
]

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
required_cols <- c("PID", "year", "market_id", "owner_mailing_clean", "log_med_rent",
                   "total_units", "market_share_units")
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

