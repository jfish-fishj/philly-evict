# r/analyze-nhood-portfolio-transitions.R
#
# Correlates net unit acquisitions by landlord type (high/low/single evictors)
# with neighborhood change trajectories (PCA gentrification index and individual
# components: income, crime, home values, demographics, business establishments).
#
# Filer classification mirrors analyze-transfer-evictions-unified.R exactly:
#   acq_rate = sum(annual_filings) / sum(annual_units) across OTHER buildings
#              pre-acquisition (unit-weighted, using EB filing rate × modal units)
#   Small portfolio: < ACQ_SMALL_PORTFOLIO_MIN_OTHER_UNITS (10) other units
#   High-filer: acq_rate > ACQ_HIGH_FILER_THRESHOLD (0.05)
#   Single-purchase: n_other <= 0 (no other buildings in portfolio at acquisition)
#
# Net acquisitions per geography × year × filer_type = acquisitions - dispositions.
# Cumulative net is compared to neighborhood change trajectories over the same window.
#
# Outputs: output/nhood_portfolio/
#   net_acq_tract_year.csv         -- tract x year x filer_type (annual)
#   net_acq_zip_year.csv           -- ZIP x year x filer_type (annual)
#   tract_portfolio_vs_nhood.csv   -- long-run cross-section: 2010-2019
#   zip_portfolio_vs_nhood.csv     -- long-run cross-section: 2006-2019
#   panel_reg_coefs.csv            -- panel regression coefficients
#   figs/                          -- scatter and trend plots

suppressPackageStartupMessages({
  library(data.table)
  library(fixest)
  library(ggplot2)
  library(scales)
  library(patchwork)
  library(sf)
})

source("r/config.R")
source("r/helper-functions.R")
cfg <- read_config("config.yml")

OUT_DIR  <- p_out(cfg, "nhood_portfolio")
FIG_DIR  <- file.path(OUT_DIR, "figs")
dir.create(FIG_DIR, recursive = TRUE, showWarnings = FALSE)
LOG_FILE <- file.path(OUT_DIR, "nhood-portfolio-transitions.log")
if (file.exists(LOG_FILE)) file.remove(LOG_FILE)

ANALYSIS_YEAR_MIN             <- 2006L
ANALYSIS_YEAR_MAX             <- 2019L
ACQ_HIGH_FILER_THRESHOLD      <- 0.05   # filings per unit per year (matches unified script)
ACQ_SMALL_PORTFOLIO_MIN_OTHER <- 10L    # other-unit threshold for small-portfolio (matches unified)
MIN_POP                       <- 200L   # drop low-population geographies
MAX_CRIME_RATE                <- 500L   # drop non-residential geographies

FILER_LEVELS <- c("High-filer", "Low-filer", "Small portfolio", "Single-purchase")
PALETTE <- c(
  "High-filer"      = "#D1495B",
  "Low-filer"       = "#2A9D8F",
  "Small portfolio" = "#E9C46A",
  "Single-purchase" = "#264653"
)

logf("=== analyze-nhood-portfolio-transitions.R ===", log_file = LOG_FILE)

# ============================================================
# SECTION 1: Load data
# ============================================================
logf("SECTION 1: Load data", log_file = LOG_FILE)

rtt <- fread(p_product(cfg, "rtt_clean"))
rtt[, PID  := normalize_pid(PID)]
rtt[, year := as.integer(year)]
rtt <- rtt[!is.na(year) & year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX]
# zip_code is stored as 9-digit ZIP+4 without hyphen; first 5 = ZIP
rtt[, zip5 := substr(sprintf("%09d", as.integer(zip_code)), 1L, 5L)]
rtt[, zip5 := fifelse(grepl("^[0-9]{5}$", zip5) & zip5 != "00000", zip5, NA_character_)]
logf("  rtt_clean [", ANALYSIS_YEAR_MIN, ",", ANALYSIS_YEAR_MAX, "]: ",
     nrow(rtt), " rows, ", rtt[, uniqueN(PID)], " PIDs", log_file = LOG_FILE)

bldg_cols <- c("PID", "year", "GEOID", "pm.zip", "total_units",
                "filing_rate_eb_pre_covid", "num_filings",
                "infousa_find_mean_k", "med_rent")
bldg_cols <- intersect(bldg_cols, fread(p_product(cfg, "bldg_panel_blp"), nrows = 0L) |> names())
bldg <- fread(p_product(cfg, "bldg_panel_blp"), select = bldg_cols)
bldg[, PID  := normalize_pid(PID)]
bldg[, year := as.integer(year)]
logf("  bldg_panel_blp: ", nrow(bldg), " rows, ", bldg[, uniqueN(PID)], " PIDs",
     log_file = LOG_FILE)

xwalk_cong <- fread(p_product(cfg, "xwalk_pid_conglomerate"))
xwalk_cong[, PID  := normalize_pid(PID)]
xwalk_cong[, year := as.integer(year)]
logf("  xwalk_pid_conglomerate: ", nrow(xwalk_cong), " rows", log_file = LOG_FILE)

# Load owner_category for PHA exclusion in Section 11
xwalk_entity_cat <- fread(p_product(cfg, "xwalk_pid_entity"),
                          select = c("PID", "year", "owner_category"))
xwalk_entity_cat[, PID := normalize_pid(PID)]
xwalk_entity_cat[, year := as.integer(year)]

zip_dt   <- fread(p_product(cfg, "nhood_zip_year_panel"))
tract_dt <- fread(p_product(cfg, "nhood_tract_year_panel"))
# fread reads 11-digit FIPS codes as integer64; coerce to character everywhere
tract_dt[, tract_fips := as.character(tract_fips)]
zip_dt[,   zip        := as.character(zip)]
logf("  nhood_zip_year_panel:   ", nrow(zip_dt),   " rows", log_file = LOG_FILE)
logf("  nhood_tract_year_panel: ", nrow(tract_dt), " rows", log_file = LOG_FILE)

# ============================================================
# SECTION 2: Geography crosswalk (modal PID → tract, PID → ZIP)
# ============================================================
logf("SECTION 2: Geography crosswalk", log_file = LOG_FILE)

bldg[, tract_fips := fifelse(
  !is.na(GEOID) & nchar(as.character(GEOID)) >= 11L,
  substr(as.character(GEOID), 1L, 11L), NA_character_
)]
bldg[, zip5_bldg := sub("^_+", "", as.character(`pm.zip`))]
bldg[, zip5_bldg := fifelse(grepl("^[0-9]{5}$", zip5_bldg), zip5_bldg, NA_character_)]

mode_char <- function(x) {
  x <- x[!is.na(x) & nchar(trimws(x)) > 0L]
  if (length(x) == 0L) return(NA_character_)
  names(sort(table(x), decreasing = TRUE))[1L]
}
pid_geo <- bldg[, .(
  tract_fips  = first(tract_fips),
  zip5        = first(zip5_bldg),
  units_modal = as.integer(first(total_units))
), by = PID]
logf("  PID geo xwalk: ", nrow(pid_geo), " PIDs | ",
     pid_geo[!is.na(tract_fips), .N], " with tract | ",
     pid_geo[!is.na(zip5), .N], " with ZIP", log_file = LOG_FILE)

# ============================================================
# SECTION 3: Classify transfers by buyer/seller filer type
#             (unit-weighted, mirrors analyze-transfer-evictions-unified.R)
# ============================================================
logf("SECTION 3: Classify transfers by filer type", log_file = LOG_FILE)

# Full-period LOO classification — conglomerate type as a structural attribute.
# Uses actual num_filings (not EB proxy) over the full analysis panel.
# Solo → "Single-purchase"; Small portfolio → "Small portfolio";
# Low/High-evicting portfolio → "Low-filer" / "High-filer".
n_analysis_years <- ANALYSIS_YEAR_MAX - ANALYSIS_YEAR_MIN + 1L

bldg_loo <- merge(
  bldg[!is.na(total_units) & total_units > 0,
       .(PID, year,
         num_filings = fifelse(is.na(num_filings), 0L, as.integer(num_filings)),
         total_units)],
  xwalk_cong[, .(PID, year, conglomerate_id)],
  by = c("PID", "year")
)
assert_has_cols(bldg_loo, c("PID", "year", "conglomerate_id", "num_filings", "total_units"),
                "bldg_loo for compute_loo_filing_type")

loo_type_nhood <- compute_loo_filing_type(
  pid_yr_dt            = bldg_loo,
  loo_min_unit_years   = ACQ_SMALL_PORTFOLIO_MIN_OTHER * n_analysis_years,
  loo_filing_threshold = ACQ_HIGH_FILER_THRESHOLD
)

loo_type_nhood[, filer_type := fcase(
  portfolio_evict_group_cong == "High-evicting portfolio", "High-filer",
  portfolio_evict_group_cong == "Low-evicting portfolio",  "Low-filer",
  portfolio_evict_group_cong == "Small portfolio",         "Small portfolio",
  default                                                = "Single-purchase"
)]
loo_type_nhood[, filer_type := factor(filer_type, levels = FILER_LEVELS)]

logf("  LOO filer type distribution (full-period ", ANALYSIS_YEAR_MIN, "-", ANALYSIS_YEAR_MAX, "):",
     log_file = LOG_FILE)
print(loo_type_nhood[, .N, keyby = filer_type])

# --- BUYER classification ---
# Buyer = conglomerate that owns PID at year t (post-transfer)
rtt_buy_raw <- merge(
  rtt[, .(PID, year, zip5)],
  xwalk_cong[, .(PID, year, conglomerate_id)],
  by = c("PID", "year"), all.x = TRUE
)
buy_classified <- merge(
  rtt_buy_raw,
  loo_type_nhood[, .(PID, conglomerate_id, filer_type)],
  by = c("PID", "conglomerate_id"), all.x = TRUE
)
buy_classified[is.na(filer_type), filer_type := factor("Single-purchase", levels = FILER_LEVELS)]
logf("  Buyer filer type distribution:", log_file = LOG_FILE)
print(buy_classified[, .N, keyby = filer_type])

# --- SELLER classification ---
# Seller = conglomerate that owned PID in year t-1 (pre-transfer)
rtt_sell_raw <- merge(
  rtt[, .(PID, year, zip5)],
  xwalk_cong[, .(PID, year_next = year + 1L, conglomerate_id)],
  by.x = c("PID", "year"), by.y = c("PID", "year_next"),
  all.x = TRUE
)
sell_classified <- merge(
  rtt_sell_raw,
  loo_type_nhood[, .(PID, conglomerate_id, filer_type)],
  by = c("PID", "conglomerate_id"), all.x = TRUE
)
sell_classified[is.na(filer_type), filer_type := factor("Single-purchase", levels = FILER_LEVELS)]
logf("  Seller filer type distribution:", log_file = LOG_FILE)
print(sell_classified[, .N, keyby = filer_type])

# ============================================================
# SECTION 4: Net acquisitions by geography × year × filer_type
# ============================================================
logf("SECTION 4: Net acquisitions", log_file = LOG_FILE)

# Attach geography (tract / ZIP) using bldg modal values; prefer RTT zip5
geo_xwalk <- pid_geo[, .(PID, tract_fips, zip5_bldg = zip5, units_modal)]

enrich_geo <- function(dt) {
  dt <- merge(dt, geo_xwalk, by = "PID", all.x = TRUE)
  dt[, zip_final := fifelse(!is.na(zip5) & nchar(zip5) == 5L, zip5, zip5_bldg)]
  dt[, n_units := pmax(fifelse(is.na(units_modal) | units_modal < 1L, 1L, units_modal), 1L)]
  dt
}

buy_geo  <- enrich_geo(buy_classified)
sell_geo <- enrich_geo(sell_classified)

# Aggregate net acquisitions by geography × year × filer_type
make_net <- function(buy_dt, sell_dt, geo_col) {
  buy_sub  <- buy_dt[ !is.na(filer_type) & !is.na(get(geo_col))]
  sell_sub <- sell_dt[!is.na(filer_type) & !is.na(get(geo_col))]

  acq <- buy_sub[ , .(n_acq  = .N, units_acq  = sum(n_units, na.rm = TRUE)),
                  by = c(geo_col, "year", "filer_type")]
  dis <- sell_sub[, .(n_sell = .N, units_sell = sum(n_units, na.rm = TRUE)),
                  by = c(geo_col, "year", "filer_type")]

  # Full cross of geographies × years × types so zeros appear
  geos  <- unique(c(buy_sub[[geo_col]],  sell_sub[[geo_col]]))
  yrs   <- ANALYSIS_YEAR_MIN:ANALYSIS_YEAR_MAX
  types <- FILER_LEVELS
  grid  <- CJ(geo = geos, year = yrs, filer_type = types)
  setnames(grid, "geo", geo_col)
  grid[, filer_type := factor(filer_type, levels = FILER_LEVELS)]

  net <- merge(grid, acq, by = c(geo_col, "year", "filer_type"), all.x = TRUE)
  net <- merge(net,  dis, by = c(geo_col, "year", "filer_type"), all.x = TRUE)
  net[is.na(n_acq),   n_acq   := 0L]
  net[is.na(units_acq),  units_acq  := 0L]
  net[is.na(n_sell),  n_sell  := 0L]
  net[is.na(units_sell), units_sell := 0L]
  net[, net_transfers := n_acq   - n_sell]
  net[, net_units     := units_acq - units_sell]
  net
}

net_tract <- make_net(
  buy_geo[ , c("PID", "year", "tract_fips", "filer_type", "n_units"), with = FALSE],
  sell_geo[, c("PID", "year", "tract_fips", "filer_type", "n_units"), with = FALSE],
  "tract_fips"
)
net_zip <- make_net(
  buy_geo[ , .(PID, year, zip5 = zip_final, filer_type, n_units)],
  sell_geo[, .(PID, year, zip5 = zip_final, filer_type, n_units)],
  "zip5"
)

net_tract[, tract_fips := as.character(tract_fips)]
net_zip[,   zip5       := as.character(zip5)]
logf("  Annual net_acq (tract): ", nrow(net_tract), " rows", log_file = LOG_FILE)
logf("  Annual net_acq (ZIP):   ", nrow(net_zip),   " rows", log_file = LOG_FILE)

# make total units by zip-year and tract-year
zip_year <- bldg[,list(
  total_units = sum(total_units, na.rm = TRUE)
), by = .(zip5 = pm.zip, year)]

tract_year <- bldg[,list(
  total_units = sum(total_units, na.rm = TRUE)
), by = .(tract_fips = tract_fips, year)]

# merge
net_zip = merge(net_zip, zip_year, by.x = c("zip5", "year"), by.y = c("zip5", "year"), all.x = TRUE)
net_tract = merge(net_tract, tract_year, by.x = c("tract_fips", "year"), by.y = c("tract_fips", "year"), all.x = TRUE)

fwrite(net_tract, file.path(OUT_DIR, "net_acq_tract_year.csv"))
fwrite(net_zip,   file.path(OUT_DIR, "net_acq_zip_year.csv"))

# Cumulative net and gross acquisitions over study windows
cum_net <- function(dt, geo_col, year_from, year_to) {
  dt[year >= year_from & year <= year_to, .(
    total_units_mean = mean(total_units, na.rm = TRUE),
    cum_net_transfers = sum(net_transfers, na.rm = TRUE),
    cum_net_units     = sum(net_units,     na.rm = TRUE),
    cum_acq_units     = sum(units_acq,     na.rm = TRUE),   # gross acquisitions
    cum_sell_units    = sum(units_sell,    na.rm = TRUE),   # gross dispositions
    n_years_active    = sum(abs(net_transfers) > 0L)
  ), by = c(geo_col, "filer_type")]
}

cum_tract_1019 <- cum_net(net_tract, "tract_fips", 2010L, 2019L)
cum_tract_0619 <- cum_net(net_tract, "tract_fips", 2006L, 2019L)
cum_zip_0619   <- cum_net(net_zip,   "zip5",       2006L, 2019L)

# ============================================================
# SECTION 5: Neighborhood PCA
# ============================================================
logf("SECTION 5: Neighborhood PCA", log_file = LOG_FILE)

# Filter low-population and non-residential geographies (mirrors nhood-change.qmd)
filter_geo <- function(dt, unit_col, pop_var = "nanda_totpop",
                       min_pop = MIN_POP, max_crime_thresh = MAX_CRIME_RATE) {
  max_pop_dt   <- dt[, .(max_pop   = max(get(pop_var),   na.rm = TRUE)), by = c(unit_col)]
  max_crime_dt <- dt[!is.na(rate_total),
                     .(max_crime = max(rate_total, na.rm = TRUE)), by = c(unit_col)]
  dt <- dt[max_pop_dt[  max_pop   >= min_pop,          c(unit_col), with = FALSE], on = unit_col]
  dt <- dt[max_crime_dt[max_crime <= max_crime_thresh,  c(unit_col), with = FALSE], on = unit_col]
  dt
}
tract_dt <- filter_geo(tract_dt, "tract_fips")
zip_dt   <- filter_geo(zip_dt,   "zip")
logf("  After population/crime filter — tract: ", tract_dt[, uniqueN(tract_fips)],
     "  ZIP: ", zip_dt[, uniqueN(zip)], log_file = LOG_FILE)

# --- TRACT PCA: 2010 vs 2019 ---
tract_10 <- tract_dt[year == 2010L]
tract_19 <- tract_dt[year == 2019L]
tract_lr <- merge(
  tract_10[, .(tract_fips,
               income_10   = acs_med_hh_income, rent_10    = acs_med_rent,
               homeval_10  = acs_med_home_value, pct_blk_10 = acs_pct_black,
               pov_10      = acs_poverty_rate,   crime_10   = rate_total,
               nanda_10    = nanda_total_establishments, pop_10 = nanda_totpop)],
  tract_19[, .(tract_fips,
               income_19   = acs_med_hh_income, rent_19    = acs_med_rent,
               homeval_19  = acs_med_home_value, pct_blk_19 = acs_pct_black,
               pov_19      = acs_poverty_rate,   crime_19   = rate_total,
               nanda_19    = nanda_total_establishments)],
  by = "tract_fips"
)
tract_lr[, `:=`(
  d_income    = log(pmax(income_19,  1)) - log(pmax(income_10,  1)),
  d_rent      = log(pmax(rent_19,    1)) - log(pmax(rent_10,    1)),
  d_home_val  = log(pmax(homeval_19, 1)) - log(pmax(homeval_10, 1)),
  d_pct_black = pct_blk_19 - pct_blk_10,
  d_poverty   = pov_19     - pov_10,
  d_crime     = crime_19   - crime_10,
  d_nanda     = log1p(nanda_19) - log1p(nanda_10)
)]

pca_tract_vars <- c("d_income", "d_rent", "d_home_val", "d_pct_black",
                    "d_poverty", "d_crime", "d_nanda")
pca_tr_input <- tract_lr[complete.cases(tract_lr[, ..pca_tract_vars])]
pca_tr       <- prcomp(pca_tr_input[, ..pca_tract_vars], scale. = TRUE)

pc1_sign_tr <- sign(pca_tr$rotation["d_income", "PC1"])
pca_tr_input[, pc1_gent := pca_tr$x[, 1] * pc1_sign_tr]
pca_tr_input[, pc2      := pca_tr$x[, 2]]
var_exp_tr <- pca_tr$sdev^2 / sum(pca_tr$sdev^2)
logf("  Tract PCA: PC1 = ", round(100 * var_exp_tr[1], 1), "% variance  PC2 = ",
     round(100 * var_exp_tr[2], 1), "%", log_file = LOG_FILE)
logf("  PC1 loadings (signed → positive = gentrifying):", log_file = LOG_FILE)
print(round(pca_tr$rotation[, 1] * pc1_sign_tr, 3))

# --- Gentrification status classification ---
# Gentrifiable = below-median income in 2010 (within Philadelphia)
# Gentrified   = gentrifiable AND percentile rank rose ≥ GENT_RANK_THRESH pp (2010→2019)
# Did not gentrify = gentrifiable but rank change < threshold
# Not gentrifiable = above-median income in 2010
GENT_RANK_THRESH <- 10L  # percentage-point improvement in city-wide income rank

income_median_10 <- median(pca_tr_input$income_10, na.rm = TRUE)

# City-wide percentile rank (0–100) using all tracts in pca_tr_input
pca_tr_input[, rank_pct_10 := frank(income_10, na.last = "keep", ties.method = "average") /
               sum(!is.na(income_10)) * 100]
pca_tr_input[, rank_pct_19 := frank(income_19, na.last = "keep", ties.method = "average") /
               sum(!is.na(income_19)) * 100]
pca_tr_input[, d_rank_pct := rank_pct_19 - rank_pct_10]
pca_tr_input[, gentrifiable := !is.na(income_10) & income_10 < income_median_10]

pca_tr_input[, gent_status := fcase(
  !gentrifiable,                                              "Not gentrifiable",
  gentrifiable & !is.na(d_rank_pct) & d_rank_pct >= GENT_RANK_THRESH, "Gentrified",
  default                                                   = "Did not gentrify"
)]
pca_tr_input[, gent_status := factor(
  gent_status, levels = c("Gentrified", "Did not gentrify", "Not gentrifiable")
)]

logf("  Gentrification status (threshold = +", GENT_RANK_THRESH, " pct-pt rank change):",
     log_file = LOG_FILE)
print(pca_tr_input[, .N, keyby = gent_status])

# Save lookup for downstream scripts (transfer-evictions-unified.R)
gent_lookup <- pca_tr_input[, .(tract_fips, gent_status, gentrifiable,
                                  rank_pct_10, rank_pct_19, d_rank_pct,
                                  income_10, income_19)]
fwrite(gent_lookup, file.path(OUT_DIR, "tract_gent_status.csv"))
logf("  Saved tract_gent_status.csv", log_file = LOG_FILE)

# --- ZIP PCA: 2006 vs {irs_max_year} ---
irs_end_yr <- max(zip_dt$year[!is.na(zip_dt$avg_hh)], na.rm = TRUE)
zip_06     <- zip_dt[year == 2006L]
zip_end    <- zip_dt[year == irs_end_yr]
zip_19     <- zip_dt[year == 2019L]

zip_lr <- merge(
  zip_06[,  .(zip, income_06 = avg_hh, hp_06    = mean_home_price,
               crime_06  = rate_total, nanda_06 = nanda_total_establishments)],
  zip_end[, .(zip, income_end = avg_hh, hp_end  = mean_home_price)],
  by = "zip"
)
zip_lr <- merge(
  zip_lr,
  zip_19[, .(zip, crime_19 = rate_total, nanda_19 = nanda_total_establishments)],
  by = "zip", all.x = TRUE
)

# Optionally attach ACS % Black at ZIP level
if ("acs_pct_black" %in% names(zip_dt)) {
  z10_blk <- zip_dt[year == 2010L & !is.na(acs_pct_black), .(zip, pct_blk_10 = acs_pct_black)]
  z19_blk <- zip_dt[year == 2019L & !is.na(acs_pct_black), .(zip, pct_blk_19 = acs_pct_black)]
  zip_lr  <- merge(zip_lr, z10_blk, by = "zip", all.x = TRUE)
  zip_lr  <- merge(zip_lr, z19_blk, by = "zip", all.x = TRUE)
  zip_lr[, d_pct_black := pct_blk_19 - pct_blk_10]
} else {
  zip_lr[, d_pct_black := NA_real_]
}

zip_lr[, `:=`(
  d_log_income = log(pmax(income_end, 1)) - log(pmax(income_06, 1)),
  d_log_hp     = log(pmax(hp_end,     1)) - log(pmax(hp_06,     1)),
  d_crime      = crime_19  - crime_06,
  d_nanda      = log1p(nanda_19) - log1p(nanda_06)
)]

pca_zip_all  <- c("d_log_income", "d_log_hp", "d_crime", "d_pct_black", "d_nanda")
pca_zip_vars <- pca_zip_all[sapply(pca_zip_all, function(v)
  v %in% names(zip_lr) && sum(!is.na(zip_lr[[v]])) >= 5L)]
logf("  ZIP PCA vars: ", paste(pca_zip_vars, collapse = ", "), log_file = LOG_FILE)

pca_zip_input <- zip_lr[complete.cases(zip_lr[, ..pca_zip_vars])]
if (nrow(pca_zip_input) >= 5L) {
  pca_zip     <- prcomp(pca_zip_input[, ..pca_zip_vars], scale. = TRUE)
  pc1_sign_zip <- sign(pca_zip$rotation["d_log_income", "PC1"])
  pca_zip_input[, pc1_gent := pca_zip$x[, 1] * pc1_sign_zip]
  var_exp_zip <- pca_zip$sdev^2 / sum(pca_zip$sdev^2)
  logf("  ZIP PCA: PC1 = ", round(100 * var_exp_zip[1], 1), "% variance", log_file = LOG_FILE)
  logf("  ZIP PC1 loadings (signed → positive = gentrifying):", log_file = LOG_FILE)
  print(round(pca_zip$rotation[, 1] * pc1_sign_zip, 3))
} else {
  logf("  WARNING: Insufficient ZIP data for PCA (n=", nrow(pca_zip_input), ")", log_file = LOG_FILE)
  pca_zip_input[, pc1_gent := NA_real_]
}

# ============================================================
# SECTION 6: Long-run cross-section
# ============================================================
logf("SECTION 6: Long-run cross-section", log_file = LOG_FILE)

# Pivot cumulative net acquisitions to wide (one row per geography)
pivot_wide <- function(dt, geo_col) {
  out <- unique(dt[, c(geo_col), with = FALSE])
  for (tp in FILER_LEVELS) {
    pfx     <- gsub("[^a-zA-Z]", "_", tolower(tp))
    net_col <- paste0("net_units_", pfx)
    acq_col <- paste0("acq_units_", pfx)
    sub     <- dt[as.character(filer_type) == tp,
                  c(geo_col, "cum_net_units", "cum_acq_units"), with = FALSE]
    setnames(sub, c("cum_net_units", "cum_acq_units"), c(net_col, acq_col))
    out <- merge(out, sub, by = geo_col, all.x = TRUE)
    out[is.na(get(net_col)), (net_col) := 0L]
    out[is.na(get(acq_col)), (acq_col) := 0L]
  }
  unit_cols <- grep("^net_units_", names(out), value = TRUE)
  acq_cols  <- grep("^acq_units_", names(out), value = TRUE)
  out[, total_net_units  := rowSums(.SD, na.rm = TRUE), .SDcols = unit_cols]
  out[, total_acq_units  := rowSums(.SD, na.rm = TRUE), .SDcols = acq_cols]
  # Share = high-filer gross acquisitions / total gross acquisitions (well-defined everywhere)
  out[, high_filer_acq_share := fifelse(
    total_acq_units > 0,
    acq_units_high_filer / total_acq_units,
    NA_real_
  )]
  out
}

tract_net_wide <- pivot_wide(cum_tract_1019, "tract_fips")
zip_net_wide   <- pivot_wide(cum_zip_0619,   "zip5")

# merge back on total units from cum_tract_1019 to use as size variable in plots
tract_net_wide <- merge(tract_net_wide, cum_tract_1019[, .(tract_fips, total_units_mean)], by = "tract_fips", all.x = TRUE)
zip_net_wide <- merge(zip_net_wide, cum_zip_0619[, .(zip5, total_units_mean)], by.x = "zip5", by.y = "zip5", all.x = TRUE)


logf("  High-filer acq share — tract median: ",
     round(median(tract_net_wide$high_filer_acq_share, na.rm = TRUE), 3),
     "  ZIP median: ",
     round(median(zip_net_wide$high_filer_acq_share,   na.rm = TRUE), 3),
     log_file = LOG_FILE)

# Merge with PCA scores and nhood components
tract_cs <- merge(
  pca_tr_input[, .(tract_fips, pc1_gent, pc2, d_income, d_rent, d_home_val,
                    d_pct_black, d_poverty, d_crime, d_nanda, pop_10,
                    gent_status, gentrifiable, rank_pct_10, rank_pct_19, d_rank_pct,
                    income_10, income_19)],
  tract_net_wide, by = "tract_fips"
)
zip_cs <- merge(
  pca_zip_input[, c("zip", "pc1_gent", pca_zip_vars), with = FALSE],
  zip_net_wide,
  by.x = "zip", by.y = "zip5"
)

logf("  Tract cross-section: ", nrow(tract_cs), " tracts", log_file = LOG_FILE)
logf("  ZIP cross-section:   ", nrow(zip_cs),   " ZIPs",   log_file = LOG_FILE)
fwrite(tract_cs, file.path(OUT_DIR, "tract_portfolio_vs_nhood.csv"))
fwrite(zip_cs,   file.path(OUT_DIR, "zip_portfolio_vs_nhood.csv"))

# ============================================================
# SECTION 7: Scatter plots — portfolio change vs nhood trajectory
# ============================================================
logf("SECTION 7: Scatter plots", log_file = LOG_FILE)

theme_pf <- function() {
  theme_minimal(base_size = 10) %+replace% theme(
    panel.grid.minor = element_blank(),
    strip.text       = element_text(face = "bold"),
    plot.title       = element_text(face = "bold", size = 11),
    plot.subtitle    = element_text(size = 8.5, color = "grey40")
  )
}

# 2×2 grid: one panel per filer type, x = pc1, y = cumulative net units
plot_net_vs_pc1 <- function(cs_dt, x_lab = "Neighborhood PC1 (higher = gentrifying)",
                             title_sfx = "") {
  type_cols <- setNames(
    paste0("net_units_", gsub("[^a-zA-Z]", "_", tolower(FILER_LEVELS))),
    FILER_LEVELS
  )
  plts <- lapply(FILER_LEVELS, function(tp) {
    col <- type_cols[[tp]]
    if (!col %in% names(cs_dt)) return(NULL)
    sub <- cs_dt[!is.na(pc1_gent) & !is.na(get(col))]
    ggplot(sub, aes(size = total_units_mean, x = pc1_gent, y = get(col))) +
      geom_hline(yintercept = 0, linetype = "dashed", color = "grey60") +
      geom_point(color = PALETTE[[tp]], alpha = 0.7) +
      geom_smooth(method = "lm", se = TRUE, color = "grey25", linewidth = 0.75, aes(weight = total_units_mean)) +
      labs(x = x_lab, y = "Cumulative net units acquired", title = tp) +
      theme_pf()
  })
  plts <- Filter(Negate(is.null), plts)
  wrap_plots(plts, ncol = 2) +
    plot_annotation(
      title    = paste0("Net acquisitions by landlord type vs neighborhood change", title_sfx),
      subtitle = "Positive = net buyer; negative = net seller"
    )
}

plot_net_vs_inc <- function(cs_dt, x_lab = "Percent Change in Income",
                            title_sfx = "") {
  type_cols <- setNames(
    paste0("net_units_", gsub("[^a-zA-Z]", "_", tolower(FILER_LEVELS))),
    FILER_LEVELS
  )
  plts <- lapply(FILER_LEVELS, function(tp) {
    col <- type_cols[[tp]]
    if (!col %in% names(cs_dt)) return(NULL)
    sub <- cs_dt[!is.na(d_income) & !is.na(get(col))]
    ggplot(sub, aes(size = total_units_mean, x = d_income, y = get(col))) +
      geom_hline(yintercept = 0, linetype = "dashed", color = "grey60") +
      geom_point(color = PALETTE[[tp]], alpha = 0.7) +
      geom_vline(aes(xintercept = 0.29, linetype = "overall inflation"), color = "grey60") +
      # make vline dashed
      scale_linetype_manual(name = "", values = c("overall inflation" = "dashed")) +
      # label vline as "CPI"
      geom_smooth(method = "lm", se = TRUE, color = "grey25", linewidth = 0.75, aes(weight = total_units_mean)) +
      labs(x = x_lab, y = "Cumulative net units acquired", title = tp) +
      theme_pf()
  })
  plts <- Filter(Negate(is.null), plts)
  wrap_plots(plts, ncol = 2) +
    plot_annotation(
      title    = paste0("Net acquisitions by landlord type vs neighborhood change", title_sfx),
      subtitle = "Positive = net buyer; negative = net seller"
    )
}


p_tract <- plot_net_vs_pc1(tract_cs, title_sfx = " (tract, 2010–2019)")
p_zip   <- plot_net_vs_pc1(zip_cs,   title_sfx = " (ZIP, 2006–2019)")
ggsave(file.path(FIG_DIR, "tract_net_acq_vs_pc1.png"), p_tract, width = 9, height = 7, dpi = 180, bg = 'white')
ggsave(file.path(FIG_DIR, "zip_net_acq_vs_pc1.png"),   p_zip,   width = 9, height = 7, dpi = 180, bg = 'white')

zip_cs$d_income = zip_cs$d_log_income
p_tract_inc <- plot_net_vs_inc(tract_cs, title_sfx = " (tract, 2010–2019)")
p_zip_inc   <- plot_net_vs_inc(zip_cs,   title_sfx = " (ZIP, 2006–2019)")
ggsave(file.path(FIG_DIR, "tract_net_acq_vs_inc.png"), p_tract_inc, width = 9, height = 7, dpi = 180, bg = 'white')
ggsave(file.path(FIG_DIR, "zip_net_acq_vs_inc.png"),   p_zip_inc,   width = 9, height = 7, dpi = 180, bg = 'white')

# just the high filers
p_zip_inc_hf <-p_zip_inc[[1]] +
  labs(title = "High-filer net acquisitions vs neighborhood change (ZIP, 2006–2019)") +
  # make all the fonts way bigger
  theme(
    plot.title = element_text(size = 16, face = "bold"),
    axis.title = element_text(size = 14),
    axis.text = element_text(size = 12),
    # same w/ legends
    legend.title = element_text(size = 14),
    legend.text = element_text(size = 12)
  ) +
  # label overall inflation at 0.29% directly on the plot
  # line is already there, just needs a label
  annotate("text", x = 0.36, y = max(zip_cs$net_units_high_filer, na.rm = TRUE) * 0.9,
           label = "Overall inflation (CPI)", angle = 0,  size = 4, color = "blue") +
  # make the linetype dashed
  # line is already there, just needs a new linetype
  theme(legend.position = "bottom")

# export
ggsave(file.path(FIG_DIR, "zip_net_acq_high_filer_vs_inc.png"), p_zip_inc_hf, width = 9, height = 7, dpi = 180, bg = 'white')

# High-filer share vs PC1 summary scatter
plot_share_vs_pc1 <- function(cs_dt, x_lab = "Neighborhood PC1 (higher = gentrifying)",
                               title_sfx = "") {
  sub <- cs_dt[!is.na(pc1_gent) & !is.na(high_filer_acq_share)]
  ggplot(sub, aes(x = pc1_gent, y = high_filer_acq_share)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "grey60") +
    geom_point(color = PALETTE[["High-filer"]], alpha = 0.7, size = 2.2) +
    geom_smooth(method = "lm", se = TRUE, color = "grey25", linewidth = 0.75) +
    scale_y_continuous(labels = label_percent(accuracy = 1)) +
    labs(x = x_lab, y = "High-filer share of net acquisitions",
         title = paste0("High-eviction-landlord share vs neighborhood trajectory", title_sfx)) +
    theme_pf()
}
ggsave(file.path(FIG_DIR, "tract_hifiler_share_vs_pc1.png"),
       plot_share_vs_pc1(tract_cs, title_sfx = " (tract)"), width = 7, height = 5, dpi = 180)
ggsave(file.path(FIG_DIR, "zip_hifiler_share_vs_pc1.png"),
       plot_share_vs_pc1(zip_cs,   title_sfx = " (ZIP)"),   width = 7, height = 5, dpi = 180)

# ============================================================
# SECTION 7b: Tract maps for deck / writeup consumption
# ============================================================
logf("SECTION 7b: Tract maps", log_file = LOG_FILE)

bg_shp_path <- p_input(cfg, "philly_bg_shp")
bg_sf <- st_read(bg_shp_path, quiet = TRUE)
if (!"GEO_ID" %in% names(bg_sf)) {
  stop("philly_bg_shp is missing required column GEO_ID needed to build tract maps.")
}
bg_sf$tract_fips <- substr(sub(".*US", "", bg_sf$GEO_ID), 1L, 11L)
tract_sf <- aggregate(
  bg_sf["tract_fips"],
  by = list(bg_sf$tract_fips),
  FUN = function(x) x[1L],
  do_union = TRUE
)
setDT(tract_cs)
tract_map_dt <- unique(tract_cs[, .(tract_fips, gent_status, income_10, pc1_gent)])
tract_sf <- merge(tract_sf, tract_map_dt, by = "tract_fips", all.x = TRUE)
tract_sf <- st_as_sf(tract_sf)

GENT_PALETTE <- c(
  "Gentrified"       = "#E76F51",
  "Did not gentrify" = "#457B9D",
  "Not gentrifiable" = "#A8DADC"
)

map_theme <- function() {
  theme_void(base_size = 11) +
    theme(
      legend.position  = "bottom",
      legend.key.width = grid::unit(1.5, "cm"),
      plot.title       = element_text(face = "bold", size = 11, hjust = 0.5),
      plot.subtitle    = element_text(size = 8, color = "grey40", hjust = 0.5),
      legend.text      = element_text(size = 8),
      legend.title     = element_text(size = 9),
      plot.margin      = margin(4, 4, 4, 4)
    )
}

p_gent_status <- ggplot(tract_sf) +
  geom_sf(aes(fill = gent_status), color = "white", linewidth = 0.1) +
  scale_fill_manual(
    values = GENT_PALETTE,
    na.value = "grey85",
    name = NULL,
    guide = guide_legend(nrow = 1)
  ) +
  labs(
    title = "Gentrification status (2010-2019)",
    subtitle = "Bottom-half-income tracts in 2010 are classified by subsequent income-rank change"
  ) +
  map_theme()

p_income_2010 <- ggplot(tract_sf) +
  geom_sf(aes(fill = income_10), color = "white", linewidth = 0.1) +
  scale_fill_distiller(
    palette = "YlGnBu",
    direction = 1,
    na.value = "grey85",
    name = "Median HH income",
    labels = label_dollar()
  ) +
  labs(title = "Tract median household income in 2010") +
  map_theme() +
  theme(legend.key.height = grid::unit(0.4, "cm"))

p_pc1 <- ggplot(tract_sf) +
  geom_sf(aes(fill = pc1_gent), color = "white", linewidth = 0.1) +
  scale_fill_distiller(
    palette = "RdBu",
    direction = 1,
    na.value = "grey85",
    name = "PC1"
  ) +
  labs(title = "Gentrification PCA index (PC1)") +
  map_theme() +
  theme(legend.key.height = grid::unit(0.4, "cm"))

ggsave(file.path(FIG_DIR, "tract_gent_status_map.png"), p_gent_status, width = 6.5, height = 5.5, dpi = 180)
ggsave(file.path(FIG_DIR, "tract_income_2010_map.png"), p_income_2010, width = 6.5, height = 5.5, dpi = 180)
ggsave(file.path(FIG_DIR, "tract_gent_pc1_map.png"), p_pc1, width = 6.5, height = 5.5, dpi = 180)
ggsave(
  file.path(FIG_DIR, "tract_gent_status_income_panel.png"),
  p_gent_status + p_income_2010 + plot_layout(ncol = 2),
  width = 11,
  height = 5.5,
  dpi = 180
)
logf("  Saved tract map figures to ", FIG_DIR, log_file = LOG_FILE)

# ============================================================
# SECTION 8: Individual nhood component scatter grids
# ============================================================
logf("SECTION 8: Component scatter grids", log_file = LOG_FILE)

make_component_grid <- function(cs_dt, nhood_vars, nhood_labels,
                                 filer_col, title_root) {
  plts <- lapply(seq_along(nhood_vars), function(i) {
    v <- nhood_vars[i]; lbl <- nhood_labels[i]
    if (!v %in% names(cs_dt) || !filer_col %in% names(cs_dt)) return(NULL)
    sub <- cs_dt[!is.na(get(v)) & !is.na(get(filer_col))]
    ggplot(sub, aes(x = get(v), y = get(filer_col))) +
      geom_hline(yintercept = 0, linetype = "dashed", color = "grey65") +
      geom_point(color = "#264653", alpha = 0.65, size = 1.8) +
      geom_smooth(method = "lm", se = TRUE, color = "#D1495B", linewidth = 0.7) +
      labs(x = lbl, y = "Cumulative net units") +
      theme_pf()
  })
  plts <- Filter(Negate(is.null), plts)
  wrap_plots(plts, ncol = 3) + plot_annotation(title = title_root)
}

tract_nhood_vars   <- c("d_income", "d_rent", "d_pct_black", "d_poverty", "d_crime", "d_nanda")
tract_nhood_labels <- c("Δ log income", "Δ log rent", "Δ % Black",
                         "Δ Poverty rate", "Δ Crime rate", "Δ log(1+Establishments)")

for (ft in c("High-filer", "Low-filer")) {
  col  <- paste0("net_units_", gsub("[^a-zA-Z]", "_", tolower(ft)))
  if (!col %in% names(tract_cs)) next
  p    <- make_component_grid(tract_cs, tract_nhood_vars, tract_nhood_labels, col,
                               title_root = paste0(ft, ": net acquisitions vs components (tract, 2010–2019)"))
  fname <- paste0("tract_", gsub("[^a-zA-Z]", "_", tolower(ft)), "_vs_components.png")
  ggsave(file.path(FIG_DIR, fname), p, width = 10, height = 7, dpi = 180)
}

# ============================================================
# SECTION 9: Panel regressions
# ============================================================
logf("SECTION 9: Panel regressions", log_file = LOG_FILE)

# Wide: one row per tract × year, one column per filer type
net_tract_wide <- dcast(
  net_tract[!is.na(filer_type)],
  total_units + tract_fips + year ~ filer_type,
  value.var = "net_units", fill = 0L
)
setnames(net_tract_wide, gsub("[^a-zA-Z0-9]", "_", names(net_tract_wide)))
setnames(net_tract_wide, gsub("_{2,}", "_",       names(net_tract_wide)))
setnames(net_tract_wide, gsub("_$",    "",         names(net_tract_wide)))

# Join annual nhood outcomes
nhood_annual_cols <- intersect(
  c("tract_fips", "year", "acs_med_hh_income", "rate_total",
    "nanda_total_establishments", "acs_pct_black", "acs_med_rent", "nanda_totpop"),
  names(tract_dt)
)
panel_dt <- merge(net_tract_wide, tract_dt[, ..nhood_annual_cols],
                  by = c("tract_fips", "year"), all.x = TRUE)

setorder(panel_dt, tract_fips, year)
# Use log levels as RHS with two-way FE — within-tract income trend identifies sorting.
# NOTE: Do NOT use d_log_income with two-way FE. First-differencing the RHS + demeaning
# with tract FE is double-demeaning (within FE of an already-differenced variable), which
# is inconsistent. Use levels + FE OR pure first differences without tract FE.
panel_dt[, log_income := fifelse(acs_med_hh_income > 0, log(acs_med_hh_income), NA_real_)]
panel_dt[, log_rent   := fifelse(acs_med_rent > 0,      log(acs_med_rent),      NA_real_)]

hf_col <- grep("High_filer|High.filer", names(panel_dt), value = TRUE, ignore.case = TRUE)[1L]
lf_col <- grep("Low_filer|Low.filer",   names(panel_dt), value = TRUE, ignore.case = TRUE)[1L]

reg_results <- list()
if (!is.na(hf_col) && !is.na(lf_col) &&
    "log_income" %in% names(panel_dt) && "log_rent" %in% names(panel_dt)) {

  reg_panel <- panel_dt[!is.na(log_income) & !is.na(log_rent) &
                          year >= 2010L & year <= 2019L]

  # Panel: net acquisitions by type ~ log income + log rent (levels) + tract + year FE
  # Identifies: within-tract, within-year variation in income/rent predicting acquisition
  # Weighted by total_units so larger geographies count more
  m_hf_acq <- feols(
    as.formula(paste0(hf_col, " ~ log_income + log_rent + rate_total | tract_fips + year")),
    data = reg_panel, cluster = ~tract_fips, weights = ~total_units
  )
  m_lf_acq <- feols(
    as.formula(paste0(lf_col, " ~ log_income + log_rent + rate_total | tract_fips + year")),
    data = reg_panel, cluster = ~tract_fips, weights = ~total_units
  )
  # Reverse: does lagged high-filer net buying predict subsequent income growth?
  # Uses log_income as outcome with two-way FE (fully consistent: levels throughout)
  setorder(reg_panel, tract_fips, year)
  reg_panel[, lag_hf := shift(get(hf_col)), by = tract_fips]
  reg_panel[, lag_lf := shift(get(lf_col)), by = tract_fips]
  m_nhood <- feols(
    log_income ~ lag_hf + lag_lf | tract_fips + year,
    data = reg_panel[!is.na(lag_hf)], cluster = ~tract_fips, weights = ~total_units
  )

  for (nm in c("hf_acq_on_nhood", "lf_acq_on_nhood", "nhood_on_lagged_acq")) {
    m <- list(hf_acq_on_nhood = m_hf_acq, lf_acq_on_nhood = m_lf_acq,
              nhood_on_lagged_acq = m_nhood)[[nm]]
    coefs_dt <- as.data.table(coeftable(m), keep.rownames = "term")
    coefs_dt[, model := nm]
    reg_results[[nm]] <- coefs_dt
  }
  logf("  Panel reg (hf acq ~ log_income + log_rent + crime, two-way FE):", log_file = LOG_FILE)
  print(summary(m_hf_acq))
  fwrite(rbindlist(reg_results, fill = TRUE), file.path(OUT_DIR, "panel_reg_coefs.csv"))
  saveRDS(
    list(m_hf_acq = m_hf_acq, m_lf_acq = m_lf_acq, m_nhood = m_nhood,
         hf_col = hf_col, lf_col = lf_col),
    file.path(OUT_DIR, "panel_reg_models.rds")
  )
  logf("  Panel reg models saved as RDS", log_file = LOG_FILE)
} else {
  logf("  WARNING: Missing columns for panel regressions — skipping", log_file = LOG_FILE)
}

# ============================================================
# SECTION 9b: Cross-section pseudo first stage
# ============================================================
# Regress cumulative net acquisitions on long-run nhood components (cross-section).
# Coefficients tell us which nhood changes most predict high-filer accumulation.
# Fitted values give a data-driven "slumlord attractiveness" index — an alternative
# to PCA that weights components by their actual relevance to portfolio sorting.
# NOTE on EB rate: filing_rate_eb_pre_covid is a static pre-COVID estimate and may
# incorporate post-acquisition changes for landlords who increased filings after buying.
# This biases classification toward labeling post-acquisition "improvers" as high-filers.
# TODO: ideally use only years strictly pre-acquisition per building. Hold off for now.
logf("SECTION 9b: Cross-section pseudo first stage", log_file = LOG_FILE)

xsec_vars <- intersect(pca_tract_vars, names(tract_cs))
if (length(xsec_vars) >= 2L && "net_units_high_filer" %in% names(tract_cs)) {
  xsec_fml_hf <- as.formula(paste("net_units_high_filer ~", paste(xsec_vars, collapse = " + ")))
  xsec_fml_lf <- as.formula(paste("net_units_low_filer  ~", paste(xsec_vars, collapse = " + ")))

  m_xsec_hf <- feols(xsec_fml_hf, data = tract_cs[complete.cases(tract_cs[, ..xsec_vars])],
                     weights = ~total_units_mean)
  m_xsec_lf <- feols(xsec_fml_lf, data = tract_cs[complete.cases(tract_cs[, ..xsec_vars])],
                     weights = ~total_units_mean)

  logf("  Cross-section HF ~ nhood components:", log_file = LOG_FILE)
  print(summary(m_xsec_hf))

  # Add predicted "slumlord attractiveness" to tract_cs for plotting
  tract_cs[complete.cases(tract_cs[, ..xsec_vars]),
           xsec_pred_hf := fitted(m_xsec_hf)]
  fwrite(tract_cs, file.path(OUT_DIR, "tract_portfolio_vs_nhood.csv"))

  saveRDS(
    list(m_xsec_hf = m_xsec_hf, m_xsec_lf = m_xsec_lf, xsec_vars = xsec_vars),
    file.path(OUT_DIR, "xsec_reg_models.rds")
  )
  logf("  Cross-section models saved", log_file = LOG_FILE)
} else {
  logf("  Skipping cross-section regression (insufficient variables)", log_file = LOG_FILE)
}

# ============================================================
# SECTION 10: City-wide diagnostics
# ============================================================
logf("SECTION 10: City-wide diagnostics", log_file = LOG_FILE)

city_annual <- net_tract[, .(
  net_units     = sum(net_units,     na.rm = TRUE),
  net_transfers = sum(net_transfers, na.rm = TRUE),
  n_acq         = sum(n_acq,         na.rm = TRUE),
  n_sell        = sum(n_sell,         na.rm = TRUE)
), by = .(filer_type, year)]

p_city <- ggplot(city_annual[!is.na(filer_type)],
                  aes(x = year, y = net_units, color = filer_type)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "grey60") +
  geom_line(linewidth = 0.9) + geom_point(size = 1.8) +
  scale_color_manual(values = PALETTE, guide = "none") +
  facet_wrap(~filer_type, scales = "free_y", ncol = 2) +
  labs(x = NULL, y = "Net units acquired city-wide",
       title = "City-wide annual net unit acquisitions by landlord type") +
  theme_pf() + theme(legend.position = "none")
ggsave(file.path(FIG_DIR, "citywide_net_acq_by_type.png"), p_city, width = 9, height = 6, dpi = 180)

# Also save filer classification summary
filer_summary <- rbindlist(list(
  buy_classified[,  .(side = "buyer",  .N), by = filer_type],
  sell_classified[, .(side = "seller", .N), by = filer_type]
))
fwrite(filer_summary, file.path(OUT_DIR, "filer_classification_summary.csv"))

# ============================================================
# SECTION 11: Portfolio composition from the tenant's perspective
#   For a unit in a below/above-median-income neighborhood,
#   what does the owning conglomerate's portfolio look like?
#   Two income measures: (A) ACS tract median HH income 2010,
#   (B) InfoUSA building-level mean real HH income.
#   Two panels each: all landlords / large only (cong ≥10 units).
#   Unit-weighted throughout. PHA excluded.
# ============================================================
logf("SECTION 11: Portfolio composition from the tenant's perspective", log_file = LOG_FILE)

# Step 1: 2010 ownership snapshot — PHA excluded
pha_pids_2010 <- xwalk_entity_cat[year == 2010L & owner_category == "PHA", unique(PID)]
logf("  PHA PIDs excluded: ", length(pha_pids_2010), log_file = LOG_FILE)

cong10 <- xwalk_cong[year == 2010L & !PID %in% pha_pids_2010, .(PID, conglomerate_id)]
cong10 <- merge(cong10, pid_geo[, .(PID, tract_fips, units_modal)], by = "PID", all.x = TRUE)
cong10 <- merge(cong10,
                gent_lookup[, .(tract_fips, income_10, gentrifiable)],
                by = "tract_fips", all.x = TRUE)
cong10[is.na(units_modal), units_modal := 1L]  # rare

# Join 2010 InfoUSA income per building (building-level, not tract-level)
if ("infousa_find_mean_k" %in% names(bldg)) {
  infousa_2010 <- bldg[year == 2010L & !is.na(infousa_find_mean_k),
                       .(PID, infousa_income = infousa_find_mean_k)]
  cong10 <- merge(cong10, infousa_2010, by = "PID", all.x = TRUE)
  infousa_median <- median(cong10$infousa_income, na.rm = TRUE)
  cong10[, infousa_below_median := !is.na(infousa_income) & infousa_income < infousa_median]
  logf("  InfoUSA 2010 income: ", cong10[!is.na(infousa_income), .N], " PIDs | ",
       "median = $", round(infousa_median * 1000), log_file = LOG_FILE)
} else {
  stop("infousa_find_mean_k not found in bldg — re-run with updated bldg_cols")
}

logf("  cong10: ", nrow(cong10), " rows | ",
     cong10[!is.na(income_10), .N], " with ACS income | ",
     cong10[!is.na(infousa_income), .N], " with InfoUSA income",
     log_file = LOG_FILE)

# ── Helper: unit-weighted mean summary by nhood_type ──────────────────────────
make_port_summary <- function(dt, panel_label) {
  dt[nhood_type != "Unknown", {
    w   <- units_modal
    wtd <- function(x) weighted.mean(x, w, na.rm = TRUE)
    .(
      panel                     = panel_label,
      n_units                   = sum(w,          na.rm = TRUE),
      n_buildings               = .N,
      wtd_mean_cong_props       = wtd(cong_n_props),
      wtd_mean_cong_units       = wtd(cong_total_units),
      wtd_mean_cong_tracts      = wtd(cong_n_tracts),
      wtd_mean_cong_sd_income   = wtd(cong_sd_income),
      wtd_mean_cong_mean_income = wtd(cong_mean_income),
      wtd_mean_pct_below        = wtd(cong_pct_below_median)
    )
  }, by = nhood_type][order(nhood_type)]
}

# ── VERSION A: ACS tract median income ────────────────────────────────────────
# Per-conglomerate stats using ACS income_10
cong_stats_acs <- cong10[, .(
  cong_n_props          = .N,
  cong_total_units      = sum(units_modal,  na.rm = TRUE),
  cong_n_tracts         = uniqueN(tract_fips[!is.na(tract_fips)]),
  cong_mean_income      = weighted.mean(income_10, units_modal, na.rm = TRUE),
  cong_sd_income        = sd(income_10,     na.rm = TRUE),
  cong_pct_below_median = {
    d <- sum(units_modal, na.rm = TRUE)
    if (d > 0) sum(units_modal[gentrifiable == TRUE], na.rm = TRUE) / d else NA_real_
  }
), by = conglomerate_id]

pid_cong_acs <- merge(
  cong10[, .(PID, conglomerate_id, units_modal, income_10, gentrifiable)],
  cong_stats_acs, by = "conglomerate_id"
)
pid_cong_acs[, nhood_type := fcase(
  gentrifiable == TRUE,  "Below-median-income nhood",
  gentrifiable == FALSE, "Above-median-income nhood",
  default                = "Unknown"
)]

comp_acs_all   <- make_port_summary(pid_cong_acs, "All landlords")
comp_acs_large <- make_port_summary(
  pid_cong_acs[cong_total_units >= 10L], "Large portfolios (≥10 units)"
)
comp_acs <- rbind(comp_acs_all, comp_acs_large)

# ── VERSION B: InfoUSA building-level income ──────────────────────────────────
# Portfolio SIZE stats (n_props, total_units, n_tracts) come from the full
# conglomerate — same as ACS version. Only income/pct_below are InfoUSA-based,
# computed from InfoUSA-covered buildings only (74% of 2010 buildings).
cong_income_iu <- cong10[!is.na(infousa_income), .(
  cong_mean_income      = weighted.mean(infousa_income, units_modal, na.rm = TRUE),
  cong_sd_income        = sd(infousa_income,     na.rm = TRUE),
  cong_pct_below_median = {
    d <- sum(units_modal, na.rm = TRUE)
    if (d > 0) sum(units_modal[infousa_below_median == TRUE], na.rm = TRUE) / d else NA_real_
  }
), by = conglomerate_id]

# Join size stats (from full conglomerate) + income stats (InfoUSA-covered only)
cong_stats_iu <- merge(
  cong_stats_acs[, .(conglomerate_id, cong_n_props, cong_total_units, cong_n_tracts)],
  cong_income_iu, by = "conglomerate_id"
)

pid_cong_iu <- merge(
  cong10[!is.na(infousa_income),
         .(PID, conglomerate_id, units_modal, infousa_income, infousa_below_median)],
  cong_stats_iu, by = "conglomerate_id"
)
pid_cong_iu[, nhood_type := fcase(
  infousa_below_median == TRUE,  "Below-median-income bldg",
  infousa_below_median == FALSE, "Above-median-income bldg",
  default                        = "Unknown"
)]

comp_iu_all   <- make_port_summary(pid_cong_iu, "All landlords")
comp_iu_large <- make_port_summary(
  pid_cong_iu[cong_total_units >= 10L], "Large portfolios (≥10 units)"
)
comp_iu <- rbind(comp_iu_all, comp_iu_large)

# ── VERSION C: Unified rent (med_rent), deflated to 2010 dollars ──────────────
# Coverage is patchy per year (4–14%), so deflate to real 2010$ and average
# across all available years per PID. Key stat: SD of portfolio rent (does the
# landlord mix high-rent and low-rent buildings, or specialize?).
if ("med_rent" %in% names(bldg)) {
  rent_dt <- bldg[!is.na(med_rent) & med_rent > 0, .(PID, year, med_rent)]

  # Year-only deflator: mean log rent by year (simple price index)
  year_fe <- rent_dt[, .(log_rent_mean = mean(log(med_rent), na.rm = TRUE)), by = year]
  ref_log <- year_fe[year == 2010L, log_rent_mean]
  if (length(ref_log) == 0L) ref_log <- year_fe[, mean(log_rent_mean)]
  year_fe[, deflator := exp(log_rent_mean - ref_log)]  # >1 means rents rose above 2010
  rent_dt <- merge(rent_dt, year_fe[, .(year, deflator)], by = "year")
  rent_dt[, rent_real := med_rent / deflator]  # constant 2010 dollars

  # Aggregate to PID: mean real rent across all observed years
  pid_rent <- rent_dt[, .(rent_real_mean = mean(rent_real, na.rm = TRUE)), by = PID]
  rent_median <- median(pid_rent$rent_real_mean, na.rm = TRUE)
  pid_rent[, rent_below_median := rent_real_mean < rent_median]

  logf("  Rent: ", nrow(pid_rent), " PIDs | median real rent (2010$) = $",
       round(rent_median), log_file = LOG_FILE)

  cong10_rent <- merge(cong10[, .(PID, conglomerate_id, units_modal)],
                       pid_rent, by = "PID")

  # Per-conglomerate rent stats (income = rent here)
  cong_income_rent <- cong10_rent[, .(
    cong_mean_income      = weighted.mean(rent_real_mean, units_modal, na.rm = TRUE),
    cong_sd_income        = sd(rent_real_mean,     na.rm = TRUE),
    cong_pct_below_median = {
      d <- sum(units_modal, na.rm = TRUE)
      if (d > 0) sum(units_modal[rent_below_median == TRUE], na.rm = TRUE) / d else NA_real_
    }
  ), by = conglomerate_id]

  # Size stats from full conglomerate (ACS)
  cong_stats_rent <- merge(
    cong_stats_acs[, .(conglomerate_id, cong_n_props, cong_total_units, cong_n_tracts)],
    cong_income_rent, by = "conglomerate_id"
  )

  pid_cong_rent <- merge(
    cong10_rent[, .(PID, conglomerate_id, units_modal, rent_real_mean, rent_below_median)],
    cong_stats_rent, by = "conglomerate_id"
  )
  pid_cong_rent[, nhood_type := fcase(
    rent_below_median == TRUE,  "Below-median-rent bldg",
    rent_below_median == FALSE, "Above-median-rent bldg",
    default                     = "Unknown"
  )]

  comp_rent_all   <- make_port_summary(pid_cong_rent, "All landlords")
  comp_rent_large <- make_port_summary(
    pid_cong_rent[cong_total_units >= 10L], "Large portfolios (≥10 units)"
  )
  comp_rent <- rbind(comp_rent_all, comp_rent_large)
  cat("=== Rent version ===\n"); print(comp_rent)
} else {
  stop("med_rent not found in bldg — re-run with updated bldg_cols")
}

# Step 5: Write outputs
fwrite(comp_acs,  file.path(OUT_DIR, "portfolio_composition_acs.csv"))
fwrite(comp_iu,   file.path(OUT_DIR, "portfolio_composition_infousa.csv"))
fwrite(comp_rent, file.path(OUT_DIR, "portfolio_composition_rent.csv"))
fwrite(pid_cong_acs[, .(PID, conglomerate_id, units_modal, income_10, gentrifiable,
                         nhood_type, cong_n_props, cong_total_units, cong_n_tracts,
                         cong_sd_income, cong_mean_income, cong_pct_below_median)],
       file.path(OUT_DIR, "portfolio_composition_raw_acs.csv"))
logf("  Saved portfolio_composition_acs.csv, portfolio_composition_infousa.csv",
     log_file = LOG_FILE)
cat("=== ACS version ===\n"); print(comp_acs)
cat("=== InfoUSA version ===\n"); print(comp_iu)

logf("=== Done. Outputs in: ", OUT_DIR, " ===", log_file = LOG_FILE)
