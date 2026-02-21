## ============================================================
## address-history-analysis.R
## ============================================================
## Purpose: Analyze tenant address histories and eviction persistence.
##          Tracks households across moves using InfoUSA panel data
##          to test whether tenants from high-eviction buildings
##          end up in other high-eviction buildings.
##
## Inputs:  infousa_cleaned, infousa_address_xwalk, bldg_panel_blp
## Outputs: tables/evict_persist.tex
## ============================================================

library(data.table)
library(fixest)
library(ggalluvial)
library(ggplot2)

# ---- Config & logging ----
source("r/config.R")
cfg <- read_config()
log_file <- p_out(cfg, "logs", "address-history-analysis.log")
logf("=== Starting address-history-analysis.R ===", log_file = log_file)

# ---- Load data via config ----
logf("Loading inputs...", log_file = log_file)

philly_infousa_dt <- fread(p_input(cfg, "infousa_cleaned"))
philly_infousa_dt[, obs_id := .I]
logf("  infousa_cleaned: ", format(nrow(philly_infousa_dt), big.mark = ","), " rows",
     log_file = log_file)

info_usa_xwalk <- fread(p_product(cfg, "infousa_address_xwalk"))
logf("  infousa_address_xwalk: ", format(nrow(info_usa_xwalk), big.mark = ","), " rows",
     log_file = log_file)

## bldg_panel_blp has everything: filing rates, rents, occupancy, parcel chars,
## GEOID, unit bins, and rental flags. Replaces separate loads of bldg_panel,
## license_long_min, infousa_occupancy, and parcel_building_2024.
philly_rent_df <- fread(p_product(cfg, "bldg_panel_blp"))
logf("  bldg_panel_blp: ", format(nrow(philly_rent_df), big.mark = ","), " rows",
     log_file = log_file)

# ---- Build eviction summary by PID ----
philly_evict_df <- philly_rent_df[, .(
  filing_rate = mean(filing_rate_raw, na.rm = TRUE),
  total_filings = sum(num_filings, na.rm = TRUE)
), by = PID]
logf("Eviction summary: ", format(nrow(philly_evict_df), big.mark = ","), " unique PIDs",
     log_file = log_file)

# ---- Identify rental PIDs ----
# bldg_panel_blp is already filtered to ever-rental PIDs in make-analytic-sample.R.
# Also include InfoUSA renter-status linked PIDs for completeness.
rental_PIDs <- unique(c(
  philly_rent_df[, unique(PID)],
  info_usa_xwalk[
    n_sn_ss_c %in% philly_infousa_dt[owner_renter_status %in% 0:3, n_sn_ss_c],
    unique(PID)
  ]
))
logf("Rental PIDs (union): ", format(length(rental_PIDs), big.mark = ","),
     log_file = log_file)

# ---- Merge InfoUSA with parcel crosswalk ----
n_pre <- nrow(philly_infousa_dt)
philly_infousa_dt_m <- merge(
  philly_infousa_dt,
  info_usa_xwalk[num_parcels_matched == 1 & !is.na(PID)],
  by = "n_sn_ss_c"
)
logf("Merge InfoUSA -> xwalk: ", format(n_pre, big.mark = ","), " -> ",
     format(nrow(philly_infousa_dt_m), big.mark = ","), " rows (",
     round(100 * nrow(philly_infousa_dt_m) / n_pre, 1), "% matched)",
     log_file = log_file)

# ---- Merge on building panel data (occupancy, parcel chars, rents, filings) ----
# Select only the columns we need from bldg_panel_blp to avoid column collisions
bldg_cols <- c("PID", "year", "total_units", "GEOID", "num_units_bin",
               "filing_rate_raw", "num_filings", "log_med_rent", "med_rent")
bldg_cols_avail <- intersect(bldg_cols, names(philly_rent_df))

n_pre <- nrow(philly_infousa_dt_m)
philly_infousa_dt_m <- merge(
  philly_infousa_dt_m,
  philly_rent_df[, ..bldg_cols_avail],
  by = c("PID", "year"),
  all.x = TRUE
)
logf("Merge -> bldg_panel_blp: ", format(n_pre, big.mark = ","), " -> ",
     format(nrow(philly_infousa_dt_m), big.mark = ","), " rows", log_file = log_file)

# Fill NA filings with 0
philly_infousa_dt_m[is.na(num_filings), num_filings := 0L]
philly_infousa_dt_m[is.na(filing_rate_raw), filing_rate_raw := 0]

# ---- Flag rentals ----
philly_infousa_dt_m[, rental := PID %in% rental_PIDs]
n_rental <- philly_infousa_dt_m[rental == TRUE, .N]
logf("Rental flag: ", format(n_rental, big.mark = ","), " / ",
     format(nrow(philly_infousa_dt_m), big.mark = ","), " rows (",
     round(100 * n_rental / nrow(philly_infousa_dt_m), 1), "%)",
     log_file = log_file)

# Owner-renter status vs rental flag
ors_tab <- philly_infousa_dt_m[, .(
  count = .N,
  sum_rental = sum(rental == TRUE)
), by = owner_renter_status]
ors_tab[, pct_rental := round(sum_rental / count, 2)]
setorder(ors_tab, owner_renter_status)
logf("Owner-renter status vs rental flag:\n",
     paste(capture.output(print(ors_tab)), collapse = "\n"),
     log_file = log_file)

# ---- Construct lagged address/eviction variables within household ----
setorder(philly_infousa_dt_m, familyid, year)
lag_cols <- c("n_sn_ss_c", "filing_rate_raw", "rental", "total_units",
              "ge_census_tract", "GEOID", "pm.zip", "log_med_rent")
lag_names <- paste0("prev_", c("address", "evict", "rental", "total_units",
                               "ge_census_tract", "geoid", "pm.zip", "log_med_rent"))
for (i in seq_along(lag_cols)) {
  philly_infousa_dt_m[, (lag_names[i]) := shift(get(lag_cols[i]), type = "lag"),
                      by = familyid]
}

# ---- Identify movers (rental-to-rental) ----
philly_infousa_dt_m[, move := n_sn_ss_c != prev_address]
movers <- philly_infousa_dt_m[
  move == TRUE &
    !is.na(prev_address) &
    rental == TRUE &
    !is.na(familyid) &
    prev_rental == TRUE
]
logf("Movers (rental-to-rental): ", format(nrow(movers), big.mark = ","), " rows",
     log_file = log_file)

# ============================================================
# TRACKING DESCRIPTIVES
# ============================================================

# ---- Step 1: Tracking statistics per household ----
logf("--- Tracking descriptives ---", log_file = log_file)

hh_tracking <- philly_infousa_dt_m[rental == TRUE, .(
  n_addresses = uniqueN(n_sn_ss_c),
  n_years     = uniqueN(year),
  min_year    = min(year),
  max_year    = max(year)
), by = familyid]
hh_tracking[, mover_status := fifelse(n_addresses >= 2, "mover", "stayer")]

logf("Tracking stats (rental HHs):", log_file = log_file)
logf("  N unique familyids: ", format(nrow(hh_tracking), big.mark = ","), log_file = log_file)
logf("  Distribution of addresses per HH:", log_file = log_file)
addr_dist <- hh_tracking[, .(N = .N), by = .(n_addr_grp = fifelse(n_addresses >= 4, "4+", as.character(n_addresses)))]
addr_dist[, pct := round(100 * N / sum(N), 1)]
setorder(addr_dist, n_addr_grp)
logf(paste(capture.output(print(addr_dist)), collapse = "\n"), log_file = log_file)
logf("  Median years observed: ", median(hh_tracking$n_years), log_file = log_file)
logf("  Mean years observed: ", round(mean(hh_tracking$n_years), 2), log_file = log_file)
logf("  % trackable 2+ years: ", round(100 * mean(hh_tracking$n_years >= 2), 1), "%",
     log_file = log_file)

# ---- Step 2: Load and merge race/gender data ----
logf("Loading race and gender imputation data...", log_file = log_file)

# Race imputation
race_dt <- fread(p_product(cfg, "infousa_race_imputed_person"))
logf("  infousa_race_imputed_person: ", format(nrow(race_dt), big.mark = ","), " rows",
     log_file = log_file)
race_hoh <- race_dt[person_num == 1 & !is.na(familyid), .(
  familyid, year, p_white, p_black, p_hispanic, p_asian, p_other,
  race_hat, race_impute_status
)]
rm(race_dt)
logf("  Head-of-household race records: ", format(nrow(race_hoh), big.mark = ","),
     log_file = log_file)

# Gender imputation (much more complete than raw gender_1 which is 91% missing)
gender_dt <- fread(p_product(cfg, "infousa_gender_imputed_person"))
logf("  infousa_gender_imputed_person: ", format(nrow(gender_dt), big.mark = ","), " rows",
     log_file = log_file)
gender_hoh <- gender_dt[person_num == 1 & !is.na(familyid), .(
  familyid, year, p_female, gender_hat
)]
rm(gender_dt)
logf("  Head-of-household gender records: ", format(nrow(gender_hoh), big.mark = ","),
     log_file = log_file)

# Merge race onto the merged infousa panel
philly_infousa_dt_m <- merge(
  philly_infousa_dt_m,
  race_hoh,
  by = c("familyid", "year"),
  all.x = TRUE
)
logf("  Race merge: ", round(100 * mean(!is.na(philly_infousa_dt_m$race_hat)), 1),
     "% of rows matched", log_file = log_file)

# Merge gender
philly_infousa_dt_m <- merge(
  philly_infousa_dt_m,
  gender_hoh,
  by = c("familyid", "year"),
  all.x = TRUE
)
logf("  Gender merge: ", round(100 * mean(!is.na(philly_infousa_dt_m$gender_hat)), 1),
     "% of rows matched", log_file = log_file)

# ---- Step 3: Descriptive comparison table — movers vs stayers vs all renters ----
logf("Building tracking comparison table...", log_file = log_file)

# Tag each row with mover_status
philly_infousa_dt_m <- merge(
  philly_infousa_dt_m,
  hh_tracking[, .(familyid, n_addresses, n_years, mover_status)],
  by = "familyid",
  all.x = TRUE
)

# Function to compute group summary
compute_group_stats <- function(dt, group_label) {
  hh_level <- dt[, .(n_addresses = n_addresses[1], n_years = n_years[1]), by = familyid]
  data.table(
    group           = group_label,
    n_hh_years      = nrow(dt),
    n_unique_hh     = uniqueN(dt$familyid),
    mean_addresses  = round(mean(hh_level$n_addresses, na.rm = TRUE), 2),
    mean_years_obs  = round(mean(hh_level$n_years, na.rm = TRUE), 2),
    pct_black       = round(100 * mean(dt$p_black, na.rm = TRUE), 1),
    pct_white       = round(100 * mean(dt$p_white, na.rm = TRUE), 1),
    pct_hispanic    = round(100 * mean(dt$p_hispanic, na.rm = TRUE), 1),
    pct_female_hoh  = round(100 * mean(dt$p_female, na.rm = TRUE), 1),
    mean_filing_rate = round(mean(dt$filing_rate_raw, na.rm = TRUE), 4),
    mean_total_units = round(mean(dt$total_units, na.rm = TRUE), 1),
    mean_log_rent   = round(mean(dt$log_med_rent, na.rm = TRUE), 3),
    n_unique_tracts = uniqueN(dt$ge_census_tract[!is.na(dt$ge_census_tract)])
  )
}

rental_dt <- philly_infousa_dt_m[rental == TRUE]
comp_table <- rbindlist(list(
  compute_group_stats(rental_dt, "All Rental HHs"),
  compute_group_stats(rental_dt[mover_status == "stayer"], "Stayers (1 address)"),
  compute_group_stats(rental_dt[mover_status == "mover"], "Movers (2+ addresses)")
))

logf("Comparison table:\n",
     paste(capture.output(print(comp_table, nclass = Inf)), collapse = "\n"),
     log_file = log_file)

# ---- Step 4: Neighborhood race characterization for movers ----
logf("Neighborhood characterization for movers...", log_file = log_file)

# Load BG race priors
bg_priors_path <- file.path(
  cfg$paths$processed_dir, "xwalks", "bg_renter_race_priors_2013.csv"
)
bg_priors <- fread(bg_priors_path)
logf("  BG race priors: ", format(nrow(bg_priors), big.mark = ","), " BGs",
     log_file = log_file)

# movers already has geoid (current BG) and prev_geoid (previous BG)
# — but note: movers hasn't been clean_names'd yet at this point, use GEOID/prev_geoid
# Actually movers is defined above at line 128 but clean_names happens later (line 150).
# So we work on philly_infousa_dt_m directly for the mover subset before clean_names.

# Build mover-level neighborhood data from philly_infousa_dt_m
mover_nbhd <- philly_infousa_dt_m[
  !is.na(familyid) & rental == TRUE & mover_status == "mover" &
    !is.na(GEOID) & !is.na(prev_address)
]
# Keep only actual move rows
mover_nbhd <- mover_nbhd[n_sn_ss_c != prev_address & prev_rental == TRUE & !is.na(prev_geoid)]

# Merge BG priors for destination (current GEOID)
mover_nbhd <- merge(
  mover_nbhd,
  bg_priors[, .(bg_geoid, dest_pct_black = p_black_prior)],
  by.x = "GEOID", by.y = "bg_geoid",
  all.x = TRUE
)
# Merge BG priors for origin (prev_geoid)
mover_nbhd <- merge(
  mover_nbhd,
  bg_priors[, .(bg_geoid, orig_pct_black = p_black_prior)],
  by.x = "prev_geoid", by.y = "bg_geoid",
  all.x = TRUE
)

nbhd_by_race <- mover_nbhd[
  !is.na(race_hat) & !is.na(orig_pct_black) & !is.na(dest_pct_black),
  .(
    n_moves          = .N,
    mean_orig_black  = round(mean(orig_pct_black), 3),
    mean_dest_black  = round(mean(dest_pct_black), 3),
    mean_change      = round(mean(dest_pct_black - orig_pct_black), 4)
  ),
  by = race_hat
]
setorder(nbhd_by_race, race_hat)
logf("Neighborhood pct_black at origin vs destination by mover race:\n",
     paste(capture.output(print(nbhd_by_race)), collapse = "\n"),
     log_file = log_file)

# ---- Step 4b: Building-level eviction data + LOO tract eviction rate ----
logf("--- Building eviction data and LOO tract eviction rate ---", log_file = log_file)

# Get PID-level EB eviction intensity and total filings (building-level, pre-COVID)
# n_years_pre2019 counts how many panel years each PID has before 2020,
# so we can annualize filings / unit-years for the LOO tract rate.
pid_evict <- philly_rent_df[, .(
  filing_rate_eb_pre_covid = filing_rate_eb_pre_covid[1],
  total_filings_pre2019    = total_filings_pre2019[1],
  total_units_mean         = mean(total_units, na.rm = TRUE),
  n_years_pre2019          = sum(year <= 2019, na.rm = TRUE),
  GEOID_bldg               = GEOID[which.max(!is.na(GEOID))]
), by = PID]
pid_evict[, census_tract := substr(GEOID_bldg, 1, 11)]
# Unit-years: annualize the denominator so rate is filings per unit per year
pid_evict[, unit_years_pre2019 := total_units_mean * pmax(n_years_pre2019, 1L)]

# Helper: bin eviction intensity, separating true never-filers
make_evict_bin <- function(eb_rate, total_filings) {
  never <- is.na(total_filings) | total_filings == 0L
  fcase(
    never, "No filings",
    eb_rate <= 0.05, "(0-5%]",
    eb_rate <= 0.10, "(5-10%]",
    eb_rate <= 0.20, "(10-20%]",
    eb_rate > 0.20, "20%+",
    default = NA_character_
  )
}
evict_bin_levels <- c("No filings", "(0-5%]", "(5-10%]", "(10-20%]", "20%+")

# Build LOO tract eviction rate (annualized: filings per unit-year):
# For each PID, tract eviction rate = (sum of filings in tract excl. this PID) /
#                                      (sum of unit-years in tract excl. this PID)
tract_totals <- pid_evict[
  !is.na(census_tract) & !is.na(total_filings_pre2019) & !is.na(unit_years_pre2019),
  .(
    tract_total_filings    = sum(total_filings_pre2019, na.rm = TRUE),
    tract_total_unit_years = sum(unit_years_pre2019, na.rm = TRUE),
    tract_n_bldgs          = .N
  ),
  by = census_tract
]

pid_evict <- merge(pid_evict, tract_totals, by = "census_tract", all.x = TRUE)

# Leave-one-out: subtract this building's contribution
pid_evict[, loo_tract_filings    := tract_total_filings - fifelse(is.na(total_filings_pre2019), 0, total_filings_pre2019)]
pid_evict[, loo_tract_unit_years := tract_total_unit_years - fifelse(is.na(unit_years_pre2019), 0, unit_years_pre2019)]
pid_evict[, loo_tract_evict_rate := fifelse(
  loo_tract_unit_years > 0,
  loo_tract_filings / loo_tract_unit_years,
  NA_real_
)]

logf("  LOO tract eviction rate: mean=", round(mean(pid_evict$loo_tract_evict_rate, na.rm = TRUE), 4),
     ", median=", round(median(pid_evict$loo_tract_evict_rate, na.rm = TRUE), 4),
     ", N non-missing=", pid_evict[!is.na(loo_tract_evict_rate), .N],
     log_file = log_file)

# ---- Step 4c: Lag PIDs on full panel for origin/destination tracking ----
setorder(philly_infousa_dt_m, familyid, year)
philly_infousa_dt_m[, prev_PID := shift(PID, type = "lag"), by = familyid]

# ---- Step 4d: Inflow analysis — where do tenants at high-evict buildings come from? ----
logf("--- Inflow analysis: origin characteristics by destination eviction intensity ---",
     log_file = log_file)

# Merge destination eviction data onto mover_nbhd
mover_nbhd <- merge(
  mover_nbhd,
  pid_evict[, .(PID, dest_evict_eb = filing_rate_eb_pre_covid,
                dest_filings_pre2019 = total_filings_pre2019,
                dest_loo_tract_evict = loo_tract_evict_rate)],
  by = "PID",
  all.x = TRUE
)

# Get prev_PID from full panel
mover_nbhd[, prev_PID := philly_infousa_dt_m[
  match(paste(mover_nbhd$familyid, mover_nbhd$year),
        paste(philly_infousa_dt_m$familyid, philly_infousa_dt_m$year)),
  prev_PID
]]

# Merge origin eviction data
mover_nbhd <- merge(
  mover_nbhd,
  pid_evict[, .(PID, orig_evict_eb = filing_rate_eb_pre_covid,
                orig_filings_pre2019 = total_filings_pre2019,
                orig_loo_tract_evict = loo_tract_evict_rate)],
  by.x = "prev_PID", by.y = "PID",
  all.x = TRUE
)

# Bin eviction intensity for origin and destination
mover_nbhd[, dest_evict_bin := factor(
  make_evict_bin(dest_evict_eb, dest_filings_pre2019),
  levels = evict_bin_levels
)]
mover_nbhd[, orig_evict_bin := factor(
  make_evict_bin(orig_evict_eb, orig_filings_pre2019),
  levels = evict_bin_levels
)]

logf("  Movers with dest eviction data: ",
     mover_nbhd[!is.na(dest_evict_bin), .N], " / ", nrow(mover_nbhd),
     log_file = log_file)

# Summarize origin characteristics by destination eviction bin
inflow_summary <- mover_nbhd[
  !is.na(dest_evict_bin) & !is.na(orig_pct_black),
  .(
    n_moves              = .N,
    mean_orig_pct_black  = round(mean(orig_pct_black, na.rm = TRUE), 3),
    mean_dest_pct_black  = round(mean(dest_pct_black, na.rm = TRUE), 3),
    mean_orig_evict_eb   = round(mean(orig_evict_eb, na.rm = TRUE), 4),
    mean_orig_loo_tract  = round(mean(orig_loo_tract_evict, na.rm = TRUE), 4),
    mean_dest_loo_tract  = round(mean(dest_loo_tract_evict, na.rm = TRUE), 4),
    mover_pct_black      = round(mean(p_black, na.rm = TRUE), 3),
    mover_pct_female     = round(mean(p_female, na.rm = TRUE), 3),
    mean_orig_rent       = round(mean(prev_log_med_rent, na.rm = TRUE), 3),
    mean_dest_rent       = round(mean(log_med_rent, na.rm = TRUE), 3)
  ),
  by = dest_evict_bin
][order(dest_evict_bin)]

logf("Inflow summary by destination eviction intensity:\n",
     paste(capture.output(print(inflow_summary)), collapse = "\n"),
     log_file = log_file)

fwrite(inflow_summary, p_out(cfg, "tables", "inflow_by_dest_eviction_bin.csv"))

# ---- Step 4e: Outflow analysis — where do tenants FROM high-evict buildings go? ----
logf("--- Outflow analysis: destination characteristics by origin eviction intensity ---",
     log_file = log_file)

outflow_summary <- mover_nbhd[
  !is.na(orig_evict_bin) & !is.na(dest_pct_black),
  .(
    n_moves              = .N,
    mean_orig_pct_black  = round(mean(orig_pct_black, na.rm = TRUE), 3),
    mean_dest_pct_black  = round(mean(dest_pct_black, na.rm = TRUE), 3),
    mean_dest_evict_eb   = round(mean(dest_evict_eb, na.rm = TRUE), 4),
    mean_orig_loo_tract  = round(mean(orig_loo_tract_evict, na.rm = TRUE), 4),
    mean_dest_loo_tract  = round(mean(dest_loo_tract_evict, na.rm = TRUE), 4),
    delta_loo_tract      = round(mean(dest_loo_tract_evict - orig_loo_tract_evict, na.rm = TRUE), 4),
    mover_pct_black      = round(mean(p_black, na.rm = TRUE), 3),
    mover_pct_female     = round(mean(p_female, na.rm = TRUE), 3),
    mean_orig_rent       = round(mean(prev_log_med_rent, na.rm = TRUE), 3),
    mean_dest_rent       = round(mean(log_med_rent, na.rm = TRUE), 3)
  ),
  by = orig_evict_bin
][order(orig_evict_bin)]

logf("Outflow summary by origin eviction intensity:\n",
     paste(capture.output(print(outflow_summary)), collapse = "\n"),
     log_file = log_file)

fwrite(outflow_summary, p_out(cfg, "tables", "outflow_by_orig_eviction_bin.csv"))

# ---- Step 4f: 3-move trajectory analysis ----
# Track households across: origin → middle (high/low evict) → destination
# Question: do people who pass through high-evicting buildings end up in
# higher-eviction neighborhoods than where they started?
logf("--- 3-move trajectory analysis ---", log_file = log_file)

# Build a spell-level panel: each row is a (familyid, PID, spell_start, spell_end)
# Then identify sequences of 3 consecutive spells (origin → middle → dest)
setorder(philly_infousa_dt_m, familyid, year)

# Create spell ID: changes when PID changes within familyid
philly_infousa_dt_m[, pid_changed := PID != shift(PID, type = "lag"), by = familyid]
philly_infousa_dt_m[is.na(pid_changed), pid_changed := TRUE]  # first obs
philly_infousa_dt_m[, spell_id := cumsum(pid_changed), by = familyid]

# Collapse to spell level
spells <- philly_infousa_dt_m[
  rental == TRUE & !is.na(PID),
  .(
    spell_start = min(year),
    spell_end   = max(year),
    n_years     = .N,
    GEOID       = GEOID[which.max(!is.na(GEOID))]
  ),
  by = .(familyid, spell_id, PID)
]
setorder(spells, familyid, spell_start)

# Merge eviction data onto spells
spells <- merge(
  spells,
  pid_evict[, .(PID, evict_eb = filing_rate_eb_pre_covid,
                filings_pre2019 = total_filings_pre2019,
                loo_tract_evict = loo_tract_evict_rate)],
  by = "PID",
  all.x = TRUE
)

# Number spells sequentially within household
spells[, spell_seq := seq_len(.N), by = familyid]
spells[, n_spells := .N, by = familyid]

# Bin the middle spell
spells[, evict_bin := factor(
  make_evict_bin(evict_eb, filings_pre2019),
  levels = evict_bin_levels
)]

logf("  Total spells: ", nrow(spells),
     "; HHs with 3+ spells: ", spells[n_spells >= 3, uniqueN(familyid)],
     log_file = log_file)

# Build 3-spell sequences: origin (spell s), middle (spell s+1), destination (spell s+2)
# Self-join: for each middle spell, get the previous and next
middle <- spells[spell_seq >= 2 & spell_seq <= n_spells - 1]
setnames(middle,
  c("PID", "GEOID", "evict_eb", "filings_pre2019", "loo_tract_evict", "evict_bin",
    "spell_start", "spell_end", "n_years"),
  c("mid_PID", "mid_GEOID", "mid_evict_eb", "mid_filings_pre2019", "mid_loo_tract_evict",
    "mid_evict_bin", "mid_start", "mid_end", "mid_years")
)

origin <- spells[, .(familyid, spell_seq,
  orig_PID = PID, orig_GEOID = GEOID, orig_evict_eb = evict_eb,
  orig_filings = filings_pre2019, orig_loo_tract = loo_tract_evict,
  orig_start = spell_start, orig_end = spell_end
)]

dest <- spells[, .(familyid, spell_seq,
  dest_PID = PID, dest_GEOID = GEOID, dest_evict_eb = evict_eb,
  dest_filings = filings_pre2019, dest_loo_tract = loo_tract_evict,
  dest_start = spell_start, dest_end = spell_end
)]

# Join: middle spell s → origin spell s-1, dest spell s+1
trajectories <- merge(
  middle[, .(familyid, spell_seq, mid_PID, mid_GEOID, mid_evict_eb,
             mid_filings_pre2019, mid_loo_tract_evict, mid_evict_bin,
             mid_start, mid_end, mid_years)],
  origin[, .(familyid, spell_seq_mid = spell_seq + 1L,
             orig_PID, orig_GEOID, orig_evict_eb, orig_filings, orig_loo_tract,
             orig_start, orig_end)],
  by.x = c("familyid", "spell_seq"),
  by.y = c("familyid", "spell_seq_mid")
)

trajectories <- merge(
  trajectories,
  dest[, .(familyid, spell_seq_mid = spell_seq - 1L,
           dest_PID, dest_GEOID, dest_evict_eb, dest_filings, dest_loo_tract,
           dest_start, dest_end)],
  by.x = c("familyid", "spell_seq"),
  by.y = c("familyid", "spell_seq_mid")
)

logf("  3-spell trajectories: ", nrow(trajectories), " sequences from ",
     trajectories[, uniqueN(familyid)], " households", log_file = log_file)

# Define high-evict middle spell (>= 10% EB filing rate)
trajectories[, mid_high_evict := mid_evict_eb >= 0.10 &
               !is.na(mid_filings_pre2019) & mid_filings_pre2019 > 0]

# Key outcome: change in LOO tract eviction rate from origin to destination
trajectories[, delta_loo_tract := dest_loo_tract - orig_loo_tract]

# Also bin origin eviction intensity for stratification
trajectories[, orig_evict_bin := factor(
  make_evict_bin(orig_evict_eb, orig_filings),
  levels = evict_bin_levels
)]

# Summary: trajectory outcomes by whether middle building is high-evict
traj_summary <- trajectories[
  !is.na(mid_high_evict) & !is.na(orig_loo_tract) & !is.na(dest_loo_tract),
  .(
    n_trajectories       = .N,
    n_households         = uniqueN(familyid),
    mean_orig_loo_tract  = round(mean(orig_loo_tract, na.rm = TRUE), 4),
    mean_mid_evict_eb    = round(mean(mid_evict_eb, na.rm = TRUE), 4),
    mean_dest_loo_tract  = round(mean(dest_loo_tract, na.rm = TRUE), 4),
    mean_delta_loo_tract = round(mean(delta_loo_tract, na.rm = TRUE), 4),
    median_delta_loo     = round(median(delta_loo_tract, na.rm = TRUE), 4),
    mean_orig_evict_eb   = round(mean(orig_evict_eb, na.rm = TRUE), 4),
    mean_dest_evict_eb   = round(mean(dest_evict_eb, na.rm = TRUE), 4)
  ),
  by = mid_high_evict
]

logf("3-move trajectory summary (high vs non-high middle building):\n",
     paste(capture.output(print(traj_summary)), collapse = "\n"),
     log_file = log_file)

# Finer: by middle building eviction bin
traj_by_mid_bin <- trajectories[
  !is.na(mid_evict_bin) & !is.na(orig_loo_tract) & !is.na(dest_loo_tract),
  .(
    n_traj               = .N,
    mean_orig_loo_tract  = round(mean(orig_loo_tract, na.rm = TRUE), 4),
    mean_dest_loo_tract  = round(mean(dest_loo_tract, na.rm = TRUE), 4),
    mean_delta_loo_tract = round(mean(delta_loo_tract, na.rm = TRUE), 4),
    mean_orig_evict_eb   = round(mean(orig_evict_eb, na.rm = TRUE), 4),
    mean_dest_evict_eb   = round(mean(dest_evict_eb, na.rm = TRUE), 4)
  ),
  by = mid_evict_bin
][order(mid_evict_bin)]

logf("3-move trajectory by middle building eviction bin:\n",
     paste(capture.output(print(traj_by_mid_bin)), collapse = "\n"),
     log_file = log_file)

fwrite(traj_by_mid_bin, p_out(cfg, "tables", "trajectory_by_mid_eviction_bin.csv"))

# Regression: delta_loo_tract ~ mid_evict_bin, controlling for origin tract
traj_reg_dt <- trajectories[
  !is.na(mid_evict_bin) & !is.na(delta_loo_tract) &
    !is.na(orig_GEOID) & nzchar(orig_GEOID)
]

if (nrow(traj_reg_dt) > 100) {
  # (A) Unconditional
  m_traj_uncond <- feols(
    delta_loo_tract ~ i(mid_evict_bin, ref = "(0-5%]"),
    data = traj_reg_dt,
    cluster = ~ orig_GEOID
  )

  # (B) Controlling for origin tract FE
  traj_reg_dt[, orig_tract := substr(orig_GEOID, 1, 11)]
  m_traj_orig_fe <- feols(
    delta_loo_tract ~ i(mid_evict_bin, ref = "(0-5%]") | orig_tract,
    data = traj_reg_dt,
    cluster = ~ orig_GEOID
  )

  # (C) Destination LOO tract evict in levels (not change), with origin FE
  m_traj_dest_level <- feols(
    dest_loo_tract ~ i(mid_evict_bin, ref = "(0-5%]") | orig_tract,
    data = traj_reg_dt,
    cluster = ~ orig_GEOID
  )

  logf("Trajectory regressions:", log_file = log_file)
  logf("  (A) delta_loo_tract ~ mid_evict_bin (unconditional):", log_file = log_file)
  logf(paste(capture.output(summary(m_traj_uncond)), collapse = "\n"), log_file = log_file)
  logf("  (B) delta_loo_tract ~ mid_evict_bin | orig_tract:", log_file = log_file)
  logf(paste(capture.output(summary(m_traj_orig_fe)), collapse = "\n"), log_file = log_file)
  logf("  (C) dest_loo_tract ~ mid_evict_bin | orig_tract:", log_file = log_file)
  logf(paste(capture.output(summary(m_traj_dest_level)), collapse = "\n"), log_file = log_file)

  traj_tab <- etable(
    m_traj_uncond, m_traj_orig_fe, m_traj_dest_level,
    title = "3-move trajectory: effect of middle building eviction intensity",
    headers = c("Delta LOO Tract", "Delta + Orig FE", "Dest Level + Orig FE"),
    tex = TRUE
  )
  writeLines(traj_tab, p_out(cfg, "tables", "trajectory_regressions.tex"))
  logf("Wrote trajectory regression table", log_file = log_file)
}

logf("--- End inflow/outflow/trajectory analysis ---", log_file = log_file)

# ---- Step 5: Write tracking descriptives outputs ----

# CSV (long format)
comp_long <- melt(comp_table, id.vars = "group", variable.name = "metric", value.name = "value")
qa_csv_path <- p_out(cfg, "qa", "address_history_tracking_descriptives.csv")
dir.create(dirname(qa_csv_path), showWarnings = FALSE, recursive = TRUE)
fwrite(comp_long, qa_csv_path)
logf("Wrote tracking descriptives CSV -> ", qa_csv_path, log_file = log_file)

# LaTeX table
tex_path <- p_out(cfg, "tables", "tracking_descriptives.tex")
dir.create(dirname(tex_path), showWarnings = FALSE, recursive = TRUE)

# Format numbers with commas for display
fmt_num <- function(x) {
  ifelse(abs(x) >= 1000, format(round(x), big.mark = ","), as.character(x))
}

# Build LaTeX manually for full control
tex_lines <- c(
  "\\begin{table}[htbp]",
  "\\centering",
  "\\caption{Tracking Descriptives: Movers vs.\\ Stayers}",
  "\\label{tab:tracking_descriptives}",
  "\\begin{tabular}{lrrr}",
  "\\toprule",
  paste0(" & ", paste(comp_table$group, collapse = " & "), " \\\\"),
  "\\midrule"
)

row_labels <- c(
  n_hh_years = "N HH-Years",
  n_unique_hh = "N Unique HHs",
  mean_addresses = "Mean Addresses per HH",
  mean_years_obs = "Mean Years Observed",
  pct_black = "\\% Black (mean posterior)",
  pct_white = "\\% White (mean posterior)",
  pct_hispanic = "\\% Hispanic (mean posterior)",
  pct_female_hoh = "\\% Female HoH",
  mean_filing_rate = "Mean Filing Rate",
  mean_total_units = "Mean Building Size (units)",
  mean_log_rent = "Mean Log Rent",
  n_unique_tracts = "N Unique Tracts"
)

for (metric in names(row_labels)) {
  vals <- comp_table[[metric]]
  if (metric %in% c("n_hh_years", "n_unique_hh", "n_unique_tracts")) {
    vals_fmt <- format(vals, big.mark = ",")
  } else {
    vals_fmt <- as.character(vals)
  }
  tex_lines <- c(tex_lines,
    paste0(row_labels[metric], " & ", paste(vals_fmt, collapse = " & "), " \\\\")
  )
}

tex_lines <- c(tex_lines,
  "\\bottomrule",
  "\\end{tabular}",
  "\\end{table}"
)
writeLines(tex_lines, tex_path)
logf("Wrote tracking descriptives LaTeX -> ", tex_path, log_file = log_file)

logf("--- End tracking descriptives ---", log_file = log_file)

# ---- Unit size bins (current and previous) ----
bin_units <- function(x) {
  fifelse(x == 1, "1",
  fifelse(x <= 5, "2-5",
  fifelse(x <= 50, "6-50",
  fifelse(x <= 100, "51-100",
  fifelse(x > 100, "101+", NA_character_)))))
}
movers[, num_units_bins := bin_units(total_units)]
movers[, prev_num_units_bins := bin_units(prev_total_units)]

# Clean column names for fixest (GEOID -> geoid, PID -> pid, etc.)
movers <- janitor::clean_names(movers)

# ---- Imputed rent (PID mean across years) ----
movers[, imputed_rent := mean(log_med_rent, na.rm = TRUE), by = pid]
movers[, imputed_prev_rent := mean(prev_log_med_rent, na.rm = TRUE), by = pid]

# ---- Eviction persistence regressions ----
logf("Running fixest regressions...", log_file = log_file)

m1 <- feols(
  filing_rate_raw ~ prev_evict,
  cluster = ~pid,
  data = movers[filing_rate_raw <= 1 & prev_evict <= 1]
)

m2 <- feols(
  filing_rate_raw ~ prev_evict | num_units_bins + prev_num_units_bins + geoid + prev_geoid,
  cluster = ~pid,
  data = movers[filing_rate_raw <= 1 & prev_evict <= 1]
)

m3 <- feols(
  filing_rate_raw ~ prev_evict + imputed_rent + imputed_prev_rent |
    num_units_bins + prev_num_units_bins + geoid + prev_geoid,
  cluster = ~pid,
  data = movers[filing_rate_raw <= 0.5 & prev_evict <= 0.5]
)

m4 <- feols(
  filing_rate_raw ~ prev_evict + log_med_rent + prev_log_med_rent |
    num_units_bins + prev_num_units_bins + geoid + prev_geoid,
  cluster = ~pid,
  data = movers[filing_rate_raw <= 1 & prev_evict <= 1]
)

# High-filer indicator regressions
movers[, high_filing := filing_rate_raw > 0.1]
movers[, high_filing_prev := prev_evict > 0.1]

m5_no_rent <- feols(
  high_filing ~ high_filing_prev |
    num_units_bins + prev_num_units_bins + geoid + prev_geoid,
  cluster = ~pid,
  data = movers[filing_rate_raw <= 0.5 & prev_evict <= 0.5]
)

m5 <- feols(
  high_filing ~ high_filing_prev + log_med_rent + prev_log_med_rent |
    num_units_bins + prev_num_units_bins + geoid + prev_geoid,
  cluster = ~pid,
  data = movers[
    filing_rate_raw <= 0.5 & prev_evict <= 0.5 & total_units > 1 & prev_total_units > 1
  ]
)

m_rent <- feols(
  log_med_rent ~ prev_evict + prev_log_med_rent + filing_rate_raw |
    num_units_bins + prev_num_units_bins + geoid + prev_geoid,
  cluster = ~pid,
  data = movers[filing_rate_raw <= 0.5 & prev_evict <= 0.5]
)

log_model <- function(label, mod, log_file) {
  s <- summary(mod)
  ct <- s$coeftable
  logf("  ", label, " (N=", mod$nobs, "):", log_file = log_file)
  for (v in rownames(ct)) {
    logf("    ", v, ": coef=", round(ct[v, "Estimate"], 4),
         " se=", round(ct[v, "Std. Error"], 4),
         " p=", format.pval(ct[v, "Pr(>|t|)"], digits = 3),
         log_file = log_file)
  }
}

logf("Regression results:", log_file = log_file)
log_model("m1 (bivariate)", m1, log_file)
log_model("m2 (+ unit/BG FE)", m2, log_file)
log_model("m3 (+ FE + imputed rent)", m3, log_file)
log_model("m4 (+ FE + actual rent)", m4, log_file)
log_model("m5_no_rent (high-filer + FE)", m5_no_rent, log_file)
log_model("m5 (high-filer + FE + rent)", m5, log_file)
log_model("m_rent (rent ~ prev_evict + FE)", m_rent, log_file)

# ---- Output regression table ----
fixest::setFixest_etable(digits = 4, fitstat = c("n"))

setFixest_dict(
  num_units_bins = "Units (current)",
  prev_num_units_bins = "Units (previous)",
  geoid = "Block Group (current)",
  prev_geoid = "Block Group (previous)",
  prev_evict = "Previous Eviction Filing Rate",
  imputed_rent = "Imputed Rent (current)",
  imputed_prev_rent = "Imputed Rent (previous)",
  log_med_rent = "Rent (current)",
  prev_log_med_rent = "Rent (previous)"
)

tex_out <- p_out(cfg, "tables", "evict_persist.tex")
etable_lines <- etable(
  list(m1, m2, m4),
  title = "Effect of Previous Eviction Filing Rate on Current Filing Rate",
  label = "tab:evict_persist",
  drop = "Constant",
  tex = TRUE
)
writeLines(etable_lines, tex_out)
logf("Wrote regression table -> ", tex_out, log_file = log_file)

# ---- Correlations ----
cor_evict <- movers[
  prev_total_units >= 1 & total_units >= 1 &
    !is.na(log_med_rent) & !is.na(prev_log_med_rent) &
    filing_rate_raw <= 0.5 & prev_evict <= 0.5,
  cor(prev_evict, filing_rate_raw, use = "complete.obs")
]
cor_rent <- movers[
  prev_total_units >= 1 & total_units >= 1 &
    filing_rate_raw <= 0.5 & prev_evict <= 0.5,
  cor(log_med_rent, prev_log_med_rent, use = "complete.obs")
]
logf("Correlations (multi-unit, filing_rate <= 0.5):", log_file = log_file)
logf("  prev_evict <-> filing_rate_raw: ", round(cor_evict, 4), log_file = log_file)
logf("  log_med_rent <-> prev_log_med_rent: ", round(cor_rent, 4), log_file = log_file)

# ---- Eviction bin transitions ----
movers[, `:=`(
  prev_evict_bin = pmin(round(prev_evict / 0.05) * 0.05, 0.25),
  evict_bin = pmin(round(filing_rate_raw / 0.05) * 0.05, 0.25)
)]

flows <- movers[
  !is.na(prev_evict_bin) & !is.na(evict_bin) &
    total_units > 1 & prev_total_units > 1 &
    prev_evict <= 0.5 & filing_rate_raw <= 0.5,
  .N,
  by = .(prev_evict_bin, evict_bin)
]
setorder(flows, prev_evict_bin, evict_bin)
flows[, total_from_bin := sum(N)]
flows[, percent := round(N / total_from_bin, 3)]
flows[, total_to_bin := sum(N), by = evict_bin]
flows[, percent_to := round(N / total_to_bin, 3)]
flows[, `:=`(
  prev_evict_bin_factor = as.factor(prev_evict_bin),
  evict_bin_factor = as.factor(evict_bin)
)]

# ---- High-filer transition matrix ----
transition_evict <- movers[
  prev_total_units > 1 & total_units > 1,
  .(
    count = .N,
    from_high_to_low = sum(high_filing == FALSE & high_filing_prev == TRUE),
    from_low_to_high = sum(high_filing == TRUE & high_filing_prev == FALSE),
    stay_high = sum(high_filing == TRUE & high_filing_prev == TRUE),
    stay_low = sum(high_filing == FALSE & high_filing_prev == FALSE)
  )
]
transition_evict_pct <- copy(transition_evict)
count_val <- transition_evict_pct$count
transition_evict_pct[, names(transition_evict_pct) := lapply(.SD, function(x) x / count_val)]

logf("Eviction transition matrix (multi-unit movers):\n",
     paste(capture.output(print(transition_evict_pct)), collapse = "\n"),
     log_file = log_file)

# ---- Rent decile transitions ----
movers[, imp_rent_decile := frank(imputed_rent, ties.method = "dense"), by = year]
movers[, imp_rent_decile := ceiling(10 * imp_rent_decile / max(imp_rent_decile)), by = year]
movers[, imp_prev_rent_decile := frank(imputed_prev_rent, ties.method = "dense"), by = year]
movers[, imp_prev_rent_decile := ceiling(10 * imp_prev_rent_decile / max(imp_prev_rent_decile)),
       by = year]

movers[, low_rent := imp_rent_decile <= 2]
movers[, low_prev_rent := imp_prev_rent_decile <= 2]

transition_rent <- movers[
  prev_total_units > 1 & total_units > 1,
  .(
    count = sum(!is.na(low_rent) & !is.na(low_prev_rent)),
    from_high_to_low = sum(low_rent == FALSE & low_prev_rent == TRUE, na.rm = TRUE),
    from_low_to_high = sum(low_rent == TRUE & low_prev_rent == FALSE, na.rm = TRUE),
    stay_high = sum(low_rent == TRUE & low_prev_rent == TRUE, na.rm = TRUE),
    stay_low = sum(low_rent == FALSE & low_prev_rent == FALSE, na.rm = TRUE)
  )
]

logf("Rent transition matrix (multi-unit movers):\n",
     paste(capture.output(print(transition_rent)), collapse = "\n"),
     log_file = log_file)

# ---- Random-assignment counterfactual ----
set.seed(cfg$run$seed %||% 123L)

probs <- movers[
  filing_rate_raw <= 1 & prev_evict <= 1 & prev_total_units > 1 & total_units > 1,
  .N,
  by = high_filing
][, prob := N / sum(N)]$prob

sim_df <- data.table(
  high_filing = sample(c(FALSE, TRUE), size = 10000, replace = TRUE, prob = probs),
  high_filing_prev = sample(c(FALSE, TRUE), size = 10000, replace = TRUE, prob = probs)
)
sim_transitions <- sim_df[, .(
  count = .N,
  from_high_to_low = sum(high_filing == FALSE & high_filing_prev == TRUE),
  from_low_to_high = sum(high_filing == TRUE & high_filing_prev == FALSE),
  stay_high = sum(high_filing == TRUE & high_filing_prev == TRUE),
  stay_low = sum(high_filing == FALSE & high_filing_prev == FALSE)
)]
sim_count <- sim_transitions$count
sim_transitions_pct <- copy(sim_transitions)
sim_transitions_pct[, names(sim_transitions_pct) := lapply(.SD, function(x) x / sim_count)]

logf("Random-assignment counterfactual:\n",
     paste(capture.output(print(sim_transitions_pct)), collapse = "\n"),
     log_file = log_file)

# ---- Bin scatter data ----
bin_scatter <- movers[
  filing_rate_raw <= 1 & prev_total_units > 1 & total_units > 1 & prev_evict_bin <= 1,
  .(mean_evict = mean(filing_rate_raw), .N),
  by = prev_evict_bin
]

# ---- Plots ----
logf("Generating plots...", log_file = log_file)

# Alluvial flow plot
p_alluvial <- ggplot(
  flows[evict_bin == 0.0],
  aes(axis1 = prev_evict_bin_factor, axis2 = evict_bin_factor, y = percent_to)
) +
  scale_x_discrete(limits = c("Input", "Output"), expand = c(.1, .05)) +
  geom_alluvium(aes(fill = prev_evict_bin_factor), width = 1 / 12, alpha = 0.9) +
  geom_stratum(width = 1 / 12, fill = "grey90", color = "grey40") +
  geom_text(stat = "stratum", aes(label = after_stat(stratum)), size = 3.5) +
  labs(title = "Flows from Inputs to Outputs",
       y = "Weight", x = NULL, fill = "Input") +
  scale_fill_viridis_d(option = "C") +
  theme_minimal(base_size = 12) +
  theme(panel.grid = element_blank())

# Bin scatter
p_binscatter <- ggplot(
  bin_scatter[mean_evict <= 1 & prev_evict_bin <= 1],
  aes(x = prev_evict_bin, y = mean_evict)
) +
  geom_point() +
  geom_smooth() +
  geom_smooth(method = "lm", color = "black") +
  geom_abline(slope = 1, intercept = 0, color = "red")

# Raw scatter
p_scatter <- ggplot(
  movers[
    filing_rate_raw <= 0.5 & prev_evict <= 0.5 &
      prev_total_units > 1 & total_units > 1
  ],
  aes(x = prev_evict, y = filing_rate_raw)
) +
  geom_point(alpha = 0.1) +
  geom_smooth() +
  geom_abline(slope = 1, intercept = 0, color = "red")

logf("=== Finished address-history-analysis.R ===", log_file = log_file)
