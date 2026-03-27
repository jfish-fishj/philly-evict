# r/cg-type-stability.R
#
# Diagnostic: how stable is conglomerate type classification across years?
#
# For each conglomerate × year, computes the annual filing rate and classifies
# into 4 types (Solo / Small portfolio / Low-evicting / High-evicting) using
# the same thresholds as the analysis scripts. Then asks: how often do
# conglomerates switch type across years, and how much of the stock do they
# represent? Also compares annual snapshots to a full-period classification.
#
# Outputs: output/cg_type_stability/
#   cg_type_stability_summary.csv     -- one row per conglomerate
#   cg_type_annual_distribution.csv   -- type counts and unit-years by year
#   cg_type_transition_matrix.csv     -- year-to-year transition counts
#   cg_type_annual_vs_full.csv        -- cross-tab: annual type vs full-period type

suppressPackageStartupMessages({ library(data.table) })
source("r/config.R")
source("r/helper-functions.R")
cfg <- read_config("config.yml")

HIGH_FILER_THRESHOLD      <- 0.05   # filings per unit-year (matches analysis scripts)
SMALL_PORTFOLIO_MIN_UNITS <- 10L    # minimum other units for non-"Small" classification
YEAR_MIN                  <- 2011L
YEAR_MAX                  <- 2019L
N_YEARS                   <- YEAR_MAX - YEAR_MIN + 1L
TYPE_LEVELS <- c("Solo", "Small portfolio", "Low-evicting", "High-evicting")

OUT_DIR <- file.path("output", "cg_type_stability")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

cat(sprintf("[%s] Loading data...\n", format(Sys.time(), "%H:%M:%S")))

bldg <- fread(p_product(cfg, "bldg_panel_blp"),
              select = c("PID", "year", "total_units", "num_filings"))
bldg[, PID  := normalize_pid(PID)]
bldg[, year := as.integer(year)]
bldg <- bldg[year >= YEAR_MIN & year <= YEAR_MAX]
cat(sprintf("  bldg_panel_blp [%d-%d]: %d rows, %d PIDs\n",
            YEAR_MIN, YEAR_MAX, nrow(bldg), bldg[, uniqueN(PID)]))

xwalk_cong <- fread(p_product(cfg, "xwalk_pid_conglomerate"),
                    select = c("PID", "year", "conglomerate_id"))
xwalk_cong[, PID             := normalize_pid(PID)]
xwalk_cong[, year            := as.integer(year)]
xwalk_cong[, conglomerate_id := as.character(conglomerate_id)]
cat(sprintf("  xwalk_pid_conglomerate: %d rows\n", nrow(xwalk_cong)))

# ============================================================
# Join conglomerate membership onto building panel
# PIDs with no conglomerate membership = solo (own conglomerate)
# ============================================================
bp <- merge(bldg, xwalk_cong, by = c("PID", "year"), all.x = TRUE)
bp[is.na(conglomerate_id), conglomerate_id := PID]

# ============================================================
# Annual conglomerate totals
# ============================================================
cong_yr <- bp[, .(
  n_pids      = uniqueN(PID),
  tot_filings = sum(num_filings,  na.rm = TRUE),
  tot_units   = sum(total_units,  na.rm = TRUE)
), by = .(conglomerate_id, year)]

cong_yr[, ann_rate := fifelse(tot_units > 0, tot_filings / tot_units, NA_real_)]

# Classify: Solo = 1 building; Small = < min units; High/Low based on rate
classify_type <- function(n_pids, tot_units, rate, min_units, hi_threshold) {
  fcase(
    n_pids == 1L,                  "Solo",
    tot_units < min_units,         "Small portfolio",
    rate >= hi_threshold,          "High-evicting",
    default =                      "Low-evicting"
  )
}

cong_yr[, type_annual := classify_type(n_pids, tot_units, ann_rate,
                                        SMALL_PORTFOLIO_MIN_UNITS, HIGH_FILER_THRESHOLD)]
cong_yr[, type_annual := factor(type_annual, levels = TYPE_LEVELS)]

# ============================================================
# Full-period conglomerate totals
# ============================================================
cong_full <- bp[, .(
  n_pids_full  = uniqueN(PID),
  full_filings = sum(num_filings,  na.rm = TRUE),
  full_units   = sum(total_units,  na.rm = TRUE)
), by = conglomerate_id]

cong_full[, full_rate := fifelse(full_units > 0, full_filings / full_units, NA_real_)]

# For full-period, scale the unit threshold by number of years so it represents
# the same average annual size (i.e., 10 units × 9 years = 90 unit-years)
cong_full[, type_full := classify_type(n_pids_full, full_units, full_rate,
                                        SMALL_PORTFOLIO_MIN_UNITS * N_YEARS,
                                        HIGH_FILER_THRESHOLD)]
cong_full[, type_full := factor(type_full, levels = TYPE_LEVELS)]

# ============================================================
# Compare annual vs full-period
# ============================================================
comp <- merge(cong_yr, cong_full[, .(conglomerate_id, type_full)],
              by = "conglomerate_id", all.x = TRUE)

comp[, disagrees_with_full := (type_annual != type_full)]

# Within each conglomerate: does annual type ever change?
comp[, ever_changes := (uniqueN(type_annual) > 1L), by = conglomerate_id]

# ============================================================
# Output 1: Summary per conglomerate
# ============================================================
cong_summary <- comp[, .(
  ever_changes_annual  = any(ever_changes),
  ever_disagrees_full  = any(disagrees_with_full),
  unit_years           = sum(tot_units, na.rm = TRUE),
  n_years_obs          = .N,
  type_full            = first(type_full)
), by = conglomerate_id]

total_uy     <- sum(cong_summary$unit_years)
pct_change   <- sum(cong_summary$unit_years[cong_summary$ever_changes_annual],  na.rm=TRUE) / total_uy
pct_disagree <- sum(cong_summary$unit_years[cong_summary$ever_disagrees_full], na.rm=TRUE) / total_uy

# ============================================================
# Output 2: Annual type distribution
# ============================================================
annual_dist <- cong_yr[, .(
  n_congs    = .N,
  unit_years = sum(tot_units, na.rm = TRUE)
), by = .(year, type_annual)]
setorder(annual_dist, year, type_annual)

# ============================================================
# Output 3: Year-to-year transition matrix
# ============================================================
trans <- merge(
  cong_yr[, .(conglomerate_id, year, type_t  = type_annual)],
  cong_yr[, .(conglomerate_id, year = year - 1L, type_t1 = type_annual)],
  by = c("conglomerate_id", "year")
)
trans_mat <- trans[, .(n = .N), by = .(type_t, type_t1)]
setorder(trans_mat, type_t, type_t1)

# Also add unit-year-weighted version
trans_wt <- merge(
  trans,
  cong_yr[, .(conglomerate_id, year, tot_units)],
  by = c("conglomerate_id", "year")
)
trans_mat_wt <- trans_wt[, .(unit_years = sum(tot_units, na.rm = TRUE)),
                           by = .(type_t, type_t1)]
setorder(trans_mat_wt, type_t, type_t1)

# ============================================================
# Output 4: Annual vs full-period cross-tab
# ============================================================
ann_vs_full <- comp[, .(
  n_cong_years = .N,
  unit_years   = sum(tot_units, na.rm = TRUE)
), by = .(type_annual, type_full)]
setorder(ann_vs_full, type_full, type_annual)

# ============================================================
# Write outputs
# ============================================================
fwrite(cong_summary,  file.path(OUT_DIR, "cg_type_stability_summary.csv"))
fwrite(annual_dist,   file.path(OUT_DIR, "cg_type_annual_distribution.csv"))
fwrite(trans_mat,     file.path(OUT_DIR, "cg_type_transition_matrix.csv"))
fwrite(trans_mat_wt,  file.path(OUT_DIR, "cg_type_transition_matrix_unitwt.csv"))
fwrite(ann_vs_full,   file.path(OUT_DIR, "cg_type_annual_vs_full.csv"))

# ============================================================
# Print key stats
# ============================================================
cat(sprintf("\n=== Conglomerate type stability (%d-%d) ===\n", YEAR_MIN, YEAR_MAX))
cat(sprintf("  Total conglomerates:  %d\n", nrow(cong_summary)))
cat(sprintf("  Total unit-years:     %.0f\n", total_uy))
cat(sprintf("  Annual type EVER changes (unit-yr weighted):             %.1f%%\n", 100 * pct_change))
cat(sprintf("  Annual type disagrees with full-period (unit-yr wtd):    %.1f%%\n", 100 * pct_disagree))

cat("\nFull-period type distribution:\n")
print(cong_full[, .(n_congs = .N, unit_years = sum(full_units)), keyby = type_full])

cat("\nTransition matrix — counts (type at year t → type at year t+1):\n")
print(dcast(trans_mat, type_t ~ type_t1, value.var = "n", fill = 0L))

cat("\nTransition matrix — unit-years (type at year t → type at year t+1):\n")
print(dcast(trans_mat_wt, type_t ~ type_t1, value.var = "unit_years", fill = 0L))

cat("\nAnnual vs full-period type cross-tab (unit-years):\n")
print(dcast(ann_vs_full, type_annual ~ type_full, value.var = "unit_years", fill = 0L))

cat(sprintf("\nOutputs written to: %s\n", OUT_DIR))
