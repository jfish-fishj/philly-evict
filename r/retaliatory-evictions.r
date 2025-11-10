# ============================================================
# Philly permits × evictions: ingest, clean, panel, regressions
# Memory-conscious version (data.table-heavy)
# ============================================================

# ---- Threads / libs ----
Sys.setenv("OMP_THREAD_LIMIT" = as.integer(parallel::detectCores() %/% 2))
data.table::setDTthreads(parallel::detectCores() %/% 2)

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(lubridate)
  library(fixest)
  library(ggplot2)
  # broom only for tidy(); if you want to drop this dep, replace with manual coeftable parsing
  library(broom)
})

# ---- Helper sourcing (optional) ----
safe_source <- function(f) if (file.exists(f)) source(f, chdir = TRUE)
safe_source("r/helper-functions.R")   # may define business_regex, clean_name, theme_philly_evict

# ---- Helpers (lightweight) ----
to_yq <- function(d) { d <- as.IDate(d); sprintf("%d_Q%d", year(d), quarter(d)) }

if (!exists("business_regex", inherits = TRUE)) {
  business_terms <- c("LLC","L\\.?L\\.?C\\.?","INC","INC\\.","COMPANY","CO\\.","CO\\b",
                      "LP\\b","L\\.?P\\.?","PLC","CORP\\.?","TRUST","ENTERPRISES?",
                      "INVEST(OR|MENTS)?","PROPERT(Y|IES)","HOLDINGS?","MANAGEMENT",
                      "GROUP","LLP","PC\\b","P\\.?C\\.?")
  business_regex <- regex(paste0("(", paste(business_terms, collapse="|"), ")"), ignore_case = TRUE)
}
if (!exists("clean_name", inherits = TRUE)) {
  clean_name <- function(x) {
    x <- toupper(x)
    x <- str_replace_all(x, "[^A-Z0-9\\s]", " ")
    str_squish(x)
  }
}

# ---- Paths ----
permits_path      <- "~/Desktop/data/philly-evict/open-data/building_data_quarter.csv"
parcels_path      <- "~/Desktop/data/philly-evict/processed/parcel_building_2024.csv"
evict_path        <- "~/Desktop/data/philly-evict/evict_address_cleaned.csv"
rentals_panel     <- "~/Desktop/data/philly-evict/processed/final_panel.csv"
rentals_list_path <- "~/Desktop/data/philly-evict/processed/rent_licenses.csv"
xwalk_path        <- "~/Desktop/data/philly-evict/philly_evict_address_agg_xwalk.csv"

# ============================================================
# 1) READ
# ============================================================
permits <- fread(permits_path)
parcels <- fread(parcels_path)
ev      <- fread(evict_path)
rentals_data <- fread(rentals_panel, select = "PID")
rentals_list <- fread(rentals_list_path, select = "PID")
ev_xwalk <- fread(xwalk_path, select = c("PID","num_parcels_matched","n_sn_ss_c"))

# rentals universe
rentals <- unique(rbindlist(list(rentals_data, rentals_list), use.names = TRUE, fill = TRUE))
rm(rentals_data, rentals_list); gc()

# ============================================================
# 2) EVICTIONS: CLEAN + FILTER
# ============================================================
# Pick a date column
date_candidates <- c("d_filing","filing_date","filed_date","date")
have_date <- intersect(names(ev), date_candidates)
if (length(have_date) == 0L) stop("No filing date column found in evictions data.")
date_col_ev <- have_date[1]

d_e <- suppressWarnings(ymd(ev[[date_col_ev]]))
na_mask <- is.na(d_e)
if (any(na_mask)) d_e[na_mask] <- suppressWarnings(ymd_hms(ev[[date_col_ev]][na_mask]))
ev[, date := as.IDate(d_e)]
ev <- ev[!is.na(date)]

# defendant cleanup + commercial flag
if (!"defendant" %in% names(ev)) stop("Column 'defendant' not found in evictions data.")
ev[, clean_defendant_name := clean_name(defendant)]
ev[, commercial_alt := str_detect(clean_defendant_name, business_regex)]

# address key
addr_keys <- intersect(names(ev), c("n_sn_ss_c","normalized_address","address_key"))
if (length(addr_keys) == 0L) stop("Need an address key column (e.g., n_sn_ss_c).")
addr_key <- addr_keys[1]

# dedup (address × plaintiff × defendant × date)
plaintiff_col <- if ("plaintiff" %in% names(ev)) "plaintiff" else addr_key
ev[, dup := .N, by = c(addr_key, plaintiff_col, "clean_defendant_name", date_col_ev)]
ev[, dup := dup > 1]

# additional columns if missing
if (!"total_rent" %in% names(ev)) ev[, total_rent := NA_real_]
if (!"commercial" %in% names(ev)) ev[, commercial := NA_character_]

ev[, year := year(date)]
ev[, year_quarter := to_yq(date)]

# filter
ev <- ev[
  (is.na(commercial) | commercial != "t") &
    commercial_alt == FALSE &
    dup == FALSE &
    year %in% 2006:2024 &
    (is.na(total_rent) | total_rent <= 5e4) &
    !is.na(get(addr_key))
]

# free early columns not used later
ev[, c("dup") := NULL]; gc()

# ============================================================
# 3) EVICTION ↔ PID XWALK AND AGG TO PID×YQ
# ============================================================
ev_xwalk <- ev_xwalk[num_parcels_matched == 1L]
# pad PID
ev_xwalk[, PID := str_pad(PID, 9, "left", "0")]
setnames(ev_xwalk, "n_sn_ss_c", addr_key)

# attach PID to evictions
setkeyv(ev_xwalk, addr_key); setkeyv(ev, addr_key)
ev <- ev[ev_xwalk, on = addr_key, nomatch = 0L]

# aggregate to PID × year_quarter
ev_agg <- ev[!is.na(PID), .(num_evictions = .N), by = .(PID, year_quarter)]

# ---- A) keep a tiny eviction frame for ev_m_par before removing 'ev' ----
# Keep only what we need for ev_m_par
ev_light <- ev[, .(PID, year, year_quarter,
                   plaintiff = get(if ("plaintiff" %in% names(ev)) "plaintiff" else addr_key),
                   defendant, total_rent = if ("total_rent" %in% names(ev)) total_rent else NA_real_,
                   ongoing_rent = if ("ongoing_rent" %in% names(ev)) ongoing_rent else NA_real_,
                   date)]

# Minimal cleaning for rent fields & IDs
ev_light[, PID := stringr::str_pad(PID, 9, "left", "0")]
ev_light[, pid_yq := paste(PID, year_quarter)]


rm(ev, ev_xwalk); gc()

# ============================================================
# 4) PERMITS CLEAN + MERGE EVICTIONS
# ============================================================
# Standardize PID
if ("parcel_number" %in% names(permits)) {
  permits[, PID := str_pad(parcel_number, 9, "left", "0")]
} else if ("PID" %in% names(permits)) {
  permits[, PID := str_pad(PID, 9, "left", "0")]
} else stop("permits needs 'parcel_number' or 'PID'.")

# Normalize period -> year_quarter
if ("period" %in% names(permits)) {
  permits[, year_quarter := str_replace(period, "-", "_")]
} else if (!"year_quarter" %in% names(permits)) {
  stop("permits needs 'period' or 'year_quarter'.")
}

# Keep only rentals PIDs (reduce size before merge)
rentals[, PID := str_pad(PID, 9, "left", "0")]
setkey(permits, PID); setkey(rentals, PID)
permits <- permits[rentals, on = "PID", nomatch = 0L]
rm(rentals); gc()

# merge eviction counts
setkey(permits, PID, year_quarter); setkey(ev_agg, PID, year_quarter)
permits <- ev_agg[permits]                         # left join permits; NA → 0 below
permits[is.na(num_evictions), num_evictions := 0L]
rm(ev_agg); gc()

# year
permits[, year := as.integer(str_sub(year_quarter, 1, 4))]
# filter years now to shrink data before more work
permits <- permits[year %in% 2007:2024]
gc()

# ============================================================
# 5) OUTCOMES + (Severe / Non-Severe) FLAGS
# ============================================================

# outcome
permits[, filed_eviction := as.integer(num_evictions > 0)]

# -- Severe vs Non-Severe complaint families (binary contemporaneous flags) --
# Severe = heat, fire, drainage, property maintenance
permits[, filed_severe := as.integer(
  (get("heat_complaint_count"              ) >= 1) |
    (get("fire_complaint_count"              ) >= 1) |
    (get("drainage_complaint_count"          ) >= 1) |
    (get("property_maintenance_complaint_count") >= 1)
)]

# Non-Severe = building, emergency service, zoning, trash/weeds, license business, program initiative, vacant property
permits[, filed_non_severe := as.integer(
  (get("building_complaint_count"            ) >= 1) |
    (get("emergency_service_complaint_count"   ) >= 1) |
    (get("zoning_complaint_count"              ) >= 1) |
    (get("trash_weeds_complaint_count"         ) >= 1) |
    (get("license_business_complaint_count"    ) >= 1) |
    (get("program_initiative_complaint_count"  ) >= 1) |
    (get("vacant_property_complaint_count"     ) >= 1)
)]

# order by panel keys
setorder(permits, PID, year_quarter)

# convenient horizon for this script
H <- 4L  # 1-year lead/lag window

# lags/leads ONLY for the severe/non-severe indicators
permits[, paste0("lag_severe_",       1:H) := lapply(1:H, function(h) data.table::shift(filed_severe,       n=h, type="lag")),  by = PID]
permits[, paste0("lead_severe_",      1:H) := lapply(1:H, function(h) data.table::shift(filed_severe,       n=h, type="lead")), by = PID]
permits[, paste0("lag_non_severe_",   1:H) := lapply(1:H, function(h) data.table::shift(filed_non_severe,   n=h, type="lag")),  by = PID]
permits[, paste0("lead_non_severe_",  1:H) := lapply(1:H, function(h) data.table::shift(filed_non_severe,  n=h, type="lead")), by = PID]

# Build a compact RHS constructor for severe/non-severe
lead_lag_terms_stem <- function(stem, H) {
  c(paste0("lag_", stem, "_", H:1L), paste0("filed_", stem), paste0("lead_", stem, "_", 1L:H))
}
rhs_from_spec_simple <- function(stem, H, controls = character(0)) {
  paste(c(lead_lag_terms_stem(stem, H), controls), collapse = " + ")
}

# ============================================================
# 6) MERGE PARCEL CHARACTERISTICS (trim to avoid collisions)
# ============================================================
parcels[, PID := str_pad(PID, 9, "left", "0")]
drop_cols <- setdiff(intersect(names(parcels), names(permits)), "PID")
if (length(drop_cols)) parcels[, (drop_cols) := NULL]

setkey(parcels, PID); setkey(permits, PID)
permits <- parcels[permits]  # join parcels onto permits

# small parcel slice (for ev_m_par later)
parcels_min <- copy(parcels[, .(PID, num_units, total_livable_area, market_value, building_code_description_new_fixed)])
parcels_min[, PID := stringr::str_pad(PID, 9, "left", "0")]

rm(parcels); gc()

# light imputations
if (!"num_units" %in% names(permits))           permits[, num_units := NA_real_]
if (!"total_livable_area" %in% names(permits))  permits[, total_livable_area := NA_real_]
if (!"market_value" %in% names(permits))        permits[, market_value := NA_real_]

grp_col <- if ("building_code_description_new_fixed" %in% names(permits)) "building_code_description_new_fixed" else "PID"
permits[, num_units_imp_alt :=
          fifelse(is.na(num_units), mean(num_units, na.rm = TRUE), num_units),
        by = grp_col]

# 'retaliatory' labeling and pid_yq
permits[, pid_yq := paste(PID, year_quarter)]
if (!"filed_complaint" %in% names(permits)) permits[, filed_complaint := as.integer(total_complaints >= 1)]
if (!"lag_complaint_1" %in% names(permits)) permits[, lag_complaint_1  := data.table::shift(filed_complaint, 1L, type="lag"), by=PID]
if (!"lead_complaint_1" %in% names(permits)) permits[, lead_complaint_1 := data.table::shift(filed_complaint, 1L, type="lead"), by=PID]

permits[, retaliatory := fifelse(
  filed_eviction == 1 & filed_complaint == 1, "Retaliatory",
  fifelse(filed_eviction == 1 & (lag_complaint_1 == 1 | lead_complaint_1 == 1), "Pluasibly Retaliatory",
          fifelse(filed_eviction == 1, "Non-Retaliatory", NA_character_))
)]
ret_tab <- permits[, .(pid_yq, retaliatory)]
setkey(ret_tab, pid_yq)

gc()

# ============================================================
# 7) REGRESSION SPEC
# ============================================================
REG_Y        <- "filed_eviction"
REG_CONTROLS <- c()   # e.g., c("log1p(total_livable_area)", "log1p(market_value)")
REG_FE       <- c("year_quarter", "PID")
REG_CLUSTER  <- "~ PID"
REG_WEIGHTS  <- NULL

# cast FE as factors
for (fe in REG_FE) if (fe %in% names(permits) && !is.factor(permits[[fe]])) permits[, (fe) := as.factor(get(fe))]

build_fml <- function(y, x_string, fe_vec) {
  fe_string <- if (length(fe_vec)) paste(fe_vec, collapse = " + ") else ""
  as.formula(if (nzchar(fe_string)) sprintf("%s ~ %s | %s", y, x_string, fe_string)
             else sprintf("%s ~ %s", y, x_string))
}

# ============================================================
# 8) DEFINE SAMPLES
# ============================================================
# base_keep <- permits[total_livable_area > 1 & total_livable_area < 5e6 & !is.na(PID)]
# dt_full <- base_keep[]
# dt_pre  <- base_keep[year <= 2019]
# dt_post <- base_keep[year >  2021]
rm(permits); gc()

# ============================================================
# 9) DISTRIBUTED-LAG REGS with SEVERE / NON-SEVERE
# ============================================================
# Severe DL
m_severe <- feols(
  build_fml(REG_Y, rhs_from_spec_simple("severe", H, REG_CONTROLS), REG_FE),
  data    = dt_pre,
  cluster = as.formula(REG_CLUSTER),
  weights = if (is.null(REG_WEIGHTS)) NULL else as.formula(REG_WEIGHTS)
)

# Non-Severe DL
m_non_severe <- feols(
  build_fml(REG_Y, rhs_from_spec_simple("non_severe", H, REG_CONTROLS), REG_FE),
  data    = dt_pre,
  cluster = as.formula(REG_CLUSTER),
  weights = if (is.null(REG_WEIGHTS)) NULL else as.formula(REG_WEIGHTS)
)

cat("\n==== ETable: Distributed lags (Pre-COVID) ====\n")
print(etable(list(Severe = m_severe, NonSevere = m_non_severe),
             se.below = TRUE, digits = 3, signif.code = TRUE))

# Compare plots
coef_df <- rbind(
  broom::tidy(m_severe)[, `:=`(family = "Severe")],
  broom::tidy(m_non_severe)[, `:=`(family = "Non-Severe")]
)
coef_df <- coef_df[
  grepl("^(lag_severe_|lead_severe_|filed_severe$|lag_non_severe_|lead_non_severe_|filed_non_severe$)", term)
]
coef_df[, timing := fifelse(grepl("^lag_", term), -as.integer(stringr::str_extract(term, "\\d+$")),
                            fifelse(grepl("^lead_", term),  as.integer(stringr::str_extract(term, "\\d+$")), 0L))]
setorder(coef_df, family, timing)

theme_tpl <- if (exists("theme_philly_evict")) theme_philly_evict() else theme_minimal()
p_dl <- ggplot(coef_df, aes(x = timing, y = estimate, color = family)) +
  geom_point() +
  geom_errorbar(aes(ymin = estimate - 1.96 * std.error, ymax = estimate + 1.96 * std.error), width = 0.2) +
  scale_x_reverse(breaks = -H:H) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  labs(title = "Distributed Lags (Pre-COVID): Severe vs Non-Severe",
       caption = "Severe: heat, fire, drainage, property maintenance; Non-severe: building, emergency service, zoning, trash/weeds, license business, program initiative, vacant property.",
       x = "Quarters relative to complaint", y = "Coefficient") +
  theme_tpl
print(p_dl)

# ============================================================
# 10) LP-DiD with 1-year fade-out (using lpdid)
# ============================================================
# Ensure package (safe install if missing)
if (!requireNamespace("lpdid", quietly = TRUE)) {
  tryCatch({
    if (!requireNamespace("devtools", quietly = TRUE)) install.packages("devtools")
    devtools::install_github("alexCardazzi/lpdid")
  }, error = function(e) message("Install lpdid manually if needed: alexCardazzi/lpdid"))
}
library(lpdid)

# prepare panel for lpdid
#DTlp <- copy(dt_pre)
DTlp = base_keep[year <= 2019]#[PID %in% sample(PID, 1000)]
DTlp[, year_quarter := as.factor(year_quarter)]  # time FE handled internally
# convert year_quarter to time index
DTlp[,t_idx := as.integer(as.factor(year_quarter))]
horiz <- 0:4  # 1 year in quarters
DTlp_sample = DTlp[]
# any complaint as treatment
fit_lp_comp <- lpdid(
  df       = DTlp_sample,
  y          = "filed_eviction",
  treat_status          = "filed_complaint",
  unit_index         = "PID",
  time_index          = "t_idx",
  nonabsorbing_lag          = 6,
  window = c(-4,4),
  #ylags      = 1,                             # include ∆y lag
  #covariates = REG_CONTROLS,                  # optional level covariates
  cluster    = "PID"                          # cluster by PID
)

# Severe as treatment
fit_lp_sev <- lpdid(
  df       = DTlp,
  y          = "filed_eviction",
  treat_status          = "filed_severe",
  unit_index         = "PID",
  time_index          = "t_idx",
  nonabsorbing_lag          = 6,
  window = c(-4,4),
  #ylags      = 1,                             # include ∆y lag
  #covariates = REG_CONTROLS,                  # optional level covariates
  cluster    = "PID"                          # cluster by PID
)

# Non-Severe as treatment
fit_lp_nsev <-lpdid(
  df       = DTlp,
  y          = "filed_eviction",
  treat_status          = "filed_non_severe",
  unit_index         = "PID",
  time_index          = "t_idx",
  nonabsorbing_lag          = 6,
  window = c(-4,4),
  #ylags      = 1,                             # include ∆y lag
  #covariates = REG_CONTROLS,                  # optional level covariates
  cluster    = "PID"                          # cluster by PID
)

# Tidy IRFs (package returns standard elements; fall back to broom if needed)
tidy_lp <- function(fit) {
  df = fit$coeftable
  data.table(
    h    = as.integer(rownames(df)) -5,
    beta = df[, "Estimate"],
    lo   = df[, "Estimate"] - 1.96 * df[, "Std. Error"],
    hi   = df[, "Estimate"] + 1.96 * df[, "Std. Error"]
  )
}

irf_sev  <- tidy_lp(fit_lp_sev)[, family := "Severe"]
irf_nsev <- tidy_lp(fit_lp_nsev)[, family := "Non-Severe"]
irf_both <- rbind(irf_sev, irf_nsev)

p_lp <- ggplot(irf_both, aes(x = h, y = beta, color = family)) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_point() +
  geom_errorbar(aes(ymin = lo, ymax = hi), width = 0.15) +
  scale_x_continuous(breaks = -4:4) +
  scale_y_continuous(limits = c(-0.03, 0.09), breaks = seq(-0.03, 0.09,0.01)) +
  labs(x = "Horizon h (quarters)", y = "Effect on Δ^h filed_eviction",
       subtitle = "6-quarter fade out",
       caption = "Severe is heat, fire, drainage, property maintenance; Non-severe is building, emergency service, zoning, trash/weeds, license business, program initiative, vacant property.",
       title = "Local Projections DiD (Pre-COVID): Severe vs Non-Severe") +
  theme_philly_evict()
print(p_lp)

# save
ggsave(
  filename = "/Users/joefish/Documents/GitHub/philly-evictions/figs/lp_did_severe_nonsevere.png",
  plot     = p_lp,
  width    = 10,
  height   = 7,
  dpi      = 300,
  bg = "white"
)

# do just complaints
irf_comp  <- tidy_lp(fit_lp_comp)[, family := "Any Complaint"]
p_lp_comp <- ggplot(irf_comp, aes(x = h, y = beta))
p_lp_comp <- p_lp_comp +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_point() +
  geom_errorbar(aes(ymin = lo, ymax = hi), width = 0.15) +
  scale_x_continuous(breaks = -4:4) +
  scale_y_continuous(limits = c(-0.03, 0.09), breaks = seq(-0.03, 0.09,0.01)) +
  labs(x = "Horizon h (quarters)", y = "Effect on Δ^h filed_eviction",
       subtitle = "6-quarter fade out",
       title = "Local Projections DiD (Pre-COVID): Any Complaint") +
  theme_philly_evict()

print(p_lp_comp)

# (Optional) save tables/plots here as needed
# save
ggsave(
  filename = "/Users/joefish/Documents/GitHub/philly-evictions/figs/lp_did_any_complaint.png",
  plot     = p_lp_comp,
  width    = 10,
  height   = 7,
  dpi      = 300,
  bg = "white"
)
permits[,t_idx := as.integer(as.factor(year_quarter))]
permits_sample = permits[PID %in% sample(PID, 1000)]

# now fit on retaliation
fit_lp_retaliatory <- lpdid(
  df       = permits_sample,
  y          = "per_no_grace",
  treat_status          = "retaliatory",
  unit_index         = "PID",
  time_index          = "t_idx",
  nonabsorbing_lag          = 12,
  window = c(-4,8),
  #ylags      = 1,                             # include ∆y lag
  #covariates = REG_CONTROLS,                  # optional level covariates
  cluster    = "PID"                          # cluster by PID
)

# now plot retaliatory
irf_retaliatory  <- tidy_lp(fit_lp_retaliatory)[, family := "Retaliatory Status"]
p_lp_retaliatory <- ggplot(irf_retaliatory, aes(x = h, y = beta)) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_point() +
  geom_errorbar(aes(ymin = lo, ymax = hi), width = 0.15) +
  scale_x_continuous(breaks = -4:4) +
  #scale_y_continuous(limits = c(-0.03, 0.09), breaks = seq(-0.03, 0.09,0.01)) +
  labs(x = "Horizon h (quarters)", y = "Effect on Δ^h",
       subtitle = "Local Projections DiD (Pre-COVID): 6-quarter fade out",
       title = "Percent of Cases for <= 1 month backrent") +
  theme_philly_evict()

print(p_lp_retaliatory)

# save
ggsave(
  filename = "/Users/joefish/Documents/GitHub/philly-evictions/figs/lp_did_retaliatory_status.png",
  plot     = p_lp_retaliatory,
  width    = 10,
  height   = 7,
  dpi      = 300,
  bg = "white"
)

# mean number of permits by year built + market value + building type bin
permits_agg = permits[,list(
  total_permits = sum(total_permits),
  electrical_permit_count = sum(electrical_permit_count),
  mechanical_permit_count = sum(mechanical_permit_count),
  residential_building_permit_count = sum(residential_building_permit_count),
  plumbing_permit_count = sum(plumbing_permit_count),
  market_value = mean(market_value, na.rm=TRUE),
  total_livable_area = mean(total_livable_area, na.rm=TRUE),
  total_area = mean(total_area, na.rm=TRUE),
  num_units_imp_alt = mean(num_units_imp_alt, na.rm=TRUE),
  num_evictions = sum(num_evictions, na.rm=TRUE),
  building_code_description_new_fixed = first(building_code_description_new_fixed),
  year_built = first(year_built),
  quality_grade_fixed = first(quality_grade_fixed),
  num_years = .N
), by = PID]

permits_agg[,eviction_filings_per_unit := num_evictions / num_units_imp_alt]
permits_agg[,high_filing := eviction_filings_per_unit > quantile(eviction_filings_per_unit, 0.75, na.rm=TRUE)]
permits_agg[,year_built_decade := floor(year_built / 10) * 10]
fixest::fepois(
  total_permits ~ high_filing + log(market_value) + log(total_livable_area) + log(num_units_imp_alt) |  year_built_decade+ building_code_description_new_fixed + quality_grade_fixed,
  data = permits_agg[]
)

fixest::fepois(
  residential_building_permit_count ~ high_filing + log(market_value) + log(total_livable_area) + log(num_units_imp_alt) |  year_built_decade+ building_code_description_new_fixed + quality_grade_fixed,
  data = permits_agg[]
)

fixest::fepois(
  mechanical_permit_count ~ high_filing + log(market_value) + log(total_livable_area) + log(num_units_imp_alt) |  year_built_decade+ building_code_description_new_fixed + quality_grade_fixed,
  data = permits_agg[]
)

fixest::fepois(
  electrical_permit_count ~ high_filing + log(market_value) + log(total_livable_area) + log(num_units_imp_alt) |  year_built_decade+ building_code_description_new_fixed + quality_grade_fixed,
  data = permits_agg[]
)
rentals_agg = rentals_data[year <= 2019,list(
  log_med_rent = mean(log_med_rent, na.rm=TRUE),
  year = round(mean(year))
), by=.(PID = as.integer(PID))]
# merge rental data with permits
rental_m = merge(
  rentals_agg,
  permits_agg %>% mutate(PID = as.integer(PID) ), by = "PID"
)

fixest::fepois(
  total_permits ~ log_med_rent + high_filing + log(market_value) + log(total_livable_area) + log(num_units_imp_alt) | year+ year_built_decade+ building_code_description_new_fixed + quality_grade_fixed,
  data = rental_m[]
)


# ============================================================
# APPENDIX (OPTIONAL): Per-complaint lags/leads (moved to end)
# ============================================================
# If you still want the old per-complaint mk_lags_leads, keep it here and run after all above.
# complaint_stems <- c("heat_complaint","emergency_service_complaint","fire_complaint",
#   "building_complaint","complaint","vacant_property_complaint","drainage_complaint",
#   "property_maintenance_complaint","zoning_complaint","trash_weeds_complaint",
#   "license_business_complaint","program_initiative_complaint")
# mk_lags_leads <- function(DT, stem, H=5L) {
#   lag_names  <- paste0("lag_",  stem, "_", 1:H)
#   lead_names <- paste0("lead_", stem, "_", 1:H)
#   filed_col  <- paste0("filed_", stem)
#   if (!filed_col %in% names(DT)) DT[, (filed_col) := 0L]
#   for (h in 1:H) {
#     DT[, (lag_names[h])  := data.table::shift(get(filed_col),  n=h, type="lag"),  by=PID]
#     DT[, (lead_names[h]) := data.table::shift(get(filed_col),  n=h, type="lead"), by=PID]
#   }
# }
# for (s in complaint_stems) mk_lags_leads(base_keep, s, H=5L)


# ============================================================
# 6) MERGE PARCEL CHARACTERISTICS (trim to avoid collisions)
# ============================================================
parcels[, PID := str_pad(PID, 9, "left", "0")]
# drop overlapping columns (except PID) in parcels IN-PLACE before merge
drop_cols <- setdiff(intersect(names(parcels), names(permits)), "PID")
if (length(drop_cols)) parcels[, (drop_cols) := NULL]

setkey(parcels, PID); setkey(permits, PID)
permits <- parcels[permits]  # join parcels onto permits
# ---- B) small parcel slice used later for ev_m_par ----
parcels_min <- parcels[, .(PID,
                           num_units,
                           total_livable_area,
                           market_value,
                           building_code_description_new_fixed)]
parcels_min[, PID := stringr::str_pad(PID, 9, "left", "0")]


rm(parcels); gc()

# imputations (light)
if (!"num_units" %in% names(permits)) permits[, num_units := NA_real_]
if (!"total_livable_area" %in% names(permits)) permits[, total_livable_area := NA_real_]
if (!"market_value" %in% names(permits)) permits[, market_value := NA_real_]

grp_col <- if ("building_code_description_new_fixed" %in% names(permits)) "building_code_description_new_fixed" else "PID"
permits[, num_units_imp_alt :=
          fifelse(is.na(num_units), mean(num_units, na.rm = TRUE), num_units),
        by = grp_col]

gc()
# ---- C) 'retaliatory' labeling off the permits panel ----
# Ensure these exist (you already built them a few steps earlier)
if (!"filed_complaint" %in% names(permits)) permits[, filed_complaint := 0L]
if (!"filed_eviction" %in% names(permits))  permits[, filed_eviction := as.integer(num_evictions > 0)]
if (!"lag_complaint_1" %in% names(permits)) permits[, lag_complaint_1 := data.table::shift(filed_complaint, 1L, type = "lag"), by = PID]
if (!"lead_complaint_1" %in% names(permits)) permits[, lead_complaint_1 := data.table::shift(filed_complaint, 1L, type = "lead"), by = PID]

permits[, pid_yq := paste(PID, year_quarter)]
permits[, retaliatory := fifelse(
  filed_eviction == 1 & filed_complaint == 1, "Retaliatory",
  fifelse(filed_eviction == 1 & (lag_complaint_1 == 1 | lead_complaint_1 == 1), "Pluasibly Retaliatory",
          fifelse(filed_eviction == 1, "Non-Retaliatory", NA_character_))
)]
ret_tab <- permits[, .(pid_yq, retaliatory)]
setkey(ret_tab, pid_yq)



# ============================================================
# 7) STANDARDIZED REGRESSION BLOCK (single-edit spec)
# ============================================================

# ===== REGRESSION SPEC — EDIT HERE =====
REG_Y        <- "filed_eviction"
REG_CONTROLS <-c() #c("log1p(total_livable_area)", "log1p(market_value)")
REG_FE       <-c("year_quarter","PID")
# If you want PID FE globally, add "PID" to REG_FE
REG_CLUSTER  <- "~PID"
REG_WEIGHTS  <- NULL      # e.g., "~num_units" or NULL
H            <- 4L        # lead/lag horizon used in models & plots
# =======================================

# cast FE as factors (in-place)
for (fe in REG_FE) if (fe %in% names(permits) && !is.factor(permits[[fe]])) permits[, (fe) := as.factor(get(fe))]

# Build RHS per family
lead_lag_terms <- function(stem, H) c(paste0("lag_", stem, "_", H:1L),
                                      paste0("filed_", stem),
                                      paste0("lead_", stem, "_", 1L:H))
rhs_from_spec <- function(stem, H, controls) paste(c(lead_lag_terms(stem, H), controls), collapse = " + ")
fe_part <- function(FE) if (length(FE)) paste(FE, collapse = " + ") else ""
build_fml <- function(y, x_string, fe_vec) {
  fe_string <- fe_part(fe_vec)
  as.formula(if (nzchar(fe_string)) sprintf("%s ~ %s | %s", y, x_string, fe_string)
             else sprintf("%s ~ %s", y, x_string))
}

# Complaint families (order is output order)
complaint_families <- data.table(
  name = c("Heat","Fire","Building","Any","Zoning","Vacant","Drainage","PropertyMaint",
           "EmergencyService","TrashWeeds","LicenseBusiness","ProgramInitiative"
           ),
  base = c("heat_complaint","fire_complaint","building_complaint","complaint",
           "zoning_complaint","vacant_property_complaint","drainage_complaint",
           "property_maintenance_complaint",  "emergency_service_complaint",
           "trash_weeds_complaint","license_business_complaint",
           "program_initiative_complaint"

           )
)

fit_family <- function(dt, fam_base) {
  x_str <- rhs_from_spec(fam_base, H, REG_CONTROLS)
  fml  <- build_fml(REG_Y, x_str, REG_FE)
  feols(
    fml,
    data    = dt,
    cluster = as.formula(REG_CLUSTER),
    weights = if (is.null(REG_WEIGHTS)) NULL else as.formula(REG_WEIGHTS)
  )
}

coef_plot <- function(model, base_stem, title, subtitle = NULL) {
  theme_tpl <- if (exists("theme_philly_evict")) theme_philly_evict() else theme_minimal()
  td <- broom::tidy(model)
  # keep only stem terms
  td <- td[str_detect(td$term, base_stem), ]
  if (nrow(td) == 0L) return(ggplot() + labs(title = paste(title, "(no matched terms)")) + theme_tpl)

  # build timing from term
  timing <- rep(NA_integer_, nrow(td))
  lag_idx  <- str_detect(td$term, "^lag_")
  lead_idx <- str_detect(td$term, "^lead_")
  zero_idx <- str_detect(td$term, paste0("^filed_", base_stem, "$"))
  timing[lag_idx]  <- -as.integer(str_extract(td$term[lag_idx], "\\d+$"))
  timing[lead_idx] <-  as.integer(str_extract(td$term[lead_idx], "\\d+$"))
  timing[zero_idx] <- 0L

  td$timing <- timing
  setDT(td); setorder(td, timing)

  ggplot(td, aes(x = timing, y = estimate)) +
    geom_point() +
    geom_errorbar(aes(ymin = estimate - 1.96 * std.error,
                      ymax = estimate + 1.96 * std.error), width = 0.2) +
    scale_y_continuous() +
    scale_x_reverse(breaks = -H:H) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    labs(title = title, subtitle = subtitle, x = "Quarters relative to complaint", y = "Coefficient") +
    theme_tpl
}

run_block <- function(dt, label_for_titles, subtitle_for_plots = NULL) {
  # Fit in declared order
  models <- vector("list", nrow(complaint_families))
  names(models) <- complaint_families$name
  for (i in seq_len(nrow(complaint_families))) {
    base_i <- complaint_families$base[i]
    models[[i]] <- fit_family(dt, base_i)
    gc()
  }

  # Plots (on-demand)
  plots <- vector("list", nrow(complaint_families))
  names(plots) <- complaint_families$name
  for (i in seq_len(nrow(complaint_families))) {
    plots[[i]] <- coef_plot(models[[i]],
                            base_stem = complaint_families$base[i],
                            title     = sprintf("%s — %s", names(models)[i], label_for_titles),
                            subtitle  = subtitle_for_plots)
  }

  et <- fixest::etable(models,
                       order = names(models),
                       se.below = TRUE,
                       digits = 3)

  list(models = models, plots = plots, etable = et)
}

# ============================================================
# 8) DEFINE SAMPLES (shrink early to save memory)
# ============================================================
# global base filter
base_keep <- permits[
  total_livable_area > 1 & total_livable_area < 5e6 & !is.na(PID)
]
rm(permits); gc()
# split
dt_full <- base_keep[]
dt_pre  <- base_keep[year <= 2019]
dt_post <- base_keep[year > 2021]
# free master after split if tight on RAM
# rm( base_keep); gc()

# ============================================================
# 9) RUN & OUTPUT
# ============================================================
blk_full <- run_block(dt_full, "Full sample (2007–2024)")
blk_pre  <- run_block(dt_pre,  "Pre-COVID (≤2019)", "Positive quarters = future complaints")
#blk_post <- run_block(dt_post, "Post-2021 (>2021)", "Positive quarters = future complaints")

# free split data if not needed further
rm(dt_full, dt_pre, dt_post); gc()

cat("\n==== ETable: Full sample ====\n");  print(blk_full$etable)
cat("\n==== ETable: Pre-COVID ====\n");   print(blk_pre$etable)
cat("\n==== ETable: Post-2021 ====\n");  print(blk_post$etable)

# ------- Optional: save etables/plots (commented) -------
# etable(blk_full$models, file = "etable_full.html")
# etable(blk_pre$models,  file = "etable_pre.html")
# etable(blk_post$models, file = "etable_post.html")
# for (nm in names(blk_full$plots)) ggsave(sprintf("coef_full_%s.png", nm), blk_full$plots[[nm]], width=7, height=4, dpi=300)
# for (nm in names(blk_pre$plots))  ggsave(sprintf("coef_pre_%s.png",  nm), blk_pre$plots[[nm]],  width=7, height=4, dpi=300)
# for (nm in names(blk_post$plots)) ggsave(sprintf("coef_post_%s.png", nm), blk_post$plots[[nm]], width=7, height=4, dpi=300)

# ============================================================
# make_block_outputs(): combine plots + genericize etable names
# ============================================================
# Inputs:
#   block : list(models, plots, etable) returned by run_block()
#   label : character, shown in the plot title/subtitle
# Returns:
#   list(plot = ggplot object, etable = fixest etable)
#
make_block_outputs <- function(block, label = NULL) {
  stopifnot(is.list(block), !is.null(block$models))
  models <- block$models
  fam_names <- names(models)
  if (is.null(fam_names) || anyNA(fam_names)) fam_names <- paste0("M", seq_along(models))

  # === 1) Build one combined coefficient-data.frame for plotting ===
  # tidy all models and parse timing from term names
  all_dt <- data.table::rbindlist(
    lapply(seq_along(models), function(i) {
      m <- models[[i]]
      td <- broom::tidy(m)
      if (!nrow(td)) return(NULL)

      # keep only lead/lag/impact terms
      keep <- grepl("^(lag_|lead_|filed_)", td$term)
      td <- td[keep, , drop = FALSE]
      if (!nrow(td)) return(NULL)

      # timing: lag_k -> -k; lead_k -> +k; filed_* -> 0
      timing <- rep(NA_integer_, nrow(td))
      lag_idx  <- grepl("^lag_",  td$term)
      lead_idx <- grepl("^lead_", td$term)
      zero_idx <- grepl("^filed_", td$term)

      # extract trailing integer for lag/lead
      get_k <- function(x) {
        k <- stringr::str_extract(x, "\\d+$")
        as.integer(k)
      }
      timing[lag_idx]  <- -get_k(td$term[lag_idx])
      timing[lead_idx] <-  get_k(td$term[lead_idx])
      timing[zero_idx] <-  0L

      data.table::data.table(
        family   = fam_names[i],
        term     = td$term,
        estimate = td$estimate,
        std.error= td$std.error,
        timing   = timing
      )
    }),
    use.names = TRUE, fill = TRUE
  )
  if (is.null(all_dt) || !nrow(all_dt)) {
    warning("No lead/lag terms found across models; returning empty plot and original etable.")
    return(list(plot = ggplot() + theme_minimal(), etable = block$etable))
  }

  # order for nicer lines
  data.table::setorder(all_dt, family, timing)
  all_dt[,severe := fifelse(str_detect(family, "Fire|Heat|PropertyMain|Drainage"), "Severe Complaint", "non-Severe Complaint")]

  # pick end points for simple text labels (last available timing per family)
  end_pts <- all_dt[, .SD[.N], by = family]  # last row within each family

  # choose theme if available
  theme_tpl <- if (exists("theme_philly_evict")) theme_philly_evict() else ggplot2::theme_minimal()

  plt <- ggplot2::ggplot(all_dt, ggplot2::aes(x = timing, y = estimate, group = family, color = family)) +
    ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
    ggplot2::geom_line() +
    ggplot2::geom_point() +
    ggplot2::geom_errorbar(
      ggplot2::aes(ymin = estimate - 1.96 * std.error,
                   ymax = estimate + 1.96 * std.error),
      width = 0.2, alpha = 0.6
    ) +
    # annotate which are severe / non-severe in upper right corner
    ggplot2::scale_x_reverse(breaks = sort(unique(all_dt$timing))) +
    ggplot2::labs(
      title = if (is.null(label)) "Lead/Lag Coefficients" else paste0("Lead/Lag Coefficients — ", label),
      x = "Quarters relative to complaint",
      y = "Coefficient"
    ) +
    ggplot2::guides(linetype = "none", shape = "none") +  # legend replaced by end labels
    theme_tpl +
    ggplot2::coord_cartesian(clip = "off") +
    ggplot2::theme(plot.margin = ggplot2::margin(5.5, 30, 5.5, 5.5)) + # room for right-side labels
    facet_wrap(~severe, ncol=2)
  # === 2) Rebuild etable with generic lead/lag names ===
  # Build a dict that maps e.g. 'lag_fire_complaint_3' -> 'lag_3', 'lead_heat_complaint_2' -> 'lead_2',
  # and 'filed_*' -> 'lag_0'
  unique_terms <- unique(unlist(lapply(models, function(m) rownames(fixest::coeftable(m)))))
  # some models may include FEs etc. coeftable rows are only coefficients, so this is fine.

  map_term <- function(term) {
    if (grepl("^lag_.*_(\\d+)$", term)) {
      k <- stringr::str_replace(term, "^lag_.*_(\\d+)$", "\\1")
      return(paste0("lag_", k))
    }
    if (grepl("^lead_.*_(\\d+)$", term)) {
      k <- stringr::str_replace(term, "^lead_.*_(\\d+)$", "\\1")
      return(paste0("lead_", k))
    }
    if (grepl("^filed_", term)) {
      return("lag_0")
    }
    # keep other covariates / controls as-is
    term
  }
  dict_vec <- stats::setNames(vapply(unique_terms, map_term, character(1L)), unique_terms)

  # Fresh etable using the dict
  et_generic <- fixest::etable(
    models,
    dict = dict_vec,
    order = names(models),
    se.below = TRUE,
    digits = 3
  )

  list(plot = plt, etable = et_generic)
}

out_pre  <- make_block_outputs(blk_pre,  label = "Pre-COVID (≤2019)")
out_pre$etable
end_pts = out_pre$plot$data[, .SD[.N], by = family]
# make the severe non-severe string as a paste(collapse = "\n)
end_pts_str = out_pre$plot$data[, .(severe_str = paste0(unique(family), collapse = "\n")), by = severe]
end_pts_str[,family := NA]
out_pre$plot +
  #scale_x_reverse(breaks = seq(-4,4)) +
  geom_vline(aes(xintercept = 1), linetype=2)+
  ggplot2::geom_text(
    data = end_pts_str,
    ggplot2::aes(
      x = 4,  # nudge right
      y = 0.15,
      label = severe_str,

    ),
    hjust = 0,
    vjust = 1,
    size = 4,
    show.legend = FALSE,
    color = "Black"
  )
  # break legend into severe / non-sever

#### no grace ####
# ============================================================
# No-grace (indicator) block: clustered means, plot, regression
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(purrr)
  library(tidyr)
  library(broom)
  library(forcats)
  library(ggplot2)
  library(ggrepel)
  library(fixest)
})

make_no_grace_block <- function(
    ev_m_par,
    years        = 2010:2019,
    bins_order   = c("1","2-4","5-9","10-19","20-49","50-99","100-199","200+"),
    keep_groups  = c("Non-Retaliatory","Pluasibly Retaliatory","Retaliatory"),
    ref_group    = "Non-Retaliatory",
    ref_bin      = "1"
) {
  # Ensure num_units_bins exists; if not, create the standard one
  if (!"num_units_bins" %in% names(ev_m_par)) {
    ev_m_par <- ev_m_par %>%
      mutate(num_units_bins = cut(
        num_units_imp_alt %||% num_units,
        breaks = c(-Inf,1,4,9,19,49,99,199, Inf),
        labels = c("1","2-4","5-9","10-19","20-49","50-99","100-199","200+"),
        ordered_result = TRUE
      ))
  }

  # 0) Filter + factor setup
  dat0 <- ev_m_par %>%
    filter(year %in% years) %>%
    filter(!is.na(no_grace), !is.na(num_units_bins), retaliatory %in% keep_groups) %>%
    mutate(
      units_bin   = factor(as.character(num_units_bins), levels = bins_order, ordered = TRUE),
      retaliatory = factor(retaliatory, levels = keep_groups)
    )

  # --- helper: mean with PID-clustered CI via feols(y ~ 1) ---
  fit_mean_ci <- function(df) {
    m <- feols(no_grace ~ 1, cluster = ~ PID, data = df)
    out <- tidy(m, conf.int = TRUE)
    tibble(
      mean = out$estimate[1],
      lo   = out$conf.low[1],
      hi   = out$conf.high[1],
      n    = nrow(df),
      n_pid= dplyr::n_distinct(df$PID)
    )
  }

  # 1) Clustered means for every (bin × group)
  means_df <- dat0 %>%
    group_by(units_bin, retaliatory) %>%
    group_modify(~ fit_mean_ci(.x)) %>%
    ungroup()

  # 2) Baseline per bin (weighted by cell size, like your GRACE code)
  baseline <- means_df %>%
    group_by(units_bin) %>%
    summarise(base_mean = weighted.mean(mean, w = n), .groups = "drop")

  plot_df <- means_df %>%
    left_join(baseline, by = "units_bin") %>%
    mutate(delta_vs_base = mean - base_mean)

  # 3) Plot
  pd <- position_dodge(width = 0.55)
  theme_tpl <- if (exists("theme_philly_evict")) theme_philly_evict() else theme_minimal(base_size = 12)

  p <- ggplot(plot_df, aes(x = units_bin, y = mean, color = retaliatory)) +
    geom_hline(yintercept = mean(plot_df$mean), linetype = 2, linewidth = 0.3) +
    geom_errorbar(aes(ymin = lo, ymax = hi), width = 0, position = pd) +
    geom_point(size = 2.2, position = pd) +
    geom_point(
      data = plot_df,
      aes(x = units_bin, y = base_mean),
      inherit.aes = FALSE, shape = 21, size = 3, stroke = 0.7, fill = NA, color = "black"
    ) +
    ggrepel::geom_text_repel(
      data = plot_df %>% filter(retaliatory != ref_group),
      aes(label = scales::label_number(accuracy = 0.1, suffix = " pp")(100 * delta_vs_base)),
      position = position_dodge(width = 1.55),
      vjust = -0.9, size = 3, show.legend = FALSE
    ) +
    scale_color_discrete(NULL) +
    labs(
      x = "Number of Units (bins)",
      y = "NO GRACE (group mean, PID-clustered 95% CI)",
      title = "No-Grace by Retaliation Status within Unit Bins",
      subtitle = "Open black circle = baseline mean per bin; labels = difference vs baseline (percentage points)",
      caption = "Means/CIs from feols(no_grace ~ 1, cluster = ~PID) run within each bin × group."
    ) +
    theme_tpl +
    theme(legend.position = "top")

  # 4) Regression with bin × group interactions (LPM, PID FE optional via your upstream pipeline)
  #    Ref levels set by factors above; this uses ref_group as the base in i()
  dat0 <- dat0 %>%
    mutate(
      units_bin   = fct_relevel(units_bin, ref_bin),
      retaliatory = fct_relevel(retaliatory, ref_group)
    )

  m_no_grace <- feols(
    no_grace ~ i(units_bin, retaliatory, ref = ref_bin) | PID,
    data = dat0,
    cluster = ~ PID
    # optionally: family = "binomial"  # but LPM is fine w/ clustered SEs
  )

  et_no_grace <- etable(m_no_grace, se.below = TRUE, digits = 3)

  list(
    data      = dat0,
    means_df  = means_df,
    plot_df   = plot_df,
    plot      = p,
    model     = m_no_grace,
    etable    = et_no_grace
  )
}

# ===== Example usage =====
# ---- D) ev_m_par: merge evictions + parcels + retaliatory labels ----
# Backrent logic
ev_light[, months_backrent := fifelse(!is.na(ongoing_rent) & ongoing_rent > 0, total_rent / ongoing_rent, NA_real_)]
ev_light[, no_grace := as.integer(months_backrent <= 1)]
ev_light[, grace    := as.integer(months_backrent > 1)]
ev_light[, missing_rent := as.integer(is.na(ongoing_rent) | ongoing_rent == 0)]

# Optional plaintiff standardization
plaintiff_clean_fun <- if (exists("standardize_owner_names")) standardize_owner_names else clean_name
ev_light[, plaintiff_clean := plaintiff_clean_fun(plaintiff)]

# Join parcel attributes (units, area, value)
setkey(parcels_min, PID); setkey(ev_light, PID)
ev_m_par <- parcels_min[ev_light, on = "PID"]

# Impute units (by building type if available, otherwise PID)
grp_col_ev <- if ("building_code_description_new_fixed" %in% names(ev_m_par)) "building_code_description_new_fixed" else "PID"
ev_m_par[, num_units_imp_alt := fifelse(is.na(num_units), mean(num_units, na.rm = TRUE), num_units), by = grp_col_ev]

# Unit bins
ev_m_par[, num_units_bins := cut(
  num_units_imp_alt,
  breaks = c(-Inf,1,4,9,19,49, Inf),
  labels = c("1","2-4","5-9","10-19","20-49","50+"),
  ordered_result = TRUE
)]

# Attach retaliatory label from the permits panel
setkey(ev_m_par, pid_yq)
ev_m_par <- ret_tab[ev_m_par, on = "pid_yq"]

# Tidy the outcome & year filter if you want a default narrow set later
# (You can skip filtering here; your plotting/helper handles years.)
# ev_m_par <- ev_m_par[year %in% 2007:2019]

# attach retaliatory aggs to ret_tab
ev_m_par_agg = ev_m_par[,list(any_no_grace = as.integer(any(no_grace == 1)),
                              sum_no_grace = sum(no_grace==1) ), by = .(PID, year_quarter)]
# Housekeeping (free sources you no longer need)
rm(ret_tab, ev_light)  # keep parcels_min if you want it elsewhere
gc()

permits <- merge(permits, ev_m_par_agg, by = c("PID","year_quarter"), all.x = TRUE)
permits[,any_no_grace := fifelse(is.na(any_no_grace), 0L, any_no_grace)]
permits[,sum_no_grace := fifelse(is.na(sum_no_grace), 0L, sum_no_grace)]
permits[,per_no_grace := fifelse(num_evictions > 0, sum_no_grace / num_evictions, 0)]
grace_model <- feols(per_no_grace ~lead_severe_4 +
       lead_severe_3 + lead_severe_2  +
        filed_severe + lag_severe_1 + lag_severe_2 + lag_severe_3 + lag_severe_4 | year_quarter + PID,
      data = permits[year <= 2019], cluster = ~PID)

coefplot(grace_model)

out_ng <- make_no_grace_block(ev_m_par)
print(out_ng$etable)
print(out_ng$plot)

grace_coef_df = grace_model %>%
  broom::tidy() %>%
  filter(str_detect(term, "^(lag_severe_|lead_severe_|filed_severe$)")) %>%
  mutate(timing = case_when(
    str_detect(term, "lag_severe_") ~ -as.integer(str_extract(term, "\\d+$")),
    str_detect(term, "lead_severe_") ~ as.integer(str_extract(term, "\\d+$")),
    str_detect(term, "filed_severe") ~ 0L
  )) %>%
  # add row at t= -1
  bind_rows(tibble(
    term = "lag_severe_-1",
    estimate = 0,
    std.error = 0,
    timing = 1L
  )) %>%
  arrange(timing)

grace_plt <- ggplot(grace_coef_df, aes(x = timing, y = estimate)) +
  geom_point() +
  geom_errorbar(aes(ymin = estimate - 1.96 * std.error,
                    ymax = estimate + 1.96 * std.error), width = 0.2) +
  scale_y_continuous() +
  scale_x_reverse(breaks = -H:H) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  labs(title = "Distributed Lag Model:\nEffect on Percent of Cases for <= 1 month back rent",
       x = "Quarters relative to severe complaint",
       y = "Coefficient") +
  theme_philly_evict()
grace_plt


ggsave("figs/grace_plt.png", grace_plt, width=8, height=10, dpi=300, bg = "white")
# group all non-severe into one category for regressions
dt_pre[,non_severe := as.numeric(filed_vacant_property_complaint |
                      filed_zoning_complaint |
         filed_building_complaint |
         filed_emergency_service_complaint|
                      filed_trash_weeds_complaint |
                      filed_license_business_complaint |
                      filed_program_initiative_complaint
        )]
dt_pre[,filed_non_severe := as.integer(non_severe > 0)]
# lead lags of non-severe
dt_pre[,
                 `:=`(
                   lag_non_severe_1 = data.table::shift((filed_non_severe),  n = 1, type = "lag"),
                   lag_non_severe_2 = data.table::shift((filed_non_severe),  n = 2, type = "lag"),
                   lag_non_severe_3 = data.table::shift((filed_non_severe),  n = 3, type = "lag"),
                   lag_non_severe_4 = data.table::shift((filed_non_severe),  n = 4, type = "lag"),
                   lag_non_severe_5 = data.table::shift((filed_non_severe),  n = 5, type = "lag"),
                   lead_non_severe_1= data.table::shift((filed_non_severe),  n = 1, type = "lead"),
                   lead_non_severe_2= data.table::shift((filed_non_severe),  n = 2, type = "lead"),
                   lead_non_severe_3= data.table::shift((filed_non_severe),  n = 3, type = "lead"),
                   lead_non_severe_4= data.table::shift((filed_non_severe),  n = 4, type = "lead"),
                   lead_non_severe_5= data.table::shift((filed_non_severe),  n = 5, type = "lead")
                 ),  by = PID]

dt_pre[,filed_severe := as.integer(filed_heat_complaint |
                             filed_fire_complaint |
                             filed_property_maintenance_complaint |
                             filed_drainage_complaint
)]

# severe leads / lags
dt_pre[,
        `:=`(
          lag_severe_1 = data.table::shift((filed_sever),  n = 1, type = "lag"),
          lag_severe_2 = data.table::shift((filed_sever),  n = 2, type = "lag"),
          lag_severe_3 = data.table::shift((filed_sever),  n = 3, type = "lag"),
          lag_severe_4 = data.table::shift((filed_sever),  n = 4, type = "lag"),
          lag_severe_5 = data.table::shift((filed_sever),  n = 5, type = "lag"),
          lead_severe_1= data.table::shift((filed_sever),  n = 1, type = "lead"),
          lead_severe_2= data.table::shift((filed_sever),  n = 2, type = "lead"),
          lead_severe_3= data.table::shift((filed_sever),  n = 3, type = "lead"),
          lead_severe_4= data.table::shift((filed_sever),  n = 4, type = "lead"),
          lead_severe_5= data.table::shift((filed_sever),  n = 5, type = "lead")
        ),  by = PID]


# regressions
m_non_severe <- feols(
  build_fml(
    REG_Y,
    rhs_from_spec("non_severe", H, REG_CONTROLS),
    REG_FE
  ),
  data    = dt_pre,
  cluster = as.formula(REG_CLUSTER),
  weights = if (is.null(REG_WEIGHTS)) NULL else as.formula(REG_WEIGHTS)
)

feols(filed_eviction ~ lag_non_severe_4 + lag_non_severe_3 + lag_non_severe_2 +
        lag_non_severe_1 + filed_non_severe + lead_non_severe_1 +
        lead_non_severe_2 + lead_non_severe_3 + lead_non_severe_4 |year_quarter + PID, data = dt_pre, cluster = ~PID) |>
  summary()

m_severe <- feols(
  build_fml(
    REG_Y,
    rhs_from_spec("severe", H, REG_CONTROLS),
    REG_FE
  ),
  data    = dt_pre,
  cluster = as.formula(REG_CLUSTER),
  weights = if (is.null(REG_WEIGHTS)) NULL else as.formula(REG_WEIGHTS)
)
summary(m_severe)
coefplot(m_severe)

# extract and plot severe; non severe
coef_df = broom::tidy(m_severe) %>%
  mutate(family = "Severe Complaint") %>%
  bind_rows(broom::tidy(m_non_severe) %>%
              mutate(family = "Non-Severe Complaint")) %>%
  filter(str_detect(term, "^(lag_severe_|lead_severe_|lag_non_severe_|lead_non_severe_|filed_severe$|filed_non_severe$)")) %>%
  mutate(timing = case_when(
    str_detect(term, "^lag_severe_") ~ -as.integer(str_extract(term, "\\d+$")),
    str_detect(term, "^lead_severe_") ~  as.integer(str_extract(term, "\\d+$")),
    str_detect(term, "^lag_non_severe_") ~ -as.integer(str_extract(term, "\\d+$")),
    str_detect(term, "^lead_non_severe_") ~  as.integer(str_extract(term, "\\d+$")),
    str_detect(term, "^filed_severe$") ~ 0L,
    str_detect(term, "^filed_non_severe$") ~ 0L,
    TRUE ~ NA_integer_
  )) %>%
  arrange(family, timing)


ggplot(coef_df, aes(x = timing, y = estimate, color = family)) +
  geom_point() +
  geom_errorbar(aes(ymin = estimate - 1.96 * std.error,
                    ymax = estimate + 1.96 * std.error), width = 0.2) +
  scale_y_continuous() +

  scale_x_reverse(breaks = -H:H) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  labs(title = "Severe Complaints Lead/Lag Coefficients - Pre-COVID",
       caption = "Severe complaints: heat, fire, drainage, property maintenance. \nNon-severe complaints: building, emergency service, zoning, trash/weeds, license business,",
       x = "Quarters relative to complaint", y = "Coefficient") +
  theme_philly_evict()


#### maintence stuff ####
permits[,quality_grade_coarse := fifelse(
  quality_grade_fixed %in% c("A+","A","A-"), "A",
  fifelse(quality_grade_fixed %in% c("B+","B","B-"), "B",
          fifelse(quality_grade_fixed %in% c("C+","C","C-"), "C",
                  fifelse(quality_grade_fixed %in% c("D+","D","D-"), "D",
                          fifelse(quality_grade_fixed %in% c("F+","F","F-"), "F", "Unknown")))))
]
# Row; SFH; Apartment; Large Apartment; Other
permits[,building_code_description_new_fixed_coarse :=case_when(
  str_detect(building_code_description_new_fixed, "ROW|TWIN") ~ "Row",
  str_detect(building_code_description_new_fixed, "HIGH RISE") ~ "High Rise Apartment",
  str_detect(building_code_description_new_fixed, "LOW|MID") ~ "Mid Size Apartment",
  str_detect(building_code_description_new_fixed, "APARTMENT") ~ "Other Apartment",
  TRUE ~ "Other"
)]
permits[,filed_permit := total_permits > 0]
permits[,violated := total_violations > 0]
permits_qm <- feols(filed_permit ~ quality_grade_coarse + log(num_units_imp_alt)|t_idx, data = permits, cluster = ~PID)
evicts_qm <- feols(filed_eviction ~ quality_grade_coarse + log(num_units_imp_alt)|t_idx, data = permits, cluster = ~PID)
complaints_qm <- feols(filed_complaint ~ quality_grade_coarse + log(num_units_imp_alt)|t_idx, data = permits, cluster = ~PID)
complaints_sever_qm <- feols(filed_severe ~ quality_grade_coarse + log(num_units_imp_alt)|t_idx, data = permits, cluster = ~PID)
violations_qm <- feols(violated ~ quality_grade_coarse + log(num_units_imp_alt)|t_idx, data = permits, cluster = ~PID)

setFixest_dict(
  c(
    "quality_grade_coarseB" = "Quality B vs A",
    "quality_grade_coarseC" = "Quality C vs A",
    "quality_grade_coarseD" = "Quality D vs A",
    "quality_grade_coarseF" = "Quality F vs A",
    "quality_grade_coarseUnknown" = "Quality Unknown vs A",
    "t_idx" = "Time FE",
    "log(num_units_imp_alt)" = "Log(Number of Units)",
    "permits_qm" = "Permit Filed",
    "evicts_qm" = "Eviction Filed",
    "complaints_qm" = "Any Complaint Filed",
    "complaints_sever_qm" = "Severe Complaint Filed",
    "violations_qm" = "Violation Recorded"
  )
)
# row means
row_means <- permits[,list(
  permit_rate    = mean(filed_permit/num_units_imp_alt, na.rm=TRUE),
  eviction_rate  = mean(filed_eviction/num_units_imp_alt, na.rm=TRUE),
  complaint_rate = mean(filed_complaint/num_units_imp_alt, na.rm=TRUE),
  severe_rate    = mean(filed_severe/num_units_imp_alt, na.rm=TRUE),
  violation_rate = mean(violated/num_units_imp_alt, na.rm=TRUE)
)]

# rows as char vector
row_means_vec = list("per unit mean" =c(
#'Mean',
row_means$permit_rate,
row_means$eviction_rate,
row_means$complaint_rate,
row_means$severe_rate
))

etable(permits_qm, evicts_qm, complaints_qm, complaints_sever_qm,
       se.below = TRUE, digits = 3, extralines = row_means_vec, tex = T) %>%
  writeLines("tables/quality_grade_effects.tex")

# maintence regs
permits[,total_filings := sum(num_evictions), by=PID]
permits[,filing_rate := total_filings / num_units_imp_alt]
permits[,filing_rate_cuts := cut(
  filing_rate,
  breaks = c(-Inf,0,0.01,0.02,0.05,0.1, Inf),
  labels = c("0","(0,1%]","(1%,2%]","(2%,5%]","(5%,10%]","10%+"),
  ordered_result = TRUE
)]

permits_agg <- permits[year <= 2019, list(
  total_permits = sum(total_permits),
  total_plumbing_permit_count = sum(plumbing_permit_count,na.rm = T),
  total_electrical_permit_count = sum(electrical_permit_count,na.rm = T),
  total_mechanical_permit_count = sum(mechanical_permit_count,na.rm = T),
  total_filings = sum(num_evictions),
  quality_grade_fixed = first(quality_grade_fixed),
  building_code_description_new_fixed = first(building_code_description_new_fixed),
  total_area = first(total_area),
  year_built = first(year_built),
  num_units_imp_alt = first(num_units_imp_alt),
  GEOID = first(GEOID),
  CT_ID_10 = first(CT_ID_10)
), by=.(PID = as.integer(PID))]

permits_agg[,filing_rate := total_filings / num_units_imp_alt / 12]
permits_agg[,high_filing := filing_rate > 0.1]
permits_agg[,year_built_decade := floor(year_built / 10) * 10]
maintence <- fepois(total_permits ~ high_filing  + log(num_units_imp_alt)  ,
       data = permits_agg, cluster = ~PID)

maintence_prop_chars <- fepois(total_permits ~ high_filing  + log(num_units_imp_alt) + log(total_area)|GEOID + building_code_description_new_fixed + quality_grade_fixed +year_built_decade,
                        data = permits_agg, cluster = ~PID)

rentals_data <- fread(rentals_panel)
rentals_inf_adj <- feols(log_med_rent ~ year, data = rentals_data)
rentals_data[,rent_inf_adj := log_med_rent - predict(rentals_inf_adj, newdata = rentals_data)]
rent_agg = rentals_data[,list(
  avg_rent_inf_adj = mean(rent_inf_adj, na.rm=TRUE),
  log_med_rent = mean(log_med_rent, na.rm=TRUE),
  source = first(source)
), by=.(PID)]

permits_agg = merge(
permits_agg,
rent_agg, by="PID", all.x=TRUE
)

maintence_prop_chars_rent <- fepois(total_permits ~ high_filing  + log(num_units_imp_alt) + log(total_area)  +  log_med_rent  |GEOID + building_code_description_new_fixed + quality_grade_fixed +year_built_decade,
                        data = permits_agg, cluster = ~PID)

mechanical_prop_chars_rent <- fepois(total_mechanical_permit_count ~ high_filing  + log(num_units_imp_alt) + log(total_area)  +  log_med_rent  |GEOID + building_code_description_new_fixed + quality_grade_fixed +year_built_decade,
                                    data = permits_agg, cluster = ~PID)

electrical_prop_chars_rent <- fepois(total_electrical_permit_count ~ high_filing  + log(num_units_imp_alt) + log(total_area)  +  log_med_rent  |GEOID + building_code_description_new_fixed + quality_grade_fixed +year_built_decade,
                                  data = permits_agg, cluster = ~PID)

setFixest_dict(
  c(
    "high_filingTRUE" = "High Filing Rate",
    "log(num_units_imp_alt)" = "Log(Number of Units)",
    "log(total_area)" = "Log(Total Area)",
    "log_med_rent" = "Log(Median Rent)",
    "GEOID" = "Census Block Group FE",
    "building_code_description_new_fixed" = "Building Type FE",
    "quality_grade_fixed" = "Quality Grade FE",
    "year_built_decade" = "Year Built Decade FE"
  )
)

permit_means <- permits_agg[,list(
  permit_rate = mean(total_permits / num_units_imp_alt, na.rm=TRUE),
  mechanical_permit_rate = mean(total_mechanical_permit_count / num_units_imp_alt, na.rm=TRUE),
  electrical_permit_rate = mean(total_electrical_permit_count / num_units_imp_alt, na.rm=TRUE)
)]

row_means_permit_vec = list("per unit mean" =c(
  # 'Mean',
  permit_means$permit_rate,
  permit_means$permit_rate,
  permit_means$permit_rate,
  permit_means$mechanical_permit_rate,
  permit_means$electrical_permit_rate
))

etable(list(maintence, maintence_prop_chars, maintence_prop_chars_rent,mechanical_prop_chars_rent, electrical_prop_chars_rent),
       extralines = row_means_permit_vec,
       tex = T,
       se.below = TRUE, digits = 3) %>%
  writeLines("tables/maintence_effects.tex")

