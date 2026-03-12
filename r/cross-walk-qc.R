## ============================================================
## cross-walk-qc.R
## ============================================================
## Purpose:
##   Crosswalk quality checks for eviction -> parcel -> InfoUSA coverage.
##   Focus period: 2006-2019.
##
## Outputs:
##   - output/qa/crosswalk_qc_funnel_2006_2019.csv
##   - output/qa/crosswalk_qc_funnel_pooled_2006_2019.csv
##   - output/qa/crosswalk_qc_dropped_characteristics_2006_2019.csv
##   - output/qa/crosswalk_qc_dropped_by_year_2006_2019.csv
##   - output/qa/crosswalk_qc_dropped_vs_matched_lpm_2006_2019.csv
##   - output/qa/crosswalk_qc_summary_2006_2019.txt
##   - output/logs/cross-walk-qc.log
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
})

source("r/config.R")
source("r/lib/race_imputation_utils.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "cross-walk-qc.log")
qa_dir <- p_out(cfg, "qa")
dir.create(qa_dir, recursive = TRUE, showWarnings = FALSE)

logf("=== Starting cross-walk-qc.R ===", log_file = log_file)

## ---- Helpers ----

assert_has_required_cols <- function(dt, cols, label) {
  miss <- setdiff(cols, names(dt))
  if (length(miss) > 0L) {
    stop("Missing required columns in ", label, ": ", paste(miss, collapse = ", "))
  }
}

normalize_pid <- function(x) {
  x <- as.character(x)
  x <- gsub("[^0-9]", "", x)
  x[nchar(x) == 0L] <- NA_character_
  stringr::str_pad(x, width = 9L, side = "left", pad = "0")
}

assert_rows_unchanged <- function(n_before, n_after, label) {
  if (!identical(n_before, n_after)) {
    stop(label, " changed row count unexpectedly: ", n_before, " -> ", n_after)
  }
}

## ---- Load core inputs ----

ev_path <- p_product(cfg, "evictions_clean")
ev <- fread(ev_path, select = c("id", "year"))
assert_has_required_cols(ev, c("id", "year"), "evictions_clean")
ev[, id := as.character(id)]
ev[, year := suppressWarnings(as.integer(year))]
logf("Loaded evictions_clean: ", format(nrow(ev), big.mark = ","), " rows", log_file = log_file)

xw_case_path <- p_product(cfg, "evict_address_xwalk_case")
xw_case <- fread(xw_case_path, select = c("id", "PID", "num_parcels_matched"))
assert_has_required_cols(xw_case, c("id", "PID", "num_parcels_matched"), "evict_address_xwalk_case")
xw_case[, id := as.character(id)]
xw_case[, PID := normalize_pid(PID)]
logf("Loaded evict_address_xwalk_case: ", format(nrow(xw_case), big.mark = ","), " rows", log_file = log_file)

xw_info_path <- p_product(cfg, "infousa_address_xwalk")
xw_info <- fread(xw_info_path, select = c("PID", "num_parcels_matched"))
assert_has_required_cols(xw_info, c("PID", "num_parcels_matched"), "infousa_address_xwalk")
xw_info[, PID := normalize_pid(PID)]
logf("Loaded infousa_address_xwalk: ", format(nrow(xw_info), big.mark = ","), " rows", log_file = log_file)

bldg_path <- p_product(cfg, "bldg_panel_blp")
bldg <- fread(bldg_path, select = c("PID", "year", "GEOID", "filing_rate_eb_pre_covid"))
assert_has_required_cols(bldg, c("PID", "year", "GEOID", "filing_rate_eb_pre_covid"), "bldg_panel_blp")
bldg[, PID := normalize_pid(PID)]
bldg[, year := suppressWarnings(as.integer(year))]
logf("Loaded bldg_panel_blp (selected cols): ", format(nrow(bldg), big.mark = ","), " rows", log_file = log_file)

## ---- Restrict to requested years ----

analysis_years <- 2006L:2019L
ev <- ev[year %in% analysis_years]
logf("Filtered evictions to 2006-2019: ", format(nrow(ev), big.mark = ","), " rows", log_file = log_file)

## ---- Case-level unique PID crosswalk ----

xw_case_unique <- xw_case[
  num_parcels_matched == 1L &
    !is.na(id) & nzchar(id) & tolower(id) != "unknown" &
    !is.na(PID),
  .(id, PID)
]
xw_case_unique <- unique(xw_case_unique, by = c("id", "PID"))

dup_ids <- xw_case_unique[, .N, by = id][N > 1L]
if (nrow(dup_ids) > 0L) {
  stop("evict_address_xwalk_case unique subset is not unique on id; duplicate ids: ", nrow(dup_ids))
}
assert_unique(xw_case_unique, "id", "xw_case_unique")
logf("Unique-case PID links: ", format(nrow(xw_case_unique), big.mark = ","), " cases", log_file = log_file)
logf("Dropped xwalk rows with id == 'Unknown' before id join to avoid non-key collisions.",
     log_file = log_file)

n_before <- nrow(ev)
case_panel <- merge(ev, xw_case_unique, by = "id", all.x = TRUE)
assert_rows_unchanged(n_before, nrow(case_panel), "Merge evictions -> xw_case_unique")
logf("Merged evictions -> unique PID links; rows unchanged at ", format(nrow(case_panel), big.mark = ","), log_file = log_file)

## ---- PID coverage flags ----

infousa_pid <- unique(xw_info[num_parcels_matched == 1L & !is.na(PID), PID])
rate_pid <- unique(bldg[!is.na(PID) & !is.na(filing_rate_eb_pre_covid), PID])
both_pid <- intersect(infousa_pid, rate_pid)

logf("Unique InfoUSA PIDs (unique parcel match): ", format(length(infousa_pid), big.mark = ","), log_file = log_file)
logf("Unique PIDs with non-missing eviction rate: ", format(length(rate_pid), big.mark = ","), log_file = log_file)
logf("Unique PIDs in both sets: ", format(length(both_pid), big.mark = ","), log_file = log_file)

case_panel[, in_infousa := !is.na(PID) & (PID %chin% infousa_pid)]
case_panel[, has_evict_rate := !is.na(PID) & (PID %chin% rate_pid)]
case_panel[, in_infousa_and_rate := !is.na(PID) & (PID %chin% both_pid)]
case_panel[, dropped_no_infousa := !is.na(PID) & !in_infousa]

## ---- Funnel outputs by year ----

funnel_year <- case_panel[, .(
  total_evictions = .N,
  evictions_with_unique_pid = sum(!is.na(PID)),
  evictions_without_unique_pid = sum(is.na(PID)),
  evictions_unique_pid_with_infousa_pid = sum(!is.na(PID) & in_infousa, na.rm = TRUE),
  evictions_dropped_no_infousa_pid = sum(!is.na(PID) & !in_infousa, na.rm = TRUE),
  evictions_unique_pid_with_infousa_and_evict_rate = sum(!is.na(PID) & in_infousa_and_rate, na.rm = TRUE),
  evictions_unique_pid_missing_infousa_or_rate = sum(!is.na(PID) & !in_infousa_and_rate, na.rm = TRUE),
  evictions_infousa_pid_but_missing_evict_rate = sum(!is.na(PID) & in_infousa & !has_evict_rate, na.rm = TRUE),
  unique_eviction_pids = uniqueN(PID[!is.na(PID)]),
  unique_eviction_pids_with_infousa = uniqueN(PID[!is.na(PID) & in_infousa]),
  unique_eviction_pids_with_infousa_and_evict_rate = uniqueN(PID[!is.na(PID) & in_infousa_and_rate])
), by = year][order(year)]

funnel_year[, share_unique_pid := fifelse(total_evictions > 0, evictions_with_unique_pid / total_evictions, NA_real_)]
funnel_year[, share_drop_no_infousa_given_unique := fifelse(evictions_with_unique_pid > 0, evictions_dropped_no_infousa_pid / evictions_with_unique_pid, NA_real_)]
funnel_year[, share_kept_infousa_and_rate_given_unique := fifelse(evictions_with_unique_pid > 0, evictions_unique_pid_with_infousa_and_evict_rate / evictions_with_unique_pid, NA_real_)]

funnel_pooled <- case_panel[, .(
  total_evictions = .N,
  evictions_with_unique_pid = sum(!is.na(PID)),
  evictions_without_unique_pid = sum(is.na(PID)),
  evictions_unique_pid_with_infousa_pid = sum(!is.na(PID) & in_infousa, na.rm = TRUE),
  evictions_dropped_no_infousa_pid = sum(!is.na(PID) & !in_infousa, na.rm = TRUE),
  evictions_unique_pid_with_infousa_and_evict_rate = sum(!is.na(PID) & in_infousa_and_rate, na.rm = TRUE),
  evictions_unique_pid_missing_infousa_or_rate = sum(!is.na(PID) & !in_infousa_and_rate, na.rm = TRUE),
  evictions_infousa_pid_but_missing_evict_rate = sum(!is.na(PID) & in_infousa & !has_evict_rate, na.rm = TRUE),
  unique_eviction_pids = uniqueN(PID[!is.na(PID)]),
  unique_eviction_pids_with_infousa = uniqueN(PID[!is.na(PID) & in_infousa]),
  unique_eviction_pids_with_infousa_and_evict_rate = uniqueN(PID[!is.na(PID) & in_infousa_and_rate])
)]
funnel_pooled[, share_unique_pid := fifelse(total_evictions > 0, evictions_with_unique_pid / total_evictions, NA_real_)]
funnel_pooled[, share_drop_no_infousa_given_unique := fifelse(evictions_with_unique_pid > 0, evictions_dropped_no_infousa_pid / evictions_with_unique_pid, NA_real_)]
funnel_pooled[, share_kept_infousa_and_rate_given_unique := fifelse(evictions_with_unique_pid > 0, evictions_unique_pid_with_infousa_and_evict_rate / evictions_with_unique_pid, NA_real_)]

funnel_year_path <- file.path(qa_dir, "crosswalk_qc_funnel_2006_2019.csv")
funnel_pool_path <- file.path(qa_dir, "crosswalk_qc_funnel_pooled_2006_2019.csv")
fwrite(funnel_year, funnel_year_path)
fwrite(funnel_pooled, funnel_pool_path)
logf("Wrote funnel tables: ", funnel_year_path, " and ", funnel_pool_path, log_file = log_file)

## ---- GEOID attachment for dropped-case composition checks ----

logf("Building PID-year GEOID table...", log_file = log_file)
bldg_geo <- bldg[!is.na(PID) & !is.na(year), .(PID, year, bg_geoid_year = normalize_bg_geoid(GEOID))]
bldg_geo[, geoid_present := as.integer(!is.na(bg_geoid_year))]
setorder(bldg_geo, PID, year, -geoid_present)
bldg_geo <- unique(bldg_geo, by = c("PID", "year"))
bldg_geo[, geoid_present := NULL]
assert_unique(bldg_geo, c("PID", "year"), "bldg_geo")
logf("Built bldg_geo: ", format(nrow(bldg_geo), big.mark = ","), " unique PID-year rows", log_file = log_file)

pid_geo <- bldg_geo[, .(
  bg_geoid_pid = {
    g <- bg_geoid_year[!is.na(bg_geoid_year)]
    if (length(g) == 0L) NA_character_ else g[1L]
  }
), by = PID]
assert_unique(pid_geo, "PID", "pid_geo")
logf("Built pid_geo: ", format(nrow(pid_geo), big.mark = ","), " unique PIDs", log_file = log_file)

n_before <- nrow(case_panel)
case_panel <- merge(case_panel, bldg_geo, by = c("PID", "year"), all.x = TRUE)
assert_rows_unchanged(n_before, nrow(case_panel), "Merge case_panel -> bldg_geo")
logf("Merged case_panel -> bldg_geo (rows unchanged)", log_file = log_file)

n_before <- nrow(case_panel)
case_panel <- merge(case_panel, pid_geo, by = "PID", all.x = TRUE)
assert_rows_unchanged(n_before, nrow(case_panel), "Merge case_panel -> pid_geo")
logf("Merged case_panel -> pid_geo (rows unchanged)", log_file = log_file)

case_panel[, bg_geoid := fcoalesce(bg_geoid_year, bg_geoid_pid)]
case_panel[, tract_geoid := fifelse(!is.na(bg_geoid), substr(bg_geoid, 1L, 11L), NA_character_)]

## ---- Tract % Black from BG priors ----

prior_year <- suppressWarnings(as.integer(cfg$race_priors$year %||% 2013L))
if (!is.finite(prior_year)) prior_year <- 2013L
prior_type <- cfg$race_priors$prior_type %||% "renter"
prior_candidates <- default_prior_candidates(cfg, prior_year, "block_group", prior_type = prior_type)
priors_path <- prior_candidates[file.exists(prior_candidates)][1L]
if (is.na(priors_path) || !nzchar(priors_path)) {
  stop("Could not find block-group race priors file for year=", prior_year, ", type=", prior_type)
}

priors_bg <- fread(priors_path)
assert_has_required_cols(priors_bg, c("bg_geoid", "p_black_prior"), "BG race priors")
priors_bg[, bg_geoid := normalize_bg_geoid(bg_geoid)]
priors_bg <- priors_bg[!is.na(bg_geoid) & !is.na(p_black_prior)]
priors_bg[, tract_geoid := substr(bg_geoid, 1L, 11L)]

tract_black <- priors_bg[, .(
  tract_p_black = mean(p_black_prior, na.rm = TRUE),
  n_bg = .N
), by = tract_geoid]
assert_unique(tract_black, "tract_geoid", "tract_black")

n_before <- nrow(case_panel)
case_panel <- merge(case_panel, tract_black[, .(tract_geoid, tract_p_black)], by = "tract_geoid", all.x = TRUE)
assert_rows_unchanged(n_before, nrow(case_panel), "Merge case_panel -> tract_black")
logf("Attached tract_p_black using priors file: ", priors_path, log_file = log_file)

## ---- Dropped vs matched composition (unique-PID cases only) ----

case_unique <- case_panel[!is.na(PID)]
case_unique[, group := fifelse(dropped_no_infousa, "dropped_no_infousa", "matched_to_infousa")]

case_median_black <- case_unique[!is.na(tract_p_black), median(tract_p_black)]

comp <- case_unique[, .(
  n_cases = .N,
  n_with_tract_black = sum(!is.na(tract_p_black)),
  pct_with_tract_black = mean(!is.na(tract_p_black)),
  mean_tract_p_black = mean(tract_p_black, na.rm = TRUE),
  median_tract_p_black = median(tract_p_black, na.rm = TRUE),
  share_tract_p_black_ge_50 = mean(tract_p_black >= 0.50, na.rm = TRUE),
  share_tract_p_black_ge_case_median = mean(tract_p_black >= case_median_black, na.rm = TRUE)
), by = group][order(group)]

comp_path <- file.path(qa_dir, "crosswalk_qc_dropped_characteristics_2006_2019.csv")
fwrite(comp, comp_path)

comp_mean_pct <- comp[, .(
  dropped_no_infousa = group,
  n_cases,
  mean_pct_black = 100 * mean_tract_p_black,
  median_pct_black = 100 * median_tract_p_black
)]
comp_mean_pct_path <- file.path(qa_dir, "crosswalk_qc_mean_pct_black_by_dropped_no_infousa_2006_2019.csv")
fwrite(comp_mean_pct, comp_mean_pct_path)

comp_year <- case_unique[, .(
  n_cases = .N,
  n_with_tract_black = sum(!is.na(tract_p_black)),
  mean_tract_p_black = mean(tract_p_black, na.rm = TRUE),
  share_tract_p_black_ge_50 = mean(tract_p_black >= 0.50, na.rm = TRUE)
), by = .(year, group)][order(year, group)]
comp_year_path <- file.path(qa_dir, "crosswalk_qc_dropped_by_year_2006_2019.csv")
fwrite(comp_year, comp_year_path)

comp_year_mean_pct <- comp_year[, .(
  year,
  dropped_no_infousa = group,
  n_cases,
  mean_pct_black = 100 * mean_tract_p_black
)]
comp_year_mean_pct_path <- file.path(qa_dir, "crosswalk_qc_mean_pct_black_by_dropped_no_infousa_year_2006_2019.csv")
fwrite(comp_year_mean_pct, comp_year_mean_pct_path)

## ---- LPM: dropped_no_infousa on tract % Black + year FE ----

reg_dt <- case_unique[!is.na(tract_p_black)]
reg_dt[, dropped_int := as.integer(dropped_no_infousa)]

if (nrow(reg_dt) >= 100L) {
  m <- lm(dropped_int ~ tract_p_black + factor(year), data = reg_dt)
  sm <- summary(m)$coefficients
  coef_row <- "tract_p_black"
  if (!(coef_row %in% rownames(sm))) stop("LPM failed to produce tract_p_black coefficient.")
  lpm_out <- data.table(
    term = coef_row,
    estimate = sm[coef_row, "Estimate"],
    std_error = sm[coef_row, "Std. Error"],
    t_value = sm[coef_row, "t value"],
    p_value = sm[coef_row, "Pr(>|t|)"],
    n_obs = nrow(reg_dt)
  )
} else {
  lpm_out <- data.table(
    term = "tract_p_black",
    estimate = NA_real_,
    std_error = NA_real_,
    t_value = NA_real_,
    p_value = NA_real_,
    n_obs = nrow(reg_dt)
  )
}
lpm_path <- file.path(qa_dir, "crosswalk_qc_dropped_vs_matched_lpm_2006_2019.csv")
fwrite(lpm_out, lpm_path)

## ---- Plaintext summary ----

summary_lines <- c(
  "Crosswalk QC summary (2006-2019)",
  paste0("Generated: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("Total evictions: ", format(funnel_pooled$total_evictions, big.mark = ",")),
  paste0("With unique PID: ", format(funnel_pooled$evictions_with_unique_pid, big.mark = ",")),
  paste0("Dropped due to no InfoUSA PID: ", format(funnel_pooled$evictions_dropped_no_infousa_pid, big.mark = ",")),
  paste0("Drop share among unique PID: ", round(100 * funnel_pooled$share_drop_no_infousa_given_unique, 2), "%"),
  "",
  "Dropped vs matched tract composition (unique PID cases only):",
  paste(capture.output(print(comp)), collapse = "\n"),
  "",
  "LPM: dropped_no_infousa ~ tract_p_black + year FE",
  paste(capture.output(print(lpm_out)), collapse = "\n")
)

summary_path <- file.path(qa_dir, "crosswalk_qc_summary_2006_2019.txt")
writeLines(summary_lines, summary_path)

logf("Wrote dropped-case composition outputs: ", comp_path, " and ", comp_year_path, log_file = log_file)
logf("Wrote mean %% Black summaries: ", comp_mean_pct_path, " and ", comp_year_mean_pct_path, log_file = log_file)
logf("Wrote LPM output: ", lpm_path, log_file = log_file)
logf("Wrote summary text: ", summary_path, log_file = log_file)
logf("=== Finished cross-walk-qc.R ===", log_file = log_file)
