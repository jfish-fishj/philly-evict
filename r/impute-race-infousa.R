## ============================================================
## impute-race-infousa.R
## ============================================================
## Purpose: WRU-based race imputation on InfoUSA household data.
##
## Data flow:
##   infousa_clean (wide, HH-year rows, persons 1-5)
##     -> melt to person-level
##     -> construct bg_geoid from InfoUSA census columns
##     -> join PID via n_sn_ss_c -> xwalk_infousa_to_parcel
##     -> crosscheck InfoUSA vs parcel-linked GEOID
##     -> flag rental PIDs and eviction PIDs
##     -> dedup on (first_u, last_u, bg_geoid) for WRU
##     -> run WRU predict_race
##     -> merge predictions back
##     -> write person-level output + QA
##
## Inputs (via config):
##   - cfg$products$infousa_clean
##   - cfg$products$xwalk_infousa_to_parcel
##   - cfg$products$parcel_occupancy_panel  (for GEOID crosscheck)
##   - cfg$products$ever_rentals_panel      (rental PID flag)
##   - cfg$products$evict_address_xwalk_case (eviction flag)
##   - BG race priors file (from race_priors config)
##   - Name probability files (first/last/middle)
##
## Outputs:
##   - cfg$products$infousa_race_imputed_person
##   - output/qa/infousa_race_imputed_qa.txt
##   - output/logs/impute-race-infousa.log
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(wru)
})

source("r/config.R")
source("r/lib/race_imputation_utils.R")

## ---- CLI args ----

parse_cli_args <- function(args) {
  out <- list()
  if (length(args) == 0L) return(out)
  for (arg in args) {
    if (!startsWith(arg, "--")) next
    arg <- sub("^--", "", arg)
    parts <- strsplit(arg, "=", fixed = TRUE)[[1]]
    key <- gsub("-", "_", parts[1])
    val <- if (length(parts) >= 2L) paste(parts[-1], collapse = "=") else "TRUE"
    out[[key]] <- val
  }
  out
}

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

## ---- Config ----

race_cfg <- cfg$race_priors %||% list()
inference_geo <- "block_group"  # InfoUSA provides BG-level census geo
priors_year <- suppressWarnings(as.integer(opts$prior_year %||% race_cfg$year %||% 2013L))
if (is.na(priors_year) || priors_year < 1900L || priors_year > 2100L) priors_year <- 2013L
prior_type <- opts$prior_type %||% race_cfg$prior_type %||% "renter"
bg_shrink_enabled <- tolower(as.character(opts$bg_shrink_enabled %||% race_cfg$bg_shrink_enabled %||% "true")) %chin% c("true", "1", "yes", "y")
bg_shrink_min_n <- suppressWarnings(as.numeric(opts$bg_shrink_min_n %||% race_cfg$bg_shrink_min_n %||% 100))
bg_shrink_k <- suppressWarnings(as.numeric(opts$bg_shrink_k %||% race_cfg$bg_shrink_k %||% bg_shrink_min_n))
if (!is.finite(bg_shrink_min_n) || bg_shrink_min_n < 0) bg_shrink_min_n <- 100
if (!is.finite(bg_shrink_k) || bg_shrink_k <= 0) bg_shrink_k <- bg_shrink_min_n

candidate_priors <- default_prior_candidates(cfg, priors_year, inference_geo, prior_type = prior_type)
default_priors_path <- candidate_priors[file.exists(candidate_priors)][1]
priors_path <- opts$geo_priors %||% opts$priors %||% default_priors_path

first_probs_path <- opts$first_probs %||% p_in(cfg, "name-files/first_nameRaceProbs.csv")
last_probs_path <- opts$last_probs %||% p_in(cfg, "name-files/last_nameRaceProbs.csv")
middle_probs_path <- opts$middle_probs %||% p_in(cfg, "name-files/middle_nameRaceProbs.csv")

out_person_path <- opts$output %||% p_product(cfg, "infousa_race_imputed_person")
out_qa_path <- opts$output_qa %||% p_out(cfg, "qa", "infousa_race_imputed_qa.txt")

sample_n <- suppressWarnings(as.integer(opts$sample_n %||% NA_character_))
seed <- suppressWarnings(as.integer(opts$seed %||% cfg$run$seed %||% 123L))
rental_only <- tolower(as.character(opts$rental_only %||% "false")) %chin% c("true", "1", "yes", "y")

wru_year <- suppressWarnings(as.integer(opts$wru_year %||% 2010L))
retry <- suppressWarnings(as.integer(opts$retry %||% 3L))
census_key <- opts$census_key %||% Sys.getenv("CENSUS_API_KEY", unset = "")
if (is.na(wru_year) || !(wru_year %in% c(2000L, 2010L, 2020L))) {
  stop("wru_year must be one of 2000, 2010, or 2020.")
}
if (is.na(retry) || retry < 0L) retry <- 3L

log_file <- p_out(cfg, "logs", "impute-race-infousa.log")
logf("=== Starting impute-race-infousa.R (WRU) ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf("Inference geography: ", inference_geo, log_file = log_file)
logf("Priors file: ", priors_path, log_file = log_file)
logf("WRU year: ", wru_year, log_file = log_file)
logf("BG shrink: enabled=", bg_shrink_enabled, ", min_n=", bg_shrink_min_n, ", k=", bg_shrink_k, log_file = log_file)
logf("Rental-only mode: ", rental_only, log_file = log_file)

if (is.null(priors_path) || is.na(priors_path) || !nzchar(priors_path) || !file.exists(priors_path)) {
  stop("Priors file not found: ", priors_path)
}
if (!file.exists(first_probs_path) || !file.exists(last_probs_path) || !file.exists(middle_probs_path)) {
  stop("One or more name probability files are missing.")
}

dir.create(dirname(out_person_path), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_qa_path), recursive = TRUE, showWarnings = FALSE)

## ============================================================
## Step 1: Load & melt InfoUSA to person-level
## ============================================================

logf("Loading infousa_clean product...", log_file = log_file)
infousa_path <- p_product(cfg, "infousa_clean")

# Select only needed columns to save memory
keep_cols <- c(
  "familyid", "family_id", "locationid", "year", "n_sn_ss_c", "pm.uid",
  "ge_census_state_2010", "ge_census_county", "ge_census_tract", "ge_census_bg",
  paste0("first_name_", 1:5),
  paste0("middle_name_", 1:5),
  paste0("last_name_", 1:5),
  paste0("individual_id_", 1:5),
  paste0("ethnicity_code_", 1:5),
  paste0("gender_", 1:5),
  paste0("age_", 1:5)
)

# Read only columns that exist
hdr <- names(fread(infousa_path, nrows = 0L))
keep_cols <- intersect(keep_cols, hdr)
wide <- fread(infousa_path, select = keep_cols)
setDT(wide)
n_wide <- nrow(wide)
logf("Loaded infousa_clean: ", n_wide, " HH-year rows, ", length(keep_cols), " columns", log_file = log_file)

# Sample mode: subsample HH-year rows before melt
if (!is.na(sample_n) && sample_n > 0L && sample_n < n_wide) {
  set.seed(seed)
  wide <- wide[sample(.N, sample_n)]
  logf("Sample mode: kept ", nrow(wide), " HH-year rows from ", n_wide, " with seed=", seed, log_file = log_file)
  n_wide <- nrow(wide)
}

# Melt persons 1-5 to long format
person_slots <- 1:5
melt_list <- vector("list", length(person_slots))

for (slot in person_slots) {
  s <- as.character(slot)
  fn_col <- paste0("first_name_", s)
  ln_col <- paste0("last_name_", s)

  # Skip if columns don't exist

if (!(fn_col %in% names(wide)) && !(ln_col %in% names(wide))) next

  # Build slot columns
  slot_cols <- c(
    first_name = fn_col,
    last_name = ln_col,
    middle_name = paste0("middle_name_", s),
    individual_id = paste0("individual_id_", s),
    ethnicity_code = paste0("ethnicity_code_", s),
    gender = paste0("gender_", s),
    age = paste0("age_", s)
  )

  # Keep only columns that exist
  slot_cols <- slot_cols[slot_cols %in% names(wide)]

  # Extract HH-level cols + slot cols
  hh_cols <- c("familyid", "family_id", "locationid", "year", "n_sn_ss_c", "pm.uid",
               "ge_census_state_2010", "ge_census_county", "ge_census_tract", "ge_census_bg")
  hh_cols <- intersect(hh_cols, names(wide))

  p <- wide[, c(hh_cols, unname(slot_cols)), with = FALSE]

  # Rename slot columns to generic names
  for (nm in names(slot_cols)) {
    if (slot_cols[[nm]] %in% names(p)) {
      setnames(p, slot_cols[[nm]], nm)
    }
  }

  p[, person_num := slot]
  p[, head_of_household := (slot == 1L)]
  melt_list[[slot]] <- p
}

persons <- rbindlist(melt_list, use.names = TRUE, fill = TRUE)
rm(wide, melt_list)
gc(verbose = FALSE)

# Drop rows where both first_name and last_name are missing
persons <- persons[
  (!is.na(first_name) & nzchar(as.character(first_name))) |
    (!is.na(last_name) & nzchar(as.character(last_name)))
]
n_persons <- nrow(persons)
logf("Melted to person-level: ", n_persons, " rows (after dropping empty name rows)", log_file = log_file)

## ============================================================
## Step 2: Link PID via n_sn_ss_c -> xwalk_infousa_to_parcel
## ============================================================

logf("Loading xwalk_infousa_to_parcel...", log_file = log_file)
xwalk_path <- p_product(cfg, "xwalk_infousa_to_parcel")
xwalk <- fread(xwalk_path, select = c("n_sn_ss_c", "parcel_number", "match_tier"))
setDT(xwalk)

# Deduplicate: one winner per n_sn_ss_c (take best match_tier)
xwalk <- xwalk[!is.na(n_sn_ss_c) & nzchar(n_sn_ss_c) & !is.na(parcel_number)]
setorder(xwalk, n_sn_ss_c, match_tier)
xwalk <- xwalk[, .SD[1L], by = n_sn_ss_c]
xwalk[, match_tier := NULL]
setnames(xwalk, "parcel_number", "PID")
n_xwalk <- nrow(xwalk)
logf("Xwalk: ", n_xwalk, " unique n_sn_ss_c -> PID mappings", log_file = log_file)

# Join PID
persons[xwalk, PID := i.PID, on = "n_sn_ss_c"]
n_pid_linked <- persons[!is.na(PID), .N]
logf("PID link: ", n_pid_linked, " / ", n_persons, " persons linked to a PID (",
     round(100 * n_pid_linked / n_persons, 1), "%)", log_file = log_file)
rm(xwalk)

## ============================================================
## Step 3: Assign GEOID
## ============================================================

# Primary: InfoUSA census columns -> BG GEOID
# Guard: state must be a valid non-zero FIPS code; county/tract/bg must be present
persons[, bg_geoid_infousa := {
  st_raw <- suppressWarnings(as.integer(ge_census_state_2010))
  co_raw <- suppressWarnings(as.integer(ge_census_county))
  tr_raw <- suppressWarnings(as.integer(ge_census_tract))
  bg_raw <- suppressWarnings(as.integer(ge_census_bg))
  valid <- is.finite(st_raw) & st_raw > 0L &
           is.finite(co_raw) & co_raw > 0L &
           is.finite(tr_raw) &
           is.finite(bg_raw) & bg_raw >= 0L
  st <- str_pad(as.character(st_raw), width = 2L, side = "left", pad = "0")
  co <- str_pad(as.character(co_raw), width = 3L, side = "left", pad = "0")
  tr <- str_pad(as.character(tr_raw), width = 6L, side = "left", pad = "0")
  bg <- as.character(bg_raw)
  raw <- paste0(st, co, tr, bg)
  out <- normalize_bg_geoid(raw)
  out[!valid] <- NA_character_
  out
}]
n_infousa_bg <- persons[!is.na(bg_geoid_infousa), .N]
logf("InfoUSA BG GEOID coverage: ", n_infousa_bg, " / ", n_persons, " (",
     round(100 * n_infousa_bg / n_persons, 1), "%)", log_file = log_file)

# Crosscheck: PID -> GEOID from parcel_occupancy_panel
logf("Loading parcel_occupancy_panel for GEOID crosscheck...", log_file = log_file)
pop_path <- p_product(cfg, "parcel_occupancy_panel")
pid_geo <- fread(pop_path, select = c("PID", "GEOID"))
setDT(pid_geo)
pid_geo[, GEOID := normalize_bg_geoid(GEOID)]
pid_geo <- unique(pid_geo[!is.na(PID) & !is.na(GEOID)], by = "PID")
# If PID maps to multiple GEOIDs, take first (most are unique)
pid_geo <- pid_geo[, .(GEOID = GEOID[1L]), by = PID]

persons[pid_geo, bg_geoid_parcel := i.GEOID, on = "PID"]
n_parcel_bg <- persons[!is.na(bg_geoid_parcel), .N]
logf("Parcel-linked BG GEOID coverage: ", n_parcel_bg, " / ", n_persons, " (",
     round(100 * n_parcel_bg / n_persons, 1), "%)", log_file = log_file)
rm(pid_geo)

# GEOID agreement where both present
both_present <- persons[!is.na(bg_geoid_infousa) & !is.na(bg_geoid_parcel)]
n_both <- nrow(both_present)
n_agree <- both_present[bg_geoid_infousa == bg_geoid_parcel, .N]
n_disagree <- n_both - n_agree
agree_rate <- if (n_both > 0L) round(n_agree / n_both, 4) else NA_real_
logf("GEOID agreement (InfoUSA vs parcel): ", n_agree, " / ", n_both,
     " agree (", round(100 * agree_rate, 1), "%), ", n_disagree, " disagree",
     log_file = log_file)
rm(both_present)

# Coalesce: prefer parcel-linked BG (spatially matched), InfoUSA census as fallback
persons[, bg_geoid := fcoalesce(bg_geoid_parcel, bg_geoid_infousa)]
persons[, geo_source := fifelse(
  !is.na(bg_geoid_parcel), "parcel_linked",
  fifelse(!is.na(bg_geoid_infousa), "infousa_census", "missing")
)]
n_geoid_final <- persons[!is.na(bg_geoid), .N]
logf("Final BG GEOID coverage: ", n_geoid_final, " / ", n_persons, " (",
     round(100 * n_geoid_final / n_persons, 1), "%)", log_file = log_file)
logf("Geo source counts:\n",
     paste(capture.output(print(persons[, .N, by = geo_source][order(-N)])), collapse = "\n"),
     log_file = log_file)

## ============================================================
## Step 4: Filter & flag
## ============================================================

# Rental PID flag
logf("Loading ever_rentals_panel for rental PID flag...", log_file = log_file)
ever_path <- p_product(cfg, "ever_rentals_panel")
rental_pids <- unique(fread(ever_path, select = "PID")$PID)
rental_pids <- rental_pids[!is.na(rental_pids)]
persons[, is_rental_pid := PID %in% rental_pids]
n_rental <- persons[is_rental_pid == TRUE, .N]
logf("Rental PID coverage: ", n_rental, " / ", n_persons, " persons at rental PIDs (",
     round(100 * n_rental / n_persons, 1), "%)", log_file = log_file)
rm(rental_pids)

if (isTRUE(rental_only)) {
  n_before_rental_only <- nrow(persons)
  persons <- persons[is_rental_pid == TRUE]
  n_after_rental_only <- nrow(persons)
  n_persons <- n_after_rental_only
  logf("Applied rental-only filter: kept ", n_after_rental_only, " / ", n_before_rental_only,
       " person rows (", round(100 * n_after_rental_only / pmax(1, n_before_rental_only), 1), "%)",
       log_file = log_file)
}

# Eviction PID flag
logf("Loading evict_address_xwalk_case for eviction PID flag...", log_file = log_file)
evict_xwalk_path <- p_product(cfg, "evict_address_xwalk_case")
evict_pids <- unique(fread(evict_xwalk_path, select = "PID")$PID)
evict_pids <- evict_pids[!is.na(evict_pids)]
persons[, in_eviction_data := PID %in% evict_pids]
n_evict <- persons[in_eviction_data == TRUE, .N]
logf("Eviction PID flag: ", n_evict, " / ", n_persons, " persons at eviction PIDs",
     log_file = log_file)
rm(evict_pids)

# Impute status
persons[, first_u := toupper(str_squish(as.character(first_name)))]
persons[, last_u := toupper(str_squish(as.character(last_name)))]
persons[first_u %in% c("", "NA"), first_u := NA_character_]
persons[last_u %in% c("", "NA"), last_u := NA_character_]

persons[, has_pid := !is.na(PID)]
persons[, impute_status := "ok"]
persons[is.na(last_u) | !nzchar(last_u), impute_status := "missing_surname"]
persons[impute_status == "ok" & is.na(bg_geoid), impute_status := "no_geoid"]

logf("Impute status counts:\n",
     paste(capture.output(print(persons[, .N, by = impute_status][order(-N)])), collapse = "\n"),
     log_file = log_file)
logf("PID linkage among ok rows: ", persons[impute_status == "ok" & has_pid == TRUE, .N],
     " / ", persons[impute_status == "ok", .N], " (",
     round(100 * persons[impute_status == "ok" & has_pid == TRUE, .N] / persons[impute_status == "ok", .N], 1),
     "%)", log_file = log_file)

## ============================================================
## Step 5: Dedup for WRU
## ============================================================

targets <- persons[impute_status == "ok"]
logf("Target rows for WRU: ", nrow(targets), log_file = log_file)

u <- unique(targets[, .(first_u, last_u, bg_geoid)])
logf("Deduped for WRU: ", nrow(u), " unique (first, last, bg_geoid) from ", nrow(targets), " target rows (",
     round(nrow(targets) / nrow(u), 1), "x reduction)", log_file = log_file)

## ============================================================
## Step 6: Load priors & build WRU census.data
## ============================================================

logf("Loading and preparing race priors...", log_file = log_file)
write_wru_name_cache(first_probs_path, last_probs_path, middle_probs_path, log_file = log_file)

priors_raw <- fread(priors_path)
setDT(priors_raw)
geo_priors <- coerce_priors_to_geo(priors_raw, target_geo = inference_geo)
rm(priors_raw)

if (isTRUE(bg_shrink_enabled)) {
  geo_priors <- apply_bg_low_n_tract_shrink(
    geo_priors,
    min_n = bg_shrink_min_n,
    k = bg_shrink_k,
    log_file = log_file
  )
}
prior_source_geo <- unique(geo_priors$prior_source_geo)[1]
geo_priors[, prior_source_geo := NULL]
census_data <- build_wru_census_data_from_priors(geo_priors, census_geo = inference_geo, year = wru_year)
logf("Built WRU census.data for states: ", paste(names(census_data), collapse = ", "),
     " (prior_source=", prior_source_geo, ")", log_file = log_file)

## ============================================================
## Step 7: Run WRU
## ============================================================

# Prepare deduped rows for WRU voter file format
u[, `:=`(
  surname = last_u,
  first = fifelse(!is.na(first_u) & nzchar(first_u), first_u, NA_character_),
  state = state_abbr_from_fips(substr(bg_geoid, 1L, 2L)),
  county_code = substr(bg_geoid, 3L, 5L),
  geo_mid_code = substr(bg_geoid, 6L, 11L),
  bg_code = substr(bg_geoid, 12L, 12L),
  geo_chr = bg_geoid
)]

n_bad_state <- u[is.na(state), .N]
if (n_bad_state > 0L) {
  bad <- unique(u[is.na(state), substr(bg_geoid, 1L, 2L)])
  logf("WARNING: Dropping ", n_bad_state, " deduped rows with unmappable state FIPS: ",
       paste(head(bad, 10L), collapse = ", "), log_file = log_file)
  u <- u[!is.na(state)]
}

u_first <- u[!is.na(first) & nzchar(first)]
u_last <- u[is.na(first) | !nzchar(first)]

logf("WRU split: ", nrow(u_first), " surname+first, ", nrow(u_last), " surname-only", log_file = log_file)

pred_first <- run_wru_predict(
  vf = build_wru_voter_file(u_first, census_geo = inference_geo, include_first = TRUE),
  names_to_use = "surname, first",
  census_data = census_data,
  census_geo = inference_geo,
  year = wru_year,
  census_key = census_key,
  retry = retry,
  log_file = log_file,
  skip_bad_geos = TRUE
)

pred_last <- run_wru_predict(
  vf = build_wru_voter_file(u_last, census_geo = inference_geo, include_first = FALSE),
  names_to_use = "surname",
  census_data = census_data,
  census_geo = inference_geo,
  year = wru_year,
  census_key = census_key,
  retry = retry,
  log_file = log_file,
  skip_bad_geos = TRUE
)

pred_u <- rbindlist(list(pred_first, pred_last), use.names = TRUE, fill = TRUE)
setDT(pred_u)
rm(pred_first, pred_last, census_data)

## ============================================================
## Step 8: Post-process predictions
## ============================================================

pred_u <- pred_u[, .(
  first_u,
  last_u,
  geo_chr,
  p_white = as.numeric(pred.whi),
  p_black = as.numeric(pred.bla),
  p_hispanic = as.numeric(pred.his),
  p_asian = as.numeric(pred.asi),
  p_other = as.numeric(pred.oth),
  race_impute_method = fifelse(
    !is.na(first_u) & nzchar(first_u),
    "wru_firstname+surname+block_group",
    "wru_surname+block_group"
  )
)]

# Normalize probabilities to sum=1
prob_cols <- c("p_white", "p_black", "p_hispanic", "p_asian", "p_other")
pred_u[, psum := rowSums(.SD), .SDcols = prob_cols]
pred_u[is.finite(psum) & psum > 0, `:=`(
  p_white = p_white / psum,
  p_black = p_black / psum,
  p_hispanic = p_hispanic / psum,
  p_asian = p_asian / psum,
  p_other = p_other / psum
)]
pred_u[, psum := NULL]

pred_u[, race_impute_status := fifelse(
  is.na(p_white) | is.na(p_black) | is.na(p_hispanic) | is.na(p_asian) | is.na(p_other),
  "name_lookup_failed",
  "ok"
)]

# Derived labels
pred_u[, race_hat := NA_character_]
pred_u[, race_hat_p := NA_real_]
pred_u[, race_entropy := NA_real_]
ok_idx <- pred_u[race_impute_status == "ok", which = TRUE]
if (length(ok_idx) > 0L) {
  mat <- as.matrix(pred_u[ok_idx, .(p_white, p_black, p_hispanic, p_asian, p_other)])
  idx <- max.col(mat, ties.method = "first")
  labs <- c("white", "black", "hispanic", "asian", "other")
  pred_u[ok_idx, race_hat := labs[idx]]
  pred_u[ok_idx, race_hat_p := mat[cbind(seq_len(nrow(mat)), idx)]]
  pred_u[ok_idx, race_entropy := entropy5(
    pred_u[ok_idx]$p_white, pred_u[ok_idx]$p_black, pred_u[ok_idx]$p_hispanic,
    pred_u[ok_idx]$p_asian, pred_u[ok_idx]$p_other
  )]
}

assert_unique(pred_u, c("first_u", "last_u", "geo_chr"), "wru predictions")
n_pred_ok <- pred_u[race_impute_status == "ok", .N]
n_pred_fail <- pred_u[race_impute_status != "ok", .N]
logf("WRU results: ", n_pred_ok, " ok, ", n_pred_fail, " name_lookup_failed",
     log_file = log_file)

## ============================================================
## Step 9: Merge predictions back to full persons table
## ============================================================

setkey(persons, first_u, last_u, bg_geoid)
setkey(pred_u, first_u, last_u, geo_chr)

persons[pred_u, `:=`(
  p_white = i.p_white,
  p_black = i.p_black,
  p_hispanic = i.p_hispanic,
  p_asian = i.p_asian,
  p_other = i.p_other,
  race_hat = i.race_hat,
  race_hat_p = i.race_hat_p,
  race_entropy = i.race_entropy,
  race_impute_method = i.race_impute_method,
  race_impute_status = i.race_impute_status
), on = .(first_u = first_u, last_u = last_u, bg_geoid = geo_chr)]

# Fill race_impute_status for non-target rows
persons[is.na(race_impute_status) & impute_status == "ok", race_impute_status := "name_lookup_failed"]
persons[is.na(race_impute_status), race_impute_status := impute_status]
rm(pred_u)

## ============================================================
## Step 10: Output
## ============================================================

# Select output columns
out_cols <- c(
  "familyid", "family_id", "locationid", "individual_id",
  "person_num", "head_of_household", "year",
  "PID", "bg_geoid", "geo_source",
  "first_name", "last_name", "middle_name",
  "ethnicity_code", "gender", "age",
  "p_white", "p_black", "p_hispanic", "p_asian", "p_other",
  "race_hat", "race_hat_p", "race_entropy",
  "race_impute_method", "race_impute_status",
  "in_eviction_data", "is_rental_pid", "impute_status"
)
out_cols <- intersect(out_cols, names(persons))

logf("Writing person-level output: ", out_person_path, log_file = log_file)
fwrite(persons[, ..out_cols], out_person_path)
logf("Wrote ", nrow(persons), " rows to ", out_person_path, log_file = log_file)

## ============================================================
## Step 11: Building-level racial composition panel (PID × year)
## ============================================================

# Build BG prior lookup (used by building diagnostics and QA)
pri_geo <- copy(geo_priors)[, .(
  geo_geoid,
  prior_p_white = as.numeric(p_white_prior),
  prior_p_black = as.numeric(p_black_prior),
  prior_p_hispanic = as.numeric(p_hispanic_prior)
)]
pri_geo <- unique(pri_geo, by = "geo_geoid")

logf("Building PID x year racial composition panel...", log_file = log_file)

# Aggregate imputed persons to building-year level
# Use probability-weighted means (not argmax) for composition shares
bldg_race <- persons[race_impute_status == "ok" & !is.na(PID), .(
  num_households = uniqueN(familyid),
  num_persons    = .N,
  pct_white      = mean(p_white, na.rm = TRUE),
  pct_black      = mean(p_black, na.rm = TRUE),
  pct_hispanic   = mean(p_hispanic, na.rm = TRUE),
  pct_asian      = mean(p_asian, na.rm = TRUE),
  pct_other      = mean(p_other, na.rm = TRUE),
  mean_entropy   = mean(race_entropy, na.rm = TRUE),
  n_head_of_hh   = sum(head_of_household, na.rm = TRUE),
  bg_geoid       = bg_geoid[1L]  # modal/first BG for diagnostics
), by = .(PID, year)]

assert_unique(bldg_race, c("PID", "year"), "bldg_race panel")
logf("Building race panel: ", nrow(bldg_race), " PID x year rows, ",
     bldg_race[, uniqueN(PID)], " unique PIDs", log_file = log_file)

# Write building-level product
out_bldg_race_path <- p_product(cfg, "infousa_building_race_panel")
dir.create(dirname(out_bldg_race_path), recursive = TRUE, showWarnings = FALSE)
fwrite(bldg_race, out_bldg_race_path)
logf("Wrote building race panel: ", out_bldg_race_path, log_file = log_file)

# ---- Diagnostics: within-BG variation ----

# Merge BG priors onto building panel for comparison
bldg_bench <- merge(bldg_race, pri_geo, by.x = "bg_geoid", by.y = "geo_geoid", all.x = TRUE)

# 1) Building vs BG correlation (all buildings with >= 3 persons)
bldg_bench_3 <- bldg_bench[num_persons >= 3 & !is.na(prior_p_black)]
cor_bldg_bg_black <- if (nrow(bldg_bench_3) > 10L) {
  round(cor(bldg_bench_3$pct_black, bldg_bench_3$prior_p_black, use = "complete.obs"), 4)
} else NA_real_
cor_bldg_bg_white <- if (nrow(bldg_bench_3) > 10L) {
  round(cor(bldg_bench_3$pct_white, bldg_bench_3$prior_p_white, use = "complete.obs"), 4)
} else NA_real_

# 2) Within-BG standard deviation of building pct_black
#    (How much do buildings within the same BG-year differ?)
within_bg <- bldg_race[num_persons >= 3, .(
  n_bldgs    = .N,
  bg_mean    = mean(pct_black),
  bg_sd      = sd(pct_black),
  bg_range   = max(pct_black) - min(pct_black)
), by = .(bg_geoid, year)]
within_bg <- within_bg[n_bldgs >= 2]

# 3) ICC: fraction of total variance in pct_black explained by BG
#    (1 - ICC = within-BG share of variance)
if (nrow(bldg_race[num_persons >= 3]) > 100L) {
  bldg_tmp <- bldg_race[num_persons >= 3 & !is.na(bg_geoid)]
  total_var <- var(bldg_tmp$pct_black, na.rm = TRUE)
  bg_means <- bldg_tmp[, .(bg_mean = mean(pct_black)), by = bg_geoid]
  bldg_tmp[bg_means, bg_mean := i.bg_mean, on = "bg_geoid"]
  within_var <- mean((bldg_tmp$pct_black - bldg_tmp$bg_mean)^2, na.rm = TRUE)
  between_var <- total_var - within_var
  icc_black <- round(between_var / total_var, 4)
  rm(bldg_tmp, bg_means)
} else {
  icc_black <- NA_real_
  total_var <- NA_real_
  within_var <- NA_real_
}

logf("Within-BG variation diagnostics (buildings with >= 3 persons):", log_file = log_file)
logf("  Building vs BG cor(pct_black): ", cor_bldg_bg_black, log_file = log_file)
logf("  Building vs BG cor(pct_white): ", cor_bldg_bg_white, log_file = log_file)
logf("  ICC(pct_black, BG): ", icc_black,
     " (between=", round(between_var, 6), ", within=", round(within_var, 6), ")",
     log_file = log_file)
logf("  Median within-BG SD(pct_black): ", round(median(within_bg$bg_sd, na.rm = TRUE), 4),
     " (N BG-years with 2+ bldgs: ", nrow(within_bg), ")", log_file = log_file)
logf("  Median within-BG range(pct_black): ", round(median(within_bg$bg_range, na.rm = TRUE), 4),
     log_file = log_file)

# 4) Year-over-year persistence of building composition
setorder(bldg_race, PID, year)
bldg_race[, prev_pct_black := shift(pct_black, type = "lag"), by = PID]
bldg_race[, prev_pct_white := shift(pct_white, type = "lag"), by = PID]
bldg_race[, prev_year := shift(year, type = "lag"), by = PID]
# Only use consecutive years
consec <- bldg_race[!is.na(prev_pct_black) & (year - prev_year) == 1L & num_persons >= 3]
autocor_black <- if (nrow(consec) > 10L) {
  round(cor(consec$pct_black, consec$prev_pct_black, use = "complete.obs"), 4)
} else NA_real_
autocor_white <- if (nrow(consec) > 10L) {
  round(cor(consec$pct_white, consec$prev_pct_white, use = "complete.obs"), 4)
} else NA_real_

# Mean absolute year-over-year change
mean_abs_chg_black <- if (nrow(consec) > 0L) {
  round(mean(abs(consec$pct_black - consec$prev_pct_black), na.rm = TRUE), 4)
} else NA_real_

logf("Year-over-year persistence (consecutive years, >= 3 persons):", log_file = log_file)
logf("  Autocorrelation pct_black: ", autocor_black, " (N=", nrow(consec), ")", log_file = log_file)
logf("  Autocorrelation pct_white: ", autocor_white, log_file = log_file)
logf("  Mean |delta pct_black|: ", mean_abs_chg_black, log_file = log_file)

# 5) Distribution summary of building pct_black
bldg_race_3 <- bldg_race[num_persons >= 3]
pct_black_quantiles <- if (nrow(bldg_race_3) > 0L) {
  round(quantile(bldg_race_3$pct_black, probs = c(0, 0.1, 0.25, 0.5, 0.75, 0.9, 1), na.rm = TRUE), 4)
} else rep(NA_real_, 7)

logf("Building pct_black distribution (>= 3 persons, N=", nrow(bldg_race_3), "):", log_file = log_file)
logf("  ", paste(names(pct_black_quantiles), "=", pct_black_quantiles, collapse = ", "), log_file = log_file)

# Clean up lag columns before final output was already written
bldg_race[, c("prev_pct_black", "prev_pct_white", "prev_year") := NULL]

## ============================================================
## Step 12: QA report
## ============================================================

status_tab <- persons[, .N, by = impute_status][order(-N)]
race_status_tab <- persons[, .N, by = race_impute_status][order(-N)]
method_tab <- persons[!is.na(race_impute_method), .N, by = race_impute_method][order(-N)]
geo_source_tab <- persons[, .N, by = geo_source][order(-N)]
year_coverage <- persons[race_impute_status == "ok", .N, by = year][order(year)]
year_total <- persons[, .N, by = year][order(year)]

# BG benchmark: compare WRU-imputed mean racial composition per BG to BG priors
bg_wru <- persons[race_impute_status == "ok", .(
  wru_p_white = mean(p_white, na.rm = TRUE),
  wru_p_black = mean(p_black, na.rm = TRUE),
  wru_p_hispanic = mean(p_hispanic, na.rm = TRUE),
  wru_n = .N
), by = bg_geoid]

bg_bench <- merge(bg_wru, pri_geo, by.x = "bg_geoid", by.y = "geo_geoid", all.x = TRUE)
bg_bench <- bg_bench[!is.na(prior_p_white) & wru_n >= 10L]

cor_white <- if (nrow(bg_bench) > 5L) round(cor(bg_bench$wru_p_white, bg_bench$prior_p_white, use = "complete.obs"), 4) else NA_real_
cor_black <- if (nrow(bg_bench) > 5L) round(cor(bg_bench$wru_p_black, bg_bench$prior_p_black, use = "complete.obs"), 4) else NA_real_
mad_white <- if (nrow(bg_bench) > 0L) round(mean(abs(bg_bench$wru_p_white - bg_bench$prior_p_white), na.rm = TRUE), 4) else NA_real_
mad_black <- if (nrow(bg_bench) > 0L) round(mean(abs(bg_bench$wru_p_black - bg_bench$prior_p_black), na.rm = TRUE), 4) else NA_real_

# Cross-tab: WRU race_hat vs InfoUSA ethnicity_code
xtab <- persons[race_impute_status == "ok" & !is.na(ethnicity_code) & !is.na(race_hat),
                .N, by = .(ethnicity_code, race_hat)]
xtab_wide <- if (nrow(xtab) > 0L) {
  dcast(xtab, ethnicity_code ~ race_hat, value.var = "N", fill = 0L)
} else {
  data.table(ethnicity_code = character())
}

# Head-of-household vs all persons
hoh_tab <- persons[race_impute_status == "ok", .(
  n = .N,
  mean_p_white = round(mean(p_white, na.rm = TRUE), 4),
  mean_p_black = round(mean(p_black, na.rm = TRUE), 4),
  mean_p_hispanic = round(mean(p_hispanic, na.rm = TRUE), 4)
), by = head_of_household]

# Write QA
qa_lines <- c(
  "InfoUSA Race Imputation QA (WRU)",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  paste0("Input HH-year rows: ", n_wide),
  paste0("Person rows (after melt + filter): ", n_persons),
  paste0("Rows written: ", nrow(persons)),
  paste0("WRU year: ", wru_year),
  paste0("Inference geography: ", inference_geo),
  paste0("Prior source geography: ", prior_source_geo),
  paste0("Priors file: ", priors_path),
  "",
  "=== Impute Status Counts ===",
  capture.output(print(status_tab)),
  "",
  "=== Race Impute Status Counts ===",
  capture.output(print(race_status_tab)),
  "",
  "=== Method Counts ===",
  capture.output(print(method_tab)),
  "",
  "=== Geo Source Counts ===",
  capture.output(print(geo_source_tab)),
  "",
  "=== Coverage by Year (ok rows / total) ===",
  capture.output(print(merge(year_total, year_coverage, by = "year", all.x = TRUE, suffixes = c("_total", "_ok")))),
  "",
  "=== GEOID Agreement: InfoUSA BG vs Parcel-linked BG ===",
  paste0("Both present: ", n_both),
  paste0("Agree: ", n_agree, " (", round(100 * agree_rate, 1), "%)"),
  paste0("Disagree: ", n_disagree),
  "",
  "=== BG Benchmark: WRU vs Priors (BGs with >= 10 persons) ===",
  paste0("BGs compared: ", nrow(bg_bench)),
  paste0("Correlation (white): ", cor_white),
  paste0("Correlation (black): ", cor_black),
  paste0("MAD (white): ", mad_white),
  paste0("MAD (black): ", mad_black),
  "",
  "=== Cross-tab: WRU race_hat vs InfoUSA ethnicity_code ===",
  capture.output(print(xtab_wide)),
  "",
  "=== Head-of-Household vs All Persons (ok rows) ===",
  capture.output(print(hoh_tab)),
  "",
  "=== PID Link & Rental/Eviction Flags ===",
  paste0("PID linked: ", n_pid_linked, " / ", n_persons, " (", round(100 * n_pid_linked / n_persons, 1), "%)"),
  paste0("At rental PID: ", n_rental, " / ", n_persons, " (", round(100 * n_rental / n_persons, 1), "%)"),
  paste0("At eviction PID: ", n_evict, " / ", n_persons),
  "",
  "=== Building-Level Race Panel (PID x year) ===",
  paste0("Rows: ", nrow(bldg_race)),
  paste0("Unique PIDs: ", bldg_race[, uniqueN(PID)]),
  paste0("Median num_persons per PID-year: ", median(bldg_race$num_persons)),
  paste0("Output: ", out_bldg_race_path),
  "",
  "=== Within-BG Variation (buildings with >= 3 persons) ===",
  paste0("Building vs BG cor(pct_black): ", cor_bldg_bg_black),
  paste0("Building vs BG cor(pct_white): ", cor_bldg_bg_white),
  paste0("ICC(pct_black, BG): ", icc_black),
  paste0("  Total var: ", round(total_var, 6),
         ", Between-BG: ", round(between_var, 6),
         ", Within-BG: ", round(within_var, 6)),
  paste0("Median within-BG SD(pct_black): ", round(median(within_bg$bg_sd, na.rm = TRUE), 4),
         " (N BG-years: ", nrow(within_bg), ")"),
  paste0("Median within-BG range(pct_black): ", round(median(within_bg$bg_range, na.rm = TRUE), 4)),
  "",
  "=== Year-over-Year Persistence (consecutive years, >= 3 persons) ===",
  paste0("N consecutive PID-year pairs: ", nrow(consec)),
  paste0("Autocorrelation pct_black: ", autocor_black),
  paste0("Autocorrelation pct_white: ", autocor_white),
  paste0("Mean |delta pct_black|: ", mean_abs_chg_black),
  "",
  "=== Building pct_black Distribution (>= 3 persons) ===",
  paste0("N building-years: ", nrow(bldg_race_3)),
  paste(names(pct_black_quantiles), "=", pct_black_quantiles, collapse = ", ")
)

writeLines(qa_lines, con = out_qa_path)
logf("Wrote QA report: ", out_qa_path, log_file = log_file)
logf("=== Finished impute-race-infousa.R (WRU) ===", log_file = log_file)
