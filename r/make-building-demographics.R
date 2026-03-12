## ============================================================
## make-building-demographics.R
## ============================================================
## Purpose: Build PID x year building demographics from InfoUSA
##          race + gender person-level imputation outputs.
##
## Inputs (via config):
##   - cfg$products$infousa_race_imputed_person
##   - cfg$products$infousa_gender_imputed_person
##
## Output:
##   - cfg$products$infousa_building_demographics_panel
##   - output/qa/infousa_building_demographics_qa.txt
##   - output/logs/make-building-demographics.log
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
})

source("r/config.R")

parse_cli_args <- function(args) {
  out <- list()
  if (length(args) == 0L) return(out)
  for (arg in args) {
    if (!startsWith(arg, "--")) next
    arg <- sub("^--", "", arg)
    parts <- strsplit(arg, "=", fixed = TRUE)[[1L]]
    key <- gsub("-", "_", parts[1L])
    val <- if (length(parts) >= 2L) paste(parts[-1L], collapse = "=") else "TRUE"
    out[[key]] <- val
  }
  out
}

normalize_pid <- function(x) str_pad(as.character(x), width = 9L, side = "left", pad = "0")
normalize_chr_id <- function(x) {
  x <- as.character(x)
  x[!nzchar(x)] <- NA_character_
  x
}

default_dt_threads <- function() {
  n_phys <- suppressWarnings(as.integer(parallel::detectCores(logical = FALSE)))
  n_logi <- suppressWarnings(as.integer(parallel::detectCores(logical = TRUE)))
  n_base <- if (is.finite(n_phys) && n_phys > 0L) {
    n_phys
  } else if (is.finite(n_logi) && n_logi > 0L) {
    max(1L, n_logi %/% 2L)
  } else {
    4L
  }
  as.integer(max(1L, min(8L, n_base)))
}

resolve_dt_threads <- function(cli_val, env_val, fallback) {
  cand <- suppressWarnings(as.integer(cli_val))
  if (length(cand) == 0L || is.na(cand)) cand <- suppressWarnings(as.integer(env_val))
  if (length(cand) == 0L) cand <- NA_integer_
  if (!is.finite(cand) || cand < 1L) cand <- fallback
  as.integer(cand)
}

choose_merge_keys <- function(race_dt, gender_dt) {
  candidates <- list(
    c("familyid", "family_id", "locationid", "year", "person_num", "individual_id"),
    c("familyid", "family_id", "locationid", "year", "person_num"),
    c("familyid", "locationid", "year", "person_num"),
    c("family_id", "locationid", "year", "person_num")
  )
  for (k in candidates) {
    if (!all(k %in% names(race_dt)) || !all(k %in% names(gender_dt))) next
    if (uniqueN(race_dt, by = k) == nrow(race_dt) && uniqueN(gender_dt, by = k) == nrow(gender_dt)) {
      return(k)
    }
  }
  stop("Could not find a unique shared person-level merge key between race and gender InfoUSA outputs.")
}

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

race_path <- opts$race_input %||% p_product(cfg, "infousa_race_imputed_person")
gender_path <- opts$gender_input %||% p_product(cfg, "infousa_gender_imputed_person")
out_path <- opts$output %||% p_product(cfg, "infousa_building_demographics_panel")
out_qa <- opts$output_qa %||% p_out(cfg, "qa", "infousa_building_demographics_qa.txt")
log_file <- p_out(cfg, "logs", "make-building-demographics.log")

dt_threads_default <- default_dt_threads()
dt_threads_requested <- resolve_dt_threads(
  cli_val = opts$dt_threads %||% opts$threads,
  env_val = Sys.getenv("PHILLY_DT_THREADS", unset = ""),
  fallback = dt_threads_default
)
setDTthreads(dt_threads_requested)

logf("=== Starting make-building-demographics.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf(
  "data.table threads: active=",
  data.table::getDTthreads(),
  " requested=",
  dt_threads_requested,
  " default=",
  dt_threads_default,
  log_file = log_file
)
logf("Race input: ", race_path, log_file = log_file)
logf("Gender input: ", gender_path, log_file = log_file)
logf("Output: ", out_path, log_file = log_file)

if (!file.exists(race_path)) stop("Missing race input: ", race_path)
if (!file.exists(gender_path)) stop("Missing gender input: ", gender_path)

race_need <- c(
  "familyid", "family_id", "locationid", "individual_id", "person_num",
  "PID", "year", "bg_geoid", "head_of_household", "in_eviction_data", "is_rental_pid",
  "infousa_link_type", "infousa_link_id", "infousa_anchor_pid", "infousa_ownership_unsafe", "infousa_xwalk_status",
  "race_impute_status", "race_hat",
  "p_white", "p_black", "p_hispanic", "p_asian", "p_other"
)
gender_need <- c(
  "familyid", "family_id", "locationid", "individual_id", "person_num",
  "PID", "year", "gender_impute_status", "gender_hat", "p_female", "p_male"
)

race_hdr <- names(fread(race_path, nrows = 0L))
gender_hdr <- names(fread(gender_path, nrows = 0L))
race <- fread(race_path, select = intersect(race_need, race_hdr))
gender <- fread(gender_path, select = intersect(gender_need, gender_hdr))
setDT(race)
setDT(gender)
n_race_rows_in <- nrow(race)
n_gender_rows_in <- nrow(gender)

assert_has_cols(race, c("PID", "year", "race_impute_status", "p_black", "p_white"), "race person file")
assert_has_cols(gender, c("PID", "year", "gender_impute_status", "p_female", "p_male"), "gender person file")

merge_keys <- choose_merge_keys(race, gender)
logf("Using merge keys: ", paste(merge_keys, collapse = ", "), log_file = log_file)

assert_unique(race, merge_keys, "race person table on merge keys")
assert_unique(gender, merge_keys, "gender person table on merge keys")

race[, PID := normalize_pid(PID)]
gender[, PID := normalize_pid(PID)]
race[, year := as.integer(year)]
gender[, year := as.integer(year)]
for (cc in c("p_white", "p_black", "p_hispanic", "p_asian", "p_other")) {
  if (!(cc %in% names(race))) race[, (cc) := NA_real_]
  race[, (cc) := as.numeric(get(cc))]
}
for (cc in c("p_female", "p_male")) gender[, (cc) := as.numeric(get(cc))]

gender_keep <- unique(c(merge_keys, "PID", "year", "gender_impute_status", "gender_hat", "p_female", "p_male"))
gender <- gender[, ..gender_keep]

# Memory-conscious left join: add gender columns onto race in place rather than
# materializing a full merged copy of both tables.
race[gender, on = merge_keys, `:=`(
  PID_g = i.PID,
  year_g = i.year,
  gender_impute_status = i.gender_impute_status,
  gender_hat = i.gender_hat,
  p_female = i.p_female,
  p_male = i.p_male
)]
persons <- race
logf("Merged person rows: ", nrow(persons), log_file = log_file)
rm(race, gender)
gc(verbose = FALSE)

if ("PID_g" %in% names(persons)) {
  persons[is.na(PID) | PID == "000000000", PID := PID_g]
  persons[, PID_g := NULL]
}
if ("year_g" %in% names(persons)) {
  persons[is.na(year), year := as.integer(year_g)]
  persons[, year_g := NULL]
}

hh_id <- if ("familyid" %in% names(persons)) "familyid" else if ("family_id" %in% names(persons)) "family_id" else NULL

# Drop unused columns immediately after the person-level merge to reduce peak memory
# during Phase 2A link-metadata normalization and aggregation prep.
pre_keep <- unique(c(
  "PID", "year", "bg_geoid", "head_of_household", "in_eviction_data", "is_rental_pid",
  "race_impute_status", "race_hat", "gender_impute_status", "gender_hat",
  "p_white", "p_black", "p_hispanic", "p_asian", "p_other", "p_female", "p_male",
  "infousa_link_id", "infousa_link_type", "infousa_anchor_pid", "infousa_ownership_unsafe", "infousa_xwalk_status",
  hh_id
))
pre_keep <- intersect(pre_keep, names(persons))
persons <- persons[, ..pre_keep]
gc(verbose = FALSE)
logf("Trimmed merged person table before normalization: ", length(pre_keep), " columns retained", log_file = log_file)

persons[["race_ok"]] <- persons[["race_impute_status"]] == "ok"
persons[["gender_ok"]] <- persons[["gender_impute_status"]] == "ok"
persons[["demo_ok"]] <- persons[["race_ok"]] == TRUE & persons[["gender_ok"]] == TRUE

valid_pid_year_expr <- quote(!is.na(PID) & nzchar(PID) & PID != "000000000" & !is.na(year))
n_person_rows_pid_year <- persons[eval(valid_pid_year_expr), .N]
if (!n_person_rows_pid_year) stop("No PID-year person rows after merging race and gender outputs.")
logf("Person rows with valid PID/year after merge cleanup: ", n_person_rows_pid_year, log_file = log_file)

for (cc in c("p_white", "p_black", "p_hispanic", "p_asian", "p_other", "p_female", "p_male")) {
  vv <- persons[[cc]]
  vv[!is.finite(vv)] <- NA_real_
  set(persons, j = cc, value = vv)
}

# Phase 2A: condo-aware InfoUSA linkage metadata (fallbacks preserve parcel-only behavior)
if (!"infousa_link_type" %in% names(persons)) persons[["infousa_link_type"]] <- "parcel"
if (!"infousa_link_id" %in% names(persons)) persons[["infousa_link_id"]] <- persons$PID
if (!"infousa_anchor_pid" %in% names(persons)) persons[["infousa_anchor_pid"]] <- persons$PID
if (!"infousa_ownership_unsafe" %in% names(persons)) persons[["infousa_ownership_unsafe"]] <- FALSE
if (!"infousa_xwalk_status" %in% names(persons)) persons[["infousa_xwalk_status"]] <- "unknown"

infousa_link_type_vec <- as.character(persons[["infousa_link_type"]])
infousa_link_type_vec <- ifelse(infousa_link_type_vec == "condo_group", "condo_group", "parcel")
set(persons, j = "infousa_link_type", value = infousa_link_type_vec)

infousa_link_id_vec <- normalize_chr_id(persons[["infousa_link_id"]])
infousa_anchor_pid_vec <- as.character(persons[["infousa_anchor_pid"]])
idx_anchor_missing <- is.na(infousa_anchor_pid_vec) | !nzchar(infousa_anchor_pid_vec)
if (any(idx_anchor_missing)) {
  infousa_anchor_pid_vec[idx_anchor_missing] <- persons$PID[idx_anchor_missing]
}
infousa_anchor_pid_vec <- normalize_pid(infousa_anchor_pid_vec)
idx_link_missing_condo <- which(is.na(infousa_link_id_vec) & infousa_link_type_vec == "condo_group")
if (length(idx_link_missing_condo) > 0L) {
  infousa_link_id_vec[idx_link_missing_condo] <- paste0("cg_missing_anchor_", infousa_anchor_pid_vec[idx_link_missing_condo])
}
idx_link_missing <- which(is.na(infousa_link_id_vec))
if (length(idx_link_missing) > 0L) {
  infousa_link_id_vec[idx_link_missing] <- persons$PID[idx_link_missing]
}
set(persons, j = "infousa_link_id", value = infousa_link_id_vec)
set(persons, j = "infousa_anchor_pid", value = infousa_anchor_pid_vec)
set(persons, j = "infousa_ownership_unsafe", value = as.logical(fcoalesce(persons[["infousa_ownership_unsafe"]], FALSE)))
infousa_xwalk_status_vec <- as.character(persons[["infousa_xwalk_status"]])
infousa_xwalk_status_vec[is.na(infousa_xwalk_status_vec)] <- "unknown"
set(persons, j = "infousa_xwalk_status", value = infousa_xwalk_status_vec)

# Keep only columns needed for aggregation to reduce memory footprint before link-level grouping.
agg_keep <- unique(c(
  "PID", "year", "bg_geoid", "head_of_household", "in_eviction_data", "is_rental_pid",
  "race_impute_status", "race_hat", "gender_impute_status", "gender_hat",
  "race_ok", "gender_ok", "demo_ok",
  "p_white", "p_black", "p_hispanic", "p_asian", "p_other", "p_female", "p_male",
  "infousa_link_id", "infousa_link_type", "infousa_anchor_pid", "infousa_ownership_unsafe", "infousa_xwalk_status",
  hh_id
))
agg_keep <- intersect(agg_keep, names(persons))
extra_cols <- setdiff(names(persons), agg_keep)
if (length(extra_cols) > 0L) {
  persons <- persons[, ..agg_keep]
}
gc(verbose = FALSE)
logf("Trimmed person columns for aggregation: ", length(agg_keep), " columns retained", log_file = log_file)
n_person_rows_merged <- n_person_rows_pid_year

# Condo group membership (used to propagate condo-group shares back to parcel rows)
condo_membership_path <- tryCatch(p_product(cfg, "condo_parcel_group_membership"), error = function(e) NULL)
condo_members <- data.table(
  infousa_link_id = character(),
  PID = character(),
  infousa_anchor_pid_group = character(),
  condo_group_size_pids = integer()
)
if (!is.null(condo_membership_path) && file.exists(condo_membership_path)) {
  cm_hdr <- names(fread(condo_membership_path, nrows = 0L))
  cm_keep <- intersect(c("parcel_number", "condo_group_id", "anchor_pid", "group_size_pids"), cm_hdr)
  cm <- fread(condo_membership_path, select = cm_keep)
  setDT(cm)
  if (all(c("parcel_number", "condo_group_id") %in% names(cm))) {
    cm[, PID := normalize_pid(parcel_number)]
    cm[, infousa_link_id := normalize_chr_id(condo_group_id)]
    if ("anchor_pid" %in% names(cm)) cm[, infousa_anchor_pid_group := normalize_pid(anchor_pid)] else cm[, infousa_anchor_pid_group := PID]
    if (!"group_size_pids" %in% names(cm)) cm[, group_size_pids := NA_integer_]
    condo_members <- unique(
      cm[!is.na(infousa_link_id) & !is.na(PID),
         .(infousa_link_id, PID, infousa_anchor_pid_group, condo_group_size_pids = as.integer(group_size_pids))],
      by = c("infousa_link_id", "PID")
    )
    assert_unique(condo_members, c("infousa_link_id", "PID"), "condo_members (link_id, PID)")
    logf("Loaded condo parcel group membership: ", nrow(condo_members), " link_id x PID rows (",
         condo_members[, uniqueN(infousa_link_id)], " condo groups)", log_file = log_file)
  } else {
    logf("WARNING: condo_parcel_group_membership missing required columns; skipping condo propagation fallback", log_file = log_file)
  }
} else {
  logf("WARNING: condo_parcel_group_membership product not found; condo-group demographics propagation disabled", log_file = log_file)
}

aggregate_demog <- function(dt, by_cols, hh_id_col = NULL, link_type_filter = NULL, link_type_negate = FALSE) {
  agg_j <- quote({
  n_total <- .N
  n_race_ok <- sum(race_ok, na.rm = TRUE)
  n_gender_ok <- sum(gender_ok, na.rm = TRUE)
  n_demo_ok <- sum(demo_ok, na.rm = TRUE)
  idx_r <- race_ok == TRUE
  idx_g <- gender_ok == TRUE
  idx_d <- demo_ok == TRUE
  out <- list(
    infousa_num_persons_total = n_total,
    infousa_num_persons_race_ok = n_race_ok,
    infousa_num_persons_gender_ok = n_gender_ok,
    infousa_num_persons_demog_ok = n_demo_ok,
    infousa_share_persons_demog_ok = ifelse(n_total > 0, n_demo_ok / n_total, NA_real_),
    infousa_pct_white = mean(p_white[idx_r], na.rm = TRUE),
    infousa_pct_black = mean(p_black[idx_r], na.rm = TRUE),
    infousa_pct_hispanic = mean(p_hispanic[idx_r], na.rm = TRUE),
    infousa_pct_asian = mean(p_asian[idx_r], na.rm = TRUE),
    infousa_pct_other = mean(p_other[idx_r], na.rm = TRUE),
    infousa_pct_female = mean(p_female[idx_g], na.rm = TRUE),
    infousa_pct_male = mean(p_male[idx_g], na.rm = TRUE),
    infousa_pct_white_female = mean((p_white * p_female)[idx_d], na.rm = TRUE),
    infousa_pct_white_male = mean((p_white * p_male)[idx_d], na.rm = TRUE),
    infousa_pct_black_female = mean((p_black * p_female)[idx_d], na.rm = TRUE),
    infousa_pct_black_male = mean((p_black * p_male)[idx_d], na.rm = TRUE),
    infousa_pct_hispanic_female = mean((p_hispanic * p_female)[idx_d], na.rm = TRUE),
    infousa_pct_hispanic_male = mean((p_hispanic * p_male)[idx_d], na.rm = TRUE),
    infousa_pct_asian_female = mean((p_asian * p_female)[idx_d], na.rm = TRUE),
    infousa_pct_asian_male = mean((p_asian * p_male)[idx_d], na.rm = TRUE),
    infousa_pct_other_female = mean((p_other * p_female)[idx_d], na.rm = TRUE),
    infousa_pct_other_male = mean((p_other * p_male)[idx_d], na.rm = TRUE),
    infousa_pct_black_hat = mean(race_hat[idx_r] == "black", na.rm = TRUE),
    infousa_pct_female_hat = mean(gender_hat[idx_g] == "female", na.rm = TRUE),
    infousa_any_eviction_data = any(as.logical(in_eviction_data), na.rm = TRUE),
    infousa_any_rental_pid = any(as.logical(is_rental_pid), na.rm = TRUE),
    infousa_bg_geoid_first = bg_geoid[which.max(!is.na(bg_geoid) & nzchar(as.character(bg_geoid)))],
    infousa_any_ownership_unsafe = any(as.logical(infousa_ownership_unsafe), na.rm = TRUE),
    infousa_xwalk_status_mode = {
      xs <- infousa_xwalk_status[!is.na(infousa_xwalk_status) & nzchar(as.character(infousa_xwalk_status))]
      if (length(xs) == 0L) NA_character_ else names(sort(table(xs), decreasing = TRUE))[1L]
    }
  )
  if (!is.null(hh_id_col) && hh_id_col %in% names(dt)) {
    out$infousa_num_households <- uniqueN(get(hh_id_col))
  } else {
    out$infousa_num_households <- NA_integer_
  }
  out
  })
  if (is.null(link_type_filter)) {
    return(dt[, eval(agg_j), by = by_cols])
  }
  if (isTRUE(link_type_negate)) {
    return(dt[infousa_link_type != link_type_filter, eval(agg_j), by = by_cols])
  }
  dt[infousa_link_type == link_type_filter, eval(agg_j), by = by_cols]
}

aggregate_demog_by_year <- function(dt, years_vec, by_cols, hh_id_col = NULL,
                                    link_type_filter = NULL, link_type_negate = FALSE,
                                    label = "agg") {
  out_list <- vector("list", length(years_vec))
  for (i in seq_along(years_vec)) {
    yy <- years_vec[[i]]
    dt_year <- dt[year == yy & !is.na(PID) & nzchar(PID) & PID != "000000000"]
    out_list[[i]] <- aggregate_demog(
      dt_year,
      by_cols = by_cols,
      hh_id_col = hh_id_col,
      link_type_filter = link_type_filter,
      link_type_negate = link_type_negate
    )
    if ((i %% 5L) == 0L || i == length(years_vec)) {
      logf("  [", label, "] completed year ", yy, " (", i, "/", length(years_vec), ")", log_file = log_file)
      gc(verbose = FALSE)
    }
  }
  rbindlist(out_list, use.names = TRUE, fill = TRUE)
}

gc(verbose = FALSE)
years_all <- sort(unique(persons$year))

logf("Aggregating parcel-linked demographics to PID x year...", log_file = log_file)
bldg_demo_parcel <- aggregate_demog_by_year(
  persons,
  years_vec = years_all,
  by_cols = c("PID", "year"),
  hh_id_col = hh_id,
  link_type_filter = "condo_group",
  link_type_negate = TRUE,
  label = "parcel"
)
bldg_demo_parcel[, `:=`(
  infousa_link_type = "parcel",
  infousa_link_id = PID,
  infousa_anchor_pid = PID,
  infousa_demog_provenance = "parcel",
  infousa_condo_group_size_pids = NA_integer_
)]

logf("Aggregating condo-group-linked demographics to link_id x year...", log_file = log_file)
bldg_demo_condo_link <- aggregate_demog_by_year(
  persons,
  years_vec = years_all,
  by_cols = c("infousa_link_id", "infousa_anchor_pid", "year"),
  hh_id_col = hh_id,
  link_type_filter = "condo_group",
  link_type_negate = FALSE,
  label = "condo"
)
bldg_demo_condo_link[, infousa_link_type := "condo_group"]
rm(persons)
gc(verbose = FALSE)

n_link_rows_total <- nrow(bldg_demo_parcel) + nrow(bldg_demo_condo_link)
logf("Link-level demographics rows (parcel PID-years + condo-group link-years): ", n_link_rows_total, log_file = log_file)

# Expand condo-group link rows back to PID x year (parcel-native rows already built).
bldg_demo_condo <- copy(bldg_demo_condo_link)
if (nrow(bldg_demo_condo) > 0L) {
  n_condo_link_rows_before <- nrow(bldg_demo_condo)
  bldg_demo_condo <- merge(
    bldg_demo_condo,
    condo_members,
    by = "infousa_link_id",
    all.x = TRUE,
    allow.cartesian = TRUE
  )
  n_condo_unmapped <- bldg_demo_condo[is.na(PID), .N]
  if (n_condo_unmapped > 0L) {
    logf("WARNING: ", n_condo_unmapped, " condo-group demographics rows could not map to condo member PIDs and were dropped",
         log_file = log_file)
  }
  bldg_demo_condo <- bldg_demo_condo[!is.na(PID)]
  bldg_demo_condo[, infousa_anchor_pid := fcoalesce(infousa_anchor_pid_group, infousa_anchor_pid)]
  bldg_demo_condo[, `:=`(
    infousa_demog_provenance = "condo_group",
    infousa_condo_group_size_pids = condo_group_size_pids
  )]
  # Condo-group totals (households / person counts) are not parcel-assignable.
  for (cc in intersect(c(
    "infousa_num_persons_total", "infousa_num_persons_race_ok", "infousa_num_persons_gender_ok",
    "infousa_num_persons_demog_ok", "infousa_num_households"
  ), names(bldg_demo_condo))) {
    bldg_demo_condo[, (cc) := NA_integer_]
  }
  logf("Condo-group demographics propagation: ", n_condo_link_rows_before, " link-year rows expanded to ",
       nrow(bldg_demo_condo), " PID-year candidates", log_file = log_file)
}

if (!nrow(bldg_demo_parcel) && !nrow(bldg_demo_condo)) {
  stop("No PID-year rows after parcel/condo demographics expansion.")
}

assert_unique(bldg_demo_parcel, c("PID", "year"), "parcel-linked demographics (PID, year)")
if (nrow(bldg_demo_condo) > 0L) {
  assert_unique(bldg_demo_condo, c("PID", "year"), "condo-propagated demographics (PID, year)")
}

# Parcel-native rows take precedence. Fill only missing PID-years from condo-group
# propagation to avoid a large rbind/sort/dedupe pass over the full table.
setkey(bldg_demo_parcel, PID, year)
if (nrow(bldg_demo_condo) > 0L) {
  setkey(bldg_demo_condo, PID, year)
  n_condo_candidates_before_fill <- nrow(bldg_demo_condo)
  bldg_demo_condo_fill <- bldg_demo_condo[!bldg_demo_parcel]
  logf("Condo-group fill rows retained after parcel-precedence anti-join: ",
       nrow(bldg_demo_condo_fill), " / ", n_condo_candidates_before_fill, log_file = log_file)
  bldg_demo <- rbindlist(list(bldg_demo_parcel, bldg_demo_condo_fill), use.names = TRUE, fill = TRUE)
  rm(bldg_demo_condo_fill)
} else {
  bldg_demo <- copy(bldg_demo_parcel)
}
rm(bldg_demo_parcel, bldg_demo_condo, bldg_demo_condo_link)
gc(verbose = FALSE)

for (cc in names(bldg_demo)) {
  if (startsWith(cc, "infousa_pct_") || startsWith(cc, "infousa_share_")) {
    bldg_demo[, (cc) := as.numeric(get(cc))]
    bldg_demo[!is.finite(get(cc)), (cc) := NA_real_]
  }
}
bldg_demo[, infousa_pct_intersection_sum := as.numeric(infousa_pct_white_female + infousa_pct_white_male +
            infousa_pct_black_female + infousa_pct_black_male +
            infousa_pct_hispanic_female + infousa_pct_hispanic_male +
            infousa_pct_asian_female + infousa_pct_asian_male +
            infousa_pct_other_female + infousa_pct_other_male)]

assert_unique(bldg_demo, c("PID", "year"), "infousa building demographics panel")

dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_qa), recursive = TRUE, showWarnings = FALSE)
fwrite(bldg_demo, out_path)

qa_lines <- c(
  "InfoUSA Building Demographics QA",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  paste0("Race input rows: ", n_race_rows_in),
  paste0("Gender input rows: ", n_gender_rows_in),
  paste0("Merged person rows with PID/year: ", n_person_rows_merged),
  paste0("Link-level demographics rows: ", n_link_rows_total),
  paste0("Building panel rows (PID-year): ", nrow(bldg_demo)),
  paste0("Unique PIDs: ", uniqueN(bldg_demo$PID)),
  "",
  "Merge keys:",
  paste0("  ", paste(merge_keys, collapse = ", ")),
  "",
  "Demographics coverage summary:",
  capture.output(print(bldg_demo[, .(
    mean_persons_total = mean(infousa_num_persons_total, na.rm = TRUE),
    mean_share_demog_ok = mean(infousa_share_persons_demog_ok, na.rm = TRUE),
    median_share_demog_ok = median(infousa_share_persons_demog_ok, na.rm = TRUE),
    mean_pct_black = mean(infousa_pct_black, na.rm = TRUE),
    mean_pct_female = mean(infousa_pct_female, na.rm = TRUE)
  )])),
  "",
  "Phase 2A provenance summary:",
  capture.output(print(bldg_demo[, .N, by = .(infousa_demog_provenance, infousa_any_ownership_unsafe)][order(-N)])),
  "",
  "Intersection sum diagnostic (should be close to 1 where demog_ok is high):",
  capture.output(print(bldg_demo[, .(
    p01 = quantile(infousa_pct_intersection_sum, 0.01, na.rm = TRUE),
    p50 = quantile(infousa_pct_intersection_sum, 0.50, na.rm = TRUE),
    p99 = quantile(infousa_pct_intersection_sum, 0.99, na.rm = TRUE)
  )]))
)
writeLines(qa_lines, out_qa)

logf("Wrote building demographics panel: ", out_path, log_file = log_file)
logf("Wrote QA: ", out_qa, log_file = log_file)
logf("=== Finished make-building-demographics.R ===", log_file = log_file)
