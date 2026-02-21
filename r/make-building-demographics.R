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

logf("=== Starting make-building-demographics.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf("Race input: ", race_path, log_file = log_file)
logf("Gender input: ", gender_path, log_file = log_file)
logf("Output: ", out_path, log_file = log_file)

if (!file.exists(race_path)) stop("Missing race input: ", race_path)
if (!file.exists(gender_path)) stop("Missing gender input: ", gender_path)

race_need <- c(
  "familyid", "family_id", "locationid", "individual_id", "person_num",
  "PID", "year", "bg_geoid", "head_of_household", "in_eviction_data", "is_rental_pid",
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

persons <- merge(race, gender, by = merge_keys, all.x = TRUE, suffixes = c("", "_g"))
logf("Merged person rows: ", nrow(persons), log_file = log_file)

if ("PID_g" %in% names(persons)) {
  persons[is.na(PID) | PID == "000000000", PID := PID_g]
  persons[, PID_g := NULL]
}
if ("year_g" %in% names(persons)) {
  persons[is.na(year), year := as.integer(year_g)]
  persons[, year_g := NULL]
}

persons[, race_ok := race_impute_status == "ok"]
persons[, gender_ok := gender_impute_status == "ok"]
persons[, demo_ok := race_ok == TRUE & gender_ok == TRUE]
persons <- persons[!is.na(PID) & nzchar(PID) & PID != "000000000" & !is.na(year)]

if (!nrow(persons)) stop("No PID-year person rows after merging race and gender outputs.")

for (cc in c("p_white", "p_black", "p_hispanic", "p_asian", "p_other", "p_female", "p_male")) {
  persons[!is.finite(get(cc)), (cc) := NA_real_]
}

hh_id <- if ("familyid" %in% names(persons)) "familyid" else if ("family_id" %in% names(persons)) "family_id" else NULL

bldg_demo <- persons[, {
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
    infousa_bg_geoid_first = bg_geoid[which.max(!is.na(bg_geoid) & nzchar(as.character(bg_geoid)))]
  )
  if (!is.null(hh_id) && hh_id %in% names(.SD)) {
    out$infousa_num_households <- uniqueN(.SD[[hh_id]])
  } else {
    out$infousa_num_households <- NA_integer_
  }
  out
}, by = .(PID, year)]

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
  paste0("Race input rows: ", nrow(race)),
  paste0("Gender input rows: ", nrow(gender)),
  paste0("Merged person rows with PID/year: ", nrow(persons)),
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
