## ============================================================
## impute-gender-infousa.R
## ============================================================
## Purpose: Impute gender probabilities on InfoUSA person rows
##          using first names, aligned to infousa_race_imputed_person.
##
## Inputs (via config):
##   - cfg$products$infousa_race_imputed_person
##
## Outputs:
##   - cfg$products$infousa_gender_imputed_person
##   - output/qa/infousa_gender_imputed_qa.txt
##   - output/logs/impute-gender-infousa.log
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

to_lower_name <- function(x) {
  x <- as.character(x)
  x <- tolower(str_squish(x))
  x[x %in% c("", "na")] <- NA_character_
  x
}

normalize_gender_raw <- function(x) {
  x <- toupper(str_squish(as.character(x)))
  out <- fifelse(
    x %chin% c("F", "FEMALE"), "female",
    fifelse(x %chin% c("M", "MALE"), "male", NA_character_)
  )
  out
}

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

input_path <- opts$input %||% p_product(cfg, "infousa_race_imputed_person")
lookup_path <- opts$lookup %||% NA_character_
backend <- tolower(opts$backend %||% "gender_pkg")
confidence_threshold <- suppressWarnings(as.numeric(opts$confidence_threshold %||% 0.7))
sample_n <- suppressWarnings(as.integer(opts$sample_n %||% NA_character_))
seed <- suppressWarnings(as.integer(opts$seed %||% cfg$run$seed %||% 123L))

out_person <- opts$output %||% p_product(cfg, "infousa_gender_imputed_person")
out_qa <- opts$output_qa %||% p_out(cfg, "qa", "infousa_gender_imputed_qa.txt")
log_file <- p_out(cfg, "logs", "impute-gender-infousa.log")

logf("=== Starting impute-gender-infousa.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf("Input: ", input_path, log_file = log_file)
logf("Backend: ", backend, ", lookup: ", lookup_path, log_file = log_file)

if (!file.exists(input_path)) stop("Missing input file: ", input_path)

dir.create(dirname(out_person), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_qa), recursive = TRUE, showWarnings = FALSE)

dt <- fread(input_path)
setDT(dt)
assert_has_cols(dt, c("first_name", "year"), "infousa race-imputed person input")

person_key_cols <- c("familyid", "family_id", "locationid", "year", "person_num")
person_key_cols <- person_key_cols[person_key_cols %in% names(dt)]
if (length(person_key_cols) < 3L) {
  stop("Insufficient person key columns in input. Expected at least family/location/year/person fields.")
}
assert_unique(dt, person_key_cols, "infousa person input")

n_input <- nrow(dt)
if (!is.na(sample_n) && sample_n > 0L && sample_n < n_input) {
  set.seed(seed)
  dt <- dt[sample(.N, sample_n)]
  logf("Sample mode: kept ", nrow(dt), " rows from ", n_input, " with seed=", seed, log_file = log_file)
}

dt[, first_l := to_lower_name(first_name)]
dt[, gender_raw_norm := if ("gender" %in% names(dt)) normalize_gender_raw(gender) else NA_character_]
dt[, `:=`(p_male = NA_real_, p_female = NA_real_, gender_impute_status = "missing_first_name")]

targets <- dt[!is.na(first_l), .(first_l)]
u <- unique(targets, by = "first_l")
logf("Unique first names to score: ", nrow(u), log_file = log_file)

if (backend == "gender_pkg" && requireNamespace("gender", quietly = TRUE)) {
  g <- gender::gender(
    names = u$first_l,
    years = c(1932, 2012),
    method = "ssa"
  )
  setDT(g)
  if ("name" %in% names(g)) g[, first_l := to_lower_name(name)]
  if ("proportion_male" %in% names(g)) g[, p_male := as.numeric(proportion_male)]
  if ("proportion_female" %in% names(g)) g[, p_female := as.numeric(proportion_female)]
  if (!("p_male" %in% names(g)) && "p_female" %in% names(g)) g[, p_male := 1 - p_female]
  if (!("p_female" %in% names(g)) && "p_male" %in% names(g)) g[, p_female := 1 - p_male]
  if (!all(c("p_male", "p_female") %in% names(g)) && "gender" %in% names(g)) {
    g[, p_male := fifelse(gender == "male", 0.9, fifelse(gender == "female", 0.1, NA_real_))]
    g[, p_female := fifelse(gender == "female", 0.9, fifelse(gender == "male", 0.1, NA_real_))]
  }
  g <- unique(g[, .(first_l, p_male, p_female)], by = "first_l")
  g[!is.finite(p_male), p_male := NA_real_]
  g[!is.finite(p_female), p_female := NA_real_]
  g[, psum := p_male + p_female]
  g[psum > 0, `:=`(p_male = p_male / psum, p_female = p_female / psum)]
  g[, psum := NULL]

  setkey(g, first_l)
  setkey(dt, first_l)
  dt[g, `:=`(p_male = i.p_male, p_female = i.p_female)]
  dt[!is.na(first_l) & !is.na(p_male), gender_impute_status := "ok"]
  dt[!is.na(first_l) & is.na(p_male), gender_impute_status := "name_not_found"]
} else if (!is.na(lookup_path) && file.exists(lookup_path)) {
  g <- fread(lookup_path)
  setDT(g)
  if ("name" %in% names(g)) g[, first_l := to_lower_name(name)]
  if ("first_name" %in% names(g)) g[, first_l := to_lower_name(first_name)]
  if ("p_female" %in% names(g)) g[, p_female := as.numeric(p_female)]
  if ("p_male" %in% names(g)) g[, p_male := as.numeric(p_male)]
  if (!("p_male" %in% names(g)) && "p_female" %in% names(g)) g[, p_male := 1 - p_female]
  if (!("p_female" %in% names(g)) && "p_male" %in% names(g)) g[, p_female := 1 - p_male]
  assert_has_cols(g, c("first_l", "p_male", "p_female"), "gender lookup")
  g <- unique(g[, .(first_l, p_male, p_female)], by = "first_l")
  setkey(g, first_l)
  setkey(dt, first_l)
  dt[g, `:=`(p_male = i.p_male, p_female = i.p_female)]
  dt[!is.na(first_l) & !is.na(p_male), gender_impute_status := "ok"]
  dt[!is.na(first_l) & is.na(p_male), gender_impute_status := "name_not_found"]
} else {
  dt[!is.na(first_l), gender_impute_status := "gender_package_missing"]
}

dt[, gender_hat := NA_character_]
dt[gender_impute_status == "ok", gender_hat := fifelse(p_female >= p_male, "female", "male")]
dt[, gender_hat_p := fifelse(gender_impute_status == "ok", pmax(p_female, p_male), NA_real_)]
dt[, is_gender_confident := !is.na(gender_hat_p) & gender_hat_p >= confidence_threshold]
dt[, gender_impute_method := fifelse(gender_impute_status == "ok", paste0("first_name_", backend), NA_character_)]

status_tab <- dt[, .N, by = gender_impute_status][order(-N)]
hat_tab <- dt[gender_impute_status == "ok", .N, by = gender_hat][order(-N)]
cov_known_raw <- dt[!is.na(gender_raw_norm), .N]
cov_total <- nrow(dt)
agree_tab <- dt[gender_impute_status == "ok" & !is.na(gender_raw_norm),
                .(N = .N, agree = mean(gender_hat == gender_raw_norm, na.rm = TRUE)),
                by = .(gender_raw_norm, gender_hat)]
overall_agree <- dt[gender_impute_status == "ok" & !is.na(gender_raw_norm),
                    .(N = .N, agree = mean(gender_hat == gender_raw_norm, na.rm = TRUE))]

out_cols <- c(
  "familyid", "family_id", "locationid", "individual_id", "person_num", "head_of_household",
  "year", "PID", "bg_geoid", "geo_source",
  "first_name", "last_name", "middle_name", "gender",
  "in_eviction_data", "is_rental_pid",
  "p_male", "p_female", "gender_hat", "gender_hat_p", "is_gender_confident",
  "gender_impute_method", "gender_impute_status"
)
out_cols <- unique(c(intersect(out_cols, names(dt)),
                     # keep race columns when input is infousa_race_imputed_person
                     intersect(c("p_white", "p_black", "p_hispanic", "p_asian", "p_other",
                                 "race_hat", "race_hat_p", "race_entropy", "race_impute_method",
                                 "race_impute_status", "impute_status"), names(dt))))

fwrite(dt[, ..out_cols], out_person)

qa_lines <- c(
  "InfoUSA Gender Imputation QA",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  paste0("Input rows: ", n_input),
  paste0("Rows written: ", nrow(dt)),
  paste0("Backend: ", backend),
  paste0("Sample_n: ", ifelse(is.na(sample_n), "ALL", as.character(sample_n))),
  "",
  "=== Gender Impute Status Counts ===",
  capture.output(print(status_tab)),
  "",
  "=== Gender Hat Counts (ok only) ===",
  capture.output(print(hat_tab)),
  "",
  "=== Raw Gender Coverage and Concordance ===",
  paste0("Rows with raw known F/M: ", cov_known_raw, " / ", cov_total,
         " (", round(100 * cov_known_raw / pmax(1, cov_total), 2), "%)"),
  capture.output(print(overall_agree)),
  "",
  "=== Concordance by Raw/Imputed Gender ===",
  capture.output(print(agree_tab))
)
writeLines(qa_lines, out_qa)

logf("Wrote person output: ", out_person, log_file = log_file)
logf("Wrote QA output: ", out_qa, log_file = log_file)
logf("=== Finished impute-gender-infousa.R ===", log_file = log_file)
