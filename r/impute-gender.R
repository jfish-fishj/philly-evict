## ============================================================
## impute-gender.R
## ============================================================
## Purpose: Impute gender probabilities from parsed first names
##          in preflight output.
##
## Primary backend: gender::gender(method = "ssa")
## Offline fallback: optional lookup CSV passed via --lookup
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
    parts <- strsplit(arg, "=", fixed = TRUE)[[1]]
    key <- gsub("-", "_", parts[1])
    val <- if (length(parts) >= 2L) paste(parts[-1], collapse = "=") else "TRUE"
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

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

input_path <- opts$input %||% p_out(cfg, "qa", "race_imputation_preflight_person.csv")
lookup_path <- opts$lookup %||% NA_character_
backend <- tolower(opts$backend %||% "gender_pkg")

out_person <- opts$output_person %||% p_out(cfg, "qa", "gender_imputed_person_sample.csv")
out_case <- opts$output_case %||% p_out(cfg, "qa", "gender_imputed_case_sample.csv")
out_qa <- opts$output_qa %||% p_out(cfg, "qa", "gender_imputed_qa_sample.txt")

sample_n <- suppressWarnings(as.integer(opts$sample_n %||% 5000L))
seed <- suppressWarnings(as.integer(opts$seed %||% cfg$run$seed %||% 123L))
confidence_threshold <- suppressWarnings(as.numeric(opts$confidence_threshold %||% 0.7))

log_file <- p_out(cfg, "logs", "impute-gender.log")
logf("=== Starting impute-gender.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf("Input: ", input_path, log_file = log_file)
logf("Backend: ", backend, ", lookup: ", lookup_path, log_file = log_file)

dt <- fread(input_path)
setDT(dt)
required_cols <- c("id", "is_defendant", "impute_status", "first_name")
assert_has_cols(dt, required_cols, "gender preflight input")
n_input <- nrow(dt)
if (!("is_analysis_defendant" %in% names(dt))) {
  dt[, is_analysis_defendant := is_defendant == TRUE & impute_status != "counterclaim_party"]
}
dt[, is_analysis_defendant := as.logical(is_analysis_defendant)]

if (!is.na(sample_n) && sample_n > 0L && sample_n < nrow(dt)) {
  set.seed(seed)
  dt <- dt[sample(.N, sample_n)]
  logf("Sample mode: kept ", nrow(dt), " rows from ", n_input, " with seed=", seed, log_file = log_file)
}

dt[, first_l := to_lower_name(first_name)]
dt[, gender_impute_status := NA_character_]
dt[, `:=`(p_male = NA_real_, p_female = NA_real_)]

targets <- dt[is_analysis_defendant == TRUE & impute_status == "ok" & !is.na(first_l)]
u <- unique(targets[, .(first_l)])
logf("Unique first names to score: ", nrow(u), log_file = log_file)

if (backend == "gender_pkg" && requireNamespace("gender", quietly = TRUE)) {
  g <- gender::gender(
    names = u$first_l,
    years = c(1932, 2012),
    method = "ssa"
  )
  setDT(g)
  # Normalize varying column names across versions/methods.
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
  g[, s := p_male + p_female]
  g[s > 0, `:=`(p_male = p_male / s, p_female = p_female / s)]
  g[, s := NULL]
  setkey(g, first_l)
  setkey(dt, first_l)
  dt[g, `:=`(p_male = i.p_male, p_female = i.p_female)]
  dt[is_analysis_defendant == TRUE & impute_status == "ok" & !is.na(first_l) & !is.na(p_male), gender_impute_status := "ok"]
  dt[is_analysis_defendant == TRUE & impute_status == "ok" & !is.na(first_l) & is.na(p_male), gender_impute_status := "name_not_found"]
} else if (!is.na(lookup_path) && file.exists(lookup_path)) {
  g <- fread(lookup_path)
  setDT(g)
  # Expected minimal columns: name + p_female (or p_male).
  nm <- names(g)
  if ("name" %in% nm) g[, first_l := to_lower_name(name)]
  if ("first_name" %in% nm) g[, first_l := to_lower_name(first_name)]
  if ("p_female" %in% nm) g[, p_female := as.numeric(p_female)]
  if ("p_male" %in% nm) g[, p_male := as.numeric(p_male)]
  if (!("p_male" %in% names(g)) && "p_female" %in% names(g)) g[, p_male := 1 - p_female]
  if (!("p_female" %in% names(g)) && "p_male" %in% names(g)) g[, p_female := 1 - p_male]
  need <- c("first_l", "p_male", "p_female")
  assert_has_cols(g, need, "gender lookup")
  g <- unique(g[, ..need], by = "first_l")
  setkey(g, first_l)
  setkey(dt, first_l)
  dt[g, `:=`(p_male = i.p_male, p_female = i.p_female)]
  dt[is_analysis_defendant == TRUE & impute_status == "ok" & !is.na(first_l) & !is.na(p_male), gender_impute_status := "ok"]
  dt[is_analysis_defendant == TRUE & impute_status == "ok" & !is.na(first_l) & is.na(p_male), gender_impute_status := "name_not_found"]
} else {
  dt[is_analysis_defendant == TRUE & impute_status == "ok" & !is.na(first_l), gender_impute_status := "gender_package_missing"]
}

dt[is.na(gender_impute_status) & is_defendant == TRUE & impute_status != "ok", gender_impute_status := impute_status]
dt[is.na(gender_impute_status) & is_defendant == FALSE, gender_impute_status := "not_defendant"]
dt[is.na(gender_impute_status), gender_impute_status := "missing_first_name"]

dt[, gender_hat := NA_character_]
dt[gender_impute_status == "ok", gender_hat := fifelse(p_female >= p_male, "female", "male")]
dt[, gender_hat_p := fifelse(gender_impute_status == "ok", pmax(p_female, p_male), NA_real_)]
dt[, is_gender_confident := !is.na(gender_hat_p) & gender_hat_p >= confidence_threshold]

def <- dt[is_analysis_defendant == TRUE]
case_base <- def[, .(
  n_defendants_total = .N,
  n_defendants_gender_imputed = sum(gender_impute_status == "ok", na.rm = TRUE),
  share_defendants_gender_imputed = fifelse(.N > 0L, sum(gender_impute_status == "ok", na.rm = TRUE) / .N, NA_real_)
), by = id]
case_base[, case_gender_impute_status := fifelse(n_defendants_gender_imputed > 0L, "ok", "no_imputed_defendants")]

case_ok <- def[gender_impute_status == "ok", .(
  case_p_female = mean(p_female, na.rm = TRUE),
  case_p_male = mean(p_male, na.rm = TRUE),
  case_p_female_sum = sum(p_female, na.rm = TRUE),
  case_p_male_sum = sum(p_male, na.rm = TRUE),
  case_any_female = 1 - prod(1 - p_female),
  case_any_male = 1 - prod(1 - p_male)
), by = id]

case_out <- merge(case_base, case_ok, by = "id", all.x = TRUE)
case_reason <- def[, .N, by = .(id, gender_impute_status)][order(id, -N, gender_impute_status)]
case_reason <- case_reason[, .SD[1], by = id][, .(id, case_reason_top = gender_impute_status)]
case_out <- merge(case_out, case_reason, by = "id", all.x = TRUE)
case_out[case_gender_impute_status == "ok", case_reason_top := NA_character_]

status_tab <- dt[, .N, by = gender_impute_status][order(-N)]
case_tab <- case_out[, .N, by = case_gender_impute_status][order(-N)]

fwrite(dt, out_person)
fwrite(case_out, out_case)

qa_lines <- c(
  "Gender Imputation QA",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  paste0("Input rows: ", n_input),
  paste0("Rows written: ", nrow(dt)),
  paste0("Defendant rows excluded as counterclaim_party: ",
         dt[is_defendant == TRUE & is_analysis_defendant == FALSE, .N]),
  paste0("Backend: ", backend),
  paste0("Sample_n: ", ifelse(is.na(sample_n), "ALL", as.character(sample_n))),
  "",
  "Status counts",
  capture.output(print(status_tab)),
  "",
  "Case status counts",
  capture.output(print(case_tab))
)
writeLines(qa_lines, con = out_qa)

logf("Wrote person output: ", out_person, log_file = log_file)
logf("Wrote case output: ", out_case, log_file = log_file)
logf("Wrote QA output: ", out_qa, log_file = log_file)
logf("=== Finished impute-gender.R ===", log_file = log_file)
