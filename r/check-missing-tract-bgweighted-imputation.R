## ============================================================
## check-missing-tract-bgweighted-imputation.R
## ============================================================
## Purpose:
##   QA check for no-tract defendant rows:
##   1) Build a single synthetic prior vector from block-group priors,
##      weighted by case filings.
##   2) Re-impute the missing-tract rows using that synthetic prior.
##   3) Compare Black/White probabilities vs baseline imputed rows.
##
## Outputs (all to output/qa by default):
##   - <prefix>_prior_meta.csv
##   - <prefix>_synthetic_priors.csv
##   - <prefix>_impute_input.csv
##   - <prefix>_imputed_person.csv
##   - <prefix>_imputed_case.csv
##   - <prefix>_imputed_qa.txt
##   - <prefix>_name_prob.csv
##   - <prefix>_compare.csv
##   - <prefix>_delta.csv
##   - <prefix>_by_role.csv
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

normalize_bg_geoid <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- str_trim(x)
  x <- str_replace_all(x, "[^0-9]", "")
  x[nchar(x) == 0L] <- NA_character_
  short <- !is.na(x) & nchar(x) < 12L
  x[short] <- str_pad(x[short], width = 12L, side = "left", pad = "0")
  x[!is.na(x) & nchar(x) > 12L] <- NA_character_
  x
}

`%|||%` <- function(x, y) if (length(x) == 0L || all(is.na(x))) y else x

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

race_cfg <- cfg$race_priors %||% list()
priors_year <- suppressWarnings(as.integer(opts$prior_year %||% race_cfg$year %||% 2013L))
if (is.na(priors_year) || priors_year < 1900L || priors_year > 2100L) priors_year <- 2013L

person_path <- opts$person %||% p_out(cfg, "qa", "race_imputed_person_full_bgpref.csv")
bg_priors_path <- opts$bg_priors %||% p_proc(cfg, "xwalks", paste0("bg_renter_race_priors_", priors_year, ".csv"))
out_dir <- opts$output_dir %||% p_out(cfg, "qa")
prefix <- as.character(opts$prefix %||% "missing_tract_bgweighted")
status_target <- as.character(opts$status_target %||% "no_tract")
synthetic_tract <- as.character(opts$synthetic_tract %||% "42101000100")
impute_script <- as.character(opts$impute_script %||% "r/impute-race.R")

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
log_file <- p_out(cfg, "logs", "check-missing-tract-bgweighted-imputation.log")

out_prior_meta <- file.path(out_dir, paste0(prefix, "_prior_meta.csv"))
out_synth_priors <- file.path(out_dir, paste0(prefix, "_synthetic_priors.csv"))
out_impute_input <- file.path(out_dir, paste0(prefix, "_impute_input.csv"))
out_person <- file.path(out_dir, paste0(prefix, "_imputed_person.csv"))
out_case <- file.path(out_dir, paste0(prefix, "_imputed_case.csv"))
out_qa <- file.path(out_dir, paste0(prefix, "_imputed_qa.txt"))
out_name_prob <- file.path(out_dir, paste0(prefix, "_name_prob.csv"))
out_compare <- file.path(out_dir, paste0(prefix, "_compare.csv"))
out_delta <- file.path(out_dir, paste0(prefix, "_delta.csv"))
out_by_role <- file.path(out_dir, paste0(prefix, "_by_role.csv"))

logf("=== Starting check-missing-tract-bgweighted-imputation.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf("Person input: ", person_path, log_file = log_file)
logf("BG priors input: ", bg_priors_path, log_file = log_file)
logf("Status target: ", status_target, log_file = log_file)
logf("Synthetic tract geoid: ", synthetic_tract, log_file = log_file)

if (!file.exists(person_path)) stop("Missing person file: ", person_path)
if (!file.exists(bg_priors_path)) stop("Missing BG priors file: ", bg_priors_path)
if (!file.exists(impute_script)) stop("Missing impute script: ", impute_script)

person <- fread(person_path)
setDT(person)

required_person <- c("id", "is_analysis_defendant", "race_impute_status", "GEOID")
assert_has_cols(person, required_person, "person input")
person[, is_analysis_defendant := as.logical(is_analysis_defendant)]
person[, bg_geoid := normalize_bg_geoid(GEOID)]

## Case-level filing weights by BG: modal BG among analysis defendants.
case_bg <- person[
  is_analysis_defendant == TRUE & !is.na(bg_geoid),
  .N,
  by = .(id, bg_geoid)
][order(id, -N, bg_geoid), .SD[1L], by = id][, .(id, bg_geoid)]
assert_unique(case_bg, "id", "case_bg")
bg_case_counts <- case_bg[, .(n_cases = .N), by = bg_geoid]
assert_unique(bg_case_counts, "bg_geoid", "bg_case_counts")

pri_bg <- fread(bg_priors_path)
setDT(pri_bg)
assert_has_cols(pri_bg, c("bg_geoid", "p_white_prior", "p_black_prior", "p_hispanic_prior", "p_asian_prior", "p_other_prior"), "bg priors")
pri_bg[, bg_geoid := normalize_bg_geoid(bg_geoid)]
pri_bg <- pri_bg[!is.na(bg_geoid)]
pri_bg <- unique(pri_bg, by = "bg_geoid")
assert_unique(pri_bg, "bg_geoid", "bg priors unique")

j <- merge(
  bg_case_counts,
  pri_bg[, .(bg_geoid, p_white_prior, p_black_prior, p_hispanic_prior, p_asian_prior, p_other_prior)],
  by = "bg_geoid",
  all.x = TRUE
)

join_rate <- j[, mean(!is.na(p_black_prior), na.rm = TRUE)]
if (!is.finite(join_rate) || join_rate <= 0) stop("Unable to build weighted prior: BG join_rate is zero.")

synth <- j[!is.na(p_black_prior), .(
  p_white_prior = weighted.mean(p_white_prior, n_cases, na.rm = TRUE),
  p_black_prior = weighted.mean(p_black_prior, n_cases, na.rm = TRUE),
  p_hispanic_prior = weighted.mean(p_hispanic_prior, n_cases, na.rm = TRUE),
  p_asian_prior = weighted.mean(p_asian_prior, n_cases, na.rm = TRUE),
  p_other_prior = weighted.mean(p_other_prior, n_cases, na.rm = TRUE),
  total_renters = sum(n_cases, na.rm = TRUE)
)]
synth[, tract_geoid := synthetic_tract]
setcolorder(synth, c("tract_geoid", "p_white_prior", "p_black_prior", "p_hispanic_prior", "p_asian_prior", "p_other_prior", "total_renters"))
fwrite(synth, out_synth_priors)

prior_meta <- data.table(
  metric = c(
    "n_cases_with_modal_bg",
    "n_bgs_with_cases",
    "bg_prior_join_rate",
    "weighted_prior_black",
    "weighted_prior_white",
    "weighted_prior_hispanic",
    "weighted_prior_asian",
    "weighted_prior_other"
  ),
  value = c(
    nrow(case_bg),
    nrow(bg_case_counts),
    join_rate,
    synth$p_black_prior,
    synth$p_white_prior,
    synth$p_hispanic_prior,
    synth$p_asian_prior,
    synth$p_other_prior
  )
)
fwrite(prior_meta, out_prior_meta)

## Build clean impute input for target missing rows.
src <- person[is_analysis_defendant == TRUE & race_impute_status == status_target]
if (!nrow(src)) stop("No rows found for status_target='", status_target, "'.")

col_or_na_chr <- function(col, default = NA_character_) if (col %in% names(src)) as.character(src[[col]]) else rep(default, nrow(src))
col_or_na_int <- function(col, default = NA_integer_) suppressWarnings(as.integer(if (col %in% names(src)) src[[col]] else rep(default, nrow(src))))

impute_input <- data.table(
  id = as.character(src$id),
  role = col_or_na_chr("role", "Defendant #1"),
  case_year = col_or_na_int("case_year"),
  GEOID = NA_character_,
  tract_geoid = synthetic_tract,
  geoid_source = "synthetic_bgweighted_prior_check",
  name = col_or_na_chr("name"),
  name_clean = col_or_na_chr("name_clean"),
  first_name = toupper(str_squish(col_or_na_chr("first_name"))),
  middle_name = toupper(str_squish(col_or_na_chr("middle_name"))),
  last_name = toupper(str_squish(col_or_na_chr("last_name"))),
  name_token_n = col_or_na_int("name_token_n"),
  is_defendant = TRUE,
  is_analysis_defendant = TRUE,
  impute_status = "ok"
)

impute_input[first_name %in% c("", "NA"), first_name := NA_character_]
impute_input[middle_name %in% c("", "NA"), middle_name := NA_character_]
impute_input[last_name %in% c("", "NA"), last_name := NA_character_]
impute_input[name_clean %in% c("", "NA"), name_clean := NA_character_]
impute_input <- impute_input[!is.na(last_name)]

if (!nrow(impute_input)) stop("No target rows with usable surname after cleaning.")
fwrite(impute_input, out_impute_input)
logf("Prepared synthetic-impute input rows: ", nrow(impute_input), log_file = log_file)

## Re-use main imputation script for consistency.
cmd <- c(
  impute_script,
  paste0("--config=", cfg$meta$config_path),
  paste0("--input=", out_impute_input),
  paste0("--tract_priors=", out_synth_priors),
  "--sample_n=10000000",
  paste0("--output_person=", out_person),
  paste0("--output_case=", out_case),
  paste0("--output_qa=", out_qa),
  paste0("--output_name_prob_qa=", out_name_prob)
)
logf("Running synthetic-prior re-imputation via: Rscript ", paste(cmd, collapse = " "), log_file = log_file)
status <- system2("Rscript", args = cmd)
if (!identical(status, 0L)) stop("Synthetic-prior re-imputation failed with status ", status)

chk <- fread(out_person)
setDT(chk)
chk[, is_analysis_defendant := as.logical(is_analysis_defendant)]

baseline <- person[is_analysis_defendant == TRUE & race_impute_status == "ok"]
target_ok <- chk[is_analysis_defendant == TRUE & race_impute_status == "ok"]

if (!nrow(target_ok)) stop("No ok rows in synthetic re-imputation output.")

compare <- data.table(
  group = c("baseline_ok_analysis_defendants", paste0("former_", status_target, "_bgweighted_imputed")),
  n = c(nrow(baseline), nrow(target_ok)),
  mean_p_black = c(baseline[, mean(p_black, na.rm = TRUE)], target_ok[, mean(p_black, na.rm = TRUE)]),
  mean_p_white = c(baseline[, mean(p_white, na.rm = TRUE)], target_ok[, mean(p_white, na.rm = TRUE)]),
  share_hat_black = c(baseline[, mean(race_hat == "black", na.rm = TRUE)], target_ok[, mean(race_hat == "black", na.rm = TRUE)]),
  share_hat_white = c(baseline[, mean(race_hat == "white", na.rm = TRUE)], target_ok[, mean(race_hat == "white", na.rm = TRUE)])
)
fwrite(compare, out_compare)

delta <- data.table(
  metric = c(
    "weighted_bg_prior_black",
    "weighted_bg_prior_white",
    "target_mean_p_black_minus_baseline",
    "target_share_hat_black_minus_baseline"
  ),
  value = c(
    synth$p_black_prior,
    synth$p_white_prior,
    compare[group != "baseline_ok_analysis_defendants", mean_p_black] %|||% NA_real_ -
      compare[group == "baseline_ok_analysis_defendants", mean_p_black] %|||% NA_real_,
    compare[group != "baseline_ok_analysis_defendants", share_hat_black] %|||% NA_real_ -
      compare[group == "baseline_ok_analysis_defendants", share_hat_black] %|||% NA_real_
  )
)
fwrite(delta, out_delta)

by_role <- target_ok[, .(
  n = .N,
  mean_p_black = mean(p_black, na.rm = TRUE),
  mean_p_white = mean(p_white, na.rm = TRUE),
  share_hat_black = mean(race_hat == "black", na.rm = TRUE),
  share_hat_white = mean(race_hat == "white", na.rm = TRUE)
), by = role][order(-n)]
fwrite(by_role, out_by_role)

logf("Wrote prior meta: ", out_prior_meta, log_file = log_file)
logf("Wrote compare table: ", out_compare, log_file = log_file)
logf("Wrote delta table: ", out_delta, log_file = log_file)
logf("Wrote by-role table: ", out_by_role, log_file = log_file)
logf("=== Finished check-missing-tract-bgweighted-imputation.R ===", log_file = log_file)
