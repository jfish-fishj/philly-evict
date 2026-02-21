## ============================================================
## impute-race.R
## ============================================================
## Purpose: Impute race probabilities using wru::predict_race
## on preflight-parsed defendant names.
##
## Geography inference is config-driven and supports:
## census.geo in {"tract", "block_group", "block"}.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(wru)
})

source("r/config.R")
source("r/lib/race_imputation_utils.R")

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
  if (!is.null(out$tract_priors) && is.null(out$geo_priors) && is.null(out$priors)) {
    out$geo_priors <- out$tract_priors
    out$deprecated_geo_priors_alias <- "tract_priors"
  }
  out$tract_priors <- NULL
  out
}

default_name_prob_qa_path <- function(out_person_path) {
  b <- basename(out_person_path)
  if (grepl("^race_imputed_person_", b)) {
    b <- sub("^race_imputed_person_", "race_name_parse_prob_bgqa_", b)
  } else {
    b <- paste0("race_name_parse_prob_bgqa_", b)
  }
  file.path(dirname(out_person_path), b)
}

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

input_path <- opts$input %||% p_out(cfg, "qa", "race_imputation_preflight_person.csv")
race_cfg <- cfg$race_priors %||% list()
inference_geo <- canonicalize_census_geo(
  opts$census_geo %||% opts$inference_geo %||% race_cfg$output_geography %||% race_cfg$build_geography %||% "block_group"
)
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

out_person <- opts$output_person %||% p_out(cfg, "qa", "race_imputed_person_sample.csv")
out_case <- opts$output_case %||% p_out(cfg, "qa", "race_imputed_case_sample.csv")
out_qa <- opts$output_qa %||% p_out(cfg, "qa", "race_imputed_qa_sample.txt")
out_name_prob_qa <- opts$output_name_prob_qa %||% default_name_prob_qa_path(out_person)

sample_n <- NA #suppressWarnings(as.integer(opts$sample_n %||% 5000L))
seed <- suppressWarnings(as.integer(opts$seed %||% cfg$run$seed %||% 123L))
confidence_threshold <- suppressWarnings(as.numeric(opts$confidence_threshold %||% 0.7))
lodermeier_case_threshold <- suppressWarnings(as.numeric(opts$case_class_threshold %||% 0.8))
wru_year <- suppressWarnings(as.integer(opts$wru_year %||% 2010L))
retry <- suppressWarnings(as.integer(opts$retry %||% 3L))
census_key <- opts$census_key %||% Sys.getenv("CENSUS_API_KEY", unset = "")

if (is.na(wru_year) || !(wru_year %in% c(2000L, 2010L, 2020L))) {
  stop("wru_year must be one of 2000, 2010, or 2020.")
}
if (is.na(retry) || retry < 0L) retry <- 3L
if (!is.finite(lodermeier_case_threshold) || lodermeier_case_threshold <= 0 || lodermeier_case_threshold >= 1) {
  stop("case_class_threshold must be in (0, 1).")
}

log_file <- p_out(cfg, "logs", "impute-race.log")
logf("=== Starting impute-race.R (WRU) ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf("Input: ", input_path, log_file = log_file)
logf("Inference geography: ", inference_geo, log_file = log_file)
logf("Priors file (required): ", priors_path, log_file = log_file)
logf("Name probability files: first=", first_probs_path, ", last=", last_probs_path, ", middle=", middle_probs_path, log_file = log_file)
logf("Name-probability QA output: ", out_name_prob_qa, log_file = log_file)
logf("WRU year: ", wru_year, log_file = log_file)
logf(
  "BG low-N tract shrink config: enabled=", bg_shrink_enabled,
  ", min_n=", bg_shrink_min_n, ", k=", bg_shrink_k,
  log_file = log_file
)
if (!is.null(opts$deprecated_geo_priors_alias) && opts$deprecated_geo_priors_alias == "tract_priors") {
  logf("CLI alias --tract_priors is deprecated; prefer --geo_priors or --priors.", log_file = log_file)
}

if (is.null(priors_path) || is.na(priors_path) || !nzchar(priors_path) || !file.exists(priors_path)) {
  stop("Priors file not found: ", priors_path)
}
if (!file.exists(first_probs_path) || !file.exists(last_probs_path) || !file.exists(middle_probs_path)) {
  stop("One or more name probability files are missing.")
}

write_wru_name_cache(first_probs_path, last_probs_path, middle_probs_path, log_file = log_file)

# Load and sample input

dt <- fread(input_path)
setDT(dt)
required_cols <- c("id", "is_defendant", "impute_status", "first_name", "last_name")
assert_has_cols(dt, required_cols, "race preflight input")
if (!any(c("tract_geoid", "bg_geoid", "block_geoid", "GEOID") %in% names(dt))) {
  stop("Input must include at least one geography column: tract_geoid, bg_geoid, block_geoid, or GEOID.")
}

n_input <- nrow(dt)
if (!is.na(sample_n) && sample_n > 0L && sample_n < nrow(dt)) {
  set.seed(seed)
  dt <- dt[sample(.N, sample_n)]
  logf("Sample mode: kept ", nrow(dt), " rows from ", n_input, " with seed=", seed, log_file = log_file)
}

# Load priors, coerce to configured geography, and build wru census.data object
priors_raw <- fread(priors_path)
setDT(priors_raw)
geo_priors <- coerce_priors_to_geo(priors_raw, target_geo = inference_geo)
if (inference_geo == "block_group" && isTRUE(bg_shrink_enabled)) {
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
logf(
  "Built WRU census.data for states: ", paste(names(census_data), collapse = ", "),
  " (geo=", inference_geo, ", prior_source=", prior_source_geo, ")",
  log_file = log_file
)

# Prepare target rows

dt[, row_id := .I]
dt[, first_u := toupper(str_squish(as.character(first_name)))]
dt[, last_u := toupper(str_squish(as.character(last_name)))]
dt[first_u %in% c("", "NA"), first_u := NA_character_]
dt[last_u %in% c("", "NA"), last_u := NA_character_]
dt <- assign_target_geoid(dt, target_geo = inference_geo)
dt[, inference_geo := inference_geo]
logf(
  "Geography assignment source counts (", inference_geo, "):\n",
  paste(capture.output(print(dt[, .N, by = geo_source][order(-N)])), collapse = "\n"),
  log_file = log_file
)
if (!("is_analysis_defendant" %in% names(dt))) {
  dt[, is_analysis_defendant := is_defendant == TRUE & impute_status != "counterclaim_party"]
}
dt[, is_analysis_defendant := as.logical(is_analysis_defendant)]

# flag NA where missing last name
dt[is.na(last_u) & is_defendant == TRUE, impute_status := "missing_surname"]

targets <- dt[is_analysis_defendant == TRUE & impute_status == "ok"]
logf("Target rows (analysis-defendant & preflight ok): ", nrow(targets), log_file = log_file)



if (targets[is.na(geo_chr), .N] > 0L) {
  stop("Found defendant/impute_status=ok rows with missing ", inference_geo, " geoid.")
}
if (targets[is.na(last_u), .N] > 0L) {
  stop("Found defendant/impute_status=ok rows with missing surname.")
}

u <- unique(targets[, .(first_u, last_u, geo_chr)])
u[, `:=`(
  surname = last_u,
  first = fifelse(!is.na(first_u) & nzchar(first_u), first_u, NA_character_),
  state = state_abbr_from_fips(substr(geo_chr, 1L, 2L)),
  county_code = substr(geo_chr, 3L, 5L),
  geo_mid_code = substr(geo_chr, 6L, 11L)
)]
if (inference_geo == "block_group") u[, bg_code := substr(geo_chr, 12L, 12L)]
if (inference_geo == "block") u[, block_code := substr(geo_chr, 12L, 15L)]

if (u[is.na(state), .N] > 0L) {
  bad <- unique(u[is.na(state), substr(geo_chr, 1L, 2L)])
  stop("Could not map state code for some geographies. Example codes: ", paste(head(bad, 10L), collapse = ", "))
}

u_first <- u[!is.na(first) & nzchar(first)]
u_last <- u[is.na(first) | !nzchar(first)]

pred_first <- run_wru_predict(
  vf = build_wru_voter_file(u_first, census_geo = inference_geo, include_first = TRUE),
  names_to_use = "surname, first",
  census_data = census_data,
  census_geo = inference_geo,
  year = wru_year,
  census_key = census_key,
  retry = retry,
  log_file = log_file
)

pred_last <- run_wru_predict(
  vf = build_wru_voter_file(u_last, census_geo = inference_geo, include_first = FALSE),
  names_to_use = "surname",
  census_data = census_data,
  census_geo = inference_geo,
  year = wru_year,
  census_key = census_key,
  retry = retry,
  log_file = log_file
)

pred_u <- rbindlist(list(pred_first, pred_last), use.names = TRUE, fill = TRUE)
setDT(pred_u)

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
    paste0("wru_firstname+surname+", inference_geo),
    paste0("wru_surname+", inference_geo)
  )
)]

pred_u[, prior_level := paste0(inference_geo, "_wru")]
pred_u[, race_impute_status := fifelse(
  is.na(p_white) | is.na(p_black) | is.na(p_hispanic) | is.na(p_asian) | is.na(p_other),
  "name_lookup_failed",
  "ok"
)]

ok_pred <- pred_u[race_impute_status == "ok"]
ok_pred[, psum := rowSums(.SD), .SDcols = c("p_white", "p_black", "p_hispanic", "p_asian", "p_other")]
ok_pred[psum > 0, `:=`(
  p_white = p_white / psum,
  p_black = p_black / psum,
  p_hispanic = p_hispanic / psum,
  p_asian = p_asian / psum,
  p_other = p_other / psum
)]
ok_pred[, psum := NULL]
pred_u[ok_pred, `:=`(
  p_white = i.p_white,
  p_black = i.p_black,
  p_hispanic = i.p_hispanic,
  p_asian = i.p_asian,
  p_other = i.p_other
), on = .(first_u, last_u, geo_chr)]

assert_unique(pred_u, c("first_u", "last_u", "geo_chr"), "wru prediction rows")

# Merge predictions back
setkey(dt, first_u, last_u, geo_chr)
setkey(pred_u, first_u, last_u, geo_chr)

dt[pred_u, `:=`(
  race_impute_status = i.race_impute_status,
  race_impute_method = i.race_impute_method,
  prior_level = i.prior_level,
  p_white = i.p_white,
  p_black = i.p_black,
  p_hispanic = i.p_hispanic,
  p_asian = i.p_asian,
  p_other = i.p_other
)]

dt[is.na(race_impute_status) & is_defendant == TRUE & impute_status != "ok", race_impute_status := impute_status]
dt[is.na(race_impute_status) & is_defendant == TRUE & impute_status == "ok", race_impute_status := "name_lookup_failed"]
dt[is.na(race_impute_status) & is_defendant == FALSE, race_impute_status := "not_defendant"]


# Derived single-label outputs

dt[, race_hat := NA_character_]
dt[, race_hat_p := NA_real_]
dt[, race_entropy := NA_real_]
ok_rows <- dt[race_impute_status == "ok", which = TRUE]
if (length(ok_rows) > 0L) {
  mat <- as.matrix(dt[ok_rows, .(p_white, p_black, p_hispanic, p_asian, p_other)])
  idx <- max.col(mat, ties.method = "first")
  labs <- c("white", "black", "hispanic", "asian", "other")
  dt[ok_rows, race_hat := labs[idx]]
  dt[ok_rows, race_hat_p := mat[cbind(seq_len(nrow(mat)), idx)]]
  dt[ok_rows, race_entropy := entropy5(
    dt[ok_rows]$p_white, dt[ok_rows]$p_black, dt[ok_rows]$p_hispanic,
    dt[ok_rows]$p_asian, dt[ok_rows]$p_other
  )]
}

dt[, is_confident := !is.na(race_hat_p) & race_hat_p >= confidence_threshold]

# Case-level aggregates (defendant rows only)
# Counterclaim-linked defendants are excluded from analysis denominators.
def <- dt[is_analysis_defendant == TRUE]
case_base <- def[, .(
  n_defendants_total = .N,
  n_defendants_imputed = sum(race_impute_status == "ok", na.rm = TRUE),
  share_defendants_imputed = fifelse(.N > 0L, sum(race_impute_status == "ok", na.rm = TRUE) / .N, NA_real_)
), by = id]
case_base[, case_impute_status := fifelse(n_defendants_imputed > 0L, "ok", "no_imputed_defendants")]

case_ok <- def[race_impute_status == "ok", .(
  case_p_white = mean(p_white, na.rm = TRUE),
  case_p_black = mean(p_black, na.rm = TRUE),
  case_p_hispanic = mean(p_hispanic, na.rm = TRUE),
  case_p_asian = mean(p_asian, na.rm = TRUE),
  case_p_other = mean(p_other, na.rm = TRUE),
  case_p_white_sum = sum(p_white, na.rm = TRUE),
  case_p_black_sum = sum(p_black, na.rm = TRUE),
  case_p_hispanic_sum = sum(p_hispanic, na.rm = TRUE),
  case_p_asian_sum = sum(p_asian, na.rm = TRUE),
  case_p_other_sum = sum(p_other, na.rm = TRUE),
  case_any_white = 1 - prod(1 - p_white),
  case_any_black = 1 - prod(1 - p_black),
  case_any_hispanic = 1 - prod(1 - p_hispanic),
  case_any_asian = 1 - prod(1 - p_asian),
  case_any_other = 1 - prod(1 - p_other)
), by = id]

case_out <- merge(case_base, case_ok, by = "id", all.x = TRUE)
case_reason <- def[, .N, by = .(id, race_impute_status)][order(id, -N, race_impute_status)]
case_reason <- case_reason[, .SD[1], by = id][, .(id, case_reason_top = race_impute_status)]
case_out <- merge(case_out, case_reason, by = "id", all.x = TRUE)
case_out[case_impute_status == "ok", case_reason_top := NA_character_]

# Robustness classification mirroring Lodermeier (2025), Section 3.2:
# white if Pr(all tenants white) >= threshold;
# minority if Pr(any tenant minority) >= threshold.
case_probs <- def[race_impute_status == "ok", .(
  p_all_white_case = prod(p_white),
  n_modal_race_hat = uniqueN(race_hat),
  modal_race_hat = race_hat[1L]
), by = id]
case_out <- merge(case_out, case_probs, by = "id", all.x = TRUE)
case_out[, case_full_coverage := n_defendants_total > 0L & n_defendants_imputed == n_defendants_total]
case_out[case_full_coverage == FALSE, p_all_white_case := NA_real_]
case_out[, p_any_minority_case := fifelse(!is.na(p_all_white_case), 1 - p_all_white_case, NA_real_)]
case_out[, case_race_class_l80 := fifelse(
  case_full_coverage == FALSE, "unclassified_missing",
  fifelse(p_all_white_case >= lodermeier_case_threshold, "white",
          fifelse(p_any_minority_case >= lodermeier_case_threshold, "minority", "unclassified_uncertain"))
)]
case_out[, case_minority_detail_l80 := NA_character_]
case_out[case_race_class_l80 == "minority" & n_modal_race_hat == 1L & modal_race_hat %chin% c("black", "hispanic", "asian"),
         case_minority_detail_l80 := modal_race_hat]
case_out[case_race_class_l80 == "minority" & is.na(case_minority_detail_l80), case_minority_detail_l80 := "other_mixed"]
case_out[, case_race_group_l80 := fifelse(
  case_race_class_l80 == "white", "White",
  fifelse(case_race_class_l80 == "minority",
          fifelse(case_minority_detail_l80 == "black", "Black",
                  fifelse(case_minority_detail_l80 == "hispanic", "Hispanic",
                          fifelse(case_minority_detail_l80 == "asian", "Asian", "Other/Mixed"))),
          NA_character_)
)]
race_comp <- case_out[!is.na(case_race_group_l80), .N, by = case_race_group_l80][order(case_race_group_l80)]
race_comp[, share := N / sum(N)]
unclassified_share <- case_out[, mean(startsWith(case_race_class_l80, "unclassified"), na.rm = TRUE)]

status_tab <- dt[, .N, by = race_impute_status][order(-N)]
method_tab <- dt[, .N, by = race_impute_method][order(-N)]
prior_tab <- dt[race_impute_status == "ok", .N, by = prior_level][order(-N)]
case_tab <- case_out[, .N, by = case_impute_status][order(-N)]
ok_prob_sum <- dt[race_impute_status == "ok", rowSums(.SD), .SDcols = c("p_white", "p_black", "p_hispanic", "p_asian", "p_other")]

# QA extract: original + parsed names + final probabilities + configured priors.
pri_geo <- copy(geo_priors)[, .(
  geo_geoid,
  prior_p_white = as.numeric(p_white_prior),
  prior_p_black = as.numeric(p_black_prior),
  prior_p_hispanic = as.numeric(p_hispanic_prior),
  prior_p_asian = as.numeric(p_asian_prior),
  prior_p_other = as.numeric(p_other_prior),
  prior_total_renters = as.numeric(total_renters),
  prior_total_pop = as.numeric(total_pop)
)]
pri_geo <- unique(pri_geo, by = "geo_geoid")

qa_name_prob <- dt[, .(
  id,
  role,
  case_year,
  GEOID,
  tract_geoid,
  bg_geoid,
  block_geoid,
  geo_chr,
  geo_source,
  inference_geo,
  name_original = name,
  name_clean,
  first_name,
  middle_name,
  last_name,
  name_token_n,
  is_analysis_defendant,
  impute_status,
  race_impute_status,
  race_impute_method,
  p_white,
  p_black,
  p_hispanic,
  p_asian,
  p_other,
  race_hat,
  race_hat_p
)]

qa_name_prob[pri_geo, `:=`(
  prior_p_white = i.prior_p_white,
  prior_p_black = i.prior_p_black,
  prior_p_hispanic = i.prior_p_hispanic,
  prior_p_asian = i.prior_p_asian,
  prior_p_other = i.prior_p_other,
  prior_total_renters = i.prior_total_renters,
  prior_total_pop = i.prior_total_pop
), on = .(geo_chr = geo_geoid)]
qa_name_prob[, prior_joined := !is.na(prior_p_white) | !is.na(prior_p_black) |
               !is.na(prior_p_hispanic) | !is.na(prior_p_asian) | !is.na(prior_p_other)]

if (inference_geo == "block_group") {
  qa_name_prob[, `:=`(
    bg_p_white_prior = prior_p_white,
    bg_p_black_prior = prior_p_black,
    bg_p_hispanic_prior = prior_p_hispanic,
    bg_p_asian_prior = prior_p_asian,
    bg_p_other_prior = prior_p_other,
    bg_total_renters = prior_total_renters,
    bg_total_pop = prior_total_pop,
    bg_prior_joined = prior_joined
  )]
} else {
  qa_name_prob[, `:=`(
    bg_p_white_prior = NA_real_,
    bg_p_black_prior = NA_real_,
    bg_p_hispanic_prior = NA_real_,
    bg_p_asian_prior = NA_real_,
    bg_p_other_prior = NA_real_,
    bg_total_renters = NA_real_,
    bg_total_pop = NA_real_,
    bg_prior_joined = FALSE
  )]
}

fwrite(dt, out_person)
fwrite(case_out, out_case)
fwrite(qa_name_prob, out_name_prob_qa)

qa_lines <- c(
  "Race Imputation QA (WRU)",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  paste0("Input rows: ", n_input),
  paste0("Rows written: ", nrow(dt)),
  paste0("Sample_n: ", ifelse(is.na(sample_n), "ALL", as.character(sample_n))),
  paste0("Defendant rows excluded as counterclaim_party: ",
         dt[is_defendant == TRUE & is_analysis_defendant == FALSE, .N]),
  paste0("Lodermeier case-class threshold: ", lodermeier_case_threshold),
  paste0("WRU year: ", wru_year),
  paste0("Inference geography: ", inference_geo),
  paste0("Prior source geography (from priors file): ", prior_source_geo),
  paste0("Census source: priors file -> ", inference_geo, "-coerced wru census.data (", priors_path, ")"),
  "",
  "Status counts",
  capture.output(print(status_tab)),
  "",
  "Method counts",
  capture.output(print(method_tab)),
  "",
  "Prior level counts (ok rows)",
  capture.output(print(prior_tab)),
  "",
  "Case status counts",
  capture.output(print(case_tab)),
  "",
  "Lodermeier-style case race classification (white/minority @ threshold)",
  capture.output(print(case_out[, .N, by = case_race_class_l80][order(-N)])),
  capture.output(print(race_comp)),
  paste0("Unclassified case share (missing + uncertain): ", round(unclassified_share, 6)),
  "",
  paste0("Name-probability QA file: ", out_name_prob_qa),
  paste0("Name-probability QA rows: ", nrow(qa_name_prob)),
  paste0("Name-probability QA prior join rate (", inference_geo, "): ", round(mean(qa_name_prob$prior_joined, na.rm = TRUE), 6)),
  "",
  paste0("Prob sum |mean abs(1-sum p)| on ok rows: ",
         ifelse(length(ok_prob_sum), round(mean(abs(1 - ok_prob_sum), na.rm = TRUE), 8), NA_real_))
)
writeLines(qa_lines, con = out_qa)

logf("Wrote person output: ", out_person, log_file = log_file)
logf("Wrote case output: ", out_case, log_file = log_file)
logf("Wrote name-probability QA output: ", out_name_prob_qa, log_file = log_file)
logf("Wrote QA output: ", out_qa, log_file = log_file)
logf("=== Finished impute-race.R (WRU) ===", log_file = log_file)
