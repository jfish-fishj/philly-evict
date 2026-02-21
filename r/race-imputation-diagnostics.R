## ============================================================
## race-imputation-diagnostics.R
## ============================================================
## Purpose: QA diagnostics for race imputation outputs:
##   - Benchmarks (tract-renter composition, weighting schemes)
##   - Tract disproportion diagnostics
##   - Missingness / selection diagnostics and bounds
##
## Inputs default to output from r/impute-race.R.
## Outputs:
##   - output/qa/race_benchmark_report.txt
##   - output/qa/race_tract_disproportion_report.txt
##   - output/qa/race_missingness_report.txt
##   - output/qa/race_tract_deciles.csv
##   - output/qa/race_tract_counts.csv
##   - output/qa/race_missingness_by_year.csv
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

normalize_tract_geoid <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- str_trim(x)
  x <- str_replace_all(x, "[^0-9]", "")
  x[nchar(x) == 0L] <- NA_character_
  too_long <- !is.na(x) & nchar(x) > 11L
  x[too_long] <- NA_character_
  short <- !is.na(x) & nchar(x) < 11L
  x[short] <- str_pad(x[short], width = 11L, side = "left", pad = "0")
  x
}

normalize_bg_geoid <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- str_trim(x)
  x <- str_replace_all(x, "[^0-9]", "")
  x[nchar(x) == 0L] <- NA_character_
  too_long <- !is.na(x) & nchar(x) > 12L
  x[too_long] <- NA_character_
  short <- !is.na(x) & nchar(x) < 12L
  x[short] <- str_pad(x[short], width = 12L, side = "left", pad = "0")
  x
}

pick_scale <- function(tp) {
  has_renters <- "total_renters" %in% names(tp) && any(is.finite(as.numeric(tp$total_renters)) & as.numeric(tp$total_renters) > 0, na.rm = TRUE)
  has_pop <- "total_pop" %in% names(tp) && any(is.finite(as.numeric(tp$total_pop)) & as.numeric(tp$total_pop) > 0, na.rm = TRUE)

  tp[, total_renters := if ("total_renters" %in% names(tp)) as.numeric(total_renters) else NA_real_]
  tp[, total_pop := if ("total_pop" %in% names(tp)) as.numeric(total_pop) else NA_real_]

  if (has_renters) {
    tp[, scale_w := fifelse(is.finite(total_renters) & total_renters > 0, total_renters,
                            fifelse(has_pop & is.finite(total_pop) & total_pop > 0, total_pop, 1))]
    note <- if (has_pop) "primary=total_renters; fallback=total_pop then constant 1" else "primary=total_renters; fallback=constant 1"
  } else if (has_pop) {
    tp[, scale_w := fifelse(is.finite(total_pop) & total_pop > 0, total_pop, 1)]
    note <- "primary=total_pop; fallback=constant 1"
  } else {
    tp[, scale_w := 1]
    note <- "primary=constant 1 (no total_renters/total_pop found)"
  }
  list(dt = tp, note = note)
}

weighted_black <- function(dt, w_col = "scale_w", p_col = "p_black_prior") {
  num <- dt[!is.na(get(p_col)) & !is.na(get(w_col)), sum(get(w_col) * get(p_col), na.rm = TRUE)]
  den <- dt[!is.na(get(w_col)), sum(get(w_col), na.rm = TRUE)]
  if (!is.finite(den) || den <= 0) return(NA_real_)
  num / den
}

safe_quantiles <- function(x, probs = c(0, 0.1, 0.5, 0.9, 1)) {
  x <- x[is.finite(x)]
  if (!length(x)) return(rep(NA_real_, length(probs)))
  as.numeric(quantile(x, probs = probs, na.rm = TRUE, names = FALSE, type = 7))
}

mode_first <- function(x) {
  x <- x[!is.na(x)]
  if (!length(x)) return(NA_character_)
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

coerce_priors_to_tract <- function(priors_raw) {
  p <- copy(priors_raw)
  req_prob <- c("p_white_prior", "p_black_prior", "p_hispanic_prior", "p_asian_prior", "p_other_prior")
  assert_has_cols(p, req_prob, "priors input")

  has_tract <- "tract_geoid" %in% names(p)
  has_bg <- "bg_geoid" %in% names(p)
  if (!has_tract && !has_bg) {
    stop("Priors file must include either tract_geoid or bg_geoid.")
  }

  if (!("total_renters" %in% names(p))) p[, total_renters := NA_real_]
  if (!("total_pop" %in% names(p))) p[, total_pop := NA_real_]
  p[, total_renters := as.numeric(total_renters)]
  p[, total_pop := as.numeric(total_pop)]

  if (has_bg) {
    p[, bg_geoid := normalize_bg_geoid(bg_geoid)]
    p <- p[!is.na(bg_geoid)]
    p[, tract_geoid := substr(bg_geoid, 1L, 11L)]
  } else {
    p[, tract_geoid := normalize_tract_geoid(tract_geoid)]
    p <- p[!is.na(tract_geoid)]
  }

  p[, w := fifelse(
    is.finite(total_renters) & total_renters > 0, total_renters,
    fifelse(is.finite(total_pop) & total_pop > 0, total_pop, 1)
  )]

  out <- p[, .(
    w_sum = sum(w, na.rm = TRUE),
    white_num = sum(p_white_prior * w, na.rm = TRUE),
    black_num = sum(p_black_prior * w, na.rm = TRUE),
    hisp_num = sum(p_hispanic_prior * w, na.rm = TRUE),
    asian_num = sum(p_asian_prior * w, na.rm = TRUE),
    other_num = sum(p_other_prior * w, na.rm = TRUE),
    total_renters = sum(fifelse(is.finite(total_renters) & total_renters > 0, total_renters, 0), na.rm = TRUE),
    total_pop = sum(fifelse(is.finite(total_pop) & total_pop > 0, total_pop, 0), na.rm = TRUE)
  ), by = tract_geoid]

  out[, `:=`(
    p_white_prior = fifelse(w_sum > 0, white_num / w_sum, NA_real_),
    p_black_prior = fifelse(w_sum > 0, black_num / w_sum, NA_real_),
    p_hispanic_prior = fifelse(w_sum > 0, hisp_num / w_sum, NA_real_),
    p_asian_prior = fifelse(w_sum > 0, asian_num / w_sum, NA_real_),
    p_other_prior = fifelse(w_sum > 0, other_num / w_sum, NA_real_)
  )]
  out[, c("w_sum", "white_num", "black_num", "hisp_num", "asian_num", "other_num") := NULL]
  assert_unique(out, "tract_geoid", "tract priors (coerced)")
  out
}

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

person_path <- opts$person %||% p_out(cfg, "qa", "race_imputed_person_sample.csv")
case_path <- opts$case %||% p_out(cfg, "qa", "race_imputed_case_sample.csv")
race_cfg <- cfg$race_priors %||% list()
priors_year <- suppressWarnings(as.integer(opts$prior_year %||% race_cfg$year %||% 2013L))
if (is.na(priors_year) || priors_year < 1900L || priors_year > 2100L) priors_year <- 2013L
default_bg_renter_priors <- p_proc(cfg, "xwalks", paste0("bg_renter_race_priors_", priors_year, ".csv"))
default_tract_renter_priors <- p_proc(cfg, "xwalks", paste0("tract_renter_race_priors_", priors_year, ".csv"))
default_bg_total_priors <- p_proc(cfg, "xwalks", paste0("bg_race_priors_", priors_year, ".csv"))
prefer_bg <- tolower(as.character(race_cfg$output_geography %||% "block_group")) == "block_group"
candidate_priors <- if (prefer_bg) {
  c(default_bg_renter_priors, default_tract_renter_priors)
} else {
  c(default_tract_renter_priors, default_bg_renter_priors)
}
default_priors_path <- candidate_priors[file.exists(candidate_priors)][1]
priors_path <- opts$priors %||% default_priors_path
totalpop_priors_path <- opts$priors_totalpop %||% default_bg_total_priors

out_dir <- opts$output_dir %||% p_out(cfg, "qa")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

benchmark_report <- file.path(out_dir, "race_benchmark_report.txt")
disprop_report <- file.path(out_dir, "race_tract_disproportion_report.txt")
missing_report <- file.path(out_dir, "race_missingness_report.txt")
decile_csv <- file.path(out_dir, "race_tract_deciles.csv")
tract_counts_csv <- file.path(out_dir, "race_tract_counts.csv")
miss_year_csv <- file.path(out_dir, "race_missingness_by_year.csv")

log_file <- p_out(cfg, "logs", "race-imputation-diagnostics.log")
logf("=== Starting race-imputation-diagnostics.R ===", log_file = log_file)
logf("Person input: ", person_path, log_file = log_file)
logf("Case input: ", case_path, log_file = log_file)
logf("Priors input: ", priors_path, log_file = log_file)
logf("Total-pop priors input: ", totalpop_priors_path, log_file = log_file)

if (!file.exists(person_path)) stop("Missing person file: ", person_path)
person <- fread(person_path)
setDT(person)

if (file.exists(case_path)) {
  case_dt <- fread(case_path)
  setDT(case_dt)
} else {
  case_dt <- NULL
}

if (is.null(priors_path) || is.na(priors_path) || !nzchar(priors_path) || !file.exists(priors_path)) {
  stop("Missing priors file: ", priors_path)
}
priors_raw <- fread(priors_path)
setDT(priors_raw)
priors <- coerce_priors_to_tract(priors_raw)

scale_info <- pick_scale(priors)
priors <- scale_info$dt
scale_note <- scale_info$note

run_cmd <- paste(c("Rscript r/race-imputation-diagnostics.R", commandArgs(trailingOnly = TRUE)), collapse = " ")
ts <- format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")

person[, tract_norm := NA_character_]
if ("GEOID" %in% names(person)) {
  person[, GEOID := normalize_bg_geoid(GEOID)]
  person[!is.na(GEOID), tract_norm := substr(GEOID, 1L, 11L)]
}
if ("tract_chr" %in% names(person)) {
  person[is.na(tract_norm), tract_norm := normalize_tract_geoid(tract_chr)]
} else if ("tract_geoid" %in% names(person)) {
  person[is.na(tract_norm), tract_norm := normalize_tract_geoid(tract_geoid)]
}
person[, case_year := suppressWarnings(as.integer(case_year))]
if (!("is_analysis_defendant" %in% names(person))) {
  if ("impute_status" %in% names(person)) {
    person[, is_analysis_defendant := is_defendant == TRUE & impute_status != "counterclaim_party"]
  } else if ("race_impute_status" %in% names(person)) {
    person[, is_analysis_defendant := is_defendant == TRUE & race_impute_status != "counterclaim_party"]
  } else {
    person[, is_analysis_defendant := is_defendant == TRUE]
  }
}
person[, is_analysis_defendant := as.logical(is_analysis_defendant)]
def <- person[is_analysis_defendant == TRUE]
def_ok <- def[race_impute_status == "ok"]
n_counterclaim_excluded <- person[is_defendant == TRUE & is_analysis_defendant == FALSE, .N]

# tract counts (defendant and case)
def_counts <- def[!is.na(tract_norm), .(n_defendants_t = .N), by = .(tract_geoid = tract_norm)]
assert_unique(def_counts, "tract_geoid", "def_counts")

case_tract <- def[!is.na(tract_norm), .N, by = .(id, tract_norm)][order(id, -N, tract_norm), .SD[1L], by = id][, .(id, tract_geoid = tract_norm)]
case_counts <- case_tract[, .(n_cases_t = .N), by = tract_geoid]
assert_unique(case_counts, "tract_geoid", "case_counts")

tract_stats <- merge(priors, def_counts, by = "tract_geoid", all.x = TRUE)
tract_stats <- merge(tract_stats, case_counts, by = "tract_geoid", all.x = TRUE)
tract_stats[is.na(n_defendants_t), n_defendants_t := 0L]
tract_stats[is.na(n_cases_t), n_cases_t := 0L]

fwrite(tract_stats, tract_counts_csv)

## -------------------- Part A: Benchmarks --------------------
bench_all <- weighted_black(priors, w_col = "scale_w", p_col = "p_black_prior")
sample_tracts <- unique(def[!is.na(tract_norm), tract_norm])
bench_sample_tracts <- weighted_black(priors[tract_geoid %chin% sample_tracts], w_col = "scale_w", p_col = "p_black_prior")
bench_deftract <- weighted_black(merge(priors, def_counts, by = "tract_geoid", all.x = FALSE)[, .(tract_geoid, p_black_prior, scale_w = n_defendants_t)], w_col = "scale_w", p_col = "p_black_prior")
bench_casetract <- weighted_black(merge(priors, case_counts, by = "tract_geoid", all.x = FALSE)[, .(tract_geoid, p_black_prior, scale_w = n_cases_t)], w_col = "scale_w", p_col = "p_black_prior")

# Make race_hat robust in case upstream schema changes.
if (!("race_hat" %in% names(def_ok))) {
  pmat <- as.matrix(def_ok[, .(p_white, p_black, p_hispanic, p_asian, p_other)])
  def_ok[, race_hat := c("white", "black", "hispanic", "asian", "other")[max.col(pmat, ties.method = "first")]]
}

ok_mean_p_black <- def_ok[, mean(p_black, na.rm = TRUE)]
ok_share_hat_black <- def_ok[, mean(race_hat == "black", na.rm = TRUE)]
ok_share_hat_white <- def_ok[, mean(race_hat == "white", na.rm = TRUE)]
ok_share_hat_black_over_bw <- def_ok[
  ,
  {
    b <- mean(race_hat == "black", na.rm = TRUE)
    w <- mean(race_hat == "white", na.rm = TRUE)
    ifelse((b + w) > 0, b / (b + w), NA_real_)
  }
]

role_decomp <- function(role_name) {
  x <- def_ok[role == role_name]
  if (!nrow(x)) return(NULL)
  b <- x[, mean(race_hat == "black", na.rm = TRUE)]
  w <- x[, mean(race_hat == "white", na.rm = TRUE)]
  data.table(
    role = role_name,
    n = nrow(x),
    mean_p_black = x[, mean(p_black, na.rm = TRUE)],
    share_hat_black = b,
    share_hat_white = w,
    share_hat_black_over_bw = ifelse((b + w) > 0, b / (b + w), NA_real_)
  )
}

decomp_dt <- rbindlist(
  list(role_decomp("Defendant #1"), role_decomp("Defendant #2")),
  use.names = TRUE,
  fill = TRUE
)

period_dt <- data.table()
if ("case_year" %in% names(def_ok)) {
  tmp <- copy(def_ok[role == "Defendant #1"])
  tmp[, case_year := suppressWarnings(as.integer(case_year))]
  tmp[case_year >= 2005 & case_year <= 2012, period := "2005-2012"]
  tmp[case_year >= 2013 & case_year <= 2019, period := "2013-2019"]
  tmp[case_year >= 2020 & case_year <= 2024, period := "2020-2024"]
  tmp <- tmp[!is.na(period)]
  if (nrow(tmp) > 0L) {
    period_dt <- tmp[, .(
      n = .N,
      mean_p_black = mean(p_black, na.rm = TRUE),
      share_hat_black = mean(race_hat == "black", na.rm = TRUE),
      share_hat_white = mean(race_hat == "white", na.rm = TRUE)
    ), by = period][order(period)]
    period_dt[, share_hat_black_over_bw := fifelse(
      (share_hat_black + share_hat_white) > 0,
      share_hat_black / (share_hat_black + share_hat_white),
      NA_real_
    )]
  }
}

# A3 compare renter vs totalpop priors
a3_lines <- c("A3. renter-vs-totalpop comparison", "SKIPPED: missing total-pop priors file")
if (!is.null(totalpop_priors_path) && !is.na(totalpop_priors_path) && nzchar(totalpop_priors_path) && file.exists(totalpop_priors_path)) {
  tp <- fread(totalpop_priors_path)
  setDT(tp)
  tp <- coerce_priors_to_tract(tp)
  if ("p_black_prior" %in% names(tp)) {
    tp <- unique(tp[!is.na(tract_geoid), .(tract_geoid, p_black_prior_total = as.numeric(p_black_prior))], by = "tract_geoid")
    rp <- unique(priors[, .(tract_geoid, p_black_prior_renter = as.numeric(p_black_prior))], by = "tract_geoid")
    cmp <- merge(rp, tp, by = "tract_geoid", all = FALSE)
    cmp[, delta_p_black := p_black_prior_renter - p_black_prior_total]

    def_tracts <- unique(def[!is.na(tract_norm), tract_norm])
    ok_tracts <- unique(def_ok[!is.na(tract_norm), tract_norm])
    q_all <- safe_quantiles(cmp$delta_p_black)
    q_def <- safe_quantiles(cmp[tract_geoid %chin% def_tracts, delta_p_black])
    q_ok <- safe_quantiles(cmp[tract_geoid %chin% ok_tracts, delta_p_black])
    a3_lines <- c(
      "A3. renter-vs-totalpop comparison",
      paste0("Rows compared: ", nrow(cmp)),
      paste0("All tracts delta mean: ", round(mean(cmp$delta_p_black, na.rm = TRUE), 6), "; median: ", round(q_all[3], 6),
             "; p10/p90: ", round(q_all[2], 6), "/", round(q_all[4], 6)),
      paste0("Defendant-used tracts delta mean: ", round(mean(cmp[tract_geoid %chin% def_tracts, delta_p_black], na.rm = TRUE), 6),
             "; median: ", round(q_def[3], 6), "; p10/p90: ", round(q_def[2], 6), "/", round(q_def[4], 6)),
      paste0("OK-used tracts delta mean: ", round(mean(cmp[tract_geoid %chin% ok_tracts, delta_p_black], na.rm = TRUE), 6),
             "; median: ", round(q_ok[3], 6), "; p10/p90: ", round(q_ok[2], 6), "/", round(q_ok[4], 6))
    )
  } else {
    a3_lines <- c("A3. renter-vs-totalpop comparison", paste0("SKIPPED: total-pop priors missing required columns in ", totalpop_priors_path))
  }
}

bench_lines <- c(
  "Race Benchmark Report",
  paste0("Timestamp: ", ts),
  paste0("Command: ", run_cmd),
  paste0("Person file: ", person_path),
  paste0("Case file: ", ifelse(is.null(case_dt), "MISSING", case_path)),
  paste0("Priors file used: ", priors_path),
  paste0("Scaling variable rule: ", scale_note),
  paste0("Rows in person sample: ", nrow(person)),
  paste0("Defendant rows (analysis): ", nrow(def)),
  paste0("Defendant rows excluded as counterclaim_party: ", n_counterclaim_excluded),
  paste0("OK defendant rows: ", nrow(def_ok)),
  "",
  "A2. Benchmarks",
  paste0("bench_black_renter_share_all: ", round(bench_all, 6)),
  paste0("bench_black_renter_share_sample_tracts: ", round(bench_sample_tracts, 6)),
  paste0("bench_black_renter_share_deftract (defendant-tract-weighted): ", round(bench_deftract, 6)),
  paste0("bench_black_renter_share_casetract (case-tract-weighted): ", round(bench_casetract, 6)),
  "",
  "Observed imputation outputs (defendant ok rows)",
  paste0("mean_p_black_ok: ", round(ok_mean_p_black, 6)),
  paste0("share_hat_black_ok: ", round(ok_share_hat_black, 6)),
  paste0("share_hat_white_ok: ", round(ok_share_hat_white, 6)),
  paste0("share_hat_black_over_bw_ok: ", round(ok_share_hat_black_over_bw, 6)),
  paste0("delta_mean_p_black_vs_all_tract_benchmark: ", round(ok_mean_p_black - bench_all, 6)),
  paste0("delta_mean_p_black_vs_deftract_benchmark: ", round(ok_mean_p_black - bench_deftract, 6)),
  "",
  "Role decomposition (defendant ok rows)",
  capture.output(print(decomp_dt)),
  "",
  "Defendant #1 by period (defendant ok rows)",
  if (nrow(period_dt) > 0L) capture.output(print(period_dt)) else "SKIPPED: missing/invalid case_year in person file",
  "",
  a3_lines
)
writeLines(bench_lines, con = benchmark_report)

## -------------------- Part B: Disproportion --------------------
ts_b <- copy(tract_stats)
ts_b <- ts_b[is.finite(p_black_prior)]
if (nrow(ts_b) > 0L) {
  ts_b[, rank_pct := frank(p_black_prior, ties.method = "average", na.last = "keep") / .N]
  ts_b[, p_black_decile := pmax(1L, pmin(10L, as.integer(ceiling(rank_pct * 10))))]
} else {
  ts_b[, p_black_decile := integer()]
}

total_cases <- ts_b[, sum(n_cases_t, na.rm = TRUE)]
total_defs <- ts_b[, sum(n_defendants_t, na.rm = TRUE)]
total_scale <- ts_b[, sum(scale_w, na.rm = TRUE)]

deciles <- ts_b[, .(
  n_tracts = .N,
  n_cases = sum(n_cases_t, na.rm = TRUE),
  share_cases = fifelse(total_cases > 0, sum(n_cases_t, na.rm = TRUE) / total_cases, NA_real_),
  n_defendants = sum(n_defendants_t, na.rm = TRUE),
  share_defendants = fifelse(total_defs > 0, sum(n_defendants_t, na.rm = TRUE) / total_defs, NA_real_),
  renter_weight_share = fifelse(total_scale > 0, sum(scale_w, na.rm = TRUE) / total_scale, NA_real_),
  avg_p_black_prior = mean(p_black_prior, na.rm = TRUE)
), by = p_black_decile][order(p_black_decile)]
fwrite(deciles, decile_csv)

top_share <- function(dt, top_frac = 0.1) {
  if (!nrow(dt)) return(list(case_share = NA_real_, def_share = NA_real_, renter_share = NA_real_))
  k <- max(1L, ceiling(nrow(dt) * top_frac))
  top <- dt[order(-p_black_prior)][1:k]
  list(
    case_share = ifelse(total_cases > 0, sum(top$n_cases_t, na.rm = TRUE) / total_cases, NA_real_),
    def_share = ifelse(total_defs > 0, sum(top$n_defendants_t, na.rm = TRUE) / total_defs, NA_real_),
    renter_share = ifelse(total_scale > 0, sum(top$scale_w, na.rm = TRUE) / total_scale, NA_real_)
  )
}

t10 <- top_share(ts_b, 0.1)
t20 <- top_share(ts_b, 0.2)
case_weighted_black_prior <- ifelse(total_cases > 0, ts_b[, sum(n_cases_t * p_black_prior, na.rm = TRUE) / total_cases], NA_real_)

sanity <- if (is.finite(case_weighted_black_prior) && is.finite(bench_all)) {
  diff_pp <- case_weighted_black_prior - bench_all
  if (diff_pp > 0.02) {
    paste0("YES: case-weighted tract Black prior is higher than renter-weighted all-tract benchmark by ",
           round(100 * diff_pp, 2), " pp.")
  } else if (diff_pp < -0.02) {
    paste0("NO: case-weighted tract Black prior is lower than renter-weighted all-tract benchmark by ",
           round(100 * abs(diff_pp), 2), " pp.")
  } else {
    paste0("MIXED: case-weighted tract Black prior is close to renter-weighted all-tract benchmark (diff ",
           round(100 * diff_pp, 2), " pp).")
  }
} else {
  "Insufficient data to evaluate."
}

disprop_lines <- c(
  "Race Tract Disproportion Report",
  paste0("Timestamp: ", ts),
  paste0("Command: ", run_cmd),
  paste0("Person file: ", person_path),
  paste0("Priors file used: ", priors_path),
  paste0("Scaling variable rule: ", scale_note),
  "",
  "B2. Concentration diagnostics",
  paste0("Total tracts in priors: ", nrow(ts_b)),
  paste0("Total cases with tract: ", total_cases),
  paste0("Total defendant rows with tract: ", total_defs),
  paste0("Case-weighted tract p_black_prior: ", round(case_weighted_black_prior, 6)),
  paste0("Renter-weighted all-tract p_black_prior benchmark: ", round(bench_all, 6)),
  "",
  paste0("Top 10% tracts by p_black_prior: case share=", round(t10$case_share, 6),
         "; defendant share=", round(t10$def_share, 6),
         "; renter-weight share=", round(t10$renter_share, 6)),
  paste0("Top 20% tracts by p_black_prior: case share=", round(t20$case_share, 6),
         "; defendant share=", round(t20$def_share, 6),
         "; renter-weight share=", round(t20$renter_share, 6)),
  "",
  "Decile table written to:",
  decile_csv,
  "",
  "Sanity question answer",
  paste0("\"Are eviction filings disproportionately coming from tracts with high Black renter share?\" -> ", sanity),
  "",
  "Poverty concentration",
  "SKIPPED: no tract-level poverty file wired into this script by default."
)
writeLines(disprop_lines, con = disprop_report)

## -------------------- Part C: Missingness --------------------
status_tab <- def[, .N, by = race_impute_status][order(-N)]
n_def_total <- nrow(def)
n_ok <- def[race_impute_status == "ok", .N]
share_def_imputed <- ifelse(n_def_total > 0, n_ok / n_def_total, NA_real_)

ok_with_tract_prior <- merge(def_ok[!is.na(tract_norm), .(tract_geoid = tract_norm)], priors[, .(tract_geoid, p_black_prior)], by = "tract_geoid", all.x = TRUE)
mean_prior_black_ok <- ok_with_tract_prior[, mean(p_black_prior, na.rm = TRUE)]

miss_by_year <- def[!is.na(case_year), .(
  n_def = .N,
  no_tract_rate = mean(race_impute_status == "no_tract", na.rm = TRUE),
  not_person_rate = mean(race_impute_status == "not_person", na.rm = TRUE),
  ok_rate = mean(race_impute_status == "ok", na.rm = TRUE)
), by = case_year][order(case_year)]
fwrite(miss_by_year, miss_year_csv)

miss_by_role <- def[, .(
  n_def = .N,
  no_tract_rate = mean(race_impute_status == "no_tract", na.rm = TRUE),
  ok_rate = mean(race_impute_status == "ok", na.rm = TRUE)
), by = role][order(-n_def)][1:min(10L, .N)]

addr_lines <- "Address completeness proxy SKIPPED: address fields not available."
if (all(c("address_one", "address_three") %in% names(def))) {
  def[, address_complete := !is.na(address_one) & nzchar(trimws(as.character(address_one))) &
        !is.na(address_three) & nzchar(trimws(as.character(address_three)))]
  addr_tab <- def[, .(
    n_def = .N,
    no_tract_rate = mean(race_impute_status == "no_tract", na.rm = TRUE),
    ok_rate = mean(race_impute_status == "ok", na.rm = TRUE)
  ), by = address_complete][order(address_complete)]
  addr_lines <- c("Address completeness proxy", capture.output(print(addr_tab)))
}

mean_black_ok <- def_ok[, mean(p_black, na.rm = TRUE)]
lb <- ifelse(n_def_total > 0, (n_ok * mean_black_ok) / n_def_total, NA_real_)
ub <- ifelse(n_def_total > 0, (n_ok * mean_black_ok + (n_def_total - n_ok)) / n_def_total, NA_real_)
mar <- mean_black_ok

expected_ref <- suppressWarnings(as.numeric(opts$expected_black_share %||% 0.60))
bound_msg <- if (is.finite(expected_ref) && is.finite(lb) && is.finite(ub)) {
  if (expected_ref < lb || expected_ref > ub) {
    paste0("Expected reference ", round(expected_ref, 3), " is OUTSIDE bounds [", round(lb, 3), ", ", round(ub, 3), "].")
  } else {
    paste0("Expected reference ", round(expected_ref, 3), " is inside bounds [", round(lb, 3), ", ", round(ub, 3), "].")
  }
} else {
  "Bound check unavailable."
}

case_lines <- c("Case-level diagnostics SKIPPED: case file missing.")
if (!is.null(case_dt) && nrow(case_dt) > 0L) {
  share_cov_q <- safe_quantiles(case_dt$share_defendants_imputed, probs = c(0, 0.1, 0.25, 0.5, 0.75, 0.9, 1))
  case_status <- case_dt[, .N, by = case_impute_status][order(-N)]

  # Merge case-level coverage with case tract priors (modal tract among defendants)
  case_cov <- merge(case_dt[, .(id, share_defendants_imputed, case_impute_status)], case_tract, by = "id", all.x = TRUE)
  case_cov <- merge(case_cov, priors[, .(tract_geoid, p_black_prior)], by = "tract_geoid", all.x = TRUE)
  case_cov[, coverage_band := fifelse(share_defendants_imputed >= 0.9999, "full_coverage",
                                      fifelse(share_defendants_imputed >= 0.5, "mid_coverage", "low_coverage"))]
  cov_prior <- case_cov[!is.na(p_black_prior), .(
    n_cases = .N,
    mean_p_black_prior = mean(p_black_prior, na.rm = TRUE)
  ), by = coverage_band][order(coverage_band)]

  case_lines <- c(
    "Case-level diagnostics",
    paste0("share_defendants_imputed quantiles (0/10/25/50/75/90/100): ",
           paste(round(share_cov_q, 4), collapse = ", ")),
    "case_impute_status counts:",
    capture.output(print(case_status)),
    "Coverage band vs tract p_black_prior (cases with tract known):",
    capture.output(print(cov_prior))
  )
}

missing_lines <- c(
  "Race Missingness Report",
  paste0("Timestamp: ", ts),
  paste0("Command: ", run_cmd),
  paste0("Person file: ", person_path),
  paste0("Case file: ", ifelse(is.null(case_dt), "MISSING", case_path)),
  paste0("Priors file used: ", priors_path),
  paste0("Scaling variable rule: ", scale_note),
  "",
  "C1. Defendant status counts",
  paste0("Excluded counterclaim-linked defendants: ", n_counterclaim_excluded),
  capture.output(print(status_tab)),
  paste0("share_defendants_imputed = ", round(share_def_imputed, 6)),
  "",
  "C2. Missingness correlation checks",
  paste0("Mean p_black_prior among OK defendants (tract-known): ", round(mean_prior_black_ok, 6)),
  "no_tract / ok by case_year written to:",
  miss_year_csv,
  "Top roles by missingness:",
  capture.output(print(miss_by_role)),
  "",
  addr_lines,
  "",
  "C3. Bounds on overall defendant Black share",
  paste0("n_def_total = ", n_def_total),
  paste0("n_ok = ", n_ok),
  paste0("mean_p_black_ok = ", round(mean_black_ok, 6)),
  paste0("LB = ", round(lb, 6)),
  paste0("UB = ", round(ub, 6)),
  paste0("MAR = ", round(mar, 6)),
  bound_msg,
  "",
  "C4. Case-level missingness",
  case_lines
)
writeLines(missing_lines, con = missing_report)

logf("Wrote benchmark report: ", benchmark_report, log_file = log_file)
logf("Wrote disproportion report: ", disprop_report, log_file = log_file)
logf("Wrote missingness report: ", missing_report, log_file = log_file)
logf("Wrote deciles CSV: ", decile_csv, log_file = log_file)
logf("Wrote tract counts CSV: ", tract_counts_csv, log_file = log_file)
logf("Wrote missingness-by-year CSV: ", miss_year_csv, log_file = log_file)
logf("=== Finished race-imputation-diagnostics.R ===", log_file = log_file)
