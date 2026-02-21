## ============================================================
## impute-race.R
## ============================================================
## Purpose: Impute race probabilities from parsed names (preflight output).
##          This implementation is offline-safe (no census API calls):
##          combines first-name and last-name race probability dictionaries,
##          with optional tract priors if provided.
##
## Defaults are tuned for pilot runs (sample_n = 5000).
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

to_bool <- function(x, default = FALSE) {
  if (is.null(x) || is.na(x)) return(default)
  x <- tolower(as.character(x))
  if (x %in% c("1", "true", "t", "yes", "y")) return(TRUE)
  if (x %in% c("0", "false", "f", "no", "n")) return(FALSE)
  default
}

entropy5 <- function(w, b, h, a, o, eps = 1e-12) {
  p <- cbind(w, b, h, a, o)
  p <- pmax(p, eps)
  -rowSums(p * log(p))
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

normalize_pid <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- str_trim(x)
  x <- str_replace_all(x, "[^0-9]", "")
  x[nchar(x) == 0L] <- NA_character_
  str_pad(x, width = 9, side = "left", pad = "0")
}

normalize_prob_table <- function(dt, prefix = "p_") {
  pcols <- c("whi", "bla", "his", "asi", "oth")
  for (cc in pcols) dt[, (cc) := as.numeric(get(cc))]
  dt[, s := rowSums(.SD, na.rm = TRUE), .SDcols = pcols]
  dt[s <= 0 | !is.finite(s), s := NA_real_]
  for (cc in pcols) dt[, (cc) := fifelse(!is.na(s), get(cc) / s, NA_real_)]
  setnames(dt, pcols, paste0(prefix, c("white", "black", "hispanic", "asian", "other")))
  dt[, s := NULL]
  dt[]
}

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

input_path <- opts$input %||% p_out(cfg, "qa", "race_imputation_preflight_person.csv")
first_probs_path <- opts$first_probs %||% p_in(cfg, "name-files/first_nameRaceProbs.csv")
last_probs_path <- opts$last_probs %||% p_in(cfg, "name-files/last_nameRaceProbs.csv")
prior_year <- suppressWarnings(as.integer(opts$prior_year %||% 2010L))
if (is.na(prior_year) || prior_year < 1900L || prior_year > 2100L) prior_year <- 2010L
tract_priors_path <- opts$tract_priors %||% p_proc(cfg, "xwalks", paste0("tract_race_priors_", prior_year, ".csv"))

out_person <- opts$output_person %||% p_out(cfg, "qa", "race_imputed_person_sample.csv")
out_case <- opts$output_case %||% p_out(cfg, "qa", "race_imputed_case_sample.csv")
out_qa <- opts$output_qa %||% p_out(cfg, "qa", "race_imputed_qa_sample.txt")

sample_n <- suppressWarnings(as.integer(opts$sample_n %||% 5000L))
seed <- suppressWarnings(as.integer(opts$seed %||% cfg$run$seed %||% 123L))
confidence_threshold <- suppressWarnings(as.numeric(opts$confidence_threshold %||% 0.7))
use_tract_priors <- to_bool(opts$use_tract_priors %||% "TRUE", default = TRUE)

log_file <- p_out(cfg, "logs", "impute-race.log")
logf("=== Starting impute-race.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf("Input: ", input_path, log_file = log_file)
logf("Dictionaries: first=", first_probs_path, ", last=", last_probs_path, log_file = log_file)
if (!isTRUE(use_tract_priors)) {
  stop("Tract priors are required in this pipeline. Remove --use-tract-priors=FALSE.")
}
logf("Tract priors required: TRUE, path=", tract_priors_path, log_file = log_file)

if (!file.exists(tract_priors_path)) {
  stop(
    "Tract priors file not found: ", tract_priors_path, "\n",
    "Build priors first (recommended): Rscript r/build-tract-race-priors.R --year=", prior_year, "\n",
    "This script now requires tract priors for all imputations."
  )
}

dt <- fread(input_path)
setDT(dt)
required_cols <- c("id", "is_defendant", "impute_status", "first_name", "last_name", "tract_geoid")
assert_has_cols(dt, required_cols, "race preflight input")

n_input <- nrow(dt)
if (!is.na(sample_n) && sample_n > 0L && sample_n < nrow(dt)) {
  set.seed(seed)
  dt <- dt[sample(.N, sample_n)]
  logf("Sample mode: kept ", nrow(dt), " rows from ", n_input, " with seed=", seed, log_file = log_file)
}

first_tbl <- fread(first_probs_path)
last_tbl <- fread(last_probs_path)
setDT(first_tbl); setDT(last_tbl)
first_tbl[, name := toupper(str_squish(as.character(name)))]
last_tbl[, name := toupper(str_squish(as.character(name)))]
first_tbl <- normalize_prob_table(first_tbl[, .(name, whi, bla, his, asi, oth)], prefix = "f_")
last_tbl <- normalize_prob_table(last_tbl[, .(name, whi, bla, his, asi, oth)], prefix = "l_")
setkey(first_tbl, name)
setkey(last_tbl, name)

tract_priors <- fread(tract_priors_path)
setDT(tract_priors)
need <- c("tract_geoid", "p_white_prior", "p_black_prior", "p_hispanic_prior", "p_asian_prior", "p_other_prior")
assert_has_cols(tract_priors, need, "tract priors")
tract_priors[, tract_geoid := normalize_tract_geoid(tract_geoid)]
tract_priors <- unique(tract_priors[, ..need], by = "tract_geoid")
setkey(tract_priors, tract_geoid)
logf("Loaded tract priors: ", nrow(tract_priors), " rows", log_file = log_file)

dt[, row_id := .I]
dt[, first_u := toupper(str_squish(as.character(first_name)))]
dt[, last_u := toupper(str_squish(as.character(last_name)))]
dt[first_u %in% c("", "NA"), first_u := NA_character_]
dt[last_u %in% c("", "NA"), last_u := NA_character_]
dt[, tract_chr := normalize_tract_geoid(tract_geoid)]

targets <- dt[is_defendant == TRUE & impute_status == "ok"]
logf("Target rows (defendant & preflight ok): ", nrow(targets), log_file = log_file)
if (targets[is.na(tract_chr), .N] > 0L) {
  stop("Found defendant/impute_status=ok rows with missing tract_geoid after normalization.")
}

u <- unique(targets[, .(first_u, last_u, tract_chr)])
setkey(u, first_u)
u[first_tbl, `:=`(
  f_p_white = i.f_white,
  f_p_black = i.f_black,
  f_p_hispanic = i.f_hispanic,
  f_p_asian = i.f_asian,
  f_p_other = i.f_other
)]
setkey(u, last_u)
u[last_tbl, `:=`(
  l_p_white = i.l_white,
  l_p_black = i.l_black,
  l_p_hispanic = i.l_hispanic,
  l_p_asian = i.l_asian,
  l_p_other = i.l_other
)]

u[, `:=`(
  prior_level = NA_character_,
  p_white_prior = NA_real_,
  p_black_prior = NA_real_,
  p_hispanic_prior = NA_real_,
  p_asian_prior = NA_real_,
  p_other_prior = NA_real_
)]
setkey(u, tract_chr)
u[tract_priors, `:=`(
  prior_level = "tract",
  p_white_prior = i.p_white_prior,
  p_black_prior = i.p_black_prior,
  p_hispanic_prior = i.p_hispanic_prior,
  p_asian_prior = i.p_asian_prior,
  p_other_prior = i.p_other_prior
), on = .(tract_chr = tract_geoid)]

missing_prior_rows <- u[
  is.na(p_white_prior) | is.na(p_black_prior) | is.na(p_hispanic_prior) | is.na(p_asian_prior) | is.na(p_other_prior),
  .N
]
if (missing_prior_rows > 0L) {
  missing_tracts <- unique(u[
    is.na(p_white_prior) | is.na(p_black_prior) | is.na(p_hispanic_prior) | is.na(p_asian_prior) | is.na(p_other_prior),
    tract_chr
  ])
  missing_examples <- paste(head(missing_tracts, 10L), collapse = ", ")
  stop(
    "Missing tract priors for ", length(missing_tracts), " tract(s) across ", missing_prior_rows, " unique name+tract rows. ",
    "Example tract(s): ", missing_examples
  )
}

for (cc in c("white", "black", "hispanic", "asian", "other")) {
  fcol <- paste0("f_p_", cc)
  lcol <- paste0("l_p_", cc)
  pcol <- paste0("p_", cc, "_raw")
  u[, (pcol) := get(paste0("p_", cc, "_prior")) *
      fifelse(!is.na(get(fcol)), get(fcol), 1) *
      fifelse(!is.na(get(lcol)), get(lcol), 1)]
}

u[, has_first_lookup := !is.na(f_p_white)]
u[, has_last_lookup := !is.na(l_p_white)]
u[, race_impute_status := fifelse(has_first_lookup | has_last_lookup, "ok", "name_lookup_failed")]
u[, race_impute_method := fifelse(
  has_first_lookup & has_last_lookup, "firstname+surname+prior",
  fifelse(has_last_lookup, "surname+prior",
          fifelse(has_first_lookup, "firstname+prior", "none"))
)]

u[, raw_sum := rowSums(.SD, na.rm = TRUE), .SDcols = patterns("^p_.*_raw$")]
for (cc in c("white", "black", "hispanic", "asian", "other")) {
  raw_col <- paste0("p_", cc, "_raw")
  out_col <- paste0("p_", cc)
  u[, (out_col) := fifelse(race_impute_status == "ok" & raw_sum > 0, get(raw_col) / raw_sum, NA_real_)]
}
u[, c("p_white_raw", "p_black_raw", "p_hispanic_raw", "p_asian_raw", "p_other_raw", "raw_sum") := NULL]

setkey(dt, first_u, last_u, tract_chr)
setkey(u, first_u, last_u, tract_chr)
dt[u, `:=`(
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
dt[is.na(race_impute_status) & is_defendant == FALSE, race_impute_status := "not_defendant"]

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
def <- dt[is_defendant == TRUE]
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

status_tab <- dt[, .N, by = race_impute_status][order(-N)]
method_tab <- dt[, .N, by = race_impute_method][order(-N)]
prior_tab <- dt[race_impute_status == "ok", .N, by = prior_level][order(-N)]
case_tab <- case_out[, .N, by = case_impute_status][order(-N)]
ok_prob_sum <- dt[race_impute_status == "ok", rowSums(.SD), .SDcols = c("p_white", "p_black", "p_hispanic", "p_asian", "p_other")]

fwrite(dt, out_person)
fwrite(case_out, out_case)

qa_lines <- c(
  "Race Imputation QA (Offline Name Dictionaries)",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  paste0("Input rows: ", n_input),
  paste0("Rows written: ", nrow(dt)),
  paste0("Sample_n: ", ifelse(is.na(sample_n), "ALL", as.character(sample_n))),
  "Use tract priors: TRUE (required)",
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
  paste0("Prob sum |mean abs(1-sum p)| on ok rows: ",
         ifelse(length(ok_prob_sum), round(mean(abs(1 - ok_prob_sum), na.rm = TRUE), 8), NA_real_))
)
writeLines(qa_lines, con = out_qa)

logf("Wrote person output: ", out_person, log_file = log_file)
logf("Wrote case output: ", out_case, log_file = log_file)
logf("Wrote QA output: ", out_qa, log_file = log_file)
logf("=== Finished impute-race.R ===", log_file = log_file)
