## ============================================================
## build-tract-race-priors.R
## ============================================================
## Purpose: Build race priors for WRU-style imputation
## using ACS B03002 (Philadelphia by default), and run QA checks
## including concentration in high-filing eviction tracts.
##
## Output schema (CSV):
##   tract_geoid or bg_geoid or block_geoid (depending on output_geography)
##   p_white_prior
##   p_black_prior
##   p_hispanic_prior
##   p_asian_prior
##   p_other_prior
##   total_pop
##
## Notes:
## - If --input is supplied, reads that file instead of API fetch.
## - If --input is omitted, fetches via tidycensus::get_acs(table="B03002")
##   for tract/block-group geographies.
## - For build_geography=block, provide --input with block-level columns.
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

normalize_block_geoid <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- str_trim(x)
  x <- str_replace_all(x, "[^0-9]", "")
  x[nchar(x) == 0L] <- NA_character_
  too_long <- !is.na(x) & nchar(x) > 15L
  x[too_long] <- NA_character_
  short <- !is.na(x) & nchar(x) < 15L
  x[short] <- str_pad(x[short], width = 15L, side = "left", pad = "0")
  x
}

normalize_geo <- function(x, geoid_width = 11L) {
  if (geoid_width == 15L) return(normalize_block_geoid(x))
  if (geoid_width == 12L) return(normalize_bg_geoid(x))
  normalize_tract_geoid(x)
}

pick_col <- function(nms, candidates) {
  hit <- candidates[candidates %in% nms]
  if (!length(hit)) return(NA_character_)
  hit[1L]
}

extract_b03002_priors <- function(raw_dt, geoid_width = 11L) {
  dt <- copy(raw_dt)
  nms <- names(dt)

  geo_col <- pick_col(nms, c("geo_id", "block_geoid", "bg_geoid", "block_group_geoid", "tract_geoid", "GEOID", "geoid", "TRACT_GEOID"))
  if (is.na(geo_col)) stop("No geography ID column found (expected geo_id/block_geoid/bg_geoid/tract_geoid/GEOID/geoid).")

  c_total <- pick_col(nms, c("B03002_001E", "B03002_001", "total", "totalE"))
  c_nh_total <- pick_col(nms, c("B03002_002E", "B03002_002", "nh_total", "nh_totalE"))
  c_nh_white <- pick_col(nms, c("B03002_003E", "B03002_003", "nh_white", "nh_whiteE"))
  c_nh_black <- pick_col(nms, c("B03002_004E", "B03002_004", "nh_black", "nh_blackE"))
  c_nh_aian <- pick_col(nms, c("B03002_005E", "B03002_005", "nh_aian", "nh_aianE"))
  c_nh_asian <- pick_col(nms, c("B03002_006E", "B03002_006", "nh_asian", "nh_asianE"))
  c_nh_nhpi <- pick_col(nms, c("B03002_007E", "B03002_007", "nh_nhpi", "nh_nhpiE"))
  c_hisp_any <- pick_col(nms, c("B03002_012E", "B03002_012", "hisp", "hispE", "hispanic", "hispanicE"))

  required <- c(c_total, c_nh_total, c_nh_white, c_nh_black, c_nh_aian, c_nh_asian, c_nh_nhpi, c_hisp_any)
  if (anyNA(required)) {
    stop(
      "Missing required B03002 columns. Need total, nh_total, nh_white, nh_black, nh_aian, nh_asian, nh_nhpi, hisp_any. ",
      "Available columns include: ", paste(head(nms, 30L), collapse = ", ")
    )
  }

  dt[, geo_id := normalize_geo(get(geo_col), geoid_width = geoid_width)]
  dt[, total_pop := as.numeric(get(c_total))]
  dt[, nh_total := as.numeric(get(c_nh_total))]
  dt[, nh_white := as.numeric(get(c_nh_white))]
  dt[, nh_black := as.numeric(get(c_nh_black))]
  dt[, nh_aian := as.numeric(get(c_nh_aian))]
  dt[, nh_asian := as.numeric(get(c_nh_asian))]
  dt[, nh_nhpi := as.numeric(get(c_nh_nhpi))]
  dt[, hisp_any := as.numeric(get(c_hisp_any))]

  dt[, other_raw := nh_total - (nh_white + nh_black + nh_aian + nh_asian + nh_nhpi)]
  n_other_negative <- dt[!is.na(other_raw) & other_raw < 0, .N]
  dt[, other_count := pmax(other_raw, 0)]

  out <- dt[!is.na(geo_id), .(
    geo_id,
    p_white_prior = fifelse(total_pop > 0, nh_white / total_pop, NA_real_),
    p_black_prior = fifelse(total_pop > 0, nh_black / total_pop, NA_real_),
    p_hispanic_prior = fifelse(total_pop > 0, hisp_any / total_pop, NA_real_),
    p_asian_prior = fifelse(total_pop > 0, nh_asian / total_pop, NA_real_),
    p_other_prior = fifelse(total_pop > 0, other_count / total_pop, NA_real_),
    total_pop
  )]

  assert_unique(out, "geo_id", "geo priors")
  out[, prob_sum := rowSums(.SD), .SDcols = c("p_white_prior", "p_black_prior", "p_hispanic_prior", "p_asian_prior", "p_other_prior")]

  bad_prob <- out[
    !is.na(prob_sum) &
      (p_white_prior < -1e-6 | p_black_prior < -1e-6 | p_hispanic_prior < -1e-6 |
         p_asian_prior < -1e-6 | p_other_prior < -1e-6 |
         p_white_prior > 1 + 1e-6 | p_black_prior > 1 + 1e-6 | p_hispanic_prior > 1 + 1e-6 |
         p_asian_prior > 1 + 1e-6 | p_other_prior > 1 + 1e-6)
  , .N]
  if (bad_prob > 0L) stop("Invalid prior probabilities detected in ", bad_prob, " geographies.")

  list(priors = out, n_other_negative = n_other_negative)
}

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))
race_cfg <- cfg$race_priors %||% list()

acs_year <- suppressWarnings(as.integer(opts$year %||% race_cfg$year %||% 2013L))
if (is.na(acs_year) || acs_year < 2005L || acs_year > 2030L) acs_year <- 2013L
acs_survey <- as.character(opts$survey %||% "acs5")
acs_state <- as.character(opts$state %||% "PA")
acs_county <- as.character(opts$county %||% "Philadelphia")
geo_override <- opts$geography %||% NULL
build_geography <- tolower(gsub("[[:space:]]+", "_", as.character(
  opts$build_geography %||% geo_override %||% race_cfg$build_geography %||% "block_group"
)))
output_geography <- tolower(gsub("[[:space:]]+", "_", as.character(
  opts$output_geography %||% geo_override %||% race_cfg$output_geography %||% "block_group"
)))
if (!(build_geography %chin% c("block", "block_group", "tract"))) {
  stop("build_geography must be one of: block, block_group, tract")
}
if (!(output_geography %chin% c("block", "block_group", "tract"))) {
  stop("output_geography must be one of: block, block_group, tract")
}
if (output_geography == "block" && build_geography != "block") {
  stop("output_geography=block requires build_geography=block.")
}
if (output_geography == "block_group" && build_geography == "tract") {
  stop("output_geography=block_group requires build_geography=block_group or block.")
}
build_geography_api <- if (build_geography == "block") {
  "block"
} else if (build_geography == "block_group") {
  "block group"
} else {
  "tract"
}
build_geoid_width <- if (build_geography == "block") {
  15L
} else if (build_geography == "block_group") {
  12L
} else {
  11L
}

input_path <- opts$input %||% NA_character_
default_stub <- if (output_geography == "block") {
  "block_race_priors"
} else if (output_geography == "block_group") {
  "bg_race_priors"
} else {
  "tract_race_priors"
}
out_path <- opts$output %||% p_proc(cfg, "xwalks", paste0(default_stub, "_", acs_year, ".csv"))
qa_path <- opts$output_qa %||% p_out(cfg, "qa", paste0(default_stub, "_", acs_year, "_qa.txt"))
top_path <- opts$output_top %||% p_out(cfg, "qa", paste0(default_stub, "_top_filing_tracts_", acs_year, ".csv"))
preflight_person_path <- opts$preflight_person %||% p_out(cfg, "qa", "race_imputation_preflight_person.csv")
top_n <- suppressWarnings(as.integer(opts$top_n %||% 50L))
if (is.na(top_n) || top_n < 1L) top_n <- 50L

log_file <- p_out(cfg, "logs", "build-tract-race-priors.log")
logf("=== Starting build-tract-race-priors.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf("Build geography: ", build_geography, " (API token='", build_geography_api, "')", log_file = log_file)
logf("Output geography: ", output_geography, log_file = log_file)
logf("Output: ", out_path, log_file = log_file)
logf("QA: ", qa_path, log_file = log_file)
logf("Top filing tract QA: ", top_path, log_file = log_file)

dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(qa_path), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(top_path), recursive = TRUE, showWarnings = FALSE)

raw <- NULL
source_mode <- NULL
source_desc <- NULL

if (!is.na(input_path) && nzchar(input_path) && file.exists(input_path)) {
  logf("Reading local input file: ", input_path, log_file = log_file)
  raw <- fread(input_path)
  source_mode <- "local_file"
  source_desc <- input_path
} else {
  if (build_geography == "block") {
    stop("build_geography=block currently requires a local --input file with block-level columns.")
  }
  if (!requireNamespace("tidycensus", quietly = TRUE)) {
    stop("Missing package tidycensus and no local --input was provided.")
  }
  logf("Fetching B03002 from Census API (", build_geography_api, "; ",
       acs_state, ", ", acs_county, ", ", acs_year, ", ", acs_survey, ")...", log_file = log_file)
  raw <- tryCatch(
    as.data.table(tidycensus::get_acs(
      geography = build_geography_api,
      state = acs_state,
      county = acs_county,
      year = acs_year,
      survey = acs_survey,
      table = "B03002",
      output = "wide",
      geometry = FALSE,
      cache_table = TRUE
    )),
    error = function(e) {
      stop(
        "Failed to fetch Census API B03002 at geography=", build_geography_api, ". ",
        "If running offline, provide a local file via --input=... with B03002 columns.\n",
        "Original error: ", conditionMessage(e)
      )
    }
  )
  source_mode <- "census_api"
  source_desc <- paste0(build_geography_api, "|", acs_state, "|", acs_county, "|", acs_year, "|", acs_survey, "|B03002")
}

logf("Raw rows loaded: ", nrow(raw), log_file = log_file)
built <- extract_b03002_priors(raw, geoid_width = build_geoid_width)
priors_geo <- built$priors
priors_geo[, geo_id := normalize_geo(geo_id, geoid_width = build_geoid_width)]

aggregate_to_tract <- function(priors_dt) {
  x <- copy(priors_dt)
  x <- x[!is.na(geo_id)]
  x[, tract_geoid := substr(geo_id, 1L, 11L)]
  x[, w := as.numeric(total_pop)]
  x[, w := fifelse(is.finite(w) & w > 0, w, 0)]
  out <- x[, .(
    total_pop = sum(w, na.rm = TRUE),
    white_num = sum(p_white_prior * w, na.rm = TRUE),
    black_num = sum(p_black_prior * w, na.rm = TRUE),
    hisp_num = sum(p_hispanic_prior * w, na.rm = TRUE),
    asian_num = sum(p_asian_prior * w, na.rm = TRUE),
    other_num = sum(p_other_prior * w, na.rm = TRUE)
  ), by = tract_geoid]
  out[, `:=`(
    p_white_prior = fifelse(total_pop > 0, white_num / total_pop, NA_real_),
    p_black_prior = fifelse(total_pop > 0, black_num / total_pop, NA_real_),
    p_hispanic_prior = fifelse(total_pop > 0, hisp_num / total_pop, NA_real_),
    p_asian_prior = fifelse(total_pop > 0, asian_num / total_pop, NA_real_),
    p_other_prior = fifelse(total_pop > 0, other_num / total_pop, NA_real_)
  )]
  out[, c("white_num", "black_num", "hisp_num", "asian_num", "other_num") := NULL]
  out
}

aggregate_to_bg <- function(priors_dt) {
  x <- copy(priors_dt)
  x <- x[!is.na(geo_id)]
  x[, bg_geoid := substr(geo_id, 1L, 12L)]
  x[, w := as.numeric(total_pop)]
  x[, w := fifelse(is.finite(w) & w > 0, w, 0)]
  out <- x[, .(
    total_pop = sum(w, na.rm = TRUE),
    white_num = sum(p_white_prior * w, na.rm = TRUE),
    black_num = sum(p_black_prior * w, na.rm = TRUE),
    hisp_num = sum(p_hispanic_prior * w, na.rm = TRUE),
    asian_num = sum(p_asian_prior * w, na.rm = TRUE),
    other_num = sum(p_other_prior * w, na.rm = TRUE)
  ), by = bg_geoid]
  out[, `:=`(
    p_white_prior = fifelse(total_pop > 0, white_num / total_pop, NA_real_),
    p_black_prior = fifelse(total_pop > 0, black_num / total_pop, NA_real_),
    p_hispanic_prior = fifelse(total_pop > 0, hisp_num / total_pop, NA_real_),
    p_asian_prior = fifelse(total_pop > 0, asian_num / total_pop, NA_real_),
    p_other_prior = fifelse(total_pop > 0, other_num / total_pop, NA_real_)
  )]
  out[, c("white_num", "black_num", "hisp_num", "asian_num", "other_num") := NULL]
  out
}

priors <- NULL
priors_tract_for_top <- NULL
if (output_geography == "tract") {
  if (build_geoid_width == 12L || build_geoid_width == 15L) {
    priors <- aggregate_to_tract(priors_geo)
  } else {
    priors <- copy(priors_geo)
    setnames(priors, "geo_id", "tract_geoid")
  }
  priors_tract_for_top <- copy(priors)
} else if (output_geography == "block_group") {
  if (build_geoid_width == 15L) {
    priors <- aggregate_to_bg(priors_geo)
  } else {
    priors <- copy(priors_geo)
    setnames(priors, "geo_id", "bg_geoid")
  }
  priors_tract_for_top <- aggregate_to_tract(priors_geo)
} else {
  priors <- copy(priors_geo)
  setnames(priors, "geo_id", "block_geoid")
  priors_tract_for_top <- aggregate_to_tract(priors_geo)
}

if (output_geography == "tract") {
  fwrite(priors[, .(tract_geoid, p_white_prior, p_black_prior, p_hispanic_prior, p_asian_prior, p_other_prior, total_pop)], out_path)
} else if (output_geography == "block_group") {
  fwrite(priors[, .(bg_geoid, p_white_prior, p_black_prior, p_hispanic_prior, p_asian_prior, p_other_prior, total_pop)], out_path)
} else {
  fwrite(priors[, .(block_geoid, p_white_prior, p_black_prior, p_hispanic_prior, p_asian_prior, p_other_prior, total_pop)], out_path)
}
logf("Wrote priors: ", out_path, " (", nrow(priors), " rows; output_geography=", output_geography, ")", log_file = log_file)
priors[, prob_sum := rowSums(.SD), .SDcols = c("p_white_prior", "p_black_prior", "p_hispanic_prior", "p_asian_prior", "p_other_prior")]

# Optional diagnostic: do high-filing tracts skew majority Black?
top_join <- NULL
top_summary <- "preflight file not found"
if (file.exists(preflight_person_path)) {
  pf <- fread(preflight_person_path, select = c("id", "is_defendant", "impute_status", "tract_geoid"))
  setDT(pf)
  pf[, tract_geoid := normalize_tract_geoid(tract_geoid)]

  case_tract <- pf[
    is_defendant == TRUE & impute_status == "ok" & !is.na(tract_geoid),
    .N,
    by = .(id, tract_geoid)
  ][order(id, -N, tract_geoid), .SD[1L], by = id][, .(id, tract_geoid)]

  filings_by_tract <- case_tract[, .(filed_cases = .N), by = tract_geoid][order(-filed_cases)]
  top_join <- merge(
    filings_by_tract[1:min(top_n, .N)],
    priors_tract_for_top[, .(tract_geoid, p_black_prior, p_white_prior, p_hispanic_prior)],
    by = "tract_geoid",
    all.x = TRUE
  )[order(-filed_cases)]

  fwrite(top_join, top_path)
  top_summary <- paste0(
    "top_", top_n, "_tracts mean_black=", round(mean(top_join$p_black_prior, na.rm = TRUE), 4),
    ", median_black=", round(median(top_join$p_black_prior, na.rm = TRUE), 4),
    ", share_majority_black=", round(mean(top_join$p_black_prior > 0.5, na.rm = TRUE), 4)
  )
}

qa_lines <- c(
  "Race Priors QA",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  paste0("Config: ", cfg$meta$config_path),
  paste0("Build geography: ", build_geography),
  paste0("Output geography: ", output_geography),
  paste0("Source mode: ", source_mode),
  paste0("Source desc: ", source_desc),
  paste0("Rows written: ", nrow(priors)),
  paste0(ifelse(output_geography == "block", "Blocks",
                ifelse(output_geography == "block_group", "Block groups", "Tracts")),
         " with total_pop <= 0: ", priors[is.na(total_pop) | total_pop <= 0, .N]),
  paste0("Rows with negative raw Other (clipped to 0): ", built$n_other_negative),
  paste0("Mean abs(1-sum probs): ", round(mean(abs(1 - priors$prob_sum), na.rm = TRUE), 8)),
  paste0("Black prior mean: ", round(mean(priors$p_black_prior, na.rm = TRUE), 6)),
  paste0("Black prior median: ", round(median(priors$p_black_prior, na.rm = TRUE), 6)),
  paste0("Black prior share > 0.5: ", round(mean(priors$p_black_prior > 0.5, na.rm = TRUE), 6)),
  "",
  paste0("Top filing tract diagnostic: ", top_summary)
)
writeLines(qa_lines, con = qa_path)

logf("Wrote QA: ", qa_path, log_file = log_file)
if (!is.null(top_join)) logf("Wrote top-filing-tract table: ", top_path, log_file = log_file)
logf("=== Finished build-tract-race-priors.R ===", log_file = log_file)
