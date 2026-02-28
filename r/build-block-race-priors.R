## ============================================================
## build-block-race-priors.R
## ============================================================
## Purpose: Build block-level race priors for WRU BISG imputation
## from NHGIS 2010 decennial Census SF1 block data
## (philly_blocks.csv, NHGIS dataset ds172).
##
## Table H7Z (P5 — Hispanic or Latino Origin by Race) is used for
## racial composition (NH White, NH Black, NH AIAN, NH Asian,
## NH NHPI, Hispanic/Latino, NH Other).
## Table IFP (H14 — Tenure by Race of Householder) provides
## total_renters (IFP010) for renter-weighted outputs.
##
## Two output files:
##   block_race_priors_2010.csv       - total-population priors
##   block_renter_race_priors_2010.csv - same composition, weighted
##                                       by renter HH count (IFP010)
##
## Block GEOID construction: STATEA(2) + COUNTYA(3) + TRACTA(6) +
##                           BLOCKA(4) = 15-digit standard FIPS.
##
## Output schema:
##   block_geoid, p_white_prior, p_black_prior, p_hispanic_prior,
##   p_asian_prior, p_other_prior, total_pop [, total_renters]
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

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg  <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

input_path  <- opts$input  %||% p_input(cfg, "philly_blocks_csv")
out_totpop  <- opts$output %||% p_proc(cfg, "xwalks", "block_race_priors_2010.csv")
out_renter  <- opts$output_renter %||% p_proc(cfg, "xwalks", "block_renter_race_priors_2010.csv")
qa_path     <- opts$output_qa %||% p_out(cfg, "qa", "block_race_priors_2010_qa.txt")
log_file    <- p_out(cfg, "logs", "build-block-race-priors.log")

dir.create(dirname(out_totpop), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_renter), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(qa_path),   recursive = TRUE, showWarnings = FALSE)

logf("=== Starting build-block-race-priors.R ===", log_file = log_file)
logf("Input: ", input_path, log_file = log_file)
logf("Output (totpop): ", out_totpop, log_file = log_file)
logf("Output (renter): ", out_renter, log_file = log_file)

if (!file.exists(input_path)) stop("Input file not found: ", input_path)

# ---- Load NHGIS block data ----
needed_cols <- c(
  "STATEA", "COUNTYA", "TRACTA", "BLOCKA",
  # H7Z: Hispanic/Latino Origin by Race (total population)
  "H7Z001",  # Total population
  "H7Z003",  # NH White alone
  "H7Z004",  # NH Black alone
  "H7Z005",  # NH AIAN alone
  "H7Z006",  # NH Asian alone
  "H7Z007",  # NH NHPI alone
  "H7Z008",  # NH Some Other Race alone
  "H7Z009",  # NH Two or More Races
  "H7Z010",  # Hispanic or Latino (any race)
  # IFP: Tenure by Race of Householder
  "IFP010"   # Renter-occupied HH (total)
)

raw <- fread(input_path, select = needed_cols)
setDT(raw)
logf("Raw rows loaded: ", nrow(raw), log_file = log_file)

# ---- Construct 15-digit block GEOID ----
# Standard Census FIPS: STATE(2) + COUNTY(3) + TRACT(6) + BLOCK(4)
# NHGIS stores codes as integers; pad back to canonical widths.
raw[, block_geoid := paste0(
  str_pad(as.character(STATEA),  2L, "left", "0"),
  str_pad(as.character(COUNTYA), 3L, "left", "0"),
  str_pad(as.character(TRACTA),  6L, "left", "0"),
  str_pad(as.character(BLOCKA),  4L, "left", "0")
)]
raw[, block_geoid := normalize_block_geoid(block_geoid)]

n_bad_geoid <- raw[is.na(block_geoid), .N]
if (n_bad_geoid > 0L) {
  logf("WARNING: ", n_bad_geoid, " rows with invalid block GEOID dropped.", log_file = log_file)
  raw <- raw[!is.na(block_geoid)]
}
assert_unique(raw, "block_geoid", "raw block data")

# ---- Derive race probabilities from H7Z (total population) ----
# NH Other = NH AIAN + NH NHPI + NH Some Other Race + NH Two or More Races
raw[, total_pop    := as.numeric(H7Z001)]
raw[, nh_white     := as.numeric(H7Z003)]
raw[, nh_black     := as.numeric(H7Z004)]
raw[, nh_other_raw := as.numeric(H7Z005) + as.numeric(H7Z007) +
                      as.numeric(H7Z008) + as.numeric(H7Z009)]
raw[, nh_asian     := as.numeric(H7Z006)]
raw[, hisp_any     := as.numeric(H7Z010)]
raw[, total_renters := as.numeric(IFP010)]

# Clip to non-negative
raw[, nh_other_raw := pmax(nh_other_raw, 0)]

out <- raw[, .(
  block_geoid,
  p_white_prior    = fifelse(total_pop > 0, nh_white     / total_pop, NA_real_),
  p_black_prior    = fifelse(total_pop > 0, nh_black     / total_pop, NA_real_),
  p_hispanic_prior = fifelse(total_pop > 0, hisp_any     / total_pop, NA_real_),
  p_asian_prior    = fifelse(total_pop > 0, nh_asian     / total_pop, NA_real_),
  p_other_prior    = fifelse(total_pop > 0, nh_other_raw / total_pop, NA_real_),
  total_pop,
  total_renters
)]

# ---- Validate probabilities ----
prob_cols <- c("p_white_prior", "p_black_prior", "p_hispanic_prior", "p_asian_prior", "p_other_prior")
out[, prob_sum := rowSums(.SD, na.rm = FALSE), .SDcols = prob_cols]

n_bad_probs <- out[
  !is.na(prob_sum) & (
    p_white_prior    < -1e-6 | p_black_prior    < -1e-6 |
    p_hispanic_prior < -1e-6 | p_asian_prior    < -1e-6 |
    p_other_prior    < -1e-6 |
    p_white_prior    > 1 + 1e-6 | p_black_prior > 1 + 1e-6
  ), .N
]
if (n_bad_probs > 0L) stop("Invalid prior probabilities detected in ", n_bad_probs, " blocks.")

# ---- Write total-pop priors ----
fwrite(
  out[, .(block_geoid, p_white_prior, p_black_prior, p_hispanic_prior,
          p_asian_prior, p_other_prior, total_pop)],
  out_totpop
)
logf("Wrote total-pop priors: ", out_totpop, " (", nrow(out), " rows)", log_file = log_file)

# ---- Write renter priors (same composition, total_renters weight) ----
fwrite(
  out[, .(block_geoid, p_white_prior, p_black_prior, p_hispanic_prior,
          p_asian_prior, p_other_prior, total_pop, total_renters)],
  out_renter
)
logf("Wrote renter priors:    ", out_renter, " (", nrow(out), " rows)", log_file = log_file)

# ---- QA ----
n_zero_pop    <- out[is.na(total_pop) | total_pop <= 0, .N]
n_zero_rent   <- out[is.na(total_renters) | total_renters <= 0, .N]
n_na_priors   <- out[is.na(p_white_prior), .N]
mean_abs_dev  <- out[!is.na(prob_sum), mean(abs(1 - prob_sum), na.rm = TRUE)]

qa_lines <- c(
  "Block Race Priors QA",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  paste0("Input:  ", input_path),
  paste0("Output totpop: ", out_totpop),
  paste0("Output renter: ", out_renter),
  paste0("Blocks written: ", nrow(out)),
  paste0("Blocks with total_pop <= 0 (NA priors): ", n_zero_pop),
  paste0("Blocks with total_renters <= 0: ", n_zero_rent),
  paste0("Blocks with NA priors: ", n_na_priors),
  paste0("Mean |1 - sum(priors)| on non-NA rows: ", round(mean_abs_dev, 8)),
  paste0("Black prior mean: ",   round(mean(out$p_black_prior,    na.rm = TRUE), 6)),
  paste0("Black prior median: ", round(median(out$p_black_prior,  na.rm = TRUE), 6)),
  paste0("Black prior share > 0.5: ", round(mean(out$p_black_prior > 0.5, na.rm = TRUE), 6)),
  paste0("Total pop (all blocks): ", sum(out$total_pop, na.rm = TRUE)),
  paste0("Total renters (all blocks): ", sum(out$total_renters, na.rm = TRUE))
)
writeLines(qa_lines, qa_path)
logf("Wrote QA: ", qa_path, log_file = log_file)
logf("=== Finished build-block-race-priors.R ===", log_file = log_file)
