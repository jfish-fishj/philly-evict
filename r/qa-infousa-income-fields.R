## ============================================================
## qa-infousa-income-fields.R
## ============================================================
## Purpose: Quick QA for InfoUSA modeled income/wealth fields.
##
## Outputs:
##   - output/qa/infousa_income_field_summary.csv
##   - output/qa/infousa_income_field_correlations.csv
##   - output/qa/infousa_income_field_codebook_excerpt.txt
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
})

source("r/config.R")
source("r/build-renter-poverty-geo.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "qa-infousa-income-fields.log")

logf("=== Starting qa-infousa-income-fields.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

out_dir <- p_out(cfg, "qa")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
out_summary <- file.path(out_dir, "infousa_income_field_summary.csv")
out_corr <- file.path(out_dir, "infousa_income_field_correlations.csv")
out_codebook <- file.path(out_dir, "infousa_income_field_codebook_excerpt.txt")

inf_path <- p_product(cfg, "infousa_clean")
bg_pov_path <- p_product(cfg, "bg_renter_poverty_share")
tract_pov_path <- p_product(cfg, "tract_renter_poverty_share")

fields <- data.table(
  field = c(
    "find_div_1000",
    "ppi_div_1000",
    "wealth_finder_score",
    "estmtd_home_val_div_1000",
    "owner_renter_status"
  ),
  doc_min = c(5, 5, 0, 5, 0),
  doc_max = c(500, 500, 9999, 9999, 9),
  description = c(
    "Predicted household income ($000s)",
    "Estimated purchasing power index ($000s)",
    "Modeled household wealth ($000s)",
    "Estimated home value ($000s)",
    "Owner-renter likelihood score"
  )
)

sel_cols <- c(
  "familyid",
  "year",
  "primary_family_ind",
  fields$field,
  "ge_als_census_tract_2010",
  "ge_als_census_bg_2010"
)
inf <- fread(inf_path, select = sel_cols)
assert_has_cols(inf, sel_cols, "infousa_clean income QA input")

inf <- as.data.table(inf)
inf <- inf[primary_family_ind == 1L]
if (nrow(inf) == 0L) stop("No primary-family InfoUSA rows remain for income QA.")

inf[, ge_als_census_tract_2010 := suppressWarnings(as.integer(ge_als_census_tract_2010))]
inf[, ge_als_census_bg_2010 := suppressWarnings(as.integer(ge_als_census_bg_2010))]
inf[, tract_geoid := fifelse(
  !is.na(ge_als_census_tract_2010) & ge_als_census_tract_2010 > 0L,
  sprintf("42101%06d", ge_als_census_tract_2010),
  NA_character_
)]
inf[, bg_geoid := fifelse(
  !is.na(ge_als_census_tract_2010) & ge_als_census_tract_2010 > 0L &
    !is.na(ge_als_census_bg_2010) & ge_als_census_bg_2010 > 0L,
  paste0(sprintf("42101%06d", ge_als_census_tract_2010), ge_als_census_bg_2010),
  NA_character_
)]
inf[, tract_geoid := normalize_tract_geoid(tract_geoid)]
inf[, bg_geoid := normalize_bg_geoid(bg_geoid)]

bg_pov <- fread(bg_pov_path)
tract_pov <- fread(tract_pov_path)
assert_has_cols(bg_pov, c("bg_geoid", "renter_poverty_share"), "bg poverty QA input")
assert_has_cols(tract_pov, c("tract_geoid", "renter_poverty_share"), "tract poverty QA input")
assert_unique(bg_pov, "bg_geoid", "bg poverty QA input")
assert_unique(tract_pov, "tract_geoid", "tract poverty QA input")
bg_pov[, bg_geoid := normalize_bg_geoid(bg_geoid)]
tract_pov[, tract_geoid := normalize_tract_geoid(tract_geoid)]

inf <- merge(
  inf,
  bg_pov[, .(bg_geoid, bg_poverty_share = renter_poverty_share)],
  by = "bg_geoid",
  all.x = TRUE
)
inf <- merge(
  inf,
  tract_pov[, .(tract_geoid, tract_poverty_share = renter_poverty_share)],
  by = "tract_geoid",
  all.x = TRUE
)

summary_rows <- vector("list", nrow(fields))
for (i in seq_len(nrow(fields))) {
  fld <- fields$field[i]
  x <- suppressWarnings(as.numeric(inf[[fld]]))
  nonmiss <- !is.na(x)
  in_range <- nonmiss & x >= fields$doc_min[i] & x <= fields$doc_max[i]
  valid <- x[in_range]

  summary_rows[[i]] <- data.table(
    field = fld,
    description = fields$description[i],
    n = length(x),
    pct_missing = mean(!nonmiss),
    pct_zero = mean(nonmiss & x == 0),
    pct_out_of_doc_range = mean(nonmiss & !in_range),
    min_valid = if (length(valid)) min(valid) else NA_real_,
    p01_valid = if (length(valid)) as.numeric(quantile(valid, 0.01)) else NA_real_,
    p50_valid = if (length(valid)) as.numeric(quantile(valid, 0.50)) else NA_real_,
    p99_valid = if (length(valid)) as.numeric(quantile(valid, 0.99)) else NA_real_,
    max_valid = if (length(valid)) max(valid) else NA_real_
  )
}
summary_dt <- rbindlist(summary_rows, fill = TRUE)

cor_rows <- vector("list", 2L * nrow(fields))
idx <- 0L
for (i in seq_len(nrow(fields))) {
  fld <- fields$field[i]
  x <- suppressWarnings(as.numeric(inf[[fld]]))
  in_range <- !is.na(x) & x >= fields$doc_min[i] & x <= fields$doc_max[i]

  for (geo in c("bg", "tract")) {
    pov_col <- if (identical(geo, "bg")) "bg_poverty_share" else "tract_poverty_share"
    cc <- in_range & !is.na(inf[[pov_col]])
    idx <- idx + 1L
    cor_rows[[idx]] <- data.table(
      field = fld,
      geography = geo,
      n_complete = sum(cc),
      pct_with_geo = mean(!is.na(inf[[pov_col]])),
      pearson_corr = if (sum(cc) >= 10L) suppressWarnings(stats::cor(x[cc], inf[[pov_col]][cc], method = "pearson")) else NA_real_,
      spearman_corr = if (sum(cc) >= 10L) suppressWarnings(stats::cor(x[cc], inf[[pov_col]][cc], method = "spearman")) else NA_real_
    )
  }
}
cor_dt <- rbindlist(cor_rows, fill = TRUE)

extract_codebook_excerpt <- function(docx_path, variables) {
  if (!file.exists(docx_path)) {
    return("Codebook docx not found.")
  }
  txt <- tryCatch(
    system2("textutil", c("-convert", "txt", "-stdout", docx_path), stdout = TRUE, stderr = TRUE),
    error = function(e) character()
  )
  if (!length(txt)) {
    return("Could not extract text from codebook docx.")
  }

  out <- c(
    "InfoUSA codebook excerpts for modeled income/wealth fields",
    paste("Source:", docx_path),
    ""
  )
  for (v in variables) {
    hit <- which(tolower(txt) == tolower(v))
    out <- c(out, paste0("== ", v, " =="))
    if (!length(hit)) {
      out <- c(out, "Not found in codebook export.", "")
      next
    }
    first_hit <- hit[1]
    excerpt <- txt[first_hit:min(length(txt), first_hit + 4L)]
    out <- c(out, excerpt, "")
  }
  out
}

codebook_lines <- extract_codebook_excerpt(
  docx_path = file.path(cfg$paths$repo_root, "docs", "InfoUSA_2020_codebook.docx"),
  variables = c("wealth_finder_score", "find_div_1000", "ppi_div_1000", "estmtd_home_val_div_1000", "owner_renter_status")
)

writeLines(codebook_lines, out_codebook)
fwrite(summary_dt, out_summary)
fwrite(cor_dt, out_corr)

logf(
  "Primary-family rows used for QA: ", format(nrow(inf), big.mark = ","),
  "; BG merge coverage=", sprintf("%.3f", mean(!is.na(inf$bg_poverty_share))),
  "; tract merge coverage=", sprintf("%.3f", mean(!is.na(inf$tract_poverty_share))),
  log_file = log_file
)
logf("Wrote summary: ", out_summary, log_file = log_file)
logf("Wrote correlations: ", out_corr, log_file = log_file)
logf("Wrote codebook excerpt: ", out_codebook, log_file = log_file)
logf("=== Completed qa-infousa-income-fields.R ===", log_file = log_file)
