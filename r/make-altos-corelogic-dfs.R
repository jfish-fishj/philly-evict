## ============================================================
## make-altos-corelogic-dfs.R
## ============================================================
## Purpose: Merge Altos listings with CoreLogic property data and create
##          yearly aggregations with imputed bedroom/bath counts
##
## Inputs:
##   - cfg$products$altos_clean (clean/altos_clean.csv)
##   - cfg$products$corelogic_clean (clean/corelogic_clean.csv)
##   - cfg$products$altos_corelogic_xwalk_listing (xwalks/altos_corelogic_xwalk_listing.csv)
##
## Outputs:
##   - cfg$products$altos_corelogic_agg (panels/altos_corelogic_agg.csv)
##
## Primary key: (bed_address_id, year)
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
})

# ---- Load config and set up logging ----
source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "make-altos-corelogic-dfs.log")

logf("=== Starting make-altos-corelogic-dfs.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ---- Helper functions ----
impute_units <- function(col) {
  col <- col %>% str_remove("\\.0+")
  case_when(
    str_detect(col, regex("studio", ignore_case = TRUE)) ~ 0,
    str_detect(col, regex("(zero|0)[\\s,]?(bd|bed|br[^a-z])", ignore_case = TRUE)) ~ 0,
    str_detect(col, regex("(one|1)[\\s,]?(bd|bed|br[^a-z])", ignore_case = TRUE)) ~ 1,
    str_detect(col, regex("(two|2)[\\s,]?(bd|bed|br[^a-z])", ignore_case = TRUE)) ~ 2,
    str_detect(col, regex("(three|3)[\\s,]?(bd|bed|br[^a-z])", ignore_case = TRUE)) ~ 3,
    str_detect(col, regex("(four|4)[\\s,]?(bd|bed|br[^a-z])", ignore_case = TRUE)) ~ 4,
    str_detect(col, regex("(five|5)[\\s,]?(bd|bed|br[^a-z])", ignore_case = TRUE)) ~ 5,
    TRUE ~ NA_real_
  )
}

# ---- Load input data ----
logf("Loading input data...", log_file = log_file)

# Load Altos data
altos_path <- p_product(cfg, "altos_clean")
logf("  Loading Altos: ", altos_path, log_file = log_file)
altos <- fread(altos_path)
logf("  Loaded ", nrow(altos), " Altos listings", log_file = log_file)

# Load CoreLogic data
corelogic_path <- p_product(cfg, "corelogic_clean")
logf("  Loading CoreLogic: ", corelogic_path, log_file = log_file)
corelogic <- fread(corelogic_path)
logf("  Loaded ", nrow(corelogic), " CoreLogic properties", log_file = log_file)

# Load listing-level crosswalk
xwalk_path <- p_product(cfg, "altos_corelogic_xwalk_listing")
logf("  Loading crosswalk: ", xwalk_path, log_file = log_file)

if (!file.exists(xwalk_path)) {
  stop("Crosswalk file not found. Run merge-altos-corelogic.R first: ", xwalk_path)
}
xwalk_listings <- fread(xwalk_path)
logf("  Loaded ", nrow(xwalk_listings), " crosswalk records", log_file = log_file)

# ---- Merge Altos with CoreLogic via crosswalk ----
logf("Merging Altos with CoreLogic...", log_file = log_file)

# Only use unique matches
xwalk_unique <- xwalk_listings[unique == TRUE]
logf("  Unique crosswalk matches: ", nrow(xwalk_unique), log_file = log_file)

# Merge Altos -> crosswalk -> CoreLogic
altos_m <- altos %>%
  merge(
    xwalk_unique %>% select(-contains("city")),
    by = "listing_id"
  ) %>%
  merge(
    corelogic,
    by = "clip",
    suffixes = c("_altos", "_corelogic")
  )

logf("  Merged dataset: ", nrow(altos_m), " rows", log_file = log_file)

# ---- Parse date and create time variables ----
logf("Creating date variables...", log_file = log_file)

altos_m_dates <- str_match(altos_m$date, "([0-9]{4})-([0-9]{2})-([0-9]{2})")
altos_m[, year := as.numeric(altos_m_dates[, 2])]
altos_m[, month := as.numeric(altos_m_dates[, 3])]
altos_m[, day := as.numeric(altos_m_dates[, 4])]
altos_m[, ym := year + (month - 1) / 12]
altos_m[, ymd := year + (month - 1) / 12 + (day - 1) / 365]

# ---- Impute bedroom and bath counts ----
logf("Imputing bedroom/bath counts...", log_file = log_file)

altos_m[, price := as.numeric(price)]
altos_m[, beds_txt := impute_units(property_name)]

# Coalesce beds from multiple sources: altos beds, text extraction, corelogic
altos_m[, beds_imp := coalesce(beds, beds_txt)]
# Add CoreLogic bedrooms if column exists
if ("bedrooms_all_buildings" %in% names(altos_m)) {
  altos_m[, beds_imp := coalesce(beds_imp, bedrooms_all_buildings)]
}

# Coalesce baths
altos_m[, baths_imp := baths]
if ("total_bathrooms_all_buildings" %in% names(altos_m)) {
  altos_m[, baths_imp := coalesce(baths_imp, total_bathrooms_all_buildings)]
}

altos_m[, price_per_bed := as.numeric(price) / beds_imp]
altos_m[, bed_address_id := .GRP, by = .(clip, beds_imp)]

# ---- Create census geography variables ----
logf("Creating census geography variables...", log_file = log_file)

fips_code <- "42101"  # Philadelphia County

# Build census block from fips + census_id if available
if ("census_id" %in% names(altos_m)) {
  altos_m[, census_block := paste0(
    str_pad(fips_code, width = 5, side = "left", pad = "0"),
    str_pad(census_id, width = 10, side = "left", pad = "0")
  )]
  altos_m[, census_bg := str_sub(census_block, 1, 12)]
  altos_m[, census_tract := str_sub(census_block, 1, 11)]
}

# ---- Identify voucher listings ----
logf("Identifying voucher listings...", log_file = log_file)

altos_m[, voucher := str_detect(
  property_name,
  regex("section 8|section eight|sec 8|housing choice voucher|hcv|voucher", ignore_case = TRUE)
)]
altos_m[, voucher := replace_na(voucher, FALSE)]
altos_m[, ever_voucher := max(voucher), by = clip]

# ---- Round beds and baths for aggregation ----
altos_m[, beds_imp_round := case_when(
  beds_imp <= 6 ~ round(beds_imp),
  beds_imp > 6 ~ 6L,
  TRUE ~ NA_integer_
)]

altos_m[, baths_imp_round := case_when(
  baths_imp <= 4 ~ round(baths_imp, 1),
  baths_imp > 4 & baths_imp <= 6 ~ round(baths_imp),
  baths_imp > 6 ~ 6,
  TRUE ~ NA_real_
)]

# ---- Create yearly aggregations ----
logf("Creating yearly aggregations...", log_file = log_file)

# Filter to reasonable price range
altos_m_year_aggs <- altos_m[
  price >= 500 & price <= 6000,
  list(
    price = median(price),
    month = first(month),
    beds_imp_round = first(beds_imp_round),
    baths_round = first(baths_imp_round),
    ever_voucher = first(ever_voucher),
    voucher = max(voucher),
    clip = first(clip),
    census_tract = if ("census_tract" %in% names(.SD)) first(census_tract) else NA_character_,
    census_bg = if ("census_bg" %in% names(.SD)) first(census_bg) else NA_character_
  ),
  by = .(bed_address_id, year)
]

logf("  Yearly aggregations: ", nrow(altos_m_year_aggs), " rows", log_file = log_file)

# ---- Merge back CoreLogic attributes ----
logf("Merging CoreLogic attributes...", log_file = log_file)

# Select relevant CoreLogic columns
corelogic_cols <- names(corelogic)
value_cols <- corelogic_cols[str_detect(corelogic_cols, "total_value|units|code")]
value_cols <- c("clip", intersect(value_cols, corelogic_cols))

if (length(value_cols) > 1) {
  altos_m_year_aggs <- altos_m_year_aggs %>%
    merge(
      corelogic[, ..value_cols],
      by = "clip",
      suffixes = c("_altos", "_corelogic")
    )
}

# Add stories if available
if ("stories_number" %in% names(corelogic)) {
  altos_m_year_aggs <- merge(
    altos_m_year_aggs,
    corelogic[, .(clip, stories_number)],
    by = "clip",
    all.x = TRUE
  )
  altos_m_year_aggs[, stories_num := as.numeric(stories_number)]
}

logf("  Final aggregations: ", nrow(altos_m_year_aggs), " rows", log_file = log_file)

# ---- Summary statistics ----
logf("Summary statistics:", log_file = log_file)
voucher_summary <- altos_m_year_aggs[, list(voucher_count = sum(ever_voucher), total = .N), by = year][
  , per := round(voucher_count / total, 3)
][order(year)]
for (i in 1:min(5, nrow(voucher_summary))) {
  logf("  Year ", voucher_summary$year[i], ": ", voucher_summary$voucher_count[i],
       " voucher listings (", voucher_summary$per[i] * 100, "%)", log_file = log_file)
}

# ---- Assertions ----
logf("Running assertions...", log_file = log_file)

# Check primary key uniqueness
n_rows <- nrow(altos_m_year_aggs)
n_unique <- uniqueN(altos_m_year_aggs, by = c("bed_address_id", "year"))
if (n_rows != n_unique) {
  stop("ASSERTION FAILED: Output not unique on (bed_address_id, year)")
}
logf("  Unique on (bed_address_id, year): OK", log_file = log_file)

# Check required columns exist
required_cols <- c("bed_address_id", "year", "price", "clip")
missing_cols <- setdiff(required_cols, names(altos_m_year_aggs))
if (length(missing_cols) > 0) {
  stop("ASSERTION FAILED: Missing columns: ", paste(missing_cols, collapse = ", "))
}
logf("  Required columns present: OK", log_file = log_file)

# ---- Write output ----
logf("Writing output...", log_file = log_file)

out_path <- p_product(cfg, "altos_corelogic_agg")
fwrite(altos_m_year_aggs, out_path)
logf("  Wrote ", nrow(altos_m_year_aggs), " rows to ", out_path, log_file = log_file)

logf("=== Finished make-altos-corelogic-dfs.R ===", log_file = log_file)
