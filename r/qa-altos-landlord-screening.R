#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(fs)
})

source("r/config.R")
source("r/helper-functions.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "qa-altos-landlord-screening.log")

logf("=== Starting qa-altos-landlord-screening.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

ANALYSIS_YEAR_MIN <- 2006L
ANALYSIS_YEAR_MAX <- 2019L
LOO_MIN_UNIT_YEARS <- suppressWarnings(as.integer(cfg$run$loo_min_unit_years %||% 100L))
ACQ_HIGH_FILER_THRESHOLD <- as.numeric(
  cfg$run$loo_filing_threshold %||%
    cfg$run$acq_high_filer_threshold %||%
    0.05
)
MAX_ALLOWABLE_FILING_RATE <- as.numeric(
  cfg$run$transfer_evictions_max_filing_rate %||% 0.75
)
TEXT_YEAR_MAX <- 2018L

if (!is.finite(LOO_MIN_UNIT_YEARS) || LOO_MIN_UNIT_YEARS <= 0L) {
  stop("Invalid run$loo_min_unit_years: expected a positive integer.")
}
if (!is.finite(ACQ_HIGH_FILER_THRESHOLD) || ACQ_HIGH_FILER_THRESHOLD <= 0 || ACQ_HIGH_FILER_THRESHOLD >= 1) {
  stop("Invalid LOO filing threshold: expected a number strictly between 0 and 1.")
}
if (!is.finite(MAX_ALLOWABLE_FILING_RATE) || MAX_ALLOWABLE_FILING_RATE <= 0) {
  stop("Invalid transfer-evictions filing-rate cap: expected a positive number.")
}

ACQ_BIN_LABELS <- c(
  "High-filer portfolio",
  "Low-filer portfolio",
  "Small portfolio",
  "Single-purchase"
)

normalize_pid <- function(x) {
  y <- as.character(x)
  y[!nzchar(y) | y %in% c("NA", "NaN")] <- NA_character_
  out <- stringr::str_pad(y, width = 9, side = "left", pad = "0")
  out[is.na(y)] <- NA_character_
  out
}

mode_char <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (!length(x)) return(NA_character_)
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

qa_dir <- file.path(p_out(cfg, "qa"), "altos-landlord-screening")
dir_create(qa_dir, recurse = TRUE)

logf("Loading inputs...", log_file = log_file)

altos <- fread(
  p_product(cfg, "altos_clean"),
  select = c(
    "listing_id", "pm.uid", "year", "date", "property_name", "unit_name",
    "street_address", "pm.address", "n_sn_ss_c", "price", "beds", "baths",
    "floor_size", "type"
  )
)
xwalk_path_phase1 <- tryCatch(p_product(cfg, "xwalk_altos_to_parcel"), error = function(e) NULL)
if (is.null(xwalk_path_phase1) || !file.exists(xwalk_path_phase1)) {
  stop("Missing required Altos winner crosswalk at ",
       p_product(cfg, "xwalk_altos_to_parcel"),
       ". Re-run r/make-address-parcel-xwalk.R first.")
}
xwalk_altos <- fread(xwalk_path_phase1)
xwalk_pid_cong <- fread(
  p_product(cfg, "xwalk_pid_conglomerate"),
  select = c(
    "PID", "year", "conglomerate_id", "portfolio_size_conglomerate_yr",
    "portfolio_bin_conglomerate_yr", "total_units_conglomerate_yr",
    "is_corp_conglomerate", "is_financial_intermediary_owner"
  )
)
owner_portfolio <- fread(
  p_product(cfg, "owner_portfolio"),
  select = c(
    "conglomerate_id", "n_properties_total", "n_properties_non_sheriff",
    "n_transfers", "first_acquisition_year", "last_acquisition_year", "portfolio_bin"
  )
)
xwalk_ent_cong <- fread(
  p_product(cfg, "xwalk_entity_conglomerate"),
  select = c("conglomerate_id", "owner_category")
)
bldg <- fread(
  p_product(cfg, "bldg_panel_blp"),
  select = c("PID", "year", "num_filings", "total_units", "building_type", "filing_rate_eb_pre_covid")
)
parcels <- fread(
  p_product(cfg, "parcels_clean"),
  select = c("parcel_number", "n_sn_ss_c", "owner_1", "building_code_description_new")
)

assert_has_cols(altos, c("listing_id", "pm.uid", "year", "property_name"), "altos_clean")
assert_has_cols(xwalk_pid_cong, c("PID", "year", "conglomerate_id"), "xwalk_pid_conglomerate")
assert_has_cols(owner_portfolio, c("conglomerate_id", "portfolio_bin"), "owner_portfolio")
assert_has_cols(
  bldg,
  c("PID", "year", "num_filings", "total_units", "building_type", "filing_rate_eb_pre_covid"),
  "bldg_panel_blp"
)
assert_has_cols(parcels, c("parcel_number", "n_sn_ss_c"), "parcels_clean")

altos[, listing_id := as.character(listing_id)]
assert_has_cols(
  xwalk_altos,
  c("source_address_id", "anchor_pid", "parcel_number", "xwalk_status", "link_type"),
  "xwalk_altos_to_parcel"
)
xwalk_altos[, PID := normalize_pid(fcoalesce(anchor_pid, parcel_number))]
xwalk_pid_cong[, PID := normalize_pid(PID)]
bldg[, PID := normalize_pid(PID)]
parcels[, PID := normalize_pid(parcel_number)]
assert_unique(xwalk_pid_cong, c("PID", "year"), "xwalk_pid_conglomerate PID-year")
assert_unique(parcels, "PID", "parcels_clean PID")

pid_building_type <- bldg[
  year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX & !is.na(building_type),
  .(building_type = mode_char(building_type)),
  by = PID
]

logf("  altos_clean rows: ", nrow(altos), log_file = log_file)
logf("  xwalk mode: phase1_winner_xwalk", log_file = log_file)
assert_unique(xwalk_altos, "source_address_id", "xwalk_altos_to_parcel (source_address_id)")
altos[, source_address_id := as.character(pm.uid)]
altos_matched <- merge(
  altos,
  xwalk_altos[, .(
    source_address_id = as.character(source_address_id),
    PID,
    altos_link_type = as.character(link_type),
    altos_xwalk_status = as.character(xwalk_status),
    altos_link_id = as.character(link_id)
  )],
  by = "source_address_id",
  all.x = TRUE,
  all.y = FALSE
)
altos_matched[, altos_pid_source := fifelse(!is.na(PID), "phase1_xwalk", "unmatched")]
altos_matched <- altos_matched[!is.na(PID)]
assert_unique(altos_matched, "listing_id", "altos_matched")
logf("  matched Altos listings: ", nrow(altos_matched), log_file = log_file)
logf(
  "  Altos PID sources: ",
  paste(
    altos_matched[, .N, by = altos_pid_source][order(altos_pid_source)][
      , paste0(altos_pid_source, "=", N)
    ],
    collapse = "; "
  ),
  log_file = log_file
)

altos_matched <- merge(
  altos_matched,
  xwalk_pid_cong,
  by = c("PID", "year"),
  all.x = TRUE
)
logf(
  "  matched listings with conglomerate_id: ",
  altos_matched[!is.na(conglomerate_id), .N],
  " / ",
  nrow(altos_matched),
  log_file = log_file
)

cong_cat_modal <- xwalk_ent_cong[!is.na(owner_category),
  .N, by = .(conglomerate_id, owner_category)
][order(-N)][, .SD[1L], by = conglomerate_id][
  , .(conglomerate_id, buyer_owner_category = owner_category)
]

pid_rate_pre <- bldg[
  year >= ANALYSIS_YEAR_MIN &
    year <= ANALYSIS_YEAR_MAX &
    !is.na(total_units) &
    total_units > 0 &
    !is.na(filing_rate_eb_pre_covid),
  .(
    total_units = max(total_units, na.rm = TRUE),
    filing_rate_eb_pre_covid = as.numeric(stats::median(filing_rate_eb_pre_covid, na.rm = TRUE))
  ),
  by = PID
]
assert_unique(pid_rate_pre, "PID", "pid_rate_pre")

pid_cong_unique <- unique(
  xwalk_pid_cong[
    year >= ANALYSIS_YEAR_MIN &
      year <= ANALYSIS_YEAR_MAX &
      !is.na(conglomerate_id),
    .(PID, conglomerate_id)
  ],
  by = c("PID", "conglomerate_id")
)
pid_cong_rate <- merge(
  pid_cong_unique,
  pid_rate_pre,
  by = "PID",
  all.x = FALSE,
  all.y = FALSE
)
assert_unique(pid_cong_rate, c("PID", "conglomerate_id"), "pid_cong_rate")

cong_full <- pid_cong_rate[, .(
  cong_units_full = sum(total_units, na.rm = TRUE),
  cong_rate_num_full = sum(total_units * filing_rate_eb_pre_covid, na.rm = TRUE)
), by = conglomerate_id]

pid_cong_full <- pid_cong_rate[, .(
  pid_units_cong = sum(total_units, na.rm = TRUE),
  pid_rate_num_cong = sum(total_units * filing_rate_eb_pre_covid, na.rm = TRUE)
), by = .(PID, conglomerate_id)]

loo_type <- merge(pid_cong_full, cong_full, by = "conglomerate_id", all.x = TRUE)
loo_type[, other_units_transfer_year := cong_units_full - pid_units_cong]
loo_type[, acq_rate := fifelse(
  other_units_transfer_year > 0,
  (cong_rate_num_full - pid_rate_num_cong) / other_units_transfer_year,
  NA_real_
)]
loo_type[, portfolio_evict_group_cong := fcase(
  is.na(acq_rate) | other_units_transfer_year == 0, "Solo",
  other_units_transfer_year > 0 & other_units_transfer_year < LOO_MIN_UNIT_YEARS, "Small portfolio",
  other_units_transfer_year >= LOO_MIN_UNIT_YEARS & acq_rate >= ACQ_HIGH_FILER_THRESHOLD, "High-evicting portfolio",
  other_units_transfer_year >= LOO_MIN_UNIT_YEARS & acq_rate < ACQ_HIGH_FILER_THRESHOLD, "Low-evicting portfolio",
  default = "Low-evicting portfolio"
)]
loo_type[, acq_filer_bin := fcase(
  portfolio_evict_group_cong == "High-evicting portfolio", ACQ_BIN_LABELS[1],
  portfolio_evict_group_cong == "Low-evicting portfolio", ACQ_BIN_LABELS[2],
  portfolio_evict_group_cong == "Small portfolio", ACQ_BIN_LABELS[3],
  default = ACQ_BIN_LABELS[4]
)]
loo_type <- loo_type[, .(
  PID,
  conglomerate_id,
  other_units_transfer_year,
  acq_rate,
  portfolio_evict_group_cong,
  acq_filer_bin
)]

altos_matched <- merge(
  altos_matched,
  loo_type[, .(PID, conglomerate_id, acq_filer_bin, acq_rate, other_units_transfer_year)],
  by = c("PID", "conglomerate_id"),
  all.x = TRUE
)
altos_matched <- merge(
  altos_matched,
  owner_portfolio,
  by = "conglomerate_id",
  all.x = TRUE
)
altos_matched <- merge(
  altos_matched,
  cong_cat_modal,
  by = "conglomerate_id",
  all.x = TRUE
)
altos_matched <- merge(
  altos_matched,
  parcels[, .(PID, parcel_address = n_sn_ss_c, owner_1, building_code_description_new)],
  by = "PID",
  all.x = TRUE
)
altos_matched <- merge(
  altos_matched,
  pid_building_type,
  by = "PID",
  all.x = TRUE
)

altos_matched[, property_name := as.character(property_name)]
altos_matched[, property_name_nonblank := !is.na(property_name) & nzchar(str_squish(property_name))]
altos_matched[, text_window_usable := year <= TEXT_YEAR_MAX & property_name_nonblank]

regex_section8 <- regex("section ?8|voucher", ignore_case = TRUE)
regex_no_deposit <- regex("no.+deposit|deposit free|zero deposit|no security deposit", ignore_case = TRUE)
regex_eviction <- regex("evict|eviction", ignore_case = TRUE)
regex_conviction <- regex("convict|felon|criminal|background", ignore_case = TRUE)

altos_matched[, `:=`(
  flag_section8_voucher = property_name_nonblank & str_detect(property_name, regex_section8),
  flag_no_deposit = property_name_nonblank & str_detect(property_name, regex_no_deposit),
  flag_eviction_text = property_name_nonblank & str_detect(property_name, regex_eviction),
  flag_conviction_text = property_name_nonblank & str_detect(property_name, regex_conviction)
)]
altos_matched[, `:=`(
  flag_access_friendly_any = flag_section8_voucher | flag_no_deposit,
  flag_screening_restrictive_any = flag_eviction_text | flag_conviction_text
)]
altos_matched[, flag_any_screening_text := flag_access_friendly_any | flag_screening_restrictive_any]

text_examples <- altos_matched[
  text_window_usable == TRUE & flag_any_screening_text == TRUE,
  .(
    listings = .N,
    years = paste(sort(unique(year)), collapse = ","),
    sample_type = mode_char(fifelse(flag_section8_voucher, "section8_voucher",
                                    fifelse(flag_no_deposit, "no_deposit",
                                            fifelse(flag_eviction_text, "eviction_text", "conviction_text"))))
  ),
  by = property_name
][order(-listings, property_name)]

listing_summary_bin <- altos_matched[, .(
  listings_matched = .N,
  listings_with_conglomerate = sum(!is.na(conglomerate_id)),
  listings_classified = sum(!is.na(acq_filer_bin)),
  unique_pids = uniqueN(PID),
  unique_conglomerates = uniqueN(conglomerate_id[!is.na(conglomerate_id)]),
  text_window_listings = sum(text_window_usable),
  share_text_window = mean(text_window_usable),
  share_section8_voucher = mean(flag_section8_voucher[text_window_usable]),
  share_no_deposit = mean(flag_no_deposit[text_window_usable]),
  share_eviction_text = mean(flag_eviction_text[text_window_usable]),
  share_conviction_text = mean(flag_conviction_text[text_window_usable]),
  share_access_friendly_any = mean(flag_access_friendly_any[text_window_usable]),
  share_screening_restrictive_any = mean(flag_screening_restrictive_any[text_window_usable]),
  share_any_screening_text = mean(flag_any_screening_text[text_window_usable])
), by = acq_filer_bin][order(match(acq_filer_bin, ACQ_BIN_LABELS))]

portfolio_level <- altos_matched[
  !is.na(conglomerate_id) & text_window_usable == TRUE,
  .(
    acq_filer_bin = mode_char(acq_filer_bin),
    buyer_owner_category = mode_char(buyer_owner_category),
    n_properties_total = {
      vals <- n_properties_total[!is.na(n_properties_total)]
      if (length(vals)) as.numeric(max(vals)) else NA_real_
    },
    listings = .N,
    unique_pids = uniqueN(PID),
    share_section8_voucher = mean(flag_section8_voucher),
    share_no_deposit = mean(flag_no_deposit),
    share_eviction_text = mean(flag_eviction_text),
    share_conviction_text = mean(flag_conviction_text),
    share_access_friendly_any = mean(flag_access_friendly_any),
    share_screening_restrictive_any = mean(flag_screening_restrictive_any),
    share_any_screening_text = mean(flag_any_screening_text)
  ),
  by = conglomerate_id
]

portfolio_bin_summary <- portfolio_level[!is.na(acq_filer_bin), .(
  portfolios = .N,
  listings = sum(listings),
  properties = sum(unique_pids),
  mean_portfolio_section8 = mean(share_section8_voucher),
  mean_portfolio_no_deposit = mean(share_no_deposit),
  mean_portfolio_restrictive = mean(share_screening_restrictive_any),
  mean_portfolio_any = mean(share_any_screening_text),
  weighted_section8 = weighted.mean(share_section8_voucher, w = listings),
  weighted_no_deposit = weighted.mean(share_no_deposit, w = listings),
  weighted_restrictive = weighted.mean(share_screening_restrictive_any, w = listings),
  weighted_any = weighted.mean(share_any_screening_text, w = listings)
), by = acq_filer_bin][order(match(acq_filer_bin, ACQ_BIN_LABELS))]

building_level <- altos_matched[
  !is.na(PID),
  .(
    conglomerate_id = mode_char(as.character(conglomerate_id)),
    acq_filer_bin = mode_char(acq_filer_bin),
    buyer_owner_category = mode_char(buyer_owner_category),
    parcel_address = mode_char(parcel_address),
    owner_1 = mode_char(owner_1),
    building_type = mode_char(building_type),
    listings = .N,
    years_seen = paste(sort(unique(year)), collapse = ","),
    text_window_listings = sum(text_window_usable),
    share_section8_voucher = mean(flag_section8_voucher[text_window_usable]),
    share_no_deposit = mean(flag_no_deposit[text_window_usable]),
    share_eviction_text = mean(flag_eviction_text[text_window_usable]),
    share_conviction_text = mean(flag_conviction_text[text_window_usable]),
    share_any_screening_text = mean(flag_any_screening_text[text_window_usable]),
    median_price = {
      vals <- as.numeric(price[!is.na(price)])
      if (length(vals)) stats::median(vals) else NA_real_
    }
  ),
  by = PID
]

pid_units <- bldg[
  year >= ANALYSIS_YEAR_MIN & year <= ANALYSIS_YEAR_MAX,
  .(max_total_units = max(total_units, na.rm = TRUE)),
  by = PID
]

pid_cong_coverage <- merge(
  loo_type[, .(PID, conglomerate_id, acq_filer_bin, acq_rate, other_units_transfer_year)],
  altos_matched[, .(
    altos_listings = .N,
    altos_text_window_listings = sum(text_window_usable),
    altos_any_screening_text = any(flag_any_screening_text, na.rm = TRUE),
    first_altos_year = min(year, na.rm = TRUE),
    last_altos_year = max(year, na.rm = TRUE)
  ), by = .(PID, conglomerate_id)],
  by = c("PID", "conglomerate_id"),
  all.x = TRUE
)
pid_cong_coverage <- merge(pid_cong_coverage, pid_units, by = "PID", all.x = TRUE)
pid_cong_coverage <- merge(pid_cong_coverage, pid_building_type, by = "PID", all.x = TRUE)
pid_cong_coverage <- merge(
  pid_cong_coverage,
  parcels[, .(PID, parcel_address = n_sn_ss_c, owner_1)],
  by = "PID",
  all.x = TRUE
)
pid_cong_coverage[is.na(altos_listings), `:=`(
  altos_listings = 0L,
  altos_text_window_listings = 0L,
  altos_any_screening_text = FALSE
)]
pid_cong_coverage[, ever_in_altos := altos_listings > 0L]

pid_cong_coverage_summary <- pid_cong_coverage[, .(
  pid_cong_pairs = .N,
  pids = uniqueN(PID),
  pairs_ever_in_altos = sum(ever_in_altos),
  share_pairs_ever_in_altos = mean(ever_in_altos),
  units_total = sum(max_total_units, na.rm = TRUE),
  units_on_pairs_ever_in_altos = sum(fifelse(ever_in_altos, max_total_units, 0), na.rm = TRUE)
), by = acq_filer_bin][order(match(acq_filer_bin, ACQ_BIN_LABELS))]
pid_cong_coverage_summary[, share_units_ever_in_altos := fifelse(
  units_total > 0,
  units_on_pairs_ever_in_altos / units_total,
  NA_real_
)]

high_filer_never_in_altos <- pid_cong_coverage[
  acq_filer_bin == ACQ_BIN_LABELS[1] & ever_in_altos == FALSE
][order(-max_total_units, -acq_rate, PID)]

pid_any_altos <- unique(altos_matched[, .(PID, ever_in_altos_any = TRUE)], by = "PID")
pid_any_high <- unique(
  pid_cong_coverage[acq_filer_bin == ACQ_BIN_LABELS[1], .(
    PID, max_total_units, parcel_address, owner_1, building_type
  )],
  by = "PID"
)
pid_any_high <- merge(pid_any_high, pid_any_altos, by = "PID", all.x = TRUE)
pid_any_high[is.na(ever_in_altos_any), ever_in_altos_any := FALSE]
high_filer_pid_summary <- pid_any_high[, .(
  high_filer_pids = .N,
  high_filer_pids_ever_in_altos = sum(ever_in_altos_any),
  share_high_filer_pids_ever_in_altos = mean(ever_in_altos_any),
  units_total = sum(max_total_units, na.rm = TRUE),
  units_on_pids_ever_in_altos = sum(fifelse(ever_in_altos_any, max_total_units, 0), na.rm = TRUE)
)]
high_filer_pid_summary[, share_units_on_pids_ever_in_altos := fifelse(
  units_total > 0,
  units_on_pids_ever_in_altos / units_total,
  NA_real_
)]
high_filer_pids_never_in_altos <- pid_any_high[ever_in_altos_any == FALSE][order(-max_total_units, PID)]

screening_year_summary <- altos_matched[
  text_window_usable == TRUE,
  .(
    listings = .N,
    share_section8_voucher = mean(flag_section8_voucher),
    share_no_deposit = mean(flag_no_deposit),
    share_eviction_text = mean(flag_eviction_text),
    share_conviction_text = mean(flag_conviction_text),
    share_any_screening_text = mean(flag_any_screening_text)
  ),
  by = .(year, acq_filer_bin)
][order(year, match(acq_filer_bin, ACQ_BIN_LABELS))]

listing_text_availability <- altos_matched[, .(
  listings = .N,
  property_name_nonblank = sum(property_name_nonblank),
  share_property_name_nonblank = mean(property_name_nonblank),
  text_window_usable = sum(text_window_usable)
), by = year][order(year)]

fwrite(listing_summary_bin, file.path(qa_dir, "altos_listing_summary_by_acq_filer_bin.csv"))
fwrite(portfolio_level, file.path(qa_dir, "altos_portfolio_screening_panel.csv"))
fwrite(portfolio_bin_summary, file.path(qa_dir, "altos_portfolio_screening_summary.csv"))
fwrite(building_level, file.path(qa_dir, "altos_building_screening_panel.csv"))
fwrite(pid_cong_coverage, file.path(qa_dir, "altos_pid_conglomerate_coverage.csv"))
fwrite(pid_cong_coverage_summary, file.path(qa_dir, "altos_pid_conglomerate_coverage_summary.csv"))
fwrite(high_filer_never_in_altos, file.path(qa_dir, "high_filer_pid_conglomerate_never_in_altos.csv"))
fwrite(high_filer_pid_summary, file.path(qa_dir, "high_filer_pid_altos_coverage_summary.csv"))
fwrite(high_filer_pids_never_in_altos, file.path(qa_dir, "high_filer_pids_never_in_altos.csv"))
fwrite(screening_year_summary, file.path(qa_dir, "altos_screening_year_summary.csv"))
fwrite(listing_text_availability, file.path(qa_dir, "altos_property_name_availability_by_year.csv"))
fwrite(text_examples, file.path(qa_dir, "altos_property_name_screening_examples.csv"))

logf("Wrote outputs to ", qa_dir, log_file = log_file)
logf("  matched Altos listings: ", nrow(altos_matched), log_file = log_file)
logf("  classified matched listings: ", altos_matched[!is.na(acq_filer_bin), .N], log_file = log_file)
logf("  high-filer PID-conglomerate pairs: ", pid_cong_coverage[acq_filer_bin == ACQ_BIN_LABELS[1], .N], log_file = log_file)
logf("  high-filer PID-conglomerate pairs never in Altos: ", nrow(high_filer_never_in_altos), log_file = log_file)
logf("=== Finished qa-altos-landlord-screening.R ===", log_file = log_file)
