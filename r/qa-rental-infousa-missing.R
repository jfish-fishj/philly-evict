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
    parts <- strsplit(arg, "=", fixed = TRUE)[[1L]]
    key <- gsub("-", "_", parts[1L])
    val <- if (length(parts) >= 2L) paste(parts[-1L], collapse = "=") else "TRUE"
    out[[key]] <- val
  }
  out
}

normalize_pid <- function(x) str_pad(as.character(x), width = 9L, side = "left", pad = "0")

clean_zip5 <- function(x) {
  z <- as.character(x)
  z <- str_sub(z, 1L, 5L)
  z <- str_remove(z, "^_")
  z <- str_pad(z, 5L, side = "left", pad = "0")
  z[!nzchar(z) | z == "00000"] <- NA_character_
  z
}

clean_key <- function(x) {
  y <- str_to_lower(str_squish(as.character(x)))
  y[!nzchar(y)] <- NA_character_
  y
}

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))
tag <- opts$tag %||% format(Sys.time(), "%Y%m%d_%H%M%S")
log_file <- p_out(cfg, "logs", "qa-rental-infousa-missing.log")

logf("=== Starting qa-rental-infousa-missing.R ===", log_file = log_file)
logf("Tag: ", tag, log_file = log_file)

bldg_path <- p_product(cfg, "bldg_panel_blp")
parcels_path <- p_product(cfg, "parcels_clean")
infousa_path <- p_product(cfg, "infousa_clean")
xwalk_path <- p_product(cfg, "xwalk_infousa_to_parcel")

bldg <- fread(
  bldg_path,
  select = c(
    "PID", "year", "building_type", "num_units_bin",
    "num_households", "infousa_num_households",
    "ever_rental_any_year", "rental_from_altos", "rental_from_license", "rental_from_evict"
  )
)
setDT(bldg)
bldg[, PID := normalize_pid(PID)]
bldg[, year := as.integer(year)]
bldg[, rental_row := as.logical(
  fcoalesce(ever_rental_any_year, FALSE) |
  fcoalesce(rental_from_altos, FALSE) |
  fcoalesce(rental_from_license, FALSE) |
  fcoalesce(rental_from_evict, FALSE)
)]

rental_year <- bldg[rental_row == TRUE, .(n_rental = .N), by = year]
missing_year <- bldg[rental_row == TRUE & is.na(num_households), .(n_missing = .N), by = year]
missing_year <- merge(rental_year, missing_year, by = "year", all.x = TRUE)
missing_year[is.na(n_missing), n_missing := 0L]
missing_year[, share_missing := n_missing / pmax(1, n_rental)]

miss <- bldg[rental_row == TRUE & is.na(num_households), .(
  PID, year, building_type, num_units_bin,
  rental_from_altos, rental_from_license, rental_from_evict,
  infousa_num_households
)]
if (nrow(miss) == 0L) {
  stop("No missing rental PID-year rows found in bldg_panel_blp.")
}

pid_obs <- bldg[, .(
  pid_has_any_hh_obs = any(!is.na(num_households)),
  pid_has_pre2022_hh_obs = any(year <= 2021L & !is.na(num_households)),
  pid_first_obs_year = if (any(!is.na(num_households))) min(year[!is.na(num_households)]) else NA_integer_,
  pid_last_obs_year = if (any(!is.na(num_households))) max(year[!is.na(num_households)]) else NA_integer_
), by = PID]
setkey(pid_obs, PID)

pid_miss <- unique(miss[, .(PID)])
setkey(pid_miss, PID)

parcels <- fread(parcels_path, select = c("parcel_number", "n_sn_ss_c", "pm.house", "pm.street", "pm.streetSuf", "pm.zip"))
setDT(parcels)
parcels[, PID := normalize_pid(parcel_number)]
pid_addr <- unique(parcels[, .(
  PID,
  n_sn_ss_c = clean_key(n_sn_ss_c),
  pm.house = as.numeric(pm.house),
  street_norm = clean_key(pm.street),
  suffix_norm = clean_key(pm.streetSuf),
  zip_norm = clean_zip5(pm.zip)
)], by = "PID")
setkey(pid_addr, PID)

inf <- fread(infousa_path, select = c("n_sn_ss_c", "pm.house", "pm.street", "pm.streetSuf", "pm.zip"))
setDT(inf)
inf_addr <- unique(inf[, .(
  n_sn_ss_c = clean_key(n_sn_ss_c),
  house_norm = as.numeric(pm.house),
  street_norm = clean_key(pm.street),
  suffix_norm = clean_key(pm.streetSuf),
  zip_norm = clean_zip5(pm.zip)
)], by = c("n_sn_ss_c", "house_norm", "street_norm", "suffix_norm", "zip_norm"))
inf_addr <- inf_addr[
  !is.na(street_norm) & !is.na(suffix_norm) & !is.na(zip_norm)
]

xwalk <- fread(xwalk_path, select = c("parcel_number", "n_sn_ss_c", "match_tier", "xwalk_status", "link_type"))
setDT(xwalk)
xwalk[, PID := normalize_pid(parcel_number)]
xwalk_pid <- unique(xwalk[!is.na(PID), .(PID)], by = "PID")
xwalk_addr <- unique(xwalk[!is.na(n_sn_ss_c), .(n_sn_ss_c = clean_key(n_sn_ss_c))], by = "n_sn_ss_c")
setkey(xwalk_pid, PID)

pid_diag <- copy(pid_miss)
pid_diag[pid_addr, `:=`(
  n_sn_ss_c = i.n_sn_ss_c,
  house_norm = i.pm.house,
  street_norm = i.street_norm,
  suffix_norm = i.suffix_norm,
  zip_norm = i.zip_norm
), on = "PID"]
pid_diag[pid_obs, `:=`(
  pid_has_any_hh_obs = i.pid_has_any_hh_obs,
  pid_has_pre2022_hh_obs = i.pid_has_pre2022_hh_obs,
  pid_first_obs_year = i.pid_first_obs_year,
  pid_last_obs_year = i.pid_last_obs_year
), on = "PID"]
pid_diag[xwalk_pid, pid_has_xwalk_pid := TRUE, on = "PID"]
pid_diag[is.na(pid_has_xwalk_pid), pid_has_xwalk_pid := FALSE]
pid_diag[, addr_in_infousa_exact := !is.na(n_sn_ss_c) & (n_sn_ss_c %chin% inf_addr$n_sn_ss_c)]
pid_diag[, addr_in_xwalk_key := !is.na(n_sn_ss_c) & (n_sn_ss_c %chin% xwalk_addr$n_sn_ss_c)]

near_input <- pid_diag[
  !is.na(house_norm) & !is.na(street_norm) & !is.na(suffix_norm) & !is.na(zip_norm),
  .(PID, house_norm, street_norm, suffix_norm, zip_norm)
]
near_join <- merge(
  near_input,
  inf_addr[, .(house_norm_inf = house_norm, street_norm, suffix_norm, zip_norm)],
  by = c("street_norm", "suffix_norm", "zip_norm"),
  all.x = TRUE,
  allow.cartesian = TRUE
)
near_join <- near_join[!is.na(house_norm_inf)]
if (nrow(near_join) > 0L) {
  near_join[, near_house_diff := abs(house_norm - house_norm_inf)]
  near_pid <- near_join[, .(
    near_house_diff_min = min(near_house_diff, na.rm = TRUE),
    near_house_candidate_n = .N
  ), by = PID]
} else {
  near_pid <- data.table(PID = character(), near_house_diff_min = numeric(), near_house_candidate_n = integer())
}
setkey(near_pid, PID)
pid_diag[near_pid, `:=`(
  near_house_diff_min = i.near_house_diff_min,
  near_house_candidate_n = i.near_house_candidate_n
), on = "PID"]
pid_diag[is.na(near_house_candidate_n), near_house_candidate_n := 0L]
pid_diag[, near_match_off_by_number_pid := !is.na(near_house_diff_min) & near_house_diff_min > 0 & near_house_diff_min <= 10]

miss[pid_diag, `:=`(
  n_sn_ss_c = i.n_sn_ss_c,
  pid_has_xwalk_pid = i.pid_has_xwalk_pid,
  addr_in_infousa_exact = i.addr_in_infousa_exact,
  addr_in_xwalk_key = i.addr_in_xwalk_key,
  near_house_diff_min = i.near_house_diff_min,
  near_house_candidate_n = i.near_house_candidate_n,
  near_match_off_by_number_pid = i.near_match_off_by_number_pid,
  pid_has_any_hh_obs = i.pid_has_any_hh_obs,
  pid_has_pre2022_hh_obs = i.pid_has_pre2022_hh_obs,
  pid_first_obs_year = i.pid_first_obs_year,
  pid_last_obs_year = i.pid_last_obs_year
), on = "PID"]

miss[, coverage_bucket := fifelse(
  year >= 2022L & pid_has_pre2022_hh_obs %in% TRUE,
  "expected_recent_gap",
  fifelse(
    addr_in_infousa_exact %in% TRUE & pid_has_xwalk_pid == FALSE,
    "present_in_infousa_but_unlinked",
    fifelse(
      addr_in_infousa_exact %in% FALSE & near_match_off_by_number_pid %in% TRUE,
      "near_match_off_by_number",
      fifelse(
        addr_in_infousa_exact %in% FALSE & near_match_off_by_number_pid %in% FALSE & pid_has_xwalk_pid == FALSE,
        "likely_not_in_infousa_or_bad_address",
        "temporal_gap_or_partial_coverage"
      )
    )
  )
)]

miss[, evidence_combo := paste0(
  fifelse(rental_from_license %in% TRUE, "L", "-"),
  fifelse(rental_from_altos %in% TRUE, "A", "-"),
  fifelse(rental_from_evict %in% TRUE, "E", "-")
)]

year_type_den <- bldg[rental_row == TRUE, .(n_rental = .N), by = .(year, building_type, num_units_bin)]
year_type_miss <- miss[, .(n_missing = .N), by = .(year, building_type, num_units_bin)]
year_type <- merge(year_type_den, year_type_miss, by = c("year", "building_type", "num_units_bin"), all.x = TRUE)
year_type[is.na(n_missing), n_missing := 0L]
year_type[, share_missing := n_missing / pmax(1, n_rental)]

bucket_summary <- miss[, .(n_missing = .N), by = coverage_bucket][order(-n_missing)]
bucket_year <- miss[, .(n_missing = .N), by = .(year, coverage_bucket)][order(year, -n_missing)]
bucket_evidence <- miss[, .(n_missing = .N), by = .(evidence_combo, coverage_bucket)][order(-n_missing)]

sample_rows <- miss[
  ,
  .SD[seq_len(min(.N, 25L))],
  by = coverage_bucket
][order(coverage_bucket, PID, year)]

out_prefix <- function(name) p_out(cfg, "qa", paste0(name, "_", tag, ".csv"))

fwrite(missing_year, out_prefix("rental_infousa_missing_summary_year"))
fwrite(year_type, out_prefix("rental_infousa_missing_summary_year_type"))
fwrite(bucket_summary, out_prefix("rental_infousa_missing_summary_bucket"))
fwrite(bucket_year, out_prefix("rental_infousa_missing_summary_year_bucket"))
fwrite(bucket_evidence, out_prefix("rental_infousa_missing_summary_evidence_bucket"))
fwrite(miss[order(year, PID)], out_prefix("rental_infousa_missing_pid_year_classification"))
fwrite(sample_rows, out_prefix("rental_infousa_missing_pid_year_sample_25_per_bucket"))

logf("Wrote year summary: ", out_prefix("rental_infousa_missing_summary_year"), log_file = log_file)
logf("Wrote year/type summary: ", out_prefix("rental_infousa_missing_summary_year_type"), log_file = log_file)
logf("Wrote bucket summary: ", out_prefix("rental_infousa_missing_summary_bucket"), log_file = log_file)
logf("Wrote year/bucket summary: ", out_prefix("rental_infousa_missing_summary_year_bucket"), log_file = log_file)
logf("Wrote evidence/bucket summary: ", out_prefix("rental_infousa_missing_summary_evidence_bucket"), log_file = log_file)
logf("Wrote PID-year classification rows: ", out_prefix("rental_infousa_missing_pid_year_classification"), log_file = log_file)
logf("Wrote PID-year bucket sample: ", out_prefix("rental_infousa_missing_pid_year_sample_25_per_bucket"), log_file = log_file)
logf("=== Finished qa-rental-infousa-missing.R ===", log_file = log_file)
