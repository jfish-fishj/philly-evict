## ============================================================
## build-infousa-building-income-proxy.R
## ============================================================
## Purpose: Build a PID x year InfoUSA income-proxy panel for use
## in outer-sorting empirical targets.
##
## Default proxy:
##   - primary-family households from infousa_clean
##   - income field: find_div_1000
##   - composition proxy: share of linked households with
##     find_div_1000 <= low_income_threshold_k
## ============================================================

normalize_pid <- function(x) {
  if (!requireNamespace("stringr", quietly = TRUE)) stop("Missing package: stringr")
  stringr::str_pad(as.character(x), width = 9L, side = "left", pad = "0")
}

normalize_addr_key <- function(x) {
  if (!requireNamespace("stringr", quietly = TRUE)) stop("Missing package: stringr")
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- stringr::str_squish(stringr::str_to_lower(x))
  x[!nzchar(x)] <- NA_character_
  x
}

first_nonmissing_chr <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (!length(x)) return(NA_character_)
  x[1L]
}

build_infousa_building_income_proxy <- function(cfg, log_file = NULL) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")

  settings <- cfg$run$outer_sorting$income_proxy %||% list()
  infousa_key <- as.character(settings$infousa_product_key %||% "infousa_clean")
  xwalk_key <- as.character(settings$xwalk_product_key %||% "xwalk_infousa_to_parcel")
  income_col <- as.character(settings$income_col %||% "find_div_1000")
  address_col <- as.character(settings$address_col %||% "n_sn_ss_c")
  household_id_col <- as.character(settings$household_id_col %||% "familyid")
  primary_flag_col <- as.character(settings$primary_flag_col %||% "primary_family_ind")
  year_col <- as.character(settings$year_col %||% "year")
  year_from <- as.integer(settings$year_from %||% cfg$run$outer_sorting$empirical$year_from %||% 2011L)
  year_to <- as.integer(settings$year_to %||% cfg$run$outer_sorting$empirical$year_to %||% 2019L)
  low_income_threshold_k <- as.numeric(settings$low_income_threshold_k %||% 30)

  infousa_path <- p_product(cfg, infousa_key)
  xwalk_path <- p_product(cfg, xwalk_key)

  inf_need <- c(household_id_col, year_col, primary_flag_col, address_col, income_col)
  xwalk_need <- c("source", "n_sn_ss_c", "parcel_number", "xwalk_status", "ownership_unsafe")

  inf <- data.table::fread(infousa_path, select = inf_need)
  xwalk <- data.table::fread(xwalk_path, select = xwalk_need)

  assert_has_cols(inf, inf_need, "infousa_clean income-proxy input")
  assert_has_cols(xwalk, xwalk_need, "xwalk_infousa_to_parcel income-proxy input")

  inf <- data.table::as.data.table(inf)
  data.table::setnames(
    inf,
    old = c(household_id_col, year_col, primary_flag_col, address_col, income_col),
    new = c("familyid", "year", "primary_family_ind", "n_sn_ss_c", "income_k")
  )
  inf[, `:=`(
    familyid = as.character(familyid),
    year = as.integer(year),
    primary_family_ind = as.integer(primary_family_ind),
    n_sn_ss_c = normalize_addr_key(n_sn_ss_c),
    income_k = as.numeric(income_k)
  )]
  inf <- inf[primary_family_ind == 1L]
  inf <- inf[year >= year_from & year <= year_to]
  inf <- inf[!is.na(familyid) & nzchar(familyid) & !is.na(year)]

  hh_dup_n <- inf[, .N, by = .(familyid, year)][N > 1L, .N]
  if (!is.null(log_file) && exists("logf", mode = "function") && hh_dup_n > 0L) {
    logf("build_infousa_building_income_proxy: collapsing ", hh_dup_n,
         " duplicated familyid-year keys in infousa_clean.", log_file = log_file)
  }
  if (hh_dup_n > 0L) {
    data.table::setorder(inf, familyid, year, n_sn_ss_c)
    inf <- inf[, .(
      n_sn_ss_c = first_nonmissing_chr(n_sn_ss_c),
      income_k = income_k[which.max(!is.na(income_k))]
    ), by = .(familyid, year)]
  } else {
    inf <- inf[, .(familyid, year, n_sn_ss_c, income_k)]
  }

  inf <- inf[!is.na(n_sn_ss_c) & !is.na(income_k)]
  if (nrow(inf) == 0L) stop("No InfoUSA primary-household rows remain after filtering for income proxy.")
  assert_unique(inf, c("familyid", "year"), "InfoUSA primary-household income proxy rows")

  addr_year <- inf[, .(
    n_households = .N,
    n_low_income = sum(income_k <= low_income_threshold_k, na.rm = TRUE),
    income_sum_k = sum(income_k, na.rm = TRUE)
  ), by = .(n_sn_ss_c, year)]
  rm(inf)

  xwalk <- data.table::as.data.table(xwalk)
  xwalk <- xwalk[source == "infousa"]
  xwalk[, `:=`(
    n_sn_ss_c = normalize_addr_key(n_sn_ss_c),
    PID = normalize_pid(parcel_number)
  )]
  xwalk <- xwalk[!is.na(n_sn_ss_c) & !is.na(parcel_number)]
  xwalk_addr_stats <- xwalk[, .(n_pid = uniqueN(PID)), by = n_sn_ss_c]
  n_ambig_addr <- xwalk_addr_stats[n_pid > 1L, .N]
  if (!is.null(log_file) && exists("logf", mode = "function") && n_ambig_addr > 0L) {
    logf("build_infousa_building_income_proxy: dropping ", n_ambig_addr,
         " address keys with >1 matched PID in xwalk_infousa_to_parcel.", log_file = log_file)
  }
  keep_addr <- xwalk_addr_stats[n_pid == 1L, .(n_sn_ss_c)]
  xwalk <- xwalk[keep_addr, on = "n_sn_ss_c", nomatch = 0L]
  data.table::setorder(xwalk, n_sn_ss_c, xwalk_status)
  xwalk <- xwalk[, .(
    PID = PID[1L],
    xwalk_status = first_nonmissing_chr(xwalk_status),
    ownership_unsafe = any(as.logical(ownership_unsafe), na.rm = TRUE)
  ), by = n_sn_ss_c]
  assert_unique(xwalk, "n_sn_ss_c", "InfoUSA address-to-PID income proxy xwalk")

  n_addr_year_before <- nrow(addr_year)
  addr_year <- merge(addr_year, xwalk, by = "n_sn_ss_c", all.x = TRUE)
  matched_share <- mean(!is.na(addr_year$PID))
  if (!is.null(log_file) && exists("logf", mode = "function")) {
    logf("build_infousa_building_income_proxy: matched ", sprintf("%.3f", matched_share),
         " of primary-household address-year rows to a unique PID.", log_file = log_file)
  }
  addr_year <- addr_year[!is.na(PID)]
  if (nrow(addr_year) == 0L) stop("No linked InfoUSA primary-household rows remain after address-to-PID merge.")

  years_all <- sort(unique(addr_year$year))
  proxy_list <- vector("list", length(years_all))
  for (i in seq_along(years_all)) {
    yy <- years_all[[i]]
    addr_y <- addr_year[year == yy]
    proxy_list[[i]] <- addr_y[, .(
      infousa_num_households_income_proxy = sum(n_households, na.rm = TRUE),
      infousa_find_mean_k = sum(income_sum_k, na.rm = TRUE) / sum(n_households, na.rm = TRUE),
      infousa_find_low_income_share = sum(n_low_income, na.rm = TRUE) / sum(n_households, na.rm = TRUE),
      infousa_income_proxy_any_ownership_unsafe = any(as.logical(ownership_unsafe), na.rm = TRUE),
      infousa_income_proxy_xwalk_status_mode = {
        xs <- xwalk_status[!is.na(xwalk_status) & nzchar(as.character(xwalk_status))]
        if (!length(xs)) NA_character_ else names(sort(table(xs), decreasing = TRUE))[1L]
      }
    ), by = .(PID, year)]
    if (!is.null(log_file) && exists("logf", mode = "function") && ((i %% 5L) == 0L || i == length(years_all))) {
      logf("build_infousa_building_income_proxy: aggregated year ", yy, " (", i, "/", length(years_all), ").",
           log_file = log_file)
    }
  }
  proxy <- data.table::rbindlist(proxy_list, use.names = TRUE, fill = TRUE)

  proxy[, `:=`(
    infousa_income_proxy_source = sprintf("%s_primary_family_low_income_share_le_%sk", income_col, format(low_income_threshold_k, trim = TRUE)),
    infousa_income_proxy_threshold_k = low_income_threshold_k
  )]

  assert_unique(proxy, c("PID", "year"), "infousa building income proxy panel")

  if (!is.null(log_file) && exists("logf", mode = "function")) {
    logf(
      "build_infousa_building_income_proxy: output rows=", nrow(proxy),
      ", unique_pids=", data.table::uniqueN(proxy$PID),
      ", mean_low_income_share=", sprintf("%.3f", mean(proxy$infousa_find_low_income_share, na.rm = TRUE)),
      ", address_year_rows_in=", n_addr_year_before,
      ", years=", year_from, "-", year_to,
      log_file = log_file
    )
  }

  proxy[]
}
