## ============================================================
## build-outer-sorting-empirical-moments.R
## ============================================================
## Purpose: Construct empirical target moments for the outer sorting
## calibration from bldg_panel_blp plus a building-level composition
## proxy.
## ============================================================

weighted_avg_safe <- function(x, w = NULL) {
  x <- as.numeric(x)
  keep <- is.finite(x) & !is.na(x)
  if (!any(keep)) return(NA_real_)
  if (is.null(w)) return(mean(x[keep]))
  w <- as.numeric(w)
  w_keep <- w[keep]
  if (!all(is.finite(w_keep)) || sum(w_keep, na.rm = TRUE) <= 0) {
    return(mean(x[keep]))
  }
  stats::weighted.mean(x[keep], w = pmax(w_keep, 0), na.rm = TRUE)
}

first_nonmissing_chr <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (!length(x)) return(NA_character_)
  x[1L]
}

normalize_pid_emp <- function(x) {
  if (!requireNamespace("stringr", quietly = TRUE)) stop("Missing package: stringr")
  stringr::str_pad(as.character(x), width = 9L, side = "left", pad = "0")
}

build_outer_sorting_empirical_panel <- function(cfg, log_file = NULL) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")

  settings <- cfg$run$outer_sorting$empirical %||% list()
  bldg_key <- as.character(settings$bldg_panel_product_key %||% "bldg_panel_blp")
  composition_key <- as.character(settings$composition_product_key %||% "infousa_building_income_proxy_panel")
  composition_proxy_col <- as.character(settings$composition_proxy_col %||% "infousa_find_low_income_share")
  composition_weight_col <- as.character(settings$composition_weight_col %||% "infousa_num_households_income_proxy")
  composition_source_col <- as.character(settings$composition_source_col %||% "infousa_income_proxy_source")
  year_from <- as.integer(settings$year_from %||% 2011L)
  year_to <- as.integer(settings$year_to %||% 2019L)
  high_filing_threshold <- as.numeric(settings$high_filing_threshold %||% cfg$run$outer_sorting$high_filing_threshold %||% 0.15)
  maintenance_count_col <- as.character(settings$maintenance_count_col %||% "total_permits")
  complaint_count_col <- as.character(settings$complaint_count_col %||% "total_complaints")
  filing_count_col <- as.character(settings$filing_count_col %||% "num_filings")
  units_col <- as.character(settings$units_col %||% "total_units")
  geoid_col <- as.character(settings$geoid_col %||% "GEOID")
  pid_col <- as.character(settings$pid_col %||% "PID")
  year_built_col <- as.character(settings$year_built_col %||% "year_built")
  old_bldg_cutoff_year <- as.integer(cfg$run$outer_sorting$old_bldg_cutoff_year %||% 1940L)
  new_bldg_cutoff_year <- as.integer(cfg$run$outer_sorting$new_bldg_cutoff_year %||% 2000L)

  bldg_path <- p_product(cfg, bldg_key)
  composition_path <- p_product(cfg, composition_key)

  sel_cols <- unique(c(pid_col, "year", geoid_col, units_col, maintenance_count_col, complaint_count_col, filing_count_col, year_built_col))
  dt <- data.table::fread(bldg_path, select = sel_cols)
  composition_dt <- data.table::fread(composition_path)

  assert_has_cols(dt, sel_cols, "bldg_panel_blp empirical target input")
  assert_has_cols(
    composition_dt,
    c("PID", "year", composition_proxy_col, composition_weight_col, composition_source_col),
    "outer-sorting composition proxy product"
  )
  composition_dt <- data.table::as.data.table(composition_dt)
  composition_dt[, `:=`(
    PID = normalize_pid_emp(PID),
    year = as.integer(year)
  )]
  assert_unique(composition_dt, c("PID", "year"), "outer-sorting composition proxy product")

  dt <- data.table::as.data.table(dt)
  dt <- dt[year >= year_from & year <= year_to]
  if (nrow(dt) == 0L) stop("No bldg_panel_blp rows remain in the requested year window.")

  data.table::setnames(
    dt,
    old = c(pid_col, geoid_col, units_col, maintenance_count_col, complaint_count_col, filing_count_col, year_built_col),
    new = c("b", "bg_geoid", "units_year", "M_year", "C_year", "F_year", "year_built")
  )
  dt[, b := normalize_pid_emp(b)]

  dt[, bg_geoid := normalize_bg_geoid(bg_geoid)]
  dt <- dt[!is.na(bg_geoid)]
  dt[, tract_geoid := substr(bg_geoid, 1L, 11L)]
  dt[, year_built := suppressWarnings(as.integer(year_built))]
  dt <- dt[is.na(year_built) | year_built <= year_to]
  dt <- dt[is.finite(units_year) & units_year > 0]
  if (nrow(dt) == 0L) stop("No valid building-year observations with positive units remain after filtering.")

  geo_check <- dt[, .(
    n_bg = uniqueN(bg_geoid),
    n_tract = uniqueN(tract_geoid),
    n_year_built = uniqueN(year_built[!is.na(year_built)])
  ), by = b]
  if (geo_check[n_bg > 1L | n_tract > 1L | n_year_built > 1L, .N] > 0L) {
    stop("Some PIDs map to multiple block groups/tracts within the empirical target window. Resolve geography stability before building targets.")
  }

  dt <- merge(
    dt,
    composition_dt[, .(
      b = as.character(PID),
      year,
      s_year = as.numeric(get(composition_proxy_col)),
      s_weight_year = as.numeric(get(composition_weight_col)),
      composition_measure_source = as.character(get(composition_source_col))
    )],
    by = c("b", "year"),
    all.x = TRUE
  )

  n_drop_missing_s <- dt[is.na(s_year), .N]
  n_drop_missing_pid <- dt[is.na(s_year), uniqueN(b)]
  if (!is.null(log_file) && exists("logf", mode = "function") && n_drop_missing_s > 0L) {
    logf("build_outer_sorting_empirical_panel: dropping ", n_drop_missing_s,
         " building-year rows (", n_drop_missing_pid,
         " PIDs) with missing building-level composition proxy.", log_file = log_file)
  }
  dt <- dt[!is.na(s_year)]
  if (nrow(dt) == 0L) stop("No empirical rows remain after dropping missing building-level composition proxy.")

  build_dt <- dt[, .(
    nhood = unique(tract_geoid),
    year_built = unique(year_built),
    first_year_obs = min(year, na.rm = TRUE),
    last_year_obs = max(year, na.rm = TRUE),
    observed_years = uniqueN(year),
    units_unit_years = sum(units_year, na.rm = TRUE),
    M_total = sum(M_year, na.rm = TRUE),
    C_total = sum(C_year, na.rm = TRUE),
    F_total = sum(F_year, na.rm = TRUE),
    s = weighted_avg_safe(s_year, s_weight_year),
    composition_households = sum(s_weight_year, na.rm = TRUE),
    composition_measure_source = first_nonmissing_chr(composition_measure_source),
    bg_geoid = unique(bg_geoid),
    sample_year_rows = .N
  ), by = b]

  build_dt[, first_year_active := fifelse(!is.na(year_built), pmax(year_from, year_built), year_from)]
  build_dt[, expected_years := pmax(0L, year_to - first_year_active + 1L)]
  build_dt <- build_dt[expected_years > 0L]
  mismatch_n <- build_dt[observed_years != expected_years, .N]
  if (!is.null(log_file) && exists("logf", mode = "function") && mismatch_n > 0L) {
    logf("build_outer_sorting_empirical_panel: observed_years != expected_years for ", mismatch_n,
         " PIDs; using observed_years for annualization.", log_file = log_file)
  }
  build_dt[, exposure_years := observed_years]
  build_dt <- build_dt[exposure_years > 0L]

  build_dt[, `:=`(
    units = units_unit_years / exposure_years,
    M = M_total / exposure_years,
    C = C_total / exposure_years,
    F = F_total / exposure_years
  )]
  build_dt[, `:=`(
    old_bldg = as.integer(!is.na(year_built) & year_built <= old_bldg_cutoff_year),
    new_bldg = as.integer(!is.na(year_built) & year_built >= new_bldg_cutoff_year)
  )]

  build_dt[, `:=`(
    m_rate = fifelse(units_unit_years > 0, M_total / units_unit_years, NA_real_),
    c_rate = fifelse(units_unit_years > 0, C_total / units_unit_years, NA_real_),
    f_rate = fifelse(units_unit_years > 0, F_total / units_unit_years, NA_real_),
    mbar = fifelse(units_unit_years > 0, M_total / units_unit_years, NA_real_),
    cbar = fifelse(units_unit_years > 0, C_total / units_unit_years, NA_real_)
  )]

  if (!is.null(log_file) && exists("logf", mode = "function")) {
    logf("build_outer_sorting_empirical_panel: rows=", nrow(build_dt),
         ", years=", year_from, "-", year_to,
         ", annualized_over_active_years=TRUE",
         ", composition_product_key=", composition_key,
         ", composition_proxy_col=", composition_proxy_col,
         ", composition_source=", unique(na.omit(build_dt$composition_measure_source))[1],
         ", high_filing_threshold=", high_filing_threshold,
         log_file = log_file)
  }

  list(
    panel = build_dt,
    metadata = data.table::data.table(
      year_from = year_from,
      year_to = year_to,
      high_filing_threshold = high_filing_threshold,
      composition_product_key = composition_key,
      composition_proxy_col = composition_proxy_col,
      composition_measure_source = unique(na.omit(build_dt$composition_measure_source))[1],
      old_bldg_cutoff_year = old_bldg_cutoff_year,
      new_bldg_cutoff_year = new_bldg_cutoff_year
    )
  )
}

compute_outer_sorting_empirical_moments <- function(cfg, log_file = NULL) {
  built <- build_outer_sorting_empirical_panel(cfg, log_file = log_file)
  threshold <- built$metadata$high_filing_threshold[1]
  moments <- compute_outer_sorting_moments(
    built$panel,
    high_filing_threshold = threshold,
    log_file = log_file
  )
  cbind(moments, built$metadata)
}
