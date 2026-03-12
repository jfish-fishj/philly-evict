## ============================================================
## build-renter-poverty-geo.R
## ============================================================
## Purpose: Build geography-level renter poverty share products for
## block groups and tracts.
##
## Preferred input:
##   a raw CSV with explicit renter household counts:
##   - bg_geoid or GEOID
##   - renter_hh_total
##   - renter_hh_poverty
##
## Fallback fetch:
##   if no input file is supplied, first try ACS B17019 and construct a
##   renter-family poverty proxy:
##   renter_poverty_share = renter families below poverty /
##                          all renter families
##
## If B17019 returns structurally empty counts, fall back to ACS
## C17002 overall poverty counts so the outer-sorting empirical
## target path still has a geography-level composition proxy.
## ============================================================

normalize_tract_geoid <- function(x) {
  if (!requireNamespace("stringr", quietly = TRUE)) stop("Missing package: stringr")
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- stringr::str_trim(x)
  x <- stringr::str_replace_all(x, "[^0-9]", "")
  x[nchar(x) == 0L] <- NA_character_
  too_long <- !is.na(x) & nchar(x) > 11L
  x[too_long] <- NA_character_
  short <- !is.na(x) & nchar(x) < 11L
  x[short] <- stringr::str_pad(x[short], width = 11L, side = "left", pad = "0")
  x
}

normalize_bg_geoid <- function(x) {
  if (!requireNamespace("stringr", quietly = TRUE)) stop("Missing package: stringr")
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- stringr::str_trim(x)
  x <- stringr::str_replace_all(x, "[^0-9]", "")
  x[nchar(x) == 0L] <- NA_character_
  too_long <- !is.na(x) & nchar(x) > 12L
  x[too_long] <- NA_character_
  short <- !is.na(x) & nchar(x) < 12L
  x[short] <- stringr::str_pad(x[short], width = 12L, side = "left", pad = "0")
  x
}

pick_first_col <- function(nms, candidates) {
  hit <- candidates[candidates %in% nms]
  if (!length(hit)) return(NA_character_)
  hit[1L]
}

parse_renter_poverty_input <- function(raw_dt, input_geography = c("block_group", "tract")) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")
  dt <- data.table::as.data.table(raw_dt)
  input_geography <- match.arg(input_geography)
  nms <- names(dt)

  geo_col <- if (input_geography == "block_group") {
    pick_first_col(nms, c("bg_geoid", "block_group_geoid", "GEOID", "geoid", "geo_id"))
  } else {
    pick_first_col(nms, c("tract_geoid", "TRACT_GEOID", "GEOID", "geoid", "geo_id"))
  }
  if (is.na(geo_col)) {
    stop("Could not identify a geography column for renter poverty input.")
  }

  total_col <- pick_first_col(nms, c(
    "renter_hh_total", "renter_households_total", "total_renters",
    "renter_family_total", "renter_families_total"
  ))
  pov_col <- pick_first_col(nms, c(
    "renter_hh_poverty", "renter_households_poverty", "renters_below_poverty",
    "renter_family_poverty", "renter_families_below_poverty"
  ))
  if (is.na(total_col) || is.na(pov_col)) {
    stop("Input renter poverty file must contain explicit renter total and renter poverty counts.")
  }

  out <- data.table::data.table(
    geo_id = if (input_geography == "block_group") normalize_bg_geoid(dt[[geo_col]]) else normalize_tract_geoid(dt[[geo_col]]),
    renter_hh_total = as.numeric(dt[[total_col]]),
    renter_hh_poverty = as.numeric(dt[[pov_col]])
  )
  out <- out[!is.na(geo_id)]
  assert_unique(out, "geo_id", "renter poverty input")
  out
}

fetch_renter_poverty_proxy_b17019 <- function(acs_year, state, county, geography = c("block_group", "tract")) {
  if (!requireNamespace("tidycensus", quietly = TRUE)) stop("Missing package: tidycensus")
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")
  geography <- match.arg(geography)

  vars_below <- c("B17019_005", "B17019_009", "B17019_012")
  vars_above <- c("B17019_016", "B17019_020", "B17019_023")
  vars <- c(vars_below, vars_above)

  acs_geo <- if (geography == "block_group") "block group" else "tract"
  raw <- tidycensus::get_acs(
    geography = acs_geo,
    variables = vars,
    year = as.integer(acs_year),
    survey = "acs5",
    state = state,
    county = county,
    geometry = FALSE,
    cache_table = TRUE
  )
  raw <- data.table::as.data.table(raw)
  wide <- data.table::dcast(raw, GEOID + NAME ~ variable, value.var = "estimate")
  miss <- setdiff(vars, names(wide))
  if (length(miss) > 0L) stop("Missing expected B17019 columns after ACS fetch: ", paste(miss, collapse = ", "))

  out <- wide[, .(
    geo_id = if (geography == "block_group") normalize_bg_geoid(GEOID) else normalize_tract_geoid(GEOID),
    renter_hh_poverty = B17019_005 + B17019_009 + B17019_012,
    renter_hh_total = B17019_005 + B17019_009 + B17019_012 + B17019_016 + B17019_020 + B17019_023
  )]
  out <- out[!is.na(geo_id)]
  assert_unique(out, "geo_id", "ACS B17019 renter poverty proxy")
  out
}

fetch_direct_poverty_share_c17002 <- function(acs_year, state, county, geography = c("block_group", "tract")) {
  if (!requireNamespace("tidycensus", quietly = TRUE)) stop("Missing package: tidycensus")
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")
  geography <- match.arg(geography)

  acs_geo <- if (geography == "block_group") "block group" else "tract"
  raw <- tidycensus::get_acs(
    geography = acs_geo,
    variables = c(
      total = "C17002_001",
      below_poverty_deep = "C17002_002",
      below_poverty_near = "C17002_003"
    ),
    year = as.integer(acs_year),
    survey = "acs5",
    state = state,
    county = county,
    geometry = FALSE,
    cache_table = TRUE
  )
  raw <- data.table::as.data.table(raw)
  wide <- data.table::dcast(raw, GEOID + NAME ~ variable, value.var = "estimate")
  miss <- setdiff(c("total", "below_poverty_deep", "below_poverty_near"), names(wide))
  if (length(miss) > 0L) stop("Missing expected C17002 columns after ACS fetch: ", paste(miss, collapse = ", "))
  below <- as.numeric(wide$below_poverty_deep) + as.numeric(wide$below_poverty_near)
  total <- as.numeric(wide$total)
  out <- wide[, .(
    geo_id = if (geography == "block_group") normalize_bg_geoid(GEOID) else normalize_tract_geoid(GEOID),
    renter_hh_total = NA_real_,
    renter_hh_poverty = NA_real_,
    renter_poverty_share = fifelse(total > 0, below / total, NA_real_)
  )]
  out <- out[!is.na(geo_id)]
  assert_unique(out, "geo_id", "ACS C17002 poverty share")
  out
}

prepare_renter_poverty_outputs <- function(base_dt, acs_year, measure_source) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")
  bg_dt <- data.table::copy(base_dt)
  bg_dt[, bg_geoid := normalize_bg_geoid(geo_id)]
  bg_dt <- bg_dt[!is.na(bg_geoid), .(
    bg_geoid,
    renter_hh_total,
    renter_hh_poverty
  )]
  assert_unique(bg_dt, "bg_geoid", "bg renter poverty")
  bg_dt[, renter_poverty_share := fifelse(renter_hh_total > 0, renter_hh_poverty / renter_hh_total, NA_real_)]
  bg_dt[, `:=`(acs_year = as.integer(acs_year), measure_source = measure_source)]
  data.table::setcolorder(bg_dt, c("bg_geoid", "renter_hh_total", "renter_hh_poverty", "renter_poverty_share", "acs_year", "measure_source"))

  tract_dt <- bg_dt[, .(
    renter_hh_total = sum(renter_hh_total, na.rm = TRUE),
    renter_hh_poverty = sum(renter_hh_poverty, na.rm = TRUE)
  ), by = .(tract_geoid = substr(bg_geoid, 1L, 11L))]
  tract_dt[, renter_poverty_share := fifelse(renter_hh_total > 0, renter_hh_poverty / renter_hh_total, NA_real_)]
  tract_dt[, `:=`(acs_year = as.integer(acs_year), measure_source = measure_source)]
  data.table::setcolorder(tract_dt, c("tract_geoid", "renter_hh_total", "renter_hh_poverty", "renter_poverty_share", "acs_year", "measure_source"))
  assert_unique(tract_dt, "tract_geoid", "tract renter poverty")

  list(bg = bg_dt, tract = tract_dt)
}

prepare_direct_poverty_outputs <- function(bg_dt, tract_dt, acs_year, measure_source) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")

  bg_out <- data.table::copy(bg_dt)
  bg_out[, bg_geoid := normalize_bg_geoid(geo_id)]
  bg_out <- bg_out[!is.na(bg_geoid), .(
    bg_geoid,
    renter_hh_total = as.numeric(renter_hh_total),
    renter_hh_poverty = as.numeric(renter_hh_poverty),
    renter_poverty_share = as.numeric(renter_poverty_share)
  )]
  bg_out[, `:=`(acs_year = as.integer(acs_year), measure_source = measure_source)]
  data.table::setcolorder(bg_out, c("bg_geoid", "renter_hh_total", "renter_hh_poverty", "renter_poverty_share", "acs_year", "measure_source"))
  assert_unique(bg_out, "bg_geoid", "bg direct poverty share")

  tract_out <- data.table::copy(tract_dt)
  tract_out[, tract_geoid := normalize_tract_geoid(geo_id)]
  tract_out <- tract_out[!is.na(tract_geoid), .(
    tract_geoid,
    renter_hh_total = as.numeric(renter_hh_total),
    renter_hh_poverty = as.numeric(renter_hh_poverty),
    renter_poverty_share = as.numeric(renter_poverty_share)
  )]
  tract_out[, `:=`(acs_year = as.integer(acs_year), measure_source = measure_source)]
  data.table::setcolorder(tract_out, c("tract_geoid", "renter_hh_total", "renter_hh_poverty", "renter_poverty_share", "acs_year", "measure_source"))
  assert_unique(tract_out, "tract_geoid", "tract direct poverty share")

  list(bg = bg_out, tract = tract_out)
}

build_renter_poverty_geo <- function(cfg, input_path = NULL, input_geography = NULL, log_file = NULL) {
  if (!requireNamespace("data.table", quietly = TRUE)) stop("Missing package: data.table")

  settings <- cfg$run$renter_poverty_geo %||% list()
  acs_year <- as.integer(settings$acs_year %||% 2022L)
  state <- as.character(settings$state %||% "PA")
  county <- as.character(settings$county %||% "Philadelphia")
  input_geography <- as.character(input_geography %||% settings$input_geography %||% "block_group")
  if (!(input_geography %in% c("block_group", "tract"))) {
    stop("renter_poverty_geo input_geography must be 'block_group' or 'tract'.")
  }

  source_label <- NULL
  if (!is.null(input_path) && nzchar(input_path)) {
    raw <- data.table::fread(input_path)
    base_dt <- parse_renter_poverty_input(raw, input_geography = input_geography)
    if (input_geography == "tract") {
      stop("Input renter poverty file is tract-level only. A block-group-level input is required for the intended outer sorting use.")
    }
    source_label <- "input_explicit_household_counts"
    out <- prepare_renter_poverty_outputs(base_dt, acs_year = acs_year, measure_source = source_label)
  } else {
    base_dt <- fetch_renter_poverty_proxy_b17019(
      acs_year = acs_year,
      state = state,
      county = county,
      geography = "block_group"
    )
    if (sum(base_dt$renter_hh_total, na.rm = TRUE) > 0) {
      source_label <- "acs_b17019_renter_family_proxy"
      out <- prepare_renter_poverty_outputs(base_dt, acs_year = acs_year, measure_source = source_label)
    } else {
      bg_share_dt <- fetch_direct_poverty_share_c17002(
        acs_year = acs_year,
        state = state,
        county = county,
        geography = "block_group"
      )
      tract_share_dt <- fetch_direct_poverty_share_c17002(
        acs_year = acs_year,
        state = state,
        county = county,
        geography = "tract"
      )
      source_label <- "acs_c17002_overall_poverty_share"
      out <- prepare_direct_poverty_outputs(
        bg_dt = bg_share_dt,
        tract_dt = tract_share_dt,
        acs_year = acs_year,
        measure_source = source_label
      )
    }
  }

  if (!is.null(log_file) && exists("logf", mode = "function")) {
    logf("build_renter_poverty_geo: source=", source_label,
         ", acs_year=", acs_year,
         ", bg_rows=", nrow(out$bg),
         ", tract_rows=", nrow(out$tract),
         log_file = log_file)
  }

  if (out$bg[!is.na(renter_poverty_share), .N] == 0L) {
    stop("Block-group poverty build produced zero non-missing renter_poverty_share values. Check the ACS fallback table or supply an explicit block-group input file.")
  }
  if (out$tract[!is.na(renter_poverty_share), .N] == 0L) {
    stop("Tract poverty build produced zero non-missing renter_poverty_share values. Check the ACS fallback table or supply an explicit input file.")
  }

  out
}
