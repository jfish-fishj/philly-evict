## ============================================================
## race_imputation_utils.R
## ============================================================
## Purpose: Shared WRU race-imputation utility functions.
##          Sourced by both impute-race.R (evictions) and
##          impute-race-infousa.R (InfoUSA households).
##
## Usage: source("r/lib/race_imputation_utils.R")
##
## Dependencies: data.table, stringr, wru
## Also requires: logf(), assert_has_cols(), assert_unique()
##   from r/config.R (must be sourced first).
## ============================================================

# ---- GEOID normalization ----

normalize_11digit_geoid <- function(x) {
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

normalize_geoid_any <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- str_trim(x)
  x <- str_replace_all(x, "[^0-9]", "")
  x[nchar(x) == 0L] <- NA_character_
  x[!is.na(x) & nchar(x) > 15L] <- NA_character_
  x
}

# ---- Census geography helpers ----

canonicalize_census_geo <- function(x) {
  if (is.null(x) || is.na(x) || !nzchar(as.character(x))) return("block_group")
  g <- tolower(str_squish(as.character(x)))
  g <- str_replace_all(g, "[\\-\\s]+", "_")
  if (g %chin% c("tract", "tr")) return("tract")
  if (g %chin% c("block_group", "blockgroup", "bg", "bgroup")) return("block_group")
  if (g %chin% c("block", "blk", "blocks")) return("block")
  stop("Unsupported geography: ", x, ". Use one of tract, block_group, block.")
}

geo_rank <- function(geo) {
  geo <- canonicalize_census_geo(geo)
  switch(geo, tract = 1L, block_group = 2L, block = 3L)
}

geoid_nchar <- function(geo) {
  geo <- canonicalize_census_geo(geo)
  switch(geo, tract = 11L, block_group = 12L, block = 15L)
}

geo_column_name <- function(geo) {
  geo <- canonicalize_census_geo(geo)
  switch(geo, tract = "tract_geoid", block_group = "bg_geoid", block = "block_geoid")
}

normalize_geoid <- function(x, geo) {
  geo <- canonicalize_census_geo(geo)
  switch(
    geo,
    tract = normalize_11digit_geoid(x),
    block_group = normalize_bg_geoid(x),
    block = normalize_block_geoid(x)
  )
}

# ---- Prior source selection ----

pick_prior_source_geo <- function(priors_dt, target_geo) {
  target_geo <- canonicalize_census_geo(target_geo)
  candidates <- c("tract", "block_group", "block")
  have <- vapply(
    candidates,
    FUN.VALUE = logical(1),
    FUN = function(g) {
      cc <- geo_column_name(g)
      cc %in% names(priors_dt) && priors_dt[!is.na(get(cc)), .N] > 0L
    }
  )
  if (have[[target_geo]]) return(target_geo)

  finer <- candidates[have & vapply(candidates, function(g) geo_rank(g), integer(1)) > geo_rank(target_geo)]
  if (length(finer) > 0L) {
    finer <- finer[order(vapply(finer, function(g) geo_rank(g), integer(1)), decreasing = TRUE)]
    return(finer[1L])
  }

  available <- candidates[have]
  stop(
    "Cannot build ", target_geo, " priors from available priors columns. ",
    "Available source geographies: ", paste(available, collapse = ", "), "."
  )
}

# ---- Target GEOID assignment (eviction pipeline) ----

assign_target_geoid <- function(dt, target_geo) {
  target_geo <- canonicalize_census_geo(target_geo)
  for (g in c("tract", "block_group", "block")) {
    cc <- geo_column_name(g)
    if (!(cc %in% names(dt))) dt[, (cc) := NA_character_]
    dt[, (cc) := normalize_geoid(get(cc), g)]
  }
  if (!("GEOID" %in% names(dt))) dt[, GEOID := NA_character_]
  dt[, GEOID_norm := normalize_geoid_any(GEOID)]

  direct_col <- geo_column_name(target_geo)
  dt[, geo_from_direct := get(direct_col)]
  dt[, geo_from_geoid := NA_character_]
  dt[, geo_from_block := NA_character_]
  dt[, geo_from_bg := NA_character_]

  if (target_geo == "tract") {
    dt[!is.na(GEOID_norm) & nchar(GEOID_norm) >= 11L, geo_from_geoid := substr(GEOID_norm, 1L, 11L)]
    dt[!is.na(block_geoid), geo_from_block := substr(block_geoid, 1L, 11L)]
    dt[!is.na(bg_geoid), geo_from_bg := substr(bg_geoid, 1L, 11L)]
  } else if (target_geo == "block_group") {
    dt[!is.na(GEOID_norm) & nchar(GEOID_norm) >= 12L, geo_from_geoid := substr(GEOID_norm, 1L, 12L)]
    dt[!is.na(block_geoid), geo_from_block := substr(block_geoid, 1L, 12L)]
  } else if (target_geo == "block") {
    dt[!is.na(GEOID_norm) & nchar(GEOID_norm) == 15L, geo_from_geoid := GEOID_norm]
  }

  dt[, geo_chr := normalize_geoid(
    fcoalesce(geo_from_direct, geo_from_geoid, geo_from_block, geo_from_bg),
    target_geo
  )]
  dt[, geo_source := fifelse(
    !is.na(geo_from_direct), paste0(target_geo, "_direct"),
    fifelse(!is.na(geo_from_geoid), "geoid_column",
            fifelse(!is.na(geo_from_block), "block_to_target",
                    fifelse(!is.na(geo_from_bg), "bg_to_target", "missing")))
  )]
  dt[, c("GEOID_norm", "geo_from_direct", "geo_from_geoid", "geo_from_block", "geo_from_bg") := NULL]
  dt
}

# ---- State FIPS helper ----

state_abbr_from_fips <- function(code2) {
  sf <- as.data.table(wru::state_fips)
  map <- setNames(sf$state, sf$state_code)
  out <- map[as.character(code2)]
  unname(as.character(out))
}

# ---- WRU census.data construction ----

build_wru_census_geo_frame <- function(x, census_geo, pop_cols) {
  census_geo <- canonicalize_census_geo(census_geo)
  out <- x[, .(
    county = county_code,
    tract = geo_mid_code
  )]
  if (census_geo == "block_group") out[, block_group := x$bg_code]
  if (census_geo == "block") out[, block := x$block_code]
  cbind(out, x[, ..pop_cols])
}

build_wru_census_data_from_priors <- function(geo_priors, census_geo, year = 2010L) {
  census_geo <- canonicalize_census_geo(census_geo)
  tp <- copy(geo_priors)
  need <- c("geo_geoid", "p_white_prior", "p_black_prior", "p_hispanic_prior", "p_asian_prior", "p_other_prior")
  assert_has_cols(tp, need, paste0(census_geo, " priors"))

  tp[, geo_geoid := normalize_geoid(geo_geoid, census_geo)]
  tp <- tp[!is.na(geo_geoid)]
  assert_unique(tp, "geo_geoid", paste0(census_geo, " priors (prepared)"))
  prob_cols <- c("p_white_prior", "p_black_prior", "p_hispanic_prior", "p_asian_prior", "p_other_prior")

  miss_idx <- tp[
    is.na(p_white_prior) | is.na(p_black_prior) | is.na(p_hispanic_prior) | is.na(p_asian_prior) | is.na(p_other_prior),
    which = TRUE
  ]
  if (length(miss_idx) > 0L) {
    city_mean <- tp[
      !is.na(p_white_prior) & !is.na(p_black_prior) & !is.na(p_hispanic_prior) & !is.na(p_asian_prior) & !is.na(p_other_prior),
      lapply(.SD, mean, na.rm = TRUE),
      .SDcols = prob_cols
    ]
    if (nrow(city_mean) != 1L || anyNA(city_mean)) {
      stop("Unable to compute citywide fallback priors for missing geography probabilities.")
    }
    for (cc in prob_cols) tp[miss_idx, (cc) := city_mean[[cc]]]
  }

  if (!("total_renters" %in% names(tp))) tp[, total_renters := NA_real_]
  if (!("total_pop" %in% names(tp))) tp[, total_pop := NA_real_]
  tp[, total_renters := as.numeric(total_renters)]
  tp[, total_pop := as.numeric(total_pop)]
  tp[, base_total := fifelse(
    is.finite(total_renters) & total_renters > 0, total_renters,
    fifelse(is.finite(total_pop) & total_pop > 0, total_pop, 100000)
  )]

  tp[, state_fips := substr(geo_geoid, 1L, 2L)]
  tp[, county_code := substr(geo_geoid, 3L, 5L)]
  tp[, geo_mid_code := substr(geo_geoid, 6L, 11L)]
  if (census_geo == "block_group") tp[, bg_code := substr(geo_geoid, 12L, 12L)]
  if (census_geo == "block") tp[, block_code := substr(geo_geoid, 12L, 15L)]
  tp[, state_abbr := state_abbr_from_fips(state_fips)]

  if (tp[is.na(state_abbr), .N] > 0L) {
    bad <- unique(tp[is.na(state_abbr), state_fips])
    stop("Could not map state FIPS to abbreviation for: ", paste(head(bad, 10L), collapse = ", "))
  }

  tp[, `:=`(
    P005003 = pmax(p_white_prior * base_total, 0),
    P005004 = pmax(p_black_prior * base_total, 0),
    P005005 = 0,
    P005006 = pmax(p_asian_prior * base_total, 0),
    P005007 = 0,
    P005008 = pmax(p_other_prior * base_total, 0),
    P005009 = 0,
    P005010 = pmax(p_hispanic_prior * base_total, 0)
  )]

  pop_cols <- c("P005003", "P005004", "P005005", "P005006", "P005007", "P005008", "P005009", "P005010")

  census_data <- list()
  states <- sort(unique(tp$state_abbr))
  for (st in states) {
    sdt <- tp[state_abbr == st]
    sdt <- build_wru_census_geo_frame(sdt, census_geo = census_geo, pop_cols = pop_cols)
    census_data[[st]] <- list(
      year = as.character(year),
      age = FALSE,
      sex = FALSE
    )
    census_data[[st]][[census_geo]] <- as.data.frame(sdt)
  }
  census_data
}

# ---- Prior coercion ----

coerce_priors_to_geo <- function(priors_raw, target_geo) {
  target_geo <- canonicalize_census_geo(target_geo)
  p <- copy(priors_raw)
  req_prob <- c("p_white_prior", "p_black_prior", "p_hispanic_prior", "p_asian_prior", "p_other_prior")
  assert_has_cols(p, req_prob, "priors input")

  if (!any(c("tract_geoid", "bg_geoid", "block_geoid") %in% names(p))) {
    stop("Priors file must include at least one geography column: tract_geoid, bg_geoid, or block_geoid.")
  }

  if (!("total_renters" %in% names(p))) p[, total_renters := NA_real_]
  if (!("total_pop" %in% names(p))) p[, total_pop := NA_real_]
  p[, total_renters := as.numeric(total_renters)]
  p[, total_pop := as.numeric(total_pop)]

  for (g in c("tract", "block_group", "block")) {
    cc <- geo_column_name(g)
    if (cc %in% names(p)) p[, (cc) := normalize_geoid(get(cc), g)]
  }
  source_geo <- pick_prior_source_geo(p, target_geo)
  source_col <- geo_column_name(source_geo)
  target_len <- geoid_nchar(target_geo)

  p <- p[!is.na(get(source_col))]
  p[, geo_geoid := substr(get(source_col), 1L, target_len)]
  p[, geo_geoid := normalize_geoid(geo_geoid, target_geo)]
  p <- p[!is.na(geo_geoid)]

  p[, w := fifelse(
    is.finite(total_renters) & total_renters > 0, total_renters,
    fifelse(is.finite(total_pop) & total_pop > 0, total_pop, 1)
  )]

  out <- p[, .(
    w_sum = sum(w, na.rm = TRUE),
    white_num = sum(p_white_prior * w, na.rm = TRUE),
    black_num = sum(p_black_prior * w, na.rm = TRUE),
    hisp_num = sum(p_hispanic_prior * w, na.rm = TRUE),
    asian_num = sum(p_asian_prior * w, na.rm = TRUE),
    other_num = sum(p_other_prior * w, na.rm = TRUE),
    total_renters = sum(fifelse(is.finite(total_renters) & total_renters > 0, total_renters, 0), na.rm = TRUE),
    total_pop = sum(fifelse(is.finite(total_pop) & total_pop > 0, total_pop, 0), na.rm = TRUE)
  ), by = geo_geoid]

  out[, `:=`(
    p_white_prior = fifelse(w_sum > 0, white_num / w_sum, NA_real_),
    p_black_prior = fifelse(w_sum > 0, black_num / w_sum, NA_real_),
    p_hispanic_prior = fifelse(w_sum > 0, hisp_num / w_sum, NA_real_),
    p_asian_prior = fifelse(w_sum > 0, asian_num / w_sum, NA_real_),
    p_other_prior = fifelse(w_sum > 0, other_num / w_sum, NA_real_)
  )]
  out[, c("w_sum", "white_num", "black_num", "hisp_num", "asian_num", "other_num") := NULL]
  out[, prior_source_geo := source_geo]
  assert_unique(out, "geo_geoid", paste0(target_geo, " priors (coerced)"))
  out
}

# ---- BG low-N tract shrinkage ----

apply_bg_low_n_tract_shrink <- function(geo_priors, min_n = 100, k = 100, log_file = NULL) {
  gp <- copy(geo_priors)
  if (!("geo_geoid" %in% names(gp)) || !("total_renters" %in% names(gp))) return(gp)

  gp[, geo_geoid := normalize_bg_geoid(geo_geoid)]
  gp <- gp[!is.na(geo_geoid)]
  gp[, n_eff := fifelse(is.finite(total_renters) & total_renters > 0, as.numeric(total_renters), 0)]
  if ("total_pop" %in% names(gp)) {
    gp[n_eff <= 0 & is.finite(total_pop) & total_pop > 0, n_eff := as.numeric(total_pop)]
  }
  gp[, n_eff := pmax(n_eff, 0)]
  gp[, tract_geoid := substr(geo_geoid, 1L, 11L)]

  prob_cols <- c("p_white_prior", "p_black_prior", "p_hispanic_prior", "p_asian_prior", "p_other_prior")
  w <- gp$n_eff
  w[!is.finite(w) | w < 0] <- 0
  gp[, w_bg := fifelse(w > 0, w, 1)]

  tr <- gp[, .(
    tract_white = weighted.mean(p_white_prior, w_bg, na.rm = TRUE),
    tract_black = weighted.mean(p_black_prior, w_bg, na.rm = TRUE),
    tract_hisp = weighted.mean(p_hispanic_prior, w_bg, na.rm = TRUE),
    tract_asian = weighted.mean(p_asian_prior, w_bg, na.rm = TRUE),
    tract_other = weighted.mean(p_other_prior, w_bg, na.rm = TRUE)
  ), by = tract_geoid]
  gp[tr, `:=`(
    tract_white = i.tract_white,
    tract_black = i.tract_black,
    tract_hisp = i.tract_hisp,
    tract_asian = i.tract_asian,
    tract_other = i.tract_other
  ), on = "tract_geoid"]

  gp[, shrink_target_ok := !is.na(tract_white) & !is.na(tract_black) & !is.na(tract_hisp) & !is.na(tract_asian) & !is.na(tract_other)]
  gp[, do_shrink := n_eff <= min_n & shrink_target_ok]
  gp[, lambda := fifelse(do_shrink, n_eff / (n_eff + k), 1)]
  gp[, lambda := pmin(pmax(lambda, 0), 1)]

  gp[, `:=`(
    p_white_prior = fifelse(do_shrink, lambda * p_white_prior + (1 - lambda) * tract_white, p_white_prior),
    p_black_prior = fifelse(do_shrink, lambda * p_black_prior + (1 - lambda) * tract_black, p_black_prior),
    p_hispanic_prior = fifelse(do_shrink, lambda * p_hispanic_prior + (1 - lambda) * tract_hisp, p_hispanic_prior),
    p_asian_prior = fifelse(do_shrink, lambda * p_asian_prior + (1 - lambda) * tract_asian, p_asian_prior),
    p_other_prior = fifelse(do_shrink, lambda * p_other_prior + (1 - lambda) * tract_other, p_other_prior)
  )]

  gp[, psum := rowSums(.SD), .SDcols = prob_cols]
  gp[is.finite(psum) & psum > 0, `:=`(
    p_white_prior = p_white_prior / psum,
    p_black_prior = p_black_prior / psum,
    p_hispanic_prior = p_hispanic_prior / psum,
    p_asian_prior = p_asian_prior / psum,
    p_other_prior = p_other_prior / psum
  )]

  n_shrunk <- gp[do_shrink == TRUE, .N]
  filing_proxy <- sum(gp$n_eff, na.rm = TRUE)
  shrink_proxy <- gp[do_shrink == TRUE, sum(n_eff, na.rm = TRUE)]
  logf(
    "Applied BG low-N tract shrink: min_n=", min_n, ", k=", k,
    ", shrunk_rows=", n_shrunk, "/", nrow(gp),
    ", shrunk_weight_share=", round(fifelse(filing_proxy > 0, shrink_proxy / filing_proxy, 0), 6),
    log_file = log_file
  )

  gp[, c("n_eff", "tract_geoid", "w_bg", "tract_white", "tract_black", "tract_hisp", "tract_asian", "tract_other",
         "shrink_target_ok", "do_shrink", "lambda", "psum") := NULL]
  gp
}

# ---- WRU predict wrapper ----

run_wru_predict <- function(vf, names_to_use, census_data, census_geo, year, census_key, retry, log_file, skip_bad_geos = FALSE) {
  if (!nrow(vf)) return(data.table())

  msg <- if (names_to_use == "surname") "surname-only" else "surname+first"
  logf(
    "Running wru::predict_race on ", nrow(vf), " unique rows (", msg, ", geo=", census_geo, ")...",
    log_file = log_file
  )

  pred <- tryCatch(
    wru::predict_race(
      voter.file = as.data.frame(vf),
      census.surname = TRUE,
      surname.only = FALSE,
      census.geo = census_geo,
      census.key = census_key,
      census.data = census_data,
      year = as.character(year),
      retry = retry,
      impute.missing = TRUE,
      skip_bad_geos = skip_bad_geos,
      use.counties = FALSE,
      model = "BISG",
      names.to.use = names_to_use
    ),
    error = function(e) {
      stop("wru::predict_race failed (", msg, "): ", conditionMessage(e))
    }
  )

  pred <- as.data.table(pred)
  need <- c("pred.whi", "pred.bla", "pred.his", "pred.asi", "pred.oth")
  assert_has_cols(pred, need, "wru output")
  pred
}

# ---- WRU voter file builder ----

build_wru_voter_file <- function(x, census_geo, include_first) {
  census_geo <- canonicalize_census_geo(census_geo)
  out <- x[, .(
    first_u,
    last_u,
    geo_chr,
    surname,
    state,
    county = county_code,
    tract = geo_mid_code
  )]
  if (isTRUE(include_first)) out[, first := x$first]
  if (census_geo == "block_group") out[, block_group := x$bg_code]
  if (census_geo == "block") out[, block := x$block_code]
  out
}

# ---- WRU name cache ----

write_wru_name_cache <- function(first_probs_path, last_probs_path, middle_probs_path, log_file) {
  prep_name_table <- function(path, key_col, suffix) {
    dt <- fread(path)
    assert_has_cols(dt, c("name", "whi", "bla", "his", "asi", "oth"), paste0("name probs: ", path))
    out <- dt[, .(
      nm = toupper(str_squish(as.character(name))),
      c_whi = pmax(as.numeric(whi), 0),
      c_bla = pmax(as.numeric(bla), 0),
      c_his = pmax(as.numeric(his), 0),
      c_asi = pmax(as.numeric(asi), 0),
      c_oth = pmax(as.numeric(oth), 0)
    )]
    out <- out[!is.na(nm) & nzchar(nm)]
    out <- unique(out, by = "nm")
    setnames(out, "nm", key_col)
    setnames(out, c("c_whi", "c_bla", "c_his", "c_asi", "c_oth"),
             paste0(c("c_whi", "c_bla", "c_his", "c_asi", "c_oth"), "_", suffix))
    out
  }

  dest <- tempdir()
  first_c <- prep_name_table(first_probs_path, "first_name", "first")
  mid_c <- prep_name_table(middle_probs_path, "middle_name", "middle")
  last_c <- prep_name_table(last_probs_path, "last_name", "last")
  census_last_c <- copy(last_c)

  saveRDS(first_c, file.path(dest, "wru-data-first_c.rds"))
  saveRDS(mid_c, file.path(dest, "wru-data-mid_c.rds"))
  saveRDS(last_c, file.path(dest, "wru-data-last_c.rds"))
  saveRDS(census_last_c, file.path(dest, "wru-data-census_last_c.rds"))
  logf("Wrote WRU name cache files to: ", dest, log_file = log_file)
}

# ---- Entropy ----

entropy5 <- function(w, b, h, a, o, eps = 1e-12) {
  p <- cbind(w, b, h, a, o)
  p <- pmax(p, eps)
  -rowSums(p * log(p))
}

# ---- Default prior candidates ----

default_prior_candidates <- function(cfg, year, geo, prior_type = "renter") {
  geo <- canonicalize_census_geo(geo)
  prior_type <- tolower(as.character(prior_type %||% "renter"))
  stem <- switch(geo, tract = "tract", block_group = "bg", block = "block")

  out <- character()
  if (prior_type %chin% c("renter", "renters")) {
    out <- c(out, p_proc(cfg, "xwalks", paste0(stem, "_renter_race_priors_", year, ".csv")))
  }
  if (prior_type %chin% c("totalpop", "total_pop", "all", "population")) {
    out <- c(out, p_proc(cfg, "xwalks", paste0(stem, "_race_priors_", year, ".csv")))
  } else {
    out <- c(out, p_proc(cfg, "xwalks", paste0(stem, "_race_priors_", year, ".csv")))
  }
  unique(out)
}
