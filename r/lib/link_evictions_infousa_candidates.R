## ============================================================
## link_evictions_infousa_candidates.R
## ============================================================
## Candidate generation tiers for eviction defendant <-> InfoUSA linkage.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
})

empty_candidate_dt <- function() {
  data.table(
    evict_id = character(),
    evict_person_id = character(),
    familyid = character(),
    person_num = integer(),
    inf_person_id = character(),
    match_tier = character(),
    filing_yr = integer(),
    inf_year = integer(),
    year_link_used = integer(),
    PID = character(),
    n_sn_ss_c = character(),
    zip = character(),
    bg_geoid = character(),
    tract_geoid = character(),
    bldg_size_bin = character(),
    first_name_ev = character(),
    last_name_ev = character(),
    first_name_inf = character(),
    last_name_inf = character(),
    first_name_ev_norm = character(),
    last_name_ev_norm = character(),
    first_name_inf_norm = character(),
    last_name_inf_norm = character(),
    street_number_ev = character(),
    street_name_ev = character(),
    street_number_inf = character(),
    street_name_inf = character(),
    gender_ev = character(),
    gender_inf = character()
  )
}

make_year_links <- function(ev_dt, year_window = c(0L, -1L), use_year = TRUE, log_file = NULL) {
  stopifnot(is.data.table(ev_dt))
  assert_has_cols(ev_dt, c("evict_id", "evict_person_id", "filing_yr"), "eviction people")

  if (!isTRUE(use_year)) {
    out <- unique(ev_dt[, .(evict_id, evict_person_id, filing_yr)])
    out[, `:=`(year_offset = 0L, inf_year = NA_integer_)]
    if (exists("logf", mode = "function")) {
      logf("Year-link rows built (year blocking OFF): ", nrow(out), log_file = log_file)
    }
    return(out)
  }

  offsets <- as.integer(year_window)
  offsets <- offsets[!is.na(offsets)]
  if (length(offsets) == 0L) offsets <- c(0L, -1L)

  idx <- rep(seq_len(nrow(ev_dt)), each = length(offsets))
  out <- ev_dt[idx, .(evict_id, evict_person_id, filing_yr)]
  out[, year_offset := rep(offsets, times = nrow(ev_dt))]
  out[, inf_year := as.integer(filing_yr + year_offset)]

  if (exists("logf", mode = "function")) {
    logf("Year-link rows built: ", nrow(out), " (offsets=", paste(offsets, collapse = ","), ")",
         log_file = log_file)
  }

  out
}

build_blocks_tier_A <- function(ev_dt, inf_dt, year_links, use_year = TRUE, log_file = NULL) {
  need_ev <- c("evict_id", "evict_person_id", "PID", "last_name_norm", "first_name_norm",
               "filing_yr", "bldg_size_bin", "gender", "street_number", "street_name")
  need_inf <- c("inf_person_id", "familyid", "person_num", "PID", "last_name_norm",
                "first_name_norm", "year", "gender", "street_number", "street_name")

  if (!all(need_ev %in% names(ev_dt)) || !all(need_inf %in% names(inf_dt))) {
    if (exists("logf", mode = "function")) logf("Tier A skipped: missing required columns", log_file = log_file)
    return(empty_candidate_dt())
  }

  ev <- ev_dt[!is.na(PID) & nzchar(PID) & !is.na(last_name_norm) & nzchar(last_name_norm)]
  yl <- year_links[ev, on = "evict_person_id", nomatch = 0L]

  inf <- inf_dt[!is.na(PID) & nzchar(PID) & !is.na(last_name_norm) & nzchar(last_name_norm)]

  if (isTRUE(use_year)) {
    cand <- yl[inf, on = .(PID, inf_year = year, last_name_norm), allow.cartesian = TRUE, nomatch = 0L]
  } else {
    cand <- yl[inf, on = .(PID, last_name_norm), allow.cartesian = TRUE, nomatch = 0L]
  }

  out <- cand[, .(
    evict_id,
    evict_person_id,
    familyid = coalesce_chr(familyid, ""),
    person_num = suppressWarnings(as.integer(person_num)),
    inf_person_id = inf_person_id,
    match_tier = "A",
    filing_yr,
    inf_year = if (isTRUE(use_year)) inf_year else suppressWarnings(as.integer(year)),
    year_link_used = if (isTRUE(use_year)) as.integer(inf_year - filing_yr) else 0L,
    PID,
    n_sn_ss_c,
    zip,
    bg_geoid,
    tract_geoid,
    bldg_size_bin,
    first_name_ev = first_name,
    last_name_ev = last_name,
    first_name_inf = i.first_name,
    last_name_inf = i.last_name,
    first_name_ev_norm = first_name_norm,
    last_name_ev_norm = last_name_norm,
    first_name_inf_norm = i.first_name_norm,
    last_name_inf_norm = i.last_name_norm,
    street_number_ev = street_number,
    street_name_ev = street_name,
    street_number_inf = i.street_number,
    street_name_inf = i.street_name,
    gender_ev = gender,
    gender_inf = i.gender
  )]

  if (exists("logf", mode = "function")) {
    logf("Tier A candidates: ", nrow(out), log_file = log_file)
  }

  out
}

build_blocks_tier_B <- function(ev_dt, inf_dt, year_links, use_year = TRUE, geo_level = "tract",
                                use_street_fuzzy = TRUE, log_file = NULL) {
  need_ev <- c("evict_id", "evict_person_id", "street_block", "street_block_fuzzy",
               "first_name_norm", "last_name_norm", "filing_yr", "bldg_size_bin", "gender", "tract_geoid", "bg_geoid")
  need_inf <- c("inf_person_id", "familyid", "person_num", "street_block", "street_block_fuzzy",
                "first_name_norm", "last_name_norm", "year", "gender", "tract_geoid", "bg_geoid")

  if (!all(need_ev %in% names(ev_dt)) || !all(need_inf %in% names(inf_dt))) {
    if (exists("logf", mode = "function")) logf("Tier B skipped: missing required columns", log_file = log_file)
    return(empty_candidate_dt())
  }

  geo_level <- tolower(trimws(as.character(geo_level)))
  if (!geo_level %chin% c("tract", "bg")) geo_level <- "tract"

  street_block_col <- if (isTRUE(use_street_fuzzy)) "street_block_fuzzy" else "street_block"

  if (geo_level == "bg") {
    ev <- ev_dt[!is.na(bg_geoid) & nzchar(bg_geoid) &
                  !is.na(get(street_block_col)) & nzchar(get(street_block_col))]
  } else {
    ev <- ev_dt[!is.na(tract_geoid) & nzchar(tract_geoid) &
                  !is.na(get(street_block_col)) & nzchar(get(street_block_col))]
  }
  yl <- year_links[ev, on = "evict_person_id", nomatch = 0L]

  cand <- data.table()
  if (geo_level == "bg") {
    inf <- inf_dt[!is.na(bg_geoid) & nzchar(bg_geoid) &
                    !is.na(get(street_block_col)) & nzchar(get(street_block_col))]
    if (isTRUE(use_street_fuzzy)) {
      if (isTRUE(use_year)) {
        cand <- yl[inf, on = .(bg_geoid, street_block_fuzzy, inf_year = year), allow.cartesian = TRUE, nomatch = 0L]
      } else {
        cand <- yl[inf, on = .(bg_geoid, street_block_fuzzy), allow.cartesian = TRUE, nomatch = 0L]
      }
    } else {
      if (isTRUE(use_year)) {
        cand <- yl[inf, on = .(bg_geoid, street_block, inf_year = year), allow.cartesian = TRUE, nomatch = 0L]
      } else {
        cand <- yl[inf, on = .(bg_geoid, street_block), allow.cartesian = TRUE, nomatch = 0L]
      }
    }
  } else {
    inf <- inf_dt[!is.na(tract_geoid) & nzchar(tract_geoid) &
                    !is.na(get(street_block_col)) & nzchar(get(street_block_col))]
    if (isTRUE(use_street_fuzzy)) {
      if (isTRUE(use_year)) {
        cand <- yl[inf, on = .(tract_geoid, street_block_fuzzy, inf_year = year), allow.cartesian = TRUE, nomatch = 0L]
      } else {
        cand <- yl[inf, on = .(tract_geoid, street_block_fuzzy), allow.cartesian = TRUE, nomatch = 0L]
      }
    } else {
      if (isTRUE(use_year)) {
        cand <- yl[inf, on = .(tract_geoid, street_block, inf_year = year), allow.cartesian = TRUE, nomatch = 0L]
      } else {
        cand <- yl[inf, on = .(tract_geoid, street_block), allow.cartesian = TRUE, nomatch = 0L]
      }
    }
  }

  if (nrow(cand) > 0L) {
    cand <- unique(cand, by = c("evict_person_id", "inf_person_id", "inf_year"))
  }

  out <- cand[, .(
    evict_id,
    evict_person_id,
    familyid = coalesce_chr(familyid, ""),
    person_num = suppressWarnings(as.integer(person_num)),
    inf_person_id = inf_person_id,
    match_tier = "B",
    filing_yr,
    inf_year = if (isTRUE(use_year)) inf_year else suppressWarnings(as.integer(year)),
    year_link_used = if (isTRUE(use_year)) as.integer(inf_year - filing_yr) else 0L,
    PID,
    n_sn_ss_c,
    zip,
    bg_geoid,
    tract_geoid,
    bldg_size_bin,
    first_name_ev = first_name,
    last_name_ev = last_name,
    first_name_inf = i.first_name,
    last_name_inf = i.last_name,
    first_name_ev_norm = first_name_norm,
    last_name_ev_norm = last_name_norm,
    first_name_inf_norm = i.first_name_norm,
    last_name_inf_norm = i.last_name_norm,
    street_number_ev = street_number,
    street_name_ev = street_name,
    street_number_inf = i.street_number,
    street_name_inf = i.street_name,
    gender_ev = gender,
    gender_inf = i.gender
  )]

  if (exists("logf", mode = "function")) {
    logf("Tier B candidates (geo=", geo_level,
         ", street_block=", ifelse(isTRUE(use_street_fuzzy), "fuzzy", "exact"),
         "): ", nrow(out),
         log_file = log_file)
  }

  out
}

build_blocks_tier_C <- function(ev_dt, inf_dt, year_links, use_year = TRUE, use_street_fuzzy = TRUE, log_file = NULL) {
  need_ev <- c("evict_id", "evict_person_id", "zip", "street_number", "street_name",
               "street_block", "street_block_fuzzy",
               "last_name_norm", "first_name_norm", "filing_yr", "bldg_size_bin", "gender")
  need_inf <- c("inf_person_id", "familyid", "person_num", "zip", "street_number", "street_name",
                "street_block", "street_block_fuzzy",
                "last_name_norm", "first_name_norm", "year", "gender")

  if (!all(need_ev %in% names(ev_dt)) || !all(need_inf %in% names(inf_dt))) {
    if (exists("logf", mode = "function")) logf("Tier C skipped: missing required columns", log_file = log_file)
    return(empty_candidate_dt())
  }

  street_block_col <- if (isTRUE(use_street_fuzzy)) "street_block_fuzzy" else "street_block"

  # ZIP tier is fallback for rows without tract geoid to limit candidate blow-up.
  ev <- ev_dt[!is.na(zip) & nzchar(zip) &
                (is.na(tract_geoid) | !nzchar(tract_geoid)) &
                !is.na(get(street_block_col)) & nzchar(get(street_block_col))]
  yl <- year_links[ev, on = "evict_person_id", nomatch = 0L]

  inf <- inf_dt[!is.na(zip) & nzchar(zip) &
                  !is.na(get(street_block_col)) & nzchar(get(street_block_col))]

  if (isTRUE(use_street_fuzzy)) {
    if (isTRUE(use_year)) {
      cand <- yl[inf, on = .(zip, street_block_fuzzy, inf_year = year),
                 allow.cartesian = TRUE, nomatch = 0L]
    } else {
      cand <- yl[inf, on = .(zip, street_block_fuzzy),
                 allow.cartesian = TRUE, nomatch = 0L]
    }
  } else {
    if (isTRUE(use_year)) {
      cand <- yl[inf, on = .(zip, street_block, inf_year = year),
                 allow.cartesian = TRUE, nomatch = 0L]
    } else {
      cand <- yl[inf, on = .(zip, street_block),
                 allow.cartesian = TRUE, nomatch = 0L]
    }
  }

  out <- cand[, .(
    evict_id,
    evict_person_id,
    familyid = coalesce_chr(familyid, ""),
    person_num = suppressWarnings(as.integer(person_num)),
    inf_person_id = inf_person_id,
    match_tier = "C",
    filing_yr,
    inf_year = if (isTRUE(use_year)) inf_year else suppressWarnings(as.integer(year)),
    year_link_used = if (isTRUE(use_year)) as.integer(inf_year - filing_yr) else 0L,
    PID,
    n_sn_ss_c,
    zip,
    bg_geoid,
    tract_geoid,
    bldg_size_bin,
    first_name_ev = first_name,
    last_name_ev = last_name,
    first_name_inf = i.first_name,
    last_name_inf = i.last_name,
    first_name_ev_norm = first_name_norm,
    last_name_ev_norm = last_name_norm,
    first_name_inf_norm = i.first_name_norm,
    last_name_inf_norm = i.last_name_norm,
    street_number_ev = street_number,
    street_name_ev = street_name,
    street_number_inf = i.street_number,
    street_name_inf = i.street_name,
    gender_ev = gender,
    gender_inf = i.gender
  )]

  if (exists("logf", mode = "function")) {
    logf("Tier C candidates (street_block=", ifelse(isTRUE(use_street_fuzzy), "fuzzy", "exact"),
         "): ", nrow(out), log_file = log_file)
  }

  out
}

enforce_candidate_caps <- function(cand_dt, cap_k = 50L, log_file = NULL) {
  if (!is.data.table(cand_dt)) cand_dt <- as.data.table(cand_dt)
  if (nrow(cand_dt) == 0L) {
    return(list(candidates = cand_dt, stats = data.table()))
  }

  cap_k <- as.integer(cap_k)
  if (is.na(cap_k) || cap_k <= 0L) cap_k <- 50L

  tier_rank <- c(A = 1L, B = 2L, C = 3L)
  cand_dt[, tier_rank := tier_rank[match_tier]]
  cand_dt[, cap_first_name_score := first_name_score(first_name_ev_norm, first_name_inf_norm, nick_dt = NULL)]
  cand_dt[, cap_last_name_score := first_name_score(last_name_ev_norm, last_name_inf_norm, nick_dt = NULL)]
  cand_dt[, cap_name_score := 0.65 * fifelse(is.na(cap_last_name_score), 0, cap_last_name_score) +
       0.35 * fifelse(is.na(cap_first_name_score), 0, cap_first_name_score)]
  setorder(cand_dt, evict_person_id, tier_rank, -cap_name_score, -cap_last_name_score, -cap_first_name_score, familyid, inf_person_id)

  cand_dt[, tier_candidate_n := .N, by = .(evict_person_id, match_tier)]
  cand_dt[, tier_candidate_rank := seq_len(.N), by = .(evict_person_id, match_tier)]
  cand_dt[, too_many_candidates := tier_candidate_n > cap_k]

  kept <- cand_dt[tier_candidate_rank <= cap_k]
  kept[, c("cap_first_name_score", "cap_last_name_score", "cap_name_score") := NULL]
  cand_dt[, c("cap_first_name_score", "cap_last_name_score", "cap_name_score") := NULL]

  stats <- cand_dt[, .(
    evict_id = evict_id[1L],
    num_candidates_raw = .N,
    num_candidates_capped = min(.N, cap_k),
    was_capped = any(.N > cap_k),
    tiers_attempted = paste(sort(unique(match_tier)), collapse = ",")
  ), by = evict_person_id]

  if (exists("logf", mode = "function")) {
    n_capped <- kept[too_many_candidates == TRUE, uniqueN(evict_person_id)]
    logf("Candidate cap K=", cap_k, ": kept rows=", nrow(kept), "; capped eviction rows=", n_capped,
         log_file = log_file)
  }

  list(candidates = kept, stats = stats)
}
