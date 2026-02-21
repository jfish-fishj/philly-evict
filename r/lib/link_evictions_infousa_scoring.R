## ============================================================
## link_evictions_infousa_scoring.R
## ============================================================
## Scoring and deterministic assignment utilities for linkage candidates.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
})

read_name_similarity_thresholds <- function() {
  min_first <- suppressWarnings(as.numeric(Sys.getenv("PHILLY_NAME_MIN_FIRST_SIM", unset = "0.80")))
  min_last <- suppressWarnings(as.numeric(Sys.getenv("PHILLY_NAME_MIN_LAST_SIM", unset = "0.88")))
  if (is.na(min_first)) min_first <- 0.80
  if (is.na(min_last)) min_last <- 0.88
  min_first <- max(0, min(1, min_first))
  min_last <- max(0, min(1, min_last))
  list(min_first = min_first, min_last = min_last)
}

build_fastlink_block_id <- function(dt) {
  out <- rep(NA_character_, nrow(dt))
  out[dt$match_tier == "A"] <- paste(
    "A",
    coalesce_chr(dt$PID[dt$match_tier == "A"], ""),
    coalesce_chr(dt$inf_year[dt$match_tier == "A"], ""),
    coalesce_chr(dt$last_name_ev_norm[dt$match_tier == "A"], ""),
    sep = "|"
  )
  out[dt$match_tier == "B"] <- paste(
    "B",
    fifelse(
      !is.na(dt$tract_geoid[dt$match_tier == "B"]) & nzchar(dt$tract_geoid[dt$match_tier == "B"]),
      coalesce_chr(dt$tract_geoid[dt$match_tier == "B"], ""),
      coalesce_chr(dt$bg_geoid[dt$match_tier == "B"], "")
    ),
    coalesce_chr(dt$inf_year[dt$match_tier == "B"], ""),
    sep = "|"
  )
  out[dt$match_tier == "C"] <- paste(
    "C",
    coalesce_chr(dt$zip[dt$match_tier == "C"], ""),
    coalesce_chr(dt$inf_year[dt$match_tier == "C"], ""),
    sep = "|"
  )
  out
}

extract_fastlink_pairs <- function(fl_obj, A_dt, B_dt) {
  if (is.null(fl_obj)) return(data.table())
  m <- fl_obj$matches
  p <- as.numeric(fl_obj$posterior)
  if (is.null(m)) return(data.table())

  if (is.list(m) && !is.null(m$inds.a) && !is.null(m$inds.b)) {
    ia <- as.integer(m$inds.a)
    ib <- as.integer(m$inds.b)
  } else if (is.data.frame(m) && all(c("inds.a", "inds.b") %in% names(m))) {
    ia <- as.integer(m$inds.a)
    ib <- as.integer(m$inds.b)
  } else {
    return(data.table())
  }

  ok <- !is.na(ia) & !is.na(ib) &
    ia >= 1L & ia <= nrow(A_dt) &
    ib >= 1L & ib <= nrow(B_dt)
  ia <- ia[ok]
  ib <- ib[ok]
  p <- p[seq_len(min(length(p), length(ia)))]
  ia <- ia[seq_len(length(p))]
  ib <- ib[seq_len(length(p))]

  if (length(ia) == 0L) return(data.table())
  data.table(
    evict_person_id = A_dt$evict_person_id[ia],
    inf_person_id = B_dt$inf_person_id[ib],
    p_match_fastlink = p
  )
}

run_fastlink_block <- function(sub_dt) {
  if (!requireNamespace("fastLink", quietly = TRUE)) return(data.table())
  if (nrow(sub_dt) == 0L) return(data.table())

  A <- unique(sub_dt[, .(
    evict_person_id,
    first_name = first_name_ev_norm,
    last_name = last_name_ev_norm,
    street_number = suppressWarnings(as.numeric(street_number_ev)),
    street_name = street_name_ev
  )], by = "evict_person_id")
  B <- unique(sub_dt[, .(
    inf_person_id,
    first_name = first_name_inf_norm,
    last_name = last_name_inf_norm,
    street_number = suppressWarnings(as.numeric(street_number_inf)),
    street_name = street_name_inf
  )], by = "inf_person_id")

  if (nrow(A) == 0L || nrow(B) == 0L) return(data.table())
  if (nrow(A) == 1L && nrow(B) == 1L) return(data.table())

  has_street_num <- !all(is.na(A$street_number)) && !all(is.na(B$street_number))
  has_street_name <- !all(is.na(A$street_name) | !nzchar(A$street_name)) &&
    !all(is.na(B$street_name) | !nzchar(B$street_name))

  # Plan requirement: do not run fastLink with only first/last name fields.
  if (!(has_street_num && has_street_name)) return(data.table())

  # Keep stable row order for index mapping.
  setorder(A, evict_person_id)
  setorder(B, inf_person_id)

  dfA <- as.data.frame(A[, .(first_name, last_name, street_number, street_name)])
  dfB <- as.data.frame(B[, .(first_name, last_name, street_number, street_name)])

  fl <- suppressWarnings({
    capture.output({
      fl0 <- fastLink::fastLink(
        dfA = dfA,
        dfB = dfB,
        varnames = c("first_name", "last_name", "street_number", "street_name"),
        stringdist.match = c("first_name", "last_name", "street_name"),
        numeric.match = c("street_number"),
        threshold.match = 0,
        dedupe.matches = FALSE,
        return.all = TRUE,
        n.cores = 1,
        verbose = FALSE
      )
    })
    fl0
  })

  extract_fastlink_pairs(fl, A, B)
}

run_fastlink_on_pairs <- function(candidates_dt, nickname_dict = NULL, fastlink_chunk_evict_n = NA_integer_, log_file = NULL) {
  if (!is.data.table(candidates_dt)) candidates_dt <- as.data.table(candidates_dt)
  if (nrow(candidates_dt) == 0L) return(candidates_dt)

  dt <- copy(candidates_dt)
  sim_thr <- read_name_similarity_thresholds()

  dt[, fn_match_score := first_name_score(first_name_ev_norm, first_name_inf_norm, nickname_dict)]
  dt[, ln_match_score := first_name_score(last_name_ev_norm, last_name_inf_norm, nick_dt = NULL)]
  dt[, last_name_exact := !is.na(last_name_ev_norm) & !is.na(last_name_inf_norm) &
       last_name_ev_norm == last_name_inf_norm]
  dt[, gender_agreement := gender_compatible(gender_ev, gender_inf)]
  dt[, street_number_exact := !is.na(street_number_ev) & !is.na(street_number_inf) &
       street_number_ev == street_number_inf]
  dt[, street_name_score := first_name_score(street_name_ev, street_name_inf, nick_dt = NULL)]
  dt[, has_addr_components := !is.na(street_number_ev) & !is.na(street_number_inf) &
       !is.na(street_name_ev) & !is.na(street_name_inf)]

  dt[, name_gate_pass := !is.na(fn_match_score) & !is.na(ln_match_score) &
       fn_match_score >= sim_thr$min_first & ln_match_score >= sim_thr$min_last]
  n_before_name_gate <- nrow(dt)
  dt <- dt[name_gate_pass == TRUE]
  if (exists("logf", mode = "function")) {
    logf("Name similarity gate (first>=", round(sim_thr$min_first, 3),
         ", last>=", round(sim_thr$min_last, 3), "): kept ",
         nrow(dt), " / ", n_before_name_gate, " candidate rows",
         log_file = log_file)
  }
  if (nrow(dt) == 0L) return(dt)

  # Scaffold posterior proxy: names + address components only.
  dt[, p_match := 0.50 * fifelse(is.na(ln_match_score), 0, ln_match_score) +
       0.30 * fifelse(is.na(fn_match_score), 0, fn_match_score) +
       0.10 * fifelse(is.na(street_name_score), 0, street_name_score) +
       0.10 * as.numeric(street_number_exact)]
  dt[, p_match := pmax(0, pmin(0.999, p_match))]

  # Run fastLink by strict blocking key and overwrite proxy scores where available.
  fastlink_used <- FALSE
  n_fastlink_blocks <- 0L
  n_fastlink_pairs <- 0L
  n_fastlink_fail <- 0L
  n_fastlink_timeout <- 0L
  if (requireNamespace("fastLink", quietly = TRUE)) {
    dt_fast <- dt[has_addr_components == TRUE]
    if (nrow(dt_fast) == 0L) {
      if (exists("logf", mode = "function")) {
        logf("fastLink skipped: no address-eligible candidate rows", log_file = log_file)
      }
    } else {
      dt_fast[, block_id := build_fastlink_block_id(.SD)]
      dt_fast <- dt_fast[!is.na(block_id) & nzchar(block_id)]

      if (nrow(dt_fast) == 0L) {
        if (exists("logf", mode = "function")) {
          logf("fastLink skipped: no non-missing block ids in address-eligible rows", log_file = log_file)
        }
      } else {
        t0 <- proc.time()[["elapsed"]]
        blk_sizes <- dt_fast[, .(
          n_rows = .N,
          n_ev = uniqueN(evict_person_id),
          n_inf = uniqueN(inf_person_id),
          n_addr = sum(has_addr_components %in% TRUE)
        ), by = block_id]
        elapsed_blk <- round(proc.time()[["elapsed"]] - t0, 2)
        msg1 <- paste0("blocks total: ", nrow(blk_sizes))
        msg2 <- paste0("blocks 1xK: ", blk_sizes[n_ev == 1L & n_inf > 1L, .N])
        msg3 <- paste0("blocks Kx1: ", blk_sizes[n_ev > 1L & n_inf == 1L, .N])
        msg4 <- paste0("blocks KxK: ", blk_sizes[n_ev > 1L & n_inf > 1L, .N])
        msg5 <- paste0("elapsed to build blk_sizes: ", elapsed_blk, "s")
        message(msg1)
        message(msg2)
        message(msg3)
        message(msg4)
        message(msg5)
        if (exists("logf", mode = "function")) {
          logf(msg1, log_file = log_file)
          logf(msg2, log_file = log_file)
          logf(msg3, log_file = log_file)
          logf(msg4, log_file = log_file)
          logf(msg5, log_file = log_file)
        }

        # Only run fastLink on truly ambiguous KxK blocks.
        keep_tbl <- blk_sizes[n_ev > 1L & n_inf > 1L]
        skip_non_kxk_blocks <- nrow(blk_sizes) - nrow(keep_tbl)

        max_block_rows <- suppressWarnings(as.integer(Sys.getenv("PHILLY_FASTLINK_MAX_BLOCK_ROWS", unset = "0")))
        if (is.na(max_block_rows) || max_block_rows < 0L) max_block_rows <- 0L

        min_block_rows <- suppressWarnings(as.integer(Sys.getenv("PHILLY_FASTLINK_MIN_BLOCK_ROWS", unset = "0")))
        if (is.na(min_block_rows) || min_block_rows < 0L) min_block_rows <- 0L

        skip_oversized_blocks <- 0L
        if (max_block_rows > 0L && nrow(keep_tbl) > 0L) {
          skip_oversized_blocks <- keep_tbl[n_rows > max_block_rows, .N]
          keep_tbl <- keep_tbl[n_rows <= max_block_rows]
        }

        skip_undersized_blocks <- 0L
        if (min_block_rows > 0L && nrow(keep_tbl) > 0L) {
          skip_undersized_blocks <- keep_tbl[n_rows < min_block_rows, .N]
          keep_tbl <- keep_tbl[n_rows >= min_block_rows]
        }

        keep_blocks <- keep_tbl$block_id
        max_rows <- if (nrow(blk_sizes) > 0L) max(blk_sizes$n_rows) else 0L
        if (exists("logf", mode = "function")) {
          logf(
            "fastLink blocks: total=", nrow(blk_sizes),
            "; eligible_kxk=", length(keep_blocks),
            "; skipped_non_kxk=", skip_non_kxk_blocks,
            "; skipped_oversized=", skip_oversized_blocks,
            "; skipped_undersized=", skip_undersized_blocks,
            "; max_block_rows=", max_rows,
            "; max_block_rows_cap=", max_block_rows,
            "; min_block_rows_cap=", min_block_rows,
            log_file = log_file
          )
        }

        if (length(keep_blocks) > 0L) {
          dt_fast <- dt_fast[block_id %chin% keep_blocks]
          setkey(dt_fast, block_id)

          chunk_n <- suppressWarnings(as.integer(fastlink_chunk_evict_n))
          if (is.na(chunk_n) || chunk_n <= 0L) chunk_n <- NA_integer_

          keep_tbl[, chunk_id := 1L]
          if (!is.na(chunk_n)) {
            ev_map <- unique(dt_fast[, .(evict_person_id)])
            setorder(ev_map, evict_person_id)
            ev_map[, ev_chunk_id := ((seq_len(.N) - 1L) %/% chunk_n) + 1L]
            blk_map <- unique(
              dt_fast[, .(block_id, evict_person_id)][ev_map, on = "evict_person_id", nomatch = 0L][,
                                                     .(chunk_id = min(ev_chunk_id)),
                                                     by = block_id]
            )
            if (nrow(blk_map) > 0L) {
              keep_tbl <- blk_map[keep_tbl, on = "block_id"]
              keep_tbl[is.na(chunk_id), chunk_id := 1L]
            }
            if (exists("logf", mode = "function")) {
              logf("fastLink chunking ON: eviction persons per chunk=", chunk_n,
                   "; chunks=", keep_tbl[, uniqueN(chunk_id)],
                   log_file = log_file)
            }
          }

          setorder(keep_tbl, chunk_id, block_id)
          fl_res <- vector("list", nrow(keep_tbl))
          n_progress_every <- 500L
          block_timeout_sec <- suppressWarnings(as.numeric(Sys.getenv("PHILLY_FASTLINK_BLOCK_TIMEOUT_SEC", unset = "0")))
          if (is.na(block_timeout_sec) || block_timeout_sec < 0) block_timeout_sec <- 0
          row_idx <- 0L
          chunk_ids <- sort(unique(keep_tbl$chunk_id))
          t_fast_start <- proc.time()[["elapsed"]]
          for (cid in chunk_ids) {
            chunk_tbl <- keep_tbl[chunk_id == cid]
            if (exists("logf", mode = "function")) {
              logf("fastLink chunk ", cid, ": blocks=", nrow(chunk_tbl), log_file = log_file)
            }
            for (j in seq_len(nrow(chunk_tbl))) {
              row_idx <- row_idx + 1L
              bid <- chunk_tbl$block_id[j]
              sub <- dt_fast[J(bid)]
              one <- tryCatch(
                {
                  if (block_timeout_sec > 0) {
                    setTimeLimit(cpu = Inf, elapsed = block_timeout_sec, transient = TRUE)
                    on.exit(setTimeLimit(cpu = Inf, elapsed = Inf, transient = FALSE), add = TRUE)
                  }
                  run_fastlink_block(sub)
                },
                error = function(e) {
                  msg <- conditionMessage(e)
                  if (grepl("elapsed time limit", msg, ignore.case = TRUE)) {
                    n_fastlink_timeout <<- n_fastlink_timeout + 1L
                    if (exists("logf", mode = "function") && n_fastlink_timeout %% 100L == 0L) {
                      logf("fastLink timeout count: ", n_fastlink_timeout, log_file = log_file)
                    }
                  } else {
                    n_fastlink_fail <<- n_fastlink_fail + 1L
                    if (exists("logf", mode = "function")) {
                      logf("fastLink block failed (", bid, "): ", msg, log_file = log_file)
                    }
                  }
                  data.table()
                }
              )
              if (nrow(one) > 0L) {
                one[, block_id := bid]
                fl_res[[row_idx]] <- one
              } else {
                fl_res[[row_idx]] <- data.table()
              }

              if (exists("logf", mode = "function") &&
                  (row_idx %% n_progress_every == 0L || row_idx == nrow(keep_tbl))) {
                logf("fastLink progress: ", row_idx, "/", nrow(keep_tbl), " blocks", log_file = log_file)
              }
            }
          }

          if (exists("logf", mode = "function")) {
            logf("fastLink elapsed seconds: ", round(proc.time()[["elapsed"]] - t_fast_start, 2), log_file = log_file)
          }

          fl_pairs <- rbindlist(fl_res, use.names = TRUE, fill = TRUE)
          if (nrow(fl_pairs) > 0L) {
            fastlink_used <- TRUE
            n_fastlink_pairs <- nrow(fl_pairs)
            n_fastlink_blocks <- uniqueN(fl_pairs$block_id)
            setkey(fl_pairs, evict_person_id, inf_person_id)
            setkey(dt, evict_person_id, inf_person_id)
            dt[fl_pairs, p_match := i.p_match_fastlink]
            dt[, p_match := pmax(0, pmin(0.999, p_match))]
          }
        }
      }
    }
  } else if (exists("logf", mode = "function")) {
    logf("fastLink package not installed; using proxy deterministic scores only", log_file = log_file)
  }

  setorder(dt, evict_person_id, -p_match, -ln_match_score, -fn_match_score, familyid, inf_person_id)
  dt[, score_rank := seq_len(.N), by = evict_person_id]
  dt[, best_p := p_match[1L], by = evict_person_id]
  dt[, second_p := fifelse(.N >= 2L, p_match[2L], NA_real_), by = evict_person_id]
  dt[, top2_margin := best_p - fifelse(is.na(second_p), 0, second_p)]
  dt[, is_top_candidate := score_rank == 1L]

  if (exists("logf", mode = "function")) {
    logf(
      "Scored candidate pairs: ", nrow(dt),
      "; fastLink_used=", fastlink_used,
      "; fastLink_blocks=", n_fastlink_blocks,
      "; fastLink_pairs=", n_fastlink_pairs,
      "; fastLink_failures=", n_fastlink_fail,
      "; fastLink_timeouts=", n_fastlink_timeout,
      log_file = log_file
    )
  }

  dt
}

run_direct_exact_passes <- function(ev_dt, inf_dt, log_file = NULL) {
  if (!is.data.table(ev_dt)) ev_dt <- as.data.table(ev_dt)
  if (!is.data.table(inf_dt)) inf_dt <- as.data.table(inf_dt)

  req_ev <- c(
    "evict_id", "evict_person_id", "filing_yr", "bldg_size_bin", "gender",
    "first_name", "last_name", "first_name_norm", "last_name_norm",
    "address_norm", "street_name"
  )
  req_inf <- c(
    "familyid", "person_num", "inf_person_id", "year", "gender",
    "first_name", "last_name", "first_name_norm", "last_name_norm",
    "address_norm", "street_name"
  )
  if (!all(req_ev %in% names(ev_dt)) || !all(req_inf %in% names(inf_dt))) {
    if (exists("logf", mode = "function")) {
      logf("Direct exact passes skipped: missing required columns", log_file = log_file)
    }
    return(list(person_matches = data.table(), remaining_ev = copy(ev_dt)))
  }

  ev_work <- copy(ev_dt)
  out_parts <- list()
  out_idx <- 0L

  run_one_pass <- function(pass_name, tier_name, on_cols, p_val) {
    nonlocal_out <- list(matched = data.table(), matched_ids = character())
    if (nrow(ev_work) == 0L || nrow(inf_dt) == 0L) return(nonlocal_out)

    ev_ready <- ev_work
    inf_ready <- inf_dt
    for (cc in on_cols) {
      ev_ready <- ev_ready[!is.na(get(cc)) & nzchar(as.character(get(cc)))]
      inf_ready <- inf_ready[!is.na(get(cc)) & nzchar(as.character(get(cc)))]
    }
    if (nrow(ev_ready) == 0L || nrow(inf_ready) == 0L) return(nonlocal_out)

    cand <- ev_ready[inf_ready, on = on_cols, allow.cartesian = TRUE, nomatch = 0L]
    if (nrow(cand) == 0L) return(nonlocal_out)

    pick_col <- function(dt, nm) {
      if (paste0("i.", nm) %in% names(dt)) return(paste0("i.", nm))
      if (nm %in% names(dt)) return(nm)
      NA_character_
    }
    fam_col <- pick_col(cand, "familyid")
    pnum_col <- pick_col(cand, "person_num")
    ipid_col <- pick_col(cand, "inf_person_id")
    iyear_col <- pick_col(cand, "year")
    ifn_col <- pick_col(cand, "first_name_norm")
    ig_col <- pick_col(cand, "gender")
    ifirst_col <- pick_col(cand, "first_name")
    ilast_col <- pick_col(cand, "last_name")
    if (any(is.na(c(fam_col, pnum_col, ipid_col, iyear_col, ifn_col, ig_col, ifirst_col, ilast_col)))) {
      return(nonlocal_out)
    }

    cand[, inf_familyid := as.character(get(fam_col))]
    cand[, inf_person_num := suppressWarnings(as.integer(get(pnum_col)))]
    cand[, inf_person_id_std := as.character(get(ipid_col))]
    cand[, inf_year_std := suppressWarnings(as.integer(get(iyear_col)))]
    cand[, inf_first_name_norm_std := as.character(get(ifn_col))]
    cand[, inf_gender_std := as.character(get(ig_col))]
    cand[, inf_first_name_std := as.character(get(ifirst_col))]
    cand[, inf_last_name_std := as.character(get(ilast_col))]

    fam_n <- cand[, .(n_family = uniqueN(inf_familyid)), by = evict_person_id]
    ok_ids <- fam_n[n_family == 1L, evict_person_id]
    if (length(ok_ids) == 0L) return(nonlocal_out)

    cand <- cand[evict_person_id %chin% ok_ids]
    cand[, has_person_num := as.integer(!is.na(inf_person_num))]
    setorder(cand, evict_person_id, -has_person_num, inf_familyid, inf_person_id_std)
    best <- cand[, .SD[1L], by = evict_person_id]

    fn_sc <- first_name_score(best$first_name_norm, best$inf_first_name_norm_std, nick_dt = NULL)
    g_ag <- gender_compatible(best$gender, best$inf_gender_std)

    matched <- best[, .(
      evict_id = evict_id,
      evict_person_id = evict_person_id,
      familyid = inf_familyid,
      person_num = inf_person_num,
      inf_person_id = inf_person_id_std,
      match_tier = tier_name,
      filing_yr = suppressWarnings(as.integer(filing_yr)),
      inf_year = inf_year_std,
      year_link_used = 0L,
      bldg_size_bin = bldg_size_bin,
      p_match = as.numeric(p_val),
      fn_match_score = fn_sc,
      top2_margin = 1,
      gender_agreement = g_ag,
      ambiguous_flag = FALSE,
      needs_review = FALSE,
      direct_match_rule = pass_name,
      first_name_inf = inf_first_name_std,
      last_name_inf = inf_last_name_std,
      schema_version = schema_version_tag()
    )]

    nonlocal_out$matched <- matched
    nonlocal_out$matched_ids <- matched$evict_person_id
    nonlocal_out
  }

  pass_specs <- list(
    list(name = "addr_fullname_exact", tier = "A", on = c("address_norm", "first_name_norm", "last_name_norm"), p = 0.999),
    list(name = "street_fullname_exact", tier = "C", on = c("street_name", "first_name_norm", "last_name_norm"), p = 0.990)
  )

  for (ps in pass_specs) {
    res <- run_one_pass(ps$name, ps$tier, ps$on, ps$p)
    n_hit <- nrow(res$matched)
    if (n_hit > 0L) {
      out_idx <- out_idx + 1L
      out_parts[[out_idx]] <- res$matched
      ev_work <- ev_work[!evict_person_id %chin% res$matched_ids]
    }
    if (exists("logf", mode = "function")) {
      logf("Direct exact pass ", ps$name, ": matched ", n_hit,
           "; remaining eviction persons=", nrow(ev_work),
           log_file = log_file)
    }
  }

  person_matches <- rbindlist(out_parts, use.names = TRUE, fill = TRUE)
  list(person_matches = person_matches, remaining_ev = ev_work)
}

assign_thresholds <- function(bldg_size_bin) {
  b <- coalesce_chr(bldg_size_bin, "unknown_size")

  p_thr <- fifelse(
    b == "3-4", 0.76,
    fifelse(b == "5-9", 0.77,
      fifelse(b == "10-19", 0.78,
        fifelse(b == "20-49", 0.79,
          fifelse(b == "50+", 0.80, 0.80)
        )
      )
    )
  )

  m_thr <- fifelse(
    b == "3-4", 0.02,
    fifelse(b == "5-9", 0.03,
      fifelse(b == "10-19", 0.04,
        fifelse(b == "20-49", 0.05,
          fifelse(b == "50+", 0.06, 0.06)
        )
      )
    )
  )

  list(p_threshold = p_thr, margin_threshold = m_thr)
}

apply_size_thresholds <- function(scored_dt, log_file = NULL) {
  if (!is.data.table(scored_dt)) scored_dt <- as.data.table(scored_dt)
  if (nrow(scored_dt) == 0L) return(scored_dt)

  dt <- copy(scored_dt)
  sim_thr <- read_name_similarity_thresholds()
  th <- assign_thresholds(dt$bldg_size_bin)
  dt[, p_threshold := th$p_threshold]
  dt[, margin_threshold := th$margin_threshold]

  dt[, p_threshold_adj := p_threshold]

  dt[, deterministic_pass := (
    bldg_size_bin %chin% c("1", "2") &
      match_tier %chin% c("A", "B") &
      has_addr_components == TRUE &
      fn_match_score >= sim_thr$min_first &
      ln_match_score >= sim_thr$min_last
  )]
  dt[, deterministic_unique := sum(deterministic_pass, na.rm = TRUE) == 1L, by = evict_person_id]

  dt[, fastlink_pass := (
    !bldg_size_bin %chin% c("1", "2") &
      is_top_candidate == TRUE &
      has_addr_components == TRUE &
      fn_match_score >= sim_thr$min_first &
      ln_match_score >= sim_thr$min_last &
      p_match >= p_threshold_adj &
      top2_margin >= margin_threshold
  )]

  dt[, fallback_name_pass := FALSE]

  dt[, accepted_flag := (deterministic_pass & deterministic_unique) | fastlink_pass]

  dt[, needs_review := (
    is_top_candidate == TRUE &
      accepted_flag == FALSE &
      p_match >= pmax(0, p_threshold_adj - 0.02)
  )]

  dt[, ambiguous_flag := FALSE]
  dt[is_top_candidate == TRUE & !accepted_flag & top2_margin < margin_threshold,
     ambiguous_flag := TRUE]

  if (exists("logf", mode = "function")) {
    logf("Accepted links before one-to-one resolution: ", dt[accepted_flag == TRUE, .N],
         log_file = log_file)
  }

  dt
}

resolve_one_to_one_deterministic <- function(scored_dt, log_file = NULL) {
  if (!is.data.table(scored_dt)) scored_dt <- as.data.table(scored_dt)
  if (nrow(scored_dt) == 0L) return(scored_dt)

  acc <- copy(scored_dt[accepted_flag == TRUE])
  if (nrow(acc) == 0L) return(acc)

  tier_rank_map <- c(A = 1L, B = 2L, C = 3L)
  acc[, tier_rank := tier_rank_map[match_tier]]
  acc[, year_diff_abs := abs(as.integer(inf_year) - as.integer(filing_yr))]

  # Person-stage keeps only one best InfoUSA person per eviction person.
  # Cross-case household/person collisions are resolved at case-household stage.
  setorder(acc, evict_person_id, tier_rank, -p_match, -top2_margin, -fn_match_score, year_diff_abs, familyid, inf_person_id)
  acc <- acc[, .SD[1L], by = evict_person_id]

  acc[, schema_version := schema_version_tag()]

  if (exists("logf", mode = "function")) {
    logf("Accepted links after one-to-one resolution: ", nrow(acc), log_file = log_file)
  }

  acc
}

rollup_to_case_household <- function(person_match_dt, log_file = NULL) {
  if (!is.data.table(person_match_dt)) person_match_dt <- as.data.table(person_match_dt)
  if (nrow(person_match_dt) == 0L) return(data.table())

  tier_rank_map <- c(A = 1L, B = 2L, C = 3L)
  dt <- copy(person_match_dt)
  dt[, tier_rank := tier_rank_map[match_tier]]
  dt[, year_diff_abs := abs(as.integer(inf_year) - as.integer(filing_yr))]

  hh <- dt[, .(
    n_person_links = .N,
    matched_person_nums = collapse_person_nums(person_num),
    match_tier = {
      tr <- suppressWarnings(min(tier_rank, na.rm = TRUE))
      if (is.infinite(tr) || is.na(tr)) NA_character_ else c("A", "B", "C")[tr]
    },
    bldg_size_bin = bldg_size_bin[1L],
    year_link_used = year_link_used[which.max(fifelse(is.na(p_match), -Inf, p_match))][1L],
    p_match = if (all(is.na(p_match))) NA_real_ else max(p_match, na.rm = TRUE),
    fn_match_score = if (all(is.na(fn_match_score))) NA_real_ else max(fn_match_score, na.rm = TRUE),
    top2_margin = if (all(is.na(top2_margin))) NA_real_ else max(top2_margin, na.rm = TRUE),
    gender_agreement = fifelse(
      all(is.na(gender_agreement)), NA,
      fifelse(any(gender_agreement == FALSE, na.rm = TRUE), FALSE, TRUE)
    )
  ), by = .(evict_id, familyid)]

  hh[, ambiguous_household := .N > 1L, by = evict_id]
  hh[, needs_review := FALSE]
  hh[ambiguous_household == TRUE, needs_review := TRUE]
  hh[, ambiguous_flag := top2_margin < 0.05]

  if (exists("logf", mode = "function")) {
    logf("Case-household candidates built: ", nrow(hh),
         "; ambiguous_household cases=", hh[ambiguous_household == TRUE, uniqueN(evict_id)],
         log_file = log_file)
  }

  hh
}

resolve_case_household_deterministic <- function(case_hh_dt, log_file = NULL) {
  if (!is.data.table(case_hh_dt)) case_hh_dt <- as.data.table(case_hh_dt)
  if (nrow(case_hh_dt) == 0L) return(case_hh_dt)

  tier_rank_map <- c(A = 1L, B = 2L, C = 3L)
  keep <- copy(case_hh_dt[ambiguous_household != TRUE])
  if (nrow(keep) == 0L) return(keep)

  keep[, tier_rank := tier_rank_map[match_tier]]
  keep[, year_diff_abs := abs(as.integer(year_link_used))]

  # Resolve same familyid claimed by multiple eviction cases.
  setorder(keep, familyid, tier_rank, -p_match, -top2_margin, -fn_match_score, year_diff_abs, evict_id)
  keep <- keep[, .SD[1L], by = familyid]

  # Enforce one household per eviction case.
  setorder(keep, evict_id, tier_rank, -p_match, -top2_margin, -fn_match_score, year_diff_abs, familyid)
  keep <- keep[, .SD[1L], by = evict_id]

  keep[, schema_version := schema_version_tag()]
  keep[, c("tier_rank", "year_diff_abs") := NULL]

  if (exists("logf", mode = "function")) {
    logf("Final case-household matches after deterministic resolution: ", nrow(keep), log_file = log_file)
  }

  keep
}
