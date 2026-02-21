## ============================================================
## link_evictions_infousa_qc.R
## ============================================================
## QC helpers for eviction defendant <-> InfoUSA person linkage outputs.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
})

qc_tables <- function(ev_dt, candidates_dt, scored_dt, matches_dt, candidate_stats_dt) {
  out <- list()

  keep_cols <- intersect(c("evict_id", "bldg_size_bin", "bg_geoid", "common_lastname_flag"), names(ev_dt))
  if (!"evict_id" %in% keep_cols) stop("qc_tables requires ev_dt to include evict_id")
  ev_case <- unique(ev_dt[, ..keep_cols])
  n_ev <- nrow(ev_case)
  n_match <- nrow(matches_dt)

  out$match_rate_overall <- data.table(
    n_eviction_people = n_ev,
    n_matched = n_match,
    match_rate = fifelse(n_ev > 0, n_match / n_ev, NA_real_)
  )

  out$match_rate_by_tier <- matches_dt[, .N, by = .(match_tier)][
    , .(match_tier, n_matched = N, share_matched = N / pmax(1, n_match))
  ][order(match_tier)]

  out$match_rate_by_size <- ev_case[, .(n_eviction_cases = .N), by = .(bldg_size_bin)]
  if (nrow(matches_dt) > 0L) {
    tmp <- unique(matches_dt[, .(evict_id, bldg_size_bin)])[ , .(n_matched = .N), by = .(bldg_size_bin)]
    out$match_rate_by_size <- tmp[out$match_rate_by_size, on = "bldg_size_bin"]
    out$match_rate_by_size[is.na(n_matched), n_matched := 0L]
    out$match_rate_by_size[, match_rate := n_matched / pmax(1, n_eviction_cases)]
  } else {
    out$match_rate_by_size[, `:=`(n_matched = 0L, match_rate = 0)]
  }

  if ("bg_geoid" %in% names(ev_case)) {
    out$match_rate_by_bg <- ev_case[, .(n_eviction_cases = .N), by = .(bg_geoid)]
    if (nrow(matches_dt) > 0L && "bg_geoid" %in% names(matches_dt)) {
      tmp <- unique(matches_dt[, .(evict_id, bg_geoid)])[ , .(n_matched = .N), by = .(bg_geoid)]
      out$match_rate_by_bg <- tmp[out$match_rate_by_bg, on = "bg_geoid"]
      out$match_rate_by_bg[is.na(n_matched), n_matched := 0L]
      out$match_rate_by_bg[, match_rate := n_matched / pmax(1, n_eviction_cases)]
    } else {
      out$match_rate_by_bg[, `:=`(n_matched = 0L, match_rate = 0)]
    }
  }

  if ("common_lastname_flag" %in% names(ev_case)) {
    out$match_rate_by_name_freq <- ev_case[, .(n_eviction_cases = .N), by = .(common_lastname_flag)]
    if (nrow(matches_dt) > 0L && "common_lastname_flag" %in% names(matches_dt)) {
      tmp <- unique(matches_dt[, .(evict_id, common_lastname_flag)])[ , .(n_matched = .N), by = .(common_lastname_flag)]
      out$match_rate_by_name_freq <- tmp[out$match_rate_by_name_freq, on = "common_lastname_flag"]
      out$match_rate_by_name_freq[is.na(n_matched), n_matched := 0L]
      out$match_rate_by_name_freq[, match_rate := n_matched / pmax(1, n_eviction_cases)]
    } else {
      out$match_rate_by_name_freq[, `:=`(n_matched = 0L, match_rate = 0)]
    }
  }

  top_scored <- scored_dt[is_top_candidate == TRUE]
  out$risk_indicators <- data.table(
    ambiguous_share = top_scored[ , mean(ambiguous_flag %in% TRUE, na.rm = TRUE)],
    grayzone_share = top_scored[ , mean(needs_review %in% TRUE, na.rm = TRUE)],
    pre_dedupe_many_to_one = if (nrow(candidates_dt) > 0L) {
      candidates_dt[, .N, by = familyid][N > 1L, .N]
    } else {
      0L
    },
    capped_share = if (nrow(candidate_stats_dt) > 0L) mean(candidate_stats_dt$was_capped %in% TRUE) else 0,
    ambiguous_household_share = if (nrow(matches_dt) > 0L) mean(matches_dt$ambiguous_household %in% TRUE, na.rm = TRUE) else 0
  )

  out$p_match_hist <- scored_dt[
    , .N,
    by = .(bldg_size_bin, p_bin = cut(p_match, breaks = seq(0, 1, by = 0.05), include.lowest = TRUE))
  ][order(bldg_size_bin, p_bin)]

  out$fn_match_hist <- scored_dt[
    , .N,
    by = .(match_tier, fn_bin = cut(fn_match_score, breaks = seq(0, 1, by = 0.05), include.lowest = TRUE))
  ][order(match_tier, fn_bin)]

  out
}

write_qc_tables <- function(qc_list, out_dir, prefix = "evict_infousa") {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  for (nm in names(qc_list)) {
    obj <- qc_list[[nm]]
    if (!is.data.table(obj)) next
    fwrite(obj, file.path(out_dir, paste0(prefix, "_", nm, ".csv")))
  }

  invisible(TRUE)
}

write_review_samples <- function(scored_dt,
                                 out_dir,
                                 n_highconf = 200L,
                                 n_gray = 200L,
                                 seed = 123L) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  if (!is.data.table(scored_dt) || nrow(scored_dt) == 0L) {
    fwrite(data.table(), file.path(out_dir, "review_highconf.csv"))
    fwrite(data.table(), file.path(out_dir, "review_grayzone.csv"))
    return(invisible(TRUE))
  }

  set.seed(as.integer(seed))

  top <- scored_dt[is_top_candidate == TRUE]

  high <- top[accepted_flag == TRUE & p_match >= 0.98]
  gray <- top[needs_review == TRUE]

  if (nrow(high) > n_highconf) high <- high[sample(.N, n_highconf)]
  if (nrow(gray) > n_gray) gray <- gray[sample(.N, n_gray)]

  keep_cols <- intersect(c(
    "evict_id", "evict_person_id", "familyid", "person_num", "match_tier", "bldg_size_bin",
    "first_name_ev", "last_name_ev", "first_name_inf", "last_name_inf",
    "n_sn_ss_c", "PID", "p_match", "fn_match_score", "top2_margin", "matched_person_nums",
    "ambiguous_flag", "needs_review", "accepted_flag"
  ), names(top))

  fwrite(high[, ..keep_cols], file.path(out_dir, "review_highconf.csv"))
  fwrite(gray[, ..keep_cols], file.path(out_dir, "review_grayzone.csv"))

  invisible(TRUE)
}
