## ============================================================
## make-address-parcel-xwalk.R
## ============================================================
## Purpose: Create reproducible, auditable address→parcel crosswalks
##          using a tiered matching strategy (exact → relaxed → fuzzy).
##
## Inputs:
##   - cfg$products$parcels_clean (clean/parcels_clean.csv)
##   - cfg$products$evictions_clean (clean/evictions_clean.csv)
##   - cfg$products$infousa_clean (clean/infousa_clean.csv)
##   - cfg$products$altos_clean (clean/altos_clean.csv)
##
## Outputs (per source X):
##   - cfg$products$xwalk_X_to_parcel (winner crosswalk)
##   - cfg$products$match_audit_X (candidate-level audit table)
##   - cfg$products$match_summary_X (summary statistics)
##
## Primary key: source_address_id (unique per source)
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(stringdist)
  library(digest)
})

# ---- Load config and set up logging ----
source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "make-address-parcel-xwalk.log")

logf("=== Starting make-address-parcel-xwalk.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# ============================================================
# MATCHING PARAMETERS (VERSIONED)
# ============================================================

MATCH_PARAMS <- list(
  version = "1.1.0",

  # Fuzzy matching thresholds
  fuzzy_threshold = 0.85,
  gap_threshold = 0.05,

  # Component weights for fuzzy scoring
  weights = list(
    street_sim = 0.40,
    house_match = 0.30,
    suffix_match = 0.10,
    dir_match = 0.10,
    zip_match = 0.10
  ),


  # Blocking parameters
  house_diff_max = 2,
  near_number_house_diff_max = 10,
  street_prefix_len = 3
)

logf("Match parameters version: ", MATCH_PARAMS$version, log_file = log_file)

# ============================================================
# HELPER FUNCTIONS
# ============================================================

#' Create a stable, deterministic address ID from components
#' @param house House number
#' @param street Street name
#' @param suffix Street suffix
#' @param zip Zip code
#' @return Character vector of hash-based IDs
create_stable_id <- function(house, street, suffix, zip) {
  # Normalize components
  house <- as.character(house) %>% replace_na("")
  street <- str_to_lower(street) %>% str_squish() %>% replace_na("")
  suffix <- str_to_lower(suffix) %>% str_squish() %>% replace_na("")
  zip <- as.character(zip) %>% str_sub(1, 5) %>% replace_na("")

  # Create canonical string and hash
  canonical <- paste(house, street, suffix, zip, sep = "|")
  sapply(canonical, function(x) digest(x, algo = "xxhash32"), USE.NAMES = FALSE)
}

#' Condo flag for parcels (mirrors `standardize_building_type()` condo detection in r/helper-functions.R)
infer_is_condo_parcel <- function(old_desc, new_desc) {
  old_desc <- str_to_upper(replace_na(as.character(old_desc), ""))
  new_desc <- str_to_upper(replace_na(as.character(new_desc), ""))
  grepl("CONDO", old_desc) | grepl("CONDO", new_desc)
}

#' Build deterministic condo parcel groups using common parcel address keys.
#' Phase 1 uses shared `n_sn_ss_c` among condo parcels as the grouping key.
build_condo_parcel_groups <- function(parcels_dt) {
  stopifnot(is.data.table(parcels_dt))
  assert_has_cols(parcels_dt, c("parcel_number", "n_sn_ss_c", "building_code_description", "building_code_description_new"))

  m <- copy(parcels_dt)[, .(
    parcel_number = as.character(parcel_number),
    n_sn_ss_c = str_squish(str_to_lower(replace_na(as.character(n_sn_ss_c), ""))),
    is_condo = infer_is_condo_parcel(building_code_description, building_code_description_new)
  )]

  m[, n_sn_ss_c := fifelse(nzchar(n_sn_ss_c), n_sn_ss_c, NA_character_)]
  m <- unique(m, by = c("parcel_number", "n_sn_ss_c"))
  assert_unique(m, c("parcel_number"), "parcels_dt (for condo grouping)")

  out <- copy(m)
  out[, `:=`(
    condo_group_id = NA_character_,
    anchor_pid = NA_character_,
    group_size_pids = NA_integer_,
    group_n_parcel_addresses = NA_integer_
  )]

  condo_with_addr <- out[is_condo == TRUE & !is.na(n_sn_ss_c)]
  if (nrow(condo_with_addr) > 0) {
    grp <- condo_with_addr[, .(
      group_size_pids = .N,
      group_n_parcel_addresses = uniqueN(n_sn_ss_c),
      anchor_pid = sort(as.character(parcel_number))[1L]
    ), by = n_sn_ss_c]
    grp[, condo_group_id := vapply(
      n_sn_ss_c,
      function(x) paste0("cg_", digest(paste0("n_sn_ss_c|", x), algo = "xxhash64")),
      FUN.VALUE = character(1)
    )]
    assert_unique(grp, c("n_sn_ss_c"), "condo group mapping by n_sn_ss_c")
    assert_unique(grp, c("condo_group_id"), "condo group ids")

    out[grp, `:=`(
      condo_group_id = i.condo_group_id,
      anchor_pid = i.anchor_pid,
      group_size_pids = i.group_size_pids,
      group_n_parcel_addresses = i.group_n_parcel_addresses
    ), on = "n_sn_ss_c"]
  }

  # Non-condos and condos without a shared common-address key remain parcel-linked only.
  out[is.na(anchor_pid), anchor_pid := as.character(parcel_number)]
  out[, group_key_version := "phase1_n_sn_ss_c_v1"]
  assert_unique(out, c("parcel_number"), "condo_parcel_group_membership")
  out[]
}

#' Add source-agnostic xwalk status + condo diagnostics to winner rows.
add_xwalk_phase1_diagnostics <- function(winners, all_candidates, condo_membership, source_name) {
  stopifnot(is.data.table(winners), is.data.table(all_candidates), is.data.table(condo_membership))

  if (!all(c("source_address_id", "parcel_number") %in% names(all_candidates))) {
    cand <- data.table(source_address_id = winners$source_address_id[0], parcel_number = character())
  } else {
    cand <- unique(all_candidates[, .(source_address_id, parcel_number = as.character(parcel_number))],
                   by = c("source_address_id", "parcel_number"))
  }
  cand <- cand[!is.na(parcel_number)]
  if (nrow(cand) > 0) {
    cand[condo_membership[, .(parcel_number, is_condo, condo_group_id, anchor_pid, group_size_pids)],
         `:=`(
           candidate_is_condo = i.is_condo,
           candidate_condo_group_id = i.condo_group_id,
           candidate_anchor_pid = i.anchor_pid,
           candidate_group_size_pids = i.group_size_pids
         ),
         on = "parcel_number"]
  } else {
    cand[, `:=`(
      candidate_is_condo = logical(),
      candidate_condo_group_id = character(),
      candidate_anchor_pid = character(),
      candidate_group_size_pids = integer()
    )]
  }
  cand[is.na(candidate_is_condo), candidate_is_condo := FALSE]

  cand_diag <- cand[, .(
    n_candidate_parcels = uniqueN(parcel_number),
    n_condo_candidates = uniqueN(parcel_number[candidate_is_condo %in% TRUE]),
    n_condo_groups = uniqueN(candidate_condo_group_id[!is.na(candidate_condo_group_id)]),
    candidate_share_condo = {
      n_all <- uniqueN(parcel_number)
      if (n_all > 0) uniqueN(parcel_number[candidate_is_condo %in% TRUE]) / n_all else NA_real_
    },
    condo_groupable = uniqueN(parcel_number) > 1L & uniqueN(parcel_number[candidate_is_condo %in% TRUE]) >= 2L,
    condo_groupable_majority = uniqueN(parcel_number) > 1L &&
      uniqueN(parcel_number[candidate_is_condo %in% TRUE]) >= 2L &&
      (uniqueN(parcel_number[candidate_is_condo %in% TRUE]) / uniqueN(parcel_number)) >= 0.5,
    candidate_condo_group_id = {
      cg <- candidate_condo_group_id[!is.na(candidate_condo_group_id)]
      if (length(cg) == 0) NA_character_ else sort(names(sort(table(cg), decreasing = TRUE)))[1L]
    }
  ), by = source_address_id]
  setkey(cand_diag, source_address_id)

  out <- copy(winners)
  out[, parcel_number := as.character(parcel_number)]

  out[cand_diag, `:=`(
    n_candidate_parcels = i.n_candidate_parcels,
    n_condo_candidates = i.n_condo_candidates,
    n_candidate_condo_groups = i.n_condo_groups,
    candidate_share_condo = i.candidate_share_condo,
    condo_groupable = i.condo_groupable,
    condo_groupable_majority = i.condo_groupable_majority,
    candidate_condo_group_id = i.candidate_condo_group_id
  ), on = "source_address_id"]

  out[condo_membership[, .(parcel_number, winner_is_condo = is_condo, winner_condo_group_id = condo_group_id, winner_anchor_pid = anchor_pid)],
      `:=`(
        winner_is_condo = i.winner_is_condo,
        winner_condo_group_id = i.winner_condo_group_id,
        winner_anchor_pid = i.winner_anchor_pid
      ),
      on = "parcel_number"]

  out[is.na(n_candidate_parcels), n_candidate_parcels := fifelse(is.na(parcel_number), 0L, 1L)]
  out[is.na(n_condo_candidates), n_condo_candidates := 0L]
  out[is.na(n_candidate_condo_groups), n_candidate_condo_groups := 0L]
  out[is.na(condo_groupable), condo_groupable := FALSE]
  out[is.na(condo_groupable_majority), condo_groupable_majority := FALSE]
  out[is.na(winner_is_condo), winner_is_condo := FALSE]

  out[, xwalk_status := fifelse(
    match_tier == "unmatched" & n_candidate_parcels == 0L, "unmatched_address",
    fifelse(
      (match_tier == "ambiguous" | n_candidate_parcels > 1L) & condo_groupable == TRUE,
      "ambiguous_match_condo_groupable",
      fifelse(
        (match_tier == "ambiguous" | n_candidate_parcels > 1L),
        "ambiguous_match_noncondo",
        "unique_match"
      )
    )
  )]

  # Phase 1: expose future condo-aware link metadata without changing winner parcel selection.
  out[, link_type := fifelse(
    !is.na(parcel_number) & xwalk_status == "ambiguous_match_condo_groupable" & !is.na(winner_condo_group_id),
    "condo_group",
    fifelse(!is.na(parcel_number), "parcel", NA_character_)
  )]
  out[, link_id := fifelse(
    link_type == "condo_group", winner_condo_group_id,
    fifelse(link_type == "parcel", parcel_number, NA_character_)
  )]
  out[, anchor_pid := fifelse(
    !is.na(winner_anchor_pid), winner_anchor_pid,
    fifelse(!is.na(parcel_number), parcel_number, NA_character_)
  )]
  out[, ownership_unsafe := xwalk_status == "ambiguous_match_condo_groupable"]
  out[, xwalk_phase1_version := "condo_group_diagnostics_v1"]

  # Invariant: winner xwalk remains one row per source_address_id.
  assert_unique(out, c("source_address_id"), paste0("winner xwalk + diagnostics (", source_name, ")"))
  out[]
}

#' Normalize address components for relaxed matching
#' @param dt Data table with pm.* columns
#' @return Data table with normalized columns added
normalize_address <- function(dt) {
  dt <- copy(dt)

  # Normalize street name
  dt[, street_norm := pm.street %>%
       str_to_lower() %>%
       str_squish() %>%
       # Common substitutions
       str_replace_all("\\bsaint\\b", "st") %>%
       str_replace_all("\\bmount\\b", "mt") %>%
       str_replace_all("\\bavenue\\b", "ave") %>%
       str_replace_all("\\bstreet\\b", "st") %>%
       str_replace_all("\\bdrive\\b", "dr") %>%
       str_replace_all("\\broad\\b", "rd") %>%
       str_replace_all("\\blane\\b", "ln") %>%
       str_replace_all("\\bcourt\\b", "ct") %>%
       str_replace_all("\\bplace\\b", "pl")]

  # Normalize suffix
  dt[, suffix_norm := pm.streetSuf %>%
       str_to_lower() %>%
       str_squish() %>%
       str_replace_all("\\bave(nue)?\\b", "ave") %>%
       str_replace_all("\\bst(reet)?\\b", "st") %>%
       str_replace_all("\\bdr(ive)?\\b", "dr") %>%
       str_replace_all("\\brd\\b", "rd") %>%
       str_replace_all("\\bln\\b", "ln") %>%
       str_replace_all("\\bct\\b", "ct") %>%
       str_replace_all("\\bpl\\b", "pl")]

  # Normalize direction
 dt[, dir_norm := coalesce(pm.preDir, pm.sufDir) %>%
       str_to_lower() %>%
       str_squish()]

  # Normalize zip (first 5 digits only, strip "_" prefix if present)
  dt[, zip_norm := str_sub(as.character(pm.zip), 1, 5) %>%
       str_remove("^_") %>%
       str_pad(5, "left", "0")]

  # Normalize house number
  dt[, house_norm := as.numeric(pm.house)]

  return(dt)
}

#' Compute match score between source and parcel candidate
#' @param source_dt Source addresses (data.table)
#' @param parcel_dt Parcel candidates (data.table)
#' @param weights Named list of component weights
#' @return Data.table with match scores
compute_match_score <- function(source_dt, parcel_dt, weights = MATCH_PARAMS$weights) {
  # Street similarity (Jaro-Winkler)
  street_sim <- stringdist::stringsim(
    source_dt$street_norm,
    parcel_dt$street_norm,
    method = "jw"
  )

  # House number match (1 if exact, 0.5 if within 2, 0 otherwise)
  house_diff <- abs(source_dt$house_norm - parcel_dt$house_norm)
  house_score <- fifelse(house_diff == 0, 1.0,
                        fifelse(house_diff <= 2, 0.5, 0.0))
  house_score[is.na(house_score)] <- 0

  # Suffix match (1 if match, 0 otherwise)
  suffix_score <- fifelse(source_dt$suffix_norm == parcel_dt$suffix_norm, 1.0, 0.0)
  suffix_score[is.na(suffix_score)] <- 0.5  # Missing suffix is neutral

  # Direction match
  dir_score <- fifelse(source_dt$dir_norm == parcel_dt$dir_norm, 1.0, 0.0)
  dir_score[is.na(dir_score)] <- 0.5  # Missing direction is neutral

  # Zip match
  zip_score <- fifelse(source_dt$zip_norm == parcel_dt$zip_norm, 1.0, 0.0)
  zip_score[is.na(zip_score)] <- 0

  # Weighted combination
  match_score <- (
    weights$street_sim * street_sim +
    weights$house_match * house_score +
    weights$suffix_match * suffix_score +
    weights$dir_match * dir_score +
    weights$zip_match * zip_score
  )

  return(data.table(
    street_sim = street_sim,
    house_score = house_score,
    suffix_score = suffix_score,
    dir_score = dir_score,
    zip_score = zip_score,
    match_score = match_score
  ))
}

#' Select winner from candidates using deterministic tie-breaking
#' @param candidates Data.table of candidates with match_score
#' @return Data.table with single winner per source_address_id
select_winner <- function(candidates) {
  # Rank candidates by score within each source address
  candidates[, rank := frank(-match_score, ties.method = "min"), by = source_address_id]

  # Get best and second-best scores
  candidates[, best_score := max(match_score), by = source_address_id]
  candidates[, n_candidates := .N, by = source_address_id]

  # Count ties at best score
  candidates[, n_tied := sum(match_score == best_score), by = source_address_id]
  candidates[, tie_flag := n_tied > 1]

  # Calculate score gap
  candidates[, score_gap := {
    scores <- sort(unique(match_score), decreasing = TRUE)
    if (length(scores) >= 2) scores[1] - scores[2] else NA_real_
  }, by = source_address_id]

  # Filter to top candidates
  top_candidates <- candidates[rank == 1]

  # Apply deterministic tie-breaking for ties
  top_candidates[, tie_rule := NA_character_]

  # Tie-breaker 1: Exact n_sn_ss_c match
  top_candidates[tie_flag == TRUE & n_sn_ss_c_source == n_sn_ss_c_parcel,
                tie_rule := "exact_n_sn_ss_c"]

  # Tie-breaker 2: Exact house number
  top_candidates[tie_flag == TRUE & is.na(tie_rule) & house_norm_source == house_norm_parcel,
                tie_rule := "exact_house"]

  # Tie-breaker 3: Exact suffix
  top_candidates[tie_flag == TRUE & is.na(tie_rule) & suffix_norm_source == suffix_norm_parcel,
                tie_rule := "exact_suffix"]

  # Tie-breaker 4: Smallest parcel_number (deterministic fallback)
  top_candidates[tie_flag == TRUE & is.na(tie_rule), tie_rule := "smallest_parcel"]

  # Select one winner per source using tie rules
  winners <- top_candidates[, {
    if (.N == 1) {
      .SD
    } else {
      # Apply tie-breakers in order
      result <- .SD[n_sn_ss_c_source == n_sn_ss_c_parcel]
      if (nrow(result) == 0) result <- .SD[house_norm_source == house_norm_parcel]
      if (nrow(result) == 0 || nrow(result) > 1) result <- .SD[which.min(parcel_number)]
      result[1]
    }
  }, by = source_address_id]

  return(winners)
}

#' Run tiered matching for a source dataset
#' @param source_dt Source addresses (data.table)
#' @param parcels_dt Parcel reference (data.table)
#' @param source_name Name of source (e.g., "evictions")
#' @return List with winners, audit, and summary tables
run_matching <- function(source_dt, parcels_dt, source_name, condo_membership = NULL) {
  logf("  Running matching for source: ", source_name, log_file = log_file)
  logf("    Source addresses: ", nrow(source_dt), log_file = log_file)
  logf("    Parcels: ", nrow(parcels_dt), log_file = log_file)
  if (is.null(condo_membership)) {
    condo_membership <- data.table(parcel_number = character(), is_condo = logical(),
                                   condo_group_id = character(), anchor_pid = character(),
                                   group_size_pids = integer(), group_n_parcel_addresses = integer())
  }

  # Normalize both datasets
  source_norm <- normalize_address(source_dt)
  parcels_norm <- normalize_address(parcels_dt)

  # Initialize results
  all_candidates <- data.table()
  tier_stats <- list()

  # --------------------------------------------------------
  # TIER 0: EXACT MATCH on n_sn_ss_c
  # --------------------------------------------------------
  logf("    Tier 0: Exact match on n_sn_ss_c...", log_file = log_file)

  tier0_matches <- merge(
    source_norm[, .(source_address_id, n_sn_ss_c, house_norm, street_norm, suffix_norm, dir_norm, zip_norm)],
    parcels_norm[, .(parcel_number, n_sn_ss_c, house_norm, street_norm, suffix_norm, dir_norm, zip_norm,
                    geocode_x, geocode_y)],
    by = "n_sn_ss_c",
    suffixes = c("_source", "_parcel"),
    allow.cartesian = TRUE
  )

  if (nrow(tier0_matches) > 0) {
    tier0_matches[, match_tier := "exact"]
    tier0_matches[, match_score := 1.0]
    tier0_matches[, street_sim := 1.0]
    tier0_matches[, house_score := 1.0]
    tier0_matches[, suffix_score := 1.0]
    tier0_matches[, dir_score := 1.0]
    tier0_matches[, zip_score := fifelse(zip_norm_source == zip_norm_parcel, 1.0, 0.5)]

    all_candidates <- rbind(all_candidates, tier0_matches, fill = TRUE)
  }

  tier_stats$tier0 <- tier0_matches[, uniqueN(source_address_id)]
  logf("      Matched: ", tier_stats$tier0, " addresses", log_file = log_file)

  # IDs already matched
  matched_ids <- unique(all_candidates$source_address_id)

  # --------------------------------------------------------
  # TIER 1: RELAXED EXACT MATCH (normalized components)
  # --------------------------------------------------------
  logf("    Tier 1: Relaxed match (normalized components)...", log_file = log_file)

  unmatched_source <- source_norm[!source_address_id %in% matched_ids]

  if (nrow(unmatched_source) > 0) {
    # Match on normalized house + street + suffix (ignore direction)
    tier1_matches <- merge(
      unmatched_source[, .(source_address_id, n_sn_ss_c, house_norm, street_norm, suffix_norm, dir_norm, zip_norm)],
      parcels_norm[, .(parcel_number, n_sn_ss_c, house_norm, street_norm, suffix_norm, dir_norm, zip_norm,
                      geocode_x, geocode_y)],
      by = c("house_norm", "street_norm", "suffix_norm"),
      suffixes = c("_source", "_parcel"),
      allow.cartesian = TRUE
    )

    if (nrow(tier1_matches) > 0) {
      tier1_matches[, match_tier := "relaxed"]
      tier1_matches[, match_score := 0.95]
      tier1_matches[, street_sim := 1.0]
      tier1_matches[, house_score := 1.0]
      tier1_matches[, suffix_score := 1.0]
      tier1_matches[, dir_score := fifelse(dir_norm_source == dir_norm_parcel, 1.0, 0.5)]
      tier1_matches[, zip_score := fifelse(zip_norm_source == zip_norm_parcel, 1.0, 0.5)]

      # Recalculate match_score based on components
      tier1_matches[, match_score := 0.9 + 0.05 * dir_score + 0.05 * zip_score]

      all_candidates <- rbind(all_candidates, tier1_matches, fill = TRUE)
    }

    tier_stats$tier1 <- tier1_matches[!source_address_id %in% matched_ids, uniqueN(source_address_id)]
  } else {
    tier_stats$tier1 <- 0
  }
  logf("      Matched: ", tier_stats$tier1, " additional addresses", log_file = log_file)

  # Update matched IDs
  matched_ids <- unique(all_candidates$source_address_id)

  # --------------------------------------------------------
  # TIER 1B: NEAR-NUMBER MATCH (same street/suffix/zip, close house number)
  # --------------------------------------------------------
  logf("    Tier 1b: Near-number match (same street/suffix/zip)...", log_file = log_file)

  unmatched_source <- source_norm[!source_address_id %in% matched_ids]

  if (nrow(unmatched_source) > 0) {
    tier1b_matches <- merge(
      unmatched_source[, .(source_address_id, n_sn_ss_c, house_norm, street_norm, suffix_norm, dir_norm, zip_norm)],
      parcels_norm[, .(parcel_number, n_sn_ss_c, house_norm, street_norm, suffix_norm, dir_norm, zip_norm,
                      geocode_x, geocode_y)],
      by = c("street_norm", "suffix_norm", "zip_norm"),
      suffixes = c("_source", "_parcel"),
      allow.cartesian = TRUE
    )

    if (nrow(tier1b_matches) > 0) {
      tier1b_matches[, house_diff := abs(house_norm_source - house_norm_parcel)]
      tier1b_matches <- tier1b_matches[
        !is.na(house_norm_source) &
        !is.na(house_norm_parcel) &
        house_diff > 0 &
        house_diff <= MATCH_PARAMS$near_number_house_diff_max
      ]

      if (nrow(tier1b_matches) > 0) {
        tier1b_matches <- tier1b_matches[
          ,
          .SD[house_diff == min(house_diff, na.rm = TRUE)],
          by = source_address_id
        ]
        tier1b_matches <- tier1b_matches[
          ,
          if (.N == 1L) .SD else .SD[0],
          by = source_address_id
        ]
      }

      if (nrow(tier1b_matches) > 0) {
        tier1b_matches[, match_tier := "near_number"]
        tier1b_matches[, street_sim := 1.0]
        tier1b_matches[, house_score := pmax(
          0,
          1 - (house_diff / MATCH_PARAMS$near_number_house_diff_max)
        )]
        tier1b_matches[, suffix_score := 1.0]
        tier1b_matches[, dir_score := fifelse(dir_norm_source == dir_norm_parcel, 1.0, 0.5)]
        tier1b_matches[, zip_score := 1.0]
        tier1b_matches[, match_score := 0.84 + 0.10 * house_score + 0.06 * dir_score]

        all_candidates <- rbind(all_candidates, tier1b_matches[, !"house_diff"], fill = TRUE)
      }
    }

    tier_stats$tier1b_near_number <- tier1b_matches[!source_address_id %in% matched_ids, uniqueN(source_address_id)]
  } else {
    tier_stats$tier1b_near_number <- 0
  }
  logf("      Matched: ", tier_stats$tier1b_near_number, " additional addresses", log_file = log_file)

  # Update matched IDs
  matched_ids <- unique(all_candidates$source_address_id)

  # --------------------------------------------------------
  # TIER 2: FUZZY MATCH (blocked)
  # --------------------------------------------------------
  logf("    Tier 2: Fuzzy match (blocked)...", log_file = log_file)

  unmatched_source <- source_norm[!source_address_id %in% matched_ids]

  if (nrow(unmatched_source) > 0) {
    # Create blocking key: zip + first N letters of street
    unmatched_source[, block_key := paste0(zip_norm, "_", str_sub(street_norm, 1, MATCH_PARAMS$street_prefix_len))]
    parcels_norm[, block_key := paste0(zip_norm, "_", str_sub(street_norm, 1, MATCH_PARAMS$street_prefix_len))]

    # Generate candidates via blocking
    tier2_candidates <- merge(
      unmatched_source[, .(source_address_id, n_sn_ss_c, house_norm, street_norm, suffix_norm, dir_norm, zip_norm, block_key)],
      parcels_norm[, .(parcel_number, n_sn_ss_c, house_norm, street_norm, suffix_norm, dir_norm, zip_norm, block_key,
                      geocode_x, geocode_y)],
      by = "block_key",
      suffixes = c("_source", "_parcel"),
      allow.cartesian = TRUE
    )

    # Filter by house number proximity
    tier2_candidates <- tier2_candidates[
      abs(house_norm_source - house_norm_parcel) <= MATCH_PARAMS$house_diff_max |
      is.na(house_norm_source) | is.na(house_norm_parcel)
    ]

    if (nrow(tier2_candidates) > 0) {
      # Compute match scores
      scores <- compute_match_score(
        tier2_candidates[, .(street_norm = street_norm_source, house_norm = house_norm_source,
                            suffix_norm = suffix_norm_source, dir_norm = dir_norm_source,
                            zip_norm = zip_norm_source)],
        tier2_candidates[, .(street_norm = street_norm_parcel, house_norm = house_norm_parcel,
                            suffix_norm = suffix_norm_parcel, dir_norm = dir_norm_parcel,
                            zip_norm = zip_norm_parcel)]
      )

      tier2_candidates <- cbind(tier2_candidates, scores)
      tier2_candidates[, match_tier := "fuzzy"]

      # Filter by threshold
      tier2_candidates <- tier2_candidates[match_score >= MATCH_PARAMS$fuzzy_threshold]

      if (nrow(tier2_candidates) > 0) {
        all_candidates <- rbind(all_candidates, tier2_candidates[, -"block_key", with = FALSE], fill = TRUE)
      }

      tier_stats$tier2 <- tier2_candidates[match_score >= MATCH_PARAMS$fuzzy_threshold, uniqueN(source_address_id)]
    } else {
      tier_stats$tier2 <- 0
    }

    # Clean up blocking key
    parcels_norm[, block_key := NULL]
  } else {
    tier_stats$tier2 <- 0
  }
  logf("      Matched: ", tier_stats$tier2, " additional addresses", log_file = log_file)

  # --------------------------------------------------------
  # SELECT WINNERS
  # --------------------------------------------------------
  logf("    Selecting winners...", log_file = log_file)

  if (nrow(all_candidates) > 0) {
    # Rename columns for winner selection
    setnames(all_candidates,
            c("n_sn_ss_c_source", "n_sn_ss_c_parcel"),
            c("n_sn_ss_c_source", "n_sn_ss_c_parcel"),
            skip_absent = TRUE)

    # Ensure required columns exist
    if (!"n_sn_ss_c_source" %in% names(all_candidates)) {
      all_candidates[, n_sn_ss_c_source := n_sn_ss_c]
    }
    if (!"n_sn_ss_c_parcel" %in% names(all_candidates)) {
      all_candidates[, n_sn_ss_c_parcel := n_sn_ss_c]
    }

    winners <- select_winner(all_candidates)

    # Check score gap threshold for lower-confidence matches
    winners[match_tier == "fuzzy" & score_gap < MATCH_PARAMS$gap_threshold,
           `:=`(match_tier = "ambiguous", parcel_number = NA_character_)]
  } else {
    winners <- data.table(
      source_address_id = character(),
      parcel_number = character(),
      match_tier = character(),
      match_score = numeric()
    )
  }

  # Add unmatched addresses
  all_source_ids <- source_norm$source_address_id
  matched_source_ids <- winners[!is.na(parcel_number), source_address_id]
  winner_source_ids <- unique(winners$source_address_id)
  unmatched_ids <- setdiff(all_source_ids, winner_source_ids)

  if (length(unmatched_ids) > 0) {
    unmatched_rows <- data.table(
      source_address_id = unmatched_ids,
      parcel_number = NA_character_,
      match_tier = "unmatched",
      match_score = NA_real_,
      n_candidates = 0L,
      score_gap = NA_real_,
      tie_flag = FALSE,
      tie_rule = NA_character_
    )
    winners <- rbind(winners, unmatched_rows, fill = TRUE)
  }

  # --------------------------------------------------------
  # CREATE OUTPUTS
  # --------------------------------------------------------

  # Winner crosswalk (base fields preserved for downstream compatibility)
  winner_xwalk <- winners[, .(
    source = source_name,
    source_address_id,
    parcel_number,
    match_tier,
    match_score,
    n_candidates = fifelse(is.na(n_candidates), 0L, as.integer(n_candidates)),
    score_gap,
    tie_flag = fifelse(is.na(tie_flag), FALSE, tie_flag),
    tie_rule,
    rule_version = MATCH_PARAMS$version
  )]

  # Merge back source address info
  winner_xwalk <- merge(
    winner_xwalk,
    source_norm[, .(source_address_id, n_sn_ss_c, zip_norm, street_norm)],
    by = "source_address_id",
    all.x = TRUE
  )
  setnames(winner_xwalk, c("n_sn_ss_c", "zip_norm"), c("n_sn_ss_c", "zip"))

  # Phase 1 diagnostics (no change to selected parcel_number)
  winner_xwalk <- add_xwalk_phase1_diagnostics(
    winners = winner_xwalk,
    all_candidates = all_candidates,
    condo_membership = condo_membership,
    source_name = source_name
  )

  # Audit table (all candidates)
  audit_table <- all_candidates[, .(
    source_address_id,
    parcel_number,
    match_tier,
    street_sim,
    house_score,
    suffix_score,
    dir_score,
    zip_score,
    match_score,
    house_norm_source,
    house_norm_parcel,
    street_norm_source,
    street_norm_parcel,
    suffix_norm_source,
    suffix_norm_parcel
  )]
  if (nrow(audit_table) > 0) {
    audit_table[, parcel_number := as.character(parcel_number)]
    audit_table[condo_membership[, .(parcel_number, candidate_is_condo = is_condo, candidate_condo_group_id = condo_group_id,
                                     candidate_anchor_pid = anchor_pid, candidate_group_size_pids = group_size_pids)],
               `:=`(
                 candidate_is_condo = i.candidate_is_condo,
                 candidate_condo_group_id = i.candidate_condo_group_id,
                 candidate_anchor_pid = i.candidate_anchor_pid,
                 candidate_group_size_pids = i.candidate_group_size_pids
               ),
               on = "parcel_number"]
    audit_table[is.na(candidate_is_condo), candidate_is_condo := FALSE]
  } else {
    audit_table[, `:=`(
      candidate_is_condo = logical(),
      candidate_condo_group_id = character(),
      candidate_anchor_pid = character(),
      candidate_group_size_pids = integer()
    )]
  }
  audit_table[, rank := frank(-match_score, ties.method = "min"), by = source_address_id]

  # Summary table
  xwalk_status_counts <- winner_xwalk[, .N, by = xwalk_status]
  get_status_n <- function(status) {
    n <- xwalk_status_counts[xwalk_status == status, N]
    if (length(n) == 0) 0L else as.integer(n[1])
  }

  summary_table <- data.table(
    source = source_name,
    total_addresses = length(all_source_ids),
    matched = length(matched_source_ids),
    unmatched = length(unmatched_ids),
    match_rate = round(length(matched_source_ids) / length(all_source_ids) * 100, 2),
    tier0_exact = tier_stats$tier0,
    tier1_relaxed = tier_stats$tier1,
    tier1b_near_number = tier_stats$tier1b_near_number,
    tier2_fuzzy = tier_stats$tier2,
    ambiguous = winners[match_tier == "ambiguous", .N],
    ties = winners[tie_flag == TRUE, .N],
    xwalk_unique_match = get_status_n("unique_match"),
    xwalk_ambiguous_condo_groupable = get_status_n("ambiguous_match_condo_groupable"),
    xwalk_ambiguous_noncondo = get_status_n("ambiguous_match_noncondo"),
    xwalk_unmatched_address = get_status_n("unmatched_address"),
    rule_version = MATCH_PARAMS$version
  )

  logf("    Results: ", summary_table$matched, "/", summary_table$total_addresses,
      " matched (", summary_table$match_rate, "%)", log_file = log_file)
  logf("    Xwalk status counts - unique: ", summary_table$xwalk_unique_match,
      ", ambig_condo_groupable: ", summary_table$xwalk_ambiguous_condo_groupable,
      ", ambig_noncondo: ", summary_table$xwalk_ambiguous_noncondo,
      ", unmatched: ", summary_table$xwalk_unmatched_address,
      log_file = log_file)

  return(list(
    winners = winner_xwalk,
    audit = audit_table,
    summary = summary_table
  ))
}

# ============================================================
# LOAD DATA
# ============================================================

logf("Loading input data...", log_file = log_file)

# Load parcels (reference)
parcels_path <- p_product(cfg, "parcels_clean")
logf("  Loading parcels from: ", parcels_path, log_file = log_file)
parcels <- fread(parcels_path)
logf("    Loaded ", nrow(parcels), " parcels", log_file = log_file)

# Ensure parcel_number is unique
stopifnot("parcel_number must be unique" = parcels[, uniqueN(parcel_number)] == nrow(parcels))

# Standardize parcel columns
parcels[, pm.house := as.numeric(pm.house)]
parcels[, pm.zip := as.character(pm.zip)]
parcels[, parcel_number := as.character(parcel_number)]

# Build and write condo-group membership (Phase 1 product)
logf("Building condo parcel grouping (Phase 1)...", log_file = log_file)
condo_membership <- build_condo_parcel_groups(parcels)
condo_membership_path <- p_product(cfg, "condo_parcel_group_membership")
dir.create(dirname(condo_membership_path), showWarnings = FALSE, recursive = TRUE)
fwrite(condo_membership, condo_membership_path)
logf("  Wrote condo parcel group membership: ", nrow(condo_membership), " rows -> ", condo_membership_path, log_file = log_file)
logf("  Condo parcels grouped: ", condo_membership[is_condo == TRUE & !is.na(condo_group_id), .N],
     " parcels across ", condo_membership[is_condo == TRUE & !is.na(condo_group_id), uniqueN(condo_group_id)],
     " condo groups", log_file = log_file)

# ============================================================
# PROCESS EACH SOURCE
# ============================================================

sources <- list(
  evictions = list(
    path_key = "evictions_clean",
    id_col = "pm.uid",
    xwalk_key = "xwalk_evictions_to_parcel",
    audit_key = "match_audit_evictions",
    summary_key = "match_summary_evictions"
  ),
  infousa = list(
    path_key = "infousa_clean",
    id_col = "pm.uid",
    xwalk_key = "xwalk_infousa_to_parcel",
    audit_key = "match_audit_infousa",
    summary_key = "match_summary_infousa"
  ),
  altos = list(
    path_key = "altos_clean",
    id_col = "pm.uid",
    xwalk_key = "xwalk_altos_to_parcel",
    audit_key = "match_audit_altos",
    summary_key = "match_summary_altos"
  )
)

for (source_name in names(sources)) {
  source_cfg <- sources[[source_name]]

  logf("", log_file = log_file)
  logf("Processing source: ", source_name, log_file = log_file)

  # Load source data
  source_path <- p_product(cfg, source_cfg$path_key)
  logf("  Loading from: ", source_path, log_file = log_file)

  if (!file.exists(source_path)) {
    logf("  WARNING: Source file not found, skipping: ", source_path, log_file = log_file)
    next
  }

  source_dt <- fread(source_path)
  logf("  Loaded ", nrow(source_dt), " rows", log_file = log_file)

  # Create stable address ID if needed
  id_col <- source_cfg$id_col
  if (!id_col %in% names(source_dt)) {
    logf("  Creating stable address ID...", log_file = log_file)
    source_dt[, source_address_id := create_stable_id(pm.house, pm.street, pm.streetSuf, pm.zip)]
  } else {
    source_dt[, source_address_id := as.character(get(id_col))]
  }

  # Standardize columns
  source_dt[, pm.house := as.numeric(pm.house)]
  source_dt[, pm.zip := as.character(pm.zip)]

  # Verify uniqueness of source_address_id for matching purposes
  # (Note: for evictions, pm.uid may not be unique if multiple filings per address)
  n_unique <- source_dt[, uniqueN(source_address_id)]
  logf("  Unique source addresses: ", n_unique, log_file = log_file)

  # Deduplicate to unique addresses for matching
  source_unique <- unique(source_dt, by = "source_address_id")

  # Run matching
  results <- run_matching(source_unique, parcels, source_name, condo_membership = condo_membership)

  # Write outputs
  xwalk_path <- p_product(cfg, source_cfg$xwalk_key)
  audit_path <- p_product(cfg, source_cfg$audit_key)
  summary_path <- p_product(cfg, source_cfg$summary_key)

  dir.create(dirname(xwalk_path), showWarnings = FALSE, recursive = TRUE)

  logf("  Writing winner crosswalk to: ", xwalk_path, log_file = log_file)
  fwrite(results$winners, xwalk_path)
  logf("    Wrote ", nrow(results$winners), " rows", log_file = log_file)

  logf("  Writing audit table to: ", audit_path, log_file = log_file)
  fwrite(results$audit, audit_path)
  logf("    Wrote ", nrow(results$audit), " rows", log_file = log_file)

  logf("  Writing summary to: ", summary_path, log_file = log_file)
  fwrite(results$summary, summary_path)

  # Log summary
  logf("  Summary for ", source_name, ":", log_file = log_file)
  logf("    Total: ", results$summary$total_addresses, log_file = log_file)
  logf("    Matched: ", results$summary$matched, " (", results$summary$match_rate, "%)", log_file = log_file)
  logf("    By tier - Exact: ", results$summary$tier0_exact,
      ", Relaxed: ", results$summary$tier1_relaxed,
      ", Near-number: ", results$summary$tier1b_near_number,
      ", Fuzzy: ", results$summary$tier2_fuzzy, log_file = log_file)

  # ---- ENHANCED DIAGNOSTICS ----

  # 1. Match score distribution (for non-exact matches)
  non_exact_scores <- results$audit[match_tier != "exact" & rank == 1, match_score]
  if (length(non_exact_scores) > 0) {
    score_quantiles <- quantile(non_exact_scores, probs = c(0, 0.25, 0.5, 0.75, 1), na.rm = TRUE)
    logf("  Match score distribution (non-exact, winner only):", log_file = log_file)
    logf("    Min: ", round(score_quantiles[1], 3),
        ", Q1: ", round(score_quantiles[2], 3),
        ", Median: ", round(score_quantiles[3], 3),
        ", Q3: ", round(score_quantiles[4], 3),
        ", Max: ", round(score_quantiles[5], 3), log_file = log_file)

    # Score bins
    score_bins <- cut(non_exact_scores,
                      breaks = c(0, 0.7, 0.8, 0.9, 0.95, 1.0),
                      labels = c("<0.70", "0.70-0.80", "0.80-0.90", "0.90-0.95", "0.95-1.00"),
                      include.lowest = TRUE)
    bin_counts <- table(score_bins)
    logf("    Score bins: ", paste(names(bin_counts), "=", bin_counts, collapse = ", "), log_file = log_file)
  }

  # 2. Ambiguous match analysis
  n_ambiguous <- results$winners[match_tier == "ambiguous", .N]
  n_ties <- results$winners[tie_flag == TRUE, .N]
  if (n_ambiguous > 0 || n_ties > 0) {
    logf("  Ambiguous/tie analysis:", log_file = log_file)
    logf("    Ambiguous (no clear winner): ", n_ambiguous, log_file = log_file)
    logf("    Ties (resolved by rule): ", n_ties, log_file = log_file)
  }

  # 3. Top 20 unmatched streets
  unmatched <- results$winners[is.na(parcel_number) | match_tier == "unmatched"]
  if (nrow(unmatched) > 0) {
    # Get street info from source_unique
    unmatched_with_street <- merge(
      unmatched[, .(source_address_id)],
      source_unique[, .(source_address_id, pm.street, pm.house, n_sn_ss_c)],
      by = "source_address_id",
      all.x = TRUE
    )

    top_unmatched_streets <- unmatched_with_street[, .N, by = pm.street][order(-N)][1:min(20, .N)]
    logf("  Top 20 unmatched streets:", log_file = log_file)
    for (i in 1:nrow(top_unmatched_streets)) {
      logf("    ", i, ". ", top_unmatched_streets$pm.street[i],
          " (", top_unmatched_streets$N[i], " addresses)", log_file = log_file)
    }

    # Sample unmatched addresses for inspection
    logf("  Sample unmatched addresses (first 10):", log_file = log_file)
    sample_unmatched <- head(unmatched_with_street[!is.na(n_sn_ss_c)], 10)
    for (i in 1:nrow(sample_unmatched)) {
      logf("    ", sample_unmatched$n_sn_ss_c[i], log_file = log_file)
    }
  }

  # 4. One-to-many analysis (addresses matching multiple parcels)
  addr_parcel_counts <- results$audit[rank == 1 & !is.na(parcel_number), .N, by = source_address_id]
  multi_match <- addr_parcel_counts[N > 1]
  if (nrow(multi_match) > 0) {
    logf("  One-to-many matches (address -> multiple parcels):", log_file = log_file)
    logf("    Addresses with >1 parcel candidate: ", nrow(multi_match), log_file = log_file)
    logf("    Max parcels per address: ", max(multi_match$N), log_file = log_file)
  }
}

logf("", log_file = log_file)
logf("=== Finished make-address-parcel-xwalk.R ===", log_file = log_file)
