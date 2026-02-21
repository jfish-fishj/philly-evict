## ============================================================
## link-evictions-infousa-names.R
## ============================================================
## Purpose: Link eviction cases (households) to InfoUSA households
##          via person-level candidate generation and case-level
##          deterministic resolution.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
})

source("r/config.R")
source("r/lib/link_evictions_infousa_utils.R")
source("r/lib/link_evictions_infousa_candidates.R")
source("r/lib/link_evictions_infousa_scoring.R")
source("r/lib/link_evictions_infousa_qc.R")

pick_input_key <- function(cfg, keys, default = NULL) {
  for (k in keys) {
    if (!is.null(cfg$inputs[[k]])) return(p_input(cfg, k))
  }
  default
}

pick_product_key <- function(cfg, keys) {
  for (k in keys) {
    if (!is.null(cfg$products[[k]])) return(p_product(cfg, k))
  }
  stop("None of the requested product keys exist: ", paste(keys, collapse = ", "))
}

pick_product_key_optional <- function(cfg, keys, default = NULL) {
  for (k in keys) {
    if (!is.null(cfg$products[[k]])) return(p_product(cfg, k))
  }
  default
}

make_stratified_sample <- function(dt, n, strata_cols, seed = 123L) {
  stopifnot(is.data.table(dt))
  if (nrow(dt) <= n) return(copy(dt))

  out <- copy(dt)
  strata_cols <- strata_cols[strata_cols %in% names(out)]
  if (length(strata_cols) == 0L) {
    set.seed(as.integer(seed))
    return(out[sample(.N, n)])
  }

  out[, sample_stratum := do.call(paste, c(.SD, sep = "|")), .SDcols = strata_cols]
  str_counts <- out[, .N, by = sample_stratum][order(-N)]
  str_counts[, target_n := pmax(
    1L,
    as.integer(round(as.numeric(n) * as.numeric(N) / as.numeric(sum(N))))
  )]
  str_counts[is.na(target_n), target_n := 1L]

  total_target <- str_counts[, sum(target_n)]
  if (total_target > n) {
    over <- total_target - n
    idx <- which.max(str_counts$target_n)
    str_counts[idx, target_n := pmax(1L, target_n - over)]
  }

  set.seed(as.integer(seed))
  sampled <- out[str_counts, on = "sample_stratum"][
    , .SD[sample(.N, min(.N, target_n[1L]))], by = sample_stratum
  ]

  if (nrow(sampled) > n) sampled <- sampled[sample(.N, n)]
  sampled[, sample_stratum := NULL]
  sampled
}

normalize_commercial_flag <- function(x) {
  x_chr <- tolower(trimws(as.character(x)))
  out <- rep(NA_integer_, length(x_chr))
  out[x_chr %chin% c("0", "false", "f", "no", "n")] <- 0L
  out[x_chr %chin% c("1", "true", "t", "yes", "y")] <- 1L
  num <- suppressWarnings(as.integer(x_chr))
  out[is.na(out) & !is.na(num)] <- num[is.na(out) & !is.na(num)]
  out
}

flag_is_true <- function(x) {
  x_chr <- tolower(trimws(as.character(x)))
  out <- rep(FALSE, length(x_chr))
  out[x_chr %chin% c("1", "true", "t", "yes", "y")] <- TRUE
  num <- suppressWarnings(as.integer(x_chr))
  out[!is.na(num) & num != 0L] <- TRUE
  out
}

derive_tract_geoid <- function(tract_geoid, bg_geoid) {
  tr <- as.character(tract_geoid)
  tr <- gsub("[^0-9]", "", tr)
  out <- rep(NA_character_, length(tr))
  ok11 <- !is.na(tr) & nchar(tr) == 11L
  out[ok11] <- tr[ok11]
  ok12 <- !is.na(tr) & nchar(tr) == 12L
  out[ok12] <- substr(tr[ok12], 1L, 11L)

  bg <- as.character(bg_geoid)
  bg <- gsub("[^0-9]", "", bg)
  fill <- (is.na(out) | !nzchar(out)) & !is.na(bg) & nchar(bg) >= 11L
  out[fill] <- substr(bg[fill], 1L, 11L)
  out
}

normalize_bg_geoid <- function(x) {
  d <- gsub("[^0-9]", "", as.character(x))
  out <- rep(NA_character_, length(d))
  ok12 <- !is.na(d) & nchar(d) == 12L
  out[ok12] <- d[ok12]
  out
}

normalize_zip_chr <- function(x) {
  d <- gsub("[^0-9]", "", as.character(x))
  out <- rep(NA_character_, length(d))
  ok <- !is.na(d) & nzchar(d)
  if (any(ok)) {
    dd <- d[ok]
    out_ok <- ifelse(
      nchar(dd) >= 5L,
      substr(dd, 1L, 5L),
      sprintf("%05d", suppressWarnings(as.integer(dd)))
    )
    out[ok] <- out_ok
  }
  out
}

make_street_fuzzy_key <- function(x, n = 6L) {
  x <- normalize_address_key(x)
  x <- coalesce_chr(x, "")
  x <- gsub("\\s+", "", x)
  out <- ifelse(nchar(x) >= n, substr(x, 1L, n), x)
  out[out == ""] <- NA_character_
  out
}

parse_trueish <- function(x, default = FALSE) {
  if (is.null(x)) return(default)
  x_chr <- tolower(trimws(as.character(x)))
  if (!nzchar(x_chr)) return(default)
  x_chr %chin% c("1", "true", "t", "yes", "y")
}

first_nonempty_chr <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0L) NA_character_ else x[1L]
}

compose_street_name_from_pm <- function(dt) {
  n <- nrow(dt)
  get_part <- function(col) {
    if (!col %in% names(dt)) return(rep("", n))
    x <- as.character(dt[[col]])
    x[is.na(x)] <- ""
    trimws(x)
  }
  out <- paste(get_part("pm.preDir"), get_part("pm.street"), get_part("pm.streetSuf"), get_part("pm.sufDir"))
  out <- str_squish(out)
  out[out == ""] <- NA_character_
  out
}

compose_address_key_from_pm <- function(dt) {
  n <- nrow(dt)
  get_part <- function(col) {
    if (!col %in% names(dt)) return(rep("", n))
    x <- as.character(dt[[col]])
    x[is.na(x)] <- ""
    trimws(x)
  }
  out <- paste(
    get_part("pm.house"),
    get_part("pm.preDir"),
    get_part("pm.street"),
    get_part("pm.streetSuf"),
    get_part("pm.sufDir")
  )
  out <- normalize_address_key(out)
  out
}

pm_addr_cols <- c("pm.house", "pm.preDir", "pm.street", "pm.streetSuf", "pm.sufDir")

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))
sample_mode <- isTRUE(cfg$run$sample_mode) || tolower(coalesce_chr(opts$sample_mode, "false")) == "true"

log_file <- p_out(cfg, "logs", "link-evictions-infousa-names.log")
logf("=== Starting link-evictions-infousa-names.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

# Paths
default_evict_people <- p_out(cfg, "qa", "race_imputed_person_sample.csv")
if (!file.exists(default_evict_people)) {
  default_evict_people <- p_out(cfg, "qa", "race_imputation_preflight_person.csv")
}
path_evict_people <- opts$eviction_people %||% default_evict_people
path_infousa_people <- opts$infousa_people %||%
  pick_product_key_optional(cfg, c("infousa_link_people", "infousa_gender_imputed_person"))
path_infousa_clean <- opts$infousa_clean %||% p_product(cfg, "infousa_clean")
path_evictions_clean <- opts$evictions_clean %||% p_product(cfg, "evictions_clean")
path_evict_xwalk_case <- opts$evict_case_xwalk %||% p_product(cfg, "evict_address_xwalk_case")
path_infousa_xwalk <- opts$infousa_xwalk %||% p_product(cfg, "xwalk_infousa_to_parcel")
path_occupancy <- opts$occupancy_panel %||% p_product(cfg, "parcel_occupancy_panel")
path_nicknames <- opts$nickname_dictionary %||% pick_input_key(cfg, c("nickname_dict", "nickname_dictionary"), default = "")
path_validation <- opts$validation_set %||% pick_input_key(cfg, c("evict_infousa_validation_set"), default = "")
path_sample_evict_people <- pick_product_key_optional(cfg, c("evict_infousa_sample_evict_people"), default = "")
path_sample_infousa_people <- pick_product_key_optional(cfg, c("evict_infousa_sample_infousa_people"), default = "")

use_sample_evict_file <- FALSE
use_sample_infousa_file <- FALSE
if (sample_mode) {
  if (is.null(opts$eviction_people) && nzchar(path_sample_evict_people) && file.exists(path_sample_evict_people)) {
    path_evict_people <- path_sample_evict_people
    use_sample_evict_file <- TRUE
  }
  if (is.null(opts$infousa_people) && nzchar(path_sample_infousa_people) && file.exists(path_sample_infousa_people)) {
    path_infousa_people <- path_sample_infousa_people
    use_sample_infousa_file <- TRUE
  }
}
use_prebuilt_geo_samples <- sample_mode && use_sample_evict_file && use_sample_infousa_file
if (sample_mode && use_prebuilt_geo_samples) {
  logf("Sample mode ON: using prebuilt geo sample files", log_file = log_file)
  logf("  eviction sample file: ", path_evict_people, log_file = log_file)
  logf("  infousa sample file: ", path_infousa_people, log_file = log_file)
}

out_matches <- pick_product_key(cfg, c("evict_infousa_hh_matches", "evict_infousa_name_matches"))
out_unmatched <- pick_product_key(cfg, c("evict_infousa_hh_unmatched", "evict_infousa_name_unmatched"))
out_candidate_stats <- pick_product_key(cfg, c("evict_infousa_candidate_stats"))
out_sample_manifest <- pick_product_key(cfg, c("evict_infousa_sample_manifest"))
qc_dir <- p_out(cfg, "qa", "evict_infousa_name_link")

for (p in c(out_matches, out_unmatched, out_candidate_stats, out_sample_manifest)) {
  dir.create(dirname(p), recursive = TRUE, showWarnings = FALSE)
}
dir.create(qc_dir, recursive = TRUE, showWarnings = FALSE)

# ---- Load eviction person rows ----
if (!file.exists(path_evict_people)) {
  stop("Eviction person input not found: ", path_evict_people,
       ". Pass --eviction-people=... or run r/impute-race-preflight.R first.")
}
ev <- fread(path_evict_people)
setDT(ev)
logf("Loaded eviction person rows: ", nrow(ev), " from ", path_evict_people, log_file = log_file)

if ("is_analysis_defendant" %in% names(ev)) {
  ev <- ev[is_analysis_defendant == TRUE]
} else if ("party_type" %in% names(ev)) {
  ev <- ev[tolower(party_type) == "defendant"]
} else if ("role" %in% names(ev)) {
  ev <- ev[grepl("^Defendant", role, ignore.case = TRUE)]
}

if (!all(c("id", "first_name", "last_name") %in% names(ev))) {
  stop("Eviction people data must include id, first_name, last_name.")
}

ev[, id := as.character(id)]

# Enrich case-level context from evictions_clean when available.
if (file.exists(path_evictions_clean)) {
  ec_hdr <- names(fread(path_evictions_clean, nrows = 0))
  ec_pm <- intersect(pm_addr_cols, ec_hdr)
  ec_cols <- intersect(c("id", "filing_yr", "year", "zip", "n_sn_ss_c", "commercial", ec_pm), ec_hdr)
  if ("id" %in% ec_cols) {
    ec <- fread(path_evictions_clean, select = ec_cols)
    setDT(ec)
    ec[, id := as.character(id)]
    if (!"filing_yr" %in% names(ec) && "year" %in% names(ec)) {
      ec[, filing_yr := suppressWarnings(as.integer(year))]
    }
    if (!"filing_yr" %in% names(ec)) ec[, filing_yr := NA_integer_]
    if (!"n_sn_ss_c" %in% names(ec)) ec[, n_sn_ss_c := NA_character_]
    if (!"commercial" %in% names(ec)) ec[, commercial := NA_integer_]
    if (!"zip" %in% names(ec)) ec[, zip := NA_character_]

    ec[, filing_yr := suppressWarnings(as.integer(filing_yr))]
    ec[, commercial := normalize_commercial_flag(commercial)]
    ec[, zip := normalize_zip_chr(zip)]
    ec_pm <- intersect(pm_addr_cols, names(ec))
    ec_pm_agg <- NULL
    if (length(ec_pm) > 0L) {
      ec_pm_agg <- ec[, lapply(.SD, first_nonempty_chr), by = id, .SDcols = ec_pm]
    }

    ec <- ec[, .(
      filing_yr = {
        x <- filing_yr[!is.na(filing_yr)]
        if (length(x) == 0L) NA_integer_ else as.integer(x[1L])
      },
      zip = {
        x <- as.character(zip)
        x <- x[!is.na(x) & nzchar(x)]
        if (length(x) == 0L) NA_character_ else x[1L]
      },
      n_sn_ss_c = {
        x <- as.character(n_sn_ss_c)
        x <- x[!is.na(x) & nzchar(x)]
        if (length(x) == 0L) NA_character_ else x[1L]
      },
      commercial = {
        x <- commercial[!is.na(commercial)]
        if (length(x) == 0L) NA_integer_ else as.integer(x[1L])
      }
    ), by = id]
    if (!is.null(ec_pm_agg)) {
      ec <- merge(ec, ec_pm_agg, by = "id", all.x = TRUE, sort = FALSE)
    }

    if (!"filing_yr" %in% names(ev)) ev[, filing_yr := NA_integer_]
    if (!"n_sn_ss_c" %in% names(ev)) ev[, n_sn_ss_c := NA_character_]
    if (!"commercial" %in% names(ev)) ev[, commercial := NA_integer_]
    if (!"zip" %in% names(ev)) ev[, zip := NA_character_]
    for (cc in ec_pm) {
      if (!cc %in% names(ev)) ev[, (cc) := NA_character_]
    }

    n_before_year <- ev[!is.na(filing_yr), .N]
    n_before_addr <- ev[!is.na(n_sn_ss_c) & nzchar(n_sn_ss_c), .N]
    n_before_com <- ev[!is.na(commercial), .N]
    n_before_zip <- ev[!is.na(zip) & nzchar(zip), .N]
    n_before_pm_house <- if ("pm.house" %in% names(ev)) ev[!is.na(`pm.house`) & nzchar(as.character(`pm.house`)), .N] else 0L

    ev <- merge(ev, ec, by = "id", all.x = TRUE, sort = FALSE, suffixes = c("", ".ec"))
    ev[is.na(filing_yr), filing_yr := filing_yr.ec]
    ev[is.na(zip) | !nzchar(zip), zip := zip.ec]
    ev[is.na(n_sn_ss_c) | !nzchar(n_sn_ss_c), n_sn_ss_c := n_sn_ss_c.ec]
    ev[is.na(commercial), commercial := commercial.ec]
    drop_base_ec <- intersect(c("filing_yr.ec", "zip.ec", "n_sn_ss_c.ec", "commercial.ec"), names(ev))
    if (length(drop_base_ec) > 0L) ev[, (drop_base_ec) := NULL]
    for (cc in ec_pm) {
      ec_col <- paste0(cc, ".ec")
      if (ec_col %in% names(ev)) {
        ev[(is.na(get(cc)) | !nzchar(as.character(get(cc)))) & !is.na(get(ec_col)),
           (cc) := as.character(get(ec_col))]
        ev[, (ec_col) := NULL]
      }
    }

    n_after_year <- ev[!is.na(filing_yr), .N]
    n_after_addr <- ev[!is.na(n_sn_ss_c) & nzchar(n_sn_ss_c), .N]
    n_after_com <- ev[!is.na(commercial), .N]
    n_after_zip <- ev[!is.na(zip) & nzchar(zip), .N]
    n_after_pm_house <- if ("pm.house" %in% names(ev)) ev[!is.na(`pm.house`) & nzchar(as.character(`pm.house`)), .N] else 0L
    logf(
      "Merged filing_yr/n_sn_ss_c/commercial from evictions_clean: year ", n_before_year, "->", n_after_year,
      "; address ", n_before_addr, "->", n_after_addr,
      "; commercial ", n_before_com, "->", n_after_com,
      "; zip ", n_before_zip, "->", n_after_zip,
      "; pm.house ", n_before_pm_house, "->", n_after_pm_house,
      log_file = log_file
    )
  }
}

if (!"filing_yr" %in% names(ev) || ev[!is.na(filing_yr), .N] == 0L) {
  stop("Missing filing_yr in eviction people input and evictions_clean enrichment.")
}
if (!"commercial" %in% names(ev) || ev[!is.na(commercial), .N] == 0L) {
  stop("Missing commercial flag in eviction people input and evictions_clean enrichment. Non-commercial filter (commercial==0) is required.")
}
if (!"n_sn_ss_c" %in% names(ev)) ev[, n_sn_ss_c := NA_character_]
ev[, filing_yr := suppressWarnings(as.integer(filing_yr))]
ev[, commercial := normalize_commercial_flag(commercial)]

# Enforce linkage eligibility scope.
year_min <- suppressWarnings(as.integer(opts$eviction_year_min %||% cfg$run$eviction_year_min %||% 2006L))
year_max <- suppressWarnings(as.integer(opts$eviction_year_max %||% cfg$run$eviction_year_max %||% 2019L))
if (is.na(year_min)) year_min <- 2006L
if (is.na(year_max)) year_max <- 2019L
if (year_min > year_max) stop("Invalid eviction year filter bounds: min > max")

n_before_year_filter <- nrow(ev)
ev <- ev[!is.na(filing_yr) & filing_yr >= year_min & filing_yr <= year_max]
logf("Applied eviction year filter [", year_min, ",", year_max, "]: kept ", nrow(ev), " / ", n_before_year_filter, " rows", log_file = log_file)

n_before_comm_filter <- nrow(ev)
ev <- ev[!is.na(commercial) & commercial == 0L]
logf("Applied non-commercial filter (commercial==0): kept ", nrow(ev), " / ", n_before_comm_filter, " rows", log_file = log_file)

entity_or_nonperson <- rep(FALSE, nrow(ev))
if ("is_entity" %in% names(ev)) {
  entity_or_nonperson <- entity_or_nonperson | flag_is_true(ev$is_entity)
}
if ("not_person_flag" %in% names(ev)) {
  entity_or_nonperson <- entity_or_nonperson | flag_is_true(ev$not_person_flag)
}
n_drop_entity <- sum(entity_or_nonperson, na.rm = TRUE)
if (n_drop_entity > 0L) {
  ev <- ev[!entity_or_nonperson]
  logf("Dropped rows flagged as entity/non-person (is_entity/not_person_flag): ", n_drop_entity, log_file = log_file)
}
if (nrow(ev) == 0L) stop("No eviction rows remain after year/commercial/person filters.")

# Initialize optional context columns before downstream merges.
if (!"bg_geoid" %in% names(ev)) ev[, bg_geoid := NA_character_]
ev[, bg_geoid := as.character(bg_geoid)]
if (!"gender" %in% names(ev)) ev[, gender := NA_character_]
if ("geo_chr" %in% names(ev)) {
  ev[(is.na(bg_geoid) | !nzchar(as.character(bg_geoid))) & !is.na(geo_chr) & nzchar(as.character(geo_chr)),
     bg_geoid := as.character(geo_chr)]
}

# Optional PID attach
if (!"PID" %in% names(ev) || all(is.na(ev$PID) | !nzchar(ev$PID))) {
  if (file.exists(path_evict_xwalk_case)) {
    xw_cols <- intersect(c("id", "PID"), names(fread(path_evict_xwalk_case, nrows = 0)))
    if (all(c("id", "PID") %in% xw_cols)) {
      xw <- fread(path_evict_xwalk_case, select = xw_cols)
      setDT(xw)
      ev[xw, PID := i.PID, on = "id"]
      logf("Merged PID from evict address xwalk", log_file = log_file)
    }
  }
}

# Optional building bins attach
if (!"bldg_size_bin" %in% names(ev) || all(is.na(ev$bldg_size_bin))) {
  if (file.exists(path_occupancy)) {
    occ_names <- names(fread(path_occupancy, nrows = 0))
    keep_occ <- intersect(c("PID", "year", "num_units_bin", "total_units", "bg_geoid"), occ_names)
    if (length(keep_occ) > 0L) {
      occ <- fread(path_occupancy, select = keep_occ)
      setDT(occ)
      if (!"bldg_size_bin" %in% names(occ)) {
        if ("num_units_bin" %in% names(occ)) occ[, bldg_size_bin := as.character(num_units_bin)]
        if (!"bldg_size_bin" %in% names(occ) && "total_units" %in% names(occ)) occ[, bldg_size_bin := derive_bldg_size_bin(total_units)]
      }
      has_occ_bg <- "bg_geoid" %in% names(occ)

      if (all(c("PID", "year") %in% names(occ))) {
        setnames(occ, "year", "filing_yr")
        ev[occ, bldg_size_bin := i.bldg_size_bin, on = .(PID, filing_yr)]
        if (has_occ_bg) {
          ev[occ, bg_geoid := fifelse(is.na(bg_geoid), i.bg_geoid, bg_geoid), on = .(PID, filing_yr)]
        }
      } else if ("PID" %in% names(occ)) {
        occ_p <- occ[, .SD[1L], by = PID]
        ev[occ_p, bldg_size_bin := i.bldg_size_bin, on = "PID"]
        if (has_occ_bg) {
          ev[occ_p, bg_geoid := fifelse(is.na(bg_geoid), i.bg_geoid, bg_geoid), on = "PID"]
        }
      }
      logf("Merged building bins from occupancy panel", log_file = log_file)
    }
  }
}

if (!"bldg_size_bin" %in% names(ev)) ev[, bldg_size_bin := "unknown_size"]
ev[is.na(bldg_size_bin) | !nzchar(bldg_size_bin), bldg_size_bin := "unknown_size"]
if (!"bg_geoid" %in% names(ev)) ev[, bg_geoid := NA_character_]
if (!"zip" %in% names(ev)) ev[, zip := NA_character_]
if (!"tract_geoid" %in% names(ev)) ev[, tract_geoid := NA_character_]
if (!"gender" %in% names(ev)) ev[, gender := NA_character_]
if (!"n_sn_ss_c" %in% names(ev)) ev[, n_sn_ss_c := NA_character_]
ev[, bg_geoid := normalize_bg_geoid(bg_geoid)]
ev[, zip := normalize_zip_chr(zip)]
if (ev[!is.na(bg_geoid) & nzchar(bg_geoid), .N] == 0L && "geo_chr" %in% names(ev)) {
  ev[, bg_geoid := normalize_bg_geoid(geo_chr)]
}
if (ev[!is.na(bg_geoid) & nzchar(bg_geoid), .N] == 0L) {
  logf("Warning: eviction people have no non-missing bg_geoid after bg_geoid/geo_chr normalization", log_file = log_file)
}
ev[, tract_geoid := derive_tract_geoid(tract_geoid, bg_geoid)]

# Street tokens for tier B/C
if (!"street_number" %in% names(ev)) ev[, street_number := NA_character_]
if (!"street_name" %in% names(ev)) ev[, street_name := NA_character_]
pm_street_ev <- compose_street_name_from_pm(ev)
if ("pm.house" %in% names(ev)) {
  ev[(is.na(street_number) | !nzchar(street_number)) & !is.na(`pm.house`) & nzchar(as.character(`pm.house`)),
     street_number := as.character(`pm.house`)]
}
idx_ev_pm_street <- (is.na(ev$street_name) | !nzchar(ev$street_name)) & !is.na(pm_street_ev) & nzchar(pm_street_ev)
if (any(idx_ev_pm_street)) {
  ev[idx_ev_pm_street, street_name := pm_street_ev[idx_ev_pm_street]]
}
ev[(is.na(street_number) | !nzchar(street_number)),
   street_number := str_extract(coalesce_chr(n_sn_ss_c, ""), "^[0-9]+")]
ev[(is.na(street_name) | !nzchar(street_name)),
   street_name := str_squish(str_replace(coalesce_chr(n_sn_ss_c, ""), "^[0-9]+\\s*", ""))]

# Normalize + person key
ev[, evict_id := as.character(id)]
ev[, filing_yr := as.integer(filing_yr)]
ev[, first_name_norm := normalize_name(first_name)]
ev[, last_name_norm := normalize_name(last_name)]
ev[, address_pm_norm := compose_address_key_from_pm(.SD)]
ev[, address_norm := fifelse(
  !is.na(address_pm_norm) & nzchar(address_pm_norm),
  address_pm_norm,
  normalize_address_key(n_sn_ss_c)
)]
ev[, street_number := coalesce_chr(street_number, "")]
ev[, street_name := normalize_address_key(street_name)]
street_block_ev <- rep(NA_character_, nrow(ev))
if ("pm.street" %in% names(ev)) {
  street_block_ev <- as.character(ev[["pm.street"]])
}
fill_ev <- is.na(street_block_ev) | !nzchar(trimws(street_block_ev))
street_block_ev[fill_ev] <- ev$street_name[fill_ev]
ev[, street_block := normalize_address_key(street_block_ev)]
ev[, street_block_fuzzy := make_street_fuzzy_key(street_block)]
ev[, gender := normalize_gender(gender)]

# Guardrail: matching rows require both first and last name.
n_missing_name <- ev[is.na(first_name_norm) | is.na(last_name_norm), .N]
if (n_missing_name > 0L) {
  logf("Dropped eviction rows missing first/last name: ", n_missing_name, log_file = log_file)
  ev <- ev[!is.na(first_name_norm) & !is.na(last_name_norm)]
}

# Deduplicate same (id, first_name, last_name)
pre_n <- nrow(ev)
ev <- unique(ev, by = c("evict_id", "first_name_norm", "last_name_norm"))
post_n <- nrow(ev)
if (post_n < pre_n) {
  logf("Deduplicated eviction rows on (evict_id, first_name_norm, last_name_norm): dropped ",
       pre_n - post_n, " rows", log_file = log_file)
}

ev[, evict_person_id := make_evict_person_id(evict_id, first_name_norm, last_name_norm)]

dup <- ev[, .N, by = evict_person_id][N > 1L]
if (nrow(dup) > 0L) {
  dup_path <- p_out(cfg, "qa", "evict_infousa_duplicate_evict_person_id.csv")
  fwrite(dup, dup_path)
  stop("evict_person_id not unique after dedup. See: ", dup_path)
}

# ---- Sample mode ----
sample_n <- suppressWarnings(as.integer(opts$sample_n %||% cfg$run$sample_n %||% 50000L))
seed <- suppressWarnings(as.integer(opts$seed %||% cfg$run$seed %||% 123L))
if (is.na(seed)) seed <- 123L
if (is.na(sample_n) || sample_n <= 0L) sample_n <- nrow(ev)
sample_case_n <- suppressWarnings(as.integer(opts$sample_evict_cases_n %||% cfg$run$sample_evict_cases_n %||% NA_integer_))

if (sample_mode) {
  if (use_prebuilt_geo_samples) {
    logf("Sample mode ON: prebuilt geo eviction sample rows=", nrow(ev), log_file = log_file)
  } else {
    ev <- make_stratified_sample(ev, n = sample_n, strata_cols = c("bldg_size_bin", "bg_geoid"), seed = seed)
    logf("Sample mode ON: sampled person rows=", nrow(ev), log_file = log_file)
  }

  if (!is.na(sample_case_n) && sample_case_n > 0L) {
    case_ids <- unique(ev$evict_id)
    n_cases_before <- length(case_ids)
    if (sample_case_n < n_cases_before) {
      set.seed(as.integer(seed))
      keep_cases <- sample(case_ids, sample_case_n)
      ev <- ev[evict_id %chin% keep_cases]
      logf("Sample mode ON: sampled eviction cases=", sample_case_n,
           " (from ", n_cases_before, "); person rows now=", nrow(ev),
           log_file = log_file)
    }
  }
}
fwrite(ev[, .(evict_person_id, evict_id, filing_yr, bldg_size_bin, bg_geoid)], out_sample_manifest)
ev_all <- copy(ev)

# ---- Load InfoUSA person rows ----
if (!file.exists(path_infousa_people)) {
  stop("InfoUSA person input not found: ", path_infousa_people)
}
inf <- fread(path_infousa_people)
setDT(inf)
logf("Loaded InfoUSA person rows: ", nrow(inf), " from ", path_infousa_people, log_file = log_file)

if (!all(c("familyid", "person_num", "year", "first_name", "last_name") %in% names(inf))) {
  stop("InfoUSA input must include familyid, person_num, year, first_name, last_name")
}

if (!"n_sn_ss_c" %in% names(inf)) inf[, n_sn_ss_c := NA_character_]
if (!"gender" %in% names(inf)) inf[, gender := NA_character_]
if (!"PID" %in% names(inf)) inf[, PID := NA_character_]
if (!"bg_geoid" %in% names(inf)) inf[, bg_geoid := NA_character_]
if (!"zip" %in% names(inf)) inf[, zip := NA_character_]
if (!"tract_geoid" %in% names(inf)) inf[, tract_geoid := NA_character_]
inf[, familyid := as.character(familyid)]
inf[, year := as.integer(year)]
inf[, bg_geoid := as.character(bg_geoid)]
inf[, zip := normalize_zip_chr(zip)]
inf[, tract_geoid := derive_tract_geoid(tract_geoid, bg_geoid)]

# Drop missing familyid rows per plan.
n_pre_inf <- nrow(inf)
inf <- inf[!is.na(familyid) & nzchar(as.character(familyid))]
logf("Dropped InfoUSA rows with missing familyid: ", n_pre_inf - nrow(inf), log_file = log_file)

# Enrich InfoUSA with address + parsed components from infousa_clean only when needed.
n_inf_missing_addr <- inf[is.na(n_sn_ss_c) | !nzchar(n_sn_ss_c), .N]
share_inf_missing_addr <- if (nrow(inf) > 0L) as.numeric(n_inf_missing_addr) / as.numeric(nrow(inf)) else 0
missing_pm_cols_inf <- setdiff(pm_addr_cols, names(inf))
n_inf_missing_pm_street <- if ("pm.street" %in% names(inf)) {
  inf[is.na(`pm.street`) | !nzchar(as.character(`pm.street`)), .N]
} else {
  nrow(inf)
}
share_inf_missing_pm_street <- if (nrow(inf) > 0L) as.numeric(n_inf_missing_pm_street) / as.numeric(nrow(inf)) else 1
force_infousa_clean_enrich <- parse_trueish(
  opts$force_infousa_clean_enrich %||% cfg$run$force_infousa_clean_enrich,
  default = FALSE
)
need_infousa_clean_enrich <- force_infousa_clean_enrich ||
  (length(missing_pm_cols_inf) > 0L) ||
  (n_inf_missing_addr > 0L && share_inf_missing_addr >= 0.05) ||
  (n_inf_missing_pm_street > 0L && share_inf_missing_pm_street >= 0.05)
if (need_infousa_clean_enrich && file.exists(path_infousa_clean)) {
  ic_hdr <- names(fread(path_infousa_clean, nrows = 0))
  if (!all(pm_addr_cols %in% ic_hdr)) {
    stop("InfoUSA clean product missing required parsed columns: ",
         paste(setdiff(pm_addr_cols, ic_hdr), collapse = ", "))
  }
  ic_pm <- intersect(pm_addr_cols, ic_hdr)
  ic_cols <- intersect(c("familyid", "year", "n_sn_ss_c", ic_pm), ic_hdr)
  if (all(c("familyid", "year") %in% ic_cols)) {
    ic <- fread(path_infousa_clean, select = ic_cols)
    setDT(ic)
    ic[, familyid := as.character(familyid)]
    ic[, year := as.integer(year)]
    ic <- ic[!is.na(familyid) & nzchar(familyid) & !is.na(year)]
    ic <- ic[, c(
      list(n_sn_ss_c = first_nonempty_chr(n_sn_ss_c)),
      lapply(.SD, first_nonempty_chr)
    ), by = .(familyid, year), .SDcols = ic_pm]

    for (cc in ic_pm) {
      if (!cc %in% names(inf)) inf[, (cc) := NA_character_]
    }
    n_before_addr <- inf[!is.na(n_sn_ss_c) & nzchar(n_sn_ss_c), .N]
    n_before_pm_house <- if ("pm.house" %in% names(inf)) inf[!is.na(`pm.house`) & nzchar(as.character(`pm.house`)), .N] else 0L

    inf <- merge(inf, ic, by = c("familyid", "year"), all.x = TRUE, sort = FALSE, suffixes = c("", ".ic"))
    inf[is.na(n_sn_ss_c) | !nzchar(n_sn_ss_c), n_sn_ss_c := n_sn_ss_c.ic]
    drop_ic <- intersect(c("n_sn_ss_c.ic"), names(inf))
    if (length(drop_ic) > 0L) inf[, (drop_ic) := NULL]

    for (cc in ic_pm) {
      ic_col <- paste0(cc, ".ic")
      if (ic_col %in% names(inf)) {
        inf[(is.na(get(cc)) | !nzchar(as.character(get(cc)))) & !is.na(get(ic_col)),
            (cc) := as.character(get(ic_col))]
        inf[, (ic_col) := NULL]
      }
    }
    n_after_addr <- inf[!is.na(n_sn_ss_c) & nzchar(n_sn_ss_c), .N]
    n_after_pm_house <- if ("pm.house" %in% names(inf)) inf[!is.na(`pm.house`) & nzchar(as.character(`pm.house`)), .N] else 0L
    logf("Enriched InfoUSA from infousa_clean: n_sn_ss_c ", n_before_addr, "->", n_after_addr,
         "; pm.house ", n_before_pm_house, "->", n_after_pm_house, log_file = log_file)
  } else {
    logf("InfoUSA clean product missing familyid/year columns; skipping parsed-address enrichment", log_file = log_file)
  }
} else if (!need_infousa_clean_enrich) {
  logf("Skipped infousa_clean enrichment: missing n_sn_ss_c rows=", n_inf_missing_addr,
       " (share=", round(share_inf_missing_addr, 6), "); missing pm.street rows=", n_inf_missing_pm_street,
       " (share=", round(share_inf_missing_pm_street, 6), "); threshold=0.05",
       log_file = log_file)
}

# Optional PID attach via xwalk
if (all(is.na(inf$PID) | !nzchar(inf$PID)) && file.exists(path_infousa_xwalk)) {
  xwi <- fread(path_infousa_xwalk)
  setDT(xwi)
  if (all(c("n_sn_ss_c", "PID") %in% names(xwi))) {
    inf[xwi, PID := i.PID, on = "n_sn_ss_c"]
    logf("Merged InfoUSA PID from xwalk", log_file = log_file)
  }
}

# Street tokens for tier B/C
if (!"street_number" %in% names(inf)) inf[, street_number := NA_character_]
if (!"street_name" %in% names(inf)) inf[, street_name := NA_character_]
pm_street_inf <- compose_street_name_from_pm(inf)
if ("pm.house" %in% names(inf)) {
  inf[(is.na(street_number) | !nzchar(street_number)) & !is.na(`pm.house`) & nzchar(as.character(`pm.house`)),
      street_number := as.character(`pm.house`)]
}
idx_inf_pm_street <- (is.na(inf$street_name) | !nzchar(inf$street_name)) & !is.na(pm_street_inf) & nzchar(pm_street_inf)
if (any(idx_inf_pm_street)) {
  inf[idx_inf_pm_street, street_name := pm_street_inf[idx_inf_pm_street]]
}
inf[(is.na(street_number) | !nzchar(street_number)),
    street_number := str_extract(coalesce_chr(n_sn_ss_c, ""), "^[0-9]+")]
inf[(is.na(street_name) | !nzchar(street_name)),
    street_name := str_squish(str_replace(coalesce_chr(n_sn_ss_c, ""), "^[0-9]+\\s*", ""))]

inf[, first_name_norm := normalize_name(first_name)]
inf[, last_name_norm := normalize_name(last_name)]
inf[, address_pm_norm := compose_address_key_from_pm(.SD)]
inf[, address_norm := fifelse(
  !is.na(address_pm_norm) & nzchar(address_pm_norm),
  address_pm_norm,
  normalize_address_key(n_sn_ss_c)
)]
inf[, street_number := coalesce_chr(street_number, "")]
inf[, street_name := normalize_address_key(street_name)]
street_block_inf <- rep(NA_character_, nrow(inf))
if ("pm.street" %in% names(inf)) {
  street_block_inf <- as.character(inf[["pm.street"]])
}
fill_inf <- is.na(street_block_inf) | !nzchar(trimws(street_block_inf))
street_block_inf[fill_inf] <- inf$street_name[fill_inf]
inf[, street_block := normalize_address_key(street_block_inf)]
inf[, street_block_fuzzy := make_street_fuzzy_key(street_block)]
inf[, gender := normalize_gender(gender)]
use_year_blocking <- parse_trueish(
  opts$use_year_in_blocking %||% cfg$run$use_year_in_blocking,
  default = FALSE
)
use_pid_blocking <- parse_trueish(
  opts$use_pid_in_blocking %||% cfg$run$use_pid_in_blocking,
  default = FALSE
)
use_zip_blocking <- parse_trueish(
  opts$use_zip_in_blocking %||% cfg$run$use_zip_in_blocking,
  default = FALSE
)
use_bg_blocking <- parse_trueish(
  opts$use_bg_in_blocking %||% cfg$run$use_bg_in_blocking,
  default = FALSE
)
use_street_fuzzy_blocking <- parse_trueish(
  opts$use_street_fuzzy_blocking %||% cfg$run$use_street_fuzzy_blocking,
  default = TRUE
)

if (!isTRUE(use_year_blocking)) {
  n_inf_before_collapse <- nrow(inf)
  inf <- inf[!is.na(person_num)]
  inf[, has_addr_key := as.integer(!is.na(address_norm) & nzchar(address_norm))]
  setorder(inf, familyid, person_num, -has_addr_key, -year)
  inf <- inf[, .SD[1L], by = .(familyid, person_num)]
  inf[, has_addr_key := NULL]
  inf[, inf_person_id := paste(coalesce_chr(familyid, ""), coalesce_chr(person_num, ""), sep = "|")]
  logf("Year blocking OFF: collapsed InfoUSA person-years to familyid+person_num: ",
       nrow(inf), " / ", n_inf_before_collapse, " rows", log_file = log_file)
} else {
  inf[, inf_person_id := paste(coalesce_chr(familyid, ""), coalesce_chr(person_num, ""), coalesce_chr(year, ""), sep = "|")]
}

assert_unique(inf, c("inf_person_id"), "InfoUSA people")

restrict_inf_to_evict_tracts <- parse_trueish(
  opts$restrict_inf_to_evict_tracts %||% cfg$run$restrict_inf_to_evict_tracts,
  default = sample_mode
)
if (restrict_inf_to_evict_tracts) {
  ev_tracts <- unique(ev$tract_geoid)
  ev_tracts <- ev_tracts[!is.na(ev_tracts) & nzchar(ev_tracts)]
  if (length(ev_tracts) > 0L && "tract_geoid" %in% names(inf)) {
    n_inf_before <- nrow(inf)
    inf <- inf[tract_geoid %chin% ev_tracts]
    logf("Restricted InfoUSA to eviction tracts: kept ", nrow(inf), " / ", n_inf_before,
         " rows across ", length(ev_tracts), " tracts", log_file = log_file)
  }
}

# Enforce address requirement: no-address rows are ineligible for matching.
ev_addr_before <- nrow(ev)
inf_addr_before <- nrow(inf)
ev <- ev[!is.na(address_norm) & nzchar(address_norm)]
inf <- inf[!is.na(address_norm) & nzchar(address_norm)]
logf("Applied address-required filter (address_norm from pm.* then n_sn_ss_c fallback): eviction persons ",
     nrow(ev), " / ", ev_addr_before,
     "; InfoUSA persons ", nrow(inf), " / ", inf_addr_before,
     log_file = log_file)

# Deterministic exact passes run first; matched eviction persons are removed from candidate pool.
direct_pass <- run_direct_exact_passes(ev, inf, log_file = log_file)
direct_person_matches <- direct_pass$person_matches
ev_remaining <- direct_pass$remaining_ev
logf("Direct exact pass total matches: ", nrow(direct_person_matches),
     "; remaining eviction persons for candidate generation=", nrow(ev_remaining),
     log_file = log_file)

# ---- Candidate generation ----
year_window <- parse_int_vector(opts$year_window %||% cfg$run$year_window %||% c(0L), default = c(0L))
year_links <- make_year_links(ev_remaining, year_window = year_window, use_year = use_year_blocking, log_file = log_file)

if (isTRUE(use_pid_blocking)) {
  tier_a <- build_blocks_tier_A(ev_remaining, inf, year_links, use_year = use_year_blocking, log_file = log_file)
  logf("PID blocking enabled: Tier A candidate generation ON", log_file = log_file)
} else {
  tier_a <- empty_candidate_dt()
  logf("PID blocking disabled: Tier A candidate generation OFF", log_file = log_file)
}
tier_b_geo <- if (isTRUE(use_bg_blocking)) "bg" else "tract"
tier_b <- build_blocks_tier_B(
  ev_remaining, inf, year_links,
  use_year = use_year_blocking,
  geo_level = tier_b_geo,
  use_street_fuzzy = use_street_fuzzy_blocking,
  log_file = log_file
)
logf("Tier B geo blocking level: ", tier_b_geo, log_file = log_file)
logf("Tier B street fuzzy blocking: ", ifelse(use_street_fuzzy_blocking, "ON", "OFF"), log_file = log_file)
if (isTRUE(use_zip_blocking)) {
  tier_c <- build_blocks_tier_C(
    ev_remaining, inf, year_links,
    use_year = use_year_blocking,
    use_street_fuzzy = use_street_fuzzy_blocking,
    log_file = log_file
  )
  logf("ZIP blocking enabled: Tier C candidate generation ON", log_file = log_file)
} else {
  tier_c <- empty_candidate_dt()
  logf("ZIP blocking disabled: Tier C candidate generation OFF", log_file = log_file)
}

candidates_all <- rbindlist(list(tier_a, tier_b, tier_c), use.names = TRUE, fill = TRUE)
logf("Total candidate rows before cap: ", nrow(candidates_all), log_file = log_file)

cap_k <- suppressWarnings(as.integer(opts$candidate_cap_k %||% cfg$run$candidate_cap_k %||% 50L))
if (is.na(cap_k) || cap_k <= 0L) cap_k <- 50L
capped <- enforce_candidate_caps(candidates_all, cap_k = cap_k, log_file = log_file)
candidates <- capped$candidates
candidate_stats_person <- capped$stats

# ---- Scoring + deterministic person-level assignment ----
nick_dt <- load_nickname_dict(path_nicknames, log_file = log_file)
fastlink_chunk_evict_n <- suppressWarnings(as.integer(opts$fastlink_chunk_evict_n %||% cfg$run$fastlink_chunk_evict_n %||% NA_integer_))
if (is.na(fastlink_chunk_evict_n) || fastlink_chunk_evict_n <= 0L) fastlink_chunk_evict_n <- NA_integer_
scored <- run_fastlink_on_pairs(
  candidates,
  nickname_dict = nick_dt,
  fastlink_chunk_evict_n = fastlink_chunk_evict_n,
  log_file = log_file
)
scored <- apply_size_thresholds(scored, log_file = log_file)
person_matches_model <- resolve_one_to_one_deterministic(scored, log_file = log_file)
person_matches <- rbindlist(list(direct_person_matches, person_matches_model), use.names = TRUE, fill = TRUE)
if (nrow(person_matches) > 0L) {
  setorder(person_matches, evict_person_id, -p_match, familyid)
  person_matches <- person_matches[, .SD[1L], by = evict_person_id]
}
logf("Accepted person links after direct+modeled merge: ", nrow(person_matches), log_file = log_file)

# ---- Roll up to case -> household ----
case_hh <- rollup_to_case_household(person_matches, log_file = log_file)
matches <- resolve_case_household_deterministic(case_hh, log_file = log_file)

# Attach case-level context
case_ctx <- ev_all[, .(
  bldg_size_bin = bldg_size_bin[1L],
  bg_geoid = bg_geoid[1L],
  n_defendants_on_case = uniqueN(evict_person_id)
), by = evict_id]

if (nrow(matches) > 0L) {
  matches <- merge(matches, case_ctx, by = c("evict_id", "bldg_size_bin"), all.x = TRUE)
}

# Unmatched case table
all_cases <- unique(ev_all[, .(evict_id, bldg_size_bin, bg_geoid)])
amb_cases <- unique(case_hh[ambiguous_household == TRUE, .(evict_id)])
matched_cases <- unique(matches[, .(evict_id)])

unmatched <- all_cases[!matched_cases, on = "evict_id"]
unmatched[, unmatched_reason := "no_match"]
unmatched[amb_cases, unmatched_reason := "ambiguous_household", on = "evict_id"]

# Candidate stats at case level
candidate_stats_case <- candidate_stats_person[, .(
  num_candidates = sum(num_candidates_raw),
  num_candidates_capped = sum(num_candidates_capped),
  was_capped = any(was_capped),
  tier_attempted = paste(sort(unique(unlist(strsplit(tiers_attempted, ",", fixed = TRUE)))), collapse = ",")
), by = evict_id]

case_match_counts <- person_matches[, .(n_defendants_matched = uniqueN(evict_person_id)), by = evict_id]
case_def_counts <- unique(ev_all[, .(n_defendants_on_case = uniqueN(evict_person_id)), by = evict_id])

candidate_stats_case <- merge(case_def_counts, candidate_stats_case, by = "evict_id", all.x = TRUE)
candidate_stats_case <- merge(candidate_stats_case, case_match_counts, by = "evict_id", all.x = TRUE)

candidate_stats_case[is.na(num_candidates), `:=`(num_candidates = 0, num_candidates_capped = 0, was_capped = FALSE, tier_attempted = "")]
candidate_stats_case[is.na(n_defendants_matched), n_defendants_matched := 0L]

# Optional validation set status log
if (!is.null(path_validation) && nzchar(path_validation) && file.exists(path_validation)) {
  logf("Validation set available: ", path_validation, log_file = log_file)
} else {
  logf("Validation set not provided (cfg$inputs$evict_infousa_validation_set)", log_file = log_file)
}

# ---- Write outputs ----
fwrite(matches, out_matches)
fwrite(unmatched, out_unmatched)
fwrite(candidate_stats_case, out_candidate_stats)

logf("Wrote matches (case->household): ", nrow(matches), " -> ", out_matches, log_file = log_file)
logf("Wrote unmatched cases: ", nrow(unmatched), " -> ", out_unmatched, log_file = log_file)
logf("Wrote candidate stats: ", nrow(candidate_stats_case), " -> ", out_candidate_stats, log_file = log_file)

# ---- QC ----
qc <- qc_tables(ev_all, candidates, scored, matches, candidate_stats_case)
write_qc_tables(qc, qc_dir, prefix = "evict_infousa")
write_review_samples(scored, out_dir = qc_dir, seed = seed)

logf("Wrote QC artifacts to: ", qc_dir, log_file = log_file)
logf("=== Finished link-evictions-infousa-names.R ===", log_file = log_file)
