## ============================================================
## impute-race-preflight.R
## ============================================================
## Purpose: Run a no-WRU pilot for race-imputation prep:
##   - defendant filtering and name cleaning
##   - deterministic id -> PID -> GEOID -> tract merge chain
##   - pre-WRU status assignment and method eligibility
##   - person/case preflight outputs + QA report
##
## Inputs (default via config):
##   - cfg$inputs$evictions_party_names
##   - cfg$inputs$philly_bg_shp
##   - cfg$products$evict_address_xwalk_case
##   - cfg$products$parcel_occupancy_panel
##   - cfg$products$evictions_clean (case-level lat/lon + year)
##   - data/inputs/census/Names_2010Census.csv
##   - data/inputs/name-files/first_nameRaceProbs.csv
##
## Outputs (default):
##   - output/qa/race_imputation_preflight_person.csv
##   - output/qa/race_imputation_preflight_case.csv
##   - output/qa/race_imputation_preflight_qa.txt
##   - output/logs/impute-race-preflight.log
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(sf)
  library(stringr)
})

source("r/config.R")
suppressPackageStartupMessages(source("r/helper-functions.R"))

parse_cli_args <- function(args) {
  out <- list()
  if (length(args) == 0L) return(out)
  for (arg in args) {
    if (!startsWith(arg, "--")) next
    arg <- sub("^--", "", arg)
    parts <- strsplit(arg, "=", fixed = TRUE)[[1]]
    key <- gsub("-", "_", parts[1])
    val <- if (length(parts) >= 2L) paste(parts[-1], collapse = "=") else "TRUE"
    out[[key]] <- val
  }
  out
}

coalesce_chr <- function(x, fallback = "") {
  x <- as.character(x)
  x[is.na(x)] <- fallback
  x
}

normalize_pid <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- str_trim(x)
  x <- str_replace_all(x, "[^0-9]", "")
  x[nchar(x) == 0L] <- NA_character_
  stringr::str_pad(x, width = 9, side = "left", pad = "0")
}

normalize_bg_geoid <- function(x) {
  x <- as.character(x)
  x <- str_trim(x)
  x <- str_replace_all(x, "[^0-9]", "")
  x[nchar(x) == 0L] <- NA_character_
  too_long <- !is.na(x) & nchar(x) > 12L
  x[too_long] <- NA_character_
  short <- !is.na(x) & nchar(x) < 12L
  x[short] <- stringr::str_pad(x[short], width = 12, side = "left", pad = "0")
  x
}

clean_person_name <- function(x) {
  x <- coalesce_chr(x, "")
  x <- toupper(x)
  x <- str_replace_all(x, "[’']", "")
  x <- strip_spousal_phrases(x)
  x <- str_replace_all(x, "([^\\s])&([^\\s])", "\\1 AND \\2")
  x <- str_replace_all(x, "([^\\s]),([^\\s])", "\\1 \\2")
  x <- str_replace_all(x, "&", " AND ")
  x <- str_replace_all(x, "[[:punct:]]", " ")
  x <- str_squish(x)
  x <- strip_spousal_phrases(x)
  x <- strip_occupant_phrases(x)
  # Remove generational suffixes/titles but keep initials for parser handling.
  x <- str_replace_all(x, "\\b(JR|SR|MR|MRS|MS|DR|II|III|IV|V|VI|VII|VIII|IX|X|ESQ|ESQUIRE|MD)\\b", " ")
  x <- fix_spellings(x)
  x <- strip_spousal_phrases(x)
  x <- strip_occupant_phrases(x)
  x <- str_replace_all(x, "[^A-Z ]", " ")
  x <- str_squish(x)
  x
}

combine_name_tokens <- function(tok) {
  if (length(tok) <= 1L) return(tok)
  particles <- c("DE", "DEL", "DELA", "DA", "DI", "LA", "LE", "EL", "VAN", "VON", "ST", "MC", "MAC")
  out <- character(0)
  i <- 1L
  while (i <= length(tok)) {
    if (i < length(tok) && tok[i] %chin% particles && nchar(tok[i + 1L]) > 1L) {
      out <- c(out, paste0(tok[i], tok[i + 1L]))
      i <- i + 2L
    } else {
      out <- c(out, tok[i])
      i <- i + 1L
    }
  }
  out
}

parse_name_one <- function(name_clean, surname_set, first_set) {
  if (is.na(name_clean) || !nzchar(name_clean)) {
    return(c(NA_character_, NA_character_, NA_character_, "0"))
  }

  tok <- unlist(strsplit(name_clean, " ", fixed = TRUE), use.names = FALSE)
  tok <- tok[nzchar(tok)]
  tok <- combine_name_tokens(tok)
  n_tok <- length(tok)
  if (n_tok == 0L) {
    return(c(NA_character_, NA_character_, NA_character_, "0"))
  }
  if (n_tok == 1L) {
    return(c(NA_character_, NA_character_, tok[1], "1"))
  }

  # Handle 2-token names with single-letter initials.
  if (n_tok == 2L && nchar(tok[1]) == 1L && nchar(tok[2]) > 1L) {
    return(c(NA_character_, tok[1], tok[2], "2"))
  }

  # Two-token names: prefer FIRST LAST unless reversal evidence is strong.
  # This encodes a structural prior (~75%) toward the canonical order.
  if (n_tok == 2L) {
    t1 <- tok[1L]
    t2 <- tok[2L]
    t1_first <- t1 %chin% first_set
    t1_surname <- t1 %chin% surname_set
    t2_first <- t2 %chin% first_set
    t2_surname <- t2 %chin% surname_set

    score_default <- 0.75
    score_reversed <- 0.25

    # Evidence for FIRST LAST
    if (t1_first && !t1_surname) score_default <- score_default + 0.30
    if (t2_surname && !t2_first) score_default <- score_default + 0.30
    if (t1_first && !t2_first) score_default <- score_default + 0.10
    if (t2_surname && !t1_surname) score_default <- score_default + 0.10

    # Evidence for LAST FIRST
    if (t1_surname && !t1_first) score_reversed <- score_reversed + 0.30
    if (t2_first && !t2_surname) score_reversed <- score_reversed + 0.30
    if (t1_surname && !t2_surname) score_reversed <- score_reversed + 0.10
    if (t2_first && !t1_first) score_reversed <- score_reversed + 0.10

    # Extra boost for strongly unambiguous reversed pattern.
    if (t1_surname && !t1_first && t2_first && !t2_surname) {
      score_reversed <- score_reversed + 0.30
    }

    if (score_reversed > score_default + 0.10) {
      return(c(t2, NA_character_, t1, "2"))
    }
    return(c(t1, NA_character_, t2, "2"))
  }

  # Canonical first-middle-last pattern with middle initial.
  if (n_tok >= 3L && nchar(tok[2]) == 1L) {
    middle_name <- paste(tok[2:(n_tok - 1L)], collapse = " ")
    return(c(tok[1], middle_name, tok[n_tok], as.character(n_tok)))
  }

  first_name <- tok[1]
  last_name <- tok[n_tok]

  first_is_surname <- tok[1] %chin% surname_set
  last_is_surname <- tok[n_tok] %chin% surname_set
  first_is_first <- tok[1] %chin% first_set
  last_is_first <- tok[n_tok] %chin% first_set

  idx_surname <- which(tok %chin% surname_set)

  # Reversed pattern: "SMITH JOHN"
  if (first_is_surname && last_is_first && !last_is_surname) {
    first_name <- tok[n_tok]
    last_name <- tok[1]
  } else if (first_is_surname &&
             n_tok >= 2L &&
             !(tok[2] %chin% surname_set) &&
             (tok[2] %chin% first_set) &&
             !(tok[1] %chin% first_set)) {
    first_name <- tok[2]
    last_name <- tok[1]
  } else if (length(idx_surname) == 1L) {
    last_name <- tok[idx_surname]
    cand_idx <- setdiff(seq_len(n_tok), idx_surname)
    first_name <- if (length(cand_idx) >= 1L) tok[cand_idx[1]] else NA_character_
  } else if (last_is_surname && first_is_first) {
    first_name <- tok[1]
    last_name <- tok[n_tok]
  }

  # Prevent a single-letter token from becoming first_name.
  if (!is.na(first_name) && nchar(first_name) == 1L) {
    first_name <- NA_character_
  }

  middle_tokens <- character(0)
  if (n_tok >= 3L) {
    middle_tokens <- tok[2:(n_tok - 1L)]
  } else if (n_tok == 2L && nchar(tok[1]) == 1L) {
    middle_tokens <- tok[1]
  }
  middle_name <- if (length(middle_tokens) > 0L) paste(middle_tokens, collapse = " ") else NA_character_
  if (!is.na(middle_name) && !nzchar(middle_name)) middle_name <- NA_character_

  c(first_name, middle_name, last_name, as.character(n_tok))
}

mode_chr <- function(x) {
  x <- x[!is.na(x)]
  if (!length(x)) return(NA_character_)
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

input_path <- opts$input %||% p_input(cfg, "evictions_party_names")
evictions_case_path <- opts$evictions_case %||% p_product(cfg, "evictions_clean")
xwalk_case_path <- opts$xwalk_case %||% p_product(cfg, "evict_address_xwalk_case")
parcel_panel_path <- opts$parcel_panel %||% p_product(cfg, "parcel_occupancy_panel")
bg_shp_path <- opts$bg_shp %||% p_input(cfg, "philly_bg_shp")
surname_path <- opts$surname_file %||% p_in(cfg, "census/Names_2010Census.csv")
firstname_path <- opts$firstname_file %||% p_in(cfg, "name-files/first_nameRaceProbs.csv")

out_person <- opts$output_person %||% p_out(cfg, "qa", "race_imputation_preflight_person.csv")
out_case <- opts$output_case %||% p_out(cfg, "qa", "race_imputation_preflight_case.csv")
out_qa <- opts$output_qa %||% p_out(cfg, "qa", "race_imputation_preflight_qa.txt")

sample_n <- suppressWarnings(as.integer(opts$sample_n %||% NA_character_))
seed <- suppressWarnings(as.integer(opts$seed %||% cfg$run$seed %||% 123))

log_file <- p_out(cfg, "logs", "impute-race-preflight.log")
logf("=== Starting impute-race-preflight.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf("Inputs: names=", input_path, ", xwalk_case=", xwalk_case_path,
     ", parcel_panel=", parcel_panel_path, ", evictions_case=", evictions_case_path,
     ", bg_shp=", bg_shp_path,
     ", surname=", surname_path,
     ", firstname=", firstname_path, log_file = log_file)
logf("Outputs: person=", out_person, ", case=", out_case, ", qa=", out_qa, log_file = log_file)

dir.create(dirname(out_person), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_case), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_qa), recursive = TRUE, showWarnings = FALSE)

logf("Loading main names input...", log_file = log_file)
dt <- fread(input_path)
required_cols <- c("id", "role", "name", "alias_one", "alias_two")
assert_has_cols(dt, required_cols, "names input")
setDT(dt)
n_input <- nrow(dt)

logf("Loading case-level filing dates + coordinates...", log_file = log_file)
case_raw <- fread(evictions_case_path, select = c("id", "d_filing", "year", "latitude", "longitude"))
setDT(case_raw)
case_raw[, case_year := suppressWarnings(as.integer(year))]
if ("d_filing" %in% names(case_raw)) {
  case_raw[is.na(case_year) & !is.na(d_filing),
           case_year := suppressWarnings(as.integer(substr(as.character(d_filing), 1L, 4L)))]
}
case_raw[, latitude := suppressWarnings(as.numeric(latitude))]
case_raw[, longitude := suppressWarnings(as.numeric(longitude))]

coord_conflicts <- case_raw[
  !is.na(id) & is.finite(latitude) & is.finite(longitude),
  .(n_coord = uniqueN(sprintf("%.6f|%.6f", latitude, longitude))),
  by = id
][n_coord > 1L, .N]
logf("Case IDs with >1 distinct lat/lon pair in evictions_clean: ", coord_conflicts, log_file = log_file)

case_meta <- case_raw[!is.na(id), .(
  case_year = {
    yy <- case_year[is.finite(case_year)]
    if (length(yy)) min(yy) else NA_integer_
  },
  latitude = {
    ll <- latitude[is.finite(latitude)]
    if (length(ll)) ll[1L] else NA_real_
  },
  longitude = {
    ll <- longitude[is.finite(longitude)]
    if (length(ll)) ll[1L] else NA_real_
  }
), by = id]
assert_unique(case_meta, "id", "case_meta by id")

dt <- merge(dt, case_meta, by = "id", all.x = TRUE, sort = FALSE)
n_before_year_filter <- nrow(dt)
dt <- dt[!is.na(case_year) & case_year >= 2005L]
n_after_year_filter <- nrow(dt)
logf("Applied case-year filter (>=2005): kept ", n_after_year_filter, " / ",
     n_before_year_filter, " rows", log_file = log_file)

if (!is.na(sample_n) && sample_n > 0L && sample_n < nrow(dt)) {
  set.seed(seed)
  dt <- dt[sample(.N, sample_n)]
  logf("Sample mode: kept ", nrow(dt), " rows from ", n_after_year_filter, " using seed=", seed, log_file = log_file)
}

logf("Loading id->PID xwalk...", log_file = log_file)
xwalk_case <- fread(xwalk_case_path, select = c("id", "PID", "num_parcels_matched"))
setDT(xwalk_case)
xwalk_case[, PID := normalize_pid(PID)]
xwalk_case <- xwalk_case[num_parcels_matched == 1L & !is.na(id) & !is.na(PID)]
xwalk_case <- unique(xwalk_case[, .(id, PID)])
n_xwalk_case_raw <- nrow(xwalk_case)

# Explicit many-to-many handling: keep only ids with exactly one unique PID.
xwalk_id_counts <- xwalk_case[, .(n_pid = uniqueN(PID)), by = id]
n_id_multi_pid <- xwalk_id_counts[n_pid > 1L, .N]
xwalk_case <- xwalk_case[xwalk_id_counts[n_pid == 1L, .(id)], on = "id"]
xwalk_case <- xwalk_case[, .(PID = PID[1L]), by = id]
assert_unique(xwalk_case, "id", "xwalk_case id->PID")
n_xwalk_case_unique <- nrow(xwalk_case)

logf("Loading PID->GEOID panel slice...", log_file = log_file)
pid_geo <- fread(parcel_panel_path, select = c("PID", "GEOID"))
setDT(pid_geo)
pid_geo[, PID := normalize_pid(PID)]
pid_geo[, GEOID := normalize_bg_geoid(GEOID)]
pid_geo <- unique(pid_geo[, .(PID, GEOID)])
n_pid_geo_raw <- nrow(pid_geo)

pid_geo_counts <- pid_geo[!is.na(GEOID), .(n_geoid = uniqueN(GEOID)), by = PID]
n_pid_multi_geoid <- pid_geo_counts[n_geoid > 1L, .N]
pid_geo <- pid_geo[pid_geo_counts[n_geoid == 1L, .(PID)], on = "PID"]
pid_geo <- pid_geo[!is.na(GEOID), .(GEOID = GEOID[1L]), by = PID]
assert_unique(pid_geo, "PID", "pid_geo PID->GEOID")
n_pid_geo_unique <- nrow(pid_geo)

logf("Building parcel-derived case GEOID (id->PID->GEOID)...", log_file = log_file)
case_geo <- merge(xwalk_case, pid_geo, by = "PID", all.x = TRUE)
setDT(case_geo)
case_geo <- unique(case_geo, by = "id")
setnames(case_geo, "GEOID", "GEOID_parcel")
assert_unique(case_geo, "id", "case_geo from parcel chain")

case_geo <- merge(case_meta, case_geo, by = "id", all.x = TRUE, sort = FALSE)
assert_unique(case_geo, "id", "case_geo merged with case_meta")

logf("Running spatial join from case lat/lon to block groups...", log_file = log_file)
if (!file.exists(bg_shp_path)) stop("Missing block-group shapefile: ", bg_shp_path)
bg_sf <- st_read(bg_shp_path, quiet = TRUE)
bg_cols <- names(bg_sf)
bg_geoid_col <- if ("GEOID" %in% bg_cols) {
  "GEOID"
} else if ("GEO_ID" %in% bg_cols) {
  "GEO_ID"
} else if ("geoid" %in% bg_cols) {
  "geoid"
} else {
  stop("Could not find BG GEOID column in shapefile (expected GEO_ID or GEOID).")
}
bg_sf$GEOID_spatial <- normalize_bg_geoid(gsub("^1500000US", "", as.character(bg_sf[[bg_geoid_col]])))
bg_sf <- bg_sf[!is.na(bg_sf$GEOID_spatial), c("GEOID_spatial")]

case_pts <- case_geo[is.finite(latitude) & is.finite(longitude), .(id, longitude, latitude)]
case_pts <- unique(case_pts, by = "id")
n_case_coords <- nrow(case_pts)
if (n_case_coords > 0L) {
  pts_sf <- st_as_sf(case_pts, coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)
  if (!is.na(st_crs(bg_sf)) && st_crs(bg_sf) != st_crs(pts_sf)) {
    pts_sf <- st_transform(pts_sf, st_crs(bg_sf))
  }
  join_sf <- st_join(pts_sf, bg_sf, join = st_intersects, left = TRUE)
  join_dt <- as.data.table(st_drop_geometry(join_sf))
  join_dt[, GEOID_spatial := normalize_bg_geoid(GEOID_spatial)]
  join_dt <- join_dt[, .(GEOID_spatial = mode_chr(GEOID_spatial)), by = id]
  assert_unique(join_dt, "id", "spatial join id->GEOID_spatial")
  case_geo <- merge(case_geo, join_dt, by = "id", all.x = TRUE, sort = FALSE)
} else {
  case_geo[, GEOID_spatial := NA_character_]
}

case_geo[, GEOID_parcel := normalize_bg_geoid(GEOID_parcel)]
case_geo[, GEOID_spatial := normalize_bg_geoid(GEOID_spatial)]
case_geo[, geoid_source := fifelse(!is.na(GEOID_spatial), "spatial_bg",
                                   fifelse(!is.na(GEOID_parcel), "parcel_bg", "missing"))]
case_geo[, geoid_agree := !is.na(GEOID_spatial) & !is.na(GEOID_parcel) & GEOID_spatial == GEOID_parcel]
case_geo[, GEOID := fifelse(!is.na(GEOID_spatial), GEOID_spatial, GEOID_parcel)]
case_geo[, tract_geoid := fifelse(!is.na(GEOID), substr(GEOID, 1L, 11L), NA_character_)]
assert_unique(case_geo, "id", "case_geo final id uniqueness")

n_case_parcel_geoid <- case_geo[!is.na(GEOID_parcel), .N]
n_case_spatial_geoid <- case_geo[!is.na(GEOID_spatial), .N]
n_case_geoid_final <- case_geo[!is.na(GEOID), .N]
n_case_both_geoids <- case_geo[!is.na(GEOID_spatial) & !is.na(GEOID_parcel), .N]
n_case_geoid_agree <- case_geo[geoid_agree == TRUE, .N]
n_case_geoid_disagree <- n_case_both_geoids - n_case_geoid_agree
logf("Case GEOID coverage: parcel=", n_case_parcel_geoid,
     ", spatial=", n_case_spatial_geoid,
     ", preferred=", n_case_geoid_final, log_file = log_file)
logf("Case GEOID agreement where both present: agree=", n_case_geoid_agree,
     " / ", n_case_both_geoids, " (disagree=", n_case_geoid_disagree, ")",
     log_file = log_file)

logf("Joining case GEOID (preferred spatial->parcel) into names table...", log_file = log_file)
setkey(case_geo, id)
dt[, `:=`(
  PID = NA_character_,
  GEOID = NA_character_,
  tract_geoid = NA_character_,
  GEOID_spatial = NA_character_,
  GEOID_parcel = NA_character_,
  geoid_source = NA_character_,
  geoid_agree = NA
)]
dt[case_geo, `:=`(
  PID = i.PID,
  GEOID = i.GEOID,
  tract_geoid = i.tract_geoid,
  GEOID_spatial = i.GEOID_spatial,
  GEOID_parcel = i.GEOID_parcel,
  geoid_source = i.geoid_source,
  geoid_agree = i.geoid_agree
), on = "id"]

logf("Cleaning names and deriving first/middle/last...", log_file = log_file)
dt[, is_defendant := grepl("^Defendant", role, ignore.case = TRUE)]
dt[, is_plaintiff := grepl("^Plaintiff", role, ignore.case = TRUE)]
dt[, name_clean := clean_person_name(name)]

plaintiff_keys <- unique(dt[is_plaintiff == TRUE & !is.na(name_clean) & nzchar(name_clean), .(id, name_clean)])
defendant_keys <- dt[is_defendant == TRUE & !is.na(name_clean) & nzchar(name_clean), .(row_idx = .I, id, name_clean)]
setkey(plaintiff_keys, id, name_clean)
setkey(defendant_keys, id, name_clean)
counterclaim_idx <- defendant_keys[plaintiff_keys, on = .(id, name_clean), nomatch = 0L, unique(row_idx)]

dt[, counterclaim_party_flag := FALSE]
if (length(counterclaim_idx) > 0L) dt[counterclaim_idx, counterclaim_party_flag := TRUE]
dt[, is_analysis_defendant := is_defendant == TRUE & counterclaim_party_flag == FALSE]
n_counterclaim_rows <- dt[counterclaim_party_flag == TRUE, .N]
n_counterclaim_cases <- uniqueN(dt[counterclaim_party_flag == TRUE, id])
logf("Counterclaim filter (defendant appears as plaintiff in same case): rows=", n_counterclaim_rows,
     ", cases=", n_counterclaim_cases, log_file = log_file)

placeholder_pattern <- regex("\\b(UNKNOWN|UNAUTHORIZED|TENANT|TENANTS|RESIDENT|RESIDENTS|ESTATE|ESTATES|HEIRS|AKA)\\b|\\bET\\s+AL\\b",
                             ignore_case = TRUE)
occ_leak_pattern <- occupant_token_regex

dt[, not_person_flag := name_clean == "" |
     str_detect(name_clean, business_or_nonperson_regex) |
     str_detect(name_clean, placeholder_pattern) |
     (
       str_detect(toupper(coalesce_chr(name, "")), initial_amp_initial_regex) &
         !str_detect(toupper(coalesce_chr(name, "")), spousal_marker_regex)
     )]

surnames <- fread(surname_path, select = "name")
surname_set <- unique(toupper(surnames[!is.na(name), name]))
firstnames <- fread(firstname_path, select = "name")
first_set <- unique(toupper(firstnames[!is.na(name), name]))

# Parse only deduplicated analysis-defendant names that pass the person pre-screen.
u_names <- unique(dt[is_analysis_defendant == TRUE & not_person_flag == FALSE, .(name_clean)])
logf("  Unique analysis-defendant names to parse: ", nrow(u_names), log_file = log_file)
parsed <- t(vapply(
  u_names$name_clean,
  FUN = parse_name_one,
  FUN.VALUE = character(4L),
  surname_set = surname_set,
  first_set = first_set
))
u_names[, `:=`(
  first_name = parsed[, 1],
  middle_name = parsed[, 2],
  last_name = parsed[, 3],
  name_token_n = as.integer(parsed[, 4])
)]

# Merge parsed name fields back by deduplicated name key.
dt[, `:=`(
  first_name = NA_character_,
  middle_name = NA_character_,
  last_name = NA_character_,
  name_token_n = as.integer(NA)
)]
setkey(dt, name_clean)
setkey(u_names, name_clean)
dt[u_names, `:=`(
  first_name = i.first_name,
  middle_name = i.middle_name,
  last_name = i.last_name,
  name_token_n = i.name_token_n
)]

# If occupant-style tokens survive parsing, force non-person handling.
dt[, parsed_occ_leak := (
  str_detect(coalesce_chr(name_clean, ""), occ_leak_pattern) |
  str_detect(coalesce_chr(first_name, ""), occ_leak_pattern) |
  str_detect(coalesce_chr(last_name, ""), occ_leak_pattern)
)]
dt[parsed_occ_leak == TRUE, `:=`(
  not_person_flag = TRUE,
  first_name = NA_character_,
  middle_name = NA_character_,
  last_name = NA_character_
)]

dt[, impute_status := NA_character_]
dt[is_defendant == FALSE, impute_status := "not_defendant"]
dt[is_defendant == TRUE & is_analysis_defendant == FALSE, impute_status := "counterclaim_party"]
dt[is_analysis_defendant & is.na(impute_status) & not_person_flag, impute_status := "not_person"]
dt[is_analysis_defendant & is.na(impute_status) & is.na(GEOID), impute_status := "no_tract"]
dt[is_analysis_defendant & is.na(impute_status) & !grepl("^[0-9]{11}$", coalesce_chr(tract_geoid, "")), impute_status := "invalid_tract"]
dt[is_analysis_defendant & is.na(impute_status) & (is.na(last_name) | !nzchar(last_name)), impute_status := "not_person"]
dt[is_analysis_defendant & is.na(impute_status), impute_status := "ok"]

dt[, impute_method := NA_character_]
dt[impute_status == "ok" & !is.na(first_name) & nzchar(first_name), impute_method := "firstname+surname+tract"]
dt[impute_status == "ok" & (is.na(first_name) | !nzchar(first_name)), impute_method := "surname+tract"]

dt[, short_name_id := fifelse(
  !is.na(first_name) & nzchar(first_name) & !is.na(last_name) & nzchar(last_name),
  paste(first_name, last_name, sep = "|"),
  NA_character_
)]

logf("Building case-level preflight summary...", log_file = log_file)
def <- dt[is_analysis_defendant == TRUE]
case_dt <- def[, .(
  n_defendants_total = .N,
  n_defendants_imputed = sum(impute_status == "ok", na.rm = TRUE),
  share_defendants_imputed = fifelse(.N > 0L, sum(impute_status == "ok", na.rm = TRUE) / .N, NA_real_),
  n_defendants_unique_total = uniqueN(short_name_id[!is.na(short_name_id)]),
  n_defendants_unique_imputed = uniqueN(short_name_id[impute_status == "ok" & !is.na(short_name_id)])
), by = id]
case_dt[, case_impute_status := fifelse(n_defendants_imputed > 0L, "ok", "no_imputed_defendants")]

case_reason <- def[, .N, by = .(id, impute_status)][order(id, -N, impute_status)]
case_reason <- case_reason[, .SD[1], by = id][, .(id, case_reason_top = impute_status)]
case_dt <- merge(case_dt, case_reason, by = "id", all.x = TRUE)
case_dt[case_impute_status == "ok", case_reason_top := NA_character_]

status_tab <- dt[, .N, by = impute_status][order(-N)]
method_tab <- dt[, .N, by = impute_method][order(-N)]
case_status_tab <- case_dt[, .N, by = case_impute_status][order(-N)]

n_def <- dt[is_defendant == TRUE, .N]
n_def_analysis <- dt[is_analysis_defendant == TRUE, .N]
n_ok <- dt[is_analysis_defendant == TRUE & impute_status == "ok", .N]
n_no_tract <- dt[is_analysis_defendant == TRUE & impute_status == "no_tract", .N]
n_invalid_tract <- dt[is_analysis_defendant == TRUE & impute_status == "invalid_tract", .N]
n_missing_first <- dt[impute_status == "ok" & (is.na(first_name) | !nzchar(first_name)), .N]
n_occ_clean_ok <- dt[is_analysis_defendant == TRUE & impute_status == "ok" &
                       str_detect(coalesce_chr(name_clean, ""), occ_leak_pattern), .N]
n_occ_first_ok <- dt[is_analysis_defendant == TRUE & impute_status == "ok" &
                       str_detect(coalesce_chr(first_name, ""), occ_leak_pattern), .N]
n_occ_last_ok <- dt[is_analysis_defendant == TRUE & impute_status == "ok" &
                      str_detect(coalesce_chr(last_name, ""), occ_leak_pattern), .N]
n_occ_forced_not_person <- dt[is_analysis_defendant == TRUE & parsed_occ_leak == TRUE, .N]

logf("Writing person-level preflight output...", log_file = log_file)
fwrite(dt, out_person)
logf("  Wrote ", nrow(dt), " rows to ", out_person, log_file = log_file)

logf("Writing case-level preflight output...", log_file = log_file)
fwrite(case_dt, out_case)
logf("  Wrote ", nrow(case_dt), " rows to ", out_case, log_file = log_file)

qa_lines <- c(
  "Race Imputation Preflight QA (No WRU)",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  paste0("Config: ", cfg$meta$config_path),
  "",
  "Reproducibility",
  paste0("  R.version.string: ", R.version.string),
  paste0("  data.table: ", as.character(packageVersion("data.table"))),
  paste0("  stringr: ", as.character(packageVersion("stringr"))),
  paste0("  sf: ", as.character(packageVersion("sf"))),
  "",
  "Input paths",
  paste0("  names: ", input_path),
  paste0("  evictions_case: ", evictions_case_path),
  paste0("  xwalk_case: ", xwalk_case_path),
  paste0("  parcel_panel: ", parcel_panel_path),
  paste0("  bg_shp: ", bg_shp_path),
  paste0("  surname file: ", surname_path),
  paste0("  first-name file: ", firstname_path),
  "",
  "Join diagnostics",
  paste0("  names rows: ", n_input),
  paste0("  rows after case_year >= 2005 filter: ", n_after_year_filter),
  paste0("  xwalk id->PID rows after num_parcels_matched==1 + distinct(id,PID): ", n_xwalk_case_raw),
  paste0("  xwalk ids with >1 PID dropped: ", n_id_multi_pid),
  paste0("  xwalk id->PID rows after uniqueness filter: ", n_xwalk_case_unique),
  paste0("  PID->GEOID rows after distinct(PID,GEOID): ", n_pid_geo_raw),
  paste0("  PID with >1 GEOID dropped: ", n_pid_multi_geoid),
  paste0("  PID->GEOID rows after uniqueness filter: ", n_pid_geo_unique),
  paste0("  case IDs with finite coordinates: ", n_case_coords),
  paste0("  case IDs with parcel GEOID: ", n_case_parcel_geoid),
  paste0("  case IDs with spatial GEOID: ", n_case_spatial_geoid),
  paste0("  case IDs with preferred GEOID: ", n_case_geoid_final),
  paste0("  case IDs with both spatial + parcel GEOID: ", n_case_both_geoids),
  paste0("  agreement count where both available: ", n_case_geoid_agree),
  paste0("  disagreement count where both available: ", n_case_geoid_disagree),
  paste0("  agreement rate where both available: ",
         ifelse(n_case_both_geoids > 0, round(n_case_geoid_agree / n_case_both_geoids, 6), NA_real_)),
  "",
  "Defendant coverage",
  paste0("  defendant rows (raw): ", n_def),
  paste0("  defendant rows excluded as counterclaim_party: ", n_counterclaim_rows,
         " across ", n_counterclaim_cases, " cases"),
  paste0("  defendant rows (analysis): ", n_def_analysis),
  paste0("  ok rows (eligible for WRU): ", n_ok),
  paste0("  no_tract rows: ", n_no_tract),
  paste0("  invalid_tract rows: ", n_invalid_tract),
  paste0("  ok rows missing first_name (surname+tract fallback candidate): ", n_missing_first),
  paste0("  parsed occupant leaks forced to not_person: ", n_occ_forced_not_person),
  paste0("  OCC-token leaks on ok rows (name_clean): ", n_occ_clean_ok),
  paste0("  OCC-token leaks on ok rows (first_name): ", n_occ_first_ok),
  paste0("  OCC-token leaks on ok rows (last_name): ", n_occ_last_ok),
  ""
)

status_text <- capture.output(print(status_tab))
method_text <- capture.output(print(method_tab))
case_text <- capture.output(print(case_status_tab))

qa_full <- c(
  qa_lines,
  "Counts by impute_status",
  status_text,
  "",
  "Counts by impute_method",
  method_text,
  "",
  "Counts by case_impute_status",
  case_text
)

writeLines(qa_full, con = out_qa)
logf("Wrote QA report: ", out_qa, log_file = log_file)
logf("=== Finished impute-race-preflight.R ===", log_file = log_file)
