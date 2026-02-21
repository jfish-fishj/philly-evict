## ============================================================
## make-link-evictions-infousa-sample.R
## ============================================================
## Purpose: Build geo-focused linkage sample inputs for fast iteration.
##          Keeps all eviction people and all InfoUSA people in selected
##          zips/tracts/block-groups.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
})

source("r/config.R")

parse_cli_args_local <- function(args) {
  out <- list()
  if (length(args) == 0L) return(out)
  for (arg in args) {
    if (!startsWith(arg, "--")) next
    arg <- sub("^--", "", arg)
    parts <- strsplit(arg, "=", fixed = TRUE)[[1L]]
    key <- gsub("-", "_", parts[1L])
    val <- if (length(parts) >= 2L) paste(parts[-1L], collapse = "=") else "TRUE"
    out[[key]] <- val
  }
  out
}

pick_product_key_optional_local <- function(cfg, keys, default = NULL) {
  for (k in keys) {
    if (!is.null(cfg$products[[k]])) return(p_product(cfg, k))
  }
  default
}

digits_only <- function(x) gsub("[^0-9]", "", as.character(x))

normalize_zip_chr <- function(x) {
  d <- digits_only(x)
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

normalize_bg_chr <- function(x) {
  d <- digits_only(x)
  out <- rep(NA_character_, length(d))
  ok12 <- !is.na(d) & nchar(d) == 12L
  out[ok12] <- d[ok12]
  out
}

normalize_tract_chr <- function(x) {
  d <- digits_only(x)
  out <- rep(NA_character_, length(d))
  ok11 <- !is.na(d) & nchar(d) == 11L
  out[ok11] <- d[ok11]
  ok12 <- !is.na(d) & nchar(d) == 12L
  out[ok12] <- substr(d[ok12], 1L, 11L)
  ok6 <- !is.na(d) & nchar(d) == 6L
  out[ok6] <- paste0("42101", d[ok6])
  out
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

first_nonempty_chr <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0L) NA_character_ else x[1L]
}

pm_addr_cols <- c("pm.house", "pm.preDir", "pm.street", "pm.streetSuf", "pm.sufDir")

file_has_pm_cols <- function(path, pm_cols) {
  if (!file.exists(path)) return(FALSE)
  hdr <- names(fread(path, nrows = 0))
  all(pm_cols %in% hdr)
}

parse_chr_vector <- function(x) {
  if (is.null(x)) return(character())
  if (is.atomic(x) && length(x) > 1L) return(as.character(x))
  x <- as.character(x)
  if (length(x) == 0L) return(character())
  x <- gsub("\\[|\\]|\\s", "", x)
  if (length(x) == 0L) return(character())
  if (!nzchar(x)) return(character())
  unlist(strsplit(x, ",", fixed = TRUE), use.names = FALSE)
}

opts <- parse_cli_args_local(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

log_file <- p_out(cfg, "logs", "make-link-evictions-infousa-sample.log")
logf("=== Starting make-link-evictions-infousa-sample.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

default_evict_people <- p_out(cfg, "qa", "race_imputed_person_sample.csv")
if (!file.exists(default_evict_people)) {
  default_evict_people <- p_out(cfg, "qa", "race_imputation_preflight_person.csv")
}
path_evict_people <- opts$eviction_people %||% default_evict_people
path_evictions_clean <- opts$evictions_clean %||% p_product(cfg, "evictions_clean")
path_infousa_link <- opts$infousa_people %||%
  pick_product_key_optional_local(cfg, c("infousa_link_people"), default = "")
path_infousa_clean <- opts$infousa_clean %||% p_product(cfg, "infousa_clean")

out_evict_sample <- p_product(cfg, "evict_infousa_sample_evict_people")
out_inf_sample <- p_product(cfg, "evict_infousa_sample_infousa_people")
out_manifest <- pick_product_key_optional_local(
  cfg,
  c("evict_infousa_sample_geo_manifest"),
  default = p_out(cfg, "qa", "evict_infousa_sample_geo_manifest.csv")
)

for (p in c(out_evict_sample, out_inf_sample, out_manifest)) {
  dir.create(dirname(p), recursive = TRUE, showWarnings = FALSE)
}

sel_zips <- normalize_zip_chr(parse_chr_vector(opts$sample_zips %||% cfg$run$link_sample_zips %||% character()))
sel_tracts <- normalize_tract_chr(parse_chr_vector(opts$sample_tracts %||% cfg$run$link_sample_tracts %||% character()))
sel_bgs <- normalize_bg_chr(parse_chr_vector(opts$sample_bg_geoids %||% cfg$run$link_sample_bg_geoids %||% character()))

sel_zips <- unique(sel_zips[!is.na(sel_zips) & nzchar(sel_zips)])
sel_tracts <- unique(sel_tracts[!is.na(sel_tracts) & nzchar(sel_tracts)])
sel_bgs <- unique(sel_bgs[!is.na(sel_bgs) & nzchar(sel_bgs)])

if (length(sel_zips) == 0L && length(sel_tracts) == 0L && length(sel_bgs) == 0L) {
  stop("No sample geography filters provided. Set run.link_sample_zips/tracts/bg_geoids or pass --sample-zips/--sample-tracts/--sample-bg-geoids.")
}

logf("Sample zips: ", if (length(sel_zips) > 0L) paste(sel_zips, collapse = ",") else "<none>", log_file = log_file)
logf("Sample tracts: ", if (length(sel_tracts) > 0L) paste(sel_tracts, collapse = ",") else "<none>", log_file = log_file)
logf("Sample block-groups: ", if (length(sel_bgs) > 0L) paste(sel_bgs, collapse = ",") else "<none>", log_file = log_file)

if (!file.exists(path_evict_people)) stop("Eviction people input not found: ", path_evict_people)
if (!file.exists(path_evictions_clean)) stop("Evictions clean input not found: ", path_evictions_clean)

need_build_inf_link <- !nzchar(path_infousa_link) || !file.exists(path_infousa_link)
if (need_build_inf_link) {
  logf("infousa_link_people not found; building it first via make-infousa-link-people.R", log_file = log_file)
  cmd <- c("r/make-infousa-link-people.R", paste0("--config=", cfg$meta$config_path))
  status <- system2("Rscript", args = cmd)
  path_infousa_link <- p_product(cfg, "infousa_link_people")
  if (!identical(status, 0L) || !file.exists(path_infousa_link)) {
    stop("Failed to build infousa_link_people. Expected file: ", path_infousa_link)
  }
}

ev <- fread(path_evict_people)
setDT(ev)
logf("Loaded eviction people rows: ", nrow(ev), " from ", path_evict_people, log_file = log_file)

if (!"id" %in% names(ev)) stop("Eviction people input must include id")
ev[, evict_id := as.character(id)]

if (!"tract_geoid" %in% names(ev)) ev[, tract_geoid := NA_character_]
if (!"filing_yr" %in% names(ev)) ev[, filing_yr := NA_integer_]
if (!"commercial" %in% names(ev)) ev[, commercial := NA_integer_]
bg_cols <- intersect(c("GEOID", "GEOID_spatial", "GEOID_parcel"), names(ev))
if ("geo_chr" %in% names(ev)) bg_cols <- unique(c(bg_cols, "geo_chr"))
ev[, ev_bg_raw := NA_character_]
if (length(bg_cols) > 0L) {
  for (cc in bg_cols) {
    ev[(is.na(ev_bg_raw) | !nzchar(ev_bg_raw)) & !is.na(get(cc)) & nzchar(as.character(get(cc))),
       ev_bg_raw := as.character(get(cc))]
  }
}
ev[, ev_bg_geoid := normalize_bg_chr(ev_bg_raw)]
ev[, ev_tract_geoid := normalize_tract_chr(tract_geoid)]
ev[is.na(ev_tract_geoid) & !is.na(ev_bg_geoid) & nzchar(ev_bg_geoid), ev_tract_geoid := substr(ev_bg_geoid, 1L, 11L)]
ev[, ev_bg_raw := NULL]

ec_hdr <- names(fread(path_evictions_clean, nrows = 0))
ec_pm <- intersect(pm_addr_cols, ec_hdr)
if (!all(pm_addr_cols %in% ec_hdr)) {
  stop("evictions_clean is missing required parsed address columns: ",
       paste(setdiff(pm_addr_cols, ec_hdr), collapse = ", "))
}
ec_cols <- intersect(c("id", "zip", "filing_yr", "year", "commercial", "n_sn_ss_c", ec_pm), ec_hdr)
if (!"id" %in% ec_cols || !"zip" %in% ec_cols) {
  stop("evictions_clean must include id and zip for geo sampling")
}
ev[, filing_yr := suppressWarnings(as.integer(filing_yr))]
ev[, commercial := normalize_commercial_flag(commercial)]

need_year_from_ec <- ev[!is.na(filing_yr), .N] == 0L
need_com_from_ec <- ev[!is.na(commercial), .N] == 0L
if (need_year_from_ec && !("filing_yr" %in% ec_cols || "year" %in% ec_cols)) {
  stop("Missing filing year in eviction people and evictions_clean (need filing_yr or year).")
}
if (need_com_from_ec && !"commercial" %in% ec_cols) {
  stop("Missing commercial flag in eviction people and evictions_clean.")
}
ec <- fread(path_evictions_clean, select = ec_cols)
setDT(ec)
ec[, evict_id := as.character(id)]
ec[, ev_zip := normalize_zip_chr(zip)]
if (!"filing_yr" %in% names(ec) && "year" %in% names(ec)) ec[, filing_yr := suppressWarnings(as.integer(year))]
if (!"filing_yr" %in% names(ec)) ec[, filing_yr := NA_integer_]
if (!"commercial" %in% names(ec)) ec[, commercial := NA_integer_]
ec[, filing_yr := suppressWarnings(as.integer(filing_yr))]
ec[, commercial := normalize_commercial_flag(commercial)]
ec_pm <- intersect(pm_addr_cols, names(ec))
ec <- ec[, c(
  list(
  ev_zip = {
    x <- ev_zip[!is.na(ev_zip) & nzchar(ev_zip)]
    if (length(x) == 0L) NA_character_ else x[1L]
  },
  filing_yr = {
    x <- filing_yr[!is.na(filing_yr)]
    if (length(x) == 0L) NA_integer_ else as.integer(x[1L])
  },
  commercial = {
    x <- commercial[!is.na(commercial)]
    if (length(x) == 0L) NA_integer_ else as.integer(x[1L])
  },
  n_sn_ss_c = {
    x <- as.character(n_sn_ss_c)
    x <- x[!is.na(x) & nzchar(x)]
    if (length(x) == 0L) NA_character_ else x[1L]
  }),
  lapply(.SD, first_nonempty_chr)
), by = evict_id, .SDcols = ec_pm]
ev[ec, ev_zip := i.ev_zip, on = "evict_id"]
ev[ec, filing_yr := fifelse(is.na(filing_yr), i.filing_yr, filing_yr), on = "evict_id"]
ev[ec, commercial := fifelse(is.na(commercial), i.commercial, commercial), on = "evict_id"]
if (!"n_sn_ss_c" %in% names(ev)) ev[, n_sn_ss_c := NA_character_]
ev[ec, n_sn_ss_c := fifelse(is.na(n_sn_ss_c) | !nzchar(n_sn_ss_c), i.n_sn_ss_c, n_sn_ss_c), on = "evict_id"]
for (cc in ec_pm) {
  if (!cc %in% names(ev)) ev[, (cc) := NA_character_]
  ev[ec, (cc) := fifelse(is.na(get(cc)) | !nzchar(as.character(get(cc))), as.character(get(paste0("i.", cc))), as.character(get(cc))), on = "evict_id"]
}

year_min <- suppressWarnings(as.integer(opts$eviction_year_min %||% cfg$run$eviction_year_min %||% 2006L))
year_max <- suppressWarnings(as.integer(opts$eviction_year_max %||% cfg$run$eviction_year_max %||% 2019L))
if (is.na(year_min)) year_min <- 2006L
if (is.na(year_max)) year_max <- 2019L
if (year_min > year_max) stop("Invalid eviction year filter bounds: min > max")

n_before_year <- nrow(ev)
ev <- ev[!is.na(filing_yr) & filing_yr >= year_min & filing_yr <= year_max]
logf("Applied eviction year filter [", year_min, ",", year_max, "] in sample builder: kept ",
     nrow(ev), " / ", n_before_year, " person rows", log_file = log_file)

n_before_comm <- nrow(ev)
ev <- ev[!is.na(commercial) & commercial == 0L]
logf("Applied non-commercial filter (commercial==0) in sample builder: kept ",
     nrow(ev), " / ", n_before_comm, " person rows", log_file = log_file)

entity_or_nonperson <- rep(FALSE, nrow(ev))
if ("is_entity" %in% names(ev)) {
  entity_or_nonperson <- entity_or_nonperson | flag_is_true(ev$is_entity)
}
if ("not_person_flag" %in% names(ev)) {
  entity_or_nonperson <- entity_or_nonperson | flag_is_true(ev$not_person_flag)
}
if (any(entity_or_nonperson, na.rm = TRUE)) {
  ev <- ev[!entity_or_nonperson]
  logf("Dropped rows flagged as entity/non-person (is_entity/not_person_flag): ",
       sum(entity_or_nonperson, na.rm = TRUE), log_file = log_file)
}
if (nrow(ev) == 0L) stop("No eviction person rows remain after year/commercial/entity filters.")

case_geo <- unique(ev[, .(evict_id, ev_zip, ev_tract_geoid, ev_bg_geoid)])

case_geo[, keep_case := FALSE]
if (length(sel_zips) > 0L) case_geo[ev_zip %chin% sel_zips, keep_case := TRUE]
if (length(sel_tracts) > 0L) case_geo[ev_tract_geoid %chin% sel_tracts, keep_case := TRUE]
if (length(sel_bgs) > 0L) case_geo[ev_bg_geoid %chin% sel_bgs, keep_case := TRUE]

keep_cases <- case_geo[keep_case == TRUE, unique(evict_id)]
if (length(keep_cases) == 0L) {
  stop("No eviction cases matched selected geographies.")
}

ev_sample <- ev[evict_id %chin% keep_cases]
ev_sample[, c("ev_zip", "ev_tract_geoid", "ev_bg_geoid") := NULL]
fwrite(ev_sample, out_evict_sample)
logf("Wrote eviction sample rows: ", nrow(ev_sample), " (cases=", uniqueN(ev_sample$evict_id), ") -> ", out_evict_sample, log_file = log_file)

inf <- fread(path_infousa_link)
setDT(inf)
logf("Loaded InfoUSA linkage rows: ", nrow(inf), " from ", path_infousa_link, log_file = log_file)

if (!all(c("familyid", "person_num", "year", "first_name", "last_name") %in% names(inf))) {
  stop("InfoUSA linkage file missing required columns")
}

if (!"zip" %in% names(inf)) inf[, zip := NA_character_]
if (!"tract_geoid" %in% names(inf)) inf[, tract_geoid := NA_character_]
if (!"bg_geoid" %in% names(inf)) inf[, bg_geoid := NA_character_]
if (!"n_sn_ss_c" %in% names(inf)) inf[, n_sn_ss_c := NA_character_]

missing_pm_cols_inf <- setdiff(pm_addr_cols, names(inf))
if (length(missing_pm_cols_inf) > 0L) {
  if (!file.exists(path_infousa_clean)) {
    stop("InfoUSA linkage rows are missing parsed address columns (",
         paste(missing_pm_cols_inf, collapse = ", "),
         ") and infousa_clean is unavailable for enrichment: ", path_infousa_clean)
  }
  logf("InfoUSA linkage rows missing parsed columns (", paste(missing_pm_cols_inf, collapse = ","),
       "); enriching from infousa_clean", log_file = log_file)

  ic_hdr <- names(fread(path_infousa_clean, nrows = 0))
  if (!all(c("familyid", "year") %in% ic_hdr)) {
    stop("infousa_clean must include familyid/year for parsed address enrichment.")
  }
  if (!all(pm_addr_cols %in% ic_hdr)) {
    stop("infousa_clean missing required parsed address columns: ",
         paste(setdiff(pm_addr_cols, ic_hdr), collapse = ", "))
  }
  ic_cols <- c("familyid", "year", pm_addr_cols)
  ic <- fread(path_infousa_clean, select = ic_cols)
  setDT(ic)
  ic[, familyid := as.character(familyid)]
  ic[, year := suppressWarnings(as.integer(year))]
  ic <- ic[!is.na(familyid) & nzchar(familyid) & !is.na(year)]

  ic_y <- ic[, lapply(.SD, first_nonempty_chr), by = .(familyid, year), .SDcols = pm_addr_cols]
  ic_f <- ic[, lapply(.SD, first_nonempty_chr), by = familyid, .SDcols = pm_addr_cols]

  for (cc in pm_addr_cols) {
    if (!cc %in% names(inf)) inf[, (cc) := NA_character_]
  }

  n_before_pm_street <- inf[!is.na(`pm.street`) & nzchar(as.character(`pm.street`)), .N]
  for (cc in pm_addr_cols) {
    inf[ic_y, (cc) := fifelse(
      is.na(get(cc)) | !nzchar(as.character(get(cc))),
      as.character(get(paste0("i.", cc))),
      as.character(get(cc))
    ), on = .(familyid, year)]
    inf[ic_f, (cc) := fifelse(
      is.na(get(cc)) | !nzchar(as.character(get(cc))),
      as.character(get(paste0("i.", cc))),
      as.character(get(cc))
    ), on = "familyid"]
  }
  n_after_pm_street <- inf[!is.na(`pm.street`) & nzchar(as.character(`pm.street`)), .N]
  logf("Enriched InfoUSA parsed columns from infousa_clean: pm.street ",
       n_before_pm_street, "->", n_after_pm_street, log_file = log_file)
}

inf[, zip_norm := normalize_zip_chr(zip)]
inf[, tract_norm := normalize_tract_chr(tract_geoid)]
inf[, bg_norm := normalize_bg_chr(bg_geoid)]
inf[is.na(tract_norm) & !is.na(bg_norm) & nchar(bg_norm) >= 11L, tract_norm := substr(bg_norm, 1L, 11L)]

inf[, keep_row := FALSE]
if (length(sel_zips) > 0L) inf[zip_norm %chin% sel_zips, keep_row := TRUE]
if (length(sel_tracts) > 0L) inf[tract_norm %chin% sel_tracts, keep_row := TRUE]
if (length(sel_bgs) > 0L) inf[bg_norm %chin% sel_bgs, keep_row := TRUE]

inf_sample <- inf[keep_row == TRUE]
inf_sample[, c("zip_norm", "tract_norm", "bg_norm", "keep_row") := NULL]
fwrite(inf_sample, out_inf_sample)
logf("Wrote InfoUSA sample rows: ", nrow(inf_sample), " (households=", uniqueN(inf_sample$familyid), ") -> ", out_inf_sample, log_file = log_file)

geo_manifest <- rbindlist(list(
  data.table(filter_type = "zip", value = sel_zips),
  data.table(filter_type = "tract", value = sel_tracts),
  data.table(filter_type = "bg_geoid", value = sel_bgs)
), use.names = TRUE, fill = TRUE)
fwrite(geo_manifest, out_manifest)
logf("Wrote sample geo manifest: ", out_manifest, log_file = log_file)
logf("=== Finished make-link-evictions-infousa-sample.R ===", log_file = log_file)
