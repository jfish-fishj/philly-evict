## ============================================================
## make-infousa-link-people.R
## ============================================================
## Purpose: Build a linkage-ready InfoUSA person-year product
##          with names and one-line address on the same row.
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

first_nonempty_chr <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0L) return(NA_character_)
  x[1L]
}

pm_addr_cols <- c("pm.house", "pm.preDir", "pm.street", "pm.streetSuf", "pm.sufDir")

digits_only <- function(x) {
  gsub("[^0-9]", "", as.character(x))
}

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

  # Treat 6-digit tracts as Philadelphia tract codes and prepend county/state.
  ok6 <- !is.na(d) & nchar(d) == 6L
  out[ok6] <- paste0("42101", d[ok6])

  out
}

opts <- parse_cli_args_local(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

log_file <- p_out(cfg, "logs", "make-infousa-link-people.log")
logf("=== Starting make-infousa-link-people.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

path_inf_person <- opts$infousa_people %||% p_product(cfg, "infousa_gender_imputed_person")
path_inf_clean <- opts$infousa_clean %||% p_product(cfg, "infousa_clean")
out_inf_link <- opts$output %||%
  pick_product_key_optional_local(cfg, c("infousa_link_people"), default = p_tmp(cfg, "infousa_link_people.csv"))

if (!file.exists(path_inf_person)) stop("InfoUSA person input not found: ", path_inf_person)
if (!file.exists(path_inf_clean)) stop("InfoUSA clean input not found: ", path_inf_clean)

dir.create(dirname(out_inf_link), recursive = TRUE, showWarnings = FALSE)

inf <- fread(path_inf_person)
setDT(inf)
logf("Loaded InfoUSA person rows: ", nrow(inf), " from ", path_inf_person, log_file = log_file)

need_inf <- c("familyid", "person_num", "year", "first_name", "last_name")
if (!all(need_inf %in% names(inf))) {
  stop("InfoUSA person input missing required columns: ",
       paste(setdiff(need_inf, names(inf)), collapse = ", "))
}

inf[, familyid := as.character(familyid)]
inf[, year := as.integer(year)]
if (!"bg_geoid" %in% names(inf)) inf[, bg_geoid := NA_character_]
if (!"n_sn_ss_c" %in% names(inf)) inf[, n_sn_ss_c := NA_character_]
if (!"zip" %in% names(inf)) inf[, zip := NA_character_]
if (!"tract_geoid" %in% names(inf)) inf[, tract_geoid := NA_character_]

ic_hdr <- names(fread(path_inf_clean, nrows = 0))
if (!all(pm_addr_cols %in% ic_hdr)) {
  stop("InfoUSA clean input missing required parsed address columns: ",
       paste(setdiff(pm_addr_cols, ic_hdr), collapse = ", "))
}
ic_pm <- intersect(pm_addr_cols, ic_hdr)
ic_cols <- intersect(c("familyid", "year", "n_sn_ss_c", "zip", "ge_census_tract", "ge_census_bg", ic_pm), ic_hdr)
if (!all(c("familyid", "year") %in% ic_cols)) {
  stop("InfoUSA clean input must include familyid/year for linkage: ", path_inf_clean)
}

ic <- fread(path_inf_clean, select = ic_cols)
setDT(ic)
ic[, familyid := as.character(familyid)]
ic[, year := as.integer(year)]
if (!"n_sn_ss_c" %in% names(ic)) ic[, n_sn_ss_c := NA_character_]
if (!"zip" %in% names(ic)) ic[, zip := NA_character_]
if (!"ge_census_tract" %in% names(ic)) ic[, ge_census_tract := NA_character_]
if (!"ge_census_bg" %in% names(ic)) ic[, ge_census_bg := NA_character_]
ic_pm <- intersect(pm_addr_cols, names(ic))

ic <- ic[!is.na(familyid) & nzchar(familyid) & !is.na(year)]

ic_y <- ic[, c(
  list(
    n_sn_ss_c = first_nonempty_chr(n_sn_ss_c),
    zip = first_nonempty_chr(zip),
    ge_census_tract = first_nonempty_chr(ge_census_tract),
    ge_census_bg = first_nonempty_chr(ge_census_bg)
  ),
  lapply(.SD, first_nonempty_chr)
), by = .(familyid, year), .SDcols = ic_pm]

ic_f <- ic[, c(
  list(
    n_sn_ss_c = first_nonempty_chr(n_sn_ss_c),
    zip = first_nonempty_chr(zip),
    ge_census_tract = first_nonempty_chr(ge_census_tract),
    ge_census_bg = first_nonempty_chr(ge_census_bg)
  ),
  lapply(.SD, first_nonempty_chr)
), by = familyid, .SDcols = ic_pm]

n_addr_before <- inf[!is.na(n_sn_ss_c) & nzchar(n_sn_ss_c), .N]
n_pm_before <- if ("pm.house" %in% names(inf)) inf[!is.na(`pm.house`) & nzchar(as.character(`pm.house`)), .N] else 0L
for (cc in ic_pm) {
  if (!cc %in% names(inf)) inf[, (cc) := NA_character_]
}

inf[ic_y, `:=`(
  n_sn_ss_c = fifelse(is.na(n_sn_ss_c) | !nzchar(n_sn_ss_c), i.n_sn_ss_c, n_sn_ss_c),
  zip = fifelse(is.na(zip) | !nzchar(zip), i.zip, zip),
  tract_geoid = fifelse(is.na(tract_geoid) | !nzchar(tract_geoid), i.ge_census_tract, tract_geoid)
), on = .(familyid, year)]

inf[ic_f, `:=`(
  n_sn_ss_c = fifelse(is.na(n_sn_ss_c) | !nzchar(n_sn_ss_c), i.n_sn_ss_c, n_sn_ss_c),
  zip = fifelse(is.na(zip) | !nzchar(zip), i.zip, zip),
  tract_geoid = fifelse(is.na(tract_geoid) | !nzchar(tract_geoid), i.ge_census_tract, tract_geoid)
), on = "familyid"]

for (cc in ic_pm) {
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

inf[, bg_geoid := normalize_bg_chr(bg_geoid)]
inf[ic_f, bg_from_clean := i.ge_census_bg, on = "familyid"]
inf[, bg_from_clean := normalize_bg_chr(bg_from_clean)]
inf[, bg_geoid := fifelse(is.na(bg_geoid) | !nzchar(bg_geoid), bg_from_clean, bg_geoid)]

inf[, tract_geoid := normalize_tract_chr(tract_geoid)]
inf[is.na(tract_geoid) & !is.na(bg_geoid) & nchar(bg_geoid) >= 11L, tract_geoid := substr(bg_geoid, 1L, 11L)]
inf[, zip := normalize_zip_chr(zip)]

inf[, bg_from_clean := NULL]

n_addr_after <- inf[!is.na(n_sn_ss_c) & nzchar(n_sn_ss_c), .N]
n_pm_after <- if ("pm.house" %in% names(inf)) inf[!is.na(`pm.house`) & nzchar(as.character(`pm.house`)), .N] else 0L
n_pm_street_after <- if ("pm.street" %in% names(inf)) inf[!is.na(`pm.street`) & nzchar(as.character(`pm.street`)), .N] else 0L
logf("Address line fill on person rows: before=", n_addr_before, "; after=", n_addr_after,
     "; pm.house before=", n_pm_before, "; after=", n_pm_after, log_file = log_file)
logf("Rows with pm.street populated: ", n_pm_street_after, " / ", nrow(inf), log_file = log_file)
logf("Rows with tract_geoid populated: ", inf[!is.na(tract_geoid) & nzchar(tract_geoid), .N], log_file = log_file)
logf("Rows with zip populated: ", inf[!is.na(zip) & nzchar(zip), .N], log_file = log_file)

if (!all(pm_addr_cols %in% names(inf))) {
  stop("Linkage-ready InfoUSA output missing required parsed columns: ",
       paste(setdiff(pm_addr_cols, names(inf)), collapse = ", "))
}

setorder(inf, familyid, person_num, year)
fwrite(inf, out_inf_link)
logf("Wrote linkage-ready InfoUSA file: ", out_inf_link, " (rows=", nrow(inf), ")", log_file = log_file)
logf("=== Finished make-infousa-link-people.R ===", log_file = log_file)
