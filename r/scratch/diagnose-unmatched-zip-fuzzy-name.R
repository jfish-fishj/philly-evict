## ============================================================
## diagnose-unmatched-zip-fuzzy-name.R
## ============================================================
## Purpose:
##   For currently unmatched eviction cases, test whether plausible
##   name matches exist in InfoUSA when blocking only on ZIP code.
##
## Outputs (output/qa):
##   - evict_infousa_unmatched_zip_name_fuzzy_defendant_level.csv
##   - evict_infousa_unmatched_zip_name_fuzzy_case_summary.csv
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(stringdist)
})

source("r/config.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "diagnose-unmatched-zip-fuzzy-name.log")
logf("=== Starting diagnose-unmatched-zip-fuzzy-name.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

coalesce_chr <- function(x, y = "") {
  x <- as.character(x)
  x[is.na(x)] <- y
  x
}

normalize_name <- function(x) {
  x <- coalesce_chr(x, "")
  x <- toupper(x)
  x <- str_replace_all(x, "[’']", "")
  x <- str_replace_all(x, "[^A-Z0-9 ]", " ")
  x <- str_replace_all(x, "\\b(JR|SR|II|III|IV|MR|MRS|MS|DR)\\b", " ")
  x <- str_squish(x)
  x[x == ""] <- NA_character_
  x
}

normalize_zip_chr <- function(x) {
  d <- gsub("[^0-9]", "", as.character(x))
  out <- rep(NA_character_, length(d))
  ok <- !is.na(d) & nzchar(d)
  if (any(ok)) {
    dd <- d[ok]
    out[ok] <- ifelse(
      nchar(dd) >= 5L,
      substr(dd, 1L, 5L),
      sprintf("%05d", suppressWarnings(as.integer(dd)))
    )
  }
  out
}

make_evict_person_id <- function(evict_id, first_name, last_name) {
  paste(
    coalesce_chr(evict_id, ""),
    coalesce_chr(first_name, ""),
    coalesce_chr(last_name, ""),
    sep = "|"
  )
}

first_nonempty_chr <- function(x) {
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0L) NA_character_ else x[1L]
}

path_unmatched <- p_product(cfg, "evict_infousa_hh_unmatched")
path_manifest <- p_product(cfg, "evict_infousa_sample_manifest")
path_ev_sample <- p_product(cfg, "evict_infousa_sample_evict_people")
path_inf_sample <- p_product(cfg, "evict_infousa_sample_infousa_people")
path_ev_clean <- p_product(cfg, "evictions_clean")

for (pth in c(path_unmatched, path_manifest, path_ev_sample, path_inf_sample, path_ev_clean)) {
  if (!file.exists(pth)) stop("Missing required file: ", pth)
}

unmatched <- fread(path_unmatched)
manifest <- fread(path_manifest)
ev_src <- fread(path_ev_sample)
inf_src <- fread(path_inf_sample)

setDT(unmatched)
setDT(manifest)
setDT(ev_src)
setDT(inf_src)

if (!"evict_id" %in% names(unmatched)) stop("Unmatched file missing evict_id")
if (!all(c("evict_id", "evict_person_id") %in% names(manifest))) stop("Manifest missing evict_id/evict_person_id")
if (!all(c("id", "first_name", "last_name") %in% names(ev_src))) stop("Eviction sample missing id/first_name/last_name")
if (!all(c("familyid", "person_num", "first_name", "last_name", "zip") %in% names(inf_src))) {
  stop("InfoUSA sample missing required columns")
}

unmatched_cases <- unique(as.character(unmatched$evict_id))
logf("Unmatched cases loaded: ", length(unmatched_cases), log_file = log_file)

# Reconstruct eviction persons exactly as in linkage script.
ev_src[, evict_id := as.character(id)]
ev_src[, first_name_norm := normalize_name(first_name)]
ev_src[, last_name_norm := normalize_name(last_name)]
ev_src <- ev_src[!is.na(first_name_norm) & !is.na(last_name_norm)]
ev_src <- unique(ev_src, by = c("evict_id", "first_name_norm", "last_name_norm"))
ev_src[, evict_person_id := make_evict_person_id(evict_id, first_name_norm, last_name_norm)]

# Keep only persons in current sample manifest and currently unmatched cases.
ev <- ev_src[manifest[, .(evict_person_id, evict_id)], on = "evict_person_id", nomatch = 0L]
ev <- ev[evict_id %chin% unmatched_cases]

if (nrow(ev) == 0L) stop("No unmatched eviction defendants after sample-manifest filter")

# Attach case ZIP from evictions_clean.
ev_clean_zip <- fread(path_ev_clean, select = c("id", "zip"))
setDT(ev_clean_zip)
ev_clean_zip[, id := as.character(id)]
ev_clean_zip[, zip := normalize_zip_chr(zip)]
ev_clean_zip <- ev_clean_zip[, .(zip = first_nonempty_chr(zip)), by = id]
setnames(ev_clean_zip, "id", "evict_id")

ev <- merge(ev, ev_clean_zip, by = "evict_id", all.x = TRUE, sort = FALSE)
ev[, zip := normalize_zip_chr(zip)]

logf("Unmatched eviction defendants: ", nrow(ev),
     "; with ZIP: ", ev[!is.na(zip) & nzchar(zip), .N],
     log_file = log_file)

# Build InfoUSA people universe (collapse familyid+person_num).
inf_src[, familyid := as.character(familyid)]
inf_src[, person_num := suppressWarnings(as.integer(person_num))]
inf_src[, year := suppressWarnings(as.integer(year))]
inf_src[, zip := normalize_zip_chr(zip)]
inf_src[, first_name_norm := normalize_name(first_name)]
inf_src[, last_name_norm := normalize_name(last_name)]
inf_src <- inf_src[
  !is.na(familyid) & nzchar(familyid) &
    !is.na(person_num) &
    !is.na(zip) & nzchar(zip) &
    !is.na(first_name_norm) & !is.na(last_name_norm)
]
setorder(inf_src, familyid, person_num, -year)
inf <- inf_src[, .SD[1L], by = .(familyid, person_num)]
inf[, inf_person_id := paste(familyid, person_num, sep = "|")]

inf_zip_counts <- inf[, .N, by = zip]
setkey(inf_zip_counts, zip)
setkey(inf, zip)

logf("InfoUSA people after collapse/filter: ", nrow(inf),
     "; ZIPs: ", inf[, uniqueN(zip)],
     log_file = log_file)

ev_eval <- ev[, .(
  evict_id,
  evict_person_id,
  zip,
  first_name_ev = first_name_norm,
  last_name_ev = last_name_norm
)]

ev_eval[, n_zip_candidates := inf_zip_counts[.SD, on = "zip", x.N]]
ev_eval[is.na(n_zip_candidates), n_zip_candidates := 0L]

out <- ev_eval[, .(
  evict_id,
  evict_person_id,
  zip,
  first_name_ev,
  last_name_ev,
  n_zip_candidates,
  best_inf_person_id = NA_character_,
  best_first_name_inf = NA_character_,
  best_last_name_inf = NA_character_,
  best_first_sim = NA_real_,
  best_last_sim = NA_real_,
  best_full_sim = NA_real_,
  best_score = NA_real_,
  best_exact_last_first_sim = NA_real_,
  plausible_high = FALSE,
  plausible_medium = FALSE
)]

for (i in seq_len(nrow(out))) {
  z <- out$zip[i]
  if (is.na(z) || !nzchar(z)) next
  cand <- inf[J(z), nomatch = 0L]
  if (nrow(cand) == 0L) next

  f_ev <- out$first_name_ev[i]
  l_ev <- out$last_name_ev[i]
  if (is.na(f_ev) || is.na(l_ev)) next

  first_sim <- stringdist::stringsim(f_ev, cand$first_name_norm, method = "jw", p = 0.1)
  last_sim <- stringdist::stringsim(l_ev, cand$last_name_norm, method = "jw", p = 0.1)
  full_ev <- paste(f_ev, l_ev)
  full_inf <- paste(cand$first_name_norm, cand$last_name_norm)
  full_sim <- stringdist::stringsim(full_ev, full_inf, method = "jw", p = 0.1)

  score <- 0.65 * last_sim + 0.35 * first_sim
  j <- which.max(score)

  out$best_inf_person_id[i] <- cand$inf_person_id[j]
  out$best_first_name_inf[i] <- cand$first_name_norm[j]
  out$best_last_name_inf[i] <- cand$last_name_norm[j]
  out$best_first_sim[i] <- first_sim[j]
  out$best_last_sim[i] <- last_sim[j]
  out$best_full_sim[i] <- full_sim[j]
  out$best_score[i] <- score[j]

  exact_last <- cand$last_name_norm == l_ev
  if (any(exact_last)) {
    out$best_exact_last_first_sim[i] <- max(first_sim[exact_last], na.rm = TRUE)
  }

  out$plausible_high[i] <- !is.na(out$best_exact_last_first_sim[i]) &&
    out$best_exact_last_first_sim[i] >= 0.92
  out$plausible_medium[i] <- !out$plausible_high[i] &&
    !is.na(out$best_score[i]) &&
    out$best_score[i] >= 0.93 &&
    out$best_last_sim[i] >= 0.90
}

out[, plausible_any := plausible_high | plausible_medium]

case_summary <- out[, .(
  n_defendants = .N,
  has_zip = any(!is.na(zip) & nzchar(zip)),
  any_zip_candidates = any(n_zip_candidates > 0L),
  any_plausible_high = any(plausible_high),
  any_plausible_medium = any(plausible_medium),
  any_plausible = any(plausible_any)
), by = evict_id]

case_summary[, case_status := fifelse(
  has_zip == FALSE, "no_zip",
  fifelse(any_zip_candidates == FALSE, "zip_has_no_inf_candidates",
          fifelse(any_plausible_high == TRUE, "plausible_high",
                  fifelse(any_plausible_medium == TRUE, "plausible_medium", "no_plausible_name_in_zip")))
)]

out_def <- p_out(cfg, "qa", "evict_infousa_unmatched_zip_name_fuzzy_defendant_level.csv")
out_case <- p_out(cfg, "qa", "evict_infousa_unmatched_zip_name_fuzzy_case_summary.csv")
dir.create(dirname(out_def), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_case), recursive = TRUE, showWarnings = FALSE)
fwrite(out, out_def)
fwrite(case_summary, out_case)

status_counts <- case_summary[, .N, by = case_status][order(-N)]
logf("Case status counts:", log_file = log_file)
for (i in seq_len(nrow(status_counts))) {
  logf("  ", status_counts$case_status[i], ": ", status_counts$N[i], log_file = log_file)
}

logf("Wrote defendant-level output: ", out_def, " (rows=", nrow(out), ")", log_file = log_file)
logf("Wrote case-level output: ", out_case, " (rows=", nrow(case_summary), ")", log_file = log_file)
logf("=== Finished diagnose-unmatched-zip-fuzzy-name.R ===", log_file = log_file)

