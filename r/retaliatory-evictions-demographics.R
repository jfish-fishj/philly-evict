## ============================================================
## retaliatory-evictions-demographics.R
## ============================================================
## Purpose: Unified retaliatory-evictions script for race, gender,
## and first-pass intersectionality analysis.
##
## Inputs:
##   - output/qa/race_imputed_case_*.csv
##   - output/qa/gender_imputed_case_*.csv
##   - processed/clean/evictions_clean.csv
##   - processed/xwalks/philly_evict_address_agg_xwalk.csv
##   - processed/panels/building_data_rental_quarter.parquet (preferred)
##   - processed/analytic/bldg_panel_blp.csv
##
## Outputs:
##   - output/retaliatory/*
##   - output/logs/retaliatory-evictions-demographics.log
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(fixest)
})

source("r/config.R")

parse_cli_args <- function(args) {
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

to_int <- function(x, default) {
  if (is.null(x) || length(x) == 0L) return(as.integer(default))
  x1 <- x[[1L]]
  if (is.na(x1) || !nzchar(as.character(x1))) return(as.integer(default))
  v <- suppressWarnings(as.integer(x1))
  if (is.na(v)) as.integer(default) else v
}

to_num <- function(x) suppressWarnings(as.numeric(x))

to_num_scalar <- function(x, default) {
  if (is.null(x) || length(x) == 0L) return(as.numeric(default))
  x1 <- x[[1L]]
  if (is.na(x1) || !nzchar(as.character(x1))) return(as.numeric(default))
  v <- suppressWarnings(as.numeric(x1))
  if (is.na(v)) as.numeric(default) else v
}

pick_race_case_path <- function(cfg, cli_path = NULL) {
  if (!is.null(cli_path) && nzchar(cli_path) && file.exists(cli_path)) return(cli_path)
  candidates <- c(
    p_out(cfg, "qa", "race_imputed_case_sample.csv"),
    p_out(cfg, "qa", "race_imputed_case_full_geoaware_bg.csv"),
    p_out(cfg, "qa", "race_imputed_case_full.csv")
  )
  candidates <- candidates[file.exists(candidates)]
  if (!length(candidates)) stop("No race imputed case file found in output/qa. Pass --race_case=... explicitly.")
  candidates[1L]
}

pick_gender_case_path <- function(cfg, cli_path = NULL) {
  if (!is.null(cli_path) && nzchar(cli_path) && file.exists(cli_path)) return(cli_path)
  candidates <- c(
    p_out(cfg, "qa", "gender_imputed_case_sample.csv"),
    p_out(cfg, "qa", "gender_imputed_case_full.csv")
  )
  candidates <- candidates[file.exists(candidates)]
  if (!length(candidates)) stop("No gender imputed case file found in output/qa. Pass --gender_case=... explicitly.")
  candidates[1L]
}

pick_quarter_panel_path <- function(cfg, cli_path = NULL) {
  if (!is.null(cli_path) && nzchar(cli_path)) return(cli_path)
  candidates <- c(
    p_proc(cfg, "panels", "building_data_quarter.parquet"),
    p_product(cfg, "building_data_rental_quarter"),
    p_proc(cfg, "panels", "building_data_quarter.csv")
  )
  found <- candidates[file.exists(candidates)]
  if (length(found)) return(found[1L])
  candidates[1L]
}

normalize_tract11 <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- gsub("[^0-9]", "", x)
  x[nchar(x) == 0L] <- NA_character_
  short <- !is.na(x) & nchar(x) < 11L
  x[short] <- str_pad(x[short], width = 11L, side = "left", pad = "0")
  long <- !is.na(x) & nchar(x) > 11L
  x[long] <- substr(x[long], 1L, 11L)
  x
}

quarter_from_date <- function(d) {
  y <- as.integer(format(d, "%Y"))
  m <- as.integer(format(d, "%m"))
  q <- ((m - 1L) %/% 3L) + 1L
  paste0(y, "_Q", q)
}

build_pid_filter_cmd <- function(pid_file, csv_file) {
  sprintf(
    "awk -F, 'NR==FNR{keep[$1]=1; next} FNR==1 || ($1 in keep)' %s %s",
    shQuote(pid_file),
    shQuote(csv_file)
  )
}

read_panel_slice <- function(panel_path, select_cols, pid_filter = NULL) {
  ext <- tolower(tools::file_ext(panel_path))
  if (ext == "parquet") {
    if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("Package 'arrow' is required to read parquet: ", panel_path)
    }
    ds <- arrow::open_dataset(panel_path, format = "parquet")
    avail <- names(ds)
    keep <- intersect(select_cols, avail)
    dt <- as.data.table(arrow::read_parquet(panel_path, col_select = keep))
  } else {
    hdr <- names(fread(panel_path, nrows = 0L))
    keep <- intersect(select_cols, hdr)
    if (!is.null(pid_filter) && length(pid_filter)) {
      pid_file <- tempfile(fileext = ".txt")
      writeLines(pid_filter, pid_file)
      on.exit(unlink(pid_file), add = TRUE)
      cmd <- build_pid_filter_cmd(pid_file, panel_path)
      dt <- fread(cmd = cmd, select = keep)
    } else {
      dt <- fread(panel_path, select = keep)
    }
  }
  setDT(dt)
  dt
}

extract_effects <- function(model, model_name) {
  ct <- as.data.table(summary(model)$coeftable, keep.rownames = "term")
  if ("Estimate" %in% names(ct)) setnames(ct, "Estimate", "estimate")
  if ("Std. Error" %in% names(ct)) setnames(ct, "Std. Error", "std_error")
  if ("Pr(>|t|)" %in% names(ct)) setnames(ct, "Pr(>|t|)", "p_value")
  if ("Pr(>|z|)" %in% names(ct)) setnames(ct, "Pr(>|z|)", "p_value")
  if ("t value" %in% names(ct)) setnames(ct, "t value", "stat_value")
  if ("z value" %in% names(ct)) setnames(ct, "z value", "stat_value")
  if (!all(c("estimate", "std_error", "p_value") %in% names(ct))) {
    stop("Unexpected coefficient table columns for model: ", model_name)
  }
  ct[, `:=`(
    model = model_name,
    conf_low = estimate - 1.96 * std_error,
    conf_high = estimate + 1.96 * std_error
  )]
  ct
}

latex_escape <- function(x) {
  x <- as.character(x)
  x <- gsub("\\\\", "\\\\textbackslash{}", x)
  x <- gsub("([#$%&_{}])", "\\\\\\1", x, perl = TRUE)
  x <- gsub("~", "\\\\textasciitilde{}", x, fixed = TRUE)
  x <- gsub("\\^", "\\\\textasciicircum{}", x)
  x
}

write_latex_table <- function(dt, path, digits = 4L, caption = NULL) {
  x <- copy(as.data.table(dt))
  num_cols <- names(x)[vapply(x, is.numeric, logical(1L))]
  for (cc in num_cols) x[, (cc) := round(get(cc), digits)]

  if (requireNamespace("knitr", quietly = TRUE)) {
    tab <- knitr::kable(x, format = "latex", booktabs = TRUE, digits = digits, caption = caption)
    writeLines(as.character(tab), con = path)
    return(invisible(path))
  }

  hdr <- paste(latex_escape(names(x)), collapse = " & ")
  body <- apply(x, 1L, function(rr) paste(latex_escape(rr), collapse = " & "))
  lines <- c(
    "\\begin{tabular}{", paste(rep("l", ncol(x)), collapse = ""), "}",
    "\\hline",
    paste0(hdr, " \\\\"),
    "\\hline",
    paste0(body, " \\\\"),
    "\\hline",
    "\\end{tabular}"
  )
  writeLines(lines, con = path)
  invisible(path)
}

rate_cut_levels <- c("0", "(0-5%]", "(5-10%]", "(10-20%]", "20%+")

make_rate_cuts <- function(x) {
  out <- cut(
    to_num(x),
    breaks = c(-Inf, 0, 0.05, 0.1, 0.2, Inf),
    labels = rate_cut_levels,
    include.lowest = TRUE
  )
  as.character(out)
}

make_high_filing_flag <- function(x, threshold = 0.10) {
  x_num <- to_num(x)
  out <- fifelse(
    is.na(x_num), "missing",
    fifelse(x_num >= threshold, "high_filing", "low_or_mid_filing")
  )
  as.character(out)
}

make_summary_race <- function(dt, by_cols = character()) {
  out <- dt[, .(
    n_cases = .N,
    retaliatory_share = mean(retaliatory_status == "retaliatory", na.rm = TRUE),
    plausibly_retaliatory_share = mean(retaliatory_status == "plausibly_retaliatory", na.rm = TRUE),
    non_retaliatory_share = mean(retaliatory_status == "non_retaliatory", na.rm = TRUE),
    mean_case_p_black = mean(case_p_black, na.rm = TRUE),
    mean_case_p_white = mean(case_p_white, na.rm = TRUE)
  ), by = by_cols]
  if (length(by_cols)) setorderv(out, by_cols)
  out
}

make_summary_gender <- function(dt, by_cols = character()) {
  out <- dt[, .(
    n_cases = .N,
    retaliatory_share = mean(retaliatory_status == "retaliatory", na.rm = TRUE),
    plausibly_retaliatory_share = mean(retaliatory_status == "plausibly_retaliatory", na.rm = TRUE),
    non_retaliatory_share = mean(retaliatory_status == "non_retaliatory", na.rm = TRUE),
    mean_case_p_female = mean(case_p_female, na.rm = TRUE),
    mean_case_p_male = mean(case_p_male, na.rm = TRUE)
  ), by = by_cols]
  if (length(by_cols)) setorderv(out, by_cols)
  out
}

normalize_prob_vec <- function(v, names_vec = NULL) {
  vv <- as.numeric(v)
  vv[is.na(vv)] <- 0
  s <- sum(vv)
  if (!is.finite(s) || s <= 0) {
    out <- rep(NA_real_, length(vv))
  } else {
    out <- vv / s
  }
  if (!is.null(names_vec) && length(names_vec) == length(out)) names(out) <- names_vec
  out
}

build_race_gender_share_row <- function(source_label, race_probs, female_share, n_obs = NA_integer_, notes = "") {
  rp <- normalize_prob_vec(race_probs, names_vec = c("white", "black", "hispanic", "asian", "other"))
  fem <- as.numeric(female_share)
  if (!is.finite(fem)) fem <- NA_real_
  if (is.finite(fem)) fem <- max(min(fem, 1), 0)
  male <- if (is.finite(fem)) 1 - fem else NA_real_
  data.table(
    source = source_label,
    n_obs = as.integer(n_obs),
    female_share_used = fem,
    male_share_used = male,
    race_p_white = rp[["white"]],
    race_p_black = rp[["black"]],
    race_p_hispanic = rp[["hispanic"]],
    race_p_asian = rp[["asian"]],
    race_p_other = rp[["other"]],
    share_white_female = rp[["white"]] * fem,
    share_white_male = rp[["white"]] * male,
    share_black_female = rp[["black"]] * fem,
    share_black_male = rp[["black"]] * male,
    share_hispanic_female = rp[["hispanic"]] * fem,
    share_hispanic_male = rp[["hispanic"]] * male,
    share_asian_female = rp[["asian"]] * fem,
    share_asian_male = rp[["asian"]] * male,
    share_other_female = rp[["other"]] * fem,
    share_other_male = rp[["other"]] * male,
    notes = notes
  )
}

fit_cut_heterogeneity <- function(dt, cut_col, sample_name, outcome, status_col, min_rows = 300L) {
  lvls <- sort(unique(as.character(dt[[cut_col]])))
  lvls <- lvls[!is.na(lvls) & lvls != "" & lvls != "missing"]
  if (!length(lvls)) return(data.table())

  out <- vector("list", length(lvls))
  ii <- 0L
  for (lv in lvls) {
    sub <- dt[get(cut_col) == lv]
    if (nrow(sub) < min_rows) next
    if (sub[, uniqueN(get(status_col))] < 2L) next

    fml <- as.formula(
      paste0(
        outcome, " ~ i(", status_col, ", ref = 'non_retaliatory') | ",
        "tract_geoid + building_type + num_units_bin + year_quarter"
      )
    )
    m <- feols(fml, data = sub, cluster = ~PID)
    tag <- gsub("[^A-Za-z0-9]+", "_", lv)
    eff <- extract_effects(m, paste0("fe_", sample_name, "_", cut_col, "_", tag))
    eff[, `:=`(
      heterogeneity_dim = cut_col,
      heterogeneity_cut = lv,
      sample = sample_name,
      n_obs = nrow(sub),
      n_pid = uniqueN(sub$PID)
    )]
    ii <- ii + 1L
    out[[ii]] <- eff
  }
  if (ii == 0L) return(data.table())
  rbindlist(out[seq_len(ii)], use.names = TRUE, fill = TRUE)
}

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))
cfg <- read_config(opts$config %||% Sys.getenv("PHILLY_EVICTIONS_CONFIG", unset = "config.yml"))

start_year <- to_int(opts$start_year, 2007L)
end_year <- to_int(opts$end_year, 2024L)
pre_covid_end <- to_int(opts$pre_covid_end, 2019L)
high_filing_threshold <- to_num_scalar(opts$high_filing_threshold, 0.10)

race_case_path <- pick_race_case_path(cfg, opts$race_case %||% "")
gender_case_path <- pick_gender_case_path(cfg, opts$gender_case %||% "")
evictions_path <- opts$evictions_clean %||% p_product(cfg, "evictions_clean")
xwalk_path <- opts$evict_address_xwalk %||% p_product(cfg, "evict_address_xwalk")
quarter_panel_path <- pick_quarter_panel_path(cfg, opts$building_data_quarter %||% "")
blp_panel_path <- opts$bldg_panel_blp %||% p_product(cfg, "bldg_panel_blp")
infousa_race_person_path <- opts$infousa_race_imputed_person %||% p_product(cfg, "infousa_race_imputed_person")
tract_prior_path <- opts$tract_race_priors %||% p_proc(cfg, "xwalks", "tract_race_priors_2010.csv")
bg_prior_path <- opts$bg_race_priors %||% p_proc(cfg, "xwalks", "bg_race_priors_2013.csv")

out_dir <- opts$output_dir %||% p_out(cfg, "retaliatory")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
log_file <- p_out(cfg, "logs", "retaliatory-evictions-demographics.log")

logf("=== Starting retaliatory-evictions-demographics.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf("Race case input: ", race_case_path, log_file = log_file)
logf("Gender case input: ", gender_case_path, log_file = log_file)
logf("Evictions input: ", evictions_path, log_file = log_file)
logf("Xwalk input: ", xwalk_path, log_file = log_file)
logf("Quarter complaints input: ", quarter_panel_path, log_file = log_file)
logf("BLP panel input: ", blp_panel_path, log_file = log_file)
logf("InfoUSA race-imputed person input (optional): ", infousa_race_person_path, log_file = log_file)
logf("Tract race priors input (optional): ", tract_prior_path, log_file = log_file)
logf("BG race priors input (optional): ", bg_prior_path, log_file = log_file)
logf("Output dir: ", out_dir, log_file = log_file)
logf("High filing threshold (long-run pre-2019): ", high_filing_threshold, log_file = log_file)

for (pp in c(race_case_path, gender_case_path, evictions_path, xwalk_path, quarter_panel_path, blp_panel_path)) {
  if (!file.exists(pp)) stop("Missing required input: ", pp)
}

# --------------------------
# 1) Case-level demographics
# --------------------------
race_case <- fread(race_case_path)
setDT(race_case)
assert_has_cols(race_case, c("id", "case_impute_status", "case_p_black", "case_race_group_l80"), "race case file")
race_case <- race_case[case_impute_status == "ok" & !is.na(id)]
race_case[, case_p_black := as.numeric(case_p_black)]
if ("case_p_white" %in% names(race_case)) race_case[, case_p_white := as.numeric(case_p_white)]
if ("case_p_hispanic" %in% names(race_case)) race_case[, case_p_hispanic := as.numeric(case_p_hispanic)]
if ("case_p_asian" %in% names(race_case)) race_case[, case_p_asian := as.numeric(case_p_asian)]
if ("case_p_other" %in% names(race_case)) race_case[, case_p_other := as.numeric(case_p_other)]
race_case <- race_case[!is.na(case_p_black)]
race_case <- unique(race_case, by = "id")

gender_case <- fread(gender_case_path)
setDT(gender_case)
assert_has_cols(gender_case, c("id", "case_gender_impute_status", "case_p_female", "case_p_male"), "gender case file")
gender_case <- gender_case[case_gender_impute_status == "ok" & !is.na(id)]
gender_case[, case_p_female := as.numeric(case_p_female)]
gender_case[, case_p_male := as.numeric(case_p_male)]
gender_case <- gender_case[!is.na(case_p_female)]
gender_case <- unique(gender_case, by = "id")

# -------------------------------------
# 2) Case -> PID/year_quarter (shared)
# -------------------------------------
xw <- fread(xwalk_path, select = c("PID", "n_sn_ss_c", "num_parcels_matched"))
setDT(xw)
xw <- xw[num_parcels_matched == 1L]
xw[, PID := str_pad(as.character(PID), 9L, "left", "0")]
xw <- unique(xw[, .(PID, n_sn_ss_c)], by = c("PID", "n_sn_ss_c"))

ev <- fread(evictions_path, select = c("id", "d_filing", "n_sn_ss_c", "commercial"))
setDT(ev)
ev <- ev[!is.na(id) & nzchar(id)]
ev <- ev[is.na(commercial) | tolower(commercial) != "t"]

setkey(ev, n_sn_ss_c)
setkey(xw, n_sn_ss_c)
case_pid <- ev[xw, nomatch = 0L]
case_pid[, filing_date_chr := substr(as.character(d_filing), 1L, 10L)]
case_pid[, filing_date := as.IDate(filing_date_chr, format = "%Y-%m-%d")]
case_pid <- case_pid[!is.na(filing_date)]
case_pid[, year_quarter := quarter_from_date(filing_date)]
case_pid[, year := as.integer(substr(year_quarter, 1L, 4L))]
case_pid <- case_pid[year >= start_year & year <= end_year]
case_pid <- unique(case_pid[, .(id, PID, year, year_quarter)], by = c("id", "PID", "year_quarter"))

id_mult <- case_pid[, .(n_pid = uniqueN(PID), n_yq = uniqueN(year_quarter)), by = id]
amb_ids <- id_mult[n_pid > 1L | n_yq > 1L, id]
case_pid_clean <- case_pid[!(id %in% amb_ids)]
assert_unique(case_pid_clean, "id", "case->PID/year_quarter mapping")

# ----------------------
# 3) Complaints (shared)
# ----------------------
pid_keep <- unique(case_pid_clean[, .(PID)])
complaints <- read_panel_slice(
  quarter_panel_path,
  select_cols = c("parcel_number", "period", "total_complaints", "total_severe_complaints"),
  pid_filter = pid_keep$PID
)
assert_has_cols(complaints, c("parcel_number", "period", "total_complaints"), "quarter complaints panel")
setDT(complaints)
setnames(complaints, "parcel_number", "PID")
complaints[, PID := str_pad(as.character(PID), 9L, "left", "0")]
complaints[, year_quarter := str_replace_all(as.character(period), "-", "_")]
complaints[, year := as.integer(substr(year_quarter, 1L, 4L))]
complaints <- complaints[year >= start_year & year <= end_year]
complaints[, qtr := as.integer(str_extract(year_quarter, "(?<=Q)[1-4]$"))]
complaints <- complaints[!is.na(qtr)]
complaints[, filed_complaint := as.integer(to_num(total_complaints) > 0)]
if ("total_severe_complaints" %in% names(complaints)) {
  if (complaints[, any(to_num(total_severe_complaints) > to_num(total_complaints), na.rm = TRUE)]) {
    stop("Invalid quarter complaints panel: total_severe_complaints exceeds total_complaints.")
  }
  complaints[, filed_severe_complaint := as.integer(to_num(total_severe_complaints) > 0)]
} else {
  complaints[, `:=`(total_severe_complaints = NA_real_, filed_severe_complaint = NA_integer_)]
}

complaints[, t_index := year * 4L + qtr]
setorder(complaints, PID, t_index)
complaints[, lag_complaint_1 := shift(filed_complaint, n = 1L, type = "lag"), by = PID]
complaints[, lead_complaint_1 := shift(filed_complaint, n = 1L, type = "lead"), by = PID]
complaints[, lag_severe_complaint_1 := shift(filed_severe_complaint, n = 1L, type = "lag"), by = PID]
complaints[, lead_severe_complaint_1 := shift(filed_severe_complaint, n = 1L, type = "lead"), by = PID]
for (cc in c("lag_complaint_1", "lead_complaint_1", "lag_severe_complaint_1", "lead_severe_complaint_1")) {
  complaints[is.na(get(cc)), (cc) := 0L]
}

complaints[, retaliatory_status_any := fifelse(
  filed_complaint == 1L, "retaliatory",
  fifelse(lag_complaint_1 == 1L | lead_complaint_1 == 1L, "plausibly_retaliatory", "non_retaliatory")
)]
complaints[, retaliatory_status_race := fifelse(
  filed_severe_complaint == 1L, "retaliatory",
  fifelse(lag_complaint_1 == 1L | lead_complaint_1 == 1L, "plausibly_retaliatory", "non_retaliatory")
)]
complaints[, retaliatory_severe_status := fifelse(
  filed_severe_complaint == 1L, "retaliatory_severe",
  fifelse(lag_severe_complaint_1 == 1L | lead_severe_complaint_1 == 1L, "plausibly_retaliatory_severe", "non_retaliatory_severe")
)]

complaints <- complaints[, .(
  PID, year_quarter, retaliatory_status_any, retaliatory_status_race, retaliatory_severe_status,
  total_complaints, total_severe_complaints, filed_severe_complaint
)]
assert_unique(complaints, c("PID", "year_quarter"), "complaint status panel")

# --------------------
# 4) Controls (shared)
# --------------------
ctrl_hdr <- fread(blp_panel_path, nrows = 0)
ctrl_need <- c(
  "PID", "year", "GEOID", "building_type", "structure_bin", "num_units_bin",
  "filing_rate_longrun", "filing_rate_longrun_pre2019",
  "filing_rate_eb_pre_covid", "filing_rate_eb_city_pre_covid", "filing_rate_eb_zip_pre_covid",
  "infousa_pct_black", "infousa_pct_female", "infousa_pct_black_female", "infousa_share_persons_demog_ok"
)
controls <- fread(blp_panel_path, select = intersect(ctrl_need, names(ctrl_hdr)))
setDT(controls)
assert_has_cols(controls, c("PID", "year", "GEOID"), "bldg_panel_blp controls")
if (!("building_type" %in% names(controls))) controls[, building_type := NA_character_]
if (!("structure_bin" %in% names(controls))) controls[, structure_bin := NA_character_]
if (!("num_units_bin" %in% names(controls))) controls[, num_units_bin := NA_character_]
if (!("filing_rate_longrun" %in% names(controls))) controls[, filing_rate_longrun := NA_real_]
if (!("filing_rate_longrun_pre2019" %in% names(controls))) controls[, filing_rate_longrun_pre2019 := NA_real_]
if (!("filing_rate_eb_pre_covid" %in% names(controls))) controls[, filing_rate_eb_pre_covid := NA_real_]
if (!("filing_rate_eb_city_pre_covid" %in% names(controls))) controls[, filing_rate_eb_city_pre_covid := NA_real_]
if (!("filing_rate_eb_zip_pre_covid" %in% names(controls))) controls[, filing_rate_eb_zip_pre_covid := NA_real_]
if (!("infousa_pct_black" %in% names(controls))) controls[, infousa_pct_black := NA_real_]
if (!("infousa_pct_female" %in% names(controls))) controls[, infousa_pct_female := NA_real_]
if (!("infousa_pct_black_female" %in% names(controls))) controls[, infousa_pct_black_female := NA_real_]
if (!("infousa_share_persons_demog_ok" %in% names(controls))) controls[, infousa_share_persons_demog_ok := NA_real_]

controls[, PID := str_pad(as.character(PID), 9L, "left", "0")]
controls[, year := as.integer(year)]
controls[, tract_geoid := normalize_tract11(GEOID)]
controls[, building_type_clean := fifelse(
  !is.na(building_type) & nzchar(as.character(building_type)), as.character(building_type),
  fifelse(!is.na(structure_bin) & nzchar(as.character(structure_bin)), as.character(structure_bin), "unknown")
)]
controls[, num_units_bin_clean := fifelse(
  !is.na(num_units_bin) & nzchar(as.character(num_units_bin)), as.character(num_units_bin), "missing"
)]
for (cc in c("filing_rate_longrun", "filing_rate_longrun_pre2019", "filing_rate_eb_pre_covid",
             "filing_rate_eb_city_pre_covid", "filing_rate_eb_zip_pre_covid",
             "infousa_pct_black", "infousa_pct_female", "infousa_pct_black_female", "infousa_share_persons_demog_ok")) {
  controls[, (cc) := to_num(get(cc))]
}
controls[, filing_rate_longrun_pre2019_cuts := make_rate_cuts(filing_rate_longrun_pre2019)]
controls[, filing_rate_eb_pre_covid_cuts := make_rate_cuts(filing_rate_eb_pre_covid)]
controls[, high_filing_longrun := make_high_filing_flag(filing_rate_longrun_pre2019, threshold = high_filing_threshold)]
controls[is.na(filing_rate_longrun_pre2019_cuts), filing_rate_longrun_pre2019_cuts := "missing"]
controls[is.na(filing_rate_eb_pre_covid_cuts), filing_rate_eb_pre_covid_cuts := "missing"]
controls[is.na(high_filing_longrun) | !nzchar(high_filing_longrun), high_filing_longrun := "missing"]

controls <- unique(
  controls[, .(
    PID, year, tract_geoid,
    building_type = building_type_clean,
    num_units_bin = num_units_bin_clean,
    filing_rate_longrun, filing_rate_longrun_pre2019,
    filing_rate_eb_pre_covid, filing_rate_eb_city_pre_covid, filing_rate_eb_zip_pre_covid,
    filing_rate_longrun_pre2019_cuts, filing_rate_eb_pre_covid_cuts, high_filing_longrun,
    infousa_pct_black, infousa_pct_female, infousa_pct_black_female, infousa_share_persons_demog_ok
  )],
  by = c("PID", "year")
)
assert_unique(controls, c("PID", "year"), "controls panel (PID-year)")

# --------------------------
# 5) Shared case-base panel
# --------------------------
case_base <- merge(case_pid_clean, complaints, by = c("PID", "year_quarter"), all.x = TRUE)
case_base <- merge(case_base, controls, by = c("PID", "year"), all.x = TRUE)
case_base <- case_base[!is.na(tract_geoid) & !is.na(building_type) & nzchar(building_type)]
case_base[, num_units_bin := fifelse(is.na(num_units_bin) | !nzchar(as.character(num_units_bin)), "missing", as.character(num_units_bin))]
case_base[, filing_rate_longrun_pre2019_cuts := fifelse(
  is.na(filing_rate_longrun_pre2019_cuts) | !nzchar(as.character(filing_rate_longrun_pre2019_cuts)),
  "missing",
  as.character(filing_rate_longrun_pre2019_cuts)
)]
case_base[, filing_rate_eb_pre_covid_cuts := fifelse(
  is.na(filing_rate_eb_pre_covid_cuts) | !nzchar(as.character(filing_rate_eb_pre_covid_cuts)),
  "missing",
  as.character(filing_rate_eb_pre_covid_cuts)
)]
case_base[, high_filing_longrun := fifelse(
  is.na(high_filing_longrun) | !nzchar(as.character(high_filing_longrun)),
  "missing",
  as.character(high_filing_longrun)
)]
for (cc in c("infousa_pct_black", "infousa_pct_female", "infousa_pct_black_female", "infousa_share_persons_demog_ok")) {
  case_base[, (cc) := to_num(get(cc))]
}
case_base[, tenant_comp_missing := as.integer(
  is.na(infousa_pct_black) |
    is.na(infousa_pct_female) |
    is.na(infousa_pct_black_female) |
    is.na(infousa_share_persons_demog_ok)
)]
center_or_zero <- function(x) {
  mu <- mean(x, na.rm = TRUE)
  if (!is.finite(mu)) mu <- 0
  as.numeric(fifelse(is.na(x), mu, x) - mu)
}
case_base[, infousa_pct_black_c := center_or_zero(infousa_pct_black)]
case_base[, infousa_pct_female_c := center_or_zero(infousa_pct_female)]
case_base[, infousa_pct_black_female_c := center_or_zero(infousa_pct_black_female)]
case_base[, infousa_share_persons_demog_ok_c := center_or_zero(infousa_share_persons_demog_ok)]
assert_unique(case_base, "id", "shared case base")

# ===========================================
# 6) Race analysis block (compatible outputs)
# ===========================================
panel_race <- merge(race_case, case_base, by = "id", all = FALSE)
panel_race <- panel_race[!is.na(retaliatory_status_race)]
panel_race[, retaliatory_status := retaliatory_status_race]
panel_race[, year_quarter := as.factor(year_quarter)]
panel_race[, tract_geoid := as.factor(tract_geoid)]
panel_race[, building_type := as.factor(building_type)]
panel_race[, num_units_bin := as.factor(num_units_bin)]
panel_race[, filing_rate_longrun_pre2019_cuts := factor(filing_rate_longrun_pre2019_cuts, levels = c(rate_cut_levels, "missing"))]
panel_race[, filing_rate_eb_pre_covid_cuts := factor(filing_rate_eb_pre_covid_cuts, levels = c(rate_cut_levels, "missing"))]
panel_race[, high_filing_longrun := factor(high_filing_longrun, levels = c("low_or_mid_filing", "high_filing", "missing"))]
assert_unique(panel_race, "id", "retaliatory-race case panel")

panel_race_pre <- panel_race[year <= pre_covid_end]
if (!nrow(panel_race)) stop("No rows in race panel.")
if (!nrow(panel_race_pre)) stop("No rows in pre-covid race panel.")

status_means_race <- panel_race[, .(
  n_cases = .N,
  mean_case_p_black = mean(case_p_black, na.rm = TRUE),
  mean_case_p_white = mean(case_p_white, na.rm = TRUE)
), by = retaliatory_status][order(retaliatory_status)]

status_means_race_severe <- panel_race[, .(
  n_cases = .N,
  mean_case_p_black = mean(case_p_black, na.rm = TRUE),
  mean_case_p_white = mean(case_p_white, na.rm = TRUE)
), by = retaliatory_severe_status][order(retaliatory_severe_status)]

status_means_race_building <- panel_race[, .(
  n_cases = .N,
  mean_case_p_black = mean(case_p_black, na.rm = TRUE),
  mean_case_p_white = mean(case_p_white, na.rm = TRUE)
), by = .(retaliatory_status, building_type)][order(retaliatory_status, building_type)]

desc_race_overall <- make_summary_race(panel_race)
desc_race_building <- make_summary_race(panel_race, by_cols = "building_type")
desc_race_units <- make_summary_race(panel_race, by_cols = "num_units_bin")
desc_race_longrun <- make_summary_race(panel_race, by_cols = "filing_rate_longrun_pre2019_cuts")
desc_race_eb <- make_summary_race(panel_race, by_cols = "filing_rate_eb_pre_covid_cuts")
desc_race_building_longrun <- make_summary_race(panel_race, by_cols = c("building_type", "filing_rate_longrun_pre2019_cuts"))
desc_race_high_filing <- make_summary_race(panel_race, by_cols = "high_filing_longrun")
desc_race_building_high_filing <- make_summary_race(panel_race, by_cols = c("building_type", "high_filing_longrun"))
desc_race_building_units_high_filing <- make_summary_race(panel_race, by_cols = c("building_type", "num_units_bin", "high_filing_longrun"))

m_race_raw_full <- feols(case_p_black ~ retaliatory_status, data = panel_race[year >= 2015], cluster = ~PID)
m_race_fe_full <- feols(
  case_p_black ~ retaliatory_severe_status | tract_geoid + building_type + num_units_bin + year_quarter,
  data = panel_race,
  cluster = ~PID
)
m_race_raw_pre <- feols(case_p_black ~ i(retaliatory_status, ref = "non_retaliatory"), data = panel_race_pre, cluster = ~PID)
m_race_fe_pre <- feols(
  case_p_black ~ retaliatory_severe_status + n_defendants_total | tract_geoid + building_type + num_units_bin + year_quarter,
  data = panel_race_pre,
  cluster = ~PID
)
m_race_fe_full_tenant <- feols(
  case_p_black ~ retaliatory_severe_status +
    infousa_pct_black_c + infousa_pct_female_c + infousa_pct_black_female_c + infousa_share_persons_demog_ok_c +
    tenant_comp_missing |
    tract_geoid + building_type + num_units_bin + year_quarter,
  data = panel_race,
  cluster = ~PID
)
m_race_fe_pre_tenant <- feols(
  case_p_black ~ retaliatory_severe_status + n_defendants_total +
    infousa_pct_black_c + infousa_pct_female_c + infousa_pct_black_female_c + infousa_share_persons_demog_ok_c +
    tenant_comp_missing |
    tract_geoid + building_type + num_units_bin + year_quarter,
  data = panel_race_pre,
  cluster = ~PID
)
panel_race_gap <- panel_race[is.finite(case_p_black) & is.finite(infousa_pct_black)]
panel_race_gap[, gap_case_black_vs_tenant := case_p_black - infousa_pct_black]
m_race_within_bldg_gap <- feols(
  gap_case_black_vs_tenant ~ i(retaliatory_status, ref = "non_retaliatory") | PID + year_quarter,
  data = panel_race_gap,
  cluster = ~PID
)
within_bldg_race_compare <- panel_race_gap[, .(
  n_cases = .N,
  mean_case_p_black = mean(case_p_black, na.rm = TRUE),
  mean_tenant_pct_black = mean(infousa_pct_black, na.rm = TRUE),
  mean_gap_case_minus_tenant = mean(gap_case_black_vs_tenant, na.rm = TRUE),
  mean_abs_gap_case_minus_tenant = mean(abs(gap_case_black_vs_tenant), na.rm = TRUE)
), by = retaliatory_status][order(retaliatory_status)]

panel_race_cls <- panel_race[case_race_group_l80 %in% c("Black", "White", "Hispanic", "Asian", "Other/Mixed")]
panel_race_cls[, black_case := as.integer(case_race_group_l80 == "Black")]
panel_race_cls_pre <- panel_race_cls[year <= pre_covid_end]

m_race_bin_full <- feglm(
  black_case ~ i(retaliatory_status, ref = "non_retaliatory") | tract_geoid + building_type + num_units_bin + year_quarter,
  family = "binomial",
  data = panel_race_cls,
  cluster = ~PID
)
m_race_bin_pre <- feglm(
  black_case ~ i(retaliatory_status, ref = "non_retaliatory") | tract_geoid + building_type + num_units_bin + year_quarter,
  family = "binomial",
  data = panel_race_cls_pre,
  cluster = ~PID
)

hetero_race <- rbindlist(
  list(
    fit_cut_heterogeneity(panel_race, "filing_rate_longrun_pre2019_cuts", "full", "case_p_black", "retaliatory_status"),
    fit_cut_heterogeneity(panel_race_pre, "filing_rate_longrun_pre2019_cuts", "pre", "case_p_black", "retaliatory_status"),
    fit_cut_heterogeneity(panel_race, "filing_rate_eb_pre_covid_cuts", "full", "case_p_black", "retaliatory_status"),
    fit_cut_heterogeneity(panel_race_pre, "filing_rate_eb_pre_covid_cuts", "pre", "case_p_black", "retaliatory_status")
  ),
  use.names = TRUE,
  fill = TRUE
)
if (!nrow(hetero_race)) {
  hetero_race <- data.table(
    model = character(), term = character(), estimate = numeric(), std_error = numeric(), p_value = numeric(),
    conf_low = numeric(), conf_high = numeric(), heterogeneity_dim = character(), heterogeneity_cut = character(),
    sample = character(), n_obs = integer(), n_pid = integer()
  )
}

effects_race <- rbindlist(list(
  extract_effects(m_race_raw_full, "raw_full_case_p_black"),
  extract_effects(m_race_fe_full, "fe_full_case_p_black"),
  extract_effects(m_race_raw_pre, "raw_pre_case_p_black"),
  extract_effects(m_race_fe_pre, "fe_pre_case_p_black"),
  extract_effects(m_race_fe_full_tenant, "fe_full_case_p_black_tenant_aug"),
  extract_effects(m_race_fe_pre_tenant, "fe_pre_case_p_black_tenant_aug"),
  extract_effects(m_race_within_bldg_gap, "within_bldg_gap_case_p_black"),
  extract_effects(m_race_bin_full, "fe_full_black_case_logit"),
  extract_effects(m_race_bin_pre, "fe_pre_black_case_logit")
), use.names = TRUE, fill = TRUE)
effects_race <- effects_race[!is.na(term) & nzchar(term)]

etable_race_txt <- capture.output(
  etable(
    list(
      raw_full = m_race_raw_full,
      fe_full = m_race_fe_full,
      raw_pre = m_race_raw_pre,
      fe_pre = m_race_fe_pre,
      fe_full_tenant = m_race_fe_full_tenant,
      fe_pre_tenant = m_race_fe_pre_tenant,
      within_bldg_gap = m_race_within_bldg_gap,
      logit_fe_full = m_race_bin_full,
      logit_fe_pre = m_race_bin_pre
    ),
    se.below = TRUE,
    digits = 4
  )
)
etable_race_tex <- capture.output(
  etable(
    list(
      raw_full = m_race_raw_full,
      fe_full = m_race_fe_full,
      raw_pre = m_race_raw_pre,
      fe_pre = m_race_fe_pre,
      fe_full_tenant = m_race_fe_full_tenant,
      fe_pre_tenant = m_race_fe_pre_tenant,
      within_bldg_gap = m_race_within_bldg_gap,
      logit_fe_full = m_race_bin_full,
      logit_fe_pre = m_race_bin_pre
    ),
    se.below = TRUE,
    digits = 4,
    tex = TRUE
  )
)

panel_race_out <- panel_race[, .(
  id, PID, year, year_quarter,
  retaliatory_status, retaliatory_status_any, retaliatory_severe_status,
  case_p_black, case_p_white, case_p_hispanic, case_p_asian, case_p_other,
  case_race_group_l80, n_defendants_total, n_defendants_imputed,
  tract_geoid, building_type, num_units_bin,
  filing_rate_longrun, filing_rate_longrun_pre2019,
  filing_rate_eb_pre_covid, filing_rate_eb_city_pre_covid, filing_rate_eb_zip_pre_covid,
  filing_rate_longrun_pre2019_cuts, filing_rate_eb_pre_covid_cuts, high_filing_longrun,
  infousa_pct_black, infousa_pct_female, infousa_pct_black_female, infousa_share_persons_demog_ok,
  tenant_comp_missing
)]

fwrite(panel_race_out, file.path(out_dir, "retaliatory_race_case_panel.csv"))
fwrite(status_means_race, file.path(out_dir, "retaliatory_race_status_means.csv"))
fwrite(status_means_race_severe, file.path(out_dir, "retaliatory_race_status_means_severe.csv"))
fwrite(status_means_race_building, file.path(out_dir, "retaliatory_race_status_means_by_building_type.csv"))
fwrite(desc_race_overall, file.path(out_dir, "retaliatory_race_descriptives_overall.csv"))
fwrite(desc_race_building, file.path(out_dir, "retaliatory_race_descriptives_by_building_type.csv"))
fwrite(desc_race_units, file.path(out_dir, "retaliatory_race_descriptives_by_num_units_bin.csv"))
fwrite(desc_race_longrun, file.path(out_dir, "retaliatory_race_descriptives_by_longrun_rate_cut.csv"))
fwrite(desc_race_eb, file.path(out_dir, "retaliatory_race_descriptives_by_eb_rate_cut.csv"))
fwrite(desc_race_building_longrun, file.path(out_dir, "retaliatory_race_descriptives_by_building_longrun_cut.csv"))
fwrite(desc_race_high_filing, file.path(out_dir, "retaliatory_race_descriptives_by_high_filing.csv"))
fwrite(desc_race_building_high_filing, file.path(out_dir, "retaliatory_race_descriptives_by_building_high_filing.csv"))
fwrite(desc_race_building_units_high_filing, file.path(out_dir, "retaliatory_race_descriptives_by_building_units_high_filing.csv"))
fwrite(within_bldg_race_compare, file.path(out_dir, "retaliatory_race_within_building_case_vs_tenant.csv"))
fwrite(effects_race, file.path(out_dir, "retaliatory_race_effects.csv"))
fwrite(hetero_race, file.path(out_dir, "retaliatory_race_heterogeneity_rate_cuts.csv"))
writeLines(etable_race_txt, con = file.path(out_dir, "retaliatory_race_models.txt"))
writeLines(etable_race_tex, con = file.path(out_dir, "retaliatory_race_models.tex"))
write_latex_table(status_means_race, file.path(out_dir, "retaliatory_race_status_means.tex"), digits = 4L, caption = "Mean race probabilities by retaliatory status (full sample)")
write_latex_table(effects_race[, .(model, term, estimate, std_error, p_value, conf_low, conf_high)], file.path(out_dir, "retaliatory_race_effects.tex"), digits = 4L, caption = "Retaliatory status coefficient table (race outcomes)")
write_latex_table(desc_race_overall, file.path(out_dir, "retaliatory_race_descriptives_overall.tex"), digits = 4L, caption = "Retaliatory-status descriptive shares (overall)")
write_latex_table(desc_race_longrun, file.path(out_dir, "retaliatory_race_descriptives_by_longrun_rate_cut.tex"), digits = 4L, caption = "Retaliatory-status shares by long-run filing-rate cut (pre-2019 raw)")
write_latex_table(desc_race_building_high_filing, file.path(out_dir, "retaliatory_race_descriptives_by_building_high_filing.tex"), digits = 4L, caption = "Retaliatory-status shares by building type x high-filing bin (race sample)")
write_latex_table(within_bldg_race_compare, file.path(out_dir, "retaliatory_race_within_building_case_vs_tenant.tex"), digits = 4L,
                  caption = "Within-building comparison: case Black probability vs tenant Black share")
write_latex_table(hetero_race[, .(sample, heterogeneity_dim, heterogeneity_cut, term, estimate, std_error, p_value, conf_low, conf_high, n_obs, n_pid)],
                  file.path(out_dir, "retaliatory_race_heterogeneity_rate_cuts.tex"), digits = 4L,
                  caption = "Heterogeneity in retaliatory effects by long-run filing-rate cuts")

# =============================================
# 7) Gender analysis block (compatible outputs)
# =============================================
panel_gender <- merge(gender_case, case_base, by = "id", all = FALSE)
panel_gender <- panel_gender[!is.na(retaliatory_status_any)]
panel_gender[, retaliatory_status := retaliatory_status_any]
panel_gender[, year_quarter := as.factor(year_quarter)]
panel_gender[, tract_geoid := as.factor(tract_geoid)]
panel_gender[, building_type := as.factor(building_type)]
panel_gender[, num_units_bin := as.factor(num_units_bin)]
panel_gender[, filing_rate_longrun_pre2019_cuts := factor(filing_rate_longrun_pre2019_cuts, levels = c(rate_cut_levels, "missing"))]
panel_gender[, filing_rate_eb_pre_covid_cuts := factor(filing_rate_eb_pre_covid_cuts, levels = c(rate_cut_levels, "missing"))]
panel_gender[, high_filing_longrun := factor(high_filing_longrun, levels = c("low_or_mid_filing", "high_filing", "missing"))]
assert_unique(panel_gender, "id", "retaliatory-gender case panel")

panel_gender_pre <- panel_gender[year <= pre_covid_end]
if (!nrow(panel_gender)) stop("No rows in gender panel.")
if (!nrow(panel_gender_pre)) stop("No rows in pre-covid gender panel.")

status_means_gender <- panel_gender[, .(
  n_cases = .N,
  mean_case_p_female = mean(case_p_female, na.rm = TRUE),
  mean_case_p_male = mean(case_p_male, na.rm = TRUE)
), by = retaliatory_status][order(retaliatory_status)]

desc_gender_overall <- make_summary_gender(panel_gender)
desc_gender_building <- make_summary_gender(panel_gender, by_cols = "building_type")
desc_gender_units <- make_summary_gender(panel_gender, by_cols = "num_units_bin")
desc_gender_longrun <- make_summary_gender(panel_gender, by_cols = "filing_rate_longrun_pre2019_cuts")
desc_gender_eb <- make_summary_gender(panel_gender, by_cols = "filing_rate_eb_pre_covid_cuts")
desc_gender_building_longrun <- make_summary_gender(panel_gender, by_cols = c("building_type", "filing_rate_longrun_pre2019_cuts"))
desc_gender_high_filing <- make_summary_gender(panel_gender, by_cols = "high_filing_longrun")
desc_gender_building_high_filing <- make_summary_gender(panel_gender, by_cols = c("building_type", "high_filing_longrun"))
desc_gender_building_units_high_filing <- make_summary_gender(panel_gender, by_cols = c("building_type", "num_units_bin", "high_filing_longrun"))

m_gender_raw_full <- feols(case_p_female ~ i(retaliatory_status, ref = "non_retaliatory"), data = panel_gender, cluster = ~PID)
m_gender_fe_full <- feols(
  case_p_female ~ i(retaliatory_status, ref = "non_retaliatory") | tract_geoid + building_type + num_units_bin + year_quarter,
  data = panel_gender,
  cluster = ~PID
)
m_gender_raw_pre <- feols(case_p_female ~ i(retaliatory_status, ref = "non_retaliatory"), data = panel_gender_pre, cluster = ~PID)
m_gender_fe_pre <- feols(
  case_p_female ~ i(retaliatory_status, ref = "non_retaliatory") | tract_geoid + building_type + num_units_bin + year_quarter,
  data = panel_gender_pre,
  cluster = ~PID
)
m_gender_fe_full_tenant <- feols(
  case_p_female ~ i(retaliatory_status, ref = "non_retaliatory") +
    infousa_pct_black_c + infousa_pct_female_c + infousa_pct_black_female_c + infousa_share_persons_demog_ok_c +
    tenant_comp_missing |
    tract_geoid + building_type + num_units_bin + year_quarter,
  data = panel_gender,
  cluster = ~PID
)
m_gender_fe_pre_tenant <- feols(
  case_p_female ~ i(retaliatory_status, ref = "non_retaliatory") +
    infousa_pct_black_c + infousa_pct_female_c + infousa_pct_black_female_c + infousa_share_persons_demog_ok_c +
    tenant_comp_missing |
    tract_geoid + building_type + num_units_bin + year_quarter,
  data = panel_gender_pre,
  cluster = ~PID
)
panel_gender_gap <- panel_gender[is.finite(case_p_female) & is.finite(infousa_pct_female)]
panel_gender_gap[, gap_case_female_vs_tenant := case_p_female - infousa_pct_female]
m_gender_within_bldg_gap <- feols(
  gap_case_female_vs_tenant ~ i(retaliatory_status, ref = "non_retaliatory") | PID + year_quarter,
  data = panel_gender_gap,
  cluster = ~PID
)
within_bldg_gender_compare <- panel_gender_gap[, .(
  n_cases = .N,
  mean_case_p_female = mean(case_p_female, na.rm = TRUE),
  mean_tenant_pct_female = mean(infousa_pct_female, na.rm = TRUE),
  mean_gap_case_minus_tenant = mean(gap_case_female_vs_tenant, na.rm = TRUE),
  mean_abs_gap_case_minus_tenant = mean(abs(gap_case_female_vs_tenant), na.rm = TRUE)
), by = retaliatory_status][order(retaliatory_status)]

panel_gender_bin <- copy(panel_gender)
panel_gender_bin[, female_case := as.integer(case_p_female >= 0.5)]
panel_gender_bin_pre <- panel_gender_bin[year <= pre_covid_end]

m_gender_bin_full <- feglm(
  female_case ~ i(retaliatory_status, ref = "non_retaliatory") | tract_geoid + building_type + num_units_bin + year_quarter,
  family = "binomial",
  data = panel_gender_bin,
  cluster = ~PID
)
m_gender_bin_pre <- feglm(
  female_case ~ i(retaliatory_status, ref = "non_retaliatory") | tract_geoid + building_type + num_units_bin + year_quarter,
  family = "binomial",
  data = panel_gender_bin_pre,
  cluster = ~PID
)

hetero_gender <- rbindlist(
  list(
    fit_cut_heterogeneity(panel_gender, "filing_rate_longrun_pre2019_cuts", "full", "case_p_female", "retaliatory_status"),
    fit_cut_heterogeneity(panel_gender_pre, "filing_rate_longrun_pre2019_cuts", "pre", "case_p_female", "retaliatory_status"),
    fit_cut_heterogeneity(panel_gender, "filing_rate_eb_pre_covid_cuts", "full", "case_p_female", "retaliatory_status"),
    fit_cut_heterogeneity(panel_gender_pre, "filing_rate_eb_pre_covid_cuts", "pre", "case_p_female", "retaliatory_status")
  ),
  use.names = TRUE,
  fill = TRUE
)
if (!nrow(hetero_gender)) {
  hetero_gender <- data.table(
    model = character(), term = character(), estimate = numeric(), std_error = numeric(), p_value = numeric(),
    conf_low = numeric(), conf_high = numeric(), heterogeneity_dim = character(), heterogeneity_cut = character(),
    sample = character(), n_obs = integer(), n_pid = integer()
  )
}

effects_gender <- rbindlist(list(
  extract_effects(m_gender_raw_full, "raw_full_case_p_female"),
  extract_effects(m_gender_fe_full, "fe_full_case_p_female"),
  extract_effects(m_gender_raw_pre, "raw_pre_case_p_female"),
  extract_effects(m_gender_fe_pre, "fe_pre_case_p_female"),
  extract_effects(m_gender_fe_full_tenant, "fe_full_case_p_female_tenant_aug"),
  extract_effects(m_gender_fe_pre_tenant, "fe_pre_case_p_female_tenant_aug"),
  extract_effects(m_gender_within_bldg_gap, "within_bldg_gap_case_p_female"),
  extract_effects(m_gender_bin_full, "fe_full_female_case_logit"),
  extract_effects(m_gender_bin_pre, "fe_pre_female_case_logit")
), use.names = TRUE, fill = TRUE)
effects_gender <- effects_gender[!is.na(term) & nzchar(term)]

etable_gender_txt <- capture.output(
  etable(
    list(
      raw_full = m_gender_raw_full,
      fe_full = m_gender_fe_full,
      raw_pre = m_gender_raw_pre,
      fe_pre = m_gender_fe_pre,
      fe_full_tenant = m_gender_fe_full_tenant,
      fe_pre_tenant = m_gender_fe_pre_tenant,
      within_bldg_gap = m_gender_within_bldg_gap,
      logit_fe_full = m_gender_bin_full,
      logit_fe_pre = m_gender_bin_pre
    ),
    se.below = TRUE,
    digits = 4
  )
)
etable_gender_tex <- capture.output(
  etable(
    list(
      raw_full = m_gender_raw_full,
      fe_full = m_gender_fe_full,
      raw_pre = m_gender_raw_pre,
      fe_pre = m_gender_fe_pre,
      fe_full_tenant = m_gender_fe_full_tenant,
      fe_pre_tenant = m_gender_fe_pre_tenant,
      within_bldg_gap = m_gender_within_bldg_gap,
      logit_fe_full = m_gender_bin_full,
      logit_fe_pre = m_gender_bin_pre
    ),
    se.below = TRUE,
    digits = 4,
    tex = TRUE
  )
)

panel_gender_out <- panel_gender[, .(
  id, PID, year, year_quarter, retaliatory_status,
  case_p_female, case_p_male, n_defendants_total, n_defendants_gender_imputed,
  tract_geoid, building_type, num_units_bin,
  filing_rate_longrun, filing_rate_longrun_pre2019,
  filing_rate_eb_pre_covid, filing_rate_eb_city_pre_covid, filing_rate_eb_zip_pre_covid,
  filing_rate_longrun_pre2019_cuts, filing_rate_eb_pre_covid_cuts, high_filing_longrun,
  infousa_pct_black, infousa_pct_female, infousa_pct_black_female, infousa_share_persons_demog_ok,
  tenant_comp_missing
)]

fwrite(panel_gender_out, file.path(out_dir, "retaliatory_gender_case_panel.csv"))
fwrite(status_means_gender, file.path(out_dir, "retaliatory_gender_status_means.csv"))
fwrite(desc_gender_overall, file.path(out_dir, "retaliatory_gender_descriptives_overall.csv"))
fwrite(desc_gender_building, file.path(out_dir, "retaliatory_gender_descriptives_by_building_type.csv"))
fwrite(desc_gender_units, file.path(out_dir, "retaliatory_gender_descriptives_by_num_units_bin.csv"))
fwrite(desc_gender_longrun, file.path(out_dir, "retaliatory_gender_descriptives_by_longrun_rate_cut.csv"))
fwrite(desc_gender_eb, file.path(out_dir, "retaliatory_gender_descriptives_by_eb_rate_cut.csv"))
fwrite(desc_gender_building_longrun, file.path(out_dir, "retaliatory_gender_descriptives_by_building_longrun_cut.csv"))
fwrite(desc_gender_high_filing, file.path(out_dir, "retaliatory_gender_descriptives_by_high_filing.csv"))
fwrite(desc_gender_building_high_filing, file.path(out_dir, "retaliatory_gender_descriptives_by_building_high_filing.csv"))
fwrite(desc_gender_building_units_high_filing, file.path(out_dir, "retaliatory_gender_descriptives_by_building_units_high_filing.csv"))
fwrite(within_bldg_gender_compare, file.path(out_dir, "retaliatory_gender_within_building_case_vs_tenant.csv"))
fwrite(effects_gender, file.path(out_dir, "retaliatory_gender_effects.csv"))
fwrite(hetero_gender, file.path(out_dir, "retaliatory_gender_heterogeneity_rate_cuts.csv"))
writeLines(etable_gender_txt, con = file.path(out_dir, "retaliatory_gender_models.txt"))
writeLines(etable_gender_tex, con = file.path(out_dir, "retaliatory_gender_models.tex"))
write_latex_table(status_means_gender, file.path(out_dir, "retaliatory_gender_status_means.tex"), digits = 4L, caption = "Mean gender probabilities by retaliatory status (full sample)")
write_latex_table(effects_gender[, .(model, term, estimate, std_error, p_value, conf_low, conf_high)], file.path(out_dir, "retaliatory_gender_effects.tex"), digits = 4L, caption = "Retaliatory status coefficient table (gender outcomes)")
write_latex_table(desc_gender_overall, file.path(out_dir, "retaliatory_gender_descriptives_overall.tex"), digits = 4L, caption = "Retaliatory-status descriptive shares (overall, gender sample)")
write_latex_table(desc_gender_longrun, file.path(out_dir, "retaliatory_gender_descriptives_by_longrun_rate_cut.tex"), digits = 4L, caption = "Retaliatory-status shares by long-run filing-rate cut (gender sample)")
write_latex_table(desc_gender_building_high_filing, file.path(out_dir, "retaliatory_gender_descriptives_by_building_high_filing.tex"), digits = 4L, caption = "Retaliatory-status shares by building type x high-filing bin (gender sample)")
write_latex_table(within_bldg_gender_compare, file.path(out_dir, "retaliatory_gender_within_building_case_vs_tenant.tex"), digits = 4L,
                  caption = "Within-building comparison: case Female probability vs tenant Female share")
write_latex_table(hetero_gender[, .(sample, heterogeneity_dim, heterogeneity_cut, term, estimate, std_error, p_value, conf_low, conf_high, n_obs, n_pid)],
                  file.path(out_dir, "retaliatory_gender_heterogeneity_rate_cuts.tex"), digits = 4L,
                  caption = "Heterogeneity in retaliatory effects by long-run filing-rate cuts (gender)")

# ===========================================
# 8) Intersectionality (first-pass analysis)
# ===========================================
race_intersection_keep <- intersect(
  c("id", "case_p_black", "case_p_white", "case_p_hispanic", "case_p_asian", "case_p_other", "case_race_group_l80"),
  names(race_case)
)
intersection_panel <- merge(
  case_base[, .(
    id, PID, year, year_quarter,
    retaliatory_status = retaliatory_status_any,
    tract_geoid, building_type, num_units_bin,
    filing_rate_longrun_pre2019_cuts, filing_rate_eb_pre_covid_cuts, high_filing_longrun,
    infousa_pct_black, infousa_pct_female, infousa_pct_black_female, infousa_share_persons_demog_ok,
    infousa_pct_black_c, infousa_pct_female_c, infousa_pct_black_female_c, infousa_share_persons_demog_ok_c,
    tenant_comp_missing
  )],
  race_case[, ..race_intersection_keep],
  by = "id",
  all = FALSE
)
intersection_panel <- merge(
  intersection_panel,
  gender_case[, .(id, case_p_female, case_p_male)],
  by = "id",
  all = FALSE
)
intersection_panel <- intersection_panel[!is.na(retaliatory_status)]
for (cc in c("case_p_white", "case_p_black", "case_p_hispanic", "case_p_asian", "case_p_other")) {
  if (!(cc %in% names(intersection_panel))) intersection_panel[, (cc) := NA_real_]
}
intersection_panel[, `:=`(
  black_case = as.integer(case_p_black >= 0.5),
  female_case = as.integer(case_p_female >= 0.5)
)]
intersection_panel[, black_female_case := as.integer(black_case == 1L & female_case == 1L)]
intersection_panel[, intersection_group := fifelse(
  black_case == 1L & female_case == 1L, "Black+Female",
  fifelse(black_case == 1L & female_case == 0L, "Black+Male",
          fifelse(black_case == 0L & female_case == 1L, "NonBlack+Female", "NonBlack+Male"))
)]
intersection_panel[, race_group_bw := fifelse(
  case_race_group_l80 %in% c("Black", "White"),
  as.character(case_race_group_l80),
  "Other/Unknown"
)]
intersection_panel[, gender_group := fifelse(female_case == 1L, "Female", "Male")]
intersection_panel[, intersection_group_bw := paste0(race_group_bw, "+", gender_group)]
intersection_panel[, `:=`(
  year_quarter = as.factor(year_quarter),
  tract_geoid = as.factor(tract_geoid),
  building_type = as.factor(building_type),
  num_units_bin = as.factor(num_units_bin),
  high_filing_longrun = as.factor(high_filing_longrun)
)]
assert_unique(intersection_panel, "id", "intersection panel")

intersection_desc <- intersection_panel[, .(
  n_cases = .N,
  overall_share = .N / nrow(intersection_panel),
  mean_case_p_black = mean(case_p_black, na.rm = TRUE),
  mean_case_p_female = mean(case_p_female, na.rm = TRUE),
  black_female_share = mean(black_female_case, na.rm = TRUE)
), by = .(retaliatory_status, intersection_group)][order(retaliatory_status, intersection_group)]
intersection_desc[, within_status_share := n_cases / sum(n_cases), by = retaliatory_status]

intersection_bw <- intersection_panel[race_group_bw %in% c("Black", "White")]
if (!nrow(intersection_bw)) stop("No rows in Black/White intersection panel.")
intersection_bw[, intersection_group_bw := factor(
  intersection_group_bw,
  levels = c("White+Male", "White+Female", "Black+Male", "Black+Female")
)]
intersection_bw[, `:=`(
  white_female_case = as.integer(intersection_group_bw == "White+Female"),
  black_male_case = as.integer(intersection_group_bw == "Black+Male")
)]

intersection_desc_bw <- intersection_bw[, .(
  n_cases = .N,
  overall_share = .N / nrow(intersection_bw),
  mean_case_p_black = mean(case_p_black, na.rm = TRUE),
  mean_case_p_white = mean(case_p_white, na.rm = TRUE),
  mean_case_p_female = mean(case_p_female, na.rm = TRUE)
), by = .(retaliatory_status, intersection_group_bw)][order(retaliatory_status, intersection_group_bw)]
intersection_desc_bw[, within_status_share := n_cases / sum(n_cases), by = retaliatory_status]

intersection_desc_bw_building_high_filing <- intersection_bw[, .(
  n_cases = .N
), by = .(retaliatory_status, building_type, num_units_bin, high_filing_longrun, intersection_group_bw)]
intersection_desc_bw_building_high_filing[, within_bin_share := n_cases / sum(n_cases),
                                          by = .(retaliatory_status, building_type, num_units_bin, high_filing_longrun)]
setorderv(intersection_desc_bw_building_high_filing,
          c("retaliatory_status", "building_type", "num_units_bin", "high_filing_longrun", "intersection_group_bw"))

intersection_status_summary <- intersection_panel[, .(
  n_cases = .N,
  retaliatory_share = mean(retaliatory_status == "retaliatory", na.rm = TRUE),
  plausibly_retaliatory_share = mean(retaliatory_status == "plausibly_retaliatory", na.rm = TRUE),
  non_retaliatory_share = mean(retaliatory_status == "non_retaliatory", na.rm = TRUE),
  mean_black_case = mean(black_case, na.rm = TRUE),
  mean_female_case = mean(female_case, na.rm = TRUE),
  mean_black_female_case = mean(black_female_case, na.rm = TRUE)
)]

m_intersection <- feglm(
  black_female_case ~ i(retaliatory_status, ref = "non_retaliatory") |
    tract_geoid + building_type + num_units_bin + year_quarter,
  family = "binomial",
  data = intersection_panel,
  cluster = ~PID
)
m_intersection_tenant <- feglm(
  black_female_case ~ i(retaliatory_status, ref = "non_retaliatory") +
    infousa_pct_black_c + infousa_pct_female_c + infousa_pct_black_female_c + infousa_share_persons_demog_ok_c +
    tenant_comp_missing |
    tract_geoid + building_type + num_units_bin + year_quarter,
  family = "binomial",
  data = intersection_panel,
  cluster = ~PID
)
m_intersection_white_female <- feols(
  white_female_case ~ i(retaliatory_status, ref = "non_retaliatory") |
    tract_geoid + building_type + num_units_bin + year_quarter,
  data = intersection_bw,
  cluster = ~PID
)
m_intersection_black_male <- feols(
  black_male_case ~ i(retaliatory_status, ref = "non_retaliatory") |
    tract_geoid + building_type + num_units_bin + year_quarter,
  data = intersection_bw,
  cluster = ~PID
)
m_intersection_black_female_bw <- feols(
  black_female_case ~ i(retaliatory_status, ref = "non_retaliatory") |
    tract_geoid + building_type + num_units_bin + year_quarter,
  data = intersection_bw,
  cluster = ~PID
)
intersection_bw_gap <- intersection_bw[is.finite(infousa_pct_black_female)]
intersection_bw_gap[, gap_black_female_case_vs_tenant := black_female_case - infousa_pct_black_female]
m_intersection_within_bldg_gap <- feols(
  gap_black_female_case_vs_tenant ~ i(retaliatory_status, ref = "non_retaliatory") | PID + year_quarter,
  data = intersection_bw_gap,
  cluster = ~PID
)
within_bldg_intersection_compare <- intersection_bw_gap[, .(
  n_cases = .N,
  mean_black_female_case = mean(black_female_case, na.rm = TRUE),
  mean_tenant_black_female_share = mean(infousa_pct_black_female, na.rm = TRUE),
  mean_gap_case_minus_tenant = mean(gap_black_female_case_vs_tenant, na.rm = TRUE),
  mean_abs_gap_case_minus_tenant = mean(abs(gap_black_female_case_vs_tenant), na.rm = TRUE)
), by = retaliatory_status][order(retaliatory_status)]
effects_intersection <- rbindlist(
  list(
    extract_effects(m_intersection, "fe_black_female_case_logit"),
    extract_effects(m_intersection_tenant, "fe_black_female_case_logit_tenant_aug"),
    extract_effects(m_intersection_within_bldg_gap, "within_bldg_gap_black_female_case")
  ),
  use.names = TRUE,
  fill = TRUE
)
effects_intersection <- effects_intersection[!is.na(term) & nzchar(term)]
effects_intersection_bw <- rbindlist(list(
  extract_effects(m_intersection_white_female, "fe_white_female_case_lpm"),
  extract_effects(m_intersection_black_male, "fe_black_male_case_lpm"),
  extract_effects(m_intersection_black_female_bw, "fe_black_female_case_lpm_bw")
), use.names = TRUE, fill = TRUE)
effects_intersection_bw <- effects_intersection_bw[!is.na(term) & nzchar(term)]
etable_intersection_txt <- capture.output(
  etable(
    list(
      intersection = m_intersection,
      intersection_tenant = m_intersection_tenant,
      within_bldg_gap = m_intersection_within_bldg_gap
    ),
    se.below = TRUE,
    digits = 4
  )
)
etable_intersection_tex <- capture.output(
  etable(
    list(
      intersection = m_intersection,
      intersection_tenant = m_intersection_tenant,
      within_bldg_gap = m_intersection_within_bldg_gap
    ),
    se.below = TRUE,
    digits = 4,
    tex = TRUE
  )
)
etable_intersection_bw_txt <- capture.output(
  etable(
    list(
      white_female = m_intersection_white_female,
      black_male = m_intersection_black_male,
      black_female = m_intersection_black_female_bw
    ),
    se.below = TRUE,
    digits = 4
  )
)
etable_intersection_bw_tex <- capture.output(
  etable(
    list(
      white_female = m_intersection_white_female,
      black_male = m_intersection_black_male,
      black_female = m_intersection_black_female_bw
    ),
    se.below = TRUE,
    digits = 4,
    tex = TRUE
  )
)

fwrite(intersection_panel, file.path(out_dir, "retaliatory_intersection_case_panel.csv"))
fwrite(intersection_desc, file.path(out_dir, "retaliatory_intersection_descriptives.csv"))
fwrite(intersection_desc_bw, file.path(out_dir, "retaliatory_intersection_descriptives_bw.csv"))
fwrite(intersection_desc_bw_building_high_filing, file.path(out_dir, "retaliatory_intersection_descriptives_bw_building_units_high_filing.csv"))
fwrite(intersection_status_summary, file.path(out_dir, "retaliatory_intersection_status_summary.csv"))
fwrite(within_bldg_intersection_compare, file.path(out_dir, "retaliatory_intersection_within_building_case_vs_tenant.csv"))
fwrite(effects_intersection, file.path(out_dir, "retaliatory_intersection_effects.csv"))
fwrite(effects_intersection_bw, file.path(out_dir, "retaliatory_intersection_effects_bw_multigroup.csv"))
writeLines(etable_intersection_txt, con = file.path(out_dir, "retaliatory_intersection_models.txt"))
writeLines(etable_intersection_tex, con = file.path(out_dir, "retaliatory_intersection_models.tex"))
writeLines(etable_intersection_bw_txt, con = file.path(out_dir, "retaliatory_intersection_models_bw_multigroup.txt"))
writeLines(etable_intersection_bw_tex, con = file.path(out_dir, "retaliatory_intersection_models_bw_multigroup.tex"))
write_latex_table(intersection_desc, file.path(out_dir, "retaliatory_intersection_descriptives.tex"), digits = 4L,
                  caption = "Intersectionality descriptives by retaliatory status (black vs non-black)")
write_latex_table(intersection_desc_bw, file.path(out_dir, "retaliatory_intersection_descriptives_bw.tex"), digits = 4L,
                  caption = "Intersectionality descriptives by retaliatory status (Black/White x Female/Male)")
write_latex_table(intersection_desc_bw_building_high_filing,
                  file.path(out_dir, "retaliatory_intersection_descriptives_bw_building_units_high_filing.tex"),
                  digits = 4L,
                  caption = "Black/White x Female/Male composition within retaliatory status x building type x units x high-filing bin")
write_latex_table(within_bldg_intersection_compare, file.path(out_dir, "retaliatory_intersection_within_building_case_vs_tenant.tex"), digits = 4L,
                  caption = "Within-building comparison: filed Black+Female share vs tenant Black+Female share")
write_latex_table(effects_intersection[, .(model, term, estimate, std_error, p_value, conf_low, conf_high)],
                  file.path(out_dir, "retaliatory_intersection_effects.tex"), digits = 4L,
                  caption = "Retaliatory status coefficients for Black+Female indicator")
write_latex_table(effects_intersection_bw[, .(model, term, estimate, std_error, p_value, conf_low, conf_high)],
                  file.path(out_dir, "retaliatory_intersection_effects_bw_multigroup.tex"), digits = 4L,
                  caption = "Retaliatory status coefficients for White+Female, Black+Male, and Black+Female indicators")

# =====================================================
# 9) General descriptives: unconditional group shares
#    + priors/InfoUSA comparison
# =====================================================
intersection_obs <- copy(intersection_panel)
intersection_obs[, race_group_5 := fifelse(
  case_race_group_l80 %in% c("Black", "White", "Hispanic", "Asian", "Other/Mixed"),
  as.character(case_race_group_l80),
  "Unknown"
)]
intersection_obs[, gender_group_obs := fifelse(female_case == 1L, "Female", "Male")]
intersection_obs[, group_race_gender := paste0(race_group_5, "+", gender_group_obs)]

uncond_group_observed <- intersection_obs[, .(n_cases = .N), by = .(race_group_5, gender_group_obs, group_race_gender)]
uncond_group_observed[, share_cases := n_cases / sum(n_cases)]
setorderv(uncond_group_observed, c("race_group_5", "gender_group_obs"))

evict_female_share_prob <- intersection_obs[, mean(case_p_female, na.rm = TRUE)]
evict_race_probs <- c(
  white = intersection_obs[, mean(case_p_white, na.rm = TRUE)],
  black = intersection_obs[, mean(case_p_black, na.rm = TRUE)],
  hispanic = intersection_obs[, mean(case_p_hispanic, na.rm = TRUE)],
  asian = intersection_obs[, mean(case_p_asian, na.rm = TRUE)],
  other = intersection_obs[, mean(case_p_other, na.rm = TRUE)]
)
bench_rows <- list(
  build_race_gender_share_row(
    source_label = "eviction_cases_observed_probabilities",
    race_probs = evict_race_probs,
    female_share = evict_female_share_prob,
    n_obs = nrow(intersection_obs),
    notes = "Case-level WRU race probabilities and case-level gender probabilities."
  )
)

if (file.exists(tract_prior_path)) {
  tract_priors <- fread(tract_prior_path)
  setDT(tract_priors)
  if (all(c("tract_geoid", "p_white_prior", "p_black_prior", "p_hispanic_prior", "p_asian_prior", "p_other_prior") %in% names(tract_priors))) {
    tract_join <- merge(
      intersection_obs[, .(id, tract_geoid = as.character(tract_geoid))],
      tract_priors[, .(
        tract_geoid = as.character(tract_geoid),
        p_white_prior = as.numeric(p_white_prior),
        p_black_prior = as.numeric(p_black_prior),
        p_hispanic_prior = as.numeric(p_hispanic_prior),
        p_asian_prior = as.numeric(p_asian_prior),
        p_other_prior = as.numeric(p_other_prior)
      )],
      by = "tract_geoid",
      all.x = TRUE
    )
    tract_race_probs <- c(
      white = tract_join[, mean(p_white_prior, na.rm = TRUE)],
      black = tract_join[, mean(p_black_prior, na.rm = TRUE)],
      hispanic = tract_join[, mean(p_hispanic_prior, na.rm = TRUE)],
      asian = tract_join[, mean(p_asian_prior, na.rm = TRUE)],
      other = tract_join[, mean(p_other_prior, na.rm = TRUE)]
    )
    bench_rows[[length(bench_rows) + 1L]] <- build_race_gender_share_row(
      source_label = "tract_prior_case_weighted_x_evictions",
      race_probs = tract_race_probs,
      female_share = evict_female_share_prob,
      n_obs = tract_join[!is.na(p_black_prior), .N],
      notes = "Tract priors joined to eviction cases; female share uses eviction case mean (placeholder)."
    )
  }
}

if (file.exists(bg_prior_path)) {
  bg_priors <- fread(bg_prior_path)
  setDT(bg_priors)
  if (all(c("p_white_prior", "p_black_prior", "p_hispanic_prior", "p_asian_prior", "p_other_prior") %in% names(bg_priors))) {
    if ("total_pop" %in% names(bg_priors)) {
      w <- as.numeric(bg_priors$total_pop)
      w[!is.finite(w) | w < 0] <- 0
      bg_race_probs <- c(
        white = weighted.mean(as.numeric(bg_priors$p_white_prior), w, na.rm = TRUE),
        black = weighted.mean(as.numeric(bg_priors$p_black_prior), w, na.rm = TRUE),
        hispanic = weighted.mean(as.numeric(bg_priors$p_hispanic_prior), w, na.rm = TRUE),
        asian = weighted.mean(as.numeric(bg_priors$p_asian_prior), w, na.rm = TRUE),
        other = weighted.mean(as.numeric(bg_priors$p_other_prior), w, na.rm = TRUE)
      )
      bg_n <- bg_priors[is.finite(total_pop) & total_pop > 0, .N]
      bg_note <- "BG priors population-weighted city average; female share uses eviction case mean (placeholder)."
    } else {
      bg_race_probs <- c(
        white = bg_priors[, mean(as.numeric(p_white_prior), na.rm = TRUE)],
        black = bg_priors[, mean(as.numeric(p_black_prior), na.rm = TRUE)],
        hispanic = bg_priors[, mean(as.numeric(p_hispanic_prior), na.rm = TRUE)],
        asian = bg_priors[, mean(as.numeric(p_asian_prior), na.rm = TRUE)],
        other = bg_priors[, mean(as.numeric(p_other_prior), na.rm = TRUE)]
      )
      bg_n <- nrow(bg_priors)
      bg_note <- "BG priors unweighted city average; female share uses eviction case mean (placeholder)."
    }
    bench_rows[[length(bench_rows) + 1L]] <- build_race_gender_share_row(
      source_label = "bg_prior_city_average_placeholder",
      race_probs = bg_race_probs,
      female_share = evict_female_share_prob,
      n_obs = bg_n,
      notes = bg_note
    )
  }
}

if (file.exists(infousa_race_person_path)) {
  infousa <- fread(
    infousa_race_person_path,
    select = c("p_white", "p_black", "p_hispanic", "p_asian", "p_other", "gender", "race_impute_status", "in_eviction_data")
  )
  setDT(infousa)
  infousa <- infousa[race_impute_status == "ok"]
  if (nrow(infousa)) {
    infousa[, gender := toupper(trimws(as.character(gender)))]
    infousa[, in_eviction_data_flag := {
      vv <- tolower(trimws(as.character(in_eviction_data)))
      vv %chin% c("true", "t", "1", "yes", "y")
    }]
    infousa[, gender_known := gender %chin% c("F", "M")]

    add_infousa_row <- function(dt_sub, label) {
      if (!nrow(dt_sub)) return(NULL)
      female_share_sub <- dt_sub[gender_known == TRUE, mean(gender == "F", na.rm = TRUE)]
      gender_cov <- dt_sub[, mean(gender_known, na.rm = TRUE)]
      race_probs_sub <- c(
        white = dt_sub[, mean(as.numeric(p_white), na.rm = TRUE)],
        black = dt_sub[, mean(as.numeric(p_black), na.rm = TRUE)],
        hispanic = dt_sub[, mean(as.numeric(p_hispanic), na.rm = TRUE)],
        asian = dt_sub[, mean(as.numeric(p_asian), na.rm = TRUE)],
        other = dt_sub[, mean(as.numeric(p_other), na.rm = TRUE)]
      )
      build_race_gender_share_row(
        source_label = label,
        race_probs = race_probs_sub,
        female_share = female_share_sub,
        n_obs = nrow(dt_sub),
        notes = paste0("InfoUSA race-imputed person file; female share from known F/M only (coverage=", round(gender_cov, 4), ").")
      )
    }

    row_inf_all <- add_infousa_row(infousa, "infousa_persons_all_ok")
    if (!is.null(row_inf_all)) bench_rows[[length(bench_rows) + 1L]] <- row_inf_all
    row_inf_evict <- add_infousa_row(infousa[in_eviction_data_flag == TRUE], "infousa_persons_in_eviction_data_ok")
    if (!is.null(row_inf_evict)) bench_rows[[length(bench_rows) + 1L]] <- row_inf_evict
  }
}

general_group_benchmarks <- rbindlist(bench_rows, use.names = TRUE, fill = TRUE)
baseline_row <- general_group_benchmarks[source == "eviction_cases_observed_probabilities"]
if (nrow(baseline_row) == 1L) {
  delta_cols <- setdiff(names(general_group_benchmarks), c("source", "n_obs", "notes"))
  general_group_benchmark_deltas <- copy(general_group_benchmarks)
  for (cc in delta_cols) {
    if (is.numeric(general_group_benchmark_deltas[[cc]])) {
      general_group_benchmark_deltas[, (cc) := get(cc) - baseline_row[[cc]][1L]]
    }
  }
} else {
  general_group_benchmark_deltas <- copy(general_group_benchmarks)
}

fwrite(uncond_group_observed, file.path(out_dir, "retaliatory_general_unconditional_group_shares_observed.csv"))
fwrite(general_group_benchmarks, file.path(out_dir, "retaliatory_general_unconditional_group_benchmarks.csv"))
fwrite(general_group_benchmark_deltas, file.path(out_dir, "retaliatory_general_unconditional_group_benchmark_deltas_vs_evictions.csv"))
write_latex_table(uncond_group_observed, file.path(out_dir, "retaliatory_general_unconditional_group_shares_observed.tex"), digits = 4L,
                  caption = "Unconditional eviction shares by race x gender group (observed classification)")
write_latex_table(general_group_benchmarks, file.path(out_dir, "retaliatory_general_unconditional_group_benchmarks.tex"), digits = 4L,
                  caption = "Unconditional race x gender shares: eviction observed vs tract/BG priors vs InfoUSA")
write_latex_table(general_group_benchmark_deltas, file.path(out_dir, "retaliatory_general_unconditional_group_benchmark_deltas_vs_evictions.tex"), digits = 4L,
                  caption = "Difference vs eviction-observed unconditional race x gender shares")

qa_lines <- c(
  "Retaliatory Demographics QA",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  paste0("Race case input: ", race_case_path),
  paste0("Gender case input: ", gender_case_path),
  paste0("Evictions input: ", evictions_path),
  paste0("Xwalk input: ", xwalk_path),
  paste0("Quarter panel input: ", quarter_panel_path),
  paste0("BLP panel input: ", blp_panel_path),
  paste0("High filing threshold (long-run pre-2019): ", high_filing_threshold),
  "",
  paste0("Race panel rows: ", nrow(panel_race)),
  paste0("Gender panel rows: ", nrow(panel_gender)),
  paste0("Intersection panel rows: ", nrow(intersection_panel)),
  "",
  "Race descriptives by building type x high-filing bin:",
  capture.output(print(desc_race_building_high_filing)),
  "",
  "Gender descriptives by building type x high-filing bin:",
  capture.output(print(desc_gender_building_high_filing)),
  "",
  "Intersection descriptives (Black/White x Female/Male):",
  capture.output(print(intersection_desc_bw)),
  "",
  "Within-building race case-vs-tenant comparison:",
  capture.output(print(within_bldg_race_compare)),
  "",
  "Within-building gender case-vs-tenant comparison:",
  capture.output(print(within_bldg_gender_compare)),
  "",
  "Within-building intersection case-vs-tenant comparison:",
  capture.output(print(within_bldg_intersection_compare)),
  "",
  "General unconditional shares by race x gender (observed):",
  capture.output(print(uncond_group_observed)),
  "",
  "General benchmark comparison (evictions vs priors vs InfoUSA):",
  capture.output(print(general_group_benchmarks)),
  "",
  "Race descriptives by long-run filing-rate cut:",
  capture.output(print(desc_race_longrun)),
  "",
  "Gender descriptives by long-run filing-rate cut:",
  capture.output(print(desc_gender_longrun)),
  "",
  "Intersection status summary:",
  capture.output(print(intersection_status_summary))
)
writeLines(qa_lines, con = file.path(out_dir, "retaliatory_demographics_qa.txt"))

logf("Wrote outputs to: ", out_dir, log_file = log_file)
logf("=== Finished retaliatory-evictions-demographics.R ===", log_file = log_file)
