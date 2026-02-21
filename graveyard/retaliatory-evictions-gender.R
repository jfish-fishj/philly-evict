## ============================================================
## retaliatory-evictions-gender.R
## ============================================================
## Purpose: Test whether (plausibly) retaliatory eviction filings are
## more likely to be against female tenants, conditional on tract,
## building type, and year-quarter fixed effects.
##
## This script intentionally does NOT run distributed-lag or legacy LPM
## event-study models.
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
    parts <- strsplit(arg, "=", fixed = TRUE)[[1]]
    key <- gsub("-", "_", parts[1])
    val <- if (length(parts) >= 2L) paste(parts[-1], collapse = "=") else "TRUE"
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

to_num <- function(x) suppressWarnings(as.numeric(x))

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

make_retaliatory_summary <- function(dt, by_cols = character()) {
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

fit_cut_heterogeneity <- function(dt, cut_col, sample_name, min_rows = 300L) {
  lvls <- sort(unique(as.character(dt[[cut_col]])))
  lvls <- lvls[!is.na(lvls) & lvls != "" & lvls != "missing"]
  if (!length(lvls)) return(data.table())

  out <- vector("list", length(lvls))
  ii <- 0L
  for (lv in lvls) {
    sub <- dt[get(cut_col) == lv]
    if (nrow(sub) < min_rows) next
    if (sub[, uniqueN(retaliatory_status)] < 2L) next

    m <- feols(
      case_p_female ~ i(retaliatory_status, ref = "non_retaliatory") |
        tract_geoid + building_type + num_units_bin + year_quarter,
      data = sub,
      cluster = ~PID
    )
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

gender_case_path <- pick_gender_case_path(cfg, opts$gender_case %||% "")
evictions_path <- opts$evictions_clean %||% p_product(cfg, "evictions_clean")
xwalk_path <- opts$evict_address_xwalk %||% p_product(cfg, "evict_address_xwalk")
quarter_panel_path <- pick_quarter_panel_path(cfg, opts$building_data_quarter %||% "")
analytic_path <- opts$analytic_sample %||% p_product(cfg, "analytic_sample")
blp_panel_path <- opts$bldg_panel_blp %||% p_product(cfg, "bldg_panel_blp")

out_dir <- opts$output_dir %||% p_out(cfg, "retaliatory")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
log_file <- p_out(cfg, "logs", "retaliatory-evictions-gender.log")

logf("=== Starting retaliatory-evictions-gender.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)
logf("Gender case input: ", gender_case_path, log_file = log_file)
logf("Evictions input: ", evictions_path, log_file = log_file)
logf("Xwalk input: ", xwalk_path, log_file = log_file)
logf("Quarter complaints input: ", quarter_panel_path, log_file = log_file)
logf("Analytic controls input: ", analytic_path, log_file = log_file)
logf("BLP panel input: ", blp_panel_path, log_file = log_file)
logf("Output dir: ", out_dir, log_file = log_file)

for (pp in c(gender_case_path, evictions_path, xwalk_path, quarter_panel_path, analytic_path, blp_panel_path)) {
  if (!file.exists(pp)) stop("Missing required input: ", pp)
}

# 1) Gender-imputed case outcomes
gender_case <- fread(gender_case_path)
setDT(gender_case)
assert_has_cols(gender_case, c("id", "case_gender_impute_status", "case_p_female", "case_p_male"), "gender case file")
gender_case <- gender_case[case_gender_impute_status == "ok" & !is.na(id)]
gender_case[, case_p_female := as.numeric(case_p_female)]
gender_case[, case_p_male := as.numeric(case_p_male)]
gender_case <- gender_case[!is.na(case_p_female)]
gender_case <- unique(gender_case, by = "id")

# 2) Case -> PID/year_quarter mapping from evictions + one-to-one xwalk
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

# Drop ambiguous case IDs
id_mult <- case_pid[, .(n_pid = uniqueN(PID), n_yq = uniqueN(year_quarter)), by = id]
amb_ids <- id_mult[n_pid > 1L | n_yq > 1L, id]
case_pid_clean <- case_pid[!(id %in% amb_ids)]
assert_unique(case_pid_clean, "id", "case->PID/year_quarter mapping")

# 3) Retaliatory status at PID-year_quarter from complaints panel
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
complaints[, filed_complaint := as.integer(as.numeric(total_complaints) > 0)]
if ("total_severe_complaints" %in% names(complaints)) {
  if (complaints[, any(as.numeric(total_severe_complaints) > as.numeric(total_complaints), na.rm = TRUE)]) {
    stop("Invalid quarter complaints panel: total_severe_complaints exceeds total_complaints.")
  }
  complaints[, filed_severe_complaint := as.integer(as.numeric(total_severe_complaints) > 0)]
} else {
  complaints[, `:=`(total_severe_complaints = NA_real_, filed_severe_complaint = NA_integer_)]
}
complaints[, t_index := year * 4L + qtr]
setorder(complaints, PID, t_index)
complaints[, lag_complaint_1 := shift(filed_complaint, n = 1L, type = "lag"), by = PID]
complaints[, lead_complaint_1 := shift(filed_complaint, n = 1L, type = "lead"), by = PID]
complaints[is.na(lag_complaint_1), lag_complaint_1 := 0L]
complaints[is.na(lead_complaint_1), lead_complaint_1 := 0L]
complaints[, retaliatory_status := fifelse(
  filed_complaint == 1L, "retaliatory",
  fifelse(lag_complaint_1 == 1L | lead_complaint_1 == 1L, "plausibly_retaliatory", "non_retaliatory")
)]
complaints <- complaints[, .(PID, year_quarter, retaliatory_status, total_complaints, total_severe_complaints, filed_severe_complaint)]
assert_unique(complaints, c("PID", "year_quarter"), "complaint status panel")

# 4) Controls + long-run filing rates from bldg_panel_blp (PID-year)
ctrl_hdr <- fread(blp_panel_path, nrows = 0)
ctrl_need <- c(
  "PID", "year", "GEOID", "building_type", "structure_bin", "num_units_bin",
  "filing_rate_longrun", "filing_rate_longrun_pre2019",
  "filing_rate_eb_pre_covid", "filing_rate_eb_city_pre_covid", "filing_rate_eb_zip_pre_covid"
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
             "filing_rate_eb_city_pre_covid", "filing_rate_eb_zip_pre_covid")) {
  controls[, (cc) := to_num(get(cc))]
}
controls[, filing_rate_longrun_pre2019_cuts := make_rate_cuts(filing_rate_longrun_pre2019)]
controls[, filing_rate_eb_pre_covid_cuts := make_rate_cuts(filing_rate_eb_pre_covid)]
controls[is.na(filing_rate_longrun_pre2019_cuts), filing_rate_longrun_pre2019_cuts := "missing"]
controls[is.na(filing_rate_eb_pre_covid_cuts), filing_rate_eb_pre_covid_cuts := "missing"]

controls <- unique(
  controls[, .(
    PID, year, tract_geoid,
    building_type = building_type_clean,
    num_units_bin = num_units_bin_clean,
    filing_rate_longrun, filing_rate_longrun_pre2019,
    filing_rate_eb_pre_covid, filing_rate_eb_city_pre_covid, filing_rate_eb_zip_pre_covid,
    filing_rate_longrun_pre2019_cuts, filing_rate_eb_pre_covid_cuts
  )],
  by = c("PID", "year")
)
assert_unique(controls, c("PID", "year"), "controls panel (PID-year)")

# 5) Final case-level panel
panel <- merge(gender_case, case_pid_clean, by = "id", all = FALSE)
panel <- merge(panel, complaints, by = c("PID", "year_quarter"), all.x = TRUE)
panel <- merge(panel, controls, by = c("PID", "year"), all.x = TRUE)
panel <- panel[!is.na(retaliatory_status)]
panel <- panel[!is.na(tract_geoid) & !is.na(building_type) & nzchar(building_type)]
panel[, num_units_bin := fifelse(is.na(num_units_bin) | !nzchar(as.character(num_units_bin)), "missing", as.character(num_units_bin))]
panel[, filing_rate_longrun_pre2019_cuts := fifelse(
  is.na(filing_rate_longrun_pre2019_cuts) | !nzchar(as.character(filing_rate_longrun_pre2019_cuts)),
  "missing",
  as.character(filing_rate_longrun_pre2019_cuts)
)]
panel[, filing_rate_eb_pre_covid_cuts := fifelse(
  is.na(filing_rate_eb_pre_covid_cuts) | !nzchar(as.character(filing_rate_eb_pre_covid_cuts)),
  "missing",
  as.character(filing_rate_eb_pre_covid_cuts)
)]
panel[, year_quarter := as.factor(year_quarter)]
panel[, tract_geoid := as.factor(tract_geoid)]
panel[, building_type := as.factor(building_type)]
panel[, num_units_bin := as.factor(num_units_bin)]
panel[, filing_rate_longrun_pre2019_cuts := factor(filing_rate_longrun_pre2019_cuts, levels = c(rate_cut_levels, "missing"))]
panel[, filing_rate_eb_pre_covid_cuts := factor(filing_rate_eb_pre_covid_cuts, levels = c(rate_cut_levels, "missing"))]
assert_unique(panel, "id", "retaliatory-gender case panel")

panel_pre <- panel[year <= pre_covid_end]
if (!nrow(panel)) stop("No rows in full analysis panel.")
if (!nrow(panel_pre)) stop("No rows in pre-covid analysis panel.")

status_means_full <- panel[, .(
  n_cases = .N,
  mean_case_p_female = mean(case_p_female, na.rm = TRUE),
  mean_case_p_male = mean(case_p_male, na.rm = TRUE)
), by = retaliatory_status][order(retaliatory_status)]

desc_overall <- make_retaliatory_summary(panel)
desc_by_building_type <- make_retaliatory_summary(panel, by_cols = "building_type")
desc_by_num_units_bin <- make_retaliatory_summary(panel, by_cols = "num_units_bin")
desc_by_longrun_cut <- make_retaliatory_summary(panel, by_cols = "filing_rate_longrun_pre2019_cuts")
desc_by_eb_cut <- make_retaliatory_summary(panel, by_cols = "filing_rate_eb_pre_covid_cuts")
desc_by_building_rate_cut <- make_retaliatory_summary(
  panel,
  by_cols = c("building_type", "filing_rate_longrun_pre2019_cuts")
)

# Main regressions
m_raw_full <- feols(case_p_female ~ i(retaliatory_status, ref = "non_retaliatory"), data = panel, cluster = ~PID)
m_fe_full <- feols(
  case_p_female ~ i(retaliatory_status, ref = "non_retaliatory") | tract_geoid + building_type + num_units_bin + year_quarter,
  data = panel,
  cluster = ~PID
)
m_raw_pre <- feols(case_p_female ~ i(retaliatory_status, ref = "non_retaliatory"), data = panel_pre, cluster = ~PID)
m_fe_pre <- feols(
  case_p_female ~ i(retaliatory_status, ref = "non_retaliatory") | tract_geoid + building_type + num_units_bin + year_quarter,
  data = panel_pre,
  cluster = ~PID
)

panel_bin <- copy(panel)
panel_bin[, female_case := as.integer(case_p_female >= 0.5)]
panel_bin_pre <- panel_bin[year <= pre_covid_end]

m_bin_fe_full <- feglm(
  female_case ~ i(retaliatory_status, ref = "non_retaliatory") | tract_geoid + building_type + num_units_bin + year_quarter,
  family = "binomial",
  data = panel_bin,
  cluster = ~PID
)
m_bin_fe_pre <- feglm(
  female_case ~ i(retaliatory_status, ref = "non_retaliatory") | tract_geoid + building_type + num_units_bin + year_quarter,
  family = "binomial",
  data = panel_bin_pre,
  cluster = ~PID
)

hetero_effects <- rbindlist(
  list(
    fit_cut_heterogeneity(panel, "filing_rate_longrun_pre2019_cuts", "full"),
    fit_cut_heterogeneity(panel_pre, "filing_rate_longrun_pre2019_cuts", "pre"),
    fit_cut_heterogeneity(panel, "filing_rate_eb_pre_covid_cuts", "full"),
    fit_cut_heterogeneity(panel_pre, "filing_rate_eb_pre_covid_cuts", "pre")
  ),
  use.names = TRUE,
  fill = TRUE
)
if (!nrow(hetero_effects)) {
  hetero_effects <- data.table(
    model = character(),
    term = character(),
    estimate = numeric(),
    std_error = numeric(),
    p_value = numeric(),
    conf_low = numeric(),
    conf_high = numeric(),
    heterogeneity_dim = character(),
    heterogeneity_cut = character(),
    sample = character(),
    n_obs = integer(),
    n_pid = integer()
  )
}

effects <- rbindlist(list(
  extract_effects(m_raw_full, "raw_full_case_p_female"),
  extract_effects(m_fe_full, "fe_full_case_p_female"),
  extract_effects(m_raw_pre, "raw_pre_case_p_female"),
  extract_effects(m_fe_pre, "fe_pre_case_p_female"),
  extract_effects(m_bin_fe_full, "fe_full_female_case_logit"),
  extract_effects(m_bin_fe_pre, "fe_pre_female_case_logit")
), use.names = TRUE, fill = TRUE)
effects <- effects[!is.na(term) & nzchar(term)]

etable_txt <- capture.output(
  etable(
    list(
      raw_full = m_raw_full,
      fe_full = m_fe_full,
      raw_pre = m_raw_pre,
      fe_pre = m_fe_pre,
      logit_fe_full = m_bin_fe_full,
      logit_fe_pre = m_bin_fe_pre
    ),
    se.below = TRUE,
    digits = 4
  )
)
etable_tex <- capture.output(
  etable(
    list(
      raw_full = m_raw_full,
      fe_full = m_fe_full,
      raw_pre = m_raw_pre,
      fe_pre = m_fe_pre,
      logit_fe_full = m_bin_fe_full,
      logit_fe_pre = m_bin_fe_pre
    ),
    se.below = TRUE,
    digits = 4,
    tex = TRUE
  )
)

panel_out <- panel[, .(
  id, PID, year, year_quarter, retaliatory_status,
  case_p_female, case_p_male,
  n_defendants_total, n_defendants_gender_imputed,
  tract_geoid, building_type, num_units_bin,
  filing_rate_longrun, filing_rate_longrun_pre2019,
  filing_rate_eb_pre_covid, filing_rate_eb_city_pre_covid, filing_rate_eb_zip_pre_covid,
  filing_rate_longrun_pre2019_cuts, filing_rate_eb_pre_covid_cuts
)]

fwrite(panel_out, file.path(out_dir, "retaliatory_gender_case_panel.csv"))
fwrite(status_means_full, file.path(out_dir, "retaliatory_gender_status_means.csv"))
fwrite(desc_overall, file.path(out_dir, "retaliatory_gender_descriptives_overall.csv"))
fwrite(desc_by_building_type, file.path(out_dir, "retaliatory_gender_descriptives_by_building_type.csv"))
fwrite(desc_by_num_units_bin, file.path(out_dir, "retaliatory_gender_descriptives_by_num_units_bin.csv"))
fwrite(desc_by_longrun_cut, file.path(out_dir, "retaliatory_gender_descriptives_by_longrun_rate_cut.csv"))
fwrite(desc_by_eb_cut, file.path(out_dir, "retaliatory_gender_descriptives_by_eb_rate_cut.csv"))
fwrite(desc_by_building_rate_cut, file.path(out_dir, "retaliatory_gender_descriptives_by_building_longrun_cut.csv"))
fwrite(effects, file.path(out_dir, "retaliatory_gender_effects.csv"))
fwrite(hetero_effects, file.path(out_dir, "retaliatory_gender_heterogeneity_rate_cuts.csv"))
writeLines(etable_txt, con = file.path(out_dir, "retaliatory_gender_models.txt"))
writeLines(etable_tex, con = file.path(out_dir, "retaliatory_gender_models.tex"))
write_latex_table(
  status_means_full,
  path = file.path(out_dir, "retaliatory_gender_status_means.tex"),
  digits = 4L,
  caption = "Mean gender probabilities by retaliatory status (full sample)"
)
write_latex_table(
  effects[, .(model, term, estimate, std_error, p_value, conf_low, conf_high)],
  path = file.path(out_dir, "retaliatory_gender_effects.tex"),
  digits = 4L,
  caption = "Retaliatory status coefficient table (gender outcomes)"
)
write_latex_table(
  desc_overall,
  path = file.path(out_dir, "retaliatory_gender_descriptives_overall.tex"),
  digits = 4L,
  caption = "Retaliatory-status descriptive shares (overall, gender sample)"
)
write_latex_table(
  desc_by_longrun_cut,
  path = file.path(out_dir, "retaliatory_gender_descriptives_by_longrun_rate_cut.tex"),
  digits = 4L,
  caption = "Retaliatory-status shares by long-run filing-rate cut (gender sample)"
)
write_latex_table(
  hetero_effects[, .(sample, heterogeneity_dim, heterogeneity_cut, term, estimate, std_error, p_value, conf_low, conf_high, n_obs, n_pid)],
  path = file.path(out_dir, "retaliatory_gender_heterogeneity_rate_cuts.tex"),
  digits = 4L,
  caption = "Heterogeneity in retaliatory effects by long-run filing-rate cuts (gender)"
)

qa_lines <- c(
  "Retaliatory Gender QA",
  paste0("Timestamp: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  paste0("Gender case input: ", gender_case_path),
  paste0("Evictions input: ", evictions_path),
  paste0("Xwalk input: ", xwalk_path),
  paste0("Quarter panel input: ", quarter_panel_path),
  paste0("Analytic controls input: ", analytic_path),
  paste0("BLP panel input: ", blp_panel_path),
  "",
  paste0("Gender-case rows (ok, non-missing p_female): ", nrow(gender_case)),
  paste0("Case->PID rows before ambiguity drop: ", nrow(case_pid)),
  paste0("Ambiguous case IDs dropped: ", length(amb_ids)),
  paste0("Case->PID rows after ambiguity drop: ", nrow(case_pid_clean)),
  paste0("Rows with full FE controls: ", nrow(panel)),
  paste0("Rows in pre-covid panel: ", nrow(panel_pre)),
  paste0("Rows in binary panel: ", nrow(panel_bin)),
  "",
  "Mean gender probabilities by retaliatory status (full):",
  capture.output(print(status_means_full)),
  "",
  "Retaliatory shares by long-run filing-rate cut:",
  capture.output(print(desc_by_longrun_cut)),
  "",
  "Retaliatory shares by unit-size bin:",
  capture.output(print(desc_by_num_units_bin)),
  "",
  "Heterogeneity table (head):",
  capture.output(print(head(hetero_effects, 20L))),
  "",
  paste0("Wrote panel: ", file.path(out_dir, "retaliatory_gender_case_panel.csv")),
  paste0("Wrote status means: ", file.path(out_dir, "retaliatory_gender_status_means.csv")),
  paste0("Wrote status means LaTeX: ", file.path(out_dir, "retaliatory_gender_status_means.tex")),
  paste0("Wrote descriptives overall: ", file.path(out_dir, "retaliatory_gender_descriptives_overall.csv")),
  paste0("Wrote descriptives by building: ", file.path(out_dir, "retaliatory_gender_descriptives_by_building_type.csv")),
  paste0("Wrote descriptives by units bin: ", file.path(out_dir, "retaliatory_gender_descriptives_by_num_units_bin.csv")),
  paste0("Wrote descriptives by long-run cut: ", file.path(out_dir, "retaliatory_gender_descriptives_by_longrun_rate_cut.csv")),
  paste0("Wrote descriptives by EB cut: ", file.path(out_dir, "retaliatory_gender_descriptives_by_eb_rate_cut.csv")),
  paste0("Wrote descriptives by building x long-run cut: ", file.path(out_dir, "retaliatory_gender_descriptives_by_building_longrun_cut.csv")),
  paste0("Wrote descriptives overall LaTeX: ", file.path(out_dir, "retaliatory_gender_descriptives_overall.tex")),
  paste0("Wrote descriptives by long-run cut LaTeX: ", file.path(out_dir, "retaliatory_gender_descriptives_by_longrun_rate_cut.tex")),
  paste0("Wrote effects: ", file.path(out_dir, "retaliatory_gender_effects.csv")),
  paste0("Wrote heterogeneity effects: ", file.path(out_dir, "retaliatory_gender_heterogeneity_rate_cuts.csv")),
  paste0("Wrote effects LaTeX: ", file.path(out_dir, "retaliatory_gender_effects.tex")),
  paste0("Wrote heterogeneity LaTeX: ", file.path(out_dir, "retaliatory_gender_heterogeneity_rate_cuts.tex")),
  paste0("Wrote model table: ", file.path(out_dir, "retaliatory_gender_models.txt")),
  paste0("Wrote model table LaTeX: ", file.path(out_dir, "retaliatory_gender_models.tex"))
)
writeLines(qa_lines, con = file.path(out_dir, "retaliatory_gender_qa.txt"))

logf("Wrote outputs to: ", out_dir, log_file = log_file)
logf("=== Finished retaliatory-evictions-gender.R ===", log_file = log_file)
