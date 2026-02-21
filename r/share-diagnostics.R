## ============================================================
## share-diagnostics.R
## ============================================================
## Purpose: Diagnose share quality and variation for BLP estimation.
##   - Are shares "wonky"?
##   - How much within-PID vs between-PID variation?
##   - What drives shares (building size vs occupancy dynamics)?
##   - How does FE demeaning affect price/share residual variation?
##
## Inputs: bldg_panel_blp (via config)
## Outputs: output/qa/share_diagnostics.txt, output/qa/share_*.png
## ============================================================

library(data.table)
library(ggplot2)

source("r/config.R")
cfg <- read_config()

out_dir <- p_out(cfg, "qa")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
log_file <- p_out(cfg, "logs", "share_diagnostics.log")

# ---- Load data ----
logf("Loading bldg_panel_blp", log_file = log_file)
dt <- fread(p_product(cfg, "bldg_panel_blp"))
logf(sprintf("Raw rows: %s", format(nrow(dt), big.mark = ",")), log_file = log_file)

# ---- Compute shares from full universe (same logic as make-analytic-sample.R) ----
# Market = zip × year × unit bin

if (!"num_units_bin" %in% names(dt)) {
  dt[, num_units_bin := cut(
    total_units,
    breaks = c(-Inf, 1, 5, 20, 50, Inf),
    labels = c("1", "2-5", "6-20", "21-50", "51+"),
    include.lowest = TRUE
  )]
}

# Use occupied_units_scaled if available, else occupied_units
occ_col <- if ("occupied_units_scaled" %in% names(dt)) "occupied_units_scaled" else "occupied_units"

# share_zip: occupied_units_scaled / sum(total_units) by (pm.zip, year, num_units_bin)
dt[, total_units_market := sum(total_units, na.rm = TRUE), by = .(pm.zip, year, num_units_bin)]
dt[, share_zip := fifelse(
  total_units_market > 0,
  get(occ_col) / total_units_market,
  NA_real_
)]

# share_zip_bin: alias (same as share_zip now that market = zip-year-bin)
dt[, share_zip_bin := share_zip]

# ---- Build estimation-like sample (mirrors Python filters) ----
logf("Building estimation sample", log_file = log_file)
est <- dt[year %between% c(2011, 2019)]
logf(sprintf("  After year range: %s", format(nrow(est), big.mark = ",")), log_file = log_file)

est <- est[total_units > 1]
logf(sprintf("  After unit filter: %s", format(nrow(est), big.mark = ",")), log_file = log_file)

est <- est[!is.na(med_rent) & !is.na(share_zip) & share_zip > 0]
logf(sprintf("  After dropping missing rent/shares: %s", format(nrow(est), big.mark = ",")), log_file = log_file)

est <- est[share_zip < 1]
logf(sprintf("  After share < 1: %s", format(nrow(est), big.mark = ",")), log_file = log_file)

# Market share sum < 1
est[, mkt_share_sum := sum(share_zip, na.rm = TRUE), by = .(pm.zip, year)]
est <- est[mkt_share_sum < 1]
logf(sprintf("  After market sum < 1: %s", format(nrow(est), big.mark = ",")), log_file = log_file)

# Building type filter
if ("building_type" %in% names(est)) {
  btype_re <- "LOWRISE_MULTI|MIDRISE_MULTI|HIGHRISE_MULTI|SMALL_MULTI_2_4|MULTI_BLDG_COMPLEX"
  est <- est[grepl(btype_re, building_type, ignore.case = TRUE)]
  logf(sprintf("  After building type: %s", format(nrow(est), big.mark = ",")), log_file = log_file)
}

est <- est[!is.na(market_value) & market_value > 1]
est[, log_market_value := log(market_value)]
logf(sprintf("  After market value > 0: %s", format(nrow(est), big.mark = ",")), log_file = log_file)

# Annualized rent change filter
setorder(est, PID, year)
est[, d_log_rent := c(NA, diff(log_med_rent)) / c(NA, diff(year)), by = PID]
est <- est[is.na(d_log_rent) | abs(d_log_rent) <= 0.3]
logf(sprintf("  After rent change filter: %s", format(nrow(est), big.mark = ",")), log_file = log_file)

# drop NA zips
est <- est[!is.na(pm.zip) & pm.zip != ""]
logf(sprintf("  After dropping NA zips: %s", format(nrow(est), big.mark = ",")), log_file = log_file)

# Market ID (zip × year × unit bin, matching make-analytic-sample.R)
est[, market_id := paste(pm.zip, year, num_units_bin, sep = "-")]
n_markets <- est[, uniqueN(market_id)]
n_pids <- est[, uniqueN(PID)]
logf(sprintf("  Final: %s rows, %d markets, %d PIDs",
             format(nrow(est), big.mark = ","), n_markets, n_pids), log_file = log_file)

# ---- Convenience aliases ----
est[, prices := med_rent]
est[, shares := share_zip]

# Log-odds (delta_logit)
est[, s0 := 1 - sum(shares, na.rm = TRUE), by = market_id]
est[, delta_logit := log(pmax(shares, 1e-12)) - log(pmax(s0, 1e-12))]

# Within-nest share
est[, nesting_ids := fifelse(
  !is.na(filing_rate_eb_pre_covid) & filing_rate_eb_pre_covid >= 0.1,
  "high", "low"
)]
est[, within_nest_sum := sum(shares, na.rm = TRUE), by = .(market_id, nesting_ids)]
est[, s_jg := shares / within_nest_sum]
est[, log_s_jg := log(pmax(s_jg, 1e-12))]

# ============================================================
# 1. Share distribution
# ============================================================
sink_file <- file.path(out_dir, "share_diagnostics.txt")
sink(sink_file)

cat("============================================================\n")
cat("  SHARE DIAGNOSTICS\n")
cat("============================================================\n\n")

cat("--- 1. Share distribution in estimation sample ---\n\n")
for (scol in c("share_zip", "share_zip_bin")) {
  s <- est[[scol]]
  s <- s[!is.na(s)]
  cat(sprintf("  %s:\n", scol))
  cat(sprintf("    N=%d, mean=%.6f, std=%.6f\n", length(s), mean(s), sd(s)))
  cat(sprintf("    min=%.6f, p10=%.6f, p25=%.6f\n",
              min(s), quantile(s, 0.1), quantile(s, 0.25)))
  cat(sprintf("    median=%.6f, p75=%.6f, p90=%.6f\n",
              median(s), quantile(s, 0.75), quantile(s, 0.9)))
  cat(sprintf("    max=%.6f\n", max(s)))

  mkt_sums <- est[!is.na(get(scol)),
                   .(s = sum(get(scol))),
                   by = market_id]$s
  cat(sprintf("    Market sums: mean=%.4f, median=%.4f, max=%.4f\n\n",
              mean(mkt_sums), median(mkt_sums), max(mkt_sums)))
}

# ============================================================
# 2. Within-PID share variation over time
# ============================================================
cat("--- 2. Within-PID share variation over time ---\n")

pid_stats <- est[, .(
  n_years    = uniqueN(year),
  share_mean = mean(shares, na.rm = TRUE),
  share_std  = sd(shares, na.rm = TRUE),
  share_cv   = fifelse(mean(shares, na.rm = TRUE) > 0,
                        sd(shares, na.rm = TRUE) / mean(shares, na.rm = TRUE),
                        NA_real_),
  price_mean = mean(prices, na.rm = TRUE),
  price_std  = sd(prices, na.rm = TRUE),
  price_cv   = fifelse(mean(prices, na.rm = TRUE) > 0,
                        sd(prices, na.rm = TRUE) / mean(prices, na.rm = TRUE),
                        NA_real_)
), by = PID]

multi <- pid_stats[n_years >= 2]

cat(sprintf("  PIDs with 2+ years: %d\n", nrow(multi)))
cat(sprintf("  Mean years per PID: %.1f\n", mean(multi$n_years)))

cat(sprintf("\n  Within-PID share std:\n"))
cat(sprintf("    mean=%.6f, median=%.6f\n",
            mean(multi$share_std, na.rm = TRUE),
            median(multi$share_std, na.rm = TRUE)))
cat(sprintf("    frac with std==0: %.3f\n",
            mean(multi$share_std == 0, na.rm = TRUE)))
cat(sprintf("    frac with std < 0.001: %.3f\n",
            mean(multi$share_std < 0.001, na.rm = TRUE)))

cat(sprintf("\n  Within-PID share CV (std/mean):\n"))
cat(sprintf("    mean=%.3f, median=%.3f\n",
            mean(multi$share_cv, na.rm = TRUE),
            median(multi$share_cv, na.rm = TRUE)))

cat(sprintf("\n  Within-PID price std:\n"))
cat(sprintf("    mean=%.4f, median=%.4f\n",
            mean(multi$price_std, na.rm = TRUE),
            median(multi$price_std, na.rm = TRUE)))
cat(sprintf("    frac with std==0: %.3f\n",
            mean(multi$price_std == 0, na.rm = TRUE)))

cat(sprintf("\n  Within-PID price CV:\n"))
cat(sprintf("    mean=%.4f, median=%.4f\n\n",
            mean(multi$price_cv, na.rm = TRUE),
            median(multi$price_cv, na.rm = TRUE)))

# ============================================================
# 3. Between vs within PID variation
# ============================================================
cat("--- 3. Between vs within PID variation ---\n")

for (var in c("shares", "prices")) {
  total_var <- var(est[[var]], na.rm = TRUE)
  pid_means <- est[, .(m = mean(get(var), na.rm = TRUE)), by = PID]
  est_tmp <- merge(est[, .(PID, v = get(var))], pid_means, by = "PID")
  between_var <- var(est_tmp$m, na.rm = TRUE)
  within_var  <- var(est_tmp$v - est_tmp$m, na.rm = TRUE)

  cat(sprintf("\n  %s:\n", var))
  cat(sprintf("    Total var: %.6f\n", total_var))
  cat(sprintf("    Between-PID var: %.6f (%.1f%%)\n",
              between_var, 100 * between_var / total_var))
  cat(sprintf("    Within-PID var: %.6f (%.1f%%)\n",
              within_var, 100 * within_var / total_var))
}

# ============================================================
# 4. What drives shares — building size vs dynamics?
# ============================================================
cat("\n\n--- 4. Share composition: how much is just building size? ---\n")

cor_cols <- c("shares", "total_units", "occupied_units")
if ("renter_occ" %in% names(est)) cor_cols <- c(cor_cols, "renter_occ")
cor_mat <- cor(est[, ..cor_cols], use = "pairwise.complete.obs")

cat(sprintf("  Corr(share, total_units): %.4f\n",
            cor_mat["shares", "total_units"]))
cat(sprintf("  Corr(share, occupied_units): %.4f\n",
            cor_mat["shares", "occupied_units"]))
if ("renter_occ" %in% cor_cols) {
  cat(sprintf("  Corr(share, renter_occ): %.4f\n",
              cor_mat["shares", "renter_occ"]))
}

if ("occupancy_rate" %in% names(est)) {
  occ <- est[!is.na(occupancy_rate), occupancy_rate]
  cat(sprintf("\n  Occupancy rate: mean=%.3f, std=%.3f\n", mean(occ), sd(occ)))

  pid_occ_std <- est[!is.na(occupancy_rate),
                     .(occ_std = sd(occupancy_rate, na.rm = TRUE)),
                     by = PID][!is.na(occ_std)]
  cat(sprintf("  Within-PID occ rate std: mean=%.4f, median=%.4f\n",
              mean(pid_occ_std$occ_std), median(pid_occ_std$occ_std)))
  cat(sprintf("  Frac PIDs with zero occ-rate variation: %.3f\n",
              mean(pid_occ_std$occ_std == 0)))
}

# ============================================================
# 5. Raw correlations of shares / prices / logit LHS
# ============================================================
cat("\n--- 5. Raw correlation of shares/prices ---\n")
cat(sprintf("  Corr(share, price): %.4f\n",
            cor(est$shares, est$prices, use = "complete.obs")))
cat(sprintf("  Corr(log(s)-log(s0), price): %.4f\n",
            cor(est$delta_logit, est$prices, use = "complete.obs")))
cat(sprintf("  Corr(log(s_j|g), price): %.4f\n",
            cor(est$log_s_jg, est$prices, use = "complete.obs")))

# ============================================================
# 6. Residual variation after FE demeaning
# ============================================================
cat("\n--- 6. Residual variation after FE demeaning ---\n")
cat("  (single-pass group demeaning, approximate for multi-way)\n\n")

demean_one <- function(x, groups) {
  gm <- ave(x, groups, FUN = function(z) mean(z, na.rm = TRUE))
  x - gm
}

fe_specs <- list(
  "Year only"        = list(fe = "year"),
  "GEOID + year"     = list(fe = c("GEOID", "year")),
  "Market (zip*year)" = list(fe = "market_id"),
  "Market + GEOID"   = list(fe = c("market_id", "GEOID")),
  "GEOID*year"       = list(fe = "GEOID_year")
)

est[, GEOID_year := paste(GEOID, year, sep = "_")]

for (spec_name in names(fe_specs)) {
  fe_cols <- fe_specs[[spec_name]]$fe

  p_r <- est$prices
  d_r <- est$delta_logit
  for (fe in fe_cols) {
    if (!fe %in% names(est)) next
    p_r <- demean_one(p_r, est[[fe]])
    d_r <- demean_one(d_r, est[[fe]])
  }

  pvar <- var(p_r, na.rm = TRUE)
  dvar <- var(d_r, na.rm = TRUE)
  rcor <- cor(p_r, d_r, use = "complete.obs")

  cat(sprintf("  %-25s  price_var=%.4f  delta_var=%.4f  corr(p~, d~)=%+.4f\n",
              spec_name, pvar, dvar, rcor))
}

# ============================================================
# 7. Within-PID occupancy changes by unit type
# ============================================================
cat("\n--- 7. Within-PID occupancy & share changes by unit type ---\n")

# Classify PIDs by unit size bin (use mode across years)
est[, pid_bin := num_units_bin[1], by = PID]
est[, n_years_pid := uniqueN(year), by = PID]

pid_occ_by_bin <- est[, .(
  n_years   = uniqueN(year),
  occ_mean  = mean(occupancy_rate, na.rm = TRUE),
  occ_std   = sd(occupancy_rate, na.rm = TRUE),
  share_mean = mean(shares, na.rm = TRUE),
  share_std  = sd(shares, na.rm = TRUE),
  price_std  = sd(prices, na.rm = TRUE),
  occ_range  = fifelse(.N >= 2,
                        max(occupancy_rate, na.rm = TRUE) - min(occupancy_rate, na.rm = TRUE),
                        0),
  share_range = fifelse(.N >= 2,
                         max(shares, na.rm = TRUE) - min(shares, na.rm = TRUE),
                         0),
  mean_units = mean(total_units, na.rm = TRUE)
), by = .(PID, pid_bin)]

# Focus on PIDs with 2+ years
pid_multi <- pid_occ_by_bin[n_years >= 2]

cat("\n  A) Number of PIDs by unit bin:\n")
bin_counts <- pid_multi[, .(
  n_pids = .N,
  mean_years = mean(n_years),
  mean_units = mean(mean_units)
), by = pid_bin]
setorder(bin_counts, pid_bin)
for (i in seq_len(nrow(bin_counts))) {
  r <- bin_counts[i]
  cat(sprintf("    %-8s  %4d PIDs  (mean %.1f years, mean %.0f units)\n",
              r$pid_bin, r$n_pids, r$mean_years, r$mean_units))
}

cat("\n  B) Within-PID occupancy rate std by unit bin:\n")
cat(sprintf("    %-8s  %8s  %8s  %8s  %8s  %8s\n",
            "Bin", "mean", "median", "p75", "p90", "frac=0"))
occ_by_bin <- pid_multi[, .(
  mean_std   = mean(occ_std, na.rm = TRUE),
  median_std = median(occ_std, na.rm = TRUE),
  p75_std    = quantile(occ_std, 0.75, na.rm = TRUE),
  p90_std    = quantile(occ_std, 0.90, na.rm = TRUE),
  frac_zero  = mean(occ_std == 0, na.rm = TRUE)
), by = pid_bin]
setorder(occ_by_bin, pid_bin)
for (i in seq_len(nrow(occ_by_bin))) {
  r <- occ_by_bin[i]
  cat(sprintf("    %-8s  %8.4f  %8.4f  %8.4f  %8.4f  %8.3f\n",
              r$pid_bin, r$mean_std, r$median_std, r$p75_std, r$p90_std, r$frac_zero))
}

cat("\n  C) Within-PID share std by unit bin:\n")
cat(sprintf("    %-8s  %10s  %10s  %10s  %10s  %8s\n",
            "Bin", "mean", "median", "p75", "p90", "frac<1e-3"))
share_by_bin <- pid_multi[, .(
  mean_std   = mean(share_std, na.rm = TRUE),
  median_std = median(share_std, na.rm = TRUE),
  p75_std    = quantile(share_std, 0.75, na.rm = TRUE),
  p90_std    = quantile(share_std, 0.90, na.rm = TRUE),
  frac_tiny  = mean(share_std < 0.001, na.rm = TRUE)
), by = pid_bin]
setorder(share_by_bin, pid_bin)
for (i in seq_len(nrow(share_by_bin))) {
  r <- share_by_bin[i]
  cat(sprintf("    %-8s  %10.6f  %10.6f  %10.6f  %10.6f  %8.3f\n",
              r$pid_bin, r$mean_std, r$median_std, r$p75_std, r$p90_std, r$frac_tiny))
}

cat("\n  D) Within-PID occupancy RANGE by unit bin:\n")
cat(sprintf("    %-8s  %8s  %8s  %8s  %8s\n",
            "Bin", "mean", "median", "p75", "p90"))
range_by_bin <- pid_multi[, .(
  mean_rng   = mean(occ_range, na.rm = TRUE),
  median_rng = median(occ_range, na.rm = TRUE),
  p75_rng    = quantile(occ_range, 0.75, na.rm = TRUE),
  p90_rng    = quantile(occ_range, 0.90, na.rm = TRUE)
), by = pid_bin]
setorder(range_by_bin, pid_bin)
for (i in seq_len(nrow(range_by_bin))) {
  r <- range_by_bin[i]
  cat(sprintf("    %-8s  %8.4f  %8.4f  %8.4f  %8.4f\n",
              r$pid_bin, r$mean_rng, r$median_rng, r$p75_rng, r$p90_rng))
}

cat("\n  E) Within-PID price std by unit bin:\n")
cat(sprintf("    %-8s  %8s  %8s  %8s  %8s\n",
            "Bin", "mean", "median", "p75", "frac=0"))
price_by_bin <- pid_multi[, .(
  mean_std   = mean(price_std, na.rm = TRUE),
  median_std = median(price_std, na.rm = TRUE),
  p75_std    = quantile(price_std, 0.75, na.rm = TRUE),
  frac_zero  = mean(price_std == 0, na.rm = TRUE)
), by = pid_bin]
setorder(price_by_bin, pid_bin)
for (i in seq_len(nrow(price_by_bin))) {
  r <- price_by_bin[i]
  cat(sprintf("    %-8s  %8.4f  %8.4f  %8.4f  %8.3f\n",
              r$pid_bin, r$mean_std, r$median_std, r$p75_std, r$frac_zero))
}

cat("\n  F) Corr(delta_occ_rate, delta_price) by unit bin (within-PID changes):\n")
setorder(est, PID, year)
est[, d_occ := c(NA, diff(occupancy_rate)), by = PID]
est[, d_price := c(NA, diff(prices)), by = PID]
corr_by_bin <- est[!is.na(d_occ) & !is.na(d_price),
                   .(corr = cor(d_occ, d_price, use = "complete.obs"),
                     n_obs = .N),
                   by = pid_bin]
setorder(corr_by_bin, pid_bin)
for (i in seq_len(nrow(corr_by_bin))) {
  r <- corr_by_bin[i]
  cat(sprintf("    %-8s  corr=%+.4f  (N=%d changes)\n",
              r$pid_bin, r$corr, r$n_obs))
}

# ============================================================
# 8. Within-PID raw occupancy, num_households, occupied_units_scaled
# ============================================================
cat("\n--- 8. Within-PID raw occupancy & household counts by unit bin ---\n")

raw_vars <- c("renter_occ", "occupied_units_scaled", "occupancy_rate")
if ("num_households" %in% names(est)) raw_vars <- c(raw_vars, "num_households")

for (rv in raw_vars) {
  if (!rv %in% names(est)) next

  cat(sprintf("\n  %s:\n", rv))
  vals <- est[[rv]]
  vals <- vals[!is.na(vals)]
  cat(sprintf("    Overall: N=%d, mean=%.2f, std=%.2f, min=%.2f, max=%.2f\n",
              length(vals), mean(vals), sd(vals), min(vals), max(vals)))

  # Within-PID variation
  pid_rv <- est[!is.na(get(rv)) & n_years_pid >= 2, {
    v <- as.numeric(get(rv))
    m <- mean(v, na.rm = TRUE)
    s <- sd(v, na.rm = TRUE)
    .(rv_mean  = m,
      rv_std   = s,
      rv_range = max(v, na.rm = TRUE) - min(v, na.rm = TRUE),
      rv_cv    = fifelse(m > 0, s / m, NA_real_))
  }, by = .(PID, pid_bin)]

  cat(sprintf("    Within-PID std:   mean=%.3f, median=%.3f\n",
              mean(pid_rv$rv_std, na.rm = TRUE), median(pid_rv$rv_std, na.rm = TRUE)))
  cat(sprintf("    Within-PID range: mean=%.3f, median=%.3f\n",
              mean(pid_rv$rv_range, na.rm = TRUE), median(pid_rv$rv_range, na.rm = TRUE)))
  cat(sprintf("    Within-PID CV:    mean=%.3f, median=%.3f\n",
              mean(pid_rv$rv_cv, na.rm = TRUE), median(pid_rv$rv_cv, na.rm = TRUE)))
  cat(sprintf("    Frac zero std:    %.3f\n",
              mean(pid_rv$rv_std == 0, na.rm = TRUE)))

  # By unit bin
  cat(sprintf("\n    By unit bin:\n"))
  cat(sprintf("    %-8s  %6s  %8s  %8s  %8s  %8s  %8s  %8s\n",
              "Bin", "N_pid", "std_mean", "std_med", "rng_mean", "rng_med", "cv_mean", "frac=0"))
  bin_rv <- pid_rv[, .(
    n_pid    = .N,
    std_mean = mean(rv_std, na.rm = TRUE),
    std_med  = median(rv_std, na.rm = TRUE),
    rng_mean = mean(rv_range, na.rm = TRUE),
    rng_med  = median(rv_range, na.rm = TRUE),
    cv_mean  = mean(rv_cv, na.rm = TRUE),
    frac0    = mean(rv_std == 0, na.rm = TRUE)
  ), by = pid_bin]
  setorder(bin_rv, pid_bin)
  for (i in seq_len(nrow(bin_rv))) {
    r <- bin_rv[i]
    cat(sprintf("    %-8s  %6d  %8.3f  %8.3f  %8.3f  %8.3f  %8.3f  %8.3f\n",
                r$pid_bin, r$n_pid, r$std_mean, r$std_med,
                r$rng_mean, r$rng_med, r$cv_mean, r$frac0))
  }
}

sink()
cat(sprintf("\nDiagnostics written to: %s\n", sink_file))

# ============================================================
# 7. Figures
# ============================================================
logf("Generating diagnostic plots", log_file = log_file)

# 7a. Share distribution histogram
p1 <- ggplot(est, aes(x = shares)) +
  geom_histogram(bins = 60, fill = "steelblue", color = "white", linewidth = 0.2) +
  scale_x_log10(labels = scales::label_number()) +
  labs(title = "Distribution of share_zip in estimation sample",
       x = "Share (log scale)", y = "Count") +
  theme_bw()
ggsave(file.path(out_dir, "share_zip_distribution.png"), p1, width = 8, height = 5)

# 7b. Within-PID share CV vs price CV (for PIDs with 2+ years)
p2 <- ggplot(multi[share_cv < 2 & price_cv < 0.1],
             aes(x = price_cv, y = share_cv)) +
  geom_point(alpha = 0.3, size = 1) +
  geom_smooth(method = "lm", se = FALSE, color = "red") +
  labs(title = "Within-PID variation: share CV vs price CV",
       subtitle = "Each dot = one PID (2+ years observed)",
       x = "Price CV (within PID)", y = "Share CV (within PID)") +
  theme_bw()
ggsave(file.path(out_dir, "share_vs_price_within_pid_cv.png"), p2, width = 8, height = 6)

# 7c. Share vs total_units scatter
p3 <- ggplot(est, aes(x = total_units, y = shares)) +
  geom_point(alpha = 0.2, size = 0.8) +
  geom_smooth(method = "lm", se = FALSE, color = "red") +
  scale_x_log10() +
  scale_y_log10() +
  labs(title = "Share vs building size",
       x = "Total units (log scale)", y = "Share (log scale)") +
  theme_bw()
ggsave(file.path(out_dir, "share_vs_total_units.png"), p3, width = 8, height = 6)

# 7d. Variance decomposition bar chart
decomp <- data.table(
  variable = rep(c("Shares", "Prices"), each = 2),
  component = rep(c("Between PID", "Within PID"), 2)
)
# Compute
s_total <- var(est$shares, na.rm = TRUE)
s_between <- var(est[, mean(shares, na.rm = TRUE), by = PID]$V1, na.rm = TRUE)
s_within <- var(est$shares - est[, mean(shares, na.rm = TRUE), by = PID][
  match(est$PID, PID), V1], na.rm = TRUE)

p_total <- var(est$prices, na.rm = TRUE)
p_between <- var(est[, mean(prices, na.rm = TRUE), by = PID]$V1, na.rm = TRUE)
p_within <- var(est$prices - est[, mean(prices, na.rm = TRUE), by = PID][
  match(est$PID, PID), V1], na.rm = TRUE)

decomp[, pct := c(
  s_between / s_total * 100, s_within / s_total * 100,
  p_between / p_total * 100, p_within / p_total * 100
)]

p4 <- ggplot(decomp, aes(x = variable, y = pct, fill = component)) +
  geom_col(position = "stack") +
  geom_text(aes(label = sprintf("%.1f%%", pct)),
            position = position_stack(vjust = 0.5), size = 4) +
  scale_fill_manual(values = c("Between PID" = "steelblue", "Within PID" = "coral")) +
  labs(title = "Variance decomposition: between vs within PID",
       x = NULL, y = "% of total variance", fill = NULL) +
  theme_bw() +
  theme(legend.position = "bottom")
ggsave(file.path(out_dir, "variance_decomposition.png"), p4, width = 7, height = 5)

# 7e. Within-PID share trajectories (random sample of PIDs)
set.seed(42)
sample_pids <- multi[n_years >= 3, sample(PID, min(30, .N))]
traj_dt <- est[PID %in% sample_pids, .(PID, year, shares)]

p5 <- ggplot(traj_dt, aes(x = year, y = shares, group = PID)) +
  geom_line(alpha = 0.5) +
  geom_point(size = 1, alpha = 0.5) +
  labs(title = "Share trajectories for 30 random PIDs (3+ years)",
       x = "Year", y = "share_zip") +
  theme_bw()
ggsave(file.path(out_dir, "share_trajectories_sample.png"), p5, width = 8, height = 5)

# 7f. Inside share sums by market
mkt_sums <- est[, .(inside_sum = sum(shares, na.rm = TRUE),
                     n_products = .N), by = market_id]

p6 <- ggplot(mkt_sums, aes(x = inside_sum)) +
  geom_histogram(bins = 40, fill = "steelblue", color = "white", linewidth = 0.2) +
  geom_vline(xintercept = median(mkt_sums$inside_sum),
             linetype = "dashed", color = "red") +
  labs(title = "Distribution of inside share sums by market",
       subtitle = sprintf("Median = %.3f (red dashed)", median(mkt_sums$inside_sum)),
       x = "Sum of shares within market (zip-year)", y = "Count") +
  theme_bw()
ggsave(file.path(out_dir, "inside_share_sums.png"), p6, width = 8, height = 5)

logf(sprintf("Done. Output in %s", out_dir), log_file = log_file)
cat(sprintf("\nPlots saved to: %s\n", out_dir))





