## ============================================================
## analyze-rtt-transfers.R
## ============================================================
## Purpose: Analyze real estate transfers (RTT) for rental properties,
##          focusing on high-evicting buildings, buyer composition,
##          and post-transfer eviction behavior.
##
## Inputs (from config):
##   - rtt_clean (from process-rtt-data.R)
##   - bldg_panel_blp (from make-analytic-sample.R)
##
## Outputs:
##   - output/rtt_analysis/*.csv  (13 tables)
##   - figs/rtt_*.png             (7 figures)
##   - output/logs/analyze-rtt-transfers.log
##
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(stringr)
  library(ggplot2)
  library(fixest)
})

source("r/config.R")
source("r/helper-functions.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "analyze-rtt-transfers.log")

logf("=== Starting analyze-rtt-transfers.R ===", log_file = log_file)

# Output directories
out_dir <- p_out(cfg, "rtt_analysis")
fig_dir <- file.path(cfg$paths$repo_root, "figs")
fs::dir_create(out_dir, recurse = TRUE)
fs::dir_create(fig_dir, recurse = TRUE)

normalize_pid <- function(x) {
  stringr::str_pad(as.character(x), width = 9, side = "left", pad = "0")
}

# ============================================================
# SECTION 0: Setup & Data Loading
# ============================================================
logf("--- SECTION 0: Loading data ---", log_file = log_file)

rtt <- fread(p_product(cfg, "rtt_clean"))
rtt[, PID := normalize_pid(PID)]
logf("  rtt_clean: ", nrow(rtt), " rows, ", rtt[, uniqueN(PID)], " unique PIDs", log_file = log_file)

bldg <- fread(p_product(cfg, "bldg_panel_blp"))
bldg[, PID := normalize_pid(PID)]
logf("  bldg_panel_blp: ", nrow(bldg), " rows, ", bldg[, uniqueN(PID)], " unique PIDs", log_file = log_file)

# Merge RTT -> bldg on (PID, year) — inner join
rtt[, year := as.integer(year)]
bldg[, year := as.integer(year)]

dt <- merge(rtt, bldg, by = c("PID", "year"), all = FALSE)
n_rtt <- nrow(rtt)
n_matched <- nrow(dt)
match_rate <- round(100 * n_matched / n_rtt, 1)
logf("  Merged RTT x bldg_panel: ", n_matched, " / ", n_rtt,
     " RTT rows matched (", match_rate, "%)", log_file = log_file)
logf("  Unique PIDs in merged: ", dt[, uniqueN(PID)], log_file = log_file)

# Sheriff's deed flag
dt[, is_sheriff_deed := grepl("SHERIFF", toupper(document_type))]
logf("  Sheriff's deeds: ", dt[is_sheriff_deed == TRUE, .N], " / ", nrow(dt),
     " (", round(100 * dt[is_sheriff_deed == TRUE, .N] / nrow(dt), 1), "%)",
     log_file = log_file)

# Price: no winsorization (would drop high-rises which are <1% of buildings
# but sell for much more). Use raw total_consideration.
dt[, price_w := total_consideration]
logf("  Price range: ", min(dt$price_w, na.rm = TRUE), " to ",
     max(dt$price_w, na.rm = TRUE), log_file = log_file)

# Price per unit
dt[, price_per_unit := fifelse(
  !is.na(price_w) & !is.na(total_units) & total_units > 0,
  price_w / total_units,
  NA_real_
)]

# Helper: flag small cells
flag_small <- function(n, label, threshold = 30) {
  if (n < threshold) {
    logf("  WARNING: small cell (n=", n, ") for: ", label, log_file = log_file)
  }
}

# ============================================================
# SECTION 1: Basic Summary Stats
# ============================================================
logf("--- SECTION 1: Basic summary stats ---", log_file = log_file)

# Table 1: transfer_summary_by_year.csv
tab1 <- dt[, .(
  n_transfers    = .N,
  median_price   = median(price_w, na.rm = TRUE),
  p25_price      = quantile(price_w, 0.25, na.rm = TRUE),
  p75_price      = quantile(price_w, 0.75, na.rm = TRUE),
  n_sheriff_deed = sum(is_sheriff_deed),
  pct_sheriff    = round(100 * mean(is_sheriff_deed), 1)
), by = year][order(year)]
fwrite(tab1, file.path(out_dir, "transfer_summary_by_year.csv"))
logf("  Table 1: transfer_summary_by_year.csv (", nrow(tab1), " rows)", log_file = log_file)

# Table 2: transfer_summary_by_type.csv
tab2 <- dt[!is.na(building_type), .(
  n_transfers      = .N,
  median_price     = as.double(median(price_w, na.rm = TRUE)),
  p25_price        = as.double(quantile(price_w, 0.25, na.rm = TRUE)),
  p75_price        = as.double(quantile(price_w, 0.75, na.rm = TRUE)),
  median_units     = as.double(median(total_units, na.rm = TRUE)),
  median_price_per_unit = as.double(median(price_per_unit, na.rm = TRUE)),
  n_sheriff_deed   = sum(is_sheriff_deed),
  pct_sheriff      = round(100 * mean(is_sheriff_deed), 1)
), by = building_type][order(-n_transfers)]
fwrite(tab2, file.path(out_dir, "transfer_summary_by_type.csv"))
logf("  Table 2: transfer_summary_by_type.csv (", nrow(tab2), " rows)", log_file = log_file)

# Table 3: transfer_summary_by_year_type.csv
tab3 <- dt[!is.na(building_type), .(
  n_transfers  = .N,
  median_price = median(price_w, na.rm = TRUE)
), by = .(year, building_type)][order(year, building_type)]
fwrite(tab3, file.path(out_dir, "transfer_summary_by_year_type.csv"))
logf("  Table 3: transfer_summary_by_year_type.csv (", nrow(tab3), " rows)", log_file = log_file)

# Figure 1: rtt_transfers_by_year.png
fig1_dt <- dt[, .(n = .N), by = .(year, deed_type = fifelse(is_sheriff_deed, "Sheriff's Deed", "Regular Deed"))]
p1_fig <- ggplot(fig1_dt, aes(x = year, y = n, fill = deed_type)) +
  geom_col() +
  labs(title = "RTT Transfers by Year",
       subtitle = "Rental properties matched to bldg_panel_blp",
       x = "Year", y = "Number of Transfers", fill = "Deed Type") +
  theme_philly_evict() +
  scale_fill_manual(values = c("Regular Deed" = "steelblue", "Sheriff's Deed" = "firebrick"))
ggsave(file.path(fig_dir, "rtt_transfers_by_year.png"), p1_fig, width = 12, height = 8, dpi = 150)
logf("  Figure 1: rtt_transfers_by_year.png", log_file = log_file)

# Figure 2: rtt_price_by_building_type.png
fig2_dt <- dt[!is.na(building_type) & !is.na(price_w) & price_w > 0]
p2_fig <- ggplot(fig2_dt, aes(x = reorder(building_type, log(price_w), FUN = median), y = log(price_w))) +
  geom_boxplot(fill = "steelblue", alpha = 0.7, outlier.size = 0.5) +
  coord_flip() +
  labs(title = "Sale Price by Building Type",
       subtitle = "Log(winsorized price)",
       x = "Building Type", y = "Log(Price)") +
  theme_philly_evict()
ggsave(file.path(fig_dir, "rtt_price_by_building_type.png"), p2_fig, width = 12, height = 8, dpi = 150)
logf("  Figure 2: rtt_price_by_building_type.png", log_file = log_file)

# ============================================================
# SECTION 2: Transfer Frequency
# ============================================================
logf("--- SECTION 2: Transfer frequency ---", log_file = log_file)

# Count transfers per PID
pid_freq <- dt[, .(
  n_transfers = .N,
  years       = list(sort(unique(year)))
), by = PID]

# Compute inter-sale intervals
pid_freq[, median_interval := sapply(years, function(yrs) {
  if (length(yrs) < 2) return(NA_real_)
  median(diff(sort(yrs)))
})]
pid_freq[, years := NULL]

# Merge building-level characteristics (use first obs)
pid_chars <- dt[, .(
  building_type = building_type[1],
  total_units   = total_units[1],
  high_evict    = fifelse(any(!is.na(high_evict_eb_city)), any(high_evict_eb_city == TRUE, na.rm = TRUE), NA)
), by = PID]

pid_freq <- merge(pid_freq, pid_chars, by = "PID", all.x = TRUE)
pid_freq[, flipped := n_transfers >= 2]

# Table 4: transfer_frequency_by_type.csv
tab4 <- pid_freq[!is.na(building_type), .(
  n_buildings        = .N,
  mean_transfers     = round(mean(n_transfers), 2),
  median_transfers   = as.double(median(n_transfers)),
  pct_flipped        = round(100 * mean(flipped), 1)
), by = building_type][order(-n_buildings)]
fwrite(tab4, file.path(out_dir, "transfer_frequency_by_type.csv"))
logf("  Table 4: transfer_frequency_by_type.csv (", nrow(tab4), " rows)", log_file = log_file)

# Table 5: transfer_frequency_by_eviction_status.csv
tab5 <- pid_freq[!is.na(high_evict), .(
  n_buildings          = .N,
  mean_transfers       = round(mean(n_transfers), 2),
  median_transfers     = as.double(median(n_transfers)),
  median_interval_yrs  = round(median(median_interval, na.rm = TRUE), 1),
  pct_flipped          = round(100 * mean(flipped), 1)
), by = .(high_evict)]
fwrite(tab5, file.path(out_dir, "transfer_frequency_by_eviction_status.csv"))
logf("  Table 5: transfer_frequency_by_eviction_status.csv", log_file = log_file)
for (i in seq_len(nrow(tab5))) {
  flag_small(tab5$n_buildings[i], paste0("high_evict=", tab5$high_evict[i]))
}

# Figure 3: rtt_transfer_freq_by_eviction.png
fig3_dt <- pid_freq[!is.na(high_evict)]
fig3_dt[, evict_label := fifelse(high_evict, "High-Evicting", "Not High-Evicting")]
p3_fig <- ggplot(fig3_dt, aes(x = n_transfers, fill = evict_label)) +
  geom_histogram(binwidth = 1, position = "dodge", alpha = 0.8) +
  labs(title = "Transfer Frequency by Eviction Status",
       subtitle = "Number of sales per building (PID)",
       x = "Number of Transfers", y = "Number of Buildings", fill = "") +
  theme_philly_evict() +
  scale_fill_manual(values = c("High-Evicting" = "firebrick", "Not High-Evicting" = "steelblue"))
ggsave(file.path(fig_dir, "rtt_transfer_freq_by_eviction.png"), p3_fig, width = 12, height = 8, dpi = 150)
logf("  Figure 3: rtt_transfer_freq_by_eviction.png", log_file = log_file)

# ============================================================
# SECTION 3: High-Evicting Building Transfers
# ============================================================
logf("--- SECTION 3: High-evicting building transfers ---", log_file = log_file)

# Table 6: high_evict_transfer_comparison.csv
tab6 <- dt[!is.na(high_evict_eb_city), .(
  n_transfers             = .N,
  n_pids                  = uniqueN(PID),
  transfer_rate_per_bldg_yr = round(.N / uniqueN(PID), 3),
  median_price            = median(price_w, na.rm = TRUE),
  median_price_per_unit   = median(price_per_unit, na.rm = TRUE),
  pct_sheriff             = round(100 * mean(is_sheriff_deed), 1),
  share_violations       = as.double(mean(total_violations > 1, na.rm = TRUE))
), by = .(high_evict_eb_city)]
fwrite(tab6, file.path(out_dir, "high_evict_transfer_comparison.csv"))
logf("  Table 6: high_evict_transfer_comparison.csv", log_file = log_file)
for (i in seq_len(nrow(tab6))) {
  flag_small(tab6$n_transfers[i], paste0("high_evict_eb_city=", tab6$high_evict_eb_city[i]))
}

# Table 7: transfer_by_filing_rate_bin.csv
tab7 <- dt[!is.na(filing_rate_eb_cuts), .(
  n_transfers             = .N,
  n_pids                  = uniqueN(PID),
  transfer_rate_per_bldg_yr = round(.N / uniqueN(PID), 3),
  median_price            = median(price_w, na.rm = TRUE),
  median_price_per_unit   = median(price_per_unit, na.rm = TRUE),
  pct_sheriff             = round(100 * mean(is_sheriff_deed), 1),
  share_violations       = as.double(mean(total_violations > 1, na.rm = TRUE))
), by = .(filing_rate_eb_cuts)][order(filing_rate_eb_cuts)]
fwrite(tab7, file.path(out_dir, "transfer_by_filing_rate_bin.csv"))
logf("  Table 7: transfer_by_filing_rate_bin.csv (", nrow(tab7), " rows)", log_file = log_file)
for (i in seq_len(nrow(tab7))) {
  flag_small(tab7$n_transfers[i], paste0("filing_rate_bin=", tab7$filing_rate_eb_cuts[i]))
}

# Figure 4: rtt_price_per_unit_by_filing_rate.png
fig4_dt <- dt[!is.na(filing_rate_eb_cuts) & !is.na(price_per_unit) & price_per_unit > 0]
fig4_means <- fig4_dt[, .(
  mean_log_ppu = mean(log(price_per_unit), na.rm = TRUE),
  se           = sd(log(price_per_unit), na.rm = TRUE) / sqrt(.N),
  n            = .N
), by = filing_rate_eb_cuts][order(filing_rate_eb_cuts)]

p4_fig <- ggplot(fig4_means, aes(x = filing_rate_eb_cuts, y = mean_log_ppu)) +
  geom_col(fill = "steelblue", alpha = 0.8) +
  geom_errorbar(aes(ymin = mean_log_ppu - 1.96 * se, ymax = mean_log_ppu + 1.96 * se),
                width = 0.2) +
  labs(title = "Sale Price per Unit by Filing Rate",
       subtitle = "Mean log(price/unit), 95% CI",
       x = "Filing Rate Bin (EB, pre-COVID)", y = "Mean Log(Price/Unit)") +
  theme_philly_evict()
ggsave(file.path(fig_dir, "rtt_price_per_unit_by_filing_rate.png"), p4_fig, width = 12, height = 8, dpi = 150)
logf("  Figure 4: rtt_price_per_unit_by_filing_rate.png", log_file = log_file)

# Figure 5: rtt_sheriff_share_by_eviction_bin.png
fig5_dt <- dt[!is.na(filing_rate_eb_cuts), .(
  pct_sheriff = 100 * mean(is_sheriff_deed),
  n = .N
), by = filing_rate_eb_cuts][order(filing_rate_eb_cuts)]

p5_fig <- ggplot(fig5_dt, aes(x = filing_rate_eb_cuts, y = pct_sheriff)) +
  geom_col(fill = "firebrick", alpha = 0.8) +
  labs(title = "Sheriff's Deed Share by Filing Rate Bin",
       x = "Filing Rate Bin (EB, pre-COVID)", y = "Percent Sheriff's Deed") +
  theme_philly_evict()
ggsave(file.path(fig_dir, "rtt_sheriff_share_by_eviction_bin.png"), p5_fig, width = 12, height = 8, dpi = 150)
logf("  Figure 5: rtt_sheriff_share_by_eviction_bin.png", log_file = log_file)

# ============================================================
# SECTION 4: Who Buys High-Evicting Buildings?
# ============================================================
logf("--- SECTION 4: Buyer composition ---", log_file = log_file)

# 4a: Corporate vs individual buyers
# NOTE: clean_name() has a bug (bare "|" treated as regex, corrupts names).
# Classify corp/individual directly from raw uppercased names using business_regex.
dt[, grantee_upper := toupper(trimws(as.character(grantees)))]
dt[, grantor_upper := toupper(trimws(as.character(grantors)))]
dt[, buyer_is_corp  := str_detect(grantee_upper, business_regex)]
dt[, seller_is_corp := str_detect(grantor_upper, business_regex)]

dt[, buyer_type  := fifelse(buyer_is_corp, "Corporate", "Individual")]
dt[, seller_type := fifelse(seller_is_corp, "Corporate", "Individual")]
dt[, transition  := paste0(seller_type, " -> ", buyer_type)]

# Table 8: buyer_type_by_eviction_status.csv
tab8_buyer <- dt[!is.na(high_evict_eb_city), .(
  n_transfers      = .N,
  pct_corp_buyer   = round(100 * mean(buyer_is_corp, na.rm = TRUE), 1),
  pct_corp_seller  = round(100 * mean(seller_is_corp, na.rm = TRUE), 1)
), by = .(high_evict_eb_city)]

tab8_trans <- dt[!is.na(high_evict_eb_city), .(n = .N), by = .(high_evict_eb_city, transition)]
tab8_trans[, pct := round(100 * n / sum(n), 1), by = high_evict_eb_city]

# Write both to one file with a transition matrix
tab8 <- merge(
  tab8_buyer,
  dcast(tab8_trans, high_evict_eb_city ~ transition, value.var = "pct", fill = 0),
  by = "high_evict_eb_city",
  all.x = TRUE
)
fwrite(tab8, file.path(out_dir, "buyer_type_by_eviction_status.csv"))
logf("  Table 8: buyer_type_by_eviction_status.csv", log_file = log_file)

# 4b: Repeat buyers
# Table 9: top_buyers_high_evict.csv
high_evict_transfers <- dt[high_evict_eb_city == TRUE]
if (nrow(high_evict_transfers) > 0) {
  tab9 <- high_evict_transfers[, .(
    n_acquisitions      = .N,
    n_pids              = uniqueN(PID),
    total_units_acquired = sum(total_units, na.rm = TRUE),
    median_price_per_unit = median(price_per_unit, na.rm = TRUE)
  ), by = .(buyer = grantee_upper)][order(-n_acquisitions)][1:min(.N, 20)]
  fwrite(tab9, file.path(out_dir, "top_buyers_high_evict.csv"))
  logf("  Table 9: top_buyers_high_evict.csv (", nrow(tab9), " rows)", log_file = log_file)
} else {
  logf("  WARNING: No high-evicting transfers found for Table 9", log_file = log_file)
}

# Table 10: repeat_buyer_summary.csv
buyer_counts <- dt[, .(n_purchases = .N), by = grantee_upper]
dt <- merge(dt, buyer_counts, by = "grantee_upper", all.x = TRUE)
dt[, repeat_buyer := n_purchases >= 2]

tab10 <- dt[, .(
  n_transfers               = .N,
  n_buyers                  = uniqueN(grantee_upper),
  avg_filing_rate           = round(mean(filing_rate_eb_pre_covid, na.rm = TRUE), 4),
  avg_violations            = round(mean(total_violations, na.rm = TRUE), 2),
  median_price_per_unit     = median(price_per_unit, na.rm = TRUE)
), by = .(repeat_buyer)][order(repeat_buyer)]
fwrite(tab10, file.path(out_dir, "repeat_buyer_summary.csv"))
logf("  Table 10: repeat_buyer_summary.csv", log_file = log_file)

# 4c: Post-transfer eviction behavior
logf("  Computing pre/post transfer filing rates...", log_file = log_file)

# For each transfer, get filing rates t-2..t-1 and t+1..t+2 from bldg_panel
transfer_pids <- unique(dt[, .(PID, transfer_year = year)])

# Build event-time panel: t-3 to t+3
event_rows <- list()
for (offset in -3:3) {
  tmp <- copy(transfer_pids)
  tmp[, event_time := offset]
  tmp[, year := transfer_year + offset]
  event_rows[[length(event_rows) + 1]] <- tmp
}
event_panel <- rbindlist(event_rows)

# Merge in filing rates from bldg_panel
bldg_filings <- bldg[, .(PID, year, num_filings, filing_rate_raw, total_units)]
event_panel <- merge(event_panel, bldg_filings, by = c("PID", "year"), all.x = TRUE)

# Merge transfer-level characteristics
transfer_chars <- dt[, .(
  PID, transfer_year = year,
  high_evict_at_sale = high_evict_eb_city,
  buyer_type, seller_type, is_sheriff_deed
)]
event_panel <- merge(event_panel, transfer_chars, by = c("PID", "transfer_year"), all.x = TRUE, allow.cartesian = TRUE)

# Compute filing rate per unit for event study
event_panel[, filing_rate := fifelse(
  !is.na(num_filings) & !is.na(total_units) & total_units > 0,
  num_filings / total_units,
  NA_real_
)]

# Table 11: post_transfer_eviction_change.csv
pre_post <- event_panel[!is.na(filing_rate)]
pre_dt  <- pre_post[event_time %in% c(-2, -1), .(pre_rate  = mean(filing_rate, na.rm = TRUE)), by = .(PID, transfer_year)]
post_dt <- pre_post[event_time %in% c(1, 2),   .(post_rate = mean(filing_rate, na.rm = TRUE)), by = .(PID, transfer_year)]
pp <- merge(pre_dt, post_dt, by = c("PID", "transfer_year"))
pp <- merge(pp, unique(transfer_chars), by = c("PID", "transfer_year"), all.x = TRUE)

tab11_parts <- list()

# By high_evict_at_sale
if (pp[!is.na(high_evict_at_sale), .N] > 0) {
  t11a <- pp[!is.na(high_evict_at_sale), .(
    n = .N,
    mean_pre_rate  = round(mean(pre_rate, na.rm = TRUE), 4),
    mean_post_rate = round(mean(post_rate, na.rm = TRUE), 4),
    mean_change    = round(mean(post_rate - pre_rate, na.rm = TRUE), 4)
  ), by = .(group = paste0("high_evict=", high_evict_at_sale))]
  tab11_parts[[1]] <- t11a
}

# By buyer_type
if (pp[!is.na(buyer_type), .N] > 0) {
  t11b <- pp[!is.na(buyer_type), .(
    n = .N,
    mean_pre_rate  = round(mean(pre_rate, na.rm = TRUE), 4),
    mean_post_rate = round(mean(post_rate, na.rm = TRUE), 4),
    mean_change    = round(mean(post_rate - pre_rate, na.rm = TRUE), 4)
  ), by = .(group = paste0("buyer=", buyer_type))]
  tab11_parts[[2]] <- t11b
}

# By deed_type
if (pp[!is.na(is_sheriff_deed), .N] > 0) {
  t11c <- pp[!is.na(is_sheriff_deed), .(
    n = .N,
    mean_pre_rate  = round(mean(pre_rate, na.rm = TRUE), 4),
    mean_post_rate = round(mean(post_rate, na.rm = TRUE), 4),
    mean_change    = round(mean(post_rate - pre_rate, na.rm = TRUE), 4)
  ), by = .(group = paste0("sheriff_deed=", is_sheriff_deed))]
  tab11_parts[[3]] <- t11c
}

tab11 <- rbindlist(tab11_parts)
fwrite(tab11, file.path(out_dir, "post_transfer_eviction_change.csv"))
logf("  Table 11: post_transfer_eviction_change.csv (", nrow(tab11), " rows)", log_file = log_file)
for (i in seq_len(nrow(tab11))) {
  flag_small(tab11$n[i], tab11$group[i])
}

# Figure 6: rtt_eviction_around_transfer.png
fig6_dt <- event_panel[!is.na(filing_rate) & !is.na(high_evict_at_sale)]
fig6_means <- fig6_dt[, .(
  mean_rate = mean(filing_rate, na.rm = TRUE),
  se        = sd(filing_rate, na.rm = TRUE) / sqrt(.N),
  n         = .N
), by = .(event_time, high_evict_label = fifelse(high_evict_at_sale, "High-Evicting", "Not High-Evicting"))]

p6_fig <- ggplot(fig6_means, aes(x = event_time, y = mean_rate, color = high_evict_label)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  geom_ribbon(aes(ymin = mean_rate - 1.96 * se, ymax = mean_rate + 1.96 * se, fill = high_evict_label),
              alpha = 0.15, color = NA) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "gray40") +
  scale_x_continuous(breaks = -3:3) +
  labs(title = "Eviction Filing Rate Around Ownership Transfer",
       subtitle = "Event time: 0 = year of sale",
       x = "Years Relative to Transfer", y = "Filing Rate (per unit)", color = "", fill = "") +
  theme_philly_evict() +
  scale_color_manual(values = c("High-Evicting" = "firebrick", "Not High-Evicting" = "steelblue")) +
  scale_fill_manual(values = c("High-Evicting" = "firebrick", "Not High-Evicting" = "steelblue"))
ggsave(file.path(fig_dir, "rtt_eviction_around_transfer.png"), p6_fig, width = 12, height = 8, dpi = 150)
logf("  Figure 6: rtt_eviction_around_transfer.png", log_file = log_file)

# 4d: Sheriff's deed profile
# Table 12: sheriff_deed_profile.csv
tab12 <- dt[, .(
  n_transfers         = .N,
  median_units        = as.double(median(total_units, na.rm = TRUE)),
  median_violations   = as.double(median(total_violations, na.rm = TRUE)),
  mean_filing_rate    = round(mean(filing_rate_eb_pre_covid, na.rm = TRUE), 4),
  median_rent         = median(med_rent, na.rm = TRUE),
  median_price        = median(price_w, na.rm = TRUE),
  median_price_per_unit = median(price_per_unit, na.rm = TRUE)
), by = .(deed_type = fifelse(is_sheriff_deed, "Sheriff", "Regular"))]
fwrite(tab12, file.path(out_dir, "sheriff_deed_profile.csv"))
logf("  Table 12: sheriff_deed_profile.csv", log_file = log_file)

# ============================================================
# SECTION 5: Descriptive Price Regressions (fixest::feols)
# ============================================================
# NOTE: These are descriptive associations, not causal estimates.
# High-evicting buildings differ on many unobservables (condition,
# tenant composition, management quality) that also affect price.
# ============================================================
logf("--- SECTION 5: Descriptive price regressions ---", log_file = log_file)

# Prep regression data
reg_dt <- dt[!is.na(price_w) & price_w > 0 &
               !is.na(total_units) & total_units > 0 &
               !is.na(building_type)]
reg_dt[, log_price := log(price_w)]
reg_dt[, log_units := log(total_units)]
reg_dt[, log_violations_p1 := log(pmax(total_violations, 0, na.rm = TRUE) + 1)]
reg_dt[, high_evict_num := as.numeric(high_evict_eb_city)]
reg_dt[, log_rent := fifelse(!is.na(med_rent) & med_rent > 0, log(med_rent), NA_real_)]

has_geoid <- "GEOID" %in% names(reg_dt)
if (has_geoid) {
  n_bgs <- reg_dt[!is.na(GEOID), uniqueN(GEOID)]
  logf("  GEOID (block group) available: ", n_bgs, " block groups", log_file = log_file)
} else {
  logf("  WARNING: GEOID not found in data, skipping block group FE specs", log_file = log_file)
}

# Helper to extract fixest coefs (excluding absorbed FE dummies)
extract_feols_coefs <- function(fit, spec_label) {
  ct <- as.data.table(coeftable(fit), keep.rownames = "term")
  setnames(ct, c("term", "estimate", "std_error", "t_value", "p_value"))
  ct[, estimate  := round(estimate, 4)]
  ct[, std_error := round(std_error, 4)]
  ct[, t_value   := round(t_value, 2)]
  ct[, p_value   := round(p_value, 4)]
  ct[, spec := spec_label]
  ct[, n := fit$nobs]
  ct[, r2 := round(fixest::r2(fit, "ar2"), 4)]
  ct
}

log_spec <- function(ct, label) {
  he <- ct[grepl("high_evict", term)]
  logf("  [", label, "] adj-R2=", ct$r2[1], ", N=", ct$n[1], log_file = log_file)
  if (nrow(he) > 0) {
    logf("    high_evict: ", he$estimate, " (se=", he$std_error, ", p=", he$p_value, ")",
         log_file = log_file)
  }
  fr <- ct[grepl("filing_rate_eb_pre_covid$", term)]
  if (nrow(fr) > 0) {
    logf("    filing_rate_eb: ", fr$estimate, " (se=", fr$std_error, ")", log_file = log_file)
  }
  lr <- ct[grepl("log_rent", term)]
  if (nrow(lr) > 0) {
    logf("    log_rent: ", lr$estimate, " (se=", lr$std_error, ")", log_file = log_file)
  }
}

all_coefs <- list()

# --- Spec 1: Year FE only, building controls ---
# log(price) ~ high_evict + log(units) + violations | year + building_type +
#   year_blt_decade + quality_grade + num_units_bin + num_stories_bin
logf("  Estimating Spec 1: year FE + building controls...", log_file = log_file)
fit1 <- feols(
  log_price ~ high_evict_num + log_units + log_violations_p1
  | year + building_type + year_blt_decade + quality_grade + num_units_bin + num_stories_bin,
  data = reg_dt,
  cluster = ~PID
)
c1 <- extract_feols_coefs(fit1, "year_fe")
log_spec(c1, "year_fe")
all_coefs[[1]] <- c1

# --- Spec 2: Block group x year FE + building controls ---
if (has_geoid) {
  logf("  Estimating Spec 2: GEOID x year FE + building controls...", log_file = log_file)
  fit2 <- feols(
    log_price ~ high_evict_num + log_units + log_violations_p1
    | GEOID^year + building_type + year_blt_decade + quality_grade + num_units_bin + num_stories_bin,
    data = reg_dt[!is.na(GEOID)],
    cluster = ~PID
  )
  c2 <- extract_feols_coefs(fit2, "bg_x_year_fe")
  log_spec(c2, "bg_x_year_fe")
  all_coefs[[2]] <- c2
}

# --- Spec 3: Same as Spec 2 but with continuous filing rate instead of binary ---
if (has_geoid) {
  logf("  Estimating Spec 3: GEOID x year FE, continuous filing rate...", log_file = log_file)
  fit3 <- feols(
    log_price ~ filing_rate_eb_pre_covid + log_units + log_violations_p1
    | GEOID^year + building_type + year_blt_decade + quality_grade + num_units_bin + num_stories_bin,
    data = reg_dt[!is.na(GEOID) & !is.na(filing_rate_eb_pre_covid)],
    cluster = ~PID
  )
  c3 <- extract_feols_coefs(fit3, "bg_x_year_fe_cont")
  log_spec(c3, "bg_x_year_fe_cont")
  all_coefs[[3]] <- c3
}

# --- Spec 4: Block group x year FE + building controls + log(rent) ---
if (has_geoid) {

  # residualize out year fixed effects from log_rent
  reg_dt[, log_rent_resid := log_rent - mean(log_rent, na.rm = TRUE), by = year]
  reg_dt[,longrun_rent := mean(log_rent_resid,na.rm = T), by = .(PID)]
  reg_dt[,price_to_rent_ratio := log_price - longrun_rent]
   reg_dt_rent <- reg_dt[!is.na(GEOID) & !is.na(longrun_rent)]
  logf("  Estimating Spec 4: GEOID x year FE + controls + log(longrun_rent) (N=",
       nrow(reg_dt_rent), ")...", log_file = log_file)
  fit4 <- feols(
    log_price ~ high_evict_num + log_units + longrun_rent
    |  GEOID + year + building_type + year_blt_decade  + num_units_bin + num_stories_bin,
    data = reg_dt_rent,
    cluster = ~PID
  )
  c4 <- extract_feols_coefs(fit4, "bg_x_year_fe_rent")
  log_spec(c4, "bg_x_year_fe_rent")
  all_coefs[[4]] <- c4
}

# --- Spec 5: Same as Spec 4 but continuous filing rate + rent ---
if (has_geoid) {
  reg_dt_rent2 <- reg_dt[!is.na(GEOID) & !is.na(log_rent) & !is.na(filing_rate_eb_pre_covid)]
  logf("  Estimating Spec 5: GEOID x year FE + cont filing rate + log(rent) (N=",
       nrow(reg_dt_rent2), ")...", log_file = log_file)
  fit5 <- feols(
    log_price ~ filing_rate_eb_pre_covid + log_units + log_violations_p1 + log_rent
    | GEOID^year + building_type + year_blt_decade + quality_grade + num_units_bin + num_stories_bin,
    data = reg_dt_rent2,
    cluster = ~PID
  )
  c5 <- extract_feols_coefs(fit5, "bg_x_year_fe_cont_rent")
  log_spec(c5, "bg_x_year_fe_cont_rent")
  all_coefs[[5]] <- c5
}

# Combine and write
if (length(all_coefs) > 0) {
  tab13 <- rbindlist(all_coefs)
  fwrite(tab13, file.path(out_dir, "price_hedonic_summary.csv"))
  logf("  Table 13: price_hedonic_summary.csv (", nrow(tab13), " rows, ",
       uniqueN(tab13$spec), " specs)", log_file = log_file)

  # Also produce a nice etable for LaTeX
  fits_list <- list()
  if (exists("fit1")) fits_list[["(1)"]] <- fit1
  if (exists("fit2")) fits_list[["(2)"]] <- fit2
  if (exists("fit3")) fits_list[["(3)"]] <- fit3
  if (exists("fit4")) fits_list[["(4)"]] <- fit4
  if (exists("fit5")) fits_list[["(5)"]] <- fit5
  if (length(fits_list) > 0) {
    tex_out <- file.path(out_dir, "price_hedonic_etable.tex")
    etab <- etable(fits_list, tex = TRUE,
                   title = "Descriptive Sale Price Regressions",
                   fitstat = c("n", "ar2"),
                   digits = 3)
    writeLines(etab, tex_out)
    logf("  LaTeX table: price_hedonic_etable.tex", log_file = log_file)
  }
} else {
  logf("  WARNING: No hedonic regressions could be estimated", log_file = log_file)
}

# Figure 7: rtt_price_per_unit_timeseries.png
fig7_dt <- dt[!is.na(high_evict_eb_city) & !is.na(price_per_unit) & price_per_unit > 0]
fig7_means <- fig7_dt[, .(
  median_ppu = median(price_per_unit, na.rm = TRUE),
  n = .N
), by = .(year, high_evict_label = fifelse(high_evict_eb_city, "High-Evicting", "Not High-Evicting"))]

p7_fig <- ggplot(fig7_means, aes(x = year, y = median_ppu, color = high_evict_label)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  labs(title = "Median Sale Price per Unit Over Time",
       subtitle = "High-evicting vs. other rental buildings",
       x = "Year", y = "Median Price per Unit ($)", color = "") +
  theme_philly_evict() +
  scale_color_manual(values = c("High-Evicting" = "firebrick", "Not High-Evicting" = "steelblue")) +
  scale_y_continuous(labels = scales::comma)
ggsave(file.path(fig_dir, "rtt_price_per_unit_timeseries.png"), p7_fig, width = 12, height = 8, dpi = 150)
logf("  Figure 7: rtt_price_per_unit_timeseries.png", log_file = log_file)


# do p7 as a ratio and facet by building type
fig7_means_bt <- fig7_dt[!is.na(building_type) & building_type != "COMMERCIAL" & building_type != "OTHER", .(
  median_ppu = median(price_per_unit, na.rm = TRUE),
  n = .N
), by = .(year, high_evict_label = fifelse(high_evict_eb_city, "High-Evicting", "Not High-Evicting"), building_type)]
p7_fig_ratio <- ggplot(fig7_means_bt, aes(x = year, y = median_ppu, color = high_evict_label)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  labs(title = "Median Sale Price per Unit Over Time",
       subtitle = "High-evicting vs. other rental buildings",
       x = "Year", y = "Median Price per Unit ($)", color = "") +
  theme_philly_evict() +
  scale_color_manual(values = c("High-Evicting" = "firebrick", "Not High-Evicting" = "steelblue")) +
  scale_y_continuous(labels = scales::comma) +
  facet_wrap(~ building_type, scales = "free")

# ============================================================
# DONE
# ============================================================
logf("=== Finished analyze-rtt-transfers.R ===", log_file = log_file)
logf("  Tables written to: ", out_dir, log_file = log_file)
logf("  Figures written to: ", fig_dir, log_file = log_file)
