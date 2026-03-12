## ============================================================
## make-infousa-building-income-proxy.R
## ============================================================
## Purpose: Thin wrapper to build the PID x year InfoUSA income
## proxy product used by outer-sorting empirical targets.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
})

source("r/config.R")
source("r/build-infousa-building-income-proxy.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "make-infousa-building-income-proxy.log")

logf("=== Starting make-infousa-building-income-proxy.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

out_path <- p_product(cfg, "infousa_building_income_proxy_panel")
dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)

proxy_dt <- build_infousa_building_income_proxy(cfg, log_file = log_file)
fwrite(proxy_dt, out_path)

assert_unique(proxy_dt, c("PID", "year"), "infousa_building_income_proxy_panel")

logf("Wrote infousa_building_income_proxy_panel: ", out_path, " (rows=", nrow(proxy_dt), ")", log_file = log_file)
logf("=== Completed make-infousa-building-income-proxy.R ===", log_file = log_file)
