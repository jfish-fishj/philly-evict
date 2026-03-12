## ============================================================
## run-outer-sorting-maintenance-diagnostics.R
## ============================================================
## Purpose: Thin wrapper to isolate and summarize the maintenance block
## under the current outer-sorting calibration.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(fixest)
})

source("r/config.R")
source("r/outer-sorting-maintenance-diagnostics.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "run-outer-sorting-maintenance-diagnostics.log")

logf("=== Starting run-outer-sorting-maintenance-diagnostics.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

out_dir <- p_out(cfg, "outer_sorting")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_summary <- file.path(out_dir, "outer_sorting_maintenance_block_summary.csv")
out_params <- file.path(out_dir, "outer_sorting_maintenance_block_parameters.csv")
out_grid <- file.path(out_dir, "outer_sorting_maintenance_block_grid.csv")

res <- build_outer_sorting_maintenance_diagnostics(cfg, log_file = log_file)

fwrite(res$summary, out_summary)
fwrite(res$parameters, out_params)
fwrite(res$grid, out_grid)

logf("Wrote maintenance summary: ", out_summary, log_file = log_file)
logf("Wrote maintenance parameters: ", out_params, log_file = log_file)
logf("Wrote maintenance grid: ", out_grid, " (rows=", nrow(res$grid), ")", log_file = log_file)
logf("=== Completed run-outer-sorting-maintenance-diagnostics.R ===", log_file = log_file)
