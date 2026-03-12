## ============================================================
## run-calibrate-outer-sorting.R
## ============================================================
## Purpose: Thin wrapper to calibrate baseline outer sorting model
## moments and write optimization outputs.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
})

source("r/config.R")
source("r/build-outer-sorting-params.R")
source("r/simulate-outer-sorting.R")
source("r/outer-sorting-moments.R")
source("r/calibrate-outer-sorting.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "run-calibrate-outer-sorting.log")

logf("=== Starting run-calibrate-outer-sorting.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

out_dir <- p_out(cfg, "outer_sorting")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

out_best_params <- file.path(out_dir, "outer_sorting_calibration_best_params.csv")
out_moment_cmp <- file.path(out_dir, "outer_sorting_calibration_moment_comparison.csv")
out_trace <- file.path(out_dir, "outer_sorting_calibration_optimization_trace.csv")
out_opt_summary <- file.path(out_dir, "outer_sorting_calibration_optimization_summary.csv")
out_best_diag <- file.path(out_dir, "outer_sorting_calibration_best_diagnostics.csv")
out_best_mom <- file.path(out_dir, "outer_sorting_calibration_best_moments.csv")
out_best_panel <- file.path(out_dir, "outer_sorting_calibration_best_panel.csv")

res <- calibrate_outer_sorting(cfg, log_file = log_file)

fwrite(res$best_params, out_best_params)
fwrite(res$moment_comparison, out_moment_cmp)
fwrite(res$optimization_trace, out_trace)
fwrite(res$optimization_summary, out_opt_summary)
fwrite(res$best_diagnostics, out_best_diag)
fwrite(res$best_moments, out_best_mom)
fwrite(res$best_panel, out_best_panel)

assert_unique(res$best_panel, keys = "b", name = "outer_sorting_calibration_best_panel")
logf("Wrote calibration outputs to: ", out_dir, log_file = log_file)
logf("=== Completed run-calibrate-outer-sorting.R ===", log_file = log_file)
