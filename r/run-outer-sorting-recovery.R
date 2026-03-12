## ============================================================
## run-outer-sorting-recovery.R
## ============================================================
## Purpose: Thin wrapper to run outer sorting Monte Carlo recovery.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
})

source("r/config.R")
source("r/build-outer-sorting-params.R")
source("r/simulate-outer-sorting.R")
source("r/outer-sorting-moments.R")
source("r/calibrate-outer-sorting.R")
source("r/outer-sorting-recovery.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "run-outer-sorting-recovery.log")

logf("=== Starting run-outer-sorting-recovery.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

out_dir <- p_out(cfg, "outer_sorting", "recovery")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

res <- run_outer_sorting_recovery(cfg, log_file = log_file)

fwrite(res$replications, file.path(out_dir, "outer_sorting_recovery_replications.csv"))
fwrite(res$summary, file.path(out_dir, "outer_sorting_recovery_summary.csv"))

logf("Wrote recovery outputs to: ", out_dir, log_file = log_file)
logf("=== Completed run-outer-sorting-recovery.R ===", log_file = log_file)
