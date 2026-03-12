## ============================================================
## make-outer-sorting-empirical-moments.R
## ============================================================
## Purpose: Build empirical target moments for the outer sorting
## calibration from bldg_panel_blp.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
})

source("r/config.R")
source("r/build-renter-poverty-geo.R")
source("r/outer-sorting-moments.R")
source("r/build-outer-sorting-empirical-moments.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "make-outer-sorting-empirical-moments.log")

logf("=== Starting make-outer-sorting-empirical-moments.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

out_path <- p_product(cfg, "outer_sorting_empirical_moments")
dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)

mom_dt <- compute_outer_sorting_empirical_moments(cfg, log_file = log_file)
fwrite(mom_dt, out_path)

logf("Wrote outer_sorting_empirical_moments: ", out_path, " (rows=", nrow(mom_dt), ")", log_file = log_file)
logf("=== Completed make-outer-sorting-empirical-moments.R ===", log_file = log_file)
