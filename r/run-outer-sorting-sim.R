## ============================================================
## run-outer-sorting-sim.R
## ============================================================
## Purpose: Thin wrapper to simulate the baseline outer sorting DGP
## and write processed products.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
})

source("r/config.R")
source("r/build-outer-sorting-params.R")
source("r/simulate-outer-sorting.R")
source("r/outer-sorting-moments.R")

cfg <- read_config()
log_file <- p_out(cfg, "logs", "run-outer-sorting-sim.log")

logf("=== Starting run-outer-sorting-sim.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

out_panel <- p_product(cfg, "outer_sorting_sim_panel")
out_moments <- p_product(cfg, "outer_sorting_sim_moments")
out_diagnostics <- p_product(cfg, "outer_sorting_sim_diagnostics")

dir.create(dirname(out_panel), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_moments), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_diagnostics), recursive = TRUE, showWarnings = FALSE)

par <- build_outer_sorting_params(cfg)
logf("Simulation params: B=", par$B,
     ", Nhood=", par$Nhood,
     ", seed=", par$seed,
     ", sim_years=", par$sim_year_from, "-", par$sim_year_to,
     ", mu=", par$mu,
     ", delta_L=", par$delta_L,
     ", delta_H=", par$delta_H,
     ", units_draw_mode=", par$units_draw_mode,
     ", Dalph=", par$Dalph,
     ", sigma_eta_sort=", par$sigma_eta_sort,
     ", aF=", par$aF,
     ", bF_delta=", par$bF_delta,
     ", high_filing_threshold=", par$high_filing_threshold,
     log_file = log_file)

res <- simulate_outer_sorting(par, log_file = log_file)
sim_dt <- res$panel
diag_dt <- res$diagnostics
mom_dt <- compute_outer_sorting_moments(
  sim_dt,
  high_filing_threshold = par$high_filing_threshold,
  log_file = log_file
)

assert_unique(sim_dt, keys = "b", name = "outer_sorting_sim_panel")

fwrite(sim_dt, out_panel)
fwrite(mom_dt, out_moments)
fwrite(diag_dt, out_diagnostics)

logf("Wrote outer_sorting_sim_panel: ", out_panel, " (rows=", nrow(sim_dt), ")", log_file = log_file)
logf("Wrote outer_sorting_sim_moments: ", out_moments, " (rows=", nrow(mom_dt), ")", log_file = log_file)
logf("Wrote outer_sorting_sim_diagnostics: ", out_diagnostics, " (rows=", nrow(diag_dt), ")", log_file = log_file)
logf("=== Completed run-outer-sorting-sim.R ===", log_file = log_file)
