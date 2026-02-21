# _targets.R
# Pipeline orchestration for Philly evictions project.
#
# Pipeline stages:
#   1. CLEAN    - Raw data → standardized addresses/identifiers
#   2. CROSSWALK - Link datasets via address matching and spatial joins
#   3. AGGREGATE - Build PID × time panels from linked data
#   4. ANALYSIS  - Regressions, figures, tables (not included here)
#       # Run all targets
#   tar_visnetwork()  # View dependency graph

library(targets)

# Source config + runner helpers
source("r/config.R")

tar_option_set(

packages = c(
    "yaml", "fs", "data.table", "dplyr", "stringr",
    "readr", "lubridate", "sf", "fixest", "ggplot2", "glue"
  )
)

cfg_path <- Sys.getenv(
  "PHILLY_EVICTIONS_CONFIG",
  unset = if (file.exists("config.yml")) "config.yml" else "config.example.yml"
)

list(
  # Config as a target so downstream steps re-run if config changes
  tar_target(cfg, read_config(cfg_path), cue = tar_cue(mode = "thorough")),

  # ============================================================
  # STAGE 1: CLEANING
  # These scripts are independent and can run in parallel.
  # ============================================================

  tar_target(
    clean_evictions,
    run_rscript("r/clean-eviction-addresses.R", cfg_path = cfg_path, log_name = "clean-eviction-addresses"),
    cue = tar_cue(mode = "thorough")
  ),

  tar_target(
    clean_parcels,
    run_rscript("r/clean-parcel-addresses.R", cfg_path = cfg_path, log_name = "clean-parcel-addresses"),
    cue = tar_cue(mode = "thorough")
  ),

  tar_target(
    clean_licenses,
    run_rscript("r/clean-license-addresses.R", cfg_path = cfg_path, log_name = "clean-license-addresses"),
    cue = tar_cue(mode = "thorough")
  ),

  tar_target(
    clean_altos,
    run_rscript("r/clean-altos-addresses.R", cfg_path = cfg_path, log_name = "clean-altos-addresses"),
    cue = tar_cue(mode = "thorough")
  ),

  tar_target(
    clean_infousa,
    run_rscript("r/clean-infousa-addresses.R", cfg_path = cfg_path, log_name = "clean-infousa-addresses"),
    cue = tar_cue(mode = "thorough")
  ),

  tar_target(
    clean_eviction_names,
    run_rscript("r/clean-eviction-names.R", cfg_path = cfg_path, log_name = "clean-eviction-names"),
    cue = tar_cue(mode = "thorough")
  ),

  # ============================================================
  # STAGE 2: CROSSWALKS
  # Link cleaned datasets via address matching and spatial joins.
  # Depends on Stage 1 clean outputs.
  # ============================================================

  tar_target(
    xwalk_bldg_pid,
    {
      clean_parcels  # dependency
      run_rscript("r/make_bldg_pid_xwalk.r", cfg_path = cfg_path, log_name = "make_bldg_pid_xwalk")
    },
    cue = tar_cue(mode = "thorough")
  ),

  tar_target(
    xwalk_address_parcel,
    {
      clean_evictions  # dependency
      clean_parcels    # dependency
      clean_altos      # dependency
      clean_infousa    # dependency
      run_rscript("r/make-address-parcel-xwalk.R", cfg_path = cfg_path, log_name = "make-address-parcel-xwalk")
    },
    cue = tar_cue(mode = "thorough")
  ),

  tar_target(
    xwalk_altos_parcels,
    {
      clean_altos    # dependency
      clean_parcels  # dependency
      run_rscript("r/merge-altos-parcels.R", cfg_path = cfg_path, log_name = "merge-altos-parcels")
    },
    cue = tar_cue(mode = "thorough")
  ),

  tar_target(
    xwalk_evict_parcels,
    {
      clean_evictions  # dependency
      clean_parcels    # dependency
      clean_licenses   # dependency
      run_rscript("r/merge-evictions-parcels.R", cfg_path = cfg_path, log_name = "merge-evictions-parcels")
    },
    cue = tar_cue(mode = "thorough")
  ),

  tar_target(
    xwalk_infousa_parcels,
    {
      clean_infousa  # dependency
      clean_parcels  # dependency
      run_rscript("r/merge-infousa-parcels.R", cfg_path = cfg_path, log_name = "merge-infousa-parcels")
    },
    cue = tar_cue(mode = "thorough")
  ),

  # ============================================================
  # STAGE 3: AGGREGATION
  # Build PID × time panels from linked data.
  # Depends on Stage 1 + Stage 2 outputs.
  # ============================================================

  tar_target(
    panel_rent,
    {
      clean_parcels       # dependency
      clean_licenses      # dependency
      xwalk_bldg_pid      # dependency
      xwalk_address_parcel  # dependency
      run_rscript("r/make-rent-panel.R", cfg_path = cfg_path, log_name = "make-rent-panel")
    },
    cue = tar_cue(mode = "thorough")
  ),

  tar_target(
    panel_occupancy,
    {
      panel_rent     # dependency (ever_rentals_panel)
      clean_infousa  # dependency
      run_rscript("r/make-occupancy-vars.r", cfg_path = cfg_path, log_name = "make-occupancy-vars")
    },
    cue = tar_cue(mode = "thorough")
  ),

  tar_target(
    panel_evict_aggs,
    {
      clean_evictions       # dependency
      xwalk_address_parcel  # dependency
      run_rscript("r/make-evict-aggs.R", cfg_path = cfg_path, log_name = "make-evict-aggs")
    },
    cue = tar_cue(mode = "thorough")
  ),

  tar_target(
    panel_building,
    {
      clean_parcels     # dependency
      panel_evict_aggs  # dependency
      run_rscript("r/make-building-data.R", cfg_path = cfg_path, log_name = "make-building-data")
    },
    cue = tar_cue(mode = "thorough")
  ),

  tar_target(
    analytic_sample,
    {
      panel_rent        # dependency
      panel_occupancy   # dependency
      panel_evict_aggs  # dependency
      panel_building    # dependency
      run_rscript("r/make-analytic-sample.R", cfg_path = cfg_path, log_name = "make-analytic-sample")
    },
    cue = tar_cue(mode = "thorough")
  )

  # ============================================================
  # STAGE 4: ANALYSIS
  # Analysis scripts read from processed/ and write to output/.
  # Add analysis targets here if you want them orchestrated by targets.
  # ============================================================
  # tar_target(
  #   analysis_retaliatory,
  #   {
  #     analytic_sample  # dependency
  #     run_rscript("r/retaliatory-evictions.r", cfg_path = cfg_path, log_name = "retaliatory-evictions")
  #   },
  #   cue = tar_cue(mode = "thorough")
  # )
)
