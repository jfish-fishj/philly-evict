## ============================================================
## make-renter-poverty-geo.R
## ============================================================
## Purpose: Thin wrapper to build block-group and tract renter
## poverty share products.
## ============================================================

suppressPackageStartupMessages({
  library(data.table)
})

source("r/config.R")
source("r/build-renter-poverty-geo.R")

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

opts <- parse_cli_args(commandArgs(trailingOnly = TRUE))

cfg <- read_config()
log_file <- p_out(cfg, "logs", "make-renter-poverty-geo.log")

logf("=== Starting make-renter-poverty-geo.R ===", log_file = log_file)
logf("Config: ", cfg$meta$config_path, log_file = log_file)

out_bg <- p_product(cfg, "bg_renter_poverty_share")
out_tract <- p_product(cfg, "tract_renter_poverty_share")
dir.create(dirname(out_bg), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(out_tract), recursive = TRUE, showWarnings = FALSE)

input_path <- opts$input %||% NULL
if (!is.null(input_path) && nzchar(input_path) && !grepl("^(/|[A-Za-z]:[\\/])", input_path)) {
  input_path <- file.path(cfg$paths$input_dir, input_path)
}

res <- build_renter_poverty_geo(
  cfg = cfg,
  input_path = input_path,
  input_geography = opts$input_geography %||% NULL,
  log_file = log_file
)

fwrite(res$bg, out_bg)
fwrite(res$tract, out_tract)

assert_unique(res$bg, "bg_geoid", "bg_renter_poverty_share")
assert_unique(res$tract, "tract_geoid", "tract_renter_poverty_share")

logf("Wrote bg_renter_poverty_share: ", out_bg, " (rows=", nrow(res$bg), ")", log_file = log_file)
logf("Wrote tract_renter_poverty_share: ", out_tract, " (rows=", nrow(res$tract), ")", log_file = log_file)
logf("=== Completed make-renter-poverty-geo.R ===", log_file = log_file)
